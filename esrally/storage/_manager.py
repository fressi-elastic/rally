# Licensed to Elasticsearch B.V. under one or more contributor
# license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright
# ownership. Elasticsearch B.V. licenses this file to you under
# the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# 	http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

import concurrent.futures
import logging
import os
import threading
import urllib.parse

from esrally import config
from esrally.storage._adapter import Head
from esrally.storage._client import Client
from esrally.storage._range import NO_RANGE, RangeSet
from esrally.storage._transfer import Transfer, TransferDirection
from esrally.utils.threads import ContinuousTimer, WorkersLimitError

LOG = logging.getLogger(__name__)

LOCAL_DIR = "~/.rally/storage"
MONITOR_INTERVAL = 5.0  # Seconds
THREAD_NAME_PREFIX = "esrally.storage.transfer-worker"
MULTIPART_SIZE = 64 * 1024 * 1024
MAX_WORKERS = 16


class Manager:
    """It creates and perform file transfer operations in background."""

    def __init__(
        self,
        client: Client,
        local_dir: str = LOCAL_DIR,
        monitor_interval: float = MONITOR_INTERVAL,
        multipart_size: int = MULTIPART_SIZE,
        max_workers: int = MAX_WORKERS,
    ):
        """It manages files transfers.

        It executes file transfers in background. It periodically logs the status of transfers that are in progress.

        :param local_dir: default directory used to download files to, or upload files from.
        :param monitor_interval: time interval (in seconds) separating background calls to the _monitor method.
        :param client: _client.Client instance used to allocate/reuse storage adapters.
        :param multipart_size: length of every part when working with multipart.
        :param max_workers: max number of connections per remote server when working with multipart.
        """
        self._client = client
        self._lock = threading.Lock()

        local_dir = os.path.expanduser(local_dir)
        if not os.path.isdir(local_dir):
            os.makedirs(local_dir)
        self._local_dir = local_dir
        if monitor_interval <= 0:
            raise ValueError(f"invalid monitor interval: {monitor_interval}")
        self._transfers: list[Transfer] = []
        self._monitor_timer = ContinuousTimer(interval=monitor_interval, function=self._monitor, name="esrally.storage.transfer-monitor")
        self._monitor_timer.start()
        self._multipart_size = multipart_size
        self._executor: concurrent.futures.Executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=max_workers, thread_name_prefix="esrally.storage.transfer-worker"
        )

    def shutdown(self):
        with self._lock:
            transfers = self._transfers
            self._transfers = []
        for tr in transfers:
            tr.close()
        self._monitor_timer.cancel()

    @classmethod
    def from_config(cls, cfg: config.Config) -> Manager:
        """It creates a TransferManager with initialization values taken from given configuration."""
        local_dir = cfg.opts(section="storage", key="storage.local_dir", default_value=LOCAL_DIR, mandatory=False)
        monitor_interval = cfg.opts(section="storage", key="storage.monitor_interval", default_value=MONITOR_INTERVAL, mandatory=False)
        max_workers = cfg.opts(section="storage", key="storage.max_workers", default_value=MAX_WORKERS, mandatory=False)
        multipart_size = cfg.opts(section="storage", key="storage.multipart_size", default_value=MULTIPART_SIZE, mandatory=False)
        return cls(
            local_dir=local_dir,
            monitor_interval=float(monitor_interval),
            multipart_size=multipart_size,
            max_workers=int(max_workers),
            client=Client.from_config(cfg),
        )

    def _head(self, url: str) -> Head:
        return self._client.get(url).head()

    def get(self, url: str, path: os.PathLike | str | None = None, ranges: RangeSet = NO_RANGE) -> Transfer:
        """It starts a new transfer of a file from local path to a remote url.

        :param url: remote file address.
        :param path: local file address.
        :param ranges: the part of the file to download
        :return: started transfer object.
        """
        head = self._head(url)
        tr = self._transfer(TransferDirection.DOWN, head, path, ranges)
        tr.submit(self._get, tr)
        return tr

    def _get(self, tr: Transfer):
        with tr.write_file(self._multipart_size) as stream:
            if tr.accept_ranges and tr.todo:
                try:
                    tr.submit(self._get, tr)
                except WorkersLimitError:
                    pass
            self._client.get(tr.url).get(stream, stream.range)

    # def put(self, url: str, path: os.PathLike | str | None) -> _transfer.Transfer:
    #     def put(tr: _transfer.Transfer):
    #         cl = self._client.get(urllib.parse.urlparse(tr.url).scheme)
    #         tr.size = os.path.getsize(tr.path)
    #         with tr.read_file() as fd:
    #             cl.put(url, fd, tr.range)
    #
    #     return self._submit(put, _transfer.TransferDirection.UP, url, path)

    def _transfer(
        self,
        direction: TransferDirection,
        head: Head,
        path: os.PathLike | str | None = None,
        ranges: RangeSet = NO_RANGE,
        max_workers: int = MAX_WORKERS,
    ) -> Transfer:
        if path is None:
            u = urllib.parse.urlparse(head.url)
            path = os.path.join(self._local_dir, u.netloc + u.path)
        # This also ensures the path is a string
        path = os.path.normpath(os.path.expanduser(path))
        tr = Transfer(path=path, direction=direction, head=head, executor=self._executor, ranges=ranges, max_workers=max_workers)
        with self._lock:
            self._transfers.append(tr)
        return tr

    def _monitor(self):
        with self._lock:
            transfers = self._transfers
            # It removes finished transfers
            self._transfers = [tr for tr in transfers if not tr.finished.is_set()]
        if transfers:
            LOG.info("Transfers in progress:\n  %s", "\n  ".join(tr.info() for tr in transfers))
