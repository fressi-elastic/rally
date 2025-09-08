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

import dataclasses
import datetime
import logging
import os
import time

from thespian import actors  # type: ignore[import-untyped]
from typing_extensions import Self

from esrally import actor, types, exceptions
from esrally.actor import ActorConfig
from esrally.storage._client import Client
from esrally.storage._config import StorageConfig
from esrally.storage._transfer import Transfer
from esrally.utils import convert, executors

LOG = logging.getLogger(__name__)


@dataclasses.dataclass
class Get:
    url: str
    path: str | None = None
    expected_size: int | None = None


@dataclasses.dataclass
class TransferManager:
    cfg: StorageConfig
    actor_address: actors.ActorAddress

    @classmethod
    def from_config(cls, cfg: types.AnyConfig = None) -> Self:
        """It creates a TransferManager with initialization values taken from given configuration."""
        cfg = StorageConfig.from_config(cfg)
        return cls(cfg, TransferActor.from_config(cfg))

    @property
    def system(self) -> actor.ActorSystem:
        return actor.ActorSystem.from_config(self.cfg)

    def get(
        self, url: str, path: str | None = None, expected_size: int | None = None, timeout: float | None = None
    ) -> TransferStatus:
        """It starts a new transfer of a file from a remote url to a local path.

        :param url: remote file address.
        :param path: local file address.
        :param expected_size: the expected file size in bytes.
        :param timeout: timeout in seconds.
        :return: transfer status object.
        """
        status = None
        try:
            for response in actor.request(
                self.actor_address,
                Get(url=url, path=path, expected_size=expected_size),
                timeout=timeout,
                retry_interval=1.,  # It will re-send the request every second to update the status.
            ):
                status = response.result(timeout=timeout)
                if status.finished:
                    break
        except Exception as ex:
            cause = ex
        else:
            cause = None
        if status is None:
            raise TimeoutError("File transfer request timed out.") from cause
        if not isinstance(status, TransferStatus):
            raise TypeError(f"Expected TransferStatus, got { type(status)} instead.") from cause
        if not status.finished:
            raise TransferInterrupted("File transfer interrupted.") from cause
        return status

    def shutdown(self):
        return self.system.ask(self.actor_address, actors.ActorExitRequest())


class TransferInterrupted(exceptions.RallyError):
    """Raised when a transfer has been interrupted for some reason
    """


@dataclasses.dataclass
class TransferStatus:
    url: str
    path: str
    finished: bool = False
    size: int | None = None
    transferred: int | None = None
    duration: convert.Duration = convert.Duration(0)
    progress: float | None = None
    average_speed: float | None = None


class TransferActor(actor.LocalActor):

    config_class = StorageConfig

    def __init__(self):
        """It manages files transfers.

        It executes file transfers in background. It periodically logs the status of transfers that are in progress.
        """
        super().__init__()
        self._client: Client | None = None
        self._executor: executors.Executor | None = None
        self.transfers: dict[str, Transfer] = {}
        self.timer_id: int = 0

    @property
    def client(self) -> Client:
        if self._client is None:
            self._client = Client.from_config(self.cfg)
        return self._client

    @property
    def executor(self) -> executors.Executor:
        if self._executor is None:
            self._executor = executors.ThreadPoolExecutor.from_config(self.cfg)
        return self._executor

    def configure_actor(self, cfg: ActorConfig) -> None:
        cfg = StorageConfig.from_config(cfg)
        if cfg.max_connections < 1:
            raise ValueError(f"invalid max_connections: {cfg.max_connections} < 1")

        if cfg.multipart_size < 1024 * 1024:
            raise ValueError(f"invalid multipart_size: {cfg.multipart_size} < {1024 * 1024}")

        if cfg.monitor_interval <= 0:
            raise ValueError(f"invalid monitor interval: {cfg.monitor_interval} <= 0")

        local_dir = os.path.expanduser(cfg.local_dir)
        if not os.path.isdir(local_dir):
            os.makedirs(local_dir)
        self.start_timer(cfg.monitor_interval)

    def receiveMsg_ActorExitRequest(self, msg: actors.ActorExitRequest, sender: actors.ActorAddress) -> None:
        self.shutdown()
        return self.SUPER

    def shutdown(self):
        self.stop_timer()
        transfers, self.transfers = self.transfers, {}
        for tr in transfers.values():
            try:
                tr.close()
            except Exception as ex:
                LOG.error("error closing transfer: %s, %s", tr.url, ex)

    def start_timer(self, interval: float) -> None:
        self.timer_id += 1
        self.wakeupAfter(timePeriod=datetime.timedelta(seconds=0), payload=(self.timer_id, interval))

    def stop_timer(self) -> None:
        self.timer_id = 0  # cancel timer

    def receiveMsg_WakeupMessage(self, msg: actors.WakeupMessage, sender: actors.ActorAddress) -> None:
        timer_id, interval = msg.payload
        if timer_id != self.timer_id:
            return self.SUPER
        self.update_transfers()
        self.client.monitor()
        self.wakeupAfter(timePeriod=datetime.timedelta(seconds=interval), payload=msg.payload)

    def update_transfers(self) -> None:
        """It executes periodic update operations on every unfinished transfer."""
        # It first removes finished transfers.
        self.transfers = transfers = {path: tr for path, tr in self.transfers.items() if not tr.finished}
        if not transfers:
            return

        # It updates max_connections value for each transfer
        max_connections = self.max_connections
        for tr in transfers.values():
            # It updates the limit of the number of connections for every transfer because it varies in function of
            # the number of transfers in progress.
            tr.max_connections = max_connections
            # It periodically save transfer status to ensure it will be eventually restored from the current state
            # if required.
            tr.save_status()
            # It ensures every unfinished transfer will periodically receive attention from a worker thread as soon
            # it becomes available to prevent it to get stalled forever.
            tr.start()

        # It logs updated statistics for every transfer.
        LOG.info("Transfers in progress:\n  %s", "\n  ".join(tr.info() for tr in transfers.values()))

    def receiveMsg_Get(self, request: Get, sender: actors.ActorAddress) -> None:
        """It starts a new transfer of a file from a remote url to a local path.

        :param request: it carries transfer parameters.
        :param sender: sender actor address.
        :return: transfer status object.
        """
        cfg = StorageConfig.from_config(self.cfg)
        path = request.path
        url = request.url
        if path is None:
            path = os.path.join(cfg.local_dir, url)
        # This also ensures the path is a string
        path = os.path.normpath(os.path.expanduser(path))
        transfer = self.transfers.get(path)
        if transfer is None:
            head = self.client.head(url)
            if request.expected_size is not None and head.content_length != request.expected_size:
                raise ValueError(f"mismatching document_length: got {head.content_length} bytes, wanted {request.expected_size} bytes")
            transfer = Transfer(
                client=self.client,
                url=url,
                path=path,
                document_length=head.content_length,
                executor=self.executor,
                multipart_size=cfg.multipart_size,
                crc32c=head.crc32c,
            )
            if not transfer.finished:
                self.transfers[transfer.path] = transfer
                # It sets the actual value for `max_connections` after updating the number of unfinished transfers and before
                # requesting for the first worker threads. In this way it will avoid requesting more worker threads than
                # the allowed per-transfer connections.
                transfer.start()
                self.update_transfers()

        for e in transfer.errors:
            self.send(sender, actor.Error(err=e))
        status = TransferStatus(
            url=url,
            path=path,
            finished=transfer.finished,
            size=transfer.document_length,
            transferred=transfer.done.size,
            duration=transfer.duration,
            progress=transfer.progress,
            average_speed=transfer.average_speed,
        )
        self.send(sender, status)

    @property
    def max_connections(self) -> int:
        max_connections = StorageConfig.from_config(self.cfg).max_connections
        number_of_transfers = len(self.transfers)
        if number_of_transfers > 0:
            max_workers = max(1, self.executor.max_workers or 1)
            max_connections = min(max_connections, (max_workers // number_of_transfers) + 1)
        return max_connections
