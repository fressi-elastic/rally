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
import asyncio
import dataclasses
import datetime
import logging
import os

from typing_extensions import Self

from esrally import actors, exceptions, types
from esrally.storage._client import Client
from esrally.storage._config import StorageConfig
from esrally.storage._transfer import Transfer
from esrally.utils import convert

LOG = logging.getLogger(__name__)


@dataclasses.dataclass
class GetRequest:
    url: str
    path: str | None = None
    expected_size: int | None = None
    wait: bool = True


@dataclasses.dataclass
class CancelRequest:
    path: str | None = None


@dataclasses.dataclass
class TransferManager:
    actor_address: actors.ActorAddress

    @classmethod
    def from_config(cls, cfg: types.Config | None = None) -> Self:
        """It creates a TransferManager with initialization values taken from given configuration."""
        return cls(actors.create_actor(TransferActor, cfg=cfg))

    def get(
        self, url: str, path: str | None = None, expected_size: int | None = None, wait: bool = True, timeout: float | None = None
    ) -> asyncio.Future["TransferStatus"]:
        """It starts a new transfer of a file from a remote url to a local path.

        :param url: remote file address.
        :param path: local file address.
        :param expected_size: the expected file size in bytes.
        :param wait: whether to wait for the transfer to finish.
        :param timeout: timeout in seconds for waiting for a response.
        :return: transfer status object.
        """
        return actors.request(
            self.actor_address,
            GetRequest(url=url, path=path, expected_size=expected_size, wait=wait),
            timeout=timeout,
        )

    def cancel(self, path: str) -> None:
        return actors.send(self.actor_address, CancelRequest(path=path))

    def shutdown(self):
        return actors.send(self.actor_address, actors.ActorExitRequest())


class TransferInterrupted(exceptions.RallyError):
    """Raised when a transfer has been interrupted for some reason"""


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


@dataclasses.dataclass
class TransferTimer:
    timer_id: int
    interval: float


class TransferActor(actors.AsyncActor):

    def __init__(self):
        """It manages files transfers.

        It executes file transfers in background. It periodically logs the status of transfers that are in progress.
        """
        super().__init__()
        self._client: Client | None = None
        self.transfers: dict[str, Transfer] = {}
        self.timer_id: int = 0
        self.cfg: StorageConfig | None = None
        self.client: Client | None = None

    def receiveMsg_ActorConfig(self, message: actors.ActorConfig, sender: actors.ActorAddress) -> None:
        self.cfg = cfg = StorageConfig.from_config(message)
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
        self.client = Client.from_config(cfg)

    def receiveMsg_ActorExitRequest(self, message: actors.ActorExitRequest, sender: actors.ActorAddress) -> None:
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
        self.wakeupAfter(datetime.timedelta(seconds=0), TransferTimer(self.timer_id, interval))

    def stop_timer(self) -> None:
        self.timer_id = 0  # cancel timer

    def receiveMsg_TransferTimer(self, timer: TransferTimer, sender: actors.ActorAddress) -> None:
        if timer.timer_id != self.timer_id:
            return None  # Cancelled or invalid timer
        self.update_transfers()
        self.client.monitor()
        self.wakeupAfter(timer.interval, timer)

    def update_transfers(self) -> None:
        """It executes periodic update operations on every unfinished transfer."""
        # It first removes finished transfers.
        self.transfers = transfers = {path: tr for path, tr in self.transfers.items() if not tr.finished}
        if not transfers:
            return

        # It updates max_connections value for each transfer
        max_connections = self.cfg.max_connections
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

    async def receiveMsg_GetRequest(self, request: GetRequest, sender: actors.ActorAddress) -> TransferStatus:
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
            head = await self.client.head(url)
            if request.expected_size is not None and head.content_length != request.expected_size:
                raise ValueError(f"mismatching document_length: got {head.content_length} bytes, wanted {request.expected_size} bytes")
            transfer = Transfer(
                client=self.client,
                url=url,
                path=path,
                document_length=head.content_length,
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

        if request.wait:
            await transfer

        if transfer.errors:
            raise TransferInterrupted(transfer.errors)

        return TransferStatus(
            url=url,
            path=path,
            finished=transfer.finished,
            size=transfer.document_length,
            transferred=transfer.done.size,
            duration=transfer.duration,
            progress=transfer.progress,
            average_speed=transfer.average_speed,
        )

    def receiveMsg_CancelRequest(self, request: CancelRequest, sender: actors.ActorAddress) -> None:
        transfer = self.transfers.pop(request.path, None)
        if transfer is not None:
            try:
                transfer.close()
            except Exception as ex:
                LOG.error("error closing transfer: %s, %s", transfer.url, ex)
