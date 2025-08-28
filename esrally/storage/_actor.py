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

import copy
import dataclasses
import json
import logging
import os
import multiprocessing
from collections.abc import Mapping

import filelock
from thespian import actors
from typing_extensions import Self

from esrally import actor, types
from esrally.storage._adapter import Head
from esrally.storage._config import StorageConfig
from esrally.storage._client import Client
from esrally.storage._range import Range, rangeset, RangeSet, NO_RANGE, MAX_RANGE, MAX_LENGTH
from esrally.storage._transfer import FileWriter
from esrally.utils import executors

LOG = logging.getLogger(__name__)

@dataclasses.dataclass
class GetRequest:
    url: str
    path: str | None = None
    expected_size: int | None = None
    send_status: bool = False
    handlers: list[actors.ActorAddress] = dataclasses.field(default_factory=list)
    multipart_size: int | None = None
    todo: RangeSet = MAX_RANGE


@dataclasses.dataclass
class GetStatus:
    request: GetRequest
    _head: Head | None = None
    error: Exception | None = None
    resumed_size: int  = 0
    _todo: RangeSet | None = None
    _done: RangeSet = NO_RANGE
    _document_length: int | None = None
    _crc32c: str | None = None

    def resume(self) -> None:
        if not os.path.isfile(self.request.path):
            # There is no file to recover interrupted transfer to.
            raise FileNotFoundError(f"target file not found: {self.request.path}")

        status_filename = self.request.path + ".status"
        if not os.path.isfile(status_filename):
            # There is no file to read status data from.
            raise FileNotFoundError(f"status file not found: {status_filename}")

        with filelock.FileLock(status_filename):
            with open(status_filename) as fd:
                document = json.load(fd)
                if not isinstance(document, Mapping):
                    # Invalid file format.
                    raise ValueError(f"mismatching status file format: got {type(document)}, want dict")

        # It checks the remote URL to ensure the file was downloaded from the same location.
        url = document.get("url")
        if url is None:
            raise ValueError(f"url field not found in status file: {status_filename}")
        if url != self.request.url:
            raise ValueError(f"mismatching url in status file: '{status_filename}', got '{url}', want '{self.request.url}'")

        # It checks the document length to ensure the file was the same version.
        document_length = document.get("document_length")
        if document_length is not None:
            self.document_length = document_length

        # It checks the crc32c checksum to ensure the file was the same version.
        crc32c = document.get("crc32c", None)
        if crc32c is not None:
            self.crc32c = crc32c

        # It skips the parts that has been already downloaded.
        done_text = document.get("done")
        if done_text is None:
            raise ValueError(f"done field not found in status file: {status_filename}")
        done = rangeset(done_text)
        if done:
            file_size = os.path.getsize(self.request.path)
            if done.end > file_size:
                raise ValueError(f"corrupted file size is smaller than completed part: {done.end} > {file_size} (path='{self.path}')")
            self.done |= done

        # Update the resumed size so that it will compute download speed only on the new parts.
        self.resumed_size = done.size

    def update(self, status: GetStatus):
        self.head = status.head
        self.done |= status.done

    def save(self):
        """It updates the status file."""
        document = {
            "url": self.request.url,
            "document_length": self.document_length,
            "done": str(self.done),
            "crc32c": self.crc32c,
        }
        os.makedirs(os.path.dirname(self.request.path), exist_ok=True)
        with open(self.request.path + ".status", "w") as fd:
            json.dump(document, fd)

    @property
    def document_length(self) -> int:
        if self._document_length is None:
            self._document_length = self.request.expected_size
        return self._document_length

    @document_length.setter
    def document_length(self, value: int | None) -> None:
        if value is None:
            return
        if self._document_length is None:
            cut_range = Range(0, value)
            self._todo &= cut_range
            self._done &= cut_range
            self._document_length = value
            return
        if self._document_length != value:
            raise ValueError(f"expected document length is {self._document_length}, got {value}")

    @property
    def head(self) -> Head | None:
        return self._head

    @head.setter
    def head(self, head: Head | None) -> None:
        if head is None:
            return

        if self._head is not None:
            self._head.check(head)
            return

        self.document_length = head.content_length
        self.crc32c = head.crc32c
        self._head = head

    @property
    def crc32c(self) -> str | None:
        return self._crc32c

    @crc32c.setter
    def crc32c(self, value: str | None) -> None:
        if value is None:
            return
        if self._crc32c is None:
            self._crc32c = value
            return
        if self._crc32c != value:
            raise ValueError(f"expected crc32c is {value}, got {value}")

    @property
    def todo(self) -> RangeSet:
        if self._todo is None:
            self._todo = self.request.todo
        return self._todo

    @todo.setter
    def todo(self, value: RangeSet) -> None:
        if self.document_length is not None:
            value &= Range(0, self._document_length)
        self._todo = value - self._done

    @property
    def done(self) -> RangeSet:
        return self._done

    @done.setter
    def done(self, value: RangeSet) -> None:
        if self.document_length is not None:
            value &= Range(0, self._document_length)
        self._done = value

    @property
    def finished(self) -> bool:
        return self._done == Range(0, self.document_length or MAX_LENGTH)


@dataclasses.dataclass
class TransferManager:

    cfg: StorageConfig
    actor: actors.ActorAddress

    @classmethod
    def from_config(cls, cfg: types.AnyConfig) -> Self:
        cfg = StorageConfig.from_config(cfg)
        return cls(cfg =cfg, actor=TransferActor.from_config(cfg))

    def get(
        self,
        url: str,
        path: str | None = None,
        expected_size: int | None = None,
        multipart_size: int | None = None,
        send_status: bool = True,
        handlers: list[actors.ActorAddress] | None = None
    ) -> GetStatus | None:
        multipart_size = multipart_size or self.cfg.multipart_size
        request = GetRequest(url=url, path=path, expected_size=expected_size, multipart_size=multipart_size, send_status=True, handlers=handlers)
        actor_system = actor.system_from_config(self.cfg)
        if not send_status:
            return actor_system.tell(self.actor, request)
        return actor_system.ask(self.actor, request)


class TransferActor(executors.ExecutorActor, actor.SingletonActor):

    Config = StorageConfig
    Transfer = Transfer

    # This ensures the actor is created on the same host as the transfer manager.
    require_same_host = True

    @property
    def globalName(self):
        return f"{__name__}:TransferActor"

    def __init__(self):
        super().__init__()
        self.requests: dict[str, Transfer] = {}
        self.max_connections: int = self.cfg.max_connections

    @actor.no_retry()
    def receiveMsg_GetRequest(self, request: GetRequest, sender: actors.ActorAddress) -> None:
        request.path = self.cfg.local_path(path=request.path, url=request.url)
        # It eventually creates a new transfer
        transfer = self.requests.setdefault(request.path, self.Transfer(request=request, actor=self.myAddress, cfg=self.cfg))
        if request.send_status:
            self.send(sender, transfer.status)

        if transfer.connections >= self.max_connections:
            return

        # It submits a copy of the transfer so that if we are using a threading pool executor it will modify a copy.
        transfer_copy = copy.deepcopy(transfer)
        transfer_copy.status.todo, transfer.status.todo = transfer.status.todo.split(
            transfer.request.multipart_size or self.cfg.multipart_size or MAX_LENGTH
        )
        self.submit(transfer_copy)
        transfer.connections += 1

    @actor.no_retry()
    def receiveMsg_GetStatus(self, status: GetStatus, sender: actors.ActorAddress) -> None:
        transfer = self.requests.get(status.request.path)
        if transfer is None:
            # It assumes transfer finalization has already been performed.
            return

        transfer.update_status(status)

        if transfer.finished:
            # It handles transfer termination
            transfer = self.requests.pop(status.request.path)
            for handler in transfer.request.handlers:
                self.send(handler, status)
            return

        if status.head is not None and transfer.status.head is None:
            transfer.status.head = status.head
            # It sends a status to indicate that transfer has started
            for handler in transfer.request.handlers:
                self.send(handler, status)

        if transfer.connections < self.max_connections:
            # It requests for more connections
            self.send(self.myAddress, transfer.request)

    def handle_executor_result(self, result: executors.ActorExecutorResult) -> None:
        assert isinstance(result.task.func, Transfer)
        transfer = self.requests.get(result.task.func.request.path)
        transfer.connections -= 1
        assert transfer.connections >= 0
        if result.error is not None:
            LOG.debug(f"Received transfer failure: transfer: %s, error: %s", transfer, result.error)
            transfer.cancel(error=result.error)
        assert result.value is None

    @property
    def max_connections(self) -> int:
        return max(1, min(self._max_connections, self.cfg.max_connections))

    @max_connections.setter
    def max_connections(self, value: int):
        self._max_connections = value


class TransferCancelled(Exception): ...


@dataclasses.dataclass
class Transfer:
    cfg: StorageConfig
    request: GetRequest
    actor: actors.ActorAddress
    connections: int = 0
    _finished: multiprocessing.Event = dataclasses.field(default_factory=multiprocessing.Event)
    _status: GetStatus | None = None
    _client: Client | None = None
    _actor_system: actors.ActorSystem | None = None

    def __call__(self) -> None:
        try:
            self.get()
        except Exception as ex:
            LOG.debug(f"Exception executing transfer {self}: {ex}")
            self.error = ex
        finally:
            self.actor_system.tell(self.actor, self.status)

    def get(self) -> None:
        try:
            self.status.resume()
        except Exception as ex:
            LOG.debug(f"Exception resuming transfer {self}: {ex}")
        if self.status.finished:
            return

        os.makedirs(os.path.dirname(self.request.path), exist_ok=True)
        with FileWriter(self.request.path, self.status.todo) as stream:
            try:
                self.client.get(self.request.path, stream, want=self.status.head, canceled=self._finished)
            finally:
                self.status.done |= stream.done

    @property
    def status(self) -> GetStatus:
        if self._status is None:
            self._status = GetStatus(self.request)
        return self._status

    @property
    def head(self) -> Head:
        if self.status.head is None:
            self.status.head = self.client.head(self.request.url)
            # It updates the head on the copy of transfer hold by the actor.
            self.actor_system.tell(self.actor, self.status)
        return self.status.head

    def cancel(self, error: Exception | None = None) -> None:
        self.status.error = error or TransferCancelled(f"cancel transfer: {self.request}")
        self.finished = True

    @property
    def client(self) -> Client:
        if self._client is None:
            self._client = Client.from_config(self.cfg)
        return self._client

    @property
    def finished(self) -> bool:
        return self._finished.is_set() or self.status.finished

    @finished.setter
    def finished(self, value: bool):
        if (value or self.status.finished) and not self._finished.is_set():
            self._finished.set(True)

    @property
    def actor_system(self) -> actors.ActorSystem:
        if self._actor_system is None:
            self._actor_system = actor.system_from_config(cfg=self.cfg)
        return self._actor_system

    def update_status(self, status: GetStatus) -> None:
        if status.finished:
            self.finished = True
        self.status.update(status)
