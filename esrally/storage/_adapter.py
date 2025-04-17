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

import logging
from abc import ABC, abstractmethod
from typing import NamedTuple, Protocol, runtime_checkable

from esrally.storage._range import NO_RANGE, RangeSet

LOG = logging.getLogger(__name__)


class ServiceUnavailableError(Exception):
    """It is raised when an adapter refuses providing service for example because of too many requests"""


@runtime_checkable
class Writable(Protocol):

    def write(self, data: bytes):
        pass


@runtime_checkable
class Readable(Protocol):

    def read(self, size: int = -1) -> bytes:
        pass


class Head(NamedTuple):
    url: str
    content_length: int | None
    accept_ranges: bool
    ranges: RangeSet

    @classmethod
    def create(cls, url: str, content_length: int | None = None, accept_ranges: bool | None = None, ranges: RangeSet = NO_RANGE) -> Head:
        if accept_ranges is None:
            accept_ranges = bool(ranges)
        return cls(url=url, accept_ranges=accept_ranges, content_length=content_length, ranges=ranges)


class Adapter(ABC):
    """Base class for storage class client implementation"""

    def __init__(self, url: str):
        self.url = url

    @classmethod
    @abstractmethod
    def from_url(cls, url: str):
        raise NotImplementedError

    def head(self) -> Head:
        """It gets remote file headers."""
        raise NotImplementedError(f"can't download file from {self.url}")

    def get(self, stream: Writable, ranges: RangeSet = NO_RANGE) -> Head:
        """It downloads a remote bucket object to a local file path.
        :param stream: it represents the destination file stream where to write data to.
        :param ranges: it represents the portion of the file to transfer.
        :raises NotImplementedError: in case the client or the server doesn't support this method.
        """
        raise NotImplementedError(f"can't download file from {self.url}")

    def put(self, stream: Writable, ranges: RangeSet = NO_RANGE) -> Head:
        """It uploads a local file to a remote bucket object.
        :param stream: it represents the local file path where to read data from.
        :param ranges: it represents the portion of the file to transfer.
        :raises NotImplementedError: in case the client or the server doesn't support this method.
        """
        raise NotImplementedError(f"can't upload file to {self.url}")
