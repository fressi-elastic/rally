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

import threading
from typing import BinaryIO

from esrally.storage._range import MAX_LENGTH, Range, RangeError


class StreamClosedError(Exception):
    pass


class BaseStream:

    def __init__(self, fd: BinaryIO, _range: Range | None, close=True):
        self._lock = threading.Lock()
        self._fd = fd
        position = fd.tell()
        if _range is None:
            _range = Range(position, MAX_LENGTH)
        elif position not in _range:
            raise RangeError(f"fd position not in range: {position} not in {_range}", _range)
        self._position = position
        self._range = _range
        self._close = close

    @property
    def transferred(self) -> int:
        return self._position - self._range.start

    def tell(self) -> int:
        return self._position

    def writable(self) -> bool:
        return False

    def readable(self) -> bool:
        return False

    @property
    def range(self) -> Range:
        return self._range

    def close(self):
        if self._close and self._fd is not None:
            self._fd.close()
        self._fd = None

    def __enter__(self):
        self._fd.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class OutputStream(BaseStream):
    """OutputStream provides an output stream wrapper able to prevent exceeding writing given range."""

    def __init__(self, fd: BinaryIO, _range: Range | None = None, close=True):
        if not fd.writable():
            raise ValueError("fd is not writable")
        super().__init__(fd, _range, close)

    def writable(self) -> bool:
        return True

    def write(self, data):
        with self._lock:
            if self._fd is None:
                raise StreamClosedError("stream has been closed")

            size = len(data)
            if self._range.end < MAX_LENGTH:
                # prevent exceeding file data range
                size = min(size, self._range.end - self._position)
            chunk = data[:size]
            if chunk:
                self._fd.write(data)
                self._position += size
            if len(data) > size:
                raise RangeError(f"data size exceeds file range: {len(data)} > {size}", self._range)


class InputStream(BaseStream):
    """InputStream provides an input stream wrapper able to prevent exceeding reading given range."""

    def __init__(self, fd: BinaryIO, _range: Range | None = None, close=True):
        if not fd.readable():
            raise ValueError("fd is not readable")
        super().__init__(fd, _range, close)

    def readable(self) -> bool:
        return True

    def read(self, size: int = -1) -> bytes:
        with self._lock:
            if self._fd is None:
                raise StreamClosedError("stream has been closed")

            if self._range.end < MAX_LENGTH:
                # prevent exceeding file data range
                if size < 0:
                    size = self._range.end - self._position
                else:
                    size = min(size, self._range.end - self._position)
            if size == 0:
                return b""
            data = self._fd.read(size)
            assert isinstance(data, bytes)
            size = len(data)
            self._position += size
            return data
