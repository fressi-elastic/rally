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
from collections.abc import AsyncIterator, Iterable, Mapping
from typing import TypeVar

from typing_extensions import Self

from esrally import types
from esrally.storage._adapter import Adapter, Head
from esrally.storage._range import NO_RANGE, RangeSet


class DummyAdapter(Adapter):

    HEADS: Iterable[Head] = tuple()
    DATA: Mapping[str, bytes] = {}

    @classmethod
    def match_url(cls, url: str) -> bool:
        return True

    @classmethod
    def from_config(cls, cfg: types.Config = None) -> Self:
        return cls()

    def __init__(self, heads: Iterable[Head] | None = None, data: Mapping[str, bytes] | None = None) -> None:
        if heads is None:
            heads = self.HEADS
        if data is None:
            data = self.DATA
        self.heads: Mapping[str, Head] = {h.url: h for h in heads if h.url is not None}
        self.data: Mapping[str, bytes] = copy.deepcopy(data)

    async def head(self, url: str) -> Head:
        try:
            return copy.copy(self.heads[url])
        except KeyError:
            raise FileNotFoundError from None

    async def get(self, url: str, want: Head | None = None) -> tuple[Head, AsyncIterator[bytes]]:
        ranges: RangeSet = NO_RANGE
        if want is not None:
            ranges = want.ranges
            if len(ranges) > 1:
                raise NotImplementedError("len(head.ranges) > 1")
        data = self.data[url]
        document_length: int | None = None
        if ranges:
            document_length = len(data)
            data = data[ranges.start : ranges.end]

        async def read_data() -> AsyncIterator[bytes]:
            yield data

        return Head(url, content_length=len(data), ranges=ranges, document_length=document_length), read_data()


R = TypeVar("R")
