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

import collections
import logging

import requests
import requests.adapters
import urllib3

from esrally.storage._adapter import Adapter, Head, ServiceUnavailableError, Writable
from esrally.storage._client import register_adapter_class
from esrally.storage._range import MAX_LENGTH, NO_RANGE, NoRange, Range, RangeSet

LOG = logging.getLogger(__name__)


# Size of the buffers used for file transfer content.
CHUNK_SIZE = 1 * 1024 * 1024

# It limits the maximum number of connection retries.
MAX_RETRIES = 10

# It limits the maximum sleep time between connection retries.
MAX_BACKOFF_FACTOR = 20


@register_adapter_class("http:", "https:")
class HTTPAdapter(Adapter):
    """It implements the adapter interface for http(s) protocols using the requests library."""

    def __init__(self, url: str):
        super().__init__(url)
        retry = urllib3.Retry(total=MAX_RETRIES, backoff_factor=1)
        retry.MAX_BACKOFF_FACTOR = MAX_BACKOFF_FACTOR
        self._session = requests.session()
        self._session.mount("http://", requests.adapters.HTTPAdapter(max_retries=retry))
        self._session.mount("https://", requests.adapters.HTTPAdapter(max_retries=retry))

    @classmethod
    def from_url(cls, url: str):
        return HTTPAdapter(url)

    def head(self) -> Head:
        with self._session.head(self.url) as r:
            r.raise_for_status()
            return head_from_headers(self.url, r.headers)

    def get(self, stream: Writable, ranges: RangeSet = NO_RANGE) -> Head:
        headers = {}
        if len(ranges) > 1:
            raise NotImplementedError(f"unsupported multi range requests: range is {ranges}")
        for r in ranges:
            headers["range"] = f"bytes={r.start}-"
            if r.end != MAX_LENGTH:
                headers["range"] += f"{r.end - 1}"
        with self._session.get(self.url, stream=True, allow_redirects=True, headers=headers) as r:
            head = head_from_headers(self.url, r.headers)
            if r.status_code == 503:

                raise ServiceUnavailableError()
            r.raise_for_status()
            if head.ranges != ranges:
                raise ValueError(f"unexpected content range in server response: got {head.ranges}, want: {ranges}")
            for chunk in r.iter_content(CHUNK_SIZE):
                if chunk:
                    stream.write(chunk)
            return head


def content_length_from_headers(headers: collections.abc.Mapping[str, str]) -> int | None:
    try:
        return int(headers.get("content-length", "*").strip())
    except ValueError:
        return None


def accept_ranges_from_headers(headers: collections.abc.Mapping[str, str]) -> bool:
    return headers.get("accept-ranges", "").strip() == "bytes"


def range_from_headers(headers: collections.abc.Mapping[str, str]) -> tuple[Range | NoRange, int | None]:
    content_range = headers.get("content-range", "").strip()
    if not content_range:
        return NO_RANGE, None

    if not content_range.startswith("bytes "):
        raise ValueError(f"unsupported content range: {content_range}")

    if "," in content_range:
        raise ValueError("multi range value is not unsupported")

    try:
        value = content_range[len("bytes ") :].strip().replace(" ", "")
        value, content_length_text = value.split("/", 1)
        value = value.strip()
        try:
            content_length = int(content_length_text)
        except ValueError:
            if content_length_text == "*":
                raise
            content_length = None

        start_text, end_text = value.split("-", 1)
        try:
            start = int(start_text)
        except ValueError:
            if start_text == "":
                raise
            start = 0
        try:
            end = int(end_text) + 1
        except ValueError:
            if end_text != "*":
                raise
            end = MAX_LENGTH
        return Range(start, end), content_length
    except ValueError as ex:
        raise ValueError(f"invalid content range in {content_range}: {ex}")


def head_from_headers(url: str, headers: collections.abc.Mapping[str, str]) -> Head:
    ranges, content_length = range_from_headers(headers)
    content_length = content_length_from_headers(headers) or content_length
    accept_ranges = accept_ranges_from_headers(headers)
    return Head.create(url=url, content_length=content_length, accept_ranges=accept_ranges, ranges=ranges)
