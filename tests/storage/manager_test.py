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

import os
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

import pytest

from esrally import config, types
from esrally.storage._adapter import Head
from esrally.storage._config import StorageConfig
from esrally.storage._manager import TransferManager
from esrally.storage.testing import DummyAdapter
from esrally.utils.cases import cases


@pytest.fixture(scope="function")
def cfg(request, tmpdir: os.PathLike) -> types.Config:
    cfg = StorageConfig.from_config()
    cfg.add(
        config.Scope.application,
        "storage",
        "storage.adapters",
        f"{__name__}:StorageAdapter",
    )
    cfg.add(config.Scope.application, "storage", "storage.local_dir", str(tmpdir))
    return cfg


@pytest.fixture
def manager(cfg: types.Config) -> Iterator[TransferManager]:
    manager = TransferManager.from_config(cfg)
    try:
        yield manager
    finally:
        pass  # manager.shutdown()


SIMPLE_URL = "http://example.com"
SIMPLE_DATA = b"example document"
SIMPLE_HEAD = Head(url=SIMPLE_URL, content_length=len(SIMPLE_DATA))


class StorageAdapter(DummyAdapter):

    HEADS = (SIMPLE_HEAD,)
    DATA = {
        SIMPLE_URL: SIMPLE_DATA,
    }


@dataclass
class GetCase:
    url: str
    path: os.PathLike | str | None = None
    expected_size: int | None = None
    want_data: bytes | None = None
    want_error: tuple[type[Exception], ...] = tuple()


@cases(
    simple=GetCase(url=SIMPLE_URL, want_data=SIMPLE_DATA),
    path=GetCase(url=SIMPLE_URL, want_data=SIMPLE_DATA, path="some/path"),
    expected_size=GetCase(url=SIMPLE_URL, want_data=SIMPLE_DATA, expected_size=len(SIMPLE_DATA)),
    unexpected_size=GetCase(url=SIMPLE_URL, want_error=(ValueError,), expected_size=len(SIMPLE_DATA) - 1),
)
def test_get(case: GetCase, manager: TransferManager, tmpdir: os.PathLike) -> None:
    kwargs: dict[str, Any] = {}
    if case.path is not None:
        kwargs["path"] = os.path.join(tmpdir, case.path)
    if case.expected_size is not None:
        kwargs["expected_size"] = case.expected_size

    try:
        tr = manager.get(url=case.url, **kwargs)
    except case.want_error:
        return
    if case.path is not None:
        assert os.path.join(tmpdir, case.path) == tr.path

    if case.expected_size is not None:
        assert os.path.getsize(tr.path) == case.expected_size

    tr = manager.get(url=case.url, wait=True, **kwargs)
    assert tr.finished

    if case.want_data is not None:
        assert os.path.exists(tr.path)
        if case.want_data is not None:
            with open(tr.path, "rb") as f:
                assert f.read() == SIMPLE_DATA
