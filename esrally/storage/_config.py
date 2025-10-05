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
import os
import urllib.parse
from typing import Any

from esrally import actors
from esrally.utils import convert

LOG = logging.getLogger(__name__)

ADAPTERS: tuple[str, ...] = (
    "esrally.storage._aws:S3Adapter",
    "esrally.storage._http:HTTPAdapter",
)
AWS_PROFILE: str = ""
CHUNK_SIZE: int = 64 * 1024
HEAD_TTL: float = 60.0
LOCAL_DIR: str = "~/.rally/storage"
MAX_CONNECTIONS: int = 4
MAX_RETRIES: int | str = 10
MIRROR_FILES: tuple[str, ...] = ("~/.rally/storage-mirrors.json",)
MONITOR_INTERVAL: float = 2.0  # number of seconds
MULTIPART_SIZE: int = 8 * 1024 * 1024  # number of bytes
RANDOM_SEED: Any = None
RESOLVE_TTL: float = 60.0


class StorageConfig(actors.ActorConfig):

    @property
    def adapters(self) -> tuple[str, ...]:
        return convert.to_strings(self.opts("storage", "storage.adapters", ADAPTERS, False))

    @property
    def aws_profile(self) -> str:
        return self.opts("storage", "storage.aws.profile", AWS_PROFILE, False).strip()

    @property
    def chunk_size(self) -> int:
        return int(self.opts("storage", "storage.http.chunk_size", CHUNK_SIZE, False))

    @property
    def head_ttl(self) -> float:
        return self.opts("storage", "storage.head_ttl", HEAD_TTL, False)

    @property
    def local_dir(self) -> str:
        return self.opts("storage", "storage.local_dir", LOCAL_DIR, False)

    def local_path(self, path: str | None, url: str | None = None) -> str:
        if path is None:
            if url is None:
                raise ValueError("Either path or url must be specified")
            path = os.path.join(self.local_dir, urllib.parse.urlparse(url).path)
        return os.path.normpath(os.path.expanduser(path))

    @property
    def max_connections(self) -> int:
        return int(self.opts("storage", "storage.max_connections", MAX_CONNECTIONS, False))

    @property
    def max_retries(self) -> int | str:
        return self.opts("storage", "storage.http.max_retries", MAX_RETRIES, False)

    @property
    def mirror_files(self) -> tuple[str, ...]:
        return convert.to_strings(self.opts("storage", "storage.mirror_files", MIRROR_FILES, False))

    @property
    def monitor_interval(self) -> float:
        return self.opts("storage", "storage.monitor_interval", MONITOR_INTERVAL, False)

    @property
    def multipart_size(self) -> int:
        return self.opts("storage", "storage.multipart_size", MULTIPART_SIZE, False)

    @property
    def random_seed(self) -> Any:
        return self.opts("storage", "storage.random_seed", RANDOM_SEED, False)

    @property
    def resolve_ttl(self) -> float:
        return self.opts("storage", "storage.resolve_ttl", RESOLVE_TTL, False)


DEFAULT_STORAGE_CONFIG = StorageConfig()
