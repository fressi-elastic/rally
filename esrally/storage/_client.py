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

from esrally import config
from esrally.storage._adapter import Adapter

LOG = logging.getLogger(__name__)


_ADAPTER_CLASSES = dict[str, type[Adapter]]()


def register_adapter_class(*prefixes: str):
    """This class decorator registers an implementation of the Client protocol.
    :param prefixes: list of prefixes names the adapter class has to be registered for
    :return: a type decorator
    """

    def decorator(cls: type[Adapter]) -> type[Adapter]:
        for prefix in prefixes:
            # Adapter types are sorted in descending order by prefix length.
            _ADAPTER_CLASSES[prefix] = cls
            keys_to_move = [k for k in _ADAPTER_CLASSES if len(k) < len(prefix)]
            for key in keys_to_move:
                _ADAPTER_CLASSES[key] = _ADAPTER_CLASSES.pop(key)
        return cls

    return decorator


def adapter_class(url: str) -> type[Adapter]:
    for prefix, cls in _ADAPTER_CLASSES.items():
        if url.startswith(prefix):
            return cls
    raise NotImplementedError(f"unsupported url: {url}")


class Client:
    """It handles client instances allocation allowing reusing pre-allocated instances from the same thread."""

    def __init__(self, adapter_classes: dict[str, type[Adapter]] | None = None):
        if adapter_classes is None:
            adapter_classes = _ADAPTER_CLASSES
        self._pool: dict[str, Adapter] = {}
        self._adapter_classes = dict(adapter_classes)

    @classmethod
    def from_config(cls, cfg: config.Config):
        adapter_classes = _ADAPTER_CLASSES
        adapter_names = cfg.opts(section="storage", key="storage.adapter_names", default_value=None, mandatory=False)
        if adapter_names is not None:
            adapter_classes = {}
            for prefix, adapter_cls in _ADAPTER_CLASSES.items():
                for selector in adapter_names.replace(" ", "").split(","):
                    if prefix.startswith(selector):
                        adapter_classes[prefix] = adapter_cls
        return cls(adapter_classes=adapter_classes)

    def get(self, url: str) -> Adapter:
        """It obtains a client instance for given protocol.

        It will return the same client instance when re-called with the same protocol id from the same thread. Returned
        client is intended to be used only from the calling thread.

        :param url: the target url the client is being created for.
        :return: a client instance for given protocol.
        """
        cl = self._pool.get(url)
        if cl is None:
            # It uses registered classes mapping to create a client instance for each required protocol.

            # Given the same prefix it will return the same instance later when re-called from the same thread.
            cls = adapter_class(url)
            self._pool[url] = cl = cls.from_url(url)
        return cl
