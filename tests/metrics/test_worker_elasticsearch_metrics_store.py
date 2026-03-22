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
# pylint: disable=protected-access

from __future__ import annotations

import datetime
import json
import logging
from unittest import mock

import elastic_transport

from esrally import config, metrics, paths


class MockClientFactory:
    """Mirrors tests/metrics_test.py — EsMetricsStore calls ``open()`` inside the helper, so defaults are set here."""

    def __init__(self, cfg):
        self._es = mock.create_autospec(metrics.EsClient)
        self._es.exists.return_value = False
        self._es.template_exists.return_value = False
        self._es.get_template.return_value = mock.create_autospec(elastic_transport.ObjectApiResponse, body={"index_templates": []})

    def create(self):
        return self._es


class DummyIndexTemplateProvider:
    def __init__(self, cfg):
        pass

    def metrics_template(self) -> str:
        return json.dumps(
            {
                "index_patterns": ["rally-metrics-*"],
                "template": {"mappings": {"properties": {"race-id": {"type": "keyword"}}}},
            }
        )


class StaticClock:
    NOW = 1453362707

    @staticmethod
    def now():
        return StaticClock.NOW

    @staticmethod
    def stop_watch():
        return StaticStopWatch()


class StaticStopWatch:
    def start(self):
        pass

    def stop(self):
        pass

    def split_time(self):
        return 0

    def total_time(self):
        return 0


RACE_ID = "6ebc6e53-ee20-4b0c-99b4-09697987e9f4"
RACE_TIMESTAMP = datetime.datetime(2016, 1, 31, 0, 0, 0)


def _base_cfg():
    cfg = config.Config()
    cfg.add(config.Scope.application, "system", "env.name", "unittest")
    cfg.add(config.Scope.application, "track", "params", {"shard-count": 3})
    cfg.add(config.Scope.application, "system", "race.id", RACE_ID)
    cfg.add(config.Scope.application, "system", "time.start", RACE_TIMESTAMP)
    cfg.add(config.Scope.application, "mechanic", "car.names", "defaults")
    cfg.add(config.Scope.application, "node", "rally.root", paths.rally_root())
    return cfg


def test_worker_elasticsearch_metrics_store_disabled_returns_none():
    cfg = _base_cfg()
    cfg.add(config.Scope.application, "reporting", "datastore.type", "elasticsearch")
    cfg.add(config.Scope.application, "driver", "distributed.request.metrics", False)
    assert metrics.worker_elasticsearch_metrics_store_if_enabled(cfg, "t", "c") is None


def test_worker_elasticsearch_metrics_store_in_memory_returns_none():
    cfg = _base_cfg()
    cfg.add(config.Scope.application, "reporting", "datastore.type", "in-memory")
    cfg.add(config.Scope.application, "driver", "distributed.request.metrics", True)
    assert metrics.worker_elasticsearch_metrics_store_if_enabled(cfg, "t", "c") is None


def test_worker_elasticsearch_metrics_store_probe_flush_bulk_index_contains_race_id():
    cfg = _base_cfg()
    cfg.add(config.Scope.application, "reporting", "datastore.type", "elasticsearch")
    cfg.add(config.Scope.application, "driver", "distributed.request.metrics", True)

    store = metrics.worker_elasticsearch_metrics_store_if_enabled(
        cfg,
        "track-name",
        "challenge-name",
        client_factory_class=MockClientFactory,
        index_template_provider_class=DummyIndexTemplateProvider,
        clock=StaticClock,
    )
    assert store is not None
    store.logger = mock.create_autospec(logging.Logger)
    es = store._client

    store.put_value_cluster_level("worker_es_metrics_probe", 1.0, "none")
    store.flush(refresh=False)

    es.bulk_index.assert_called_once()
    call_kw = es.bulk_index.call_args.kwargs
    items = call_kw["items"]
    assert len(items) == 1
    assert items[0]["race-id"] == RACE_ID
    assert items[0]["name"] == "worker_es_metrics_probe"
    assert items[0]["track"] == "track-name"
    assert items[0]["challenge"] == "challenge-name"
