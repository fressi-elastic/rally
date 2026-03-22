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

import datetime

import pytest

from esrally import config, metrics
from esrally.metrics.sketch_utils import RequestMetricSketchKey, SketchState

RACE_TIMESTAMP = datetime.datetime(2016, 1, 31)
RACE_ID = "6ebc6e53-ee20-4b0c-99b4-09697987e9f4"


@pytest.fixture
def in_memory_store():
    cfg = config.Config()
    cfg.add(config.Scope.application, "system", "env.name", "unittest")
    cfg.add(config.Scope.application, "track", "params", {})
    return metrics.InMemoryMetricsStore(cfg)


def test_request_sketch_table_exists_and_empty_after_init(in_memory_store):
    assert hasattr(in_memory_store, "_request_sketch_table")
    assert in_memory_store._request_sketch_table == {}


def test_request_sketch_table_empty_after_open(in_memory_store):
    in_memory_store.open(
        RACE_ID,
        RACE_TIMESTAMP,
        "test",
        "append-no-conflicts",
        "defaults",
        create=True,
    )
    assert in_memory_store._request_sketch_table == {}


def test_legacy_put_and_getters_unchanged_sketch_table_unused(in_memory_store):
    in_memory_store.open(
        RACE_ID,
        RACE_TIMESTAMP,
        "test",
        "append-no-conflicts",
        "defaults",
        create=True,
    )
    in_memory_store.put_value_cluster_level("query_latency", 10.0, "ms")
    in_memory_store.put_value_cluster_level("query_latency", 20.0, "ms")
    in_memory_store.put_value_cluster_level("query_latency", 30.0, "ms")

    assert in_memory_store._request_sketch_table == {}
    assert len(in_memory_store.docs) == 3

    pct = in_memory_store.get_percentiles("query_latency", percentiles=[50, 100])
    assert pct[50] == pytest.approx(20.0)
    assert pct[100] == pytest.approx(30.0)

    stats = in_memory_store.get_stats("query_latency")
    assert stats["count"] == 3
    assert stats["min"] == pytest.approx(10.0)
    assert stats["max"] == pytest.approx(30.0)


def test_request_sketch_table_cleared_on_second_open(in_memory_store):
    key = RequestMetricSketchKey(
        metric_name="latency",
        task="t1",
        operation="bulk",
        operation_type="bulk",
        sample_type="normal",
    )
    in_memory_store.open(
        RACE_ID,
        RACE_TIMESTAMP,
        "test",
        "append-no-conflicts",
        "defaults",
        create=True,
    )
    in_memory_store._request_sketch_table[key] = SketchState()
    assert len(in_memory_store._request_sketch_table) == 1

    in_memory_store.open(
        RACE_ID,
        RACE_TIMESTAMP,
        "test",
        "append-no-conflicts",
        "defaults",
        create=True,
    )
    assert in_memory_store._request_sketch_table == {}
