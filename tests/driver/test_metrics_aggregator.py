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
from ddsketch.ddsketch import DDSketch

from esrally import config, metrics
from esrally.driver.metrics_aggregator import (
    RequestSketchMergeDelta,
    apply_sketch_merge_delta,
    apply_throughput_bucket_deltas,
)
from esrally.driver.throughput_buckets import ThroughputBucketKey
from esrally.metrics.sketch_utils import (
    REQUEST_METRIC_SKETCH_RELATIVE_ACCURACY,
    serialize_sketch,
)
from esrally.track import track

_PCT_TOL = REQUEST_METRIC_SKETCH_RELATIVE_ACCURACY * 2

RACE_TIMESTAMP = datetime.datetime(2016, 1, 31)
RACE_ID = "6ebc6e53-ee20-4b0c-99b4-09697987e9f4"


@pytest.fixture
def in_memory_store():
    cfg = config.Config()
    cfg.add(config.Scope.application, "system", "env.name", "unittest")
    cfg.add(config.Scope.application, "track", "params", {})
    ms = metrics.InMemoryMetricsStore(cfg)
    ms.open(
        RACE_ID,
        RACE_TIMESTAMP,
        "t",
        "c",
        "car",
        create=True,
    )
    return ms


def test_apply_sketch_merge_delta_get_stats(in_memory_store):
    store = in_memory_store
    task = "index #1"
    op_type = track.OperationType.Bulk
    key = store.aggregated_request_timing_sketch_key("service_time", task=task, operation_type=op_type)

    sk = DDSketch()
    for v in range(1, 101):
        sk.add(float(v))

    apply_sketch_merge_delta(
        store,
        RequestSketchMergeDelta(key=key, sketch_bytes=serialize_sketch(sk), success_count_delta=80, failure_count_delta=20),
    )

    stats = store.get_stats("service_time", task=task, operation_type=op_type, sample_type=metrics.SampleType.Normal)
    assert stats["count"] == 100
    assert stats["avg"] == pytest.approx(50.5, rel=_PCT_TOL)
    assert stats["sum"] == pytest.approx(50.5 * 100, rel=_PCT_TOL)
    assert store.get_error_rate(task=task, operation_type=op_type, sample_type=metrics.SampleType.Normal) == pytest.approx(0.2)


def test_apply_throughput_bucket_deltas_commutative():
    k1 = ThroughputBucketKey("t1", 0, "normal", "docs/s")
    k2 = ThroughputBucketKey("t1", 1, "normal", "docs/s")
    a = {k1: 3, k2: 5}
    b = {k1: 2, k2: 1}
    c = {k1: 1, k2: 7}

    m_ab = apply_throughput_bucket_deltas(apply_throughput_bucket_deltas({}, a), b)
    m_ba = apply_throughput_bucket_deltas(apply_throughput_bucket_deltas({}, b), a)
    assert m_ab == m_ba

    m_abc = apply_throughput_bucket_deltas(apply_throughput_bucket_deltas(m_ab, c), {})
    m_acb = apply_throughput_bucket_deltas(apply_throughput_bucket_deltas(apply_throughput_bucket_deltas({}, a), c), b)
    assert m_abc == m_acb


def test_coordinator_skips_raw_sample_postprocessing_unaffected_by_metrics_aggregator():
    cfg = config.Config()
    cfg.add(config.Scope.application, "system", "env.name", "unittest")
    cfg.add(config.Scope.application, "driver", "metrics.aggregator", True)
    cfg.add(config.Scope.application, "reporting", "datastore.type", "elasticsearch")
    assert metrics.coordinator_skips_raw_sample_postprocessing(cfg) is False
