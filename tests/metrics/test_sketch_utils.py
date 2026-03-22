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

import collections
import datetime
import math

import pytest
from ddsketch.ddsketch import DEFAULT_REL_ACC, DDSketch

from esrally import config, metrics
from esrally.metrics.sketch_utils import (
    REQUEST_METRIC_SKETCH_RELATIVE_ACCURACY,
    RequestMetricSketchKey,
    approximate_sum_from_sketch,
    deserialize_sketch,
    merge_sketches,
    serialize_sketch,
    sketch_sum_and_avg,
)
from esrally.track import track

RACE_TIMESTAMP = datetime.datetime(2016, 1, 31)
RACE_ID = "6ebc6e53-ee20-4b0c-99b4-09697987e9f4"

# DDSketch default relative accuracy is ~1% (DEFAULT_REL_ACC). Quantile estimates are
# approximate; tests allow a small margin above that for floating-point noise.
_PCT_TOL = REQUEST_METRIC_SKETCH_RELATIVE_ACCURACY * 2


@pytest.fixture
def in_memory_metrics_store_sketch_path():
    cfg = config.Config()
    cfg.add(config.Scope.application, "system", "env.name", "unittest")
    cfg.add(config.Scope.application, "track", "params", {})
    ms = metrics.InMemoryMetricsStore(cfg)
    ms.open(
        RACE_ID,
        RACE_TIMESTAMP,
        "test",
        "append-no-conflicts",
        "defaults",
        create=True,
    )
    return ms


def test_request_metric_sketch_key_is_named_tuple():
    k = RequestMetricSketchKey(
        metric_name="latency",
        task="t1",
        operation="bulk",
        operation_type="bulk",
        sample_type="normal",
    )
    assert k.metric_name == "latency"
    assert k.task == "t1"
    assert k.operation == "bulk"
    assert k.operation_type == "bulk"
    assert k.sample_type == "normal"


def test_relative_accuracy_constant_matches_ddsketch_default():
    assert REQUEST_METRIC_SKETCH_RELATIVE_ACCURACY == DEFAULT_REL_ACC


def test_merge_sketches_empty_empty():
    a = DDSketch()
    b = DDSketch()
    m = merge_sketches(a, b)
    assert m.count == 0


def test_merge_sketches_nonempty_empty():
    a = DDSketch()
    for x in (1.0, 2.0, 3.0):
        a.add(x)
    b = DDSketch()
    m = merge_sketches(a, b)
    assert m.count == 3
    assert m.get_quantile_value(0.5) == pytest.approx(a.get_quantile_value(0.5))


def test_merge_sketches_empty_nonempty():
    a = DDSketch()
    b = DDSketch()
    for x in (10.0, 20.0):
        b.add(x)
    m = merge_sketches(a, b)
    assert m.count == 2


def test_merge_sketches_commutative_on_synthetic_data():
    a = DDSketch()
    b = DDSketch()
    for x in (1.0, 5.0, 9.0):
        a.add(x)
    for x in (2.0, 8.0):
        b.add(x)
    m1 = merge_sketches(a=a, b=b)
    m2 = merge_sketches(a=b, b=a)
    assert m1.count == 5
    assert m2.count == 5
    for q in (0.0, 0.25, 0.5, 0.75, 1.0):
        v1 = m1.get_quantile_value(q)
        v2 = m2.get_quantile_value(q)
        assert v1 is not None and v2 is not None
        assert math.isclose(v1, v2, rel_tol=0.0, abs_tol=1e-9)


def test_merge_sketches_monotonic_quantiles():
    s = DDSketch()
    for x in range(100):
        s.add(float(x))
    qs = []
    for q in (0.1, 0.5, 0.9):
        v = s.get_quantile_value(q)
        assert v is not None
        qs.append(v)
    assert qs == sorted(qs)
    assert qs[0] <= qs[1] <= qs[2]


def test_merge_sketches_incompatible_raises():
    a = DDSketch(relative_accuracy=0.01)
    b = DDSketch(relative_accuracy=0.05)
    a.add(1.0)
    b.add(2.0)
    with pytest.raises(ValueError, match="Cannot merge two DDSketches with different parameters"):
        merge_sketches(a, b)


def test_serialize_deserialize_roundtrip_counts_and_quantiles():
    s = DDSketch()
    for x in (0.5, 1.0, 1.5, 2.0, 100.0):
        s.add(x)
    raw = serialize_sketch(s)
    s2 = deserialize_sketch(raw)
    assert s2.count == s.count
    for q in (0.0, 0.5, 1.0):
        assert s2.get_quantile_value(q) == pytest.approx(s.get_quantile_value(q), rel=0.02)


def test_serialize_deserialize_empty_sketch():
    s = DDSketch()
    s2 = deserialize_sketch(serialize_sketch(s))
    assert s2.count == 0
    assert s2.get_quantile_value(0.5) is None


def test_sketch_sum_and_avg_uses_exact_when_present():
    s = DDSketch()
    for x in (2.0, 4.0, 8.0):
        s.add(x)
    total, avg = sketch_sum_and_avg(s)
    assert total == pytest.approx(14.0)
    assert avg == pytest.approx(14.0 / 3.0)


def test_sketch_sum_and_avg_recover_after_protobuf_deserialize():
    s = DDSketch()
    for x in range(1, 101):
        s.add(float(x))
    d = deserialize_sketch(serialize_sketch(s))
    assert d.sum == 0.0
    total, avg = sketch_sum_and_avg(d)
    assert total == pytest.approx(approximate_sum_from_sketch(d))
    assert avg == pytest.approx(total / 100.0)
    assert avg == pytest.approx(50.5, rel=REQUEST_METRIC_SKETCH_RELATIVE_ACCURACY * 2)


@pytest.mark.parametrize("metric_name", ("latency", "service_time", "processing_time"))
def test_merge_serialized_sketch_deltas_get_stats_mean_near_reference(
    in_memory_metrics_store_sketch_path, metric_name: metrics.AggregatedRequestTimingSketchMetricName
):
    """Wire bytes lose exact ``sum``/``avg``; :func:`sketch_sum_and_avg` restores approximate stats."""
    store = in_memory_metrics_store_sketch_path
    task = "index #1"
    op_type = track.OperationType.Bulk
    key = store.aggregated_request_timing_sketch_key(metric_name, task=task, operation_type=op_type)

    a = DDSketch()
    for v in range(1, 51):
        a.add(float(v))
    b = DDSketch()
    for v in range(51, 101):
        b.add(float(v))

    store.merge_request_sketch_delta(key, serialize_sketch(a))
    store.merge_request_sketch_delta(key, serialize_sketch(b))

    stats = store.get_stats(metric_name, task=task, operation_type=op_type, sample_type=metrics.SampleType.Normal)
    assert stats["count"] == 100
    assert stats["avg"] == pytest.approx(50.5, rel=_PCT_TOL)
    assert stats["sum"] == pytest.approx(50.5 * 100, rel=_PCT_TOL)


@pytest.mark.parametrize("metric_name", ("latency", "service_time", "processing_time"))
def test_merge_two_sketch_deltas_percentiles_within_ddsketch_accuracy(
    in_memory_metrics_store_sketch_path, metric_name: metrics.AggregatedRequestTimingSketchMetricName
):
    store = in_memory_metrics_store_sketch_path
    task = "index #1"
    op_type = track.OperationType.Bulk
    key = store.aggregated_request_timing_sketch_key(metric_name, task=task, operation_type=op_type)

    a = DDSketch()
    for v in range(1, 51):
        a.add(float(v))
    b = DDSketch()
    for v in range(51, 101):
        b.add(float(v))

    store.merge_request_sketch_delta(key, serialize_sketch(a))
    store.merge_request_sketch_delta(key, serialize_sketch(b))

    raw = [float(x) for x in range(1, 101)]
    sorted_raw = sorted(raw)
    percentiles = [50, 90, 99, 100]
    ref = collections.OrderedDict()
    for p in percentiles:
        ref[p] = metrics.InMemoryMetricsStore.percentile_value(sorted_raw, p)

    got = store.get_percentiles(
        metric_name,
        task=task,
        operation_type=op_type,
        sample_type=metrics.SampleType.Normal,
        percentiles=percentiles,
    )
    assert got is not None
    for p in percentiles:
        assert got[p] == pytest.approx(ref[p], rel=_PCT_TOL), f"percentile {p}"


@pytest.mark.parametrize("metric_name", ("latency", "service_time", "processing_time"))
def test_docs_only_timing_metric_unchanged_no_sketch_merge(
    in_memory_metrics_store_sketch_path, metric_name: metrics.AggregatedRequestTimingSketchMetricName
):
    """No ``merge_request_sketch_delta``: legacy doc scan only (sketch table empty)."""
    store = in_memory_metrics_store_sketch_path
    task = "index #1"
    op_type = track.OperationType.Bulk
    store.put_value_cluster_level(metric_name, 10.0, "ms", task=task, operation_type=op_type)
    store.put_value_cluster_level(metric_name, 20.0, "ms", task=task, operation_type=op_type)
    store.put_value_cluster_level(metric_name, 30.0, "ms", task=task, operation_type=op_type)

    assert store._request_sketch_table == {}

    values = store.get(metric_name, task=task, operation_type=op_type, sample_type=metrics.SampleType.Normal)
    sorted_raw = sorted(values)
    percentiles = [50, 100]
    ref = collections.OrderedDict()
    for p in percentiles:
        ref[p] = metrics.InMemoryMetricsStore.percentile_value(sorted_raw, p)

    got = store.get_percentiles(
        metric_name,
        task=task,
        operation_type=op_type,
        sample_type=metrics.SampleType.Normal,
        percentiles=percentiles,
    )
    assert got[50] == ref[50]
    assert got[100] == ref[100]

    stats = store.get_stats(metric_name, task=task, operation_type=op_type, sample_type=metrics.SampleType.Normal)
    assert stats["count"] == 3
    assert stats["min"] == 10.0
    assert stats["max"] == 30.0
    assert stats["avg"] == pytest.approx(20.0)
    assert stats["sum"] == pytest.approx(60.0)


@pytest.mark.parametrize("metric_name", ("latency", "service_time", "processing_time"))
def test_sketch_only_get_stats_get_mean_get_percentiles_single_timing_sequence(
    in_memory_metrics_store_sketch_path, metric_name: metrics.AggregatedRequestTimingSketchMetricName
):
    """Mirrors ``GlobalStatsCalculator`` timing paths: ``get_stats`` then percentiles + mean."""
    store = in_memory_metrics_store_sketch_path
    task = "index #1"
    op_type = track.OperationType.Bulk
    key = store.aggregated_request_timing_sketch_key(metric_name, task=task, operation_type=op_type)

    sk = DDSketch()
    for v in (2.0, 4.0, 8.0, 16.0):
        sk.add(v)
    store.merge_request_sketch_delta(key, sk)

    st = metrics.SampleType.Normal
    stats = store.get_stats(metric_name, task=task, operation_type=op_type, sample_type=st)
    assert stats["count"] == 4
    assert stats["avg"] == pytest.approx(sk.avg, rel=_PCT_TOL)
    assert stats["sum"] == pytest.approx(sk.sum, rel=_PCT_TOL)

    sample_size = stats["count"]
    assert sample_size > 0
    pct_list = metrics.percentiles_for_sample_size(sample_size)
    percentiles = store.get_percentiles(metric_name, task=task, operation_type=op_type, sample_type=st, percentiles=pct_list)
    mean = store.get_mean(metric_name, task=task, operation_type=op_type, sample_type=st)

    assert mean == pytest.approx(stats["avg"], rel=_PCT_TOL)
    for p, v in percentiles.items():
        q = float(p) / 100.0
        assert v == pytest.approx(sk.get_quantile_value(q), rel=_PCT_TOL)


@pytest.mark.parametrize("metric_name", ("latency", "service_time", "processing_time"))
def test_merge_accepts_live_sketch_instance(
    in_memory_metrics_store_sketch_path, metric_name: metrics.AggregatedRequestTimingSketchMetricName
):
    store = in_memory_metrics_store_sketch_path
    task = "t1"
    op_type = track.OperationType.Search
    key = store.aggregated_request_timing_sketch_key(metric_name, task=task, operation_type=op_type)
    s = DDSketch()
    s.add(42.0)
    store.merge_request_sketch_delta(key, s)
    assert store._request_sketch_table[key].merged_sketch.count == 1


def test_merge_all_three_request_timing_sketches_on_same_store(in_memory_metrics_store_sketch_path):
    """Independent merged sketches for latency, service_time, and processing_time on one store."""
    store = in_memory_metrics_store_sketch_path
    task = "index #1"
    op_type = track.OperationType.Bulk
    st = metrics.SampleType.Normal
    configs: tuple[tuple[metrics.AggregatedRequestTimingSketchMetricName, range], ...] = (
        ("latency", range(1, 101)),
        ("service_time", range(100, 200)),
        ("processing_time", range(200, 300)),
    )
    for metric_name, values in configs:
        key = store.aggregated_request_timing_sketch_key(metric_name, task=task, operation_type=op_type)
        sk = DDSketch()
        for v in values:
            sk.add(float(v))
        store.merge_request_sketch_delta(key, sk)

    assert store.get_stats("latency", task=task, operation_type=op_type, sample_type=st)["avg"] == pytest.approx(50.5, rel=_PCT_TOL)
    assert store.get_stats("service_time", task=task, operation_type=op_type, sample_type=st)["avg"] == pytest.approx(149.5, rel=_PCT_TOL)
    assert store.get_stats("processing_time", task=task, operation_type=op_type, sample_type=st)["avg"] == pytest.approx(
        249.5, rel=_PCT_TOL
    )

    for metric_name, values in configs:
        raw = [float(x) for x in values]
        sorted_raw = sorted(raw)
        p50_ref = metrics.InMemoryMetricsStore.percentile_value(sorted_raw, 50)
        got = store.get_percentiles(metric_name, task=task, operation_type=op_type, sample_type=st, percentiles=[50])
        assert got[50] == pytest.approx(p50_ref, rel=_PCT_TOL), metric_name


@pytest.mark.parametrize("metric_name", ("latency", "service_time", "processing_time"))
def test_request_timing_sketch_not_used_when_sample_type_unspecified(
    in_memory_metrics_store_sketch_path, metric_name: metrics.AggregatedRequestTimingSketchMetricName
):
    """``get_percentiles`` defaults ``sample_type`` to None → legacy path (not Normal-only sketch)."""
    store = in_memory_metrics_store_sketch_path
    task = "index #1"
    op_type = track.OperationType.Bulk
    key = store.aggregated_request_timing_sketch_key(metric_name, task=task, operation_type=op_type)
    sk = DDSketch()
    sk.add(99.0)
    store.merge_request_sketch_delta(key, sk)

    store.put_value_cluster_level(metric_name, 1.0, "ms", task=task, operation_type=op_type)

    got = store.get_percentiles(metric_name, task=task, operation_type=op_type, percentiles=[100])
    assert got[100] == pytest.approx(1.0)


def test_merge_request_sketch_delta_rejects_negative_outcome_deltas(in_memory_metrics_store_sketch_path):
    store = in_memory_metrics_store_sketch_path
    key = store.aggregated_request_timing_sketch_key("service_time", task="t", operation_type=track.OperationType.Bulk)
    sk = DDSketch()
    sk.add(1.0)
    with pytest.raises(ValueError, match="non-negative"):
        store.merge_request_sketch_delta(key, sk, success_count_delta=-1, failure_count_delta=0)
    with pytest.raises(ValueError, match="non-negative"):
        store.merge_request_sketch_delta(key, sk, success_count_delta=0, failure_count_delta=-1)


def test_get_error_rate_sketch_counters_success_only(in_memory_metrics_store_sketch_path):
    store = in_memory_metrics_store_sketch_path
    task = "index #1"
    op_type = track.OperationType.Bulk
    key = store.aggregated_request_timing_sketch_key("service_time", task=task, operation_type=op_type)
    sk = DDSketch()
    sk.add(5.0)
    store.merge_request_sketch_delta(key, sk, success_count_delta=100, failure_count_delta=0)
    assert store.get_error_rate(task, operation_type=op_type, sample_type=metrics.SampleType.Normal) == 0.0


def test_get_error_rate_sketch_counters_failure_only(in_memory_metrics_store_sketch_path):
    store = in_memory_metrics_store_sketch_path
    task = "index #1"
    op_type = track.OperationType.Bulk
    key = store.aggregated_request_timing_sketch_key("service_time", task=task, operation_type=op_type)
    sk = DDSketch()
    sk.add(1.0)
    store.merge_request_sketch_delta(key, sk, success_count_delta=0, failure_count_delta=4)
    assert store.get_error_rate(task, operation_type=op_type, sample_type=metrics.SampleType.Normal) == 1.0


def test_get_error_rate_sketch_counters_mixed(in_memory_metrics_store_sketch_path):
    store = in_memory_metrics_store_sketch_path
    task = "index #1"
    op_type = track.OperationType.Bulk
    key = store.aggregated_request_timing_sketch_key("service_time", task=task, operation_type=op_type)
    sk = DDSketch()
    sk.add(2.0)
    store.merge_request_sketch_delta(key, sk, success_count_delta=7, failure_count_delta=3)
    assert store.get_error_rate(task, operation_type=op_type, sample_type=metrics.SampleType.Normal) == pytest.approx(0.3)


def test_get_error_rate_counters_take_precedence_over_service_time_docs(in_memory_metrics_store_sketch_path):
    """Merged outcome tallies win when non-zero so sketch-only workers need not duplicate docs."""
    store = in_memory_metrics_store_sketch_path
    task = "index #1"
    op_type = track.OperationType.Bulk
    key = store.aggregated_request_timing_sketch_key("service_time", task=task, operation_type=op_type)
    sk = DDSketch()
    sk.add(3.0)
    store.merge_request_sketch_delta(key, sk, success_count_delta=9, failure_count_delta=1)
    store.put_value_cluster_level(
        "service_time",
        10.0,
        "ms",
        task=task,
        operation_type=op_type,
        sample_type=metrics.SampleType.Normal,
        meta_data={"success": True},
    )
    store.put_value_cluster_level(
        "service_time",
        11.0,
        "ms",
        task=task,
        operation_type=op_type,
        sample_type=metrics.SampleType.Normal,
        meta_data={"success": False},
    )
    assert store.get_error_rate(task, operation_type=op_type, sample_type=metrics.SampleType.Normal) == pytest.approx(0.1)


def test_merge_request_sketch_outcome_counts_order_independent(in_memory_metrics_store_sketch_path):
    store_a = in_memory_metrics_store_sketch_path
    cfg = config.Config()
    cfg.add(config.Scope.application, "system", "env.name", "unittest")
    cfg.add(config.Scope.application, "track", "params", {})
    store_b = metrics.InMemoryMetricsStore(cfg)
    store_b.open(RACE_ID, RACE_TIMESTAMP, "test", "append-no-conflicts", "defaults", create=True)
    task = "index #1"
    op_type = track.OperationType.Bulk
    key = store_a.aggregated_request_timing_sketch_key("service_time", task=task, operation_type=op_type)
    assert key == store_b.aggregated_request_timing_sketch_key("service_time", task=task, operation_type=op_type)
    da = DDSketch()
    da.add(1.0)
    db = DDSketch()
    db.add(2.0)
    store_a.merge_request_sketch_delta(key, da, success_count_delta=4, failure_count_delta=1)
    store_a.merge_request_sketch_delta(key, db, success_count_delta=1, failure_count_delta=3)
    store_b.merge_request_sketch_delta(key, db, success_count_delta=1, failure_count_delta=3)
    store_b.merge_request_sketch_delta(key, da, success_count_delta=4, failure_count_delta=1)
    sa = store_a._request_sketch_table[key]
    sb = store_b._request_sketch_table[key]
    assert sa.success_count == sb.success_count == 5
    assert sa.failure_count == sb.failure_count == 4
    assert sa.merged_sketch.count == sb.merged_sketch.count == 2
    assert store_a.get_error_rate(task, operation_type=op_type, sample_type=metrics.SampleType.Normal) == pytest.approx(4 / 9)
    assert store_b.get_error_rate(task, operation_type=op_type, sample_type=metrics.SampleType.Normal) == pytest.approx(4 / 9)


def test_aggregated_latency_sketch_key_matches_request_timing_latency(in_memory_metrics_store_sketch_path):
    store = in_memory_metrics_store_sketch_path
    task = "t1"
    op_type = track.OperationType.Bulk
    assert store.aggregated_latency_sketch_key(task=task, operation_type=op_type) == store.aggregated_request_timing_sketch_key(
        "latency", task=task, operation_type=op_type
    )
