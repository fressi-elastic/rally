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

import pytest

from esrally import metrics, track
from esrally.driver import driver
from esrally.driver.throughput_buckets import (
    ThroughputBucketAccumulator,
    ThroughputBucketKey,
    merge_bucket_totals,
)


def _task(name="index"):
    return track.Task(name, track.Operation("index-op", "bulk", param_source="driver-test-param-source"))


class TestMergeBucketTotals:
    def test_disjoint_workers_merge_sums(self):
        """Two workers cover disjoint bucket indices; merged totals match per-bucket sums."""
        epoch = 1000.0
        acc = ThroughputBucketAccumulator(epoch_seconds=epoch, bucket_width_seconds=1.0)
        t = _task()
        w1 = [
            driver.Sample(0, 1000.2, 0, 0, t, metrics.SampleType.Normal, None, 0, 0, 0, None, 100, "docs", 1, 1),
            driver.Sample(0, 1000.4, 0, 0, t, metrics.SampleType.Normal, None, 0, 0, 0, None, 50, "docs", 1, 1),
        ]
        w2 = [
            driver.Sample(1, 1002.1, 0, 0, t, metrics.SampleType.Normal, None, 0, 0, 0, None, 200, "docs", 1, 1),
        ]
        a = acc.contribute_samples(w1)
        b = acc.contribute_samples(w2)
        merged = merge_bucket_totals(a, b)
        assert merged[ThroughputBucketKey("index", acc.bucket_index(1000.2), "normal", "docs")] == 150
        assert merged[ThroughputBucketKey("index", acc.bucket_index(1002.1), "normal", "docs")] == 200

    def test_overlapping_buckets_additive(self):
        key = ThroughputBucketKey("index", 5, "normal", "docs")
        a = {key: 30}
        b = {key: 70}
        assert merge_bucket_totals(a, b) == {key: 100}

    def test_merge_commutative_sum(self):
        k1 = ThroughputBucketKey(task_name="a", bucket_index=0, sample_type="normal", ops_unit="docs")
        k2 = ThroughputBucketKey(task_name="b", bucket_index=1, sample_type="warmup", ops_unit="docs")
        a = {k1: 1, k2: 2}
        b = {k1: 10}
        assert merge_bucket_totals(a=a, b=b) == merge_bucket_totals(a=b, b=a)


class TestSampleTypeBoundaries:
    def test_same_wall_bucket_different_sample_type_distinct_keys(self):
        acc = ThroughputBucketAccumulator(epoch_seconds=500.0, bucket_width_seconds=1.0)
        t = _task()
        wall = 500.3
        s_warmup = driver.Sample(0, wall, 0, 0, t, metrics.SampleType.Warmup, None, 0, 0, 0, None, 1, "docs", 1, 1)
        s_normal = driver.Sample(0, wall, 0, 0, t, metrics.SampleType.Normal, None, 0, 0, 0, None, 2, "docs", 1, 1)
        totals = acc.contribute_samples([s_warmup, s_normal])
        bi = acc.bucket_index(wall)
        assert totals[ThroughputBucketKey("index", bi, "warmup", "docs")] == 1
        assert totals[ThroughputBucketKey("index", bi, "normal", "docs")] == 2


class TestThroughputCalculatorReference:
    def test_merged_partitions_match_full_contribution(self):
        """
        Partitioning samples across two logical workers and merging bucket maps
        reproduces a single pass over the combined list.
        """

        epoch = 38595.0
        acc = ThroughputBucketAccumulator(epoch_seconds=epoch, bucket_width_seconds=1.0)
        op = track.Operation("index", track.OperationType.Bulk, param_source="driver-test-param-source")
        task = track.Task("index", op)
        samples = [
            driver.Sample(0, 38595, 21, 0, task, metrics.SampleType.Normal, None, -1, -1, -1, None, 5000, "docs", 1, 1),
            driver.Sample(0, 38596, 22, 0, task, metrics.SampleType.Normal, None, -1, -1, -1, None, 5000, "docs", 1, 1),
            driver.Sample(0, 38597, 23, 0, task, metrics.SampleType.Normal, None, -1, -1, -1, None, 5000, "docs", 1, 1),
        ]
        part_a = samples[:1]
        part_b = samples[1:]
        full = acc.contribute_samples(samples)
        merged = merge_bucket_totals(acc.contribute_samples(part_a), acc.contribute_samples(part_b))
        assert full == merged
        assert sum(full.values()) == sum(s.total_ops for s in samples)

    def test_legacy_calculator_comparison_and_limitations(self):
        """
        Compare :class:`ThroughputBucketAccumulator` to :class:`ThroughputCalculator` on one synthetic list.

        **What we compare:** Global sum of ``total_ops`` over samples with ``throughput is None`` matches
        the sum of merged bucket totals (same inclusion rule as ``calculate_task_throughput``).

        **Known differences (not asserted as equality):**

        * **Anchoring:** ``ThroughputCalculator`` aligns its sliding window using
          ``start_time = first_sample.absolute_time - first_sample.time_period`` per task, and emits
          **rate** samples (cumulative ops / elapsed interval) when internal bucket rules fire.
          ``ThroughputBucketAccumulator`` uses **coordinator epoch** and fixed-width indices; it stores
          **raw op counts** per wall-clock bucket, not rates.

        * **Output shape:** Legacy output is a list of ``(absolute_time, relative_time, sample_type, throughput, unit)``
          points with **time-weighted** denominators; bucket totals are **additive counts** keyed by
          ``(task_name, bucket_index, sample_type, ops_unit)``. Per-bucket "throughput" from legacy
          (rate × effective interval) generally **does not** equal the naive bucket sum unless
          alignment and windowing happen to coincide.

        * **Sample type handling:** Legacy :class:`ThroughputCalculator.TaskStats` may advance the
          active ``sample_type`` across samples; bucket keys always use each sample's own ``sample_type``.

        * **Explicit throughput:** Samples with ``throughput`` set are mapped by ``map_task_throughput`` in
          legacy code and are **skipped** by ``contribute_samples``.
        """
        epoch = 38595.0
        acc = ThroughputBucketAccumulator(epoch_seconds=epoch, bucket_width_seconds=1.0)
        op = track.Operation("index", track.OperationType.Bulk, param_source="driver-test-param-source")
        task = track.Task("index", op)
        samples = [
            driver.Sample(0, 38595, 21, 0, task, metrics.SampleType.Normal, None, -1, -1, -1, None, 5000, "docs", 1, 1),
            driver.Sample(0, 38596, 22, 0, task, metrics.SampleType.Normal, None, -1, -1, -1, None, 5000, "docs", 1, 1),
            driver.Sample(0, 38597, 23, 0, task, metrics.SampleType.Normal, None, -1, -1, -1, None, 5000, "docs", 1, 1),
        ]
        bucket_totals = acc.contribute_samples(samples)
        assert sum(bucket_totals.values()) == 15000

        calc = driver.ThroughputCalculator()
        legacy = calc.calculate(samples, bucket_interval_secs=1)
        assert task in legacy
        # Rates exist for reporting; they are not the same as count/bucket_width for this synthetic path.
        assert legacy[task][0][3] == 5000

        # Illustrate mismatch vs naive count/second interpretation: first legacy point is a rate, not bucket ops.
        assert bucket_totals[ThroughputBucketKey(task.name, 0, "normal", "docs")] == 5000


def test_bucket_index_respects_width():
    acc = ThroughputBucketAccumulator(epoch_seconds=10.0, bucket_width_seconds=2.0)
    assert acc.bucket_index(10.0) == 0
    assert acc.bucket_index(11.9) == 0
    assert acc.bucket_index(12.0) == 1


def test_invalid_bucket_width():
    with pytest.raises(ValueError):
        ThroughputBucketAccumulator(epoch_seconds=0.0, bucket_width_seconds=0.0)
