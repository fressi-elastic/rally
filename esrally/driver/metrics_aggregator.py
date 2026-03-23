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

"""Pure merge helpers for coordinator-side metrics aggregation (distributed request metrics)."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from ddsketch.ddsketch import DDSketch

from esrally import metrics
from esrally.driver.throughput_buckets import ThroughputBucketKey, merge_bucket_totals
from esrally.metrics.sketch_utils import RequestMetricSketchKey, serialize_sketch
from esrally.utils import convert


@dataclass(frozen=True, slots=True)
class RequestSketchMergeDelta:
    """Wire payload merging one DDSketch delta (and optional outcome tallies) into a store."""

    key: RequestMetricSketchKey
    sketch_bytes: bytes
    success_count_delta: int = 0
    failure_count_delta: int = 0


@dataclass(frozen=True, slots=True)
class ThroughputBucketMergeDelta:
    """Additive throughput bucket totals from a worker (fixed-width buckets, coordinator epoch)."""

    buckets: Mapping[ThroughputBucketKey, int]


def apply_sketch_merge_delta(store: metrics.InMemoryMetricsStore, delta: RequestSketchMergeDelta) -> None:
    store.merge_request_sketch_delta(
        delta.key,
        delta.sketch_bytes,
        success_count_delta=delta.success_count_delta,
        failure_count_delta=delta.failure_count_delta,
    )


def apply_throughput_bucket_deltas(
    into: dict[ThroughputBucketKey, int],
    delta: Mapping[ThroughputBucketKey, int],
) -> dict[ThroughputBucketKey, int]:
    return merge_bucket_totals(into, delta)


def request_sketch_merge_deltas_from_samples(raw_samples: Sequence[Any], *, downsample_factor: int) -> list[RequestSketchMergeDelta]:
    """
    Build protobuf sketch merge messages from a worker sample batch.

    Mirrors :class:`~esrally.driver.request_metrics.RequestMetricsRecorder` downsample cadence and
    :class:`~esrally.metrics.SampleType` handling for aggregated timing streams (Normal samples only).
    """
    if downsample_factor < 1:
        raise ValueError("downsample_factor must be >= 1")

    def ms_from_seconds(seconds_val: float | None) -> float:
        v = convert.seconds_to_ms(seconds_val)
        return 0.0 if v is None else float(v)

    accum: dict[RequestMetricSketchKey, DDSketch] = {}
    success_ct: dict[tuple[str, str], int] = defaultdict(int)
    failure_ct: dict[tuple[str, str], int] = defaultdict(int)

    def sketch_for(key: RequestMetricSketchKey) -> DDSketch:
        sk = accum.get(key)
        if sk is None:
            sk = DDSketch()
            accum[key] = sk
        return sk

    def consume_dependent_sample(sample: Any) -> None:
        """Matches :meth:`RequestMetricsRecorder.__call__` dependent branch (service_time only)."""
        if sample.sample_type != metrics.SampleType.Normal:
            return
        task = sample.task.name
        op_type = sample.operation_type
        meta = sample.request_meta_data or {}
        if meta.get("success", True):
            success_ct[(task, op_type)] += 1
        else:
            failure_ct[(task, op_type)] += 1
        st_key = metrics.InMemoryMetricsStore.aggregated_request_timing_sketch_key("service_time", task=task, operation_type=op_type)
        sketch_for(st_key).add(ms_from_seconds(sample.service_time))

    def consume_primary_sample(sample: Any) -> None:
        if sample.sample_type != metrics.SampleType.Normal:
            return
        task = sample.task.name
        op_type = sample.operation_type
        meta = sample.request_meta_data or {}
        if meta.get("success", True):
            success_ct[(task, op_type)] += 1
        else:
            failure_ct[(task, op_type)] += 1

        lat_key = metrics.InMemoryMetricsStore.aggregated_request_timing_sketch_key("latency", task=task, operation_type=op_type)
        st_key = metrics.InMemoryMetricsStore.aggregated_request_timing_sketch_key("service_time", task=task, operation_type=op_type)
        pt_key = metrics.InMemoryMetricsStore.aggregated_request_timing_sketch_key("processing_time", task=task, operation_type=op_type)
        sketch_for(lat_key).add(ms_from_seconds(sample.latency))
        sketch_for(st_key).add(ms_from_seconds(sample.service_time))
        sketch_for(pt_key).add(ms_from_seconds(sample.processing_time))

        for dep in sample.dependent_timings:
            consume_dependent_sample(dep)

    for idx, sample in enumerate(raw_samples):
        if idx % downsample_factor != 0:
            continue
        consume_primary_sample(sample)

    deltas: list[RequestSketchMergeDelta] = []
    for key, sk in accum.items():
        s = f = 0
        if key.metric_name == "service_time":
            s = success_ct[(key.task, key.operation_type)]
            f = failure_ct[(key.task, key.operation_type)]
        if sk.count == 0 and s == 0 and f == 0:
            continue
        deltas.append(RequestSketchMergeDelta(key, serialize_sketch(sk), success_count_delta=s, failure_count_delta=f))
    return deltas
