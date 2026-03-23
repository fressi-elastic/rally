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

from collections.abc import Mapping
from dataclasses import dataclass

from esrally import metrics
from esrally.driver.throughput_buckets import ThroughputBucketKey, merge_bucket_totals
from esrally.metrics.sketch_utils import RequestMetricSketchKey


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
