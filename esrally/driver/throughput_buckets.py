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

"""
Fixed-width throughput buckets keyed by coordinator epoch.

This module supports **additive** merging of per-worker bucket totals so a
coordinator can combine partial histograms. Buckets are aligned to::

    bucket_index = floor((absolute_time - epoch_seconds) / bucket_width_seconds)

Only samples that participate in :meth:`ThroughputCalculator.calculate_task_throughput`
are counted: those with ``throughput is None``. Samples where the runner
supplied ``throughput`` are skipped here (the legacy calculator uses
:meth:`ThroughputCalculator.map_task_throughput` instead and does not apply
sliding-window bucket logic to them).
"""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from esrally.driver.driver import Sample


@dataclass(frozen=True, slots=True)
class ThroughputBucketKey:
    """Hashable key for additive throughput bucket totals."""

    task_name: str
    bucket_index: int
    sample_type: str
    ops_unit: str


class ThroughputBucketAccumulator:
    """
    Accumulate ``total_ops`` into fixed-width buckets relative to a coordinator epoch.
    """

    def __init__(self, epoch_seconds: float, bucket_width_seconds: float = 1.0) -> None:
        if bucket_width_seconds <= 0:
            raise ValueError("bucket_width_seconds must be positive")
        self._epoch_seconds = epoch_seconds
        self._bucket_width_seconds = bucket_width_seconds

    @property
    def epoch_seconds(self) -> float:
        return self._epoch_seconds

    @property
    def bucket_width_seconds(self) -> float:
        return self._bucket_width_seconds

    def bucket_index(self, absolute_time_seconds: float) -> int:
        return math.floor((absolute_time_seconds - self._epoch_seconds) / self._bucket_width_seconds)

    def contribute_samples(self, samples: Sequence[Sample]) -> dict[ThroughputBucketKey, int]:
        totals: dict[ThroughputBucketKey, int] = {}
        for sample in samples:
            if sample.throughput is not None:
                continue
            unit = sample.total_ops_unit if sample.total_ops_unit is not None else ""
            key = ThroughputBucketKey(
                task_name=sample.task.name,
                bucket_index=self.bucket_index(sample.absolute_time),
                sample_type=sample.sample_type.name.lower(),
                ops_unit=unit,
            )
            totals[key] = totals.get(key, 0) + sample.total_ops
        return totals


def merge_bucket_totals(
    a: Mapping[ThroughputBucketKey, int],
    b: Mapping[ThroughputBucketKey, int],
) -> dict[ThroughputBucketKey, int]:
    """
    Additively merge two bucket total maps. Missing keys are treated as zero.
    The operation is commutative for key sets and values.
    """
    merged: dict[ThroughputBucketKey, int] = dict(a)
    for key, value in b.items():
        merged[key] = merged.get(key, 0) + value
    return merged
