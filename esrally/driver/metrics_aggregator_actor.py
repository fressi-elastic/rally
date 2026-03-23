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

from typing import Any

import thespian.actors  # type: ignore[import-untyped]

from esrally import actor, metrics, types
from esrally.driver.metrics_aggregator import (
    RequestSketchMergeDelta,
    ThroughputBucketMergeDelta,
    apply_sketch_merge_delta,
    apply_throughput_bucket_deltas,
)
from esrally.driver.throughput_buckets import ThroughputBucketKey


class BootstrapMetricsAggregator:
    """Initialize :class:`MetricsAggregatorActor` with race identity aligned to :func:`metrics.metrics_store`."""

    def __init__(self, cfg: types.Config, track_name: str, challenge_name: str):
        self.config = cfg
        self.track_name = track_name
        self.challenge_name = challenge_name


class MetricsAggregatorActor(actor.RallyActor):
    """
    Coordinator-side merge of sketch and throughput-bucket deltas into an in-memory metrics store.

    Created only when ``driver.metrics.aggregator`` is true; workers are not wired to this actor yet (T13+).
    """

    def __init__(self):
        super().__init__()
        self._metrics_store: metrics.InMemoryMetricsStore | None = None
        self._throughput_totals: dict[ThroughputBucketKey, int] | None = None

    @actor.no_retry("metrics aggregator")  # pylint: disable=no-value-for-parameter
    def receiveMsg_BootstrapMetricsAggregator(self, msg: BootstrapMetricsAggregator, sender: Any) -> None:
        store = metrics.InMemoryMetricsStore(cfg=msg.config)
        race_id = msg.config.opts("system", "race.id")
        race_timestamp = msg.config.opts("system", "time.start")
        selected_car = msg.config.opts("mechanic", "car.names")
        store.open(
            race_id,
            race_timestamp,
            msg.track_name,
            msg.challenge_name,
            selected_car,
            create=True,
        )
        self._metrics_store = store
        self._throughput_totals = {}

    @actor.no_retry("metrics aggregator")  # pylint: disable=no-value-for-parameter
    def receiveMsg_RequestSketchMergeDelta(self, msg: RequestSketchMergeDelta, sender: Any) -> None:
        if self._metrics_store is None:
            self.logger.warning("RequestSketchMergeDelta before bootstrap; ignoring.")
            return
        apply_sketch_merge_delta(self._metrics_store, msg)

    @actor.no_retry("metrics aggregator")  # pylint: disable=no-value-for-parameter
    def receiveMsg_ThroughputBucketMergeDelta(self, msg: ThroughputBucketMergeDelta, sender: Any) -> None:
        if self._throughput_totals is None:
            self.logger.warning("ThroughputBucketMergeDelta before bootstrap; ignoring.")
            return
        self._throughput_totals = apply_throughput_bucket_deltas(self._throughput_totals, msg.buckets)

    @actor.no_retry("metrics aggregator")  # pylint: disable=no-value-for-parameter
    def receiveMsg_ActorExitRequest(self, msg: thespian.actors.ActorExitRequest, sender: Any) -> None:
        if self._metrics_store is not None and self._metrics_store.opened:
            self._metrics_store.close()
        self._metrics_store = None
        self._throughput_totals = None
