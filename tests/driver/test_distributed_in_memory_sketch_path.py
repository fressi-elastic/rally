# Licensed to Elasticsearch B.V. under one or more contributor
# license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright
# ownership. Elasticsearch B.V. licenses this file to you under
# the Apache License, Version 2.0 (the License); you may
# not use this file except in compliance with the License.
#
#	http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# AS IS BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""T13: distributed in-memory Option B — sketch deltas and reporting without per-request docs."""

import datetime

import pytest

from esrally import config, metrics, track
from esrally.driver import driver
from esrally.driver.metrics_aggregator import apply_sketch_merge_delta, request_sketch_merge_deltas_from_samples
from esrally.metrics import GlobalStatsCalculator


def _task():
    op = track.Operation("index", track.OperationType.Bulk, param_source="driver-test-param-source")
    return track.Task("index #1", operation=op, schedule="deterministic")


def _sample(latency_s, service_s, processing_s, success=True, client_id=0):
    t = _task()
    return driver.Sample(
        client_id,
        1000.0,
        100.0,
        0.0,
        t,
        metrics.SampleType.Normal,
        {"success": success},
        latency_s,
        service_s,
        processing_s,
        None,
        100,
        "docs",
        1,
        0.5,
    )


def test_request_sketch_merge_deltas_respects_downsample_factor():
    samples = [_sample(0.01, 0.02, 0.03) for _ in range(4)]
    deltas_all = request_sketch_merge_deltas_from_samples(samples, downsample_factor=1)
    lat_key = metrics.InMemoryMetricsStore.aggregated_request_timing_sketch_key(
        "latency", task="index #1", operation_type=track.OperationType.Bulk
    )
    lat_delta = next(d for d in deltas_all if d.key == lat_key)
    from esrally.metrics.sketch_utils import deserialize_sketch

    assert deserialize_sketch(lat_delta.sketch_bytes).count == 4

    deltas_half = request_sketch_merge_deltas_from_samples(samples, downsample_factor=2)
    lat_delta2 = next(d for d in deltas_half if d.key == lat_key)
    assert deserialize_sketch(lat_delta2.sketch_bytes).count == 2


def test_global_stats_calculator_sketch_only_synthetic_race():
    """Coordinator store has no request timing docs; merged sketches supply latency / service_time / error_rate."""
    cfg = config.Config()
    cfg.add(config.Scope.application, "system", "env.name", "unittest")
    store = metrics.InMemoryMetricsStore(cfg)
    race_ts = datetime.datetime(2016, 1, 31)
    race_id = "fb26018b-428d-4528-b36b-cf8c54a303ec"
    t = _task()
    challenge = track.Challenge(name="append-fast", schedule=[[t]], meta_data={})
    trk = track.Track(name="geonames", meta_data={})

    store.open(race_id, race_ts, "geonames", "append-fast", "defaults", create=True)

    batch_a = [
        _sample(0.010, 0.020, 0.015, success=True),
        _sample(0.012, 0.022, 0.016, success=False),
    ]
    batch_b = [_sample(0.011, 0.021, 0.014, success=True)]
    for d in request_sketch_merge_deltas_from_samples(batch_a, downsample_factor=1):
        apply_sketch_merge_delta(store, d)
    for d in request_sketch_merge_deltas_from_samples(batch_b, downsample_factor=1):
        apply_sketch_merge_delta(store, d)

    assert len(store.docs) == 0

    result = GlobalStatsCalculator(store=store, track=trk, challenge=challenge)()
    row = result.metrics("index #1")
    assert row is not None
    assert row["error_rate"] == pytest.approx(1 / 3)
    assert row["latency"]["mean"] is not None
    assert row["service_time"]["mean"] is not None
    assert row["processing_time"]["mean"] is not None
    assert row["latency"]["unit"] == "ms"


def test_request_sketch_deltas_large_batch_smoke():
    """Many samples collapse to a small number of merge messages (one per timing stream), not O(n) docs."""
    samples = [_sample(0.001 * (i % 50), 0.002 * (i % 50), 0.0015 * (i % 50)) for i in range(5000)]
    deltas = request_sketch_merge_deltas_from_samples(samples, downsample_factor=1)
    assert len(deltas) == 3

    cfg = config.Config()
    cfg.add(config.Scope.application, "system", "env.name", "unittest")
    store = metrics.InMemoryMetricsStore(cfg)
    store.open(
        "fb26018b-428d-4528-b36b-cf8c54a303ec",
        datetime.datetime(2016, 1, 31),
        "geonames",
        "append-fast",
        "defaults",
        create=True,
    )
    for d in deltas:
        apply_sketch_merge_delta(store, d)
    stats = store.get_stats(
        "latency",
        task="index #1",
        operation_type=track.OperationType.Bulk,
        sample_type=metrics.SampleType.Normal,
    )
    assert stats is not None
    assert stats["count"] == 5000
