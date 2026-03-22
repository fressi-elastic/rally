# Distributed request metrics — agent handoff (temporary)

**Status:** Temporary checklist for parallel / sequential **implementation** (separate effort from the documentation-only Cursor plan). **Delete or archive** when the feature ships.

**Canonical design:** [docs/distributed_request_metrics.rst](docs/distributed_request_metrics.rst) (includes [implementer recommendations](docs/distributed_request_metrics.rst#implementer-recommendations)).

## Principles (mandatory)

1. **Every task (T01, T02, …) starts from a clean tree where the full test suite relevant to touched areas passes** (or document a pre-existing skip).
2. **Every task ends the same way:** `pytest` for new/changed tests green; no intentional breakage behind an unset flag.
3. **Default configuration** must preserve **current behavior** until the final “enable by default” task (if ever).
4. Prefer **feature flags** (`driver.distributed_request_metrics` or sub-flags) for incomplete paths.

## Dependency graph (bottom → top)

```text
T01 → T02 → T03 → T04 → T05 → T06
                    ↓
T07 (refactor, no behavior change)
                    ↓
T08 → T09
                    ↓
T10 → T11 → T12 → T13
                    ↓
T14 (docs)   T15 (optional default-on policy)
```

Tasks T04–T06 can be one PR each or combined if tests stay focused.

---

### T01 — Add DDSketch dependency

| Field | Content |
|--------|---------|
| **Status** | **Done** |
| **Goal** | Declare the `ddsketch` (or agreed) package so CI and dev envs can import it. |
| **Start** | Clean main; tests pass. |
| **Work** | Add dependency in `pyproject.toml` / lockfile per project conventions. No Rally code changes except maybe `tests` import smoke. |
| **End** | `pip install -e .[...]` resolves; import works. |
| **Unit tests** | `tests/metrics/test_ddsketch_dependency.py` — `import ddsketch` (or package name) and construct default sketch, `add(1.0)` (ddsketch 3.x API), assert count. |
| **Integration tests** | None required. |
| **Acceptance** | CI installs deps without error. |

---

### T02 — Sketch utilities module

| Field | Content |
|--------|---------|
| **Status** | **Done** |
| **Goal** | Pure helpers: key type, `merge(a,b)`, `serialize` / `deserialize` roundtrip, relative accuracy config constant. |
| **Start** | T01 merged. |
| **Work** | `esrally/metrics/` package: `sketch_utils.py` (+ `metrics.py` moved to `__init__.py`). No `MetricsStore` coupling yet. |
| **End** | Module imported without side effects. |
| **Unit tests** | `tests/metrics/test_sketch_utils.py` — merge commutativity / monotonic quantiles on synthetic data; serialization roundtrip; empty sketch edge cases. |
| **Integration tests** | None. |
| **Acceptance** | 100% branch coverage on public helpers where reasonable. |

---

### T03 — InMemory sketch table (inactive default)

| Field | Content |
|--------|---------|
| **Status** | **Done** |
| **Goal** | `InMemoryMetricsStore` gains private `_request_sketch_table: dict[RequestMetricSketchKey, SketchState]` (or equivalent) **always empty** on construction; no behavior change for reads/writes yet. |
| **Start** | T02 merged. |
| **Work** | Add types; `open()` leaves table empty; existing `_add` / `docs` path unchanged. |
| **End** | All existing `metrics` / `driver` tests pass. |
| **Unit tests** | Extend `tests/metrics/metrics_test.py` (or new file) — assert new attributes exist, default empty, legacy `put` still works. |
| **Integration tests** | Run a subset: driver tests that use in-memory store if present in CI. |
| **Acceptance** | Zero diff in `GlobalStatsCalculator` outputs for existing flows. |

---

### T04 — `merge_request_sketch_delta` + sketch-backed `get_percentiles` (latency only first)

| Field | Content |
|--------|---------|
| **Status** | **Done** |
| **Goal** | API to merge worker/aggregator deltas into `_request_sketch_table`; `get_percentiles` for `name=="latency"` uses sketch when key matches else legacy scan. |
| **Start** | T03 merged. |
| **Work** | Implemented: `merge_request_sketch_delta`, `aggregated_latency_sketch_key`, routing for **latency** + **SampleType.Normal** only. **`get_stats`** / **`get_mean`** use the merged sketch when present so `GlobalStatsCalculator.single_latency` works with sketch-only data. Protobuf sketch bytes drop exact `sum`/`avg`; **`sketch_sum_and_avg`** in `sketch_utils` approximates sum from bucket centroids when `sum==0`. Sketch routing applies only when **`task` and `operation_type` are both non-None** (explicit stream); `sample_type` must be `Normal`. |
| **End** | Legacy-only races unchanged (no sketch → old path). |
| **Unit tests** | Synthetic: merge two deltas, `get_percentiles` matches reference sorted list within DDSketch accuracy; legacy docs-only store unchanged. |
| **Integration tests** | `InMemoryMetricsStore` scenario used in reporter tests if any; else new integration test building store + `GlobalStatsCalculator.single_latency` mock path. |
| **Acceptance** | Document accuracy tolerance in test comments. |

---

### T05 — Extend sketch routing to `service_time` and `processing_time`

| Field | Content |
|--------|---------|
| **Status** | **Done** |
| **Goal** | Same as T04 for `service_time` and `processing_time`. |
| **Start** | T04 merged. |
| **Work** | Allow-list `AGGREGATED_REQUEST_TIMING_SKETCH_METRIC_NAMES`; `aggregated_request_timing_sketch_key`; `_merged_request_timing_sketch_for_query` routes `get_percentiles` / `get_stats` (and thus `get_mean`) like T04 latency. |
| **End** | T04 tests still pass. |
| **Unit tests** | Parametrized tests over metric names. |
| **Integration tests** | One combined test hitting all three names. |
| **Acceptance** | `get_stats` / `get_mean` if already used for these names: either routed in T05 or deferred to T05b with explicit TODO in handoff. |

---

### T06 — Error rate under Option B

| Field | Content |
|--------|---------|
| **Goal** | Implement chosen strategy (explicit counters vs minimal `service_time` docs). `get_error_rate` correct for sketch-only request path. |
| **Start** | T05 merged. |
| **Work** | Extend merge delta to carry success/failure counts; merge commutative. |
| **End** | Document strategy in `distributed_request_metrics.rst` if changed. |
| **Unit tests** | Merge deltas with only successes, only failures, mixed; assert rate. |
| **Integration tests** | End-to-end with `GlobalStatsCalculator.error_rate` if feasible without full race. |
| **Acceptance** | No regression on ES `get_error_rate` (unchanged code path). |

---

### T07 — Extract `RequestMetricsRecorder` from `SamplePostprocessor`

| Field | Content |
|--------|---------|
| **Goal** | Move logic to e.g. `esrally/driver/request_metrics.py` class called by `SamplePostprocessor`; **byte-for-byte equivalent** docs for same input samples (in-memory). |
| **Start** | T06 merged (or T03 if T04–T06 delayed — then rebase). |
| **Work** | Refactor only; `Driver` still calls post-processor as today. |
| **End** | Full `tests/driver/driver_test.py` (sample post-processor tests) pass unchanged. |
| **Unit tests** | Any new module tests for thin wrappers. |
| **Integration tests** | Existing driver tests = integration. |
| **Acceptance** | No config flag required; behavior identical. |

---

### T08 — `ProgressUpdate` message

| Field | Content |
|--------|---------|
| **Goal** | New actor message type; `Driver.update_samples` also accepts progress-only updates OR dedicated handler updates `most_recent_sample_per_client` without `raw_samples`. |
| **Start** | T07 merged. |
| **Work** | **Do not remove** `UpdateSamples` yet. Workers may send `ProgressUpdate` in tests only. |
| **End** | Default behavior unchanged. |
| **Unit tests** | Driver unit test: progress message updates dict used by `update_progress_message`. |
| **Integration tests** | Actor test if available; else driver-level test with mock actor. |
| **Acceptance** | No perf regression when message unused. |

---

### T09 — Throughput bucket accumulator (pure)

| Field | Content |
|--------|---------|
| **Goal** | Module implementing additive bucket merge (dict in → dict out), fixed width, coordinator epoch in constructor; unit-tested. |
| **Start** | T08 merged. |
| **Work** | No wiring to `Driver` yet. |
| **End** | Tests pass. |
| **Unit tests** | Merge disjoint workers, overlapping buckets, sample_type boundaries. |
| **Integration tests** | Compare to `ThroughputCalculator` output on **single synthetic** merged sample list (reference impl in test). |
| **Acceptance** | Document known differences vs legacy calculator in test docstring. |

---

### T10 — Worker `EsMetricsStore` writer (flagged, single-process first)

| Field | Content |
|--------|---------|
| **Goal** | When `driver.distributed_request_metrics=true` and `reporting.datastore.type=elasticsearch`, worker process opens store with same `race-id` (from config) and can `flush` a test batch. Coordinator still runs old path (dual-write behind sub-flag **or** worker-only in integration test harness). |
| **Start** | T09 merged. |
| **Work** | Minimal wiring in worker bootstrap; guard all new paths with flag. |
| **End** | Flag **false** → zero change. |
| **Unit tests** | Mock ES client factory; assert `bulk_index` called with expected race-id. |
| **Integration tests** | Optional: ES testcontainer / nightly job — document if not in default CI. |
| **Acceptance** | Manual checklist: one local race with ES metrics + flag writes docs. |

---

### T11 — Coordinator skips `raw_samples` when flag + ES (full path)

| Field | Content |
|--------|---------|
| **Goal** | With flag on, workers are sole writers for request metrics; `UpdateSamples` removed or carries no samples; `post_process_samples` skips request loop / throughput calculator; telemetry unchanged. |
| **Start** | T10 merged. |
| **Work** | Carefully flush worker stores at join points; driver `GlobalStatsCalculator` reads from ES (already remote). |
| **End** | Flag off → old behavior. |
| **Unit tests** | Driver tests duplicated with flag on where feasible (mock store). |
| **Integration tests** | Distributed driver test with 2 workers if CI supports; else documented manual + issue for CI follow-up. |
| **Acceptance** | Parity task: percentile tables within tolerance vs baseline. |

---

### T12 — `MetricsAggregatorActor` (optional path)

| Field | Content |
|--------|---------|
| **Goal** | When `driver.metrics_aggregator=true`, workers send sketch deltas + bucket deltas to aggregator; aggregator writes ES and/or merges into coordinator `InMemoryMetricsStore`. |
| **Start** | T11 merged (or T10 + partial T11 for ES-only aggregator output). |
| **Work** | New actor file; message types; single-threaded merge. |
| **End** | Flag false → no actor created. |
| **Unit tests** | Actor logic tested without full Thespian where possible (extract merge function). |
| **Integration tests** | Multi-worker local actor system test (if Rally CI runs actor tests). |
| **Acceptance** | Load test: driver mailbox depth reduced vs baseline (optional benchmark task). |

---

### T13 — In-memory Option B end-to-end

| Field | Content |
|--------|---------|
| **Goal** | With `datastore.type=in-memory` and distributed flag, coordinator receives only sketch deltas; `to_externalizable` / race pickle includes sketch state OR expanded summary — define format; **reports** still work. |
| **Start** | T12 merged (or T11 if aggregator skipped). |
| **Work** | Wire `merge_request_sketch_delta` from messages; ensure `GlobalStatsCalculator` uses sketch getters. |
| **End** | No giant `docs` list for request metrics. |
| **Unit tests** | Full `InMemoryMetricsStore` sketch path + `GlobalStatsCalculator` on synthetic race. |
| **Integration tests** | Mini race benchmark in test mode. |
| **Acceptance** | Memory bound independent of sample count (smoke: N=1e5 simulated accepts). |

---

### T14 — Documentation

| Field | Content |
|--------|---------|
| **Goal** | `docs/configuration.rst` entries; link from `docs/index.rst` to `distributed_request_metrics.rst` (already added with design doc); update `CHANGELOG.md` if user-facing. |
| **Start** | Feature complete behind flag. |
| **Work** | RST + recipes for distributed drivers + ES credentials on workers. |
| **End** | Docs build (`tox -e docs` or project equivalent) passes. |
| **Unit tests** | N/A |
| **Integration tests** | Docs link check optional. |
| **Acceptance** | Reviewer can configure a 2-host load test from docs alone. |

---

### T15 — (Optional) Enable by default policy

| Field | Content |
|--------|---------|
| **Goal** | Product decision: flip default, deprecate old path, or keep flag indefinitely. |
| **Start** | T14 merged; soak in nightly. |
| **Work** | RFC / changelog / migration note. |
| **End** | As decided. |
| **Unit tests** | Update tests that assumed default off. |
| **Integration tests** | Full CI matrix. |
| **Acceptance** | Release notes. |

---

## Quick commands (adjust for project)

```bash
pytest tests/metrics/ tests/driver/driver_test.py -q
# docs
tox -e docs   # or sphinx-build -M html docs docs/_build
```

## Contact

Use the primary design doc and Elasticsearch Rally contributing guidelines for review expectations.
