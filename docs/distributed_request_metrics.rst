.. _distributed_request_metrics:

Distributed request metrics and sketch-based aggregation
========================================================

.. note::

   This document describes a **design** for future work. Behavior described here is **not** necessarily implemented until the corresponding tasks in the agent handoff checklist are completed.

   The **Cursor plan** tied to this effort is scoped to **producing and maintaining these documents** only; implementation is a separate epic that uses this RST and ``DISTRIBUTED_METRICS_AGENT_HANDOFF.md`` as input.

Abstract
--------

Today, the benchmark **coordinator** (``Driver`` / ``DriverActor``) collects **raw** request metric samples (see :doc:`metrics`) from all load workers, holds them in memory, and runs ``SamplePostprocessor`` (latency, service time, processing time, throughput) on the coordinator process. At high client counts this becomes a **bottleneck**.

This design moves **request-metric processing** toward **Worker** actors, lets **workers write** to the metrics datastore (Elasticsearch) where applicable, and uses **mergeable quantile sketches** (e.g. DDSketch) so actors **do not** exchange full ``Sample`` streams. For **in-memory** reporting, the design adopts **Option B**: a **sketch-aware** ``InMemoryMetricsStore`` that answers ``get_percentiles`` / ``get_stats`` from **merged sketches** per metric key instead of sorting every raw point.

Goals
-----

* **Preserve** CLI / report semantics as far as practical: percentiles, throughput summaries, error rates, and existing Elasticsearch document shapes unless a flag explicitly changes storage mode.
* **Reduce** CPU and mailbox load on ``DriverActor``: no coordinator-side ``raw_samples`` loop for the request path; no coordinator ``ThroughputCalculator`` for request throughput once bucket aggregation is in place.
* **Distribute** work: Workers run local post-processing; an optional **MetricsAggregatorActor** merges sketch deltas and throughput buckets when a single global merge step is required before persistence.
* **Sketches**: Workers (or aggregator) hold **DDSketch** (or compatible) structures keyed by stable identities; cross-actor messages carry **serialized sketch payloads** and **additive counters**, not ``Sample`` lists.

Non-goals (initially)
---------------------

* Moving **telemetry** devices (node stats, index stats, etc.) off the coordinator unless separately scoped.
* Changing track or challenge YAML semantics.

Current architecture (baseline)
-------------------------------

#. Each ``Worker`` runs ``AsyncExecutor`` loops; each completed request enqueues a ``Sample`` in a process-local ``Sampler`` (``queue.Queue``).
#. On a timer, the worker drains the queue and sends ``UpdateSamples`` to ``DriverActor``.
#. ``Driver.update_samples`` appends to ``raw_samples`` and refreshes progress state.
#. ``DriverActor`` periodically calls ``post_process_samples``, which runs ``SamplePostprocessor``: writes ``latency``, ``service_time``, ``processing_time`` via ``MetricsStore.put_value_cluster_level``, and ``ThroughputCalculator.calculate(raw_samples)`` for throughput. ``ThroughputCalculator`` keeps **state** between calls (``task_stats``, ``unprocessed``) and expects **all generators'** samples in one logical stream per ``Task``.

Relevant code:

* ``esrally/driver/driver.py`` — ``Sample``, ``Sampler``, ``SamplePostprocessor``, ``ThroughputCalculator``, ``DriverActor``, ``Worker``.
* ``esrally/metrics.py`` — ``MetricsStore``, ``InMemoryMetricsStore``, ``EsMetricsStore``, ``GlobalStatsCalculator``.

Target architecture
-------------------

Workers
   Consume ``Sampler`` output locally. Update:

   * **Sketches** per key for timing metrics (see `Sketch keys`_).
   * **Throughput buckets** (additive ``total_ops`` per aligned time bucket per task / sample type / unit).

   Flush to:

   * **Elasticsearch**: workers open ``EsMetricsStore`` (or a thin writer) with the **same** ``race-id`` and meta as the coordinator race; **bulk** index using the existing ``_put_metric`` document shape where full points are still written, **or** a reduced schema when sketch-encoded documents (Pattern 2b) are implemented later.

   * **In-memory (Option B)**: workers send **sketch merge deltas** (and error counters if used) to the coordinator or ``MetricsAggregatorActor``; the coordinator **never** materializes full per-request rows for those metrics.

Optional MetricsAggregatorActor
   Single-threaded merge of sketch bytes and throughput bucket maps from all workers; emits writes to ES and/or merged state into ``InMemoryMetricsStore``.

Driver / DriverActor
   Retains **join-point coordination**, ``JoinPointReached``, ``move_to_next_task``, and **telemetry** metrics on the coordinator ``MetricsStore``. Request path: **no** ``raw_samples`` / ``UpdateSamples`` of full ``Sample`` lists once the distributed path is enabled.

.. code-block:: text

   [Worker 1] --sketch_delta/thr_bucket--> [MetricsAggregator] --bulk--> [ES rally-metrics-*]
        |                                          |
        +------------------+----------------------+
                           v
                    [InMemory Option B: merge into sketch table]

Reporting semantics
-------------------

Elasticsearch (``EsMetricsStore``)
   ``get_percentiles`` uses Elasticsearch **percentiles aggregation** on numeric ``value`` (see ``esrally/metrics.py``). Workers can index into the same index; the coordinator does **not** need all request documents in RAM for reporting. **Pattern 2a / 2b** (synthetic quantile docs vs sketch-encoded docs) are optional optimizations for write volume.

In-memory — **Option B (chosen)**
   ``InMemoryMetricsStore`` maintains **one merged DDSketch (and counts / sums as needed) per request-metric key**. Implementations of ``get_percentiles``, ``get_stats``, and ``get_mean`` for metric names such as ``latency``, ``service_time``, and ``processing_time`` **read from the sketch** when a sketch exists for the query filter; otherwise they **fall back** to scanning ``self.docs`` for **telemetry** and **legacy** races.

   **Error rate:** ``get_error_rate`` inspects ``service_time`` documents' ``meta.success`` when no merged
   outcome tallies exist. Option B uses **explicit** ``success_count`` / ``failure_count`` on
   ``SketchState``, merged through ``InMemoryMetricsStore.merge_request_sketch_delta``; when their
   sum is non-zero, those counts determine the rate for the Normal ``service_time`` stream (explicit
   ``task`` and ``operation_type``).

Sketch keys
-----------

A stable key should identify the logical stream Rally reports today, for example:

* ``(metric_name, task, operation, operation_type, sample_type)``

Optional **time-bucket id** (e.g. wall-clock second aligned to a race-wide epoch) if sketches are flushed incrementally without merging into one global sketch per task.

Throughput buckets
------------------

Replace coordinator ``ThroughputCalculator`` **for the request path** with **additive** buckets:

* Key: ``(task, bucket_id, sample_type, ops_unit)`` with ``bucket_id`` from an agreed clock (coordinator-issued epoch recommended).
* Value: ``sum(total_ops)`` per worker; merge on aggregator or rely on ES aggregations if writing per-worker throughput points is acceptable.

Runner-provided ``throughput`` on samples (``map_task_throughput``) requires an explicit **merge policy** (documented, tested).

Configuration (illustrative)
----------------------------

New settings may include (exact names TBD):

* ``driver.distributed_request_metrics`` — enable distributed request path.
* ``driver.metrics_aggregator`` — use ``MetricsAggregatorActor`` vs direct worker → store.
* ``reporting.sketch.flush_interval_seconds`` — worker local flush period.
* Sketch accuracy parameters (DDSketch relative accuracy).

Workers on remote hosts need **reporting** / **datastore** settings in **local** config (see ``load_local_config`` in ``esrally/driver/driver.py``).

Security
--------

If workers write to Elasticsearch, they need credentials and TLS settings consistent with ``[reporting]``; avoid embedding secrets in messages. **Race identity** (``race-id``, ``race-timestamp``, environment, track, challenge, car) must be **broadcast** at benchmark start so all writers produce compatible documents.

Edge cases
----------

* **Warmup vs normal:** separate sketch keys by ``sample_type`` (as today).
* **Downsampling:** global index-based downsampling over ``raw_samples`` is not reproducible per worker; prefer per-client deterministic downsampling or rely on sketch compression.
* **Dependent timings** (``Sample.dependent_timings``): separate sketch keys or a small side channel for low-volume dependent operations.
* **Clock skew:** use coordinator-issued epoch for bucket ids where possible.
* **Join points:** workers flush before signaling completion so the aggregator / store sees a consistent step boundary.

.. _implementer_recommendations:

Recommendations for implementer agents
--------------------------------------

The following practices are **required** for this effort so the tree stays shippable and reviewers can trust each step.

1. **Vertical slices, always green**
   Each change set must leave the repository in a **fully working** state: existing CLI commands, default configuration, and CI-relevant tests pass without manual steps.

2. **Bottom-up order**
   Implement **pure** building blocks first (sketch wrapper, key types, merge semantics), then **metrics store** routing, then **driver** message and flag wiring, then **worker** integration, then **aggregator** actor. Avoid landing “half a protocol” without a feature flag.

3. **Tests on every task**
   * **Unit tests** for every new module (sketch merge, bucket arithmetic, serialization roundtrip, ``InMemoryMetricsStore`` routing).
   * **Integration tests** for any cross-actor or ES path: use existing patterns (mocks, containers, or Rally’s driver tests) so regressions are caught before merge.

4. **Feature flags**
   Default **off** until the distributed path is complete. When the flag is off, behavior must match **today’s** coordinator post-processing within floating-point / sketch accuracy where applicable.

5. **Parity checks**
   For ES mode, compare ``GlobalStatsCalculator`` / report outputs against a **baseline** run (same track, seed, reduced load) within documented tolerance. For in-memory Option B, compare percentiles and means against a **reference** implementation on a fixed synthetic sample set.

6. **Documentation**
   Update ``docs/configuration.rst`` when new options are added. Keep this design doc and the agent handoff file in sync when the protocol changes.

7. **Observability**
   Add **debug** logging for sketch merge sizes, flush counts, and bucket merges; guard verbose logs behind log level.

8. **Backwards compatibility**
   Old races and in-memory dumps must **still load**; sketch-aware getters must **fall back** to document scans when no sketch exists for a query.

Temporary handoff artifact
---------------------------

A **task-by-task** checklist for automation or other agents (including test requirements per task) lives in the repository root as ``DISTRIBUTED_METRICS_AGENT_HANDOFF.md``. Remove or archive that file when the project is complete.

References
----------

* :doc:`metrics` — metrics store concepts.
* :doc:`configuration` — ``[reporting]`` settings.
* :doc:`rally_daemon` — distributed load driver hosts.
* ``esrally/driver/driver.py`` — driver and worker implementation.
* ``esrally/metrics.py`` — store and ``GlobalStatsCalculator``.
