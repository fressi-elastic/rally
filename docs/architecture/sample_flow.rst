Sample Object Flow
==================

This document describes how **sample objects** move through Rally during a benchmark: where they are created, how they are sent between actors, where they are consumed, and what is done with them. Samples represent the outcome of a single request (or a batch of operations) executed by a load generator: latency, service time, throughput, and related metadata.

Overview
--------

Samples are created on **Worker** actors, buffered in a **Sampler**, and then:

- **Worker** sends samples to the **Driver** via **UpdateSamples** (for progress and global throughput).
- **Worker** also submits samples to the metrics store when using Elasticsearch: it post-processes its own samples and writes the resulting metrics (latency, service_time, processing_time, per-worker throughput) to its local metrics store.
- **Driver** post-processes the samples it receives (for progress and, when using ES, global throughput only) and either writes full metrics to an in-memory store or distributes only global throughput documents to Workers to write (ES case).

::

   Worker (AsyncExecutor)  →  Sampler  →  [1] SamplePostprocessor (on Worker) → Worker's metrics store (ES only)
          (creates)           (buffer)  →  [2] UpdateSamples  →  Driver  →  SamplePostprocessor (throughput only when ES)
                                                                              (progress + global throughput)

Where samples are created
-------------------------

``Sample`` and ``Sampler`` (driver.py)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- **Sample** (in ``esrally.driver.driver``) is a data object holding:

  - ``client_id``, ``absolute_time``, ``request_start``, ``task_start``
  - ``task``, ``sample_type``, ``request_meta_data``
  - ``latency``, ``service_time``, ``processing_time``
  - ``throughput``, ``total_ops``, ``total_ops_unit``, ``time_period``, ``percent_completed``
  - optional ``dependent_timings`` for dependent operations

- **Sampler** is a buffer (queue) that holds ``Sample`` instances. It is created and owned by a **Worker** actor.

Creation site: ``AsyncExecutor`` (driver.py)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Samples are created **only** in one place: inside **AsyncExecutor.__call__** (async method), which runs on a Worker.

1. The **Worker** creates a **Sampler** when it starts driving a non-join-point step (in ``Worker.drive()``):

   - ``self.sampler = Sampler(start_timestamp=time.perf_counter(), buffer_size=self.sample_queue_size)``

2. The Worker creates an **AsyncIoAdapter** with that ``sampler`` and submits it to a thread pool. The adapter runs an asyncio loop and, for each (client, task), creates an **AsyncExecutor** that receives the same ``sampler``.

3. For each completed request in the task loop, **AsyncExecutor** calls:

   - ``self.sampler.add(task, client_id, sample_type, request_meta_data, absolute_processing_start, request_start, latency, service_time, processing_time, throughput, total_ops, total_ops_unit, time_period, progress, dependent_timing)``

4. **Sampler.add()** builds a **Sample(...)** and puts it on an internal ``queue.Queue``. If the queue is full, the sample is dropped and a warning is logged.

So: **samples are created on the Worker, inside the async executor loop, and stored in the Worker's Sampler.**

Where samples are sent
----------------------

Worker → metrics store (when Elasticsearch reporting)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When reporting uses Elasticsearch, each Worker has a local metrics store and a **SamplePostprocessor**. In **send_samples()**, before sending to the Driver, the Worker runs the post-processor on its drained samples and writes the resulting metrics (latency, service_time, processing_time, throughput) to its local store. So **samples are submitted to the metrics store by the Worker** (as derived metrics), not as raw samples.

Worker → Driver: ``UpdateSamples`` message
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Samples are also sent from Worker to Driver in two situations:

1. **Periodically (wake-up)**

   The Worker schedules a wake-up every ``Worker.WAKEUP_INTERVAL_SECONDS`` (default 5). On **receiveMsg_WakeupMessage**, it calls **send_samples()**, which:

   - Reads all current samples from the Sampler: ``samples = self.sampler.samples`` (drains the queue)
   - If there are any and the Worker has a local metrics store (ES), runs **SamplePostprocessor** on the samples and writes to the Worker's metrics store
   - Sends **UpdateSamples(self.worker_id, samples)** to **self.driver_actor** (the Driver actor).

2. **At a join point**

   When the Worker reaches a join point in the schedule, in **Worker.drive()** it:

   - Waits for the executor future to complete
   - Calls **send_samples()** (same as above: drain sampler, optionally write to local store, send **UpdateSamples** to Driver)
   - Clears the sampler reference and sends **JoinPointReached(self.worker_id, task_allocations)** to the Driver.

So: **samples are sent from Worker to the Driver via UpdateSamples(worker_id, samples), and when using Elasticsearch the Worker also submits derived metrics to its local metrics store.**

Where samples are consumed
--------------------------

Driver actor receives ``UpdateSamples``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The **DriverActor** handles the message in **receiveMsg_UpdateSamples(self, msg, sender)** and delegates to the **Driver**:

- ``self.driver.update_samples(msg.samples)``

So: **samples are consumed by the Driver (the in-process Driver object used by the DriverActor).**

Driver stores and uses samples
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- **Driver.update_samples(samples)**:

  - Appends ``msg.samples`` to **self.raw_samples**
  - Updates **self.most_recent_sample_per_client[s.client_id]** for each sample (used for progress reporting)

Samples are not forwarded to any other actor as raw samples. They stay on the Driver and are used for:

- Progress reporting (latest sample per client)
- Post-processing (see below)

So: **consumption of samples happens entirely on the Driver: accumulation in raw_samples and use for progress and metrics.**

What is done with the samples
-----------------------------

Progress reporting
~~~~~~~~~~~~~~~~~~

- **Driver.update_progress_message(task_finished=False)** uses **most_recent_sample_per_client** (and thus the latest sample per client) to compute an overall progress percentage and print a progress line (e.g. "Running index [ 45% done]").
- This is called from the Driver's wake-up handler and when a task step finishes.

Post-processing: ``SamplePostprocessor``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**On the Worker (Elasticsearch metrics store only)**

When the Worker has a local metrics store (ES reporting), it runs **SamplePostprocessor** on its own samples in **send_samples()** before sending them to the Driver. That writes latency, service_time, processing_time, and throughput (per-worker) to the Worker's metrics store. So **the Worker submits samples to the metrics store** by post-processing them and writing the derived metrics locally.

**On the Driver**

The Driver periodically and at task boundaries runs **post_process_samples()**:

1. It takes a **snapshot** of the current samples: ``raw_samples = self.raw_samples`` and then clears ``self.raw_samples = []``.
2. It calls **self.sample_post_processor(raw_samples, throughput_only=...)**.

When **in-memory** reporting is used, ``throughput_only`` is False: **SamplePostprocessor** does full post-processing (per-sample latency/service_time/processing_time and throughput) and writes everything to the Driver's in-memory metrics store.

When **Elasticsearch** reporting is used, ``throughput_only`` is True: Workers have already written per-sample metrics to their local store, so the Driver's **SamplePostprocessor** only computes **global throughput** (aggregated across all workers) and writes that to the Driver's in-memory store. Per-sample metrics are not written by the Driver in this case.

**SamplePostprocessor** (when not throughput_only) does the following with a batch of samples:

1. **Per-sample (with optional downsampling)** — latency, service_time, processing_time, and dependent_timing service_time records.
2. **Throughput aggregation** — **ThroughputCalculator.calculate(raw_samples)** and write throughput metrics.
3. **Flush** — ``self.metrics_store.flush(refresh=False)``.

So: **the Driver post-processes samples for progress and for global throughput; when in-memory it writes all metrics, when ES it writes only global throughput (workers wrote per-sample metrics).**

Where those metrics go next
~~~~~~~~~~~~~~~~~~~~~~~~~~~

The Driver's metrics store is an **InMemoryMetricsStore**. After post-processing, the stored documents are handled in one of two ways:

1. **In-memory reporting**

   At task end (and at benchmark end), the Driver calls **to_externalizable(clear=True)** on that store and sends the resulting blob to the **BenchmarkActor** (race control) via **TaskFinished(metrics, …)** or **BenchmarkComplete(metrics)**. The BenchmarkActor's coordinator then does **bulk_add** into its own metrics store and uses it for results and reporting.

2. **Elasticsearch metrics store**

   When reporting uses Elasticsearch:

   - **Workers** post-process their own samples and write latency, service_time, processing_time, and per-worker throughput to their local metrics store (same race context). So Workers submit samples to the metrics store by writing the derived metrics themselves.
   - **Driver** post-processes only for global throughput (throughput_only=True). It gets the resulting documents from its in-memory store, **partitions** them across Workers, and sends each Worker **WriteMetricsToStore(memento)** with that worker's chunk. Each Worker calls **bulk_add(memento)** and **flush(refresh=False)** to write the global throughput docs.
   - So per-sample metrics are written by Workers; global throughput is written by Workers on behalf of the Driver (via WriteMetricsToStore).

At benchmark end, when using ES, the Driver waits for **WriteMetricsToStoreAck** from all Workers for the final batch, then sends **BenchmarkComplete(None)**. The BenchmarkActor's coordinator uses its own ES metrics store (same race) and computes results from data that Workers have already written.

End-to-end flow summary
-----------------------

+----------------+-----------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------+
| Stage          | Location                                      | What happens                                                                                                                     |
+================+===============================================+==================================================================================================================================+
| **Create**     | Worker: ``AsyncExecutor`` (inside async loop) | After each request (or batch), ``sampler.add(...)`` creates a ``Sample`` and enqueues it in the Worker's ``Sampler``.            |
+----------------+-----------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------+
| **Buffer**     | Worker: ``Sampler``                           | Samples sit in a queue until the Worker sends them.                                                                              |
+----------------+-----------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------+
| **Submit to store** | Worker (ES only)                          | Worker runs **SamplePostprocessor** on drained samples and writes latency/service_time/processing_time/throughput to its local  |
|                |                                               | metrics store. So the Worker submits samples to the metrics store (as derived metrics).                                           |
+----------------+-----------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------+
| **Send**       | Worker → Driver                               | Worker sends **UpdateSamples(worker_id, samples)** on periodic wake-up or at join point (after draining; after writing to store  |
|                |                                               | when ES).                                                                                                                        |
+----------------+-----------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------+
| **Receive**    | DriverActor → Driver                          | **receiveMsg_UpdateSamples** calls **Driver.update_samples(msg.samples)**.                                                       |
+----------------+-----------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------+
| **Accumulate** | Driver                                        | Samples are appended to **raw_samples** and used to update **most_recent_sample_per_client** for progress.                      |
+----------------+-----------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------+
| **Post-process** | Driver                                      | On a timer and at join points, **post_process_samples()** runs **SamplePostprocessor**: in-memory = full metrics; ES =           |
|                |                                               | **throughput_only=True** (global throughput only; workers already wrote per-sample metrics).                                     |
+----------------+-----------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------+
| **Metrics output** | Driver + Workers                           | In-memory: Driver sends metrics blob to BenchmarkActor (TaskFinished / BenchmarkComplete). ES: Workers wrote per-sample metrics  |
|                |                                               | locally; Driver partitions global throughput docs and sends **WriteMetricsToStore** to Workers; Workers **bulk_add** and **flush**.|
+----------------+-----------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------+

Key types and files
-------------------

- **Sample**, **Sampler**, **AsyncExecutor**, **AsyncIoAdapter**, **Worker**, **Driver**, **SamplePostprocessor**, **UpdateSamples**, **WriteMetricsToStore**, **WriteMetricsToStoreAck**: ``esrally/driver/driver.py``
- **ThroughputCalculator**: ``esrally/driver/driver.py`` (used by SamplePostprocessor)
- **Metrics store (InMemoryMetricsStore, EsMetricsStore)**, **put_value_cluster_level**, **bulk_add**, **to_externalizable**: ``esrally/metrics.py``
- **BenchmarkActor**, **TaskFinished**, **BenchmarkComplete**, **on_task_finished**, **on_benchmark_complete**: ``esrally/racecontrol.py``

For the broader actor setup (Driver, Workers, BenchmarkActor), see :doc:`actor_system`.
