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

"""Pure helpers for DDSketch merge and wire serialization (distributed request metrics)."""

from __future__ import annotations

import importlib
from typing import NamedTuple

import google.protobuf
from ddsketch.ddsketch import DEFAULT_REL_ACC, BaseDDSketch, DDSketch
from ddsketch.pb.proto import DDSketchProto

_pb_ver = tuple(map(int, google.protobuf.__version__.split(".")[0:2]))
_pb_mod_name = "ddsketch.pb.ddsketch_pb2" if _pb_ver >= (3, 19) else "ddsketch.pb.ddsketch_pre319_pb2"
_pb_dds = importlib.import_module(_pb_mod_name)

# Default relative accuracy for request-timing sketches (alpha in the DDSketch paper).
REQUEST_METRIC_SKETCH_RELATIVE_ACCURACY = DEFAULT_REL_ACC


class RequestMetricSketchKey(NamedTuple):
    """Logical stream identity aligned with :doc:`distributed_request_metrics` sketch keys."""

    metric_name: str
    task: str
    operation: str
    operation_type: str
    sample_type: str


class SketchState(NamedTuple):
    """
    Per-stream merged request metric state for coordinator-side in-memory aggregation (T04+).

    ``merged_sketch`` holds the combined ``DDSketch`` for one logical stream. ``success_count`` and
    ``failure_count`` are reserved for error-rate when raw per-request documents are absent.

    T03 only declares the type and an empty ``InMemoryMetricsStore._request_sketch_table``; entries
    are not populated yet.
    """

    merged_sketch: BaseDDSketch | None = None
    success_count: int = 0
    failure_count: int = 0  # paired with success_count for merged error-rate numerators/denominators


def merge_sketches(a: BaseDDSketch, b: BaseDDSketch) -> DDSketch:
    """
    Return a new ``DDSketch`` containing the values from ``a`` and ``b``.

    Merging is commutative up to sketch numerical tolerance. Both sketches must use the same
    mapping parameters (e.g. same relative accuracy / gamma); otherwise ``ValueError`` is raised.
    """
    out = DDSketch()
    if a.count > 0:
        out.merge(a)
    if b.count > 0:
        out.merge(b)
    return out


def serialize_sketch(sketch: BaseDDSketch) -> bytes:
    """Serialize a sketch to bytes (protobuf). Suitable for cross-actor payloads."""
    proto = DDSketchProto.to_proto(sketch)
    return proto.SerializeToString()


def deserialize_sketch(data: bytes) -> BaseDDSketch:
    """
    Deserialize bytes produced by :func:`serialize_sketch`.

    Note: DDSketch's protobuf mapping does not preserve min/max/sum summary fields; quantiles and
    counts remain consistent for merged values.
    """
    msg = _pb_dds.DDSketch()
    msg.ParseFromString(data)
    return DDSketchProto.from_proto(msg)


def approximate_sum_from_sketch(sketch: BaseDDSketch) -> float:
    """
    Approximate ``sum(values)`` from bucket contents after protobuf deserialize (where ``sum`` is 0).

    Uses each bin's representative ``mapping.value(key)`` times its count; matches DDSketch's
    relative accuracy about as well as quantile queries. Reads ``ddsketch`` private fields by
    necessity (no public bin iterator).
    """
    mapping = sketch._mapping  # pylint: disable=protected-access
    total = 0.0
    st = sketch._store  # pylint: disable=protected-access
    for i, bin_ct in enumerate(st.bins):
        if bin_ct:
            key = st.offset + i
            total += mapping.value(key) * bin_ct
    nst = sketch._negative_store  # pylint: disable=protected-access
    for i, bin_ct in enumerate(nst.bins):
        if bin_ct:
            key = nst.offset + i
            total += -mapping.value(key) * bin_ct
    return total


def sketch_sum_and_avg(sketch: BaseDDSketch) -> tuple[float, float]:
    """
    Return ``(sum, avg)`` for stats reporting.

    Uses exact :attr:`BaseDDSketch.sum` when non-zero (live ``add`` path). When ``sum`` is zero but
    ``count`` is positive (typical after :func:`deserialize_sketch`), falls back to
    :func:`approximate_sum_from_sketch` so coordinator-side merges over wire bytes stay usable.
    """
    count = float(sketch.count)
    if count == 0.0:
        return 0.0, float("nan")
    if sketch.sum != 0.0:
        return sketch.sum, sketch.avg
    approx = approximate_sum_from_sketch(sketch)
    return approx, approx / count
