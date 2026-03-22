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
