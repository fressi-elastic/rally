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

import math

import pytest
from ddsketch.ddsketch import DEFAULT_REL_ACC, DDSketch

from esrally.metrics.sketch_utils import (
    REQUEST_METRIC_SKETCH_RELATIVE_ACCURACY,
    RequestMetricSketchKey,
    deserialize_sketch,
    merge_sketches,
    serialize_sketch,
)


def test_request_metric_sketch_key_is_named_tuple():
    k = RequestMetricSketchKey(
        metric_name="latency",
        task="t1",
        operation="bulk",
        operation_type="bulk",
        sample_type="normal",
    )
    assert k.metric_name == "latency"
    assert k.task == "t1"
    assert k.operation == "bulk"
    assert k.operation_type == "bulk"
    assert k.sample_type == "normal"


def test_relative_accuracy_constant_matches_ddsketch_default():
    assert REQUEST_METRIC_SKETCH_RELATIVE_ACCURACY == DEFAULT_REL_ACC


def test_merge_sketches_empty_empty():
    a = DDSketch()
    b = DDSketch()
    m = merge_sketches(a, b)
    assert m.count == 0


def test_merge_sketches_nonempty_empty():
    a = DDSketch()
    for x in (1.0, 2.0, 3.0):
        a.add(x)
    b = DDSketch()
    m = merge_sketches(a, b)
    assert m.count == 3
    assert m.get_quantile_value(0.5) == pytest.approx(a.get_quantile_value(0.5))


def test_merge_sketches_empty_nonempty():
    a = DDSketch()
    b = DDSketch()
    for x in (10.0, 20.0):
        b.add(x)
    m = merge_sketches(a, b)
    assert m.count == 2


def test_merge_sketches_commutative_on_synthetic_data():
    a = DDSketch()
    b = DDSketch()
    for x in (1.0, 5.0, 9.0):
        a.add(x)
    for x in (2.0, 8.0):
        b.add(x)
    m1 = merge_sketches(a=a, b=b)
    m2 = merge_sketches(a=b, b=a)
    assert m1.count == 5
    assert m2.count == 5
    for q in (0.0, 0.25, 0.5, 0.75, 1.0):
        v1 = m1.get_quantile_value(q)
        v2 = m2.get_quantile_value(q)
        assert v1 is not None and v2 is not None
        assert math.isclose(v1, v2, rel_tol=0.0, abs_tol=1e-9)


def test_merge_sketches_monotonic_quantiles():
    s = DDSketch()
    for x in range(100):
        s.add(float(x))
    qs = []
    for q in (0.1, 0.5, 0.9):
        v = s.get_quantile_value(q)
        assert v is not None
        qs.append(v)
    assert qs == sorted(qs)
    assert qs[0] <= qs[1] <= qs[2]


def test_merge_sketches_incompatible_raises():
    a = DDSketch(relative_accuracy=0.01)
    b = DDSketch(relative_accuracy=0.05)
    a.add(1.0)
    b.add(2.0)
    with pytest.raises(ValueError, match="Cannot merge two DDSketches with different parameters"):
        merge_sketches(a, b)


def test_serialize_deserialize_roundtrip_counts_and_quantiles():
    s = DDSketch()
    for x in (0.5, 1.0, 1.5, 2.0, 100.0):
        s.add(x)
    raw = serialize_sketch(s)
    s2 = deserialize_sketch(raw)
    assert s2.count == s.count
    for q in (0.0, 0.5, 1.0):
        assert s2.get_quantile_value(q) == pytest.approx(s.get_quantile_value(q), rel=0.02)


def test_serialize_deserialize_empty_sketch():
    s = DDSketch()
    s2 = deserialize_sketch(serialize_sketch(s))
    assert s2.count == 0
    assert s2.get_quantile_value(0.5) is None
