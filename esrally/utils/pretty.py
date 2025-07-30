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

import dataclasses
import difflib
import enum
import json
import re
from collections.abc import Callable, Iterator, Mapping, Sequence
from typing import Any, Protocol, runtime_checkable

from esrally.utils import convert


class Flag(enum.Flag):
    NONE = 0
    FLAT_DICT = enum.auto()
    DUMP_EQUALS = enum.auto()
    INVERT_FILTER = enum.auto()


FieldFilter = Callable[[str | int], bool]


def dump(obj: Any, /, flags: Flag = Flag.NONE, field_filter: FieldFilter | None = None) -> str:
    """dump creates a human-readable multiline text to make easy to visualize the content of a JSON like object.

    :param obj: the object the dump has to be obtained from.
    :param flags:
        flags & FLAT_DICT != 0: it will squash nested objects to make simple reading them.
    :param field_filter: fields names for which this function returns `True` will be considered for produced output.
    :return: JSON human-readable multiline text representation of the input object.
    """
    if Flag.FLAT_DICT in flags:
        # It reduces nested dictionary to a flat one to improve readability.
        obj = flat(obj, flags=flags, field_filter=field_filter)
    else:
        obj = expand(obj, flags=flags, field_filter=field_filter)
    return json.dumps(obj, indent=2, sort_keys=True)


_HAS_DIFF = re.compile(r"^\+ ", flags=re.MULTILINE)


def diff(old: Any, new: Any, /, flags: Flag = Flag.NONE, field_filter: FieldFilter | None = None) -> str:
    """diff creates a human-readable multiline text to make easy to visualize the difference of content between two JSON like object.

    :param old: the old object the diff dump has to be obtained from.
    :param new: the new object the diff dump has to be obtained from.
    :param flags:
        flags & Flags.FLAT_DICT: it squashes nested objects to make simple reading them;
        flags & Flags.DUMP_EQUALS: in case there is no difference it will print the same as dump function.
    :param field_filter: fields names for which this function returns `True` will be considered for produced output.
    :return: JSON human-readable multiline text representation of the difference between input objects, if any, or '' otherwise.
    """
    if Flag.DUMP_EQUALS not in flags and old == new:
        return ""
    ret = "\n".join(difflib.ndiff(dump(old, flags, field_filter).splitlines(), dump(new, flags, field_filter).splitlines()))
    if Flag.DUMP_EQUALS not in flags and _HAS_DIFF.search(ret) is None:
        return ""
    return ret


_SCALAR_TYPES = (str, bytes, int, float, bool, type(None))


def expand(obj: Any, /, flags: Flag = Flag.NONE, field_filter: FieldFilter | None = None) -> Any:
    if isinstance(obj, _SCALAR_TYPES):
        return obj
    if dataclasses.is_dataclass(obj):
        obj = dataclasses.asdict(obj)
    want = not (flags & Flag.INVERT_FILTER)
    if isinstance(obj, Mapping):
        return {
            k: expand(v, flags=flags, field_filter=field_filter)
            for k, v in obj.items()
            if field_filter is None or bool(field_filter(k)) is want
        }
    if isinstance(obj, Sequence):
        return [
            expand(v, flags=flags, field_filter=field_filter)
            for k, v in enumerate(obj)
            if field_filter is None or bool(field_filter(k)) is want
        ]
    raise TypeError(f"unsupported object type: {type(obj)}")


def flat(obj: Any, /, flags: Flag = None, field_filter: FieldFilter | None = None) -> dict[str, Any]:
    """Given a JSON like object, it produces a key value flat dictionary of strings easy to read and compare.
    :param obj: a JSON like object
    :return: a flat dictionary
    """
    return expand(dict(_flat(obj)), flags=flags, field_filter=field_filter)


def _flat(obj: Any) -> Iterator[tuple[str, Any]]:
    """Recursive helper function generating the content for the flat dictionary.

    :param obj: a JSON like object
    :return: a generator of (key, value) pairs.
    """
    if isinstance(obj, _SCALAR_TYPES):
        yield "", obj
        return
    if dataclasses.is_dataclass(obj):
        obj = dataclasses.asdict(obj)
    if isinstance(obj, Mapping):
        for k1, v1 in obj.items():
            for k2, v2 in _flat(v1):
                if k2:
                    yield f"{k1}.{k2}", v2
                else:
                    yield str(k1), v2
        return
    if isinstance(obj, Sequence):
        for k1, v1 in enumerate(obj):
            for k2, v2 in _flat(v1):
                if k2:
                    yield f"{k1}.{k2}", v2
                else:
                    yield str(k1), v2
        return
    raise TypeError(f"unsupported object type: {type(obj)}")


def number(x: int | float | None) -> str:
    if x is None:
        return "N/A"
    return f"{x:,}"


def size(x: int | float | None, unit: convert.Size.Unit = convert.Size.Unit.B) -> str:
    if x is None:
        return "N/A"
    return str(convert.size(x, unit))


def duration(x: int | float | None, unit: convert.Duration.Unit = convert.Duration.Unit.S) -> str:
    if x is None:
        return "N/A"
    return str(convert.duration(x, unit))
