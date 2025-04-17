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

import sys
from abc import abstractmethod
from collections.abc import Iterable, Iterator, Sequence, Set
from itertools import chain, islice, zip_longest
from typing import overload


class NoRangeError(IndexError):
    pass


MAX_LENGTH = sys.maxsize


class RangeSet(Sequence["Range"], Set["Range"]):
    """RangeSet is the abstract base class of all implementations of sets of ranges.

    A range set is an immutable sequence of disjoint ranges sorted by its start value. It implements some mixin methods
    so that the implementation of a range sets is going to be lighter. It implements either the behaviour of a set
    and of a sequence.
    """

    @property
    @abstractmethod
    def start(self) -> int:
        raise NotImplementedError

    @property
    @abstractmethod
    def end(self) -> int:
        raise NotImplementedError

    @property
    def length(self) -> int:
        end = self.end
        if end == MAX_LENGTH:
            return MAX_LENGTH
        return self.end - self.start

    def pop(self, max_length: int | None = None) -> tuple[Range | NoRange, RangeSet]:
        if len(self) == 0:
            return NO_RANGE, NO_RANGE

        # It separates the top left range from the others.
        left, *others = self
        # It eventually splits it according to max_length value.
        left, mid = left.pop(max_length)
        # It unifies all the others.
        right = _rangeset(chain(mid, others))
        return left, right

    def __repr__(self) -> str:
        text = ", ".join(f"{r.start}-{pretty_end(r.end)}" for r in self)
        return f"{type(self).__name__}({text})"

    def __or__(self, other) -> RangeSet:
        if not isinstance(other, Iterable):
            raise TypeError(f"{other} is not iterable")
        return union(self, other)

    def __and__(self, other) -> RangeSet:
        if not isinstance(other, Iterable):
            raise TypeError(f"{other} is not iterable")
        if not isinstance(other, RangeSet):
            other = _rangeset(other)
        if not self or not other:
            return NO_RANGE
        start = max(self.start, other.start)
        end = min(self.end, other.end)
        if end <= start:
            return NO_RANGE
        ranges: list[Range] = []
        for r in union(self, other):
            if r.start >= end:
                break
            if r.end <= start:
                continue
            if r.start < start:
                r = Range(start, r.end)
            if r.end > end:
                r = Range(r.start, end)
            ranges.append(r)
        return _rangeset(ranges)

    @overload
    def __getitem__(self, key: int, /) -> Range:
        pass

    @overload
    def __getitem__(self, key: slice, /) -> RangeSet:
        pass

    def __getitem__(self, key, /) -> RangeSet:
        if isinstance(key, int):
            if key < 0:
                raise IndexError(f"index key can't be negative: {key} < 0")
            try:
                return next(islice(self, key, key + 1))
            except StopIteration:
                raise IndexError(f"index key is to big: {key} >= {len(self)}") from None
        if isinstance(key, slice):
            if key.step not in [None, 1]:
                raise ValueError(f"invalid slice step value: {key.step} is not 1 | None")
            start = int(0 if slice.start is None else slice.start)
            end = int(MAX_LENGTH if slice.stop is None else slice.stop)
            step = int(1 if slice.step is None else slice.step)
            return _rangeset(islice(self, start, end, step))
        elif isinstance(key, RangeSet):
            return self & key
        raise TypeError(f"invalid key type: {key} is not int | slice | RangeSet")

    def __eq__(self, other) -> bool:
        if not isinstance(other, RangeSet):
            return False
        if self.start != other.start or self.end != other.end:
            return False
        for r1, r2 in zip_longest(self, other):
            if r1 != r2:
                return False
        return True

    def __hash__(self):
        return hash(tuple(self))


def pretty_end(end: int) -> str:
    if end == MAX_LENGTH:
        return "*"
    return f"{end}"


class NoRange(RangeSet):

    def __contains__(self, item) -> bool:
        return False

    def __len__(self) -> int:
        return 0

    def __iter__(self) -> Iterator[Range]:
        return iter(tuple())

    def __bool__(self) -> bool:
        return False

    @property
    def start(self) -> int:
        raise NoRangeError("no start value")

    @property
    def end(self) -> int:
        raise NoRangeError("no end value")

    def __eq__(self, other):
        if isinstance(other, NoRange):
            return True
        return super().__eq__(other)


NO_RANGE = NoRange()


class Range(RangeSet):

    def __init__(self, start: int, end: int):
        if start < 0:
            raise ValueError(f"range start can't be negative: {start} < 0")
        if end < start:
            raise ValueError(f"range end can't be lesser than start: {end} < {start}")
        self._start = start
        self._end = end

    @property
    def start(self) -> int:
        return self._start

    @property
    def end(self) -> int:
        return self._end

    def pop(self, max_length: int | None = None) -> tuple[Range | NoRange, RangeSet]:
        if max_length is None:
            return self, NO_RANGE
        if max_length <= 0:
            return NO_RANGE, self
        max_end = self.start + max_length
        if max_end >= self.end:
            return self, NO_RANGE
        return Range(self.start, max_end), Range(max_end, self.end)

    def __bool__(self) -> bool:
        return True

    def __len__(self) -> int:
        return 1

    def __iter__(self) -> Iterator[Range]:
        yield self

    def __contains__(self, item) -> bool:
        if isinstance(item, int):
            return self.start <= item < self.end
        if isinstance(item, RangeSet):
            return self.start <= item.start and item.end <= self.end
        return False

    def __eq__(self, other) -> bool:
        if isinstance(other, Range):
            return self.start == other.start and self.end == other.end
        return super().__eq__(other)

    def __hash__(self) -> int:
        return hash((self.start, self.end))


ALL_RANGE = Range(0, MAX_LENGTH)


class RangeTree(RangeSet):

    def __init__(self, left: RangeSet, right: RangeSet):
        self.left = left
        self.right = right

    @property
    def start(self) -> int:
        return self.left.start

    @property
    def end(self) -> int:
        return self.right.end

    def __bool__(self) -> bool:
        return True

    def __len__(self):
        return len(self.left) + len(self.right)

    def __iter__(self) -> Iterator[Range]:
        yield from self.left
        yield from self.right

    def __contains__(self, item) -> bool:
        return item in self.left or item in self.right

    def __eq__(self, other) -> bool:
        if isinstance(other, RangeTree):
            return self.left == other.left and self.right == other.right
        return super().__eq__(other)


class RangeError(ValueError):

    def __init__(self, msg: str = "", _range: Range = None):
        super().__init__(msg)
        self.range = _range


def union(*items: Iterable[Range]) -> RangeSet:
    """It returns a sorted set of ranges given some unordered set of ranges.

    :param items: input sequences of ranges.
    :return: a sorted set of disjoint ranges.
    """
    # It sorts ranges by start value
    it: Iterator[Range] = iter(sorted((r for r in chain(*items) if r), key=lambda r: r.start))

    # It sorts ranges by start value
    try:
        left = next(it)
    except StopIteration:
        return NO_RANGE

    disjoint: list[Range] = []
    for right in it:
        if right.start <= left.end:
            # It merges touching ranges to a single one.
            left = Range(left.start, right.end)
            continue
        # It appends disjoint range.
        disjoint.append(left)
        left = right
    disjoint.append(left)
    return _rangeset(disjoint)


def intersection(*items: Iterable[Range]) -> RangeSet:
    it = iter(items)
    ret = ALL_RANGE
    for ranges in it:
        ret &= union(ranges)
        if not ret:
            return NO_RANGE
    return ret


def _rangeset(items: Iterable[Range]) -> RangeSet:
    if not isinstance(items, Sequence):
        items = list(items)
    if len(items) == 0:
        return NO_RANGE
    if len(items) == 1:
        return items[0]
    lefts = items[: len(items) // 2]
    rights = items[len(lefts) :]
    return RangeTree(_rangeset(lefts), _rangeset(rights))
