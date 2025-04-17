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

import contextlib
import enum
import logging
import os
import threading
import time
from typing import Protocol

from esrally.storage._adapter import Head, ServiceUnavailableError
from esrally.storage._io import BaseStream, OutputStream, StreamClosedError
from esrally.storage._range import ALL_RANGE, NO_RANGE, Range, RangeSet
from esrally.utils import threads

LOG = logging.getLogger(__name__)


class TransferCancelled(Exception):
    """It is raised to communicate that the transfer has been canceled."""


class RetryTransfer(Exception):
    """It is raised to communicate the task should be retried."""


class TransferDirection(enum.Enum):
    DOWN = 1
    UP = 2


class TransferStatus(enum.Enum):
    QUEUED = 0
    DOING = 1
    DONE = 2
    CANCELLED = 3
    FAILED = 4


class Executor(Protocol):
    def submit(self, fn, /, *args, **kwargs):
        """Submits a callable to be executed with the given arguments.

        Schedules the callable to be executed as fn(*args, **kwargs).
        """
        raise NotImplementedError()


MAX_WORKERS = 4


class Transfer:

    def __init__(
        self,
        head: Head,
        path: str,
        direction: TransferDirection,
        executor: Executor,
        ranges: RangeSet = NO_RANGE,
        max_workers: int = MAX_WORKERS,
    ):
        self.direction = direction
        self.path = path
        self.started = threads.TimedEvent()
        self.finished = threads.WaitGroup(max_workers=max_workers)
        self._head = head
        if not ranges:
            ranges = ALL_RANGE
        if head.content_length is not None:
            ranges &= Range(0, head.content_length)
        self._todo: RangeSet = ranges
        self._doing: list[BaseStream] = []
        self._done: RangeSet = NO_RANGE
        self._executor = executor
        self._errors = list[Exception]()
        self._lock = threading.Lock()

    def submit(self, fn, /, *args, **kwargs):
        with self._lock:
            self.finished.add(1)
            self._executor.submit(self._execute, fn, *args, **kwargs)

    @property
    def todo(self) -> RangeSet:
        return self._todo

    @property
    def url(self) -> str:
        return self._head.url

    @property
    def accept_ranges(self) -> bool:
        return self._head.accept_ranges

    @property
    def content_length(self) -> int | None:
        return self._head.content_length

    def _execute(self, fn, /, *args, **kwargs):
        if self.started.set():
            LOG.info("%s: transfer started", self.url)
        if self.finished.is_set():
            LOG.debug("%s @%s: task cancelled", self.url, pretty_func(fn))
            self.finished.done()
            return
        try:
            fn(*args, **kwargs)
        except StreamClosedError as ex:
            LOG.debug("%s @%s: task cancelled: %s", self.url, pretty_func(fn), ex)
        except ServiceUnavailableError as ex:
            LOG.info("%s @%s: service limit reached (workers=%d): %s", self.url, pretty_func(fn), self.finished.workers, ex)
            self.finished.max_workers = max(1, self.finished.workers - 1)
        except Exception as ex:
            LOG.exception("%s @%s: task failed", self.url, pretty_func(fn))
            with self._lock:
                self._errors.append(ex)
            self.close(f"task failed: {ex}")
        else:
            LOG.debug("%s @%s: task executed", self.url, pretty_func(fn))
        finally:
            self.finished.done()
            if self.finished.is_set():
                if self._errors:
                    LOG.error("%s: transfer failed", self.url)
                else:
                    LOG.info("%s: transfer succeeded", self.url)
                self.close()

    def close(self, reason: str = "closed"):
        """It cancels ann the transfer tasks."""
        with self._lock:
            self.finished.set()
            streams = self._doing
            self._doing = []
        while streams:
            stream = streams.pop()
            try:
                stream.close()
            except Exception as ex:
                LOG.warning("error closing stream: %s", ex)

    @contextlib.contextmanager
    def write_file(self, max_length: int | None = None):
        if os.path.isfile(self.path):
            fd = open(self.path, "rb+")
        else:
            os.makedirs(os.path.dirname(self.path), exist_ok=True)
            fd = open(self.path, "wb+")
        with fd:
            try:
                doing: Range
                with self._lock:
                    doing, self._todo = self._todo.pop(max_length)
                    if doing.start > 0:
                        fd.seek(doing.start)
                    stream = OutputStream(fd, doing, close=False)
                    self._doing.append(stream)
                with stream:
                    yield stream
            finally:
                with self._lock:
                    done, todo = doing.pop(fd.tell())
                    self._done |= done
                    self._todo |= todo

    def transferred(self) -> int:
        with self._lock:
            return sum(fd.transferred for fd in self._doing)

    def progress(self) -> float:
        if self.length:
            return 100.0 * self.transferred() / self.length
        return 0.0

    def duration(self) -> float:
        """Obtain the transfer duration.
        :return: the transfer duration in seconds.
        """
        started = self.started.time()
        if started is None:
            # hasn't started yet.
            return 0.0
        finished = self.finished.time()
        if finished is None:
            # hasn't finished yet.
            finished = time.monotonic()
        return finished - started

    def average_speed(self) -> float:
        """Obtain the average transfer speed .
        :return: speed in bytes per seconds.
        """
        try:
            return self.transferred() / self.duration()
        except ZeroDivisionError:
            return 0.0

    @property
    def length(self) -> int | None:
        return self._head and self._head.content_length

    def info(self) -> str:
        direction = {TransferDirection.DOWN: "<", TransferDirection.UP: ">"}.get(self.direction, "=")
        return (
            f"{direction} {self.url} {self.progress():.0f}% "
            f"{pretty_size(self.transferred())}/{pretty_size(self.length)} {pretty_time(self.duration())} "
            f"{pretty_size(self.average_speed())}/s {self.status()} {self.finished.workers} workers"
        )

    def wait(self, timeout: float | None = None) -> bool:
        if not self.finished.wait(timeout):
            return False
        for ex in self._errors:
            raise ex
        return True

    def status(self) -> TransferStatus:
        if self.finished.is_set():
            if self._errors:
                return TransferStatus.FAILED

                # if self.range.stop is None or self.range.start < self.range.stop:
                #     return TransferStatus.CANCELLED
                # else:
            return TransferStatus.DONE
        if self.started.is_set():
            return TransferStatus.DOING
        return TransferStatus.QUEUED

    def errors(self) -> list[Exception]:
        return list(self._errors)


def pretty_size(value: int | float | None) -> str:
    if value is None:
        return "?"
    value = float(value)
    if value < 1024.0:
        return f"{value:.0f}B"
    value /= 1024.0
    if value < 1024.0:
        return f"{value:.0f}KB"
    value /= 1024.0
    if value < 1024.0:
        return f"{value:.2f}MB"
    value /= 1024.0
    if value < 1024.0:
        return f"{value:.2f}GB"
    value /= 1024.0
    return f"{value:.2f}TB"


def pretty_time(value: int | float):
    value = float(value)
    if value < 60.0:
        return f"{value:.0f}s"
    value /= 60.0
    if value < 60.0:
        return f"{value:.2f}m"
    value /= 60.0
    return f"{value:.2f}h"


def pretty_func(fn):
    return getattr(fn, "__qualname__", None) or getattr(fn, "__name__", None) or str(fn)
