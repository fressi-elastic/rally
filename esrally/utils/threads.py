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

import threading
import time
from collections.abc import Callable


class ContinuousTimer(threading.Thread):
    """It calls a function every a specified number of seconds:

        t = ContinuousTimer(30.0, f, args=None, kwargs=None)
        t.start()
        t.cancel()     # stop the timer's action if it's still waiting

    This implementation is inspired by threading.Timer but with following differences:
        - the thread is daemonic by default, so it will not block the process to terminate.
        - the function is called periodically until the timer is cancelled.
    """

    def __init__(self, interval, function: Callable[[], None], name: str | None = None, daemon=True):
        super().__init__(name=name, daemon=daemon)
        self.interval = interval
        self.function = function
        self.finished = threading.Event()

    def cancel(self):
        """Stop the timer if it hasn't finished yet."""
        self.finished.set()

    def run(self):
        """It executes the function every interval seconds until the timer is cancelled."""
        self.finished.wait(self.interval)
        while not self.finished.is_set():
            self.function()
            self.finished.wait(self.interval)


class TimedEvent:
    """It re-implement threading.Event objects with a couple of additional functionalities.

    TimedEvent manages a flag that can be set to true with the set() method and reset
    to false with the clear() method. The wait() method blocks until the flag is
    true. The flag is initially false.

    On top of the threading.Event functionalities:
        - the set() method returns True in case the flag was False at the time it has been called;
        - its time() method returns the value of monotonic.time() at the time set has been called first.
    """

    # After threading.Event class (with time() method)

    def __init__(self):
        self._cond = threading.Condition(threading.Lock())
        self._flag = False
        self._time: float | None = None

    def is_set(self) -> bool:
        """Return true if and only if the internal flag is true."""
        return self._flag

    def set(self) -> bool:
        """It sets the internal flag to true.

        All threads waiting for it to become true are awakened. Threads
        that call wait() once the flag is true will not block at all.
        :return True in case the flag was False.
        """
        with self._cond:
            if self._flag:
                return False
            self._flag = True
            self._time = time.monotonic()
            self._cond.notify_all()
            return True

    def clear(self):
        """Reset the internal flag to false.

        Subsequently, threads calling wait() will block until set() is called to
        set the internal flag to true again.
        """
        with self._cond:
            self._flag = False

    def wait(self, timeout: float | None = None) -> bool:
        """Block until the internal flag is true.

        If the internal flag is true on entry, return immediately. Otherwise,
        block until another thread calls set() to set the flag to true, or until
        the optional timeout occurs.

        When the timeout argument is present and not None, it should be a
        floating point number specifying a timeout for the operation in seconds
        (or fractions thereof).

        This method returns the internal flag on exit, so it will always return
        True except if a timeout is given and the operation times out.

        """
        with self._cond:
            signaled = self._flag
            if not signaled:
                signaled = self._cond.wait(timeout)
            return signaled

    def time(self) -> float | None:
        """It gets the result of time.monotonic() at the moment the event has been set.
        :return: the monotonic time float value if set, or None otherwise.
        """
        return self._time


class WorkersLimitError(Exception):
    """Raised when a wait group reach max workers limit."""


class WaitGroup(TimedEvent):
    """It implements a go-lang style wait group on top of a timed event.

    After N-times the done method is called, the event will be set. This can be used for waiting for waiting for
    multiple works to terminate.

    Example of use:
        wg = WaitGroup()
        for i in range(10):
            wg.add(1)
            def work():
                # do something
                wg.done()
            executor.submit(work)

        wg.wait()
    """

    def __init__(self, workers: int = 0, max_workers: int | None = None):
        super().__init__()
        self._workers = workers
        self.max_workers = max_workers

    @property
    def workers(self) -> int:
        return self._workers

    def add(self, value: int) -> bool:
        with self._cond:
            new_workers = self._workers + value
            if self.max_workers is not None and new_workers > self.max_workers:
                raise WorkersLimitError(f"number of workers exceeded: {new_workers} > {self.max_workers}")
            self._workers = new_workers
            return self._workers <= 0 and self.set()

    def done(self) -> bool:
        return self.add(-1)
