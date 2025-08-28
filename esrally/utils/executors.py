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

import copy
import dataclasses
import logging
import multiprocessing
import os
import typing
from collections import defaultdict, deque
from collections.abc import Callable
from concurrent import futures
from typing import Any, Protocol, runtime_checkable, overload, TypeVar

from thespian import actors
from typing_extensions import Self, TypeAlias

from esrally import actor, log, types
from esrally.actor import BaseActor, ActorConfig
from esrally.utils import console, convert

LOG = logging.getLogger(__name__)

MAX_WORKERS: int = 63
WORKERS_NAME_PREFIX: str = "esrally-executor-worker"
USE_THREADING: bool = True
LOG_FORWARDER_LEVEL = logging.NOTSET


class ExecutorsConfig(actor.ActorConfig):

    @property
    def log_forwarder_level(self) -> int:
        return int(self.opts("executors", "executors.forwarder.log.level", LOG_FORWARDER_LEVEL or logging.root.level, False))

    @property
    def max_workers(self) -> int:
        return int(self.opts("executors", "executors.max_workers", MAX_WORKERS, False))

    @property
    def use_threading(self) -> bool:
        return convert.to_bool(self.opts("executors", "executors.use_threading", USE_THREADING, False))


TaskResultHandler: TypeAlias = Callable[["ExecutorResult"], Any]


R = TypeVar("R")

@runtime_checkable
class Executor(Protocol):

    @classmethod
    def from_config(cls, cfg: types.AnyConfig = None) -> Executor:
        cfg = ExecutorsConfig.from_config(cfg)
        if cfg.use_threading:
            return ThreadPoolExecutor.from_config(cfg)
        else:
            return ProcessPoolExecutor.from_config(cfg)

    @overload
    def submit(self, fn: Callable[[], R]) -> futures.Future[R]: ...

    @overload
    def submit(self, fn: Callable[[...], R], /, *args: typing.Any, **kwargs: typing.Any) -> futures.Future[R]: ...

    def submit(self, fn, /, *args, **kwargs) -> futures.Future[R]:
        """It submits a callable to be executed with the given arguments.

        Schedules the callable to be executed as fn(*args, **kwargs) and returns
        a Future instance representing the execution of the callable.

        :returns: Future instance representing the execution of the callable.
        """


class ThreadPoolExecutor(futures.ThreadPoolExecutor, Executor):

    @classmethod
    def from_config(cls, cfg: types.AnyConfig = None) -> Self:
        cfg = ExecutorsConfig.from_config(cfg)
        LOG.info("Creating thread pool executor: max_workers: %d", cfg.max_workers)
        return cls(max_workers=cfg.max_workers, thread_name_prefix=WORKERS_NAME_PREFIX)


class ProcessPoolExecutor(futures.ProcessPoolExecutor, Executor):

    @classmethod
    def from_config(cls, cfg: types.AnyConfig = None) -> Self:
        cfg = ExecutorsConfig.from_config(cfg)
        LOG.info("Creating process pool executor: max_workers: %d", cfg.max_workers)
        return cls(
            max_workers=cfg.max_workers,
            mp_context=multiprocessing.get_context(cfg.process_startup_method),
            initializer=ProcessPoolHelper.from_config(cfg).initialize_subprocess,
        )


@dataclasses.dataclass
class ProcessPoolHelper:

    cfg: ExecutorsConfig
    actor: actors.ActorAddress
    level: int = LOG_FORWARDER_LEVEL

    @classmethod
    def from_config(cls, cfg: types.AnyConfig = None) -> Self:
        cfg = ExecutorsConfig.from_config(cfg)
        actor_system = actor.system_from_config(cfg)
        helper = cls(
            cfg=cfg,
            actor=actor_system.createActor(ProcessPoolHelperActor, globalName=f"{__name__}:ProcessPoolHelperActor"),
            level=cfg.log_forwarder_level,
        )
        return helper

    def initialize_subprocess(self) -> None:
        """It prepares the new subprocess before taking tasks to execute."""

        # Initialize logging system.
        if self.level:
            logging.root.setLevel(self.level)
        logging.root.addHandler(LogForwarderHandler(self.level, self.actor))
        log.post_configure_logging()
        console.set_assume_tty(assume_tty=False)

        # Initialize actor system.
        actor.system_from_config(cfg=self.cfg)

        LOG.debug("Executor subprocess initialized: pid=%d.", os.getpid())

    def shutdown(self) -> None:
        """It waits for all log records to be handled before shutting down the helper actor."""
        LOG.debug("Send actor exit request.")
        actors.ActorSystem().ask(self.actor, actors.ActorExitRequest())


@dataclasses.dataclass
class LogForwarderRecord:
    record: logging.LogRecord


class LogForwarderHandler(logging.Handler):

    def __init__(self, level: int, actor_addr: actors.ActorAddress) -> None:
        super().__init__(level)
        self.actor_addr = actor_addr

    def emit(self, record: logging.LogRecord) -> None:
        actors.ActorSystem().tell(self.actor_addr, self.prepare(record))

    def prepare(self, record: logging.LogRecord) -> LogForwarderRecord:
        """
        Prepare a record for queuing. The object returned by this method is
        enqueued.

        The base implementation formats the record to merge the message and
        arguments, and removes unpickleable items from the record in-place.
        Specifically, it overwrites the record's `msg` and
        `message` attributes with the merged message (obtained by
        calling the handler's `format` method), and sets the `args`,
        `exc_info` and `exc_text` attributes to None.

        You might want to override this method if you want to convert
        the record to a dict or JSON string, or send a modified copy
        of the record while leaving the original intact.
        """
        # The format operation gets traceback text into record.exc_text
        # (if there's exception data), and also returns the formatted
        # message. We can then use this to replace the original
        # msg + args, as these might be unpickleable. We also zap the
        # exc_info, exc_text and stack_info attributes, as they are no longer
        # needed and, if not None, will typically not be pickleable.
        msg = self.format(record)
        # bpo-35726: make copy of record to avoid affecting other handlers in the chain.
        record = copy.copy(record)
        record.message = msg
        record.msg = msg
        record.args = None
        record.exc_info = None
        record.exc_text = None
        record.stack_info = None
        return LogForwarderRecord(record)


class ProcessPoolHelperActor(actor.BaseActor):

    @staticmethod
    def receiveMsg_ExecutorLogRecord(self, msg: LogForwarderRecord, sender: actors.ActorAddress) -> None:
        logging.root.handle(msg.record)


@dataclasses.dataclass
class ActorExecutorTask:
    func: Callable
    args: tuple[Any, ...] = tuple()
    kwargs: dict[str, Any] | None = None
    handlers: set[actors.ActorAddress] = dataclasses.field(default_factory=set)

    def __call__(self):
        try:
            result = self.func(*self.args, **(self.kwargs or {}))
            self.handle_result(ActorExecutorResult(self, result, None))
            return result
        except Exception as ex:
            self.handle_result(ActorExecutorResult(self, None, ex))
            raise

    def handle_result(self, result: ActorExecutorResult) -> None:
        handled = False
        for handler in self.handlers:
            try:
                actors.ActorSystem().tell(handler, result)
                handled = True
            except Exception:
                LOG.exception("error handling task result (handler=%s, result=%s)", handler, result)
        if not handled:
            if result.error is not None:
                LOG.exception("unhandled executor task error (task=%s)", result.task, exc_info=result.error)
            if result.value is not None:
                LOG.warning("unhandled executor task result (task=%s, result=%s)", result.task, result.value)


@dataclasses.dataclass
class ActorExecutorResult:
    task: ActorExecutorTask
    value: Any = None
    error: Exception | None = None


class ExecutorActor(BaseActor, Executor):
    """This is a wrapper actor class that integrates with an Executor to run asynchronous tasks.
    """

    Config = ExecutorsConfig
    Executor = Executor

    def __init__(self):
        super().__init__()
        self._executor: Executor | None = None
        self._workers: int = 0
        self._max_workers: int = max(1, self.cfg.max_workers)
        self._paused_tasks: deque[ActorExecutorTask] = deque()

    @actor.no_retry()
    def receiveMsg_ActorConfig(self, cfg: ActorConfig, sender: actors.ActorAddress):
        cfg = self.Config.from_config(cfg)
        self.max_workers = cfg.max_workers
        return self.SUPER

    @actor.no_retry()
    def receiveMsg_ActorExitRequest(self, msg: actors.ActorExitRequest, sender: actors.ActorAddress) -> None:
        if hasattr(self._executor, "shutdown"):
            self._executor.shutdown()
            self._executor = None

    @actor.no_retry()
    def receiveMsg_ActorExecutorTask(self, task: ActorExecutorTask, sender: actors.ActorAddress) -> None:
        if self._workers >= self.max_workers:
            # The executor is busy, the task will wait on actor queue.
            self._paused_tasks.append(task)
            return
        task.handlers.add(self.myAddress)
        self.executor.submit(task)
        self._workers += 1

    @actor.no_retry()
    def receiveMsg_ActorExecutorResult(self, result: ActorExecutorResult, sender: actors.ActorAddress) -> None:
        self._workers -= 1
        self.unpause_tasks()
        self.handle_executor_result(result)

    def handle_executor_result(self, result: ActorExecutorResult) -> None:
        if result.error is not None:
            raise result.error

    @property
    def max_workers(self) -> int:
        return max(1, min(self._max_workers, getattr(self.executor, 'max_workers', self.cfg.max_workers)))

    @max_workers.setter
    def max_workers(self, value: int) -> None:
        if value < 1:
            raise ValueError("max_workers must be a positive integer")
        self._max_workers = value

    @property
    def executor(self) -> Executor:
        if self._executor is None:
            self._executor = self.Executor.from_config(cfg=self.cfg)
        return self._executor

    def submit(self, fn: Callable, /, *args, **kwargs):
        """Submits a callable to be executed with the given arguments.

        Schedules the callable to be executed as fn(*args, **kwargs) and returns
        a Future instance representing the execution of the callable.

        Returns:
            A Future representing the given call.
        """
        self.receiveMsg_ActorExecutorTask(ActorExecutorTask(fn, *args, **kwargs, {self.myAddress}), self.myAddress)

    def unpause_tasks(self):
        while self._workers < self.max_workers and self._paused_tasks:
            self.receiveMsg_ActorExecutorTask(self._paused_tasks.popleft(), self.myAddress)
