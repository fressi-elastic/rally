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
import pickle
import shutil
import tempfile
import time
from collections.abc import Callable
from concurrent import futures
from typing import Any, Generic, Protocol, TypeVar, runtime_checkable

from thespian import actors
from typing_extensions import Self, TypeAlias

from esrally import actor, log, types
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
            if cfg.process_startup_method in [None, "fork"]:
                # It is unsafe forking a process after creating threads. This executor use a unique actor on a child
                # process that will run a thead pool executor for other processes.
                return LocalThreadPoolExecutor.from_config(cfg)
            return ThreadPoolExecutor.from_config(cfg)
        else:
            return ProcessPoolExecutor.from_config(cfg)

    def submit(self, fn: Callable, /, *args: Any, **kwargs: Any) -> futures.Future[R]:
        """It submits a callable to be executed with the given arguments.

        Schedules the callable to be executed as fn(*args, **kwargs) and returns
        a Future instance representing the execution of the callable.

        :returns: Future instance representing the execution of the callable.
        """

    @property
    def max_workers(self) -> int: ...


class ThreadPoolExecutor(futures.ThreadPoolExecutor, Executor):

    @classmethod
    def from_config(cls, cfg: types.AnyConfig = None) -> Self:
        cfg = ExecutorsConfig.from_config(cfg)
        LOG.info("Creating thread pool executor: max_workers: %d", cfg.max_workers)
        return cls(max_workers=cfg.max_workers, thread_name_prefix=WORKERS_NAME_PREFIX)

    @property
    def max_workers(self) -> int:
        return self._max_workers


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

    @property
    def max_workers(self) -> int:
        return self._max_workers


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
        """It prepares a record for queuing. The object returned by this method is enqueued.

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
    def receiveMsg_ExecutorLogRecord(msg: LogForwarderRecord, sender: actors.ActorAddress) -> None:
        logging.root.handle(msg.record)


@dataclasses.dataclass
class Task(Generic[R]):
    func: Callable
    args: tuple[Any, ...]
    kwargs: dict[str, Any]
    _handler: actors.ActorAddress | futures.Future[R] | None = None

    @property
    def handler(self) -> futures.Future | actors.ActorAddress:
        if self._handler is None:
            raise RuntimeError("Task handler has not been set yet.")
        return self._handler

    @handler.setter
    def handler(self, value: futures.Future | actors.ActorAddress) -> None:
        if self._handler is not None:
            raise RuntimeError("Task handler has already been set.")
        self._handler = value

    @property
    def system(self) -> actor.ActorSystem:
        return actor.ActorSystem.from_config()

    def __call__(self) -> R:
        try:
            return self._task_executed(TaskExecuted(self.func(*self.args, **self.kwargs)))
        except Exception as ex:
            self._task_failed(TaskFailed(ex))
            raise

    def _task_executed(self, executed: TaskExecuted[R]) -> R:
        res = executed.result
        if isinstance(self._handler, actors.ActorAddress):
            self.system.tell(self._handler, executed)
            return res
        if isinstance(self._handler, futures.Future):
            self._handler.set_result(res)
            return res
        LOG.debug("Unhandled task result: %s -> %r", self, res)
        return res

    def _task_failed(self, failed: TaskFailed) -> None:
        err = failed.error
        if isinstance(self._handler, actors.ActorAddress):
            self.system.tell(self._handler, failed)
            return
        if isinstance(self._handler, futures.Future):
            self._handler.set_exception(err)
            return
        LOG.exception("Unhandled task exception: %s -> %r", self, err)

    def __repr__(self) -> str:
        args = [f"{a!r}" for a in self.args] + [f"{n}={v!r}" for n, v in self.kwargs.items()]
        return f"{self.func}({', '.join(args)})"


@dataclasses.dataclass
class LocalTask(Task):
    """Serializable task that will work as a multiprocess future using a file to pass result between processes.

    Note: result() method will work only if the task is executed on the same process as this task.
    """

    result_file: str = ""

    @property
    def handler(self) -> futures.Future | actors.ActorAddress:
        try:
            return super().handler
        except RuntimeError:
            if self.result_file and os.path.isfile(self.result_file):
                self._handler = LocalFuture(self.result_file)
                return self._handler
            raise

    def _task_executed(self, executed: TaskExecuted[R]) -> R:
        if self.result_file and os.path.isfile(self.result_file):
            self.save_result(executed, self.result_file)
            return executed.result
        return super()._task_executed(executed)

    def _task_failed(self, failed: TaskFailed) -> None:
        if self.result_file and os.path.isfile(self.result_file):
            self.save_result(failed, self.result_file)
            return
        super()._task_failed(failed)

    @staticmethod
    def load_result(filename: str) -> TaskExecuted | TaskFailed | None:
        with open(filename, "rb") as f:
            try:
                result = pickle.load(f)
            except EOFError:
                return None
            if isinstance(result, (TaskExecuted, TaskFailed)):
                return result
            raise TypeError(f"Invalid result type {type(result)}")

    @staticmethod
    def save_result(result: TaskExecuted | TaskFailed, filename: str) -> None:
        with open(filename, "wb") as f:
            pickle.dump(result, f)


class LocalFuture(futures.Future[R]):

    def __init__(self, result_file: str = "") -> None:
        super().__init__()
        self.result_file = result_file

    def result(self, timeout: float | None = None) -> R:
        deadline: float | None = None
        if timeout is not None:
            deadline = time.monotonic() + timeout

        while True:
            try:
                return super().result(timeout=0.001)
            except futures.TimeoutError:
                if deadline is not None and deadline > time.monotonic():
                    raise
                if not self.result_file or not os.path.isfile(self.result_file):
                    self.cancel()
                    continue
            try:
                res = LocalTask.load_result(self.result_file)
                if isinstance(res, TaskExecuted):
                    self.set_result(res.result)
                    return res.result
                if isinstance(res, TaskFailed):
                    self.set_exception(res.error)
                    raise res.error
                raise TypeError(f"Invalid result type {type(res)}")
            except FileNotFoundError:
                continue
            finally:
                try:
                    os.remove(self.result_file)
                except OSError:
                    pass
                self.result_file = ""

    def exception(self, timeout: float | None = None) -> Exception | None:
        try:
            self.result(timeout)
        except TimeoutError:
            raise
        except Exception as ex:
            return ex
        else:
            return None


@dataclasses.dataclass
class TaskExecuted(Generic[R]):
    result: R


@dataclasses.dataclass
class TaskFailed:
    error: Exception


class LocalThreadPoolActor(actor.LocalActor):
    """It implements a global local actor that will run tasks using a thread pool executor."""

    Config = ExecutorsConfig

    def __init__(self):
        super().__init__()
        self._executor: Executor | None = None

    @actor.no_retry()
    def receiveMsg_ActorExitRequest(self, msg: actors.ActorExitRequest, sender: actors.ActorAddress) -> None:
        if hasattr(self._executor, "shutdown"):
            self._executor.shutdown()
            self._executor = None

    @actor.no_retry()
    def receiveMsg_Task(self, task: Task, sender: actors.ActorAddress) -> None:
        self.executor.submit(task)

    @property
    def max_workers(self) -> int | None:
        return max(1, getattr(self.executor, "max_workers", self.cfg.max_workers or 1))

    @property
    def executor(self) -> Executor:
        if self._executor is None:
            self._executor = ThreadPoolExecutor.from_config(cfg=self.cfg)
        return self._executor


class LocalThreadPoolExecutor(Executor):
    """Executor class that runs tasks from a global actor of class LocalThreadPoolActor running locally."""

    actor_class = LocalThreadPoolActor
    cfg: ExecutorsConfig
    task = LocalTask

    @classmethod
    def from_config(cls, cfg: types.AnyConfig = None) -> Self:
        cfg = ExecutorsConfig.from_config(cfg)
        return cls(cfg=cfg, actor=cls.actor_class.from_config(cfg=cfg))

    def __init__(self, cfg: types.AnyConfig, actor: actors.ActorAddress) -> None:
        self.cfg = ExecutorsConfig.from_config(cfg)
        self.actor = actor
        self.handler: actors.ActorAddress | None = None
        self._results_dir = tempfile.mkdtemp()

    @property
    def system(self) -> actor.ActorSystem:
        return actor.ActorSystem.from_config(self.cfg)

    def submit(self, fn, /, *args, **kwargs) -> futures.Future:
        """It submits a callable to be executed with the given arguments.

        Schedules the callable to be executed as fn(*args, **kwargs) and returns
        a Future instance representing the execution of the callable.

        :returns: Future instance representing the execution of the callable.
        """
        task: Task
        if isinstance(fn, Task):
            task = fn
            if args or kwargs:
                raise ValueError("Can't specify both args and kwargs when fn is of type Task")
        else:
            task = self.task(fn, args, kwargs)

        if self.handler:
            task.handler = self.handler
        elif isinstance(task, LocalTask):
            task.result_file = self._new_result_file()
        self.system.tell(self.actor, task)

        assert isinstance(task.handler, futures.Future)
        return task.handler

    def _new_result_file(self) -> str:
        with tempfile.NamedTemporaryFile(dir=self._results_dir, delete=False) as f:
            return f.name

    def shutdown(self) -> None:
        self._delete_result_files()

    def _delete_result_files(self) -> None:
        if not os.path.isdir(self._results_dir):
            return
        for filename in os.listdir(self._results_dir):
            path = os.path.join(self._results_dir, filename)
            try:
                result = LocalTask.load_result(path)
            except EOFError:
                LOG.debug("Task not executed.")
                continue
            except Exception:
                LOG.exception("Failed to load task result from %s", path)
                continue
            finally:
                os.remove(path)

            if result is None:
                continue
            if isinstance(result, TaskExecuted):
                LOG.debug("Unhandled task result: %s", result)
                continue
            if isinstance(result, TaskFailed):
                LOG.error("Unhandled task failure: %s", result)
                continue
            LOG.error("Invalid task result type: %s", result)

        shutil.rmtree(self._results_dir, ignore_errors=True)
