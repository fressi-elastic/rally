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
import functools
import logging
import os
import socket
import time
import traceback
import typing
import uuid
from collections.abc import Callable, Generator
from typing import Any

from thespian import actors  # type: ignore[import-untyped]
from thespian.system.logdirector import (  # type: ignore[import-untyped]
    ThespianLogForwarder,
)
from typing_extensions import TypeAlias

from esrally import config, exceptions, log, types
from esrally.utils import console

LOG = logging.getLogger(__name__)


class BenchmarkFailure:
    """It indicates a failure in the benchmark execution due to an exception."""

    def __init__(self, message, cause=None):
        self.message = message
        self.cause = cause


class BenchmarkCancelled:
    """It indicates that the benchmark has been cancelled (by the user)."""


class ActorConfig(config.Config):

    @property
    def system_base(self) -> SystemBase:
        return self.opts("actor", "actor.system.base", default_value=SYSTEM_BASE, mandatory=False)

    @property
    def fallback_system_base(self) -> SystemBase:
        return self.opts("actor", "actor.fallback.system.base", default_value=FALLBACK_SYSTEM_BASE, mandatory=False)

    @property
    def ip(self) -> str | None:
        return self.opts("actor", "actor.ip", default_value=ACTOR_IP, mandatory=False) or None

    @property
    def admin_port(self) -> int | None:
        return int(self.opts("actor", "actor.admin.port", default_value=ADMIN_PORT, mandatory=False)) or None

    @property
    def coordinator_ip(self) -> str | None:
        return self.opts("actor", "actor.coordinator.ip", default_value=COORDINATOR_IP, mandatory=False).strip() or None

    @property
    def coordinator_port(self) -> int | None:
        return int(self.opts("actor", "actor.coordinator.port", default_value=COORDINATOR_PORT, mandatory=False)) or None

    @property
    def process_startup_method(self) -> ProcessStartupMethod | None:
        return self.opts("actor", "actor.process.startup.method", default_value="", mandatory=False).strip() or None


HOSTNAME = socket.gethostname()


class BaseActor(actors.ActorTypeDispatcher):

    config_class = ActorConfig

    @classmethod
    def from_config(cls, cfg: types.AnyConfig = None) -> actors.ActorAddress:
        actor_system = ActorSystem.from_config(cfg)
        actor_address = actor_system.createActor(
            actorClass=cls, targetActorRequirements=cls.target_actor_requirements(), globalName=cls.global_name()
        )
        if cfg is not None:
            actor_system.tell(actor_address, cfg)
        return actor_address

    @classmethod
    def target_actor_requirements(cls) -> dict[str, Any]:
        return {}

    @classmethod
    def global_name(cls) -> str | None:
        return None

    # The method name is required by the actor framework
    # noinspection PyPep8Naming
    @classmethod
    def actorSystemCapabilityCheck(cls, capabilities: dict[str, Any], requirements: dict[str, Any]) -> bool:
        capabilities.setdefault("hostname", HOSTNAME)
        for name, value in requirements.items():
            current = capabilities.get(name)
            if current != value:
                # A single mismatch event is not a problem by itself as long as at least one actor system instance
                # matches the requirements.
                return False
        return True

    def __init__(self):
        super().__init__()
        self._cfg: ActorConfig | None = None
        log.post_configure_logging()
        console.set_assume_tty(assume_tty=False)
        cls = type(self)
        self.logger = logging.getLogger(f"{cls.__module__}:{cls.__name__}")
        self.logger.debug("Initializing actor (pid=%d): '%s'.", os.getpid(), self)

    @property
    def cfg(self) -> ActorConfig:
        if self._cfg is None:
            self.cfg = self.config_class.from_config()
        assert isinstance(self._cfg, ActorConfig)
        return self._cfg

    @cfg.setter
    def cfg(self, value: types.AnyConfig) -> None:
        self._cfg = cfg = self.config_class.from_config(value)
        self.configure_actor(cfg)

    def configure_actor(self, cfg: ActorConfig) -> None:
        self.logger.debug("Actor configuration received: %s.", cfg)

    def receiveMsg_ActorConfig(self, msg: ActorConfig, sender: actors.ActorAddress) -> None:
        self.cfg = msg
        self.configure_actor(msg)

    def receiveMsg_Request(self, request: Request, sender: actors.ActorAddress) -> None:
        result: Any = None
        self.send(sender, Started(request.uuid))
        try:
            result = self.receiveMessage(request.message, sender)
        except Exception as ex:
            LOG.exception("Failed processing request: %s", request)
            self.send(sender, Error(request.uuid, ex))
        finally:
            self.send(sender, Done(request.uuid, result))

    def receiveMsg_Response(self, response: Response, sender: actors.ActorAddress) -> None:
        try:
            result = response.result()
        except Exception as ex:
            self.receive_error(ex, sender)
        else:
            if result is not None:
                self.receiveMessage(result, sender)

    def receive_error(self, ex: Exception, sender: actors.ActorAddress) -> None:
        self.logger.error("Received request error from '%s': '%s'", sender, ex)

    def receiveMsg_ActorExitRequest(self, msg: actors.ActorExitRequest, sender: actors.ActorAddress) -> None:
        self.logger.debug("Received exit request from '%s': %s", sender, msg)

    def receiveMsg_ChildActorExited(self, msg: actors.ActorExitRequest, sender: actors.ActorAddress) -> None:
        self.logger.debug("Received child actor exited from '%s': %s", sender, msg)

    def receiveUnrecognizedMessage(self, msg: Any, sender: actors.ActorAddress) -> None:
        self.logger.warning("Received unrecognized message from '%s': %s", sender, msg)


class LocalActor(BaseActor):
    """Actor mixin class that ensures actors to be created only on the host where are is being required."""

    @classmethod
    def target_actor_requirements(cls) -> dict[str, Any]:
        return {"hostname": HOSTNAME}

    @classmethod
    def global_name(cls) -> str | None:
        return f"{cls.__module__}:{cls.__name__}@{HOSTNAME}"


M = typing.TypeVar("M")
ActorMessageHandler: TypeAlias = Callable[[BaseActor, M, actors.ActorAddress], None]


def no_retry() -> Callable[[ActorMessageHandler[M]], ActorMessageHandler[M]]:
    """Decorator intended for Thespian message handlers with the signature ``receiveMsg_$MSG_NAME(self, msg, sender)``.

    Thespian will assume that a message handler that raises an exception can be retried. It will then retry once and
    give up afterward just leaving a trace of that in the actor system's internal log file. However, this is usually
    *not* what we want in Rally. If handling of a message fails we instead want to notify a node higher up in the actor
    hierarchy.

    We achieve that by sending a ``BenchmarkFailure`` message to the original sender. Note that this might as well be
    the current actor (e.g. when handling a ``Wakeup`` message). In that case the actor itself is responsible for
    forwarding the benchmark failure to its parent actor.

    Example usage:

    @no_retry()
    def receiveMsg_DefuseBomb(self, msg: DefuseBomb, sender: ActorAddress) -> None:
        # might raise an exception
        pass

    If this message handler raises an exception, the decorator will turn it into a ``BenchmarkFailure`` message with its ``message``
    property set to "Error in special forces actor" which is returned to the original sender.
    """

    def decorator(handler: ActorMessageHandler[M]) -> ActorMessageHandler[M]:
        @functools.wraps(handler)
        def wrapper(self: BaseActor, msg: M, sender: actors.ActorAddress) -> None:
            try:
                return handler(self, msg, sender)
            except Exception:
                self.logger.exception("Failed handling message: %s", msg)
                # It avoids sending the exception itself because the sender process might not have the class available on
                # the load path, and it will fail while deserializing the cause.
                self.send(sender, BenchmarkFailure(traceback.format_exc()))
                return

        return wrapper

    return decorator


class RallyActor(BaseActor):

    def __init__(self):
        super().__init__()
        self.children: list[actors.ActorAddress] = []
        self.received_responses: list[typing.Any] = []
        self.status = None

    def transition_when_all_children_responded(self, sender, msg, expected_status, new_status, transition):
        """

        Waits until all children have sent a specific response message and then transitions this actor to a new status.

        :param sender: The child actor that has responded.
        :param msg: The response message.
        :param expected_status: The status in which this actor should be upon calling this method.
        :param new_status: The new status once all child actors have responded.
        :param transition: A parameter-less function to call immediately after changing the status.
        """
        if not self.is_current_status_expected(expected_status):
            raise exceptions.RallyAssertionError(
                "Received [%s] from [%s] but we are in status [%s] instead of [%s]." % (type(msg), sender, self.status, expected_status)
            )

        self.received_responses.append(msg)
        response_count = len(self.received_responses)
        expected_count = len(self.children)

        if response_count > expected_count:
            raise exceptions.RallyAssertionError(
                "Received [%d] responses but only [%d] were expected to transition from [%s] to [%s]. The responses are: %s"
                % (response_count, expected_count, self.status, new_status, self.received_responses)
            )

        if response_count < expected_count:
            self.logger.debug(
                "[%d] of [%d] child actors have responded for transition from [%s] to [%s].",
                response_count,
                expected_count,
                self.status,
                new_status,
            )
            return

        self.logger.debug(
            "All [%d] child actors have responded. Transitioning now from [%s] to [%s].", expected_count, self.status, new_status
        )
        # all nodes have responded, change status
        self.status = new_status
        self.received_responses = []
        self.sent_requests = 0
        transition()

    def send_to_children_and_transition(self, sender, msg, expected_status, new_status):
        """

        Sends the provided message to all child actors and immediately transitions to the new status.

        :param sender: The actor from which we forward this message (in case it is message forwarding), otherwise our own address.
        :param msg: The message to send.
        :param expected_status: The status in which this actor should be upon calling this method.
        :param new_status: The new status.
        """
        if not self.is_current_status_expected(expected_status):
            raise exceptions.RallyAssertionError(
                f"Received [{type(msg)}] from [{sender}] but we are in status [{self.status}] instead of [{expected_status}]."
            )

        self.logger.debug("Transitioning from [%s] to [%s].", self.status, new_status)
        self.status = new_status
        child: actors.ActorAddress
        # It removes children that are None
        self.children = list(filter(None, self.children))
        for child in self.children:
            self.send(child, msg)

    def is_current_status_expected(self, expected_status):
        # if we don't expect anything, we're always in the right status
        if not expected_status:
            return True
        # It does an explicit check for a list here because strings are also iterable, and we have a very tight control
        # over this code anyway.
        if isinstance(expected_status, list):
            return self.status in expected_status
        return self.status == expected_status


SystemBase = typing.Literal["simpleSystemBase", "multiprocQueueBase", "multiprocTCPBase", "multiprocUDPBase"]


__SYSTEM_BASE: SystemBase = "multiprocTCPBase"


def actor_system_already_running(
    ip: str | None = None,
    port: int | None = None,
    system_base: SystemBase | None = None,
) -> bool | None:
    """It determines whether an actor system is already running by opening a socket connection.

    Notes:
        - It may be possible that another system is running on the same port.
        - This is working only when system base is "multiprocTCPBase"
    """
    if system_base is None:
        system_base = __SYSTEM_BASE
    if system_base != "multiprocTCPBase":
        # This system is not supported yet.
        return None

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        try:
            ip = ip or "127.0.0.1"
            port = port or 1900
            LOG.info("Looking for an already running actor system (ip='%s', port=%d)...", ip, port)
            sock.connect((ip, port))
            return True
        except OSError as ex:
            LOG.info("Failed to connect to already running actor system (ip='%s', port=%d): %s", ip, port, ex)

    return False


def use_offline_actor_system() -> None:
    global __SYSTEM_BASE
    __SYSTEM_BASE = "multiprocQueueBase"
    LOG.info("Actor system base set to [%s]", __PROCESS_STARTUP_METHOD)


ProcessStartupMethod = typing.Literal[
    "fork",
    "forkserver",
    "spawn",
]


__PROCESS_STARTUP_METHOD: ProcessStartupMethod | None = None


def set_startup_method(method: ProcessStartupMethod) -> None:
    global __PROCESS_STARTUP_METHOD
    __PROCESS_STARTUP_METHOD = method
    LOG.info("Actor process startup method set to [%s]", __PROCESS_STARTUP_METHOD)


def bootstrap_actor_system(
    try_join: bool = False,
    prefer_local_only: bool = False,
    local_ip: str | None = None,
    admin_port: int | None = None,
    coordinator_ip: str | None = None,
    coordinator_port: int | None = None,
) -> actors.ActorSystem:
    system_base = __SYSTEM_BASE
    capabilities: dict[str, Any] = {}
    log_defs: Any = None
    if try_join and (
        system_base != "multiprocTCPBase" or actor_system_already_running(ip=local_ip, port=admin_port, system_base=system_base)
    ):
        LOG.info("Try joining already running actor system with system base [%s].", system_base)
    else:
        # All actor system are coordinator unless another coordinator is known to exist.
        capabilities["coordinator"] = True
        if system_base in ("multiprocTCPBase", "multiprocUDPBase"):
            if prefer_local_only:
                LOG.info("Bootstrapping locally running actor system with system base [%s].", system_base)
                local_ip = coordinator_ip = "127.0.0.1"

            if admin_port:
                capabilities["Admin Port"] = admin_port

            if local_ip:
                local_ip, admin_port = resolve(local_ip, admin_port)
                capabilities["ip"] = local_ip

            if coordinator_ip:
                coordinator_ip, coordinator_port = resolve(coordinator_ip, coordinator_port)
                if coordinator_port:
                    coordinator_port = int(coordinator_port)
                    if coordinator_port:
                        coordinator_ip += f":{coordinator_port}"
                capabilities["Convention Address.IPv4"] = coordinator_ip

            if coordinator_ip and local_ip and coordinator_ip != local_ip:
                capabilities["coordinator"] = False

        process_startup_method: ProcessStartupMethod | None = __PROCESS_STARTUP_METHOD
        if process_startup_method:
            capabilities["Process Startup Method"] = process_startup_method

        log_defs = log.load_configuration()
        LOG.info("Starting actor system with system base [%s] and capabilities [%s]...", system_base, capabilities)

    try:
        actor_system = actors.ActorSystem(
            systemBase=system_base,
            capabilities=capabilities,
            logDefs=log_defs,
        )
    except actors.ActorSystemException:
        LOG.exception("Could not initialize actor system with system base [%s] and capabilities [%s].", system_base, capabilities)
        raise

    LOG.info("Successfully initialized with system base [%s] and capabilities [%s].", system_base, actor_system.capabilities)
    return actor_system


SYSTEM_BASE: SystemBase = "multiprocTCPBase"
FALLBACK_SYSTEM_BASE: SystemBase = "multiprocQueueBase"
ACTOR_IP = "127.0.0.1"
ADMIN_PORT = 0
COORDINATOR_IP = ""
COORDINATOR_PORT = 0
PROCESS_STARTUP_METHOD: ProcessStartupMethod | None = None


class PoisonError(Exception):
    pass


class ActorSystem(actors.ActorSystem):

    @classmethod
    def from_config(cls, cfg: types.AnyConfig = None) -> ActorSystem:
        cfg = ActorConfig.from_config(cfg)
        first_error: Exception | None = None
        system_bases = [cfg.system_base]
        if cfg.fallback_system_base and cfg.fallback_system_base != cfg.system_base:
            system_bases.append(cfg.fallback_system_base)

        for sb in system_bases:
            try:
                return cls.create(
                    system_base=sb,
                    ip=cfg.ip,
                    admin_port=cfg.admin_port,
                    coordinator_ip=cfg.coordinator_ip,
                    coordinator_port=cfg.coordinator_port,
                    process_startup_method=cfg.process_startup_method,
                )
            except actors.ActorSystemException as ex:
                LOG.exception("Failed setting up actor system with system base '%s'", sb)
                first_error = first_error or ex
        raise first_error or Exception(f"Could not initialize actor system with system base '{cfg.system_base}'")

    @classmethod
    def create(
        cls,
        system_base: SystemBase | None = None,
        ip: str | None = None,
        admin_port: int | None = None,
        coordinator_ip: str | None = None,
        coordinator_port: int | None = None,
        process_startup_method: str | None = None,
    ) -> ActorSystem:
        if system_base and system_base not in typing.get_args(SystemBase):
            raise ValueError(f"invalid system base value: '{system_base}', valid options are: {typing.get_args(SystemBase)}")

        capabilities: dict[str, Any] = {"coordinator": True}
        if system_base in ("multiprocTCPBase", "multiprocUDPBase"):
            if ip:
                ip, admin_port = resolve(ip, admin_port)
                capabilities["ip"] = ip

            if admin_port:
                capabilities["Admin Port"] = admin_port

            if coordinator_ip:
                coordinator_ip, coordinator_port = resolve(coordinator_ip, coordinator_port)
                if coordinator_port:
                    coordinator_port = int(coordinator_port)
                    if coordinator_port:
                        coordinator_ip += f":{coordinator_port}"
                capabilities["Convention Address.IPv4"] = coordinator_ip
                if ip and coordinator_ip != ip:
                    capabilities["coordinator"] = False

        if system_base != "simpleSystemBase":
            if process_startup_method:
                if process_startup_method not in typing.get_args(ProcessStartupMethod):
                    raise ValueError(
                        f"invalid process startup method value: '{process_startup_method}', valid options are: "
                        f"{typing.get_args(ProcessStartupMethod)}"
                    )
                capabilities["Process Startup Method"] = process_startup_method

        log_defs = False
        if not isinstance(logging.root, ThespianLogForwarder):
            log_defs = log.load_configuration()
        return ActorSystem(systemBase=system_base, capabilities=capabilities, logDefs=log_defs)

    request_id: int = 0

    def request(
        self,
        actorAddr: actors.ActorAddress,
        msg: Any,
        timeout: float | None = None,
        retry_interval: float | None = None,
        retry_errors: tuple[type[Exception], ...] = tuple(),
    ) -> Generator[Any]:
        """It rewrites thespian ActorSystem.ask method raising a TimeoutError when request execution times out.

        This wraps ask method doing the following after running original implementation:
            - It yields results sent back by the actor.
            - It wraps the msg with a Request object to ensure
                - target actor will send an Error message in case of an unhandled exception.
                - target actor will finally return a Done message when finished handling the request.
            - It raises timeout exception when request execution times out.
            - It writes to log a warning for poison messages received.
            - It finally raises the last Error received by the actor.

        :param actorAddr: target actor address.
        :param msg: request message.
        :param timeout: optional timeout after which TimeoutError will be raised it the request is not executed before given deadline.
        :return: The response message from target actor, or a timeout exception if request execution times out.
        """
        if isinstance(msg, Request):
            request = msg
        else:
            request = Request(msg)

        ask_deadline: float | None = time.monotonic()
        if timeout is None:
            final_deadline = None
        else:
            final_deadline = time.monotonic() + timeout

        uuids: set[uuid.UUID] = set()
        errors: list[Exception] = []
        req_id: uuid.UUID | None = None
        res_id: uuid.UUID | None = None
        while True:
            if ask_deadline is not None and ask_deadline <= time.monotonic():
                if final_deadline is not None:
                    timeout = final_deadline - time.monotonic()
                request.uuid = req_id = uuid.uuid4()
                uuids.add(req_id)
                response = super().ask(actorAddr, request, timeout=timeout)
                if retry_interval is None:
                    ask_deadline = None
                else:
                    ask_deadline = time.monotonic() + retry_interval
            else:
                deadlines = [d for d in [ask_deadline, final_deadline] if d is not None]
                if deadlines:
                    timeout = min(deadlines) - time.monotonic()
                else:
                    timeout = None
                response = super().listen(timeout=timeout)

            result = None
            if isinstance(response, Response):
                res_id = response.uuid or res_id
                try:
                    r = response.result(timeout=timeout)
                except Exception as e:
                    if res_id in uuids:
                        errors.append(e)
                        if not isinstance(e, retry_errors):
                            retry_interval = None
                    else:
                        LOG.warning("Ignored error raised by actor (req_id: '%s'): '%s'", res_id, e)
                else:
                    result = r
                if isinstance(response, Done):
                    if res_id == req_id:
                        if retry_interval is None:
                            break
                    if res_id in uuids:
                        uuids.remove(res_id)
            elif isinstance(response, actors.PoisonMessage):
                LOG.warning("Ignored poison message: '%s'", response.details)
            elif response is not None:
                result = response

            if result is not None:
                if res_id in uuids:
                    yield result
                else:
                    LOG.warning("Ignored result returned by actor (req_id: '%s'): '%r'", res_id, result)

            if final_deadline is not None:
                timeout = final_deadline - time.monotonic()
                if timeout < 0:
                    errors.append(TimeoutError(f"Timed out while asking '{actorAddr}' for '{msg}'"))
                    break

        if errors:
            # Write to log all errors except the last one, and then it raises it.
            last_error = errors.pop()
            for error in errors:
                LOG.warning("Ignored error raised by actor (request uuid: '%s'): '%s'", request.uuid, error)
            raise last_error


def resolve(host: str, port: int | None = None, family: int = socket.AF_INET, proto: int = socket.IPPROTO_TCP) -> tuple[str, int | None]:
    address_info: tuple[Any, Any, Any, Any, tuple[Any, ...]]
    for address_info in socket.getaddrinfo(host, port=port or None, family=family, proto=proto):
        address = address_info[4]
        if len(address) == 2 and isinstance(address[0], str) and isinstance(address[1], int):
            host, port = address
    return host, port or None


@dataclasses.dataclass
class Request:
    # payload message carried by the request.
    message: Any
    # uuid is used to match a request with its responses.
    uuid: uuid.UUID | None = None


@dataclasses.dataclass
class Response:
    """Response is a message sent by actor when processing a request."""

    # uuid is used to match a request with its responses.
    uuid: uuid.UUID | None = None

    def result(self, timeout: float | None = None) -> Any:
        """It returns the result of the request or raises an exception.
        :param timeout: timeout in seconds to wait for the response (in case it is not ready)
        :return:
        """
        return None


class Started(Response):
    """Started is a Response send by actor just before starting processing request."""


@dataclasses.dataclass
class Result(Response):
    """Result is a Response send by the actor to deliver a result to the requester."""

    # res is the result value carried by this response
    res: Any = None

    def result(self, timeout: float | None = None) -> Any:
        """It returns the carried result."""
        return self.res


@dataclasses.dataclass
class Error(Response):
    """Error is a Response send by the actor to raise an exception in the requester."""

    # err is the exception value carried by this response
    err: Exception | None = None

    def result(self, timeout: float | None = None) -> None:
        """It raises the carried error."""
        if self.err:
            raise self.err


class Done(Result):
    """Done is a Response send by the actor to just after processing request."""
