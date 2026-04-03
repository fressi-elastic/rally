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

import errno
import functools
import json
import logging
import os
import random
import socket
import subprocess
import time
from collections.abc import Mapping

import pytest

from esrally import client, version
from esrally.utils import process

CONFIG_NAMES = ["in-memory-it", "es-it"]
DISTRIBUTION_8 = "8.19.13"
DISTRIBUTION_9 = "9.2.7"
DISTRIBUTIONS = [DISTRIBUTION_8, DISTRIBUTION_9]
TRACKS = ["geonames", "nyc_taxis", "http_logs", "nested"]
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


LOG = logging.getLogger(__name__)


def all_rally_configs(t):
    @functools.wraps(t)
    @pytest.mark.parametrize("cfg", CONFIG_NAMES)
    def wrapper(cfg, *args, **kwargs):
        t(cfg, *args, **kwargs)

    return wrapper


def random_rally_config(t):
    @functools.wraps(t)
    @pytest.mark.parametrize("cfg", [random.choice(CONFIG_NAMES)])
    def wrapper(cfg, *args, **kwargs):
        t(cfg, *args, **kwargs)

    return wrapper


def rally_in_mem(t):
    @functools.wraps(t)
    @pytest.mark.parametrize("cfg", ["in-memory-it"])
    def wrapper(cfg, *args, **kwargs):
        t(cfg, *args, **kwargs)

    return wrapper


def rally_es(t):
    @functools.wraps(t)
    @pytest.mark.parametrize("cfg", ["es-it"])
    def wrapper(cfg, *args, **kwargs):
        t(cfg, *args, **kwargs)

    return wrapper


def esrally_command_line_for(*, cmd: str, cfg: str | None) -> str:
    cmd = f"esrally {cmd}"
    if cfg:
        cmd += f" --configuration-name={cfg}"
    return cmd


def esrally(
    cfg: str | None, cmd: str, check: bool = True, env: Mapping[str, str] | None = None, want_output: str | None = None
) -> subprocess.CompletedProcess:
    """
    This method should be used for rally invocations of the all commands besides race.
    These commands may have different CLI options than race.
    """
    cmd = esrally_command_line_for(cmd=cmd, cfg=cfg)
    LOG.info("Running rally: %r", cmd)
    try:
        result = subprocess.run(cmd, shell=True, check=check, capture_output=True, text=True, env=env)
    except subprocess.CalledProcessError as err:
        output = "    ".join([""] + err.stdout.splitlines(keepends=True))
        pytest.fail("Failed running esrally:\n" f" - command line: {cmd}\n" f" - output: {output}\n")
        raise

    if want_output is not None:
        assert want_output in result.stdout

    return result


def race(cfg: str, command_line: str, enable_assertions: bool = True, check: bool = True, port: int | None = None) -> subprocess.CompletedProcess:
    """
    This method should be used for rally invocations of the race command.
    It sets up some defaults for how the integration tests expect to run races.
    """
    if port is not None:
        wait_until_port_is_free(port_number=port)

    race_command = f"race {command_line} --kill-running-processes --on-error='abort'"
    if enable_assertions:
        race_command += " --enable-assertions"
    try:
        return esrally(cfg, race_command, check=check)
    finally:
        if port is not None:
            wait_until_port_is_free(port_number=port)


def shell_cmd(command_line):
    """
    Executes a given command_line in a subshell.

    :param command_line: (str) The command to execute
    :return: (int) the exit code
    """

    return subprocess.call(command_line, shell=True)


def command_in_docker(command_line, python_version):
    docker_command = f"docker run --rm -v {ROOT_DIR}:/rally_ro:ro python:{python_version} bash -c '{command_line}'"
    return subprocess.run(docker_command, shell=True, check=True).returncode


def wait_until_port_is_free(port_number=39200, timeout=120):
    start = time.perf_counter()
    end = start + timeout
    while time.perf_counter() < end:
        c = socket.socket()
        connect_result = c.connect_ex(("127.0.0.1", port_number))
        # noinspection PyBroadException
        try:
            if connect_result == errno.ECONNREFUSED:
                c.close()
                return
            else:
                c.close()
                time.sleep(0.5)
        except Exception:
            pass

    raise TimeoutError(f"Port [{port_number}] is occupied after [{timeout}] seconds")


class TestCluster:

    DEFAULT_DISTRIBUTION = DISTRIBUTION_9
    DEFAULT_HTTP_PORT = 19200
    DEFAULT_CAR = "defaults,basic-license"
    DEFAULT_NODE_NAME = "it-cluster"

    def __init__(
        self,
        cfg: str,
        node_name: str | None = None,
        http_port: int | None = None,
        car: str | None = None,
        distribution_version: str | None = None,
    ):
        self.cfg = cfg
        self.installation_id: str | None = None
        self.node_name = node_name or self.DEFAULT_NODE_NAME
        self.http_port = http_port or self.DEFAULT_HTTP_PORT
        self.car = car or self.DEFAULT_CAR
        self.distribution_version = distribution_version or self.DEFAULT_DISTRIBUTION

    def install(
        self, *, node_name: str | None = None, http_port: int | None = None, car: str | None = None, distribution_version: str | None = None
    ):
        self.http_port = http_port or self.http_port
        self.car = car or self.car
        self.distribution_version = distribution_version or self.distribution_version
        self.node_name = node_name or self.node_name
        transport_port = self.http_port + 100
        result = esrally(
            self.cfg,
            f"install --configuration-name={self.cfg} --quiet --distribution-version={self.distribution_version} "
            f"--build-type=tar --http-port={self.http_port} --node={node_name} --master-nodes={node_name} --car={self.car} "
            f'--seed-hosts="127.0.0.1:{transport_port}"',
        )
        self.installation_id = json.loads(result.stdout)["installation-id"]

    def start(self, race_id: str):
        esrally(self.cfg, f'start --runtime-jdk="bundled" --installation-id={self.installation_id} --race-id={race_id}')
        es = client.EsClientFactory(hosts=[{"host": "127.0.0.1", "port": self.http_port}], client_options={}).create()
        client.wait_for_rest_layer(es)
        assert es.info()["cluster_name"] == self.cfg

    def stop(self):
        if self.installation_id:
            esrally(self.cfg, f"stop --installation-id={self.installation_id}")

    def __str__(self):
        return f"TestCluster[installation-id={self.installation_id}]"


def find_log_line(log_file, text) -> str | None:
    with open(log_file) as f:
        for line in f:
            if text in line:
                return line
    return None
