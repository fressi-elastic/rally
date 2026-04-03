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
import dataclasses
import json

import pytest

import it
from esrally.utils import cases


@dataclasses.dataclass
class SourceCase:
    revision: str | None = None
    elasticsearch_plugins: str | None = None
    pipeline: str | None = None
    source_build_method: str | None = None


@cases.cases(
    revision=SourceCase(revision="latest", elasticsearch_plugins="analysis-icu"),
    docker_build_method=SourceCase(revision="@2022-07-07", elasticsearch_plugins="analysis-icu", source_build_method="docker"),
    from_sources=SourceCase(pipeline="from-sources"),
    docker_from_sources=SourceCase(pipeline="from-sources", source_build_method="docker"),
)
def test_race_with_sources(cfg, case: SourceCase):
    port = 19200
    it.wait_until_port_is_free(port_number=port)
    cmd = f"--track=geonames --test-mode  --target-hosts=127.0.0.1:{port} --challenge=append-no-conflicts-index-only --car=4gheap,basic-license"
    if case.revision is not None:
        cmd += f" --revision={case.revision}"
    if case.elasticsearch_plugins is not None:
        cmd += f" --elasticsearch-plugins={case.elasticsearch_plugins}"
    if case.pipeline is not None:
        cmd += f" --pipeline={case.pipeline}"
    if case.source_build_method is not None:
        cmd += f" --source-build-method={case.source_build_method}"

    it.wait_until_port_is_free(port_number=port)
    it.race(cfg, cmd)


@dataclasses.dataclass
class BuildCase:
    target_arch: str
    target_os: str = "linux"
    source_build_method: str | None = None


@cases.cases(
    linux_amd64=BuildCase("x86_64"),
    linux_aarch64=BuildCase("aarch64"),
    docker_amd64=BuildCase("x86_64", source_build_method="docker"),
    docker_aarch64=BuildCase("aarch64", source_build_method="docker"),
)
def test_build(cfg: str, case: BuildCase):
    cmd = f"build --revision=latest --target-arch {case.target_arch} --target-os {case.target_os} "
    "--elasticsearch-plugins=analysis-icu --quiet"
    if case.source_build_method is not None:
        cmd += f" --source-build-method={case.source_build_method}"
    result = it.esrally(cfg, cmd)
    assert f"{case.target_os}-{case.target_arch}" in json.loads(result.stdout)["elasticsearch"]
