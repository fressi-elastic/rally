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

import logging
import threading
from datetime import datetime
from unittest import mock

import pytest

from esrally import config, metrics
from esrally.driver import driver


def _base_cfg():
    cfg = config.Config()
    cfg.add(config.Scope.application, "system", "env.name", "unittest")
    cfg.add(config.Scope.application, "system", "time.start", datetime(year=2017, month=8, day=20, hour=1, minute=0, second=0))
    cfg.add(config.Scope.application, "system", "race.id", "6ebc6e53-ee20-4b0c-99b4-09697987e9f4")
    return cfg


@pytest.mark.parametrize(
    "flag, datastore, expected",
    [
        (False, "elasticsearch", False),
        (False, "in-memory", False),
        (True, "in-memory", True),
        (True, "elasticsearch", True),
    ],
)
def test_coordinator_skips_raw_sample_postprocessing_conditions(flag, datastore, expected):
    cfg = _base_cfg()
    cfg.add(config.Scope.application, "driver", "distributed.request.metrics", flag)
    cfg.add(config.Scope.application, "reporting", "datastore.type", datastore)
    assert metrics.coordinator_skips_raw_sample_postprocessing(cfg) is expected


def test_update_samples_es_flag_skips_raw_samples_but_keeps_progress():
    cfg = _base_cfg()
    cfg.add(config.Scope.application, "driver", "distributed.request.metrics", True)
    cfg.add(config.Scope.application, "reporting", "datastore.type", "elasticsearch")

    d = driver.Driver(mock.Mock(), cfg, es_client_factory_class=mock.Mock)
    s0 = mock.Mock(client_id=0, percent_completed=0.25)
    s1 = mock.Mock(client_id=1, percent_completed=0.5)
    d.update_samples([s0, s1])
    assert d.raw_samples == []
    assert d.most_recent_sample_per_client == {0: s0, 1: s1}


def test_update_samples_in_memory_distributed_skips_raw_samples_but_keeps_progress():
    cfg = _base_cfg()
    cfg.add(config.Scope.application, "driver", "distributed.request.metrics", True)
    cfg.add(config.Scope.application, "reporting", "datastore.type", "in-memory")

    d = driver.Driver(mock.Mock(), cfg, es_client_factory_class=mock.Mock)
    s0 = mock.Mock(client_id=0, percent_completed=0.1)
    d.update_samples([s0])
    assert d.raw_samples == []
    assert d.most_recent_sample_per_client[0] is s0


def test_post_process_samples_es_flag_flushes_without_post_processor():
    cfg = _base_cfg()
    cfg.add(config.Scope.application, "driver", "distributed.request.metrics", True)
    cfg.add(config.Scope.application, "reporting", "datastore.type", "elasticsearch")

    d = driver.Driver(mock.Mock(), cfg, es_client_factory_class=mock.Mock)
    d.metrics_store = mock.Mock()
    d.metrics_store.opened = True
    pp = mock.Mock()
    d.sample_post_processor = pp
    d.raw_samples = [mock.Mock()]

    d.post_process_samples()

    pp.assert_not_called()
    d.metrics_store.flush.assert_called_once_with(refresh=False)
    assert d.raw_samples == []


def test_post_process_samples_in_memory_distributed_skips_post_processor():
    cfg = _base_cfg()
    cfg.add(config.Scope.application, "driver", "distributed.request.metrics", True)
    cfg.add(config.Scope.application, "reporting", "datastore.type", "in-memory")

    d = driver.Driver(mock.Mock(), cfg, es_client_factory_class=mock.Mock)
    d.metrics_store = mock.Mock()
    d.metrics_store.opened = True
    pp = mock.Mock()
    d.sample_post_processor = pp
    d.raw_samples = [mock.Mock()]

    d.post_process_samples()

    pp.assert_not_called()
    d.metrics_store.flush.assert_called_once_with(refresh=False)
    assert d.raw_samples == []


def test_post_process_samples_in_memory_invokes_post_processor():
    cfg = _base_cfg()
    cfg.add(config.Scope.application, "driver", "distributed.request.metrics", False)
    cfg.add(config.Scope.application, "reporting", "datastore.type", "in-memory")

    d = driver.Driver(mock.Mock(), cfg, es_client_factory_class=mock.Mock)
    d.metrics_store = mock.Mock()
    d.metrics_store.opened = True
    pp = mock.Mock()
    d.sample_post_processor = pp
    s = mock.Mock()
    d.raw_samples = [s]

    d.post_process_samples()

    pp.assert_called_once_with([s])
    d.metrics_store.flush.assert_not_called()


def test_worker_join_point_flushes_worker_elasticsearch_metrics_store():
    cfg = _base_cfg()
    ca = driver.ClientAllocations()
    ca.add(0, [driver.JoinPoint(id=0)])

    w = driver.Worker.__new__(driver.Worker)
    w.logger = logging.getLogger("esrally.tests.worker_join_flush")
    w.driver_actor = mock.Mock()
    w.config = cfg
    w.worker_id = 0
    w.current_task_index = 0
    w.next_task_index = 0
    w.client_allocations = ca
    w.executor_future = mock.Mock()
    w.cancel = threading.Event()
    w.complete = threading.Event()
    w.sampler = None
    w.send = mock.Mock()
    store = mock.Mock()
    store.opened = True
    w._worker_elasticsearch_metrics_store = store  # pylint: disable=protected-access

    driver.Worker.drive(w)

    store.flush.assert_called_once_with(refresh=False)
    w.send.assert_called_once()
