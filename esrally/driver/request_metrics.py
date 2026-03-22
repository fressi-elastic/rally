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

from collections.abc import Mapping, Sequence
from typing import Any

from esrally import metrics
from esrally.utils import convert


def merge(*args: Mapping[str, Any] | None) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for arg in args:
        if arg is not None:
            result.update(arg)
    return result


class RequestMetricsRecorder:
    def __init__(
        self,
        metrics_store: metrics.MetricsStore,
        downsample_factor: int,
        track_meta_data: Mapping[str, Any] | None,
        challenge_meta_data: Mapping[str, Any] | None,
    ) -> None:
        self.metrics_store = metrics_store
        self.downsample_factor = downsample_factor
        self.track_meta_data = track_meta_data
        self.challenge_meta_data = challenge_meta_data

    def __call__(self, raw_samples: Sequence[Any]) -> int:
        final_sample_count = 0
        for idx, sample in enumerate(raw_samples):
            if idx % self.downsample_factor == 0:
                final_sample_count += 1
                client_id_meta_data = {"client_id": sample.client_id}
                meta_data = merge(
                    self.track_meta_data,
                    self.challenge_meta_data,
                    sample.operation_meta_data,
                    sample.task.meta_data,
                    sample.request_meta_data,
                    client_id_meta_data,
                )

                self.metrics_store.put_value_cluster_level(
                    name="latency",
                    value=convert.seconds_to_ms(sample.latency),
                    unit="ms",
                    task=sample.task.name,
                    operation=sample.operation_name,
                    operation_type=sample.operation_type,
                    sample_type=sample.sample_type,
                    absolute_time=sample.absolute_time,
                    relative_time=sample.relative_time,
                    meta_data=meta_data,
                )

                self.metrics_store.put_value_cluster_level(
                    name="service_time",
                    value=convert.seconds_to_ms(sample.service_time),
                    unit="ms",
                    task=sample.task.name,
                    operation=sample.operation_name,
                    operation_type=sample.operation_type,
                    sample_type=sample.sample_type,
                    absolute_time=sample.absolute_time,
                    relative_time=sample.relative_time,
                    meta_data=meta_data,
                )

                self.metrics_store.put_value_cluster_level(
                    name="processing_time",
                    value=convert.seconds_to_ms(sample.processing_time),
                    unit="ms",
                    task=sample.task.name,
                    operation=sample.operation_name,
                    operation_type=sample.operation_type,
                    sample_type=sample.sample_type,
                    absolute_time=sample.absolute_time,
                    relative_time=sample.relative_time,
                    meta_data=meta_data,
                )

                for timing in sample.dependent_timings:
                    self.metrics_store.put_value_cluster_level(
                        name="service_time",
                        value=convert.seconds_to_ms(timing.service_time),
                        unit="ms",
                        task=timing.task.name,
                        operation=timing.operation_name,
                        operation_type=timing.operation_type,
                        sample_type=timing.sample_type,
                        absolute_time=timing.absolute_time,
                        relative_time=timing.relative_time,
                        meta_data=merge(timing.request_meta_data, client_id_meta_data),
                    )

        return final_sample_count
