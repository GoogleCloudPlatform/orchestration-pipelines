# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""Module to validate and build pipeline from yaml to actual airflow code."""

from typing import Any
from importlib.metadata import version
from orchestration_pipelines_lib.dag_generator.airflow_adapters.adapter_factory import (
    get_adapter, )

AIRFLOW_VERSION = version("apache-airflow").split("+")[0].replace(".", "_")


def generate(pipeline, tags: list[str], dag_notes: str, data_root: str) -> Any:
    """Generates the pipeline DAG.

    It uses an adapter factory to load the appropriate Airflow version adapter
    and calls its generate method.

    Args:
        pipeline: The parsed pipeline definition.
        tags: A list of tags to apply to the generated DAG.
        dag_notes: Notes or documentation for the DAG.
        data_root: The root directory for the pipeline data.

    Returns:
        The generated DAG object.
    """
    adapter = get_adapter(AIRFLOW_VERSION)
    return adapter.generate(pipeline, tags, dag_notes, data_root)


def get_actively_running_versions(pipeline_id, bundle_id) -> list[str]:
    """Retrieves the actively running versions of a pipeline.

    Args:
        pipeline_id: The ID of the pipeline.
        bundle_id: The ID of the bundle.

    Returns:
        A list of actively running pipeline versions.
    """
    adapter = get_adapter(AIRFLOW_VERSION)
    return adapter.get_actively_running_versions(pipeline_id, bundle_id)


def get_previous_default_versions(pipeline_id, bundle_id) -> list[str]:
    """Retrieves the previous default versions of a pipeline.

    Args:
        pipeline_id: The ID of the pipeline.
        bundle_id: The ID of the bundle.

    Returns:
        A list of previous default pipeline versions.
    """
    adapter = get_adapter(AIRFLOW_VERSION)
    return adapter.get_previous_default_versions(pipeline_id, bundle_id)
