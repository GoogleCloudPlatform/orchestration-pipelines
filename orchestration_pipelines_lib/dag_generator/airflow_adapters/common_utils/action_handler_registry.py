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
"""Module for action handler registry."""

from collections.abc import Callable
from dataclasses import dataclass
from functools import partial
from typing import Any

from orchestration_pipelines_lib.dag_generator.airflow_adapters.common_utils import (  # noqa: E501
    task_utils,
)
from orchestration_pipelines_lib.internal_models.actions import (
    AIActionModel,
    AirflowActionModel,
    BqOperationActionModel,
    DataformActionModel,
    DataIngestionActionModel,
    DataprocOperatorActionModel,
    DBTActionModel,
    OrchestrationPipelineActionModel,
    PythonScriptActionModel,
    PythonVirtualenvActionModel,
)


@dataclass(frozen=True, slots=True)
class AdapterImports:
    """Airflow adapter-specific imports to use for creating tasks."""

    get_python_operator: Callable[[], type]
    get_python_virtualenv_operator: Callable[[], type]
    get_trigger_dagrun_operator: Callable[[], type]
    get_variable_class: Callable[[], type]


def get_action_handlers(
    adapter_imports: AdapterImports,
) -> dict[type, Callable[[Any, Any, Any], Any]]:
    """Returns a static mapping of action models to task factory methods.

    Args:
        adapter_imports: The adapter imports to use for creating tasks.

    Returns:
        A dictionary mapping internal action models to task factory methods.
    """
    return {
        PythonScriptActionModel: partial(
            task_utils.create_python_script_task,
            adapter_imports.get_python_operator,
        ),
        PythonVirtualenvActionModel: partial(
            task_utils.create_python_virtualenv_task,
            adapter_imports.get_python_virtualenv_operator,
        ),
        BqOperationActionModel: task_utils.create_bq_operation_task,
        DataprocOperatorActionModel: task_utils.create_dataproc_operator_task,
        DBTActionModel: partial(
            task_utils.create_dbt_task,
            adapter_imports.get_python_operator,
        ),
        DataformActionModel: partial(
            task_utils.create_dataform_task,
            adapter_imports.get_variable_class,
        ),
        DataIngestionActionModel: task_utils.create_bq_dts_task,
        OrchestrationPipelineActionModel: partial(
            task_utils.create_orchestration_pipeline_trigger_task,
            adapter_imports.get_trigger_dagrun_operator,
        ),
        AIActionModel: task_utils.create_ai_task,
        AirflowActionModel: task_utils.create_airflow_task,
    }
