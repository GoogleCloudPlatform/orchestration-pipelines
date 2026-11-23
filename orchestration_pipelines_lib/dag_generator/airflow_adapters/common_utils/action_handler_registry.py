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
    runtime_kwargs,
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
from orchestration_pipelines_lib.utils.runtime_resolver import (
    register_operator_kwargs_builder,
)


@dataclass(frozen=True, slots=True)
class AdapterImports:
    """Airflow adapter-specific imports to use for creating tasks."""

    get_python_operator: Callable[[], type]
    get_python_virtualenv_operator: Callable[[], type]
    get_trigger_dagrun_operator: Callable[[], type]
    get_variable_class: Callable[[], type]
    get_param_class: Callable[[], type]


def get_action_handlers(
    adapter_imports: AdapterImports,
) -> dict[type, Callable[[Any, Any, Any], Any]]:
    """Returns a static mapping of action models to task factory methods.

    Args:
        adapter_imports: The adapter imports to use for creating tasks.

    Returns:
        A dictionary mapping internal action models to task factory methods.
    """
    register_operator_kwargs_builder(build_runtime_operator_kwargs)
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


RuntimeKwargsHandler = Callable[
    [Any, Any, runtime_kwargs.RuntimeKwargsContext], dict[str, Any]
]

RUNTIME_KWARGS_HANDLERS: dict[type, RuntimeKwargsHandler] = {
    PythonScriptActionModel: runtime_kwargs.python_kwargs,
    PythonVirtualenvActionModel: runtime_kwargs.python_kwargs,
    BqOperationActionModel: runtime_kwargs.bq_operation_kwargs,
    DataprocOperatorActionModel: runtime_kwargs.dataproc_kwargs,
    DBTActionModel: runtime_kwargs.dbt_kwargs,
    DataformActionModel: runtime_kwargs.dataform_kwargs,
    DataIngestionActionModel: runtime_kwargs.bq_dts_kwargs,
    OrchestrationPipelineActionModel: runtime_kwargs.trigger_pipeline_kwargs,
    AIActionModel: runtime_kwargs.ai_kwargs,
    AirflowActionModel: runtime_kwargs.airflow_task_kwargs,
}


def build_runtime_operator_kwargs(
    op: Any,
    action: Any,
    defaults: Any,
    *,
    base_action: Any,
    task_role: str,
    action_paths: set[str],
    is_resolved: bool,
    run_id: str,
    **_: Any,
) -> dict[str, Any]:
    """Dispatches `action` to its registered runtime operator kwargs builder."""
    kw = runtime_kwargs.policy_kwargs(action, defaults, task_role)
    if handler := RUNTIME_KWARGS_HANDLERS.get(type(action)):
        ctx = runtime_kwargs.RuntimeKwargsContext(
            op=op,
            base_action=base_action,
            task_role=task_role,
            action_paths=action_paths,
            is_resolved=is_resolved,
            run_id=run_id,
        )
        kw.update(handler(action, defaults, ctx))
    return kw
