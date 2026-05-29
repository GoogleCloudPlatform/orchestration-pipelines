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

from typing import Any, Dict

from orchestration_pipelines_lib.internal_models.actions import (
    BqOperationActionModel,
    DataformActionModel,
    DataIngestionActionModel,
    DataprocOperatorActionModel,
    DBTActionModel,
    OrchestrationPipelineActionModel,
    PythonScriptActionModel,
    PythonVirtualenvActionModel,
)


def get_action_handlers(task_factory) -> Dict[Any, Any]:
    """Returns a static mapping of action models to task factory methods.

    Args:
        task_factory: The task factory instance to use for creating tasks.

    Returns:
        A dictionary mapping internal action models to task factory methods.
    """
    return {
        PythonScriptActionModel: task_factory.create_python_script_task,
        PythonVirtualenvActionModel: task_factory.create_python_virtualenv_task,
        BqOperationActionModel: task_factory.create_bq_operation_task,
        DataprocOperatorActionModel: task_factory.create_dataproc_operator_task,
        DBTActionModel: task_factory.create_dbt_task,
        DataformActionModel: task_factory.create_dataform_task,
        DataIngestionActionModel: task_factory.create_bq_dts_task,
        OrchestrationPipelineActionModel: task_factory.create_orchestration_pipeline_trigger_task,
    }
