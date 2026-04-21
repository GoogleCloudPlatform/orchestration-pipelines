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
from orchestration_pipelines_lib.internal_models import actions


def get_action_handlers(task_factory) -> Dict[Any, Any]:
    """Returns a static mapping of internal action models to their Airflow task factory methods.

    Args:
        task_factory: The task factory instance to use for creating tasks.

    Returns:
        A dictionary mapping internal action models to task factory methods.
    """
    return {
        actions.PythonScriptActionModel:
        task_factory.create_python_script_task,
        actions.PythonVirtualenvActionModel:
        task_factory.create_python_virtualenv_task,
        actions.BqOperationActionModel: task_factory.create_bq_operation_task,
        actions.DataprocOperatorActionModel:
        task_factory.create_dataproc_operator_task,
        actions.DBTActionModel: task_factory.create_dbt_task,
        actions.DataformActionModel: task_factory.create_dataform_task,
    }
