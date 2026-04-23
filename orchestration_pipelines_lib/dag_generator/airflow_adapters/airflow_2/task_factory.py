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
"""Module to convert actions into Airflow 2 specific code."""

from __future__ import annotations
from typing import Any, Dict
import json
from orchestration_pipelines_lib.dag_generator.airflow_adapters.common_utils import utils
from orchestration_pipelines_lib.dag_generator.airflow_adapters.common_utils import task_utils
from orchestration_pipelines_lib.scripts.dbt_wrapper import invoke_dbt_run
from orchestration_pipelines_lib.utils.duration_utils import duration_to_timedelta


def create_python_script_task(action: Dict[str, Any], _: Dict[str, Any],
                              dag) -> PythonOperator:
    """Converts an action into a PythonOperator."""
    from airflow.operators.python import PythonOperator
    try:
        callable_path = action.filename
        entrypoint = action.config.pythonCallable
        user_kwargs = action.config.opKwargs or {}
        def runtime_wrapper(**kwargs):
            from orchestration_pipelines_lib.dag_generator.airflow_adapters.common_utils import utils
            python_callable = utils.import_callable(callable_path, entrypoint)
            filtered_kwargs = {k: v for k, v in kwargs.items() if k in user_kwargs}
            return python_callable(**filtered_kwargs)

        return PythonOperator(
            task_id=action.name,
            python_callable=runtime_wrapper,
            op_kwargs=action.config.opKwargs or {},
            execution_timeout=duration_to_timedelta(action.executionTimeout)
            if action.executionTimeout else None,
            doc_md=json.dumps({"op_action_name": action.name}),
            dag=dag,
        )
    except Exception as e:
        print(f"Error creating task for action '{action.name}'"
              f" from '{action.config.pythonCallable}': {e}")
        raise


def create_python_virtualenv_task(action: Dict[str, Any], _: Dict[str, Any],
                                  dag) -> PythonVirtualenvOperator:
    """Converts an action into a PythonVirtualenvOperator."""
    from airflow.operators.python import PythonVirtualenvOperator
    try:
        callable_path = action.filename
        entrypoint = action.config.pythonCallable
        python_callable = utils.import_callable(callable_path, entrypoint)
        if not callable(python_callable):
            raise ValueError(
                f"Action {action.name}: filename {callable_path} with "
                f"callable {entrypoint} did not resolve to a callable object.")

        requirements = (action.config.requirementsPath
                        if action.config.requirementsPath else
                        action.config.requirements)

        return PythonVirtualenvOperator(
            task_id=action.name,
            python_callable=python_callable,
            op_kwargs=action.config.opKwargs or {},
            requirements=requirements,
            system_site_packages=action.config.systemSitePackages,
            execution_timeout=duration_to_timedelta(action.executionTimeout)
            if action.executionTimeout else None,
            doc_md=json.dumps({"op_action_name": action.name}),
            dag=dag,
        )
    except Exception as e:
        print(f"Error creating task for action '{action.name}' "
              f"from '{action.config.pythonCallable}': {e}")
        raise


def create_bq_operation_task(action: Dict[str, Any], pipeline: Dict[str, Any],
                             dag):
    """Converts an action into a SQL job running either on Bigquery directly or via Dataproc."""
    return task_utils.create_bq_operation_task(action, pipeline, dag=dag)


def create_schedule_trigger_task(dag_kwargs, schedule_trigger):
    """Converts the input trigger config into params for the Airflow pipeline."""
    return task_utils.create_schedule_trigger_task(dag_kwargs,
                                                   schedule_trigger)


def create_dataproc_operator_task(action: Dict[str, Any],
                                  pipeline: Dict[str, Any], dag):
    """Converts an action into a Dataproc Operator."""
    return task_utils.create_dataproc_operator_task(action, pipeline, dag=dag)


def create_dbt_task(action: Dict[str, Any], _: Dict[str, Any],
                    dag) -> PythonOperator:
    """Converts an action into a PythonOperator for dbt."""
    from airflow.operators.python import PythonOperator
    try:
        op_kwargs = {
            "project_dir": action.source.path,
            "profiles_dir": action.source.path,
        }
        if action.select_models:
            op_kwargs["select_models"] = action.select_models

        return PythonOperator(
            task_id=action.name,
            python_callable=invoke_dbt_run,
            op_kwargs=op_kwargs,
            execution_timeout=duration_to_timedelta(action.executionTimeout)
            if action.executionTimeout else None,
            doc_md=json.dumps({"op_action_name": action.name}),
            dag=dag)
    except Exception as e:
        print(f"Error creating task for action '{action.name}': {e}")
        raise


def create_dataform_task(action: Dict[str, Any], pipeline: Dict[str, Any],
                         dag):
    """Converts an action into a Dataform operator.

    Depending on the execution mode, it either runs a local KubernetesPodOperator
    or invokes the Dataform service workflow operator.
    """
    from airflow.models.variable import Variable
    if action.executionMode == "local":
        # Allow overriding with an Airflow variable for flexibility
        gcs_bucket_path = Variable.get("dataform_gcs_path",
                                       action.dataform_project_path)
        return task_utils.create_local_dataform_task(action,
                                                     pipeline,
                                                     gcs_bucket_path,
                                                     dag=dag)
    else:
        return task_utils.create_service_dataform_task(action,
                                                       pipeline,
                                                       dag=dag)
