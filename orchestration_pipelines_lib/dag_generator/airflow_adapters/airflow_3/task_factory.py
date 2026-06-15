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
"""Module to convert actions into Airflow 3 specific code."""

from __future__ import annotations

import json
import logging
import re
from typing import TYPE_CHECKING, Any, Dict, Optional

from orchestration_pipelines_lib.dag_generator.airflow_adapters.common_utils import (
    task_utils,
    utils,
)
from orchestration_pipelines_lib.scripts.dbt_wrapper import invoke_dbt_run
from orchestration_pipelines_lib.utils.duration_utils import (
    duration_to_timedelta,
)

if TYPE_CHECKING:
    from airflow.providers.standard.operators.python import (
        PythonOperator,
        PythonVirtualenvOperator,
    )
    from airflow.providers.standard.operators.trigger_dagrun import (
        TriggerDagRunOperator,
    )


def _resolve_latest_pipeline_dag_id(
    current_dag_id: str,
    target_pipeline_id: str,
    bundle_id: Optional[str] = None,
) -> str:
    """Resolves the trigger DAG ID against the latest bundle version.

    If the current DAG ID is versioned, this reads the bundle manifest and
    resolves the bundle's current default version. If that fails, the raw
    target pipeline ID is returned as a safe fallback.

    For Airflow 3, uses the Airflow client API to find the latest version tagged
    as current for the target pipeline.

    If an explicit bundle_id is provided, it is used directly; otherwise the
    bundle ID is derived from the current DAG ID.
    """
    if not bundle_id:
        return target_pipeline_id

    try:
        from . import airflow_client_utils
        import airflow_client.client
        from airflow_client.client.rest import ApiException

        # Get the Airflow API client
        api_client = airflow_client_utils.get_airflow_api_client()
        dag_api = airflow_client.client.DAGApi(api_client)

        # Query the Airflow API for DAGs tagged as current for the specific
        # bundle and pipeline
        response = dag_api.get_dags(
            tags=[
                "op:is_current",
                f"op:bundle:{bundle_id}",
                f"op:pipeline:{target_pipeline_id}",
            ],
            tags_match_mode="all",
        )

        # Return the first matching DAG ID (should only be one marked as current)
        if response.dags and len(response.dags) > 0:
            return response.dags[0].dag_id
        else:
            logging.warning(
                f"No DAG found with pipeline ID '{target_pipeline_id}'. "
                f"Falling back to target pipeline ID."
            )
            return target_pipeline_id
    except Exception as e:
        logging.error(
            f"Error resolving latest bundle version for bundle '{bundle_id}' "
            f"and pipeline '{target_pipeline_id}': {e}. "
        )
        raise


def create_python_script_task(
    action: Dict[str, Any], _: Dict[str, Any], dag
) -> PythonOperator:
    """Converts an action into a PythonOperator."""
    from airflow.providers.standard.operators.python import PythonOperator

    try:
        callable_path = action.filename
        entrypoint = action.config.pythonCallable
        user_kwargs = action.config.opKwargs or {}

        def runtime_wrapper(**kwargs):
            python_callable = utils.import_callable(callable_path, entrypoint)
            filtered_kwargs = {
                k: v for k, v in kwargs.items() if k in user_kwargs
            }
            return python_callable(**filtered_kwargs)

        return PythonOperator(
            task_id=action.name,
            python_callable=runtime_wrapper,
            op_kwargs=action.config.opKwargs or {},
            execution_timeout=(
                duration_to_timedelta(action.executionTimeout)
                if action.executionTimeout
                else None
            ),
            trigger_rule=action.triggerRule,
            doc_md=json.dumps({"op_action_name": action.name}),
            dag=dag,
        )
    except Exception as e:
        logging.error(
            f"Error creating task for action '{action.name}'"
            f" from '{action.config.pythonCallable}': {e}"
        )
        raise


def create_python_virtualenv_task(
    action: Dict[str, Any], _: Dict[str, Any], dag
) -> PythonVirtualenvOperator:
    """Converts an action into a PythonVirtualenvOperator."""
    from airflow.providers.standard.operators.python import (
        PythonVirtualenvOperator,
    )

    try:
        callable_path = action.filename
        entrypoint = action.config.pythonCallable
        python_callable = utils.import_callable(callable_path, entrypoint)
        if not callable(python_callable):
            raise ValueError(
                f"Action {action.name}: filename {callable_path} with "
                f"callable {entrypoint} did not resolve to a callable object."
            )

        requirements = (
            action.config.requirementsPath
            if action.config.requirementsPath
            else action.config.requirements
        )

        return PythonVirtualenvOperator(
            task_id=action.name,
            python_callable=python_callable,
            op_kwargs=action.config.opKwargs or {},
            requirements=requirements,
            system_site_packages=action.config.systemSitePackages,
            execution_timeout=(
                duration_to_timedelta(action.executionTimeout)
                if action.executionTimeout
                else None
            ),
            trigger_rule=action.triggerRule,
            doc_md=json.dumps({"op_action_name": action.name}),
            dag=dag,
        )
    except Exception as e:
        logging.error(
            f"Error creating task for action '{action.name}' "
            f"from '{action.config.pythonCallable}': {e}"
        )
        raise


def create_bq_operation_task(
    action: Dict[str, Any], pipeline: Dict[str, Any], dag
):
    """Converts action to SQL job running on BigQuery or Dataproc."""
    return task_utils.create_bq_operation_task(action, pipeline, dag=dag)


def create_schedule_trigger_task(dag_kwargs, schedule_trigger):
    """Converts trigger config into params for Airflow pipeline."""
    return task_utils.create_schedule_trigger_task(dag_kwargs, schedule_trigger)


def create_dataproc_operator_task(
    action: Dict[str, Any], pipeline: Dict[str, Any], dag
):
    """Converts an action into a Dataproc Operator."""
    return task_utils.create_dataproc_operator_task(action, pipeline, dag=dag)


def create_dbt_task(
    action: Dict[str, Any], _: Dict[str, Any], dag
) -> PythonOperator:
    """Converts an action into a PythonOperator for dbt."""
    from airflow.providers.standard.operators.python import PythonOperator

    try:
        op_kwargs = {
            "project_dir": action.source.path,
            "profiles_dir": action.source.path,
        }
        if action.select_models:
            op_kwargs["select_models"] = action.select_models
        if action.params:
            op_kwargs["params"] = action.params

        return PythonOperator(
            task_id=action.name,
            python_callable=invoke_dbt_run,
            op_kwargs=op_kwargs,
            execution_timeout=(
                duration_to_timedelta(action.executionTimeout)
                if action.executionTimeout
                else None
            ),
            trigger_rule=action.triggerRule,
            doc_md=json.dumps({"op_action_name": action.name}),
            dag=dag,
        )
    except Exception as e:
        logging.error(f"Error creating task for action '{action.name}': {e}")
        raise


def create_dataform_task(action: Dict[str, Any], pipeline: Dict[str, Any], dag):
    """Converts an action into a Dataform operator.

    Depending on the execution mode, it either runs a local
    KubernetesPodOperator or invokes the Dataform service operator.
    """
    from airflow.sdk import Variable

    if action.executionMode == "local":
        # Allow overriding with an Airflow variable for flexibility
        gcs_bucket_path = Variable.get(
            "dataform_gcs_path", action.dataform_project_path
        )
        return task_utils.create_local_dataform_task(
            action, pipeline, gcs_bucket_path, dag=dag
        )
    else:
        return task_utils.create_service_dataform_task(
            action, pipeline, dag=dag
        )


def create_bq_dts_task(action: Dict[str, Any], pipeline: Dict[str, Any], dag):
    """Converts action to BigQuery DTS task group."""
    return task_utils.create_bq_dts_task(action, pipeline, dag=dag)


def create_orchestration_pipeline_trigger_task(
    action: Dict[str, Any], pipeline: Dict[str, Any], dag
) -> TriggerDagRunOperator:
    """Converts an action into a TriggerDagRunOperator."""
    from airflow.providers.standard.operators.trigger_dagrun import (
        TriggerDagRunOperator,
    )

    try:
        wait_for_completion = action.wait_for_completion or False

        return TriggerDagRunOperator(
            task_id=action.name,
            trigger_dag_id="{{ params.resolve_latest_pipeline_dag_id(params.current_dag_id, params.target_pipeline_id, params.bundle_id) }}",
            params={
                "resolve_latest_pipeline_dag_id": _resolve_latest_pipeline_dag_id,
                "current_dag_id": dag.dag_id,
                "target_pipeline_id": action.pipeline_id,
                "bundle_id": action.bundle_id,
            },
            wait_for_completion=wait_for_completion,
            execution_timeout=(
                duration_to_timedelta(action.executionTimeout)
                if action.executionTimeout
                else None
            ),
            trigger_rule=action.triggerRule,
            doc_md=json.dumps({"op_action_name": action.name}),
            dag=dag,
        )
    except Exception as e:
        logging.error(f"Error creating task for action '{action.name}': {e}")
        raise
