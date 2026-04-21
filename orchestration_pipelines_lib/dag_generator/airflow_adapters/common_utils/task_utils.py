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
"""Module with common conversion methods from action into Airflow code."""
from typing import Any, Dict
from datetime import datetime
import json
import pytz
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.task_group import TaskGroup
from orchestration_pipelines_lib.dag_generator.airflow_adapters.common_utils import dataproc_utils
from orchestration_pipelines_lib.dag_generator.airflow_adapters.common_utils import gcs_utils
from orchestration_pipelines_lib.utils.duration_utils import duration_to_timedelta
from orchestration_pipelines_lib.utils.file_manager import FileManager


def create_dataproc_create_batch_operator_task(action: Dict[str, Any],
                                               pipeline: Dict[str, Any], dag):
    """Converts an action into a DataprocCreateBatchOperator.

    Args:
        action: The action configuration object.
        pipeline: The pipeline configuration object.
        dag: The Airflow DAG object.

    Returns:
        An instance of DataprocCreateBatchOperator.
    """
    from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
    from google.cloud import dataproc_v1
    import uuid

    try:
        job_specific_config = {}
        if action.type in ("pyspark", "notebook"):
            wrapper_uri = gcs_utils.get_run_notebook_gcs_path()
            gcs_utils.upload_run_notebook_if_needed(wrapper_uri)
            job_specific_config[
                "pyspark_batch"] = dataproc_utils.get_pyspark_batch_config(
                    action, wrapper_uri)
        elif action.type == "sql":
            spark_sql_batch = {}
            if action.query:
                spark_sql_batch["query_list"] = {"queries": [action.query]}
            elif action.filename:
                spark_sql_batch["query_file_uri"] = action.filename
            job_specific_config["spark_sql_batch"] = spark_sql_batch

        runtime_config = action.config.resourceProfile.runtimeConfig or {}
        environment_config = action.config.resourceProfile.environmentConfig or {}

        if action.type in ("pyspark", "notebook"):
            deps_bucket = action.depsBucket or ""
            execution_config = environment_config.setdefault(
                "execution_config", {})
            if execution_config.get("staging_bucket") is None and deps_bucket:
                execution_config["staging_bucket"] = deps_bucket

        batch = dataproc_v1.types.Batch(**job_specific_config,
                                        runtime_config=runtime_config,
                                        environment_config=environment_config,
                                        labels=action.labels)
        return DataprocCreateBatchOperator(
            task_id=action.name,
            region=action.region,
            project_id=pipeline.defaults.cloudDefault.project,
            batch=batch,
            batch_id=
            f"{action.name.lower().lstrip('_-').replace('_', '-')[:50]}-{uuid.uuid4().hex[:6]}",
            execution_timeout=(duration_to_timedelta(action.executionTimeout)
                               if action.executionTimeout else None),
            impersonation_chain=action.impersonationChain,
            doc_md=json.dumps({"op_action_name": action.name}),
            dag=dag,
        )
    except Exception as e:
        print(f"Error creating task for action '{action.name}': {e}")
        raise


def create_bq_operation_task(action: Dict[str, Any], pipeline: Dict[str, Any],
                             dag):
    """Converts an action into a BigQueryInsertJobOperator.

    Args:
        action: The action configuration object.
        pipeline: The pipeline configuration object.
        dag: The Airflow DAG object.

    Returns:
        An instance of BigQueryInsertJobOperator.
    """
    from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

    try:
        if action.filename:
            file_manager = FileManager()
            query = file_manager.read(action.filename)
        else:
            query = action.query

        configuration = {
            "query": {
                "query": query,
                "useLegacySql": False,
            },
            "labels": action.labels
        }
        if not query.strip().upper().startswith("CREATE"):
            configuration["query"]["writeDisposition"] = "WRITE_TRUNCATE"
            configuration["query"]["createDisposition"] = "CREATE_IF_NEEDED"

        if action.config.destinationTable:
            parts = action.config.destinationTable.split(".")
            if len(parts) != 3:
                raise ValueError(
                    "destinationTable should be in the format 'project.dataset.table'"
                )
            configuration["query"]["destinationTable"] = {
                "projectId": parts[0],
                "datasetId": parts[1],
                "tableId": parts[2],
            }

        return BigQueryInsertJobOperator(
            task_id=action.name,
            location=action.config.location,
            project_id=pipeline.defaults.cloudDefault.project,
            configuration=configuration,
            execution_timeout=duration_to_timedelta(action.executionTimeout)
            if action.executionTimeout else None,
            gcp_conn_id="google_cloud_default",
            impersonation_chain=action.impersonationChain,
            doc_md=json.dumps({"op_action_name": action.name}),
            dag=dag,
        )
    except Exception as e:
        print(f"Error creating task for action '{action.name}': {e}")
        raise


def dataproc_ephemeral_task(action: Dict[str, Any], dag) -> TaskGroup:
    """Converts an action into a TaskGroup for an ephemeral Dataproc workflow.

    Args:
        action: The action configuration object.
        dag: The Airflow DAG object.

    Returns:
        An Airflow TaskGroup containing cluster creation, job submission, and deletion tasks.
    """
    from airflow.providers.google.cloud.operators.dataproc import (
        DataprocCreateClusterOperator, DataprocSubmitJobOperator,
        DataprocDeleteClusterOperator)

    try:
        with TaskGroup(group_id=action.name, dag=dag) as task_group:
            cluster_config = action.config.cluster_config
            if action.depsBucket:
                cluster_config["config_bucket"] = action.depsBucket

            create_cluster = DataprocCreateClusterOperator(
                task_id=f"{action.name}_create_cluster",
                project_id=action.config.project_id,
                cluster_config=cluster_config,
                region=action.config.region,
                cluster_name=action.config.cluster_name,
                impersonation_chain=action.impersonationChain,
                doc_md=json.dumps({"op_action_name": action.name}),
                labels=action.labels,
                dag=dag)

            job = {
                "placement": {
                    "cluster_name": action.config.cluster_name
                },
                "reference": {
                    "project_id": action.config.project_id,
                },
                "labels": action.labels
            }

            if action.type == "sql":
                spark_sql_job = {}
                if action.query:
                    spark_sql_job["query_list"] = {"queries": [action.query]}
                elif action.filename:
                    spark_sql_job["query_file_uri"] = action.filename
                if action.config.properties:
                    spark_sql_job["properties"] = action.config.properties
                job["spark_sql_job"] = spark_sql_job
            else:
                wrapper_uri = gcs_utils.get_run_notebook_gcs_path()
                gcs_utils.upload_run_notebook_if_needed(wrapper_uri)
                pyspark_job = dataproc_utils.get_pyspark_batch_config(
                    action, wrapper_uri)
                if action.pyFiles:
                    pyspark_job["python_file_uris"] = action.pyFiles
                pyspark_job["properties"] = action.config.properties
                job["pyspark_job"] = pyspark_job
            submit_job = DataprocSubmitJobOperator(
                task_id=f"{action.name}_submit_job",
                job=job,
                execution_timeout=duration_to_timedelta(
                    action.executionTimeout)
                if action.executionTimeout else None,
                region=action.config.region,
                project_id=action.config.project_id,
                impersonation_chain=action.impersonationChain,
                doc_md=json.dumps({"op_action_name": action.name}),
                dag=dag)

            delete_cluster = DataprocDeleteClusterOperator(
                task_id=f"{action.name}_delete_cluster",
                project_id=action.config.project_id,
                cluster_name=action.config.cluster_name,
                region=action.config.region,
                impersonation_chain=action.impersonationChain,
                doc_md=json.dumps({"op_action_name": action.name}),
                dag=dag,
                trigger_rule=TriggerRule.ALL_DONE)

            create_cluster >> submit_job >> delete_cluster
        return task_group
    except Exception as e:
        print(f"Error creating task for action '{action.name}': {e}")
        raise


def dataproc_existing_cluster(action: Dict[str, Any], pipeline: Dict[str, Any],
                              dag):
    """Converts an action into a DataprocSubmitJobOperator for an existing cluster.

    Args:
        action: The action configuration object.
        pipeline: The pipeline configuration object.
        dag: The Airflow DAG object.

    Returns:
        An instance of DataprocSubmitJobOperator.
    """
    from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator

    try:
        job = {
            "placement": {
                "cluster_name": action.config.cluster_name
            },
            "reference": {
                "project_id": action.config.project_id,
            },
            "labels": action.labels
        }

        if action.type == "sql":
            spark_sql_job = {}
            if action.query:
                spark_sql_job["query_list"] = {"queries": [action.query]}
            elif action.filename:
                spark_sql_job["query_file_uri"] = action.filename
            if action.config.properties:
                spark_sql_job["properties"] = action.config.properties
            job["spark_sql_job"] = spark_sql_job
        else:
            wrapper_uri = gcs_utils.get_run_notebook_gcs_path()
            gcs_utils.upload_run_notebook_if_needed(wrapper_uri)
            job["pyspark_job"] = dataproc_utils.get_pyspark_batch_config(
                action, wrapper_uri)
            if action.pyFiles:
                job["pyspark_job"]["python_file_uris"] = action.pyFiles
            job["pyspark_job"]["properties"] = action.config.properties

        return DataprocSubmitJobOperator(
            task_id=action.name,
            job=job,
            execution_timeout=duration_to_timedelta(action.executionTimeout)
            if action.executionTimeout else None,
            region=action.region,
            project_id=pipeline.defaults.cloudDefault.project,
            impersonation_chain=action.impersonationChain,
            doc_md=json.dumps({"op_action_name": action.name}),
            dag=dag)
    except Exception as e:
        print(f"Error creating task for action '{action.name}': {e}")
        raise


def create_schedule_trigger_task(dag_kwargs, schedule_trigger):
    """Converts the input trigger config into schedule parameters for the DAG.

    Args:
        dag_kwargs: A dictionary of DAG keyword arguments to update.
        schedule_trigger: The schedule trigger configuration object.
    """
    start_time = datetime.fromisoformat(schedule_trigger.startTime)
    end_time = datetime.fromisoformat(
        schedule_trigger.endTime) if schedule_trigger.endTime else None
    timezone = pytz.timezone(schedule_trigger.timezone)
    dag_kwargs["start_date"] = timezone.localize(start_time)
    dag_kwargs["end_date"] = timezone.localize(end_time) if end_time else None
    dag_kwargs["schedule"] = schedule_trigger.scheduleInterval
    dag_kwargs["catchup"] = schedule_trigger.catchup


def create_dataproc_operator_task(action: Dict[str, Any],
                                  pipeline: Dict[str, Any], dag):
    """Converts an action into a specific Dataproc operator or task group.

    Args:
        action: The action configuration object.
        pipeline: The pipeline configuration object.
        dag: The Airflow DAG object.

    Returns:
        An Airflow operator or TaskGroup based on the engine type and cluster mode.

    Raises:
        ValueError: If the engine type or cluster mode is not supported.
    """
    if action.engine.engineType == "dataproc-gce":
        if action.engine.clusterMode == "existing":
            return dataproc_existing_cluster(action, pipeline, dag=dag)
        elif action.engine.clusterMode == "ephemeral":
            return dataproc_ephemeral_task(action, dag=dag)
    elif action.engine.engineType == "dataproc-serverless":
        return create_dataproc_create_batch_operator_task(action,
                                                          pipeline,
                                                          dag=dag)

    raise ValueError(
        f"Unsupported notebook configuration for action {action.name}")


def _get_config_or_default(config_obj,
                           pipeline,
                           action_attribute,
                           pipeline_attribute=None):
    """Retrieves a configuration value or falls back to the pipeline default.

    Args:
        config_obj: The configuration object to check first.
        pipeline: The pipeline configuration containing defaults.
        action_attribute: The attribute name to look up in config_obj.
        pipeline_attribute: The optional attribute name to look up in pipeline defaults.

    Returns:
        The resolved configuration value.
    """
    if pipeline_attribute is None:
        pipeline_attribute = action_attribute
    value = getattr(config_obj, action_attribute, None)
    if value:
        return value
    return getattr(pipeline.defaults.cloudDefault, pipeline_attribute)


def create_service_dataform_task(action: Dict[str, Any],
                                 pipeline: Dict[str, Any], dag):
    """Converts an action into a DataformCreateWorkflowInvocationOperator.

    Args:
        action: The action configuration object.
        pipeline: The pipeline configuration object.
        dag: The Airflow DAG object.

    Returns:
        An instance of DataformCreateWorkflowInvocationOperator.
    """
    from airflow.providers.google.cloud.operators.dataform import DataformCreateWorkflowInvocationOperator

    return DataformCreateWorkflowInvocationOperator(
        task_id=action.name,
        project_id=_get_config_or_default(action.dataformServiceConfig,
                                          pipeline, "project_id", "project"),
        region=_get_config_or_default(action.dataformServiceConfig, pipeline,
                                      "region"),
        execution_timeout=duration_to_timedelta(action.executionTimeout)
        if action.executionTimeout else None,
        repository_id=action.dataformServiceConfig.repository_id,
        workflow_invocation=action.dataformServiceConfig.workflow_invocation,
        doc_md=json.dumps({"op_action_name": action.name}),
        dag=dag)


def create_local_dataform_task(action: Dict[str, Any], _: Dict[str, Any],
                               gcs_bucket_path_template: str, dag):
    """Converts an action into a KubernetesPodOperator for a Dataform workflow.

    Args:
        action: The action configuration object.
        _: Ignored pipeline configuration object.
        gcs_bucket_path_template: The GCS bucket path containing the Dataform workspace.
        dag: The Airflow DAG object.

    Returns:
        An instance of KubernetesPodOperator configured to run Dataform locally.
    """
    from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

    return KubernetesPodOperator(
        task_id=action.name,
        name="dataform-runner",
        namespace="composer-user-workloads",
        image=
        "us-docker.pkg.dev/cloud-airflow-releaser/orchestration-pipelines-basic-dataform-executor/orchestration-pipelines-basic-dataform-executor@sha256:fd7cd9673fda5994f1f90bfb3170ff6aa5ae8ed862d8ea518dddc5c48f9bd8f4",
        env_vars={"GCS_BUCKET_PATH": gcs_bucket_path_template},
        cmds=["/bin/sh", "-c"],
        arguments=[
            "gsutil -m cp -r $GCS_BUCKET_PATH/* . && dataform run --timeout=60s"
        ],
        get_logs=True,
        config_file="/home/airflow/composer_kube_config",
        image_pull_policy="Always",
        execution_timeout=(duration_to_timedelta(action.executionTimeout)
                           if action.executionTimeout else None),
        doc_md=json.dumps({"op_action_name": action.name}),
        dag=dag)
