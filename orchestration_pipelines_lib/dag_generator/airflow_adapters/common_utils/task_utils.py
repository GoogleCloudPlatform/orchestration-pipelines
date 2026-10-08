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

from __future__ import annotations

import json
import logging
import os
from collections.abc import Callable
from datetime import datetime
from typing import TYPE_CHECKING, Any

import pytz

from orchestration_pipelines_lib.dag_generator.airflow_adapters.common_utils import (  # noqa: E501
    dataproc_utils,
    gcs_utils,
    utils,
)
from orchestration_pipelines_lib.internal_models import (
    actions as internal_actions,
)
from orchestration_pipelines_lib.internal_models.actions import (
    AIActionModel,
    BqOperationActionModel,
    DataformActionModel,
    DataIngestionActionModel,
    DataprocOperatorActionModel,
    DBTActionModel,
    OrchestrationPipelineActionModel,
    PythonScriptActionModel,
    PythonVirtualenvActionModel,
)
from orchestration_pipelines_lib.internal_models.pipeline import PipelineModel
from orchestration_pipelines_lib.scripts.dbt_wrapper import invoke_dbt_run
from orchestration_pipelines_lib.utils.duration_utils import (
    duration_to_timedelta,
)
from orchestration_pipelines_lib.utils.file_manager import FileManager
from orchestration_pipelines_lib.utils.metrics import (
    ActionExecutionEngine,
    ActionExecutionType,
    wrap_operator,
    wrap_retry_operator,
)

from .retry_resolver import RetryResolver

if TYPE_CHECKING:
    from airflow.models import DAG
    from airflow.utils.task_group import TaskGroup

    from .dag_utils import DAGKwargs


def create_python_script_task(
    get_operator: Callable[[], type],
    action: PythonScriptActionModel,
    pipeline: PipelineModel,
    dag: DAG,
) -> Any:
    """Converts an action into a PythonOperator.

    Args:
        get_operator: Callable returning the Airflow PythonOperator class.
        action: The Python script action model configuration.
        pipeline: The pipeline model configuration.
        dag: The Airflow DAG instance.

    Returns:
        An instantiated Airflow operator wrapped with observability.

    Raises:
        Exception: If operator instantiation fails.
    """
    try:
        callable_path = action.filename
        entrypoint = action.config.pythonCallable
        user_kwargs = action.config.opKwargs or {}

        def runtime_wrapper(**kwargs):
            """Imports the target callable at runtime and filters kwargs."""
            python_callable = utils.import_callable(callable_path, entrypoint)
            filtered_kwargs = {
                k: v for k, v in kwargs.items() if k in user_kwargs
            }
            return python_callable(**filtered_kwargs)

        OrchestrationPythonOperator = wrap_operator(
            get_operator(),
            ActionExecutionType.from_action_type(action.type),
            ActionExecutionEngine.LOCAL,
            get_pipeline_metadata,
        )

        return OrchestrationPythonOperator(
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
            **get_action_retry_kwargs(action),
        )
    except Exception as e:
        raise RuntimeError(
            f"Failed to create task for action '{action.name}' "
            f"from '{action.config.pythonCallable}': {e}"
        ) from e


def create_python_virtualenv_task(
    get_operator: Callable[[], type],
    action: PythonVirtualenvActionModel,
    pipeline: PipelineModel,
    dag: DAG,
) -> Any:
    """Converts an action into a PythonVirtualenvOperator.

    Args:
        get_operator: Callable returning the Airflow PythonVirtualenvOperator
            class.
        action: The Python virtualenv action model configuration.
        pipeline: The pipeline model configuration.
        dag: The Airflow DAG instance.

    Returns:
        An instantiated Airflow operator wrapped with observability.

    Raises:
        ValueError: If the target entrypoint does not resolve to a callable.
        Exception: If operator instantiation fails.
    """
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

        OrchestrationPythonVirtualenvOperator = wrap_operator(
            get_operator(),
            ActionExecutionType.from_action_type(action.type),
            ActionExecutionEngine.LOCAL,
            get_pipeline_metadata,
        )

        return OrchestrationPythonVirtualenvOperator(
            task_id=action.name,
            python_callable=python_callable,
            op_kwargs=action.config.opKwargs or {},
            requirements=requirements,
            system_site_packages=action.config.systemSitePackages or False,
            execution_timeout=(
                duration_to_timedelta(action.executionTimeout)
                if action.executionTimeout
                else None
            ),
            trigger_rule=action.triggerRule,
            doc_md=json.dumps({"op_action_name": action.name}),
            dag=dag,
            **get_action_retry_kwargs(action),
        )
    except Exception as e:
        raise RuntimeError(
            f"Failed to create task for action '{action.name}' "
            f"from '{action.config.pythonCallable}': {e}"
        ) from e


def create_dbt_task(
    get_operator: Callable[[], type],
    action: DBTActionModel,
    pipeline: PipelineModel,
    dag: DAG,
) -> Any:
    """Converts an action into a PythonOperator for dbt.

    Args:
        get_operator: Callable returning the Airflow PythonOperator class.
        action: The dbt action model configuration.
        pipeline: The pipeline model configuration.
        dag: The Airflow DAG instance.

    Returns:
        An instantiated Airflow operator wrapped with observability.

    Raises:
        Exception: If operator instantiation fails.
    """
    try:
        op_kwargs: dict[str, Any] = {
            "project_dir": action.source.path,
            "profiles_dir": action.source.path,
        }
        if action.select_models:
            op_kwargs["select_models"] = action.select_models
        if action.params:
            op_kwargs["params"] = action.params

        OrchestrationPythonOperator = wrap_operator(
            get_operator(),
            ActionExecutionType.from_action_type(action.type),
            ActionExecutionEngine.LOCAL,
            get_pipeline_metadata,
        )

        return OrchestrationPythonOperator(
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
            **get_action_retry_kwargs(action),
        )
    except Exception as e:
        raise RuntimeError(
            f"Failed to create task for action '{action.name}': {e}"
        ) from e


def create_dataform_task(
    get_variable: Callable[[], type],
    action: DataformActionModel,
    pipeline: PipelineModel,
    dag: DAG,
) -> Any:
    """Converts an action into a Dataform operator.

    Depending on the execution mode, it either runs a local
    KubernetesPodOperator or invokes the Dataform service operator.

    Args:
        get_variable: Callable returning the Airflow Variable class.
        action: The Dataform action model configuration.
        pipeline: The pipeline model configuration.
        dag: The Airflow DAG instance.

    Returns:
        An instantiated Dataform operator (local pod or service operator).
    """
    if action.executionMode == "local":
        gcs_bucket_path = get_variable().get(
            "dataform_gcs_path", action.dataform_project_path
        )
        return create_local_dataform_task(
            action, pipeline, gcs_bucket_path, dag=dag
        )
    else:
        return create_service_dataform_task(action, pipeline, dag=dag)


def create_orchestration_pipeline_trigger_task(
    get_operator: Callable[[], type],
    action: OrchestrationPipelineActionModel,
    pipeline: PipelineModel,
    dag: DAG,
) -> Any:
    """Converts an action into a TriggerDagRunOperator.

    Args:
        get_operator: Callable returning the Airflow TriggerDagRunOperator
            class.
        action: The orchestration pipeline trigger action model.
        pipeline: The pipeline model configuration.
        dag: The Airflow DAG instance.

    Returns:
        An instantiated Airflow operator wrapped with observability.

    Raises:
        Exception: If operator instantiation fails.
    """
    try:
        wait_for_completion = action.wait_for_completion or False

        OrchestrationTriggerDagRunOperator = wrap_operator(
            get_operator(),
            ActionExecutionType.from_action_type(action.type),
            ActionExecutionEngine.LOCAL,
            get_pipeline_metadata,
        )

        return OrchestrationTriggerDagRunOperator(
            task_id=action.name,
            trigger_dag_id="{{ resolve_latest_pipeline_dag_id(params.target_pipeline_id, params.bundle_id) }}",  # noqa: E501
            params={
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
            **get_action_retry_kwargs(action),
        )
    except Exception as e:
        raise RuntimeError(
            f"Failed to create task for action '{action.name}': {e}"
        ) from e


def get_pipeline_metadata(dag: Any) -> tuple[str, str, str]:
    """Extracts bundle_id, version_id, and pipeline_id from a DAG object's
    doc_md property.

    Expects the structured JSON metadata to be present in `dag.doc_md`.
    """
    bundle_id = "unknown_bundle"
    version_id = "unknown_version"
    pipeline_id = (
        dag.dag_id if dag and hasattr(dag, "dag_id") else "unknown_pipeline"
    )

    if dag and hasattr(dag, "doc_md") and dag.doc_md:
        try:
            doc_data = json.loads(dag.doc_md)
            if isinstance(doc_data, dict):
                bundle_id = doc_data.get("op_bundle", bundle_id)
                version_id = doc_data.get("op_version", version_id)
                pipeline_id = doc_data.get("op_pipeline", pipeline_id)
            else:
                logging.warning(
                    "DAG '%s' doc_md is not a JSON dictionary: %s",
                    pipeline_id,
                    dag.doc_md,
                )
        except Exception as e:  # pylint: disable=broad-exception-caught
            logging.warning(
                "Failed to parse 'doc_md' of DAG '%s' as JSON: %s",
                pipeline_id,
                e,
            )

    return bundle_id, version_id, pipeline_id


def get_action_retry_kwargs(action: Any) -> dict[str, Any]:
    """Extracts Airflow retry kwargs from an action's retryPolicy.

    Args:
        action: The action configuration object.

    Returns:
        A dict containing resolved retry kwargs if defined in the action's
        retryPolicy.
    """
    return RetryResolver.resolve_action_kwargs(action)


def _upload_inline_query_to_gcs(
    dag: Any, query: str, gcs_bucket: str, logger: Any
) -> str:
    """Uploads the inline query string to GCS and returns its gs:// URI.

    The path is derived from DAG metadata to ensure isolation.
    """
    import hashlib

    from google.cloud import storage

    if not gcs_bucket:
        raise ValueError("GCS bucket must be specified for inline SQL upload.")

    bundle_id, version_id, _ = get_pipeline_metadata(dag)

    hash_value = hashlib.sha256(query.encode("utf-8")).hexdigest()

    blob_name = (
        f"data/{bundle_id}/versions/{version_id}/managed-temp/{hash_value}.sql"
    )
    gcs_uri = f"gs://{gcs_bucket}/{blob_name}"

    storage_client = storage.Client()
    bucket = storage_client.bucket(gcs_bucket)
    blob = bucket.blob(blob_name)

    logger.info(
        "Uploading inline query to %s during task execution...", gcs_uri
    )
    blob.upload_from_string(query)

    return gcs_uri


_dataproc_create_batch_inline_sql_operator_class = None


def get_dataproc_create_batch_inline_sql_operator_class():
    """Returns the DataprocCreateBatchInlineSqlOperator class."""
    global _dataproc_create_batch_inline_sql_operator_class
    if _dataproc_create_batch_inline_sql_operator_class is None:
        from airflow.providers.google.cloud.operators.dataproc import (
            DataprocCreateBatchOperator,
        )

        class DataprocCreateBatchInlineSqlOperator(DataprocCreateBatchOperator):
            """Inline SQL operator for Dataproc Create Batch."""

            def __init__(self, *, query: str, gcs_bucket: str, **kwargs):
                self.query = query
                self.gcs_bucket = gcs_bucket
                super().__init__(**kwargs)

            def execute(self, context):
                gcs_uri = _upload_inline_query_to_gcs(
                    self.dag, self.query, self.gcs_bucket, self.log
                )

                try:
                    self.batch.spark_sql_batch.query_file_uri = gcs_uri
                except AttributeError:
                    if isinstance(self.batch, dict):
                        self.batch.setdefault("spark_sql_batch", {})[
                            "query_file_uri"
                        ] = gcs_uri
                    else:
                        raise

                return super().execute(context)

        _dataproc_create_batch_inline_sql_operator_class = (
            DataprocCreateBatchInlineSqlOperator
        )

    return _dataproc_create_batch_inline_sql_operator_class


_dataproc_submit_job_inline_sql_operator_class = None


def get_dataproc_submit_job_inline_sql_operator_class():
    """Returns the DataprocSubmitJobInlineSqlOperator class."""
    global _dataproc_submit_job_inline_sql_operator_class
    if _dataproc_submit_job_inline_sql_operator_class is None:
        from airflow.providers.google.cloud.operators.dataproc import (
            DataprocSubmitJobOperator,
        )

        class DataprocSubmitJobInlineSqlOperator(DataprocSubmitJobOperator):
            """Inline SQL operator for Dataproc Submit Job."""

            def __init__(self, *, query: str, gcs_bucket: str, **kwargs):
                self.query = query
                self.gcs_bucket = gcs_bucket
                super().__init__(**kwargs)

            def execute(self, context):
                gcs_uri = _upload_inline_query_to_gcs(
                    self.dag, self.query, self.gcs_bucket, self.log
                )

                try:
                    self.job.spark_sql_job.query_file_uri = gcs_uri
                except AttributeError:
                    if isinstance(self.job, dict):
                        self.job.setdefault("spark_sql_job", {})[
                            "query_file_uri"
                        ] = gcs_uri
                    else:
                        raise

                return super().execute(context)

        _dataproc_submit_job_inline_sql_operator_class = (
            DataprocSubmitJobInlineSqlOperator
        )

    return _dataproc_submit_job_inline_sql_operator_class


def create_dataproc_create_batch_operator_task(
    action: DataprocOperatorActionModel, pipeline: PipelineModel, dag: DAG
):
    """Converts an action into a DataprocCreateBatchOperator.

    Args:
        action: The action configuration object.
        pipeline: The pipeline configuration object.
        dag: The Airflow DAG object.

    Returns:
        An instance of DataprocCreateBatchOperator.
    """
    import uuid

    from airflow.providers.google.cloud.operators.dataproc import (
        DataprocCreateBatchOperator,
    )
    from google.cloud import dataproc_v1

    try:
        job_specific_config = {}
        if action.type in ("pyspark", "notebook"):
            wrapper_uri = gcs_utils.get_run_notebook_gcs_path()
            gcs_utils.upload_run_notebook_if_needed(wrapper_uri)
            job_specific_config["pyspark_batch"] = (
                dataproc_utils.get_pyspark_batch_config(
                    action,
                    wrapper_uri,
                )
            )

        dataproc_create_batch_operator = DataprocCreateBatchOperator
        extra_kwargs = {}

        if action.type == "sql":
            spark_sql_batch = {}

            if action.params:
                spark_sql_batch["query_variables"] = action.params

            if action.query:
                dataproc_create_batch_operator = (
                    get_dataproc_create_batch_inline_sql_operator_class()
                )
                extra_kwargs["query"] = action.query
                extra_kwargs["gcs_bucket"] = os.environ.get("GCS_BUCKET")

            elif action.filename:
                spark_sql_batch["query_file_uri"] = action.filename
            job_specific_config["spark_sql_batch"] = spark_sql_batch

        runtime_config = action.config.resourceProfile.runtimeConfig or {}
        environment_config = (
            action.config.resourceProfile.environmentConfig or {}
        )

        if action.type in ("pyspark", "notebook"):
            deps_bucket = action.depsBucket or ""
            execution_config = environment_config.setdefault(
                "execution_config", {}
            )
            if execution_config.get("staging_bucket") is None and deps_bucket:
                execution_config["staging_bucket"] = deps_bucket

        batch = dataproc_v1.types.Batch(
            **job_specific_config,
            runtime_config=runtime_config,
            environment_config=environment_config,
            labels=action.labels,
        )
        batch_dict = type(batch).to_dict(batch)

        ObservableDataprocCreateBatchOperator = wrap_operator(
            dataproc_create_batch_operator,
            ActionExecutionType.from_action_type(action.type),
            ActionExecutionEngine.DATAPROC,
            get_pipeline_metadata,
        )

        return ObservableDataprocCreateBatchOperator(
            task_id=action.name,
            region=action.region,
            project_id=pipeline.defaults.cloudDefault.project,
            batch=batch_dict,
            batch_id=(
                f"{action.name.lower().lstrip('_-').replace('_', '-')[:50]}-"
                f"{uuid.uuid4().hex[:6]}"
            ),
            execution_timeout=(
                duration_to_timedelta(action.executionTimeout)
                if action.executionTimeout
                else None
            ),
            impersonation_chain=action.impersonationChain,
            trigger_rule=action.triggerRule,
            doc_md=json.dumps({"op_action_name": action.name}),
            dag=dag,
            **get_action_retry_kwargs(action),
            **extra_kwargs,
        )
    except Exception as e:
        raise RuntimeError(
            f"Failed to create task for action '{action.name}': {e}"
        ) from e


def create_bq_operation_task(
    action: BqOperationActionModel, pipeline: PipelineModel, dag: DAG
):
    """Converts an action into a BigQueryInsertJobOperator.

    Args:
        action: The action configuration object.
        pipeline: The pipeline configuration object.
        dag: The Airflow DAG object.

    Returns:
        An instance of BigQueryInsertJobOperator.
    """
    from airflow.providers.google.cloud.operators.bigquery import (
        BigQueryInsertJobOperator,
    )

    ObservableBigQueryInsertJobOperator = wrap_operator(
        BigQueryInsertJobOperator,
        ActionExecutionType.from_action_type(action.type),
        ActionExecutionEngine.BIGQUERY,
        get_pipeline_metadata,
    )

    try:
        if action.filename:
            file_manager = FileManager()
            query = file_manager.read(action.filename)
        else:
            query = action.query

        configuration = {
            # Will be converted to JobConfigurationQuery in Protobuf.
            # It can be set up via the QueryJobConfig Python class,
            # which can be imported from google.cloud.bigquery.job.query
            "query": {
                "query": query,
                "useLegacySql": False,
            },
            "labels": action.labels,
        }

        if action.params:
            configuration["query"]["queryParameters"] = []

            for key, value in action.params.items():
                param = {
                    "name": key,
                    # NOTE: The limitation of dataproc engine that requires
                    # string typed arguments is applicable as well to SQL
                    # actions run on BigQuerys.
                    "parameterType": {"type": "STRING"},
                    "parameterValue": {"value": value},
                }
                configuration["query"]["queryParameters"].append(param)

        if not query.strip().upper().startswith("CREATE"):
            configuration["query"]["writeDisposition"] = "WRITE_TRUNCATE"
            configuration["query"]["createDisposition"] = "CREATE_IF_NEEDED"

        if action.config.destinationTable:
            parts = action.config.destinationTable.split(".")
            if len(parts) != 3:
                raise ValueError(
                    "destinationTable should be in format "
                    "'project.dataset.table'"
                )
            configuration["query"]["destinationTable"] = {
                "projectId": parts[0],
                "datasetId": parts[1],
                "tableId": parts[2],
            }

        return ObservableBigQueryInsertJobOperator(
            task_id=action.name,
            location=action.config.location,
            project_id=pipeline.defaults.cloudDefault.project,
            configuration=configuration,
            execution_timeout=(
                duration_to_timedelta(action.executionTimeout)
                if action.executionTimeout
                else None
            ),
            gcp_conn_id="google_cloud_default",
            impersonation_chain=action.impersonationChain,
            trigger_rule=action.triggerRule,
            doc_md=json.dumps({"op_action_name": action.name}),
            dag=dag,
            **get_action_retry_kwargs(action),
        )
    except Exception as e:
        raise RuntimeError(
            f"Failed to create task for action '{action.name}': {e}"
        ) from e


def dataproc_ephemeral_task(
    action: DataprocOperatorActionModel, dag: DAG
) -> TaskGroup:
    """Converts an action into a TaskGroup for an ephemeral Dataproc
    workflow.

    Args:
        action: The action configuration object.
        dag: The Airflow DAG object.

    Returns:
        An Airflow TaskGroup containing cluster creation, job submission, and
        deletion tasks.
    """
    from airflow.providers.google.cloud.operators.dataproc import (
        DataprocCreateClusterOperator,
        DataprocDeleteClusterOperator,
        DataprocSubmitJobOperator,
    )
    from airflow.utils.task_group import TaskGroup

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
                trigger_rule=action.triggerRule,
                doc_md=json.dumps({"op_action_name": action.name}),
                labels=action.labels,
                dag=dag,
            ).as_setup()

            job = {
                "placement": {"cluster_name": action.config.cluster_name},
                "reference": {
                    "project_id": action.config.project_id,
                },
                "labels": action.labels,
            }

            dataproc_submit_job_operator = DataprocSubmitJobOperator
            extra_kwargs = {}

            if action.type == "sql":
                spark_sql_job = {}

                if action.params:
                    spark_sql_job["script_variables"] = action.params

                if action.query:
                    dataproc_submit_job_operator = (
                        get_dataproc_submit_job_inline_sql_operator_class()
                    )
                    extra_kwargs["query"] = action.query
                    extra_kwargs["gcs_bucket"] = os.environ.get("GCS_BUCKET")
                elif action.filename:
                    spark_sql_job["query_file_uri"] = action.filename
                if action.config.properties:
                    spark_sql_job["properties"] = action.config.properties
                job["spark_sql_job"] = spark_sql_job
            else:
                # This block handles both pyspark and notebook actions for
                # ephemeral clusters (since ephemeral Dataproc still relies on
                # DataprocSubmitJobOperator, which uses pyspark_job for
                # notebooks via the old wrapper method).
                wrapper_uri = gcs_utils.get_run_notebook_gcs_path()
                gcs_utils.upload_run_notebook_if_needed(wrapper_uri)
                pyspark_job = dataproc_utils.get_pyspark_batch_config(
                    action, wrapper_uri
                )
                if action.pyFiles:
                    pyspark_job["python_file_uris"] = action.pyFiles
                pyspark_job["properties"] = action.config.properties
                job["pyspark_job"] = pyspark_job

            ObservableDataprocSubmitJobOperator = wrap_operator(
                dataproc_submit_job_operator,
                ActionExecutionType.from_action_type(action.type),
                ActionExecutionEngine.DATAPROC,
                get_pipeline_metadata,
            )

            submit_job = ObservableDataprocSubmitJobOperator(
                task_id=f"{action.name}_submit_job",
                job=job,
                execution_timeout=(
                    duration_to_timedelta(action.executionTimeout)
                    if action.executionTimeout
                    else None
                ),
                region=action.config.region,
                project_id=action.config.project_id,
                impersonation_chain=action.impersonationChain,
                doc_md=json.dumps({"op_action_name": action.name}),
                dag=dag,
                **get_action_retry_kwargs(action),
                **extra_kwargs,
            )

            delete_cluster = DataprocDeleteClusterOperator(
                task_id=f"{action.name}_delete_cluster",
                project_id=action.config.project_id,
                cluster_name=action.config.cluster_name,
                region=action.config.region,
                impersonation_chain=action.impersonationChain,
                doc_md=json.dumps({"op_action_name": action.name}),
                dag=dag,
            ).as_teardown(setups=create_cluster)

            # pylint: disable=pointless-statement
            create_cluster >> submit_job >> delete_cluster
        return task_group
    except Exception as e:
        raise RuntimeError(
            f"Failed to create task for action '{action.name}': {e}"
        ) from e


def dataproc_existing_cluster(
    action: DataprocOperatorActionModel, pipeline: PipelineModel, dag: DAG
):
    """Converts action into DataprocSubmitJobOperator for existing
    cluster.

    Args:
        action: The action configuration object.
        pipeline: The pipeline configuration object.
        dag: The Airflow DAG object.

    Returns:
        An instance of DataprocSubmitJobOperator.
    """
    from airflow.providers.google.cloud.operators.dataproc import (
        DataprocSubmitJobOperator,
    )

    try:
        job = {
            "placement": {"cluster_name": action.config.cluster_name},
            "reference": {
                "project_id": action.config.project_id,
            },
            "labels": action.labels,
        }

        dataproc_submit_job_operator = DataprocSubmitJobOperator
        extra_kwargs = {}

        if action.type == "sql":
            spark_sql_job = {}

            if action.params:
                spark_sql_job["script_variables"] = action.params

            if action.query:
                dataproc_submit_job_operator = (
                    get_dataproc_submit_job_inline_sql_operator_class()
                )
                extra_kwargs["query"] = action.query
                extra_kwargs["gcs_bucket"] = os.environ.get("GCS_BUCKET")
            elif action.filename:
                spark_sql_job["query_file_uri"] = action.filename
            if action.config.properties:
                spark_sql_job["properties"] = action.config.properties
            job["spark_sql_job"] = spark_sql_job
        else:
            # This block handles both pyspark and notebook actions for
            # existing clusters
            wrapper_uri = gcs_utils.get_run_notebook_gcs_path()
            gcs_utils.upload_run_notebook_if_needed(wrapper_uri)
            job["pyspark_job"] = dataproc_utils.get_pyspark_batch_config(
                action, wrapper_uri
            )
            if action.pyFiles:
                job["pyspark_job"]["python_file_uris"] = action.pyFiles
            job["pyspark_job"]["properties"] = action.config.properties

        ObservableDataprocSubmitJobOperator = wrap_operator(
            dataproc_submit_job_operator,
            ActionExecutionType.from_action_type(action.type),
            ActionExecutionEngine.DATAPROC,
            get_pipeline_metadata,
        )

        return ObservableDataprocSubmitJobOperator(
            task_id=action.name,
            job=job,
            execution_timeout=(
                duration_to_timedelta(action.executionTimeout)
                if action.executionTimeout
                else None
            ),
            region=action.region,
            project_id=pipeline.defaults.cloudDefault.project,
            impersonation_chain=action.impersonationChain,
            trigger_rule=action.triggerRule,
            doc_md=json.dumps({"op_action_name": action.name}),
            dag=dag,
            **get_action_retry_kwargs(action),
            **extra_kwargs,
        )
    except Exception as e:
        raise RuntimeError(
            f"Failed to create task for action '{action.name}': {e}"
        ) from e


def create_schedule_trigger_task(dag_kwargs: DAGKwargs, schedule_trigger: Any):
    """Converts the input trigger config into schedule parameters for the
    DAG.

    Args:
        dag_kwargs: A dictionary of DAG keyword arguments to update.
        schedule_trigger: The schedule trigger configuration object.
    """
    start_time = datetime.fromisoformat(schedule_trigger.startTime)
    end_time = (
        datetime.fromisoformat(schedule_trigger.endTime)
        if schedule_trigger.endTime
        else None
    )
    timezone = pytz.timezone(schedule_trigger.timezone)
    dag_kwargs["start_date"] = timezone.localize(start_time)
    dag_kwargs["end_date"] = timezone.localize(end_time) if end_time else None
    dag_kwargs["schedule"] = schedule_trigger.scheduleInterval
    dag_kwargs["catchup"] = schedule_trigger.catchup


def create_dataproc_operator_task(
    action: DataprocOperatorActionModel, pipeline: PipelineModel, dag: DAG
):
    """Converts an action into a specific Dataproc operator or task group.

    Args:
        action: The action configuration object.
        pipeline: The pipeline configuration object.
        dag: The Airflow DAG object.

    Returns:
        An Airflow operator or TaskGroup based on engine type and mode.

    Raises:
        ValueError: If the engine type or cluster mode is not supported.
    """
    if action.engine.engineType == "dataproc-gce":
        if action.engine.clusterMode == "existing":
            return dataproc_existing_cluster(action, pipeline, dag=dag)
        elif action.engine.clusterMode == "ephemeral":
            return dataproc_ephemeral_task(action, dag=dag)
    elif action.engine.engineType == "dataproc-serverless":
        return create_dataproc_create_batch_operator_task(
            action, pipeline, dag=dag
        )

    raise ValueError(
        f"Unsupported notebook configuration for action {action.name}"
    )


def _get_config_or_default(
    config_obj, pipeline, action_attribute, pipeline_attribute=None
):
    """Retrieves a configuration value or falls back to the pipeline default.

    Args:
        config_obj: The configuration object to check first.
        pipeline: The pipeline configuration containing defaults.
        action_attribute: The attribute name to look up in config_obj.
        pipeline_attribute: The optional attribute name to look up.

    Returns:
        The resolved configuration value.
    """
    if pipeline_attribute is None:
        pipeline_attribute = action_attribute
    value = getattr(config_obj, action_attribute, None)
    if value:
        return value
    return getattr(pipeline.defaults.cloudDefault, pipeline_attribute)


def create_service_dataform_task(
    action: DataformActionModel, pipeline: PipelineModel, dag: DAG
):
    """Converts an action into a DataformCreateWorkflowInvocationOperator.

    Args:
        action: The action configuration object.
        pipeline: The pipeline configuration object.
        dag: The Airflow DAG object.

    Returns:
        An instance of DataformCreateWorkflowInvocationOperator.
    """
    from airflow.providers.google.cloud.operators.dataform import (
        DataformCreateWorkflowInvocationOperator,
    )

    ObservableDataformCreateWorkflowInvocationOperator = wrap_operator(
        DataformCreateWorkflowInvocationOperator,
        ActionExecutionType.from_action_type(action.type),
        ActionExecutionEngine.DATAFORM,
        get_pipeline_metadata,
    )

    return ObservableDataformCreateWorkflowInvocationOperator(
        task_id=action.name,
        project_id=_get_config_or_default(
            action.dataformServiceConfig, pipeline, "project_id", "project"
        ),
        region=_get_config_or_default(
            action.dataformServiceConfig, pipeline, "region"
        ),
        execution_timeout=(
            duration_to_timedelta(action.executionTimeout)
            if action.executionTimeout
            else None
        ),
        repository_id=action.dataformServiceConfig.repository_id,  # type: ignore
        workflow_invocation=action.dataformServiceConfig.workflow_invocation,  # type: ignore
        trigger_rule=action.triggerRule,
        doc_md=json.dumps({"op_action_name": action.name}),
        dag=dag,
        **get_action_retry_kwargs(action),
    )


def create_local_dataform_task(
    action: DataformActionModel,
    _: PipelineModel,
    gcs_bucket_path_template: str,
    dag: DAG,
):
    """Converts an action into a KubernetesPodOperator for a Dataform
    workflow.

    Args:
        action: The action configuration object.
        _: Ignored pipeline configuration object.
        gcs_bucket_path_template: The GCS bucket path containing the workspace.
        dag: The Airflow DAG object.

    Returns:
        An instance of KubernetesPodOperator configured to run Dataform locally.
    """
    import shlex

    from airflow.providers.cncf.kubernetes.operators.pod import (
        KubernetesPodOperator,
    )

    ObservableKubernetesPodOperator = wrap_operator(
        KubernetesPodOperator,
        ActionExecutionType.from_action_type(action.type),
        ActionExecutionEngine.LOCAL,
        get_pipeline_metadata,
    )

    labels = getattr(action, "labels", None) or {}
    params = getattr(action, "params", None) or {}

    dataform_cmd = (
        "gcloud storage cp --recursive $GCS_BUCKET_PATH/* . && "
        "dataform run --timeout=60s"
    )
    if labels:
        labels_str = ",".join(
            shlex.quote(f"{k}={v}") for k, v in labels.items()
        )
        dataform_cmd += f" --job-labels={labels_str}"
    if params:
        params_str = ",".join(
            shlex.quote(f"{k}={v}") for k, v in params.items()
        )
        dataform_cmd += f" --vars={params_str}"

    return ObservableKubernetesPodOperator(
        task_id=action.name,
        name="dataform-runner",
        namespace="composer-user-workloads",
        image="us-docker.pkg.dev/cloud-airflow-releaser/"
        "orchestration-pipelines-basic-dataform-executor/"
        "orchestration-pipelines-basic-dataform-executor"
        "@sha256:fd7cd9673fda5994f1f90bfb3170ff6aa5ae8ed862d"
        "8ea518dddc5c48f9bd8f4",
        env_vars={"GCS_BUCKET_PATH": gcs_bucket_path_template},
        cmds=["/bin/sh", "-c"],
        arguments=[dataform_cmd],
        labels=labels,
        get_logs=True,
        config_file="/home/airflow/composer_kube_config",
        image_pull_policy="Always",
        execution_timeout=(
            duration_to_timedelta(action.executionTimeout)
            if action.executionTimeout
            else None
        ),
        trigger_rule=action.triggerRule,
        doc_md=json.dumps({"op_action_name": action.name}),
        dag=dag,
        **get_action_retry_kwargs(action),
    )


def create_bq_dts_task(
    action: DataIngestionActionModel, pipeline: PipelineModel, dag: DAG
) -> TaskGroup:
    """Converts an action into a TaskGroup for a BigQuery DTS workflow.

    Args:
        action: The action configuration object.
        pipeline: The pipeline configuration object.
        dag: The Airflow DAG object.

    Returns:
        An Airflow TaskGroup containing transfer run start and sensor tasks.
    """
    from airflow.providers.google.cloud.operators.bigquery_dts import (
        BigQueryDataTransferServiceStartTransferRunsOperator,
    )
    from airflow.providers.google.cloud.sensors.bigquery_dts import (
        BigQueryDataTransferServiceTransferRunSensor,
    )
    from airflow.utils.task_group import TaskGroup

    from orchestration_pipelines_lib.utils.dict_utils import (
        iso_to_timestamp_dict,
    )

    try:
        with TaskGroup(group_id=action.name, dag=dag) as task_group:
            project_id = (
                action.config.projectId
                or pipeline.defaults.cloudDefault.project
            )
            location = (
                action.config.location or pipeline.defaults.cloudDefault.region
            )

            requested_run_time = action.config.requestedRunTime
            requested_time_range = action.config.requestedTimeRange

            if action.config.runtimeParams:
                if requested_run_time is None:
                    requested_run_time = action.config.runtimeParams.get(
                        "requested_run_time"
                    )
                if requested_time_range is None:
                    requested_time_range = action.config.runtimeParams.get(
                        "requested_time_range"
                    )

            if isinstance(requested_run_time, str):
                requested_run_time = iso_to_timestamp_dict(requested_run_time)

            if isinstance(requested_time_range, dict):
                updated_time_range = {}
                for k, v in requested_time_range.items():
                    updated_time_range[k] = (
                        iso_to_timestamp_dict(v) if isinstance(v, str) else v
                    )
                requested_time_range = updated_time_range

            if requested_run_time is None and requested_time_range is None:
                requested_run_time = {
                    "seconds": (
                        "{{ logical_date.timestamp() | int if logical_date is "
                        "defined else execution_date.timestamp() | int }}"
                    )
                }

            OrchestrationBigQueryDataTransferServiceStartTransferRunsOperator = (  # noqa: E501
                wrap_retry_operator(
                    BigQueryDataTransferServiceStartTransferRunsOperator
                )
            )

            start_task = OrchestrationBigQueryDataTransferServiceStartTransferRunsOperator(  # noqa: E501
                task_id=f"{action.name}_start",
                transfer_config_id=action.config.transferConfigId,
                project_id=project_id,
                location=location,
                requested_run_time=requested_run_time,
                requested_time_range=requested_time_range,
                impersonation_chain=action.config.impersonationChain,
                execution_timeout=(
                    duration_to_timedelta(action.executionTimeout)
                    if action.executionTimeout
                    else None
                ),
                trigger_rule=action.triggerRule,
                doc_md=json.dumps({"op_action_name": action.name}),
                dag=dag,
                **get_action_retry_kwargs(action),
            )

            ObservableBigQueryDataTransferServiceTransferRunSensor = (
                wrap_operator(
                    BigQueryDataTransferServiceTransferRunSensor,
                    ActionExecutionType.from_action_type(action.type),
                    ActionExecutionEngine.BIGQUERY,
                    get_pipeline_metadata,
                )
            )

            sensor_task = (
                ObservableBigQueryDataTransferServiceTransferRunSensor(
                    task_id=f"{action.name}_sensor",
                    transfer_config_id=action.config.transferConfigId,
                    run_id=(
                        "{{ task_instance.xcom_pull("
                        f"task_ids='{action.name}."
                        f"{action.name}_start', key='run_id')"
                        " }}"
                    ),
                    project_id=project_id,
                    location=location,
                    impersonation_chain=action.config.impersonationChain,
                    doc_md=json.dumps({"op_action_name": action.name}),
                    dag=dag,
                )
            )

            # pylint: disable=pointless-statement
            start_task >> sensor_task

        return task_group
    except Exception as e:
        raise RuntimeError(
            f"Failed to create task for action '{action.name}': {e}"
        ) from e


def _create_vertex_upload_model_task(
    action: AIActionModel, pipeline: PipelineModel, dag
):
    """Converts an AI action into an UploadModelOperator for Vertex AI.

    Args:
        action: The action configuration object.
        pipeline: The pipeline configuration object.
        dag: The Airflow DAG object.

    Returns:
        An instance of UploadModelOperator.
    """
    from airflow.providers.google.cloud.operators.vertex_ai.model_service import (  # noqa: E501
        UploadModelOperator,
    )

    try:
        project_id = action.config.project_id
        region = action.config.location

        model = {
            "display_name": action.config.model_name,
            "artifact_uri": action.config.model_artifact_uri,
            "container_spec": {
                "image_uri": action.config.serving_container_image_uri,
            },
        }
        if action.config.description:
            model["description"] = action.config.description
        if action.labels:
            model["labels"] = action.labels

        ObservableUploadModelOperator = wrap_operator(
            UploadModelOperator,
            ActionExecutionType.from_action_type(action.type),
            ActionExecutionEngine.AGENT_PLATFORM,
            get_pipeline_metadata,
        )

        return ObservableUploadModelOperator(
            task_id=action.name,
            project_id=project_id,
            region=region,
            model=model,
            execution_timeout=(
                duration_to_timedelta(action.executionTimeout)
                if action.executionTimeout
                else None
            ),
            trigger_rule=action.triggerRule,
            doc_md=json.dumps({"op_action_name": action.name}),
            dag=dag,
            **get_action_retry_kwargs(action),
        )
    except Exception as e:
        raise RuntimeError(
            f"Failed to create task for action '{action.name}': {e}"
        ) from e


def _create_vertex_batch_inference_task(
    action: AIActionModel, pipeline: PipelineModel, dag: DAG
):
    """Converts an AI action into a CreateBatchPredictionJobOperator
    for Vertex AI.

    Args:
        action: The action configuration object.
        pipeline: The pipeline configuration object.
        dag: The Airflow DAG object.

    Returns:
        An instance of CreateBatchPredictionJobOperator.
    """
    from airflow.providers.google.cloud.operators.vertex_ai.batch_prediction_job import (  # noqa: E501
        CreateBatchPredictionJobOperator,
    )

    try:
        project_id = action.config.project_id
        region = action.config.location

        extra_kwargs = {}
        if action.config.instances_format:
            extra_kwargs["instances_format"] = action.config.instances_format
        if action.config.predictions_format:
            extra_kwargs["predictions_format"] = (
                action.config.predictions_format
            )
        if action.config.bigquery_source:
            extra_kwargs["bigquery_source"] = action.config.bigquery_source
        if action.config.gcs_source:
            extra_kwargs["gcs_source"] = action.config.gcs_source
        if action.config.bigquery_destination_prefix:
            extra_kwargs["bigquery_destination_prefix"] = (
                action.config.bigquery_destination_prefix
            )
        if action.config.gcs_destination_prefix:
            extra_kwargs["gcs_destination_prefix"] = (
                action.config.gcs_destination_prefix
            )
        if action.labels:
            extra_kwargs["labels"] = action.labels
        if action.config.impersonation_chain:
            extra_kwargs["impersonation_chain"] = (
                action.config.impersonation_chain
            )

        ObservableCreateBatchPredictionJobOperator = wrap_operator(
            CreateBatchPredictionJobOperator,
            ActionExecutionType.from_action_type(action.type),
            ActionExecutionEngine.AGENT_PLATFORM,
            get_pipeline_metadata,
        )

        return ObservableCreateBatchPredictionJobOperator(
            task_id=action.name,
            project_id=project_id,
            region=region,
            job_display_name=action.config.job_display_name,
            model_name=action.config.model_name,
            machine_type="n1-standard-4",
            execution_timeout=(
                duration_to_timedelta(action.executionTimeout)
                if action.executionTimeout
                else None
            ),
            trigger_rule=action.triggerRule,
            doc_md=json.dumps({"op_action_name": action.name}),
            dag=dag,
            **get_action_retry_kwargs(action),
            **extra_kwargs,
        )
    except Exception as e:
        raise RuntimeError(
            f"Failed to create task for action '{action.name}': {e}"
        ) from e


def _create_vertex_custom_job_task(
    action: internal_actions.AIActionModel,
    pipeline: PipelineModel,
    dag: DAG,
):
    """Creates CreateCustomJobOperator for Vertex AI.

    Args:
        action: The AI action configuration model.
        pipeline: The pipeline configuration object.
        dag: The Airflow DAG object.

    Returns:
        An instance of CreateCustomJobOperator wrapped with observability.
    """
    try:
        from airflow.providers.google.cloud.operators.vertex_ai.custom_job import (
            CreateCustomJobOperator,
        )
    except ImportError as e:
        raise RuntimeError(
            "The Agent Platform CreateCustomJobOperator is not available in the installed "
            "version of apache-airflow-providers-google. Please upgrade to a "
            "newer version (>=22.5.0) to use this feature."
        ) from e

    try:
        if not isinstance(
            action.config,
            internal_actions.AgentPlatformCreateAndRunCustomJobSpecModel,
        ):
            raise TypeError(
                "Expected AgentPlatformCreateAndRunCustomJobSpecModel, "
                f"got {type(action.config).__name__}"
            )
        project_id = action.config.project_id
        region = action.config.location
        custom_job = dict(action.config.custom_job)
        if action.labels:
            existing_labels = dict(custom_job.get("labels") or {})
            existing_labels.update(action.labels)
            custom_job["labels"] = existing_labels

        ObservableCreateCustomJobOperator = wrap_operator(
            CreateCustomJobOperator,
            ActionExecutionType.from_action_type(action.type),
            ActionExecutionEngine.AGENT_PLATFORM,
            get_pipeline_metadata,
        )

        return ObservableCreateCustomJobOperator(
            task_id=action.name,
            project_id=project_id,
            region=region,
            custom_job=custom_job,
            impersonation_chain=action.config.impersonation_chain,
            execution_timeout=(
                duration_to_timedelta(action.executionTimeout)
                if action.executionTimeout
                else None
            ),
            trigger_rule=action.triggerRule,
            doc_md=json.dumps({"op_action_name": action.name}),
            dag=dag,
            **get_action_retry_kwargs(action),
        )
    except Exception as e:
        raise RuntimeError(
            f"Failed to create task for action '{action.name}': {e}"
        ) from e


def create_ai_task(
    action: internal_actions.AIActionModel,
    pipeline: PipelineModel,
    dag: DAG,
):
    """Converts an AI action into the appropriate Airflow operator.

    Args:
        action: The action configuration object.
        pipeline: The pipeline configuration object.
        dag: The Airflow DAG object.

    Returns:
        An Airflow operator for the AI action.
    """
    if action.provider != "agent_platform":
        raise ValueError(f"Unsupported AI provider: {action.provider}")

    match action.ai_action_type:
        case "model_upload":
            return _create_vertex_upload_model_task(action, pipeline, dag=dag)
        case "batch_inference":
            return _create_vertex_batch_inference_task(
                action, pipeline, dag=dag
            )
        case "create_and_run_custom_job":
            return _create_vertex_custom_job_task(action, pipeline, dag=dag)
        case _:
            raise ValueError(
                "Unsupported agent_platform action type: "
                f"{action.ai_action_type}"
            )


def create_airflow_task(action: dict[str, Any], pipeline: dict[str, Any], dag):
    """Converts Airflow task action to the appropriate Airflow task operator.

    Args:
        action: The action configuration object.
        pipeline: The pipeline configuration object.
        dag: The Airflow DAG object.

    Returns:
        An instance of the specified Airflow operator.
    """
    import importlib

    try:
        module_path, class_name = action.operator_class.rsplit(".", 1)
        module = importlib.import_module(module_path)
        operator_class = getattr(module, class_name)

        params = action.params or {}

        ObservableOperator = wrap_operator(
            operator_class,
            ActionExecutionType.from_action_type(action.type),
            ActionExecutionEngine.LOCAL,
            get_pipeline_metadata,
        )

        return ObservableOperator(
            task_id=action.name,
            execution_timeout=(
                duration_to_timedelta(action.executionTimeout)
                if action.executionTimeout
                else None
            ),
            trigger_rule=action.triggerRule,
            doc_md=json.dumps({"op_action_name": action.name}),
            dag=dag,
            **get_action_retry_kwargs(action),
            **params,
        )
    except Exception:
        logging.exception("Error creating task for action '%s'", action.name)
        raise
