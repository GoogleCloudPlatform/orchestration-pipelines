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
"""Unit tests for task utility functions."""

import json
import os
from dataclasses import replace
from datetime import timedelta
from unittest.mock import MagicMock, Mock, patch

import pytest

from orchestration_pipelines_lib.dag_generator.airflow_adapters.common_utils.task_utils import (  # noqa: E501
    _get_config_or_default,
    _upload_inline_query_to_gcs,
    create_airflow_task,
    create_ai_task,
    create_bq_dts_task,
    create_bq_operation_task,
    create_dataform_task,
    create_dataproc_create_batch_operator_task,
    create_dataproc_operator_task,
    create_dbt_task,
    create_local_dataform_task,
    create_orchestration_pipeline_trigger_task,
    create_python_script_task,
    create_python_virtualenv_task,
    create_service_dataform_task,
    dataproc_ephemeral_task,
    dataproc_existing_cluster,
    get_action_retry_kwargs,
    get_dataproc_create_batch_inline_sql_operator_class,
    get_dataproc_submit_job_inline_sql_operator_class,
    get_pipeline_metadata,
)
from orchestration_pipelines_lib.internal_models.actions import (
    AgentPlatformBatchInferenceSpecModel,
    AgentPlatformCreateAndRunCustomJobSpecModel,
    AIActionModel,
    BigQueryDtsSpecModel,
    BqOperationActionModel,
    BqOperationConfigurationModel,
    DataformActionModel,
    DataformServiceModel,
    DataIngestionActionModel,
    DataprocCreateBatchOperatorConfigurationModel,
    DataprocEphemeralConfigurationModel,
    DataprocGceExistingClusterConfigurationModel,
    DataprocOperatorActionModel,
    DBTActionModel,
    DbtLocalExecutionModel,
    EngineModel,
    OrchestrationPipelineActionModel,
    PythonScriptActionModel,
    PythonScriptConfigurationModel,
    PythonVirtualenvActionModel,
    PythonVirtualenvConfigurationModel,
    ResourceProfile,
)
from orchestration_pipelines_lib.internal_models.pipeline import (
    CloudDefaultsModel,
    DefaultsModel,
    MetaDataModel,
    PipelineModel,
    RunnerType,
)
from orchestration_pipelines_lib.scripts.dbt_wrapper import invoke_dbt_run
from tests.conftest import IS_AIRFLOW_2

if IS_AIRFLOW_2:
    from orchestration_pipelines_lib.dag_generator.airflow_adapters.airflow_2 import (  # noqa: E501
        adapter_imports,
    )
else:
    from orchestration_pipelines_lib.dag_generator.airflow_adapters.airflow_3 import (  # noqa: E501
        adapter_imports,
    )


def test_get_pipeline_metadata_from_doc_md():
    """Tests retrieving metadata from DAG's doc_md JSON property."""
    import pendulum
    from airflow.models import DAG

    dag_notes = json.dumps(
        {
            "op_bundle": "my_bundle_doc",
            "op_version": "v456",
            "op_pipeline": "my_pipeline_doc",
        }
    )
    test_dag = DAG(
        dag_id="some_dag_id", doc_md=dag_notes, start_date=pendulum.today("UTC")
    )
    bundle_id, version_id, pipeline_id = get_pipeline_metadata(test_dag)
    assert bundle_id == "my_bundle_doc"
    assert version_id == "v456"
    assert pipeline_id == "my_pipeline_doc"


def test_get_pipeline_metadata_no_doc_md_defaults(caplog):
    """Tests get_pipeline_metadata fallback when doc_md is missing."""
    import pendulum
    from airflow.models import DAG

    test_dag_no_doc = DAG(
        dag_id="my_pipeline", start_date=pendulum.today("UTC")
    )
    bundle_id, version_id, pipeline_id = get_pipeline_metadata(test_dag_no_doc)
    assert bundle_id == "unknown_bundle"
    assert version_id == "unknown_version"
    assert pipeline_id == "my_pipeline"
    test_dag_invalid_doc = DAG(
        dag_id="my_pipeline",
        doc_md="not-json",
        start_date=pendulum.today("UTC"),
    )
    with caplog.at_level("WARNING"):
        bundle_id, version_id, pipeline_id = get_pipeline_metadata(
            test_dag_invalid_doc
        )
    assert bundle_id == "unknown_bundle"
    assert version_id == "unknown_version"
    assert pipeline_id == "my_pipeline"
    assert any(
        "Failed to parse 'doc_md' of DAG 'my_pipeline' as JSON" in message
        for message in caplog.messages
    )
    test_dag_non_dict_doc = DAG(
        dag_id="my_pipeline",
        doc_md="[1, 2, 3]",
        start_date=pendulum.today("UTC"),
    )
    with caplog.at_level("WARNING"):
        bundle_id, version_id, pipeline_id = get_pipeline_metadata(
            test_dag_non_dict_doc
        )
    assert bundle_id == "unknown_bundle"
    assert version_id == "unknown_version"
    assert pipeline_id == "my_pipeline"
    assert any(
        "doc_md is not a JSON dictionary" in message
        for message in caplog.messages
    )


@patch("google.cloud.storage.Client")
def test_dataproc_create_batch_inline_sql_operator_execute(
    mock_storage_client_cls,
):
    """Tests operator uploads query to hashed path derived from doc_md."""
    import pendulum
    from airflow.models import DAG

    mock_storage_client = mock_storage_client_cls.return_value
    mock_bucket = MagicMock()
    mock_blob = MagicMock()
    mock_storage_client.bucket.return_value = mock_bucket
    mock_bucket.blob.return_value = mock_blob
    dag_notes = json.dumps(
        {
            "op_bundle": "my_bundle",
            "op_version": "v123",
            "op_pipeline": "my_pipeline",
        }
    )
    test_dag = DAG(
        dag_id="my_bundle__v__v123__my_pipeline",
        doc_md=dag_notes,
        start_date=pendulum.today("UTC").add(days=-1),
        schedule="@daily",
    )
    batch_config = {"spark_sql_batch": {}}
    DataprocCreateBatchInlineSqlOperator = (
        get_dataproc_create_batch_inline_sql_operator_class()
    )
    operator = DataprocCreateBatchInlineSqlOperator(
        task_id="test_action",
        query="SELECT 1;",
        gcs_bucket="my-example-bucket",
        region="us-central1",
        project_id="my-project",
        batch=batch_config,
        dag=test_dag,
    )
    mock_ti = MagicMock()
    mock_ti.try_number = 1
    mock_dag_run = MagicMock()
    mock_dag_run.run_id = "manual__2026-05-04T00:00:00+00:00"
    context = {"task_instance": mock_ti, "dag_run": mock_dag_run}
    import hashlib

    expected_hash = hashlib.sha256(operator.query.encode("utf-8")).hexdigest()
    expected_blob_name = (
        f"data/my_bundle/versions/v123/managed-temp/{expected_hash}.sql"
    )
    expected_gcs_uri = f"gs://my-example-bucket/{expected_blob_name}"
    with patch(
        "airflow.providers.google.cloud.operators.dataproc.DataprocCreateBatchOperator.execute"
    ) as mock_super_execute:
        operator.execute(context)
        mock_storage_client_cls.assert_called_once()
        mock_storage_client.bucket.assert_called_once_with("my-example-bucket")
        mock_bucket.blob.assert_called_once_with(expected_blob_name)
        mock_blob.upload_from_string.assert_called_once_with("SELECT 1;")
        assert (
            operator.batch["spark_sql_batch"]["query_file_uri"]
            == expected_gcs_uri
        )
        mock_super_execute.assert_called_once_with(context)


def test_create_dataproc_create_batch_operator_task_inline_sql():
    """Tests factory instantiates operator with basic fields."""
    import pendulum
    from airflow.models import DAG

    action = MagicMock()
    action.type = "sql"
    action.name = "my_sql_action"
    action.query = "SELECT 2;"
    action.filename = None
    action.depsBucket = None
    action.region = "us-central1"
    action.executionTimeout = None
    action.impersonationChain = None
    action.labels = {"label1": "value1"}
    action.triggerRule = "all_success"
    action.config.resourceProfile.runtimeConfig = {}
    action.config.resourceProfile.environmentConfig = {}
    action.params = None
    pipeline = MagicMock()
    pipeline.defaults.cloudDefault.project = "my-pipeline-project"
    pipeline.metadata.pipelineId = "overridden_dag_id_in_api_py"
    dag = DAG(
        dag_id="test_dag",
        default_args={},
        start_date=pendulum.today("UTC").add(days=-1),
        schedule="@daily",
    )
    with patch.dict(os.environ, {"GCS_BUCKET": "env-bucket"}):
        operator = create_dataproc_create_batch_operator_task(
            action, pipeline, dag
        )
        DataprocCreateBatchInlineSqlOperator = (
            get_dataproc_create_batch_inline_sql_operator_class()
        )
        assert isinstance(operator, DataprocCreateBatchInlineSqlOperator)
        assert operator.task_id == "my_sql_action"
        assert operator.query == "SELECT 2;"
        assert operator.gcs_bucket == "env-bucket"


@patch("google.cloud.storage.Client")
def test_dataproc_submit_job_inline_sql_operator_execute(
    mock_storage_client_cls,
):
    """Tests operator uploads query to hashed path derived from dag_id."""
    import pendulum
    from airflow.models import DAG

    mock_storage_client = mock_storage_client_cls.return_value
    mock_bucket = MagicMock()
    mock_blob = MagicMock()
    mock_storage_client.bucket.return_value = mock_bucket
    mock_bucket.blob.return_value = mock_blob
    dag_notes = json.dumps(
        {
            "op_bundle": "my_bundle",
            "op_version": "v123",
            "op_pipeline": "my_pipeline",
        }
    )
    test_dag = DAG(
        dag_id="my_bundle__v__v123__my_pipeline",
        doc_md=dag_notes,
        start_date=pendulum.today("UTC").add(days=-1),
        schedule="@daily",
    )
    job_config = {"spark_sql_job": {}}
    DataprocSubmitJobInlineSqlOperator = (
        get_dataproc_submit_job_inline_sql_operator_class()
    )
    operator = DataprocSubmitJobInlineSqlOperator(
        task_id="test_action",
        query="SELECT 5;",
        gcs_bucket="my-example-bucket",
        region="us-central1",
        project_id="my-project",
        job=job_config,
        dag=test_dag,
    )
    mock_ti = MagicMock()
    mock_ti.try_number = 2
    mock_dag_run = MagicMock()
    mock_dag_run.run_id = "manual__2026-05-04T00:00:00+00:00"
    context = {"task_instance": mock_ti, "dag_run": mock_dag_run}
    import hashlib

    expected_hash = hashlib.sha256(operator.query.encode("utf-8")).hexdigest()
    expected_blob_name = (
        f"data/my_bundle/versions/v123/managed-temp/{expected_hash}.sql"
    )
    expected_gcs_uri = f"gs://my-example-bucket/{expected_blob_name}"
    with patch(
        "airflow.providers.google.cloud.operators.dataproc.DataprocSubmitJobOperator.execute"
    ) as mock_super_execute:
        operator.execute(context)
        mock_storage_client_cls.assert_called_once()
        mock_storage_client.bucket.assert_called_once_with("my-example-bucket")
        mock_bucket.blob.assert_called_once_with(expected_blob_name)
        mock_blob.upload_from_string.assert_called_once_with("SELECT 5;")
        assert (
            operator.job["spark_sql_job"]["query_file_uri"] == expected_gcs_uri
        )
        mock_super_execute.assert_called_once_with(context)


def test_create_bq_dts_task():
    """Tests creating BigQuery DTS TaskGroup."""
    import pendulum
    from airflow.models import DAG
    from airflow.utils.task_group import TaskGroup

    action = MagicMock()
    action.name = "my_dts_action"
    action.config.projectId = "dts-proj"
    action.config.location = "dts-loc"
    action.config.transferConfigId = "config-789"
    action.config.runtimeParams = None
    action.config.requestedRunTime = "2026-06-23T00:00:00Z"
    action.config.requestedTimeRange = None
    action.config.impersonationChain = [
        "dts-sa@dts-proj.iam.gserviceaccount.com"
    ]
    action.executionTimeout = "1000s"
    action.triggerRule = "all_success"
    action.type = "sql"
    pipeline = MagicMock()
    pipeline.defaults.cloudDefault.project = "default-proj"
    pipeline.defaults.cloudDefault.region = "default-reg"
    dag = DAG(dag_id="test_dts_dag", start_date=pendulum.today("UTC"))
    task_group = create_bq_dts_task(action, pipeline, dag)
    assert isinstance(task_group, TaskGroup)
    assert task_group.group_id == "my_dts_action"
    children = task_group.children
    assert len(children) == 2
    assert "my_dts_action.my_dts_action_start" in children
    assert "my_dts_action.my_dts_action_sensor" in children
    start_task = children["my_dts_action.my_dts_action_start"]
    sensor_task = children["my_dts_action.my_dts_action_sensor"]
    assert start_task.transfer_config_id == "config-789"
    assert start_task.project_id == "dts-proj"
    assert start_task.location == "dts-loc"
    assert start_task.requested_run_time == {"seconds": 1782172800}
    assert start_task.impersonation_chain == [
        "dts-sa@dts-proj.iam.gserviceaccount.com"
    ]
    assert sensor_task.transfer_config_id == "config-789"
    assert sensor_task.project_id == "dts-proj"
    assert sensor_task.run_id == (
        "{{ task_instance.xcom_pull("
        "task_ids='my_dts_action.my_dts_action_start', key='run_id') }}"
    )


def test_create_bq_dts_task_with_time_range():
    """Tests creating BigQuery DTS TaskGroup with requestedTimeRange."""
    import pendulum
    from airflow.models import DAG
    from airflow.utils.task_group import TaskGroup

    action = MagicMock()
    action.name = "my_dts_action_range"
    action.config.projectId = "dts-proj"
    action.config.location = "dts-loc"
    action.config.transferConfigId = "config-789"
    action.config.runtimeParams = None
    action.config.requestedRunTime = None
    action.config.requestedTimeRange = {
        "start_time": "2026-06-20T00:00:00Z",
        "end_time": "2026-06-21T00:00:00Z",
    }
    action.config.impersonationChain = [
        "dts-sa@dts-proj.iam.gserviceaccount.com"
    ]
    action.executionTimeout = "1000s"
    action.triggerRule = "all_success"
    action.type = "pyspark"
    pipeline = MagicMock()
    pipeline.defaults.cloudDefault.project = "default-proj"
    pipeline.defaults.cloudDefault.region = "default-reg"
    dag = DAG(dag_id="test_dts_dag_range", start_date=pendulum.today("UTC"))
    task_group = create_bq_dts_task(action, pipeline, dag)
    assert isinstance(task_group, TaskGroup)
    assert task_group.group_id == "my_dts_action_range"
    children = task_group.children
    assert len(children) == 2
    assert "my_dts_action_range.my_dts_action_range_start" in children
    assert "my_dts_action_range.my_dts_action_range_sensor" in children
    start_task = children["my_dts_action_range.my_dts_action_range_start"]
    sensor_task = children["my_dts_action_range.my_dts_action_range_sensor"]
    assert start_task.transfer_config_id == "config-789"
    assert start_task.project_id == "dts-proj"
    assert start_task.location == "dts-loc"
    assert start_task.requested_run_time is None
    assert start_task.requested_time_range == {
        "start_time": {"seconds": 1781913600},
        "end_time": {"seconds": 1782000000},
    }
    assert start_task.impersonation_chain == [
        "dts-sa@dts-proj.iam.gserviceaccount.com"
    ]
    assert sensor_task.transfer_config_id == "config-789"
    assert sensor_task.project_id == "dts-proj"
    assert sensor_task.run_id == (
        "{{ task_instance.xcom_pull("
        "task_ids='my_dts_action_range.my_dts_action_range_start', "
        "key='run_id') }}"
    )


def test_create_bq_dts_task_defaults():
    """Tests creating BigQuery DTS TaskGroup defaults requested_run_time."""
    import pendulum
    from airflow.models import DAG

    action = MagicMock()
    action.name = "my_dts_action_defaults"
    action.config.projectId = None
    action.config.location = None
    action.config.transferConfigId = "config-123"
    action.config.runtimeParams = None
    action.config.requestedRunTime = None
    action.config.requestedTimeRange = None
    action.config.impersonationChain = None
    action.executionTimeout = None
    action.triggerRule = "all_success"
    action.type = "notebook"
    pipeline = MagicMock()
    pipeline.defaults.cloudDefault.project = "default-proj"
    pipeline.defaults.cloudDefault.region = "default-reg"
    dag = DAG(dag_id="test_dts_dag_defaults", start_date=pendulum.today("UTC"))
    task_group = create_bq_dts_task(action, pipeline, dag)
    start_task = task_group.children[
        "my_dts_action_defaults.my_dts_action_defaults_start"
    ]
    assert start_task.requested_run_time == {
        "seconds": (
            "{{ logical_date.timestamp() | int if logical_date is defined "
            "else execution_date.timestamp() | int }}"
        )
    }
    assert start_task.requested_time_range is None


def test_create_local_dataform_task_with_labels_and_params():
    """Tests creating local Dataform task with labels and params."""
    import pendulum
    from airflow.models import DAG
    from airflow.providers.cncf.kubernetes.operators.pod import (
        KubernetesPodOperator,
    )

    action = MagicMock()
    action.name = "my_dataform_action"
    action.labels = {"env": "prod", "team": "data"}
    action.params = {"run_date": "2024-01-01", "id": "123"}
    action.executionTimeout = "600s"
    action.triggerRule = "all_success"
    pipeline = MagicMock()
    dag = DAG(dag_id="test_dataform_dag", start_date=pendulum.today("UTC"))
    gcs_path = "gs://example-bucket/workspace"
    task = create_local_dataform_task(action, pipeline, gcs_path, dag)
    assert isinstance(task, KubernetesPodOperator)
    assert task.task_id == "my_dataform_action"
    assert task.labels == {"env": "prod", "team": "data"}
    expected_cmd = (
        "gcloud storage cp --recursive $GCS_BUCKET_PATH/* . && "
        "dataform run --timeout=60s --job-labels=env=prod,team=data "
        "--vars=run_date=2024-01-01,id=123"
    )
    assert task.arguments == [expected_cmd]
    assert task.cmds == ["/bin/sh", "-c"]


def test_create_local_dataform_task_without_labels_and_params():
    """Tests creating local Dataform task without labels and params."""
    import pendulum
    from airflow.models import DAG
    from airflow.providers.cncf.kubernetes.operators.pod import (
        KubernetesPodOperator,
    )

    action = MagicMock()
    action.name = "my_dataform_action"
    action.labels = None
    action.params = None
    action.executionTimeout = None
    action.triggerRule = "all_success"
    pipeline = MagicMock()
    dag = DAG(dag_id="test_dataform_dag", start_date=pendulum.today("UTC"))
    gcs_path = "gs://example-bucket/workspace"
    task = create_local_dataform_task(action, pipeline, gcs_path, dag)
    assert isinstance(task, KubernetesPodOperator)
    assert task.task_id == "my_dataform_action"
    assert task.labels == {}
    expected_cmd = (
        "gcloud storage cp --recursive $GCS_BUCKET_PATH/* . && "
        "dataform run --timeout=60s"
    )
    assert task.arguments == [expected_cmd]


def test_create_ai_task_vertex_upload_model():
    """Tests creating Vertex AI UploadModelOperator from AIAction."""
    import pendulum
    from airflow.models import DAG
    from airflow.providers.google.cloud.operators.vertex_ai import (
        model_service,
    )

    UploadModelOperator = model_service.UploadModelOperator

    action = MagicMock()
    action.name = "upload_model_vertex"
    action.provider = "agent_platform"
    action.ai_action_type = "model_upload"
    action.executionTimeout = "600s"
    action.triggerRule = "all_success"
    action.config.model_name = "Predictor"
    action.config.description = "Model"
    action.config.model_artifact_uri = "gs://my-bucket/models/spark_rf_model"
    action.config.serving_container_image_uri = (
        "us-docker.pkg.dev/vertex-ai/prediction/sklearn-cpu.1-4:latest"
    )
    action.config.project_id = "custom-project"
    action.config.location = "us-central1"
    action.labels = {"model_type": "rf", "env": "prod"}
    pipeline = MagicMock()
    pipeline.defaults.cloudDefault.project = "default-project"
    pipeline.defaults.cloudDefault.region = "default-region"
    dag = DAG(dag_id="test_ai_dag", start_date=pendulum.today("UTC"))
    task = create_ai_task(action, pipeline, dag)
    assert isinstance(task, UploadModelOperator)
    assert task.task_id == "upload_model_vertex"
    assert task.project_id == "custom-project"
    assert task.region == "us-central1"
    assert task.model == {
        "display_name": "Predictor",
        "artifact_uri": "gs://my-bucket/models/spark_rf_model",
        "container_spec": {
            "image_uri": (
                "us-docker.pkg.dev/vertex-ai/prediction/sklearn-cpu.1-4:latest"
            )
        },
        "description": "Model",
        "labels": {"model_type": "rf", "env": "prod"},
    }
    assert task.trigger_rule == "all_success"


def test_create_ai_task_vertex_batch_inference():
    """Tests creating CreateBatchPredictionJobOperator from AIAction."""
    import pendulum
    from airflow.models import DAG
    from airflow.providers.google.cloud.operators.vertex_ai import (  # noqa: E501
        batch_prediction_job,
    )

    CreateBatchPredictionJobOperator = (
        batch_prediction_job.CreateBatchPredictionJobOperator
    )

    action = MagicMock()
    action.name = "run_vertex_batch_prediction"
    action.provider = "agent_platform"
    action.ai_action_type = "batch_inference"
    action.executionTimeout = "1200s"
    action.triggerRule = "all_success"
    action.config.job_display_name = "days_batch_pred"
    action.config.model_name = "projects/123/locations/us-central1/models/456"
    action.config.instances_format = "bigquery"
    action.config.predictions_format = "bigquery"
    action.config.bigquery_source = "bq://my-proj.mlops.test_data"
    action.config.gcs_source = None
    action.config.bigquery_destination_prefix = "bq://my-proj.mlops"
    action.config.gcs_destination_prefix = None
    action.config.project_id = "custom-project"
    action.config.location = "us-central1"
    action.config.impersonation_chain = [
        "sa@custom-project.iam.gserviceaccount.com"
    ]
    action.labels = {"env": "staging"}
    pipeline = MagicMock()
    pipeline.defaults.cloudDefault.project = "default-project"
    pipeline.defaults.cloudDefault.region = "default-region"
    dag = DAG(dag_id="test_batch_pred_dag", start_date=pendulum.today("UTC"))
    task = create_ai_task(action, pipeline, dag)
    assert isinstance(task, CreateBatchPredictionJobOperator)
    assert task.task_id == "run_vertex_batch_prediction"
    assert task.project_id == "custom-project"
    assert task.region == "us-central1"
    assert task.job_display_name == "days_batch_pred"
    assert task.model_name == "projects/123/locations/us-central1/models/456"
    assert task.instances_format == "bigquery"
    assert task.predictions_format == "bigquery"
    assert task.bigquery_source == "bq://my-proj.mlops.test_data"
    assert task.bigquery_destination_prefix == "bq://my-proj.mlops"
    assert task.machine_type == "n1-standard-4"
    assert task.impersonation_chain == [
        "sa@custom-project.iam.gserviceaccount.com"
    ]
    assert task.labels == {"env": "staging"}


def test_create_ai_task_unsupported_provider():
    """Tests that unsupported AI provider raises ValueError."""
    action = MagicMock()
    action.provider = "unsupported_provider"
    pipeline = MagicMock()
    dag = MagicMock()
    with pytest.raises(ValueError, match="Unsupported AI provider"):
        create_ai_task(action, pipeline, dag)


def test_create_ai_task_unsupported_action_type():
    """Tests that unsupported agent_platform action type raises ValueError."""
    action = MagicMock()
    action.provider = "agent_platform"
    action.ai_action_type = "unsupported_type"
    pipeline = MagicMock()
    dag = MagicMock()
    with pytest.raises(
        ValueError, match="Unsupported agent_platform action type"
    ):
        create_ai_task(action, pipeline, dag)


@patch(
    "orchestration_pipelines_lib.dag_generator.airflow_adapters.common_utils.task_utils.gcs_utils.upload_run_notebook_if_needed"
)
@patch(
    "orchestration_pipelines_lib.dag_generator.airflow_adapters.common_utils.task_utils.gcs_utils.get_run_notebook_gcs_path",
    return_value="gs://fake/notebook_runner.py",
)
def test_dataproc_ephemeral_task_setup_and_teardown(mock_get_path, mock_upload):
    """Tests dataproc_ephemeral_task configures setup and teardown."""
    import pendulum
    from airflow.models import DAG
    from airflow.providers.google.cloud.operators.dataproc import (
        DataprocCreateClusterOperator,
        DataprocDeleteClusterOperator,
        DataprocSubmitJobOperator,
    )
    from airflow.utils.task_group import TaskGroup

    action = MagicMock()
    action.name = "ephemeral_action"
    action.type = "pyspark"
    action.config.cluster_config = {"master_config": {}}
    action.config.project_id = "test-project"
    action.config.region = "us-central1"
    action.config.cluster_name = "test-cluster"
    action.config.properties = {}
    action.depsBucket = None
    action.pyFiles = None
    action.impersonationChain = None
    action.triggerRule = "all_success"
    action.labels = {"key": "val"}
    action.executionTimeout = None
    dag = DAG(
        dag_id="test_dag_ephemeral",
        start_date=pendulum.today("UTC").add(days=-1),
        schedule="@daily",
    )
    tg = dataproc_ephemeral_task(action, dag)
    assert isinstance(tg, TaskGroup)
    create_task = dag.get_task(f"{action.name}.{action.name}_create_cluster")
    submit_task = dag.get_task(f"{action.name}.{action.name}_submit_job")
    delete_task = dag.get_task(f"{action.name}.{action.name}_delete_cluster")
    assert isinstance(create_task, DataprocCreateClusterOperator)
    assert isinstance(submit_task, DataprocSubmitJobOperator)
    assert isinstance(delete_task, DataprocDeleteClusterOperator)
    assert create_task.is_setup
    assert delete_task.is_teardown


MODULE_PATH = "orchestration_pipelines_lib.dag_generator.airflow_adapters.common_utils.task_utils"  # noqa: E501


@pytest.fixture
def mock_utils():
    """Mocks common_utils.utils in task_utils."""
    with patch(f"{MODULE_PATH}.utils", autospec=True) as mock:
        yield mock


@pytest.fixture
def mock_dag():
    """Mocks Airflow DAG object."""
    return Mock()


@pytest.fixture
def sample_dag():
    """Returns a real Airflow DAG instance for operator tests."""
    import pendulum
    from airflow.models import DAG

    return DAG(
        dag_id="test_dag",
        start_date=pendulum.today("UTC"),
        schedule="@daily",
    )


@pytest.fixture
def pipeline() -> PipelineModel:
    """Returns a PipelineModel with cloudDefault defaults configured."""
    return PipelineModel(
        defaults=DefaultsModel(
            cloudDefault=CloudDefaultsModel(
                project="default-gcp-project",
                region="europe-west1",
            )
        ),
        metadata=MetaDataModel(
            pipelineId="test-pipeline",
            description="Test pipeline",
            owner="test-owner",
        ),
        runner=RunnerType.AIRFLOW,
        triggers=[],
        actions=[],
    )


@pytest.fixture
def python_script_action_full() -> PythonScriptActionModel:
    """Returns a PythonScriptActionModel with all fields populated."""
    return PythonScriptActionModel(
        name="my_python_task",
        type="script",
        filename="my_script.py",
        dependsOn=None,
        executionTimeout="10m",
        triggerRule="all_success",
        config=PythonScriptConfigurationModel(
            pythonCallable="my_module.my_function",
            opKwargs={"param1": "value1", "param2": "value2"},
        ),
    )


@pytest.fixture
def python_script_action_minimal(
    python_script_action_full: PythonScriptActionModel,
) -> PythonScriptActionModel:
    """Returns a PythonScriptActionModel with minimal fields."""
    return replace(
        python_script_action_full,
        name="my_minimal_task",
        filename="minimal_script.py",
        executionTimeout=None,
        config=PythonScriptConfigurationModel(
            pythonCallable="minimal.func",
            opKwargs=None,
        ),
    )


def test_creates_operator_with_full_params(
    python_script_action_full,
    pipeline,
    sample_dag,
):
    """Tests creating Python script task with all parameters provided."""
    result = create_python_script_task(
        get_operator=adapter_imports.get_python_operator,
        action=python_script_action_full,
        pipeline=pipeline,
        dag=sample_dag,
    )

    assert isinstance(result, adapter_imports.get_python_operator())
    assert result.task_id == "my_python_task"
    assert callable(result.python_callable)
    assert result.op_kwargs == {"param1": "value1", "param2": "value2"}
    assert result.execution_timeout == timedelta(minutes=10)
    assert result.trigger_rule == "all_success"
    assert result.doc_md == json.dumps({"op_action_name": "my_python_task"})
    assert result.dag == sample_dag


def test_creates_operator_with_minimal_params(
    python_script_action_minimal,
    pipeline,
    sample_dag,
):
    """Tests creating Python script task with minimal configuration."""
    result = create_python_script_task(
        get_operator=adapter_imports.get_python_operator,
        action=python_script_action_minimal,
        pipeline=pipeline,
        dag=sample_dag,
    )

    assert isinstance(result, adapter_imports.get_python_operator())
    assert result.op_kwargs == {}
    assert result.execution_timeout is None
    assert result.trigger_rule == "all_success"


def test_raises_runtime_error_on_operator_exception(
    python_script_action_full,
    pipeline,
    mock_dag,
):
    """Tests RuntimeError wrapping on operator creation failure."""
    with pytest.raises(
        RuntimeError,
        match=(
            "Failed to create task for action 'my_python_task' "
            "from 'my_module.my_function':"
        ),
    ) as exc_info:
        create_python_script_task(
            get_operator=adapter_imports.get_python_operator,
            action=python_script_action_full,
            pipeline=pipeline,
            dag=mock_dag,
        )

    assert isinstance(exc_info.value.__cause__, TypeError)


def test_get_action_retry_kwargs_none():
    """Tests get_action_retry_kwargs when action has no retryPolicy."""
    action = MagicMock(retryPolicy=None)

    result = get_action_retry_kwargs(action)

    assert result == {}


def test_get_action_retry_kwargs_max_retries_only():
    """Tests get_action_retry_kwargs with maxRetries only."""
    action = MagicMock()
    action.retryPolicy.maxRetries = 3
    action.retryPolicy.fixedDelay = None

    result = get_action_retry_kwargs(action)

    assert result == {
        "retries": 3,
        "_op_custom_retry_policy": action.retryPolicy,
    }


def test_get_action_retry_kwargs_with_fixed_delay():
    """Tests get_action_retry_kwargs with maxRetries and fixedDelay."""
    action = MagicMock()
    action.retryPolicy.maxRetries = 2
    action.retryPolicy.fixedDelay.retryDelay = "30s"

    result = get_action_retry_kwargs(action)

    assert result == {
        "retries": 2,
        "retry_delay": timedelta(seconds=30),
        "_op_custom_retry_policy": action.retryPolicy,
    }


def test_create_bq_operation_task_with_retry_policy():
    """Tests that create_bq_operation_task sets retries and retry_delay."""
    import pendulum
    from airflow.models import DAG

    action = MagicMock()
    action.name = "test_bq_retry"
    action.query = "SELECT 1"
    action.filename = None
    action.params = None
    action.labels = None
    action.config.destinationTable = None
    action.config.location = "US"
    action.executionTimeout = None
    action.impersonationChain = None
    action.triggerRule = "all_success"
    action.type = "sql"
    action.retryPolicy.maxRetries = 4
    action.retryPolicy.fixedDelay.retryDelay = "1m"
    pipeline = MagicMock()
    pipeline.defaults.cloudDefault.project = "test-project"
    dag = DAG(dag_id="test_dag_bq_retry", start_date=pendulum.today("UTC"))

    task = create_bq_operation_task(action, pipeline, dag=dag)

    assert task.retries == 4
    assert task.retry_delay == timedelta(minutes=1)
    assert task._op_custom_retry_policy == action.retryPolicy


def test_runtime_wrapper_imports_and_filters_kwargs(
    python_script_action_full,
    pipeline,
    sample_dag,
    mock_utils,
):
    """Tests runtime wrapper imports callable and filters kwargs."""
    task = create_python_script_task(
        get_operator=adapter_imports.get_python_operator,
        action=python_script_action_full,
        pipeline=pipeline,
        dag=sample_dag,
    )
    mock_imported_callable = Mock()
    mock_utils.import_callable.return_value = mock_imported_callable

    wrapper_result = task.python_callable(
        param1="passed_val_1",
        param2="passed_val_2",
        airflow_context_var="should_be_ignored",
    )

    mock_utils.import_callable.assert_called_once_with(
        "my_script.py", "my_module.my_function"
    )
    mock_imported_callable.assert_called_once_with(
        param1="passed_val_1", param2="passed_val_2"
    )
    assert wrapper_result == mock_imported_callable.return_value


@pytest.fixture
def venv_action_req_path() -> PythonVirtualenvActionModel:
    """Returns a virtualenv action configured with requirementsPath."""
    return PythonVirtualenvActionModel(
        name="my_venv_task_path",
        type="python-virtual-env",
        filename="my_script.py",
        dependsOn=None,
        executionTimeout="15m",
        triggerRule="all_done",
        config=PythonVirtualenvConfigurationModel(
            pythonCallable="my_module.my_func",
            opKwargs={"param": "value"},
            requirementsPath="/path/to/requirements.txt",
            requirements=["pandas", "numpy"],
            systemSitePackages=True,
        ),
    )


@pytest.fixture
def venv_action_req_list(
    venv_action_req_path: PythonVirtualenvActionModel,
) -> PythonVirtualenvActionModel:
    """Returns a virtualenv action configured with requirements list."""
    return replace(
        venv_action_req_path,
        name="my_venv_task_list",
        executionTimeout="10m",
        triggerRule="all_success",
        config=replace(
            venv_action_req_path.config,
            requirementsPath=None,
            requirements=["pandas==2.0.0", "requests"],
            systemSitePackages=False,
        ),
    )


@pytest.fixture
def venv_action_minimal(
    venv_action_req_path: PythonVirtualenvActionModel,
) -> PythonVirtualenvActionModel:
    """Returns a virtualenv action with minimal configuration."""
    return replace(
        venv_action_req_path,
        name="my_venv_task_min",
        executionTimeout=None,
        triggerRule="all_success",
        config=PythonVirtualenvConfigurationModel(
            pythonCallable="my_module.my_func",
        ),
    )


def _dummy_venv_callable():
    """Dummy callable for PythonVirtualenvOperator tests."""


def test_creates_venv_task_prefers_requirements_path(
    venv_action_req_path,
    pipeline,
    sample_dag,
    mock_utils,
):
    """Tests creating virtualenv task prefers requirementsPath."""
    mock_utils.import_callable.return_value = _dummy_venv_callable

    result = create_python_virtualenv_task(
        get_operator=adapter_imports.get_python_virtualenv_operator,
        action=venv_action_req_path,
        pipeline=pipeline,
        dag=sample_dag,
    )

    assert isinstance(result, adapter_imports.get_python_virtualenv_operator())
    mock_utils.import_callable.assert_called_once_with(
        "my_script.py", "my_module.my_func"
    )
    assert result.task_id == "my_venv_task_path"
    assert result.python_callable is _dummy_venv_callable
    assert result.op_kwargs == {"param": "value"}
    assert result.requirements == ["/path/to/requirements.txt"]
    assert result.system_site_packages is True
    assert result.execution_timeout == timedelta(minutes=15)
    assert result.trigger_rule == "all_done"
    assert result.doc_md == json.dumps({"op_action_name": "my_venv_task_path"})
    assert result.dag == sample_dag


def test_creates_venv_task_uses_requirements_list(
    venv_action_req_list,
    pipeline,
    sample_dag,
    mock_utils,
):
    """Tests creating virtualenv task when requirementsPath is None."""
    mock_utils.import_callable.return_value = _dummy_venv_callable

    result = create_python_virtualenv_task(
        get_operator=adapter_imports.get_python_virtualenv_operator,
        action=venv_action_req_list,
        pipeline=pipeline,
        dag=sample_dag,
    )

    assert result.requirements == ["pandas==2.0.0", "requests"]
    assert result.system_site_packages is False


def test_creates_venv_task_with_minimal_params(
    venv_action_minimal,
    pipeline,
    sample_dag,
    mock_utils,
):
    """Tests creating virtualenv task with minimal configuration."""
    mock_utils.import_callable.return_value = _dummy_venv_callable

    result = create_python_virtualenv_task(
        get_operator=adapter_imports.get_python_virtualenv_operator,
        action=venv_action_minimal,
        pipeline=pipeline,
        dag=sample_dag,
    )

    assert result.op_kwargs == {}
    assert result.requirements == []
    assert result.system_site_packages is False
    assert result.execution_timeout is None
    assert result.trigger_rule == "all_success"


def test_venv_raises_runtime_error_if_not_callable(
    venv_action_req_list,
    pipeline,
    sample_dag,
    mock_utils,
):
    """Tests RuntimeError raised when target does not resolve to a callable."""
    mock_utils.import_callable.return_value = "This is a string, not a function"

    with pytest.raises(RuntimeError) as exc_info:
        create_python_virtualenv_task(
            get_operator=adapter_imports.get_python_virtualenv_operator,
            action=venv_action_req_list,
            pipeline=pipeline,
            dag=sample_dag,
        )

    assert str(exc_info.value) == (
        "Failed to create task for action 'my_venv_task_list' "
        "from 'my_module.my_func': Action my_venv_task_list: "
        "filename my_script.py with callable my_module.my_func "
        "did not resolve to a callable object."
    )
    assert isinstance(exc_info.value.__cause__, ValueError)


def test_venv_raises_runtime_error_on_operator_exception(
    venv_action_req_list,
    pipeline,
    mock_dag,
    mock_utils,
):
    """Tests RuntimeError wrapping on virtualenv operator init failure."""
    mock_utils.import_callable.return_value = _dummy_venv_callable

    with pytest.raises(
        RuntimeError,
        match=(
            "Failed to create task for action 'my_venv_task_list' "
            "from 'my_module.my_func':"
        ),
    ) as exc_info:
        create_python_virtualenv_task(
            get_operator=adapter_imports.get_python_virtualenv_operator,
            action=venv_action_req_list,
            pipeline=pipeline,
            dag=mock_dag,
        )

    assert isinstance(exc_info.value.__cause__, TypeError)


@pytest.fixture
def dbt_action_full() -> DBTActionModel:
    """Returns a DBTActionModel with all optional parameters set."""
    return DBTActionModel(
        name="dbt_full_run",
        type="dbt_pipeline",
        engine="dbt",
        executionMode="local",
        dependsOn=None,
        source=DbtLocalExecutionModel(path="/opt/dbt/my_project"),
        select_models=["my_model+", "other_model"],
        params={"vars": '{"date": "2023-01-01"}'},
        executionTimeout="30m",
        triggerRule="all_success",
    )


@pytest.fixture
def dbt_action_minimal(dbt_action_full: DBTActionModel) -> DBTActionModel:
    """Returns a DBTActionModel with minimal configuration."""
    return replace(
        dbt_action_full,
        name="dbt_minimal_run",
        source=DbtLocalExecutionModel(path="/opt/dbt/minimal_project"),
        select_models=None,
        params={},
        executionTimeout=None,
    )


def test_creates_dbt_task_with_full_params(
    dbt_action_full,
    pipeline,
    sample_dag,
):
    """Tests creating DBT task with all parameters configured."""
    result = create_dbt_task(
        get_operator=adapter_imports.get_python_operator,
        action=dbt_action_full,
        pipeline=pipeline,
        dag=sample_dag,
    )

    assert isinstance(result, adapter_imports.get_python_operator())
    assert result.task_id == "dbt_full_run"
    assert result.python_callable is invoke_dbt_run
    assert result.execution_timeout == timedelta(minutes=30)
    assert result.trigger_rule == "all_success"
    assert result.doc_md == json.dumps({"op_action_name": "dbt_full_run"})
    assert result.dag == sample_dag
    expected_op_kwargs = {
        "project_dir": "/opt/dbt/my_project",
        "profiles_dir": "/opt/dbt/my_project",
        "select_models": ["my_model+", "other_model"],
        "params": {"vars": '{"date": "2023-01-01"}'},
    }
    assert result.op_kwargs == expected_op_kwargs


def test_creates_dbt_task_with_minimal_params(
    dbt_action_minimal,
    pipeline,
    sample_dag,
):
    """Tests creating DBT task with minimal parameters."""
    result = create_dbt_task(
        get_operator=adapter_imports.get_python_operator,
        action=dbt_action_minimal,
        pipeline=pipeline,
        dag=sample_dag,
    )

    assert result.execution_timeout is None
    assert result.trigger_rule == "all_success"
    expected_op_kwargs = {
        "project_dir": "/opt/dbt/minimal_project",
        "profiles_dir": "/opt/dbt/minimal_project",
    }
    assert result.op_kwargs == expected_op_kwargs


def test_dbt_task_raises_runtime_error_on_exception(
    dbt_action_full,
    pipeline,
    mock_dag,
):
    """Tests RuntimeError wrapping when DBT operator fails."""
    with pytest.raises(
        RuntimeError,
        match="Failed to create task for action 'dbt_full_run':",
    ) as exc_info:
        create_dbt_task(
            get_operator=adapter_imports.get_python_operator,
            action=dbt_action_full,
            pipeline=pipeline,
            dag=mock_dag,
        )

    assert isinstance(exc_info.value.__cause__, TypeError)


@pytest.fixture
def orch_action_full() -> OrchestrationPipelineActionModel:
    """Returns an OrchestrationPipelineActionModel with all parameters."""
    return OrchestrationPipelineActionModel(
        name="trigger_target_pipeline",
        type="orchestration_pipeline",
        pipeline_id="target_pipeline_base",
        bundle_id="bundle_xyz",
        wait_for_completion=True,
        dependsOn=None,
        executionTimeout="1h",
        triggerRule="all_success",
    )


@pytest.fixture
def orch_action_minimal(
    orch_action_full: OrchestrationPipelineActionModel,
) -> OrchestrationPipelineActionModel:
    """Returns an OrchestrationPipelineActionModel with minimal parameters."""
    return replace(
        orch_action_full,
        name="trigger_minimal",
        bundle_id=None,
        wait_for_completion=None,
        executionTimeout=None,
    )


def test_creates_orchestration_task_with_full_params(
    orch_action_full,
    pipeline,
    sample_dag,
):
    """Tests creating trigger task with full parameters and Jinja template."""
    expected_template = "{{ resolve_latest_pipeline_dag_id(params.target_pipeline_id, params.bundle_id) }}"  # noqa: E501
    expected_params = {
        "target_pipeline_id": "target_pipeline_base",
        "bundle_id": "bundle_xyz",
    }

    result = create_orchestration_pipeline_trigger_task(
        get_operator=adapter_imports.get_trigger_dagrun_operator,
        action=orch_action_full,
        pipeline=pipeline,
        dag=sample_dag,
    )

    assert isinstance(result, adapter_imports.get_trigger_dagrun_operator())
    assert result.task_id == "trigger_target_pipeline"
    assert result.wait_for_completion is True
    assert result.execution_timeout == timedelta(hours=1)
    assert result.trigger_rule == "all_success"
    assert result.doc_md == json.dumps(
        {"op_action_name": "trigger_target_pipeline"}
    )
    assert result.dag == sample_dag
    assert result.trigger_dag_id == expected_template
    assert dict(result.params) == expected_params


def test_creates_orchestration_task_with_minimal_params(
    orch_action_minimal,
    pipeline,
    sample_dag,
):
    """Tests creating trigger task with fallback defaults."""
    result = create_orchestration_pipeline_trigger_task(
        get_operator=adapter_imports.get_trigger_dagrun_operator,
        action=orch_action_minimal,
        pipeline=pipeline,
        dag=sample_dag,
    )

    assert result.wait_for_completion is False
    assert result.execution_timeout is None
    assert result.trigger_rule == "all_success"
    assert result.params["bundle_id"] is None


def test_orchestration_task_raises_runtime_error_on_exception(
    orch_action_full,
    pipeline,
    mock_dag,
):
    """Tests RuntimeError wrapping in trigger task creation."""
    with pytest.raises(
        RuntimeError,
        match="Failed to create task for action 'trigger_target_pipeline':",
    ) as exc_info:
        create_orchestration_pipeline_trigger_task(
            get_operator=adapter_imports.get_trigger_dagrun_operator,
            action=orch_action_full,
            pipeline=pipeline,
            dag=mock_dag,
        )

    assert isinstance(exc_info.value.__cause__, TypeError)


@pytest.fixture
def dataform_action_full() -> DataformActionModel:
    """Returns a fully configured DataformActionModel."""
    return DataformActionModel(
        name="dataform_run_full",
        type="dataform_pipeline",
        executionMode="service",
        dependsOn=None,
        executionTimeout="45m",
        triggerRule="all_success",
        dataformServiceConfig=DataformServiceModel(
            repository_id="my_dataform_repo",
            workflow_invocation={
                "compilationResult": "projects/.../compilationResults/123"
            },
        ),
    )


@pytest.fixture
def dataform_action_minimal(
    dataform_action_full: DataformActionModel,
) -> DataformActionModel:
    """Returns a DataformActionModel with minimal optional parameters."""
    return replace(
        dataform_action_full,
        name="dataform_run_minimal",
        executionTimeout=None,
        dataformServiceConfig=DataformServiceModel(
            repository_id="my_minimal_repo",
            workflow_invocation={
                "compilationResult": "projects/.../compilationResults/456"
            },
        ),
    )


def test_creates_dataform_task_with_full_params(
    dataform_action_full,
    pipeline,
    sample_dag,
):
    """Tests creating service Dataform task with all parameters configured."""
    from airflow.providers.google.cloud.operators.dataform import (
        DataformCreateWorkflowInvocationOperator,
    )

    result = create_service_dataform_task(
        action=dataform_action_full, pipeline=pipeline, dag=sample_dag
    )

    assert isinstance(result, DataformCreateWorkflowInvocationOperator)
    assert result.task_id == "dataform_run_full"
    assert result.project_id == "default-gcp-project"
    assert result.region == "europe-west1"
    assert result.execution_timeout == timedelta(minutes=45)
    assert result.repository_id == "my_dataform_repo"
    assert result.workflow_invocation == {
        "compilationResult": "projects/.../compilationResults/123"
    }
    assert result.trigger_rule == "all_success"
    assert result.doc_md == json.dumps({"op_action_name": "dataform_run_full"})
    assert result.dag == sample_dag


def test_creates_dataform_task_with_minimal_params(
    dataform_action_minimal,
    pipeline,
    sample_dag,
):
    """Tests creating service Dataform task with minimal parameters."""
    result = create_service_dataform_task(
        action=dataform_action_minimal,
        pipeline=pipeline,
        dag=sample_dag,
    )

    assert result.execution_timeout is None
    assert result.trigger_rule == "all_success"


def test_create_dataform_task_delegates_to_local_when_execution_mode_is_local(
    dataform_action_full, pipeline, sample_dag
):
    """Tests create_dataform_task creates local pod operator in local mode."""
    from airflow.providers.cncf.kubernetes.operators.pod import (
        KubernetesPodOperator,
    )

    mock_variable = Mock()
    mock_variable.get.return_value = "gs://override-bucket/path"
    mock_get_variable = Mock(return_value=mock_variable)
    action = replace(
        dataform_action_full,
        name="local_dataform_action",
        executionMode="local",
        dataform_project_path="gs://default-bucket/path",
    )

    result = create_dataform_task(
        mock_get_variable, action, pipeline, sample_dag
    )

    mock_get_variable.assert_called_once_with()
    mock_variable.get.assert_called_once_with(
        "dataform_gcs_path", "gs://default-bucket/path"
    )
    assert isinstance(result, KubernetesPodOperator)
    assert result.task_id == "local_dataform_action"
    assert result.dag == sample_dag


def test_create_dataform_task_delegates_to_service_in_service_mode(
    dataform_action_full, pipeline, sample_dag
):
    """Tests create_dataform_task creates service operator in service mode."""
    from airflow.providers.google.cloud.operators.dataform import (
        DataformCreateWorkflowInvocationOperator,
    )

    mock_get_variable = Mock()

    result = create_dataform_task(
        mock_get_variable, dataform_action_full, pipeline, sample_dag
    )

    mock_get_variable.assert_not_called()
    assert isinstance(result, DataformCreateWorkflowInvocationOperator)
    assert result.task_id == "dataform_run_full"
    assert result.repository_id == "my_dataform_repo"
    assert result.dag == sample_dag


@pytest.fixture
def config_with_values() -> DataformServiceModel:
    """Returns an action configuration object with explicit values set."""
    return DataformServiceModel(
        workflow_invocation={},
        region="us-central1",
        project_id="specific-gcp-project",
    )


@pytest.fixture
def config_missing_values(
    config_with_values: DataformServiceModel,
) -> DataformServiceModel:
    """Returns an action configuration object with missing/empty values."""
    return replace(
        config_with_values,
        region=None,
        project_id="",
    )


def test_returns_value_from_config_obj_when_present(
    config_with_values, pipeline
):
    """Tests _get_config_or_default returns value from config when present."""
    result = _get_config_or_default(
        config_obj=config_with_values,
        pipeline=pipeline,
        action_attribute="region",
    )

    assert result == "us-central1"


def test_falls_back_to_pipeline_when_value_is_falsy(
    config_missing_values, pipeline
):
    """Tests fallback to pipeline when config value is None or deleted."""
    result = _get_config_or_default(
        config_obj=config_missing_values,
        pipeline=pipeline,
        action_attribute="region",
    )

    assert result == "europe-west1"


def test_falls_back_using_custom_pipeline_attribute(
    config_missing_values, pipeline
):
    """Tests fallback using custom pipeline_attribute when config is empty."""
    result = _get_config_or_default(
        config_obj=config_missing_values,
        pipeline=pipeline,
        action_attribute="project_id",
        pipeline_attribute="project",
    )

    assert result == "default-gcp-project"


def test_handles_completely_missing_attribute(config_missing_values, pipeline):
    """Tests fallback when attribute does not exist on config object."""
    result = _get_config_or_default(
        config_obj=config_missing_values,
        pipeline=pipeline,
        action_attribute="project",
    )

    assert result == "default-gcp-project"


def test_upload_inline_query_to_gcs_raises_when_bucket_missing(sample_dag):
    """Tests ValueError raised when gcs_bucket is empty."""
    with pytest.raises(ValueError, match="GCS bucket must be specified"):
        _upload_inline_query_to_gcs(sample_dag, "SELECT 1", "", Mock())


@pytest.fixture
def mock_upload_inline_query():
    """Mocks _upload_inline_query_to_gcs in task_utils."""
    with patch(
        f"{MODULE_PATH}._upload_inline_query_to_gcs",
        return_value="gs://bucket/query.sql",
        autospec=True,
    ) as mock:
        yield mock


@pytest.mark.usefixtures("mock_upload_inline_query")
def test_create_batch_inline_sql_execute_raises_on_invalid_batch(sample_dag):
    """Tests execute re-raises AttributeError when batch is not a dict."""
    operator_cls = get_dataproc_create_batch_inline_sql_operator_class()
    operator = operator_cls(
        task_id="test_batch_op",
        query="SELECT 1;",
        gcs_bucket="bucket",
        region="us-central1",
        project_id="proj",
        batch={},
        dag=sample_dag,
    )
    operator.batch = object()

    with pytest.raises(AttributeError):
        operator.execute({})


@pytest.mark.usefixtures("mock_upload_inline_query")
def test_submit_job_inline_sql_execute_raises_on_invalid_job(sample_dag):
    """Tests execute re-raises AttributeError when job is not a dict."""
    operator_cls = get_dataproc_submit_job_inline_sql_operator_class()
    operator = operator_cls(
        task_id="test_job_op",
        query="SELECT 1;",
        gcs_bucket="bucket",
        region="us-central1",
        project_id="proj",
        job={},
        dag=sample_dag,
    )
    operator.job = object()

    with pytest.raises(AttributeError):
        operator.execute({})


@pytest.fixture
def serverless_sql_action() -> DataprocOperatorActionModel:
    """Returns a serverless SQL DataprocOperatorActionModel."""
    return DataprocOperatorActionModel(
        name="sql_batch_task",
        type="sql",
        region="us-central1",
        engine=EngineModel(engineType="dataproc-serverless"),
        params={"var1": "val1"},
        query=None,
        filename="gs://bucket/script.sql",
        config=DataprocCreateBatchOperatorConfigurationModel(
            resourceProfile=ResourceProfile(
                runtimeConfig={},
                environmentConfig={},
            )
        ),
        dependsOn=None,
        executionTimeout=None,
        triggerRule="all_success",
        labels={},
    )


def test_create_batch_operator_sql_with_params_and_filename(
    serverless_sql_action, pipeline, sample_dag
):
    """Tests batch SQL task with query_variables and filename."""
    task = create_dataproc_create_batch_operator_task(
        serverless_sql_action, pipeline, dag=sample_dag
    )

    assert task.batch["spark_sql_batch"]["query_variables"] == {"var1": "val1"}
    assert (
        task.batch["spark_sql_batch"]["query_file_uri"]
        == "gs://bucket/script.sql"
    )


def test_create_batch_operator_task_raises_runtime_error_on_exception(
    serverless_sql_action, pipeline, mock_dag
):
    """Tests RuntimeError wrapping when DataprocCreateBatchOperator fails."""
    with pytest.raises(
        RuntimeError, match="Failed to create task for action 'sql_batch_task'"
    ) as exc_info:
        create_dataproc_create_batch_operator_task(
            serverless_sql_action, pipeline, dag=mock_dag
        )

    assert isinstance(exc_info.value.__cause__, TypeError)


@pytest.fixture
def bq_operation_action() -> BqOperationActionModel:
    """Returns a BqOperationActionModel with query parameters."""
    return BqOperationActionModel(
        name="bq_param_task",
        type="operation",
        engine="bq",
        filename=None,
        query="SELECT @param1",
        params={"param1": "value1"},
        labels={"env": "dev"},
        config=BqOperationConfigurationModel(
            location="US",
            destinationTable=None,
        ),
        dependsOn=None,
        executionTimeout=None,
        triggerRule="all_success",
    )


def test_create_bq_operation_task_with_params(
    bq_operation_action, pipeline, sample_dag
):
    """Tests create_bq_operation_task builds queryParameters from params."""
    task = create_bq_operation_task(
        bq_operation_action, pipeline, dag=sample_dag
    )

    query_params = task.configuration["query"]["queryParameters"]
    assert query_params == [
        {
            "name": "param1",
            "parameterType": {"type": "STRING"},
            "parameterValue": {"value": "value1"},
        }
    ]


def test_create_bq_operation_task_raises_on_invalid_destination_table(
    bq_operation_action, pipeline, sample_dag
):
    """Tests RuntimeError raised when destinationTable is invalid."""
    action = replace(
        bq_operation_action,
        name="bq_invalid_dest",
        config=BqOperationConfigurationModel(
            location="US",
            destinationTable="only_two.parts",
        ),
    )

    with pytest.raises(
        RuntimeError,
        match=(
            "Failed to create task for action 'bq_invalid_dest': "
            "destinationTable should be"
        ),
    ) as exc_info:
        create_bq_operation_task(action, pipeline, dag=sample_dag)

    assert isinstance(exc_info.value.__cause__, ValueError)


@pytest.fixture
def ephemeral_sql_action() -> DataprocOperatorActionModel:
    """Returns an ephemeral cluster SQL DataprocOperatorActionModel."""
    return DataprocOperatorActionModel(
        name="ephemeral_sql",
        type="sql",
        region="us-central1",
        engine=EngineModel(engineType="dataproc-gce", clusterMode="ephemeral"),
        depsBucket="my-deps-bucket",
        params={"p1": "v1"},
        query=None,
        filename="gs://bucket/query.sql",
        config=DataprocEphemeralConfigurationModel(
            region="us-central1",
            project_id="proj",
            cluster_name="cluster",
            cluster_config={"master_config": {}},
            properties={"prop": "val"},
        ),
        impersonationChain=None,
        dependsOn=None,
        triggerRule="all_success",
        labels={},
        executionTimeout=None,
    )


def test_dataproc_ephemeral_task_sql_with_deps_bucket_and_filename(
    ephemeral_sql_action, sample_dag
):
    """Tests ephemeral task configures depsBucket, params, and filename."""
    dataproc_ephemeral_task(ephemeral_sql_action, dag=sample_dag)

    submit_task = sample_dag.get_task("ephemeral_sql.ephemeral_sql_submit_job")
    assert (
        ephemeral_sql_action.config.cluster_config["config_bucket"]
        == "my-deps-bucket"
    )
    assert submit_task.job["spark_sql_job"]["script_variables"] == {"p1": "v1"}
    assert (
        submit_task.job["spark_sql_job"]["query_file_uri"]
        == "gs://bucket/query.sql"
    )


def test_dataproc_ephemeral_task_raises_runtime_error_on_exception(
    ephemeral_sql_action, sample_dag
):
    """Tests dataproc_ephemeral_task wraps exceptions in RuntimeError."""
    action = replace(
        ephemeral_sql_action, name="failing_ephemeral", config=None
    )

    with pytest.raises(
        RuntimeError,
        match="Failed to create task for action 'failing_ephemeral'",
    ) as exc_info:
        dataproc_ephemeral_task(action, dag=sample_dag)

    assert isinstance(exc_info.value.__cause__, AttributeError)


@pytest.fixture
def existing_cluster_sql_action() -> DataprocOperatorActionModel:
    """Returns an existing cluster SQL DataprocOperatorActionModel."""
    return DataprocOperatorActionModel(
        name="existing_sql",
        type="sql",
        region="us-central1",
        engine=EngineModel(engineType="dataproc-gce", clusterMode="existing"),
        params={"p1": "v1"},
        query=None,
        filename="gs://bucket/query.sql",
        config=DataprocGceExistingClusterConfigurationModel(
            project_id="proj",
            cluster_name="cluster",
            properties={"prop": "val"},
        ),
        impersonationChain=None,
        dependsOn=None,
        triggerRule="all_success",
        labels={},
        executionTimeout=None,
    )


def test_dataproc_existing_cluster_sql_with_params_and_filename(
    existing_cluster_sql_action, pipeline, sample_dag
):
    """Tests existing cluster SQL task with params and filename."""
    task = dataproc_existing_cluster(
        existing_cluster_sql_action, pipeline, dag=sample_dag
    )

    assert task.job["spark_sql_job"]["script_variables"] == {"p1": "v1"}
    assert (
        task.job["spark_sql_job"]["query_file_uri"] == "gs://bucket/query.sql"
    )


@pytest.fixture
def mock_notebook_gcs_utils():
    """Mocks notebook runner GCS path and upload helpers."""
    with (
        patch(
            f"{MODULE_PATH}.gcs_utils.get_run_notebook_gcs_path",
            return_value="gs://fake/runner.py",
            autospec=True,
        ),
        patch(
            f"{MODULE_PATH}.gcs_utils.upload_run_notebook_if_needed",
            autospec=True,
        ),
    ):
        yield


@pytest.mark.usefixtures("mock_notebook_gcs_utils")
def test_dataproc_existing_cluster_pyspark_with_pyfiles(
    existing_cluster_sql_action,
    pipeline,
    sample_dag,
):
    """Tests existing cluster pyspark task sets python_file_uris."""
    action = replace(
        existing_cluster_sql_action,
        name="existing_pyspark",
        type="pyspark",
        filename="gs://bucket/main.py",
        pyFiles=["gs://bucket/dep1.py"],
        params=None,
    )

    task = dataproc_existing_cluster(action, pipeline, dag=sample_dag)

    assert task.job["pyspark_job"]["python_file_uris"] == [
        "gs://bucket/dep1.py"
    ]


def test_dataproc_existing_cluster_raises_runtime_error_on_exception(
    existing_cluster_sql_action, pipeline, mock_dag
):
    """Tests dataproc_existing_cluster wraps exceptions in RuntimeError."""
    with pytest.raises(
        RuntimeError,
        match="Failed to create task for action 'existing_sql'",
    ) as exc_info:
        dataproc_existing_cluster(
            existing_cluster_sql_action, pipeline, dag=mock_dag
        )

    assert isinstance(exc_info.value.__cause__, TypeError)

@pytest.mark.parametrize(
    ("action_name", "operator_class", "params"),
    [
        (
            "empty_action",
            "airflow.operators.empty.EmptyOperator",
            {},
        ),
        (
            "bash_action",
            "airflow.operators.bash.BashOperator",
            {"bash_command": "echo 'Hello World'"},
        ),
        (
            "trigger_action",
            "airflow.operators.trigger_dagrun.TriggerDagRunOperator",
            {"trigger_dag_id": "target_pipeline_dag"},
        ),
        (
            "sql_action",
            "airflow.providers.common.sql.operators.sql.SQLExecuteQueryOperator",
            {"sql": "SELECT 1;", "conn_id": "postgres_default"},
        ),
        (
            "http_action",
            "airflow.providers.http.operators.http.HttpOperator",
            {"endpoint": "api/v1/health", "method": "GET"},
        ),
        (
            "bigquery_action",
            (
                "airflow.providers.google.cloud.operators.bigquery."
                "BigQueryInsertJobOperator"
            ),
            {
                "configuration": {
                    "query": {
                        "query": "SELECT 1;",
                        "useLegacySql": False,
                    }
                }
            },
        ),
    ],
)
def test_create_airflow_task_standard_operators(
    action_name,
    operator_class,
    params,
    sample_dag,
    pipeline,
):
    """Tests that create_airflow_task successfully instantiates common Airflow operators."""
    action = MagicMock()
    action.name = action_name
    action.type = "airflow_task"
    action.operator_class = operator_class
    action.params = params
    action.triggerRule = "all_done"
    action.executionTimeout = "120s"

    task = create_airflow_task(action, pipeline, sample_dag)

    assert task.task_id == action_name
    assert task.trigger_rule == "all_done"
    assert task.execution_timeout is not None

    for param_name, param_value in params.items():
        assert hasattr(task, param_name), f"Task missing parameter: {param_name}"
        assert getattr(task, param_name) == param_value


def test_create_airflow_task_invalid_operator_class_raises(sample_dag, pipeline):
     """Tests that invalid operator_class raises an exception."""
     action = MagicMock()
     action.name = "invalid_action"
     action.type = "airflow_task"
     action.operator_class = "airflow.operators.non_existent.FakeOperator"
     action.params = {}
     action.triggerRule = "all_success"
     action.executionTimeout = None

     with pytest.raises((ModuleNotFoundError, AttributeError)):
         create_airflow_task(action, pipeline, sample_dag)



def test_create_dataproc_operator_task_raises_on_unsupported_engine(
    existing_cluster_sql_action, pipeline, sample_dag
):
    """Tests ValueError raised for unsupported Dataproc engineType."""
    action = replace(
        existing_cluster_sql_action,
        name="bad_engine_action",
        type="notebook",
        engine=EngineModel(engineType="unsupported-engine"),  # type: ignore
    )

    with pytest.raises(ValueError, match="Unsupported notebook configuration"):
        create_dataproc_operator_task(action, pipeline, dag=sample_dag)


@pytest.fixture
def bq_dts_action() -> DataIngestionActionModel:
    """Returns a DataIngestionActionModel configured with runtimeParams."""
    return DataIngestionActionModel(
        name="dts_runtime_params",
        type="data_ingestion",
        config=BigQueryDtsSpecModel(
            projectId="proj",
            location="US",
            transferConfigId="cfg_123",
            requestedRunTime=None,
            requestedTimeRange=None,
            runtimeParams={
                "requested_run_time": "2026-01-01T00:00:00Z",
                "requested_time_range": {"start_time": "2026-01-01T00:00:00Z"},
            },
        ),
        dependsOn=None,
        executionTimeout=None,
        triggerRule="all_success",
    )


def test_create_bq_dts_task_uses_runtime_params_fallback(
    bq_dts_action, pipeline, sample_dag
):
    """Tests create_bq_dts_task reads run time and range from runtimeParams."""
    task_group = create_bq_dts_task(bq_dts_action, pipeline, dag=sample_dag)

    assert task_group.group_id == "dts_runtime_params"


def test_create_bq_dts_task_raises_runtime_error_on_exception(
    bq_dts_action, pipeline, sample_dag
):
    """Tests create_bq_dts_task wraps exceptions in RuntimeError."""
    action = replace(bq_dts_action, name="failing_dts", config=None)

    with pytest.raises(
        RuntimeError, match="Failed to create task for action 'failing_dts'"
    ) as exc_info:
        create_bq_dts_task(action, pipeline, dag=sample_dag)

    assert isinstance(exc_info.value.__cause__, AttributeError)


def test_create_vertex_upload_model_task_raises_runtime_error_on_exception(
    vertex_custom_job_action, pipeline, sample_dag
):
    """Tests _create_vertex_upload_model_task wraps errors in RuntimeError."""
    action = replace(
        vertex_custom_job_action,
        name="failing_upload_model",
        ai_action_type="model_upload",
    )

    with pytest.raises(
        RuntimeError,
        match="Failed to create task for action 'failing_upload_model'",
    ) as exc_info:
        create_ai_task(action, pipeline, dag=sample_dag)

    assert isinstance(exc_info.value.__cause__, AttributeError)


@pytest.fixture
def vertex_batch_inference_action() -> AIActionModel:
    """Returns an AIActionModel for batch inference with GCS source/dest."""
    return AIActionModel(
        name="vertex_gcs_batch",
        type="ai",
        provider="agent_platform",
        ai_action_type="batch_inference",
        config=AgentPlatformBatchInferenceSpecModel(
            project_id="proj",
            location="us-central1",
            job_display_name="job",
            model_name="model",
            instances_format="jsonl",
            predictions_format="jsonl",
            bigquery_source=None,
            gcs_source=["gs://src/input.jsonl"],
            bigquery_destination_prefix=None,
            gcs_destination_prefix="gs://dst/output",
            impersonation_chain=None,
        ),
        labels=None,
        dependsOn=None,
        executionTimeout=None,
        triggerRule="all_success",
    )


def test_create_vertex_batch_inference_task_with_gcs_source_and_dest(
    vertex_batch_inference_action, pipeline, sample_dag
):
    """Tests _create_vertex_batch_inference_task with GCS source and dest."""
    task = create_ai_task(vertex_batch_inference_action, pipeline, sample_dag)

    assert task.gcs_source == ["gs://src/input.jsonl"]
    assert task.gcs_destination_prefix == "gs://dst/output"


def test_create_vertex_batch_inference_task_raises_runtime_error_on_exception(
    vertex_batch_inference_action, vertex_custom_job_action, pipeline, sample_dag
):
    """Tests _create_vertex_batch_inference_task wraps errors in RuntimeError."""
    action = replace(
        vertex_batch_inference_action,
        config=vertex_custom_job_action.config,
    )

    with pytest.raises(
        RuntimeError,
        match="Failed to create task for action 'vertex_gcs_batch'",
    ) as exc_info:
        create_ai_task(action, pipeline, dag=sample_dag)

    assert isinstance(exc_info.value.__cause__, AttributeError)


@pytest.fixture
def vertex_custom_job_action() -> AIActionModel:
    """Returns a full AIActionModel for Vertex AI create_and_run_custom_job."""
    return AIActionModel(
        name="run_custom_job_task",
        type="ai",
        provider="agent_platform",
        ai_action_type="create_and_run_custom_job",
        executionTimeout="2h",
        dependsOn=[],
        triggerRule="all_success",
        labels={"orchestration_pipeline": "true", "env": "prod"},
        config=AgentPlatformCreateAndRunCustomJobSpecModel(
            project_id="my-project",
            location="us-central1",
            impersonation_chain=["sa-1@project.iam.gserviceaccount.com"],
            custom_job={
                "display_name": "my_custom_training_job",
                "job_spec": {
                    "worker_pool_specs": [
                        {
                            "machine_spec": {"machine_type": "n1-standard-4"},
                            "replica_count": "1",
                        }
                    ]
                },
                "labels": {"job_label": "val1"},
            },
        ),
    )


def test_create_ai_task_vertex_create_and_run_custom_job(
    vertex_custom_job_action, pipeline, sample_dag
):
    """Tests that create_ai_task creates CreateCustomJobOperator."""
    from airflow.providers.google.cloud.operators.vertex_ai import (
        custom_job as vertex_ai_custom_job,
    )

    task = create_ai_task(vertex_custom_job_action, pipeline, sample_dag)

    assert isinstance(task, vertex_ai_custom_job.CreateCustomJobOperator)
    assert task.task_id == "run_custom_job_task"
    assert task.project_id == "my-project"
    assert task.region == "us-central1"
    assert task.impersonation_chain == ["sa-1@project.iam.gserviceaccount.com"]
    assert task.execution_timeout == timedelta(hours=2)
    assert task.custom_job == {
        "display_name": "my_custom_training_job",
        "job_spec": {
            "worker_pool_specs": [
                {
                    "machine_spec": {"machine_type": "n1-standard-4"},
                    "replica_count": "1",
                }
            ]
        },
        "labels": {
            "orchestration_pipeline": "true",
            "env": "prod",
            "job_label": "val1",
        },
    }


def test_create_ai_task_vertex_create_and_run_custom_job_minimal(
    vertex_custom_job_action, pipeline, sample_dag
):
    """Tests create_ai_task with minimal custom job config and no labels."""
    action = replace(
        vertex_custom_job_action,
        labels=None,
        executionTimeout=None,
        config=AgentPlatformCreateAndRunCustomJobSpecModel(
            project_id="my-project",
            location="us-central1",
            custom_job={"display_name": "minimal_custom_job"},
        ),
    )

    task = create_ai_task(action, pipeline, sample_dag)

    assert task.execution_timeout is None
    assert task.impersonation_chain is None
    assert task.custom_job == {"display_name": "minimal_custom_job"}


def test_create_vertex_custom_job_task_raises_runtime_error_on_exception(
    vertex_custom_job_action, vertex_batch_inference_action, pipeline, sample_dag
):
    """Tests _create_vertex_custom_job_task wraps errors in RuntimeError."""
    action = replace(
        vertex_custom_job_action,
        name="failing_custom_job",
        config=vertex_batch_inference_action.config,
    )

    with pytest.raises(
        RuntimeError,
        match="Failed to create task for action 'failing_custom_job'",
    ) as exc_info:
        create_ai_task(action, pipeline, dag=sample_dag)

    assert isinstance(exc_info.value.__cause__, TypeError)
