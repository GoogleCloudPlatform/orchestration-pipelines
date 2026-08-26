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
import unittest
from unittest.mock import MagicMock, patch

from orchestration_pipelines_lib.dag_generator.airflow_adapters.common_utils.task_utils import (
    create_dataproc_create_batch_operator_task,
    get_dataproc_create_batch_inline_sql_operator_class,
    get_dataproc_submit_job_inline_sql_operator_class,
    get_pipeline_metadata,
)


class TaskUtilsTest(unittest.TestCase):

    def test_get_pipeline_metadata_from_doc_md(self):
        """Tests retrieving metadata from DAG's doc_md JSON property."""
        import pendulum
        from airflow.models import DAG

        dag_notes = json.dumps({
            "op_bundle": "my_bundle_doc",
            "op_version": "v456",
            "op_pipeline": "my_pipeline_doc"
        })
        test_dag = DAG(dag_id="some_dag_id",
                       doc_md=dag_notes,
                       start_date=pendulum.today('UTC'))

        bundle_id, version_id, pipeline_id = get_pipeline_metadata(test_dag)
        self.assertEqual(bundle_id, "my_bundle_doc")
        self.assertEqual(version_id, "v456")
        self.assertEqual(pipeline_id, "my_pipeline_doc")

    def test_get_pipeline_metadata_no_doc_md_defaults(self):
        """Tests that get_pipeline_metadata falls back to safe defaults when doc_md is missing/invalid."""
        import pendulum
        from airflow.models import DAG

        # Case 1: doc_md is missing
        test_dag_no_doc = DAG(dag_id="my_pipeline",
                              start_date=pendulum.today('UTC'))
        bundle_id, version_id, pipeline_id = get_pipeline_metadata(
            test_dag_no_doc)
        self.assertEqual(bundle_id, "unknown_bundle")
        self.assertEqual(version_id, "unknown_version")
        self.assertEqual(pipeline_id, "my_pipeline")

        # Case 2: doc_md is invalid JSON
        test_dag_invalid_doc = DAG(dag_id="my_pipeline",
                                   doc_md="not-json",
                                   start_date=pendulum.today('UTC'))
        with self.assertLogs(level="WARNING") as log:
            bundle_id, version_id, pipeline_id = get_pipeline_metadata(
                test_dag_invalid_doc)
        self.assertEqual(bundle_id, "unknown_bundle")
        self.assertEqual(version_id, "unknown_version")
        self.assertEqual(pipeline_id, "my_pipeline")
        self.assertTrue(
            any("Failed to parse 'doc_md' of DAG 'my_pipeline' as JSON" in
                message for message in log.output))

        # Case 3: doc_md is valid JSON but not a dictionary
        test_dag_non_dict_doc = DAG(dag_id="my_pipeline",
                                    doc_md="[1, 2, 3]",
                                    start_date=pendulum.today('UTC'))
        with self.assertLogs(level="WARNING") as log:
            bundle_id, version_id, pipeline_id = get_pipeline_metadata(
                test_dag_non_dict_doc)
        self.assertEqual(bundle_id, "unknown_bundle")
        self.assertEqual(version_id, "unknown_version")
        self.assertEqual(pipeline_id, "my_pipeline")
        self.assertTrue(
            any("doc_md is not a JSON dictionary" in message
                for message in log.output))

    @patch("google.cloud.storage.Client")
    def test_dataproc_create_batch_inline_sql_operator_execute(
            self, mock_storage_client_cls):
        """Tests that the operator uploads the query to the correct hashed path derived from doc_md."""
        import pendulum
        from airflow.models import DAG

        mock_storage_client = mock_storage_client_cls.return_value
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_storage_client.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob

        # Create a real DAG with doc_md metadata populated
        dag_notes = json.dumps({
            "op_bundle": "my_bundle",
            "op_version": "v123",
            "op_pipeline": "my_pipeline"
        })
        test_dag = DAG(dag_id="my_bundle__v__v123__my_pipeline",
                       doc_md=dag_notes,
                       start_date=pendulum.today('UTC').add(days=-1),
                       schedule="@daily")

        batch_config = {"spark_sql_batch": {}}

        DataprocCreateBatchInlineSqlOperator = get_dataproc_create_batch_inline_sql_operator_class(
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

        # Mock Airflow context
        mock_ti = MagicMock()
        mock_ti.try_number = 1
        mock_dag_run = MagicMock()
        mock_dag_run.run_id = "manual__2026-05-04T00:00:00+00:00"
        context = {
            "task_instance": mock_ti,
            "dag_run": mock_dag_run,
        }

        # Calculate expected hash (uses query contents)
        import hashlib
        expected_hash = hashlib.sha256(
            operator.query.encode('utf-8')).hexdigest()
        expected_blob_name = f"data/my_bundle/versions/v123/managed-temp/{expected_hash}.sql"
        expected_gcs_uri = f"gs://my-example-bucket/{expected_blob_name}"

        # Mock super().execute to avoid actually calling Dataproc API
        with patch(
                "airflow.providers.google.cloud.operators.dataproc.DataprocCreateBatchOperator.execute"
        ) as mock_super_execute:
            operator.execute(context)

            # Verify storage client calls
            mock_storage_client_cls.assert_called_once()
            mock_storage_client.bucket.assert_called_once_with(
                "my-example-bucket")
            mock_bucket.blob.assert_called_once_with(expected_blob_name)
            mock_blob.upload_from_string.assert_called_once_with("SELECT 1;")

            # Verify that the batch object was updated with the correct URI
            self.assertEqual(
                operator.batch["spark_sql_batch"]["query_file_uri"],
                expected_gcs_uri)

            mock_super_execute.assert_called_once_with(context)

    def test_create_dataproc_create_batch_operator_task_inline_sql(self):
        """Tests that the factory function correctly instantiates the operator with basic fields."""
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

        dag = DAG(dag_id="test_dag",
                  default_args={},
                  start_date=pendulum.today('UTC').add(days=-1),
                  schedule="@daily")

        with patch.dict(os.environ, {"GCS_BUCKET": "env-bucket"}):
            operator = create_dataproc_create_batch_operator_task(
                action, pipeline, dag)

            DataprocCreateBatchInlineSqlOperator = get_dataproc_create_batch_inline_sql_operator_class(
            )
            self.assertIsInstance(operator,
                                  DataprocCreateBatchInlineSqlOperator)
            self.assertEqual(operator.task_id, "my_sql_action")
            self.assertEqual(operator.query, "SELECT 2;")
            self.assertEqual(operator.gcs_bucket, "env-bucket")
            # bundle_id, version_id, pipeline_id are no longer attributes on the operator instance

    @patch("google.cloud.storage.Client")
    def test_dataproc_submit_job_inline_sql_operator_execute(
            self, mock_storage_client_cls):
        """Tests that the operator uploads the query to the correct hashed path derived from dag_id for existing clusters."""
        import pendulum
        from airflow.models import DAG

        mock_storage_client = mock_storage_client_cls.return_value
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_storage_client.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob

        # Create a real DAG with doc_md metadata populated
        dag_notes = json.dumps({
            "op_bundle": "my_bundle",
            "op_version": "v123",
            "op_pipeline": "my_pipeline"
        })
        test_dag = DAG(dag_id="my_bundle__v__v123__my_pipeline",
                       doc_md=dag_notes,
                       start_date=pendulum.today('UTC').add(days=-1),
                       schedule="@daily")

        job_config = {"spark_sql_job": {}}

        DataprocSubmitJobInlineSqlOperator = get_dataproc_submit_job_inline_sql_operator_class(
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

        # Mock Airflow context
        mock_ti = MagicMock()
        mock_ti.try_number = 2
        mock_dag_run = MagicMock()
        mock_dag_run.run_id = "manual__2026-05-04T00:00:00+00:00"
        context = {
            "task_instance": mock_ti,
            "dag_run": mock_dag_run,
        }

        # Calculate expected hash (uses query contents)
        import hashlib
        expected_hash = hashlib.sha256(
            operator.query.encode('utf-8')).hexdigest()
        expected_blob_name = f"data/my_bundle/versions/v123/managed-temp/{expected_hash}.sql"
        expected_gcs_uri = f"gs://my-example-bucket/{expected_blob_name}"

        # Mock super().execute to avoid calling Dataproc API
        with patch(
                "airflow.providers.google.cloud.operators.dataproc.DataprocSubmitJobOperator.execute"
        ) as mock_super_execute:
            operator.execute(context)

            # Verify storage client calls
            mock_storage_client_cls.assert_called_once()
            mock_storage_client.bucket.assert_called_once_with(
                "my-example-bucket")
            mock_bucket.blob.assert_called_once_with(expected_blob_name)
            mock_blob.upload_from_string.assert_called_once_with("SELECT 5;")

            # Verify that the job object was updated with the correct URI
            self.assertEqual(operator.job["spark_sql_job"]["query_file_uri"],
                             expected_gcs_uri)

            mock_super_execute.assert_called_once_with(context)

    def test_create_bq_dts_task(self):
        """Tests creating BigQuery DTS TaskGroup."""
        import pendulum
        from airflow.models import DAG
        from airflow.utils.task_group import TaskGroup

        from orchestration_pipelines_lib.dag_generator.airflow_adapters.common_utils.task_utils import (
            create_bq_dts_task,
        )

        action = MagicMock()
        action.name = "my_dts_action"
        action.config.projectId = "dts-proj"
        action.config.location = "dts-loc"
        action.config.transferConfigId = "config-789"
        action.config.runtimeParams = None
        action.config.requestedRunTime = "2026-06-23T00:00:00Z"
        action.config.requestedTimeRange = None
        action.config.impersonationChain = ["dts-sa@dts-proj.iam.gserviceaccount.com"]
        action.executionTimeout = "1000s"
        action.triggerRule = "all_success"
        action.type = "sql"

        pipeline = MagicMock()
        pipeline.defaults.cloudDefault.project = "default-proj"
        pipeline.defaults.cloudDefault.region = "default-reg"

        dag = DAG(
            dag_id="test_dts_dag",
            start_date=pendulum.today("UTC"),
        )

        task_group = create_bq_dts_task(action, pipeline, dag)

        self.assertIsInstance(task_group, TaskGroup)
        self.assertEqual(task_group.group_id, "my_dts_action")

        children = task_group.children
        self.assertEqual(len(children), 2)
        self.assertIn("my_dts_action.my_dts_action_start", children)
        self.assertIn("my_dts_action.my_dts_action_sensor", children)

        start_task = children["my_dts_action.my_dts_action_start"]
        sensor_task = children["my_dts_action.my_dts_action_sensor"]

        self.assertEqual(start_task.transfer_config_id, "config-789")
        self.assertEqual(start_task.project_id, "dts-proj")
        self.assertEqual(start_task.location, "dts-loc")
        self.assertEqual(start_task.requested_run_time, {"seconds": 1782172800})
        self.assertEqual(start_task.impersonation_chain, ["dts-sa@dts-proj.iam.gserviceaccount.com"])

        self.assertEqual(sensor_task.transfer_config_id, "config-789")
        self.assertEqual(sensor_task.project_id, "dts-proj")
        self.assertEqual(
            sensor_task.run_id,
            "{{ task_instance.xcom_pull("
            "task_ids='my_dts_action.my_dts_action_start', "
            "key='run_id') }}",
        )

    def test_create_bq_dts_task_with_time_range(self):
        """Tests creating BigQuery DTS TaskGroup with requestedTimeRange."""
        import pendulum
        from airflow.models import DAG
        from airflow.utils.task_group import TaskGroup

        from orchestration_pipelines_lib.dag_generator.airflow_adapters.common_utils.task_utils import (
            create_bq_dts_task,
        )

        action = MagicMock()
        action.name = "my_dts_action_range"
        action.config.projectId = "dts-proj"
        action.config.location = "dts-loc"
        action.config.transferConfigId = "config-789"
        action.config.runtimeParams = None
        action.config.requestedRunTime = None
        action.config.requestedTimeRange = {"start_time": "2026-06-20T00:00:00Z", "end_time": "2026-06-21T00:00:00Z"}
        action.config.impersonationChain = ["dts-sa@dts-proj.iam.gserviceaccount.com"]
        action.executionTimeout = "1000s"
        action.triggerRule = "all_success"
        action.type = "pyspark"

        pipeline = MagicMock()
        pipeline.defaults.cloudDefault.project = "default-proj"
        pipeline.defaults.cloudDefault.region = "default-reg"

        dag = DAG(
            dag_id="test_dts_dag_range",
            start_date=pendulum.today("UTC"),
        )

        task_group = create_bq_dts_task(action, pipeline, dag)

        self.assertIsInstance(task_group, TaskGroup)
        self.assertEqual(task_group.group_id, "my_dts_action_range")

        children = task_group.children
        self.assertEqual(len(children), 2)
        self.assertIn("my_dts_action_range.my_dts_action_range_start", children)
        self.assertIn("my_dts_action_range.my_dts_action_range_sensor", children)

        start_task = children["my_dts_action_range.my_dts_action_range_start"]
        sensor_task = children["my_dts_action_range.my_dts_action_range_sensor"]

        self.assertEqual(start_task.transfer_config_id, "config-789")
        self.assertEqual(start_task.project_id, "dts-proj")
        self.assertEqual(start_task.location, "dts-loc")
        self.assertIsNone(start_task.requested_run_time)
        self.assertEqual(
            start_task.requested_time_range,
            {
                "start_time": {"seconds": 1781913600},
                "end_time": {"seconds": 1782000000},
            },
        )
        self.assertEqual(start_task.impersonation_chain, ["dts-sa@dts-proj.iam.gserviceaccount.com"])

        self.assertEqual(sensor_task.transfer_config_id, "config-789")
        self.assertEqual(sensor_task.project_id, "dts-proj")
        self.assertEqual(
            sensor_task.run_id,
            "{{ task_instance.xcom_pull("
            "task_ids='my_dts_action_range.my_dts_action_range_start', "
            "key='run_id') }}",
        )

    def test_create_bq_dts_task_defaults(self):
        """Tests creating BigQuery DTS TaskGroup defaults requested_run_time."""
        import pendulum
        from airflow.models import DAG

        from orchestration_pipelines_lib.dag_generator.airflow_adapters.common_utils.task_utils import (
            create_bq_dts_task,
        )

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

        dag = DAG(
            dag_id="test_dts_dag_defaults",
            start_date=pendulum.today("UTC"),
        )

        task_group = create_bq_dts_task(action, pipeline, dag)

        start_task = task_group.children[
            "my_dts_action_defaults.my_dts_action_defaults_start"
        ]
        self.assertEqual(
            start_task.requested_run_time,
            {
                "seconds": (
                    "{{ logical_date.timestamp() | int if logical_date is "
                    "defined else execution_date.timestamp() | int }}"
                )
            },
        )
        self.assertIsNone(start_task.requested_time_range)

    def test_create_local_dataform_task_with_labels_and_params(self):
        """Tests creating local Dataform task with labels and params."""
        import pendulum
        from airflow.models import DAG
        from airflow.providers.cncf.kubernetes.operators.pod import (
            KubernetesPodOperator,
        )

        from orchestration_pipelines_lib.dag_generator.airflow_adapters.common_utils.task_utils import (
            create_local_dataform_task,
        )

        action = MagicMock()
        action.name = "my_dataform_action"
        action.labels = {"env": "prod", "team": "data"}
        action.params = {"run_date": "2024-01-01", "id": "123"}
        action.executionTimeout = "600s"
        action.triggerRule = "all_success"

        pipeline = MagicMock()
        dag = DAG(
            dag_id="test_dataform_dag",
            start_date=pendulum.today("UTC"),
        )
        gcs_path = "gs://example-bucket/workspace"

        task = create_local_dataform_task(action, pipeline, gcs_path, dag)

        self.assertIsInstance(task, KubernetesPodOperator)
        self.assertEqual(task.task_id, "my_dataform_action")
        self.assertEqual(task.labels, {"env": "prod", "team": "data"})

        expected_cmd = (
            "gcloud storage cp --recursive $GCS_BUCKET_PATH/* . && dataform run "
            "--timeout=60s --job-labels=env=prod,team=data "
            "--vars=run_date=2024-01-01,id=123"
        )
        self.assertEqual(task.arguments, [expected_cmd])
        self.assertEqual(task.cmds, ["/bin/sh", "-c"])

    def test_create_local_dataform_task_without_labels_and_params(self):
        """Tests creating local Dataform task without labels and params."""
        import pendulum
        from airflow.models import DAG
        from airflow.providers.cncf.kubernetes.operators.pod import (
            KubernetesPodOperator,
        )

        from orchestration_pipelines_lib.dag_generator.airflow_adapters.common_utils.task_utils import (
            create_local_dataform_task,
        )

        action = MagicMock()
        action.name = "my_dataform_action"
        action.labels = None
        action.params = None
        action.executionTimeout = None
        action.triggerRule = "all_success"

        pipeline = MagicMock()
        dag = DAG(
            dag_id="test_dataform_dag",
            start_date=pendulum.today("UTC"),
        )
        gcs_path = "gs://example-bucket/workspace"

        task = create_local_dataform_task(action, pipeline, gcs_path, dag)

        self.assertIsInstance(task, KubernetesPodOperator)
        self.assertEqual(task.task_id, "my_dataform_action")
        self.assertEqual(task.labels, {})

        expected_cmd = (
            "gcloud storage cp --recursive $GCS_BUCKET_PATH/* . && dataform run --timeout=60s"
        )
        self.assertEqual(task.arguments, [expected_cmd])

    def test_create_ai_task_vertex_upload_model(self):
        """Tests creating Vertex AI UploadModelOperator from AIAction."""
        import pendulum
        from airflow.models import DAG
        from airflow.providers.google.cloud.operators.vertex_ai.model_service import (
            UploadModelOperator,
        )
        from orchestration_pipelines_lib.dag_generator.airflow_adapters.common_utils.task_utils import (
            create_ai_task,
        )
        action = MagicMock()
        action.name = "upload_model_vertex"
        action.provider = "agent_platform"
        action.ai_action_type = "model_upload"
        action.executionTimeout = "600s"
        action.triggerRule = "all_success"
        action.config.model_name = "Predictor"
        action.config.description = "Model"
        action.config.model_artifact_uri = "gs://my-bucket/models/spark_rf_model"
        action.config.serving_container_image_uri = "us-docker.pkg.dev/vertex-ai/prediction/sklearn-cpu.1-4:latest"
        action.config.project_id = "custom-project"
        action.config.location = "us-central1"
        action.labels = {"model_type": "rf", "env": "prod"}
        pipeline = MagicMock()
        pipeline.defaults.cloudDefault.project = "default-project"
        pipeline.defaults.cloudDefault.region = "default-region"
        dag = DAG(
            dag_id="test_ai_dag",
            start_date=pendulum.today("UTC"),
        )

        task = create_ai_task(action, pipeline, dag)

        self.assertIsInstance(task, UploadModelOperator)
        self.assertEqual(task.task_id, "upload_model_vertex")
        self.assertEqual(task.project_id, "custom-project")
        self.assertEqual(task.region, "us-central1")
        self.assertEqual(
            task.model,
            {
                "display_name": "Predictor",
                "artifact_uri": "gs://my-bucket/models/spark_rf_model",
                "container_spec": {
                    "image_uri": "us-docker.pkg.dev/vertex-ai/prediction/sklearn-cpu.1-4:latest"
                },
                "description": "Model",
                "labels": {"model_type": "rf", "env": "prod"},
            },
        )
        self.assertEqual(task.trigger_rule, "all_success")


    def test_create_ai_task_vertex_batch_inference(self):
        """Tests creating Vertex AI CreateBatchPredictionJobOperator from AIAction."""
        import pendulum
        from airflow.models import DAG
        from airflow.providers.google.cloud.operators.vertex_ai.batch_prediction_job import (
            CreateBatchPredictionJobOperator,
        )

        from orchestration_pipelines_lib.dag_generator.airflow_adapters.common_utils.task_utils import (
            create_ai_task,
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
        action.config.impersonation_chain = ["sa@custom-project.iam.gserviceaccount.com"]
        action.labels = {"env": "staging"}

        pipeline = MagicMock()
        pipeline.defaults.cloudDefault.project = "default-project"
        pipeline.defaults.cloudDefault.region = "default-region"

        dag = DAG(
            dag_id="test_batch_pred_dag",
            start_date=pendulum.today("UTC"),
        )

        task = create_ai_task(action, pipeline, dag)

        self.assertIsInstance(task, CreateBatchPredictionJobOperator)
        self.assertEqual(task.task_id, "run_vertex_batch_prediction")
        self.assertEqual(task.project_id, "custom-project")
        self.assertEqual(task.region, "us-central1")
        self.assertEqual(task.job_display_name, "days_batch_pred")
        self.assertEqual(
            task.model_name, "projects/123/locations/us-central1/models/456"
        )
        self.assertEqual(task.instances_format, "bigquery")
        self.assertEqual(task.predictions_format, "bigquery")
        self.assertEqual(task.bigquery_source, "bq://my-proj.mlops.test_data")
        self.assertEqual(
            task.bigquery_destination_prefix, "bq://my-proj.mlops"
        )
        self.assertEqual(task.machine_type, "n1-standard-4")
        self.assertEqual(
            task.impersonation_chain,
            ["sa@custom-project.iam.gserviceaccount.com"],
        )
        self.assertEqual(task.labels, {"env": "staging"})

    def test_create_ai_task_unsupported_provider(self):
        """Tests that unsupported AI provider raises ValueError."""
        from orchestration_pipelines_lib.dag_generator.airflow_adapters.common_utils.task_utils import (
            create_ai_task,
        )
        action = MagicMock()
        action.provider = "unsupported_provider"
        pipeline = MagicMock()
        dag = MagicMock()

        with self.assertRaisesRegex(ValueError, "Unsupported AI provider"):
            create_ai_task(action, pipeline, dag)

    def test_create_ai_task_unsupported_action_type(self):
        """Tests that unsupported agent_platform action type raises ValueError."""
        from orchestration_pipelines_lib.dag_generator.airflow_adapters.common_utils.task_utils import (
            create_ai_task,
        )
        action = MagicMock()
        action.provider = "agent_platform"
        action.ai_action_type = "unsupported_type"
        pipeline = MagicMock()
        dag = MagicMock()

        with self.assertRaisesRegex(
            ValueError, "Unsupported agent_platform action type"
        ):
            create_ai_task(action, pipeline, dag)

    @patch("orchestration_pipelines_lib.dag_generator.airflow_adapters.common_utils.task_utils.gcs_utils.upload_run_notebook_if_needed")
    @patch("orchestration_pipelines_lib.dag_generator.airflow_adapters.common_utils.task_utils.gcs_utils.get_run_notebook_gcs_path", return_value="gs://fake/notebook_runner.py")
    def test_dataproc_ephemeral_task_setup_and_teardown(
        self, mock_get_path, mock_upload
    ):
        """Tests that dataproc_ephemeral_task configures create_cluster as setup and delete_cluster as teardown."""
        import pendulum
        from airflow.models import DAG
        from airflow.providers.google.cloud.operators.dataproc import (
            DataprocCreateClusterOperator,
            DataprocDeleteClusterOperator,
            DataprocSubmitJobOperator,
        )
        from airflow.utils.task_group import TaskGroup
        from orchestration_pipelines_lib.dag_generator.airflow_adapters.common_utils.task_utils import (
            dataproc_ephemeral_task,
        )
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
        self.assertIsInstance(tg, TaskGroup)
        create_task = dag.get_task(f"{action.name}.{action.name}_create_cluster")
        submit_task = dag.get_task(f"{action.name}.{action.name}_submit_job")
        delete_task = dag.get_task(f"{action.name}.{action.name}_delete_cluster")
        self.assertIsInstance(create_task, DataprocCreateClusterOperator)
        self.assertIsInstance(submit_task, DataprocSubmitJobOperator)
        self.assertIsInstance(delete_task, DataprocDeleteClusterOperator)
        self.assertTrue(create_task.is_setup)
        self.assertTrue(delete_task.is_teardown)

    def test_create_dataset_trigger_task_all_condition(self):
        """Tests create_dataset_trigger_task with condition: all."""
        from orchestration_pipelines_lib.dag_generator.airflow_adapters.common_utils.task_utils import (
            create_dataset_trigger_task,
        )
        from orchestration_pipelines_lib.internal_models.triggers import (
            DatasetTriggerModel,
        )

        dag_kwargs = {}
        trigger = DatasetTriggerModel(
            uris=["gs://bucket/data1.parquet", "gs://bucket/data2.parquet"],
            condition="all",
        )
        create_dataset_trigger_task(dag_kwargs, trigger)

        self.assertIn("schedule", dag_kwargs)
        self.assertFalse(dag_kwargs["catchup"])
        self.assertIsNone(dag_kwargs["end_date"])
        self.assertIsNotNone(dag_kwargs["start_date"])
        self.assertEqual(len(dag_kwargs["schedule"]), 2)

    def test_create_dataset_trigger_task_any_condition(self):
        """Tests create_dataset_trigger_task with condition: any."""
        from orchestration_pipelines_lib.dag_generator.airflow_adapters.common_utils.task_utils import (
            create_dataset_trigger_task,
        )
        from orchestration_pipelines_lib.internal_models.triggers import (
            DatasetTriggerModel,
        )

        dag_kwargs = {}
        trigger = DatasetTriggerModel(
            uris=["gs://bucket/data1.parquet", "gs://bucket/data2.parquet"],
            condition="any",
        )
        create_dataset_trigger_task(dag_kwargs, trigger)

        self.assertIn("schedule", dag_kwargs)
        self.assertFalse(dag_kwargs["catchup"])
        self.assertIsNone(dag_kwargs["end_date"])
        self.assertIsNotNone(dag_kwargs["start_date"])

    def test_get_dataset_outlets_none_and_empty(self):
        """Tests get_dataset_outlets returns None for empty or None outlets."""
        from orchestration_pipelines_lib.dag_generator.airflow_adapters.common_utils.task_utils import (
            get_dataset_outlets,
        )

        action_none = MagicMock()
        action_none.outlets = None
        self.assertIsNone(get_dataset_outlets(action_none))

        action_empty = MagicMock()
        action_empty.outlets = []
        self.assertIsNone(get_dataset_outlets(action_empty))

        action_no_attr = object()
        self.assertIsNone(get_dataset_outlets(action_no_attr))

    def test_get_dataset_outlets_populated(self):
        """Tests get_dataset_outlets returns list of Dataset/Asset objects."""
        from orchestration_pipelines_lib.dag_generator.airflow_adapters.common_utils.task_utils import (
            get_dataset_outlets,
        )

        action = MagicMock()
        action.outlets = [
            "gs://bucket/data.parquet",
            "bq://project.dataset.table",
        ]
        outlets = get_dataset_outlets(action)
        self.assertIsNotNone(outlets)
        self.assertEqual(len(outlets), 2)
        uris = [getattr(o, "uri", str(o)).rstrip("/") for o in outlets]
        self.assertEqual(
            uris, ["gs://bucket/data.parquet", "bq://project.dataset.table"]
        )

    def test_create_bq_operation_task_with_outlets(self):
        """Tests create_bq_operation_task sets outlets on BigQuery operator."""
        import pendulum
        from airflow.models import DAG
        from orchestration_pipelines_lib.dag_generator.airflow_adapters.common_utils.task_utils import (
            create_bq_operation_task,
        )

        action = MagicMock()
        action.name = "bq_outlets_action"
        action.type = "operation"
        action.query = "SELECT 1;"
        action.filename = None
        action.labels = None
        action.params = None
        action.config.destinationTable = None
        action.config.location = "US"
        action.executionTimeout = None
        action.impersonationChain = None
        action.triggerRule = "all_success"
        action.outlets = ["bq://my-project.my_dataset.my_table"]

        pipeline = MagicMock()
        pipeline.defaults.cloudDefault.project = "my-project"

        dag = DAG(
            dag_id="test_bq_outlets_dag",
            start_date=pendulum.today("UTC"),
        )

        task = create_bq_operation_task(action, pipeline, dag)
        self.assertIsNotNone(task.outlets)
        uris = [getattr(o, "uri", str(o)).rstrip("/") for o in task.outlets]
        self.assertEqual(uris, ["bq://my-project.my_dataset.my_table"])

    def test_create_dataproc_batch_operator_with_outlets(self):
        """Tests create_dataproc_create_batch_operator_task sets outlets on batch operator."""
        import pendulum
        from airflow.models import DAG
        from orchestration_pipelines_lib.dag_generator.airflow_adapters.common_utils.task_utils import (
            create_dataproc_create_batch_operator_task,
        )

        action = MagicMock()
        action.type = "sql"
        action.name = "dataproc_batch_outlets"
        action.query = "SELECT 1;"
        action.filename = None
        action.depsBucket = None
        action.region = "us-central1"
        action.executionTimeout = None
        action.impersonationChain = None
        action.labels = {}
        action.triggerRule = "all_success"
        action.config.resourceProfile.runtimeConfig = {}
        action.config.resourceProfile.environmentConfig = {}
        action.params = None
        action.outlets = ["gs://bucket/batch_output.parquet"]

        pipeline = MagicMock()
        pipeline.defaults.cloudDefault.project = "my-project"

        dag = DAG(
            dag_id="test_dataproc_batch_dag",
            start_date=pendulum.today("UTC"),
        )

        with patch.dict(os.environ, {"GCS_BUCKET": "env-bucket"}):
            operator = create_dataproc_create_batch_operator_task(
                action, pipeline, dag
            )
            self.assertIsNotNone(operator.outlets)
            uris = [getattr(o, "uri", str(o)).rstrip("/") for o in operator.outlets]
            self.assertEqual(uris, ["gs://bucket/batch_output.parquet"])

    @patch("orchestration_pipelines_lib.dag_generator.airflow_adapters.common_utils.task_utils.gcs_utils.upload_run_notebook_if_needed")
    @patch("orchestration_pipelines_lib.dag_generator.airflow_adapters.common_utils.task_utils.gcs_utils.get_run_notebook_gcs_path", return_value="gs://fake/runner.py")
    def test_dataproc_ephemeral_task_outlets_on_submit_job_only(
        self, mock_get_path, mock_upload
    ):
        """Tests that ephemeral Dataproc task group attaches outlets only to submit_job."""
        import pendulum
        from airflow.models import DAG
        from orchestration_pipelines_lib.dag_generator.airflow_adapters.common_utils.task_utils import (
            dataproc_ephemeral_task,
        )

        action = MagicMock()
        action.name = "ephemeral_outlets_action"
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
        action.outlets = ["gs://bucket/ephemeral_output.parquet"]

        dag = DAG(
            dag_id="test_dag_ephemeral_outlets",
            start_date=pendulum.today("UTC"),
        )

        dataproc_ephemeral_task(action, dag)

        create_task = dag.get_task(f"{action.name}.{action.name}_create_cluster")
        submit_task = dag.get_task(f"{action.name}.{action.name}_submit_job")
        delete_task = dag.get_task(f"{action.name}.{action.name}_delete_cluster")

        self.assertFalse(create_task.outlets)
        self.assertFalse(delete_task.outlets)
        self.assertIsNotNone(submit_task.outlets)
        uris = [getattr(o, "uri", str(o)).rstrip("/") for o in submit_task.outlets]
        self.assertEqual(uris, ["gs://bucket/ephemeral_output.parquet"])

    def test_create_service_dataform_task_with_outlets(self):
        """Tests create_service_dataform_task sets outlets on Dataform operator."""
        import pendulum
        from airflow.models import DAG
        from orchestration_pipelines_lib.dag_generator.airflow_adapters.common_utils.task_utils import (
            create_service_dataform_task,
        )

        action = MagicMock()
        action.name = "service_dataform_action"
        action.type = "dataform_pipeline"
        action.dataformServiceConfig.project_id = "my-proj"
        action.dataformServiceConfig.region = "us-central1"
        action.dataformServiceConfig.repository_id = "my-repo"
        action.dataformServiceConfig.workflow_invocation = {"compilation_result": "cr1"}
        action.executionTimeout = None
        action.triggerRule = "all_success"
        action.outlets = ["bq://my-proj.dataform_dataset.dataform_table"]

        pipeline = MagicMock()
        dag = DAG(
            dag_id="test_service_dataform_dag",
            start_date=pendulum.today("UTC"),
        )

        task = create_service_dataform_task(action, pipeline, dag)
        self.assertIsNotNone(task.outlets)
        uris = [getattr(o, "uri", str(o)).rstrip("/") for o in task.outlets]
        self.assertEqual(uris, ["bq://my-proj.dataform_dataset.dataform_table"])

    def test_create_bq_dts_task_with_outlets(self):
        """Tests create_bq_dts_task sets outlets on start_task."""
        import pendulum
        from airflow.models import DAG
        from orchestration_pipelines_lib.dag_generator.airflow_adapters.common_utils.task_utils import (
            create_bq_dts_task,
        )

        action = MagicMock()
        action.name = "dts_outlets_action"
        action.type = "data_ingestion"
        action.config.projectId = "dts-proj"
        action.config.location = "dts-loc"
        action.config.transferConfigId = "config-123"
        action.config.runtimeParams = None
        action.config.requestedRunTime = "2026-06-23T00:00:00Z"
        action.config.requestedTimeRange = None
        action.config.impersonationChain = None
        action.executionTimeout = None
        action.triggerRule = "all_success"
        action.outlets = ["bq://dts-proj.dts_dataset.dts_table"]

        pipeline = MagicMock()
        pipeline.defaults.cloudDefault.project = "default-proj"
        pipeline.defaults.cloudDefault.region = "default-reg"

        dag = DAG(
            dag_id="test_dts_outlets_dag",
            start_date=pendulum.today("UTC"),
        )

        task_group = create_bq_dts_task(action, pipeline, dag)
        sensor_task = task_group.children[f"{action.name}.{action.name}_sensor"]
        self.assertIsNotNone(sensor_task.outlets)
        uris = [getattr(o, "uri", str(o)).rstrip("/") for o in sensor_task.outlets]
        self.assertEqual(uris, ["bq://dts-proj.dts_dataset.dts_table"])

    def test_create_ai_task_with_outlets(self):
        """Tests create_ai_task sets outlets on AI operators."""
        import pendulum
        from airflow.models import DAG
        from orchestration_pipelines_lib.dag_generator.airflow_adapters.common_utils.task_utils import (
            create_ai_task,
        )

        action = MagicMock()
        action.name = "upload_model_outlets"
        action.type = "ai"
        action.provider = "agent_platform"
        action.ai_action_type = "model_upload"
        action.executionTimeout = None
        action.triggerRule = "all_success"
        action.config.model_name = "ModelName"
        action.config.description = None
        action.config.model_artifact_uri = "gs://bucket/model"
        action.config.serving_container_image_uri = "gcr.io/img"
        action.config.project_id = "custom-project"
        action.config.location = "us-central1"
        action.labels = None
        action.outlets = ["gs://bucket/uploaded_model"]

        pipeline = MagicMock()
        dag = DAG(
            dag_id="test_ai_outlets_dag",
            start_date=pendulum.today("UTC"),
        )

        task = create_ai_task(action, pipeline, dag)
        self.assertIsNotNone(task.outlets)
        uris = [getattr(o, "uri", str(o)).rstrip("/") for o in task.outlets]
        self.assertEqual(uris, ["gs://bucket/uploaded_model"])

    def test_airflow_2_python_script_task_with_outlets(self):
        """Tests Airflow 2 create_python_script_task with outlets."""
        import pendulum
        from airflow.models import DAG
        from orchestration_pipelines_lib.dag_generator.airflow_adapters.airflow_2 import (
            task_factory as af2_task_factory,
        )

        action = MagicMock()
        action.name = "af2_py_action"
        action.type = "script"
        action.filename = "my_script.py"
        action.config.pythonCallable = "main"
        action.config.opKwargs = {}
        action.executionTimeout = None
        action.triggerRule = "all_success"
        action.outlets = ["gs://bucket/python_output.csv"]

        dag = DAG(
            dag_id="test_af2_py_dag",
            start_date=pendulum.today("UTC"),
        )

        with patch("orchestration_pipelines_lib.dag_generator.airflow_adapters.common_utils.utils.import_callable", return_value=lambda: None):
            task = af2_task_factory.create_python_script_task(action, {}, dag)
            self.assertIsNotNone(task.outlets)
            uris = [getattr(o, "uri", str(o)).rstrip("/") for o in task.outlets]
            self.assertEqual(uris, ["gs://bucket/python_output.csv"])

    def test_airflow_3_python_script_task_with_outlets(self):
        """Tests Airflow 3 create_python_script_task with outlets."""
        import pendulum
        from airflow.models import DAG
        from orchestration_pipelines_lib.dag_generator.airflow_adapters.airflow_3 import (
            task_factory as af3_task_factory,
        )

        action = MagicMock()
        action.name = "af3_py_action"
        action.type = "script"
        action.filename = "my_script.py"
        action.config.pythonCallable = "main"
        action.config.opKwargs = {}
        action.executionTimeout = None
        action.triggerRule = "all_success"
        action.outlets = ["gs://bucket/af3_output.csv"]

        dag = DAG(
            dag_id="test_af3_py_dag",
            start_date=pendulum.today("UTC"),
        )

        with patch("orchestration_pipelines_lib.dag_generator.airflow_adapters.common_utils.utils.import_callable", return_value=lambda: None):
            task = af3_task_factory.create_python_script_task(action, {}, dag)
            self.assertIsNotNone(task.outlets)
            uris = [getattr(o, "uri", str(o)).rstrip("/") for o in task.outlets]
            self.assertEqual(uris, ["gs://bucket/af3_output.csv"])


if __name__ == "__main__":
    unittest.main()


