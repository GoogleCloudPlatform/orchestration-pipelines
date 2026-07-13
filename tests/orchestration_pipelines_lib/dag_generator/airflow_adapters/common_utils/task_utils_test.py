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


if __name__ == "__main__":
    unittest.main()
