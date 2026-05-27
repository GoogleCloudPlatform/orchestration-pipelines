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
"""Unit tests for the core DAG generation logic."""

import os
import sys
import unittest
from datetime import datetime
import pytz
from unittest.mock import patch

from airflow.operators.empty import EmptyOperator
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateBatchOperator,
    DataprocSubmitJobOperator,
)

from orchestration_pipelines_lib import api

# Define the project root to reliably locate test data files
_PROJECT_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", ".."))
_TEST_BUNDLE_ID = "example-bundle"
_TEST_DEFAULT_VERSION_ID = "a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p"
_TEST_NON_DEFAULT_VERSION_ID = "7d3b9e4a1f8c2b5d0e6a3f9c8d2a1b7e4f0c6b9d"

_API_MODULE = sys.modules["orchestration_pipelines_lib.api"]


class TestApi(unittest.TestCase):
    """Test suite for the DAG generator."""

    def _setup_and_generate_dags_with_error(self, pipeline_id,
                                            mock_get_versions,
                                            mock_to_raise_exception,
                                            error_message):
        """Helper to set up mocks, generate DAGs, and trigger an error."""
        expected_dag_id = _get_expected_dag_id(pipeline_id, parsing_failed=True)

        # Configure the mock to raise an exception
        mock_to_raise_exception.side_effect = Exception(error_message)
        mock_get_versions.return_value = [_TEST_DEFAULT_VERSION_ID]

        if hasattr(_API_MODULE, expected_dag_id):
            delattr(_API_MODULE, expected_dag_id)
        api.generate_dags(_get_data_root_path(), _TEST_BUNDLE_ID, pipeline_id,
                          _API_MODULE.__dict__)

    def _assert_dummy_dag_was_created(self, pipeline_id):
        """Helper to assert that a dummy DAG was created for a given pipeline."""
        expected_dag_id = _get_expected_dag_id(pipeline_id, parsing_failed=True)
        self.assertTrue(hasattr(_API_MODULE, expected_dag_id))
        dag = getattr(_API_MODULE, expected_dag_id)

        self.assertEqual(len(dag.tasks), 1)
        error_task = dag.tasks[0]
        self.assertEqual(error_task.task_id, "parsing_failed")
        self.assertIsInstance(error_task, EmptyOperator)

    def _run_and_assert_successful_generation(self,
                                              pipeline_id,
                                              expected_operator_type,
                                              mock_get_versions,
                                              is_paused=False,
                                              is_current=True):
        """
        Helper to run DAG generation and assert its successful creation.

        This encapsulates the common test logic for success scenarios.
        """
        expected_dag_id = _get_expected_dag_id(pipeline_id,
                                               is_current=is_current)
        mock_get_versions.return_value = [
            _TEST_DEFAULT_VERSION_ID, _TEST_NON_DEFAULT_VERSION_ID
        ]

        if hasattr(_API_MODULE, expected_dag_id):
            delattr(_API_MODULE, expected_dag_id)

        api.generate_dags(_get_data_root_path(), _TEST_BUNDLE_ID, pipeline_id,
                          _API_MODULE.__dict__)
        # Assert a DAG was created and has the correct ID
        self.assertTrue(hasattr(_API_MODULE, expected_dag_id),
                        f"DAG '{expected_dag_id}' was not generated.")
        dag = getattr(_API_MODULE, expected_dag_id)
        self.assertIsInstance(dag, DAG)
        self.assertEqual(dag.dag_id, expected_dag_id)

        # Assert basic properties (tags)
        self.assertIn("op:orchestration_pipeline", dag.tags)
        self.assertIn(f"op:bundle:{_TEST_BUNDLE_ID}", dag.tags)
        if is_current:
            self.assertIn(f"op:version:{_TEST_DEFAULT_VERSION_ID}", dag.tags)
        else:
            self.assertIn(f"op:version:{_TEST_NON_DEFAULT_VERSION_ID}",
                          dag.tags)

        # Assert schedule properties
        if is_paused or not is_current:
            self.assertIn(dag.schedule_interval, [None, ""])
            self.assertIn(dag.start_date, [None, ""])
            self.assertIn(dag.end_date, [None, ""])
        else:
            self.assertEqual(dag.schedule_interval, "0 5 * * *")
            self.assertEqual(
                dag.start_date,
                datetime(2025, 10, 1, 0, 0, tzinfo=pytz.timezone("UTC")))
            self.assertEqual(
                dag.end_date,
                datetime(2026, 10, 1, 0, 0, tzinfo=pytz.timezone("UTC")))
            self.assertFalse(dag.catchup)
            self.assertEqual(dag.timezone.name, "UTC")

        # Assert it's a valid, non-dummy DAG
        self.assertGreater(len(dag.tasks), 0)
        self.assertNotIn("parsing_failed", [t.task_id for t in dag.tasks])

        # Assert that the specific operator task exists
        self.assertTrue(
            any(isinstance(t, expected_operator_type) for t in dag.tasks),
            f"Expected operator '{expected_operator_type.__name__}' not found in DAG '{expected_dag_id}'."
        )

    @patch("airflow.utils.db.create_session")
    @patch(
        "orchestration_pipelines_lib.utils.versions_utils.get_versions_to_parse"
    )
    @patch("airflow.utils.dag_cycle_tester.check_cycle")
    def test_generate_dag_with_cycle_creates_dummy_dag(self, mock_check_cycle,
                                                       mock_get_versions,
                                                       mock_session):
        """Tests that a DAG with a cycle results in a dummy DAG."""
        pipeline_id = "fail-step4-structural-integrity"
        error_message = "A cycle has been detected in the DAG"
        self._setup_and_generate_dags_with_error(pipeline_id, mock_get_versions,
                                                 mock_check_cycle,
                                                 error_message)
        self._assert_dummy_dag_was_created(pipeline_id)

    @patch("airflow.utils.db.create_session")
    @patch(
        "orchestration_pipelines_lib.utils.versions_utils.get_versions_to_parse"
    )
    @patch("orchestration_pipelines_lib.api._read_parse_and_convert_pipeline")
    def test_generate_dag_with_model_validation_error_creates_dummy_dag(
            self, mock_read_parse, mock_get_versions, mock_session):
        """Tests that a schema validation error results in a dummy DAG."""
        pipeline_id = "fail-step2-model-validation"
        error_message = "Model validation failed"
        self._setup_and_generate_dags_with_error(pipeline_id, mock_get_versions,
                                                 mock_read_parse, error_message)
        self._assert_dummy_dag_was_created(pipeline_id)

    @patch("airflow.utils.db.create_session")
    @patch(
        "orchestration_pipelines_lib.utils.versions_utils.get_versions_to_parse"
    )
    @patch("orchestration_pipelines_lib.dag_generator.core.generate")
    def test_generate_dag_with_generation_error_creates_dummy_dag(
            self, mock_generate, mock_get_versions, mock_session):
        """Tests that a DAG generation error results in a dummy DAG."""
        pipeline_id = "fail-step3-model-validation"
        error_message = "DAG generation failed"
        self._setup_and_generate_dags_with_error(pipeline_id, mock_get_versions,
                                                 mock_generate, error_message)
        self._assert_dummy_dag_was_created(pipeline_id)

    @patch("airflow.models.variable.Variable.get")
    @patch(
        "orchestration_pipelines_lib.utils.file_manager.FileManager.get_blob_reference"
    )
    @patch("airflow.utils.db.create_session")
    @patch(
        "orchestration_pipelines_lib.utils.versions_utils.get_versions_to_parse"
    )
    def test_generate_dag_for_dataform_pipeline_local_success(
            self, mock_get_versions, mock_session, mock_get_blob_reference,
            mock_variable_get):
        """Tests successful DAG generation for dataform-pipeline-local.yml."""
        # Mock dependencies for local dataform task
        mock_get_blob_reference.return_value = "gs://example-bucket/dataform/project/file.sql"
        mock_variable_get.return_value = "gs://example-bucket/dataform/project"

        self._run_and_assert_successful_generation(
            pipeline_id="dataform-pipeline-local",
            expected_operator_type=KubernetesPodOperator,
            mock_get_versions=mock_get_versions)

    @patch(
        "orchestration_pipelines_lib.dag_generator.airflow_adapters.common_utils.task_utils.gcs_utils.upload_run_notebook_if_needed"
    )
    @patch("orchestration_pipelines_lib.utils.file_manager.FileManager.exists")
    @patch.dict(os.environ, {"GCS_BUCKET": "example-bucket"})
    @patch("airflow.utils.db.create_session")
    @patch(
        "orchestration_pipelines_lib.utils.versions_utils.get_versions_to_parse"
    )
    def test_generate_dag_for_dataproc_create_batch_pipeline_success(
            self, mock_get_versions, mock_session, mock_fm_exists,
            mock_upload_notebook):
        """Tests successful DAG generation for dataproc-create-batch-pipeline.yml."""
        # Mock that the referenced files exist to avoid actual file system checks.
        mock_fm_exists.return_value = True

        self._run_and_assert_successful_generation(
            pipeline_id="dataproc-create-batch-pipeline",
            expected_operator_type=DataprocCreateBatchOperator,
            mock_get_versions=mock_get_versions,
            is_paused=True)

    @patch(
        "orchestration_pipelines_lib.dag_generator.airflow_adapters.common_utils.task_utils.gcs_utils.upload_run_notebook_if_needed"
    )
    @patch.dict(os.environ, {"GCS_BUCKET": "example-bucket"})
    @patch(
        "orchestration_pipelines_lib.utils.file_manager.FileManager._read_gcs_file"
    )
    @patch("orchestration_pipelines_lib.utils.file_manager.FileManager.exists")
    @patch("airflow.utils.db.create_session")
    @patch(
        "orchestration_pipelines_lib.utils.versions_utils.get_versions_to_parse"
    )
    def test_generate_dag_for_dataproc_create_batch_pipeline_resource_profile_gcs_overrides_success(
            self, mock_get_versions, mock_session, mock_fm_exists,
            mock_read_gcs_file, mock_upload_notebook):
        """Tests successful DAG generation for dataproc-create-batch-pipeline-resource-profile-gcs-overrides.yml."""
        mock_fm_exists.return_value = True
        mock_read_gcs_file.return_value = "definition:\n  runtimeConfig:\n    properties:\n      prop1: val1"
        self._run_and_assert_successful_generation(
            pipeline_id=
            "dataproc-create-batch-pipeline-resource-profile-gcs-overrides",
            expected_operator_type=DataprocCreateBatchOperator,
            mock_get_versions=mock_get_versions)

    @patch(
        "orchestration_pipelines_lib.dag_generator.airflow_adapters.common_utils.task_utils.gcs_utils.upload_run_notebook_if_needed"
    )
    @patch.dict(os.environ, {"GCS_BUCKET": "example-bucket"})
    @patch(
        "orchestration_pipelines_lib.utils.file_manager.FileManager.get_blob_reference"
    )
    @patch("orchestration_pipelines_lib.utils.file_manager.FileManager.exists")
    @patch("airflow.utils.db.create_session")
    @patch(
        "orchestration_pipelines_lib.utils.versions_utils.get_versions_to_parse"
    )
    def test_generate_dag_for_dataproc_ephemeral_inline_pyspark_pipeline_success(
            self, mock_get_versions, mock_session, mock_fm_exists,
            mock_get_blob_reference, mock_upload_notebook):
        """Tests successful DAG generation for a pyspark job on an ephemeral Dataproc cluster with inline config."""
        from airflow.utils.trigger_rule import TriggerRule
        from airflow.providers.google.cloud.operators.dataproc import DataprocDeleteClusterOperator

        mock_fm_exists.return_value = True
        mock_get_blob_reference.return_value = "gs://fake/path/to/script.py"

        self._run_and_assert_successful_generation(
            pipeline_id="dataproc-ephemeral-inline-pyspark",
            expected_operator_type=DataprocSubmitJobOperator,
            mock_get_versions=mock_get_versions)

        expected_dag_id = _get_expected_dag_id(
            "dataproc-ephemeral-inline-pyspark")
        dag = getattr(_API_MODULE, expected_dag_id)
        delete_cluster_task = next(
            t for t in dag.tasks
            if isinstance(t, DataprocDeleteClusterOperator))
        self.assertEqual(delete_cluster_task.trigger_rule, TriggerRule.ALL_DONE)

    @patch(
        "orchestration_pipelines_lib.dag_generator.airflow_adapters.common_utils.task_utils.gcs_utils.upload_run_notebook_if_needed"
    )
    @patch.dict(os.environ, {"GCS_BUCKET": "example-bucket"})
    @patch(
        "orchestration_pipelines_lib.utils.file_manager.FileManager.get_blob_reference"
    )
    @patch(
        "orchestration_pipelines_lib.utils.file_manager.FileManager._read_gcs_file"
    )
    @patch("orchestration_pipelines_lib.utils.file_manager.FileManager.exists")
    @patch("airflow.utils.db.create_session")
    @patch(
        "orchestration_pipelines_lib.utils.versions_utils.get_versions_to_parse"
    )
    def test_generate_dag_for_dataproc_ephemeral_gcs_resource_profile_pyspark_pipeline_success(
            self, mock_get_versions, mock_session, mock_fm_exists,
            mock_read_gcs_file, mock_get_blob_reference, mock_upload_notebook):
        """Tests successful DAG generation for a pyspark job on an ephemeral Dataproc cluster with GCS config."""
        mock_fm_exists.return_value = True
        mock_read_gcs_file.return_value = "definition:\n config:\n    gceClusterConfig:\n      zoneUri: some-zone"
        mock_get_blob_reference.return_value = "gs://fake/path/to/script.py"

        self._run_and_assert_successful_generation(
            pipeline_id="dataproc-ephemeral-gcs-resource-profile-pyspark",
            expected_operator_type=DataprocSubmitJobOperator,
            mock_get_versions=mock_get_versions)

    @patch(
        "orchestration_pipelines_lib.dag_generator.airflow_adapters.common_utils.task_utils.gcs_utils.upload_run_notebook_if_needed"
    )
    @patch.dict(os.environ, {"GCS_BUCKET": "example-bucket"})
    @patch(
        "orchestration_pipelines_lib.utils.file_manager.FileManager.get_blob_reference"
    )
    @patch(
        "orchestration_pipelines_lib.utils.file_manager.FileManager._read_gcs_file"
    )
    @patch("orchestration_pipelines_lib.utils.file_manager.FileManager.exists")
    @patch("airflow.utils.db.create_session")
    @patch(
        "orchestration_pipelines_lib.utils.versions_utils.get_versions_to_parse"
    )
    def test_generate_dag_for_dataproc_ephemeral_gcs_resource_profile_pyspark_overrides_success(
            self, mock_get_versions, mock_session, mock_fm_exists,
            mock_read_gcs_file, mock_get_blob_reference, mock_upload_notebook):
        """Tests successful DAG generation for a pyspark job on an ephemeral Dataproc cluster with GCS config and overrides."""
        mock_fm_exists.return_value = True
        mock_read_gcs_file.return_value = "definition:\n config:\n    gceClusterConfig:\n      zoneUri: some-zone"
        mock_get_blob_reference.return_value = "gs://fake/path/to/script.py"

        self._run_and_assert_successful_generation(
            pipeline_id=
            "dataproc-ephemeral-gcs-resource-profile-pyspark-overrides",
            expected_operator_type=DataprocSubmitJobOperator,
            mock_get_versions=mock_get_versions)

    @patch(
        "orchestration_pipelines_lib.dag_generator.airflow_adapters.common_utils.task_utils.gcs_utils.upload_run_notebook_if_needed"
    )
    @patch.dict(os.environ, {"GCS_BUCKET": "example-bucket"})
    @patch(
        "orchestration_pipelines_lib.utils.file_manager.FileManager.get_blob_reference"
    )
    @patch(
        "orchestration_pipelines_lib.dag_generator.airflow_adapters.common_utils.gcs_utils.read_local_file_content_from_path"
    )
    @patch("orchestration_pipelines_lib.utils.file_manager.FileManager.exists")
    @patch("airflow.utils.db.create_session")
    @patch(
        "orchestration_pipelines_lib.utils.versions_utils.get_versions_to_parse"
    )
    def test_generate_dag_for_dataproc_ephemeral_relative_resource_profile_pyspark_pipeline_success(
            self, mock_get_versions, mock_session, mock_fm_exists,
            mock_read_local, mock_get_blob_reference, mock_upload_notebook):
        """Tests successful DAG generation for a pyspark job on an ephemeral Dataproc cluster with relative path config."""
        mock_fm_exists.return_value = True
        mock_get_blob_reference.return_value = "gs://fake/path/to/script.py"
        mock_read_local.return_value = "gceClusterConfig:\n  zoneUri: some-zone"

        self._run_and_assert_successful_generation(
            pipeline_id="dataproc-ephemeral-relative-resource-profile-pyspark",
            expected_operator_type=DataprocSubmitJobOperator,
            mock_get_versions=mock_get_versions)

    @patch("airflow.utils.db.create_session")
    @patch(
        "orchestration_pipelines_lib.utils.versions_utils.get_versions_to_parse"
    )
    def test_generate_dag_for_python_script_pipeline_success(
            self, mock_get_versions, mock_session):
        """Tests successful DAG generation for python-script-pipeline.yml."""
        # The import_callable function uses DAGS_FOLDER to find the script.
        # We mock it to point to our test data directory.
        dags_folder = os.path.join(_get_data_root_path(), _TEST_BUNDLE_ID,
                                   "versions", _TEST_DEFAULT_VERSION_ID)
        with patch.dict(os.environ, {"DAGS_FOLDER": dags_folder}):
            self._run_and_assert_successful_generation(
                pipeline_id="python-script-pipeline",
                expected_operator_type=PythonOperator,
                mock_get_versions=mock_get_versions)

    @patch(
        "orchestration_pipelines_lib.dag_generator.airflow_adapters.common_utils.task_utils.FileManager"
    )
    @patch(
        "orchestration_pipelines_lib.utils.file_manager.FileManager.get_blob_reference"
    )
    @patch("airflow.utils.db.create_session")
    @patch(
        "orchestration_pipelines_lib.utils.versions_utils.get_versions_to_parse"
    )
    def test_generate_dag_for_sql_on_bigquery_success(self, mock_get_versions,
                                                      mock_session,
                                                      mock_get_blob_reference,
                                                      mock_task_factory_fm):
        """Tests successful DAG generation for sql-on-bigquery.yml."""
        # Mock get_blob_reference to prevent local path resolution errors.
        mock_get_blob_reference.return_value = "gs://fake/path/to/query.sql"

        # Mock the FileManager used inside the task factory to prevent GCS calls.
        mock_task_factory_fm.return_value.read.return_value = "SELECT 1;"

        self._run_and_assert_successful_generation(
            pipeline_id="sql-on-bigquery",
            expected_operator_type=BigQueryInsertJobOperator,
            mock_get_versions=mock_get_versions)

    @patch(
        "orchestration_pipelines_lib.dag_generator.airflow_adapters.common_utils.task_utils.gcs_utils.upload_run_notebook_if_needed"
    )
    @patch.dict(os.environ, {"GCS_BUCKET": "example-bucket"})
    @patch(
        "orchestration_pipelines_lib.utils.file_manager.FileManager.get_blob_reference"
    )
    @patch("airflow.utils.db.create_session")
    @patch(
        "orchestration_pipelines_lib.utils.versions_utils.get_versions_to_parse"
    )
    def test_generate_dag_for_dataproc_existing_cluster_script_pipeline_success(
            self, mock_get_versions, mock_session, mock_get_blob_reference,
            mock_upload_notebook):
        """Tests successful DAG generation for dataproc-existing-cluster-script-pipeline.yml."""
        # Mock GCS interactions.
        mock_get_blob_reference.return_value = "gs://fake/path/to/script.py"

        self._run_and_assert_successful_generation(
            pipeline_id="dataproc-existing-cluster-script-pipeline",
            expected_operator_type=DataprocSubmitJobOperator,
            mock_get_versions=mock_get_versions)

    @patch(
        "orchestration_pipelines_lib.utils.file_manager.FileManager.get_blob_reference"
    )
    @patch("airflow.utils.db.create_session")
    @patch(
        "orchestration_pipelines_lib.utils.versions_utils.get_versions_to_parse"
    )
    def test_generate_dag_for_sql_on_dataproc_serverless_success(
            self, mock_get_versions, mock_session, mock_get_blob_reference):
        """Tests successful DAG generation for sql-on-dataproc-serverless.yml."""
        # Mock GCS interactions to prevent errors on file path resolution.
        mock_get_blob_reference.return_value = "gs://fake/path/to/query.sql"

        self._run_and_assert_successful_generation(
            pipeline_id="sql-on-dataproc-serverless",
            expected_operator_type=DataprocCreateBatchOperator,
            mock_get_versions=mock_get_versions)

    @patch.dict(os.environ, {"GCS_BUCKET": "example-bucket"})
    @patch(
        "orchestration_pipelines_lib.utils.file_manager.FileManager.get_blob_reference"
    )
    @patch("airflow.utils.db.create_session")
    @patch(
        "orchestration_pipelines_lib.utils.versions_utils.get_versions_to_parse"
    )
    def test_generate_dag_for_sql_on_dataproc_serverless_inline_success(
        self,
        mock_get_versions,
        mock_session,
        mock_get_blob_reference,
    ):
        """Tests successful DAG generation for sql-on-dataproc-serverless-inline.yml."""
        mock_get_blob_reference.return_value = "gs://example-bucket/data/example-bundle/versions/a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p/sql_queries/run-sql-on-dataproc.sql"

        self._run_and_assert_successful_generation(
            pipeline_id="sql-on-dataproc-serverless-inline",
            expected_operator_type=DataprocCreateBatchOperator,
            mock_get_versions=mock_get_versions,
        )

    @patch.dict(os.environ, {"GCS_BUCKET": "example-bucket"})
    @patch(
        "orchestration_pipelines_lib.utils.file_manager.FileManager.get_blob_reference"
    )
    @patch("airflow.utils.db.create_session")
    @patch(
        "orchestration_pipelines_lib.utils.versions_utils.get_versions_to_parse"
    )
    def test_generate_dag_for_sql_on_dataproc_gce_existing_inline_success(
            self, mock_get_versions, mock_session, mock_get_blob_reference):
        """Tests successful DAG generation for sql-on-dataproc-gce-existing-inline.yml."""
        mock_get_blob_reference.return_value = "gs://example-bucket/data/example-bundle/versions/a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p/sql_queries/run-sql-on-existing-cluster.sql"

        self._run_and_assert_successful_generation(
            pipeline_id="sql-on-dataproc-gce-existing-inline",
            expected_operator_type=DataprocSubmitJobOperator,
            mock_get_versions=mock_get_versions,
        )

    @patch.dict(os.environ, {"GCS_BUCKET": "example-bucket"})
    @patch(
        "orchestration_pipelines_lib.utils.file_manager.FileManager.get_blob_reference"
    )
    @patch("airflow.utils.db.create_session")
    @patch(
        "orchestration_pipelines_lib.utils.versions_utils.get_versions_to_parse"
    )
    def test_generate_dag_for_sql_on_dataproc_gce_ephemeral_inline_success(
            self, mock_get_versions, mock_session, mock_get_blob_reference):
        """Tests successful DAG generation for sql-on-dataproc-gce-ephemeral-inline.yml."""
        mock_get_blob_reference.return_value = "gs://example-bucket/data/example-bundle/versions/a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p/sql_queries/run-sql-on-ephemeral-cluster.sql"

        self._run_and_assert_successful_generation(
            pipeline_id="sql-on-dataproc-gce-ephemeral-inline",
            expected_operator_type=DataprocSubmitJobOperator,
            mock_get_versions=mock_get_versions,
        )

    @patch(
        "orchestration_pipelines_lib.utils.versions_utils.get_versions_to_parse"
    )
    def test_generate_dags_skips_pipelines_not_in_bundle_version(
            self, mock_get_versions):
        """
        Tests that generate_dags does not create a DAG if the pipeline is 
        missing from the specified bundle version.
        """
        pipeline_id = "a-pipeline"
        expected_dag_id = _get_expected_dag_id(pipeline_id)
        mock_get_versions.return_value = [_TEST_DEFAULT_VERSION_ID]

        api.generate_dags(_get_data_root_path(), _TEST_BUNDLE_ID, pipeline_id,
                          _API_MODULE.__dict__)
        self.assertFalse(
            hasattr(_API_MODULE, expected_dag_id),
            f"DAG '{expected_dag_id}' was generated although it is not in bundle version '{_TEST_DEFAULT_VERSION_ID}'."
        )

    @patch("airflow.utils.db.create_session")
    @patch(
        "orchestration_pipelines_lib.utils.versions_utils.get_versions_to_parse"
    )
    def test_generate_dag_for_non_default_running_pipeline(
            self, mock_get_versions, mock_session):
        """
        Tests that if there is currently running pipeline that is not in
        default bundle version it creates a DAG without triggers specified.
        """

        dags_folder = os.path.join(_get_data_root_path(), _TEST_BUNDLE_ID,
                                   "versions", _TEST_NON_DEFAULT_VERSION_ID)
        with patch.dict(os.environ, {"DAGS_FOLDER": dags_folder}):
            self._run_and_assert_successful_generation(
                pipeline_id="python-script-pipeline-previous",
                expected_operator_type=PythonOperator,
                mock_get_versions=mock_get_versions,
                is_current=False)


def _get_data_root_path():
    return os.path.join(_PROJECT_ROOT,
                        "tests/orchestration_pipelines_lib/test-data/")


def _get_expected_dag_id(pipeline_id, parsing_failed=False, is_current=True):
    prefix = "ERROR__" if parsing_failed else ""
    if is_current:
        return f"{prefix}{_TEST_BUNDLE_ID}__v__{_TEST_DEFAULT_VERSION_ID}__{pipeline_id}"
    else:
        return f"{prefix}{_TEST_BUNDLE_ID}__v__{_TEST_NON_DEFAULT_VERSION_ID}__{pipeline_id}"


if __name__ == "__main__":
    unittest.main()
