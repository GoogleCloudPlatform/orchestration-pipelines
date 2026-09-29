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

import contextlib
import os
from datetime import datetime, timedelta
from typing import Any, Generator, Iterator
from unittest.mock import MagicMock, patch

import pytest
import pytz
from airflow.models import DAG
from airflow.providers.cncf.kubernetes.operators.pod import (
    KubernetesPodOperator,
)
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateBatchOperator,
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
)
from airflow.providers.google.cloud.operators.vertex_ai import (
    custom_job as vertex_ai_custom_job,
)

from orchestration_pipelines_lib import api
from orchestration_pipelines_lib.utils import file_manager
from tests.conftest import IS_AIRFLOW_2

if IS_AIRFLOW_2:
    from orchestration_pipelines_lib.dag_generator.airflow_adapters.airflow_2 import (  # noqa: E501
        adapter_imports,
    )
else:
    from orchestration_pipelines_lib.dag_generator.airflow_adapters.airflow_3 import (  # noqa: E501
        adapter_imports,
    )

PythonOperator = adapter_imports.get_python_operator()
Variable = adapter_imports.get_variable_class()

# Define the project root to reliably locate test data files
_PROJECT_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..")
)
_TEST_BUNDLE_ID = "example-bundle"
_TEST_DEFAULT_VERSION_ID = "a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p"
_TEST_NON_DEFAULT_VERSION_ID = "7d3b9e4a1f8c2b5d0e6a3f9c8d2a1b7e4f0c6b9d"


def _get_data_root_path():
    """Returns the absolute path to the test data directory."""
    return os.path.join(
        _PROJECT_ROOT, "tests/orchestration_pipelines_lib/test-data/"
    )


def _get_expected_dag_id(pipeline_id, parsing_failed=False, is_current=True):
    """Returns the expected Airflow DAG ID for a given pipeline and version."""
    prefix = "ERROR__" if parsing_failed else ""
    version_id = (
        _TEST_DEFAULT_VERSION_ID if is_current else _TEST_NON_DEFAULT_VERSION_ID
    )

    return f"{prefix}{_TEST_BUNDLE_ID}__v__{version_id}__{pipeline_id}"


def _get_dynamic_exists_side_effect(pipeline_id):
    """Returns a side_effect function for mock_fm_exists that dynamically checks
    for the existence of the *pipeline definition file* (.yml or .yaml)
    in the real file system and returns True for all other file paths to
    preserve the original intent of mocking referenced files as existing.
    """
    test_data_root = _get_data_root_path()
    bundle_version_path = os.path.join(
        test_data_root, _TEST_BUNDLE_ID, "versions", _TEST_DEFAULT_VERSION_ID
    )

    yml_path_abs = os.path.join(bundle_version_path, f"{pipeline_id}.yml")
    yaml_path_abs = os.path.join(bundle_version_path, f"{pipeline_id}.yaml")

    yml_exists_in_fs = os.path.exists(yml_path_abs)
    yaml_exists_in_fs = os.path.exists(yaml_path_abs)

    def exists_side_effect(path):
        is_pipeline_check = f"/{pipeline_id}." in path

        if is_pipeline_check:
            is_yml_check = path.endswith(f"{pipeline_id}.yml")
            is_yaml_check = path.endswith(f"{pipeline_id}.yaml")

            if is_yml_check:
                return yml_exists_in_fs

            if is_yaml_check:
                return yaml_exists_in_fs
        return True

    return exists_side_effect


def _setup_error_mocks(
    mock_get_versions: MagicMock,
    mock_to_raise_exception: MagicMock,
    error_message: str,
) -> None:
    """Helper to set up mocks for error scenarios."""
    mock_to_raise_exception.side_effect = Exception(error_message)
    mock_get_versions.return_value = [_TEST_DEFAULT_VERSION_ID]


def _assert_dummy_dag_was_created(pipeline_id, globals_dict):
    """Helper to assert that a dummy DAG was created for a given pipeline."""
    expected_dag_id = _get_expected_dag_id(pipeline_id, parsing_failed=True)
    assert expected_dag_id in globals_dict
    dag = globals_dict[expected_dag_id]
    assert len(dag.tasks) == 1
    error_task = dag.tasks[0]
    assert error_task.task_id == "parsing_failed"
    assert isinstance(error_task, PythonOperator)


def _assert_successful_generation(
    pipeline_id: str,
    dag: DAG,
    expected_operator_type: Any,
    is_paused: bool = False,
    is_current: bool = True,
) -> None:
    """Helper to assert its successful creation."""
    expected_dag_id = _get_expected_dag_id(pipeline_id, is_current=is_current)
    assert isinstance(dag, DAG)
    assert dag.dag_id == expected_dag_id
    assert "op:orchestration_pipeline" in dag.tags
    assert f"op:bundle:{_TEST_BUNDLE_ID}" in dag.tags
    if is_current:
        assert f"op:version:{_TEST_DEFAULT_VERSION_ID}" in dag.tags
    else:
        assert f"op:version:{_TEST_NON_DEFAULT_VERSION_ID}" in dag.tags
    schedule = dag.schedule_interval if IS_AIRFLOW_2 else dag.schedule  # type: ignore
    if is_paused or not is_current:
        assert schedule in [None, ""]
        assert dag.start_date in [None, ""]
        assert dag.end_date in [None, ""]
    else:
        assert schedule == "0 5 * * *"
        assert dag.start_date == datetime(
            2025, 10, 1, 0, 0, tzinfo=pytz.timezone("UTC")
        )
        assert dag.end_date == datetime(
            2026, 10, 1, 0, 0, tzinfo=pytz.timezone("UTC")
        )
        assert not dag.catchup
        assert dag.timezone.name == "UTC"
    assert len(dag.tasks) > 0
    assert "parsing_failed" not in [t.task_id for t in dag.tasks]
    assert any(isinstance(t, expected_operator_type) for t in dag.tasks), (
        f"Expected operator '{expected_operator_type.__name__}' not found in "
        f"DAG '{expected_dag_id}'."
    )


@pytest.fixture(autouse=True)
def mock_get_blob_ref() -> Iterator[MagicMock]:
    """Patches FileManager.get_blob_reference with default GCS bucket URI."""
    with patch(
        "orchestration_pipelines_lib.utils.file_manager.FileManager."
        "get_blob_reference"
    ) as mock:
        mock.side_effect = lambda path: (
            f"gs://example-bucket/{os.path.basename(path)}" if path else None
        )
        yield mock


@pytest.fixture(autouse=True)
def mock_session() -> Iterator[MagicMock]:
    """Patches airflow.utils.db.create_session."""
    session_mock = MagicMock()

    @contextlib.contextmanager
    def _mock_session_cm() -> Generator[MagicMock, None, None]:
        yield session_mock

    with patch("airflow.utils.db.create_session", create=True) as mock:
        mock.side_effect = _mock_session_cm
        yield mock


@pytest.fixture(autouse=True)
def mock_get_versions() -> Iterator[MagicMock]:
    """Patches get_versions_to_parse."""
    with patch(
        "orchestration_pipelines_lib.utils.versions_utils.get_versions_to_parse"
    ) as mock:
        mock.return_value = [
            _TEST_DEFAULT_VERSION_ID,
            _TEST_NON_DEFAULT_VERSION_ID,
        ]
        yield mock


@pytest.fixture
def mock_fm_exists() -> Iterator[MagicMock]:
    """Patches FileManager.exists."""
    with patch(
        "orchestration_pipelines_lib.utils.file_manager.FileManager.exists"
    ) as mock:
        yield mock


@pytest.fixture
def mock_upload_notebook() -> Iterator[MagicMock]:
    """Patches upload_run_notebook_if_needed."""
    with patch(
        "orchestration_pipelines_lib.dag_generator.airflow_adapters."
        "common_utils.task_utils.gcs_utils.upload_run_notebook_if_needed"
    ) as mock:
        yield mock


@pytest.fixture
def mock_read_gcs_file() -> Iterator[MagicMock]:
    """Patches FileManager._read_gcs_file."""
    with patch(
        "orchestration_pipelines_lib.utils.file_manager.FileManager."
        "_read_gcs_file"
    ) as mock:
        yield mock


@pytest.fixture
def mock_gcs_bucket_env() -> Iterator[None]:
    """Patches os.environ with GCS_BUCKET set to example-bucket."""
    with patch.dict(os.environ, {"GCS_BUCKET": "example-bucket"}):
        yield


@patch("airflow.utils.dag_cycle_tester.check_cycle")
def test_generate_dag_with_cycle_creates_dummy_dag(
    mock_check_cycle: MagicMock, mock_get_versions: MagicMock
) -> None:
    """Tests that a DAG with a cycle results in a dummy DAG."""
    pipeline_id = "fail-step4-structural-integrity"
    error_message = "A cycle has been detected in the DAG"
    _setup_error_mocks(mock_get_versions, mock_check_cycle, error_message)
    globals_dict = {}

    api.generate_dags(
        _get_data_root_path(),
        _TEST_BUNDLE_ID,
        pipeline_id,
        globals_dict,
    )

    _assert_dummy_dag_was_created(pipeline_id, globals_dict)


@patch(
    "orchestration_pipelines_lib.utils.pipeline_repository."
    "PipelineRepository.get_versioned_pipeline"
)
def test_generate_dag_with_model_validation_error_creates_dummy_dag(
    mock_get_versioned_pipeline: MagicMock, mock_get_versions: MagicMock
) -> None:
    """Tests that a schema validation error results in a dummy DAG."""
    pipeline_id = "fail-step2-model-validation"
    error_message = "Model validation failed"
    _setup_error_mocks(
        mock_get_versions,
        mock_get_versioned_pipeline,
        error_message,
    )
    globals_dict = {}

    api.generate_dags(
        _get_data_root_path(),
        _TEST_BUNDLE_ID,
        pipeline_id,
        globals_dict,
    )

    _assert_dummy_dag_was_created(pipeline_id, globals_dict)


@patch("orchestration_pipelines_lib.dag_generator.core.generate")
def test_generate_dag_with_generation_error_creates_dummy_dag(
    mock_generate: MagicMock, mock_get_versions: MagicMock
) -> None:
    """Tests that a DAG generation error results in a dummy DAG."""
    pipeline_id = "fail-step3-model-validation"
    error_message = "DAG generation failed"
    _setup_error_mocks(mock_get_versions, mock_generate, error_message)
    globals_dict = {}

    api.generate_dags(
        _get_data_root_path(),
        _TEST_BUNDLE_ID,
        pipeline_id,
        globals_dict,
    )

    _assert_dummy_dag_was_created(pipeline_id, globals_dict)


def test_generate_dags_with_missing_pipeline_file_creates_dummy_dag(
    mock_get_blob_ref: MagicMock, mock_get_versions: MagicMock
) -> None:
    """Tests that a missing pipeline definition file results in a dummy DAG
    with proper tags.
    """
    pipeline_id = "sql-on-dataproc-serverless"
    mock_get_blob_ref.side_effect = (
        file_manager.OrchestrationPipelinesFileNotFoundError(
            f"File not found: {pipeline_id}.yml"
        )
    )
    mock_get_versions.return_value = [_TEST_DEFAULT_VERSION_ID]
    globals_dict = {}
    expected_dag_id = _get_expected_dag_id(pipeline_id, parsing_failed=True)
    globals_dict = {}

    api.generate_dags(
        _get_data_root_path(),
        _TEST_BUNDLE_ID,
        pipeline_id,
        globals_dict,
    )

    _assert_dummy_dag_was_created(pipeline_id, globals_dict)
    dag = globals_dict[expected_dag_id]
    assert "op:orchestration_pipeline" in dag.tags
    assert f"op:bundle:{_TEST_BUNDLE_ID}" in dag.tags
    assert f"op:version:{_TEST_DEFAULT_VERSION_ID}" in dag.tags
    assert "op:is_current" in dag.tags


@pytest.mark.parametrize(
    "pipeline_id, expected_operator_type, is_paused",
    [
        ("dataproc-create-batch-pipeline", DataprocCreateBatchOperator, True),
        (
            "dataproc-create-batch-pipeline-resource-profile-gcs-overrides",
            DataprocCreateBatchOperator,
            False,
        ),
    ],
)
@pytest.mark.usefixtures("mock_upload_notebook", "mock_gcs_bucket_env")
def test_generate_dag_dataproc_batch_pipelines_success(
    pipeline_id: str,
    expected_operator_type: Any,
    is_paused: bool,
    mock_fm_exists: MagicMock,
    mock_read_gcs_file: MagicMock,
) -> None:
    """Tests successful DAG generation for dataproc batch pipelines."""
    expected_dag_id = _get_expected_dag_id(pipeline_id)
    mock_fm_exists.side_effect = _get_dynamic_exists_side_effect(pipeline_id)
    if (
        pipeline_id
        == "dataproc-create-batch-pipeline-resource-profile-gcs-overrides"
    ):
        mock_read_gcs_file.return_value = (
            "definition:\n  runtimeConfig:\n    properties:\n      prop1: val1"
        )
    globals_dict = {}

    api.generate_dags(
        _get_data_root_path(),
        _TEST_BUNDLE_ID,
        pipeline_id,
        globals_dict,
    )

    assert expected_dag_id in globals_dict
    dag = globals_dict[expected_dag_id]
    _assert_successful_generation(
        pipeline_id, dag, expected_operator_type, is_paused=is_paused
    )


@pytest.mark.parametrize(
    "pipeline_id, expected_operator_type",
    [
        (
            "dataproc-existing-cluster-script-pipeline",
            DataprocSubmitJobOperator,
        ),
        ("sql-on-dataproc-serverless", DataprocCreateBatchOperator),
        ("sql-on-dataproc-serverless-inline", DataprocCreateBatchOperator),
        ("sql-on-dataproc-gce-existing-inline", DataprocSubmitJobOperator),
        ("sql-on-dataproc-gce-ephemeral-inline", DataprocSubmitJobOperator),
    ],
)
@pytest.mark.usefixtures("mock_upload_notebook", "mock_gcs_bucket_env")
def test_generate_dag_standard_pipelines_success(
    pipeline_id: str, expected_operator_type: Any
) -> None:
    """Tests successful DAG generation for standard pipelines."""
    expected_dag_id = _get_expected_dag_id(pipeline_id)
    globals_dict = {}

    api.generate_dags(
        _get_data_root_path(),
        _TEST_BUNDLE_ID,
        pipeline_id,
        globals_dict,
    )

    assert expected_dag_id in globals_dict
    dag = globals_dict[expected_dag_id]
    _assert_successful_generation(pipeline_id, dag, expected_operator_type)


@pytest.mark.parametrize(
    "pipeline_id, mock_file_content",
    [
        (
            "dataproc-ephemeral-gcs-resource-profile-pyspark",
            "definition:\n config:\n    gceClusterConfig:\n      zoneUri: some-zone",
        ),
        (
            "dataproc-ephemeral-gcs-resource-profile-pyspark-overrides",
            "definition:\n config:\n    gceClusterConfig:\n      zoneUri: some-zone",
        ),
    ],
)
@pytest.mark.usefixtures("mock_upload_notebook", "mock_gcs_bucket_env")
def test_generate_dag_dataproc_ephemeral_gcs_success(
    pipeline_id: str,
    mock_file_content: str,
    mock_fm_exists: MagicMock,
    mock_read_gcs_file: MagicMock,
) -> None:
    """Tests successful DAG generation for ephemeral clusters with GCS config."""
    expected_dag_id = _get_expected_dag_id(pipeline_id)
    mock_fm_exists.side_effect = _get_dynamic_exists_side_effect(pipeline_id)
    mock_read_gcs_file.return_value = mock_file_content
    globals_dict = {}

    api.generate_dags(
        _get_data_root_path(),
        _TEST_BUNDLE_ID,
        pipeline_id,
        globals_dict,
    )

    assert expected_dag_id in globals_dict
    dag = globals_dict[expected_dag_id]
    _assert_successful_generation(pipeline_id, dag, DataprocSubmitJobOperator)


@patch.object(Variable, "get")
def test_generate_dag_for_dataform_pipeline_local_success(
    mock_variable_get: MagicMock,
) -> None:
    """Tests successful DAG generation for dataform-pipeline-local.yml."""
    pipeline_id = "dataform-pipeline-local"
    expected_dag_id = _get_expected_dag_id(pipeline_id)
    mock_variable_get.return_value = "gs://example-bucket/dataform/project"
    globals_dict = {}

    api.generate_dags(
        _get_data_root_path(),
        _TEST_BUNDLE_ID,
        pipeline_id,
        globals_dict,
    )

    assert expected_dag_id in globals_dict
    dag = globals_dict[expected_dag_id]
    _assert_successful_generation(pipeline_id, dag, KubernetesPodOperator)


@pytest.mark.usefixtures("mock_upload_notebook", "mock_gcs_bucket_env")
def test_generate_dag_for_dataproc_ephemeral_inline_pyspark_pipeline_success(
    mock_fm_exists: MagicMock, mock_get_blob_ref: MagicMock
) -> None:
    """Tests successful DAG generation for a pyspark job on an ephemeral
    Dataproc cluster with inline config.
    """
    pipeline_id = "dataproc-ephemeral-inline-pyspark"
    expected_dag_id = _get_expected_dag_id(pipeline_id)
    mock_fm_exists.side_effect = _get_dynamic_exists_side_effect(pipeline_id)
    mock_get_blob_ref.return_value = "gs://fake/path/to/script.py"
    globals_dict = {}

    api.generate_dags(
        _get_data_root_path(),
        _TEST_BUNDLE_ID,
        pipeline_id,
        globals_dict,
    )

    assert expected_dag_id in globals_dict
    dag = globals_dict[expected_dag_id]
    _assert_successful_generation(pipeline_id, dag, DataprocSubmitJobOperator)

    create_cluster_task = next(
        t for t in dag.tasks if isinstance(t, DataprocCreateClusterOperator)
    )
    delete_cluster_task = next(
        t for t in dag.tasks if isinstance(t, DataprocDeleteClusterOperator)
    )
    assert create_cluster_task.is_setup
    assert delete_cluster_task.is_teardown


@patch(
    "orchestration_pipelines_lib.dag_generator.airflow_adapters."
    "common_utils.gcs_utils.read_local_file_content_from_path"
)
@pytest.mark.usefixtures("mock_upload_notebook", "mock_gcs_bucket_env")
def test_generate_dag_for_dataproc_ephemeral_relative_resource_profile_pyspark_pipeline_success(
    mock_read_local: MagicMock,
    mock_fm_exists: MagicMock,
    mock_get_blob_ref: MagicMock,
) -> None:
    """Tests successful DAG generation for a pyspark job on an ephemeral
    Dataproc cluster with relative path config.
    """
    pipeline_id = "dataproc-ephemeral-relative-resource-profile-pyspark"
    expected_dag_id = _get_expected_dag_id(pipeline_id)
    mock_fm_exists.side_effect = _get_dynamic_exists_side_effect(pipeline_id)
    mock_get_blob_ref.return_value = "gs://fake/path/to/script.py"
    mock_read_local.return_value = "gceClusterConfig:\n  zoneUri: some-zone"
    globals_dict = {}

    api.generate_dags(
        _get_data_root_path(),
        _TEST_BUNDLE_ID,
        pipeline_id,
        globals_dict,
    )

    assert expected_dag_id in globals_dict
    dag = globals_dict[expected_dag_id]
    _assert_successful_generation(pipeline_id, dag, DataprocSubmitJobOperator)


def test_generate_dag_for_python_script_pipeline_success() -> None:
    """Tests successful DAG generation for python-script-pipeline.yml."""
    pipeline_id = "python-script-pipeline"
    expected_dag_id = _get_expected_dag_id(pipeline_id)
    dags_folder = os.path.join(
        _get_data_root_path(),
        _TEST_BUNDLE_ID,
        "versions",
        _TEST_DEFAULT_VERSION_ID,
    )
    globals_dict = {}

    with patch.dict(os.environ, {"DAGS_FOLDER": dags_folder}):
        api.generate_dags(
            _get_data_root_path(),
            _TEST_BUNDLE_ID,
            pipeline_id,
            globals_dict,
        )

    assert expected_dag_id in globals_dict
    dag = globals_dict[expected_dag_id]
    _assert_successful_generation(pipeline_id, dag, PythonOperator)


@patch(
    "orchestration_pipelines_lib.dag_generator.airflow_adapters."
    "common_utils.task_utils.FileManager"
)
def test_generate_dag_for_sql_on_bigquery_success(
    mock_task_factory_fm: MagicMock,
) -> None:
    """Tests successful DAG generation for sql-on-bigquery.yml."""
    pipeline_id = "sql-on-bigquery"
    expected_dag_id = _get_expected_dag_id(pipeline_id)
    mock_task_factory_fm.return_value.read.return_value = "SELECT 1;"

    globals_dict = {}

    api.generate_dags(
        _get_data_root_path(),
        _TEST_BUNDLE_ID,
        pipeline_id,
        globals_dict,
    )

    assert expected_dag_id in globals_dict
    dag = globals_dict[expected_dag_id]
    _assert_successful_generation(pipeline_id, dag, BigQueryInsertJobOperator)


def test_generate_dags_skips_pipelines_not_in_bundle_version(
    mock_get_versions: MagicMock,
) -> None:
    """Tests that generate_dags does not create a DAG if the pipeline is
    missing from the specified bundle version.
    """
    pipeline_id = "a-pipeline"
    expected_dag_id = _get_expected_dag_id(pipeline_id)
    mock_get_versions.return_value = [_TEST_DEFAULT_VERSION_ID]
    globals_dict = {}

    api.generate_dags(
        _get_data_root_path(),
        _TEST_BUNDLE_ID,
        pipeline_id,
        globals_dict,
    )

    assert expected_dag_id not in globals_dict, (
        f"DAG '{expected_dag_id}' was generated although it is not in bundle "
        f"version '{_TEST_DEFAULT_VERSION_ID}'."
    )


def test_generate_dag_for_non_default_running_pipeline() -> None:
    """Tests that if there is currently running pipeline that is not in
    default bundle version it creates a DAG without triggers specified.
    """
    dags_folder = os.path.join(
        _get_data_root_path(),
        _TEST_BUNDLE_ID,
        "versions",
        _TEST_NON_DEFAULT_VERSION_ID,
    )

    pipeline_id = "python-script-pipeline-previous"
    expected_dag_id = _get_expected_dag_id(pipeline_id, is_current=False)
    globals_dict = {}

    with patch.dict(os.environ, {"DAGS_FOLDER": dags_folder}):
        api.generate_dags(
            _get_data_root_path(),
            _TEST_BUNDLE_ID,
            pipeline_id,
            globals_dict,
        )

    assert expected_dag_id in globals_dict
    dag = globals_dict[expected_dag_id]
    _assert_successful_generation(
        pipeline_id, dag, PythonOperator, is_current=False
    )


def test_generate_dag_with_trigger_rule(mock_get_versions: MagicMock) -> None:
    """Tests that custom trigger rule is correctly set on generated tasks."""
    mock_get_versions.return_value = [_TEST_DEFAULT_VERSION_ID]
    pipeline_id = "trigger-rule-pipeline"
    expected_dag_id = _get_expected_dag_id(pipeline_id)
    globals_dict = {}

    api.generate_dags(
        _get_data_root_path(),
        _TEST_BUNDLE_ID,
        pipeline_id,
        globals_dict,
    )

    assert expected_dag_id in globals_dict
    dag = globals_dict[expected_dag_id]
    assert isinstance(dag, DAG)
    tasks_map = {t.task_id: t for t in dag.tasks}
    assert "normal_python_code" in tasks_map
    assert tasks_map["normal_python_code"].trigger_rule == "all_failed"
    assert "failed_python_code" in tasks_map
    assert tasks_map["failed_python_code"].trigger_rule == "all_success"


def test_generate_vertex_ai_upload_model_pipeline() -> None:
    """Tests that api.generate correctly creates a DAG with
    UploadModelOperator.
    """
    from airflow.providers.google.cloud.operators.vertex_ai.model_service import (  # noqa: E501
        UploadModelOperator,
    )

    example_path = os.path.join(
        _PROJECT_ROOT, "examples/pipeline-vertex-ai-upload-model.yml"
    )
    globals_dict = {}

    api.generate(example_path, globals_dict)

    assert "pipeline-vertex-ai-upload-model" in globals_dict
    dag = globals_dict["pipeline-vertex-ai-upload-model"]
    assert isinstance(dag, DAG)
    tasks_map = {t.task_id: t for t in dag.tasks}
    assert "upload_model_vertex" in tasks_map
    upload_task = tasks_map["upload_model_vertex"]
    assert isinstance(upload_task, UploadModelOperator)
    assert upload_task.project_id == "your-gcp-project-id"
    assert upload_task.region == "us-central1"
    assert upload_task.model == {
        "display_name": "Predictor",
        "artifact_uri": "gs://your-bucket-name/models/spark_rf_model",
        "container_spec": {
            "image_uri": (
                "us-docker.pkg.dev/vertex-ai/prediction/sklearn-cpu.1-4:latest"
            )
        },
        "description": "Prediction model",
        "labels": {"orchestration_pipeline": "true"},
    }


def test_generate_non_versioned_success() -> None:
    """Tests successful DAG generation using api.generate
    (non-versioned path).
    """
    pipeline_definition_file = os.path.join(
        _get_data_root_path(),
        "example-bundle/versions/a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p/"
        "sql-on-dataproc-serverless.yml",
    )
    globals_dict = {}

    api.generate(pipeline_definition_file, globals_dict=globals_dict)

    expected_dag_id = "sql-on-dataproc-serverless"
    assert expected_dag_id in globals_dict
    dag = globals_dict[expected_dag_id]
    assert isinstance(dag, DAG)
    assert dag.dag_id == expected_dag_id
    assert "op:orchestration_pipeline" in dag.tags
    assert f"op:pipeline:{expected_dag_id}" in dag.tags
    assert "op:owner:example-owner" in dag.tags
    assert "op:unversioned" in dag.tags
    assert "dataproc-serverless" in dag.tags
    assert "example" in dag.tags
    assert f"op:bundle:{_TEST_BUNDLE_ID}" not in dag.tags
    assert f"op:version:{_TEST_DEFAULT_VERSION_ID}" not in dag.tags
    assert "op:origination:GIT_CI_CD" not in dag.tags
    schedule = dag.schedule_interval if IS_AIRFLOW_2 else dag.schedule  # type: ignore
    assert schedule == "0 5 * * *"
    assert dag.start_date == datetime(
        2025, 10, 1, 0, 0, tzinfo=pytz.timezone("UTC")
    )
    assert dag.end_date == datetime(
        2026, 10, 1, 0, 0, tzinfo=pytz.timezone("UTC")
    )
    assert not dag.catchup
    assert dag.timezone.name == "UTC"


def test_generate_vertex_ai_batch_inference_pipeline() -> None:
    """Tests that api.generate correctly creates a DAG with
    CreateBatchPredictionJobOperator.
    """
    from airflow.providers.google.cloud.operators.vertex_ai.batch_prediction_job import (  # noqa: E501
        CreateBatchPredictionJobOperator,
    )

    example_path = os.path.join(
        _PROJECT_ROOT, "examples/pipeline-vertex-ai-batch-inference.yml"
    )
    globals_dict = {}

    api.validate(example_path)
    api.generate(example_path, globals_dict)

    assert "pipeline-vertex-ai-batch-inference" in globals_dict
    dag = globals_dict["pipeline-vertex-ai-batch-inference"]
    assert isinstance(dag, DAG)
    tasks_map = {t.task_id: t for t in dag.tasks}
    assert "run_vertex_batch_prediction" in tasks_map
    batch_task = tasks_map["run_vertex_batch_prediction"]
    assert isinstance(batch_task, CreateBatchPredictionJobOperator)
    assert batch_task.project_id == "your-gcp-project-id"
    assert batch_task.region == "us-central1"
    assert batch_task.job_display_name == "days_batch_prediction"
    assert (
        batch_task.model_name
        == "projects/your-gcp-project-id/locations/us-central1/models/123456789"
    )
    assert batch_task.instances_format == "bigquery"
    assert batch_task.predictions_format == "bigquery"
    assert (
        batch_task.bigquery_source
        == "bq://your-gcp-project-id.mlops.inference_dataset"
    )
    assert (
        batch_task.bigquery_destination_prefix
        == "bq://your-gcp-project-id.mlops"
    )
    assert batch_task.machine_type == "n1-standard-4"
    assert batch_task.labels == {"orchestration_pipeline": "true"}


@pytest.mark.usefixtures("mock_gcs_bucket_env")
def test_generate_with_dag_root_parameter() -> None:
    """Tests successful DAG generation when using the dag_root parameter."""
    globals_dict = {}
    pipeline_path = (
        "example-bundle/versions/"
        "a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p/sql-on-dataproc-serverless.yml"
    )
    dag_root = _get_data_root_path()

    api.generate(pipeline_path, globals_dict, dag_root)

    expected_dag_id = "sql-on-dataproc-serverless"
    assert expected_dag_id in globals_dict
    dag = globals_dict[expected_dag_id]
    assert isinstance(dag, DAG)
    assert dag.dag_id == expected_dag_id


def test_generate_vertex_ai_custom_job_pipeline() -> None:
    """Tests that api.generate creates a DAG with CreateCustomJobOperator."""
    example_path = os.path.join(
        _PROJECT_ROOT, "examples/pipeline-vertex-ai-custom-job.yml"
    )
    globals_dict = {}
    expected_custom_job = {
        "display_name": "my_custom_training_job",
        "job_spec": {
            "worker_pool_specs": [
                {
                    "machine_spec": {"machine_type": "n1-standard-4"},
                    "replica_count": "1",
                    "container_spec": {
                        "image_uri": (
                            "us-docker.pkg.dev/vertex-ai/training/"
                            "tf-cpu.2-12.py310:latest"
                        ),
                        "command": ["python", "train.py"],
                    },
                }
            ]
        },
        "labels": {"orchestration_pipeline": "true"},
    }

    api.validate(example_path)
    api.generate(example_path, globals_dict)

    assert "pipeline-vertex-ai-custom-job" in globals_dict
    dag = globals_dict["pipeline-vertex-ai-custom-job"]
    assert isinstance(dag, DAG)
    tasks_map = {t.task_id: t for t in dag.tasks}
    assert "run_vertex_custom_job" in tasks_map
    custom_job_task = tasks_map["run_vertex_custom_job"]
    assert isinstance(
        custom_job_task, vertex_ai_custom_job.CreateCustomJobOperator
    )
    assert custom_job_task.project_id == "your-gcp-project-id"
    assert custom_job_task.region == "us-central1"
    assert custom_job_task.execution_timeout == timedelta(hours=2)
    assert custom_job_task.custom_job == expected_custom_job
