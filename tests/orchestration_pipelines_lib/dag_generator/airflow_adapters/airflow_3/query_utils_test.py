# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""Unit tests for query_utils in Airflow 3 adapter."""

from unittest.mock import MagicMock, patch

import pytest

from orchestration_pipelines_lib.dag_generator.airflow_adapters.airflow_3.query_utils import (  # noqa: E501
    get_dag_tags_with_all_required_tags,
    resolve_latest_pipeline_dag_id,
)

CLIENT_UTILS_PATH = "orchestration_pipelines_lib.dag_generator.airflow_adapters.airflow_3.airflow_client_utils"  # noqa: E501
MODULE_PATH = "orchestration_pipelines_lib.dag_generator.airflow_adapters.airflow_3.query_utils"  # noqa: E501


@pytest.fixture
def mock_get_tags():
    """Fixture to mock get_dag_tags_with_all_required_tags."""
    mock_path = f"{MODULE_PATH}.get_dag_tags_with_all_required_tags"
    with patch(mock_path, autospec=True) as mock:
        yield mock


@pytest.fixture
def mock_logging():
    """Fixture mocking logging module in query_utils."""
    mock_path = f"{MODULE_PATH}.logging"
    with patch(mock_path, autospec=True) as mock:
        yield mock


@pytest.fixture
def api_response_with_dag():
    """Fixture simulating valid API response with DAG."""
    from airflow_client.client.models.dag_collection_response import (
        DAGCollectionResponse,
    )
    from airflow_client.client.models.dag_response import DAGResponse

    dag = DAGResponse.model_construct(dag_id="target_pipeline_v2_1_0")
    return DAGCollectionResponse.model_construct(dags=[dag], total_entries=1)


@pytest.fixture
def api_response_empty():
    """Fixture simulating empty API response."""
    from airflow_client.client.models.dag_collection_response import (
        DAGCollectionResponse,
    )

    return DAGCollectionResponse.model_construct(dags=[], total_entries=0)


@pytest.fixture
def default_kwargs():
    """Default arguments for resolve_latest_pipeline_dag_id."""
    return {
        "target_pipeline_id": "target_pipeline_base",
    }


@patch(f"{CLIENT_UTILS_PATH}.get_airflow_api_client", autospec=True)
@patch("airflow_client.client.DAGApi", autospec=True)
def test_get_dag_tags_with_all_required_tags_calls_api(
    mock_dag_api_class, mock_get_api_client
):
    """Tests that get_dag_tags_with_all_required_tags fetches DAGs matching
    all required tags from DAGApi.
    """
    mock_api_client = MagicMock()
    mock_get_api_client.return_value = mock_api_client
    mock_dag_api = MagicMock()
    mock_dag_api_class.return_value = mock_dag_api
    mock_response = MagicMock()
    mock_dag_api.get_dags.return_value = mock_response

    pipeline_id = "test_pipeline"
    bundle_id = "test_bundle"

    result = get_dag_tags_with_all_required_tags(pipeline_id, bundle_id)

    mock_get_api_client.assert_called_once()
    mock_dag_api_class.assert_called_once_with(mock_api_client)
    mock_dag_api.get_dags.assert_called_once_with(
        tags=[
            "op:is_current",
            f"op:bundle:{bundle_id}",
            f"op:pipeline:{pipeline_id}",
        ],
        tags_match_mode="all",
    )
    assert result == mock_response


def test_returns_target_when_no_bundle_id(mock_get_tags, default_kwargs):
    """Returns target_pipeline_id immediately when bundle_id is missing."""
    result = resolve_latest_pipeline_dag_id(**default_kwargs, bundle_id=None)

    assert result == "target_pipeline_base"
    mock_get_tags.assert_not_called()


def test_returns_resolved_dag_id_when_bundle_exists(
    mock_get_tags, api_response_with_dag, default_kwargs
):
    """Returns latest DAG version from API query when bundle exists."""
    mock_get_tags.return_value = api_response_with_dag

    result = resolve_latest_pipeline_dag_id(
        **default_kwargs, bundle_id="bundle_123"
    )

    assert result == "target_pipeline_v2_1_0"
    mock_get_tags.assert_called_once_with("target_pipeline_base", "bundle_123")


def test_falls_back_to_target_when_no_dags_found(
    mock_get_tags, api_response_empty, mock_logging, default_kwargs
):
    """Logs warning and falls back to target when API returns empty DAG list."""
    mock_get_tags.return_value = api_response_empty

    result = resolve_latest_pipeline_dag_id(
        **default_kwargs, bundle_id="bundle_123"
    )

    assert result == "target_pipeline_base"
    mock_logging.warning.assert_called_once()
    assert (
        "No DAG found with pipeline ID" in mock_logging.warning.call_args[0][0]
    )


def test_raises_and_logs_exception_on_api_error(
    mock_get_tags, mock_logging, default_kwargs
):
    """Logs and re-raises API exceptions."""
    expected_exception = Exception("Airflow API Timeout")
    mock_get_tags.side_effect = expected_exception

    with pytest.raises(Exception) as exc_info:
        resolve_latest_pipeline_dag_id(**default_kwargs, bundle_id="bundle_123")

    assert exc_info.value is expected_exception
    mock_logging.error.assert_called_once()
    assert (
        "Error resolving latest bundle version"
        in mock_logging.error.call_args[0][0]
    )
