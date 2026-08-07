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
"""Unit tests for query_utils in Airflow 2 adapter."""

from unittest.mock import MagicMock, patch

import pytest

from orchestration_pipelines_lib.dag_generator.airflow_adapters.airflow_2.query_utils import (  # noqa: E501
    get_dag_tags_with_all_required_tags,
    resolve_latest_pipeline_dag_id,
)

MODULE_PATH = "orchestration_pipelines_lib.dag_generator.airflow_adapters.airflow_2.query_utils"  # noqa: E501


@pytest.fixture
def db_session():
    """Provides an in-memory SQLite session with the DagTag table."""
    from airflow.models import DagTag
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker

    engine = create_engine("sqlite:///:memory:")
    DagTag.__table__.create(engine)
    session_factory = sessionmaker(bind=engine)
    with session_factory() as session:
        yield session


@pytest.fixture
def mock_session():
    """Fixture to mock Airflow session."""
    return MagicMock()


@pytest.fixture
def mock_create_session(mock_session):
    """Fixture mocking create_session context manager."""
    with patch("airflow.utils.session.create_session", autospec=True) as mock:
        mock.return_value.__enter__.return_value = mock_session
        yield mock


@pytest.fixture
def mock_get_tags():
    """Fixture mocking get_dag_tags_with_all_required_tags."""
    with patch(
        f"{MODULE_PATH}.get_dag_tags_with_all_required_tags", autospec=True
    ) as mock:
        yield mock


@pytest.fixture
def mock_logging():
    """Fixture mocking logging in query_utils."""
    with patch(f"{MODULE_PATH}.logging", autospec=True) as mock:
        yield mock


@pytest.fixture
def default_kwargs():
    """Default arguments for resolve_latest_pipeline_dag_id."""
    return {
        "target_pipeline_id": "target_pipeline_base",
    }


def test_get_dag_tags_with_all_required_tags_returns_matching_dags(db_session):
    """Returns only dag_ids that have all three required tags."""
    from airflow.models import DagTag

    bundle_id = "test_bundle"
    matching_dag_id = "matching_dag"
    partial_dag_id = "partial_dag"
    other_bundle_dag_id = "other_bundle_dag"

    db_session.add_all(
        [
            DagTag(dag_id=matching_dag_id, name="op:is_current"),
            DagTag(dag_id=matching_dag_id, name=f"op:bundle:{bundle_id}"),
            DagTag(dag_id=matching_dag_id, name="op:pipeline:test_pipeline"),
            DagTag(dag_id=matching_dag_id, name="extra_tag"),

            DagTag(dag_id=partial_dag_id, name="op:is_current"),
            DagTag(dag_id=partial_dag_id, name=f"op:bundle:{bundle_id}"),

            DagTag(dag_id=other_bundle_dag_id, name="op:is_current"),
            DagTag(dag_id=other_bundle_dag_id, name="op:bundle:other_bundle"),
            DagTag(dag_id=other_bundle_dag_id, name="op:pipeline:test_pipeline"),
        ]
    )
    db_session.commit()

    result = get_dag_tags_with_all_required_tags(
        db_session, "test_pipeline", bundle_id
    ).all()

    assert result == [(matching_dag_id,)]


def test_returns_target_when_no_bundle_id(mock_get_tags, default_kwargs):
    """Returns target_pipeline_id immediately when bundle_id is missing."""
    result = resolve_latest_pipeline_dag_id(**default_kwargs, bundle_id=None)

    assert result == "target_pipeline_base"
    mock_get_tags.assert_not_called()


@pytest.mark.usefixtures("mock_create_session")
def test_returns_resolved_dag_id_when_bundle_exists(
    mock_session,
    mock_get_tags,
    default_kwargs,
):
    """Returns resolved dag_id when matching DAG is found in DB."""
    mock_query = MagicMock()
    mock_query.first.return_value = ("target_pipeline_v2_1_0",)
    mock_get_tags.return_value = mock_query

    result = resolve_latest_pipeline_dag_id(
        **default_kwargs, bundle_id="bundle_123"
    )

    assert result == "target_pipeline_v2_1_0"
    mock_get_tags.assert_called_once_with(
        mock_session, "target_pipeline_base", "bundle_123"
    )


@pytest.mark.usefixtures("mock_create_session")
def test_falls_back_to_target_when_no_dags_found(
    mock_get_tags,
    mock_logging,
    default_kwargs,
):
    """Logs warning and falls back to target when no matching DAG is found."""
    mock_query = MagicMock()
    mock_query.first.return_value = None
    mock_get_tags.return_value = mock_query

    result = resolve_latest_pipeline_dag_id(
        **default_kwargs, bundle_id="bundle_123"
    )

    assert result == "target_pipeline_base"
    mock_logging.warning.assert_called_once()
    assert (
        "No DAG found with pipeline ID 'target_pipeline_base'"
        in mock_logging.warning.call_args[0][0]
    )


@pytest.mark.usefixtures("mock_create_session")
def test_raises_and_logs_exception_on_db_error(
    mock_get_tags,
    mock_logging,
    default_kwargs,
):
    """Logs and re-raises database query exceptions."""
    expected_exception = Exception("DB connection timeout")
    mock_get_tags.side_effect = expected_exception

    with pytest.raises(Exception) as exc_info:
        resolve_latest_pipeline_dag_id(**default_kwargs, bundle_id="bundle_123")

    assert exc_info.value is expected_exception
    mock_logging.error.assert_called_once()
    assert (
        "Error resolving latest bundle version"
        in mock_logging.error.call_args[0][0]
    )
