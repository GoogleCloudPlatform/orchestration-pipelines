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
# limitations under the License.
#
"""Unit tests for the core functions of Airflow 2."""

import unittest
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy.exc import IntegrityError
from sqlalchemy.sql.selectable import Subquery

from orchestration_pipelines_lib.dag_generator.airflow_adapters.airflow_2.core import (  # noqa: E501
    _extract_versions,
    _get_dag_tags_subquery,
    _get_tags,
    _get_task_instance_notes,
    _get_task_instances,
    _upsert_dag_run_note,
    _upsert_task_instance_note,
    _upsert_task_instance_notes,
    get_actively_running_versions,
    get_deps,
    get_previous_default_versions,
    init_orchestration_pipeline_context,
    send_notification_email,
    task_factory,
)

ADAPTERS_MODULE = "orchestration_pipelines_lib.dag_generator.airflow_adapters"
TARGET_MODULE = f"{ADAPTERS_MODULE}.airflow_2.core"

MOCK_CREATE_SESSION = "airflow.utils.session.create_session"
MOCK_DAG_RUN_NOTE = "airflow.models.dagrun.DagRunNote"
MOCK_TASK_INSTANCE = "airflow.models.TaskInstance"
MOCK_TASK_INSTANCE_NOTE = "airflow.models.taskinstance.TaskInstanceNote"
MOCK_DAG_TAG = "airflow.models.DagTag"

MOCK_EXTRACT_ADDITIONAL_NOTES = (
    f"{TARGET_MODULE}.dag_utils.extract_additional_notes"
)
MOCK_UPSERT_DAG_RUN_NOTE = f"{TARGET_MODULE}._upsert_dag_run_note"
MOCK_UPSERT_TASK_INSTANCE_NOTES = f"{TARGET_MODULE}._upsert_task_instance_notes"
MOCK_GET_TASK_INSTANCE_NOTES = f"{TARGET_MODULE}._get_task_instance_notes"
MOCK_GET_TASK_INSTANCES = f"{TARGET_MODULE}._get_task_instances"
MOCK_UPSERT_TASK_INSTANCE_NOTE = f"{TARGET_MODULE}._upsert_task_instance_note"


@pytest.fixture
def mock_dag_run():
    """Provides a mocked Airflow DagRun object with basic identifiers."""
    return MagicMock(id="run_1", dag_id="test_dag", run_id="run_1")


@pytest.fixture
def mock_dag():
    """Sets up the complex DAG structure with mocked tasks."""
    return MagicMock(
        tasks=[
            MagicMock(task_id="task_a", doc_md="Doc A"),
            MagicMock(task_id="task_b", doc_md="Doc B"),
            MagicMock(task_id="task_c", doc_md=""),
        ]
    )


@pytest.fixture
def note_content():
    """Provides a sample JSON string to be used as note content."""
    return '{"key": "value"}'


@pytest.fixture
def mock_session():
    """Provides a generic mock representing a database session."""
    return MagicMock()


@pytest.fixture
def mock_create_session(mock_session):
    """Patches the DB session creation context manager to yield the
    mocked database session.
    """
    with patch(MOCK_CREATE_SESSION) as mock:
        mock.return_value.__enter__.return_value = mock_session
        yield mock


@pytest.fixture
def full_context(mock_dag_run, mock_dag, mock_create_session):
    """Provides a complete mocked Airflow execution context dictionary
    containing the DAG, DagRun, and DB session maker.
    """
    return {
        "dag_run": mock_dag_run,
        "dag": mock_dag,
        "create_session_mock": mock_create_session,
    }


@pytest.fixture
def additional_notes():
    """Test note content."""
    return '{"op_version": "2.0"}'


@pytest.fixture
def existing_notes_map():
    """Provides a fresh dictionary for the existing notes map per test."""
    return {}


@pytest.fixture
def mock_task_instance():
    """Sets up a mock task instance with predefined attributes."""
    return MagicMock(
        dag_id="test_dag", task_id="task_a", run_id="test_run", map_index=-1
    )


@pytest.fixture
def new_content():
    """Provides the test note content."""
    return "New Doc A"


@pytest.fixture
def task_instances():
    """Provides a dictionary of mocked Airflow task instances with
    specific task IDs and map indices for easy lookup.
    """
    return {
        "a": MagicMock(task_id="task_a", map_index=-1),
        "b_0": MagicMock(task_id="task_b", map_index=0),
        "b_1": MagicMock(task_id="task_b", map_index=1),
        "c": MagicMock(task_id="task_c", map_index=-1),
    }


@pytest.fixture
def mock_task_instances(task_instances):
    """Provides a flat list of the mocked Airflow task instances."""
    return [
        task_instances["a"],
        task_instances["b_0"],
        task_instances["b_1"],
        task_instances["c"],
    ]


@pytest.fixture
def mock_existing_notes():
    """Provides a list of mocked task instance notes representing records
    that already exist in the database.
    """
    return [
        MagicMock(task_id="task_a", map_index=-1),
        MagicMock(task_id="task_b", map_index=0),
    ]


@patch(MOCK_UPSERT_TASK_INSTANCE_NOTES)
@patch(MOCK_UPSERT_DAG_RUN_NOTE)
@patch(MOCK_EXTRACT_ADDITIONAL_NOTES, return_value='{"notes": "extracted"}')
def test_init_orchestration_pipeline_context_with_valid_context_succeeds(
    mock_extract_additional_notes,
    mock_upsert_dag_run_note,
    mock_upsert_task_instance_notes,
    note_content,
    full_context,
    mock_session,
    mock_create_session,
):
    """Tests the successful execution of the context initialization."""
    init_orchestration_pipeline_context(note_content, **full_context)

    mock_extract_additional_notes.assert_called_once_with(note_content)
    mock_create_session.assert_called_once()
    mock_upsert_dag_run_note.assert_called_once()
    mock_upsert_task_instance_notes.assert_called_once()
    mock_session.commit.assert_called_once()
    mock_session.rollback.assert_not_called()


def test_init_orchestration_pipeline_context_without_dag_run_raises_value_error(
    note_content, full_context, mock_create_session
):
    """Test that a ValueError is raised and no DB session is created
    when 'dag_run' is missing from the context.
    """
    full_context["dag_run"] = None

    with pytest.raises(ValueError, match="Missing 'dag_run'"):
        init_orchestration_pipeline_context(note_content, **full_context)

    mock_create_session.assert_not_called()


def test_init_orchestration_pipeline_context_without_dag_raises_value_error(
    note_content, full_context, mock_create_session
):
    """Test that a ValueError is raised and no DB session is created
    when 'dag' is missing from the context.
    """
    full_context["dag"] = None

    with pytest.raises(ValueError, match="Missing 'dag'"):
        init_orchestration_pipeline_context(note_content, **full_context)

    mock_create_session.assert_not_called()


@patch(
    MOCK_UPSERT_DAG_RUN_NOTE,
    side_effect=IntegrityError("mock_db_error", {}, {}),
)
def test_init_orchestration_pipeline_context_with_integrity_error_rolls_back_and_swallows_exception(  # noqa: E501
    mock_upsert_dag_run_note,
    note_content,
    full_context,
    mock_session,
    mock_create_session,
):
    """Test that an IntegrityError during the DB upsert triggers a
    rollback and is safely swallowed without raising an exception.
    """
    try:
        init_orchestration_pipeline_context(note_content, **full_context)
    except Exception:
        pytest.fail("Must not raise IntegrityError")

    mock_session.rollback.assert_called_once()
    mock_session.commit.assert_not_called()


@patch(
    MOCK_UPSERT_DAG_RUN_NOTE, side_effect=ValueError("A general database issue")
)
def test_init_orchestration_pipeline_context_with_db_exception_rolls_back_and_raises(  # noqa: E501
    mock_upsert_dag_run_note,
    note_content,
    full_context,
    mock_session,
    mock_create_session,
):
    """Test that an unexpected exception during the DB operation
    triggers a rollback, prevents a commit, and propagates the error.
    """
    with pytest.raises(ValueError, match="A general database issue"):
        init_orchestration_pipeline_context(note_content, **full_context)

    mock_session.rollback.assert_called_once()
    mock_session.commit.assert_not_called()


@patch(
    MOCK_UPSERT_TASK_INSTANCE_NOTES,
    side_effect=IntegrityError("mock_ti_db_error", {}, {}),
)
@patch(MOCK_UPSERT_DAG_RUN_NOTE)
def test_init_orchestration_pipeline_context_with_integrity_error_on_ti_notes_rolls_back(  # noqa: E501
    mock_upsert_dag_run_note,
    mock_upsert_task_instance_notes,
    note_content,
    full_context,
    mock_session,
    mock_create_session,
):
    """Test that an IntegrityError raised during task instance notes upsert
    after dag run note upsert succeeds still rolls back safely.
    """
    init_orchestration_pipeline_context(note_content, **full_context)

    mock_upsert_dag_run_note.assert_called_once()
    mock_session.rollback.assert_called_once()
    mock_session.commit.assert_not_called()


@patch(MOCK_DAG_RUN_NOTE)
def test_upsert_dag_run_note_without_existing_note_inserts_new_note(
    MockDagRunNote,
    mock_session,
    mock_dag_run,
    additional_notes,
):
    """Test that a new note is successfully created and added to the session if
    none exists.
    """
    mock_session.query().filter_by().first.return_value = None
    mock_new_note = MockDagRunNote.return_value

    def _fake_dag_run_note_init(content=None, **kwargs):
        mock_new_note.content = content
        return mock_new_note

    MockDagRunNote.side_effect = _fake_dag_run_note_init

    _upsert_dag_run_note(mock_session, additional_notes, mock_dag_run)

    MockDagRunNote.assert_called_once_with(additional_notes)
    assert mock_new_note.dag_run_id == mock_dag_run.id
    assert mock_new_note.content == additional_notes
    mock_session.add.assert_called_once_with(mock_new_note)


@patch(MOCK_DAG_RUN_NOTE)
def test_upsert_dag_run_note_with_different_content_updates_existing_note(
    MockDagRunNote, mock_session, mock_dag_run, additional_notes
):
    """Test that an existing note's content is updated in-place when new content
    differs.
    """
    mock_existing_note = MagicMock(content='{"op_version": "1.0"}')
    mock_session.query().filter_by().first.return_value = mock_existing_note

    _upsert_dag_run_note(mock_session, additional_notes, mock_dag_run)

    assert mock_existing_note.content == additional_notes
    MockDagRunNote.assert_not_called()
    mock_session.add.assert_not_called()


@patch(MOCK_DAG_RUN_NOTE)
def test_upsert_dag_run_note_with_same_content_does_not_update(
    MockDagRunNote, mock_session, mock_dag_run, additional_notes
):
    """Test that no database session modifications occur if the existing note
    has identical content.
    """
    mock_existing_note = MagicMock(content=additional_notes)
    mock_session.query().filter_by().first.return_value = mock_existing_note

    _upsert_dag_run_note(mock_session, additional_notes, mock_dag_run)

    assert mock_existing_note.content == additional_notes
    MockDagRunNote.assert_not_called()
    mock_session.add.assert_not_called()


@patch(MOCK_TASK_INSTANCE_NOTE)
def test_get_task_instance_notes_with_valid_session_returns_notes(
    MockTaskInstanceNote,
    mock_session,
    mock_dag_run,
):
    """Test that _get_task_instance_notes builds and executes the query
    correctly.
    """
    mock_result = [MagicMock(), MagicMock()]
    mock_session.query.return_value.filter.return_value.all.return_value = (
        mock_result
    )

    result = _get_task_instance_notes(mock_session, mock_dag_run)

    assert result == mock_result
    mock_session.query.assert_called_once_with(MockTaskInstanceNote)
    mock_session.query().filter.assert_called_once()
    mock_session.query().filter().all.assert_called_once()


@patch(MOCK_TASK_INSTANCE)
def test_get_task_instances_with_valid_session_returns_instances(
    mock_task_instance_class,
    mock_session,
    mock_dag_run,
):
    """Test that _get_task_instances builds and executes the query correctly."""
    mock_result = [MagicMock(), MagicMock()]
    mock_session.query.return_value.filter.return_value.all.return_value = (
        mock_result
    )

    result = _get_task_instances(mock_session, mock_dag_run)

    assert result == mock_result
    mock_session.query.assert_called_once_with(mock_task_instance_class)
    mock_session.query().filter.assert_called_once()
    mock_session.query().filter().all.assert_called_once()


@patch(MOCK_TASK_INSTANCE_NOTE)
def test_upsert_task_instance_note_without_existing_note_inserts_new_note(
    MockTaskInstanceNote,
    mock_session,
    existing_notes_map,
    mock_task_instance,
    new_content,
):
    """Test that a new task instance note is created and added to the session if
    none exists.
    """
    mock_new_ti_note = MockTaskInstanceNote.return_value

    _upsert_task_instance_note(
        mock_session,
        existing_notes_map,
        mock_task_instance,
        new_content,
    )

    MockTaskInstanceNote.assert_called_once_with(new_content)
    assert mock_new_ti_note.dag_id == "test_dag"
    assert mock_new_ti_note.task_id == "task_a"
    assert mock_new_ti_note.run_id == "test_run"
    assert mock_new_ti_note.map_index == -1
    mock_session.add.assert_called_once_with(mock_new_ti_note)


@patch(MOCK_TASK_INSTANCE_NOTE)
def test_upsert_task_instance_note_with_different_content_updates_existing_note(
    MockTaskInstanceNote,
    mock_session,
    existing_notes_map,
    mock_task_instance,
    new_content,
):
    """Test that an existing task instance note is updated in-place when content
    changes.
    """
    mock_existing_note = MagicMock(content="Old Doc A")
    existing_notes_map[("task_a", -1)] = mock_existing_note

    _upsert_task_instance_note(
        mock_session,
        existing_notes_map,
        mock_task_instance,
        new_content,
    )

    assert mock_existing_note.content == new_content
    MockTaskInstanceNote.assert_not_called()
    mock_session.add.assert_not_called()


@patch(MOCK_TASK_INSTANCE_NOTE)
def test_upsert_task_instance_note_with_same_content_does_not_update(
    MockTaskInstanceNote,
    mock_session,
    existing_notes_map,
    mock_task_instance,
    new_content,
):
    """Test that no database or session changes occur if the note content is
    already identical.
    """
    mock_existing_note = MagicMock(content=new_content)
    existing_notes_map[("task_a", -1)] = mock_existing_note

    _upsert_task_instance_note(
        mock_session,
        existing_notes_map,
        mock_task_instance,
        new_content,
    )

    assert mock_existing_note.content == new_content
    MockTaskInstanceNote.assert_not_called()
    mock_session.add.assert_not_called()


@patch(MOCK_UPSERT_TASK_INSTANCE_NOTE, new_callable=MagicMock)
@patch(MOCK_GET_TASK_INSTANCES, autospec=True)
@patch(MOCK_GET_TASK_INSTANCE_NOTES, autospec=True)
def test_upsert_task_instance_notes_with_multiple_instances_upserts_all_notes(
    mock_get_task_instance_notes,
    mock_get_task_instances,
    mock_upsert_task_instance_note,
    mock_session,
    mock_dag,
    mock_dag_run,
    mock_existing_notes,
    mock_task_instances,
    task_instances,
):
    """Test the full notes upsert orchestrator flow for multiple task
    instances.
    """
    mock_get_task_instance_notes.return_value = mock_existing_notes
    mock_get_task_instances.return_value = mock_task_instances
    expected_existing_notes_map = {
        (n.task_id, n.map_index): n for n in mock_existing_notes
    }

    _upsert_task_instance_notes(mock_session, mock_dag, mock_dag_run)

    mock_get_task_instance_notes.assert_called_once_with(
        mock_session, mock_dag_run
    )
    mock_get_task_instances.assert_called_once_with(mock_session, mock_dag_run)
    assert mock_upsert_task_instance_note.call_count == 3
    mock_upsert_task_instance_note.assert_any_call(
        mock_session, expected_existing_notes_map, task_instances["a"], "Doc A"
    )
    mock_upsert_task_instance_note.assert_any_call(
        mock_session,
        expected_existing_notes_map,
        task_instances["b_0"],
        "Doc B",
    )
    mock_upsert_task_instance_note.assert_any_call(
        mock_session,
        expected_existing_notes_map,
        task_instances["b_1"],
        "Doc B",
    )


@patch(MOCK_UPSERT_TASK_INSTANCE_NOTE, new_callable=MagicMock)
@patch(MOCK_GET_TASK_INSTANCES, autospec=True)
@patch(MOCK_GET_TASK_INSTANCE_NOTES, autospec=True)
def test_upsert_task_instance_notes_without_doc_md_skips_upsert(
    mock_get_task_instance_notes,
    mock_get_task_instances,
    mock_upsert_task_instance_note,
    mock_session,
    mock_dag,
    mock_dag_run,
    task_instances,
):
    """Test that task instances whose task definition lacks markdown docs are
    skipped.
    """
    mock_get_task_instance_notes.return_value = []
    mock_get_task_instances.return_value = [task_instances["c"]]

    _upsert_task_instance_notes(mock_session, mock_dag, mock_dag_run)

    mock_upsert_task_instance_note.assert_not_called()


@patch(MOCK_UPSERT_TASK_INSTANCE_NOTE, new_callable=MagicMock)
@patch(MOCK_GET_TASK_INSTANCES, autospec=True)
@patch(MOCK_GET_TASK_INSTANCE_NOTES, autospec=True)
def test_upsert_task_instance_notes_with_task_missing_from_dag_skips_upsert(
    mock_get_task_instance_notes,
    mock_get_task_instances,
    mock_upsert_task_instance_note,
    mock_session,
    mock_dag,
    mock_dag_run,
):
    """Test that task instances whose task_id is absent from dag.tasks are
    safely skipped.
    """
    orphan_ti = MagicMock(task_id="orphan_task_not_in_dag", map_index=-1)
    mock_get_task_instance_notes.return_value = []
    mock_get_task_instances.return_value = [orphan_ti]

    _upsert_task_instance_notes(mock_session, mock_dag, mock_dag_run)

    mock_upsert_task_instance_note.assert_not_called()


def test_get_deps_returns_expected_dependencies():
    """Tests that get_deps returns the expected Airflow 2 dependencies."""
    from airflow.operators.python import PythonOperator

    from orchestration_pipelines_lib.dag_generator.airflow_adapters.common_utils import (  # noqa: E501
        dag_utils,
    )

    deps = get_deps()

    assert deps == dag_utils.AirflowVersionedDependencies(
        task_factory=task_factory,
        emails_callback=send_notification_email,
        init_pipeline_context=init_orchestration_pipeline_context,
        init_task_operator=PythonOperator,
    )


@pytest.fixture
def mock_db():
    """Fixture to mock Airflow DB session, models and query chain."""
    with (
        patch(
            "airflow.utils.session.create_session", autospec=True
        ) as mock_create_session,
        patch("airflow.models.DagRun", autospec=True) as MockDagRun,
        patch("airflow.utils.state.State", autospec=True) as MockState,
    ):
        mock_query = MagicMock()
        mock_query.filter.return_value = mock_query

        mock_session_context = MagicMock()
        mock_session_context.__enter__.return_value = mock_session_context
        mock_session_context.query.return_value = mock_query
        mock_create_session.return_value = mock_session_context

        MockState.RUNNING = "running"
        MockState.QUEUED = "queued"

        yield {
            "create_session": mock_create_session,
            "session_context": mock_session_context,
            "query": mock_query,
            "DagRun": MockDagRun,
            "State": MockState,
        }


def _run_and_assert_active_versions(
    mock_db, pipeline_id, bundle_id, mock_runs, expected_versions
):
    """Helper function to run the target function and assert DB queries."""
    mock_db["query"].all.return_value = mock_runs
    active_states = [mock_db["State"].RUNNING, mock_db["State"].QUEUED]
    dag_id_pattern = f"{bundle_id}__v__%__{pipeline_id}"

    result = get_actively_running_versions(pipeline_id, bundle_id)

    mock_db["create_session"].assert_called_once()
    mock_db["session_context"].query.assert_called_once_with(
        mock_db["DagRun"].dag_id
    )
    mock_db["query"].filter.assert_called_once()
    mock_db["DagRun"].state.in_.assert_called_once_with(active_states)
    mock_db["DagRun"].dag_id.like.assert_called_once_with(dag_id_pattern)
    assert sorted(result) == sorted(expected_versions)


def test_get_actively_running_versions_with_active_runs_returns_parsed_versions(
    mock_db,
):
    """Test that active versions are successfully parsed and de-duplicated from
    running DAG IDs.
    """
    pipeline_id = "my_pipe"
    bundle_id = "b1"
    mock_runs = [
        (f"{bundle_id}__v__1.0.0__{pipeline_id}",),
        (f"{bundle_id}__v__1.0.0__{pipeline_id}",),
        (f"{bundle_id}__v__2.1.3-beta__{pipeline_id}",),
        (f"{bundle_id}__v__3.0__{pipeline_id}",),
    ]
    expected_versions = ["1.0.0", "2.1.3-beta", "3.0"]

    _run_and_assert_active_versions(
        mock_db=mock_db,
        pipeline_id=pipeline_id,
        bundle_id=bundle_id,
        mock_runs=mock_runs,
        expected_versions=expected_versions,
    )


def test_get_actively_running_versions_without_active_runs_returns_empty_list(
    mock_db,
):
    """Test that an empty list is returned when there are no active DAG runs in
    the database.
    """
    pipeline_id = "my_pipe"
    bundle_id = "b1"

    _run_and_assert_active_versions(
        mock_db=mock_db,
        pipeline_id=pipeline_id,
        bundle_id=bundle_id,
        mock_runs=[],
        expected_versions=[],
    )


def test_get_actively_running_versions_with_specific_ids_returns_filtered_versions(  # noqa: E501
    mock_db,
):
    """Test that the version parsing behaves correctly for different
    combinations of bundle and pipeline IDs.
    """
    pipeline_id = "other_pipe"
    bundle_id = "b2"
    mock_runs = [
        (f"{bundle_id}__v__abc__{pipeline_id}",),
    ]
    expected_versions = ["abc"]

    _run_and_assert_active_versions(
        mock_db=mock_db,
        pipeline_id=pipeline_id,
        bundle_id=bundle_id,
        mock_runs=mock_runs,
        expected_versions=expected_versions,
    )


@pytest.fixture
def previous_versions_setup():
    """Sets up the mock DB session, pipeline and bundle identifiers,
    and expected metadata tags.
    """
    pipeline_id = "test_pipe"
    bundle_id = "b1"

    return {
        "pipeline_id": pipeline_id,
        "bundle_id": bundle_id,
        "mock_session": MagicMock(),
        "expected_tags": [
            "op:is_current",
            f"op:bundle:{bundle_id}",
            f"op:pipeline:{pipeline_id}",
        ],
    }


def test_extract_versions_with_valid_tags_returns_deduplicated_versions():
    """Test that versions are correctly parsed and de-duplicated from a list of
    DAG tag tuples.
    """
    input_tags = [
        ("dag_1", "op:version:v1.0.0"),
        ("dag_2", "op:version:v2.0.0"),
        ("dag_1", "op:version:v1.0.0"),
        ("dag_3", "op:version:v3.0.0"),
        ("dag_5", "op:version:"),
    ]
    expected_versions = ["v1.0.0", "v2.0.0", "v3.0.0"]

    result = _extract_versions(input_tags)

    assert len(result) == len(expected_versions)
    assert sorted(result) == sorted(expected_versions)


def test_extract_versions_without_version_tags_returns_empty_list():
    """Test that an empty list is returned when none of the tags contain a valid
    version suffix.
    """
    input_tags: list[tuple[str, str]] = [
        ("dag_1", "op:version:"),
        ("dag_2", "op:version:"),
    ]
    expected_versions: list[str] = []

    result = _extract_versions(input_tags)

    assert result == expected_versions


@patch(MOCK_DAG_TAG)
@patch("sqlalchemy.func")
def test_get_dag_tags_subquery_with_valid_ids_returns_subquery(
    mock_func, MockDagTag, previous_versions_setup
):
    """Test that _get_dag_tags_subquery constructs the correct SQLAlchemy
    aggregation query.
    """
    setup = previous_versions_setup
    mock_session = setup["mock_session"]
    mock_query = MagicMock()
    mock_session.query.return_value = mock_query
    mock_query.filter.return_value = mock_query
    mock_query.group_by.return_value = mock_query
    mock_query.having.return_value = mock_query

    result = _get_dag_tags_subquery(
        mock_session, setup["pipeline_id"], setup["bundle_id"]
    )

    mock_session.query.assert_called_once_with(MockDagTag.dag_id)
    MockDagTag.name.in_.assert_called_once_with(setup["expected_tags"])
    mock_query.filter.assert_called_once()
    mock_query.group_by.assert_called_once_with(MockDagTag.dag_id)
    mock_func.count.assert_called_once_with(MockDagTag.name)
    mock_func.count.return_value.__eq__.assert_called_once_with(3)
    mock_query.having.assert_called_once()
    mock_query.subquery.assert_called_once()
    assert result == mock_query.subquery.return_value


@patch(MOCK_DAG_TAG)
def test_get_tags_with_valid_subquery_returns_version_tags(
    MockDagTag, previous_versions_setup
):
    """Test that _get_tags builds the query to retrieve version tags filtered
    by the subquery.
    """
    mock_session = previous_versions_setup["mock_session"]
    mock_subquery = MagicMock(spec=Subquery)
    mock_runs = [
        ("dag_1", "op:version:v1.0.0"),
        ("dag_2", "op:version:v2.0.0"),
    ]
    mock_query = MagicMock()
    mock_session.query.return_value = mock_query
    mock_query.filter.return_value = mock_query
    mock_query.all.return_value = mock_runs

    result = _get_tags(mock_session, mock_subquery)

    mock_session.query.assert_called_once_with(
        MockDagTag.dag_id, MockDagTag.name
    )
    MockDagTag.dag_id.in_.assert_called_once_with(mock_subquery)
    MockDagTag.name.like.assert_called_once_with("op:version:%")
    mock_query.filter.assert_called_once()
    mock_query.all.assert_called_once()
    assert result == mock_runs


@pytest.fixture
def prev_default_versions_setup():
    """Sets up mock pipeline and bundle identifiers alongside the expected
    version list.
    """
    return {
        "pipeline_id": "test_pipe",
        "bundle_id": "b1",
        "expected_versions": ["v1.0.0", "v2.0.0"],
    }


@patch(f"{TARGET_MODULE}._extract_versions", autospec=True)
@patch(f"{TARGET_MODULE}._get_tags", autospec=True)
@patch(f"{TARGET_MODULE}._get_dag_tags_subquery", autospec=True)
@patch(MOCK_CREATE_SESSION, autospec=True)
def test_get_previous_default_versions_with_valid_ids_orchestrates_data_retrieval(  # noqa: E501
    mock_create_session,
    mock_get_dag_tags_subquery,
    mock_get_tags,
    mock_extract_versions,
    prev_default_versions_setup,
):
    """Test that the orchestrator executes all underlying database and
    extraction steps in sequence.
    """
    setup = prev_default_versions_setup
    pipeline_id = setup["pipeline_id"]
    bundle_id = setup["bundle_id"]
    expected_versions = setup["expected_versions"]
    mock_session = MagicMock()
    mock_create_session.return_value.__enter__.return_value = mock_session
    mock_subquery_instance = MagicMock(spec=Subquery)
    mock_get_dag_tags_subquery.return_value = mock_subquery_instance
    mock_tags_output = [("dag1", "tag1")]
    mock_get_tags.return_value = mock_tags_output
    mock_extract_versions.return_value = expected_versions

    result = get_previous_default_versions(pipeline_id, bundle_id)

    mock_create_session.assert_called_once()
    mock_get_dag_tags_subquery.assert_called_once_with(
        mock_session, pipeline_id, bundle_id
    )
    mock_get_tags.assert_called_once_with(mock_session, mock_subquery_instance)
    mock_extract_versions.assert_called_once_with(mock_tags_output)
    assert result == expected_versions


if __name__ == "__main__":
    unittest.main()
