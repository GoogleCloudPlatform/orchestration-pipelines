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

import json
import unittest
from typing import Any
from unittest.mock import ANY, MagicMock, patch

import pytest
from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy.exc import IntegrityError
from sqlalchemy.sql.selectable import Subquery

from orchestration_pipelines_lib.dag_generator.airflow_adapters.airflow_2.core import (  # noqa: E501
    _build_dag_kwargs,
    _configure_dag_schedule,
    _create_init_task,
    _create_tasks,
    _extract_additional_notes,
    _extract_versions,
    _get_dag_tags_subquery,
    _get_tags,
    _get_task_instance_notes,
    _get_task_instances,
    _set_dependencies,
    _upsert_dag_run_note,
    _upsert_task_instance_note,
    _upsert_task_instance_notes,
    generate,
    get_actively_running_versions,
    get_previous_default_versions,
    init_orchestration_pipeline_context,
)

TARGET_MODULE = (
    "orchestration_pipelines_lib.dag_generator.airflow_adapters.airflow_2.core"
)

MOCK_CREATE_SESSION = "airflow.utils.session.create_session"
MOCK_DAG_RUN_NOTE = "airflow.models.dagrun.DagRunNote"
MOCK_TASK_INSTANCE = "airflow.models.TaskInstance"
MOCK_TASK_INSTANCE_NOTE = "airflow.models.taskinstance.TaskInstanceNote"
MOCK_DAG_TAG = "airflow.models.DagTag"
MOCK_SCHEDULE_TRIGGER_MODEL = (
    "orchestration_pipelines_lib.internal_models.triggers.ScheduleTriggerModel"
)

MOCK_PARTIAL = "functools.partial"
MOCK_ACTION_REGISTRY = f"{TARGET_MODULE}.action_handler_registry"

MOCK_EXTRACT_ADDITIONAL_NOTES = f"{TARGET_MODULE}._extract_additional_notes"
MOCK_UPSERT_DAG_RUN_NOTE = f"{TARGET_MODULE}._upsert_dag_run_note"
MOCK_UPSERT_TASK_INSTANCE_NOTES = f"{TARGET_MODULE}._upsert_task_instance_notes"
MOCK_GET_TASK_INSTANCE_NOTES = f"{TARGET_MODULE}._get_task_instance_notes"
MOCK_GET_TASK_INSTANCES = f"{TARGET_MODULE}._get_task_instances"
MOCK_UPSERT_TASK_INSTANCE_NOTE = f"{TARGET_MODULE}._upsert_task_instance_note"
MOCK_BUILD_DAG_KWARGS = f"{TARGET_MODULE}._build_dag_kwargs"
MOCK_CONFIGURE_DAG_SCHEDULE = f"{TARGET_MODULE}._configure_dag_schedule"
MOCK_CREATE_INIT_TASK = f"{TARGET_MODULE}._create_init_task"
MOCK_CREATE_TASKS = f"{TARGET_MODULE}._create_tasks"
MOCK_SET_DEPENDENCIES = f"{TARGET_MODULE}._set_dependencies"
MOCK_SEND_FAILURE_NOTIFICATION_EMAIL = (
    f"{TARGET_MODULE}.send_failure_notification_email"
)
MOCK_TASK_FACTORY = f"{TARGET_MODULE}.task_factory"


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


def test_extract_additional_notes_with_valid_json_returns_allowed_keys():
    """Test extraction of only allowed metadata keys from a valid JSON dict."""
    input_json = {
        "op_bundle": "my_bundle",
        "op_version": "1.0",
        "op_owner": "user@google.com",
        "ignored_key": "some_value",
        "op_pipeline": "pipeline_name",
    }
    expected_dict = {
        "op_bundle": "my_bundle",
        "op_version": "1.0",
        "op_owner": "user@google.com",
        "op_pipeline": "pipeline_name",
    }
    input_content = json.dumps(input_json)
    expected_output = json.dumps(expected_dict, indent=4)

    result = _extract_additional_notes(input_content)

    assert result == expected_output


@pytest.mark.parametrize(
    "empty_input",
    [
        None,
        "",
    ],
)
def test_extract_additional_notes_with_empty_input_returns_empty_string(
    empty_input,
):
    """Test that passing None or an empty string returns an empty string."""
    result = _extract_additional_notes(empty_input)

    assert result == ""


@pytest.mark.parametrize(
    "non_dict_json",
    [
        "[1, 2, 3]",
        '"simple_string"',
    ],
)
def test_extract_additional_notes_with_non_dict_json_returns_empty_string(
    non_dict_json,
):
    """Test that passing valid JSON that is not a dictionary returns an empty
    string.
    """
    result = _extract_additional_notes(non_dict_json)

    assert result == ""


def test_extract_additional_notes_with_invalid_json_raises_json_decode_error():
    """Test that passing syntactically invalid JSON raises a JSONDecodeError."""
    with pytest.raises(json.JSONDecodeError):
        _extract_additional_notes("invalid json")


def test_extract_additional_notes_without_allowed_keys_returns_empty_string():
    """Test that a dictionary with zero matching metadata keys returns an empty
    string.
    """
    input_content = json.dumps({"key_a": 1, "key_b": 2})

    result = _extract_additional_notes(input_content)

    assert result == ""


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


@pytest.fixture
def pipeline_setup():
    """Sets up the mock pipeline metadata, configuration defaults, and pipeline
    model.
    """
    mock_defaults = MagicMock(executionConfigDefault=MagicMock(retries=3))
    mock_metadata = MagicMock(
        pipelineId="test_pipe", description="Desc", owner="team"
    )
    mock_pipeline = MagicMock(metadata=mock_metadata, defaults=mock_defaults)
    return {
        "pipeline": mock_pipeline,
        "pipeline_id": "my_pipe",
        "bundle_id": "bundle_id",
    }


def test_build_dag_kwargs_with_data_root_returns_kwargs_with_template_searchpath(  # noqa: E501
    pipeline_setup,
):
    """Test that dag_kwargs are correctly populated when a valid data_root is
    provided.
    """
    tags = ["tag1", "tag2"]
    dag_notes = "## Pipeline Notes"
    data_root = "/path/to/data"
    expected_kwargs = {
        "dag_id": "test_pipe",
        "description": "Desc",
        "default_args": {
            "owner": "team",
            "retries": 3,
        },
        "tags": tags,
        "template_searchpath": [data_root],
        "doc_md": dag_notes,
        "on_failure_callback": [ANY, ANY],
        "on_success_callback": [ANY],
    }

    result = _build_dag_kwargs(
        pipeline_setup["pipeline"],
        tags,
        dag_notes,
        data_root,
        pipeline_setup["bundle_id"],
        pipeline_setup["pipeline_id"],
    )

    assert result == expected_kwargs


def test_build_dag_kwargs_without_data_root_returns_kwargs_without_template_searchpath(  # noqa: E501
    pipeline_setup,
):
    """Test that template_searchpath is empty when an empty data_root is
    provided.
    """
    tags = []
    dag_notes = "Notes"
    data_root = ""

    result = _build_dag_kwargs(
        pipeline_setup["pipeline"],
        tags,
        dag_notes,
        data_root,
        pipeline_setup["bundle_id"],
        pipeline_setup["pipeline_id"],
    )

    assert result.get("template_searchpath") == []


@pytest.fixture
def schedule_setup():
    """Sets up empty DAG kwargs and mock objects for schedule and non-schedule
    triggers.
    """
    return {
        "dag_kwargs": {},
        "mock_schedule_trigger": MagicMock(spec=Any),
        "mock_non_schedule_trigger": MagicMock(spec=Any),
    }


@patch(MOCK_TASK_FACTORY)
def test_configure_dag_schedule_with_schedule_trigger_creates_trigger_task(
    mock_task_factory, schedule_setup
):
    """Test that a schedule trigger task is created and 'schedule' is not set
    when a valid trigger exists.
    """
    dag_kwargs = schedule_setup["dag_kwargs"]
    mock_schedule_trigger = schedule_setup["mock_schedule_trigger"]
    mock_non_schedule_trigger = schedule_setup["mock_non_schedule_trigger"]
    triggers = [
        mock_non_schedule_trigger,
        mock_schedule_trigger,
    ]

    with patch(
        MOCK_SCHEDULE_TRIGGER_MODEL,
        new=type(mock_schedule_trigger),
    ):
        _configure_dag_schedule(dag_kwargs, triggers)

    mock_task_factory.create_schedule_trigger_task.assert_called_once_with(
        dag_kwargs, mock_schedule_trigger
    )
    assert "schedule" not in dag_kwargs


@patch(MOCK_TASK_FACTORY)
def test_configure_dag_schedule_without_schedule_trigger_sets_schedule_none(
    mock_task_factory, schedule_setup
):
    """Test that 'schedule' is set to None and no task is created when no
    schedule trigger is found.
    """
    dag_kwargs = schedule_setup["dag_kwargs"]
    mock_non_schedule_trigger = schedule_setup["mock_non_schedule_trigger"]
    triggers = [mock_non_schedule_trigger]

    class MockScheduleTriggerModel:
        pass

    with patch(
        MOCK_SCHEDULE_TRIGGER_MODEL,
        new=MockScheduleTriggerModel,
    ):
        _configure_dag_schedule(dag_kwargs, triggers)

    assert dag_kwargs.get("schedule") is None
    mock_task_factory.create_schedule_trigger_task.assert_not_called()


@pytest.fixture
def init_task_setup():
    """Sets up the mock DAG and documentation notes for the initialization
    task.
    """
    return {"mock_dag": MagicMock(spec=DAG), "dag_notes": "Notes for init"}


@patch("airflow.operators.python.PythonOperator", spec=PythonOperator)
@patch(f"{TARGET_MODULE}.init_orchestration_pipeline_context")
def test_create_init_task_with_valid_inputs_creates_python_operator(
    mock_init_callable, MockPythonOperator, init_task_setup
):
    """Test that the PythonOperator is instantiated with correct parameters and
    linked to the DAG.
    """
    mock_dag = init_task_setup["mock_dag"]
    dag_notes = init_task_setup["dag_notes"]
    bundle_id = "bundle_id"
    pipeline_id = "pipeline_id"

    _create_init_task(bundle_id, pipeline_id, mock_dag, dag_notes)

    MockPythonOperator.assert_called_once_with(
        task_id="init_orchestration_pipeline_context",
        python_callable=mock_init_callable,
        op_args=[dag_notes],
        dag=mock_dag,
        on_failure_callback=[ANY],
        on_success_callback=[ANY],
    )


@pytest.fixture
def create_tasks_setup():
    """Sets up mock actions, action handlers mapping, and the pipeline with
    registered actions.
    """
    mock_dag = MagicMock(spec=DAG)
    mock_pipeline = MagicMock()

    action_type_a = type("ActionA", (object,), {"name": "task_a"})
    action_type_b = type("ActionB", (object,), {"name": "task_b"})

    action_a = action_type_a()
    action_b = action_type_b()
    action_c_no_handler = MagicMock(name="task_c")

    mock_pipeline.actions = [
        action_a,
        action_b,
        action_c_no_handler,
    ]

    mock_handler_a = MagicMock(return_value=MagicMock(task_id="task_a_obj"))
    mock_handler_b = MagicMock(return_value=MagicMock(task_id="task_b_obj"))

    action_handlers = {
        action_type_a: mock_handler_a,
        action_type_b: mock_handler_b,
    }

    return {
        "mock_dag": mock_dag,
        "mock_pipeline": mock_pipeline,
        "action_a": action_a,
        "action_b": action_b,
        "mock_handler_a": mock_handler_a,
        "mock_handler_b": mock_handler_b,
        "action_handlers": action_handlers,
    }


def test_create_tasks_with_registered_handlers_creates_tasks(
    create_tasks_setup,
):
    """Test that tasks are successfully created and mapped when matching action
    handlers are found.
    """
    setup = create_tasks_setup
    mock_dag = setup["mock_dag"]
    mock_pipeline = setup["mock_pipeline"]
    mock_handler_a = setup["mock_handler_a"]
    mock_handler_b = setup["mock_handler_b"]

    tasks = _create_tasks(mock_dag, setup["action_handlers"], mock_pipeline)

    mock_handler_a.assert_called_once_with(
        setup["action_a"], mock_pipeline, dag=mock_dag
    )
    mock_handler_b.assert_called_once_with(
        setup["action_b"], mock_pipeline, dag=mock_dag
    )
    assert len(tasks) == 2
    assert "task_a" in tasks
    assert "task_b" in tasks
    assert tasks["task_a"] == mock_handler_a.return_value
    assert tasks["task_b"] == mock_handler_b.return_value


def test_create_tasks_without_registered_handlers_skips_actions(
    create_tasks_setup,
):
    """Test that actions without any registered handler are silently ignored
    during task creation.
    """
    setup = create_tasks_setup

    tasks = _create_tasks(
        setup["mock_dag"], setup["action_handlers"], setup["mock_pipeline"]
    )

    assert len(tasks) == 2
    setup["mock_handler_a"].assert_called_once()
    setup["mock_handler_b"].assert_called_once()


@pytest.fixture
def dependencies_setup():
    """Sets up mock upstream and downstream tasks, and various mock action
    dependency scenarios.
    """
    task_up = MagicMock(name="upstream", set_upstream=MagicMock())
    task_down = MagicMock(name="downstream", set_upstream=MagicMock())
    tasks = {"up": task_up, "down": task_down}

    mock_action_depends = MagicMock(dependsOn=["up"])
    mock_action_depends.name = "down"

    mock_action_no_depends = MagicMock(dependsOn=None)
    mock_action_no_depends.name = "down"

    mock_action_no_task = MagicMock(dependsOn=["up"])
    mock_action_no_task.name = "missing"

    return {
        "task_up": task_up,
        "task_down": task_down,
        "tasks": tasks,
        "mock_action_depends": mock_action_depends,
        "mock_action_no_depends": mock_action_no_depends,
        "mock_action_no_task": mock_action_no_task,
    }


def test_set_dependencies_with_valid_upstream_sets_dependency(
    dependencies_setup,
):
    """Test that the downstream task correctly registers the upstream task as
    its dependency.
    """
    setup = dependencies_setup

    _set_dependencies(setup["tasks"], setup["mock_action_depends"])

    setup["task_down"].set_upstream.assert_called_once_with(setup["task_up"])


def test_set_dependencies_without_upstream_does_nothing(dependencies_setup):
    """Test that no task relationships are modified when an action has no
    defined dependencies.
    """
    setup = dependencies_setup

    _set_dependencies(setup["tasks"], setup["mock_action_no_depends"])

    setup["task_down"].set_upstream.assert_not_called()
    setup["task_up"].set_upstream.assert_not_called()


def test_set_dependencies_with_action_not_in_tasks_does_nothing(
    dependencies_setup,
):
    """Test that task configuration is skipped if the action itself does not
    exist in the task dictionary.
    """
    setup = dependencies_setup
    mock_action_depends = setup["mock_action_depends"]
    mock_action_depends.name = "missing_task"

    _set_dependencies(setup["tasks"], mock_action_depends)

    setup["task_down"].set_upstream.assert_not_called()
    setup["task_up"].set_upstream.assert_not_called()


def test_set_dependencies_with_unresolved_dependency_raises_value_error(
    dependencies_setup,
):
    """Test that a ValueError is raised when an action references a non-existent
    upstream task.
    """
    setup = dependencies_setup
    mock_action_depends = setup["mock_action_depends"]
    mock_action_depends.dependsOn = ["missing_dep"]

    with pytest.raises(
        ValueError,
        match=(
            "Task missing_dep being upstream dependency for down"
            " does not exist."
        ),
    ):
        _set_dependencies(setup["tasks"], mock_action_depends)


@pytest.fixture
def generate_setup():
    """Sets up the mock pipeline model, pipeline tags, and the mock
    Airflow DAG.
    """
    mock_pipeline = MagicMock(
        metadata=MagicMock(pipelineId="test_pipe", description="Desc"),
        defaults=MagicMock(executionConfigDefault=MagicMock(retries=3)),
        notifications=MagicMock(),
        triggers=[],
        actions=[],
    )

    return {
        "mock_pipeline": mock_pipeline,
        "tags": ["t1"],
        "dag_notes": "Notes",
        "data_root": "/data",
        "pipeline_id": "my_pipe",
        "bundle_id": "bundle_id",
        "mock_final_dag": MagicMock(spec=DAG),
    }


@patch("airflow.models.DAG", autospec=True)
@patch(MOCK_SET_DEPENDENCIES, autospec=True)
@patch(MOCK_CREATE_TASKS, autospec=True)
@patch(MOCK_CREATE_INIT_TASK, autospec=True)
@patch(MOCK_CONFIGURE_DAG_SCHEDULE, autospec=True)
@patch(MOCK_BUILD_DAG_KWARGS, autospec=True)
@patch(MOCK_ACTION_REGISTRY)
def test_generate_with_valid_pipeline_orchestrates_dag_creation(
    mock_action_registry,
    mock_build_dag_kwargs,
    mock_configure_dag_schedule,
    mock_create_init_task,
    mock_create_tasks,
    mock_set_dependencies,
    MockDAG,
    generate_setup,
):
    """Test that the DAG generation flow correctly invokes all helper and
    configuration functions.
    """
    setup = generate_setup
    mock_pipeline = setup["mock_pipeline"]
    tags = setup["tags"]
    dag_notes = setup["dag_notes"]
    data_root = setup["data_root"]
    bundle_id = setup["bundle_id"]
    pipeline_id = setup["pipeline_id"]

    mock_handlers = {}
    mock_action_registry.get_action_handlers.return_value = mock_handlers
    mock_kwargs_in = {"dag_id": "test_pipe", "doc_md": dag_notes}
    mock_build_dag_kwargs.return_value = mock_kwargs_in
    mock_tasks_out = {"t1": MagicMock(), "t2": MagicMock()}
    mock_create_tasks.return_value = mock_tasks_out

    result_dag = generate(
        mock_pipeline,
        tags,
        dag_notes,
        data_root,
        bundle_id,
        pipeline_id,
    )

    mock_action_registry.get_action_handlers.assert_called_once()
    mock_build_dag_kwargs.assert_called_once_with(
        mock_pipeline,
        tags,
        dag_notes,
        data_root,
        bundle_id,
        pipeline_id,
    )
    mock_configure_dag_schedule.assert_called_once_with(
        mock_kwargs_in, mock_pipeline.triggers
    )
    MockDAG.assert_called_once_with(**mock_kwargs_in)
    mock_dag_instance = MockDAG.return_value
    mock_create_init_task.assert_called_once_with(
        bundle_id, pipeline_id, mock_dag_instance, dag_notes
    )
    mock_create_tasks.assert_called_once_with(
        mock_dag_instance, mock_handlers, mock_pipeline
    )
    assert mock_set_dependencies.call_count == len(mock_pipeline.actions)
    assert result_dag == mock_dag_instance


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
