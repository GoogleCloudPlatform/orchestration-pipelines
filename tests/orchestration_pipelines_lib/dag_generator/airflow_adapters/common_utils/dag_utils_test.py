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
"""Tests for dag_utils."""

import json
import unittest
from datetime import datetime
from functools import partial
from typing import Any
from unittest.mock import ANY, MagicMock, patch

import pytest
import pytz

from orchestration_pipelines_lib.dag_generator.airflow_adapters.common_utils.dag_utils import (  # noqa: E501
    AirflowVersionedDependencies,
    DAGKwargs,
    _build_dag_kwargs,
    _configure_dag_schedule,
    _create_init_task,
    _create_tasks,
    _set_dependencies,
    extract_additional_notes,
    generate,
)
from orchestration_pipelines_lib.dag_generator.airflow_adapters.common_utils.task_utils import (  # noqa: E501
    create_schedule_trigger_task,
)
from tests.conftest import IS_AIRFLOW_2

try:
    # Airflow 3
    from airflow.sdk import DAG  # pyright: ignore[reportMissingImports]
except ImportError:
    # Airflow 2
    from airflow.models import DAG  # pyright: ignore[reportMissingImports]


ADAPTERS_MODULE = "orchestration_pipelines_lib.dag_generator.airflow_adapters"
COMMON_UTILS = f"{ADAPTERS_MODULE}.common_utils"

TARGET_MODULE = f"{COMMON_UTILS}.dag_utils"

MOCK_SCHEDULE_TRIGGER_MODEL = (
    "orchestration_pipelines_lib.internal_models.triggers.ScheduleTriggerModel"
)

MOCK_ACTION_REGISTRY = f"{TARGET_MODULE}.action_handler_registry"
MOCK_BUILD_DAG_KWARGS = f"{TARGET_MODULE}._build_dag_kwargs"
MOCK_CONFIGURE_DAG_SCHEDULE = f"{TARGET_MODULE}._configure_dag_schedule"
MOCK_CREATE_INIT_TASK = f"{TARGET_MODULE}._create_init_task"
MOCK_CREATE_TASKS = f"{TARGET_MODULE}._create_tasks"
MOCK_SET_DEPENDENCIES = f"{TARGET_MODULE}._set_dependencies"


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


@pytest.fixture
def mock_pipeline():
    """Creates a basic mock pipeline model with default configurations."""
    pipeline = MagicMock()

    pipeline.metadata.pipelineId = "test_pipeline"
    pipeline.metadata.description = "Test description"
    pipeline.metadata.owner = "test_owner"
    pipeline.defaults.executionConfigDefault.retries = 3
    pipeline.triggers = []
    pipeline.notifications = None
    pipeline.actions = []

    return pipeline


@pytest.fixture
def mock_notification():
    """Creates a mock notification configuration with empty callbacks."""
    notification = MagicMock()

    notification.onPipelineFailure = None
    notification.onPipelineSuccess = None
    notification.onPipelineComplete = None

    return notification


@pytest.fixture
def mock_trigger():
    """Creates a mock schedule trigger with default settings."""
    from orchestration_pipelines_lib.internal_models.triggers import (
        ScheduleTriggerModel,
    )

    mock_trigger = MagicMock(spec=ScheduleTriggerModel)

    mock_trigger.startTime = "2025-07-21 10:30:45"
    mock_trigger.endTime = "2025-07-22 10:30:45"
    mock_trigger.timezone = "UTC"
    mock_trigger.scheduleInterval = None
    mock_trigger.catchup = False

    return mock_trigger


def test_build_dag_kwargs_with_data_root_returns_kwargs_with_template_searchpath(  # noqa: E501
    pipeline_setup,
):
    """Test that dag_kwargs are correctly populated when a valid data_root is
    provided.
    """
    tags = ["tag1", "tag2"]
    dag_notes = "## Pipeline Notes"
    data_root = "/path/to/data"
    mock_task_factory = MagicMock()
    mock_task_factory._resolve_latest_pipeline_dag_id = MagicMock()
    mock_emails_callback = MagicMock()
    expected_kwargs = {
        "dag_id": "test_pipe",
        "description": "Desc",
        "default_args": {
            "owner": "team",
            "retries": 3,
        },
        "tags": tags,
        "template_searchpath": [data_root],
        "user_defined_macros": {
            "resolve_latest_pipeline_dag_id": mock_task_factory._resolve_latest_pipeline_dag_id,  # noqa: E501
        },
        "doc_md": dag_notes,
        "on_failure_callback": [ANY, ANY],
        "on_success_callback": [ANY, ANY],
    }

    result = _build_dag_kwargs(
        pipeline_setup["pipeline"],
        tags,
        dag_notes,
        data_root,
        pipeline_setup["bundle_id"],
        pipeline_setup["pipeline_id"],
        mock_emails_callback,
        task_factory=mock_task_factory,
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
    mock_task_factory = MagicMock()
    mock_emails_callback = MagicMock()

    result = _build_dag_kwargs(
        pipeline_setup["pipeline"],
        tags,
        dag_notes,
        data_root,
        pipeline_setup["bundle_id"],
        pipeline_setup["pipeline_id"],
        mock_emails_callback,
        task_factory=mock_task_factory,
    )

    assert result.get("template_searchpath") == []


def test_build_dag_kwargs_without_notifications_adds_only_finish_callback(
    pipeline_setup,
):
    """Test that only the finish_callback is added to on_failure_callbacks
    when pipeline.notifications is None.
    """
    mock_pipeline = pipeline_setup["pipeline"]
    mock_pipeline.notifications = None
    mock_task_factory = MagicMock()

    result = _build_dag_kwargs(
        mock_pipeline,
        tags=[],
        dag_notes="",
        data_root="/data",
        bundle_id=pipeline_setup["bundle_id"],
        pipeline_id=pipeline_setup["pipeline_id"],
        emails_callback=MagicMock(),
        task_factory=mock_task_factory,
    )

    assert len(result["on_failure_callback"]) == 1  # type: ignore
    assert result["on_failure_callback"][0] == result["on_success_callback"][0]  # type: ignore


def test_build_dag_kwargs_without_on_pipeline_failure_adds_only_finish_callback(
    pipeline_setup,
):
    """Test that only the finish_callback is added to on_failure_callbacks
    when pipeline.notifications exists but onPipelineFailure is None.
    """
    mock_pipeline = pipeline_setup["pipeline"]
    mock_pipeline.notifications = MagicMock()
    mock_pipeline.notifications.onPipelineFailure = None
    mock_pipeline.notifications.onPipelineSuccess = None
    mock_task_factory = MagicMock()

    result = _build_dag_kwargs(
        mock_pipeline,
        tags=[],
        dag_notes="",
        data_root="/data",
        bundle_id=pipeline_setup["bundle_id"],
        pipeline_id=pipeline_setup["pipeline_id"],
        emails_callback=MagicMock(),
        task_factory=mock_task_factory,
    )

    assert len(result["on_failure_callback"]) == 1  # type: ignore
    assert result["on_failure_callback"][0] == result["on_success_callback"][0]  # type: ignore


def test_build_dag_kwargs_with_emails_configures_emails_callback(
    pipeline_setup,
):
    """Test that the emails callback is correctly set up as a partial function
    with the correct arguments for both failure and success notifications.
    """
    mock_pipeline = pipeline_setup["pipeline"]
    test_emails = ["test1@google.com", "test2@google.com"]
    mock_pipeline.notifications.onPipelineFailure.email = test_emails
    mock_pipeline.notifications.onPipelineSuccess.email = test_emails
    mock_task_factory = MagicMock()
    mock_emails_callback = MagicMock()

    result = _build_dag_kwargs(
        mock_pipeline,
        tags=[],
        dag_notes="",
        data_root="/data",
        bundle_id=pipeline_setup["bundle_id"],
        pipeline_id=pipeline_setup["pipeline_id"],
        emails_callback=mock_emails_callback,
        task_factory=mock_task_factory,
    )

    failure_callbacks = result["on_failure_callback"]
    assert len(failure_callbacks) == 2  # type: ignore
    failure_email_partial = failure_callbacks[1]  # type: ignore
    assert isinstance(failure_email_partial, partial)
    assert failure_email_partial.func == mock_emails_callback
    assert failure_email_partial.args == (test_emails, False)

    success_callbacks = result["on_success_callback"]
    assert len(success_callbacks) == 2  # type: ignore
    success_email_partial = success_callbacks[1]  # type: ignore
    assert isinstance(success_email_partial, partial)
    assert success_email_partial.func == mock_emails_callback
    assert success_email_partial.args == (test_emails, True)


def test_build_dag_kwargs_with_only_on_pipeline_failure_configures_only_failure_email(  # noqa: E501
    pipeline_setup,
):
    """Test that only on_failure_callback receives the email partial when
    onPipelineSuccess is None.
    """
    mock_pipeline = pipeline_setup["pipeline"]
    mock_pipeline.notifications = MagicMock()
    mock_pipeline.notifications.onPipelineFailure.email = ["fail@google.com"]
    mock_pipeline.notifications.onPipelineSuccess = None
    mock_task_factory = MagicMock()
    mock_emails_callback = MagicMock()

    result = _build_dag_kwargs(
        mock_pipeline,
        tags=[],
        dag_notes="",
        data_root="/data",
        bundle_id=pipeline_setup["bundle_id"],
        pipeline_id=pipeline_setup["pipeline_id"],
        emails_callback=mock_emails_callback,
        task_factory=mock_task_factory,
    )

    assert len(result["on_failure_callback"]) == 2  # type: ignore
    assert result["on_failure_callback"][1].args == (["fail@google.com"], False)  # type: ignore
    assert len(result["on_success_callback"]) == 1  # type: ignore


def test_build_dag_kwargs_with_only_on_pipeline_success_configures_only_success_email(  # noqa: E501
    pipeline_setup,
):
    """Test that only on_success_callback receives the email partial when
    onPipelineFailure is None.
    """
    mock_pipeline = pipeline_setup["pipeline"]
    mock_pipeline.notifications = MagicMock()
    mock_pipeline.notifications.onPipelineFailure = None
    mock_pipeline.notifications.onPipelineSuccess.email = ["ok@google.com"]
    mock_task_factory = MagicMock()
    mock_emails_callback = MagicMock()

    result = _build_dag_kwargs(
        mock_pipeline,
        tags=[],
        dag_notes="",
        data_root="/data",
        bundle_id=pipeline_setup["bundle_id"],
        pipeline_id=pipeline_setup["pipeline_id"],
        emails_callback=mock_emails_callback,
        task_factory=mock_task_factory,
    )

    assert len(result["on_failure_callback"]) == 1  # type: ignore
    assert len(result["on_success_callback"]) == 2  # type: ignore
    assert result["on_success_callback"][1].args == (["ok@google.com"], True)  # type: ignore


def test_build_dag_kwargs_with_retry_policy_sets_default_args_and_custom_key(
    pipeline_setup,
):
    """Test that dag_kwargs default_args includes retries, retry_delay, and
    _op_custom_retry_policy.
    """
    from datetime import timedelta

    from orchestration_pipelines_lib.internal_models.actions import (
        FixedDelayStrategyModel,
        RetryPolicyModel,
    )

    policy = RetryPolicyModel(
        maxRetries=5,
        fixedDelay=FixedDelayStrategyModel(retryDelay="2m"),
    )
    pipeline_setup["pipeline"].defaults.retryPolicy = policy
    mock_task_factory = MagicMock()
    mock_emails_callback = MagicMock()

    result = _build_dag_kwargs(
        pipeline_setup["pipeline"],
        tags=[],
        dag_notes="Notes",
        data_root="",
        bundle_id=pipeline_setup["bundle_id"],
        pipeline_id=pipeline_setup["pipeline_id"],
        emails_callback=mock_emails_callback,
        task_factory=mock_task_factory,
    )

    assert result["default_args"]["retries"] == 5
    assert result["default_args"]["retry_delay"] == timedelta(minutes=2)
    assert result["default_args"]["_op_custom_retry_policy"] == policy


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


def test_configure_dag_schedule_with_schedule_trigger_creates_trigger_task(
    schedule_setup,
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
    mock_task_factory = MagicMock()

    with patch(
        MOCK_SCHEDULE_TRIGGER_MODEL,
        new=type(mock_schedule_trigger),
    ):
        _configure_dag_schedule(dag_kwargs, triggers, mock_task_factory)

    mock_task_factory.create_schedule_trigger_task.assert_called_once_with(
        dag_kwargs, mock_schedule_trigger
    )
    assert "schedule" not in dag_kwargs


def test_configure_dag_schedule_with_dataset_trigger_creates_dataset_trigger_task():
    """Test that a dataset trigger task is created when a DatasetTriggerModel
    exists.
    """
    from orchestration_pipelines_lib.internal_models.triggers import (
        DatasetTriggerModel,
    )

    dag_kwargs: DAGKwargs = {}
    dataset_trigger = DatasetTriggerModel(
        uris=["bq://proj.ds.tbl"], condition="all"
    )
    mock_task_factory = MagicMock()

    _configure_dag_schedule(dag_kwargs, [dataset_trigger], mock_task_factory)

    mock_task_factory.create_dataset_trigger_task.assert_called_once_with(
        dag_kwargs, dataset_trigger
    )
    mock_task_factory.create_schedule_trigger_task.assert_not_called()


def test_configure_dag_schedule_without_schedule_trigger_sets_schedule_none(
    schedule_setup,
):
    """Test that 'schedule' is set to None and no task is created when no
    schedule trigger is found.
    """
    dag_kwargs = schedule_setup["dag_kwargs"]
    mock_non_schedule_trigger = schedule_setup["mock_non_schedule_trigger"]
    triggers = [mock_non_schedule_trigger]
    mock_task_factory = MagicMock()

    class MockScheduleTriggerModel:
        pass

    with patch(
        MOCK_SCHEDULE_TRIGGER_MODEL,
        new=MockScheduleTriggerModel,
    ):
        _configure_dag_schedule(dag_kwargs, triggers, mock_task_factory)

    assert dag_kwargs.get("schedule") is None
    mock_task_factory.create_schedule_trigger_task.assert_not_called()


def test_configure_dag_schedule_populates_dag_kwargs_schedule_fields(
    mock_trigger,
):
    """Test that schedule trigger fields populate start_date, end_date,
    schedule, and catchup in DAGKwargs.
    """
    dag_kwargs: DAGKwargs = {}
    mock_trigger.startTime = "2025-07-21T10:30:45"
    mock_trigger.endTime = "2025-07-22T10:30:45"
    mock_trigger.timezone = "UTC"
    mock_trigger.scheduleInterval = "@daily"
    mock_trigger.catchup = True
    mock_task_factory = MagicMock()
    mock_task_factory.create_schedule_trigger_task.side_effect = (
        create_schedule_trigger_task
    )

    _configure_dag_schedule(dag_kwargs, [mock_trigger], mock_task_factory)

    tz = pytz.timezone("UTC")
    assert dag_kwargs["start_date"] == tz.localize(
        datetime(2025, 7, 21, 10, 30, 45)
    )
    assert dag_kwargs["end_date"] == tz.localize(
        datetime(2025, 7, 22, 10, 30, 45)
    )
    assert dag_kwargs["schedule"] == "@daily"
    assert dag_kwargs["catchup"] is True


def test_configure_dag_schedule_without_end_time_sets_end_date_none(
    mock_trigger,
):
    """Test that end_date in DAGKwargs is None when schedule trigger has no
    endTime.
    """
    dag_kwargs: DAGKwargs = {}
    mock_trigger.startTime = "2025-07-21T10:30:45"
    mock_trigger.endTime = None
    mock_trigger.timezone = "UTC"
    mock_trigger.scheduleInterval = "@hourly"
    mock_trigger.catchup = False
    mock_task_factory = MagicMock()
    mock_task_factory.create_schedule_trigger_task.side_effect = (
        create_schedule_trigger_task
    )

    _configure_dag_schedule(dag_kwargs, [mock_trigger], mock_task_factory)

    tz = pytz.timezone("UTC")
    assert dag_kwargs["start_date"] == tz.localize(
        datetime(2025, 7, 21, 10, 30, 45)
    )
    assert dag_kwargs["end_date"] is None
    assert dag_kwargs["schedule"] == "@hourly"
    assert dag_kwargs["catchup"] is False


@pytest.fixture
def init_task_setup():
    """Sets up the mock DAG and documentation notes for the initialization
    task.
    """
    return {"mock_dag": MagicMock(spec=DAG), "dag_notes": "Notes for init"}


def test_create_init_task_with_valid_inputs_creates_python_operator(
    init_task_setup,
):
    """Test that the operator is instantiated with correct parameters and
    linked to the DAG.
    """
    mock_dag = init_task_setup["mock_dag"]
    dag_notes = init_task_setup["dag_notes"]
    bundle_id = "bundle_id"
    pipeline_id = "pipeline_id"
    mock_init_pipeline_context = MagicMock()
    mock_init_task_operator = MagicMock()

    _create_init_task(
        dag=mock_dag,
        dag_notes=dag_notes,
        bundle_id=bundle_id,
        pipeline_id=pipeline_id,
        init_pipeline_context=mock_init_pipeline_context,
        init_task_operator=mock_init_task_operator,
    )

    mock_init_task_operator.assert_called_once_with(
        task_id="init_orchestration_pipeline_context",
        python_callable=mock_init_pipeline_context,
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


@pytest.fixture
def patch_airflow():
    """Patches Airflow DAG depending on Airflow version."""
    if IS_AIRFLOW_2:
        dag_import_path = "airflow.models.DAG"
    else:
        dag_import_path = "airflow.sdk.DAG"

    with patch(dag_import_path) as mock_dag:
        yield mock_dag


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
    generate_setup,
    patch_airflow,
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
    mock_task_factory = MagicMock()
    mock_emails_callback = MagicMock()
    mock_init_pipeline_context = MagicMock()
    mock_init_task_operator = MagicMock()
    mock_handlers = {}
    mock_action_registry.get_action_handlers.return_value = mock_handlers
    mock_kwargs_in = {"dag_id": "test_pipe", "doc_md": dag_notes}
    mock_build_dag_kwargs.return_value = mock_kwargs_in
    mock_tasks_out = {"t1": MagicMock(), "t2": MagicMock()}
    mock_create_tasks.return_value = mock_tasks_out
    MockDAG = patch_airflow

    result_dag = generate(
        mock_pipeline,
        tags,
        dag_notes,
        data_root,
        bundle_id,
        pipeline_id,
        AirflowVersionedDependencies(
            task_factory=mock_task_factory,
            emails_callback=mock_emails_callback,
            init_pipeline_context=mock_init_pipeline_context,
            init_task_operator=mock_init_task_operator,
        ),
    )

    mock_action_registry.get_action_handlers.assert_called_once_with(
        mock_task_factory
    )
    mock_build_dag_kwargs.assert_called_once_with(
        mock_pipeline,
        tags,
        dag_notes,
        data_root,
        bundle_id,
        pipeline_id,
        mock_emails_callback,
        mock_task_factory,
    )
    mock_configure_dag_schedule.assert_called_once_with(
        mock_kwargs_in, mock_pipeline.triggers, mock_task_factory
    )
    MockDAG.assert_called_once_with(**mock_kwargs_in)
    mock_dag_instance = MockDAG.return_value
    mock_create_init_task.assert_called_once_with(
        mock_dag_instance,
        dag_notes,
        bundle_id,
        pipeline_id,
        mock_init_pipeline_context,
        mock_init_task_operator,
    )
    mock_create_tasks.assert_called_once_with(
        mock_dag_instance, mock_handlers, mock_pipeline
    )
    assert mock_set_dependencies.call_count == len(mock_pipeline.actions)
    assert result_dag == mock_dag_instance


def test_generate_with_basic_pipeline_creates_dag_and_init_task(
    mock_pipeline,
    patch_airflow,
    mock_bundle_id,
    mock_pipeline_id,
):
    """Test that generate successfully sets up a basic DAG along
    with its init task.
    """
    mock_dag_class = patch_airflow
    mock_task_factory = MagicMock()
    mock_emails_callback = MagicMock()
    mock_init_pipeline_context = MagicMock()
    mock_init_task_operator = MagicMock()

    dag = generate(
        pipeline=mock_pipeline,
        tags=["tag1", "tag2"],
        dag_notes="My documentation",
        data_root="/test/root",
        bundle_id=mock_bundle_id,
        pipeline_id=mock_pipeline_id,
        versioned_deps=AirflowVersionedDependencies(
            task_factory=mock_task_factory,
            emails_callback=mock_emails_callback,
            init_pipeline_context=mock_init_pipeline_context,
            init_task_operator=mock_init_task_operator,
        ),
    )

    mock_dag_class.assert_called_once_with(
        dag_id="test_pipeline",
        description="Test description",
        default_args={
            "owner": "test_owner",
            "retries": 3,
        },
        tags=["tag1", "tag2"],
        template_searchpath=["/test/root"],
        doc_md="My documentation",
        user_defined_macros={"resolve_latest_pipeline_dag_id": ANY},
        on_failure_callback=[ANY],
        on_success_callback=[ANY],
        schedule=None,
    )
    mock_init_task_operator.assert_called_once_with(
        task_id="init_orchestration_pipeline_context",
        python_callable=mock_init_pipeline_context,
        op_args=["My documentation"],
        dag=mock_dag_class.return_value,
        on_failure_callback=[ANY],
        on_success_callback=[ANY],
    )
    assert dag == mock_dag_class.return_value


def test_generate_with_schedule_and_notifications_configures_dag_callbacks(
    mock_pipeline,
    mock_notification,
    mock_trigger,
    patch_airflow,
    mock_bundle_id,
    mock_pipeline_id,
):
    """Test that schedule triggers and email notifications
    are appropriately configured.
    """
    mock_task_factory = MagicMock()
    mock_emails_callback = MagicMock()
    mock_init_pipeline_context = MagicMock()
    mock_init_task_operator = MagicMock()
    mock_pipeline.triggers = [mock_trigger]
    mock_notification.onPipelineFailure = MagicMock()
    mock_pipeline.notifications = mock_notification
    mock_pipeline.notifications.onPipelineFailure.email = ["alert@example.com"]
    mock_dag_class = patch_airflow

    generate(
        mock_pipeline,
        ["tag1"],
        "Notes",
        "",
        mock_bundle_id,
        mock_pipeline_id,
        AirflowVersionedDependencies(
            task_factory=mock_task_factory,
            emails_callback=mock_emails_callback,
            init_pipeline_context=mock_init_pipeline_context,
            init_task_operator=mock_init_task_operator,
        ),
    )

    mock_task_factory.create_schedule_trigger_task.assert_called_once_with(
        ANY, mock_trigger
    )
    call_kwargs = mock_dag_class.call_args.kwargs
    assert "on_failure_callback" in call_kwargs
    failure_callbacks = call_kwargs["on_failure_callback"]
    assert len(failure_callbacks) == 2
    email_callback = failure_callbacks[1]
    assert isinstance(email_callback, partial)
    assert email_callback.args == (["alert@example.com"], False)


def test_generate_with_on_pipeline_success_configures_dag_success_callback(
    mock_pipeline,
    mock_notification,
    patch_airflow,
    mock_bundle_id,
    mock_pipeline_id,
):
    """Test that onPipelineSuccess email notifications are configured on DAG
    on_success_callback during generate.
    """
    mock_task_factory = MagicMock()
    mock_emails_callback = MagicMock()
    mock_init_pipeline_context = MagicMock()
    mock_init_task_operator = MagicMock()
    mock_notification.onPipelineSuccess = MagicMock()
    mock_notification.onPipelineSuccess.email = ["ok@example.com"]
    mock_pipeline.notifications = mock_notification
    mock_dag_class = patch_airflow

    generate(
        mock_pipeline,
        ["tag1"],
        "Notes",
        "",
        mock_bundle_id,
        mock_pipeline_id,
        AirflowVersionedDependencies(
            task_factory=mock_task_factory,
            emails_callback=mock_emails_callback,
            init_pipeline_context=mock_init_pipeline_context,
            init_task_operator=mock_init_task_operator,
        ),
    )

    call_kwargs = mock_dag_class.call_args.kwargs
    assert len(call_kwargs["on_failure_callback"]) == 1
    success_callbacks = call_kwargs["on_success_callback"]
    assert len(success_callbacks) == 2
    email_callback = success_callbacks[1]
    assert isinstance(email_callback, partial)
    assert email_callback.args == (["ok@example.com"], True)


@patch(MOCK_ACTION_REGISTRY)
def test_generate_with_actions_creates_tasks_and_sets_dependencies(
    mock_registry_direct,
    mock_pipeline,
    patch_airflow,
    mock_bundle_id,
    mock_pipeline_id,
):
    """Test that tasks are generated from actions and their dependencies
    are correctly resolved.
    """
    mock_task_factory = MagicMock()
    mock_emails_callback = MagicMock()
    mock_init_pipeline_context = MagicMock()
    mock_init_task_operator = MagicMock()

    class MockAction:
        def __init__(self, name, dependsOn):
            self.name = name
            self.dependsOn = dependsOn

    action_a = MockAction("task_a", [])
    action_b = MockAction("task_b", ["task_a"])
    mock_pipeline.actions = [action_a, action_b]
    mock_task_a_obj = MagicMock()
    mock_task_b_obj = MagicMock()

    def fake_handler(*args, **kwargs):
        action = args[0] if args else kwargs.get("action")
        if action.name == "task_a":  # type: ignore
            return mock_task_a_obj
        if action.name == "task_b":  # type: ignore
            return mock_task_b_obj

    mock_registry_direct.get_action_handlers.return_value = {
        MockAction: fake_handler
    }

    generate(
        mock_pipeline,
        [],
        "Notes",
        "/root",
        mock_bundle_id,
        mock_pipeline_id,
        AirflowVersionedDependencies(
            task_factory=mock_task_factory,
            emails_callback=mock_emails_callback,
            init_pipeline_context=mock_init_pipeline_context,
            init_task_operator=mock_init_task_operator,
        ),
    )

    mock_task_b_obj.set_upstream.assert_called_once_with(mock_task_a_obj)
    mock_task_a_obj.set_upstream.assert_not_called()


@pytest.fixture
def mock_bundle_id():
    """Returns a mock bundle id for testing."""
    return "test_bundle_id"


@pytest.fixture
def mock_pipeline_id():
    """Returns a mock pipeline id for testing."""
    return "test_pipeline_id"


@patch(MOCK_ACTION_REGISTRY)
def test_generate_with_missing_dependency_raises_value_error(
    mock_registry_direct,
    mock_pipeline,
    patch_airflow,
    mock_bundle_id,
    mock_pipeline_id,
):
    """Test that generate throws a ValueError
    if an action refers to an unspecified dependency.
    """

    class MockAction:
        def __init__(self, name, dependsOn):
            self.name = name
            self.dependsOn = dependsOn

    action_b = MockAction("task_b", ["task_a"])
    mock_pipeline.actions = [action_b]

    def fake_handler(*args, **kwargs):
        return MagicMock(name="MockedTask_B")

    mock_registry_direct.get_action_handlers.return_value = {
        MockAction: fake_handler
    }
    task_factory = MagicMock()
    emails_callback = MagicMock()
    init_pipeline_context = MagicMock()
    init_task_operator = MagicMock()

    with pytest.raises(ValueError) as exc_info:
        generate(
            mock_pipeline,
            [],
            "Notes",
            "/root",
            mock_bundle_id,
            mock_pipeline_id,
            AirflowVersionedDependencies(
                task_factory=task_factory,
                emails_callback=emails_callback,
                init_pipeline_context=init_pipeline_context,
                init_task_operator=init_task_operator,
            ),
        )

    assert (
        "Task task_a being upstream dependency for task_b does not exist."
        in str(exc_info.value)
    )


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

    result = extract_additional_notes(input_content)

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
    result = extract_additional_notes(empty_input)

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
    result = extract_additional_notes(non_dict_json)

    assert result == ""


def test_extract_additional_notes_with_invalid_json_raises_json_decode_error():
    """Test that passing syntactically invalid JSON raises a JSONDecodeError."""
    with pytest.raises(json.JSONDecodeError):
        extract_additional_notes("invalid json")


def test_extract_additional_notes_without_allowed_keys_returns_empty_string():
    """Test that a dictionary with zero matching metadata keys returns an empty
    string.
    """
    input_content = json.dumps({"key_a": 1, "key_b": 2})

    result = extract_additional_notes(input_content)

    assert result == ""


if __name__ == "__main__":
    unittest.main()
