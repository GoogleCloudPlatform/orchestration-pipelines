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
"""Tests for the Airflow 3 core module."""

from unittest.mock import MagicMock, patch

import pytest
from airflow_client.client import ApiException
from airflow_client.client.exceptions import ServiceException
from tenacity import RetryError

from orchestration_pipelines_lib.dag_generator.airflow_adapters.airflow_3.core import (  # noqa: E501
    _calculate_updates,
    _get_task_instances,
    _set_additional_notes,
    _update_metadata,
    _update_task_instances,
    get_actively_running_versions,
    get_deps,
    get_previous_default_versions,
    init_orchestration_pipeline_context,
)

ADAPTERS_MODULE = "orchestration_pipelines_lib.dag_generator.airflow_adapters"
AIRFLOW = f"{ADAPTERS_MODULE}.airflow_3"
AIRFLOW_CLIENT = f"{AIRFLOW}.airflow_client_utils.get_airflow_api_client"
COMMON_UTILS = f"{ADAPTERS_MODULE}.common_utils"
TARGET_MODULE = f"{AIRFLOW}.core"


@pytest.fixture
def mock_pipeline():
    """Fixture providing a mocked pipeline model."""
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
def patch_airflow():
    """Fixture to patch generic Airflow dependencies."""
    with (
        patch("airflow.sdk.DAG") as mock_dag,
        patch(
            "airflow.providers.standard.operators.python.PythonOperator"
        ) as mock_python_operator,
    ):
        yield mock_dag, mock_python_operator


@pytest.fixture
def patch_internals():
    """Fixture to patch internal DAG generator dependencies."""
    with (
        patch(f"{COMMON_UTILS}.action_handler_registry") as mock_registry,
        patch(f"{TARGET_MODULE}.task_factory") as mock_task_factory,
        patch(f"{TARGET_MODULE}.email_utils") as mock_email_utils,
        patch(
            f"{TARGET_MODULE}.init_orchestration_pipeline_context"
        ) as mock_init_context,
    ):
        yield (
            mock_registry,
            mock_task_factory,
            mock_email_utils,
            mock_init_context,
        )


@pytest.fixture
def mock_dag():
    """Fixture providing a mocked Airflow DAG with predefined tasks."""
    dag = MagicMock()
    task_1 = MagicMock(task_id="task_1", doc_md="Note 1")
    task_2 = MagicMock(task_id="task_2", doc_md="Note 2")
    dag.tasks = [task_1, task_2]
    return dag


@pytest.fixture
def mock_dag_run():
    """Fixture providing a mocked Airflow DagRun."""
    dag_run = MagicMock()
    dag_run.dag_id = "test_dag_id"
    dag_run.run_id = "test_run_id_123"
    return dag_run


@pytest.fixture
def mock_dag_run_api():
    """Fixture providing a mocked DagRunApi."""
    return MagicMock()


@pytest.fixture
def mock_ti_factory():
    """Fixture providing a factory for mocked TaskInstances."""

    def _create_mock_ti(
        task_id: str, map_index: int = -1, note: str | None = None
    ):
        ti = MagicMock()
        ti.task_id = task_id
        ti.map_index = map_index
        ti.note = note
        return ti

    return _create_mock_ti


@pytest.fixture
def mock_task_instance_api():
    """Fixture providing a mocked TaskInstanceApi."""
    return MagicMock()


@pytest.fixture
def patch_helpers():
    """Fixture to patch helper methods in the target module."""
    with (
        patch(f"{TARGET_MODULE}._set_additional_notes") as mock_set_notes,
        patch(f"{TARGET_MODULE}._get_task_instances") as mock_get_tis,
        patch(f"{TARGET_MODULE}._calculate_updates") as mock_calc_updates,
        patch(f"{TARGET_MODULE}._update_task_instances") as mock_update_tis,
    ):
        yield mock_set_notes, mock_get_tis, mock_calc_updates, mock_update_tis


@pytest.fixture
def patch_init_dependencies():
    """Fixture setting up all mocks needed for
    init_orchestration_pipeline_context.
    """
    with (
        patch(
            f"{TARGET_MODULE}.dag_utils.extract_additional_notes"
        ) as mock_extract,
        patch(
            f"{TARGET_MODULE}.airflow_client_utils.get_airflow_api_client"
        ) as mock_get_client,
        patch("airflow_client.client.DagRunApi") as mock_dag_run_api_cls,
        patch("airflow_client.client.TaskInstanceApi") as mock_ti_api_cls,
        patch(f"{TARGET_MODULE}._update_metadata") as mock_update_metadata,
    ):
        mock_client_instance = MagicMock()
        mock_get_client.return_value = mock_client_instance

        mock_dag_run_api_instance = MagicMock()
        mock_dag_run_api_cls.return_value = mock_dag_run_api_instance

        mock_ti_api_instance = MagicMock()
        mock_ti_api_cls.return_value = mock_ti_api_instance

        yield {
            "extract": mock_extract,
            "get_client": mock_get_client,
            "dag_run_api_cls": mock_dag_run_api_cls,
            "dag_run_api_instance": mock_dag_run_api_instance,
            "ti_api_cls": mock_ti_api_cls,
            "ti_api_instance": mock_ti_api_instance,
            "update_metadata": mock_update_metadata,
            "client_instance": mock_client_instance,
        }


def test_init_orchestration_pipeline_context_success(patch_init_dependencies):
    """Tests that init_orchestration_pipeline_context succeeds and
    updates metadata.
    """
    mock_dag = MagicMock()
    mock_dag_run = MagicMock()
    deps = patch_init_dependencies
    deps["extract"].return_value = "Extracted note"

    init_orchestration_pipeline_context(
        note_content='{"some": "json"}', dag=mock_dag, dag_run=mock_dag_run
    )

    deps["extract"].assert_called_once_with('{"some": "json"}')
    deps["get_client"].assert_called_once()
    deps["dag_run_api_cls"].assert_called_once_with(deps["client_instance"])
    deps["ti_api_cls"].assert_called_once_with(deps["client_instance"])
    deps["update_metadata"].assert_called_once_with(
        mock_dag_run,
        mock_dag,
        "Extracted note",
        deps["dag_run_api_instance"],
        deps["ti_api_instance"],
    )


def test_init_orchestration_pipeline_context_raises_api_exception(
    patch_init_dependencies, capsys
):
    """Tests that init_orchestration_pipeline_context logs and raises
    ApiException on failure.
    """
    mock_dag = MagicMock()
    mock_dag_run = MagicMock()
    deps = patch_init_dependencies
    deps["extract"].return_value = "Extracted note"
    deps["update_metadata"].side_effect = ApiException(
        status=500, reason="Test API Error"
    )

    with pytest.raises(ApiException):
        init_orchestration_pipeline_context(
            note_content="test_content", dag=mock_dag, dag_run=mock_dag_run
        )

    captured = capsys.readouterr()
    assert (
        "Failed when calling Airflow Python Client API during metadata application"  # noqa: E501
        in captured.out
    )
    assert "Test API Error" in captured.out


def test_update_metadata_happy_path(
    mock_dag_run,
    mock_dag,
    mock_dag_run_api,
    mock_task_instance_api,
    patch_helpers,
):
    """Tests that metadata update follows the happy path correctly."""
    mock_set_notes, mock_get_tis, mock_calc_updates, mock_update_tis = (
        patch_helpers
    )
    additional_notes = "Additional note"
    mock_task_instances = [MagicMock(), MagicMock()]
    mock_get_tis.return_value = mock_task_instances
    expected_entities = [{"task_id": "task_1", "note": "New note"}]
    mock_calc_updates.return_value = expected_entities

    _update_metadata(
        mock_dag_run,
        mock_dag,
        additional_notes,
        mock_dag_run_api,
        mock_task_instance_api,
    )

    mock_set_notes.assert_called_once_with(
        mock_dag_run, additional_notes, mock_dag_run_api
    )
    mock_get_tis.assert_called_once_with(mock_dag_run, mock_task_instance_api)
    mock_calc_updates.assert_called_once_with(
        mock_task_instances, {"task_1": "Note 1", "task_2": "Note 2"}
    )
    mock_update_tis.assert_called_once_with(
        expected_entities, mock_dag_run, mock_task_instance_api
    )


def test_update_metadata_skips_update_if_no_entities(
    mock_dag_run,
    mock_dag,
    mock_dag_run_api,
    mock_task_instance_api,
    patch_helpers,
):
    """Tests that update is skipped if no entities are calculated."""
    (
        _mock_set_notes,
        _mock_get_tis,
        mock_calc_updates,
        mock_update_tis,
    ) = patch_helpers
    mock_calc_updates.return_value = []

    _update_metadata(
        mock_dag_run,
        mock_dag,
        "Note",
        mock_dag_run_api,
        mock_task_instance_api,
    )

    mock_calc_updates.assert_called_once()
    mock_update_tis.assert_not_called()


@patch("time.sleep", return_value=None)
def test_update_metadata_retries_on_service_exception(
    mock_sleep,
    mock_dag_run,
    mock_dag,
    mock_dag_run_api,
    mock_task_instance_api,
    patch_helpers,
):
    """Tests that metadata update retries on ServiceException."""
    _mock_set_notes, mock_get_tis, mock_calc_updates, mock_update_tis = (
        patch_helpers
    )
    mock_get_tis.side_effect = [
        ServiceException(status=500, reason="API Error 1"),
        ServiceException(status=503, reason="API Error 2"),
        [MagicMock()],
    ]
    mock_calc_updates.return_value = [{"task_id": "task_1", "note": "OK"}]

    _update_metadata(
        mock_dag_run,
        mock_dag,
        "Retry test",
        mock_dag_run_api,
        mock_task_instance_api,
    )

    assert mock_get_tis.call_count == 3
    mock_update_tis.assert_called_once()


@patch("time.sleep", return_value=None)
def test_update_metadata_fails_after_max_retries(
    mock_sleep,
    mock_dag_run,
    mock_dag,
    mock_dag_run_api,
    mock_task_instance_api,
    patch_helpers,
):
    """Tests that metadata update fails after reaching maximum retries."""
    _mock_set_notes, mock_get_tis, _mock_calc_updates, mock_update_tis = (
        patch_helpers
    )
    mock_get_tis.side_effect = ServiceException(
        status=500, reason="Internal Server Error"
    )

    with pytest.raises(RetryError):
        _update_metadata(
            mock_dag_run,
            mock_dag,
            "Fail test",
            mock_dag_run_api,
            mock_task_instance_api,
        )

    assert mock_get_tis.call_count == 3
    mock_update_tis.assert_not_called()


def test_set_additional_notes_updates_run_note(mock_dag_run, mock_dag_run_api):
    """Tests that _set_additional_notes patches the DAG run with notes."""
    additional_notes = "Important operational note"

    _set_additional_notes(mock_dag_run, additional_notes, mock_dag_run_api)

    mock_dag_run_api.patch_dag_run.assert_called_once_with(
        dag_id="test_dag_id",
        dag_run_id="test_run_id_123",
        dag_run_patch_body={"note": additional_notes},
        update_mask=["note"],
    )


def test_set_additional_notes_handles_empty_notes(
    mock_dag_run, mock_dag_run_api
):
    """Tests that _set_additional_notes handles empty notes correctly."""
    empty_notes = ""

    _set_additional_notes(mock_dag_run, empty_notes, mock_dag_run_api)

    mock_dag_run_api.patch_dag_run.assert_called_once_with(
        dag_id="test_dag_id",
        dag_run_id="test_run_id_123",
        dag_run_patch_body={"note": ""},
        update_mask=["note"],
    )


def test_get_task_instances_returns_list(
    mock_dag_run, mock_task_instance_api
):
    """Tests that _get_task_instances returns the list of task instances."""
    expected_instances_list = [
        MagicMock(task_id="task_1"),
        MagicMock(task_id="task_2"),
    ]
    mock_api_response = MagicMock()
    mock_api_response.task_instances = expected_instances_list
    mock_task_instance_api.get_task_instances.return_value = mock_api_response

    result = _get_task_instances(mock_dag_run, mock_task_instance_api)

    mock_task_instance_api.get_task_instances.assert_called_once_with(
        dag_id="test_dag_id", dag_run_id="test_run_id_123"
    )
    assert result == expected_instances_list


@patch("airflow_client.client.BulkBodyBulkTaskInstanceBody")
def test_update_task_instances_sends_correct_payload(
    mock_bulk_body_class, mock_dag_run, mock_task_instance_api
):
    """Tests that _update_task_instances sends the correct payload
    to the API.
    """
    entities = [{"task_id": "task_1", "note": "New note"}]
    mock_bulk_body_instance = MagicMock()
    mock_bulk_body_class.from_dict.return_value = mock_bulk_body_instance
    expected_payload = {
        "actions": [
            {
                "action": "update",
                "action_on_non_existence": "skip",
                "entities": entities,
            }
        ]
    }

    _update_task_instances(entities, mock_dag_run, mock_task_instance_api)

    mock_bulk_body_class.from_dict.assert_called_once_with(expected_payload)
    mock_task_instance_api.bulk_task_instances.assert_called_once_with(
        dag_id="test_dag_id",
        dag_run_id="test_run_id_123",
        bulk_body_bulk_task_instance_body=mock_bulk_body_instance,
    )


@patch("airflow_client.client.BulkBodyBulkTaskInstanceBody")
def test_update_task_instances_with_empty_entities(
    mock_bulk_body_class, mock_dag_run, mock_task_instance_api
):
    """Tests that _update_task_instances handles empty entities list
    correctly.
    """
    entities = []
    mock_bulk_body_instance = MagicMock()
    mock_bulk_body_class.from_dict.return_value = mock_bulk_body_instance
    expected_payload = {
        "actions": [
            {
                "action": "update",
                "action_on_non_existence": "skip",
                "entities": [],
            }
        ]
    }

    _update_task_instances(entities, mock_dag_run, mock_task_instance_api)

    mock_bulk_body_class.from_dict.assert_called_once_with(expected_payload)
    mock_task_instance_api.bulk_task_instances.assert_called_once()


def test_calculate_updates_returns_empty_when_no_doc_md(mock_ti_factory):
    """Tests that _calculate_updates returns empty list when doc_md_map
    is empty.
    """
    task_instances = [mock_ti_factory("task_1", note="Old note")]
    doc_md_map = {}

    result = _calculate_updates(task_instances, doc_md_map)

    assert result == []


def test_calculate_updates_returns_empty_when_new_content_is_empty(
    mock_ti_factory,
):
    """Tests that _calculate_updates returns empty list when new content
    is empty.
    """
    task_instances = [mock_ti_factory("task_1", note="Old note")]
    doc_md_map = {"task_1": ""}

    result = _calculate_updates(task_instances, doc_md_map)

    assert result == []


def test_calculate_updates_returns_empty_when_note_is_identical(
    mock_ti_factory,
):
    """Tests that _calculate_updates returns empty list when notes
    are identical.
    """
    task_instances = [mock_ti_factory("task_1", note="Perfectly fine note")]
    doc_md_map = {"task_1": "Perfectly fine note"}

    result = _calculate_updates(task_instances, doc_md_map)

    assert result == []


def test_calculate_updates_returns_update_when_note_is_different(
    mock_ti_factory,
):
    """Tests that _calculate_updates returns an update when the note differs."""
    task_instances = [mock_ti_factory("task_1", note="Old, outdated note")]
    doc_md_map = {"task_1": "Fresh new note"}

    result = _calculate_updates(task_instances, doc_md_map)

    assert result == [{"task_id": "task_1", "note": "Fresh new note"}]


def test_calculate_updates_returns_update_when_note_was_none(mock_ti_factory):
    """Tests that _calculate_updates returns an update when previous note
    was None.
    """
    task_instances = [mock_ti_factory("task_1", note=None)]
    doc_md_map = {"task_1": "First time note"}

    result = _calculate_updates(task_instances, doc_md_map)

    assert result == [{"task_id": "task_1", "note": "First time note"}]


def test_calculate_updates_handles_multiple_instances_mixed_scenarios(
    mock_ti_factory,
):
    """Tests that _calculate_updates properly handles various concurrent update
    scenarios.
    """
    task_instances = [
        mock_ti_factory("task_update", note="Old note"),
        mock_ti_factory("task_identical", note="Same note"),
        mock_ti_factory("task_empty_doc", note="Keep me"),
        mock_ti_factory("task_new", note=None),
    ]
    doc_md_map = {
        "task_update": "New note",
        "task_identical": "Same note",
        "task_empty_doc": "",
        "task_new": "Brand new",
    }

    result = _calculate_updates(task_instances, doc_md_map)

    expected = [
        {"task_id": "task_update", "note": "New note"},
        {"task_id": "task_new", "note": "Brand new"},
    ]
    assert result == expected


def test_calculate_updates_handles_mapped_tasks(mock_ti_factory):
    """Tests that _calculate_updates processes mapped tasks accurately."""
    task_instances = [
        mock_ti_factory("mapped_task", map_index=0, note="Old note 0"),
        mock_ti_factory("mapped_task", map_index=1, note="Target note"),
        mock_ti_factory("mapped_task", map_index=2, note=None),
    ]
    doc_md_map = {"mapped_task": "Target note"}

    result = _calculate_updates(task_instances, doc_md_map)

    expected = [
        {"task_id": "mapped_task", "note": "Target note"},
        {"task_id": "mapped_task", "note": "Target note"},
    ]
    assert result == expected


def test_calculate_updates_with_task_missing_from_doc_md_map_skips_task(
    mock_ti_factory,
):
    """Tests that _calculate_updates safely skips task instances whose task_id
    is not present in doc_md_map.
    """
    task_instances = [mock_ti_factory("orphan_task", note=None)]
    doc_md_map = {"other_task": "Some documentation"}

    result = _calculate_updates(task_instances, doc_md_map)

    assert result == []


@pytest.fixture
def mock_bundle_id():
    """Fixture providing a mocked bundle ID."""
    return "test_bundle_id"


@pytest.fixture
def mock_pipeline_id():
    """Fixture providing a mocked pipeline ID."""
    return "test_pipeline_id"


def test_get_deps_returns_expected_dependencies():
    """Tests that get_deps returns the expected Airflow 3 dependencies."""
    from airflow.providers.standard.operators.python import (  # type: ignore
        PythonOperator,
    )

    from orchestration_pipelines_lib.dag_generator.airflow_adapters.airflow_3.core import (  # noqa: E501
        email_utils,
        task_factory,
    )
    from orchestration_pipelines_lib.dag_generator.airflow_adapters.common_utils import (  # noqa: E501
        dag_utils,
    )

    deps = get_deps()

    assert deps == dag_utils.AirflowVersionedDependencies(
        task_factory=task_factory,
        emails_callback=email_utils.send_notification_email,
        init_pipeline_context=init_orchestration_pipeline_context,
        init_task_operator=PythonOperator,
    )


@pytest.fixture
def mock_dag_run_client():
    """Fixture providing a mocked DAG Run Client API instance."""
    with (
        patch(AIRFLOW_CLIENT) as _mock_get_client,
        patch("airflow_client.client.DagRunApi") as mock_dag_run_api_class,
    ):
        mock_api_instance = MagicMock()
        mock_dag_run_api_class.return_value = mock_api_instance
        yield mock_api_instance


def test_get_actively_running_versions_success(mock_dag_run_client):
    """Tests that get_actively_running_versions returns a distinct list
    of running pipeline versions.
    """
    mock_run_1 = MagicMock()
    mock_run_1.dag_id = "my_bundle__v__1.0.0__my_pipeline"
    mock_run_2 = MagicMock()
    mock_run_2.dag_id = "my_bundle__v__1.0.0__my_pipeline"
    mock_run_3 = MagicMock()
    mock_run_3.dag_id = "my_bundle__v__2.5.1__my_pipeline"
    mock_run_4 = MagicMock()
    mock_run_4.dag_id = "other_bundle__v__1.0.0__my_pipeline"
    mock_run_5 = MagicMock()
    mock_run_5.dag_id = "my_bundle__v__1.0.0__other_pipeline"
    mock_response = MagicMock()
    mock_response.dag_runs = [
        mock_run_1,
        mock_run_2,
        mock_run_3,
        mock_run_4,
        mock_run_5,
    ]
    mock_dag_run_client.get_dag_runs.return_value = mock_response

    versions = get_actively_running_versions("my_pipeline", "my_bundle")

    assert set(versions) == {"1.0.0", "2.5.1"}
    mock_dag_run_client.get_dag_runs.assert_called_once_with(
        dag_id="~",
        state=["running", "queued"],
    )


def test_get_actively_running_versions_no_runs_returned(mock_dag_run_client):
    """Tests that get_actively_running_versions returns empty when there are
    no runs.
    """
    mock_response = MagicMock()
    mock_response.dag_runs = []
    mock_dag_run_client.get_dag_runs.return_value = mock_response

    versions = get_actively_running_versions("my_pipeline", "my_bundle")

    assert versions == []


def test_get_actively_running_versions_with_none_dag_runs_returns_empty(
    mock_dag_run_client,
):
    """Tests that get_actively_running_versions returns empty when API returns
    None for dag_runs.
    """
    mock_response = MagicMock()
    mock_response.dag_runs = None
    mock_dag_run_client.get_dag_runs.return_value = mock_response

    versions = get_actively_running_versions("my_pipeline", "my_bundle")

    assert versions == []


def test_get_actively_running_versions_no_matching_runs(mock_dag_run_client):
    """Tests that get_actively_running_versions returns empty when runs
    do not match bundle or pipeline.
    """
    mock_run_1 = MagicMock()
    mock_run_1.dag_id = "some_random_dag_id"
    mock_run_2 = MagicMock()
    mock_run_2.dag_id = "my_bundle__v__1.0.0__wrong_pipeline"
    mock_response = MagicMock()
    mock_response.dag_runs = [mock_run_1, mock_run_2]
    mock_dag_run_client.get_dag_runs.return_value = mock_response

    versions = get_actively_running_versions("my_pipeline", "my_bundle")

    assert versions == []


def test_get_actively_running_versions_api_exception(
    mock_dag_run_client, capsys
):
    """Tests that get_actively_running_versions handles API exceptions
    gracefully.
    """
    mock_dag_run_client.get_dag_runs.side_effect = ApiException(
        "Internal Server Error"
    )

    versions = get_actively_running_versions("my_pipeline", "my_bundle")

    assert versions == []
    captured = capsys.readouterr()
    assert "Exception when calling DagRunApi->get_dag_runs" in captured.out


@pytest.fixture
def mock_airflow_client():
    """Fixture providing a mocked DAG API instance."""
    with (
        patch(AIRFLOW_CLIENT) as _mock_get_client,
        patch("airflow_client.client.DAGApi") as mock_dag_api_class,
    ):
        mock_api_instance = MagicMock()
        mock_dag_api_class.return_value = mock_api_instance
        yield mock_api_instance


def test_get_previous_default_versions_success(mock_airflow_client):
    """Tests that get_previous_default_versions parses tags and returns distinct
    default versions.
    """
    mock_tag_1 = MagicMock()
    mock_tag_1.name = "op:version:1.0.0"
    mock_tag_2 = MagicMock()
    mock_tag_2.name = "op:other_value"
    mock_dag_1 = MagicMock()
    mock_dag_1.tags = [mock_tag_1, mock_tag_2]

    mock_tag_3 = MagicMock()
    mock_tag_3.name = "op:version:2.0.0"
    mock_dag_2 = MagicMock()
    mock_dag_2.tags = [mock_tag_3]

    mock_response = MagicMock()
    mock_response.dags = [mock_dag_1, mock_dag_2]
    mock_airflow_client.get_dags.return_value = mock_response

    versions = get_previous_default_versions("my_pipeline", "my_bundle")

    assert set(versions) == {"1.0.0", "2.0.0"}
    mock_airflow_client.get_dags.assert_called_once_with(
        tags=[
            "op:is_current",
            "op:bundle:my_bundle",
            "op:pipeline:my_pipeline",
        ],
        tags_match_mode="all",
    )


def test_get_previous_default_versions_no_dags_found(mock_airflow_client):
    """Tests that get_previous_default_versions returns empty when no DAGs
    are found.
    """
    mock_response = MagicMock()
    mock_response.dags = []
    mock_airflow_client.get_dags.return_value = mock_response

    versions = get_previous_default_versions("my_pipeline", "my_bundle")

    assert versions == []


def test_get_previous_default_versions_with_tag_missing_name_attribute_skips_tag(  # noqa: E501
    mock_airflow_client,
):
    """Tests that get_previous_default_versions safely skips tag objects that
    lack a 'name' attribute.
    """
    tag_without_name = object()
    valid_tag = MagicMock()
    valid_tag.name = "op:version:3.1.0"
    mock_dag = MagicMock()
    mock_dag.tags = [tag_without_name, valid_tag]
    mock_response = MagicMock()
    mock_response.dags = [mock_dag]
    mock_airflow_client.get_dags.return_value = mock_response

    versions = get_previous_default_versions("my_pipeline", "my_bundle")

    assert versions == ["3.1.0"]


def test_get_previous_default_versions_no_version_tags(mock_airflow_client):
    """Tests that get_previous_default_versions returns empty when DAGs lack
    version tags.
    """
    mock_tag = MagicMock()
    mock_tag.name = "op:some_other_tag"
    mock_tag_empty = MagicMock()
    mock_tag_empty.name = "op:version:"
    mock_dag = MagicMock()
    mock_dag.tags = [mock_tag, mock_tag_empty]
    mock_dag_no_tags = MagicMock()
    mock_dag_no_tags.tags = None
    mock_response = MagicMock()
    mock_response.dags = [mock_dag, mock_dag_no_tags]
    mock_airflow_client.get_dags.return_value = mock_response

    versions = get_previous_default_versions("my_pipeline", "my_bundle")

    assert versions == []


def test_get_previous_default_versions_api_exception(
    mock_airflow_client, capsys
):
    """Tests that get_previous_default_versions handles API exceptions
    gracefully.
    """
    mock_airflow_client.get_dags.side_effect = ApiException(
        "API connection failed"
    )

    versions = get_previous_default_versions("my_pipeline", "my_bundle")

    assert versions == []
    captured = capsys.readouterr()
    assert "Exception when calling DAGApi->get_dags" in captured.out
