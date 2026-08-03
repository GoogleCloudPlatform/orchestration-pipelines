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
"""Module to validate and build pipeline from YAML in Airflow 2."""

import json
from functools import partial
from typing import TYPE_CHECKING, Any, TypedDict

from orchestration_pipelines_lib.dag_generator.airflow_adapters.common_utils import (  # noqa: E501
    action_handler_registry,
)
from orchestration_pipelines_lib.dag_generator.airflow_adapters.common_utils.utils import (  # noqa: E501
    init_context_callback,
    pipeline_run_callback,
)

# Airflow and SQLAlchemy imports moved inside functions to reduce import tax
from . import task_factory
from .email_utils import send_failure_notification_email

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable

    from airflow.models import DAG, DagRun, TaskInstance
    from airflow.models.dag import DagStateChangeCallback, ScheduleArg
    from airflow.models.taskinstance import TaskInstanceNote
    from airflow.utils.context import Context
    from sqlalchemy.orm import Session
    from sqlalchemy.sql.selectable import Subquery

    from orchestration_pipelines_lib.internal_models.pipeline import (
        AnyAction,
        AnyScheduleTrigger,
        PipelineModel,
    )


class DAGKwargs(TypedDict, total=False):
    """A not comprehensive list of keys for Airflow DAG constructor."""

    dag_id: str
    description: str | None
    default_args: dict[str, "Any"]
    tags: list[str] | None
    template_searchpath: "str | Iterable[str] | None"
    schedule: "ScheduleArg"
    doc_md: str | None
    on_failure_callback: (
        "DagStateChangeCallback | list[DagStateChangeCallback] | None"
    )
    on_success_callback: (
        "DagStateChangeCallback | list[DagStateChangeCallback] | None"
    )


def _get_dag_run(context: "Context") -> "DagRun":
    dag_run = context.get("dag_run")
    if not dag_run:
        raise ValueError(
            "Missing 'dag_run' in the Airflow execution context. "
            "This function must be executed within an active DAG run."
        )

    return dag_run


def _get_dag(context: "Context") -> "DAG":
    dag = context.get("dag")
    if not dag:
        raise ValueError(
            "Missing 'dag' in the Airflow execution context. "
            "The active DAG object is required to initialize pipeline metadata."
        )

    return dag


def init_orchestration_pipeline_context(note_content: str, **context):
    """Initializes the orchestration pipeline context for a DAG run.

    Extracts specific metadata from the provided notes content and applies
    it to the DAG Run and its Task Instances via the Airflow database.

    Args:
        note_content: JSON string containing the DAG documentation.
        **context: The Airflow task execution context.
    """
    from airflow.utils.session import create_session
    from sqlalchemy.exc import IntegrityError

    dag_run = _get_dag_run(context)  # pyright: ignore[reportArgumentType]
    dag = _get_dag(context)  # pyright: ignore[reportArgumentType]

    additional_notes = _extract_additional_notes(note_content)
    with create_session() as session:
        try:
            _upsert_dag_run_note(session, additional_notes, dag_run)
            _upsert_task_instance_notes(session, dag, dag_run)

            session.commit()
        except IntegrityError:
            # If a parallel task committed a note first, roll back
            # and move on
            session.rollback()
        except Exception:
            session.rollback()
            raise


def _extract_additional_notes(note_content: str | None) -> str:
    # Filter note_content to keep only specific fields
    if not note_content:
        return ""

    notes_data = json.loads(note_content)
    if not isinstance(notes_data, dict):
        return ""

    allowed_keys = [
        "op_bundle",
        "op_version",
        "op_pipeline",
        "op_owner",
        "op_origination",
        "op_deployment_details",
        "op_repository",
        "op_branch",
        "op_commit_sha",
        "op_is_current",
    ]

    notes_dict = {k: v for k, v in notes_data.items() if k in allowed_keys}
    if not notes_dict:
        return ""

    return json.dumps(notes_dict, indent=4)


def _upsert_dag_run_note(
    session: "Session", additional_notes: str, dag_run: "DagRun"
):
    from airflow.models.dagrun import DagRunNote

    # 1. Update/Insert DAG RUN Note (Single query/operation)
    dr_note = session.query(DagRunNote).filter_by(dag_run_id=dag_run.id).first()
    if dr_note:
        if dr_note.content != additional_notes:
            dr_note.content = additional_notes
    else:
        # Bypass __init__ arguments to avoid TypeError
        new_dr_note = DagRunNote(additional_notes)
        new_dr_note.dag_run_id = dag_run.id
        session.add(new_dr_note)


def _upsert_task_instance_notes(
    session: "Session", dag: "DAG", dag_run: "DagRun"
):
    existing_notes_map = {
        (n.task_id, n.map_index): n
        for n in _get_task_instance_notes(session, dag_run)
    }
    doc_md_map = {task.task_id: task.doc_md for task in dag.tasks}

    for task_instance in _get_task_instances(session, dag_run):
        new_content = doc_md_map.get(task_instance.task_id, "")
        if not new_content:
            continue

        _upsert_task_instance_note(
            session, existing_notes_map, task_instance, new_content
        )


def _upsert_task_instance_note(
    session: "Session",
    existing_notes_map: dict[tuple[str, int], "TaskInstanceNote"],
    task_instance: "TaskInstance",
    new_content: str,
):
    from airflow.models.taskinstance import TaskInstanceNote

    existing_note_obj = existing_notes_map.get(
        (task_instance.task_id, task_instance.map_index)
    )

    if existing_note_obj:
        # Only update if changed to reduce DB noise
        if existing_note_obj.content != new_content:
            existing_note_obj.content = new_content
    else:
        # Bypass __init__ arguments to avoid TypeError
        new_ti_note = TaskInstanceNote(new_content)
        new_ti_note.dag_id = task_instance.dag_id
        new_ti_note.task_id = task_instance.task_id
        new_ti_note.run_id = task_instance.run_id
        new_ti_note.map_index = task_instance.map_index

        session.add(new_ti_note)


def _get_task_instance_notes(
    session: "Session", dag_run: "DagRun"
) -> list["TaskInstanceNote"]:
    from airflow.models.taskinstance import TaskInstanceNote

    return (
        session.query(TaskInstanceNote)
        .filter(
            TaskInstanceNote.dag_id == dag_run.dag_id,
            TaskInstanceNote.run_id == dag_run.run_id,
        )  # pyright: ignore[reportOptionalCall]
        .all()
    )


def _get_task_instances(
    session: "Session", dag_run: "DagRun"
) -> list["TaskInstance"]:
    from airflow.models import TaskInstance

    return (
        session.query(TaskInstance)
        .filter(
            TaskInstance.dag_id == dag_run.dag_id,
            TaskInstance.run_id == dag_run.run_id,
        )  # pyright: ignore[reportOptionalCall]
        .all()
    )


def generate(
    pipeline: "PipelineModel",
    tags: list[str],
    dag_notes: str,
    data_root: str,
    bundle_id: str | None,
    pipeline_id: str,
) -> "DAG":
    """Generates the Airflow DAG for the given pipeline model.

    Args:
        pipeline: The parsed pipeline model.
        tags: A list of tags to apply to the generated DAG.
        dag_notes: The markdown documentation/notes for the DAG.
        data_root: Root directory for pipeline data used for template search.
        bundle_id: The ID of the bundle.
        pipeline_id: The ID of the pipeline.

    Returns:
        The fully constructed Airflow DAG.

    Raises:
        ValueError: If a task dependency cannot be resolved.
    """
    from airflow.models import DAG

    action_handlers = action_handler_registry.get_action_handlers(task_factory)

    dag_kwargs = _build_dag_kwargs(
        pipeline, tags, dag_notes, data_root, bundle_id, pipeline_id
    )
    _configure_dag_schedule(dag_kwargs, pipeline.triggers)
    dag = DAG(**dag_kwargs)
    _create_init_task(bundle_id, pipeline_id, dag, dag_notes)

    tasks = _create_tasks(dag, action_handlers, pipeline)

    # 3. Add cross-task dependencies
    for action in pipeline.actions:
        _set_dependencies(tasks, action)

    return dag


def _build_dag_kwargs(
    pipeline: "PipelineModel",
    tags: list[str],
    dag_notes: str,
    data_root: str,
    bundle_id: str | None,
    pipeline_id: str,
) -> DAGKwargs:
    finish_callback = pipeline_run_callback(bundle_id, pipeline_id)
    on_failure_callbacks = [finish_callback]

    if pipeline.notifications and pipeline.notifications.onPipelineFailure:
        emails = pipeline.notifications.onPipelineFailure.email
        on_failure_callback = partial(send_failure_notification_email, emails)
        on_failure_callbacks.append(on_failure_callback)

    return {
        "dag_id": pipeline.metadata.pipelineId,
        "description": pipeline.metadata.description,
        "default_args": {
            "owner": pipeline.metadata.owner,
            "retries": pipeline.defaults.executionConfigDefault.retries,
        },
        "tags": tags,
        "template_searchpath": [data_root] if data_root else [],
        "doc_md": dag_notes,
        "on_failure_callback": on_failure_callbacks,
        "on_success_callback": [finish_callback],
    }


def _configure_dag_schedule(
    dag_kwargs: DAGKwargs, triggers: list["AnyScheduleTrigger"]
):
    from orchestration_pipelines_lib.internal_models.triggers import (
        ScheduleTriggerModel,
    )

    schedule_trigger = next(
        (t for t in triggers if isinstance(t, ScheduleTriggerModel)),
        None,
    )

    if schedule_trigger:
        task_factory.create_schedule_trigger_task(dag_kwargs, schedule_trigger)
    else:
        dag_kwargs["schedule"] = None


def _create_init_task(
    bundle_id: str | None, pipeline_id: str, dag: "DAG", dag_notes: str
):
    from airflow.operators.python import PythonOperator

    task_finish_callback = init_context_callback(bundle_id, pipeline_id)

    _ = PythonOperator(
        task_id="init_orchestration_pipeline_context",
        python_callable=init_orchestration_pipeline_context,
        op_args=[dag_notes],
        dag=dag,
        on_failure_callback=[task_finish_callback],
        on_success_callback=[task_finish_callback],
    )


def _create_tasks(
    dag: "DAG",
    action_handlers: dict[type, "Callable"],
    pipeline: "PipelineModel",
) -> dict[str, "Any"]:
    tasks = {}

    # 2. Create tasks in a task group and explicitly associate them with the dag
    for action in pipeline.actions:
        handler = action_handlers.get(type(action))

        if not handler:
            continue

        # IMPORTANT: Ensure your handler passes 'dag=dag' to the
        # Operator constructor
        task_obj = handler(action, pipeline, dag=dag)
        tasks[action.name] = task_obj

    return tasks


def _set_dependencies(tasks: dict[str, "Any"], action: "AnyAction"):
    if not (action.dependsOn and action.name in tasks):
        return

    current_task = tasks[action.name]
    for dep_name in action.dependsOn:
        if dep_name not in tasks:
            raise ValueError(
                f"Task {dep_name} being upstream dependency for "
                f"{action.name} does not exist."
            )

        upstream_task = tasks[dep_name]
        # Relationships are safely set on the objects directly
        current_task.set_upstream(upstream_task)


def get_actively_running_versions(pipeline_id, bundle_id) -> list[str]:
    """Retrieves a list of actively running versions for a given pipeline.

    Queries the Airflow database to find any DAG runs currently in 'running' or
    'queued' states that match the bundle and pipeline ID pattern.
    """
    from airflow.models import DagRun
    from airflow.utils.session import create_session
    from airflow.utils.state import State

    active_states = [State.RUNNING, State.QUEUED]
    with create_session() as session:
        runs: list[tuple[str]] = (
            session.query(DagRun.dag_id)
            .filter(
                DagRun.state.in_(active_states),  # type: ignore
                DagRun.dag_id.like(f"{bundle_id}__v__%__{pipeline_id}"),  # type: ignore
            )  # pyright: ignore[reportOptionalCall]
            .all()
        )
    version_ids = list(
        {
            x[0]
            .removeprefix(f"{bundle_id}__v__")
            .removesuffix(f"__{pipeline_id}")
            for x in runs
        }
    )
    return version_ids


def get_previous_default_versions(
    pipeline_id: str, bundle_id: str
) -> list[str]:
    """Retrieves a list of previous default versions for a given pipeline.

    Queries the Airflow database for DAGs tagged as current for the specific
    bundle and pipeline.
    """
    from airflow.utils.session import create_session

    with create_session() as session:
        subquery = _get_dag_tags_subquery(session, pipeline_id, bundle_id)
        tags = _get_tags(session, subquery)

        return _extract_versions(tags)


def _get_dag_tags_subquery(
    session: "Session", pipeline_id: str, bundle_id: str
) -> "Subquery":
    # 1. Subquery to find dag_ids that have ALL THREE required tags.
    # This uses a "Tag Intersection" pattern (GROUP BY + HAVING COUNT)
    # which avoids multiple joins and table scans.
    from airflow.models import DagTag
    from sqlalchemy import func

    return (
        session.query(DagTag.dag_id)
        .filter(
            DagTag.name.in_(
                [
                    "op:is_current",
                    f"op:bundle:{bundle_id}",
                    f"op:pipeline:{pipeline_id}",
                ]
            )
        )  # pyright: ignore[reportOptionalCall]
        .group_by(DagTag.dag_id)
        .having(func.count(DagTag.name) == 3)
        .subquery()
    )


def _get_tags(
    session: "Session", subquery: "Subquery"
) -> list[tuple[str, str]]:
    # 2. Outer query to fetch ONLY the version tags for the matching DAGs.
    # This is pure tag-based filtering and completely decouples the query
    # from the dag_id naming convention.
    from airflow.models import DagTag

    return (
        session.query(DagTag.dag_id, DagTag.name)
        .filter(DagTag.dag_id.in_(subquery), DagTag.name.like("op:version:%"))  # pyright: ignore[reportOptionalCall]
        .all()
    )


def _extract_versions(tags: list[tuple[str, str]]) -> list[str]:
    versions: set[str] = set()
    for _, tag_name in tags:
        version_id = tag_name.removeprefix("op:version:")
        if version_id:
            versions.add(version_id)

    return list(versions)
