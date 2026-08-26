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

from typing import TYPE_CHECKING

from orchestration_pipelines_lib.dag_generator.airflow_adapters.common_utils import (  # noqa: E501
    dag_utils,
)

# Airflow and SQLAlchemy imports moved inside functions to reduce import tax
from . import task_factory
from .email_utils import send_notification_email

if TYPE_CHECKING:
    from airflow.models import DAG, DagRun, TaskInstance
    from airflow.models.taskinstance import TaskInstanceNote
    from airflow.utils.context import Context
    from sqlalchemy.orm import Session
    from sqlalchemy.sql.selectable import Subquery


def get_deps() -> dag_utils.AirflowVersionedDependencies:
    """Returns Airflow 2 specific dependencies for DAG generation."""
    from airflow.operators.python import PythonOperator

    return dag_utils.AirflowVersionedDependencies(
        task_factory=task_factory,
        emails_callback=send_notification_email,
        init_pipeline_context=init_orchestration_pipeline_context,
        init_task_operator=PythonOperator,
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

    additional_notes = dag_utils.extract_additional_notes(note_content)
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


def _upsert_dag_run_note(
    session: "Session", additional_notes: str, dag_run: "DagRun"
):
    from airflow.models.dagrun import DagRunNote

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
    """Subquery to find dag_ids that have the required tags.

    This uses a "Tag Intersection" pattern (GROUP BY + HAVING COUNT)
    which avoids multiple joins and table scans.
    """
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
    """Outer query to fetch ONLY the version tags for the matching DAGs.

    This is pure tag-based filtering and completely decouples the query
    from the dag_id naming convention.
    """
    from airflow.models import DagTag

    return (
        session.query(DagTag.dag_id, DagTag.name)
        .filter(
            DagTag.dag_id.in_(subquery), DagTag.name.like("op:version:%")
        )  # pyright: ignore[reportOptionalCall]
        .all()
    )


def _extract_versions(tags: list[tuple[str, str]]) -> list[str]:
    versions: set[str] = set()
    for _, tag_name in tags:
        version_id = tag_name.removeprefix("op:version:")
        if version_id:
            versions.add(version_id)

    return list(versions)
