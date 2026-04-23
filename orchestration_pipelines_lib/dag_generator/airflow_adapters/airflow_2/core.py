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
from typing import Any
from functools import partial


# Airflow and SQLAlchemy imports moved inside functions to reduce import tax
from . import task_factory
from .email_utils import send_failure_notification_email
from orchestration_pipelines_lib.dag_generator.airflow_adapters.common_utils import action_handler_registry
from orchestration_pipelines_lib.internal_models.pipeline import PipelineModel
from orchestration_pipelines_lib.internal_models.triggers import ScheduleTriggerModel


def init_orchestration_pipeline_context(note_content: str, **context):
    from airflow.utils.session import create_session
    from airflow.models.dagrun import DagRunNote
    from airflow.models.taskinstance import TaskInstanceNote
    from airflow.models import TaskInstance
    from sqlalchemy.exc import IntegrityError
    """Initializes the orchestration pipeline context for a DAG run.

    Extracts specific metadata from the provided notes content and applies
    it to the DAG Run and its Task Instances via the Airflow database.

    Args:
        note_content: The JSON string containing the DAG documentation and metadata.
        **context: The Airflow task execution context.
    """
    dag_run = context.get("dag_run")
    dag = context.get("dag")
    # Filter note_content to keep only specific fields
    additional_notes = ""
    if note_content:
        notes_data = json.loads(note_content)
        if isinstance(notes_data, dict):
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
            notes_dict = {
                k: v
                for k, v in notes_data.items() if k in allowed_keys
            }
            if notes_dict:
                additional_notes = json.dumps(notes_dict, indent=4)
    with create_session() as session:
        try:
            # 1. Update/Insert DAG RUN Note (Single query/operation)
            dr_note = session.query(DagRunNote).filter_by(
                dag_run_id=dag_run.id).first()
            if dr_note:
                if dr_note.content != additional_notes:
                    dr_note.content = additional_notes
            else:
                # Bypass __init__ arguments to avoid TypeError
                new_dr_note = DagRunNote(additional_notes)
                new_dr_note.dag_run_id = dag_run.id
                new_dr_note.content = additional_notes
                session.add(new_dr_note)
            # 2. Update/Insert TASK INSTANCE Note
            task_instance_existing_notes = session.query(
                TaskInstanceNote).filter(
                    TaskInstanceNote.dag_id == dag_run.dag_id,
                    TaskInstanceNote.run_id == dag_run.run_id).all()
            existing_notes_map = {
                (n.task_id, n.map_index): n
                for n in task_instance_existing_notes
            }
            doc_md_map = {task.task_id: task.doc_md for task in dag.tasks}
            task_instances = session.query(TaskInstance).filter(
                TaskInstance.dag_id == dag_run.dag_id,
                TaskInstance.run_id == dag_run.run_id).all()
            for task_instance in task_instances:
                new_content = doc_md_map.get(task_instance.task_id, "")
                if not new_content:
                    continue
                existing_note_obj = existing_notes_map.get(
                    (task_instance.task_id, task_instance.map_index))
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
            session.commit()
        except IntegrityError:
            # If a parallel task committed a note first, just roll back and move on
            session.rollback()
        except Exception:
            session.rollback()
            raise


def generate(pipeline: PipelineModel, tags: list[str], dag_notes: str,
             data_root: str) -> Any:
    from airflow.models import DAG
    from airflow.operators.python import PythonOperator
    """Generates the Airflow DAG for the given pipeline model.

    Args:
        pipeline: The parsed pipeline model.
        tags: A list of tags to apply to the generated DAG.
        dag_notes: The markdown documentation/notes for the DAG.
        data_root: The root directory for the pipeline data used for template search.

    Returns:
        The fully constructed Airflow DAG.

    Raises:
        ValueError: If a task dependency cannot be resolved.
    """

    # Defines list of non-relative path to the additional folders where jinja will look for
    # templates. For example, .txt file format is treated as jinja template. By default,
    # it searches in /home/airflow/gcs/dags folder first.
    template_searchpath = []
    if data_root:
        template_searchpath.append(data_root)

    action_handlers = action_handler_registry.get_action_handlers(task_factory)

    schedule_trigger = next(
        (t for t in pipeline.triggers if isinstance(t, ScheduleTriggerModel)),
        None)

    dag_kwargs = {
        "dag_id": pipeline.metadata.pipelineId,
        "description": pipeline.metadata.description,
        "default_args": {
            "owner": pipeline.metadata.owner,
            "retries": pipeline.defaults.executionConfigDefault.retries,
        },
        "tags": tags,
        "template_searchpath": template_searchpath,
    }

    if pipeline.notifications and pipeline.notifications.onPipelineFailure:
        emails = pipeline.notifications.onPipelineFailure.email
        on_failure_callback = partial(send_failure_notification_email, emails)
        dag_kwargs["on_failure_callback"] = on_failure_callback

    if schedule_trigger:
        task_factory.create_schedule_trigger_task(dag_kwargs, schedule_trigger)
    else:
        dag_kwargs["schedule"] = None

    dag_kwargs["doc_md"] = dag_notes

    dag = DAG(**dag_kwargs)
    _ = PythonOperator(task_id="init_orchestration_pipeline_context",
                       python_callable=init_orchestration_pipeline_context,
                       op_args=[dag_notes],
                       dag=dag)
    tasks = {}
    # 2. Create tasks in a task group and explicitly associate them with the dag
    for action in pipeline.actions:
        handler = action_handlers.get(type(action))
        if handler:
            # IMPORTANT: Ensure your handler passes 'dag=dag' to the Operator constructor
            task_obj = handler(action, pipeline, dag=dag)
            tasks[action.name] = task_obj
    # 3. Add cross-task dependencies
    for action in pipeline.actions:
        if action.dependsOn and action.name in tasks:
            current_task = tasks[action.name]
            for dep_name in action.dependsOn:
                if dep_name not in tasks:
                    raise ValueError(
                        f"Task {dep_name} being upstream dependency for "
                        f"{action.name} does not exist.")
                upstream_task = tasks[dep_name]
                # Relationships are safely set on the objects directly
                current_task.set_upstream(upstream_task)

    return dag


def get_actively_running_versions(pipeline_id, bundle_id) -> list[str]:
    """Retrieves a list of actively running versions for a given pipeline.

    Queries the Airflow database to find any DAG runs currently in 'running' or
    'queued' states that match the bundle and pipeline ID pattern.
    """
    from airflow.utils.state import State
    from airflow.utils.session import create_session
    from airflow.models import DagRun
    active_states = [State.RUNNING, State.QUEUED]
    with create_session() as session:
        runs = (session.query(DagRun.dag_id).filter(
            DagRun.state.in_(active_states),
            DagRun.dag_id.like(f"{bundle_id}__v__%__{pipeline_id}"),
        ).all())
    version_ids = list(set([
        x[0].removeprefix(f"{bundle_id}__v__").removesuffix(
            f"__{pipeline_id}") for x in runs
    ]))
    return version_ids


def get_previous_default_versions(pipeline_id: str,
                                  bundle_id: str) -> list[str]:
    """Retrieves a list of previous default versions for a given pipeline.

    Queries the Airflow database for DAGs tagged as current for the specific
    bundle and pipeline.
    """
    from airflow.models.dag import DagTag
    from airflow.utils.session import create_session
    from sqlalchemy import func
    with create_session() as session:
        # 1. Subquery to find dag_ids that have ALL THREE required tags.
        # This uses a "Tag Intersection" pattern (GROUP BY + HAVING COUNT)
        # which avoids multiple joins and table scans.
        subquery = (
            session.query(DagTag.dag_id)
            .filter(DagTag.name.in_([
                "op:is_current",
                f"op:bundle:{bundle_id}",
                f"op:pipeline:{pipeline_id}"
            ]))
            .group_by(DagTag.dag_id)
            .having(func.count(DagTag.name) == 3)
            .subquery()
        )

        # 2. Outer query to fetch ONLY the version tags for the matching DAGs.
        # This is pure tag-based filtering and completely decouples the query
        # from the dag_id naming convention.
        tags = (
            session.query(DagTag.dag_id, DagTag.name)
            .filter(
                DagTag.dag_id.in_(subquery),
                DagTag.name.like("op:version:%")
            )
            .all()
        )

        versions = set()
        for _, tag_name in tags:
            version_id = tag_name.removeprefix("op:version:")
            if version_id:
                versions.add(version_id)

        return list(versions)
