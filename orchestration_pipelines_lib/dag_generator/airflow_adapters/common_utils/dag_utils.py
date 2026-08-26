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
"""Utility functions for generating Airflow DAGs from pipeline models.

This module provides common functionalities used by Airflow adapters
to construct DAG objects, configure their properties, and generate corresponding
tasks and dependencies.
"""

import json
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from functools import partial
from typing import TYPE_CHECKING, Any, TypedDict

from orchestration_pipelines_lib.dag_generator.airflow_adapters.common_utils import (  # noqa: E501
    action_handler_registry,
)
from orchestration_pipelines_lib.dag_generator.airflow_adapters.common_utils.retry_resolver import (  # noqa: E501
    RetryResolver,
)
from orchestration_pipelines_lib.dag_generator.airflow_adapters.common_utils.utils import (  # noqa: E501
    init_context_callback,
    pipeline_run_callback,
)

if TYPE_CHECKING:
    from datetime import datetime

    from airflow.models import DAG
    from airflow.models.dag import DagStateChangeCallback, ScheduleArg
    from airflow.utils.context import Context

    from orchestration_pipelines_lib.internal_models.pipeline import (
        AnyAction,
        AnyScheduleTrigger,
        PipelineModel,
    )


class DAGKwargs(TypedDict, total=False):
    """A non-exhaustive list of keys for Airflow DAG constructor."""

    dag_id: str
    description: str | None
    default_args: dict[str, Any]
    tags: list[str] | None
    template_searchpath: str | Iterable[str] | None
    schedule: "ScheduleArg"
    start_date: "datetime"
    end_date: "datetime | None"
    catchup: bool
    doc_md: str | None
    user_defined_macros: dict[str, Any] | None
    on_failure_callback: (
        "DagStateChangeCallback | list[DagStateChangeCallback] | None"
    )
    on_success_callback: (
        "DagStateChangeCallback | list[DagStateChangeCallback] | None"
    )


@dataclass(frozen=True, slots=True)
class AirflowVersionedDependencies:
    """Airflow version-specific dependencies required for DAG generation."""

    task_factory: Any
    emails_callback: Callable[[list[str], bool, "Context"], None]
    init_pipeline_context: Callable[..., None]
    init_task_operator: type


def generate(
    pipeline: "PipelineModel",
    tags: list[str],
    dag_notes: str,
    data_root: str,
    bundle_id: str | None,
    pipeline_id: str,
    versioned_deps: AirflowVersionedDependencies,
) -> "DAG":
    """Generates the Airflow DAG for the given pipeline model.

    Args:
        pipeline: The parsed pipeline model.
        tags: A list of tags to apply to the generated DAG.
        dag_notes: The markdown documentation/notes for the DAG.
        data_root: Root directory for pipeline data used for template search.
        bundle_id: The ID of the bundle.
        pipeline_id: The ID of the pipeline.
        versioned_deps: Object containing Airflow version-specific dependencies
            (task_factory, emails_callback, init_pipeline_context,
            init_task_operator).

    Returns:
        The fully constructed Airflow DAG.

    Raises:
        ValueError: If a task dependency cannot be resolved.
    """
    try:
        from airflow.sdk import DAG  # pyright: ignore[reportMissingImports]
    except ImportError:
        from airflow.models import DAG  # pyright: ignore[reportMissingImports]

    action_handlers = action_handler_registry.get_action_handlers(
        versioned_deps.task_factory
    )

    dag_kwargs = _build_dag_kwargs(
        pipeline,
        tags,
        dag_notes,
        data_root,
        bundle_id,
        pipeline_id,
        versioned_deps.emails_callback,
        versioned_deps.task_factory,
    )
    _configure_dag_schedule(
        dag_kwargs, pipeline.triggers, versioned_deps.task_factory
    )

    dag = DAG(**dag_kwargs)
    _create_init_task(
        dag,
        dag_notes,
        bundle_id,
        pipeline_id,
        versioned_deps.init_pipeline_context,
        versioned_deps.init_task_operator,
    )

    tasks = _create_tasks(dag, action_handlers, pipeline)

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
    emails_callback: Callable[[list[str], bool, "Context"], None],
    task_factory,
) -> DAGKwargs:
    finish_callback = pipeline_run_callback(bundle_id, pipeline_id)
    on_failure_callbacks = [finish_callback]
    on_success_callbacks = [finish_callback]

    if pipeline.notifications:
        if pipeline.notifications.onPipelineFailure:
            emails = pipeline.notifications.onPipelineFailure.email
            on_failure_callback = partial(emails_callback, emails, False)
            on_failure_callbacks.append(on_failure_callback)

        if pipeline.notifications.onPipelineSuccess:
            emails = pipeline.notifications.onPipelineSuccess.email
            on_success_callback = partial(emails_callback, emails, True)
            on_success_callbacks.append(on_success_callback)

    return {
        "dag_id": pipeline.metadata.pipelineId,
        "description": pipeline.metadata.description,
        "default_args": {
            "owner": pipeline.metadata.owner,
            **RetryResolver.resolve_default_args(pipeline.defaults),
        },
        "tags": tags,
        "template_searchpath": [data_root] if data_root else [],
        "doc_md": dag_notes,
        "user_defined_macros": {
            "resolve_latest_pipeline_dag_id": task_factory._resolve_latest_pipeline_dag_id,  # noqa: E501
        },
        "on_failure_callback": on_failure_callbacks,
        "on_success_callback": on_success_callbacks,
    }


def _configure_dag_schedule(
    dag_kwargs: DAGKwargs, triggers: list["AnyScheduleTrigger"], task_factory
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
    dag: "DAG",
    dag_notes: str,
    bundle_id: str | None,
    pipeline_id: str,
    init_pipeline_context: Callable[[str, dict], None],
    init_task_operator: type,
):
    task_finish_callback = init_context_callback(bundle_id, pipeline_id)

    init_task_operator(
        task_id="init_orchestration_pipeline_context",
        python_callable=init_pipeline_context,
        op_args=[dag_notes],
        dag=dag,
        on_failure_callback=[task_finish_callback],
        on_success_callback=[task_finish_callback],
    )


def _create_tasks(
    dag: "DAG",
    action_handlers: dict[type, Callable],
    pipeline: "PipelineModel",
) -> dict[str, Any]:
    """Create tasks in a task group and explicitly associate them with the dag.

    Args:
        dag: The DAG to attach the tasks to.
        action_handlers: A dictionary mapping action types to their handlers.
        pipeline: The pipeline model.

    Returns:
        A dictionary mapping action names to their corresponding tasks.
    """
    tasks = {}

    for action in pipeline.actions:
        handler = action_handlers.get(type(action))

        if not handler:
            continue

        # IMPORTANT: Ensure your handler passes 'dag=dag' to the
        # Operator constructor
        task_obj = handler(action, pipeline, dag=dag)
        tasks[action.name] = task_obj

    return tasks


def _set_dependencies(tasks: dict[str, Any], action: "AnyAction"):
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


def extract_additional_notes(note_content: str | None) -> str:
    """Filter note_content to keep only specific fields."""
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
