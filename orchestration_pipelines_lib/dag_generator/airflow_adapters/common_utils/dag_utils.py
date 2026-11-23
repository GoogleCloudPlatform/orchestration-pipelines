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

import copy
import json
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from functools import partial
from typing import TYPE_CHECKING, Any, TypedDict

from orchestration_pipelines_lib.dag_generator.airflow_adapters.common_utils import (  # noqa: E501
    action_handler_registry,
    task_utils,
)
from orchestration_pipelines_lib.dag_generator.airflow_adapters.common_utils.retry_resolver import (  # noqa: E501
    RetryResolver,
)
from orchestration_pipelines_lib.dag_generator.airflow_adapters.common_utils.utils import (  # noqa: E501
    dynamic_email_callback,
    init_context_callback,
    pipeline_run_callback,
)
from orchestration_pipelines_lib.utils import dict_utils, runtime_resolver

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
    params: dict[str, Any]


@dataclass(frozen=True, slots=True)
class AirflowVersionedDependencies:
    """Airflow version-specific dependencies required for DAG generation."""

    adapter_imports: action_handler_registry.AdapterImports
    resolve_latest_pipeline_dag_id: Callable[[str, str | None], str]
    emails_callback: Callable[[list[str], bool, "Context"], None]
    init_pipeline_context: Callable[..., None]


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
            (adapter_imports, resolve_latest_pipeline_dag_id,
            emails_callback, init_pipeline_context).

    Returns:
        The fully constructed Airflow DAG.

    Raises:
        ValueError: If a task dependency cannot be resolved.
    """
    try:
        from airflow.sdk import DAG  # pyright: ignore[reportMissingImports]
    except ImportError:
        from airflow.models import DAG  # pyright: ignore[reportMissingImports]

    adapter_imports = versioned_deps.adapter_imports
    action_handlers = action_handler_registry.get_action_handlers(
        adapter_imports
    )

    dag_kwargs = _build_dag_kwargs(
        pipeline,
        tags,
        dag_notes,
        data_root,
        bundle_id,
        pipeline_id,
        versioned_deps.emails_callback,
        versioned_deps.resolve_latest_pipeline_dag_id,
        adapter_imports.get_param_class,
    )
    _configure_dag_schedule(dag_kwargs, pipeline.triggers)

    dag = DAG(**dag_kwargs)
    _create_init_task(
        dag,
        dag_notes,
        bundle_id,
        pipeline_id,
        versioned_deps.init_pipeline_context,
        adapter_imports.get_python_operator(),
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
    resolve_latest_pipeline_dag_id: Callable[[str, str | None], str],
    get_param_class: Callable[[], type],
) -> DAGKwargs:
    finish_callback = pipeline_run_callback(bundle_id, pipeline_id)
    on_failure_callbacks = [finish_callback]
    on_success_callbacks = [finish_callback]

    raw_bindings = (
        pipeline.dynamicBindings
        if isinstance(pipeline.dynamicBindings, Mapping)
        else {}
    )
    dynamic_bindings = {
        ".".join(dict_utils.to_snake_case(seg) for seg in k.split(".")): v
        for k, v in raw_bindings.items()
        if isinstance(k, str)
    }
    failure_expr = dynamic_bindings.get(
        "notifications.on_pipeline_failure.email"
    )
    success_expr = dynamic_bindings.get(
        "notifications.on_pipeline_success.email"
    )
    runtime_pipeline_ctx = (
        runtime_resolver.build_runtime_pipeline_context(pipeline)
        if failure_expr or success_expr
        else None
    )
    notifications = pipeline.notifications
    for cel_expr, notification_config, is_success, target_callbacks in (
        (
            failure_expr,
            notifications.onPipelineFailure if notifications else None,
            False,
            on_failure_callbacks,
        ),
        (
            success_expr,
            notifications.onPipelineSuccess if notifications else None,
            True,
            on_success_callbacks,
        ),
    ):
        if cel_expr:
            target_callbacks.append(
                dynamic_email_callback(
                    runtime_pipeline_ctx, cel_expr, is_success, emails_callback
                )
            )
        elif notification_config:
            target_callbacks.append(
                partial(emails_callback, notification_config.email, is_success)
            )

    dag_kwargs: DAGKwargs = {
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
            "resolve_latest_pipeline_dag_id": resolve_latest_pipeline_dag_id,
        },
        "on_failure_callback": on_failure_callbacks,
        "on_success_callback": on_success_callbacks,
    }

    if pipeline.parameters:
        param_cls = get_param_class()
        dag_kwargs["params"] = {
            name: param_cls(
                default=param.default,
                type=param.type,
                description=param.description,
            )
            for name, param in pipeline.parameters.items()
        }

    return dag_kwargs


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
        task_utils.create_schedule_trigger_task(dag_kwargs, schedule_trigger)
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
    runtime_resolver.register_operator_kwargs_builder(
        action_handler_registry.build_runtime_operator_kwargs
    )
    tasks = {}
    runtime_pipeline_ctx = runtime_resolver.build_runtime_pipeline_context(
        pipeline
    )
    referenced_outputs: set[str] = set()
    for act in pipeline.actions:
        referenced_outputs.update(
            runtime_resolver.referenced_output_actions(act)
        )

    for action in pipeline.actions:
        handler = action_handlers.get(type(action))

        if not handler:
            continue

        # IMPORTANT: Ensure your handler passes 'dag=dag' to the
        # Operator constructor
        task_obj = handler(action, pipeline, dag=dag)
        _attach_runtime_metadata(
            task_obj,
            action,
            runtime_pipeline_ctx,
            is_output_referenced=action.name in referenced_outputs,
        )
        tasks[action.name] = task_obj

    return tasks


def _attach_runtime_metadata(
    task_obj: Any,
    action: "AnyAction",
    runtime_pipeline_ctx: runtime_resolver.RuntimePipelineContext,
    *,
    is_output_referenced: bool = False,
) -> None:
    """Attaches lightweight action and pipeline metadata onto Airflow tasks."""
    children = getattr(task_obj, "children", None)
    tasks = children.values() if isinstance(children, Mapping) else (task_obj,)
    for task in tasks:
        task_role = getattr(
            task, "_op_task_role", runtime_resolver.TASK_ROLE_MAIN
        )
        is_entry = task_role in runtime_resolver.ENTRY_TASK_ROLES
        is_output = (
            is_output_referenced
            and task_role in runtime_resolver.OUTPUT_TASK_ROLES
        )
        task._op_is_entry_task = is_entry
        task._op_is_output_task = is_output

        needs_dynamic = bool(
            (is_entry and runtime_resolver.get_run_if_expr(action))
            or runtime_resolver.has_applicable_bindings(
                runtime_pipeline_ctx, action, task_role
            )
        )
        if needs_dynamic or is_output:
            task._op_action_model = (
                copy.deepcopy(action) if needs_dynamic else action
            )
        if needs_dynamic:
            task._op_pipeline_model = runtime_pipeline_ctx
            task._op_kwargs_builder = (
                action_handler_registry.build_runtime_operator_kwargs
            )


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
