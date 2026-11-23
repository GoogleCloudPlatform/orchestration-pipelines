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
"""Module with uncommon util methods."""

import importlib
import logging
import os
import sys
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from orchestration_pipelines_lib.utils.metrics import (
    BasicStatus,
    PipelineRunTriggerType,
    report_init_context,
    report_pipeline_run,
)

if TYPE_CHECKING:
    from airflow.utils.context import Context


def import_callable(path: str, function: str) -> Any:
    """Dynamically imports a callable from a Python module.

    Args:
        path: The relative path to the Python module file.
        function: The name of the callable function to import.

    Returns:
        The imported callable object, or None if an error occurs during import.
    """
    try:
        dags_folder = os.environ.get("DAGS_FOLDER", "/home/airflow/gcs/dags")
        module_path = os.path.join(dags_folder, path)
        if not os.path.exists(module_path):
            raise ImportError(f"Module file not found at: {module_path}")
        module_spec = importlib.util.spec_from_file_location(
            "test_script_1", module_path
        )
        if not module_spec:
            raise ImportError(f"Cannot find spec for module at {module_path}.")
        module = importlib.util.module_from_spec(module_spec)
        sys.modules["test_script_1"] = module
        module_spec.loader.exec_module(module)
        if not hasattr(module, function):
            raise AttributeError(
                f"Module {module_path} has no attribute '{function}'"
            )
        return getattr(module, function)
    except (ImportError, AttributeError) as e:
        logging.error(
            "Failed to import callable '%s' from '%s': %s", function, path, e
        )
    except Exception as e:  # pylint: disable=broad-exception-caught
        logging.error(
            "Unexpected error importing callable '%s' from '%s': %s",
            function,
            path,
            e,
        )


def pipeline_run_callback(
    bundle_id: str | None, pipeline_id: str
) -> Callable[["Context"], None]:
    """Prepares a callback for pipeline runs."""

    def callback(context: "Context") -> None:
        dag_run = context.get("dag_run")

        if not dag_run:
            return

        trigger_type = PipelineRunTriggerType.from_dag_run_type(
            dag_run.run_type
        )
        status = BasicStatus.from_dag_run_state(dag_run.state)

        report_pipeline_run(
            bundle_id,
            pipeline_id,
            trigger_type,
            status,
        )

    return callback


def init_context_callback(
    bundle_id: str | None, pipeline_id: str
) -> Callable[["Context"], None]:
    """Prepares a callback for the initialization task context."""

    def callback(context: "Context") -> None:
        ti = context.get("ti") or context.get("task_instance")

        if not ti:
            return

        status = BasicStatus.from_task_instance_state(ti.state)
        report_init_context(bundle_id, pipeline_id, status)

    return callback


def dynamic_email_callback(
    pipeline: Any,
    cel_expr: str,
    is_success: bool,
    emails_callback: Callable[[list[str], bool, "Context"], None],
) -> Callable[["Context"], None]:
    """Prepares a callback that resolves notification emails via CEL."""

    def callback(context: "Context | None" = None) -> None:
        from orchestration_pipelines_lib.utils import runtime_resolver

        if context is None:
            try:
                from airflow.sdk import get_current_context  # pyright: ignore
            except ImportError:  # pragma: no cover
                from airflow.operators.python import (  # pyright: ignore
                    get_current_context,
                )
            context = get_current_context()

        eval_ctx = runtime_resolver.build_cel_eval_context(
            None, context, pipeline_model=pipeline
        )
        resolved = runtime_resolver.evaluate_cel_expression(cel_expr, eval_ctx)
        if not isinstance(resolved, list) or not all(
            isinstance(email, str) for email in resolved
        ):
            raise TypeError(
                "Notification email expected list of strings, "
                f"got {type(resolved).__name__}."
            )
        if emails := [email for email in resolved if email]:
            emails_callback(emails, is_success, context)

    return callback
