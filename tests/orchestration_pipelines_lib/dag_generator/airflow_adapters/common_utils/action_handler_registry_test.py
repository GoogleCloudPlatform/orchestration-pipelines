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
"""Unit tests for the action handler registry."""

from functools import partial

import pytest

from orchestration_pipelines_lib.dag_generator.airflow_adapters.common_utils import (  # noqa: E501
    action_handler_registry as registry,
)
from orchestration_pipelines_lib.dag_generator.airflow_adapters.common_utils import (  # noqa: E501
    task_utils,
)
from orchestration_pipelines_lib.internal_models import actions


def _assert_partial(handler: object, func: object, arg: object) -> None:
    """Asserts that handler is a partial wrapping func with single arg."""
    assert isinstance(handler, partial)
    assert handler.func is func
    assert handler.args == (arg,)

@pytest.fixture(scope="module")
def adapters():
    def get_python_op() -> type:
        return type

    def get_venv_op() -> type:
        return type

    def get_trigger_op() -> type:
        return type

    def get_variable() -> type:
        return type

    adapter_imports = registry.AdapterImports(
        get_python_operator=get_python_op,
        get_python_virtualenv_operator=get_venv_op,
        get_trigger_dagrun_operator=get_trigger_op,
        get_variable_class=get_variable,
    )

    return adapter_imports

@pytest.fixture(scope="module")
def handlers(adapters):
    return registry.get_action_handlers(adapters)

@pytest.mark.parametrize(
    ("action_model", "expected_handler", "adapter_attr"),
    [
        (
            actions.PythonScriptActionModel,
            task_utils.create_python_script_task,
            "get_python_operator",
        ),
        (
            actions.PythonVirtualenvActionModel,
            task_utils.create_python_virtualenv_task,
            "get_python_virtualenv_operator",
        ),
        (
            actions.DBTActionModel,
            task_utils.create_dbt_task,
            "get_python_operator",
        ),
        (
            actions.DataformActionModel,
            task_utils.create_dataform_task,
            "get_variable_class",
        ),
        (
            actions.OrchestrationPipelineActionModel,
            task_utils.create_orchestration_pipeline_trigger_task,
            "get_trigger_dagrun_operator",
        ),
        (
            actions.BqOperationActionModel,
            task_utils.create_bq_operation_task,
            None,
        ),
        (
            actions.DataprocOperatorActionModel,
            task_utils.create_dataproc_operator_task,
            None,
        ),
        (
            actions.DataIngestionActionModel,
            task_utils.create_bq_dts_task,
            None,
        ),
        (
            actions.AIActionModel,
            task_utils.create_ai_task,
            None,
        ),
        (
            actions.AirflowActionModel,
            task_utils.create_airflow_task,
            None,
        ),
    ]
)
def test_get_action_handlers(action_model, expected_handler, adapter_attr, handlers, adapters):
    """Tests that the static action handler mapping resolves correctly."""
    if adapter_attr is not None:
        _assert_partial(
            handlers[action_model],
            expected_handler,
            getattr(adapters, adapter_attr),
        )
    else:
        assert handlers[action_model] is expected_handler

def test_handlers_numbers(handlers):
    """Test check if the number of handlers is correct."""
    assert len(handlers) == 10
