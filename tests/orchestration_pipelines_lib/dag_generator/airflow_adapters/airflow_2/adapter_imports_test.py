# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""Unit tests for adapter_imports in Airflow 2 adapter."""

from orchestration_pipelines_lib.dag_generator.airflow_adapters.airflow_2.adapter_imports import (  # noqa: E501
    get_imports,
    get_python_operator,
    get_python_virtualenv_operator,
    get_trigger_dagrun_operator,
    get_variable_class,
)
from orchestration_pipelines_lib.dag_generator.airflow_adapters.common_utils.action_handler_registry import (  # noqa: E501
    AdapterImports,
)


def test_get_imports_returns_adapter_import_getters():
    """Tests get_imports returns all expected getter callables."""
    imports = get_imports()

    assert imports == AdapterImports(
        get_python_operator=get_python_operator,
        get_python_virtualenv_operator=get_python_virtualenv_operator,
        get_trigger_dagrun_operator=get_trigger_dagrun_operator,
        get_variable_class=get_variable_class,
    )


def test_get_python_operator_returns_airflow_2_operator():
    """Tests get_python_operator returns PythonOperator."""
    from airflow.operators.python import PythonOperator

    assert get_python_operator() is PythonOperator


def test_get_python_virtualenv_operator_returns_airflow_2_operator():
    """Tests get_python_virtualenv_operator returns PythonVirtualenvOperator."""
    from airflow.operators.python import PythonVirtualenvOperator

    assert get_python_virtualenv_operator() is PythonVirtualenvOperator


def test_get_variable_class_returns_airflow_2_variable():
    """Tests get_variable_class returns Variable class."""
    from airflow.models.variable import Variable

    assert get_variable_class() is Variable


def test_get_trigger_dagrun_operator_returns_airflow_2_operator():
    """Tests get_trigger_dagrun_operator returns TriggerDagRunOperator."""
    from airflow.operators.trigger_dagrun import TriggerDagRunOperator

    assert get_trigger_dagrun_operator() is TriggerDagRunOperator
