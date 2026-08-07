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
"""Module providing Airflow 3 adapter imports."""

from typing import TYPE_CHECKING

from orchestration_pipelines_lib.dag_generator.airflow_adapters.common_utils.action_handler_registry import (  # noqa: E501
    AdapterImports,
)

if TYPE_CHECKING:
    from airflow.providers.standard.operators.python import (
        PythonOperator,
        PythonVirtualenvOperator,
    )
    from airflow.providers.standard.operators.trigger_dagrun import (
        TriggerDagRunOperator,
    )
    from airflow.sdk import Variable


def get_imports() -> AdapterImports:
    """Returns the adapter imports for Airflow 3."""
    return AdapterImports(
        get_python_operator=get_python_operator,
        get_python_virtualenv_operator=get_python_virtualenv_operator,
        get_trigger_dagrun_operator=get_trigger_dagrun_operator,
        get_variable_class=get_variable_class,
    )


def get_python_operator() -> "type[PythonOperator]":
    """Returns the PythonOperator class."""
    from airflow.providers.standard.operators.python import PythonOperator

    return PythonOperator


def get_python_virtualenv_operator() -> "type[PythonVirtualenvOperator]":
    """Returns the PythonVirtualenvOperator class."""
    from airflow.providers.standard.operators.python import (
        PythonVirtualenvOperator,
    )

    return PythonVirtualenvOperator


def get_variable_class() -> "type[Variable]":
    """Returns the Variable class."""
    from airflow.sdk import Variable

    return Variable


def get_trigger_dagrun_operator() -> "type[TriggerDagRunOperator]":
    """Returns the TriggerDagRunOperator class."""
    from airflow.providers.standard.operators.trigger_dagrun import (
        TriggerDagRunOperator,
    )

    return TriggerDagRunOperator
