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
"""Unit tests for Airflow 2 task_factory."""

import unittest
from unittest.mock import MagicMock

import pytest

from orchestration_pipelines_lib.dag_generator.airflow_adapters.airflow_2 import (
    task_factory,
)


class TaskFactoryAirflow2Test(unittest.TestCase):
    """Tests for Airflow 2 task_factory."""

    def test_create_python_script_task_raises_runtime_error_from_exception(
        self,
    ):
        """Tests that create_python_script_task wraps exceptions in RuntimeError."""
        action = MagicMock()
        action.name = "my_python_script_action"
        action.type = "python_script"
        action.config.pythonCallable = "my_func"
        action.executionTimeout = 123
        dag = MagicMock()

        with pytest.raises(RuntimeError) as exc_info:
            task_factory.create_python_script_task(action, {}, dag)

        assert str(exc_info.value) == (
            "Failed to create task for action 'my_python_script_action' "
            "from 'my_func': 'int' object has no attribute 'strip'"
        )

    def test_create_python_virtualenv_task_raises_runtime_error_from_exception(
        self,
    ):
        """Tests that create_python_virtualenv_task wraps exceptions in RuntimeError."""
        action = MagicMock()
        action.name = "my_virtualenv_action"
        action.filename = "script.py"
        action.config.pythonCallable = "my_func"
        dag = MagicMock()

        with pytest.raises(RuntimeError) as exc_info:
            task_factory.create_python_virtualenv_task(action, {}, dag)

        assert str(exc_info.value) == (
            "Failed to create task for action 'my_virtualenv_action' "
            "from 'my_func': Action my_virtualenv_action: filename script.py "
            "with callable my_func did not resolve to a callable object."
        )

    def test_create_dbt_task_raises_runtime_error_from_exception(self):
        """Tests that create_dbt_task wraps exceptions in RuntimeError."""
        action = MagicMock()
        action.name = "my_dbt_action"
        action.type = "dbt"
        action.source = None
        dag = MagicMock()

        with pytest.raises(RuntimeError) as exc_info:
            task_factory.create_dbt_task(action, {}, dag)

        assert str(exc_info.value) == (
            "Failed to create task for action 'my_dbt_action': "
            "'NoneType' object has no attribute 'path'"
        )

    def test_create_orchestration_pipeline_trigger_task_raises_runtime_error(
        self,
    ):
        """Tests that create_orchestration_pipeline_trigger_task wraps exceptions in RuntimeError."""
        action = MagicMock()
        action.name = "my_trigger_action"
        action.type = "orchestration_pipeline"
        action.executionTimeout = 123
        dag = MagicMock()

        with pytest.raises(RuntimeError) as exc_info:
            task_factory.create_orchestration_pipeline_trigger_task(
                action, {}, dag
            )

        assert str(exc_info.value) == (
            "Failed to create task for action 'my_trigger_action': "
            "'int' object has no attribute 'strip'"
        )


if __name__ == "__main__":
    unittest.main()
