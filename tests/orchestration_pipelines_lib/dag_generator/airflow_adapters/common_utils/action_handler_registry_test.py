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

import unittest
from unittest.mock import MagicMock

from orchestration_pipelines_lib.dag_generator.airflow_adapters.common_utils import (  # noqa: E501
    action_handler_registry as registry,
)
from orchestration_pipelines_lib.internal_models import actions


class ActionHandlerRegistryTest(unittest.TestCase):
    """Tests for the action handler registry."""

    def test_get_action_handlers(self):
        """Tests that the static action handler mapping resolves correctly."""
        # Create mock task factory
        mock_task_factory = MagicMock()
        mock_task_factory.create_python_script_task = "python_script_handler"
        mock_task_factory.create_python_virtualenv_task = (
            "python_virtualenv_handler"
        )
        mock_task_factory.create_bq_operation_task = "bq_operation_handler"
        mock_task_factory.create_dataproc_operator_task = (
            "dataproc_operator_handler"
        )
        mock_task_factory.create_dbt_task = "dbt_handler"
        mock_task_factory.create_dataform_task = "dataform_handler"
        mock_task_factory.create_bq_dts_task = "data_ingestion_handler"
        mock_task_factory.create_orchestration_pipeline_trigger_task = (
            "orchestration_pipeline_handler"
        )

        handlers = registry.get_action_handlers(mock_task_factory)

        self.assertEqual(handlers[actions.PythonScriptActionModel],
                         "python_script_handler")
        self.assertEqual(handlers[actions.PythonVirtualenvActionModel],
                         "python_virtualenv_handler")
        self.assertEqual(handlers[actions.BqOperationActionModel],
                         "bq_operation_handler")
        self.assertEqual(handlers[actions.DataprocOperatorActionModel],
                         "dataproc_operator_handler")
        self.assertEqual(handlers[actions.DBTActionModel], "dbt_handler")
        self.assertEqual(handlers[actions.DataformActionModel],
                         "dataform_handler")
        self.assertEqual(
            handlers[actions.DataIngestionActionModel], "data_ingestion_handler"
        )
        self.assertEqual(
            handlers[actions.OrchestrationPipelineActionModel],
            "orchestration_pipeline_handler",
        )
        self.assertEqual(len(handlers), 8)


if __name__ == "__main__":
    unittest.main()
