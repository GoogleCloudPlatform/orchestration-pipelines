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
"""Unit tests for uncommon utility functions."""

import os
import sys
import unittest
from unittest.mock import MagicMock, patch

import pytest

from orchestration_pipelines_lib.dag_generator.airflow_adapters.common_utils import (  # noqa: E501
    utils as util,
)

TARGET_MODULE = "orchestration_pipelines_lib.dag_generator.airflow_adapters.common_utils.utils"  # noqa: E501

MOCK_REPORT_RUN = f"{TARGET_MODULE}.report_pipeline_run"
MOCK_BASIC_STATUS = f"{TARGET_MODULE}.BasicStatus"
MOCK_TRIGGER_TYPE = f"{TARGET_MODULE}.PipelineRunTriggerType"


@pytest.fixture
def bundle_id():
    """Provides a sample bundle identifier for testing."""
    return "my_bundle_123"


@pytest.fixture
def pipeline_id():
    """Provides a sample pipeline identifier for testing."""
    return "my_pipeline_456"


@pytest.fixture
def mock_dag_run():
    """Creates a mock DAG run with a predefined run type and state."""
    return MagicMock(run_type="manual", state="success")


@pytest.fixture
def valid_context(mock_dag_run):
    """Provides a valid execution context dictionary containing
    a mock DAG run.
    """
    return {"dag_run": mock_dag_run}


class UtilsTest(unittest.TestCase):  # noqa: D101
    def test_import_callable_success(self):
        """Tests successful dynamic import of a function."""
        mock_module = MagicMock()

        def dummy_func():
            return "hello"

        setattr(mock_module, "target_func", dummy_func)  # noqa: B010

        with (
            patch("os.path.exists", return_value=True),
            patch("importlib.util.spec_from_file_location") as mock_spec_func,
            patch("importlib.util.module_from_spec", return_value=mock_module),
            patch.dict(os.environ, {"DAGS_FOLDER": "/tmp/dags"}),
        ):
            mock_spec = MagicMock()
            mock_spec_func.return_value = mock_spec

            result = util.import_callable("test_module.py", "target_func")

            self.assertEqual(result(), "hello")
            self.assertIn("test_script_1", sys.modules)

    def test_import_callable_errors(self):
        """Tests various failure modes for import_callable using subtests."""
        error_scenarios = [
            ("file_not_found", "non_existent.py", "func", True),
            ("attribute_missing", "exists.py", "missing_func", False),
        ]

        for name, file_path, func_name, should_mock_missing in error_scenarios:
            with self.subTest(scenario=name):
                # We mock os.path.exists to return False
                # for the file_not_found case
                with (
                    patch(
                        "os.path.exists", return_value=not should_mock_missing
                    ),
                    patch("importlib.util.spec_from_file_location"),
                    patch(
                        "importlib.util.module_from_spec",
                        return_value=MagicMock(spec=[]),
                    ),
                    patch.dict(os.environ, {"DAGS_FOLDER": "/tmp/dags"}),
                    self.assertLogs(level="ERROR"),
                ):
                    result = util.import_callable(file_path, func_name)
                    self.assertIsNone(result)

    def tearDown(self):
        """Clean up the global sys.modules to prevent side effects."""
        if "test_script_1" in sys.modules:
            del sys.modules["test_script_1"]


@patch(MOCK_REPORT_RUN)
@patch(MOCK_BASIC_STATUS)
@patch(MOCK_TRIGGER_TYPE)
def test_pipeline_run_callback_success(
    mock_trigger_type_class,
    mock_basic_status_class,
    mock_report_run,
    bundle_id,
    pipeline_id,
    valid_context,
    mock_dag_run,
):
    """Tests that the generated callback correctly processes a valid context
    and reports the pipeline run.
    """
    mock_trigger_type_class.from_dag_run_type.return_value = "MOCK_TRIGGER"
    mock_basic_status_class.from_dag_run_state.return_value = "MOCK_STATUS"

    callback = util.pipeline_run_callback(bundle_id, pipeline_id)
    callback(valid_context)

    mock_trigger_type_class.from_dag_run_type.assert_called_once_with(
        mock_dag_run.run_type
    )
    mock_basic_status_class.from_dag_run_state.assert_called_once_with(
        mock_dag_run.state
    )
    mock_report_run.assert_called_once_with(
        bundle_id, pipeline_id, "MOCK_TRIGGER", "MOCK_STATUS"
    )


@patch(MOCK_REPORT_RUN)
def test_pipeline_run_callback_early_return_no_dag_run(
    mock_report_run, bundle_id, pipeline_id
):
    """Tests that the callback returns early without reporting if the context
    lacks a DAG run.
    """
    invalid_context = {}

    callback = util.pipeline_run_callback(bundle_id, pipeline_id)
    callback(invalid_context)  # type: ignore

    mock_report_run.assert_not_called()


@patch(MOCK_REPORT_RUN)
@patch(MOCK_BASIC_STATUS)
@patch(MOCK_TRIGGER_TYPE)
def test_pipeline_run_callback_with_none_bundle_id(
    mock_trigger_type_class,
    mock_basic_status_class,
    mock_report_run,
    pipeline_id,
    valid_context,
):
    """Tests the successful execution and reporting of the callback
    when bundle_id is None.
    """
    mock_trigger_type_class.from_dag_run_type.return_value = "MOCK_TRIGGER"
    mock_basic_status_class.from_dag_run_state.return_value = "MOCK_STATUS"

    callback = util.pipeline_run_callback(None, pipeline_id)
    callback(valid_context)

    mock_report_run.assert_called_once_with(
        None, pipeline_id, "MOCK_TRIGGER", "MOCK_STATUS"
    )


if __name__ == "__main__":
    unittest.main()
