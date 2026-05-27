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
from unittest.mock import patch, MagicMock

# Adjust this import to match your actual file path
from orchestration_pipelines_lib.dag_generator.airflow_adapters.common_utils import utils as util


class UtilsTest(unittest.TestCase):

    def test_import_callable_success(self):
        """Tests successful dynamic import of a function."""
        mock_module = MagicMock()

        def dummy_func():
            return "hello"

        setattr(mock_module, "target_func", dummy_func)

        with patch("os.path.exists", return_value=True), \
             patch("importlib.util.spec_from_file_location") as mock_spec_func, \
             patch("importlib.util.module_from_spec", return_value=mock_module), \
             patch.dict(os.environ, {"DAGS_FOLDER": "/tmp/dags"}):

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
                # We mock os.path.exists to return False for the file_not_found case
                with patch("os.path.exists", return_value=not should_mock_missing), \
                     patch("importlib.util.spec_from_file_location"), \
                     patch("importlib.util.module_from_spec", return_value=MagicMock(spec=[])), \
                     patch.dict(os.environ, {"DAGS_FOLDER": "/tmp/dags"}), \
                     self.assertLogs(level='ERROR'):

                    result = util.import_callable(file_path, func_name)
                    self.assertIsNone(result)

    def tearDown(self):
        """Clean up the global sys.modules to prevent side effects."""
        if "test_script_1" in sys.modules:
            del sys.modules["test_script_1"]


if __name__ == '__main__':
    unittest.main()
