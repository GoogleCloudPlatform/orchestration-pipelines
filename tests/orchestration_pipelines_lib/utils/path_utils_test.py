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
"""Unit tests for the path_utils module."""
import unittest

from orchestration_pipelines_lib.utils import path_utils


class TestPathUtils(unittest.TestCase):
    """Test suite for path utility functions."""

    def setUp(self):
        """Set up common variables for tests."""
        self.local_data_root = "/data"
        self.dag_id = "my_test_dag"
        self.dag_version = "v1.2.3"

    def test_get_manifest_path(self):
        """Tests that the manifest path is constructed correctly."""
        expected_path = "/data/my_test_dag/manifest.yml"
        actual_path = path_utils.get_manifest_path(self.local_data_root,
                                                   self.dag_id)
        self.assertEqual(actual_path, expected_path)

    def test_get_version_path(self):
        """Tests that the version-specific path is constructed correctly."""
        expected_path = "/data/my_test_dag/versions/v1.2.3"
        actual_path = path_utils.get_version_path(self.local_data_root,
                                                  self.dag_id,
                                                  self.dag_version)
        self.assertEqual(actual_path, expected_path)

    def test_resolve_versioned_path(self):
        """Tests that a versioned file path is resolved correctly."""
        file_path = "queries/my_query.sql"
        expected_path = "/data/my_test_dag/versions/v1.2.3/queries/my_query.sql"
        actual_path = path_utils.resolve_versioned_path(
            self.local_data_root, self.dag_id, self.dag_version, file_path)
        self.assertEqual(actual_path, expected_path)


if __name__ == "__main__":
    unittest.main()
