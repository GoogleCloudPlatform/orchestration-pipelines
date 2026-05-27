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
"""Unit tests for the VersionResolver class."""
import unittest
from unittest.mock import MagicMock, patch

from orchestration_pipelines_lib.utils.file_manager import (
    OrchestrationPipelinesFileReadError,
    OrchestrationPipelinesInvalidPathError)
from orchestration_pipelines_lib.utils.version_resolver import VersionResolver


class TestVersionResolver(unittest.TestCase):
    """Test suite for the VersionResolver."""

    def setUp(self):
        """Set up the test case."""
        self.dag_id = "test_dag"
        self.local_data_root = "/data"
        self.mock_file_manager = MagicMock()
        self.resolver = VersionResolver(dag_id=self.dag_id,
                                        file_manager=self.mock_file_manager,
                                        local_data_root=self.local_data_root)

    @patch("orchestration_pipelines_lib.utils.path_utils.get_manifest_path")
    def test_get_version_from_manifest_success(self, mock_get_manifest_path):
        """Tests successfully reading the version from the manifest."""
        manifest_path = "/data/test_dag/manifest.yml"
        manifest_content = "default_version: v1.0.0"
        mock_get_manifest_path.return_value = manifest_path
        self.mock_file_manager.read.return_value = manifest_content

        # pylint: disable=protected-access
        version = self.resolver._get_version_from_manifest()

        self.assertEqual(version, "v1.0.0")
        mock_get_manifest_path.assert_called_once_with(self.local_data_root,
                                                       self.dag_id)
        self.mock_file_manager.read.assert_called_once_with(manifest_path)

    def test_get_version_from_manifest_read_error(self):
        """Tests handling of a file read error."""
        self.mock_file_manager.read.side_effect = OrchestrationPipelinesFileReadError
        with self.assertRaises(ValueError):
            # pylint: disable=protected-access
            self.resolver._get_version_from_manifest()

    def test_get_version_from_manifest_yaml_error(self):
        """Tests handling of invalid YAML content."""
        self.mock_file_manager.read.return_value = "default_version: v1.0.0:"
        with self.assertRaises(ValueError):
            # pylint: disable=protected-access
            self.resolver._get_version_from_manifest()

    def test_get_version_from_manifest_not_a_dict(self):
        """Tests that an error is raised if the manifest is not a dict."""
        self.mock_file_manager.read.return_value = "- item1\n- item2"
        with self.assertRaises(TypeError):
            # pylint: disable=protected-access
            self.resolver._get_version_from_manifest()

    def test_get_version_from_manifest_missing_key(self):
        """Tests that an error is raised if default_version is missing."""
        self.mock_file_manager.read.return_value = "other_key: value"
        with self.assertRaises(LookupError):
            # pylint: disable=protected-access
            self.resolver._get_version_from_manifest()

    def test_get_version_from_manifest_key_not_string(self):
        """Tests that an error is raised if default_version is not a string."""
        self.mock_file_manager.read.return_value = "default_version: 123"
        with self.assertRaises(LookupError):
            # pylint: disable=protected-access
            self.resolver._get_version_from_manifest()

    @patch("orchestration_pipelines_lib.utils.path_utils.get_version_path")
    def test_validate_version_exists_success(self, mock_get_version_path):
        """Tests successful validation of an existing version path."""
        version_path = "/data/test_dag/versions/v1.0.0"
        mock_get_version_path.return_value = version_path
        self.mock_file_manager.exists.return_value = True

        # pylint: disable=protected-access
        self.resolver._validate_version_exists("v1.0.0")

        mock_get_version_path.assert_called_once_with(self.local_data_root,
                                                      self.dag_id, "v1.0.0")
        self.mock_file_manager.exists.assert_called_once_with(version_path)

    def test_validate_version_exists_failure(self):
        """Tests that an error is raised for a non-existent version path."""
        self.mock_file_manager.exists.return_value = False
        with self.assertRaises(OrchestrationPipelinesInvalidPathError):
            # pylint: disable=protected-access
            self.resolver._validate_version_exists("v1.0.0")

    @patch.object(VersionResolver, "_validate_version_exists")
    def test_get_dag_version_from_params(self, mock_validate):
        """Tests getting the DAG version from runtime parameters."""
        params = {"pipeline_version": "param_version"}
        version = self.resolver.get_dag_version(params)
        self.assertEqual(version, "param_version")
        mock_validate.assert_called_once_with("param_version")

    @patch.object(VersionResolver, "_validate_version_exists")
    def test_get_dag_version_from_params_as_int(self, mock_validate):
        """Tests getting the DAG version from params when it's an int."""
        params = {"pipeline_version": 123}
        version = self.resolver.get_dag_version(params)
        self.assertEqual(version, "123")
        mock_validate.assert_called_once_with("123")

    @patch.object(VersionResolver, "_validate_version_exists")
    @patch.object(VersionResolver, "_get_version_from_manifest")
    def test_get_dag_version_from_manifest(self, mock_get_from_manifest,
                                           mock_validate):
        """Tests falling back to the manifest for the DAG version."""
        mock_get_from_manifest.return_value = "manifest_version"

        # Test with no params
        version1 = self.resolver.get_dag_version(None)
        self.assertEqual(version1, "manifest_version")
        mock_validate.assert_called_with("manifest_version")

        # Test with params but no pipeline_version key
        version2 = self.resolver.get_dag_version({"other_param": "value"})
        self.assertEqual(version2, "manifest_version")
        mock_validate.assert_called_with("manifest_version")

        self.assertEqual(mock_get_from_manifest.call_count, 2)
        self.assertEqual(mock_validate.call_count, 2)


if __name__ == "__main__":
    unittest.main()
