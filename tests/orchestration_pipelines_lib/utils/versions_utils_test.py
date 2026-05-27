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
"""Unit tests for the versions_utils module."""

import unittest
from unittest.mock import MagicMock, patch
from orchestration_pipelines_models.manifest.manifest import Manifest
from orchestration_pipelines_lib.utils import versions_utils


class TestBundleVersionManipulation(unittest.TestCase):
    """Test suite for versions manipulation logic."""

    def setUp(self):
        """Set up the test case."""
        self.pipeline_id = "test_pipeline"
        self.bundle_id = "test_bundle"

    @patch(
        "orchestration_pipelines_lib.dag_generator.core.get_previous_default_versions"
    )
    @patch(
        "orchestration_pipelines_lib.dag_generator.core.get_actively_running_versions"
    )
    def test_get_versions_to_parse(self, mock_get_actively_running_versions, mock_get_previous_default_versions):
        """Tests that default and actively running versions are unioned correctly."""
        # Setup mock Manifest
        mock_manifest = MagicMock(spec=Manifest)
        mock_manifest.get_default_version.return_value = "v1.0.0"
        mock_manifest.get_bundle_id.return_value = self.bundle_id
        mock_get_actively_running_versions.return_value = {"v1.1.0", "v2.0.0"}
        mock_get_previous_default_versions.return_value = {"v1.0.0"}

        # Expected union of default version + active versions
        expected_versions = {"v1.0.0", "v1.1.0", "v2.0.0"}

        result = versions_utils.get_versions_to_parse(self.pipeline_id,
                                                      mock_manifest)

        self.assertEqual(result, expected_versions)


if __name__ == "__main__":
    unittest.main()
