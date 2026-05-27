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
"""Unit tests for the manifest.py module."""

import unittest

from orchestration_pipelines_models.manifest.manifest import (
    VersionDeploymentInfo, Manifest)
from orchestration_pipelines_models.manifest.manifest_pb2 import (
    Manifest as ManifestModel, DeploymentOrigination)


class TestManifest(unittest.TestCase):
    """Test suite for the Manifest wrapper class."""

    def setUp(self):
        """Set up a sample manifest object for testing."""
        self.manifest_dict = {
            "bundle":
            "test-bundle",
            "default_version":
            "v2",
            "paused_pipelines": ["pipeline-a"],
            "versions_history": [
                {
                    "timestamp": "2026-01-01T00:00:00Z",
                    "version_id": "v1",
                    "pipelines": ["pipeline-a", "pipeline-b"],
                    "metadata": {
                        "origination": "GIT_CI_CD",
                        "deployment_details": {
                            "git_repo": "repo/v1",
                            "git_branch": "main",
                            "commit_sha": "abc",
                        },
                    },
                },
                {
                    "timestamp": "2026-01-02T00:00:00Z",
                    "version_id": "v2",
                    "pipelines": ["pipeline-a", "pipeline-c"],
                    "metadata": {
                        "origination": "LOCAL_DEPLOY",
                    },
                },
                # Add a duplicate version with an older timestamp to test sorting
                {
                    "timestamp": "2026-01-01T12:00:00Z",
                    "version_id": "v2",
                    "pipelines": ["pipeline-a", "pipeline-d"],
                    "metadata": {
                        "origination": "GIT_CI_CD",
                        "deployment_details": {
                            "git_repo": "repo/v2-old",
                            "git_branch": "develop-old",
                            "commit_sha": "ghi",
                        },
                    },
                },
            ],
        }
        self.manifest_wrapper = Manifest.from_dict(self.manifest_dict)

    def test_from_dict(self):
        """Tests object creation from a dictionary."""
        self.assertIsNotNone(self.manifest_wrapper)
        self.assertIsInstance(self.manifest_wrapper.manifest, ManifestModel)
        self.assertEqual(self.manifest_wrapper.manifest.bundle, "test-bundle")

        # Test with None and empty dict
        self.assertIsNone(Manifest.from_dict(None))
        manifest_from_empty = Manifest.from_dict({})
        self.assertIsNotNone(manifest_from_empty)
        self.assertEqual(manifest_from_empty.get_bundle_id(), "")

    def test_init_with_invalid_type(self):
        """Tests that __init__ raises a TypeError for incorrect input types."""
        with self.assertRaises(TypeError):
            Manifest({})  # Expects a ManifestModel protobuf object

    def test_get_bundle_id(self):
        """Tests the get_bundle_id method."""
        self.assertEqual(self.manifest_wrapper.get_bundle_id(), "test-bundle")

    def test_get_default_version(self):
        """Tests the get_default_version method."""
        self.assertEqual(self.manifest_wrapper.get_default_version(), "v2")

    def test_is_current(self):
        """Tests the is_current method."""
        self.assertTrue(self.manifest_wrapper.is_current("v2"))
        self.assertFalse(self.manifest_wrapper.is_current("v1"))
        self.assertFalse(self.manifest_wrapper.is_current("v3"))
        self.assertFalse(self.manifest_wrapper.is_current(None))
        self.assertFalse(self.manifest_wrapper.is_current(""))

    def test_is_paused(self):
        """Tests the is_paused method."""
        self.assertTrue(self.manifest_wrapper.is_paused("pipeline-a"))
        self.assertFalse(self.manifest_wrapper.is_paused("pipeline-b"))

    def test_is_pipeline_in_bundle(self):
        """Tests the is_pipeline_in_bundle method, ensuring it uses the latest version entry."""
        # The latest "v2" entry contains "pipeline-c", not "pipeline-d"
        self.assertTrue(
            self.manifest_wrapper.is_pipeline_in_bundle("v2", "pipeline-c"))
        self.assertFalse(
            self.manifest_wrapper.is_pipeline_in_bundle("v2", "pipeline-d"))

        # Test for v1
        self.assertTrue(
            self.manifest_wrapper.is_pipeline_in_bundle("v1", "pipeline-b"))

        # Test for non-existent version
        self.assertFalse(
            self.manifest_wrapper.is_pipeline_in_bundle("v3", "pipeline-a"))

    def test_get_deployment_details(self):
        """Tests get_deployment_details, ensuring it returns details from the latest version entry."""
        details = self.manifest_wrapper.get_deployment_details("v2")
        self.assertIsInstance(details, VersionDeploymentInfo)
        self.assertEqual(details.origination,
                         DeploymentOrigination.LOCAL_DEPLOY)
        self.assertIsNone(details.git_repo)
        self.assertIsNone(details.git_branch)
        self.assertIsNone(details.commit_sha)

    def test_get_deployment_details_non_existent_version(self):
        """Tests get_deployment_details for a version that does not exist."""
        self.assertIsNone(self.manifest_wrapper.get_deployment_details("v3"))

    def test_get_deployment_details_no_metadata(self):
        """Tests get_deployment_details when a version history entry has no metadata."""
        manifest_dict = {"versions_history": [{"version_id": "v1"}]}
        manifest_wrapper = Manifest.from_dict(manifest_dict)
        self.assertIsNone(manifest_wrapper.get_deployment_details("v1"))

    def test_get_deployment_details_no_deployment_details_in_metadata(self):
        """Tests get_deployment_details when metadata is missing deployment details."""
        manifest_dict = {
            "versions_history": [{
                "version_id": "v1",
                "metadata": {
                    "origination": "GIT_CI_CD"
                },
            }]
        }
        manifest_wrapper = Manifest.from_dict(manifest_dict)
        details = manifest_wrapper.get_deployment_details("v1")
        self.assertIsInstance(details, VersionDeploymentInfo)
        self.assertEqual(details.origination, DeploymentOrigination.GIT_CI_CD)
        self.assertIsNone(details.git_repo)

    def test_get_latest_version_history_entry(self):
        """Tests the private _get_latest_version_history_entry method."""
        # Testing private methods is sometimes necessary for complex internal logic.
        latest_v2 = self.manifest_wrapper._get_latest_version_history_entry(
            "v2")
        self.assertIsNotNone(latest_v2)
        self.assertEqual(latest_v2.timestamp, "2026-01-02T00:00:00Z")
        self.assertEqual(latest_v2.metadata.origination,
                         DeploymentOrigination.LOCAL_DEPLOY)

    def test_no_versions_history(self):
        """Tests behavior when the manifest has no versions_history."""
        manifest_wrapper = Manifest.from_dict({
            "bundle": "b",
            "default_version": "v1"
        })
        self.assertIsNone(manifest_wrapper.get_deployment_details("v1"))
        self.assertFalse(manifest_wrapper.is_pipeline_in_bundle("v1", "p1"))


if __name__ == "__main__":
    unittest.main()
