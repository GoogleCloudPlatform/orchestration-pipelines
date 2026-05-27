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
"""Unit tests for the VersionedFileManager class."""
import unittest
from unittest.mock import patch

from orchestration_pipelines_lib.utils.file_manager import \
    OrchestrationPipelinesInitializationError
from orchestration_pipelines_lib.utils.versioned_file_manager import \
    VersionedFileManager


class TestVersionedFileManager(unittest.TestCase):
    """Test suite for the VersionedFileManager."""

    def setUp(self):
        """Set up the test case."""
        self.pipeline_id = "test_pipeline"
        self.current_version = "v1.0.0"
        self.local_data_root = "/data"
        self.bundle_id = "test_bundle"
        self.versioned_fm = VersionedFileManager(
            pipeline_id=self.pipeline_id,
            current_version=self.current_version,
            local_data_root=self.local_data_root,
            bundle_id=self.bundle_id)

    # pylint: disable=protected-access
    def test_initialization_success(self):
        """Tests successful initialization of VersionedFileManager."""
        self.assertEqual(self.versioned_fm._pipeline_id, self.pipeline_id)
        self.assertEqual(self.versioned_fm._current_version,
                         self.current_version)
        self.assertEqual(self.versioned_fm._local_data_root,
                         self.local_data_root)
        self.assertEqual(self.versioned_fm.bundle_id, self.bundle_id)

    def test_initialization_failure_no_pipeline_id(self):
        """Tests that initialization fails without a pipeline_id."""
        with self.assertRaises(OrchestrationPipelinesInitializationError):
            VersionedFileManager(pipeline_id="",
                                 current_version=self.current_version,
                                 bundle_id=self.bundle_id)

    def test_initialization_failure_no_current_version(self):
        """Tests that initialization fails without a current_version."""
        with self.assertRaises(OrchestrationPipelinesInitializationError):
            VersionedFileManager(pipeline_id=self.pipeline_id,
                                 current_version="",
                                 bundle_id=self.bundle_id)

    def test_initialization_failure_no_bundle_id(self):
        """Tests that initialization fails without a bundle_id."""
        with self.assertRaises(OrchestrationPipelinesInitializationError):
            VersionedFileManager(pipeline_id=self.pipeline_id,
                                 current_version=self.current_version,
                                 bundle_id="")

    @patch(
        "orchestration_pipelines_lib.utils.path_utils.resolve_versioned_path")
    def test_resolve_path_local(self, mock_resolve_versioned_path):
        """Tests that local relative paths are resolved correctly."""
        local_path = "some/file.txt"
        expected_path = "/data/test_bundle/versions/v1.0.0/some/file.txt"
        mock_resolve_versioned_path.return_value = expected_path

        resolved_path = self.versioned_fm.resolve_path(local_path)

        self.assertEqual(resolved_path, expected_path)
        mock_resolve_versioned_path.assert_called_once_with(
            self.local_data_root, self.bundle_id, self.current_version,
            local_path)

    def test_extract_relative_path(self):
        """Tests extract_relative_path with valid root and full path."""
        full_path = "/data/test_bundle/versions/a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p/some/file.txt"
        expected_path = "test_bundle/versions/a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p/some/file.txt"

        relative_path = self.versioned_fm.extract_relative_path(full_path)
        self.assertEqual(relative_path, expected_path)

    def test_resolve_path_gcs(self):
        """Tests that GCS paths are returned unresolved."""
        gcs_path = "gs://example-bucket/path/to/file.txt"
        self.assertEqual(self.versioned_fm.resolve_path(gcs_path), gcs_path)

    @patch("orchestration_pipelines_lib.utils.file_manager.FileManager.read")
    @patch(
        "orchestration_pipelines_lib.utils.versioned_file_manager.VersionedFileManager.resolve_path"
    )
    def test_read_uses_resolved_path(self, mock_resolve_path, mock_super_read):
        """Tests that read() calls the base method with a resolved path."""
        mock_resolve_path.return_value = "resolved/path/to/my_file.txt"
        self.versioned_fm.read("my_file.txt")
        mock_super_read.assert_called_once_with("resolved/path/to/my_file.txt")

    @patch("orchestration_pipelines_lib.utils.file_manager.FileManager.exists")
    @patch(
        "orchestration_pipelines_lib.utils.versioned_file_manager.VersionedFileManager.resolve_path"
    )
    def test_exists_uses_resolved_path(self, mock_resolve_path,
                                       mock_super_exists):
        """Tests that exists() calls the base method with a resolved path."""
        mock_resolve_path.return_value = "resolved/path/to/my_file.txt"
        self.versioned_fm.exists("my_file.txt")
        mock_super_exists.assert_called_once_with(
            "resolved/path/to/my_file.txt")
