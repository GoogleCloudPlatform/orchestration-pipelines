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
"""Unit tests for the PipelineRepository class."""

import unittest
from unittest.mock import MagicMock, patch

from orchestration_pipelines_lib.utils.pipeline_repository import PipelineRepository
from orchestration_pipelines_lib.utils.file_manager import FileManager


class TestPipelineRepository(unittest.TestCase):
    """Test suite for the PipelineRepository."""

    def setUp(self):
        self.data_root = "/data"
        self.mock_file_manager = MagicMock(spec=FileManager)
        self.repository = PipelineRepository(
            data_root=self.data_root, file_manager=self.mock_file_manager
        )

    def test_resolve_with_fallback_path_exists(self):
        """Tests that _resolve_with_fallback returns the path directly if it exists."""
        base_path = "/data/bundle/manifest.yml"
        self.mock_file_manager.exists.return_value = True

        resolved = self.repository._resolve_with_fallback(base_path)

        self.assertEqual(resolved, base_path)
        self.mock_file_manager.exists.assert_called_once_with(base_path)

    def test_resolve_with_fallback_yml_to_yaml(self):
        """Tests fallback from .yml to .yaml when .yml doesn't exist but .yaml does."""
        base_path = "/data/bundle/manifest.yml"
        expected_fallback = "/data/bundle/manifest.yaml"

        def exists_side_effect(path):
            return path == expected_fallback

        self.mock_file_manager.exists.side_effect = exists_side_effect

        resolved = self.repository._resolve_with_fallback(base_path)

        self.assertEqual(resolved, expected_fallback)

    def test_resolve_with_fallback_yaml_to_yml(self):
        """Tests fallback from .yaml to .yml when .yaml doesn't exist but .yml does."""
        base_path = "/data/bundle/manifest.yaml"
        expected_fallback = "/data/bundle/manifest.yml"

        def exists_side_effect(path):
            return path == expected_fallback

        self.mock_file_manager.exists.side_effect = exists_side_effect

        resolved = self.repository._resolve_with_fallback(base_path)

        self.assertEqual(resolved, expected_fallback)

    def test_resolve_with_fallback_none_exists(self):
        """Tests that the original path is returned if neither exists."""
        base_path = "/data/bundle/manifest.yml"
        self.mock_file_manager.exists.return_value = False

        resolved = self.repository._resolve_with_fallback(base_path)

        self.assertEqual(resolved, base_path)

    @patch("orchestration_pipelines_models.manifest.manifest.Manifest.from_dict")
    @patch("orchestration_pipelines_lib.utils.path_utils.get_manifest_path")
    def test_get_manifest(self, mock_get_manifest_path, mock_from_dict):
        """Tests get_manifest reads and parses manifest with fallback resolution."""
        bundle_id = "my-bundle"
        manifest_path = "/data/my-bundle/manifest.yml"
        mock_get_manifest_path.return_value = manifest_path

        # Mock fallback resolution
        self.mock_file_manager.exists.return_value = True
        self.mock_file_manager.read.return_value = "bundle: my-bundle"
        mock_manifest_instance = MagicMock()
        mock_from_dict.return_value = mock_manifest_instance

        manifest = self.repository.get_manifest(bundle_id)

        mock_get_manifest_path.assert_called_once_with(self.data_root, bundle_id)
        self.mock_file_manager.read.assert_called_once_with(manifest_path)
        mock_from_dict.assert_called_once_with({"bundle": "my-bundle"})
        self.assertEqual(manifest, mock_manifest_instance)

    @patch("orchestration_pipelines_models.orchestration_pipelines_model.OrchestrationPipelinesModel.build")
    @patch("orchestration_pipelines_lib.utils.path_utils.resolve_versioned_path")
    def test_get_versioned_pipeline(
        self, mock_resolve_versioned_path, mock_build
    ):
        """Tests get_versioned_pipeline resolves versioned path and builds model."""
        bundle_id = "my-bundle"
        pipeline_id = "my-pipeline"
        version_id = "v1"
        versioned_path = "/data/my-bundle/versions/v1/my-pipeline.yml"

        mock_resolve_versioned_path.return_value = versioned_path
        self.mock_file_manager.exists.return_value = True
        self.mock_file_manager.read.return_value = "pipeline_id: my-pipeline"

        mock_parsed_model = MagicMock()
        mock_build.return_value = mock_parsed_model

        result = self.repository.get_versioned_pipeline(
            bundle_id=bundle_id, pipeline_id=pipeline_id, version_id=version_id
        )

        mock_resolve_versioned_path.assert_called_once_with(
            self.data_root, bundle_id, version_id, "my-pipeline.yml"
        )
        self.mock_file_manager.read.assert_called_once_with(versioned_path)
        mock_build.assert_called_once_with({"pipeline_id": "my-pipeline"})
        self.assertEqual(result, mock_parsed_model)

    @patch("orchestration_pipelines_models.orchestration_pipelines_model.OrchestrationPipelinesModel.build")
    def test_get_pipeline(self, mock_build):
        """Tests get_pipeline directly reads the pipeline file without fallback."""
        pipeline_path = "/custom/path/pipeline.yml"
        self.mock_file_manager.read.return_value = "pipeline_id: custom-pipeline"

        mock_parsed_model = MagicMock()
        mock_build.return_value = mock_parsed_model

        result = self.repository.get_pipeline(pipeline_path=pipeline_path)

        self.mock_file_manager.read.assert_called_once_with(pipeline_path)
        mock_build.assert_called_once_with({"pipeline_id": "custom-pipeline"})
        self.assertEqual(result, mock_parsed_model)