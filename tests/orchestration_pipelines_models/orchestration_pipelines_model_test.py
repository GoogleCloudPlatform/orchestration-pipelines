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
"""Unit tests for the OrchestrationPipelinesModel wrapper."""

import unittest
from unittest.mock import MagicMock, patch

from orchestration_pipelines_models.orchestration_pipelines_model import (
    OrchestrationPipelinesModel, )
from orchestration_pipelines_models.pipeline_v1_model.protos.orchestration_pipeline_pb2 import (
    OrchestrationPipeline, )


class TestOrchestrationPipelinesModel(unittest.TestCase):
    """Test suite for the OrchestrationPipelinesModel."""

    @patch(
        "orchestration_pipelines_models.orchestration_pipelines_model.OrchestrationPipelineBuilder"
    )
    def test_build_v1_model_success(self, mock_builder):
        """Tests successful build of a v1.0 model."""
        mock_pipeline = MagicMock(spec=OrchestrationPipeline)
        mock_builder.build.return_value = mock_pipeline
        pipeline_def = {"modelVersion": "1.0", "some_key": "some_value"}

        result = OrchestrationPipelinesModel.build(pipeline_def)

        self.assertEqual(result, mock_pipeline)
        mock_builder.build.assert_called_once_with(pipeline_def)

    @patch(
        "orchestration_pipelines_models.orchestration_pipelines_model.OrchestrationPipelineBuilder"
    )
    def test_build_v1_model_snake_case_success(self, mock_builder):
        """Tests successful build of a v1.0 model using snake_case."""
        mock_pipeline = MagicMock(spec=OrchestrationPipeline)
        mock_builder.build.return_value = mock_pipeline
        pipeline_def = {"model_version": "1.0", "some_key": "some_value"}

        result = OrchestrationPipelinesModel.build(pipeline_def)

        self.assertEqual(result, mock_pipeline)
        mock_builder.build.assert_called_once_with(pipeline_def)

    @patch(
        "orchestration_pipelines_models.orchestration_pipelines_model.OrchestrationPipelineBuilder"
    )
    def test_build_legacy_v2_model_camel_case_maps_to_v1(self, mock_builder):
        """Tests that legacy 'v2' modelVersion is correctly mapped to '1.0'."""
        pipeline_def = {"modelVersion": "v2", "some_key": "some_value"}

        OrchestrationPipelinesModel.build(pipeline_def)

        expected_builder_dict = pipeline_def.copy()
        expected_builder_dict["model_version"] = "1.0"
        mock_builder.build.assert_called_once_with(expected_builder_dict)

    @patch(
        "orchestration_pipelines_models.orchestration_pipelines_model.OrchestrationPipelineBuilder"
    )
    def test_build_legacy_v2_model_snake_case_maps_to_v1(self, mock_builder):
        """Tests that legacy 'v2' model_version is correctly mapped to '1.0'."""
        pipeline_def = {"model_version": "v2", "some_key": "some_value"}

        OrchestrationPipelinesModel.build(pipeline_def)

        expected_builder_dict = {
            "model_version": "1.0",
            "some_key": "some_value"
        }
        mock_builder.build.assert_called_once_with(expected_builder_dict)

    def test_build_invalid_input_type_raises_error(self):
        """Tests that a non-dict input raises a TypeError."""
        with self.assertRaisesRegex(TypeError, "Input must be a dictionary"):
            OrchestrationPipelinesModel.build("not a dict")

    def test_build_missing_model_version_raises_error(self):
        """Tests that a missing modelVersion raises a ValueError."""
        with self.assertRaisesRegex(ValueError,
                                    "Invalid or missing 'model_version'"):
            OrchestrationPipelinesModel.build({"some_key": "some_value"})

    def test_build_unsupported_model_version_raises_error(self):
        """Tests that an unsupported modelVersion raises a ValueError."""
        with self.assertRaisesRegex(ValueError,
                                    "Invalid or missing 'model_version'"):
            OrchestrationPipelinesModel.build({"modelVersion": "99.0"})


if __name__ == "__main__":
    unittest.main()
