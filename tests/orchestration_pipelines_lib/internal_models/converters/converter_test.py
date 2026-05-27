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
"""Unit tests for the converter module."""

import unittest
from unittest.mock import MagicMock, patch

from orchestration_pipelines_lib.internal_models.converters import converter
from orchestration_pipelines_lib.internal_models.pipeline import \
    PipelineModel as InternalModel
from orchestration_pipelines_lib.utils.file_manager import FileManager
from orchestration_pipelines_models.pipeline_v1_model.protos.orchestration_pipeline_pb2 import \
    OrchestrationPipeline as V1Model


class TestConverter(unittest.TestCase):
    """Test suite for the main converter function."""

    def setUp(self):
        """Set up mocks for testing."""
        self.mock_file_manager = MagicMock(spec=FileManager)
        self.mock_v1_model = MagicMock(spec=V1Model)
        self.mock_internal_model = MagicMock(spec=InternalModel)

    @patch(
        "orchestration_pipelines_lib.internal_models.converters.converter.ConverterV1ToInternal"
    )
    def test_convert_with_v1_model(self, mock_v1_converter_class):
        """Tests that the v1 converter is used for a v1 model."""
        mock_v1_instance = mock_v1_converter_class.return_value
        mock_v1_instance.convert_to_internal_model.return_value = self.mock_internal_model

        result = converter.convert(self.mock_v1_model, self.mock_file_manager)

        mock_v1_converter_class.assert_called_once_with(self.mock_file_manager)
        mock_v1_instance.convert_to_internal_model.assert_called_once_with(
            self.mock_v1_model)
        self.assertEqual(result, self.mock_internal_model)

    def test_convert_with_unknown_model_type_raises_error(self):
        """Tests that a TypeError is raised for an unknown model type."""
        unknown_model = "this is not a model"
        with self.assertRaisesRegex(TypeError, "Unknown model type"):
            converter.convert(unknown_model, self.mock_file_manager)


if __name__ == "__main__":
    unittest.main()
