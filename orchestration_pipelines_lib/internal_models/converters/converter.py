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
"""Module to convert any supported model to internal model."""

from orchestration_pipelines_lib.internal_models.converters.v1_model_converter import \
    ConverterV1ToInternal
from orchestration_pipelines_lib.utils.file_manager import FileManager
from orchestration_pipelines_models.pipeline_v1_model.protos.orchestration_pipeline_pb2 import \
    OrchestrationPipeline as V1Model
from orchestration_pipelines_lib.internal_models.pipeline import \
    PipelineModel as InternalModel


def convert(model, file_manager: FileManager) -> InternalModel:
    """Converts a supported pipeline model to an internal model.

    Args:
        model: The pipeline model to convert (e.g., V1Model).
        file_manager: The file manager used for file operations and resolution.

    Returns:
        The converted internal pipeline model.

    Raises:
        TypeError: If the provided model type is not supported.
    """
    if isinstance(model, V1Model):
        return ConverterV1ToInternal(file_manager).convert_to_internal_model(
            model)

    raise TypeError(f"Unknown model type: {type(model)}")
