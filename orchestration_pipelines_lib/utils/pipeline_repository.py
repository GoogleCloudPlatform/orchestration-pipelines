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
"""Repository class abstracting data access for orchestration pipelines and
manifests.
"""
from __future__ import annotations

import os

import yaml

from orchestration_pipelines_lib.utils import path_utils
from orchestration_pipelines_lib.utils.file_manager import FileManager
from orchestration_pipelines_models.manifest.manifest import Manifest
from orchestration_pipelines_models.orchestration_pipelines_model import (
    OrchestrationPipelinesModel,
)


class PipelineRepository:
    """Repository class abstracting data access for orchestration pipelines and
    manifests.
    """

    def __init__(self, data_root: str, file_manager: FileManager | None = None):
        """Initializes the repository with a specific data/dags
        root directory.
        """
        self.data_root = data_root
        self.file_manager = file_manager or FileManager()

    def _resolve_with_fallback(self, base_path: str) -> str:
        """Resolves .yml / .yaml files interchangeably, preferring the
        specified one.
        """
        if self.file_manager.exists(base_path):
            return base_path

        base, ext = os.path.splitext(base_path)
        if ext == ".yml" and self.file_manager.exists(base + ".yaml"):
            return base + ".yaml"
        elif ext == ".yaml" and self.file_manager.exists(base + ".yml"):
            return base + ".yml"

        return base_path

    def get_manifest(self, bundle_id: str) -> Manifest:
        """Loads and parses the manifest for a given bundle_id with fallback
        to .yaml.
        """
        manifest_path = path_utils.get_manifest_path(self.data_root, bundle_id)
        resolved_path = self._resolve_with_fallback(manifest_path)

        manifest_content = self.file_manager.read(resolved_path)
        parsed_manifest = yaml.safe_load(manifest_content)
        return Manifest.from_dict(parsed_manifest)

    def get_versioned_pipeline(
        self,
        bundle_id: str,
        pipeline_id: str,
        version_id: str,
        file_manager: FileManager | None = None,
    ) -> OrchestrationPipelinesModel:
        """Loads a versioned pipeline with fallback to .yaml."""
        file_manager = file_manager or self.file_manager
        pipeline_file = f"{pipeline_id}.yml"
        versioned_path = path_utils.resolve_versioned_path(
            self.data_root, bundle_id, version_id, pipeline_file
        )
        resolved_path = self._resolve_with_fallback(versioned_path)

        definition_content = file_manager.read(resolved_path)
        pipeline_definition = yaml.safe_load(definition_content)
        parsed_pipeline = OrchestrationPipelinesModel.build(pipeline_definition)
        return parsed_pipeline

    def get_pipeline(
        self, pipeline_path: str, file_manager: FileManager | None = None
    ) -> OrchestrationPipelinesModel:
        """Loads an unversioned pipeline definition directly from path with
        fallback.
        """
        file_manager = file_manager or self.file_manager

        resolved_path = self._resolve_with_fallback(pipeline_path)

        definition_content = file_manager.read(resolved_path)
        pipeline_definition = yaml.safe_load(definition_content)
        parsed_pipeline = OrchestrationPipelinesModel.build(pipeline_definition)
        return parsed_pipeline
