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
"""Provides the VersionedFileManager for version-aware file access."""
import os
from typing import Optional
from google.cloud import storage
from orchestration_pipelines_lib.utils import path_utils
from orchestration_pipelines_lib.utils.file_manager import (
    OrchestrationPipelinesInitializationError, FileManager)


class VersionedFileManager(FileManager):
    """Extends FileManager to handle version-aware path resolution.

    This class resolves relative paths based on a pipeline's versioning
    structure before using the base FileManager to perform file operations.
    """

    def __init__(self,
                 pipeline_id: str,
                 current_version: str,
                 bundle_id: str,
                 local_data_root: str = "/orchestration_pipelines",
                 gcs_client: Optional[storage.Client] = None):
        """Initializes the VersionedFileManager.

        Args:
            pipeline_id: The unique identifier of the current pipeline.
            current_version: The version hash of the current pipeline execution.
            bundle_id: The identifier of the bundle.
            local_data_root: The local mount point for the '/data/' directory.
            gcs_client: An optional pre-configured GCS client.

        Raises:
            OrchestrationPipelinesInitializationError: If pipeline_id, current_version,
                or bundle_id is empty.
        """
        super().__init__(gcs_client)
        if not pipeline_id:
            raise OrchestrationPipelinesInitializationError(
                "pipeline_id cannot be empty.")
        if not current_version:
            raise OrchestrationPipelinesInitializationError(
                "current_version cannot be empty.")
        if not bundle_id:
            raise OrchestrationPipelinesInitializationError(
                "bundle_id cannot be empty.")
        self._pipeline_id = pipeline_id
        self._current_version = current_version
        self.bundle_id = bundle_id
        self._local_data_root = local_data_root

    def resolve_path(self, file_path: str) -> str:
        """Resolves a relative path to an absolute path based on the version.

        - Relative paths are resolved to:
          /orchestration_pipelines/<bundle_id>/versions/<version_id>/<file_path>
        - GCS URIs ('gcs://...') are returned as-is.

        Args:
            file_path: The relative or absolute path to resolve.

        Returns:
            The resolved absolute path or GCS URI, or None if file_path is None.
        """
        if file_path is None:
            return None
        if self._is_gcs_blob(file_path):
            return file_path
        return path_utils.resolve_versioned_path(self._local_data_root,
                                                 self.bundle_id,
                                                 self._current_version,
                                                 file_path)

    def extract_relative_path(self, full_path, _=None):
        """Resolves to a relative path from full path based on the version.

        - Relative paths are resolved from:
          /orchestration_pipelines/<bundle_id>/versions/<version_id>/<file_path>
          to:
          /<bundle_id>/versions/<version_id>/<file_path>

        Args:
            full_path: The full absolute path to extract the relative path from.
            _: Ignored argument (maintained for compatibility).

        Returns:
            The extracted relative path.
        """
        return os.path.relpath(full_path, start=self._local_data_root)

    def read(self, file_path: str) -> str:
        """Resolves a version-aware path and reads its content.

        Args:
            file_path: The file path to read.

        Returns:
            The content of the file.
        """
        resolved_path = self.resolve_path(file_path)
        return super().read(resolved_path)

    def exists(self, file_path: str) -> bool:
        """Resolves a version-aware path and checks if it exists.

        Args:
            file_path: The file path to check.

        Returns:
            True if the file exists, False otherwise.
        """
        resolved_path = self.resolve_path(file_path)
        return super().exists(resolved_path)
