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
"""Provides the VersionResolver for determining a pipeline's version."""
import logging
from typing import Any, Dict, Optional
import yaml

from orchestration_pipelines_lib.utils.file_manager import (
    OrchestrationPipelinesFileReadError,
    OrchestrationPipelinesInvalidPathError,
    FileManager,
)
from orchestration_pipelines_lib.utils import path_utils

logger = logging.getLogger(__name__)


class VersionResolver:
    """Determines and validates the version for a pipeline execution."""

    def __init__(self,
                 dag_id: str,
                 file_manager: FileManager,
                 local_data_root: str = "/declarative_pipelines"):
        """Initializes the VersionResolver.

        Args:
            dag_id: The ID of the pipeline whose version is being resolved.
            file_manager: A generic FileManager for reading the manifest.
            local_data_root: The local mount point for the '/data/' directory.
        """
        self._dag_id = dag_id
        self._file_manager = file_manager
        self._local_data_root = local_data_root

    def _get_version_from_manifest(self) -> str:
        """Retrieves the default version from the pipeline's manifest.

        Returns:
            The default version string from the manifest.

        Raises:
            ValueError: If the manifest cannot be read or parsed.
            TypeError: If the manifest is not a valid YAML dictionary.
            LookupError: If 'default_version' is missing or not a string.
        """
        # Construct the absolute path to the manifest file
        manifest_path = path_utils.get_manifest_path(self._local_data_root,
                                                     self._dag_id)
        logger.info("Reading manifest from: %s", manifest_path)
        try:
            manifest_content = self._file_manager.read(manifest_path)
            manifest = yaml.safe_load(manifest_content)
        except (yaml.YAMLError, OrchestrationPipelinesFileReadError) as e:
            raise ValueError(
                f"Failed to read or parse manifest for DAG '{self._dag_id}' "
                f"at '{manifest_path}'") from e

        if not isinstance(manifest, dict):
            raise TypeError(f"Manifest at '{manifest_path}' is not a valid "
                            "YAML dictionary.")

        default_version = manifest.get("default_version")
        if not isinstance(default_version, str):
            raise LookupError(
                f"'default_version' is missing or not a string in manifest "
                f"for DAG '{self._dag_id}' at '{manifest_path}'")

        logger.info("Found default_version: '%s' for DAG '%s'",
                    default_version, self._dag_id)
        return default_version

    def _validate_version_exists(self, dag_version: str) -> None:
        """Validates that a directory for the specified DAG version exists.

        Args:
            dag_version: The pipeline version to validate.

        Raises:
            OrchestrationPipelinesInvalidPathError: If the version path does not exist.
        """
        # Construct the absolute path to the versioned directory
        version_path = path_utils.get_version_path(self._local_data_root,
                                                   self._dag_id, dag_version)
        logger.info("Validating existence of version path for: %s",
                    version_path)

        if not self._file_manager.exists(version_path):
            raise OrchestrationPipelinesInvalidPathError(
                f"Version '{dag_version}' for pipeline '{self._dag_id}' not "
                f"found. Path does not exist: {version_path}")
        logger.info("Version path exists for: %s", version_path)

    def get_dag_version(self, params: Optional[Dict[str, Any]]) -> str:
        """Determines the pipeline version and validates its existence.

        The version is determined by:
        1.  Checking for 'pipeline_version' in the provided `params`.
        2.  If not in `params`, reading 'default_version' from the manifest.

        Args:
            params: A dictionary of parameters that may contain the pipeline version.

        Returns:
            The resolved and validated pipeline version string.
        """
        dag_version = None
        if params and isinstance(params,
                                 dict) and "pipeline_version" in params:
            dag_version = str(params["pipeline_version"])
            logger.info(
                "Using pipeline_version from params: '%s' for DAG '%s'",
                dag_version, self._dag_id)
        else:
            logger.info(
                "pipeline_version not in params for DAG '%s'. "
                "Reading from manifest.", self._dag_id)
            dag_version = self._get_version_from_manifest()

        self._validate_version_exists(dag_version)

        return dag_version
