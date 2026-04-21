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
"""Provides utility functions for path generation."""
import os


def get_manifest_path(local_data_root: str, bundle_id: str) -> str:
    """Constructs the absolute path to a pipeline's manifest.yml file.

    Args:
        local_data_root: The local mount point for the '/data/' directory.
        bundle_id: The ID of the bundle.

    Returns:
        The absolute path to the manifest.yml.
    """
    return os.path.join(local_data_root, bundle_id, "manifest.yml")


def get_version_path(local_data_root: str, dag_id: str,
                     dag_version: str) -> str:
    """Constructs the path to a specific version directory of a pipeline.

    Args:
        local_data_root: The local mount point for the '/data/' directory.
        dag_id: The ID of the pipeline.
        dag_version: The version string of the pipeline.

    Returns:
        The absolute path to the version directory.
    """
    return os.path.join(local_data_root, dag_id, "versions", dag_version)


def resolve_versioned_path(local_data_root: str, bundle_id: str,
                           version_id: str, file_path: str) -> str:
    """Resolves a relative path to an absolute, versioned path.

    Path is resolved to:
    <local_data_root>/<bundle_id>/versions/<version_id>/<file_path>

    Args:
        local_data_root: The local mount point for the '/data/' directory.
        bundle_id: The ID of the bundle.
        version_id: The version ID.
        file_path: The relative file path to resolve.

    Returns:
        The normalized absolute versioned path.
    """
    return os.path.normpath(
        os.path.join(local_data_root, bundle_id, "versions", version_id,
                     file_path))
