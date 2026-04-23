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
"""
Provides the FileManager utility for file access.

This module defines a generic FileManager for reading files and checking for
their existence. It operates on absolute local paths or full GCS URIs and has
no knowledge of versioning. It also defines a set of custom exceptions for
handling various file-related errors.
"""
import os
from typing import Optional, Any


_GCS_CLIENT = None

def _get_gcs_client():
    global _GCS_CLIENT
    if _GCS_CLIENT is None:
        from google.cloud import storage
        _GCS_CLIENT = storage.Client()
    return _GCS_CLIENT



class OrchestrationPipelinesFileReadError(Exception):
    """Base exception for file reading issues within the library."""
    pass


class OrchestrationPipelinesFileNotFoundError(
        OrchestrationPipelinesFileReadError):
    """Indicates that the resolved file or location does not exist."""
    pass


class OrchestrationPipelinesInitializationError(Exception):
    """Error while initializing a class from the orchestration pipelines library."""
    pass


class OrchestrationPipelinesInvalidPathError(ValueError):
    """Indicates an invalid file path format."""
    pass


class FileManager:
    """Manages file existence checks and content reading from absolute paths.

    This class operates on absolute local paths or full GCS URIs and has no
    knowledge of versioning or relative path structures.
    """

    def __init__(self, gcs_client: Optional[Any] = None):
        """Initializes the FileManager.

        Args:
            gcs_client: An optional pre-configured GCS client instance.
        """
        self._gcs_client = gcs_client or _get_gcs_client()

    def _get_gcs_client(self):
        """Lazily initializes the GCS client."""
        if self._gcs_client is None:
            from google.cloud import storage
            self._gcs_client = storage.Client()
        return self._gcs_client

    def resolve_path(self, file_path: str) -> str:
        """Resolves a file path.

        Args:
            file_path: The file path to resolve.

        Returns:
            The resolved file path.
        """
        return file_path

    def extract_relative_path(self,
                              full_path: str,
                              local_data_root: str = "/") -> str:
        """Returns the relative path from full_path without local_data_root.

        Path is resolved from:
        <local_data_root>/<file_path>
        to:
        <file_path>

        Args:
            full_path: The full path to extract the relative path from.
            local_data_root: The root directory to resolve the relative path from.

        Returns:
            The extracted relative path.
        """
        return os.path.relpath(full_path, start=local_data_root)

    def _construct_local_path(self, relative_path: str) -> str:
        """Constructs a local path based on the DAGS_FOLDER environment variable.

        Args:
            relative_path: The relative path to append to DAGS_FOLDER.

        Returns:
            The constructed local path.
        """
        dags_folder = os.environ.get("DAGS_FOLDER", ".")
        return os.path.join(dags_folder, relative_path)

    def _read_local_file(self, path: str) -> Optional[str]:
        """Reads content from a local file if it exists and is a file.

        Args:
            path: The relative path to the local file.

        Returns:
            The content of the file as a string.

        Raises:
            OrchestrationPipelinesFileNotFoundError: If the file does not exist.
            OrchestrationPipelinesInvalidPathError: If the path is not a file.
            OrchestrationPipelinesFileReadError: If an error occurs during reading.
        """
        full_path = self._construct_local_path(path)
        try:
            with open(full_path, "r", encoding="utf-8") as f:
                return f.read()
        except FileNotFoundError as e:
            raise OrchestrationPipelinesFileNotFoundError(
                f"File '{full_path}' does not exist.") from e
        except IsADirectoryError as e:
            raise OrchestrationPipelinesInvalidPathError(
                f"'{full_path}' is not a file.") from e
        except Exception as e:
            raise OrchestrationPipelinesFileReadError(
                f"Error reading local file '{path}': {e}") from e

    def _parse_gcs_uri(self, gcs_uri: str) -> tuple[str, str]:
        """Parses a GCS URI into bucket name and blob path.

        Args:
            gcs_uri: The GCS URI to parse (e.g., 'gs://bucket/path/to/blob').

        Returns:
            A tuple containing the bucket name and blob path.

        Raises:
            OrchestrationPipelinesInvalidPathError: If the URI format is invalid or missing the bucket name.
        """
        if not gcs_uri.startswith("gs://"):
            raise OrchestrationPipelinesInvalidPathError(
                f"Invalid GCS URI format: {gcs_uri}")
        try:
            parts = gcs_uri[5:].split("/", 1)
            bucket_name = parts[0]
            if not bucket_name:
                raise OrchestrationPipelinesInvalidPathError(
                    f"Missing bucket name in GCS URI: {gcs_uri}")
            blob_path = parts[1] if len(parts) > 1 else ""
            return bucket_name, blob_path
        except IndexError as e:
            raise OrchestrationPipelinesInvalidPathError(
                f"Invalid GCS URI format: {gcs_uri}") from e

    def _read_gcs_file(self, gcs_uri: str) -> str:
        """Reads content from a Google Cloud Storage object.

        Args:
            gcs_uri: The GCS URI of the object to read.

        Returns:
            The content of the GCS object as a string.

        Raises:
            OrchestrationPipelinesFileNotFoundError: If the object does not exist.
            OrchestrationPipelinesInvalidPathError: If the URI format is invalid.
            OrchestrationPipelinesFileReadError: If an error occurs during access.
        """
        bucket_name, blob_path = self._parse_gcs_uri(gcs_uri)
        try:
            bucket = self._get_gcs_client().get_bucket(bucket_name)
            blob = bucket.blob(blob_path)
            return blob.download_as_text()
        except Exception as e:
            from google.api_core import exceptions
            if isinstance(e, exceptions.NotFound):
                raise OrchestrationPipelinesFileNotFoundError(
                    f"GCS object '{gcs_uri}' does not exist.") from e
            raise OrchestrationPipelinesFileReadError(
                f"Error accessing GCS object '{gcs_uri}': {e}") from e

    def read_absolute_path(self, path: str) -> str:
        """Reads the content of a file from an absolute path.

        Args:
            path: The absolute local path of the file to read.

        Returns:
            The content of the file as a string.

        Raises:
            OrchestrationPipelinesFileNotFoundError: If the file does not exist.
            OrchestrationPipelinesInvalidPathError: If the path is not a file.
            OrchestrationPipelinesFileReadError: If an error occurs during reading.
        """
        try:
            with open(path, "r", encoding="utf-8") as f:
                return f.read()
        except FileNotFoundError as e:
            raise OrchestrationPipelinesFileNotFoundError(
                f"File '{path}' does not exist.") from e
        except IsADirectoryError as e:
            raise OrchestrationPipelinesInvalidPathError(
                f"'{path}' is not a file.") from e
        except Exception as e:
            raise OrchestrationPipelinesFileReadError(
                f"Error reading local file '{path}': {e}") from e

    def read(self, file_path: str) -> str:
        """Reads the content of a file from a given path.

        Args:
            file_path: The absolute local path or full GCS URI of the file to
                       read.

        Returns:
            File content read from the given path.
        """
        if self._is_gcs_blob(file_path):
            return self._read_gcs_file(file_path)
        else:
            return self._read_local_file(file_path)

    def exists(self, file_path: str) -> bool:
        """Validates whether a file or location exists at the given path.

        This checks for both files and directories. For GCS, it also checks for
        prefixes (folders).

        Args:
            file_path: The absolute local path or full GCS URI to check.

        Returns:
            True if the resolved path exists and is accessible, False otherwise.
        """
        try:
            if self._is_gcs_blob(file_path):
                bucket_name, blob_path = self._parse_gcs_uri(file_path)
                bucket = self._gcs_client.get_bucket(bucket_name)

                # A GCS path can refer to a blob or a "folder" (prefix).
                # 1. Check if a blob exists at the exact path.
                if blob_path and bucket.blob(blob_path).exists():
                    return True

                # 2. If not, check if it's a folder (prefix).
                # An empty blob_path means the bucket root. A bucket always
                # exists if get_bucket() succeeds.
                if not blob_path:
                    return True

                # For non-root prefixes, check for objects under it.
                prefix = blob_path if blob_path.endswith(
                    "/") else blob_path + "/"
                return any(bucket.list_blobs(prefix=prefix, max_results=1))
            else:
                return os.path.exists(self._construct_local_path(file_path))
        except Exception:  # Includes GCS errors and invalid paths
            return False

    def get_blob_reference(self, resolved_path: str) -> str:
        """Returns a GCS blob URI for a given resolved path.

        If the path is already a GCS URI, it's returned as is after checking
        for existence. If it's a local path within a known Cloud Composer GCS
        mount point (e.g., /home/airflow/gcs/), it's converted to its
        corresponding GCS URI.

        Args:
            resolved_path: The resolved absolute local path or full GCS URI.

        Returns:
            The corresponding GCS blob URI.

        Raises:
            OrchestrationPipelinesFileNotFoundError: If the path does not exist.
            OrchestrationPipelinesInvalidPathError: If a local path cannot be
                converted to a GCS URI.
            OrchestrationPipelinesInitializationError: If GCS_BUCKET env var is not set.
        """
        if not self.exists(resolved_path):
            raise OrchestrationPipelinesFileNotFoundError(
                f"File not found: '{resolved_path}'")

        if self._is_gcs_blob(resolved_path):
            return resolved_path

        gcs_bucket = os.environ.get("GCS_BUCKET")
        if not gcs_bucket:
            raise OrchestrationPipelinesInitializationError(
                "GCS_BUCKET environment variable not set. Cannot determine GCS path for local file."
            )

        # In Cloud Composer, gs://<bucket> is mounted at /home/airflow/gcs
        mount_point = "/home/airflow/gcs/"
        if resolved_path.startswith(mount_point):
            blob_name = resolved_path.removeprefix(mount_point)
            blob_reference = f"gs://{gcs_bucket}/{blob_name}"
            if self.exists(blob_reference):
                return blob_reference

        raise OrchestrationPipelinesInvalidPathError(
            f"Cannot determine GCS reference for local path '{resolved_path}'. "
            "Path is not within the expected Cloud Composer mount point '/home/airflow/gcs/'."
        )

    def _is_gcs_blob(self, file_path: str) -> bool:
        """Checks if a given file path is a Google Cloud Storage URI.

        Args:
            file_path: The file path to check.

        Returns:
            True if the file path starts with 'gs://', False otherwise.
        """
        return file_path.startswith("gs://")
