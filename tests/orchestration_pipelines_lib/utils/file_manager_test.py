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
"""Unit tests for the FileManager class."""
import unittest
from unittest.mock import MagicMock, mock_open, patch

from google.api_core import exceptions as gcp_exceptions

from orchestration_pipelines_lib.utils.file_manager import (
    FileManager,
    OrchestrationPipelinesFileNotFoundError,
    OrchestrationPipelinesFileReadError,
    OrchestrationPipelinesInitializationError,
    OrchestrationPipelinesInvalidPathError,
)


class TestFileManager(unittest.TestCase):
    """Test suite for the generic FileManager."""

    def setUp(self):
        # Mock GCS client
        self.mock_gcs_client = MagicMock()
        self.mock_bucket = MagicMock()
        self.mock_blob = MagicMock()
        self.mock_gcs_client.get_bucket.return_value = self.mock_bucket
        self.mock_bucket.blob.return_value = self.mock_blob

        self.file_manager = FileManager(gcs_client=self.mock_gcs_client)

    # pylint: disable=protected-access
    def test_initialization_success(self):
        """Tests that initialization success."""
        self.assertIsNotNone(self.file_manager._gcs_client)

    @patch("google.cloud.storage.Client")
    def test_initialization_with_default_gcs_client(self, mock_client):
        """Tests that initialization with default gcs client."""
        from orchestration_pipelines_lib.utils import file_manager
        file_manager._GCS_CLIENT = None
        fm = FileManager()
        fm._get_gcs_client()
        mock_client.assert_called_once()

    @patch("os.path.exists", return_value=True)
    @patch("os.path.isfile", return_value=True)
    # Mocks are required by the patch decorator but not used in the test.
    # pylint: disable=unused-argument
    def test_read_local_file_success(self, mock_isfile, mock_exists):
        """Tests that read local file success."""
        local_path = "/path/to/my_file.txt"
        file_content = "hello world"
        with patch("builtins.open",
                   mock_open(read_data=file_content)) as mock_file:
            content = self.file_manager.read(local_path)
            self.assertEqual(content, file_content)
            mock_file.assert_called_once_with(local_path, encoding="utf-8")

    @patch("os.path.exists", return_value=False)
    # Mocks are required by the patch decorator but not used in the test.
    # pylint: disable=unused-argument
    def test_read_local_file_not_found(self, mock_exists):
        """Tests that read local file not found."""
        with patch("builtins.open", side_effect=FileNotFoundError("not found")):
            with self.assertRaises(OrchestrationPipelinesFileNotFoundError):
                self.file_manager.read("/non_existent.txt")

    @patch("os.path.exists", return_value=True)
    @patch("os.path.isfile", return_value=False)
    # Mocks are required by the patch decorator but not used in the test.
    # pylint: disable=unused-argument
    def test_read_local_path_is_not_a_file(self, mock_isfile, mock_exists):
        """Tests that read local path is not a file."""
        with patch("builtins.open", side_effect=IsADirectoryError("is a directory")):
            with self.assertRaises(OrchestrationPipelinesInvalidPathError):
                self.file_manager.read("/a_directory")

    @patch("os.path.exists", return_value=True)
    @patch("os.path.isfile", return_value=True)
    # Mocks are required by the patch decorator but not used in the test.
    # pylint: disable=unused-argument
    def test_read_local_file_read_error(self, mock_isfile, mock_exists):
        """Tests that read local file read error."""
        with patch("builtins.open", side_effect=OSError("read error")):
            with self.assertRaises(OrchestrationPipelinesFileReadError):
                self.file_manager.read("/my_file.txt")

    def test_read_gcs_file_success(self):
        """Tests that read gcs file success."""
        gcs_uri = "gs://example-bucket/my_file.txt"
        file_content = "gcs content"
        self.mock_blob.exists.return_value = True
        self.mock_blob.download_as_text.return_value = file_content
        content = self.file_manager.read(gcs_uri)
        self.assertEqual(content, file_content)
        self.mock_gcs_client.get_bucket.assert_called_with("example-bucket")
        self.mock_bucket.blob.assert_called_with("my_file.txt")
        self.mock_blob.download_as_text.assert_called_once()

    def test_read_gcs_file_not_found(self):
        """Tests that read gcs file not found."""
        gcs_uri = "gs://example-bucket/non_existent.txt"
        self.mock_blob.download_as_text.side_effect = gcp_exceptions.NotFound("not found")
        with self.assertRaises(OrchestrationPipelinesFileNotFoundError):
            self.file_manager.read(gcs_uri)

    def test_read_gcs_invalid_uri(self):
        """Tests that read gcs invalid uri."""
        with self.assertRaises(OrchestrationPipelinesInvalidPathError):
            self.file_manager.read("gs://")
        with self.assertRaises(OrchestrationPipelinesInvalidPathError):
            self.file_manager.read("gs:///no-bucket")

    def test_read_gcs_access_error(self):
        """Tests that read gcs access error."""
        gcs_uri = "gs://example-bucket/my_file.txt"
        self.mock_gcs_client.get_bucket.side_effect = gcp_exceptions.Forbidden(
            "no access")
        with self.assertRaises(OrchestrationPipelinesFileReadError):
            self.file_manager.read(gcs_uri)

    @patch("os.path.exists")
    def test_exists_local_file_true(self, mock_exists):
        """Tests that exists local file true."""
        mock_exists.return_value = True
        self.assertTrue(self.file_manager.exists("/some/file.txt"))
        mock_exists.assert_called_once_with("/some/file.txt")

    @patch("os.path.exists")
    def test_exists_local_file_false(self, mock_exists):
        """Tests that exists local file false."""
        mock_exists.return_value = False
        self.assertFalse(self.file_manager.exists("/some/file.txt"))

    def test_exists_gcs_object_true(self):
        """Tests that exists gcs object true."""
        gcs_uri = "gs://example-bucket/my_file.txt"
        self.mock_blob.exists.return_value = True
        self.assertTrue(self.file_manager.exists(gcs_uri))
        self.mock_gcs_client.get_bucket.assert_called_with("example-bucket")
        self.mock_bucket.blob.assert_called_with("my_file.txt")

    def test_exists_gcs_object_false(self):
        """Tests that exists gcs object false."""
        gcs_uri = "gs://example-bucket/my_file.txt"
        self.mock_blob.exists.return_value = False
        self.assertFalse(self.file_manager.exists(gcs_uri))

    def test_exists_gcs_access_error(self):
        """Tests that exists gcs access error."""
        gcs_uri = "gs://example-bucket/my_file.txt"
        self.mock_gcs_client.get_bucket.side_effect = gcp_exceptions.Forbidden(
            "no access")
        self.assertFalse(self.file_manager.exists(gcs_uri))

    # pylint: disable=protected-access
    def test_is_gcs_blob(self):
        """Tests that is gcs blob."""
        self.assertTrue(self.file_manager._is_gcs_blob("gs://bucket/blob"))
        self.assertFalse(self.file_manager._is_gcs_blob("/local/path"))
        self.assertFalse(self.file_manager._is_gcs_blob("http://bucket/blob"))

    # pylint: disable=protected-access
    def test_parse_gcs_uri_private_method(self):
        """Tests for the private _parse_gcs_uri method."""
        # Success cases
        bucket, blob = self.file_manager._parse_gcs_uri(
            "gs://example-bucket/my/blob/path.txt")
        self.assertEqual(bucket, "example-bucket")
        self.assertEqual(blob, "my/blob/path.txt")

        bucket, blob = self.file_manager._parse_gcs_uri("gs://example-bucket/")
        self.assertEqual(bucket, "example-bucket")
        self.assertEqual(blob, "")

        bucket, blob = self.file_manager._parse_gcs_uri("gs://example-bucket")
        self.assertEqual(bucket, "example-bucket")
        self.assertEqual(blob, "")

        # Failure cases
        with self.assertRaises(OrchestrationPipelinesInvalidPathError):
            self.file_manager._parse_gcs_uri("not-a-gcs-uri")
        with self.assertRaises(OrchestrationPipelinesInvalidPathError):
            self.file_manager._parse_gcs_uri("gs://")
        with self.assertRaises(OrchestrationPipelinesInvalidPathError):
            self.file_manager._parse_gcs_uri("gs:///no-bucket")

    # pylint: disable=protected-access
    def test_read_gcs_file_private_method_scenarios(self):
        """Tests that read gcs file private method scenarios."""
        gcs_uri = "gs://example-bucket/my_file.txt"

        # Reset mocks
        self.mock_gcs_client.reset_mock()
        self.mock_bucket.reset_mock()
        self.mock_blob.reset_mock()
        # Invalid URI format
        with self.assertRaises(OrchestrationPipelinesInvalidPathError):
            self.file_manager._read_gcs_file("not-gcs-uri")
        with self.assertRaises(OrchestrationPipelinesInvalidPathError):
            self.file_manager._read_gcs_file("gs://")
        with self.assertRaises(OrchestrationPipelinesInvalidPathError):
            self.file_manager._read_gcs_file("gs:///blob")

        # GCS API error
        self.mock_gcs_client.get_bucket.side_effect = gcp_exceptions.NotFound(
            "not found")
        with self.assertRaises(OrchestrationPipelinesFileReadError):
            self.file_manager._read_gcs_file(gcs_uri)
        self.mock_gcs_client.get_bucket.side_effect = None  # Reset side effect

        # Blob does not exist
        self.mock_blob.download_as_text.side_effect = gcp_exceptions.NotFound("not found")
        with self.assertRaises(OrchestrationPipelinesFileNotFoundError):
            self.file_manager._read_gcs_file(gcs_uri)

        # Success
        self.mock_blob.download_as_text.side_effect = None
        self.mock_blob.download_as_text.return_value = "gcs content"
        content = self.file_manager._read_gcs_file(gcs_uri)
        self.assertEqual(content, "gcs content")

    @patch.object(FileManager, "exists")
    def test_get_blob_reference_gcs_path_success(self, mock_exists):
        """Tests get_blob_reference with an existing GCS path."""
        gcs_path = "gs://example-bucket/my_file.txt"
        mock_exists.return_value = True
        result = self.file_manager.get_blob_reference(gcs_path)
        self.assertEqual(result, gcs_path)
        mock_exists.assert_called_once_with(gcs_path)

    @patch.dict("os.environ", {"GCS_BUCKET": "example-bucket"})
    @patch.object(FileManager, "exists")
    def test_get_blob_reference_local_path_success(self, mock_exists):
        """Tests get_blob_reference with a valid local path."""
        local_path = "/home/airflow/gcs/dags/my_dag.py"
        gcs_path = "gs://example-bucket/dags/my_dag.py"

        # Both local path and GCS path exist
        mock_exists.side_effect = [True, True]

        result = self.file_manager.get_blob_reference(local_path)
        self.assertEqual(result, gcs_path)
        self.assertEqual(mock_exists.call_count, 2)
        mock_exists.assert_any_call(local_path)
        mock_exists.assert_any_call(gcs_path)

    @patch.object(FileManager, "exists")
    def test_get_blob_reference_file_not_found(self, mock_exists):
        """Tests get_blob_reference when the file does not exist."""
        mock_exists.return_value = False
        with self.assertRaises(OrchestrationPipelinesFileNotFoundError):
            self.file_manager.get_blob_reference("/non/existent/file.txt")
        mock_exists.assert_called_once_with("/non/existent/file.txt")

    @patch.dict("os.environ", {}, clear=True)
    @patch.object(FileManager, "exists")
    def test_get_blob_reference_no_gcs_bucket_env(self, mock_exists):
        """Tests get_blob_reference when GCS_BUCKET env var is not set."""
        local_path = "/home/airflow/gcs/dags/my_dag.py"
        mock_exists.return_value = True
        with self.assertRaises(OrchestrationPipelinesInitializationError):
            self.file_manager.get_blob_reference(local_path)
        mock_exists.assert_called_once_with(local_path)

    @patch.dict("os.environ", {"GCS_BUCKET": "example-bucket"})
    @patch.object(FileManager, "exists")
    def test_get_blob_reference_path_outside_mount_point(self, mock_exists):
        """Tests get_blob_reference with a path outside the mount point."""
        local_path = "/some/other/path/file.txt"
        mock_exists.return_value = True
        with self.assertRaises(OrchestrationPipelinesInvalidPathError):
            self.file_manager.get_blob_reference(local_path)
        mock_exists.assert_called_once_with(local_path)

    @patch.dict("os.environ", {"GCS_BUCKET": "example-bucket"})
    @patch.object(FileManager, "exists")
    def test_get_blob_reference_gcs_blob_does_not_exist(self, mock_exists):
        """Tests get_blob_reference when the corresponding GCS blob doesn't exist."""
        local_path = "/home/airflow/gcs/dags/my_dag.py"
        gcs_path = "gs://example-bucket/dags/my_dag.py"

        mock_exists.side_effect = [True, False]

        with self.assertRaises(OrchestrationPipelinesInvalidPathError):
            self.file_manager.get_blob_reference(local_path)

        self.assertEqual(mock_exists.call_count, 2)
        mock_exists.assert_any_call(local_path)
        mock_exists.assert_any_call(gcs_path)

    # pylint: disable=protected-access
    def test_extract_relative_path_success(self):
        """Tests extract_relative_path with valid root and full path."""
        local_data_root = "/home/airflow/gcs/data"
        full_path = (
            "/home/airflow/gcs/data/test_bundle/versions/"
            "a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p/scripts/requirements.txt")

        expected_relative_path = (
            "test_bundle/versions/a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p/scripts/requirements.txt"
        )

        result = self.file_manager.extract_relative_path(
            full_path, local_data_root)
        self.assertEqual(result, expected_relative_path)

    def test_extract_relative_path_without_local_data_root(self):
        """Tests that trailing slashes in root are handled correctly."""
        full_path = "/home/airflow/gcs/data/test_bundle/file.txt"
        expected_relative_path = "home/airflow/gcs/data/test_bundle/file.txt"

        result = self.file_manager.extract_relative_path(full_path)
        self.assertEqual(result, expected_relative_path)


if __name__ == "__main__":
    unittest.main()
