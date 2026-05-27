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

import os
import unittest
from unittest.mock import MagicMock, mock_open, patch

from orchestration_pipelines_lib.dag_generator.airflow_adapters.common_utils import (
    gcs_utils as util,
)


class GcsUtilsTest(unittest.TestCase):

    @patch("google.cloud.storage.Client")
    @patch("importlib.resources.files")
    def test_upload_run_notebook_if_needed_missing(self, mock_res_files,
                                                   mock_storage_client):
        """Test upload occurs when the blob does not exist."""
        # Setup GCS Mocks
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_blob.exists.return_value = False

        mock_storage_client.return_value.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob

        mock_path = MagicMock()
        mock_res_files.return_value.joinpath.return_value = mock_path

        with patch.dict(os.environ, {"GCS_BUCKET": "example-bucket"}):
            with patch("importlib.resources.as_file") as mock_as_file:
                mock_as_file.return_value.__enter__.return_value = "/local/path/file.py"

                util.upload_run_notebook_if_needed("gs://example-bucket/target")

                mock_blob.upload_from_filename.assert_called_once_with(
                    "/local/path/file.py")

    def test_get_run_notebook_gcs_path_success(self):
        """Test path construction when env var is set."""
        with patch.dict(os.environ, {"GCS_BUCKET": "example-bucket"}):
            expected = "gs://example-bucket/data/run_notebook.py"
            self.assertEqual(util.get_run_notebook_gcs_path(), expected)

    def test_get_run_notebook_gcs_path_error(self):
        """Test ValueError when GCS_BUCKET is missing."""
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(ValueError):
                util.get_run_notebook_gcs_path()

    @patch("google.cloud.storage.Client")
    def test_get_gcs_file_content(self, mock_storage_client):
        """Test downloading and decoding GCS content."""
        mock_blob = MagicMock()
        mock_blob.download_as_string.return_value = b"hello cloud"
        mock_storage_client.return_value.bucket.return_value.blob.return_value = mock_blob

        content = util.get_gcs_file_content("gs://bucket/path/file.txt")

        self.assertEqual(content, "hello cloud")
        mock_blob.download_as_string.assert_called_once()

    def test_read_local_file_content(self):
        """Tests reading a file relative to DAGS_FOLDER with and without the env var."""
        scenarios = [
            # scenario_name, env_dict, expected_full_path
            ("with_custom_env", {
                "DAGS_FOLDER": "/custom/dags/path"
            }, "/custom/dags/path/test_file.txt"),
            ("with_default_env", {}, "/home/airflow/gcs/dags/test_file.txt")
        ]

        for name, env_dict, expected_path in scenarios:
            with self.subTest(scenario=name):
                m_open = mock_open(read_data="relative file content")

                with patch("builtins.open", m_open):
                    with patch.dict(os.environ, env_dict, clear=True):
                        content = util.read_local_file_content("test_file.txt")

                        self.assertEqual(content, "relative file content")
                        m_open.assert_called_once_with(expected_path, encoding="utf-8")

    def test_read_local_file_content_from_path(self):
        """Tests reading a file from a direct, explicit path."""
        m_open = mock_open(read_data="absolute file content")
        test_path = "/absolute/path/to/my_data.json"

        with patch("builtins.open", m_open):
            content = util.read_local_file_content_from_path(test_path)

            self.assertEqual(content, "absolute file content")
            m_open.assert_called_once_with(test_path, encoding="utf-8")


if __name__ == "__main__":
    unittest.main()
