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

    def setUp(self):
        util._UPLOADED_RUN_NOTEBOOK_PATHS.clear()
        util._get_run_notebook_content_hash.cache_clear()

    def tearDown(self):
        util._UPLOADED_RUN_NOTEBOOK_PATHS.clear()
        util._get_run_notebook_content_hash.cache_clear()

    @patch("google.cloud.storage.Client")
    @patch("importlib.resources.files")
    def test_upload_run_notebook_if_needed_missing(
        self, mock_res_files, mock_storage_client
    ):
        """Test upload occurs with if_generation_match=0 when blob is missing."""
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_blob.exists.return_value = False

        mock_storage_client.return_value.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob

        mock_path = MagicMock()
        mock_res_files.return_value.joinpath.return_value = mock_path

        with patch("importlib.resources.as_file") as mock_as_file:
            mock_as_file.return_value.__enter__.return_value = (
                "/local/path/file.py"
            )

            util.upload_run_notebook_if_needed(
                "gs://example-bucket/data/run_notebook_abc123def456.py"
            )

            mock_blob.exists.assert_called_once()
            mock_blob.reload.assert_not_called()
            mock_blob.upload_from_filename.assert_called_once_with(
                "/local/path/file.py", if_generation_match=0
            )

    @patch("google.cloud.storage.Client")
    @patch("importlib.resources.files")
    def test_upload_run_notebook_if_needed_existing_skips_upload(
        self, mock_res_files, mock_storage_client
    ):
        """Test existing content-addressed blob skips upload without reload or local I/O."""
        mock_blob = MagicMock()
        mock_blob.exists.return_value = True
        mock_storage_client.return_value.bucket.return_value.blob.return_value = (
            mock_blob
        )

        util.upload_run_notebook_if_needed(
            "gs://example-bucket/data/run_notebook_abc123def456.py"
        )

        mock_blob.exists.assert_called_once()
        mock_blob.reload.assert_not_called()
        mock_blob.upload_from_filename.assert_not_called()
        mock_res_files.assert_not_called()

    @patch("google.cloud.storage.Client")
    @patch("importlib.resources.files")
    def test_upload_run_notebook_if_needed_concurrent_precondition_failed(
        self, mock_res_files, mock_storage_client
    ):
        """Test PreconditionFailed from a concurrent upload is ignored and memoized."""
        from google.api_core.exceptions import PreconditionFailed

        mock_blob = MagicMock()
        mock_blob.exists.return_value = False
        mock_blob.upload_from_filename.side_effect = PreconditionFailed(
            "already exists"
        )
        mock_storage_client.return_value.bucket.return_value.blob.return_value = (
            mock_blob
        )

        with patch("importlib.resources.as_file") as mock_as_file:
            mock_as_file.return_value.__enter__.return_value = (
                "/local/path/file.py"
            )
            gcs_path = "gs://example-bucket/data/run_notebook_abc123def456.py"
            util.upload_run_notebook_if_needed(gcs_path)
            # Second call in the same process should be memoized (no extra RPCs).
            util.upload_run_notebook_if_needed(gcs_path)

        mock_storage_client.assert_called_once()
        mock_blob.upload_from_filename.assert_called_once_with(
            "/local/path/file.py", if_generation_match=0
        )

    @patch("google.cloud.storage.Client")
    def test_upload_run_notebook_if_needed_memoizes_per_process(
        self, mock_storage_client
    ):
        """Test subsequent calls for the same GCS path skip GCS RPCs."""
        mock_blob = MagicMock()
        mock_blob.exists.return_value = True
        mock_storage_client.return_value.bucket.return_value.blob.return_value = (
            mock_blob
        )

        gcs_path_a = "gs://example-bucket/data/run_notebook_aaaaaaaaaaaa.py"
        gcs_path_b = "gs://other-bucket/data/run_notebook_aaaaaaaaaaaa.py"

        util.upload_run_notebook_if_needed(gcs_path_a)
        util.upload_run_notebook_if_needed(gcs_path_a)
        self.assertEqual(mock_storage_client.call_count, 1)
        self.assertEqual(mock_blob.exists.call_count, 1)

        util.upload_run_notebook_if_needed(gcs_path_b)
        self.assertEqual(mock_storage_client.call_count, 2)
        self.assertEqual(mock_blob.exists.call_count, 2)

    @patch("google.cloud.storage.Client")
    @patch("importlib.resources.files")
    def test_upload_run_notebook_if_needed_ttl_expiry_rechecks_and_reuploads(
        self, mock_res_files, mock_storage_client
    ):
        """Test expired TTL triggers a fresh check and re-upload if blob was deleted."""
        mock_blob = MagicMock()
        mock_blob.exists.side_effect = [True, False]
        mock_storage_client.return_value.bucket.return_value.blob.return_value = (
            mock_blob
        )

        gcs_path = "gs://example-bucket/data/run_notebook_aaaaaaaaaaaa.py"
        with (
            patch(
                "orchestration_pipelines_lib.dag_generator.airflow_adapters."
                "common_utils.gcs_utils.time.monotonic",
                side_effect=[
                    100.0,  # 1st call entry (miss)
                    100.0,  # 1st call cache store after exists() == True
                    200.0,  # 2nd call entry (within 300s TTL -> hit)
                    401.0,  # 3rd call entry (after 300s TTL -> expired)
                    401.0,  # 3rd call cache store after re-upload
                ],
            ),
            patch("importlib.resources.as_file") as mock_as_file,
        ):
            mock_as_file.return_value.__enter__.return_value = (
                "/local/path/file.py"
            )
            util.upload_run_notebook_if_needed(gcs_path)
            util.upload_run_notebook_if_needed(gcs_path)
            self.assertEqual(mock_blob.exists.call_count, 1)
            mock_blob.upload_from_filename.assert_not_called()

            # After TTL expires and blob was deleted in GCS, it re-uploads.
            util.upload_run_notebook_if_needed(gcs_path)
            self.assertEqual(mock_blob.exists.call_count, 2)
            mock_blob.upload_from_filename.assert_called_once_with(
                "/local/path/file.py", if_generation_match=0
            )

    def test_get_run_notebook_gcs_path_success(self):
        """Test content-addressed path construction when env var is set."""
        import hashlib
        import importlib.resources

        notebook_bytes = (
            importlib.resources.files("orchestration_pipelines_lib")
            .joinpath("run_notebook.py")
            .read_bytes()
        )
        expected_hash = hashlib.sha256(notebook_bytes).hexdigest()[:12]
        expected = (
            f"gs://example-bucket/data/run_notebook_{expected_hash}.py"
        )
        with patch.dict(os.environ, {"GCS_BUCKET": "example-bucket"}):
            self.assertEqual(util.get_run_notebook_gcs_path(), expected)

    @patch("importlib.resources.files")
    def test_get_run_notebook_gcs_path_changes_with_content(
        self, mock_res_files
    ):
        """Test path hash updates when run_notebook.py content changes."""
        import hashlib

        content_v1 = b"print('v1')\n"
        content_v2 = b"print('v2')\n"
        hash_v1 = hashlib.sha256(content_v1).hexdigest()[:12]
        hash_v2 = hashlib.sha256(content_v2).hexdigest()[:12]

        mock_res_files.return_value.joinpath.return_value.read_bytes.return_value = (
            content_v1
        )
        with patch.dict(os.environ, {"GCS_BUCKET": "example-bucket"}):
            path_v1 = util.get_run_notebook_gcs_path()
            # Cached call should not re-read the file.
            self.assertEqual(util.get_run_notebook_gcs_path(), path_v1)
        self.assertEqual(
            path_v1, f"gs://example-bucket/data/run_notebook_{hash_v1}.py"
        )
        self.assertEqual(
            mock_res_files.return_value.joinpath.return_value.read_bytes.call_count,
            1,
        )

        util._get_run_notebook_content_hash.cache_clear()
        mock_res_files.return_value.joinpath.return_value.read_bytes.return_value = (
            content_v2
        )
        with patch.dict(os.environ, {"GCS_BUCKET": "example-bucket"}):
            path_v2 = util.get_run_notebook_gcs_path()
        self.assertEqual(
            path_v2, f"gs://example-bucket/data/run_notebook_{hash_v2}.py"
        )
        self.assertNotEqual(path_v1, path_v2)

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
