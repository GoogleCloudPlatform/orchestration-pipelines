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
"""Unit tests for Dataproc utility functions."""
import os

import unittest
from unittest.mock import patch, MagicMock
from orchestration_pipelines_lib.dag_generator.airflow_adapters.common_utils import dataproc_utils as util


class DataprocUtilsTest(unittest.TestCase):

    @patch("google.cloud.dataproc_v1.SessionTemplateControllerClient")
    @patch("google.api_core.client_options.ClientOptions")
    @patch("google.cloud.dataproc_v1.GetSessionTemplateRequest")
    def test_get_session_template(self, mock_request_cls,
                                  mock_client_options_cls, mock_client_cls):
        """Tests that the Dataproc client and request are constructed correctly."""
        mock_client_instance = mock_client_cls.return_value
        mock_client_instance.get_session_template.return_value = "mock_session_template"

        result = util.get_session_template("europe-west1", "my-template-name")

        self.assertEqual(result, "mock_session_template")

        # Verify ClientOptions was called with the correct regional endpoint
        mock_client_options_cls.assert_called_once_with(
            api_endpoint="europe-west1-dataproc.googleapis.com:443")

        # Verify the client was instantiated with those options
        mock_client_cls.assert_called_once_with(
            client_options=mock_client_options_cls.return_value)

        # Verify the request was built and passed correctly
        mock_request_cls.assert_called_once_with(name="my-template-name")
        mock_client_instance.get_session_template.assert_called_once_with(
            request=mock_request_cls.return_value)

    def test_sanitized(self):
        """Tests that the repository_config is sanitized if it exists."""
        scenarios = {
            "has_runtime_and_repo": (True, True),
            "has_runtime_no_repo": (True, False),
            "no_runtime": (False, False),
        }

        for name, (has_runtime, has_repo) in scenarios.items():
            with self.subTest(scenario=name):
                # Construct mock SessionTemplate based on scenario
                mock_template = MagicMock()

                if not has_runtime:
                    mock_template.runtime_config = None
                elif not has_repo:
                    del mock_template.runtime_config.repository_config
                else:
                    mock_template.runtime_config.repository_config = "unsupported_value"

                # Execute
                result = util.sanitized(mock_template)

                # Verify
                if has_runtime and has_repo:
                    self.assertIsNone(result.runtime_config.repository_config)
                elif has_runtime and not has_repo:
                    self.assertFalse(
                        hasattr(result.runtime_config, "repository_config"))
                else:
                    self.assertIsNone(result.runtime_config)

    def test_get_pyspark_batch_config_notebook(self):
        """Tests batch config generation for a notebook action."""
        # Creating an object to match the attribute access in the function (action.type)
        action = type(
            'obj', (object, ), {
                'type': 'notebook',
                'filename': 'my_analysis.ipynb',
                'archives': ['gs://bucket/archive.zip'],
                'pyFiles': []
            })

        with patch.dict(os.environ, {"GCS_BUCKET": "example-bucket"}):
            config = util.get_pyspark_batch_config(
                action, "gs://wrapper/run_notebook.py")

            self.assertEqual(config["main_python_file_uri"],
                             "gs://wrapper/run_notebook.py")
            self.assertEqual(
                config["args"],
                ["my_analysis.ipynb", "example-bucket", "{{ run_id }}"])
            self.assertEqual(config["archive_uris"],
                             ["gs://bucket/archive.zip"])

    def test_get_pyspark_batch_config_script(self):
        """Tests batch config generation for a standard pyspark script action."""
        action = type(
            'obj',
            (object, ),
            {
                'type': 'pyspark',
                'filename': 'gs://bucket/script.py',
                'archives': None,  # Testing the default empty list fallback
                'pyFiles': []
            })

        with patch.dict(os.environ, {"GCS_BUCKET": "example-bucket"}):
            config = util.get_pyspark_batch_config(
                action, "gs://wrapper/run_notebook.py")

            self.assertEqual(config["main_python_file_uri"],
                             "gs://bucket/script.py")
            self.assertNotIn(
                "args", config)  # Scripts don't get the wrapper args injected
            self.assertEqual(config["archive_uris"], [])


if __name__ == "__main__":
    unittest.main()
