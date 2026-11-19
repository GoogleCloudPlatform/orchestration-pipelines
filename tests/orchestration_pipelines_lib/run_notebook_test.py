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
"""Unit tests for the run_notebook wrapper script."""

import sys
import unittest
from unittest.mock import MagicMock, patch

from orchestration_pipelines_lib import run_notebook


class RunNotebookTest(unittest.TestCase):
    """Tests output path resolution and papermill invocation in run_notebook."""

    def test_build_output_path_custom_gcs_prefix(self):
        """Tests custom gs:// prefixes (with/without trailing slash and bucket-only)."""
        cases = [
            (
                "gs://src-bucket/notebooks/etl.ipynb",
                "gs://archive-bucket/outputs/prod",
                "manual__2026-01-01",
                "gs://archive-bucket/outputs/prod/etl-output-manual__2026-01-01.ipynb",
            ),
            (
                "gs://src-bucket/notebooks/etl.ipynb",
                "gs://archive-bucket/outputs/prod/",
                "manual__2026-01-01",
                "gs://archive-bucket/outputs/prod/etl-output-manual__2026-01-01.ipynb",
            ),
            (
                "gs://src-bucket/notebooks/etl.ipynb",
                "gs://archive-bucket",
                "run_42",
                "gs://archive-bucket/etl-output-run_42.ipynb",
            ),
            (
                "gs://src-bucket/notebooks/etl.ipynb",
                "gs://archive-bucket/",
                "run_42",
                "gs://archive-bucket/etl-output-run_42.ipynb",
            ),
            (
                "gs://src-bucket/notebooks/etl.ipynb",
                "gs://archive-bucket///",
                "run_42",
                "gs://archive-bucket/etl-output-run_42.ipynb",
            ),
        ]
        for input_nb, output_target, run_id, expected in cases:
            with self.subTest(output_target=output_target):
                self.assertEqual(
                    run_notebook.build_output_path(
                        input_nb, output_target, run_id
                    ),
                    expected,
                )

    def test_build_output_path_invalid_gcs_prefix_raises(self):
        """Tests bucketless or whitespace-bucket gs:// prefixes raise ValueError."""
        for invalid_target in (
            "gs://",
            "gs:///",
            "gs:///outputs",
            "gs://   ",
            "gs://my bucket/outputs",
        ):
            with self.subTest(output_target=invalid_target):
                with self.assertRaisesRegex(
                    ValueError, "Invalid GCS output URI prefix"
                ):
                    run_notebook.build_output_path(
                        "gs://src-bucket/notebooks/etl.ipynb",
                        invalid_target,
                        "run_1",
                    )

    def test_build_output_path_bare_bucket_fallback(self):
        """Tests bare bucket name writes under composer_orchestration_pipelines_resources/."""
        self.assertEqual(
            run_notebook.build_output_path(
                "gs://src-bucket/notebooks/etl.ipynb",
                "composer-bucket",
                "run_1",
            ),
            "gs://composer-bucket/composer_orchestration_pipelines_resources/etl-output-run_1.ipynb",
        )

    def test_main_executes_papermill_with_custom_prefix_and_params(self):
        """Tests main() passes custom output URI and JSON parameters to papermill."""
        mock_pm = MagicMock()
        with patch.dict(sys.modules, {"papermill": mock_pm}):
            run_notebook.main(
                [
                    "gs://src-bucket/nb.ipynb",
                    "gs://out-bucket/archived/",
                    "run_99",
                    '{"env": "prod"}',
                ]
            )

        mock_pm.execute_notebook.assert_called_once_with(
            "gs://src-bucket/nb.ipynb",
            "gs://out-bucket/archived/nb-output-run_99.ipynb",
            parameters={"env": "prod"},
            kernel_name="python3",
        )

    def test_main_executes_papermill_with_fallback_bucket_and_no_params(self):
        """Tests main() falls back to default subdirectory when bare bucket is given."""
        mock_pm = MagicMock()
        with patch.dict(sys.modules, {"papermill": mock_pm}):
            run_notebook.main(
                [
                    "gs://src-bucket/nb.ipynb",
                    "composer-bucket",
                    "run_1",
                ]
            )

        mock_pm.execute_notebook.assert_called_once_with(
            "gs://src-bucket/nb.ipynb",
            "gs://composer-bucket/composer_orchestration_pipelines_resources/nb-output-run_1.ipynb",
            parameters={},
            kernel_name="python3",
        )

    def test_main_propagates_papermill_exception(self):
        """Tests main() re-raises exceptions from papermill.execute_notebook."""
        mock_pm = MagicMock()
        mock_pm.execute_notebook.side_effect = RuntimeError("notebook failed")
        with patch.dict(sys.modules, {"papermill": mock_pm}):
            with self.assertRaisesRegex(RuntimeError, "notebook failed"):
                run_notebook.main(
                    [
                        "gs://src-bucket/nb.ipynb",
                        "gs://out-bucket/archived",
                        "run_1",
                    ]
                )


if __name__ == "__main__":
    unittest.main()
