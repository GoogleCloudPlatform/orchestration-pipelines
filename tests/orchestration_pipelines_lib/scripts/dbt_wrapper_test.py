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
"""Unit tests for the dbt_wrapper module."""

from unittest.mock import MagicMock, call, patch

import pytest

from orchestration_pipelines_lib.scripts.dbt_wrapper import (
    _build_cli_args,
    _sync_outputs_to_gcs,
    invoke_dbt,
)


def test_build_cli_args_with_all_options():
    """Tests that _build_cli_args constructs all expected DBT CLI flags."""
    invocation = {
        "command": "build",
        "select": ["tag:nightly", "my_model+"],
        "exclude": ["legacy_model"],
        "vars": {"env": "prod"},
        "full_refresh": True,
        "fail_fast": True,
        "threads": 4,
        "target": "prod",
        "extra_args": ["--store-failures"],
    }

    args = _build_cli_args(
        invocation,
        project_dir="/path/to/proj",
        profiles_dir="/path/to/prof",
        default_params={"default_key": "default_val", "env": "dev"},
        target_path="/tmp/dbt/target",
        log_path="/tmp/dbt/logs",
    )

    assert args == [
        "build",
        "--project-dir",
        "/path/to/proj",
        "--profiles-dir",
        "/path/to/prof",
        "--target-path",
        "/tmp/dbt/target",
        "--log-path",
        "/tmp/dbt/logs",
        "--select",
        "tag:nightly my_model+",
        "--exclude",
        "legacy_model",
        "--vars",
        '{"default_key": "default_val", "env": "prod"}',
        "--full-refresh",
        "--fail-fast",
        "--threads",
        "4",
        "--target",
        "prod",
        "--store-failures",
    ]


def test_build_cli_args_freshness_maps_to_source_freshness():
    """Tests that 'freshness' expands to ['source', 'freshness']."""
    args = _build_cli_args(
        {"command": "freshness"},
        project_dir="/proj",
        profiles_dir="/prof",
    )

    assert args == [
        "source",
        "freshness",
        "--project-dir",
        "/proj",
        "--profiles-dir",
        "/prof",
    ]


@patch(
    "orchestration_pipelines_lib.scripts.dbt_wrapper.tempfile.TemporaryDirectory"
)
@patch("dbt.cli.main.dbtRunner")
def test_invoke_dbt_legacy_parameters_succeed(
    mock_dbt_runner_cls, mock_temp_dir
):
    """Tests legacy select_models and params execution when invocations is None."""
    mock_temp_dir.return_value.__enter__.return_value = "/tmp/dbt_123"
    mock_runner = mock_dbt_runner_cls.return_value
    mock_node = MagicMock()
    mock_node.node.name = "model_1"
    mock_node.status = "success"
    mock_runner.invoke.return_value = MagicMock(
        success=True, result=[mock_node], exception=None
    )

    invoke_dbt(
        project_dir="/proj",
        profiles_dir="/prof",
        select_models=["model_1"],
        params={"key": "val"},
    )

    mock_runner.invoke.assert_called_once_with(
        [
            "run",
            "--project-dir",
            "/proj",
            "--profiles-dir",
            "/prof",
            "--target-path",
            "/tmp/dbt_123/target",
            "--log-path",
            "/tmp/dbt_123/logs",
            "--select",
            "model_1",
            "--vars",
            '{"key": "val"}',
        ]
    )


@patch(
    "orchestration_pipelines_lib.scripts.dbt_wrapper.tempfile.TemporaryDirectory"
)
@patch("dbt.cli.main.dbtRunner")
def test_invoke_dbt_sequential_invocations_with_none_result_succeed(
    mock_dbt_runner_cls, mock_temp_dir
):
    """Tests sequential execution across multiple invocations (including when result is None)."""
    mock_temp_dir.return_value.__enter__.return_value = "/tmp/dbt_123"
    mock_runner = mock_dbt_runner_cls.return_value
    mock_runner.invoke.side_effect = [
        MagicMock(success=True, result=None, exception=None),
        MagicMock(success=True, result=[], exception=None),
    ]

    invoke_dbt(
        project_dir="/proj",
        profiles_dir="/prof",
        invocations=[
            {"command": "seed"},
            {
                "command": "test",
                "select": ["model_1"],
                "fail_fast": True,
            },
        ],
    )

    assert mock_runner.invoke.call_args_list == [
        call(
            [
                "seed",
                "--project-dir",
                "/proj",
                "--profiles-dir",
                "/prof",
                "--target-path",
                "/tmp/dbt_123/target",
                "--log-path",
                "/tmp/dbt_123/logs",
            ]
        ),
        call(
            [
                "test",
                "--project-dir",
                "/proj",
                "--profiles-dir",
                "/prof",
                "--target-path",
                "/tmp/dbt_123/target",
                "--log-path",
                "/tmp/dbt_123/logs",
                "--select",
                "model_1",
                "--fail-fast",
            ]
        ),
    ]


@patch("orchestration_pipelines_lib.scripts.dbt_wrapper._sync_outputs_to_gcs")
@patch("dbt.cli.main.dbtRunner")
def test_invoke_dbt_raises_runtime_error_and_still_syncs_outputs_on_failure(
    mock_dbt_runner_cls, mock_sync_outputs
):
    """Tests that a failing invocation raises RuntimeError, halts subsequent commands, and still syncs outputs."""
    mock_runner = mock_dbt_runner_cls.return_value
    failed_node = MagicMock()
    failed_node.node.name = "bad_model"
    failed_node.status = "error"
    failed_node.message = "Compilation Error"
    mock_runner.invoke.return_value = MagicMock(
        success=False,
        result=[failed_node],
        exception=RuntimeError("DBT internal error"),
    )

    with pytest.raises(
        RuntimeError, match="dbt build failed. Check logs for details."
    ):
        invoke_dbt(
            project_dir="/proj",
            profiles_dir="/prof",
            invocations=[
                {"command": "build"},
                {"command": "test"},
            ],
            output_gcs_uri="gs://my-bucket/dbt-outputs/",
        )

    assert mock_runner.invoke.call_count == 1
    mock_sync_outputs.assert_called_once()


@patch("google.cloud.storage.Client")
def test_sync_outputs_to_gcs_uploads_manifest_and_run_results_only(
    mock_storage_client_cls, tmp_path
):
    """Tests that _sync_outputs_to_gcs uploads only manifest.json and run_results.json."""
    target_dir = tmp_path / "target"
    target_dir.mkdir()
    run_results = target_dir / "run_results.json"
    run_results.write_text('{"results": []}')
    manifest = target_dir / "manifest.json"
    manifest.write_text('{"nodes": {}}')
    compiled_sql = target_dir / "compiled.sql"
    compiled_sql.write_text("SELECT 1")

    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    dbt_log = log_dir / "dbt.log"
    dbt_log.write_text("log line")

    mock_bucket = mock_storage_client_cls.return_value.bucket.return_value

    _sync_outputs_to_gcs(
        str(tmp_path),
        "gs://my-bucket/orchestration_pipelines_outputs/b1/versions/v1/p1/r1/t1/",
        MagicMock(),
    )

    mock_storage_client_cls.return_value.bucket.assert_called_once_with(
        "my-bucket"
    )
    uploaded_blobs = {c.args[0] for c in mock_bucket.blob.call_args_list}
    assert uploaded_blobs == {
        "orchestration_pipelines_outputs/b1/versions/v1/p1/r1/t1/run_results.json",
        "orchestration_pipelines_outputs/b1/versions/v1/p1/r1/t1/manifest.json",
    }
