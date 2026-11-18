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

import logging
import os
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, call, patch

import pytest
from dbt.cli.main import cli

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


@pytest.mark.parametrize("command", ["freshness", "source freshness"])
def test_build_cli_args_freshness_maps_to_source_freshness(command):
    """Tests that 'freshness' and 'source freshness' expand to ['source', 'freshness']."""
    args = _build_cli_args(
        {"command": command},
        project_dir="/proj",
        profiles_dir="/prof",
        target_path="/tmp/dbt/target",
        log_path="/tmp/dbt/logs",
    )

    assert args == [
        "source",
        "freshness",
        "--project-dir",
        "/proj",
        "--profiles-dir",
        "/prof",
        "--target-path",
        "/tmp/dbt/target",
        "--log-path",
        "/tmp/dbt/logs",
    ]


def test_build_cli_args_deps_omits_target_path(tmp_path):
    """Tests that 'deps' omits --target-path because dbt-core rejects it."""
    proj_dir = tmp_path / "proj"
    prof_dir = tmp_path / "prof"
    proj_dir.mkdir()
    prof_dir.mkdir()

    args = _build_cli_args(
        {"command": "deps", "vars": {"pkg_branch": "main"}},
        project_dir=str(proj_dir),
        profiles_dir=str(prof_dir),
        target_path=str(tmp_path / "target"),
        log_path=str(tmp_path / "logs"),
    )

    assert "--target-path" not in args
    assert args == [
        "deps",
        "--project-dir",
        str(proj_dir),
        "--profiles-dir",
        str(prof_dir),
        "--log-path",
        str(tmp_path / "logs"),
        "--vars",
        '{"pkg_branch": "main"}',
    ]

    ctx = cli.commands["deps"].make_context("deps", args[1:])
    assert ctx.params["project_dir"] == str(proj_dir)
    assert ctx.params["profiles_dir"] == str(prof_dir)


def test_build_cli_args_run_operation_with_macro_and_args(tmp_path):
    """Tests that 'run-operation' passes positional macro and JSON --args."""
    proj_dir = tmp_path / "proj"
    prof_dir = tmp_path / "prof"
    proj_dir.mkdir()
    prof_dir.mkdir()

    args = _build_cli_args(
        {
            "command": "run-operation",
            "macro": "grant_select",
            "args": {"role": "reporter", "dry_run": False, "days": 7},
            "target": "prod",
        },
        project_dir=str(proj_dir),
        profiles_dir=str(prof_dir),
        target_path=str(tmp_path / "target"),
        log_path=str(tmp_path / "logs"),
    )

    assert args == [
        "run-operation",
        "grant_select",
        "--project-dir",
        str(proj_dir),
        "--profiles-dir",
        str(prof_dir),
        "--target-path",
        str(tmp_path / "target"),
        "--log-path",
        str(tmp_path / "logs"),
        "--args",
        '{"role": "reporter", "dry_run": false, "days": 7}',
        "--target",
        "prod",
    ]

    ctx = cli.commands["run-operation"].make_context("run-operation", args[1:])
    assert ctx.params["macro"] == "grant_select"
    assert ctx.params["args"] == {
        "role": "reporter",
        "dry_run": False,
        "days": 7,
    }


@pytest.mark.parametrize("same_profiles_dir", [True, False])
@patch("orchestration_pipelines_lib.scripts.dbt_wrapper._sync_outputs_to_gcs")
@patch("dbt.cli.main.dbtRunner")
def test_invoke_dbt_isolates_project_copy_for_deps(
    mock_dbt_runner_cls, mock_sync_outputs, tmp_path, same_profiles_dir
):
    """Tests isolated local copy, profiles_dir handling, ignore patterns, symlinks, and GCS sync for deps."""
    proj_dir = tmp_path / "gcs_project"
    proj_dir.mkdir()
    (proj_dir / "dbt_project.yml").write_text("name: my_proj\n")
    (proj_dir / "packages.yml").write_text("packages: []\n")
    for ignored_dir in ("dbt_packages", "target", "logs"):
        stale_dir = proj_dir / ignored_dir
        stale_dir.mkdir()
        (stale_dir / "manifest.json").write_text('{"stale": true}')
    os.symlink(tmp_path / "does_not_exist", proj_dir / "dangling_link")

    prof_dir = proj_dir if same_profiles_dir else tmp_path / "profiles"
    prof_dir.mkdir(exist_ok=True)

    def _invoke_side_effect(cli_args):
        copied_dir = Path(cli_args[cli_args.index("--project-dir") + 1])
        if cli_args[0] == "deps":
            assert (copied_dir / "dbt_project.yml").exists()
            assert (copied_dir / "packages.yml").exists()
            for ignored_dir in ("dbt_packages", "target", "logs"):
                assert not (copied_dir / ignored_dir).exists()
            assert os.path.islink(copied_dir / "dangling_link")
            pkg_dir = copied_dir / "dbt_packages" / "demo_pkg"
            pkg_dir.mkdir(parents=True)
            (pkg_dir / "marker.txt").write_text("installed")
            (copied_dir / "package-lock.yml").write_text("locked")
        elif cli_args[0] == "run":
            assert (
                copied_dir / "dbt_packages" / "demo_pkg" / "marker.txt"
            ).exists()
        return MagicMock(success=True, result=None, exception=None)

    mock_runner = mock_dbt_runner_cls.return_value
    mock_runner.invoke.side_effect = _invoke_side_effect

    invoke_dbt(
        project_dir=str(proj_dir),
        profiles_dir=str(prof_dir),
        invocations=[{"command": "deps"}, {"command": "run"}],
        output_gcs_uri="gs://my-bucket/outputs/",
    )

    assert len(mock_runner.invoke.call_args_list) == 2
    deps_args, run_args = [c.args[0] for c in mock_runner.invoke.call_args_list]
    copied_dir = deps_args[deps_args.index("--project-dir") + 1]
    expected_profiles_dir = copied_dir if same_profiles_dir else str(prof_dir)

    assert copied_dir != str(proj_dir)
    assert run_args[run_args.index("--project-dir") + 1] == copied_dir
    assert (
        deps_args[deps_args.index("--profiles-dir") + 1]
        == expected_profiles_dir
    )
    assert (
        run_args[run_args.index("--profiles-dir") + 1] == expected_profiles_dir
    )
    assert not (proj_dir / "dbt_packages" / "demo_pkg").exists()
    assert not (proj_dir / "package-lock.yml").exists()
    mock_sync_outputs.assert_called_once()
    assert os.path.basename(mock_sync_outputs.call_args.args[0]) == "target"


@patch("dbt.cli.main.dbtRunner")
def test_invoke_dbt_restores_cwd_between_invocations_and_uses_abs_paths(
    mock_dbt_runner_cls, tmp_path, monkeypatch
):
    """Tests that cwd is restored after each invocation and paths are absolute."""
    proj_dir = tmp_path / "rel_proj"
    prof_dir = tmp_path / "rel_prof"
    other_dir = tmp_path / "elsewhere"
    proj_dir.mkdir()
    prof_dir.mkdir()
    other_dir.mkdir()

    monkeypatch.chdir(tmp_path)
    cwds_before_invoke: list[str] = []

    def _invoke_side_effect(cli_args):
        cwds_before_invoke.append(os.getcwd())
        if cli_args[0] == "deps":
            os.chdir(other_dir)
        return MagicMock(success=True, result=None, exception=None)

    mock_runner = mock_dbt_runner_cls.return_value
    mock_runner.invoke.side_effect = _invoke_side_effect

    invoke_dbt(
        project_dir="rel_proj",
        profiles_dir="rel_prof",
        invocations=[{"command": "deps"}, {"command": "run"}],
    )

    assert cwds_before_invoke == [str(tmp_path), str(tmp_path)]
    assert os.getcwd() == str(tmp_path)
    for inv_call in mock_runner.invoke.call_args_list:
        args = inv_call.args[0]
        assert os.path.isabs(args[args.index("--project-dir") + 1])
        assert args[args.index("--profiles-dir") + 1] == str(prof_dir)


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


@patch("dbt.cli.main.dbtRunner")
def test_invoke_dbt_logs_unique_id_and_handles_none_message_for_run_operation(
    mock_dbt_runner_cls, caplog
):
    """Tests RunResultOutput logging fallback to unique_id and None message handling."""
    mock_runner = mock_dbt_runner_cls.return_value
    ok_item = SimpleNamespace(
        unique_id="macro.my_proj.grant_select",
        status="success",
        message=None,
    )
    mock_runner.invoke.return_value = MagicMock(
        success=True, result=[ok_item], exception=None
    )

    with caplog.at_level(logging.INFO, logger="airflow.task"):
        invoke_dbt(
            project_dir="/proj",
            profiles_dir="/prof",
            invocations=[{"command": "run-operation", "macro": "grant_select"}],
        )

    assert "• macro.my_proj.grant_select: success" in caplog.text

    caplog.clear()
    failed_no_msg = SimpleNamespace(
        unique_id="macro.my_proj.failing_macro",
        status="error",
        message=None,
    )
    failed_unknown = SimpleNamespace(status="error", message=None)
    mock_runner.invoke.return_value = MagicMock(
        success=False,
        result=[failed_no_msg, failed_unknown],
        exception=None,
    )

    with caplog.at_level(logging.ERROR, logger="airflow.task"):
        with pytest.raises(
            RuntimeError,
            match="dbt run-operation failed. Check logs for details.",
        ):
            invoke_dbt(
                project_dir="/proj",
                profiles_dir="/prof",
                invocations=[
                    {"command": "run-operation", "macro": "failing_macro"}
                ],
            )

    assert "❌ Failed: macro.my_proj.failing_macro (error)" in caplog.text
    assert "❌ Failed: unknown (error)" in caplog.text
    assert "None" not in caplog.text


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
