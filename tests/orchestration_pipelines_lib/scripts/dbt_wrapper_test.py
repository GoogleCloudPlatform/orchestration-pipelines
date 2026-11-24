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


def test_build_knowledge_catalog_cmd_defaults():
    """Tests _build_knowledge_catalog_cmd builds expected gcloud CLI flags."""
    from orchestration_pipelines_lib.scripts.dbt_wrapper import (
        _build_knowledge_catalog_cmd,
    )

    cmd = _build_knowledge_catalog_cmd(
        "/tmp/dbt/target",
        {
            "project_id": "my-proj",
            "location": "us-central1",
            "entry_group": "my-dbt-group",
            "storage_uri": "gs://my-bucket/dbt-imports",
        },
    )

    assert cmd == [
        "gcloud",
        "dataplex",
        "dbt",
        "metadata-jobs",
        "create",
        "--project=my-proj",
        "--location=us-central1",
        "--artifacts-path=/tmp/dbt/target",
        "--entry-group=my-dbt-group",
        "--storage-uri=gs://my-bucket/dbt-imports/",
        "--quiet",
        "--async",
    ]


def test_build_knowledge_catalog_cmd_all_options():
    """Tests _build_knowledge_catalog_cmd honors all optional flags."""
    from orchestration_pipelines_lib.scripts.dbt_wrapper import (
        _build_knowledge_catalog_cmd,
    )

    cmd = _build_knowledge_catalog_cmd(
        "/tmp/dbt/target",
        {
            "project_id": "my-proj",
            "location": "europe-west1",
            "entry_group": "my-dbt-group",
            "storage_uri": "gs://my-bucket/dbt-imports///",
            "aspects_only": True,
            "skip_bigquery_link": True,
            "include_entry_links": False,
            "async_mode": False,
        },
    )

    assert cmd == [
        "gcloud",
        "dataplex",
        "dbt",
        "metadata-jobs",
        "create",
        "--project=my-proj",
        "--location=europe-west1",
        "--artifacts-path=/tmp/dbt/target",
        "--entry-group=my-dbt-group",
        "--storage-uri=gs://my-bucket/dbt-imports/",
        "--quiet",
        "--aspects-only",
        "--skip-bigquery-link",
        "--no-include-entry-links",
    ]


@patch("orchestration_pipelines_lib.scripts.dbt_wrapper.subprocess.run")
def test_ingest_metadata_skips_when_manifest_missing(mock_run, tmp_path, caplog):
    """Tests ingestion skips cleanly when manifest.json was not produced."""
    from orchestration_pipelines_lib.scripts.dbt_wrapper import (
        _ingest_metadata_to_knowledge_catalog,
    )

    target_dir = tmp_path / "target"
    target_dir.mkdir()
    with caplog.at_level(logging.WARNING, logger="airflow.task"):
        _ingest_metadata_to_knowledge_catalog(
            str(target_dir),
            {
                "project_id": "my-proj",
                "location": "us-central1",
                "entry_group": "my-group",
                "storage_uri": "gs://my-bucket/imports/",
            },
            logging.getLogger("airflow.task"),
        )

    mock_run.assert_not_called()
    assert "manifest.json not found" in caplog.text


@patch("orchestration_pipelines_lib.scripts.dbt_wrapper.subprocess.run")
def test_ingest_metadata_happy_path_runs_single_gcloud_call(
    mock_run, tmp_path, caplog
):
    """Tests happy path invokes gcloud directly without extra --version call."""
    from orchestration_pipelines_lib.scripts.dbt_wrapper import (
        _GCLOUD_ASYNC_TIMEOUT_SECONDS,
        _GCLOUD_SYNC_TIMEOUT_SECONDS,
        _ingest_metadata_to_knowledge_catalog,
    )

    target_dir = tmp_path / "target"
    target_dir.mkdir()
    (target_dir / "manifest.json").write_text('{"nodes": {}}')

    config = {
        "project_id": "my-proj",
        "location": "us-central1",
        "entry_group": "my-group",
        "storage_uri": "gs://my-bucket/imports/",
    }
    logger = logging.getLogger("airflow.task")

    mock_run.return_value = SimpleNamespace(
        returncode=0,
        stdout="name: projects/my-proj/locations/us-central1/metadataJobs/job-1\n",
        stderr="Create request issued.\n",
    )
    with caplog.at_level(logging.INFO, logger="airflow.task"):
        _ingest_metadata_to_knowledge_catalog(str(target_dir), config, logger)

    mock_run.assert_called_once_with(
        [
            "gcloud",
            "dataplex",
            "dbt",
            "metadata-jobs",
            "create",
            "--project=my-proj",
            "--location=us-central1",
            f"--artifacts-path={target_dir}",
            "--entry-group=my-group",
            "--storage-uri=gs://my-bucket/imports/",
            "--quiet",
            "--async",
        ],
        capture_output=True,
        text=True,
        check=False,
        timeout=_GCLOUD_ASYNC_TIMEOUT_SECONDS,
    )
    assert "Successfully triggered Dataplex Knowledge Catalog" in caplog.text

    caplog.clear()
    mock_run.reset_mock()
    sync_config = {**config, "async_mode": False}
    with caplog.at_level(logging.INFO, logger="airflow.task"):
        _ingest_metadata_to_knowledge_catalog(
            str(target_dir), sync_config, logger
        )
    assert mock_run.call_args.kwargs["timeout"] == _GCLOUD_SYNC_TIMEOUT_SECONDS
    assert "Successfully completed Dataplex Knowledge Catalog" in caplog.text


@patch("orchestration_pipelines_lib.scripts.dbt_wrapper.subprocess.run")
def test_ingest_metadata_missing_gcloud_binary_raises(mock_run, tmp_path):
    """Tests FileNotFoundError raises clear worker upgrade message."""
    from orchestration_pipelines_lib.scripts.dbt_wrapper import (
        _ingest_metadata_to_knowledge_catalog,
    )

    target_dir = tmp_path / "target"
    target_dir.mkdir()
    (target_dir / "manifest.json").write_text('{"nodes": {}}')

    mock_run.side_effect = FileNotFoundError("gcloud not found")
    with pytest.raises(
        RuntimeError,
        match=(
            "gcloud CLI is not installed.*gcloud-cli >= 587\\.0\\.0\\..*"
            "Upgrade your worker/environment to have gcloud-cli 587\\.0\\.0\\+"
        ),
    ):
        _ingest_metadata_to_knowledge_catalog(
            str(target_dir),
            {
                "project_id": "my-proj",
                "location": "us-central1",
                "entry_group": "my-group",
                "storage_uri": "gs://my-bucket/imports/",
            },
            logging.getLogger("airflow.task"),
        )


@patch("orchestration_pipelines_lib.scripts.dbt_wrapper.subprocess.run")
def test_ingest_metadata_timeout_expired_raises(mock_run, tmp_path):
    """Tests subprocess.TimeoutExpired raises a bounded timeout RuntimeError."""
    import subprocess

    from orchestration_pipelines_lib.scripts.dbt_wrapper import (
        _GCLOUD_ASYNC_TIMEOUT_SECONDS,
        _ingest_metadata_to_knowledge_catalog,
    )

    target_dir = tmp_path / "target"
    target_dir.mkdir()
    (target_dir / "manifest.json").write_text('{"nodes": {}}')

    mock_run.side_effect = subprocess.TimeoutExpired(
        cmd=["gcloud"], timeout=_GCLOUD_ASYNC_TIMEOUT_SECONDS
    )
    with pytest.raises(
        RuntimeError,
        match=f"gcloud dataplex dbt metadata-jobs create timed out after {_GCLOUD_ASYNC_TIMEOUT_SECONDS}s",
    ):
        _ingest_metadata_to_knowledge_catalog(
            str(target_dir),
            {
                "project_id": "my-proj",
                "location": "us-central1",
                "entry_group": "my-group",
                "storage_uri": "gs://my-bucket/imports/",
            },
            logging.getLogger("airflow.task"),
        )


@patch("orchestration_pipelines_lib.scripts.dbt_wrapper.subprocess.run")
def test_ingest_metadata_invalid_choice_checks_version_lazily(
    mock_run, tmp_path
):
    """Tests invalid choice 'dbt' checks gcloud --version and surfaces upgrade hint."""
    from orchestration_pipelines_lib.scripts.dbt_wrapper import (
        _ingest_metadata_to_knowledge_catalog,
    )

    target_dir = tmp_path / "target"
    target_dir.mkdir()
    (target_dir / "manifest.json").write_text('{"nodes": {}}')

    config = {
        "project_id": "my-proj",
        "location": "us-central1",
        "entry_group": "my-group",
        "storage_uri": "gs://my-bucket/imports/",
    }
    logger = logging.getLogger("airflow.task")

    mock_run.side_effect = [
        SimpleNamespace(
            returncode=2,
            stdout="",
            stderr="ERROR: (gcloud.dataplex) Invalid choice: 'dbt'.",
        ),
        SimpleNamespace(
            returncode=0,
            stdout="Google Cloud SDK 586.0.0\ncore 2026.09.20\n",
            stderr="",
        ),
    ]
    with pytest.raises(
        RuntimeError,
        match=(
            "Installed gcloud-cli version 586\\.0\\.0 is older than the "
            "minimum required version 587\\.0\\.0.*"
            "Upgrade your worker/environment to have gcloud-cli 587\\.0\\.0\\+"
        ),
    ):
        _ingest_metadata_to_knowledge_catalog(str(target_dir), config, logger)

    assert mock_run.call_count == 2
    assert mock_run.call_args_list[1].args[0] == ["gcloud", "--version"]


@pytest.mark.parametrize(
    "version_effect",
    [
        SimpleNamespace(
            returncode=0, stdout="Google Cloud SDK 590.0.0\n", stderr=""
        ),
        SimpleNamespace(returncode=0, stdout="custom wrapper\n", stderr=""),
        SimpleNamespace(returncode=1, stdout="", stderr="err"),
        OSError("cannot run"),
    ],
)
@patch("orchestration_pipelines_lib.scripts.dbt_wrapper.subprocess.run")
def test_ingest_metadata_invalid_choice_falls_back_to_original_error(
    mock_run, tmp_path, version_effect
):
    """Tests original gcloud error surfaces if version probe does not find old SDK."""
    from orchestration_pipelines_lib.scripts.dbt_wrapper import (
        _ingest_metadata_to_knowledge_catalog,
    )

    target_dir = tmp_path / "target"
    target_dir.mkdir()
    (target_dir / "manifest.json").write_text('{"nodes": {}}')

    mock_run.side_effect = [
        SimpleNamespace(
            returncode=2,
            stdout="",
            stderr="ERROR: (gcloud.dataplex) Invalid choice: 'dbt'.",
        ),
        version_effect,
    ]
    with pytest.raises(
        RuntimeError,
        match="gcloud dataplex dbt metadata-jobs create failed with exit code 2.*Invalid choice: 'dbt'",
    ):
        _ingest_metadata_to_knowledge_catalog(
            str(target_dir),
            {
                "project_id": "my-proj",
                "location": "us-central1",
                "entry_group": "my-group",
                "storage_uri": "gs://my-bucket/imports/",
            },
            logging.getLogger("airflow.task"),
        )


@patch("orchestration_pipelines_lib.scripts.dbt_wrapper.subprocess.run")
def test_ingest_metadata_other_failure_does_not_probe_version(
    mock_run, tmp_path
):
    """Tests non-version errors (e.g. PERMISSION_DENIED) do not run gcloud --version."""
    from orchestration_pipelines_lib.scripts.dbt_wrapper import (
        _ingest_metadata_to_knowledge_catalog,
    )

    target_dir = tmp_path / "target"
    target_dir.mkdir()
    (target_dir / "manifest.json").write_text('{"nodes": {}}')

    mock_run.return_value = SimpleNamespace(
        returncode=1, stdout="", stderr="PERMISSION_DENIED: missing role"
    )
    with pytest.raises(
        RuntimeError,
        match="gcloud dataplex dbt metadata-jobs create failed with exit code 1.*PERMISSION_DENIED",
    ):
        _ingest_metadata_to_knowledge_catalog(
            str(target_dir),
            {
                "project_id": "my-proj",
                "location": "us-central1",
                "entry_group": "my-group",
                "storage_uri": "gs://my-bucket/imports/",
            },
            logging.getLogger("airflow.task"),
        )

    assert mock_run.call_count == 1


@patch(
    "orchestration_pipelines_lib.scripts.dbt_wrapper._ingest_metadata_to_knowledge_catalog"
)
@patch("dbt.cli.main.dbtRunner")
def test_invoke_dbt_triggers_knowledge_catalog_on_success_and_dbt_failure(
    mock_dbt_runner_cls, mock_ingest, tmp_path
):
    """Tests invoke_dbt runs Knowledge Catalog ingestion in finally even if dbt fails."""
    kc_config = {
        "project_id": "my-proj",
        "location": "us-central1",
        "entry_group": "my-group",
        "storage_uri": "gs://my-bucket/imports/",
    }
    mock_runner = mock_dbt_runner_cls.return_value
    mock_runner.invoke.return_value = MagicMock(
        success=True, result=[], exception=None
    )

    invoke_dbt(
        project_dir=str(tmp_path),
        profiles_dir=str(tmp_path),
        invocations=[{"command": "build"}],
        knowledge_catalog=kc_config,
    )
    mock_ingest.assert_called_once()
    assert mock_ingest.call_args.args[1] == kc_config

    mock_ingest.reset_mock()
    mock_runner.invoke.return_value = MagicMock(
        success=False, result=[], exception=None
    )
    with pytest.raises(RuntimeError, match="dbt build failed"):
        invoke_dbt(
            project_dir=str(tmp_path),
            profiles_dir=str(tmp_path),
            invocations=[{"command": "build"}],
            knowledge_catalog=kc_config,
        )
    mock_ingest.assert_called_once()


@patch(
    "orchestration_pipelines_lib.scripts.dbt_wrapper._ingest_metadata_to_knowledge_catalog"
)
@patch("dbt.cli.main.dbtRunner")
def test_invoke_dbt_propagates_kc_error_when_dbt_succeeds(
    mock_dbt_runner_cls, mock_ingest, tmp_path
):
    """Tests ingestion errors fail the task when dbt itself succeeded."""
    mock_runner = mock_dbt_runner_cls.return_value
    mock_runner.invoke.return_value = MagicMock(
        success=True, result=[], exception=None
    )
    mock_ingest.side_effect = RuntimeError("PERMISSION_DENIED")

    with pytest.raises(RuntimeError, match="PERMISSION_DENIED"):
        invoke_dbt(
            project_dir=str(tmp_path),
            profiles_dir=str(tmp_path),
            invocations=[{"command": "run"}],
            knowledge_catalog={
                "project_id": "my-proj",
                "location": "us-central1",
                "entry_group": "my-group",
                "storage_uri": "gs://my-bucket/imports/",
            },
        )


@patch(
    "orchestration_pipelines_lib.scripts.dbt_wrapper._ingest_metadata_to_knowledge_catalog"
)
@patch("dbt.cli.main.dbtRunner")
def test_invoke_dbt_suppresses_kc_error_when_dbt_already_failed(
    mock_dbt_runner_cls, mock_ingest, tmp_path, caplog
):
    """Tests ingestion errors are logged without masking the root dbt failure."""
    mock_runner = mock_dbt_runner_cls.return_value
    mock_runner.invoke.return_value = MagicMock(
        success=False, result=[], exception=None
    )
    mock_ingest.side_effect = RuntimeError("gcloud-cli 586.0.0 too old")

    with (
        caplog.at_level(logging.ERROR, logger="airflow.task"),
        pytest.raises(RuntimeError, match="dbt run failed"),
    ):
        invoke_dbt(
            project_dir=str(tmp_path),
            profiles_dir=str(tmp_path),
            invocations=[{"command": "run"}],
            knowledge_catalog={
                "project_id": "my-proj",
                "location": "us-central1",
                "entry_group": "my-group",
                "storage_uri": "gs://my-bucket/imports/",
            },
        )

    assert (
        "Failed to ingest dbt metadata into Knowledge Catalog: gcloud-cli 586.0.0 too old"
        in caplog.text
    )


@pytest.mark.parametrize(
    "exc_cls",
    [KeyboardInterrupt, TimeoutError],
)
@patch("orchestration_pipelines_lib.scripts.dbt_wrapper._sync_outputs_to_gcs")
@patch(
    "orchestration_pipelines_lib.scripts.dbt_wrapper._ingest_metadata_to_knowledge_catalog"
)
@patch("dbt.cli.main.dbtRunner")
def test_invoke_dbt_skips_finally_on_timeout_or_cancellation(
    mock_dbt_runner_cls, mock_ingest, mock_sync, tmp_path, exc_cls
):
    """Tests GCS sync and KC ingestion are skipped on timeout/cancellation."""
    mock_runner = mock_dbt_runner_cls.return_value
    mock_runner.invoke.side_effect = exc_cls("timed out")

    with pytest.raises(exc_cls):
        invoke_dbt(
            project_dir=str(tmp_path),
            profiles_dir=str(tmp_path),
            invocations=[{"command": "run"}],
            output_gcs_uri="gs://my-bucket/outputs/",
            knowledge_catalog={
                "project_id": "my-proj",
                "location": "us-central1",
                "entry_group": "my-group",
                "storage_uri": "gs://my-bucket/imports/",
            },
        )

    mock_sync.assert_not_called()
    mock_ingest.assert_not_called()
