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
"""Module to invoke dbt commands programmatically using dbtRunner."""

import json
import logging
import os
import re
import shutil
import subprocess
import tempfile
from collections.abc import Iterable
from typing import Any

MIN_GCLOUD_CLI_VERSION = (587, 0, 0)
_GCLOUD_VERSION_RE = re.compile(r"Google Cloud SDK\s+(\d+)\.(\d+)\.(\d+)")
_GCLOUD_VERSION_TIMEOUT_SECONDS = 30
_GCLOUD_ASYNC_TIMEOUT_SECONDS = 300
_GCLOUD_SYNC_TIMEOUT_SECONDS = 1800


def _build_cli_args(
    invocation: dict[str, Any],
    project_dir: str,
    profiles_dir: str,
    default_params: dict[str, Any] | None = None,
    target_path: str | None = None,
    log_path: str | None = None,
) -> list[str]:
    """Builds the CLI argument list for a single dbtRunner invocation."""
    command = invocation.get("command")
    if command in ("freshness", "source freshness"):
        cli_args = ["source", "freshness"]
    elif command == "run-operation":
        macro = invocation.get("macro")
        cli_args = ["run-operation", macro] if macro else ["run-operation"]
    else:
        cli_args = [command]

    cli_args.extend(
        [
            "--project-dir",
            project_dir,
            "--profiles-dir",
            profiles_dir,
        ]
    )

    if target_path and command != "deps":
        cli_args.extend(["--target-path", target_path])

    if log_path:
        cli_args.extend(["--log-path", log_path])

    inv_args = invocation.get("args")
    if inv_args:
        cli_args.extend(["--args", json.dumps(inv_args)])

    select = invocation.get("select")
    if select:
        cli_args.extend(["--select", " ".join(select)])

    exclude = invocation.get("exclude")
    if exclude:
        cli_args.extend(["--exclude", " ".join(exclude)])

    inv_vars = invocation.get("vars")
    merged_vars = (default_params or {}) | (inv_vars or {})
    if merged_vars:
        cli_args.extend(["--vars", json.dumps(merged_vars)])

    if invocation.get("full_refresh"):
        cli_args.append("--full-refresh")

    if invocation.get("fail_fast"):
        cli_args.append("--fail-fast")

    threads = invocation.get("threads")
    if threads and threads > 0:
        cli_args.extend(["--threads", str(threads)])

    target = invocation.get("target")
    if target:
        cli_args.extend(["--target", target])

    extra_args = invocation.get("extra_args")
    if extra_args:
        cli_args.extend(extra_args)

    return cli_args


def _sync_outputs_to_gcs(
    local_dir: str,
    output_gcs_uri: str,
    logger: logging.Logger,
) -> None:
    """Uploads DBT target and log artifacts from local_dir to output_gcs_uri."""
    if not output_gcs_uri.startswith("gs://"):
        raise ValueError(f"Invalid GCS URI format: {output_gcs_uri}")

    parts = output_gcs_uri[5:].split("/", 1)
    bucket_name = parts[0]
    prefix = parts[1].rstrip("/") if len(parts) > 1 else ""

    from google.cloud import storage

    client = storage.Client()
    bucket = client.bucket(bucket_name)

    for root, _, files in os.walk(local_dir):
        for filename in files:
            if filename in ["manifest.json", "run_results.json"]:
                local_file = os.path.join(root, filename)
                blob_name = f"{prefix}/{filename}" if prefix else filename
                bucket.blob(blob_name).upload_from_filename(local_file)

    logger.info("📦 Synced dbt output artifacts to %s", output_gcs_uri)


def _format_min_gcloud_version() -> str:
    """Formats MIN_GCLOUD_CLI_VERSION as a dotted version string."""
    return ".".join(map(str, MIN_GCLOUD_CLI_VERSION))


def _get_old_gcloud_version_error() -> str | None:
    """Checks gcloud version after a CLI failure and returns an upgrade hint
    if the installed SDK is older than MIN_GCLOUD_CLI_VERSION.
    """
    try:
        result = subprocess.run(
            ["gcloud", "--version"],
            capture_output=True,
            text=True,
            check=False,
            timeout=_GCLOUD_VERSION_TIMEOUT_SECONDS,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None

    if result.returncode != 0:
        return None

    match = _GCLOUD_VERSION_RE.search(result.stdout or "")
    if not match:
        return None

    version = tuple(int(part) for part in match.groups())
    if version < MIN_GCLOUD_CLI_VERSION:
        installed = ".".join(map(str, version))
        required = _format_min_gcloud_version()
        return (
            f"Installed gcloud-cli version {installed} is older than the "
            f"minimum required version {required} for "
            "`gcloud dataplex dbt metadata-jobs create`. Upgrade your "
            f"worker/environment to have gcloud-cli {required}+."
        )
    return None


def _is_timeout_or_cancellation(exc: BaseException | None) -> bool:
    """Returns True when exc represents a task timeout or cancellation."""
    if exc is None:
        return False
    return not isinstance(exc, Exception) or isinstance(exc, TimeoutError)


def _build_knowledge_catalog_cmd(
    target_dir: str,
    config: dict[str, Any],
) -> list[str]:
    """Builds the `gcloud dataplex dbt metadata-jobs create` command."""
    normalized_storage_uri = f"{config['storage_uri'].rstrip('/')}/"

    cmd = [
        "gcloud",
        "dataplex",
        "dbt",
        "metadata-jobs",
        "create",
        f"--project={config['project_id']}",
        f"--location={config['location']}",
        f"--artifacts-path={target_dir}",
        f"--entry-group={config['entry_group']}",
        f"--storage-uri={normalized_storage_uri}",
        "--quiet",
    ]

    if config.get("aspects_only"):
        cmd.append("--aspects-only")
    if config.get("skip_bigquery_link"):
        cmd.append("--skip-bigquery-link")
    if config.get("include_entry_links") is False:
        cmd.append("--no-include-entry-links")
    if config.get("async_mode", True):
        cmd.append("--async")

    return cmd


def _ingest_metadata_to_knowledge_catalog(
    target_dir: str,
    config: dict[str, Any],
    logger: logging.Logger,
) -> None:
    """Ingests dbt artifacts from target_dir into Dataplex Knowledge Catalog."""
    manifest_path = os.path.join(target_dir, "manifest.json")
    if not os.path.isfile(manifest_path):
        logger.warning(
            "Skipping Knowledge Catalog metadata ingestion: manifest.json "
            "not found in %s.",
            target_dir,
        )
        return

    cmd = _build_knowledge_catalog_cmd(target_dir, config)
    async_mode = config.get("async_mode", True)
    timeout = (
        _GCLOUD_ASYNC_TIMEOUT_SECONDS
        if async_mode
        else _GCLOUD_SYNC_TIMEOUT_SECONDS
    )
    logger.info(
        "Ingesting dbt metadata into Knowledge Catalog with command: %s",
        " ".join(cmd),
    )

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=False,
            timeout=timeout,
        )
    except FileNotFoundError as exc:
        required = _format_min_gcloud_version()
        raise RuntimeError(
            "gcloud CLI is not installed in the worker environment. "
            "Dataplex Knowledge Catalog metadata ingestion requires "
            f"gcloud-cli >= {required}. Upgrade your worker/environment "
            f"to have gcloud-cli {required}+."
        ) from exc
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError(
            "gcloud dataplex dbt metadata-jobs create timed out after "
            f"{timeout}s."
        ) from exc

    if result.stdout:
        logger.info("gcloud stdout:\n%s", result.stdout.strip())
    if result.returncode != 0:
        stderr_msg = result.stderr.strip() if result.stderr else ""
        if "invalid choice: 'dbt'" in stderr_msg.lower():
            version_err = _get_old_gcloud_version_error()
            if version_err:
                raise RuntimeError(version_err)
        raise RuntimeError(
            f"gcloud dataplex dbt metadata-jobs create failed with exit "
            f"code {result.returncode}: {stderr_msg}"
        )
    if result.stderr:
        logger.info("gcloud stderr:\n%s", result.stderr.strip())
    action = "triggered" if async_mode else "completed"
    logger.info(
        "✅ Successfully %s Dataplex Knowledge Catalog metadata "
        "ingestion for entry group '%s'.",
        action,
        config["entry_group"],
    )


def _get_result_node_name(result: Any) -> str | None:
    """Extracts a display name from a dbt result item."""
    node = getattr(result, "node", None)
    return getattr(node, "name", None) or getattr(result, "unique_id", None)


def invoke_dbt(
    project_dir: str,
    profiles_dir: str,
    select_models: list | None = None,
    params: dict | None = None,
    invocations: list[dict[str, Any]] | None = None,
    output_gcs_uri: str | None = None,
    knowledge_catalog: dict[str, Any] | None = None,
):
    """Wraps dbtRunner to execute one or more dbt commands programmatically.

    Args:
        project_dir: The directory containing the dbt project.
        profiles_dir: The directory containing the dbt profiles.yml file.
        select_models: An optional legacy list of specific dbt models to run.
        params: An optional dictionary of dbt parameters/variables.
        invocations: An optional ordered list of structured dbt invocations.
        output_gcs_uri: An optional GCS URI to upload target and log artifacts.
        knowledge_catalog: Optional Dataplex Knowledge Catalog ingestion
            configuration executed via ``gcloud dataplex dbt metadata-jobs
            create`` (requires ``gcloud-cli >= 587.0.0``).

    Raises:
        RuntimeError: If any dbt invocation fails or encounters an exception.
    """
    from dbt.cli.main import dbtRunner, dbtRunnerResult

    project_dir = os.path.abspath(project_dir)
    profiles_dir = os.path.abspath(profiles_dir)

    logger = logging.getLogger("airflow.task")
    logger.info("🚀 Initializing dbtRunner for project: %s", project_dir)

    dbt = dbtRunner()

    normalized_invocations = invocations or [
        {
            "command": "run",
            "select": select_models,
            "vars": params,
        }
    ]
    has_deps = any(
        inv.get("command") == "deps" for inv in normalized_invocations
    )

    orig_cwd = os.getcwd()
    with tempfile.TemporaryDirectory() as tmp_dir:
        effective_project_dir = project_dir
        effective_profiles_dir = profiles_dir
        if has_deps:
            effective_project_dir = os.path.join(tmp_dir, "project")
            logger.info(
                "Copying dbt project from %s to %s for isolated dbt deps "
                "execution",
                project_dir,
                effective_project_dir,
            )
            shutil.copytree(
                project_dir,
                effective_project_dir,
                symlinks=True,
                ignore=shutil.ignore_patterns("dbt_packages", "target", "logs"),
            )
            if effective_profiles_dir == project_dir:
                effective_profiles_dir = effective_project_dir

        target_dir = os.path.join(tmp_dir, "target")
        log_dir = os.path.join(tmp_dir, "logs")
        dbt_exc: BaseException | None = None
        try:
            for inv in normalized_invocations:
                command_name = inv.get("command")
                cli_args = _build_cli_args(
                    inv,
                    effective_project_dir,
                    effective_profiles_dir,
                    default_params=params,
                    target_path=target_dir,
                    log_path=log_dir,
                )

                logger.info("Running command args: %s", cli_args)

                try:
                    res: dbtRunnerResult = dbt.invoke(cli_args)
                finally:
                    os.chdir(orig_cwd)

                if res.success:
                    logger.info(
                        "✅ dbt %s finished successfully!", command_name
                    )
                    if isinstance(res.result, Iterable):
                        for r in res.result:
                            node_name = _get_result_node_name(r)
                            status = getattr(r, "status", None)
                            if node_name is not None and status is not None:
                                logger.info("   • %s: %s", node_name, status)
                else:
                    if res.exception:
                        logger.error("❌ System Exception: %s", res.exception)
                    if isinstance(res.result, Iterable):
                        for r in res.result:
                            status = getattr(r, "status", None)
                            if status and status != "success":
                                node_name = (
                                    _get_result_node_name(r) or "unknown"
                                )
                                message = getattr(r, "message", None)
                                if message:
                                    logger.error(
                                        "❌ Failed: %s - %s",
                                        node_name,
                                        message,
                                    )
                                else:
                                    logger.error(
                                        "❌ Failed: %s (%s)",
                                        node_name,
                                        status,
                                    )
                    raise RuntimeError(
                        f"dbt {command_name} failed. Check logs for details."
                    )
        except BaseException as exc:
            dbt_exc = exc
            raise
        finally:
            if not _is_timeout_or_cancellation(dbt_exc):
                if output_gcs_uri:
                    try:
                        _sync_outputs_to_gcs(target_dir, output_gcs_uri, logger)
                    except Exception as e:  # pylint: disable=broad-exception-caught
                        logger.error(
                            "Failed to sync dbt outputs to %s: %s",
                            output_gcs_uri,
                            e,
                        )
                if knowledge_catalog:
                    try:
                        _ingest_metadata_to_knowledge_catalog(
                            target_dir, knowledge_catalog, logger
                        )
                    except Exception as e:  # pylint: disable=broad-exception-caught
                        if dbt_exc is not None:
                            logger.error(
                                "Failed to ingest dbt metadata into Knowledge "
                                "Catalog: %s",
                                e,
                            )
                        else:
                            raise
