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
import tempfile
from collections.abc import Iterable
from typing import Any


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
    cli_args = ["source", "freshness"] if command == "freshness" else [command]
    cli_args.extend(
        [
            "--project-dir",
            project_dir,
            "--profiles-dir",
            profiles_dir,
        ]
    )

    if target_path:
        cli_args.extend(["--target-path", target_path])

    if log_path:
        cli_args.extend(["--log-path", log_path])

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


def invoke_dbt(
    project_dir: str,
    profiles_dir: str,
    select_models: list | None = None,
    params: dict | None = None,
    invocations: list[dict[str, Any]] | None = None,
    output_gcs_uri: str | None = None,
):
    """Wraps dbtRunner to execute one or more dbt commands programmatically.

    Args:
        project_dir: The directory containing the dbt project.
        profiles_dir: The directory containing the dbt profiles.yml file.
        select_models: An optional legacy list of specific dbt models to run.
        params: An optional dictionary of dbt parameters/variables.
        invocations: An optional ordered list of structured dbt invocations.
        output_gcs_uri: An optional GCS URI to upload target and log artifacts.

    Raises:
        RuntimeError: If any dbt invocation fails or encounters an exception.
    """
    from dbt.cli.main import dbtRunner, dbtRunnerResult

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

    with tempfile.TemporaryDirectory() as tmp_dir:
        target_dir = os.path.join(tmp_dir, "target")
        log_dir = os.path.join(tmp_dir, "logs")
        try:
            for inv in normalized_invocations:
                command_name = inv.get("command")
                cli_args = _build_cli_args(
                    inv,
                    project_dir,
                    profiles_dir,
                    default_params=params,
                    target_path=target_dir,
                    log_path=log_dir,
                )

                logger.info("Running command args: %s", cli_args)

                res: dbtRunnerResult = dbt.invoke(cli_args)

                if res.success:
                    logger.info(
                        "✅ dbt %s finished successfully!", command_name
                    )
                    if isinstance(res.result, Iterable):
                        for r in res.result:
                            node_name = getattr(
                                getattr(r, "node", None), "name", None
                            )
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
                                node_name = getattr(
                                    getattr(r, "node", None), "name", "unknown"
                                )
                                message = getattr(r, "message", "")
                                logger.error(
                                    "❌ Failed: %s - %s", node_name, message
                                )
                    raise RuntimeError(
                        f"dbt {command_name} failed. Check logs for details."
                    )
        finally:
            if output_gcs_uri:
                try:
                    _sync_outputs_to_gcs(tmp_dir, output_gcs_uri, logger)
                except Exception as e:  # pylint: disable=broad-exception-caught
                    logger.error(
                        "Failed to sync dbt outputs to %s: %s",
                        output_gcs_uri,
                        e,
                    )
