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
"""Task output harvesting and tiered 10KB-1MB XCom/GCS envelope storage."""

from __future__ import annotations

import datetime
import decimal
import importlib
import json
import logging
import math
import os
import pathlib
from collections.abc import Mapping
from typing import Any, TypedDict

from google.api_core import exceptions as gcp_exceptions

from orchestration_pipelines_lib.utils import file_manager

logger = logging.getLogger(__name__)

XCOM_DB_THRESHOLD_BYTES = 10 * 1024
XCOM_MAX_LIMIT_BYTES = 1024 * 1024
OFFLOAD_URI_KEY = "__op_gcs_offload_uri__"


class BigQueryMetadata(TypedDict):
    """Harvested `.metadata` fields for BigQuery SQL actions."""

    job_id: str | None
    rows_written: int
    bytes_billed: int


class DataprocMetadata(TypedDict, total=False):
    """Harvested `.metadata` fields for Dataproc Batch or GCE Job actions."""

    batch_id: str | None
    job_id: str | None


class DataformMetadata(TypedDict):
    """Harvested `.metadata` fields for Dataform Service actions."""

    invocation_id: str | None
    failed_actions: list[str]
    successful_actions: list[str]


class DbtMetadata(TypedDict):
    """Harvested `.metadata` fields for dbt actions."""

    run_id: str
    failed_actions: list[str]
    successful_actions: list[str]


class BigQueryDtsMetadata(TypedDict):
    """Harvested `.metadata` fields for BigQuery Data Transfer Service actions."""

    run_id: str | None
    run_state: str
    start_time: Any
    end_time: Any


class TaskOutputEnvelope(TypedDict):
    """Unpacked task output envelope containing `.value` and `.metadata`."""

    value: Any
    metadata: dict[str, Any]


def _get_field(source: Any, key: str, default: Any = None) -> Any:
    if isinstance(source, Mapping):
        return source.get(key, default)
    return getattr(source, key, default)


def _extract_bucket(raw_uri: str | None, *, require_gs: bool = False) -> str:
    if not raw_uri or (require_gs and not raw_uri.startswith("gs://")):
        return ""
    return raw_uri.removeprefix("gs://").strip("/").split("/", 1)[0]


def _sanitize_segment(value: Any) -> str:
    """Sanitizes a value for use as a safe GCS path segment."""
    segment = str(value).replace("/", "_")
    return "_" if segment in ("", ".", "..") else segment


def _build_task_output_uri(
    bucket: str, operator: Any, context: Mapping[str, Any], filename: str
) -> str:
    dag_id = (
        getattr(operator, "dag_id", None)
        or _get_field(getattr(operator, "dag", None), "dag_id")
        or "unknown_dag"
    )
    run_id = (
        context.get("run_id")
        or _get_field(context.get("dag_run"), "run_id")
        or "unknown_run"
    )
    task_id = getattr(operator, "task_id", None) or "unknown_task"
    task_instance = context.get("ti") or context.get("task_instance")
    try_number = int(getattr(task_instance, "try_number", None) or 1)
    path = pathlib.PurePosixPath(
        "data",
        "task_outputs",
        _sanitize_segment(dag_id),
        _sanitize_segment(run_id),
        _sanitize_segment(task_id),
        str(try_number),
        filename,
    )
    return f"gs://{bucket}/{path}"


def _gcs_blob(gcs_uri: str) -> Any:
    if not gcs_uri.startswith("gs://"):
        raise ValueError(f"Expected gs:// URI, got '{gcs_uri}'.")
    bucket_name, blob_name = gcs_uri.removeprefix("gs://").split("/", 1)
    return file_manager._get_gcs_client().bucket(bucket_name).blob(blob_name)


def _raise_if_exceeds_limit(size_bytes: int, task_id: str) -> None:
    if size_bytes > XCOM_MAX_LIMIT_BYTES:
        raise ValueError(
            f"Task '{task_id}' output size ({size_bytes} bytes) exceeds the "
            f"1 MB limit ({XCOM_MAX_LIMIT_BYTES} bytes). Write large datasets "
            "to a table and pass the table name instead."
        )


def normalize_json_value(value: Any) -> Any:
    """Recursively converts Python values into RFC 8259 JSON-safe primitives."""
    if value is None or isinstance(value, (str, int, bool)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, (datetime.date, datetime.time)):
        return value.isoformat()
    if isinstance(value, decimal.Decimal):
        if not value.is_finite():
            return None
        return (
            int(value) if value == value.to_integral_value() else float(value)
        )
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    if isinstance(value, Mapping):
        return {str(k): normalize_json_value(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [normalize_json_value(item) for item in value]
    if isinstance(value, (set, frozenset)):
        return sorted(
            (normalize_json_value(item) for item in value),
            key=lambda item: (type(item).__name__, str(item)),
        )
    return str(value)


def pack_output_envelope(
    value: Any,
    metadata: Mapping[str, Any] | None,
    context: Mapping[str, Any],
    operator: Any,
) -> dict[str, Any]:
    """Packs task `.value` and `.metadata` with 10KB-1MB GCS offloading."""
    envelope = {
        "value": normalize_json_value(value),
        "metadata": normalize_json_value(metadata or {}),
    }
    serialized = json.dumps(
        envelope, allow_nan=False, separators=(",", ":")
    ).encode("utf-8")
    _raise_if_exceeds_limit(
        len(serialized), str(getattr(operator, "task_id", None) or "")
    )

    gcs_bucket = _extract_bucket(os.environ.get("GCS_BUCKET"))
    if len(serialized) >= XCOM_DB_THRESHOLD_BYTES and gcs_bucket:
        gcs_uri = _build_task_output_uri(
            gcs_bucket, operator, context, "xcom_offload.json"
        )
        _gcs_blob(gcs_uri).upload_from_string(
            serialized, content_type="application/json"
        )
        return {OFFLOAD_URI_KEY: gcs_uri}
    return envelope


def unpack_output_envelope(raw_xcom: Any) -> TaskOutputEnvelope | None:
    """Unpacks an XCom `return_value` envelope, resolving GCS pointers."""
    match raw_xcom:
        case None:
            return None
        case Mapping() if (
            offload_uri := raw_xcom.get(OFFLOAD_URI_KEY)
        ) is not None:
            try:
                return json.loads(
                    _gcs_blob(str(offload_uri)).download_as_bytes()
                )
            except gcp_exceptions.NotFound as err:
                logger.warning(
                    "Could not read offloaded XCom at '%s': %s",
                    offload_uri,
                    err,
                )
                return None
        case {"value": val, "metadata": Mapping() as meta} if (
            len(raw_xcom) == 2
        ):
            return {"value": val, "metadata": dict(meta)}
        case _:
            return {"value": normalize_json_value(raw_xcom), "metadata": {}}


def inject_dataproc_output_uri(
    operator: Any, context: Mapping[str, Any]
) -> None:
    """Injects `PIPELINE_OUTPUT_URI` Spark properties for PySpark/Notebooks."""
    action_model = getattr(operator, "_op_action_model", None)
    if not getattr(operator, "_op_is_output_task", True) or getattr(
        action_model, "type", None
    ) not in ("pyspark", "notebook"):
        return

    bucket = (
        _extract_bucket(getattr(action_model, "depsBucket", None))
        or _extract_bucket(
            getattr(action_model, "filename", None), require_gs=True
        )
        or _extract_bucket(os.environ.get("GCS_BUCKET"))
    )
    if not bucket:
        return

    if isinstance(batch := getattr(operator, "batch", None), dict):
        container, sub_key = batch, "runtime_config"
    elif isinstance(job := getattr(operator, "job", None), dict):
        container, sub_key = job, "pyspark_job"
    else:
        return

    output_uri = _build_task_output_uri(
        bucket, operator, context, "output.json"
    )
    operator._op_pipeline_output_uri = output_uri
    spark_props = {
        "spark.dataproc.driverEnv.PIPELINE_OUTPUT_URI": output_uri,
        "spark.driverEnv.PIPELINE_OUTPUT_URI": output_uri,
        "spark.yarn.appMasterEnv.PIPELINE_OUTPUT_URI": output_uri,
    }
    sub_config = container.get(sub_key) or {}
    container[sub_key] = {
        **sub_config,
        "properties": {**(sub_config.get("properties") or {}), **spark_props},
    }


def _resolve_gcp_hook(
    operator: Any, module_path: str, class_name: str, **extra_kwargs: Any
) -> Any:
    if (hook := getattr(operator, "hook", None)) is not None:
        return hook
    conn_id = getattr(operator, "gcp_conn_id", None) or getattr(
        operator, "gcp_cloud_conn_id", None
    )
    if not isinstance(conn_id, str):
        return None
    hook_cls = getattr(importlib.import_module(module_path), class_name)
    return hook_cls(
        gcp_conn_id=conn_id,
        impersonation_chain=getattr(operator, "impersonation_chain", None),
        **extra_kwargs,
    )


def _extract_bigquery_output(
    operator: Any, raw_result: Any, fetch_value: bool, failed: bool
) -> tuple[Any, BigQueryMetadata]:
    job_id = (
        raw_result
        if isinstance(raw_result, str) and raw_result
        else getattr(operator, "job_id", None)
    )
    metadata: BigQueryMetadata = {
        "job_id": job_id,
        "rows_written": 0,
        "bytes_billed": 0,
    }
    if not job_id:
        return None, metadata

    try:
        hook = _resolve_gcp_hook(
            operator,
            "airflow.providers.google.cloud.hooks.bigquery",
            "BigQueryHook",
        )
        if hook is None:
            return None, metadata
        job = hook.get_job(
            job_id=job_id,
            project_id=getattr(operator, "project_id", None),
            location=getattr(operator, "location", None),
        )
        metadata["rows_written"] = int(
            getattr(job, "num_dml_affected_rows", 0) or 0
        )
        metadata["bytes_billed"] = int(
            getattr(job, "total_bytes_billed", 0) or 0
        )
        if fetch_value and not failed:
            row = next(iter(job.result(max_results=1)), None)
            return (dict(row) if row is not None else {}), metadata
    except Exception as err:  # pylint: disable=broad-exception-caught
        logger.warning(
            "Could not extract BigQuery output for job '%s': %s", job_id, err
        )
    return None, metadata


def _extract_dataproc_output(
    operator: Any, raw_result: Any, failed: bool
) -> tuple[Any, DataprocMetadata]:
    if hasattr(operator, "batch_id"):
        name = str(_get_field(raw_result, "name") or "")
        metadata: DataprocMetadata = {
            "batch_id": name.rsplit("/", 1)[-1]
            or getattr(operator, "batch_id", None)
        }
    else:
        job_id = (
            raw_result
            if isinstance(raw_result, str) and raw_result
            else getattr(operator, "job_id", None)
        )
        metadata = {"job_id": job_id}

    output_uri = getattr(operator, "_op_pipeline_output_uri", None)
    if not output_uri or failed:
        return None, metadata

    try:
        blob = _gcs_blob(output_uri)
        if not blob.exists():
            return None, metadata
        if getattr(blob, "size", None) is None and hasattr(blob, "reload"):
            blob.reload()
        blob_size = getattr(blob, "size", None)
        payload = (
            b""
            if isinstance(blob_size, int) and blob_size > XCOM_MAX_LIMIT_BYTES
            else blob.download_as_bytes()
        )
    except Exception as err:  # pylint: disable=broad-exception-caught
        logger.warning(
            "Could not read Dataproc output from '%s': %s", output_uri, err
        )
        return None, metadata

    reported_size = blob_size if isinstance(blob_size, int) else 0
    _raise_if_exceeds_limit(
        max(reported_size, len(payload)),
        str(getattr(operator, "task_id", None) or ""),
    )
    try:
        return json.loads(payload), metadata
    except (UnicodeDecodeError, json.JSONDecodeError) as err:
        logger.warning(
            "Invalid JSON in Dataproc output '%s': %s", output_uri, err
        )
        return None, metadata


def _extract_dataform_metadata(
    operator: Any, context: Mapping[str, Any], raw_result: Any
) -> DataformMetadata:
    proj = getattr(operator, "project_id", None)
    region = getattr(operator, "region", None)
    repo_id = getattr(operator, "repository_id", None)
    raw_name = _get_field(raw_result, "name")
    inv_name = raw_name if isinstance(raw_name, str) and raw_name else None
    ti = context.get("ti") or context.get("task_instance")
    if not inv_name and callable(xcom_pull := getattr(ti, "xcom_pull", None)):
        conf = xcom_pull(
            task_ids=getattr(operator, "task_id", None),
            key="dataform_workflow_invocation_config",
        )
        if (inv_id := _get_field(conf, "workflow_invocation_id")) and all(
            (proj, region, repo_id)
        ):
            inv_name = (
                f"projects/{proj}/locations/{region}/repositories/"
                f"{repo_id}/workflowInvocations/{inv_id}"
            )

    failed_actions: list[str] = []
    successful_actions: list[str] = []
    if inv_name and repo_id:
        try:
            from google.cloud.dataform_v1beta1.types import (
                WorkflowInvocationAction,
            )

            hook = _resolve_gcp_hook(
                operator,
                "airflow.providers.google.cloud.hooks.dataform",
                "DataformHook",
            )
            if hook is not None:
                for item in hook.query_workflow_invocation_actions(
                    project_id=proj,
                    region=region,
                    repository_id=repo_id,
                    workflow_invocation_id=inv_name.rsplit("/", 1)[-1],
                ):
                    name = _get_field(_get_field(item, "target"), "name")
                    state = _get_field(item, "state")
                    state_name = (
                        WorkflowInvocationAction.State(state).name
                        if isinstance(state, int)
                        else getattr(state, "name", state)
                    )
                    if name and state_name == "FAILED":
                        failed_actions.append(str(name))
                    elif name and state_name == "SUCCEEDED":
                        successful_actions.append(str(name))
        except Exception as err:  # pylint: disable=broad-exception-caught
            logger.warning(
                "Could not query Dataform invocation actions for '%s': %s",
                inv_name,
                err,
            )
    return {
        "invocation_id": inv_name,
        "failed_actions": failed_actions,
        "successful_actions": successful_actions,
    }


def _extract_dbt_metadata(
    operator: Any, context: Mapping[str, Any]
) -> DbtMetadata:
    run_id = str(
        context.get("run_id")
        or _get_field(context.get("dag_run"), "run_id")
        or ""
    )
    metadata: DbtMetadata = {
        "run_id": run_id,
        "failed_actions": [],
        "successful_actions": [],
    }
    project_dir = _get_field(
        getattr(operator, "op_kwargs", None), "project_dir"
    )
    if not isinstance(project_dir, str) or not project_dir:
        return metadata

    results_path = pathlib.Path(project_dir) / "target" / "run_results.json"
    if not results_path.is_file():
        return metadata

    ti_start = getattr(
        context.get("ti") or context.get("task_instance"), "start_date", None
    )
    try:
        if (
            isinstance(ti_start, datetime.datetime)
            and results_path.stat().st_mtime < ti_start.timestamp() - 1.0
        ):
            return metadata
        doc = json.loads(results_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as err:
        logger.warning(
            "Could not parse dbt run_results.json at '%s': %s",
            results_path,
            err,
        )
        return metadata

    if inv_id := _get_field(_get_field(doc, "metadata"), "invocation_id"):
        metadata["run_id"] = str(inv_id)
    for entry in _get_field(doc, "results") or ():
        node_id = _get_field(entry, "unique_id")
        status = _get_field(entry, "status")
        if node_id and status == "success":
            metadata["successful_actions"].append(str(node_id))
        elif node_id and status in ("error", "fail", "runtime error"):
            metadata["failed_actions"].append(str(node_id))
    return metadata


def _extract_dts_metadata(
    operator: Any, raw_result: Any, failed: bool
) -> BigQueryDtsMetadata:
    op_run_id = getattr(operator, "run_id", None)
    run_id = (
        op_run_id
        if isinstance(op_run_id, str) and op_run_id and "{{" not in op_run_id
        else (
            str(
                _get_field(raw_result, "name")
                or (raw_result if isinstance(raw_result, str) else "")
            ).rsplit("/", 1)[-1]
            or None
        )
    )
    run_obj = raw_result
    transfer_config_id = getattr(operator, "transfer_config_id", None)
    if run_obj is None and run_id and transfer_config_id:
        try:
            # Upstream apache-airflow-providers-google spells this 'BiqQuery...' (with a 'q').
            hook = _resolve_gcp_hook(
                operator,
                "airflow.providers.google.cloud.hooks.bigquery_dts",
                "BiqQueryDataTransferServiceHook",
                location=getattr(operator, "location", None),
            )
            if hook is not None:
                run_obj = hook.get_transfer_run(
                    run_id=run_id,
                    transfer_config_id=transfer_config_id,
                    project_id=getattr(operator, "project_id", None),
                )
        except Exception as err:  # pylint: disable=broad-exception-caught
            logger.warning(
                "Could not fetch BigQuery DTS run '%s': %s", run_id, err
            )

    raw_state = _get_field(run_obj, "state")
    if isinstance(raw_state, int):
        from google.cloud.bigquery_datatransfer_v1.types import TransferState

        state_name = TransferState(raw_state).name if raw_state else None
    else:
        state_name = getattr(raw_state, "name", None) or (
            raw_state if isinstance(raw_state, str) and raw_state else None
        )
    return {
        "run_id": run_id,
        "run_state": state_name or ("FAILED" if failed else "SUCCEEDED"),
        "start_time": _get_field(run_obj, "start_time"),
        "end_time": _get_field(run_obj, "end_time"),
    }


def extract_and_pack_output(
    operator: Any,
    context: Mapping[str, Any],
    raw_result: Any,
    *,
    failed: bool = False,
) -> dict[str, Any]:
    """Harvests engine `.value` and `.metadata` into a packed XCom envelope."""
    action_model = getattr(operator, "_op_action_model", None)
    action_type = getattr(action_model, "type", "")
    op_action_type = str(getattr(operator, "_op_action_type", ""))
    engine = str(getattr(operator, "_op_engine", ""))

    output_value: Any = None
    metadata: dict[str, Any] = {}
    if action_type == "data_ingestion" or op_action_type == "DATA_INGESTION":
        metadata = _extract_dts_metadata(operator, raw_result, failed)
    elif action_type == "operation" or engine == "BIGQUERY":
        output_value, metadata = _extract_bigquery_output(
            operator,
            raw_result,
            bool(getattr(action_model, "fetchOutputValue", False)),
            failed,
        )
    elif engine == "DATAPROC" or action_type in ("pyspark", "notebook", "sql"):
        output_value, metadata = _extract_dataproc_output(
            operator, raw_result, failed
        )
    elif action_type == "dataform_pipeline":
        metadata = _extract_dataform_metadata(operator, context, raw_result)
    elif action_type == "dbt_pipeline":
        metadata = _extract_dbt_metadata(operator, context)
    else:
        output_value = None if failed else raw_result
    return pack_output_envelope(output_value, metadata, context, operator)
