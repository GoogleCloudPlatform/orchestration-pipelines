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
"""Shared operator payload builders and runtime kwargs resolution for CEL bindings."""

from __future__ import annotations

import copy
import dataclasses
import json
import os
import re
import shlex
from collections.abc import Mapping, Sequence
from typing import Any

from orchestration_pipelines_lib.internal_models.actions import (
    AIActionModel,
    AirflowActionModel,
    BqOperationActionModel,
    DataformActionModel,
    DataIngestionActionModel,
    DataprocOperatorActionModel,
    DBTActionModel,
    OrchestrationPipelineActionModel,
    PythonScriptActionModel,
    PythonVirtualenvActionModel,
)
from orchestration_pipelines_lib.utils.dict_utils import iso_to_timestamp_dict
from orchestration_pipelines_lib.utils.duration_utils import (
    duration_to_timedelta,
)
from orchestration_pipelines_lib.utils.runtime_resolver import (
    TASK_ROLE_CREATE_CLUSTER,
    TASK_ROLE_DELETE_CLUSTER,
    TASK_ROLE_DTS_SENSOR,
    filter_airflow_task_params,
    merge_rendered,
)

from . import dataproc_utils, gcs_utils
from .retry_resolver import RetryResolver

_RUNTIME_SQL_PARAM_RE = re.compile(r"\$\{([a-zA-Z0-9_-]+)\}")
_NO_TIMEOUT_ROLES = frozenset(
    {
        TASK_ROLE_CREATE_CLUSTER,
        TASK_ROLE_DELETE_CLUSTER,
        TASK_ROLE_DTS_SENSOR,
    }
)


@dataclasses.dataclass(frozen=True, slots=True)
class RuntimeKwargsContext:
    """Context passed to per-action runtime kwargs builders."""

    op: Any
    base_action: Any
    task_role: str
    action_paths: set[str]
    is_resolved: bool
    run_id: str


def _has_bound_prefix(paths: set[str], prefix: str) -> bool:
    return prefix in paths or any(
        p.startswith((f"{prefix}.", f"{prefix}[")) for p in paths
    )


def _align_map_order(
    base: Mapping[str, Any] | None, resolved: Mapping[str, Any] | None
) -> dict[str, Any]:
    """Aligns `resolved` key order with `base` so list projections stay aligned."""
    if not resolved:
        return {}
    ordered = {k: resolved[k] for k in (base or ()) if k in resolved}
    for k, v in resolved.items():
        if k not in ordered:
            ordered[k] = v
    return ordered


def _interpolate_runtime_sql_params(
    query: str, params: Mapping[str, Any]
) -> str:
    """Replaces '${key}' placeholders at runtime without touching Jinja."""
    return _RUNTIME_SQL_PARAM_RE.sub(
        lambda m: (
            str(val)
            if (k := m.group(1)) in params and (val := params[k]) is not None
            else m.group(0)
        ),
        query,
    )


def build_local_dataform_cmd(
    labels: Mapping[str, Any] | None, params: Mapping[str, Any] | None
) -> str:
    """Builds the shell command executed by local Dataform Kubernetes pods."""
    cmd = (
        "gcloud storage cp --recursive $GCS_BUCKET_PATH/* . && "
        "dataform run --timeout=60s"
    )
    if labels:
        labels_str = ",".join(
            shlex.quote(f"{k}={v}") for k, v in labels.items()
        )
        cmd += f" --job-labels={labels_str}"
    if params:
        params_str = ",".join(
            shlex.quote(f"{k}={v}") for k, v in params.items()
        )
        cmd += f" --vars={params_str}"
    return cmd


def _parse_local_dataform_vars(cmd: str) -> dict[str, str]:
    """Extracts `--vars=k=v,...` tokens from a shell-quoted Dataform command."""
    for token in shlex.split(cmd):
        if token.startswith("--vars="):
            return dict(
                item.partition("=")[::2]
                for item in token.removeprefix("--vars=").split(",")
                if "=" in item
            )
    return {}


def _parse_pyspark_args(args: Sequence[Any]) -> dict[str, str]:
    parsed: dict[str, str] = {}
    for i in range(0, len(args) - 1, 2):
        flag = args[i]
        if isinstance(flag, str) and flag.startswith("--"):
            parsed[flag[2:]] = str(args[i + 1])
    return parsed


def build_dbt_op_kwargs(
    action: DBTActionModel,
    defaults: Any = None,
    *,
    output_gcs_uri: str | None = None,
) -> dict[str, Any]:
    """Builds `op_kwargs` for `invoke_dbt` PythonOperator tasks."""
    op_kwargs: dict[str, Any] = {
        "project_dir": action.source.path,
        "profiles_dir": action.source.path,
    }
    if action.invocations:
        op_kwargs["invocations"] = [
            {f.name: getattr(inv, f.name) for f in dataclasses.fields(inv)}
            for inv in action.invocations
        ]
    if action.params:
        op_kwargs["params"] = dict(action.params)
    if output_gcs_uri:
        op_kwargs["output_gcs_uri"] = output_gcs_uri

    if action.knowledge_catalog:
        kc = action.knowledge_catalog
        if not kc.entry_group:
            raise ValueError(
                "Knowledge Catalog ingestion requires 'entry_group'."
            )
        raw_storage_uri = kc.storage_uri
        if not raw_storage_uri:
            if output_gcs_uri:
                raw_storage_uri = (
                    f"{output_gcs_uri.rstrip('/')}/dataplex_metadata/"
                )
            else:
                raise ValueError(
                    "Knowledge Catalog ingestion requires 'storage_uri', "
                    "'output_gcs_uri', or the GCS_BUCKET environment "
                    "variable to be set."
                )

        if not raw_storage_uri.startswith("gs://"):
            raise ValueError(
                f"Invalid Knowledge Catalog storage_uri: {raw_storage_uri!r}"
            )
        remainder = raw_storage_uri.removeprefix("gs://").rstrip("/")
        bucket = remainder.split("/", 1)[0]
        if not bucket or any(ch.isspace() for ch in bucket):
            raise ValueError(
                f"Invalid Knowledge Catalog storage_uri: {raw_storage_uri!r}"
            )
        storage_uri = f"gs://{remainder}/"

        cloud_default = getattr(defaults, "cloudDefault", None)
        project_id = kc.project_id or getattr(cloud_default, "project", "")
        location = kc.location or getattr(cloud_default, "region", "")

        op_kwargs["knowledge_catalog"] = {
            "project_id": project_id,
            "location": location,
            "entry_group": kc.entry_group,
            "storage_uri": storage_uri,
            "aspects_only": kc.aspects_only,
            "skip_bigquery_link": kc.skip_bigquery_link,
            "include_entry_links": kc.include_entry_links,
            "async_mode": kc.async_mode,
        }
    return op_kwargs


def build_bq_configuration(
    query: str | None,
    labels: Mapping[str, str] | None,
    params: Mapping[str, Any] | None,
    destination_table: str | None,
) -> dict[str, Any]:
    """Builds and validates the `configuration` dict for BigQuery SQL jobs."""
    sql = query or ""
    configuration: dict[str, Any] = {
        # Will be converted to JobConfigurationQuery in Protobuf. It can be set
        # up via google.cloud.bigquery.job.query.QueryJobConfig.
        "query": {"query": query, "useLegacySql": False},
        "labels": labels,
    }
    if params:
        configuration["query"]["queryParameters"] = [
            {
                "name": key,
                # NOTE: The limitation of Dataproc engines requiring string-typed
                # arguments applies to BigQuery SQL actions as well.
                "parameterType": {"type": "STRING"},
                "parameterValue": {"value": value},
            }
            for key, value in params.items()
        ]
    if not sql.strip().upper().startswith("CREATE"):
        configuration["query"]["writeDisposition"] = "WRITE_TRUNCATE"
        configuration["query"]["createDisposition"] = "CREATE_IF_NEEDED"
    if destination_table:
        parts = destination_table.split(".")
        if len(parts) != 3 or not all(parts):
            raise ValueError(
                "destinationTable should be in format 'project.dataset.table'"
            )
        configuration["query"]["destinationTable"] = {
            "projectId": parts[0],
            "datasetId": parts[1],
            "tableId": parts[2],
        }
    return configuration


def resolve_bq_dts_time_kwargs(spec: Any) -> dict[str, Any]:
    """Resolves `requested_run_time` and `requested_time_range` for BQ DTS."""
    run_time: Any = spec.requestedRunTime
    time_range: Any = spec.requestedTimeRange
    if spec.runtimeParams:
        if run_time is None:
            run_time = spec.runtimeParams.get("requested_run_time")
        if time_range is None:
            time_range = spec.runtimeParams.get("requested_time_range")
    if isinstance(run_time, str):
        run_time = iso_to_timestamp_dict(run_time)
    if isinstance(time_range, dict):
        time_range = {
            k: iso_to_timestamp_dict(v) if isinstance(v, str) else v
            for k, v in time_range.items()
        }
    if run_time is None and time_range is None:
        run_time = {
            "seconds": (
                "{{ logical_date.timestamp() | int if logical_date is "
                "defined else execution_date.timestamp() | int }}"
            )
        }
    return {
        "requested_run_time": run_time,
        "requested_time_range": time_range,
    }


def _dataproc_pyspark_config(
    action: DataprocOperatorActionModel,
    *,
    wrapper_uri: str | None = None,
    bucket_name: str | None = None,
    run_id: str | None = None,
) -> dict[str, Any]:
    if action.type == "notebook" and not wrapper_uri:
        wrapper_uri = (
            gcs_utils.get_run_notebook_gcs_path()
            if os.environ.get("GCS_BUCKET")
            else "gs://placeholder/run_notebook.py"
        )
    cfg = dataproc_utils.get_pyspark_batch_config(action, wrapper_uri)
    args = cfg.get("args")
    if action.type == "notebook" and isinstance(args, list) and len(args) >= 3:
        if args[1] is None and bucket_name is not None:
            args[1] = bucket_name
        if run_id is not None and args[2] == "{{ run_id }}":
            args[2] = run_id
    return cfg


def build_dataproc_batch_payload(
    action: DataprocOperatorActionModel,
    *,
    wrapper_uri: str | None = None,
    bucket_name: str | None = None,
    run_id: str | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Builds `(batch_dict, extra_kwargs)` for `DataprocCreateBatchOperator`."""
    from google.cloud import dataproc_v1

    job_specific_config: dict[str, Any] = {}
    extra_kwargs: dict[str, Any] = {}
    if action.type in ("pyspark", "notebook"):
        job_specific_config["pyspark_batch"] = _dataproc_pyspark_config(
            action,
            wrapper_uri=wrapper_uri,
            bucket_name=bucket_name,
            run_id=run_id,
        )
    elif action.type == "sql":
        spark_sql_batch: dict[str, Any] = {}
        if action.params:
            spark_sql_batch["query_variables"] = dict(action.params)
        if action.query:
            extra_kwargs["query"] = action.query
            extra_kwargs["gcs_bucket"] = os.environ.get("GCS_BUCKET")
        elif action.filename:
            spark_sql_batch["query_file_uri"] = action.filename
        job_specific_config["spark_sql_batch"] = spark_sql_batch

    runtime_config = copy.deepcopy(
        action.config.resourceProfile.runtimeConfig or {}
    )
    environment_config = copy.deepcopy(
        action.config.resourceProfile.environmentConfig or {}
    )
    if action.type in ("pyspark", "notebook"):
        execution_config = environment_config.setdefault("execution_config", {})
        if execution_config.get("staging_bucket") is None and action.depsBucket:
            execution_config["staging_bucket"] = action.depsBucket

    batch = dataproc_v1.types.Batch(
        **job_specific_config,
        runtime_config=runtime_config,
        environment_config=environment_config,
        labels=action.labels,
    )
    return type(batch).to_dict(batch), extra_kwargs


def build_dataproc_job_payload(
    action: DataprocOperatorActionModel,
    *,
    wrapper_uri: str | None = None,
    bucket_name: str | None = None,
    run_id: str | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Builds `(job_dict, extra_kwargs)` for `DataprocSubmitJobOperator`."""
    job: dict[str, Any] = {
        "placement": {"cluster_name": action.config.cluster_name},
        "reference": {"project_id": action.config.project_id},
        "labels": action.labels,
    }
    extra_kwargs: dict[str, Any] = {}
    if action.type == "sql":
        spark_sql_job: dict[str, Any] = {}
        if action.params:
            spark_sql_job["script_variables"] = dict(action.params)
        if action.query:
            extra_kwargs["query"] = action.query
            extra_kwargs["gcs_bucket"] = os.environ.get("GCS_BUCKET")
        elif action.filename:
            spark_sql_job["query_file_uri"] = action.filename
        if action.config.properties:
            spark_sql_job["properties"] = dict(action.config.properties)
        job["spark_sql_job"] = spark_sql_job
    else:
        # Handles both pyspark and notebook actions for GCE clusters via
        # DataprocSubmitJobOperator (using pyspark_job with run_notebook.py).
        pyspark_job = _dataproc_pyspark_config(
            action,
            wrapper_uri=wrapper_uri,
            bucket_name=bucket_name,
            run_id=run_id,
        )
        if action.pyFiles:
            pyspark_job["python_file_uris"] = action.pyFiles
        pyspark_job["properties"] = action.config.properties
        job["pyspark_job"] = pyspark_job
    return job, extra_kwargs


def build_vertex_model_dict(action: AIActionModel) -> dict[str, Any]:
    """Builds the `model` dict for Vertex AI `UploadModelOperator`."""
    cfg: Any = action.config
    model: dict[str, Any] = {
        "display_name": cfg.model_name,
        "artifact_uri": cfg.model_artifact_uri,
        "container_spec": {"image_uri": cfg.serving_container_image_uri},
    }
    if cfg.description:
        model["description"] = cfg.description
    if action.labels:
        model["labels"] = dict(action.labels)
    return model


def build_vertex_custom_job_dict(action: AIActionModel) -> dict[str, Any]:
    """Builds the `custom_job` dict for Vertex AI `CreateCustomJobOperator`."""
    cfg: Any = action.config
    custom_job = dict(cfg.custom_job or {})
    if action.labels:
        custom_job["labels"] = {
            **(custom_job.get("labels") or {}),
            **action.labels,
        }
    return custom_job


def policy_kwargs(action: Any, defaults: Any, task_role: str) -> dict[str, Any]:
    """Builds execution timeout and retry kwargs for `action`."""
    kw: dict[str, Any] = {}
    if task_role not in _NO_TIMEOUT_ROLES:
        kw["execution_timeout"] = (
            duration_to_timedelta(action.executionTimeout)
            if action.executionTimeout
            else None
        )
    if task_role != TASK_ROLE_DTS_SENSOR:
        kw.update(
            RetryResolver.resolve_policy_kwargs(action.retryPolicy)
            if action.retryPolicy
            else RetryResolver.resolve_default_args(defaults)
        )
    return kw


def python_kwargs(
    action: PythonScriptActionModel | PythonVirtualenvActionModel,
    _defaults: Any,
    _ctx: RuntimeKwargsContext,
) -> dict[str, Any]:
    """Builds runtime kwargs for Python script and virtualenv operators."""
    return {"op_kwargs": dict(action.config.opKwargs or {})}


def dbt_kwargs(
    action: DBTActionModel, defaults: Any, ctx: RuntimeKwargsContext
) -> dict[str, Any]:
    """Builds runtime kwargs for dbt PythonOperator tasks."""
    cur_op_kwargs = getattr(ctx.op, "op_kwargs", None)
    output_gcs_uri = (
        cur_op_kwargs.get("output_gcs_uri")
        if isinstance(cur_op_kwargs, Mapping)
        else None
    )
    return {
        "op_kwargs": build_dbt_op_kwargs(
            action, defaults, output_gcs_uri=output_gcs_uri
        )
    }


def dataform_kwargs(
    action: DataformActionModel, defaults: Any, ctx: RuntimeKwargsContext
) -> dict[str, Any]:
    """Builds runtime kwargs for local and service Dataform operators."""
    if action.executionMode == "service":
        cfg = action.dataformServiceConfig
        return {
            "project_id": cfg.project_id or defaults.cloudDefault.project,  # type: ignore[union-attr]
            "region": cfg.region or defaults.cloudDefault.region,  # type: ignore[union-attr]
            "repository_id": cfg.repository_id,  # type: ignore[union-attr]
            "workflow_invocation": cfg.workflow_invocation,  # type: ignore[union-attr]
        }

    labels_bound = _has_bound_prefix(ctx.action_paths, "labels")
    params_bound = _has_bound_prefix(ctx.action_paths, "params")
    if ctx.is_resolved and (labels_bound or params_bound):
        cur_labels = getattr(ctx.op, "labels", None) or {}
        eff_labels = (
            merge_rendered(
                cur_labels, ctx.base_action.labels or {}, action.labels or {}
            )
            if labels_bound
            else cur_labels
        )
        cur_args = getattr(ctx.op, "arguments", None) or ()
        cur_vars = (
            _parse_local_dataform_vars(cur_args[0])
            if cur_args and isinstance(cur_args[0], str)
            else {}
        )
        eff_params = (
            merge_rendered(
                cur_vars, ctx.base_action.params or {}, action.params or {}
            )
            if params_bound
            else cur_vars
        )
    else:
        eff_labels = dict(action.labels or {})
        eff_params = dict(action.params or {})
    return {
        "arguments": [build_local_dataform_cmd(eff_labels, eff_params)],
        "labels": dict(eff_labels),
    }


def bq_operation_kwargs(
    action: BqOperationActionModel, defaults: Any, ctx: RuntimeKwargsContext
) -> dict[str, Any]:
    """Builds runtime kwargs for BigQueryInsertJobOperator."""
    cur_cfg = getattr(ctx.op, "configuration", None) or {}
    cur_query_cfg = (
        cur_cfg.get("query") or {} if isinstance(cur_cfg, Mapping) else {}
    )
    query = str(cur_query_cfg.get("query") or action.query or "")
    params: Mapping[str, Any] | None = action.params
    if ctx.is_resolved:
        cur_qp = {
            item["name"]: item.get("parameterValue", {}).get("value")
            for item in (cur_query_cfg.get("queryParameters") or ())
            if isinstance(item, Mapping) and isinstance(item.get("name"), str)
        }
        aligned = _align_map_order(ctx.base_action.params, action.params)
        params = (
            merge_rendered(cur_qp, ctx.base_action.params or {}, aligned)
            if aligned
            else None
        )
        if params and _has_bound_prefix(ctx.action_paths, "params"):
            query = _interpolate_runtime_sql_params(query, params)

    return {
        "location": action.config.location,
        "project_id": defaults.cloudDefault.project,
        "configuration": build_bq_configuration(
            query, action.labels, params, action.config.destinationTable
        ),
        "impersonation_chain": action.impersonationChain,
    }


def dataproc_kwargs(
    action: DataprocOperatorActionModel,
    defaults: Any,
    ctx: RuntimeKwargsContext,
) -> dict[str, Any]:
    """Builds runtime kwargs for Dataproc Serverless and GCE operators."""
    is_batch = action.engine.engineType == "dataproc-serverless"
    if not is_batch and ctx.task_role in (
        TASK_ROLE_CREATE_CLUSTER,
        TASK_ROLE_DELETE_CLUSTER,
    ):
        kw: dict[str, Any] = {
            "project_id": action.config.project_id,
            "region": action.config.region,
            "cluster_name": action.config.cluster_name,
            "impersonation_chain": action.impersonationChain,
        }
        if ctx.task_role == TASK_ROLE_CREATE_CLUSTER:
            cluster_config = copy.deepcopy(action.config.cluster_config or {})
            if action.depsBucket:
                cluster_config["config_bucket"] = action.depsBucket
            kw["cluster_config"] = cluster_config
            kw["labels"] = action.labels
        return kw

    container = getattr(ctx.op, "batch" if is_batch else "job", None)
    sub = (
        container.get("pyspark_batch" if is_batch else "pyspark_job")
        if isinstance(container, Mapping)
        else None
    )
    sub_map = sub if isinstance(sub, Mapping) else {}
    cur_args = (
        sub_map.get("args") if isinstance(sub_map.get("args"), list) else ()
    )
    nb_kw = (
        {
            "wrapper_uri": str(sub_map.get("main_python_file_uri") or ""),
            "bucket_name": (
                cur_args[1]
                if len(cur_args) > 1 and isinstance(cur_args[1], str)
                else ""
            ),
        }
        if action.type == "notebook"
        else {}
    )

    eff_action = action
    if ctx.is_resolved and action.type in ("notebook", "pyspark"):
        aligned = _align_map_order(ctx.base_action.params, action.params)
        if action.type == "notebook":
            try:
                cur_params = (
                    json.loads(cur_args[3])
                    if len(cur_args) > 3 and isinstance(cur_args[3], str)
                    else {}
                )
            except json.JSONDecodeError as err:
                task_name = getattr(ctx.op, "task_id", None) or action.name
                raise ValueError(
                    f"Failed to parse rendered notebook params JSON for task "
                    f"'{task_name}': {err}"
                ) from err
        else:
            cur_params = _parse_pyspark_args(cur_args)
        merged_params = (
            merge_rendered(cur_params, ctx.base_action.params or {}, aligned)
            if aligned
            else None
        )
        eff_action = dataclasses.replace(action, params=merged_params)

    run_id = ctx.run_id if ctx.is_resolved else None
    if is_batch:
        batch_dict, extra_kwargs = build_dataproc_batch_payload(
            eff_action, run_id=run_id, **nb_kw
        )
        return {
            "region": eff_action.region,
            "project_id": defaults.cloudDefault.project,
            "batch": batch_dict,
            "impersonation_chain": eff_action.impersonationChain,
            **extra_kwargs,
        }
    is_ephemeral = eff_action.engine.clusterMode == "ephemeral"
    job_dict, extra_kwargs = build_dataproc_job_payload(
        eff_action, run_id=run_id, **nb_kw
    )
    return {
        "job": job_dict,
        "region": (
            eff_action.config.region if is_ephemeral else eff_action.region
        ),
        "project_id": (
            eff_action.config.project_id
            if is_ephemeral
            else defaults.cloudDefault.project
        ),
        "impersonation_chain": eff_action.impersonationChain,
        **extra_kwargs,
    }


def bq_dts_kwargs(
    action: DataIngestionActionModel, defaults: Any, ctx: RuntimeKwargsContext
) -> dict[str, Any]:
    """Builds runtime kwargs for BigQuery DTS start and sensor tasks."""
    spec = action.config
    kw: dict[str, Any] = {
        "transfer_config_id": spec.transferConfigId,
        "project_id": spec.projectId or defaults.cloudDefault.project,
        "location": spec.location or defaults.cloudDefault.region,
        "impersonation_chain": spec.impersonationChain,
    }
    if ctx.task_role != TASK_ROLE_DTS_SENSOR:
        kw.update(resolve_bq_dts_time_kwargs(spec))
    return kw


def trigger_pipeline_kwargs(
    action: OrchestrationPipelineActionModel,
    _defaults: Any,
    _ctx: RuntimeKwargsContext,
) -> dict[str, Any]:
    """Builds runtime kwargs for TriggerDagRunOperator tasks."""
    return {
        "wait_for_completion": bool(action.wait_for_completion),
        "conf": dict(action.params) if action.params else None,
    }


def ai_kwargs(
    action: AIActionModel, _defaults: Any, _ctx: RuntimeKwargsContext
) -> dict[str, Any]:
    """Builds runtime kwargs for Vertex AI Agent Platform operators."""
    cfg: Any = action.config
    if action.ai_action_type == "model_upload":
        return {
            "project_id": cfg.project_id,
            "region": cfg.location,
            "model": build_vertex_model_dict(action),
        }
    if action.ai_action_type == "batch_inference":
        return {
            "project_id": cfg.project_id,
            "region": cfg.location,
            "job_display_name": cfg.job_display_name,
            "model_name": cfg.model_name,
            "instances_format": cfg.instances_format or "jsonl",
            "predictions_format": cfg.predictions_format or "jsonl",
            "bigquery_source": cfg.bigquery_source,
            "gcs_source": cfg.gcs_source,
            "bigquery_destination_prefix": cfg.bigquery_destination_prefix,
            "gcs_destination_prefix": cfg.gcs_destination_prefix,
            "labels": action.labels,
            "impersonation_chain": cfg.impersonation_chain,
        }
    if action.ai_action_type == "create_and_run_custom_job":
        return {
            "project_id": cfg.project_id,
            "region": cfg.location,
            "custom_job": build_vertex_custom_job_dict(action),
            "impersonation_chain": cfg.impersonation_chain,
        }
    return {}


def airflow_task_kwargs(
    action: AirflowActionModel, _defaults: Any, _ctx: RuntimeKwargsContext
) -> dict[str, Any]:
    """Builds runtime kwargs for generic Airflow operator tasks."""
    return filter_airflow_task_params(action.params)
