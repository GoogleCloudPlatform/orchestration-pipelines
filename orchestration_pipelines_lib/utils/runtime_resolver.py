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
"""Dynamic binding resolution, runtime context building, and operator mixin."""

from __future__ import annotations

import copy
import dataclasses
import datetime
import itertools
import json
import logging
from collections.abc import Callable, Iterable, Mapping
from enum import StrEnum
from typing import Any

from orchestration_pipelines_lib.internal_models.actions import (
    AIActionModel,
    BqOperationActionModel,
    DataformActionModel,
    DataIngestionActionModel,
    DataprocOperatorActionModel,
    PythonScriptActionModel,
    PythonVirtualenvActionModel,
)
from orchestration_pipelines_lib.internal_models.converters import (
    v1_model_converter,
)
from orchestration_pipelines_lib.utils import (
    cel_evaluator,
    dict_utils,
    file_manager,
    versioned_file_manager,
)
from orchestration_pipelines_lib.utils import output_harvester as harvester
from orchestration_pipelines_lib.utils.cel_evaluator import (
    evaluate_cel_expression as evaluate_cel_expression,
)
from orchestration_pipelines_models.pipeline_v1_model.pipeline_validation import (
    PipelineValidator,
)
from orchestration_pipelines_models.pipeline_v1_model.protos import (
    orchestration_pipeline_pb2 as v1_pipeline_protos,
)
from orchestration_pipelines_models.utils import cel_utils

try:
    # Airflow 3.2+ relocates core control-flow exceptions under `airflow.sdk`.
    import airflow.sdk.exceptions as _af_exc  # pyright: ignore
    from airflow.sdk.exceptions import (  # pyright: ignore
        AirflowSkipException,
        TaskDeferred,
    )
except ImportError:  # pragma: no cover
    import airflow.exceptions as _af_exc  # pyright: ignore
    from airflow.exceptions import (  # pyright: ignore
        AirflowSkipException,
        TaskDeferred,
    )

_NON_FAILURE_SIGNAL_NAMES = frozenset(
    {
        "AirflowSkipException",
        "TaskDeferred",
        "AirflowRescheduleException",
        "DagRunTriggerException",
        "DownstreamTasksSkipped",
    }
)
_NON_FAILURE_SIGNALS = tuple(
    {
        AirflowSkipException,
        TaskDeferred,
        *(
            cls
            for name in _NON_FAILURE_SIGNAL_NAMES
            if isinstance(cls := getattr(_af_exc, name, None), type)
        ),
    }
)

logger = logging.getLogger(__name__)


class TaskRole(StrEnum):
    """Role of an Airflow operator within a single task or TaskGroup action."""

    MAIN = "main"
    CREATE_CLUSTER = "create_cluster"
    SUBMIT_JOB = "submit_job"
    DELETE_CLUSTER = "delete_cluster"
    DTS_START = "dts_start"
    DTS_SENSOR = "dts_sensor"


TASK_ROLE_MAIN = TaskRole.MAIN.value
TASK_ROLE_CREATE_CLUSTER = TaskRole.CREATE_CLUSTER.value
TASK_ROLE_SUBMIT_JOB = TaskRole.SUBMIT_JOB.value
TASK_ROLE_DELETE_CLUSTER = TaskRole.DELETE_CLUSTER.value
TASK_ROLE_DTS_START = TaskRole.DTS_START.value
TASK_ROLE_DTS_SENSOR = TaskRole.DTS_SENSOR.value

ENTRY_TASK_ROLES = frozenset(
    {TaskRole.MAIN, TaskRole.CREATE_CLUSTER, TaskRole.DTS_START}
)
OUTPUT_TASK_ROLES = frozenset(
    {TaskRole.MAIN, TaskRole.SUBMIT_JOB, TaskRole.DTS_SENSOR}
)

PROTECTED_AIRFLOW_TASK_ATTRS = frozenset(
    {
        "task_id",
        "dag",
        "dag_id",
        "task_group",
        "owner",
        "start_date",
        "end_date",
        "depends_on_past",
        "execution_timeout",
        "trigger_rule",
        "doc_md",
        "retries",
        "retry_delay",
        "retry_exponential_backoff",
        "max_retry_delay",
        "on_failure_callback",
        "on_success_callback",
        "on_retry_callback",
        "on_execute_callback",
        "on_skipped_callback",
    }
)

_GCP_ACTION_TYPES = (
    BqOperationActionModel,
    DataprocOperatorActionModel,
    DataIngestionActionModel,
    AIActionModel,
)
_PARAM_TYPES: dict[str, type | tuple[type, ...]] = {
    "number": (int, float),
    "boolean": bool,
    "string": str,
    "array": (list, tuple),
}

OperatorKwargsBuilder = Callable[..., Mapping[str, Any]]
_default_kwargs_builder: OperatorKwargsBuilder | None = None


def register_operator_kwargs_builder(
    builder: OperatorKwargsBuilder | None,
) -> None:
    """Registers the default builder mapping resolved models to operator kwargs."""
    global _default_kwargs_builder
    _default_kwargs_builder = builder


def is_non_failure_signal(err: BaseException) -> bool:
    """Returns True for Airflow control-flow signals (skip/defer/reschedule)."""
    return (
        isinstance(err, _NON_FAILURE_SIGNALS)
        or type(err).__name__ in _NON_FAILURE_SIGNAL_NAMES
    )


def filter_airflow_task_params(
    params: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Filters private and protected Airflow operator attributes from `params`."""
    return {
        k: copy.deepcopy(v)
        for k, v in (params or {}).items()
        if not k.startswith("_") and k not in PROTECTED_AIRFLOW_TASK_ATTRS
    }


@dataclasses.dataclass(frozen=True, slots=True)
class RuntimePipelineContext:
    """Action-free pipeline metadata attached only to dynamic tasks."""

    defaults: Any
    metadata: Any
    parameters: Any = None
    dynamicBindings: dict[str, str] | None = None
    v1_defaults_bytes: bytes | None = None


def build_runtime_pipeline_context(pipeline: Any) -> RuntimePipelineContext:
    """Extracts lightweight pipeline metadata without `actions` or `triggers`."""
    if isinstance(pipeline, RuntimePipelineContext):
        return pipeline
    return RuntimePipelineContext(
        defaults=getattr(pipeline, "defaults", None),
        metadata=getattr(pipeline, "metadata", None),
        parameters=getattr(pipeline, "parameters", None),
        dynamicBindings=getattr(pipeline, "dynamicBindings", None),
        v1_defaults_bytes=getattr(pipeline, "v1_defaults_bytes", None)
        or getattr(pipeline, "_v1_defaults_bytes", None),
    )


def _event_sort_key(event: Any) -> float:
    ts = getattr(event, "timestamp", None)
    if isinstance(ts, datetime.datetime):
        return ts.timestamp()
    return float(ts) if isinstance(ts, (int, float)) else 0.0


def _extract_asset_event_params(context: Mapping[str, Any]) -> dict[str, Any]:
    raw_events = context.get("triggering_asset_events") or context.get(
        "triggering_dataset_events"
    )
    if not isinstance(raw_events, Mapping):
        return {}
    extracted: dict[str, Any] = {}
    all_events = itertools.chain.from_iterable(
        group for group in raw_events.values() if group
    )
    for event in sorted(all_events, key=_event_sort_key):
        if isinstance(extra := getattr(event, "extra", None), Mapping):
            payload = extra.get("params")
            extracted.update(payload if isinstance(payload, Mapping) else extra)
    return extracted


def _resolve_output_task_id(dag: Any, action_name: str) -> str:
    """Maps a TaskGroup action name to the inner task that pushes XCom output."""
    task_dict = getattr(dag, "task_dict", None)
    if isinstance(task_dict, Mapping) and action_name not in task_dict:
        for suffix in ("submit_job", "sensor"):
            candidate = f"{action_name}.{action_name}_{suffix}"
            if candidate in task_dict:
                return candidate
    return action_name


def _coerce_param(name: str, value: Any, param_type: str) -> Any:
    if (
        value is None
        or not isinstance(value, _PARAM_TYPES[param_type])
        or (param_type == "number" and not dict_utils.is_finite_number(value))
    ):
        raise TypeError(
            f"Parameter '{name}' expected {param_type}, "
            f"got {type(value).__name__}."
        )
    if param_type == "array":
        if not all(
            isinstance(x, (str, bool)) or dict_utils.is_finite_number(x)
            for x in value
        ):
            raise TypeError(
                f"Parameter '{name}' array items must be non-null primitives."
            )
        value = list(value)
    return dict_utils.normalize_proto_json(value)


def _resolve_parameters(
    pipeline_model: Any, context: Mapping[str, Any], dag_run: Any
) -> dict[str, Any]:
    raw_params = getattr(pipeline_model, "parameters", None)
    model_params = raw_params if isinstance(raw_params, Mapping) else {}
    resolved = {name: param.default for name, param in model_params.items()}

    for source in (
        context.get("params"),
        _extract_asset_event_params(context),
        getattr(dag_run, "conf", None),
    ):
        if not isinstance(source, Mapping):
            continue
        for key, raw_val in source.items():
            if pipeline_model is None or key in model_params:
                val = (
                    raw_val.resolve()
                    if hasattr(raw_val, "resolve")
                    else raw_val
                )
                param_model = model_params.get(key)
                resolved[key] = (
                    _coerce_param(key, val, param_model.type)
                    if param_model is not None
                    else val
                )
    return resolved


_RUN_IF_BINDING_KEYS = frozenset({"run_if", "runIf"})


def get_run_if_expr(action_model: Any) -> str | None:
    """Returns the CEL `run_if` expression from `action_model` or its bindings."""
    if (
        isinstance(run_if := getattr(action_model, "runIf", None), str)
        and run_if
    ):
        return run_if
    if isinstance(
        bindings := getattr(action_model, "dynamicBindings", None), Mapping
    ):
        for key in ("run_if", "runIf"):
            if isinstance(expr := bindings.get(key), str) and expr:
                return expr
    return None


def referenced_output_actions(
    action_model: Any, *, include_run_if: bool = True
) -> set[str]:
    """Returns upstream action names referenced via `outputs.*` in CEL."""
    exprs: list[str] = []
    if include_run_if and (run_if := get_run_if_expr(action_model)):
        exprs.append(run_if)
    if isinstance(
        bindings := getattr(action_model, "dynamicBindings", None), Mapping
    ):
        exprs.extend(
            e
            for k, e in bindings.items()
            if k not in _RUN_IF_BINDING_KEYS and isinstance(e, str) and e
        )

    referenced: set[str] = set()
    for expr in exprs:
        try:
            referenced.update(
                cel_utils.validate_and_analyze_cel(
                    expr, warn=False
                ).output_actions
            )
        except ValueError:
            return set(getattr(action_model, "dependsOn", None) or ())
    return referenced


def build_cel_eval_context(
    operator: Any, context: Mapping[str, Any], *, pipeline_model: Any = None
) -> dict[str, Any]:
    """Constructs the `{parameters, run, outputs}` activation context."""
    pipeline_model = pipeline_model or getattr(
        operator, "_op_pipeline_model", None
    )
    action_model = getattr(operator, "_op_action_model", None)
    dag = getattr(operator, "dag", None) or context.get("dag")
    dag_run = context.get("dag_run")
    resolved_params = _resolve_parameters(pipeline_model, context, dag_run)

    date_candidates = (
        context.get("logical_date"),
        context.get("execution_date"),
        getattr(dag_run, "logical_date", None),
        getattr(dag_run, "execution_date", None),
        getattr(dag_run, "run_after", None),
        getattr(dag_run, "start_date", None),
    )
    raw_dt = next(
        (dt for dt in date_candidates if isinstance(dt, datetime.datetime)),
        datetime.datetime.now(datetime.UTC),
    )
    utc_dt = (
        raw_dt.replace(tzinfo=datetime.UTC)
        if raw_dt.tzinfo is None
        else raw_dt.astimezone(datetime.UTC)
    )

    run_id = (
        context.get("run_id")
        or getattr(dag_run, "run_id", None)
        or "unknown_run"
    )
    outputs_by_action: dict[str, Any] = {}
    ti = context.get("ti") or context.get("task_instance")
    depends_on = getattr(action_model, "dependsOn", None)
    if callable(getattr(ti, "xcom_pull", None)) and isinstance(
        depends_on, (list, tuple, set)
    ):
        needed = set(depends_on) & referenced_output_actions(
            action_model,
            include_run_if=getattr(operator, "_op_is_entry_task", True),
        )
        for dep_name in needed:
            unpacked = harvester.unpack_output_envelope(
                ti.xcom_pull(
                    task_ids=_resolve_output_task_id(dag, dep_name),
                    key="return_value",
                )
            )
            if unpacked is not None:
                outputs_by_action[dep_name] = unpacked

    return {
        "parameters": resolved_params,
        "run": {
            "id": str(run_id),
            "trigger_time": utc_dt.isoformat().replace("+00:00", "Z"),
            "trigger_date": utc_dt.strftime("%Y-%m-%d"),
        },
        "outputs": outputs_by_action,
    }


def _same_value(a: Any, b: Any) -> bool:
    return type(a) is type(b) and a == b


def merge_rendered(current: Any, base: Any, resolved: Any) -> Any:
    """Preserves Jinja-rendered leaves in `current` where `base` is unchanged."""
    if isinstance(base, str) and base and _same_value(base, resolved):
        return copy.deepcopy(current)
    if (
        isinstance(current, Mapping)
        and isinstance(base, Mapping)
        and isinstance(resolved, Mapping)
    ):
        merged: dict[Any, Any] = {}
        for k, cur_v in current.items():
            if k not in base:
                merged[k] = copy.deepcopy(cur_v)
            elif k in resolved:
                merged[k] = merge_rendered(cur_v, base[k], resolved[k])
        for k, res_v in resolved.items():
            if k not in merged:
                merged[k] = copy.deepcopy(res_v)
        return merged
    if (
        isinstance(current, list)
        and isinstance(base, list)
        and isinstance(resolved, list)
        and len(current) == len(base) == len(resolved)
    ):
        return [
            merge_rendered(c, b, r) for c, b, r in zip(current, base, resolved)
        ]
    return copy.deepcopy(resolved)


def sync_operator_kwargs(
    op: Any,
    base_kwargs: Mapping[str, Any],
    resolved_kwargs: Mapping[str, Any],
) -> None:
    """Updates `op` with changed fields while preserving Jinja-rendered leaves."""
    for attr, resolved_val in resolved_kwargs.items():
        base_val = base_kwargs.get(attr)
        if not _same_value(base_val, resolved_val):
            setattr(
                op,
                attr,
                merge_rendered(getattr(op, attr, None), base_val, resolved_val),
            )


def _resolve_onto_proto(
    msg: Any, bindings: list[tuple[str, str]], activation: Mapping[str, Any]
) -> list[tuple[str, Any]]:
    """Evaluates `bindings` onto `msg` and validates only the modified fields."""
    if msg is None or not bindings:
        return []
    resolved: list[tuple[str, Any]] = []
    for raw_path, expr in bindings:
        canonical_path = PipelineValidator.resolve_binding_target(
            msg.DESCRIPTOR, raw_path
        )[1]
        val = cel_evaluator.evaluate_with_activation(expr, activation)
        dict_utils.set_proto_path(msg, canonical_path, val)
        resolved.append((canonical_path, val))
    for canonical_path, _ in resolved:
        PipelineValidator.validate_bound_path(msg, canonical_path)
    return resolved


def _str_bindings(source: Any, prefix: str = "") -> list[tuple[str, str]]:
    raw = getattr(source, "dynamicBindings", None)
    if not isinstance(raw, Mapping):
        return []
    return [
        (key, expr)
        for key, expr in raw.items()
        if isinstance(key, str)
        and key not in _RUN_IF_BINDING_KEYS
        and key.startswith(prefix)
        and isinstance(expr, str)
    ]


def _consumes_gcp_defaults(action_model: Any) -> bool:
    return isinstance(action_model, _GCP_ACTION_TYPES) or (
        isinstance(action_model, DataformActionModel)
        and action_model.executionMode == "service"
    )


def _applicable_defaults_bindings(
    pipeline_model: Any, action_model: Any, task_role: str = TASK_ROLE_MAIN
) -> list[tuple[str, str]]:
    """Filters `defaults.*` bindings to those actually used by `action_model`."""
    uses_gcp = _consumes_gcp_defaults(action_model)
    uses_retry = task_role != TASK_ROLE_DTS_SENSOR
    applicable: list[tuple[str, str]] = []
    for key, expr in _str_bindings(pipeline_model, "defaults."):
        if key in (
            "defaults.project_id",
            "defaults.projectId",
            "defaults.location",
        ):
            if uses_gcp:
                applicable.append((key, expr))
        elif uses_retry:
            applicable.append((key, expr))
    return applicable


def has_applicable_bindings(
    pipeline_model: Any, action_model: Any, task_role: str = TASK_ROLE_MAIN
) -> bool:
    """Returns True if `action_model` has dynamic bindings to apply at runtime."""
    return bool(
        _str_bindings(action_model)
        or _applicable_defaults_bindings(
            pipeline_model, action_model, task_role
        )
    )


def _build_runtime_file_manager(operator: Any) -> file_manager.FileManager:
    dag = getattr(operator, "dag", None)
    searchpath = getattr(dag, "template_searchpath", None)
    if isinstance(searchpath, str):
        data_root = searchpath or None
    elif isinstance(searchpath, Iterable):
        data_root = next(iter(searchpath), None)
    else:
        data_root = None
    doc_md = getattr(dag, "doc_md", None)
    if isinstance(doc_md, str) and doc_md:
        try:
            meta = json.loads(doc_md)
        except ValueError:
            meta = {}
        if isinstance(meta, Mapping) and all(
            meta.get(k) for k in ("op_bundle", "op_version", "op_pipeline")
        ):
            return versioned_file_manager.VersionedFileManager(
                pipeline_id=str(meta["op_pipeline"]),
                current_version=str(meta["op_version"]),
                bundle_id=str(meta["op_bundle"]),
                local_data_root=data_root or "/data",
            )
    return file_manager.FileManager(data_root=data_root)


def apply_dynamic_bindings(
    operator: Any,
    eval_ctx: Mapping[str, Any],
    *,
    activation: Mapping[str, Any] | None = None,
) -> None:
    """Evaluates dynamic bindings, reconverts models, and syncs `operator`."""
    pipeline_model = getattr(operator, "_op_pipeline_model", None)
    base_action = getattr(operator, "_op_base_action_model", None) or getattr(
        operator, "_op_action_model", None
    )
    task_role = getattr(operator, "_op_task_role", TASK_ROLE_MAIN)
    defaults_bindings = _applicable_defaults_bindings(
        pipeline_model, base_action, task_role
    )
    action_bindings = _str_bindings(base_action)
    if not defaults_bindings and not action_bindings:
        return

    raw_bytes = getattr(base_action, "_v1_action_bytes", None)
    if not isinstance(raw_bytes, bytes) or pipeline_model is None:
        return

    if getattr(operator, "_op_base_action_model", None) is None:
        operator._op_base_action_model = base_action

    def_bytes = getattr(pipeline_model, "v1_defaults_bytes", None) or getattr(
        pipeline_model, "_v1_defaults_bytes", None
    )
    base_defaults = pipeline_model.defaults
    cloud_defaults = base_defaults.cloudDefault
    v1_defaults = (
        v1_pipeline_protos.Defaults.FromString(def_bytes)
        if isinstance(def_bytes, bytes)
        else v1_pipeline_protos.Defaults(
            project_id=cloud_defaults.project, location=cloud_defaults.region
        )
    )

    if activation is None:
        activation = cel_evaluator.build_cel_activation(eval_ctx)
    default_vals = _resolve_onto_proto(
        v1_defaults,
        [(p.removeprefix("defaults."), e) for p, e in defaults_bindings],
        activation,
    )
    default_paths = {f"defaults.{path}" for path, _ in default_vals}

    v1_action = v1_pipeline_protos.Action.FromString(raw_bytes)
    oneof_name = v1_action.WhichOneof("action")
    v1_inner = getattr(v1_action, oneof_name) if oneof_name else None
    resolved_values = _resolve_onto_proto(v1_inner, action_bindings, activation)
    action_paths = {path for path, _ in resolved_values}

    converter = v1_model_converter.ConverterV1ToInternal(
        _build_runtime_file_manager(operator)
    )
    resolved_defaults = (
        converter.convert_defaults(v1_defaults)
        if defaults_bindings
        else base_defaults
    )
    resolved_action = converter.reconvert_action(
        v1_action,
        v1_defaults,
        base_action,
        pipeline_model.metadata.tags,
        bound_paths=action_paths,
    )

    kwargs_builder = (
        getattr(operator, "_op_kwargs_builder", None) or _default_kwargs_builder
    )
    if callable(kwargs_builder):
        common = {
            "task_role": task_role,
            "action_paths": action_paths,
            "all_paths": default_paths | action_paths,
            "run_id": str(eval_ctx["run"]["id"]),
            "base_action": base_action,
        }
        base_kw = kwargs_builder(
            operator, base_action, base_defaults, is_resolved=False, **common
        )
        resolved_kw = kwargs_builder(
            operator,
            resolved_action,
            resolved_defaults,
            is_resolved=True,
            **common,
        )
        sync_operator_kwargs(operator, base_kw, resolved_kw)
    else:
        logger.warning(
            "No operator kwargs builder registered for task '%s'; "
            "resolved dynamic bindings were not applied to operator attributes.",
            getattr(operator, "task_id", getattr(base_action, "name", "")),
        )

    if isinstance(
        resolved_action, (PythonScriptActionModel, PythonVirtualenvActionModel)
    ) and isinstance(op_kw := getattr(operator, "op_kwargs", None), Mapping):
        resolved_action.config.opKwargs = dict(op_kw)

    operator.__dict__.pop("hook", None)
    operator._op_action_model = resolved_action
    operator._op_resolved_defaults = resolved_defaults


def _is_deferred_resume(context: Any) -> bool:
    if not isinstance(context, Mapping):
        return False
    ti = context.get("ti") or context.get("task_instance")
    if ti is None:
        return False
    ti_ctx = getattr(ti, "_ti_context_from_server", None)
    return any(
        isinstance(method, str) and bool(method)
        for method in (
            getattr(ti, "next_method", None),
            getattr(ti_ctx, "next_method", None),
        )
    )


def _push_failure_output(operator: Any, context: Any) -> None:
    ti = (
        context.get("ti") or context.get("task_instance")
        if isinstance(context, Mapping)
        else None
    )
    if not callable(xcom_push := getattr(ti, "xcom_push", None)):
        return
    try:
        xcom_push(
            key="return_value",
            value=harvester.extract_and_pack_output(
                operator, context, None, failed=True
            ),
        )
    except Exception as push_err:  # pylint: disable=broad-exception-caught
        logger.warning(
            "Failed to push failure metadata XCom for '%s': %s",
            getattr(operator, "task_id", ""),
            push_err,
        )


class RuntimeResolutionMixin:
    """Mixin for CEL evaluation, `run_if` skipping, and output harvesting."""

    _op_action_model: Any = None
    _op_pipeline_model: Any = None
    _op_task_role: str = TASK_ROLE_MAIN
    _op_is_entry_task: bool = True
    _op_is_output_task: bool = True
    _op_bindings_applied: bool = False

    def pre_execute(self, context: Any) -> None:
        """Evaluates `run_if` and applies dynamic bindings before execution."""
        action_model = getattr(self, "_op_action_model", None)
        pipeline_model = getattr(self, "_op_pipeline_model", None)
        if action_model is not None or pipeline_model is not None:
            run_if = (
                get_run_if_expr(action_model)
                if getattr(self, "_op_is_entry_task", True)
                and not _is_deferred_resume(context)
                else None
            )
            has_run_if = isinstance(run_if, str) and bool(run_if)
            task_role = getattr(self, "_op_task_role", TASK_ROLE_MAIN)
            has_bindings = has_applicable_bindings(
                pipeline_model, action_model, task_role
            )
            if has_run_if or has_bindings:
                eval_ctx = build_cel_eval_context(self, context)
                activation = cel_evaluator.build_cel_activation(eval_ctx)
                if has_run_if:
                    cond = cel_evaluator.evaluate_with_activation(
                        run_if, activation
                    )
                    if not isinstance(cond, bool):
                        raise TypeError(
                            f"Action '{getattr(action_model, 'name', '')}' "
                            f"run_if must evaluate to bool, "
                            f"got {type(cond).__name__}."
                        )
                    if not cond:
                        raise AirflowSkipException(
                            f"Action '{getattr(action_model, 'name', '')}' "
                            f"skipped: run_if ({run_if!r}) evaluated to False."
                        )
                if has_bindings:
                    apply_dynamic_bindings(
                        self, eval_ctx, activation=activation
                    )
                    self._op_bindings_applied = True
            harvester.inject_dataproc_output_uri(self, context)

        if callable(super_pre := getattr(super(), "pre_execute", None)):
            super_pre(context)

    def _run_and_harvest(self, context: Any, runner: Callable[[], Any]) -> Any:
        if getattr(self, "_op_in_harvest", False):
            return runner()
        has_output = getattr(
            self, "_op_action_model", None
        ) is not None and getattr(self, "_op_is_output_task", True)
        self._op_in_harvest = True
        try:
            raw_result = runner()
        except Exception as err:
            if has_output and not is_non_failure_signal(err):
                _push_failure_output(self, context)
            raise
        finally:
            self._op_in_harvest = False
        return (
            harvester.extract_and_pack_output(
                self, context, raw_result, failed=False
            )
            if has_output
            else raw_result
        )

    def execute(self, context: Any, **kwargs: Any) -> Any:
        """Executes operator and wraps output in `{value, metadata}`."""
        super_exec = super().execute  # type: ignore[misc]
        return self._run_and_harvest(
            context, lambda: super_exec(context, **kwargs)
        )

    def resume_execution(
        self,
        next_method: str,
        next_kwargs: dict[str, Any] | None,
        context: Any,
    ) -> Any:
        """Restores bindings on resumed worker and harvests deferred output."""
        action_model = getattr(self, "_op_action_model", None)
        pipeline_model = getattr(self, "_op_pipeline_model", None)
        task_role = getattr(self, "_op_task_role", TASK_ROLE_MAIN)
        if not getattr(
            self, "_op_bindings_applied", False
        ) and has_applicable_bindings(pipeline_model, action_model, task_role):
            eval_ctx = build_cel_eval_context(self, context)
            apply_dynamic_bindings(self, eval_ctx)
        if action_model is not None or pipeline_model is not None:
            harvester.inject_dataproc_output_uri(self, context)
        super_resume = super().resume_execution  # type: ignore[misc]
        return self._run_and_harvest(
            context,
            lambda: super_resume(next_method, next_kwargs, context),
        )
