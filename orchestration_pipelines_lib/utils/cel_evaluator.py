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
"""Runtime CEL compilation, activation building, and expression evaluation."""

from __future__ import annotations

import functools
import threading
from collections.abc import Callable, Mapping
from typing import TYPE_CHECKING, Any

from orchestration_pipelines_lib.utils import output_harvester as harvester
from orchestration_pipelines_models.utils import cel_utils

if TYPE_CHECKING:
    import celpy

_CEL_BINARY_NUMERIC_OPS = (
    "_<_", "_<=_", "_>_", "_>=_", "_==_", "_!=_", "_+_", "_-_", "_*_", "_/_"
)  # fmt: skip
_NULL_EQ_RESULTS: dict[str, bool] = {"_==_": False, "_!=_": True}
_COMPILE_LOCK = threading.Lock()


def _wrap_binary_op(
    fn: Callable[..., Any],
    numeric_types: tuple[type, ...],
    double_cls: type,
    bool_cls: type,
    error_cls: type,
    *,
    null_result: bool | None = None,
) -> Callable[..., Any]:
    """Handles `null` equality and promotes mixed `int`/`double` operands."""

    def _wrapped(*args: Any) -> Any:
        if any(isinstance(arg, error_cls) for arg in args):
            return fn(*args)
        if len(args) == 2:
            if null_result is not None and (
                (args[0] is None) ^ (args[1] is None)
            ):
                return bool_cls(null_result)
            if all(isinstance(arg, numeric_types) for arg in args) and any(
                isinstance(arg, double_cls) for arg in args
            ):
                args = tuple(double_cls(float(arg)) for arg in args)
        return fn(*args)

    return _wrapped


def _wrap_in_op(
    fn: Callable[..., Any],
    numeric_types: tuple[type, ...],
    list_cls: type,
    bool_cls: type,
) -> Callable[..., Any]:
    """Allows numeric membership checks across `int` and `double` list items."""

    def _wrapped(lhs: Any, rhs: Any) -> Any:
        if isinstance(lhs, numeric_types) and isinstance(rhs, list_cls):
            target = _cel_to_python(lhs)
            return bool_cls(
                any(
                    isinstance(item, numeric_types)
                    and _cel_to_python(item) == target
                    for item in rhs
                )
            )
        return fn(lhs, rhs)

    return _wrapped


@functools.cache
def _get_cel_env() -> tuple[celpy.Environment, dict[str, Any]]:
    import celpy
    from celpy import celtypes
    from celpy import evaluation as cel_eval

    numeric_types = (
        celtypes.IntType,
        celtypes.UintType,
        celtypes.DoubleType,
    )
    base_fns = cel_eval.base_functions
    custom_fns = {
        **base_fns,
        **{
            op: _wrap_binary_op(
                base_fns[op],
                numeric_types,
                celtypes.DoubleType,
                celtypes.BoolType,
                cel_eval.CELEvalError,
                null_result=_NULL_EQ_RESULTS.get(op),
            )
            for op in _CEL_BINARY_NUMERIC_OPS
            if op in base_fns
        },
        "_in_": _wrap_in_op(
            base_fns["_in_"],
            numeric_types,
            celtypes.ListType,
            celtypes.BoolType,
        ),
    }
    return celpy.Environment(), custom_fns


@functools.lru_cache(maxsize=512)
def _compile_cel(raw_expr: str) -> Any:
    expr = cel_utils.normalize_cel_expression(raw_expr, warn=False)
    with _COMPILE_LOCK:
        env, custom_fns = _get_cel_env()
        return env.program(env.compile(expr), functions=custom_fns)


def _cel_to_python(value: Any) -> Any:
    from celpy import celtypes

    if isinstance(value, celtypes.BoolType):
        return bool(value)
    if isinstance(value, (celtypes.IntType, celtypes.UintType)):
        return int(value)
    if isinstance(value, celtypes.DoubleType):
        return float(value)
    if isinstance(value, celtypes.StringType):
        return str(value)
    if isinstance(value, celtypes.ListType):
        return [_cel_to_python(item) for item in value]
    if isinstance(value, celtypes.MapType):
        return {_cel_to_python(k): _cel_to_python(v) for k, v in value.items()}
    return value


def _prepare_outputs(outputs: Any) -> dict[str, Any]:
    """Omits top-level `value: None` so `has(outputs.task.value)` is False."""
    if not isinstance(outputs, Mapping):
        return {}
    prepared: dict[str, Any] = {}
    for task_id, envelope in outputs.items():
        norm = harvester.normalize_json_value(envelope)
        if isinstance(norm, Mapping):
            prepared[str(task_id)] = {
                str(k): v
                for k, v in norm.items()
                if not (k == "value" and v is None)
            }
        else:
            prepared[str(task_id)] = norm
    return prepared


def build_cel_activation(eval_ctx: Mapping[str, Any]) -> dict[str, Any]:
    """Builds a `celpy` activation mapping from `{parameters, run, outputs}`."""
    import celpy

    return {
        "parameters": celpy.json_to_cel(
            harvester.normalize_json_value(eval_ctx.get("parameters") or {})
        ),
        "run": celpy.json_to_cel(
            harvester.normalize_json_value(eval_ctx.get("run") or {})
        ),
        "outputs": celpy.json_to_cel(
            _prepare_outputs(eval_ctx.get("outputs") or {})
        ),
    }


def evaluate_with_activation(
    raw_expr: str, activation: Mapping[str, Any]
) -> Any:
    """Evaluates `raw_expr` against a pre-built CEL activation mapping."""
    return _cel_to_python(_compile_cel(raw_expr).evaluate(activation))


def evaluate_cel_expression(raw_expr: str, eval_ctx: Mapping[str, Any]) -> Any:
    """Evaluates a CEL expression against `{parameters, run, outputs}`."""
    return evaluate_with_activation(raw_expr, build_cel_activation(eval_ctx))
