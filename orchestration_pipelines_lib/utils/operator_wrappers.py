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
"""Airflow operator wrapper mixins for retries, metrics, and runtime resolution."""

from __future__ import annotations

import functools
from collections.abc import Callable, Generator, Mapping
from contextlib import contextmanager
from datetime import timedelta
from typing import TYPE_CHECKING, Any, TypeVar, cast

from airflow.models import BaseOperator as _ModelsBaseOperator

from orchestration_pipelines_lib.utils import metrics
from orchestration_pipelines_lib.utils.metrics import (
    ActionExecutionEngine,
    ActionExecutionType,
    BasicStatus,
)
from orchestration_pipelines_lib.utils.runtime_resolver import (
    RuntimeResolutionMixin,
    is_non_failure_signal,
)

try:
    from airflow.sdk import (  # pyright: ignore[reportMissingImports]
        BaseOperator as _SdkBaseOperator,
    )
except ImportError:  # pragma: no cover
    BASE_OPERATOR_CLASSES: tuple[type, ...] = (_ModelsBaseOperator,)
else:
    BASE_OPERATOR_CLASSES = (
        (_ModelsBaseOperator,)
        if _SdkBaseOperator is _ModelsBaseOperator
        else (_ModelsBaseOperator, _SdkBaseOperator)
    )

if TYPE_CHECKING:
    try:
        from airflow.sdk import DAG, Context
    except ImportError:
        from airflow import DAG
        from airflow.utils.context import Context

T = TypeVar("T")


@contextmanager
def _action_observability_context(
    operator_instance: Any,
    context: Context,
    action_type: ActionExecutionType,
    engine: ActionExecutionEngine,
    get_pipeline_metadata: Callable[[DAG], tuple[str, str, str]],
    *,
    report_success: bool = True,
) -> Generator[None, None, None]:
    dag_obj = getattr(operator_instance, "dag", None) or (
        context.get("dag") if isinstance(context, Mapping) else None
    )
    bundle_id, _, pipeline_id = get_pipeline_metadata(dag_obj)  # type: ignore[arg-type]

    try:
        yield
        if report_success:
            metrics.report_action_execution(
                bundle_id, pipeline_id, action_type, engine, BasicStatus.SUCCESS
            )
    except Exception as err:
        if not is_non_failure_signal(err):
            metrics.report_action_execution(
                bundle_id, pipeline_id, action_type, engine, BasicStatus.FAILED
            )
        raise


class RetryMixin:
    """Mixin for pipeline operators that manages custom retry policies."""

    _op_custom_retry_policy: Any = None

    def __init__(
        self, *args: Any, _op_custom_retry_policy: Any = None, **kwargs: Any
    ) -> None:
        """Initializes RetryMixin and resolves retry policy kwargs."""
        if _op_custom_retry_policy is None:
            dag = kwargs.get("dag")
            if (
                dag
                and hasattr(dag, "default_args")
                and isinstance(dag.default_args, dict)
            ):
                _op_custom_retry_policy = dag.default_args.get(
                    "_op_custom_retry_policy"
                )

        if _op_custom_retry_policy:
            from orchestration_pipelines_lib.dag_generator.airflow_adapters.common_utils.retry_resolver import (  # noqa: E501
                CUSTOM_RETRY_POLICY_KEY,
                RetryResolver,
            )

            for k, v in RetryResolver.resolve_policy_kwargs(
                _op_custom_retry_policy
            ).items():
                if k != CUSTOM_RETRY_POLICY_KEY:
                    kwargs.setdefault(k, v)

        kwargs.pop("_op_custom_retry_policy", None)
        super().__init__(*args, **kwargs)
        self._op_custom_retry_policy = _op_custom_retry_policy

    def _calculate_retry_delay(self, context: Any) -> timedelta:
        """Dynamically calculates retry delay based on ti.try_number."""
        policy = getattr(self, "_op_custom_retry_policy", None)
        if not policy:
            return getattr(self, "retry_delay", timedelta(seconds=0))

        from orchestration_pipelines_lib.dag_generator.airflow_adapters.common_utils.retry_resolver import (  # noqa: E501
            RetryResolver,
        )

        delay_fn = RetryResolver.get_retry_delay_callable(policy)
        return delay_fn(context)

    def _sync_retry_delay(self, context: Any) -> None:
        if getattr(self, "_op_custom_retry_policy", None):
            self.retry_delay = self._calculate_retry_delay(context)

    def pre_execute(self, context: Any) -> None:
        """Runs pre_execute and updates retry_delay on completion or failure."""
        try:
            if callable(super_pre := getattr(super(), "pre_execute", None)):
                super_pre(context)
        finally:
            self._sync_retry_delay(context)

    def execute(self, context: Any, **kwargs: Any) -> Any:
        """Executes operator and updates retry_delay dynamically."""
        self._sync_retry_delay(context)
        return super().execute(context, **kwargs)  # type: ignore[misc]

    def resume_execution(
        self,
        next_method: str,
        next_kwargs: dict[str, Any] | None,
        context: Any,
    ) -> Any:
        """Resumes a deferred operator and updates retry_delay on completion."""
        try:
            return super().resume_execution(  # type: ignore[misc]
                next_method, next_kwargs, context
            )
        finally:
            self._sync_retry_delay(context)


class MetricsMixin:
    """Mixin for pipeline operators that emits execution metrics."""

    _op_action_type: ActionExecutionType
    _op_engine: ActionExecutionEngine
    _op_get_pipeline_metadata: Callable[[DAG], tuple[str, str, str]]

    def pre_execute(self, context: Any) -> None:
        """Reports FAILED metrics if pre_execute raises a non-signal error."""
        super_pre = getattr(super(), "pre_execute", None)
        if not callable(super_pre):
            return
        with _action_observability_context(
            self,
            context,
            self._op_action_type,
            self._op_engine,
            self._op_get_pipeline_metadata,
            report_success=False,
        ):
            super_pre(context)

    def _run_with_metrics(self, context: Any, runner: Callable[[], Any]) -> Any:
        if getattr(self, "_op_in_metrics", False):
            return runner()
        self._op_in_metrics = True
        try:
            with _action_observability_context(
                self,
                context,
                self._op_action_type,
                self._op_engine,
                self._op_get_pipeline_metadata,
            ):
                return runner()
        finally:
            self._op_in_metrics = False

    def execute(self, context: Any, **kwargs: Any) -> Any:
        """Executes operator within the action observability context."""
        super_exec = super().execute  # type: ignore[misc]
        return self._run_with_metrics(
            context, lambda: super_exec(context, **kwargs)
        )

    def resume_execution(
        self,
        next_method: str,
        next_kwargs: dict[str, Any] | None,
        context: Any,
    ) -> Any:
        """Resumes a deferred operator within the action observability context."""
        super_resume = super().resume_execution  # type: ignore[misc]
        return self._run_with_metrics(
            context, lambda: super_resume(next_method, next_kwargs, context)
        )


def _is_airflow_operator_cls(cls: Any) -> bool:
    return isinstance(cls, type) and issubclass(cls, BASE_OPERATOR_CLASSES)


@functools.cache
def _build_wrapped_operator_cls(
    base_operator_class: type,
    mixins: tuple[type, ...],
    action_type: ActionExecutionType | None = None,
    engine: ActionExecutionEngine | None = None,
    get_pipeline_metadata: Callable[[DAG], tuple[str, str, str]] | None = None,
) -> type:
    """Builds and caches a dynamic operator subclass composed with `mixins`."""
    cls_name = f"Orchestration{base_operator_class.__name__}"
    cls_qualname = f"Orchestration{base_operator_class.__qualname__}"

    def __init__(
        self: Any,
        *args: Any,
        _op_custom_retry_policy: Any = None,
        **kwargs: Any,
    ) -> None:
        super(orchestration_cls, self).__init__(
            *args,
            _op_custom_retry_policy=_op_custom_retry_policy,
            **kwargs,
        )

    namespace: dict[str, Any] = {
        "__init__": __init__,
        "__qualname__": cls_qualname,
        "__module__": base_operator_class.__module__,
    }
    if action_type is not None:
        namespace["_op_action_type"] = action_type
    if engine is not None:
        namespace["_op_engine"] = engine
    if get_pipeline_metadata is not None:
        namespace["_op_get_pipeline_metadata"] = staticmethod(
            get_pipeline_metadata
        )

    orchestration_cls = type(
        cls_name, (*mixins, base_operator_class), namespace
    )
    return orchestration_cls


def wrap_retry_operator(base_operator_class: type[T]) -> type[T]:
    """Creates a cached wrapper operator class with RetryMixin and runtime resolution."""
    if not _is_airflow_operator_cls(base_operator_class):
        return base_operator_class
    return cast(
        type[T],
        _build_wrapped_operator_cls(
            base_operator_class, (RetryMixin, RuntimeResolutionMixin)
        ),
    )


def wrap_operator(
    base_operator_class: type[T],
    action_type: ActionExecutionType,
    engine: ActionExecutionEngine,
    get_pipeline_metadata: Callable[[DAG], tuple[str, str, str]],
) -> type[T]:
    """Creates a cached composite wrapper operator class using mixins."""
    if not _is_airflow_operator_cls(base_operator_class):
        return base_operator_class
    # Required MRO: RetryMixin -> MetricsMixin -> RuntimeResolutionMixin -> BaseOperator
    # so pre_execute/resume_execution resolves _op_custom_retry_policy before RetryMixin
    # computes retry_delay, and output harvesting runs inside MetricsMixin's context.
    return cast(
        type[T],
        _build_wrapped_operator_cls(
            base_operator_class,
            (RetryMixin, MetricsMixin, RuntimeResolutionMixin),
            action_type,
            engine,
            get_pipeline_metadata,
        ),
    )
