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
"""Unit tests for operator wrapper mixins, caching, and runtime observability."""

from __future__ import annotations

import types
from datetime import timedelta
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from orchestration_pipelines_lib.internal_models.actions import (
    ExponentialBackoffStrategyModel,
    FixedDelayStrategyModel,
    RetryPolicyModel,
)
from orchestration_pipelines_lib.utils.metrics import BasicStatus
from orchestration_pipelines_lib.utils.operator_wrappers import (
    MetricsMixin,
    RetryMixin,
    _action_observability_context,
    _build_wrapped_operator_cls,
    wrap_operator,
    wrap_retry_operator,
)
from orchestration_pipelines_lib.utils.runtime_resolver import (
    AirflowSkipException,
    RuntimeResolutionMixin,
    TaskDeferred,
)

WRAPPER_MODULE = "orchestration_pipelines_lib.utils.operator_wrappers"
MOCK_REPORT_ACTION = (
    "orchestration_pipelines_lib.utils.metrics.report_action_execution"
)
MOCK_BASIC_STATUS = f"{WRAPPER_MODULE}.BasicStatus"
MOCK_BASE_OPERATOR_CLASSES = f"{WRAPPER_MODULE}.BASE_OPERATOR_CLASSES"
MOCK_OBSERVABILITY_CONTEXT = f"{WRAPPER_MODULE}._action_observability_context"


class DummyBaseOperator:
    """A dummy base class simulating Airflow's BaseOperator."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        for k, v in kwargs.items():
            setattr(self, k, v)

    def execute(self, context: Any, **kwargs: Any) -> Any:
        return "base_execute_result"


class DummyOperator(DummyBaseOperator):
    """A test operator simulating a concrete implementation."""


class NotAnOperator:
    """A class that does NOT inherit from BaseOperator."""


@pytest.fixture(autouse=True)
def _clear_wrapper_cache():
    _build_wrapped_operator_cls.cache_clear()
    yield
    _build_wrapped_operator_cls.cache_clear()


@pytest.fixture
def action_type():
    return MagicMock(name="action_type")


@pytest.fixture
def engine():
    return MagicMock(name="engine")


@pytest.fixture
def get_pipeline_metadata():
    mock_callable = MagicMock()
    mock_callable.return_value = ("test_bundle", "ignored", "test_pipeline")
    return mock_callable


@pytest.fixture
def context_dict():
    return {"dag": MagicMock(name="context_dag")}


@pytest.fixture
def operator_instance():
    inst = DummyOperator()
    inst.dag = MagicMock(name="instance_dag")  # type: ignore[attr-defined]
    return inst


@patch(MOCK_BASIC_STATUS)
@patch(MOCK_REPORT_ACTION)
def test_observability_context_reports_success_and_failure(
    mock_report_action,
    mock_basic_status_class,
    operator_instance,
    context_dict,
    action_type,
    engine,
    get_pipeline_metadata,
):
    """Tests observability context reports SUCCESS/FAILED and respects report_success."""
    mock_basic_status_class.SUCCESS = "MOCK_SUCCESS"
    mock_basic_status_class.FAILED = "MOCK_FAILED"

    with _action_observability_context(
        operator_instance,
        context_dict,
        action_type,
        engine,
        get_pipeline_metadata,
    ):
        pass
    mock_report_action.assert_called_once_with(
        "test_bundle", "test_pipeline", action_type, engine, "MOCK_SUCCESS"
    )
    mock_report_action.reset_mock()

    with _action_observability_context(
        operator_instance,
        context_dict,
        action_type,
        engine,
        get_pipeline_metadata,
        report_success=False,
    ):
        pass
    mock_report_action.assert_not_called()

    with (
        pytest.raises(ValueError, match="Mocked execution error"),
        _action_observability_context(
            operator_instance,
            context_dict,
            action_type,
            engine,
            get_pipeline_metadata,
        ),
    ):
        raise ValueError("Mocked execution error")
    mock_report_action.assert_called_once_with(
        "test_bundle", "test_pipeline", action_type, engine, "MOCK_FAILED"
    )


@pytest.mark.parametrize(
    "signal_exc",
    [
        AirflowSkipException("skipped"),
        type("DagRunTriggerException", (Exception,), {})(),
    ],
)
@patch(MOCK_REPORT_ACTION, autospec=True)
def test_observability_context_reraises_skip_without_reporting(
    mock_report_action,
    signal_exc,
    operator_instance,
    context_dict,
    action_type,
    engine,
    get_pipeline_metadata,
):
    """Tests non-failure signals are re-raised without emitting metrics."""
    with (
        pytest.raises(type(signal_exc)),
        _action_observability_context(
            operator_instance,
            context_dict,
            action_type,
            engine,
            get_pipeline_metadata,
        ),
    ):
        raise signal_exc
    mock_report_action.assert_not_called()


@patch(MOCK_BASE_OPERATOR_CLASSES, (DummyBaseOperator,))
def test_wrap_operator_memoizes_classes_and_handles_airflow_2_and_3_bases(
    action_type, engine, get_pipeline_metadata
):
    """Tests wrapper caching, non-operator guard, and multi-BaseOperator support."""
    assert (
        wrap_operator(NotAnOperator, action_type, engine, get_pipeline_metadata)
        is NotAnOperator
    )
    assert wrap_retry_operator(NotAnOperator) is NotAnOperator

    cls1 = wrap_operator(
        DummyOperator, action_type, engine, get_pipeline_metadata
    )
    cls2 = wrap_operator(
        DummyOperator, action_type, engine, get_pipeline_metadata
    )
    assert cls1 is cls2

    r1 = wrap_retry_operator(DummyOperator)
    r2 = wrap_retry_operator(DummyOperator)
    assert r1 is r2
    assert r1 is not cls1
    assert issubclass(r1, (DummyOperator, RetryMixin, RuntimeResolutionMixin))
    assert not issubclass(r1, MetricsMixin)

    class FakeAirflow2Base:
        def __init__(self, **kw: Any) -> None:
            pass

    class FakeAirflow3SdkBase:
        def __init__(self, **kw: Any) -> None:
            pass

    class SdkOnlyOp(FakeAirflow3SdkBase):
        pass

    with patch(
        MOCK_BASE_OPERATOR_CLASSES, (FakeAirflow2Base, FakeAirflow3SdkBase)
    ):
        wrapped_sdk = wrap_operator(
            SdkOnlyOp, action_type, engine, get_pipeline_metadata
        )
        assert wrapped_sdk is not SdkOnlyOp
        assert issubclass(wrapped_sdk, (SdkOnlyOp, MetricsMixin, RetryMixin))


@patch(MOCK_OBSERVABILITY_CONTEXT)
@patch(MOCK_BASE_OPERATOR_CLASSES, (DummyBaseOperator,))
def test_wrap_operator_creates_wrapper_and_forwards_sentinel_kwargs(
    mock_context_manager,
    action_type,
    engine,
    get_pipeline_metadata,
    context_dict,
):
    """Tests wrapped execute observability context and sentinel kwargs forwarding."""
    mock_context_manager.return_value.__enter__.return_value = None
    received_kwargs: dict[str, Any] = {}

    class SentinelOp(DummyBaseOperator):
        def execute(self, context: Any, **kwargs: Any) -> str:
            received_kwargs.update(kwargs)
            return "sentinel_ok"

    WrappedClass = wrap_operator(
        SentinelOp, action_type, engine, get_pipeline_metadata
    )
    instance = WrappedClass()
    assert WrappedClass.__name__ == "OrchestrationSentinelOp"
    assert (
        instance.execute(
            context_dict, OrchestrationSentinelOp__sentinel="sentinel_val"
        )
        == "sentinel_ok"
    )
    assert received_kwargs == {
        "OrchestrationSentinelOp__sentinel": "sentinel_val"
    }
    mock_context_manager.assert_called_once_with(
        instance, context_dict, action_type, engine, get_pipeline_metadata
    )


@patch(MOCK_OBSERVABILITY_CONTEXT)
@patch(MOCK_BASE_OPERATOR_CLASSES, (DummyBaseOperator,))
def test_wrap_operator_retry_policy_inheritance_override_and_backoff(
    mock_context_manager, action_type, engine, get_pipeline_metadata
):
    """Tests default_args inheritance, explicit override, and exponential backoff."""
    mock_context_manager.return_value.__enter__.return_value = None
    dag_policy = RetryPolicyModel(
        maxRetries=2, fixedDelay=FixedDelayStrategyModel(retryDelay="10s")
    )
    exp_policy = RetryPolicyModel(
        maxRetries=4,
        exponentialBackoff=ExponentialBackoffStrategyModel(
            initialDelay="10s",
            maxDelay="100s",
            multiplier=2.0,
            randomizeJitter=False,
        ),
    )
    mock_dag = MagicMock(default_args={"_op_custom_retry_policy": dag_policy})
    WrappedClass = wrap_operator(
        DummyOperator, action_type, engine, get_pipeline_metadata
    )

    inherited = WrappedClass(dag=mock_dag)
    assert (inherited.retries, inherited.retry_delay) == (
        2,
        timedelta(seconds=10),
    )

    overridden = WrappedClass(dag=mock_dag, _op_custom_retry_policy=exp_policy)
    assert (overridden.retries, overridden.retry_delay) == (
        4,
        timedelta(seconds=10),
    )
    overridden.execute({"dag": mock_dag, "ti": MagicMock(try_number=3)})
    assert overridden.retry_delay == timedelta(seconds=40)


@patch(MOCK_REPORT_ACTION, autospec=True)
@patch(MOCK_BASE_OPERATOR_CLASSES, (DummyBaseOperator,))
def test_wrap_operator_pre_execute_failure_skip_and_retry_delay(
    mock_report_action, action_type, engine, get_pipeline_metadata
):
    """Tests pre_execute emits FAILED on error, ignores skip, and avoids double SUCCESS."""
    policy = RetryPolicyModel(
        maxRetries=3,
        exponentialBackoff=ExponentialBackoffStrategyModel(
            initialDelay="10s", multiplier=2.0, randomizeJitter=False
        ),
    )
    ctx = {"dag": MagicMock(), "ti": MagicMock(try_number=2), "params": {}}
    wrapped_cls = wrap_operator(
        DummyOperator, action_type, engine, get_pipeline_metadata
    )

    # 1. Non-bool run_if raises TypeError in pre_execute -> FAILED + retry_delay updated.
    bad_op = wrapped_cls(
        dag=ctx["dag"],
        _op_action_model=types.SimpleNamespace(
            name="t_bad", type="script", runIf="123"
        ),
        _op_custom_retry_policy=policy,
    )
    with pytest.raises(TypeError, match="must evaluate to bool"):
        bad_op.pre_execute(ctx)
    mock_report_action.assert_called_once_with(
        "test_bundle", "test_pipeline", action_type, engine, BasicStatus.FAILED
    )
    assert bad_op.retry_delay == timedelta(seconds=20)
    mock_report_action.reset_mock()

    # 2. run_if=false raises AirflowSkipException -> no metric emitted.
    skip_op = wrapped_cls(
        dag=ctx["dag"],
        _op_action_model=types.SimpleNamespace(
            name="t_skip", type="script", runIf="false"
        ),
    )
    with pytest.raises(AirflowSkipException):
        skip_op.pre_execute(ctx)
    mock_report_action.assert_not_called()

    # 3. Successful pre_execute -> execute emits SUCCESS strictly once.
    ok_op = wrapped_cls(
        dag=ctx["dag"],
        _op_action_model=types.SimpleNamespace(
            name="t_ok", type="script", runIf="true"
        ),
    )
    ok_op.pre_execute(ctx)
    mock_report_action.assert_not_called()
    assert ok_op.execute(ctx) == {
        "value": "base_execute_result",
        "metadata": {},
    }
    mock_report_action.assert_called_once_with(
        "test_bundle", "test_pipeline", action_type, engine, BasicStatus.SUCCESS
    )


@patch(MOCK_REPORT_ACTION, autospec=True)
@patch(MOCK_BASE_OPERATOR_CLASSES, (DummyBaseOperator,))
def test_wrap_operator_reports_failure_when_harvest_raises(
    mock_report_action, action_type, engine, get_pipeline_metadata, context_dict
):
    """Tests oversized output harvesting errors emit a FAILED metric."""

    class _OversizedOp(DummyBaseOperator):
        def execute(self, context: Any, **kwargs: Any) -> str:
            return "x" * (1024 * 1024 + 10)

    wrapped_cls = wrap_operator(
        _OversizedOp, action_type, engine, get_pipeline_metadata
    )
    op = wrapped_cls(
        dag=types.SimpleNamespace(default_args={}),
        _op_action_model=types.SimpleNamespace(name="t1", type="script"),
    )
    with pytest.raises(ValueError, match="exceeds"):
        op.execute(context_dict)
    assert mock_report_action.call_args.args[-1] == BasicStatus.FAILED


@patch(MOCK_REPORT_ACTION, autospec=True)
@patch(MOCK_BASE_OPERATOR_CLASSES, (DummyBaseOperator,))
def test_wrap_operator_deferrable_resume_and_execute_reentrancy(
    mock_report_action, action_type, engine, get_pipeline_metadata
):
    """Tests resume_execution (including next_method='execute') emits once and wraps once."""

    class _DeferrableOp(DummyBaseOperator):
        resumed = False

        def execute(self, context: Any, **kwargs: Any) -> Any:
            if not self.resumed:
                raise TaskDeferred(trigger=MagicMock(), method_name="on_done")
            return {"from_execute": True}

        def resume_execution(
            self, next_method: str, next_kwargs: Any, context: Any
        ) -> Any:
            return getattr(self, next_method)(
                context=context, **(next_kwargs or {})
            )

        def on_done(self, context: Any, event: Any = None) -> Any:
            if event == "fail":
                raise RuntimeError("trigger error")
            return {"rows": event}

    policy = RetryPolicyModel(
        maxRetries=3,
        exponentialBackoff=ExponentialBackoffStrategyModel(
            initialDelay="10s", multiplier=2.0, randomizeJitter=False
        ),
    )
    pushed: dict[str, Any] = {}
    ctx = {
        "dag": MagicMock(),
        "ti": MagicMock(
            try_number=2,
            xcom_push=lambda key, value: pushed.__setitem__(key, value),
        ),
    }
    wrapped_cls = wrap_operator(
        _DeferrableOp, action_type, engine, get_pipeline_metadata
    )
    inst = wrapped_cls(
        _op_action_model=types.SimpleNamespace(name="t1", type="script"),
        _op_custom_retry_policy=policy,
    )
    inst.dag = ctx["dag"]  # type: ignore[attr-defined]

    with pytest.raises(TaskDeferred):
        inst.execute(ctx)
    mock_report_action.assert_not_called()
    assert not pushed

    assert inst.resume_execution("on_done", {"event": 5}, ctx) == {
        "value": {"rows": 5},
        "metadata": {},
    }
    mock_report_action.assert_called_once_with(
        "test_bundle", "test_pipeline", action_type, engine, BasicStatus.SUCCESS
    )
    mock_report_action.reset_mock()

    # Re-entering execute() via next_method="execute" must emit SUCCESS once
    # and wrap XCom output once (not {"value": {"value": ...}}).
    inst.resumed = True
    assert inst.resume_execution("execute", {}, ctx) == {
        "value": {"from_execute": True},
        "metadata": {},
    }
    mock_report_action.assert_called_once_with(
        "test_bundle", "test_pipeline", action_type, engine, BasicStatus.SUCCESS
    )

    with pytest.raises(RuntimeError, match="trigger error"):
        inst.resume_execution("on_done", {"event": "fail"}, ctx)
    assert mock_report_action.call_args.args[-1] == BasicStatus.FAILED
    assert pushed["return_value"] == {"value": None, "metadata": {}}
    assert inst.retry_delay == timedelta(seconds=20)
