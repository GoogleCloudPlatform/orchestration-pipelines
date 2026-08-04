"""Tests for the metrics module."""

import logging
from unittest.mock import MagicMock, Mock, patch

import pytest
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunType

from orchestration_pipelines_lib.utils.metrics import (
    MODULE_NAME,
    VERSION_LABEL,
    ActionExecutionEngine,
    ActionExecutionType,
    BasicStatus,
    ParsingStatus,
    PipelineRunTriggerType,
    _action_observability_context,
    _emit_metric,
    _incr_callback,
    _rename_observability_class,
    _timing_callback,
    report_action_execution,
    report_init_context,
    report_parsing,
    report_pipeline_run,
    wrap_observability_operator,
)

TARGET_MODULE = "orchestration_pipelines_lib.utils.metrics"


MOCK_REPORT_ACTION = f"{TARGET_MODULE}.report_action_execution"
MOCK_BASIC_STATUS = f"{TARGET_MODULE}.BasicStatus"
MOCK_BASE_OPERATOR = f"{TARGET_MODULE}.BaseOperator"
MOCK_OBSERVABILITY_CONTEXT = f"{TARGET_MODULE}._action_observability_context"


@pytest.fixture
def action_type():
    """Provides a mock action execution type."""
    return MagicMock(name="action_type")


@pytest.fixture
def engine():
    """Provides a mock action execution engine."""
    return MagicMock(name="engine")


@pytest.fixture
def get_pipeline_metadata():
    """Provides a mock callable returning tuple of (bundle_id, ignored, pipeline_id)."""  # noqa: E501
    mock_callable = MagicMock()
    mock_callable.return_value = ("test_bundle", "ignored", "test_pipeline")
    return mock_callable


@pytest.fixture
def context_dict():
    """Provides a mock Airflow context dictionary."""
    return {"dag": MagicMock(name="context_dag")}


@pytest.fixture
def operator_instance():
    """Provides an instance of the DummyOperator with a mock DAG attached."""
    inst = DummyOperator()
    inst.dag = MagicMock(name="instance_dag")  # type: ignore
    return inst


@patch(f"{TARGET_MODULE}._timing_callback")
@patch(f"{TARGET_MODULE}._emit_metric")
def test_report_parsing(mock_emit_metric, mock_timing_callback):
    """Tests report_parsing delegates metrics to _emit_metric."""
    bundle_id = "my_bundle"
    pipeline_id = "my_pipe"
    status = ParsingStatus.SUCCESS
    duration = 125.5

    mocked_callback_result = MagicMock()
    mock_timing_callback.return_value = mocked_callback_result

    report_parsing(bundle_id, pipeline_id, status, duration)

    assert mock_emit_metric.call_count == 2

    _, kwargs = mock_emit_metric.call_args_list[0]
    assert kwargs["bundle_id"] == bundle_id
    assert kwargs["pipeline_id"] == pipeline_id
    assert kwargs["metric"] == "parse"
    assert kwargs["status"] == status
    assert kwargs["metric_callback"] == _incr_callback

    _, kwargs = mock_emit_metric.call_args_list[1]
    assert kwargs["bundle_id"] == bundle_id
    assert kwargs["pipeline_id"] == pipeline_id
    assert kwargs["metric"] == "parse.duration"
    assert kwargs["status"] == status
    assert kwargs["metric_callback"] == mocked_callback_result
    mock_timing_callback.assert_called_once_with(duration)


@patch(f"{TARGET_MODULE}._emit_metric")
def test_report_pipeline_run(mock_emit_metric):
    """Tests report_pipeline_run calls _emit_metric correctly."""
    bundle_id = "my_bundle"
    pipeline_id = "my_pipe"
    trigger_type = PipelineRunTriggerType.SCHEDULED
    status = BasicStatus.SUCCESS

    report_pipeline_run(bundle_id, pipeline_id, trigger_type, status)

    mock_emit_metric.assert_called_once_with(
        bundle_id=bundle_id,
        pipeline_id=pipeline_id,
        metric="pipeline_run",
        status="SUCCESS",
        metric_callback=_incr_callback,
        additional_tags={"trigger_type": "SCHEDULED"},
    )


@patch(f"{TARGET_MODULE}._emit_metric")
def test_report_pipeline_run_default_bundle(mock_emit_metric):
    """Tests report_pipeline_run handles None bundle_id."""
    bundle_id = None
    pipeline_id = "my_pipe"
    trigger_type = PipelineRunTriggerType.MANUAL
    status = BasicStatus.FAILED

    report_pipeline_run(bundle_id, pipeline_id, trigger_type, status)

    mock_emit_metric.assert_called_once_with(
        bundle_id=bundle_id,
        pipeline_id=pipeline_id,
        metric="pipeline_run",
        status="FAILED",
        metric_callback=_incr_callback,
        additional_tags={"trigger_type": "MANUAL"},
    )


@patch(f"{TARGET_MODULE}._emit_metric")
def test_report_action_execution(mock_emit_metric):
    """Tests report_action_execution correctly invokes _emit_metric."""
    bundle_id = "test_bundle"
    pipeline_id = "test_pipeline"
    action_type = ActionExecutionType.NOTEBOOK
    engine = ActionExecutionEngine.DATAFORM
    status = BasicStatus.SUCCESS

    report_action_execution(bundle_id, pipeline_id, action_type, engine, status)

    mock_emit_metric.assert_called_once_with(
        bundle_id=bundle_id,
        pipeline_id=pipeline_id,
        metric="action_execution",
        status="SUCCESS",
        metric_callback=_incr_callback,
        additional_tags={"action_type": "NOTEBOOK", "engine": "DATAFORM"},
    )


@patch(f"{TARGET_MODULE}._emit_metric")
def test_report_action_execution_with_none_bundle_id(mock_emit_metric):
    """Tests report_action_execution correctly invokes _emit_metric with None bundle_id."""  # noqa: E501
    bundle_id = None
    pipeline_id = "test_pipeline"
    action_type = ActionExecutionType.PYTHON
    engine = ActionExecutionEngine.LOCAL
    status = BasicStatus.FAILED

    report_action_execution(bundle_id, pipeline_id, action_type, engine, status)

    mock_emit_metric.assert_called_once_with(
        bundle_id=None,
        pipeline_id=pipeline_id,
        metric="action_execution",
        status="FAILED",
        metric_callback=_incr_callback,
        additional_tags={"action_type": "PYTHON", "engine": "LOCAL"},
    )


@patch(f"{TARGET_MODULE}._emit_metric")
def test_report_init_context(mock_emit_metric):
    """Tests report_init_context correctly invokes _emit_metric."""
    bundle_id = "test_bundle"
    pipeline_id = "test_pipeline"
    status = BasicStatus.SUCCESS

    report_init_context(bundle_id, pipeline_id, status)

    mock_emit_metric.assert_called_once_with(
        bundle_id=bundle_id,
        pipeline_id=pipeline_id,
        metric="init_context",
        status="SUCCESS",
        metric_callback=_incr_callback,
    )


@patch(f"{TARGET_MODULE}._emit_metric")
def test_report_init_context_with_none_bundle_id(mock_emit_metric):
    """Tests report_init_context correctly invokes _emit_metric with None bundle_id."""  # noqa: E501
    bundle_id = None
    pipeline_id = "test_pipeline"
    status = BasicStatus.FAILED

    report_init_context(bundle_id, pipeline_id, status)

    mock_emit_metric.assert_called_once_with(
        bundle_id=None,
        pipeline_id=pipeline_id,
        metric="init_context",
        status="FAILED",
        metric_callback=_incr_callback,
    )


@patch(f"{TARGET_MODULE}.Stats.incr")
def test_incr_callback(mock_stats_incr):
    """Tests _incr_callback correctly invokes Stats.incr."""
    tags = {"status": "SUCCESS"}
    topic = "test.topic"

    _incr_callback(topic, tags=tags)

    mock_stats_incr.assert_called_once_with(topic, tags=tags)


@patch(f"{TARGET_MODULE}.Stats.timing")
def test_timing_callback(mock_stats_timing):
    """Tests _timing_callback lambda invokes Stats.timing."""
    duration = 42.0
    tags = {"status": "PARSING_ERROR"}
    topic = "test.duration.topic"
    callback_fn = _timing_callback(duration)

    callback_fn(topic, tags)

    mock_stats_timing.assert_called_once_with(topic, duration, tags=tags)


def test_emit_metric_success():
    """Tests successful metric emission for both StatsD and OTel."""
    mock_callback = MagicMock()

    bundle_id = "my_bundle"
    pipeline_id = "my_pipe"
    metric = "my_metric"
    status = ParsingStatus.SUCCESS

    expected_statsd_name = f"{MODULE_NAME}.{bundle_id}.{pipeline_id}.{metric}.SUCCESS.{VERSION_LABEL}"  # noqa: E501
    expected_otel_name = f"{MODULE_NAME}.{metric}"
    expected_tags = {
        "status": "SUCCESS",
        "library_version": VERSION_LABEL,
        "pipeline_name": pipeline_id,
        "bundle_name": bundle_id,
    }

    _emit_metric(
        bundle_id=bundle_id,
        pipeline_id=pipeline_id,
        metric=metric,
        status=str(status),
        metric_callback=mock_callback,
    )

    assert mock_callback.call_count == 2
    mock_callback.assert_any_call(expected_statsd_name, None)
    mock_callback.assert_any_call(expected_otel_name, expected_tags)


def test_emit_metric_default_bundle_id():
    """Tests fallback to 'default' bundle ID if None given."""
    mock_callback = MagicMock()

    bundle_id = None
    pipeline_id = "my_pipe"
    metric = "my_metric"
    status = ParsingStatus.SUCCESS
    metric_callback = mock_callback

    expected_statsd_name = (
        f"{MODULE_NAME}.default.{pipeline_id}.{metric}.SUCCESS.{VERSION_LABEL}"
    )

    _emit_metric(
        bundle_id=bundle_id,
        pipeline_id=pipeline_id,
        metric=metric,
        status=status,
        metric_callback=metric_callback,
    )

    args, _ = mock_callback.call_args_list[0]
    assert args[0] == expected_statsd_name


def test_emit_metric_statsd_exception(caplog):
    """Tests StatsD exceptions are caught and logged."""
    bundle_id = None
    pipeline_id = "my_pipe"
    metric = "my_metric"
    status = ParsingStatus.SUCCESS
    mock_callback = MagicMock(side_effect=[Exception("StatsD is down"), None])

    with caplog.at_level(logging.WARNING):
        _emit_metric(
            bundle_id=bundle_id,
            pipeline_id=pipeline_id,
            metric=metric,
            status=status,
            metric_callback=mock_callback,
        )

    assert mock_callback.call_count == 2
    assert "Could not emit StatsD metric 'my_metric'" in caplog.text
    assert "StatsD is down" in caplog.text


def test_emit_metric_otel_exception(caplog):
    """Tests OTel exceptions are caught and logged."""
    bundle_id = None
    pipeline_id = "my_pipe"
    metric = "my_metric"
    status = ParsingStatus.SUCCESS
    mock_callback = MagicMock(side_effect=[None, Exception("OTel is down")])

    with caplog.at_level(logging.WARNING):
        _emit_metric(
            bundle_id=bundle_id,
            pipeline_id=pipeline_id,
            metric=metric,
            status=status,
            metric_callback=mock_callback,
        )

    assert mock_callback.call_count == 2
    assert "Could not emit OTel metric 'my_metric'" in caplog.text
    assert "OTel is down" in caplog.text


def test_emit_metric_with_additional_tags():
    """Tests emitting metrics when additional tags are provided."""
    mock_callback = Mock()
    tags_to_add = {
        "trigger_type": str(PipelineRunTriggerType.SCHEDULED),
        "test_key": "test_value",
    }
    bundle_id = "test_bundle"
    pipeline_id = "test_pipeline"
    metric = "test_metric"
    status = BasicStatus.SUCCESS

    _emit_metric(
        bundle_id=bundle_id,
        pipeline_id=pipeline_id,
        metric=metric,
        status=str(status),
        metric_callback=mock_callback,
        additional_tags=tags_to_add,
    )

    expected_statsd_topic = f"{MODULE_NAME}.test_bundle.test_pipeline.test_metric.SCHEDULED.test_value.SUCCESS.{VERSION_LABEL}"  # noqa: E501
    expected_tags = {
        "status": "SUCCESS",
        "library_version": VERSION_LABEL,
        "pipeline_name": pipeline_id,
        "bundle_name": bundle_id,
        "trigger_type": "SCHEDULED",
        "test_key": "test_value",
    }
    assert mock_callback.call_count == 2
    mock_callback.assert_any_call(expected_statsd_topic, None)
    mock_callback.assert_any_call(f"{MODULE_NAME}.test_metric", expected_tags)


def test_emit_metric_with_no_additional_tags():
    """Tests emitting metrics without any additional tags."""
    mock_callback = Mock()

    bundle_id = "test_bundle"
    pipeline_id = "test_pipeline"
    metric = "test_metric"
    status = BasicStatus.FAILED

    _emit_metric(
        bundle_id=bundle_id,
        pipeline_id=pipeline_id,
        metric=metric,
        status=str(status),
        metric_callback=mock_callback,
    )

    expected_statsd_topic = f"{MODULE_NAME}.test_bundle.test_pipeline.test_metric.FAILED.{VERSION_LABEL}"  # noqa: E501
    expected_tags = {
        "status": "FAILED",
        "library_version": VERSION_LABEL,
        "pipeline_name": "test_pipeline",
        "bundle_name": "test_bundle",
    }
    assert mock_callback.call_count == 2
    mock_callback.assert_any_call(expected_statsd_topic, None)
    mock_callback.assert_any_call(f"{MODULE_NAME}.test_metric", expected_tags)


@pytest.mark.parametrize(
    "dag_run_state, expected_status",
    [
        (DagRunState.SUCCESS.value, BasicStatus.SUCCESS),
        (DagRunState.FAILED.value, BasicStatus.FAILED),
        (DagRunState.RUNNING.value, BasicStatus.FAILED),
        (DagRunState.QUEUED.value, BasicStatus.FAILED),
        ("some_random_string", BasicStatus.FAILED),
    ],
)
def test_basic_status_from_dag_run_state(
    dag_run_state: str, expected_status: BasicStatus
):
    """Tests BasicStatus.from_dag_run_state."""
    result = BasicStatus.from_dag_run_state(dag_run_state)

    assert result == expected_status


@pytest.mark.parametrize(
    "dag_run_type, expected_trigger_type",
    [
        (DagRunType.MANUAL.value, PipelineRunTriggerType.MANUAL),
        (DagRunType.SCHEDULED.value, PipelineRunTriggerType.SCHEDULED),
        (DagRunType.BACKFILL_JOB.value, PipelineRunTriggerType.UNKNOWN),
        ("random_type", PipelineRunTriggerType.UNKNOWN),
        (None, PipelineRunTriggerType.UNKNOWN),
    ],
)
def test_pipeline_run_trigger_type_from_dag_run_type(
    dag_run_type: str | None, expected_trigger_type: PipelineRunTriggerType
):
    """Tests PipelineRunTriggerType.from_dag_run_type."""
    result = PipelineRunTriggerType.from_dag_run_type(dag_run_type)

    assert result == expected_trigger_type


class DummyBaseOperator:
    """A dummy base class simulating Airflow's BaseOperator."""

    def execute(self, context):  # noqa: D102
        return "base_execute_result"


class DummyOperator(DummyBaseOperator):
    """A test operator simulating a concrete implementation."""

    pass


class NotAnOperator:
    """A class that does NOT inherit from the dummy BaseOperator."""

    pass


def test_rename_observability_class():
    """Tests that the wrapper class is renamed with the correct suffix."""

    class OriginalClass:
        pass

    class WrappedClass:
        pass

    _rename_observability_class(WrappedClass, OriginalClass)

    assert WrappedClass.__name__ == "OriginalClassObservability"
    assert (
        WrappedClass.__qualname__
        == f"{OriginalClass.__qualname__}Observability"
    )



@patch(MOCK_BASIC_STATUS)
@patch(MOCK_REPORT_ACTION)
def test_observability_context_reports_success(
    mock_report_action,
    mock_basic_status_class,
    operator_instance,
    context_dict,
    action_type,
    engine,
    get_pipeline_metadata,
):
    """Tests that a successful execution within the context manager reports SUCCESS."""  # noqa: E501
    mock_basic_status_class.SUCCESS = "MOCK_SUCCESS"

    with _action_observability_context(
        operator_instance,
        context_dict,
        action_type,
        engine,
        get_pipeline_metadata,
    ):
        pass

    get_pipeline_metadata.assert_called_once_with(operator_instance.dag)
    mock_report_action.assert_called_once_with(
        "test_bundle", "test_pipeline", action_type, engine, "MOCK_SUCCESS"
    )


@patch(MOCK_BASIC_STATUS)
@patch(MOCK_REPORT_ACTION)
def test_observability_context_reports_failure_and_raises(
    mock_report_action,
    mock_basic_status_class,
    operator_instance,
    context_dict,
    action_type,
    engine,
    get_pipeline_metadata,
):
    """Tests that an exception within the context manager reports FAILED and re-raises."""  # noqa: E501
    mock_basic_status_class.FAILED = "MOCK_FAILED"

    with pytest.raises(ValueError, match="Mocked execution error"):
        with _action_observability_context(
            operator_instance,
            context_dict,
            action_type,
            engine,
            get_pipeline_metadata,
        ):
            raise ValueError("Mocked execution error")

    mock_report_action.assert_called_once_with(
        "test_bundle", "test_pipeline", action_type, engine, "MOCK_FAILED"
    )


@patch(MOCK_BASE_OPERATOR, DummyBaseOperator)
def test_wrap_operator_returns_early_for_invalid_class(
    action_type, engine, get_pipeline_metadata
):
    """Tests that classes not inheriting from BaseOperator are returned untouched."""  # noqa: E501
    result = wrap_observability_operator(
        NotAnOperator,  # type: ignore
        action_type,
        engine,
        get_pipeline_metadata,
    )
    assert result is NotAnOperator


@patch(MOCK_OBSERVABILITY_CONTEXT)
@patch(MOCK_BASE_OPERATOR, DummyBaseOperator)
def test_wrap_operator_creates_wrapper_and_executes(
    mock_context_manager,
    action_type,
    engine,
    get_pipeline_metadata,
    context_dict,
):
    """Tests that a valid base class is wrapped and uses the context manager on execute."""  # noqa: E501
    mock_context_manager.return_value.__enter__.return_value = None
    WrappedClass = wrap_observability_operator(
        DummyOperator,  # type: ignore
        action_type,
        engine,
        get_pipeline_metadata,
    )

    instance = WrappedClass()  # type: ignore
    result = instance.execute(context_dict)

    assert result == "base_execute_result"
    assert WrappedClass.__name__ == "DummyOperatorObservability"
    assert issubclass(WrappedClass, DummyOperator)
    mock_context_manager.assert_called_once_with(
        instance, context_dict, action_type, engine, get_pipeline_metadata
    )
