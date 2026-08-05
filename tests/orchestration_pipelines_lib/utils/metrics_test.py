"""Tests for the metrics module."""

import logging
from unittest.mock import MagicMock, call, patch

from orchestration_pipelines_lib.utils.metrics import (
    MODULE_NAME,
    VERSION_LABEL,
    ParsingStatus,
    _emit_metric,
    _incr_callback,
    _timing_callback,
    report_parsing,
)

TARGET_MODULE = "orchestration_pipelines_lib.utils.metrics"


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

    args1, _ = mock_emit_metric.call_args_list[0]
    assert args1[0] == bundle_id
    assert args1[1] == pipeline_id
    assert args1[2] == "parse"
    assert args1[3] == status
    assert args1[4] == _incr_callback

    args2, _ = mock_emit_metric.call_args_list[1]
    assert args2[0] == bundle_id
    assert args2[1] == pipeline_id
    assert args2[2] == "parse.duration"
    assert args2[3] == status
    assert args2[4] == mocked_callback_result
    mock_timing_callback.assert_called_once_with(duration)


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

    expected_statsd_name = f"{MODULE_NAME}.{bundle_id}.{pipeline_id}.{metric}.{status}.{VERSION_LABEL}"  # noqa: E501
    expected_otel_name = f"{MODULE_NAME}.{metric}"
    expected_tags = {
        "status": "SUCCESS",
        "library_version": VERSION_LABEL,
        "pipeline_name": pipeline_id,
        "bundle_name": bundle_id,
    }

    _emit_metric(bundle_id, pipeline_id, metric, str(status), mock_callback)

    assert mock_callback.call_count == 2
    mock_callback.assert_has_calls(
        [
            call(expected_statsd_name, None),
            call(expected_otel_name, expected_tags),
        ]
    )


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

    _emit_metric(bundle_id, pipeline_id, metric, status, metric_callback)

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
        _emit_metric(bundle_id, pipeline_id, metric, status, mock_callback)

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
        _emit_metric(bundle_id, pipeline_id, metric, status, mock_callback)

    assert mock_callback.call_count == 2
    assert "Could not emit OTel metric 'my_metric'" in caplog.text
    assert "OTel is down" in caplog.text
