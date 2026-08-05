"""Metrics utilities for tracking DAG parsing operations."""

import logging
from collections.abc import Callable
from enum import Enum

from airflow.stats import Stats

from orchestration_pipelines_lib import __version__

MODULE_NAME = "orchestration_pipeline"
VERSION_LABEL = str(__version__).replace(".", "-")

logger = logging.getLogger(__name__)

MetricCallback = Callable[[str, dict[str, str] | None], None]


class ParsingStatus(str, Enum):
    """DAG parsing statuses."""

    SUCCESS = "SUCCESS"
    PARSING_ERROR = "PARSING_ERROR"
    MISSING_FILE = "MISSING_FILE"
    AIRFLOW_ERROR = "AIRFLOW_ERROR"
    INTERNAL = "INTERNAL"

    def __str__(self) -> str:
        """Returns string representation of the enum value."""
        return self.value


def report_parsing(
    bundle_id: str | None,
    pipeline_id: str,
    status: ParsingStatus,
    duration: float,
) -> None:
    """Emits parsing metrics."""
    _emit_metric(bundle_id, pipeline_id, "parse", str(status), _incr_callback)
    _emit_metric(
        bundle_id,
        pipeline_id,
        "parse.duration",
        str(status),
        _timing_callback(duration),
    )


def _incr_callback(topic: str, tags: dict[str, str] | None = None):
    Stats.incr(topic, tags=tags)


def _timing_callback(duration: float) -> MetricCallback:
    return lambda topic, tags: Stats.timing(topic, duration, tags=tags)


def _emit_metric(
    bundle_id: str | None,
    pipeline_id: str,
    metric: str,
    status: str,
    metric_callback: MetricCallback,
) -> None:
    """Emits a metric to both StatsD and OpenTelemetry."""
    bundle_id = bundle_id or "default"

    try:
        metric_callback(
            f"{MODULE_NAME}.{bundle_id}.{pipeline_id}.{metric}.{status}.{VERSION_LABEL}",
            None,
        )
    except Exception as err:
        logger.warning(f"Could not emit StatsD metric '{metric}'. Error: {err}")

    tags = {
        "status": status,
        "library_version": VERSION_LABEL,
        "pipeline_name": pipeline_id,
        "bundle_name": bundle_id,
    }

    try:
        metric_callback(f"{MODULE_NAME}.{metric}", tags)
    except Exception as err:
        logger.warning(f"Could not emit OTel metric '{metric}'. Error: {err}")
