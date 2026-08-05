"""Metrics utility functions."""

import logging
from collections.abc import Callable
from enum import Enum

from airflow.stats import Stats
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunType

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


class BasicStatus(str, Enum):
    """Basic status enum for the cases, where Success and Failed is enough."""

    SUCCESS = "SUCCESS"
    FAILED = "FAILED"

    def __str__(self) -> str:
        """Returns string representation of the enum value."""
        return self.value

    @staticmethod
    def from_dag_run_state(dag_run_state: str) -> "BasicStatus":
        """Converts DAG run state to BasicStatus."""
        if dag_run_state == DagRunState.SUCCESS.value:
            return BasicStatus.SUCCESS

        return BasicStatus.FAILED


class PipelineRunTriggerType(str, Enum):
    """Available trigger types."""

    MANUAL = "MANUAL"
    SCHEDULED = "SCHEDULED"
    UNKNOWN = "UNKNOWN"

    def __str__(self) -> str:
        """Returns string representation of the enum value."""
        return self.value

    @staticmethod
    def from_dag_run_type(
        dag_run_type: str | None,
    ) -> "PipelineRunTriggerType":
        """Converts DAG run type to PipelineRunTriggerType."""
        return (
            dag_run_type
            and TRIGGER_TYPE_MAPPING.get(dag_run_type)
            or PipelineRunTriggerType.UNKNOWN
        )


TRIGGER_TYPE_MAPPING = {
    DagRunType.MANUAL.value: PipelineRunTriggerType.MANUAL,
    DagRunType.SCHEDULED.value: PipelineRunTriggerType.SCHEDULED,
}


def report_parsing(
    bundle_id: str | None,
    pipeline_id: str,
    status: ParsingStatus,
    duration: float,
) -> None:
    """Emits parsing metrics."""
    _emit_metric(
        bundle_id=bundle_id,
        pipeline_id=pipeline_id,
        metric="parse",
        status=str(status),
        metric_callback=_incr_callback,
    )
    _emit_metric(
        bundle_id=bundle_id,
        pipeline_id=pipeline_id,
        metric="parse.duration",
        status=str(status),
        metric_callback=_timing_callback(duration),
    )


def report_pipeline_run(
    bundle_id: str | None,
    pipeline_id: str,
    trigger_type: PipelineRunTriggerType,
    status: BasicStatus,
) -> None:
    """Emits pipeline run metrics."""
    _emit_metric(
        bundle_id=bundle_id,
        pipeline_id=pipeline_id,
        metric="pipeline_run",
        status=str(status),
        metric_callback=_incr_callback,
        additional_tags={"trigger_type": str(trigger_type)},
    )


def _incr_callback(topic: str, tags: dict[str, str] | None = None):
    Stats.incr(topic, tags=tags)


def _timing_callback(duration: float) -> MetricCallback:
    return lambda topic, tags: Stats.timing(topic, duration, tags=tags)


def _emit_metric(
    *,
    bundle_id: str | None,
    pipeline_id: str,
    metric: str,
    status: str,
    metric_callback: MetricCallback,
    additional_tags: dict[str, str] | None = None,
) -> None:
    """Emits a metric to both StatsD and OpenTelemetry."""
    bundle_id = bundle_id or "default"

    statsd_tags_suffix = ""
    additional_tags = additional_tags or {}

    for tag in additional_tags.values():
        statsd_tags_suffix += f".{tag}"

    try:
        metric_callback(
            f"{MODULE_NAME}.{bundle_id}.{pipeline_id}.{metric}{statsd_tags_suffix}.{status}.{VERSION_LABEL}",
            None,
        )
    except Exception as err:
        logger.warning(f"Could not emit StatsD metric '{metric}'. Error: {err}")

    tags = {
        "status": status,
        "library_version": VERSION_LABEL,
        "pipeline_name": pipeline_id,
        "bundle_name": bundle_id,
        **additional_tags,
    }

    try:
        metric_callback(f"{MODULE_NAME}.{metric}", tags)
    except Exception as err:
        logger.warning(f"Could not emit OTel metric '{metric}'. Error: {err}")
