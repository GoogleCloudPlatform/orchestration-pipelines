"""Metrics utility functions."""

import logging
from collections.abc import Callable, Generator
from contextlib import contextmanager
from enum import Enum
from typing import TYPE_CHECKING, TypeVar, cast

from airflow.models import BaseOperator
from airflow.stats import Stats
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.types import DagRunType

from orchestration_pipelines_lib import __version__

if TYPE_CHECKING:
    try:
        from airflow.sdk import DAG, Context
    except ImportError:
        from airflow import DAG
        from airflow.utils.context import Context


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


class ActionExecutionType(str, Enum):
    """Action execution types."""

    PYTHON = "PYTHON"
    PYSPARK = "PYSPARK"
    NOTEBOOK = "NOTEBOOK"
    SQL = "SQL"
    DATA_INGESTION = "DATA_INGESTION"
    ORCHESTRATION_PIPELINE = "ORCHESTRATION_PIPELINE"
    DBT_PIPELINE = "DBT_PIPELINE"
    DATAFORM_PIPELINE = "DATAFORM_PIPELINE"
    AI = "AI"
    UNKNOWN = "UNKNOWN"

    def __str__(self) -> str:
        """Returns string representation of the enum value."""
        return self.value

    @staticmethod
    def from_action_type(action_type: str) -> "ActionExecutionType":
        """Converts action type to ActionExecutionType."""
        result = ACTION_TYPE_MAPPING.get(action_type)

        if not result:
            logger.warning(
                f"Unknown action type: {action_type} "
                "to map to ActionExecutionType"
            )
            return ActionExecutionType.UNKNOWN

        return result


ACTION_TYPE_MAPPING = {
    "python-virtual-env": ActionExecutionType.PYTHON,
    "script": ActionExecutionType.PYTHON,
    "operation": ActionExecutionType.SQL,
    "dbt_pipeline": ActionExecutionType.DBT_PIPELINE,
    "dataform_pipeline": ActionExecutionType.DATAFORM_PIPELINE,
    "data_ingestion": ActionExecutionType.DATA_INGESTION,
    "orchestration_pipeline": ActionExecutionType.ORCHESTRATION_PIPELINE,
    "notebook": ActionExecutionType.NOTEBOOK,
    "pyspark": ActionExecutionType.PYSPARK,
    "sql": ActionExecutionType.SQL,
    "ai": ActionExecutionType.AI,
}


class ActionExecutionEngine(str, Enum):
    """Action execution engines."""

    BIGQUERY = "BIGQUERY"
    DATAPROC = "DATAPROC"
    DATAFORM = "DATAFORM"
    LOCAL = "LOCAL"
    AGENT_PLATFORM = "AGENT_PLATFORM"

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

    @staticmethod
    def from_task_instance_state(ti_state: str | None) -> "BasicStatus":
        """Converts task instance state to BasicStatus."""
        if ti_state == TaskInstanceState.SUCCESS.value:
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


def report_action_execution(
    bundle_id: str | None,
    pipeline_id: str,
    action_type: ActionExecutionType,
    engine: ActionExecutionEngine,
    status: BasicStatus,
):
    """Emits action execution metrics."""
    _emit_metric(
        bundle_id=bundle_id,
        pipeline_id=pipeline_id,
        metric="action_execution",
        status=str(status),
        metric_callback=_incr_callback,
        additional_tags={
            "action_type": str(action_type),
            "engine": str(engine),
        },
    )


def report_init_context(
    bundle_id: str | None,
    pipeline_id: str,
    status: BasicStatus,
):
    """Emits init context metrics."""
    _emit_metric(
        bundle_id=bundle_id,
        pipeline_id=pipeline_id,
        metric="init_context",
        status=str(status),
        metric_callback=_incr_callback,
    )


T = TypeVar("T", bound=BaseOperator)


def wrap_observability_operator(
    base_operator_class: type[T],
    action_type: ActionExecutionType,
    engine: ActionExecutionEngine,
    get_pipeline_metadata: Callable[["DAG"], tuple[str, str, str]],
) -> type[T]:
    """Factory function to create a custom observability operator that inherits
    from the base Airflow operator and injects metric-emitting logic.
    """
    if not issubclass(base_operator_class, BaseOperator):
        return base_operator_class

    class ActionObservabilityOperator(base_operator_class):
        """Wrapper operator for pipeline actions
        that emits OP execution metrics.
        """

        def execute(self, context):
            with _action_observability_context(
                self, context, action_type, engine, get_pipeline_metadata
            ):
                return super().execute(context)

    _rename_observability_class(
        ActionObservabilityOperator, base_operator_class
    )

    return cast(type[T], ActionObservabilityOperator)


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


@contextmanager
def _action_observability_context(
    operator_instance: BaseOperator,
    context: "Context",
    action_type: ActionExecutionType,
    engine: ActionExecutionEngine,
    get_pipeline_metadata: Callable[["DAG"], tuple[str, str, str]],
) -> Generator[None, None, None]:
    dag_obj = operator_instance.dag or context.get("dag")
    bundle_id, _, pipeline_id = get_pipeline_metadata(dag_obj)

    def report(status: BasicStatus):
        report_action_execution(
            bundle_id,
            pipeline_id,
            action_type,
            engine,
            status,
        )

    try:
        yield
        report(BasicStatus.SUCCESS)
    except Exception:
        report(BasicStatus.FAILED)
        raise


def _rename_observability_class(wrapped_class: type, base_class: type) -> None:
    suffix = "Observability"
    wrapped_class.__name__ = f"{base_class.__name__}{suffix}"
    wrapped_class.__qualname__ = f"{base_class.__qualname__}{suffix}"
