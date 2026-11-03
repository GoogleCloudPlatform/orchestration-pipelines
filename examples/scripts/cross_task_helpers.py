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
"""Helper callables for the cross-task data sharing example pipeline."""

import logging
from collections.abc import Sequence
from typing import Any

logger = logging.getLogger(__name__)


def prepare_execution_context(
    table_name: str,
    run_id: str,
    trigger_date: str,
    regions: Sequence[str],
) -> dict[str, Any]:
    """Builds runtime batch metadata for downstream SQL and Python actions."""
    result = {
        "batch_tag": f"{table_name}_{trigger_date}",
        "run_id": run_id,
        "primary_region": regions[0] if regions else "",
    }
    logger.info("Prepared execution context: %s", result)
    return result


def optional_enrichment(batch_tag: str) -> dict[str, str]:
    """Optional enrichment step gated by a `runIf` condition."""
    return {"message": f"enriched_{batch_tag}", "status": "ENRICHED"}


def summarize_pipeline_run(
    run_id: str,
    trigger_time: str,
    bq_job_id: str | None,
    row_count: int | str | None,
    enrichment_msg: str,
) -> dict[str, Any]:
    """Summarizes parameters, run metadata, and upstream task outputs."""
    summary = {
        "run_id": run_id,
        "trigger_time": trigger_time,
        "bq_job_id": bq_job_id,
        "row_count": int(row_count) if row_count is not None else None,
        "enrichment_msg": enrichment_msg,
    }
    logger.info("Pipeline execution summary: %s", summary)
    return summary
