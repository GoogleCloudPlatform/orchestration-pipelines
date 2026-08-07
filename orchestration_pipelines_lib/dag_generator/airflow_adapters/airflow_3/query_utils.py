# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""Query utilities for Airflow 3."""

import logging
from typing import TYPE_CHECKING

from orchestration_pipelines_lib.dag_generator.airflow_adapters.airflow_3 import (  # noqa: E501
    airflow_client_utils,
)

if TYPE_CHECKING:
    from airflow_client.client.models.dag_collection_response import (
        DAGCollectionResponse,
    )


def get_dag_tags_with_all_required_tags(
    pipeline_id: str, bundle_id: str
) -> "DAGCollectionResponse":
    """Returns DAGs that have all the required tags via Airflow API.

    Args:
        pipeline_id: The pipeline ID
        bundle_id: The bundle ID

    Returns:
        Response with DAGs matching all required tags
    """
    import airflow_client.client

    api_client = airflow_client_utils.get_airflow_api_client()
    dag_api = airflow_client.client.DAGApi(api_client)
    return dag_api.get_dags(
        tags=[
            "op:is_current",
            f"op:bundle:{bundle_id}",
            f"op:pipeline:{pipeline_id}",
        ],
        tags_match_mode="all",
    )


def resolve_latest_pipeline_dag_id(
    target_pipeline_id: str,
    bundle_id: str | None = None,
) -> str:
    """Returns the Airflow DAG ID for the latest version of a target pipeline.

    Queries the Airflow API for the DAG tagged as ``op:is_current`` that
    matches ``target_pipeline_id`` and ``bundle_id``. If ``bundle_id`` is not
    provided or no matching DAG is found, falls back to returning
    ``target_pipeline_id`` unchanged.

    Args:
        target_pipeline_id: The pipeline ID whose latest DAG ID should be
            resolved.
        bundle_id: Optional bundle ID scoping the target pipeline.

    Returns:
        The Airflow DAG ID for the latest version of ``target_pipeline_id``, or
        ``target_pipeline_id`` itself as a fallback.
    """
    if not bundle_id:
        return target_pipeline_id

    try:
        response = get_dag_tags_with_all_required_tags(
            target_pipeline_id, bundle_id
        )

        # Return the first matching DAG ID (only one marked as current)
        if response.dags:
            return response.dags[0].dag_id

        logging.warning(
            f"No DAG found with pipeline ID '{target_pipeline_id}'. "
            "Falling back to target pipeline ID."
        )
        return target_pipeline_id
    except Exception as e:
        logging.error(
            f"Error resolving latest bundle version for bundle '{bundle_id}' "
            f"and pipeline '{target_pipeline_id}': {e}. "
        )
        raise
