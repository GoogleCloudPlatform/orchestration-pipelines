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
"""Query utilities for Airflow 2."""

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sqlalchemy.orm import Query, Session


def get_dag_tags_with_all_required_tags(
    session: "Session", pipeline_id: str, bundle_id: str
) -> "Query":
    """Returns a query for dag_ids that have all the required tags.

    This uses a "Tag Intersection" pattern (GROUP BY + HAVING COUNT)
    which avoids multiple joins and table scans.

    Args:
        session: SQLAlchemy session
        pipeline_id: The pipeline ID
        bundle_id: The bundle ID

    Returns:
        Query for dag_ids that have all three required tags
    """
    from airflow.models import DagTag
    from sqlalchemy import func

    return (
        session.query(DagTag.dag_id)
        .filter(
            DagTag.name.in_(
                [
                    "op:is_current",
                    f"op:bundle:{bundle_id}",
                    f"op:pipeline:{pipeline_id}",
                ]
            )
        )
        .group_by(DagTag.dag_id)
        .having(func.count(DagTag.name) == 3)
    )


def resolve_latest_pipeline_dag_id(
    target_pipeline_id: str,
    bundle_id: str | None = None,
) -> str:
    """Returns the Airflow DAG ID for the latest version of a target pipeline.

    Queries the Airflow metadata database for the DAG tagged as
    ``op:is_current`` that matches ``target_pipeline_id`` and ``bundle_id``.
    If ``bundle_id`` is not provided or no matching DAG is found, falls back
    to returning ``target_pipeline_id`` unchanged.

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

    from airflow.utils.session import create_session

    try:
        with create_session() as session:
            matching_dags = get_dag_tags_with_all_required_tags(
                session, target_pipeline_id, bundle_id
            ).first()

            if matching_dags:
                return matching_dags[0]
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
