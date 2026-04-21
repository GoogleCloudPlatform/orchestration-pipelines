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
"""Utilities for generating dummy DAGs for parsing errors."""

import json
from datetime import datetime
from typing import Optional, List

from airflow.models import DAG
from airflow.operators.empty import EmptyOperator

_ERROR_DAG_PREFIX = "ERROR__"


def create(dag_base_id: str, error_message: str, tags: List[str],
           doc_md: Optional[str]):
    """Creates a dummy Airflow DAG to surface parsing errors in the UI.

    Args:
        dag_base_id: The base ID of the DAG that failed to parse.
        error_message: The error message detailing why the parsing failed.
        tags: A list of tags to associate with the dummy DAG.
        doc_md: An optional JSON-formatted string containing documentation metadata.

    Returns:
        An instantiated Airflow DAG containing a single EmptyOperator indicating failure.
    """
    dag_id = _ERROR_DAG_PREFIX + dag_base_id
    error_doc_md = {"op_error": f"\n\n````\n{error_message}\n````"}
    if doc_md:
        error_doc_md.update(json.loads(doc_md))

    dag = DAG(dag_id=dag_id,
              start_date=datetime(2023, 1, 1),
              schedule=None,
              catchup=False,
              tags=tags,
              doc_md=json.dumps(error_doc_md))

    EmptyOperator(task_id="parsing_failed", dag=dag)

    return dag
