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
"""Unit tests for the dummy_dag utility."""

import json
from typing import NamedTuple, TypedDict

import pytest

from orchestration_pipelines_lib.utils.dummy_dag import (
    create as create_dummy_dag,
)

try:
    from airflow.providers.standard.operators.python import (
        PythonOperator,
    )
    from airflow.sdk import DAG
    from airflow.sdk.exceptions import AirflowFailException
except ImportError:
    from airflow.exceptions import AirflowFailException
    from airflow.models import DAG
    from airflow.operators.python import PythonOperator


class DocMdDict(TypedDict):
    """Typed structure for the doc_md metadata dictionary."""

    op_bundle: str
    op_version: str
    op_pipeline: str
    op_owner: str
    op_is_current: bool
    op_is_paused: bool


class DummyDagSetup(NamedTuple):
    """Common test data for dummy DAG tests."""

    dag_base_id: str
    error_message: str
    tags: list[str]
    doc_md_dict: DocMdDict
    doc_md_str: str


@pytest.fixture
def dummy_dag_setup() -> DummyDagSetup:
    """Sets up common variables for dummy DAG tests."""
    doc_md_dict: DocMdDict = {
        "op_bundle": "test-bundle",
        "op_version": "v1.2.3",
        "op_pipeline": "test-pipeline",
        "op_owner": "test-owner",
        "op_is_current": True,
        "op_is_paused": False,
    }
    return DummyDagSetup(
        dag_base_id="test_bundle__v__v1.2.3__test-pipeline",
        error_message="A critical error occurred during parsing.",
        tags=[
            "op:orchestration_pipeline",
            "op:bundle:test-bundle",
            "op:version:v1.2.3",
            "op:pipeline:test-pipeline",
            "op:owner:test-owner",
            "customer-tag",
        ],
        doc_md_dict=doc_md_dict,
        doc_md_str=json.dumps(doc_md_dict),
    )


def test_create_dummy_dag_with_doc_md(dummy_dag_setup: DummyDagSetup):
    """Tests dummy DAG creation when a valid doc_md is provided."""
    expected_dag_id = f"ERROR__{dummy_dag_setup.dag_base_id}"

    dag = create_dummy_dag(
        dag_base_id=dummy_dag_setup.dag_base_id,
        error_message=dummy_dag_setup.error_message,
        tags=dummy_dag_setup.tags,
        doc_md=dummy_dag_setup.doc_md_str,
    )

    assert isinstance(dag, DAG)
    assert dag.dag_id == expected_dag_id
    schedule = (
        dag.schedule_interval
        if hasattr(dag, "schedule_interval")
        else dag.schedule
    )
    assert schedule is None
    assert not dag.catchup
    assert set(dag.tags) == set(dummy_dag_setup.tags)
    assert dag.doc_md is not None
    final_doc_md = json.loads(dag.doc_md)
    assert "op_error" in final_doc_md
    assert dummy_dag_setup.error_message in final_doc_md["op_error"]
    assert final_doc_md["op_bundle"] == dummy_dag_setup.doc_md_dict["op_bundle"]
    assert final_doc_md["op_owner"] == dummy_dag_setup.doc_md_dict["op_owner"]
    assert len(dag.tasks) == 1
    task = dag.get_task("parsing_failed")
    assert isinstance(task, PythonOperator)
    with pytest.raises(
        AirflowFailException, match=dummy_dag_setup.error_message
    ):
        task.python_callable(**task.op_kwargs)


@pytest.mark.parametrize("doc_md", [None, ""])
def test_create_dummy_dag_without_doc_md(
    dummy_dag_setup: DummyDagSetup, doc_md: str | None
):
    """Tests dummy DAG creation when doc_md is None or an empty string."""
    expected_dag_id = f"ERROR__{dummy_dag_setup.dag_base_id}"

    dag = create_dummy_dag(
        dag_base_id=dummy_dag_setup.dag_base_id,
        error_message=dummy_dag_setup.error_message,
        tags=dummy_dag_setup.tags,
        doc_md=doc_md,
    )

    assert isinstance(dag, DAG)
    assert dag.dag_id == expected_dag_id
    assert set(dag.tags) == set(dummy_dag_setup.tags)
    assert dag.doc_md is not None
    parsed_doc_md = json.loads(dag.doc_md)
    assert "op_error" in parsed_doc_md
    assert dummy_dag_setup.error_message in parsed_doc_md["op_error"]
    assert len(parsed_doc_md) == 1
    assert len(dag.tasks) == 1
    task = dag.get_task("parsing_failed")
    assert isinstance(task, PythonOperator)
    with pytest.raises(
        AirflowFailException, match=dummy_dag_setup.error_message
    ):
        task.python_callable(**task.op_kwargs)
