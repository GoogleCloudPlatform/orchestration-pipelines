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
import unittest

from airflow.models import DAG
from airflow.operators.empty import EmptyOperator

from orchestration_pipelines_lib.utils.dummy_dag import create as create_dummy_dag


class TestDummyDag(unittest.TestCase):
    """Test suite for dummy DAG creation utility."""

    def setUp(self):
        """Set up common variables and mocks for tests."""
        self.dag_base_id = "test_bundle__v__v1.2.3__test-pipeline"
        self.error_message = "A critical error occurred during parsing."
        self.tags = [
            "op:orchestration_pipeline",
            "op:bundle:test-bundle",
            "op:version:v1.2.3",
            "op:pipeline:test-pipeline",
            "op:owner:test-owner",
            "customer-tag",
        ]
        self.doc_md_dict = {
            "op_bundle": "test-bundle",
            "op_version": "v1.2.3",
            "op_pipeline": "test-pipeline",
            "op_owner": "test-owner",
            "op_is_current": True,
            "op_is_paused": False,
        }
        self.doc_md_str = json.dumps(self.doc_md_dict)

    def test_create_dummy_dag_with_doc_md(self):
        """Tests dummy DAG creation when a valid doc_md is provided."""
        dag = create_dummy_dag(dag_base_id=self.dag_base_id,
                               error_message=self.error_message,
                               tags=self.tags,
                               doc_md=self.doc_md_str)

        # Assert basic DAG properties
        self.assertIsInstance(dag, DAG)
        expected_dag_id = f"ERROR__{self.dag_base_id}"
        self.assertEqual(dag.dag_id, expected_dag_id)
        self.assertIsNone(dag.schedule_interval)
        self.assertFalse(dag.catchup)
        self.assertEqual(dag.tags, self.tags)

        # Assert doc_md content
        final_doc_md = json.loads(dag.doc_md)
        self.assertIn("op_error", final_doc_md)
        self.assertIn(self.error_message, final_doc_md["op_error"])
        # Check that original metadata is preserved
        self.assertEqual(final_doc_md["op_bundle"],
                         self.doc_md_dict["op_bundle"])
        self.assertEqual(final_doc_md["op_owner"],
                         self.doc_md_dict["op_owner"])

        # Assert task structure
        self.assertEqual(len(dag.tasks), 1)
        task = dag.get_task("parsing_failed")
        self.assertIsInstance(task, EmptyOperator)

    def test_create_dummy_dag_without_doc_md(self):
        """
        Tests that a dummy DAG is successfully created when doc_md is None.
        """
        dag = create_dummy_dag(dag_base_id=self.dag_base_id,
                               error_message=self.error_message,
                               tags=self.tags,
                               doc_md=None)

        # Assert basic DAG properties
        self.assertIsInstance(dag, DAG)
        expected_dag_id = f"ERROR__{self.dag_base_id}"
        self.assertEqual(dag.dag_id, expected_dag_id)
        self.assertEqual(dag.tags, self.tags)

        # Assert doc_md content
        doc_md = json.loads(dag.doc_md)
        self.assertIn("op_error", doc_md)
        self.assertIn(self.error_message, doc_md["op_error"])
        self.assertEqual(len(doc_md), 1)  # Only op_error should be present

        # Assert task structure
        self.assertEqual(len(dag.tasks), 1)
        self.assertIsInstance(dag.get_task("parsing_failed"), EmptyOperator)

    def test_create_dummy_dag_with_empty_doc_md(self):
        """Tests dummy DAG creation for a paused and non-current pipeline."""
        dag = create_dummy_dag(dag_base_id=self.dag_base_id,
                               error_message=self.error_message,
                               tags=self.tags,
                               doc_md="")

        # Assert basic DAG properties
        self.assertIsInstance(dag, DAG)
        expected_dag_id = f"ERROR__{self.dag_base_id}"
        self.assertEqual(dag.dag_id, expected_dag_id)
        self.assertEqual(dag.tags, self.tags)

        # Assert doc_md
        doc_md = json.loads(dag.doc_md)
        self.assertIn("op_error", doc_md)
        self.assertIn(self.error_message, doc_md["op_error"])
        self.assertEqual(len(doc_md), 1)  # Only op_error should be present

        # Assert task structure
        self.assertEqual(len(dag.tasks), 1)
        self.assertIsInstance(dag.get_task("parsing_failed"), EmptyOperator)


if __name__ == '__main__':
    unittest.main()
