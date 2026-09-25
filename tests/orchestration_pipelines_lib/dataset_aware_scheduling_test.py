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
"""End-to-end integration tests for dataset-aware scheduling."""

import json
import os
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from airflow.models import DAG

from orchestration_pipelines_lib import api
from orchestration_pipelines_lib.utils.file_manager import FileManager
from orchestration_pipelines_lib.utils.pipeline_metadata import PipelineMetadata
from orchestration_pipelines_lib.utils.pipeline_repository import PipelineRepository
from orchestration_pipelines_models.manifest.manifest import Manifest

_PROJECT_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..")
)


class DatasetAwareSchedulingIntegrationTest(unittest.TestCase):
    """Integration test suite for dataset triggers and action outlets."""

    def setUp(self):
        self.blob_ref_patcher = patch(
            "orchestration_pipelines_lib.utils.file_manager.FileManager.get_blob_reference"
        )
        self.mock_get_blob_ref = self.blob_ref_patcher.start()
        self.mock_get_blob_ref.side_effect = lambda path: (
            f"gs://example-bucket/{os.path.basename(path)}" if path else None
        )

    def tearDown(self):
        self.blob_ref_patcher.stop()

    def _get_uris_from_outlets(self, outlets):
        """Extracts URI strings from a collection of Dataset/Asset objects."""
        if not outlets:
            return []
        return [
            getattr(item, "uri", getattr(item, "name", str(item))).rstrip("/")
            for item in outlets
        ]

    def test_example_dataset_producer_pipeline(self):
        """Tests that examples/pipeline-dataset-producer.yml validates and generates DAG with outlets."""
        producer_path = os.path.join(
            _PROJECT_ROOT, "examples/pipeline-dataset-producer.yml"
        )
        api.validate(producer_path)

        examples_dir = os.path.join(_PROJECT_ROOT, "examples")
        with patch.dict(os.environ, {"DAGS_FOLDER": examples_dir, "GCS_BUCKET": "example-bucket"}):
            globals_dict = {}
            api.generate(producer_path, globals_dict)

            self.assertIn("pipeline-dataset-producer", globals_dict)
            dag = globals_dict["pipeline-dataset-producer"]
            self.assertIsInstance(dag, DAG)

            tasks_map = {t.task_id: t for t in dag.tasks}

            # Check SQL task outlets
            self.assertIn("transform_daily_sales", tasks_map)
            sql_task = tasks_map["transform_daily_sales"]
            sql_outlets = self._get_uris_from_outlets(sql_task.outlets)
            self.assertEqual(sql_outlets, ["bq://my-project.my_dataset.daily_sales"])

            # Check Python task outlets
            self.assertIn("export_sales_to_gcs", tasks_map)
            python_task = tasks_map["export_sales_to_gcs"]
            python_outlets = self._get_uris_from_outlets(python_task.outlets)
            self.assertEqual(python_outlets, ["gs://my-bucket/sales/daily.csv"])

    def test_example_dataset_consumer_pipeline(self):
        """Tests that examples/pipeline-dataset-consumer.yml validates and generates DAG scheduled on datasets."""
        consumer_path = os.path.join(
            _PROJECT_ROOT, "examples/pipeline-dataset-consumer.yml"
        )
        api.validate(consumer_path)

        with patch.dict(os.environ, {"GCS_BUCKET": "example-bucket"}):
            globals_dict = {}
            api.generate(consumer_path, globals_dict)

            self.assertIn("pipeline-dataset-consumer", globals_dict)
            dag = globals_dict["pipeline-dataset-consumer"]
            self.assertIsInstance(dag, DAG)

            # Verify dataset schedule
            self.assertIsNotNone(dag.schedule)
            self.assertIsInstance(dag.schedule, list)
            schedule_uris = self._get_uris_from_outlets(dag.schedule)
            self.assertEqual(
                schedule_uris,
                [
                    "bq://my-project.my_dataset.daily_sales",
                    "gs://my-bucket/sales/daily.csv",
                ],
            )
            self.assertFalse(dag.catchup)

            # Verify doc_md captures dataset triggers
            doc_data = json.loads(dag.doc_md)
            self.assertIn("op_datasets", doc_data)
            self.assertEqual(
                doc_data["op_datasets"]["uris"],
                [
                    "bq://my-project.my_dataset.daily_sales",
                    "gs://my-bucket/sales/daily.csv",
                ],
            )
            self.assertEqual(doc_data["op_datasets"]["condition"], "all")

    def test_producer_consumer_lineage_matching(self):
        """Tests end-to-end lineage: producer outlets match consumer triggers."""
        with tempfile.TemporaryDirectory() as temp_dir:
            producer_file = os.path.join(temp_dir, "upstream_producer.yml")
            consumer_file = os.path.join(temp_dir, "downstream_consumer.yml")

            target_uri = "gs://my-bucket/processed_data.parquet"

            producer_yaml = f"""
modelVersion: "1.0"
pipelineId: "upstream_producer"
description: "Upstream producer pipeline"
runner: "airflow"
owner: "data_team"
defaults:
  projectId: "test-project"
  location: "us-central1"
  executionConfig:
    retries: 0
triggers:
  - schedule:
      interval: "0 0 * * *"
      startTime: "2026-01-01T00:00:00"
      catchup: false
      timezone: "UTC"
actions:
  - sql:
      name: "produce_dataset"
      query:
        inline: "SELECT 1"
      engine:
        bigquery:
          location: "US"
      outlets:
        - "{target_uri}"
"""
            consumer_yaml = f"""
modelVersion: "1.0"
pipelineId: "downstream_consumer"
description: "Downstream consumer pipeline"
runner: "airflow"
owner: "analytics_team"
defaults:
  projectId: "test-project"
  location: "us-central1"
  executionConfig:
    retries: 0
triggers:
  - datasets:
      uris:
        - "{target_uri}"
      condition: "all"
actions:
  - sql:
      name: "consume_dataset"
      query:
        inline: "SELECT 2"
      engine:
        bigquery:
          location: "US"
"""
            with open(producer_file, "w", encoding="utf-8") as f:
                f.write(producer_yaml)
            with open(consumer_file, "w", encoding="utf-8") as f:
                f.write(consumer_yaml)

            api.validate(producer_file)
            api.validate(consumer_file)

            globals_dict = {}
            api.generate(producer_file, globals_dict)
            api.generate(consumer_file, globals_dict)

            producer_dag = globals_dict["upstream_producer"]
            consumer_dag = globals_dict["downstream_consumer"]

            producer_task = producer_dag.get_task("produce_dataset")
            produced_uris = self._get_uris_from_outlets(producer_task.outlets)
            self.assertIn(target_uri, produced_uris)

            consumer_schedule_uris = self._get_uris_from_outlets(consumer_dag.schedule)
            self.assertIn(target_uri, consumer_schedule_uris)
            self.assertEqual(produced_uris, consumer_schedule_uris)

    def test_consumer_with_any_condition(self):
        """Tests consumer DAG schedule generation with condition: any."""
        with tempfile.TemporaryDirectory() as temp_dir:
            consumer_file = os.path.join(temp_dir, "consumer_any.yml")
            consumer_yaml = """
modelVersion: "1.0"
pipelineId: "consumer_any"
description: "Downstream consumer with condition: any"
runner: "airflow"
owner: "analytics_team"
defaults:
  projectId: "test-project"
  location: "us-central1"
  executionConfig:
    retries: 0
triggers:
  - datasets:
      uris:
        - "gs://bucket/dataset_a.parquet"
        - "gs://bucket/dataset_b.parquet"
      condition: "any"
actions:
  - sql:
      name: "process_either_dataset"
      query:
        inline: "SELECT 1"
      engine:
        bigquery:
          location: "US"
"""
            with open(consumer_file, "w", encoding="utf-8") as f:
                f.write(consumer_yaml)

            api.validate(consumer_file)
            globals_dict = {}
            api.generate(consumer_file, globals_dict)

            dag = globals_dict["consumer_any"]
            self.assertIsNotNone(dag.schedule)
            self.assertFalse(dag.catchup)

            doc_data = json.loads(dag.doc_md)
            self.assertIn("op_datasets", doc_data)
            self.assertEqual(doc_data["op_datasets"]["condition"], "any")
            self.assertEqual(
                doc_data["op_datasets"]["uris"],
                ["gs://bucket/dataset_a.parquet", "gs://bucket/dataset_b.parquet"],
            )

    def test_paused_pipeline_clears_dataset_triggers_but_preserves_outlets(self):
        """Tests that a paused versioned pipeline clears schedule while keeping task outlets."""
        with tempfile.TemporaryDirectory() as temp_dir:
            pipeline_file = os.path.join(temp_dir, "versioned_paused.yml")
            pipeline_yaml = """
modelVersion: "1.0"
pipelineId: "versioned_paused"
description: "Versioned pipeline that is paused"
runner: "airflow"
owner: "data_team"
defaults:
  projectId: "test-project"
  location: "us-central1"
  executionConfig:
    retries: 0
triggers:
  - datasets:
      uris:
        - "gs://bucket/incoming.parquet"
      condition: "all"
actions:
  - sql:
      name: "paused_task"
      query:
        inline: "SELECT 1"
      engine:
        bigquery:
          location: "US"
      outlets:
        - "gs://bucket/outgoing.parquet"
"""
            with open(pipeline_file, "w", encoding="utf-8") as f:
                f.write(pipeline_yaml)

            mock_manifest = MagicMock(spec=Manifest)
            mock_manifest.get_bundle_id.return_value = "my-bundle"
            mock_manifest.is_paused.return_value = True
            mock_manifest.is_current.return_value = True
            mock_manifest.get_deployment_details.return_value = None

            metadata = PipelineMetadata(
                pipeline_id="versioned_paused",
                source_filepath=pipeline_file,
                manifest=mock_manifest,
                version_id="v1.0.0",
            )

            file_manager = FileManager()
            repository = PipelineRepository(data_root="")
            globals_dict = {}

            api._generate_dag(
                file_manager=file_manager,
                pipeline_definition_path=pipeline_file,
                repository=repository,
                dag_id="versioned_paused_dag",
                metadata=metadata,
                data_root=None,
                globals_dict=globals_dict,
                bundle_id="my-bundle",
                pipeline_id="versioned_paused",
                version_id=None,
            )

            dag = globals_dict["versioned_paused_dag"]
            self.assertIsNone(dag.schedule)

            # Task outlets must still be preserved
            task = dag.get_task("paused_task")
            outlets = self._get_uris_from_outlets(task.outlets)
            self.assertEqual(outlets, ["gs://bucket/outgoing.parquet"])

    def test_non_current_pipeline_clears_dataset_triggers_but_preserves_outlets(self):
        """Tests that a non-current versioned pipeline clears schedule while keeping task outlets."""
        with tempfile.TemporaryDirectory() as temp_dir:
            pipeline_file = os.path.join(temp_dir, "versioned_old.yml")
            pipeline_yaml = """
modelVersion: "1.0"
pipelineId: "versioned_old"
description: "Older versioned pipeline that is not current"
runner: "airflow"
owner: "data_team"
defaults:
  projectId: "test-project"
  location: "us-central1"
  executionConfig:
    retries: 0
triggers:
  - datasets:
      uris:
        - "gs://bucket/incoming.parquet"
      condition: "all"
actions:
  - sql:
      name: "old_task"
      query:
        inline: "SELECT 1"
      engine:
        bigquery:
          location: "US"
      outlets:
        - "gs://bucket/outgoing.parquet"
"""
            with open(pipeline_file, "w", encoding="utf-8") as f:
                f.write(pipeline_yaml)

            mock_manifest = MagicMock(spec=Manifest)
            mock_manifest.get_bundle_id.return_value = "my-bundle"
            mock_manifest.is_paused.return_value = False
            mock_manifest.is_current.return_value = False
            mock_manifest.get_deployment_details.return_value = None

            metadata = PipelineMetadata(
                pipeline_id="versioned_old",
                source_filepath=pipeline_file,
                manifest=mock_manifest,
                version_id="v0.9.0",
            )

            file_manager = FileManager()
            repository = PipelineRepository(data_root="")
            globals_dict = {}

            api._generate_dag(
                file_manager=file_manager,
                pipeline_definition_path=pipeline_file,
                repository=repository,
                dag_id="versioned_old_dag",
                metadata=metadata,
                data_root=None,
                globals_dict=globals_dict,
                bundle_id="my-bundle",
                pipeline_id="versioned_old",
                version_id=None,
            )

            dag = globals_dict["versioned_old_dag"]
            self.assertIsNone(dag.schedule)

            # Task outlets must still be preserved
            task = dag.get_task("old_task")
            outlets = self._get_uris_from_outlets(task.outlets)
            self.assertEqual(outlets, ["gs://bucket/outgoing.parquet"])

    def test_validation_rejects_conflicting_triggers(self):
        """Tests that api.validate rejects pipelines with both schedule and datasets triggers."""
        with tempfile.TemporaryDirectory() as temp_dir:
            conflict_file = os.path.join(temp_dir, "conflict_triggers.yml")
            conflict_yaml = """
modelVersion: "1.0"
pipelineId: "conflict_triggers"
description: "Pipeline with conflicting triggers"
runner: "airflow"
owner: "data_team"
defaults:
  projectId: "test-project"
  location: "us-central1"
  executionConfig:
    retries: 0
triggers:
  - schedule:
      interval: "0 0 * * *"
      startTime: "2026-01-01T00:00:00"
      catchup: false
      timezone: "UTC"
  - datasets:
      uris:
        - "gs://bucket/data.parquet"
      condition: "all"
actions:
  - sql:
      name: "task_1"
      query:
        inline: "SELECT 1"
      engine:
        bigquery:
          location: "US"
"""
            with open(conflict_file, "w", encoding="utf-8") as f:
                f.write(conflict_yaml)

            with self.assertRaises(ValueError) as ctx:
                api.validate(conflict_file)
            self.assertIn("cannot configure both 'schedule' and 'datasets'", str(ctx.exception))

    def test_validation_rejects_invalid_condition(self):
        """Tests that api.validate rejects dataset triggers with invalid condition."""
        with tempfile.TemporaryDirectory() as temp_dir:
            invalid_cond_file = os.path.join(temp_dir, "invalid_cond.yml")
            invalid_cond_yaml = """
modelVersion: "1.0"
pipelineId: "invalid_cond"
description: "Pipeline with invalid dataset trigger condition"
runner: "airflow"
owner: "data_team"
defaults:
  projectId: "test-project"
  location: "us-central1"
  executionConfig:
    retries: 0
triggers:
  - datasets:
      uris:
        - "gs://bucket/data.parquet"
      condition: "invalid_condition"
actions:
  - sql:
      name: "task_1"
      query:
        inline: "SELECT 1"
      engine:
        bigquery:
          location: "US"
"""
            with open(invalid_cond_file, "w", encoding="utf-8") as f:
                f.write(invalid_cond_yaml)

            with self.assertRaises(ValueError) as ctx:
                api.validate(invalid_cond_file)
            self.assertIn("Allowed values are 'all' or 'any'", str(ctx.exception))

    def test_validation_rejects_invalid_dataset_uris(self):
        """Tests that api.validate rejects empty or malformed dataset URIs."""
        with tempfile.TemporaryDirectory() as temp_dir:
            invalid_uri_file = os.path.join(temp_dir, "invalid_uri.yml")
            invalid_uri_yaml = """
modelVersion: "1.0"
pipelineId: "invalid_uri"
description: "Pipeline with invalid dataset URI"
runner: "airflow"
owner: "data_team"
defaults:
  projectId: "test-project"
  location: "us-central1"
  executionConfig:
    retries: 0
triggers:
  - datasets:
      uris:
        - ""
      condition: "all"
actions:
  - sql:
      name: "task_1"
      query:
        inline: "SELECT 1"
      engine:
        bigquery:
          location: "US"
"""
            with open(invalid_uri_file, "w", encoding="utf-8") as f:
                f.write(invalid_uri_yaml)

            with self.assertRaises(ValueError) as ctx:
                api.validate(invalid_uri_file)
            self.assertIn("cannot be an empty string", str(ctx.exception))

    def test_ai_batch_inference_action_with_outlets(self):
        """Tests that AI batch inference action correctly attaches outlets to operator."""
        with tempfile.TemporaryDirectory() as temp_dir:
            ai_file = os.path.join(temp_dir, "ai_pipeline.yml")
            ai_yaml = """
modelVersion: "1.0"
pipelineId: "ai_pipeline"
description: "AI batch inference with outlets"
runner: "airflow"
owner: "ml_team"
defaults:
  projectId: "test-project"
  location: "us-central1"
  executionConfig:
    retries: 0
triggers:
  - datasets:
      uris:
        - "bq://test-project.raw.input_features"
      condition: "all"
actions:
  - ai:
      name: "run_inference"
      agentPlatform:
        batchInference:
          jobDisplayName: "test_batch_prediction"
          modelName: "projects/test-project/locations/us-central1/models/123"
          instancesFormat: "bigquery"
          predictionsFormat: "bigquery"
          bigquerySource: "bq://test-project.raw.input_features"
          bigqueryDestinationPrefix: "bq://test-project.predictions"
      outlets:
        - "bq://test-project.predictions.output_table"
"""
            with open(ai_file, "w", encoding="utf-8") as f:
                f.write(ai_yaml)

            api.validate(ai_file)
            globals_dict = {}
            api.generate(ai_file, globals_dict)

            dag = globals_dict["ai_pipeline"]
            self.assertIsNotNone(dag.schedule)
            task = dag.get_task("run_inference")
            outlets = self._get_uris_from_outlets(task.outlets)
            self.assertEqual(outlets, ["bq://test-project.predictions.output_table"])


if __name__ == "__main__":
    unittest.main()
