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
"""Unit tests for the pipeline_metadata utility."""

import json
import unittest
from unittest.mock import MagicMock

from orchestration_pipelines_lib.internal_models.pipeline import (
    CloudDefaultsModel, DefaultsModel, ExecutionConfigDefaultsModel,
    MetaDataModel, PipelineModel, RunnerType)
from orchestration_pipelines_lib.internal_models.triggers import \
    ScheduleTriggerModel
from orchestration_pipelines_lib.utils.pipeline_metadata import PipelineMetadata
from orchestration_pipelines_models.manifest.manifest import Manifest
from orchestration_pipelines_models.manifest.manifest_pb2 import DeploymentOrigination


class TestPipelineMetadata(unittest.TestCase):
    """Test suite for the PipelineMetadata class."""

    def setUp(self):
        """Set up common variables and mocks for tests."""
        # Mock Manifest
        self.mock_manifest = MagicMock(spec=Manifest)
        self.mock_manifest.get_bundle_id.return_value = "test-bundle"
        self.mock_manifest.is_paused.return_value = False
        self.mock_manifest.is_current.return_value = True

        mock_deployment_details = MagicMock()
        mock_deployment_details.origination = DeploymentOrigination.GIT_CI_CD
        mock_deployment_details.git_repo = "github.com/my-org/my-repo"
        mock_deployment_details.git_branch = "main"
        mock_deployment_details.commit_sha = "abcdef123"
        self.mock_manifest.get_deployment_details.return_value = mock_deployment_details

        # Real PipelineModel
        self.pipeline_id = "test-pipeline"
        self.version_id = "v1.0.0"
        self.pipeline_model = PipelineModel(
            defaults=DefaultsModel(
                cloudDefault=CloudDefaultsModel(project="test-proj",
                                                region="us-central1"),
                executionConfigDefault=ExecutionConfigDefaultsModel(
                    retries=1)),
            metadata=MetaDataModel(pipelineId=self.pipeline_id,
                                   description="A test pipeline.",
                                   owner="test-owner",
                                   tags=[
                                       "customer-tag-1",
                                       "op:owner:should-be-filtered",
                                       "customer-tag-2"
                                   ]),
            runner=RunnerType.AIRFLOW,
            triggers=[
                ScheduleTriggerModel(type="schedule",
                                     scheduleInterval="0 0 * * *",
                                     startTime="2023-01-01T00:00:00",
                                     endTime="2024-01-01T00:00:00",
                                     catchup=False,
                                     timezone="America/New_York")
            ],
            actions=[])

    def test_initialization_and_properties(self):
        """Test that basic properties are correctly initialized."""
        metadata = PipelineMetadata(pipeline_id=self.pipeline_id,
                                    manifest=self.mock_manifest,
                                    version_id=self.version_id)
        # Check internal attributes are set as expected
        self.assertEqual(metadata._pipeline_id, self.pipeline_id)
        self.assertEqual(metadata._bundle_id, "test-bundle")
        self.assertEqual(metadata._version_id, self.version_id)
        self.assertFalse(metadata._is_paused)
        self.assertTrue(metadata._is_current)
        self.assertEqual(metadata._origination, "GIT_CI_CD")
        self.assertEqual(metadata._repo, "github.com/my-org/my-repo")
        self.assertEqual(metadata._branch, "main")
        self.assertEqual(metadata._commit, "abcdef123")

    def test_is_paused(self):
        """Tests the is_paused method."""
        # Case 1: Pipeline is not paused
        self.mock_manifest.is_paused.return_value = False
        metadata_not_paused = PipelineMetadata(pipeline_id=self.pipeline_id,
                                               manifest=self.mock_manifest,
                                               version_id=self.version_id)
        self.assertFalse(metadata_not_paused.is_paused())

        # Case 2: Pipeline is paused
        self.mock_manifest.is_paused.return_value = True
        metadata_paused = PipelineMetadata(pipeline_id=self.pipeline_id,
                                           manifest=self.mock_manifest,
                                           version_id=self.version_id)
        self.assertTrue(metadata_paused.is_paused())

    def test_tags_generation(self):
        """Test the generation of DAG tags."""
        metadata = PipelineMetadata(pipeline_id=self.pipeline_id,
                                    manifest=self.mock_manifest,
                                    version_id=self.version_id)
        tags = metadata.generate_tags(
            owner=self.pipeline_model.metadata.owner,
            customer_tags=self.pipeline_model.metadata.tags)

        # Check for orchestration tags
        self.assertIn("op:orchestration_pipeline", tags)
        self.assertIn("op:bundle:test-bundle", tags)
        self.assertIn("op:version:v1.0.0", tags)
        self.assertIn("op:pipeline:test-pipeline", tags)
        self.assertIn("op:owner:test-owner", tags)
        self.assertIn("op:origination:GIT_CI_CD", tags)
        self.assertIn("op:is_current", tags)
        self.assertNotIn("op:is_paused", tags)

        # Check for customer tags (and filtering)
        self.assertIn("customer-tag-1", tags)
        self.assertIn("customer-tag-2", tags)
        self.assertNotIn("op:owner:should-be-filtered", tags)

    def test_tags_paused_and_not_current(self):
        """Test tags when pipeline is paused and not the current version."""
        self.mock_manifest.is_paused.return_value = True
        self.mock_manifest.is_current.return_value = False

        metadata = PipelineMetadata(pipeline_id=self.pipeline_id,
                                    manifest=self.mock_manifest,
                                    version_id=self.version_id)
        tags = metadata.generate_tags(
            owner=self.pipeline_model.metadata.owner,
            customer_tags=self.pipeline_model.metadata.tags)

        self.assertIn("op:is_paused", tags)
        self.assertNotIn("op:is_current", tags)

    def test_doc_md_generation_full(self):
        """Test the generation of the doc_md JSON string with all fields."""
        metadata = PipelineMetadata(pipeline_id=self.pipeline_id,
                                    manifest=self.mock_manifest,
                                    version_id=self.version_id)
        schedule_trigger = self.pipeline_model.triggers[0]
        doc_md_str = metadata.generate_doc_md(
            owner=self.pipeline_model.metadata.owner,
            schedule_trigger=schedule_trigger)
        doc_md = json.loads(doc_md_str)

        expected_doc = {
            "op_bundle": "test-bundle",
            "op_version": "v1.0.0",
            "op_pipeline": "test-pipeline",
            "op_owner": "test-owner",
            "op_origination": "GIT_CI_CD",
            "op_is_paused": False,
            "op_is_current": True,
            "op_deployment_details": {
                "op_repository": "github.com/my-org/my-repo",
                "op_branch": "main",
                "op_commit_sha": "abcdef123"
            },
            "op_schedule": {
                "scheduleInterval": "0 0 * * *",
                "startTime": "2023-01-01T00:00:00",
                "endTime": "2024-01-01T00:00:00",
                "catchup": False,
                "timezone": "America/New_York"
            }
        }
        self.assertDictEqual(doc_md, expected_doc)

    def test_doc_md_generation_minimal(self):
        """Test doc_md generation with minimal data (no schedule, no deployment details)."""
        # Modify pipeline to have no triggers
        self.pipeline_model.triggers = []
        # Modify manifest to have no deployment details
        self.mock_manifest.get_deployment_details.return_value = None
        metadata = PipelineMetadata(pipeline_id=self.pipeline_id,
                                    manifest=self.mock_manifest,
                                    version_id=self.version_id)
        doc_md_str = metadata.generate_doc_md(
            owner=self.pipeline_model.metadata.owner, schedule_trigger=None)
        doc_md = json.loads(doc_md_str)

        self.assertNotIn("op_deployment_details", doc_md)
        self.assertNotIn("op_schedule", doc_md)

        # Check that other fields are still present
        self.assertEqual(doc_md["op_bundle"], "test-bundle")
        self.assertEqual(doc_md["op_pipeline"], self.pipeline_id)
        self.assertEqual(doc_md["op_origination"], "")

    def test_empty_and_none_values(self):
        """Test behavior with empty or None values for optional fields."""
        self.pipeline_model.metadata.owner = ""
        self.pipeline_model.metadata.tags = None
        self.mock_manifest.get_deployment_details.return_value.origination = None

        metadata = PipelineMetadata(pipeline_id=self.pipeline_id,
                                    manifest=self.mock_manifest,
                                    version_id=self.version_id)

        # Test tags
        tags = metadata.generate_tags(
            owner=self.pipeline_model.metadata.owner,
            customer_tags=self.pipeline_model.metadata.tags)
        self.assertIn("op:owner:", tags)  # Should be present with empty value
        self.assertNotIn("customer-tag-1", tags)  # Should not be present

        # Test doc_md
        doc_md = json.loads(
            metadata.generate_doc_md(owner=self.pipeline_model.metadata.owner,
                                     schedule_trigger=None))
        self.assertEqual(doc_md["op_owner"], "")
        self.assertEqual(doc_md["op_origination"], "")
