# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""Tests for the generated protobuf field attributes."""

import unittest

from orchestration_pipelines_models.pipeline_v1_model.protos import (
    orchestration_pipeline_pb2,
    validation_pb2,
)


def _collect_parameterizable_fields(descriptor, prefix=""):
    """Recursively collects all 'Message.field' paths with is_parameterizable=True."""
    result = set()
    msg_path = f"{prefix}.{descriptor.name}" if prefix else descriptor.name
    for field in descriptor.fields:
        options = field.GetOptions()
        if options.Extensions[validation_pb2.is_parameterizable]:
            result.add(f"{msg_path}.{field.name}")
    for nested in descriptor.nested_types:
        if not nested.GetOptions().map_entry:
            result.update(_collect_parameterizable_fields(nested, msg_path))
    return result


class TestProtoFieldAttributes(unittest.TestCase):
    """Tests that protobuf fields have the expected validation attributes."""

    def test_orchestration_pipeline_fields(self):
        """Tests validation attributes on OrchestrationPipeline fields."""
        descriptor = orchestration_pipeline_pb2.OrchestrationPipeline.DESCRIPTOR

        pipeline_id_options = descriptor.fields_by_name[
            "pipeline_id"
        ].GetOptions()
        runner_options = descriptor.fields_by_name["runner"].GetOptions()
        actions_options = descriptor.fields_by_name["actions"].GetOptions()
        field_names = descriptor.fields_by_name

        self.assertTrue(
            pipeline_id_options.Extensions[validation_pb2.is_required]
        )
        self.assertEqual(
            pipeline_id_options.Extensions[validation_pb2.regex],
            "^[a-zA-Z0-9_-]+$",
        )
        self.assertEqual(
            pipeline_id_options.Extensions[validation_pb2.min_len], 1
        )
        self.assertEqual(
            pipeline_id_options.Extensions[validation_pb2.max_len], 64
        )
        self.assertTrue(
            runner_options.Extensions[validation_pb2.disallow_zero_enum]
        )
        self.assertEqual(
            actions_options.Extensions[validation_pb2.min_items], 1
        )
        self.assertIn("parameters", field_names)
        self.assertIn("dynamic_bindings", field_names)

    def test_parameter_message_fields(self):
        """Tests validation attributes and enum values on Parameter message."""
        descriptor = orchestration_pipeline_pb2.Parameter.DESCRIPTOR
        expected_enum_values = {
            "type_undefined": 0,
            "string": 1,
            "number": 2,
            "boolean": 3,
            "array": 4,
        }

        type_options = descriptor.fields_by_name["type"].GetOptions()
        default_field = descriptor.fields_by_name["default_value"]
        default_options = default_field.GetOptions()
        actual_enum_values = dict(
            orchestration_pipeline_pb2.Parameter.Type.items()
        )

        self.assertTrue(
            type_options.Extensions[validation_pb2.disallow_zero_enum]
        )
        self.assertEqual(default_field.json_name, "default")
        self.assertTrue(default_options.Extensions[validation_pb2.is_required])
        self.assertIn("description", descriptor.fields_by_name)
        self.assertEqual(actual_enum_values, expected_enum_values)

    def test_execution_config_fields(self):
        """Tests validation attributes on ExecutionConfig fields."""
        descriptor = orchestration_pipeline_pb2.ExecutionConfig.DESCRIPTOR

        retries_options = descriptor.fields_by_name["retries"].GetOptions()

        self.assertTrue(retries_options.HasExtension(validation_pb2.min_value))
        self.assertEqual(
            retries_options.Extensions[validation_pb2.min_value], 0
        )

    def test_schedule_trigger_fields(self):
        """Tests validation attributes on ScheduleTrigger fields."""
        descriptor = orchestration_pipeline_pb2.ScheduleTrigger.DESCRIPTOR

        interval_options = descriptor.fields_by_name["interval"].GetOptions()
        start_time_options = descriptor.fields_by_name[
            "start_time"
        ].GetOptions()

        self.assertTrue(interval_options.Extensions[validation_pb2.is_required])
        self.assertTrue(
            interval_options.Extensions[validation_pb2.is_cron_expression]
        )
        self.assertTrue(
            start_time_options.Extensions[validation_pb2.is_required]
        )
        self.assertTrue(
            start_time_options.Extensions[validation_pb2.is_iso8601_timestamp]
        )

    def test_all_actions_have_dynamic_bindings_and_run_if(self):
        """Tests that every action variant defines dynamic_bindings and run_if."""
        action_fields = (
            orchestration_pipeline_pb2.Action.DESCRIPTOR.oneofs_by_name[
                "action"
            ].fields
        )
        expected_fields = {"dynamic_bindings", "run_if"}

        missing_by_action = {
            field.message_type.name: expected_fields
            - set(field.message_type.fields_by_name)
            for field in action_fields
        }

        self.assertEqual(
            missing_by_action,
            {field.message_type.name: set() for field in action_fields},
        )

    def test_is_parameterizable_annotations(self):
        """Tests that exactly the expected leaf fields have is_parameterizable=True."""
        message_descriptors = (
            orchestration_pipeline_pb2.DESCRIPTOR.message_types_by_name.values()
        )
        expected = {
            # Pipeline-level operational & infrastructure settings
            "Defaults.project_id",
            "Defaults.location",
            "ExecutionConfig.retries",
            "FixedDelayStrategy.retry_delay",
            "ExponentialBackoffStrategy.initial_delay",
            "ExponentialBackoffStrategy.max_delay",
            "ExponentialBackoffStrategy.multiplier",
            "ExponentialBackoffStrategy.randomize_jitter",
            "RetryPolicy.max_retries",
            "OnPipelineFailure.email",
            "OnPipelineSuccess.email",
            # Engines
            "BigQueryEngine.location",
            "BigQueryEngine.destination_table",
            "BigQueryEngine.impersonation_chain",
            "DataprocExistingClusterConfiguration.cluster_name",
            "DataprocExistingClusterConfiguration.location",
            "DataprocExistingClusterConfiguration.project_id",
            "DataprocExistingClusterConfiguration.impersonation_chain",
            "DataprocExistingClusterConfiguration.properties",
            "DataprocEphemeralConfiguration.cluster_name",
            "DataprocEphemeralConfiguration.location",
            "DataprocEphemeralConfiguration.project_id",
            "DataprocEphemeralConfiguration.properties",
            "DataprocEphemeralConfiguration.impersonation_chain",
            "DataprocClusterResourceProfile.path",
            "DataprocClusterResourceProfile.external_config_path",
            "DataprocClusterResourceProfile.InlineConfig.config",
            "DataprocBatchResourceProfile.path",
            "DataprocBatchResourceProfile.external_config_path",
            "DataprocBatchResourceProfile.InlineConfig.runtime_config",
            "DataprocBatchResourceProfile.InlineConfig.environment_config",
            "DataprocServerlessBatchEngine.location",
            "DataprocServerlessBatchEngine.impersonation_chain",
            # Actions
            "PythonAction.execution_timeout",
            "PythonAction.op_kwargs",
            "PythonAction.run_if",
            "PysparkAction.execution_timeout",
            "PysparkAction.staging_bucket",
            "PysparkAction.params",
            "PysparkAction.labels",
            "PysparkAction.run_if",
            "NotebookAction.execution_timeout",
            "NotebookAction.staging_bucket",
            "NotebookAction.params",
            "NotebookAction.labels",
            "NotebookAction.run_if",
            "SqlAction.execution_timeout",
            "SqlAction.params",
            "SqlAction.labels",
            "SqlAction.run_if",
            "PipelineAction.execution_timeout",
            "PipelineAction.params",
            "PipelineAction.labels",
            "PipelineAction.run_if",
            "DataformServiceExecution.location",
            "DataformServiceExecution.project_id",
            "DataformServiceExecution.repository_id",
            "DataformServiceExecution.workflow_invocation",
            "DbtAirflowExecution.select_models",
            "DbtAirflowExecution.tags",
            "DataIngestionAction.execution_timeout",
            "DataIngestionAction.run_if",
            "BigQueryDtsSpec.transfer_config_id",
            "BigQueryDtsSpec.requested_run_time",
            "BigQueryDtsSpec.impersonation_chain",
            "BigQueryDtsSpec.project_id",
            "BigQueryDtsSpec.location",
            "BigQueryDtsSpec.TimeRange.start_time",
            "BigQueryDtsSpec.TimeRange.end_time",
            "OrchestrationPipelineAction.execution_timeout",
            "OrchestrationPipelineAction.wait_for_completion",
            "OrchestrationPipelineAction.params",
            "OrchestrationPipelineAction.run_if",
            "AIAction.execution_timeout",
            "AIAction.labels",
            "AIAction.run_if",
            "AgentPlatform.project_id",
            "AgentPlatform.location",
            "AgentPlatformModelUpload.model_name",
            "AgentPlatformModelUpload.description",
            "AgentPlatformModelUpload.model_artifact_uri",
            "AgentPlatformBatchInference.job_display_name",
            "AgentPlatformBatchInference.model_name",
            "AgentPlatformBatchInference.instances_format",
            "AgentPlatformBatchInference.predictions_format",
            "AgentPlatformBatchInference.bigquery_source",
            "AgentPlatformBatchInference.gcs_source",
            "AgentPlatformBatchInference.bigquery_destination_prefix",
            "AgentPlatformBatchInference.gcs_destination_prefix",
            "AgentPlatformBatchInference.impersonation_chain",
            "AgentPlatformCreateAndRunCustomJob.custom_job",
            "AgentPlatformCreateAndRunCustomJob.execution_timeout",
            "AgentPlatformCreateAndRunCustomJob.impersonation_chain",
            "AirflowTaskAction.execution_timeout",
            "AirflowTaskAction.params",
            "AirflowTaskAction.run_if",
        }

        actual = set()
        for msg_desc in message_descriptors:
            actual.update(_collect_parameterizable_fields(msg_desc))

        self.assertSetEqual(actual, expected)


if __name__ == "__main__":
    unittest.main()
