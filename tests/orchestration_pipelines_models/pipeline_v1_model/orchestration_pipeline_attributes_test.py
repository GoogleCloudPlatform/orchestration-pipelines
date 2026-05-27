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


class TestProtoFieldAttributes(unittest.TestCase):
    """Tests that protobuf fields have the expected validation attributes."""

    def test_orchestration_pipeline_fields(self):
        """Tests validation attributes on OrchestrationPipeline fields."""
        descriptor = orchestration_pipeline_pb2.OrchestrationPipeline.DESCRIPTOR

        # Test pipeline_id field
        pipeline_id_field = descriptor.fields_by_name["pipeline_id"]
        pipeline_id_options = pipeline_id_field.GetOptions()
        self.assertTrue(
            pipeline_id_options.HasExtension(validation_pb2.is_required))
        self.assertTrue(
            pipeline_id_options.Extensions[validation_pb2.is_required])
        self.assertEqual(
            pipeline_id_options.Extensions[validation_pb2.regex],
            "^[a-zA-Z0-9_-]+$",
        )
        self.assertEqual(
            pipeline_id_options.Extensions[validation_pb2.min_len], 1)
        self.assertEqual(
            pipeline_id_options.Extensions[validation_pb2.max_len], 64)

        # Test runner field
        runner_field = descriptor.fields_by_name["runner"]
        runner_options = runner_field.GetOptions()
        self.assertTrue(
            runner_options.HasExtension(validation_pb2.disallow_zero_enum))
        self.assertTrue(
            runner_options.Extensions[validation_pb2.disallow_zero_enum])

        # Test actions field
        actions_field = descriptor.fields_by_name["actions"]
        actions_options = actions_field.GetOptions()
        self.assertTrue(actions_options.HasExtension(validation_pb2.min_items))
        self.assertEqual(actions_options.Extensions[validation_pb2.min_items],
                         1)

    def test_execution_config_fields(self):
        """Tests validation attributes on ExecutionConfig fields."""
        descriptor = orchestration_pipeline_pb2.ExecutionConfig.DESCRIPTOR

        # Test retries field
        retries_field = descriptor.fields_by_name["retries"]
        retries_options = retries_field.GetOptions()
        self.assertTrue(retries_options.HasExtension(validation_pb2.min_value))
        self.assertEqual(retries_options.Extensions[validation_pb2.min_value],
                         0)

    def test_schedule_trigger_fields(self):
        """Tests validation attributes on ScheduleTrigger fields."""
        descriptor = orchestration_pipeline_pb2.ScheduleTrigger.DESCRIPTOR

        # Test interval field
        interval_field = descriptor.fields_by_name["interval"]
        interval_options = interval_field.GetOptions()
        self.assertTrue(
            interval_options.Extensions[validation_pb2.is_required])
        self.assertTrue(
            interval_options.Extensions[validation_pb2.is_cron_expression])

        # Test start_time field
        start_time_field = descriptor.fields_by_name["start_time"]
        start_time_options = start_time_field.GetOptions()
        self.assertTrue(
            start_time_options.Extensions[validation_pb2.is_required])
        self.assertTrue(
            start_time_options.Extensions[validation_pb2.is_iso8601_timestamp])


if __name__ == "__main__":
    unittest.main()
