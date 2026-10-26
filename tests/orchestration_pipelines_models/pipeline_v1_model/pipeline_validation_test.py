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
# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""Unit tests for the PipelineValidator."""

import unittest

import pytest
from google.protobuf import struct_pb2

from orchestration_pipelines_models.pipeline_v1_model.pipeline_validation import (
    PipelineValidator,
)
from orchestration_pipelines_models.pipeline_v1_model.protos.orchestration_pipeline_pb2 import (
    Action,
    AgentPlatform,
    AgentPlatformBatchInference,
    AgentPlatformCreateAndRunCustomJob,
    AgentPlatformModelUpload,
    AIAction,
    BigQueryEngine,
    Defaults,
    ExecutionConfig,
    ExponentialBackoffStrategy,
    FixedDelayStrategy,
    LocalEngine,
    OrchestrationPipeline,
    Parameter,
    PipelineRunner,
    PysparkAction,
    PythonAction,
    PythonEngine,
    Query,
    RetryPolicy,
    ScheduleTrigger,
    SqlAction,
    SqlEngine,
    Trigger,
    TriggerRule,
)


class TestPipelineValidator(unittest.TestCase):
    """Test suite for the PipelineValidator."""

    def setUp(self):
        """Set up a baseline valid pipeline object for each test."""
        self.pipeline = OrchestrationPipeline(
            model_version="1.0",
            pipeline_id="valid-pipeline-id",
            runner=PipelineRunner.airflow,
            owner="valid-owner",
            defaults=Defaults(
                project_id="example-project",
                location="us-central1",
                execution_config=ExecutionConfig(retries=1),
            ),
            actions=[
                Action(python=PythonAction(
                    name="valid-action-name",
                    main_file_path="/path/to/script.py",
                    python_callable="main",
                    engine=PythonEngine(local=LocalEngine()),
                ))
            ],
        )

    def test_valid_pipeline_succeeds(self):
        """Tests that a valid pipeline passes validation."""
        try:
            PipelineValidator.validate(self.pipeline)
        except ValueError as e:
            self.fail(f"Validation failed unexpectedly: {e}")

    def test_is_required_string_fails(self):
        """Tests failure when a required string field is empty."""
        self.pipeline.pipeline_id = ""
        with self.assertRaisesRegex(
                ValueError,
            ("Error for field 'pipeline_id': field is required and cannot be "
             "an empty string."),
        ):
            PipelineValidator.validate(self.pipeline)

    def test_is_required_message_fails(self):
        """Tests failure when a required message field is not set."""
        self.pipeline.ClearField("defaults")
        with self.assertRaisesRegex(
                ValueError, "Error for field 'defaults': field is required."):
            PipelineValidator.validate(self.pipeline)

    def test_regex_fails(self):
        """Tests failure when a string field does not match its regex pattern."""
        self.pipeline.pipeline_id = "invalid!"
        with self.assertRaisesRegex(
                ValueError,
                "Error for field 'pipeline_id': value 'invalid!' does not match regex pattern",
        ):
            PipelineValidator.validate(self.pipeline)

    def test_max_len_fails(self):
        """Tests failure when a string field exceeds its maximum length."""
        self.pipeline.pipeline_id = "a" * 65
        with self.assertRaisesRegex(
                ValueError,
                "Error for field 'pipeline_id': length must be at most 64, but is 65"
        ):
            PipelineValidator.validate(self.pipeline)

    def test_min_value_fails(self):
        """Tests failure when a numeric field is below its minimum value."""
        self.pipeline.defaults.execution_config.retries = -1
        with self.assertRaisesRegex(
                ValueError,
                "Error for field 'defaults.execution_config.retries': value -1 must be at least 0",
        ):
            PipelineValidator.validate(self.pipeline)

    def test_min_items_fails(self):
        """Tests failure when a repeated field has fewer items than required."""
        self.pipeline.ClearField("actions")
        with self.assertRaisesRegex(
                ValueError,
                "Error for field 'actions': must have at least 1 items, but has 0"
        ):
            PipelineValidator.validate(self.pipeline)

    def test_disallow_zero_enum_fails(self):
        """Tests failure when an enum field has the disallowed zero value."""
        self.pipeline.runner = PipelineRunner.pipeline_runner_undefined
        with self.assertRaisesRegex(
                ValueError,
                "Error for field 'runner': must not be the default enum value"
        ):
            PipelineValidator.validate(self.pipeline)

    def test_is_cron_expression_fails(self):
        """Tests failure for an invalid cron expression string."""
        trigger = Trigger(schedule=ScheduleTrigger(
            interval="not a cron", start_time="2025-01-01T00:00:00Z"))
        self.pipeline.triggers.extend([trigger])
        with self.assertRaisesRegex(
                ValueError,
            (r"Error for field 'triggers\[0\].schedule.interval': "
             r".*Invalid CRON expression"),
        ):
            PipelineValidator.validate(self.pipeline)

    def test_is_iso8601_timestamp_fails(self):
        """Tests failure for an invalid ISO 8601 timestamp string."""
        trigger = Trigger(schedule=ScheduleTrigger(interval="* * * * *",
                                                   start_time="2025/01/01"))
        self.pipeline.triggers.extend([trigger])
        with self.assertRaisesRegex(
                ValueError,
            (r"Error for field 'triggers\[0\].schedule.start_time': "
             r"Invalid isoformat string: '2025/01/01'"),
        ):
            PipelineValidator.validate(self.pipeline)

    def test_is_iso8601_timestamp_start_time_trailing_quotes(self):
        """Tests descriptive error when start_time has mismatched trailing quotes."""
        trigger = Trigger(schedule=ScheduleTrigger(
            interval="* * * * *",
            start_time='2025-10-01T00:00:00"\'',  # trailing quotes
        ))
        self.pipeline.triggers.extend([trigger])
        with self.assertRaisesRegex(
                ValueError,
            (r"Error for field 'triggers\[0\].schedule.start_time': "
             r"mismatched quote boundaries\."),
        ):
            PipelineValidator.validate(self.pipeline)

    def test_is_iso8601_timestamp_end_time_leading_quote(self):
        """Tests descriptive error when end_time has a mismatched leading quote."""
        trigger = Trigger(schedule=ScheduleTrigger(
            interval="* * * * *",
            start_time="2025-09-01T00:00:00",
            end_time='"2026-10-01T00:00:00',  # leading quote
        ))
        self.pipeline.triggers.extend([trigger])
        with self.assertRaisesRegex(
                ValueError,
            (r"Error for field 'triggers\[0\].schedule.end_time': "
             r"mismatched quote boundaries\."),
        ):
            PipelineValidator.validate(self.pipeline)

    def test_is_cron_expression_trailing_quote(self):
        """Tests descriptive error when cron interval has a mismatched trailing quote."""
        trigger = Trigger(schedule=ScheduleTrigger(
            interval='* * * * *"', start_time="2025-01-01T00:00:00Z"
        ))
        self.pipeline.triggers.extend([trigger])
        with self.assertRaisesRegex(
                ValueError,
            (r"Error for field 'triggers\[0\].schedule.interval': "
             r"mismatched quote boundaries\."),
        ):
            PipelineValidator.validate(self.pipeline)

    def test_iso8601_duration_leading_quote(self):
        """Tests descriptive error when duration has a mismatched leading quote."""
        self.pipeline.actions[0].python.execution_timeout = '"1h'
        with self.assertRaisesRegex(
                ValueError,
            (r"Error for field 'actions\[0\].python.execution_timeout': "
             r"mismatched quote boundaries\."),
        ):
            PipelineValidator.validate(self.pipeline)

    def test_iana_timezone_leading_quote(self):
        """Tests descriptive error when timezone has a mismatched leading quote."""
        trigger = Trigger(schedule=ScheduleTrigger(
            interval="* * * * *",
            start_time="2025-01-01T00:00:00Z",
            timezone='"UTC',
        ))
        self.pipeline.triggers.extend([trigger])
        with self.assertRaisesRegex(
                ValueError,
            (r"Error for field 'triggers\[0\].schedule.timezone': "
             r"mismatched quote boundaries\."),
        ):
            PipelineValidator.validate(self.pipeline)


    def test_duration_fails(self):
        """Tests failure for an invalid duration string."""
        self.pipeline.actions[0].python.execution_timeout = "10 minutes"
        with self.assertRaisesRegex(
                ValueError,
                r"Error for field 'actions\[0\].python.execution_timeout': Invalid duration format",
        ):
            PipelineValidator.validate(self.pipeline)

    def test_is_iana_timezone_fails(self):
        """Tests failure for an invalid IANA timezone string."""
        trigger = Trigger(schedule=ScheduleTrigger(
            interval="* * * * *",
            start_time="2025-01-01T00:00:00Z",
            timezone="Mars/Olympus_Mons",
        ))
        self.pipeline.triggers.extend([trigger])
        with self.assertRaisesRegex(
                ValueError,
            (r"Error for field 'triggers\[0\].schedule.timezone': "
             r".*is not a valid timezone"),
        ):
            PipelineValidator.validate(self.pipeline)

    def test_nested_message_validation_fails(self):
        """Tests that validation recurses into nested messages."""
        self.pipeline.defaults.execution_config.retries = -1
        with self.assertRaisesRegex(
                ValueError,
                "Error for field 'defaults.execution_config.retries': value -1 must be at least 0",
        ):
            PipelineValidator.validate(self.pipeline)

    def test_repeated_message_validation_fails(self):
        """Tests that validation recurses into items in a repeated message field."""
        invalid_action = Action(pyspark=PysparkAction(
            name="invalid-pyspark",
            main_file_path="/path/to/job.py",
            # Missing required 'engine' field
        ))
        self.pipeline.actions.extend([invalid_action])
        with self.assertRaisesRegex(
                ValueError,
                r"Error for field 'actions\[1\].pyspark.engine': field is required",
        ):
            PipelineValidator.validate(self.pipeline)

    def test_optional_empty_fields_succeed(self):
        """Tests that validation succeeds when optional fields are empty or unset."""
        self.pipeline.ClearField("description")
        self.pipeline.ClearField("tags")
        self.pipeline.ClearField("notifications")
        try:
            PipelineValidator.validate(self.pipeline)
        except ValueError as e:
            self.fail(
                f"Validation failed unexpectedly for optional empty fields: {e}"
            )

    def test_valid_duration_succeeds(self):
        """Tests that a valid duration string passes validation."""
        self.pipeline.actions[0].python.execution_timeout = "1h 30m"
        try:
            PipelineValidator.validate(self.pipeline)
        except ValueError as e:
            self.fail(
                f"Validation failed unexpectedly for valid duration: {e}")

    def test_invalid_map_key(self):
        """Tests failure when a map key does not match its regex pattern."""
        action = Action(
            sql=SqlAction(
                name="test-SQL",
                params={"": "value"},
                engine=SqlEngine(),
                query=Query(inline="SELECT 1 WHERE name='{{ params.name }}'"),
            )
        )
        self.pipeline.actions.extend([action])

        with self.assertRaisesRegex(
            ValueError,
            (
                r"Error for field 'actions\[1\]\.sql.params\.<>': "
                r"value '' does not match regex pattern .+"
            ),
        ):
            PipelineValidator.validate(self.pipeline)

    def test_invalid_map_value(self):
        """Tests failure when a map value does not match its regex pattern."""
        action = Action(
            sql=SqlAction(
                name="test-SQL",
                params={"name": "';"},
                engine=SqlEngine(),
                query=Query(inline="SELECT 1 WHERE name='{{ params.name }}'"),
            )
        )
        self.pipeline.actions.extend([action])

        with self.assertRaisesRegex(
            ValueError,
            (
                r"Error for field 'actions\[1\]\.sql.params\['name'\]': "
                r"value '';' does not match regex pattern .+"
            ),
        ):
            PipelineValidator.validate(self.pipeline)

    def test_duplicate_action_name_fails(self):
        """Tests failure when two actions have the same name."""
        duplicate_action = Action(python=PythonAction(
            name="valid-action-name",  # Same name as the one in setUp
            main_file_path="/path/to/another/script.py",
            python_callable="main",
            engine=PythonEngine(local=LocalEngine()),
        ))
        self.pipeline.actions.extend([duplicate_action])
        with self.assertRaisesRegex(
                ValueError,
            (r"Error for field 'actions\[1\].python.name': "
             r"Duplicate action name 'valid-action-name' found."),
        ):
            PipelineValidator.validate(self.pipeline)

    def test_undefined_dependency_fails(self):
        """Tests failure when an action depends on a non-existent action."""
        self.pipeline.actions[0].python.depends_on.append(
            "non-existent-action")
        with self.assertRaisesRegex(
                ValueError,
            (r"Error for field 'actions\[0\].python.depends_on': "
             r"Action 'valid-action-name' depends on undefined action "
             r"'non-existent-action'."),
        ):
            PipelineValidator.validate(self.pipeline)

    def test_validation_success_when_no_cycle(self):
        """Tests success when pipeline has no circular flow."""
        action_2 = Action(python=PythonAction(
                    name="valid-action-name-2",
                    main_file_path="/path/to/script.py",
                    python_callable="main",
                    engine=PythonEngine(local=LocalEngine()),
                    depends_on=[self.pipeline.actions[0].python.name]
                ))
        action_3 = Action(python=PythonAction(
                    name="valid-action-name-3",
                    main_file_path="/path/to/script.py",
                    python_callable="main",
                    engine=PythonEngine(local=LocalEngine()),
                    depends_on=[action_2.python.name]
                ))
        self.pipeline.actions.append(action_2)
        self.pipeline.actions.append(action_3)

        caught_exception = None
        try:
            PipelineValidator.validate(self.pipeline)
        except ValueError as e:
            caught_exception = e

        self.assertIsNone(caught_exception, f"Validation raised {caught_exception}")

    def test_validation_fails_when_cycle_exists(self):
        """Tests failure when actions have circular dependencies."""
        action_2 = Action(python=PythonAction(
                    name="valid-action-name-2",
                    main_file_path="/path/to/script.py",
                    python_callable="main",
                    engine=PythonEngine(local=LocalEngine()),
                    depends_on=[self.pipeline.actions[0].python.name]
                ))
        action_3 = Action(python=PythonAction(
                    name="valid-action-name-3",
                    main_file_path="/path/to/script.py",
                    python_callable="main",
                    engine=PythonEngine(local=LocalEngine()),
                    depends_on=[action_2.python.name]
                ))
        # add other actions and incorrect dependency
        self.pipeline.actions[0].python.depends_on.append(action_3.python.name)
        self.pipeline.actions.append(action_2)
        self.pipeline.actions.append(action_3)

        with self.assertRaisesRegex(
                ValueError,
            (r"Circular dependency detected: "),
        ):
            PipelineValidator.validate(self.pipeline)


    def test_valid_ai_action_succeeds(self):
        """Tests that a pipeline with a valid AI action passes validation."""
        ai_action = Action(
            ai=AIAction(
                name="upload-model",
                agent_platform=AgentPlatform(
                    model_upload=AgentPlatformModelUpload(
                        model_name="my-model",
                        model_artifact_uri="gs://bucket/model",
                        serving_container_image_uri="gcr.io/test/image:latest",
                    )
                ),
            )
        )
        self.pipeline.actions.append(ai_action)

        result = PipelineValidator.validate(self.pipeline)

        self.assertIsNone(result)

    def test_ai_action_missing_model_name_fails(self):
        """Tests that missing model_name in model_upload fails validation."""
        ai_action = Action(
            ai=AIAction(
                name="upload-model",
                agent_platform=AgentPlatform(
                    model_upload=AgentPlatformModelUpload(
                        model_name="",
                        model_artifact_uri="gs://bucket/model",
                        serving_container_image_uri="gcr.io/test/image:latest",
                    )
                ),
            )
        )
        self.pipeline.actions.append(ai_action)

        with self.assertRaises(ValueError) as cm:
            PipelineValidator.validate(self.pipeline)

        self.assertIn(
            "Error for field 'actions[1].ai.agent_platform.model_upload.model_name': "
            "field is required and cannot be an empty string.",
            str(cm.exception),
        )

    def test_ai_action_missing_artifact_uri_fails(self):
        """Tests that missing model_artifact_uri in model_upload fails validation."""
        ai_action = Action(
            ai=AIAction(
                name="upload-model",
                agent_platform=AgentPlatform(
                    model_upload=AgentPlatformModelUpload(
                        model_name="my-model",
                        model_artifact_uri="",
                        serving_container_image_uri="gcr.io/test/image:latest",
                    )
                ),
            )
        )
        self.pipeline.actions.append(ai_action)

        with self.assertRaises(ValueError) as cm:
            PipelineValidator.validate(self.pipeline)

        self.assertIn(
            "Error for field 'actions[1].ai.agent_platform.model_upload.model_artifact_uri': "
            "field is required and cannot be an empty string.",
            str(cm.exception),
        )

    def test_ai_action_missing_serving_image_fails(self):
        """Tests that missing serving_container_image_uri in model_upload fails validation."""
        ai_action = Action(
            ai=AIAction(
                name="upload-model",
                agent_platform=AgentPlatform(
                    model_upload=AgentPlatformModelUpload(
                        model_name="my-model",
                        model_artifact_uri="gs://bucket/model",
                        serving_container_image_uri="",
                    )
                ),
            )
        )
        self.pipeline.actions.append(ai_action)

        with self.assertRaises(ValueError) as cm:
            PipelineValidator.validate(self.pipeline)

        self.assertIn(
            "Error for field 'actions[1].ai.agent_platform.model_upload.serving_container_image_uri': "
            "field is required and cannot be an empty string.",
            str(cm.exception),
        )

    def test_valid_ai_action_batch_inference_succeeds(self):
        """Tests that a pipeline with a valid batch inference AI action passes validation."""
        ai_action = Action(
            ai=AIAction(
                name="batch-prediction",
                agent_platform=AgentPlatform(
                    batch_inference=AgentPlatformBatchInference(
                        job_display_name="my-batch-job",
                        model_name="projects/p/locations/l/models/m",
                        instances_format="bigquery",
                        predictions_format="bigquery",
                        bigquery_source="bq://p.d.t",
                        bigquery_destination_prefix="bq://p.d",
                    )
                ),
            )
        )
        self.pipeline.actions.append(ai_action)
        try:
            PipelineValidator.validate(self.pipeline)
        except ValueError as e:
            self.fail(f"Validation failed unexpectedly: {e}")

    def test_ai_action_batch_inference_missing_job_display_name_fails(self):
        """Tests that missing job_display_name in batch_inference fails validation."""
        ai_action = Action(
            ai=AIAction(
                name="batch-prediction",
                agent_platform=AgentPlatform(
                    batch_inference=AgentPlatformBatchInference(
                        job_display_name="",
                        model_name="projects/p/locations/l/models/m",
                    )
                ),
            )
        )
        self.pipeline.actions.append(ai_action)
        with self.assertRaisesRegex(
            ValueError,
            "Error for field 'actions\\[1\\]\\.ai\\.agent_platform\\.batch_inference\\.job_display_name': "
            "field is required and cannot be an empty string.",
        ):
            PipelineValidator.validate(self.pipeline)

    def test_ai_action_batch_inference_missing_model_name_fails(self):
        """Tests that missing model_name in batch_inference fails validation."""
        ai_action = Action(
            ai=AIAction(
                name="batch-prediction",
                agent_platform=AgentPlatform(
                    batch_inference=AgentPlatformBatchInference(
                        job_display_name="my-batch-job",
                        model_name="",
                    )
                ),
            )
        )
        self.pipeline.actions.append(ai_action)
        with self.assertRaisesRegex(
            ValueError,
            "Error for field 'actions\\[1\\]\\.ai\\.agent_platform\\.batch_inference\\.model_name': "
            "field is required and cannot be an empty string.",
        ):
            PipelineValidator.validate(self.pipeline)

    def test_ai_action_invalid_name_fails(self):
        """Tests that invalid name in AIAction fails validation."""
        ai_action = Action(
            ai=AIAction(
                name="invalid name with spaces",
                agent_platform=AgentPlatform(
                    model_upload=AgentPlatformModelUpload(
                        model_name="my-model",
                        model_artifact_uri="gs://bucket/model",
                        serving_container_image_uri="gcr.io/test/image:latest",
                    )
                ),
            )
        )
        self.pipeline.actions.append(ai_action)

        with self.assertRaises(ValueError) as cm:
            PipelineValidator.validate(self.pipeline)

        self.assertIn(
            "Error for field 'actions[1].ai.name': value 'invalid name with spaces' does not match regex pattern",
            str(cm.exception),
        )

    def test_defaults_valid_retry_policy(self):
        """Tests that a valid retry policy in defaults passes validation."""
        self.pipeline.defaults.retry_policy.CopyFrom(
            RetryPolicy(
                max_retries=3,
                fixed_delay=FixedDelayStrategy(retry_delay="2m"),
            )
        )
        PipelineValidator.validate(self.pipeline)

    def test_defaults_retry_policy_negative_max_retries_fails(self):
        """Tests that negative max_retries in defaults retry policy fails validation."""
        self.pipeline.defaults.retry_policy.CopyFrom(
            RetryPolicy(
                max_retries=-1,
                fixed_delay=FixedDelayStrategy(retry_delay="2m"),
            )
        )
        with self.assertRaisesRegex(
            ValueError,
            "Error for field 'defaults\\.retry_policy\\.max_retries': value -1 must be at least 0\\.0\\.",
        ):
            PipelineValidator.validate(self.pipeline)

    def test_defaults_retry_policy_invalid_duration_fails(self):
        """Tests that an invalid retry_delay duration in defaults fails validation."""
        self.pipeline.defaults.retry_policy.CopyFrom(
            RetryPolicy(
                max_retries=2,
                fixed_delay=FixedDelayStrategy(retry_delay="not-a-duration"),
            )
        )
        with self.assertRaisesRegex(
            ValueError,
            "Error for field 'defaults\\.retry_policy\\.fixed_delay\\.retry_delay': Invalid duration format",
        ):
            PipelineValidator.validate(self.pipeline)

    def test_action_valid_retry_policy(self):
        """Tests that a valid retry policy on an action passes validation."""
        self.pipeline.actions[0].python.retry_policy.CopyFrom(
            RetryPolicy(
                max_retries=5,
                fixed_delay=FixedDelayStrategy(retry_delay="30s"),
            )
        )
        PipelineValidator.validate(self.pipeline)

    def test_action_retry_policy_negative_max_retries_fails(self):
        """Tests that negative max_retries on an action retry policy fails validation."""
        self.pipeline.actions[0].python.retry_policy.CopyFrom(
            RetryPolicy(
                max_retries=-2,
                fixed_delay=FixedDelayStrategy(retry_delay="10s"),
            )
        )
        with self.assertRaisesRegex(
            ValueError,
            "Error for field 'actions\\[0\\]\\.python\\.retry_policy\\.max_retries': value -2 must be at least 0\\.0\\.",
        ):
            PipelineValidator.validate(self.pipeline)

    def test_defaults_retry_policy_missing_max_retries_fails(self):
        """Tests that omitting max_retries in RetryPolicy fails validation."""
        self.pipeline.defaults.retry_policy.CopyFrom(
            RetryPolicy(
                fixed_delay=FixedDelayStrategy(retry_delay="2m"),
            )
        )
        with self.assertRaisesRegex(
            ValueError,
            "Error for field 'defaults\\.retry_policy\\.max_retries': field is required\\.",
        ):
            PipelineValidator.validate(self.pipeline)

    def test_defaults_retry_policy_zero_max_retries_passes(self):
        """Tests that explicitly setting max_retries=0 passes validation."""
        self.pipeline.defaults.retry_policy.CopyFrom(
            RetryPolicy(max_retries=0)
        )
        PipelineValidator.validate(self.pipeline)

    def test_defaults_retry_policy_missing_retry_delay_fails(self):
        """Tests that omitting retry_delay in FixedDelayStrategy fails validation."""
        self.pipeline.defaults.retry_policy.CopyFrom(
            RetryPolicy(
                max_retries=3,
                fixed_delay=FixedDelayStrategy(),
            )
        )
        with self.assertRaisesRegex(
            ValueError,
            "Error for field 'defaults\\.retry_policy\\.fixed_delay\\.retry_delay': field is required and cannot be an empty string\\.",
        ):
            PipelineValidator.validate(self.pipeline)

    def test_defaults_retry_policy_zero_duration_fails(self):
        """Tests that a zero retry_delay duration ('0s') fails validation."""
        self.pipeline.defaults.retry_policy.CopyFrom(
            RetryPolicy(
                max_retries=2,
                fixed_delay=FixedDelayStrategy(retry_delay="0s"),
            )
        )
        with self.assertRaisesRegex(
            ValueError,
            "Error for field 'defaults\\.retry_policy\\.fixed_delay\\.retry_delay': Invalid duration: '0s'\\. Duration must be strictly positive",
        ):
            PipelineValidator.validate(self.pipeline)

    def test_defaults_deprecated_execution_config_emits_warning(self):
        """Tests that using deprecated execution_config emits a non-blocking DeprecationWarning."""
        import warnings

        self.pipeline.defaults.execution_config.retries = 2
        with warnings.catch_warnings(record=True) as recorded:
            warnings.simplefilter("always", DeprecationWarning)
            PipelineValidator.validate(self.pipeline)

        deprecation_messages = [
            str(w.message)
            for w in recorded
            if issubclass(w.category, DeprecationWarning)
        ]
        self.assertTrue(
            any(
                "defaults.execution_config" in msg and "deprecated" in msg
                for msg in deprecation_messages
            )
        )

    def test_action_valid_exponential_backoff(self):
        """Tests that a valid exponential backoff retry policy passes validation."""
        self.pipeline.actions[0].python.retry_policy.CopyFrom(
            RetryPolicy(
                max_retries=4,
                exponential_backoff=ExponentialBackoffStrategy(
                    initial_delay="15s",
                    max_delay="10m",
                    multiplier=2.0,
                    randomize_jitter=True,
                ),
            )
        )
        PipelineValidator.validate(self.pipeline)

    def test_action_exponential_backoff_missing_initial_delay_fails(self):
        """Tests that missing initial_delay in exponential backoff fails validation."""
        self.pipeline.actions[0].python.retry_policy.CopyFrom(
            RetryPolicy(
                max_retries=4,
                exponential_backoff=ExponentialBackoffStrategy(
                    max_delay="10m",
                ),
            )
        )
        with self.assertRaisesRegex(
            ValueError,
            "Error for field 'actions\\[0\\]\\.python\\.retry_policy\\.exponential_backoff\\.initial_delay': field is required",
        ):
            PipelineValidator.validate(self.pipeline)

    def test_action_exponential_backoff_invalid_duration_fails(self):
        """Tests that invalid initial_delay in exponential backoff fails validation."""
        self.pipeline.actions[0].python.retry_policy.CopyFrom(
            RetryPolicy(
                max_retries=4,
                exponential_backoff=ExponentialBackoffStrategy(
                    initial_delay="invalid",
                ),
            )
        )
        with self.assertRaisesRegex(
            ValueError,
            "Error for field 'actions\\[0\\]\\.python\\.retry_policy\\.exponential_backoff\\.initial_delay': Invalid duration format",
        ):
            PipelineValidator.validate(self.pipeline)

    def test_dbt_invocation_valid_commands_succeed(self):
        """Tests that all supported DBT invocation commands pass validation."""
        from orchestration_pipelines_models.pipeline_v1_model.protos.orchestration_pipeline_pb2 import (
            DbtAirflowExecution,
            DbtFrameworkSpec,
            DbtInvocation,
            PipelineAction,
            PipelineFramework,
        )

        valid_commands = [
            "build",
            "run",
            "test",
            "snapshot",
            "freshness",
            "seed",
            "compile",
        ]
        self.pipeline.actions[0].CopyFrom(
            Action(
                pipeline=PipelineAction(
                    name="dbt-valid",
                    framework=PipelineFramework(
                        dbt=DbtFrameworkSpec(
                            airflow_worker=DbtAirflowExecution(
                                project_directory_path="dbt_project"
                            ),
                            invocations=[
                                DbtInvocation(command=cmd)
                                for cmd in valid_commands
                            ],
                        )
                    ),
                )
            )
        )
        PipelineValidator.validate(self.pipeline)

    def test_dbt_invocation_invalid_command_fails(self):
        """Tests that an unsupported DBT invocation command fails validation."""
        from orchestration_pipelines_models.pipeline_v1_model.protos.orchestration_pipeline_pb2 import (
            DbtAirflowExecution,
            DbtFrameworkSpec,
            DbtInvocation,
            PipelineAction,
            PipelineFramework,
        )

        self.pipeline.actions[0].CopyFrom(
            Action(
                pipeline=PipelineAction(
                    name="dbt-invalid-cmd",
                    framework=PipelineFramework(
                        dbt=DbtFrameworkSpec(
                            airflow_worker=DbtAirflowExecution(
                                project_directory_path="dbt_project"
                            ),
                            invocations=[DbtInvocation(command="unsupported")],
                        )
                    ),
                )
            )
        )
        with self.assertRaisesRegex(
            ValueError,
            r"Error for field 'actions\[0\]\.pipeline\.framework\.dbt\.invocations\[0\]\.command': value 'unsupported' does not match regex pattern",
        ):
            PipelineValidator.validate(self.pipeline)

    def test_dbt_invocation_negative_threads_fails(self):
        """Tests that negative threads on DbtInvocation fails validation."""
        from orchestration_pipelines_models.pipeline_v1_model.protos.orchestration_pipeline_pb2 import (
            DbtAirflowExecution,
            DbtFrameworkSpec,
            DbtInvocation,
            PipelineAction,
            PipelineFramework,
        )

        self.pipeline.actions[0].CopyFrom(
            Action(
                pipeline=PipelineAction(
                    name="dbt-negative-threads",
                    framework=PipelineFramework(
                        dbt=DbtFrameworkSpec(
                            airflow_worker=DbtAirflowExecution(
                                project_directory_path="dbt_project"
                            ),
                            invocations=[
                                DbtInvocation(command="run", threads=-1)
                            ],
                        )
                    ),
                )
            )
        )
        with self.assertRaisesRegex(
            ValueError,
            r"Error for field 'actions\[0\]\.pipeline\.framework\.dbt\.invocations\[0\]\.threads': value -1 must be at least 0",
        ):
            PipelineValidator.validate(self.pipeline)

    def test_dbt_deprecated_select_models_and_tags_emit_warning(self):
        """Tests that using deprecated select_models and tags emits DeprecationWarning."""
        import warnings

        from orchestration_pipelines_models.pipeline_v1_model.protos.orchestration_pipeline_pb2 import (
            DbtAirflowExecution,
            DbtFrameworkSpec,
            PipelineAction,
            PipelineFramework,
        )

        self.pipeline.actions[0].CopyFrom(
            Action(
                pipeline=PipelineAction(
                    name="dbt-legacy",
                    framework=PipelineFramework(
                        dbt=DbtFrameworkSpec(
                            airflow_worker=DbtAirflowExecution(
                                project_directory_path="dbt_project",
                                select_models=["model_1"],
                                tags=["tag_1"],
                            )
                        )
                    ),
                )
            )
        )
        with warnings.catch_warnings(record=True) as recorded:
            warnings.simplefilter("always", DeprecationWarning)
            PipelineValidator.validate(self.pipeline)

        deprecation_messages = [
            str(w.message)
            for w in recorded
            if issubclass(w.category, DeprecationWarning)
        ]
        self.assertTrue(
            any("select_models" in msg for msg in deprecation_messages)
        )
        self.assertTrue(any("tags" in msg for msg in deprecation_messages))


@pytest.fixture
def valid_pipeline_for_custom_job():
    """Provides a baseline valid OrchestrationPipeline for custom job tests."""
    return OrchestrationPipeline(
        model_version="1.0",
        pipeline_id="valid-pipeline-id",
        runner=PipelineRunner.airflow,
        owner="valid-owner",
        defaults=Defaults(
            project_id="test-project",
            location="us-central1",
        ),
        triggers=[
            Trigger(
                schedule=ScheduleTrigger(
                    interval="@daily",
                    start_time="2026-01-01T00:00:00Z",
                    timezone="UTC",
                )
            )
        ],
    )


@pytest.fixture
def custom_job_struct() -> struct_pb2.Struct:
    """Provides a minimal valid CustomJob protobuf Struct."""
    job_struct = struct_pb2.Struct()
    job_struct.update({"displayName": "my-custom-job"})
    return job_struct


def test_valid_ai_action_create_and_run_custom_job_succeeds(
    valid_pipeline_for_custom_job,
    custom_job_struct,
):
    """Tests that valid create_and_run_custom_job passes validation."""
    ai_action = Action(
        ai=AIAction(
            name="custom-job-action",
            agent_platform=AgentPlatform(
                create_and_run_custom_job=AgentPlatformCreateAndRunCustomJob(
                    custom_job=custom_job_struct,
                    execution_timeout="2h",
                )
            ),
        )
    )
    valid_pipeline_for_custom_job.actions.append(ai_action)

    PipelineValidator.validate(valid_pipeline_for_custom_job)


def test_ai_action_create_and_run_custom_job_invalid_execution_timeout_fails(
    valid_pipeline_for_custom_job,
    custom_job_struct,
):
    """Tests invalid execution_timeout in create_and_run_custom_job fails."""
    ai_action = Action(
        ai=AIAction(
            name="custom-job-action",
            agent_platform=AgentPlatform(
                create_and_run_custom_job=AgentPlatformCreateAndRunCustomJob(
                    custom_job=custom_job_struct,
                    execution_timeout="invalid-duration",
                )
            ),
        )
    )
    valid_pipeline_for_custom_job.actions.append(ai_action)

    expected_error = (
        r"Error for field 'actions\[0\]\.ai\.agent_platform"
        r"\.create_and_run_custom_job\.execution_timeout'"
    )
    with pytest.raises(ValueError, match=expected_error):
        PipelineValidator.validate(valid_pipeline_for_custom_job)


def test_ai_action_create_and_run_custom_job_missing_custom_job_fails(
    valid_pipeline_for_custom_job,
):
    """Tests that missing custom_job in create_and_run_custom_job fails."""
    ai_action = Action(
        ai=AIAction(
            name="custom-job-action",
            agent_platform=AgentPlatform(
                create_and_run_custom_job=AgentPlatformCreateAndRunCustomJob(
                    execution_timeout="2h",
                )
            ),
        )
    )
    valid_pipeline_for_custom_job.actions.append(ai_action)
    expected_error = (
        r"Error for field 'actions\[0\]\.ai\.agent_platform"
        r"\.create_and_run_custom_job\.custom_job': field is required\."
    )
    with pytest.raises(ValueError, match=expected_error):
        PipelineValidator.validate(valid_pipeline_for_custom_job)


@pytest.fixture
def base_pipeline():
    """Provides a minimal valid OrchestrationPipeline with two actions."""
    return OrchestrationPipeline(
        model_version="1.0",
        pipeline_id="valid-pipeline-id",
        runner=PipelineRunner.airflow,
        owner="valid-owner",
        parameters={
            "env": Parameter(
                type=Parameter.Type.string,
                default_value=struct_pb2.Value(string_value="prod"),
            )
        },
        defaults=Defaults(project_id="test-project", location="us-central1"),
        actions=[
            Action(
                python=PythonAction(
                    name="step_1",
                    main_file_path="step1.py",
                    python_callable="main",
                    engine=PythonEngine(local=LocalEngine()),
                )
            ),
            Action(
                python=PythonAction(
                    name="step_2",
                    depends_on=["step_1"],
                    main_file_path="step2.py",
                    python_callable="main",
                    engine=PythonEngine(local=LocalEngine()),
                )
            ),
        ],
    )


@pytest.mark.parametrize(
    ("param_name", "param_type", "default_value", "error_pattern"),
    [
        ("invalid-name", Parameter.Type.string, struct_pb2.Value(string_value="v"), r"Error for field 'parameters\['invalid-name'\]': invalid parameter name"),
        ("param", Parameter.Type.type_undefined, struct_pb2.Value(string_value="v"), r"Error for field 'parameters\['param'\]\.type': must not be the default enum value"),
        ("param", Parameter.Type.string, None, r"Error for field 'parameters\['param'\]\.default_value': field is required"),
        ("param", Parameter.Type.string, struct_pb2.Value(null_value=struct_pb2.NULL_VALUE), r"Error for field 'parameters\['param'\]\.default_value': non-null default value is required"),
        ("param", Parameter.Type.number, struct_pb2.Value(string_value="not-a-number"), r"default value kind 'string_value' does not match parameter type 'number'"),
        (
            "param",
            Parameter.Type.array,
            struct_pb2.Value(list_value=struct_pb2.ListValue(values=[struct_pb2.Value(null_value=struct_pb2.NULL_VALUE)])),
            r"array parameter defaults must contain only non-null primitive values",
        ),
    ],
)
def test_parameter_definitions_invalid(
    base_pipeline, param_name, param_type, default_value, error_pattern
):
    base_pipeline.parameters[param_name].CopyFrom(
        Parameter(type=param_type, default_value=default_value)
    )

    with pytest.raises(ValueError, match=error_pattern):
        PipelineValidator.validate(base_pipeline)


@pytest.mark.parametrize(
    ("root_bindings", "action_bindings", "error_pattern"),
    [
        ({"pipeline_id": "parameters.env"}, {}, "Error for field 'pipeline_id': field is not parameterizable"),
        ({"actions[0].python.execution_timeout": "parameters.env"}, {}, "action-level binding .* must be defined in the action's own 'dynamic_bindings'"),
        ({"triggers[0]": "parameters.env"}, {}, "Error for field 'triggers': field is not parameterizable"),
        ({}, {"main_file_path": "parameters.env"}, r"Error for field 'actions\[0\]\.python\.main_file_path': field is not parameterizable"),
        ({}, {"engine": "parameters.env"}, r"Error for field 'actions\[0\]\.python\.engine': field is not parameterizable"),
        ({}, {"labels[0]": "parameters.env"}, r"Error for field 'actions\[0\]\.python\.dynamic_bindings': invalid field 'labels\[0\]'"),
        ({"notifications.on_pipeline_failure.email[0]": "parameters.env"}, {}, r"Error for field 'notifications\.on_pipeline_failure\.email': individual list elements cannot be parameterized"),
        ({}, {"execution_timeout.sub": "parameters.env"}, r"Error for field 'actions\[0\]\.python\.execution_timeout': primitive and repeated fields must be parameterized as a whole property"),
        ({}, {"unknown_field": "parameters.env"}, r"Error for field 'actions\[0\]\.python\.dynamic_bindings': invalid field 'unknown_field'"),
    ],
)
def test_dynamic_bindings_invalid_targets(
    base_pipeline, root_bindings, action_bindings, error_pattern
):
    base_pipeline.dynamic_bindings.update(root_bindings)
    base_pipeline.actions[0].python.dynamic_bindings.update(action_bindings)

    with pytest.raises(ValueError, match=error_pattern):
        PipelineValidator.validate(base_pipeline)


@pytest.mark.parametrize(
    ("overrides", "error_pattern"),
    [
        ({"root_bindings": {"defaults.project_id": "outputs.step_1.value.proj"}}, r"pipeline-level dynamic bindings cannot reference 'outputs\.\*'"),
        ({"root_bindings": {"defaults.project_id": "run.unknown"}}, "Invalid or unknown reference 'run.unknown'"),
        ({"root_bindings": {"defaults.project_id": "parameters.missing"}}, "references undefined parameter 'missing'"),
        ({"step2_bindings": {"execution_timeout": "outputs.non_existent.value.t"}}, "references undefined action 'non_existent'"),
        ({"step2_depends_on": [], "step2_bindings": {"execution_timeout": "outputs.step_1.value.t"}}, "references outputs of action 'step_1', which is not in 'depends_on'"),
        ({"step1_run_if": "parameters.env == 'prod'", "step2_bindings": {"execution_timeout": "outputs.step_1.value.t"}}, r"Upstream action 'step_1' may be skipped; guard with 'has\(outputs\.step_1\.\.\.\)'"),
        ({"step1_trigger_rule": TriggerRule.all_done, "step2_run_if": "outputs.step_1.value.ok"}, r"Upstream action 'step_1' may be skipped; guard with 'has\(outputs\.step_1\.\.\.\)'"),
    ],
)
def test_dynamic_bindings_and_run_if_expression_errors(
    base_pipeline, overrides, error_pattern
):
    base_pipeline.dynamic_bindings.update(overrides.get("root_bindings", {}))
    step1, step2 = base_pipeline.actions[0].python, base_pipeline.actions[1].python
    step1.run_if = overrides.get("step1_run_if", "")
    step1.trigger_rule = overrides.get("step1_trigger_rule", TriggerRule.trigger_rule_undefined)
    if "step2_depends_on" in overrides:
        del step2.depends_on[:]
        step2.depends_on.extend(overrides["step2_depends_on"])
    step2.dynamic_bindings.update(overrides.get("step2_bindings", {}))
    step2.run_if = overrides.get("step2_run_if", "")

    with pytest.raises(ValueError, match=error_pattern):
        PipelineValidator.validate(base_pipeline)


def test_dynamic_bindings_bypass_format_checks_and_allow_has_guard(base_pipeline):
    base_pipeline.defaults.project_id = ""
    base_pipeline.dynamic_bindings["defaults.project_id"] = "parameters.env"
    base_pipeline.actions[0].python.run_if = "parameters.env == 'prod'"
    base_pipeline.actions.append(
        Action(
            sql=SqlAction(
                name="step_3",
                depends_on=["step_1"],
                execution_timeout="",
                params={"dynamic_key": "", "static_key": "valid_val"},
                engine=SqlEngine(bigquery=BigQueryEngine()),
                query=Query(inline="SELECT 1"),
                dynamic_bindings={
                    "execution_timeout": "has(outputs.step_1.value) ? outputs.step_1.value.t : '10m'",
                    "params.dynamic_key": "run.trigger_date",
                },
            )
        )
    )

    PipelineValidator.validate(base_pipeline)


if __name__ == "__main__":
    unittest.main()
