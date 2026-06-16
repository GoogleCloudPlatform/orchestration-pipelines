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

from orchestration_pipelines_models.pipeline_v1_model.pipeline_validation import (
    PipelineValidator,
)
from orchestration_pipelines_models.pipeline_v1_model.protos.orchestration_pipeline_pb2 import (
    Action,
    Defaults,
    ExecutionConfig,
    LocalEngine,
    OrchestrationPipeline,
    PipelineRunner,
    PysparkAction,
    PythonAction,
    PythonEngine,
    Query,
    ScheduleTrigger,
    SqlAction,
    SqlEngine,
    Trigger,
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


if __name__ == "__main__":
    unittest.main()
