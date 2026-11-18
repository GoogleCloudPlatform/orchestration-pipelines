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
"""Tests for runner and version compatibility proto options."""

import re

import pytest
from google.protobuf import descriptor_pb2
from google.protobuf.descriptor import (
    Descriptor,
    EnumDescriptor,
    EnumValueDescriptor,
    FieldDescriptor,
    FileDescriptor,
)

from orchestration_pipelines_models.pipeline_v1_model.protos import (
    orchestration_pipeline_pb2,
    validation_pb2,
)

VERSION_REGEX = re.compile(
    r"^(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)(?:\.(?:0|[1-9]\d*))?$"
)
SupportRules = tuple[tuple[str, str], ...]
CollectedRules = dict[str, SupportRules]


def _get_supported_on(field: FieldDescriptor) -> SupportRules:
    """Returns supported_on entries for a field as (runner, min_version) tuples."""
    entries = field.GetOptions().Extensions[validation_pb2.supported_on]
    return tuple((entry.runner, entry.min_version) for entry in entries)


def _get_enum_value_supported_on(
    enum_value: EnumValueDescriptor,
) -> SupportRules:
    """Returns enum_value_supported_on entries as (runner, min_version) tuples."""
    entries = enum_value.GetOptions().Extensions[
        validation_pb2.enum_value_supported_on
    ]
    return tuple((entry.runner, entry.min_version) for entry in entries)


def _collect_all_supported_on(
    descriptor: Descriptor, prefix: str | None = None
) -> CollectedRules:
    """Recursively collects all 'Message.field' paths with supported_on entries."""
    result: CollectedRules = {}
    msg_path = f"{prefix}.{descriptor.name}" if prefix else descriptor.name

    for field in descriptor.fields:
        entries = _get_supported_on(field)
        if entries:
            result[f"{msg_path}.{field.name}"] = entries

    for nested in descriptor.nested_types:
        if not nested.GetOptions().map_entry:
            result.update(_collect_all_supported_on(nested, msg_path))

    return result


def _collect_message_enums(
    descriptor: Descriptor,
) -> list[EnumDescriptor]:
    """Recursively collects enum descriptors nested inside a message."""
    enums = list(descriptor.enum_types)
    for nested in descriptor.nested_types:
        if not nested.GetOptions().map_entry:
            enums.extend(_collect_message_enums(nested))
    return enums


def _collect_all_enums(
    file_descriptor: FileDescriptor,
) -> list[EnumDescriptor]:
    """Collects all top-level and nested enum descriptors in a proto file."""
    enums = list(file_descriptor.enum_types_by_name.values())
    for msg_descriptor in file_descriptor.message_types_by_name.values():
        enums.extend(_collect_message_enums(msg_descriptor))
    return enums


def _collect_all_enum_supported_on(
    file_descriptor: FileDescriptor,
) -> CollectedRules:
    """Collects all enum value paths with enum_value_supported_on entries."""
    result: CollectedRules = {}
    for enum_descriptor in _collect_all_enums(file_descriptor):
        for value in enum_descriptor.values:
            entries = _get_enum_value_supported_on(value)
            if entries:
                result[f"{enum_descriptor.full_name}.{value.name}"] = entries
    return result


@pytest.mark.parametrize(
    ("field_name", "expected_number"),
    [
        ("runner", 1),
        ("min_version", 2),
    ],
)
def test_runner_support_message_fields(
    field_name: str, expected_number: int
) -> None:
    """Verifies RunnerSupport defines expected string fields."""
    msg_descriptor = validation_pb2.RunnerSupport.DESCRIPTOR

    field = msg_descriptor.fields_by_name[field_name]

    assert field.number == expected_number
    assert field.type == FieldDescriptor.TYPE_STRING


@pytest.mark.parametrize(
    ("extension_name", "expected_number", "expected_containing_type"),
    [
        ("supported_on", 50014, "google.protobuf.FieldOptions"),
        ("enum_value_supported_on", 60001, "google.protobuf.EnumValueOptions"),
    ],
)
def test_compatibility_extensions_registered(
    extension_name: str,
    expected_number: int,
    expected_containing_type: str,
) -> None:
    """Verifies compatibility extension definitions on descriptor options."""
    expected_message_type = validation_pb2.RunnerSupport.DESCRIPTOR

    extension: FieldDescriptor = getattr(validation_pb2, extension_name)

    assert extension.number == expected_number
    assert extension.is_repeated
    assert extension.containing_type is not None
    assert extension.containing_type.full_name == expected_containing_type
    assert extension.message_type == expected_message_type


def test_enum_value_supported_on_round_trip() -> None:
    """Verifies enum_value_supported_on serializes and parses on EnumValueOptions."""
    options = descriptor_pb2.EnumValueOptions()
    options.Extensions[validation_pb2.enum_value_supported_on].add(
        runner="airflow", min_version="3.0"
    )

    parsed = descriptor_pb2.EnumValueOptions.FromString(
        options.SerializeToString()
    )
    entries = parsed.Extensions[validation_pb2.enum_value_supported_on]

    assert [(entry.runner, entry.min_version) for entry in entries] == [
        ("airflow", "3.0")
    ]


def test_all_top_level_actions_declare_supported_on() -> None:
    """Ensures every Action.action oneof branch declares supported_on."""
    action_fields = orchestration_pipeline_pb2.Action.DESCRIPTOR.oneofs_by_name[
        "action"
    ].fields

    missing = [
        field.name for field in action_fields if not _get_supported_on(field)
    ]

    assert action_fields
    assert missing == []


def test_all_triggers_declare_supported_on() -> None:
    """Ensures every Trigger.trigger oneof branch declares supported_on."""
    trigger_fields = (
        orchestration_pipeline_pb2.Trigger.DESCRIPTOR.oneofs_by_name[
            "trigger"
        ].fields
    )

    missing = [
        field.name for field in trigger_fields if not _get_supported_on(field)
    ]

    assert trigger_fields
    assert missing == []


def test_all_engines_declare_supported_on() -> None:
    """Ensures every engine oneof branch across all engine messages declares supported_on."""
    action_fields = orchestration_pipeline_pb2.Action.DESCRIPTOR.oneofs_by_name[
        "action"
    ].fields
    engine_messages_by_name: dict[str, Descriptor] = {}
    for action_field in action_fields:
        action_msg = action_field.message_type
        if action_msg is not None and "engine" in action_msg.fields_by_name:
            engine_field = action_msg.fields_by_name["engine"]
            if engine_field.message_type is not None:
                engine_messages_by_name[engine_field.message_type.full_name] = (
                    engine_field.message_type
                )
    for (
        msg_descriptor
    ) in orchestration_pipeline_pb2.DESCRIPTOR.message_types_by_name.values():
        if "engine" in msg_descriptor.oneofs_by_name:
            engine_messages_by_name.setdefault(
                msg_descriptor.full_name, msg_descriptor
            )
    engine_messages = list(engine_messages_by_name.values())

    missing_engine_oneof = [
        msg.name
        for msg in engine_messages
        if "engine" not in msg.oneofs_by_name
    ]
    missing_supported_on = [
        f"{msg.name}.{field.name}"
        for msg in engine_messages
        if "engine" in msg.oneofs_by_name
        for field in msg.oneofs_by_name["engine"].fields
        if not _get_supported_on(field)
    ]

    assert engine_messages
    assert missing_engine_oneof == []
    assert missing_supported_on == []


def test_all_framework_execution_branches_declare_supported_on() -> None:
    """Ensures every framework execution oneof branch declares supported_on."""
    framework_fields = (
        orchestration_pipeline_pb2.PipelineFramework.DESCRIPTOR.oneofs_by_name[
            "framework"
        ].fields
    )
    framework_messages = [
        field.message_type
        for field in framework_fields
        if field.message_type is not None
    ]

    missing_execution_oneof = [
        msg.name
        for msg in framework_messages
        if "execution" not in msg.oneofs_by_name
    ]
    execution_fields = [
        (msg.name, exec_field)
        for msg in framework_messages
        if "execution" in msg.oneofs_by_name
        for exec_field in msg.oneofs_by_name["execution"].fields
    ]
    missing_supported_on = [
        f"{msg_name}.{exec_field.name}"
        for msg_name, exec_field in execution_fields
        if not _get_supported_on(exec_field)
    ]

    assert len(framework_messages) == len(framework_fields)
    assert missing_execution_oneof == []
    assert execution_fields
    assert missing_supported_on == []


def test_supported_on_entries_use_announced_runners_and_valid_versions() -> (
    None
):
    """Ensures compatibility annotations use valid runners, versions, and no duplicates."""
    file_descriptor = orchestration_pipeline_pb2.DESCRIPTOR
    valid_runners = {
        name
        for name, number in orchestration_pipeline_pb2.PipelineRunner.items()
        if number != 0
    }
    all_annotations: CollectedRules = {}
    for msg_descriptor in file_descriptor.message_types_by_name.values():
        all_annotations.update(_collect_all_supported_on(msg_descriptor))
    all_annotations.update(_collect_all_enum_supported_on(file_descriptor))

    invalid_runners = [
        (path, runner)
        for path, entries in all_annotations.items()
        for runner, _ in entries
        if runner not in valid_runners
    ]
    invalid_versions = [
        (path, min_version)
        for path, entries in all_annotations.items()
        for _, min_version in entries
        if min_version and not VERSION_REGEX.match(min_version)
    ]
    duplicate_runners = [
        path
        for path, entries in all_annotations.items()
        if len({runner for runner, _ in entries}) != len(entries)
    ]

    assert all_annotations
    assert invalid_runners == []
    assert invalid_versions == []
    assert duplicate_runners == []
