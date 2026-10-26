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
"""Validation logic for v1 orchestration pipeline models."""

import re
import warnings
from collections.abc import Collection, MutableMapping
from datetime import datetime
from graphlib import CycleError, TopologicalSorter

from google.protobuf.descriptor import Descriptor, FieldDescriptor
from google.protobuf.message import Message

# This assumes that the compiled protobuf files (e.g., validation_pb2.py)
# are available in your Python path. You might need to adjust the import
# path based on your project structure.
from orchestration_pipelines_models.pipeline_v1_model.protos import (
    orchestration_pipeline_pb2,
    validation_pb2,
)
from orchestration_pipelines_models.utils import cel_utils, time_utils

_INDEXED_SEGMENT_RE = re.compile(r"^([a-zA-Z0-9_]+)\[(\d+)\]$")
_PARAM_NAME_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
_PARAM_TYPE_TO_VALUE_KIND = {
    orchestration_pipeline_pb2.Parameter.Type.string: "string_value",
    orchestration_pipeline_pb2.Parameter.Type.number: "number_value",
    orchestration_pipeline_pb2.Parameter.Type.boolean: "bool_value",
    orchestration_pipeline_pb2.Parameter.Type.array: "list_value",
}
_PRIMITIVE_VALUE_KINDS = frozenset(
    {"string_value", "number_value", "bool_value"}
)


class PipelineValidator:
    """Performs validation on pipeline protobuf messages based on custom field
    options defined in validation.proto.
    """

    @classmethod
    def validate(cls, message: Message):
        """Validates a protobuf message.

        This method performs two stages of validation:
        1.  Recursively validates all fields and nested messages against the
            custom validation options defined in the .proto files.
        2.  Performs pipeline-level validation for cross-field consistency,
            such as checking for unique action names and valid dependencies.

        Args:
            message: The protobuf message instance to validate.

        Raises:
            ValueError: If any validation rule is violated.
        """
        is_pipeline = (
            message.DESCRIPTOR
            == orchestration_pipeline_pb2.OrchestrationPipeline.DESCRIPTOR
        )
        validated_bindings = (
            cls._collect_validated_bindings(message) if is_pipeline else {}
        )
        cls._validate_message(message, "", validated_bindings)

        if is_pipeline:
            cls._validate_pipeline_level_rules(message, validated_bindings)

    @classmethod
    def _validate_message(
        cls,
        message: Message,
        path_prefix: str,
        bound_paths: Collection[str] = (),
    ):
        """Helper method to recursively validate a message."""
        descriptor = message.DESCRIPTOR

        # Do not validate Struct fields, as they can contain arbitrary data that
        # doesn't conform to the validation schema.
        if descriptor.full_name in (
            "google.protobuf.Struct",
            "google.protobuf.Value",
        ):
            return

        # 1. Loop over all defined fields to check for presence, count, and
        # default-value-based validations. These checks need to run even if
        # the field has its default value (e.g., 0, empty list) and thus
        # wouldn't be in `ListFields()`.
        for field in descriptor.fields:
            current_field_path = (
                f"{path_prefix}.{field.name}" if path_prefix else field.name
            )
            if current_field_path in bound_paths:
                continue
            options = field.GetOptions()
            if (
                options.HasExtension(validation_pb2.is_required)
                and options.Extensions[validation_pb2.is_required]
            ):
                cls._validate_is_required(
                    message, field, current_field_path, bound_paths
                )

            value = getattr(message, field.name)
            if options.HasExtension(validation_pb2.min_items):
                cls._validate_min_items(
                    field,
                    value,
                    options.Extensions[validation_pb2.min_items],
                    current_field_path,
                )
            if (
                options.HasExtension(validation_pb2.disallow_zero_enum)
                and options.Extensions[validation_pb2.disallow_zero_enum]
            ):
                cls._validate_disallow_zero_enum(
                    field, value, current_field_path
                )
            if options.HasExtension(validation_pb2.min_value):
                cls._validate_min_value(
                    field,
                    value,
                    options.Extensions[validation_pb2.min_value],
                    current_field_path,
                )
            if options.HasExtension(validation_pb2.min_len):
                cls._validate_min_len(
                    field,
                    value,
                    options.Extensions[validation_pb2.min_len],
                    current_field_path,
                )

        # 2. Validate the values of all fields that are actually set
        # (non-default).
        for field, value in message.ListFields():
            current_field_path = (
                f"{path_prefix}.{field.name}" if path_prefix else field.name
            )
            if current_field_path not in bound_paths:
                cls._validate_field_value(
                    field, value, current_field_path, bound_paths
                )

            # 3. Recurse into nested messages.
            if field.type == FieldDescriptor.TYPE_MESSAGE:
                if cls.is_field_repeated(field):
                    if cls.is_map_field(field):
                        val_field = field.message_type.fields_by_name["value"]
                        if val_field.type == FieldDescriptor.TYPE_MESSAGE:
                            for k, v in value.items():
                                cls._validate_message(
                                    v,
                                    f"{current_field_path}['{k}']",
                                    bound_paths,
                                )
                        continue
                    for i, item in enumerate(value):
                        cls._validate_message(
                            item, f"{current_field_path}[{i}]", bound_paths
                        )
                else:
                    cls._validate_message(
                        value, current_field_path, bound_paths
                    )

    @staticmethod
    def find_field(descriptor: Descriptor, name: str) -> FieldDescriptor | None:
        """Looks up a Protobuf field by snake_case or camelCase name."""
        return descriptor.fields_by_name.get(
            name
        ) or descriptor.fields_by_camelcase_name.get(name)

    @staticmethod
    def is_field_repeated(field: FieldDescriptor) -> bool:
        """Checks if field is repeated across different protobuf backends."""
        try:
            return field.cardinality == FieldDescriptor.CARDINALITY_REPEATED
        except AttributeError:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", DeprecationWarning)
                return field.label == FieldDescriptor.LABEL_REPEATED

    @staticmethod
    def is_map_field(field: FieldDescriptor) -> bool:
        """Checks if the FieldDescriptor represents a Protobuf map field."""
        return (
            field.cpp_type == FieldDescriptor.CPPTYPE_MESSAGE
            and isinstance(field.message_type, Descriptor)
            and field.message_type.GetOptions().map_entry
        )

    @staticmethod
    def is_struct_field(field: FieldDescriptor) -> bool:
        """Checks if the FieldDescriptor is a google.protobuf.Struct field."""
        return (
            field.type == FieldDescriptor.TYPE_MESSAGE
            and field.message_type.full_name == "google.protobuf.Struct"
        )

    @staticmethod
    def is_field_parameterizable(field: FieldDescriptor) -> bool:
        """Checks whether a field has (is_parameterizable) = true."""
        options = field.GetOptions()
        return bool(
            options.HasExtension(validation_pb2.is_parameterizable)
            and options.Extensions[validation_pb2.is_parameterizable]
        )

    @classmethod
    def _validate_field_value(
        cls,
        field: FieldDescriptor,
        value,
        path: str,
        bound_paths: Collection[str] = (),
    ):
        """Validates a single field's value based on its options."""
        options = field.GetOptions()

        if options.deprecated:
            warnings.warn(
                f"Field '{path}' is deprecated and will be removed in a future "
                "version.",
                DeprecationWarning,
                stacklevel=2,
            )

        # Validations for non-default values.
        # Checks for min_items, min_value, min_len and disallow_zero_enum are
        # handled in _validate_message as they need to run on default values.
        if options.HasExtension(validation_pb2.regex):
            cls._validate_regex(
                field, value, options.Extensions[validation_pb2.regex], path
            )

        if options.HasExtension(validation_pb2.map_key_regex):
            pattern = options.Extensions[validation_pb2.map_key_regex]
            cls._validate_map_key_regex(field, value, pattern, path)

        if options.HasExtension(validation_pb2.map_value_regex):
            pattern = options.Extensions[validation_pb2.map_value_regex]
            cls._validate_map_value_regex(
                field, value, pattern, path, bound_paths
            )

        if options.HasExtension(validation_pb2.max_len):
            cls._validate_max_len(
                field, value, options.Extensions[validation_pb2.max_len], path
            )

        if (
            options.HasExtension(validation_pb2.is_cron_expression)
            and options.Extensions[validation_pb2.is_cron_expression]
        ):
            cls._validate_is_cron_expression(field, value, path)

        if (
            options.HasExtension(validation_pb2.is_iso8601_timestamp)
            and options.Extensions[validation_pb2.is_iso8601_timestamp]
        ):
            cls._validate_is_iso8601_timestamp(field, value, path)

        if (
            options.HasExtension(validation_pb2.is_iso8601_duration)
            and options.Extensions[validation_pb2.is_iso8601_duration]
        ):
            cls._validate_is_iso8601_duration(field, value, path)

        if (
            options.HasExtension(validation_pb2.is_iana_timezone)
            and options.Extensions[validation_pb2.is_iana_timezone]
        ):
            cls._validate_is_iana_timezone(field, value, path)

    @classmethod
    def _validate_is_required(
        cls,
        message: Message,
        field: FieldDescriptor,
        path: str,
        bound_paths: Collection[str] = (),
    ):
        """Validates that a required field is set."""
        if cls.is_field_repeated(field):
            if not getattr(message, field.name):
                raise ValueError(
                    f"Error for field '{path}': field is required and cannot "
                    "be empty."
                )
        elif field.cpp_type == FieldDescriptor.CPPTYPE_STRING:
            if not getattr(message, field.name):
                raise ValueError(
                    f"Error for field '{path}': field is required and cannot "
                    "be an empty string."
                )
        elif field.type == FieldDescriptor.TYPE_MESSAGE or field.has_presence:
            if not message.HasField(field.name) and not any(
                bound_path.startswith(f"{path}.") for bound_path in bound_paths
            ):
                raise ValueError(
                    f"Error for field '{path}': field is required."
                )

    @classmethod
    def _validate_regex(
        cls, field: FieldDescriptor, value, pattern: str, path: str
    ):
        """Validates that a string field matches a regex pattern."""
        if field.cpp_type == FieldDescriptor.CPPTYPE_STRING and pattern:
            cls._validate_string_regex(value, pattern, path)

    @staticmethod
    def _validate_string_regex(value: str, pattern: str, path: str):
        """Validates that a string matches given regex pattern."""
        if not re.match(pattern, value):
            raise ValueError(
                f"Error for field '{path}': value '{value}' does not match "
                f"regex pattern '{pattern}'."
            )

    @classmethod
    def _validate_min_value(
        cls, field: FieldDescriptor, value, min_val: float, path: str
    ):
        """Validates that a numeric field is above a minimum value."""
        if field.cpp_type in (
            FieldDescriptor.CPPTYPE_INT32,
            FieldDescriptor.CPPTYPE_INT64,
            FieldDescriptor.CPPTYPE_UINT32,
            FieldDescriptor.CPPTYPE_UINT64,
            FieldDescriptor.CPPTYPE_DOUBLE,
            FieldDescriptor.CPPTYPE_FLOAT,
        ):
            if value < min_val:
                raise ValueError(
                    f"Error for field '{path}': value {value} must be at "
                    f"least {min_val}."
                )

    @classmethod
    def _validate_min_items(
        cls, field: FieldDescriptor, value, min_i: int, path: str
    ):
        """Validates that a repeated field has a minimum number of items."""
        if cls.is_field_repeated(field):
            if len(value) < min_i:
                raise ValueError(
                    f"Error for field '{path}': must have at least {min_i} "
                    f"items, but has {len(value)}."
                )

    @classmethod
    def _validate_disallow_zero_enum(
        cls, field: FieldDescriptor, value, path: str
    ):
        """Validates that an enum field is not its default zero value."""
        if field.type == FieldDescriptor.TYPE_ENUM:
            if value == 0:
                raise ValueError(
                    f"Error for field '{path}': must not be the default enum "
                    "value (0)."
                )

    @classmethod
    def _validate_min_len(
        cls, field: FieldDescriptor, value, min_l: int, path: str
    ):
        """Validates that a string field has a minimum length."""
        if field.cpp_type == FieldDescriptor.CPPTYPE_STRING:
            if len(value) < min_l:
                raise ValueError(
                    f"Error for field '{path}': length must be at least "
                    f"{min_l}, but is {len(value)}."
                )

    @classmethod
    def _validate_max_len(
        cls, field: FieldDescriptor, value, max_l: int, path: str
    ):
        """Validates that a string field has a maximum length."""
        if field.cpp_type == FieldDescriptor.CPPTYPE_STRING:
            if len(value) > max_l:
                raise ValueError(
                    f"Error for field '{path}': length must be at most "
                    f"{max_l}, but is {len(value)}."
                )

    @classmethod
    def _validate_is_cron_expression(
        cls, field: FieldDescriptor, value, path: str
    ):
        """Validates that a string field is a valid cron expression."""
        if field.cpp_type == FieldDescriptor.CPPTYPE_STRING and value:
            try:
                cls._check_quote_issues(value)
                time_utils.check_cron_expression(value)
            except ValueError as e:
                raise ValueError(f"Error for field '{path}': {e}") from e

    @classmethod
    def _check_quote_issues(cls, value: str):
        """Detects if a value contains quotes, invalid for these fields."""
        if '"' in value or "'" in value:
            # Check if it looks like a quoting error (starts/ends with quote)
            if value.startswith(('"', "'")) or value.endswith(('"', "'")):
                if len(value) < 2 or value[0] != value[-1]:
                    raise ValueError("mismatched quote boundaries.")
                raise ValueError(
                    "value should not be wrapped in literal quotes."
                )

            # General error for quotes anywhere else
            raise ValueError("field contains invalid quote characters.")

    @classmethod
    def _validate_is_iso8601_timestamp(
        cls, field: FieldDescriptor, value, path: str
    ):
        """Validates that a string field is a valid ISO 8601 timestamp."""
        if field.cpp_type == FieldDescriptor.CPPTYPE_STRING and value:
            try:
                cls._check_quote_issues(value)
                # Replacing 'Z' with '+00:00' for broader Python version
                # compatibility with fromisoformat().
                datetime.fromisoformat(value.replace("Z", "+00:00"))
            except ValueError as e:
                raise ValueError(f"Error for field '{path}': {e}") from e

    @classmethod
    def _validate_is_iso8601_duration(
        cls, field: FieldDescriptor, value, path: str
    ):
        """Validates that a string field is a valid duration string."""
        if field.cpp_type == FieldDescriptor.CPPTYPE_STRING and value:
            try:
                cls._check_quote_issues(value)
                time_utils.check_duration(value)
            except (ValueError, TypeError) as e:
                raise ValueError(f"Error for field '{path}': {e}") from e

    @classmethod
    def _validate_is_iana_timezone(
        cls, field: FieldDescriptor, value, path: str
    ):
        """Validates that a string field is a valid IANA timezone."""
        if field.cpp_type == FieldDescriptor.CPPTYPE_STRING and value:
            try:
                cls._check_quote_issues(value)
                time_utils.check_timezone(value)
            except ValueError as e:
                raise ValueError(f"Error for field '{path}': {e}") from e

    @classmethod
    def _collect_validated_bindings(
        cls, pipeline: Message
    ) -> dict[str, tuple[str, Message | None]]:
        """Resolves dynamic_bindings into {canonical_path: (expr, action)}."""
        for raw_path in pipeline.dynamic_bindings:
            if raw_path == "actions" or raw_path.startswith(
                ("actions.", "actions[")
            ):
                raise ValueError(
                    f"Error for field 'dynamic_bindings': action-level "
                    f"binding '{raw_path}' must be defined in the action's "
                    "own 'dynamic_bindings', not at the pipeline root."
                )

        scopes = [
            (pipeline.DESCRIPTOR, pipeline.dynamic_bindings, "", None),
            *(
                (actual.DESCRIPTOR, actual.dynamic_bindings, prefix, actual)
                for _, _, actual, prefix in cls._iter_actions(pipeline)
            ),
        ]
        bindings: dict[str, tuple[str, Message | None]] = {}
        for desc, raw_map, prefix, owner in scopes:
            for raw_path, raw_expr in raw_map.items():
                field, rel_path = cls._resolve_binding_target(
                    desc, raw_path, prefix
                )
                full_path = f"{prefix}.{rel_path}" if prefix else rel_path
                if not cls.is_field_parameterizable(field):
                    raise ValueError(
                        f"Error for field '{full_path}': field is not "
                        "parameterizable."
                    )
                bindings[full_path] = (raw_expr, owner)
        return bindings

    @classmethod
    def _raise_list_element_error(
        cls, field_path: str, field: FieldDescriptor
    ) -> None:
        if not cls.is_field_parameterizable(field):
            raise ValueError(
                f"Error for field '{field_path}': field is not parameterizable."
            )
        raise ValueError(
            f"Error for field '{field_path}': individual list elements cannot "
            "be parameterized; parameterize the entire list with a single "
            "'${ ... }' expression."
        )

    @classmethod
    def _resolve_binding_target(
        cls, root_desc: Descriptor, raw_path: str, error_prefix: str
    ) -> tuple[FieldDescriptor, str]:
        """Resolves a dot-delimited binding path to its FieldDescriptor."""
        prefix = f"{error_prefix}." if error_prefix else ""
        segments = raw_path.split(".")
        curr_desc = root_desc
        canonical: list[str] = []
        for idx, segment in enumerate(segments):
            indexed = _INDEXED_SEGMENT_RE.match(segment)
            name = indexed.group(1) if indexed else segment
            field = cls.find_field(curr_desc, name)
            if field is None or (
                indexed
                and (
                    not cls.is_field_repeated(field) or cls.is_map_field(field)
                )
            ):
                raise ValueError(
                    f"Error for field '{prefix}dynamic_bindings': invalid "
                    f"field '{segment}' in path '{raw_path}'."
                )
            if indexed and (
                field.type != FieldDescriptor.TYPE_MESSAGE
                or idx == len(segments) - 1
            ):
                cls._raise_list_element_error(
                    f"{prefix}{'.'.join([*canonical, field.name])}", field
                )

            canonical.append(
                f"{field.name}[{indexed.group(2)}]" if indexed else field.name
            )
            if not indexed and (
                cls.is_map_field(field) or cls.is_struct_field(field)
            ):
                tail = segments[idx + 1 :]
                if tail and (leaf_idx := _INDEXED_SEGMENT_RE.match(tail[-1])):
                    list_path = ".".join(
                        [*canonical, *tail[:-1], leaf_idx.group(1)]
                    )
                    cls._raise_list_element_error(f"{prefix}{list_path}", field)
                canonical.extend(tail)
                return field, ".".join(canonical)
            if idx < len(segments) - 1:
                if field.type != FieldDescriptor.TYPE_MESSAGE or (
                    cls.is_field_repeated(field) and not indexed
                ):
                    raise ValueError(
                        f"Error for field '{prefix}{'.'.join(canonical)}': "
                        "primitive and repeated fields must be parameterized "
                        "as a whole property."
                    )
                curr_desc = field.message_type

        return field, ".".join(canonical)

    @staticmethod
    def _iter_actions(pipeline: Message):
        """Yields (index, action_type, action_message, path_prefix) tuples."""
        for i, action_wrapper in enumerate(pipeline.actions):
            action_type = action_wrapper.WhichOneof("action")
            if action_type:
                yield (
                    i,
                    action_type,
                    getattr(action_wrapper, action_type),
                    f"actions[{i}].{action_type}",
                )

    @classmethod
    def _validate_pipeline_level_rules(
        cls,
        pipeline: Message,
        validated_bindings: dict[str, tuple[str, Message | None]] | None = None,
    ):
        """Validates action uniqueness and dependencies at pipeline level."""
        cls._validate_parameters(pipeline)

        action_name_map = {}  # Maps action name to its index in the pipeline.
        actions_by_name: dict[str, Message] = {}
        # Stores tuples of (dep_name, action_index, action_type, action_name).
        all_dependencies = []

        for i, action_type, actual_action, prefix in cls._iter_actions(
            pipeline
        ):
            action_name = actual_action.name

            # 1. Check for unique action names.
            if action_name in action_name_map:
                raise ValueError(
                    f"Error for field '{prefix}.name': "
                    f"Duplicate action name '{action_name}' found."
                )
            action_name_map[action_name] = i
            actions_by_name[action_name] = actual_action

            # Collect all dependencies to check after action names are known.
            if hasattr(actual_action, "depends_on"):
                for dep in actual_action.depends_on:
                    all_dependencies.append((dep, i, action_type, action_name))

        # 2. Check for undefined dependencies
        action_names = action_name_map.keys()
        action_to_dependencies = {action: [] for action in action_names}
        for (
            dep_name,
            action_index,
            action_type,
            action_name,
        ) in all_dependencies:
            action_to_dependencies[action_name].append(dep_name)
            if dep_name not in action_names:
                field_name = f"actions[{action_index}].{action_type}.depends_on"
                raise ValueError(
                    f"Error for field '{field_name}': "
                    f"Action '{action_name}' depends on undefined action "
                    f"'{dep_name}'."
                )

        # 3. Check for cycle
        try:
            ts = TopologicalSorter(action_to_dependencies)
            ts.prepare()
        except CycleError as e:
            cycle_path = " -> ".join(e.args[1])
            raise ValueError(
                f"Circular dependency detected: {cycle_path}"
            ) from e

        cls._validate_dynamic_expressions(
            pipeline, actions_by_name, validated_bindings or {}
        )

    @classmethod
    def _validate_parameters(cls, pipeline: Message) -> None:
        """Validates top-level pipeline parameter names and default types."""
        for name, param in pipeline.parameters.items():
            prefix = f"parameters['{name}']"
            if not _PARAM_NAME_RE.match(name):
                raise ValueError(
                    f"Error for field '{prefix}': invalid parameter name "
                    f"'{name}'."
                )
            kind = param.default_value.WhichOneof("kind")
            if kind in (None, "null_value"):
                raise ValueError(
                    f"Error for field '{prefix}.default_value': non-null "
                    "default value is required."
                )
            if kind != _PARAM_TYPE_TO_VALUE_KIND.get(param.type):
                type_name = orchestration_pipeline_pb2.Parameter.Type.Name(
                    param.type
                )
                raise ValueError(
                    f"Error for field '{prefix}.default_value': default value "
                    f"kind '{kind}' does not match parameter type "
                    f"'{type_name}'."
                )
            if param.type == orchestration_pipeline_pb2.Parameter.Type.array:
                if any(
                    item.WhichOneof("kind") not in _PRIMITIVE_VALUE_KINDS
                    for item in param.default_value.list_value.values
                ):
                    raise ValueError(
                        f"Error for field '{prefix}.default_value': array "
                        "parameter defaults must contain only non-null "
                        "primitive values."
                    )
            str_values = (
                param.description,
                param.default_value.string_value,
                *(
                    v.string_value
                    for v in param.default_value.list_value.values
                ),
            )
            if any(cel_utils.has_cel_reference(s) for s in str_values):
                raise ValueError(
                    "Error for field 'parameters': parameter definitions "
                    "cannot contain dynamic '${...}' expressions."
                )

    @classmethod
    def _validate_dynamic_expressions(
        cls,
        pipeline: Message,
        actions_by_name: dict[str, Message],
        validated_bindings: dict[str, tuple[str, Message | None]],
    ) -> None:
        """Validates CEL expressions, reference scopes, and has() guards."""
        defined_params = set(pipeline.parameters.keys())
        entries = [
            *(
                (path, expr, owner)
                for path, (expr, owner) in validated_bindings.items()
            ),
            *(
                (f"{prefix}.run_if", actual.run_if, actual)
                for _, _, actual, prefix in cls._iter_actions(pipeline)
                if actual.run_if
            ),
        ]

        for field_path, raw_expr, owner in entries:
            analysis = cls._analyze_cel(raw_expr, field_path, defined_params)
            if owner is None:
                if analysis.output_actions:
                    raise ValueError(
                        f"Error for field '{field_path}': pipeline-level "
                        "dynamic bindings cannot reference 'outputs.*'."
                    )
                continue

            depends_on = set(getattr(owner, "depends_on", ()))
            for ref_action in sorted(analysis.output_actions):
                if ref_action not in actions_by_name:
                    raise ValueError(
                        f"Error for field '{field_path}': CEL expression "
                        f"references undefined action '{ref_action}'."
                    )
                if ref_action not in depends_on:
                    raise ValueError(
                        f"Error for field '{field_path}': Action "
                        f"'{owner.name}' references outputs of action "
                        f"'{ref_action}', which is not in 'depends_on'."
                    )
                if (
                    cls._is_action_skippable(actions_by_name[ref_action])
                    and ref_action not in analysis.guarded_output_actions
                ):
                    raise ValueError(
                        f"Error for field '{field_path}': Upstream action "
                        f"'{ref_action}' may be skipped; guard with "
                        f"'has(outputs.{ref_action}...)'."
                    )

    @staticmethod
    def _analyze_cel(
        raw_expr: str, field_path: str, defined_params: set[str]
    ) -> cel_utils.CelExpressionAnalysis:
        try:
            analysis = cel_utils.validate_and_analyze_cel(raw_expr)
        except ValueError as e:
            raise ValueError(f"Error for field '{field_path}': {e}") from e
        for param_name in sorted(analysis.parameters):
            if param_name not in defined_params:
                raise ValueError(
                    f"Error for field '{field_path}': CEL expression "
                    f"references undefined parameter '{param_name}'."
                )
        return analysis

    @staticmethod
    def _is_action_skippable(action: Message) -> bool:
        """Checks if an action has run_if or a non-all_success trigger_rule."""
        has_run_if = bool(
            getattr(action, "run_if", "")
            or "run_if" in getattr(action, "dynamic_bindings", {})
        )
        trigger_rule = getattr(
            action,
            "trigger_rule",
            orchestration_pipeline_pb2.TriggerRule.trigger_rule_undefined,
        )
        return has_run_if or trigger_rule not in (
            orchestration_pipeline_pb2.TriggerRule.trigger_rule_undefined,
            orchestration_pipeline_pb2.TriggerRule.all_success,
        )

    @classmethod
    def _validate_map_key_regex(
        cls, field: FieldDescriptor, value, pattern: str, path: str
    ):
        """Validates the keys of a Protobuf map against a regex pattern."""
        if (
            field.message_type.full_name != "google.protobuf.Struct"
            and not cls.is_map_field(field)
        ):
            raise RuntimeError(
                f"Internal Error: Map key regex validation called on field "
                f"'{path}' which is not defined as a Protobuf map."
            )

        if not isinstance(value, MutableMapping):
            raise TypeError(
                f"Field '{path}' expected a map-like object, "
                f"got '{type(value).__name__}'."
            )

        for key in value.keys():
            if not isinstance(key, str):
                raise TypeError(
                    f"Map key at '{path}' expected type 'str', "
                    f"got '{type(key).__name__}'."
                )

            cls._validate_string_regex(key, pattern, f"{path}.<{key}>")

    @classmethod
    def _validate_map_value_regex(
        cls,
        field: FieldDescriptor,
        value,
        pattern: str,
        path: str,
        bound_paths: Collection[str] = (),
    ):
        if not cls.is_map_field(field):
            raise RuntimeError(
                f"Internal Error: Map value regex validation called on field "
                f"'{path}' which is not defined as a Protobuf map."
            )

        if not isinstance(value, MutableMapping):
            raise TypeError(
                f"Field '{path}' expected a map-like object, "
                f"got '{type(value).__name__}'."
            )

        for key, val in value.items():
            if f"{path}.{key}" in bound_paths:
                continue
            if not isinstance(val, str):
                raise TypeError(
                    f"Map value at '{path}'['{key}'] expected type 'str', "
                    f"got '{type(val).__name__}'."
                )

            cls._validate_string_regex(val, pattern, f"{path}['{key}']")
