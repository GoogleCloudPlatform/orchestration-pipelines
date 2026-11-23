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
"""Dictionary and protobuf Struct/path manipulation utilities."""

import json
import math
import re
from collections.abc import Mapping
from datetime import datetime
from typing import Any, Dict, Type, TypeVar

from google.protobuf.descriptor import Descriptor, FieldDescriptor
from google.protobuf.json_format import MessageToDict, ParseDict
from google.protobuf.message import Message

from orchestration_pipelines_lib.utils import output_harvester as harvester
from orchestration_pipelines_models.pipeline_v1_model.pipeline_validation import (
    PipelineValidator,
    parse_indexed_segment,
)

T = TypeVar("T")

_INT_CPP_TYPES = (
    FieldDescriptor.CPPTYPE_INT32,
    FieldDescriptor.CPPTYPE_INT64,
    FieldDescriptor.CPPTYPE_UINT32,
    FieldDescriptor.CPPTYPE_UINT64,
)
_FLOAT_CPP_TYPES = (
    FieldDescriptor.CPPTYPE_FLOAT,
    FieldDescriptor.CPPTYPE_DOUBLE,
)


def to_snake_case(key: str) -> str:
    """Converts a string from CamelCase or PascalCase to snake_case.

    Args:
        key: The string to convert.

    Returns:
        The snake_case representation of the input string.
    """
    # Handles cases like 'HTTPStatus' -> 'HTTP_Status'
    s1 = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", key)
    # Handles cases like 'camelCase' -> 'camel_Case' and
    # 'PascalCase' -> 'Pascal_Case'
    s2 = re.sub(r"([a-z\d])([A-Z])", r"\1_\2", s1)
    return s2.lower()


def struct_to_dict(struct_msg: Message) -> Dict:
    """Converts a protobuf Message to a dictionary.

    Args:
        struct_msg: The protobuf Message to convert.

    Returns:
        A dictionary representation of the message, or None if the input
        message is empty or None.
    """
    if not struct_msg:
        return None
    return MessageToDict(struct_msg, preserving_proto_field_name=True)


def normalize_struct(
    struct_msg: Message, target_message_class: Type[T]
) -> T | None:
    """Normalizes a protobuf Struct message into a specific message class.

    Converts the input struct into a dictionary and then parses it into
    an instance of the target message class. This process helps handle
    potential differences in field naming conventions (e.g., camelCase vs.
    snake_case) between the source struct data and the target protobuf
    message definition.

    Args:
        struct_msg: The source protobuf Struct or Message to convert.
        target_message_class: The protobuf Message class to parse into.

    Returns:
        An instance of target_message_class populated with the data from
        struct_msg, or None if the input struct is empty or None.
    """
    # 1. Get original dictionary from the Struct
    original_dict = struct_to_dict(struct_msg)
    # 2. Create a message and parse the dict into it.
    return (
        dict_to_struct(original_dict, target_message_class)
        if original_dict
        else None
    )


def coerce_to_engine_str(value: Any) -> str:
    """Coerces a resolved value to a string for CLI/SQL engines."""
    if value is None:
        raise TypeError("Dynamic expression resolved to null.")
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (list, dict, tuple)):
        return json.dumps(harvester.normalize_json_value(value))
    return str(value)


def _coerce_string_map_entry(map_name: str, entry_key: Any, val: Any) -> str:
    if val is None:
        raise TypeError(f"Map entry '{entry_key}' in '{map_name}' is null.")
    if isinstance(val, str):
        return val
    return coerce_to_engine_str(normalize_proto_json(val))


def _coerce_string_maps(data: Any, descriptor: Descriptor) -> Any:
    """Coerces values targeting `map<string, string>` proto fields to `str`."""
    if not isinstance(data, Mapping):
        return data
    out: dict[str, Any] = {}
    for key, val in data.items():
        field = PipelineValidator.find_field(descriptor, key)
        if field is None or val is None:
            out[key] = val
        elif PipelineValidator.is_map_field(field):
            val_field = field.message_type.fields_by_name["value"]
            if val_field.type == FieldDescriptor.TYPE_STRING and isinstance(
                val, Mapping
            ):
                out[key] = {
                    k: _coerce_string_map_entry(key, k, v)
                    for k, v in val.items()
                }
            elif val_field.type == FieldDescriptor.TYPE_MESSAGE and isinstance(
                val, Mapping
            ):
                out[key] = {
                    k: _coerce_string_maps(v, val_field.message_type)
                    for k, v in val.items()
                }
            else:
                out[key] = val
        elif (
            field.type == FieldDescriptor.TYPE_MESSAGE
            and not PipelineValidator.is_struct_field(field)
        ):
            sub_desc = field.message_type
            out[key] = (
                [_coerce_string_maps(item, sub_desc) for item in val]
                if isinstance(val, list)
                else _coerce_string_maps(val, sub_desc)
            )
        else:
            out[key] = val
    return out


def dict_to_struct(
    message_dict: Dict[str, Any], target_message_class: Type[T]
) -> T:
    """Parses a dictionary into a protobuf Message instance.

    Args:
        message_dict: The dictionary containing the data to parse.
        target_message_class: The protobuf Message class to parse into.

    Returns:
        An instance of target_message_class populated with the data from
        the dictionary.
    """
    parsed_message = target_message_class()
    pb = getattr(parsed_message, "_pb", parsed_message)
    ParseDict(
        js_dict=_coerce_string_maps(message_dict, pb.DESCRIPTOR),
        message=pb,
    )
    return parsed_message


def iso_to_timestamp_dict(iso_str: str) -> Dict[str, int]:
    """Converts an ISO 8601 timestamp string to a protobuf Timestamp dict."""
    if not iso_str:
        return None
    dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
    return {"seconds": int(dt.timestamp())}


def normalize_proto_json(val: Any) -> Any:
    """Converts integer-valued floats in parsed protobuf JSON to `int`."""
    if isinstance(val, float) and val.is_integer():
        return int(val)
    if isinstance(val, list):
        return [normalize_proto_json(x) for x in val]
    if isinstance(val, dict):
        return {k: normalize_proto_json(x) for k, x in val.items()}
    return val


def is_finite_number(value: Any) -> bool:
    """Returns True if `value` is a finite `int` or `float` (excluding `bool`)."""
    return (
        not isinstance(value, bool)
        and isinstance(value, (int, float))
        and math.isfinite(value)
    )


def set_nested_path(target: dict[str, Any], path: str, value: Any) -> None:
    """Sets a potentially dotted or bracket-indexed path inside `target`."""
    if path in target:
        target[path] = value
        return
    head, _, tail = path.partition(".")
    if indexed := parse_indexed_segment(head):
        name, indices = indexed
        cursor: Any = target.get(name) if isinstance(target, Mapping) else None
        for idx in indices[:-1]:
            if not isinstance(cursor, list) or idx >= len(cursor):
                raise IndexError(
                    f"Cannot index '{head}' in nested path '{path}'."
                )
            cursor = cursor[idx]
        last_idx = indices[-1]
        if not isinstance(cursor, list) or last_idx >= len(cursor):
            raise IndexError(f"Cannot index '{head}' in nested path '{path}'.")
        if not tail:
            cursor[last_idx] = value
        elif isinstance(cursor[last_idx], dict):
            set_nested_path(cursor[last_idx], tail, value)
        else:
            raise TypeError(
                f"Cannot set '{tail}' on non-dict item at '{head}'."
            )
        return
    if not tail:
        target[head] = value
        return
    if not isinstance(target.get(head), dict):
        target[head] = {}
    set_nested_path(target[head], tail, value)


def _coerce_proto_scalar(value: Any, field: FieldDescriptor) -> Any:
    if value is None:
        raise TypeError(f"Field '{field.name}' resolved to null.")
    if field.type == FieldDescriptor.TYPE_STRING:
        return coerce_to_engine_str(value)
    if field.type == FieldDescriptor.TYPE_BOOL:
        if isinstance(value, bool):
            return value
        raise TypeError(
            f"Field '{field.name}' expected bool, got {type(value).__name__}."
        )
    if field.cpp_type in _INT_CPP_TYPES:
        if is_finite_number(value) and (
            isinstance(value, int) or value.is_integer()
        ):
            return int(value)
        raise TypeError(
            f"Field '{field.name}' expected int, got {type(value).__name__}."
        )
    if field.cpp_type in _FLOAT_CPP_TYPES:
        if is_finite_number(value):
            return float(value)
        raise TypeError(
            f"Field '{field.name}' expected float, got {type(value).__name__}."
        )
    raise TypeError(f"Unsupported scalar type for field '{field.name}'.")


def set_proto_path(message: Message, dotted_path: str, value: Any) -> None:
    """Writes a resolved Python value onto `message` at `dotted_path`."""
    head, _, tail = dotted_path.partition(".")
    indexed = parse_indexed_segment(head)
    field = message.DESCRIPTOR.fields_by_name[indexed[0] if indexed else head]
    if indexed:
        elem_idx = indexed[1][0]
        seq = getattr(message, field.name)
        if tail:
            set_proto_path(seq[elem_idx], tail, value)
        elif field.message_type is None:
            seq[elem_idx] = _coerce_proto_scalar(value, field)
        else:
            raise TypeError(
                f"Indexed message field '{head}' requires a subfield path."
            )
        return

    if field.message_type is not None:
        is_map = PipelineValidator.is_map_field(field)
        is_struct = PipelineValidator.is_struct_field(field)
        if (
            (is_map or is_struct)
            and not tail
            and not isinstance(value, Mapping)
        ):
            raise TypeError(
                f"Field '{field.name}' expected mapping, "
                f"got {type(value).__name__}."
            )
        if is_map:
            map_field = getattr(message, field.name)
            val_desc = field.message_type.fields_by_name["value"]
            if not tail:
                map_field.clear()
                for k, v in value.items():
                    map_field[str(k)] = _coerce_proto_scalar(v, val_desc)
            else:
                map_field[tail] = _coerce_proto_scalar(value, val_desc)
            return

        if is_struct:
            struct_msg = getattr(message, field.name)
            current = struct_to_dict(struct_msg) or {}
            if not tail:
                current = harvester.normalize_json_value(dict(value))
            else:
                set_nested_path(
                    current, tail, harvester.normalize_json_value(value)
                )
            struct_msg.Clear()
            if current:
                ParseDict(current, struct_msg)
            return

        if tail:
            set_proto_path(getattr(message, field.name), tail, value)
        return

    if PipelineValidator.is_field_repeated(field):
        if not isinstance(value, (list, tuple)):
            raise TypeError(
                f"Field '{field.name}' expected list, "
                f"got {type(value).__name__}."
            )
        repeated = getattr(message, field.name)
        del repeated[:]
        repeated.extend(_coerce_proto_scalar(item, field) for item in value)
    elif not tail:
        setattr(message, field.name, _coerce_proto_scalar(value, field))
