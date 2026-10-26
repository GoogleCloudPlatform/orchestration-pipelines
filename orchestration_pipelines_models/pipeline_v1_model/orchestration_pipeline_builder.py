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
"""Builds an OrchestrationPipeline protobuf message from a dictionary."""

import copy
from typing import Any

from google.protobuf import json_format
from google.protobuf.descriptor import Descriptor, FieldDescriptor

from orchestration_pipelines_models.pipeline_v1_model.pipeline_validation import (
    PipelineValidator,
)
from orchestration_pipelines_models.pipeline_v1_model.protos.orchestration_pipeline_pb2 import (
    Action,
    OrchestrationPipeline,
)
from orchestration_pipelines_models.utils import cel_utils

_CPP_TYPE_PLACEHOLDERS = {
    FieldDescriptor.CPPTYPE_BOOL: False,
    FieldDescriptor.CPPTYPE_INT32: 0,
    FieldDescriptor.CPPTYPE_INT64: 0,
    FieldDescriptor.CPPTYPE_UINT32: 0,
    FieldDescriptor.CPPTYPE_UINT64: 0,
    FieldDescriptor.CPPTYPE_ENUM: 0,
    FieldDescriptor.CPPTYPE_FLOAT: 0.0,
    FieldDescriptor.CPPTYPE_DOUBLE: 0.0,
    FieldDescriptor.CPPTYPE_STRING: "",
}


def _extract_field_cel(
    value: str, error_path: str, *, require_cel_root: bool = True
) -> str | None:
    try:
        return cel_utils.extract_cel_expression(
            value, require_cel_root=require_cel_root
        )
    except ValueError as e:
        raise ValueError(f"Error for field '{error_path}': {e}") from e


def _get_placeholder(field: FieldDescriptor) -> Any:
    if PipelineValidator.is_field_repeated(
        field
    ) and not PipelineValidator.is_map_field(field):
        return []
    return (
        {}
        if field.type == FieldDescriptor.TYPE_MESSAGE
        else _CPP_TYPE_PLACEHOLDERS.get(field.cpp_type, "")
    )


class OrchestrationPipelineBuilder:
    """Builds an OrchestrationPipeline from a dictionary."""

    @classmethod
    def build(cls, pipeline_def: dict[str, Any]) -> OrchestrationPipeline:
        """Maps a dictionary to a validated OrchestrationPipeline protobuf."""
        processed_def = cls._preprocess_pipeline(copy.deepcopy(pipeline_def))
        pipeline = OrchestrationPipeline()
        json_format.ParseDict(
            js_dict=processed_def,
            message=pipeline,
            ignore_unknown_fields=False,
        )
        PipelineValidator.validate(pipeline)
        return pipeline

    @classmethod
    def _preprocess_pipeline(cls, raw_def: dict[str, Any]) -> dict[str, Any]:
        """Extracts '${...}' CEL expressions into dynamic_bindings maps."""
        scopes: list[tuple[dict[str, Any], Descriptor, str]] = [
            (raw_def, OrchestrationPipeline.DESCRIPTOR, "")
        ]
        if isinstance(actions := raw_def.get("actions"), list):
            for idx, wrapper in enumerate(actions):
                if isinstance(wrapper, dict):
                    for action_key, action_dict in wrapper.items():
                        field = PipelineValidator.find_field(
                            Action.DESCRIPTOR, action_key
                        )
                        if field is not None and isinstance(action_dict, dict):
                            scopes.append(
                                (
                                    action_dict,
                                    field.message_type,
                                    f"actions[{idx}].{field.name}",
                                )
                            )

        for target, descriptor, err_prefix in scopes:
            bindings: dict[str, str] = {}
            cls._preprocess_dict(
                target, descriptor, "", bindings, error_prefix=err_prefix
            )
            if bindings:
                target.setdefault("dynamic_bindings", {}).update(bindings)

        return raw_def

    @classmethod
    def _preprocess_dict(
        cls,
        target: dict[str, Any],
        descriptor: Descriptor | None,
        path_prefix: str,
        bindings: dict[str, str],
        *,
        error_prefix: str = "",
    ) -> None:
        """Recursively extracts '${...}' expressions into dynamic_bindings."""
        for key, value in list(target.items()):
            field = (
                PipelineValidator.find_field(descriptor, key)
                if descriptor
                else None
            )
            if descriptor and (
                field is None
                or field.name in ("dynamic_bindings", "parameters", "actions")
            ):
                continue

            name = field.name if field else key
            path = f"{path_prefix}.{name}" if path_prefix else name
            err_path = f"{error_prefix}.{path}" if error_prefix else path
            if field and error_prefix and not path_prefix and name == "run_if":
                if isinstance(value, str) and (
                    expr := _extract_field_cel(
                        value, err_path, require_cel_root=False
                    )
                ):
                    target[key] = expr
                continue

            if isinstance(value, str):
                if (expr := _extract_field_cel(value, err_path)) is not None:
                    bindings[path] = expr
                    target[key] = _get_placeholder(field) if field else ""
                continue

            if isinstance(value, dict):
                if (
                    field is None
                    or PipelineValidator.is_struct_field(field)
                    or PipelineValidator.is_map_field(field)
                ):
                    cls._preprocess_dict(
                        value,
                        None,
                        path,
                        bindings,
                        error_prefix=error_prefix,
                    )
                elif (
                    field.type == FieldDescriptor.TYPE_MESSAGE
                    and not PipelineValidator.is_field_repeated(field)
                ):
                    cls._preprocess_dict(
                        value,
                        field.message_type,
                        path,
                        bindings,
                        error_prefix=error_prefix,
                    )
            elif isinstance(value, list):
                is_msg_list = field is not None and (
                    field.type == FieldDescriptor.TYPE_MESSAGE
                    and not PipelineValidator.is_map_field(field)
                    and field.message_type.full_name != "google.protobuf.Value"
                )
                child_desc = field.message_type if is_msg_list else None
                elem_ph = (
                    {}
                    if is_msg_list
                    else (
                        _CPP_TYPE_PLACEHOLDERS.get(field.cpp_type, "")
                        if field
                        else ""
                    )
                )
                for idx, item in enumerate(value):
                    if isinstance(item, dict):
                        cls._preprocess_dict(
                            item,
                            child_desc,
                            f"{path}[{idx}]",
                            bindings,
                            error_prefix=error_prefix,
                        )
                    elif (
                        isinstance(item, str)
                        and (
                            expr := _extract_field_cel(
                                item, f"{err_path}[{idx}]"
                            )
                        )
                        is not None
                    ):
                        bindings[f"{path}[{idx}]"] = expr
                        value[idx] = elem_ph
