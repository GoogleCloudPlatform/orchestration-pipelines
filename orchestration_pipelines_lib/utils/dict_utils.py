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
"""Dictionary manipulation utilities."""

import re
from typing import Dict, Type, Any
from google.protobuf.json_format import MessageToDict, ParseDict
from google.protobuf.message import Message


def to_snake_case(key: str) -> str:
    """Converts a string from CamelCase or PascalCase to snake_case.

    Args:
        key: The string to convert.

    Returns:
        The snake_case representation of the input string.
    """
    # Handles cases like 'HTTPStatus' -> 'HTTP_Status'
    s1 = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", key)
    # Handles cases like 'camelCase' -> 'camel_Case' and 'PascalCase' -> 'Pascal_Case'
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


def normalize_struct(struct_msg: Message, target_message_class: Type[Message]):
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
    return dict_to_struct(original_dict,
                          target_message_class) if original_dict else None


def dict_to_struct(message_dict: Dict[str, Any],
                   target_message_class: Type[Message]):
    """Parses a dictionary into a protobuf Message instance.

    Args:
        message_dict: The dictionary containing the data to parse.
        target_message_class: The protobuf Message class to parse into.

    Returns:
        An instance of target_message_class populated with the data from
        the dictionary.
    """
    parsed_message = target_message_class()
    ParseDict(js_dict=message_dict, message=parsed_message._pb)
    return parsed_message
