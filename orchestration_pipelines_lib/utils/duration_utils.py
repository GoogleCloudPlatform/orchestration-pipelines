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
"""Duration utility functions."""

import re
from datetime import timedelta

from orchestration_pipelines_models.utils.time_utils import (
    ISO8601_DURATION_RE,
    iso_match_to_timedelta,
)


def duration_to_timedelta(value: str) -> timedelta:
    """Parses a duration string into a timedelta object.

    Supports ISO 8601 durations (e.g., 'PT30M', 'PT1H30M', 'P1DT2H', 'P2W')
    and shorthand formats like '10m', '1.5h', and '1h 30m'. Supported units are
    's' (seconds), 'm' (minutes), 'h' (hours), 'd' (days), and 'w' (weeks).

    Args:
        value: The duration string to parse.

    Returns:
        A timedelta object representing the parsed duration.
    """
    stripped = value.strip()
    is_negative = stripped.startswith("-")
    unsigned = stripped[1:] if is_negative else stripped

    if unsigned.lower().startswith("p"):
        iso_match = ISO8601_DURATION_RE.match(unsigned)
        if not iso_match:
            return timedelta(0)
        delta = iso_match_to_timedelta(iso_match)
        return -delta if is_negative else delta

    multipliers = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}

    normalized = stripped.lower()

    pairs = re.findall(r"(\d*\.?\d+)([smhdw])", normalized)

    total_seconds = 0.0
    for amount_str, unit in pairs:
        total_seconds += float(amount_str) * multipliers[unit]

    if is_negative:
        total_seconds = -total_seconds

    return timedelta(seconds=total_seconds)
