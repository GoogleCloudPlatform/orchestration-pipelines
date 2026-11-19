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
"""Utility functions for orchestration pipelines models."""

import re
from datetime import datetime, timedelta

import pytz

ISO8601_DURATION_RE = re.compile(
    r"^P(?:"
    r"(?P<weeks>\d+(?:\.\d+)?)W"
    r"|"
    r"(?=\d|T)"
    r"(?:(?P<days>\d+(?:\.\d+)?)D)?"
    r"(?:T(?=\d)"
    r"(?:(?P<hours>\d+(?:\.\d+)?)H)?"
    r"(?:(?P<minutes>\d+(?:\.\d+)?)M)?"
    r"(?:(?P<seconds>\d+(?:\.\d+)?)S)?"
    r")?"
    r")$",
    re.IGNORECASE,
)
_SHORTHAND_DURATION_RE = re.compile(r"^(\s*\d+[smhdw]\s*)+$")

_MONTHS = [
    "JAN",
    "FEB",
    "MAR",
    "APR",
    "MAY",
    "JUN",
    "JUL",
    "AUG",
    "SEP",
    "OCT",
    "NOV",
    "DEC",
]
_DAYS = ["SUN", "MON", "TUE", "WED", "THU", "FRI", "SAT"]

_CRON_FIELDS = [
    ("minute", 0, 59, None),
    ("hour", 0, 23, None),
    ("day of month", 1, 31, None),
    ("month", 1, 12, _MONTHS),
    ("day of week", 0, 7, _DAYS),  # 0 or 7 can be Sunday
]

_FIELD_PATTERN = re.compile(r"^[0-9*\/\-,a-z]+$", re.IGNORECASE)


def _normalize_value(val, mapping):
    """Converts JAN->1, MON->1, etc., for range checking."""
    if val.isdigit():
        return int(val)
    if mapping:
        try:
            # Handle case-insensitive symbolic names
            return mapping.index(val.upper()) + (0 if mapping == _DAYS else 1)
        except ValueError:
            pass
    return None


def _check_part(part, min_v, max_v, mapping):
    """Validates a single part of a comma-separated list."""
    # Handle steps: */5 or 1-10/2
    if "/" in part:
        base, step = part.split("/", 1)
        if not (step.isdigit() and int(step) > 0):
            return False
        return _check_part(base, min_v, max_v, mapping)

    # Handle ranges: 1-5 or JAN-MAR
    if "-" in part:
        start_str, end_str = part.split("-", 1)
        start = _normalize_value(start_str, mapping)
        end = _normalize_value(end_str, mapping)

        if start is None or end is None:
            return False
        return min_v <= start <= max_v and min_v <= end <= max_v

    # Handle wildcards and constants
    if part == "*":
        return True

    val = _normalize_value(part, mapping)
    return val is not None and min_v <= val <= max_v


def is_valid_field(field_str, min_val, max_val, mapping):
    """Checks if a single cron field string is valid."""
    if not _FIELD_PATTERN.match(field_str):
        return False
    return all(
        _check_part(p, min_val, max_val, mapping) for p in field_str.split(",")
    )


def check_cron_expression(value):
    """Validates that a string is a valid 5-field cron expression or alias."""
    if not value or not isinstance(value, str):
        raise ValueError("CRON expression must be a non-empty string.")

    aliases = [
        "@yearly",
        "@annually",
        "@monthly",
        "@weekly",
        "@daily",
        "@midnight",
        "@hourly",
        "@once",
    ]
    if value.strip().lower() in aliases:
        return

    parts = value.strip().split()
    if len(parts) != 5:
        raise ValueError(
            f"Invalid CRON expression (must have 5 fields): '{value}'"
        )

    for (name, min_val, max_val, mapping), part in zip(
        _CRON_FIELDS, parts, strict=False
    ):
        if not is_valid_field(part, min_val, max_val, mapping):
            raise ValueError(f"Invalid {name} field: '{part}'")


def check_timezone(value):
    """Validates that a string is a recognized IANA timezone."""
    try:
        pytz.timezone(value)
    except (pytz.UnknownTimeZoneError, AttributeError, TypeError) as e:
        raise ValueError(f"'{value}' is not a valid timezone") from e


def check_iso8601_timestamp(value: str, *, allow_timezone: bool = True) -> None:
    """Validates that a string is a valid ISO 8601 timestamp.

    Args:
        value: The timestamp string to validate.
        allow_timezone: Whether timezone suffixes or offsets (e.g., 'Z',
            '+00:00') are allowed. Set to False for fields paired with a
            separate timezone field (e.g., ScheduleTrigger start_time/end_time).

    Raises:
        TypeError: If value is not a string.
        ValueError: If value is not a valid ISO 8601 timestamp or contains a
            timezone suffix/offset when allow_timezone is False.
    """
    if not isinstance(value, str):
        raise TypeError(
            "Timestamp must be a string (e.g., '2026-01-01T00:00:00'), "
            f"got {type(value).__name__}."
        )

    dt = datetime.fromisoformat(value)
    if not allow_timezone and dt.tzinfo is not None:
        raise ValueError(
            f"Timestamp '{value}' must not include a timezone suffix or "
            "offset (e.g., 'Z' or '+00:00'); use the 'timezone' field instead "
            "(e.g., '2025-10-01T00:00:00')."
        )


def iso_match_to_timedelta(iso_match: re.Match[str]) -> timedelta:
    """Converts an ISO8601_DURATION_RE match into a timedelta."""
    groups = iso_match.groupdict()
    return timedelta(
        weeks=float(groups["weeks"] or 0),
        days=float(groups["days"] or 0),
        hours=float(groups["hours"] or 0),
        minutes=float(groups["minutes"] or 0),
        seconds=float(groups["seconds"] or 0),
    )


def check_duration(value):
    """Validates that a string is a valid positive duration.

    Supports ISO 8601 durations (e.g., 'PT30M', 'PT1H30M', 'P1D', 'P2W')
    and shorthand durations (e.g., '30s', '5m', '1h 30m').
    Allowed units: s (seconds), m (minutes), h (hours), d (days), w (weeks).
    """
    if not isinstance(value, str):
        raise TypeError(
            "Duration must be a string (e.g., 'PT10M' or '10m'), "
            f"got {type(value).__name__}."
        )

    stripped = value.strip()
    iso_match = ISO8601_DURATION_RE.match(stripped)
    if iso_match:
        present_values = [
            v for v in iso_match.groupdict().values() if v is not None
        ]
        if any("." in v for v in present_values[:-1]):
            raise ValueError(
                f"Invalid duration format: '{value}'. "
                "In ISO 8601 durations, only the smallest unit may contain "
                "a fractional value."
            )
        try:
            delta = iso_match_to_timedelta(iso_match)
        except OverflowError as e:
            raise ValueError(
                f"Invalid duration: '{value}'. "
                "Duration is too large to represent as a timedelta."
            ) from e
        if delta <= timedelta(0):
            raise ValueError(
                f"Invalid duration: '{value}'. "
                "Duration must be strictly positive (> 0)."
            )
        return

    normalized = stripped.lower()
    if not _SHORTHAND_DURATION_RE.match(normalized):
        raise ValueError(
            f"Invalid duration format: '{value}'. "
            "Expected an ISO 8601 duration (e.g., 'PT30M', 'PT1H30M', 'P1D') "
            "or shorthand <number><unit> (e.g., '1h 30m', '30s'). "
            "Valid units are: s, m, h, d, w."
        )

    amounts = [int(n) for n in re.findall(r"(\d+)[smhdw]", normalized)]
    if sum(amounts) <= 0:
        raise ValueError(
            f"Invalid duration: '{value}'. "
            "Duration must be strictly positive (> 0)."
        )
