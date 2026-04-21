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

import re
from datetime import timedelta


def duration_to_timedelta(value: str) -> timedelta:
    """Parses a duration string into a timedelta object.

    Supports formats like '10m', '1.5h', and '1h 30m'. Supported units are
    's' (seconds), 'm' (minutes), 'h' (hours), 'd' (days), and 'w' (weeks).

    Args:
        value: The duration string to parse.

    Returns:
        A timedelta object representing the parsed duration.
    """
    multipliers = {'s': 1, 'm': 60, 'h': 3600, 'd': 86400, 'w': 604800}

    pairs = re.findall(r'(\d*\.?\d+)([smhdw])', value.strip().lower())

    total_seconds = 0.0
    for amount_str, unit in pairs:
        total_seconds += float(amount_str) * multipliers[unit]

    return timedelta(seconds=total_seconds)
