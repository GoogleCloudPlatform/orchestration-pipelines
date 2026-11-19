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
"""Unit tests for utility functions of models."""

import unittest

from orchestration_pipelines_models.utils import time_utils as util


class UtilTest(unittest.TestCase):

    def test_check_cron_expression_valid(self):
        """Tests that check cron expression valid."""
        valid_expressions = [
            '* * * * *', '0 0 1 1 *', '0 0 1 JAN *', '0 0 * * MON',
            '*/15 * * * *', '0 0-23/2 * * *', '0 0 1,15 * *', '@yearly',
            '@annually', '@monthly', '@weekly', '@daily', '@midnight',
            '@hourly', '@once',
        ]

        for expr in valid_expressions:
            with self.subTest(expr=expr):
                util.check_cron_expression(expr)

    def test_check_cron_expression_invalid(self):
        """Tests that check cron expression invalid."""
        invalid_expressions = {
            'invalid number of fields': '0 * * *',
            'another invalid number of fields': '* * * * * *',
            'invalid character': '* * * * ?',
            'minute out of range': '60 * * * *',
            'hour out of range': '* 24 * * *',
            'day of month out of range': '* * 32 * *',
            'month out of range': '* * * 13 *',
            'day of week out of range': '* * * * 8',
            'invalid step': '* * * * */A',
            'non-string': 12345,
            'empty string': '',
            'just spaces': '     ',
            'invalid month name': '* * * FOO *',
            'invalid day name': '* * * * BAR',
        }

        for name, expr in invalid_expressions.items():
            with self.subTest(name=name, expr=expr):
                with self.assertRaises(ValueError):
                    util.check_cron_expression(expr)

    def test_check_cron_expression_invalid_parts(self):
        """Tests that check cron expression invalid parts."""
        # Maps the part name to the invalid value and the expected field name in error
        invalid_parts = [
            ("minute", "60", "minute"),
            ("hour", "24", "hour"),
            ("day of month", "32", "day of month"),
            ("month", "13", "month"),
            ("day of week", "8", "day of week"),
            ("minute", "*/0", "minute"),  # Step must be > 0
        ]

        for field_name, invalid_val, error_key in invalid_parts:
            parts = ['*', '*', '*', '*', '*']
            if field_name == "minute": parts[0] = invalid_val
            elif field_name == "hour": parts[1] = invalid_val
            elif field_name == "day of month": parts[2] = invalid_val
            elif field_name == "month": parts[3] = invalid_val
            elif field_name == "day of week": parts[4] = invalid_val
            cron_expr = " ".join(parts)
            with self.subTest(field=field_name, expr=cron_expr):
                with self.assertRaisesRegex(ValueError,
                                            f"Invalid {error_key} field"):
                    util.check_cron_expression(cron_expr)

    def test_check_timezone_valid(self):
        """Tests that check timezone valid."""
        valid_timezones = ['UTC', 'America/New_York', 'Europe/London']

        for tz in valid_timezones:
            with self.subTest(tz=tz):
                util.check_timezone(tz)

    def test_check_timezone_invalid(self):
        """Tests that check timezone invalid."""
        # Note: 'cet' and 'gmt' are often valid in pytz depending on version,
        # but 'Invalid/Timezone' is a safe bet for failure.
        invalid_timezones = ['Invalid/Timezone', 'Mars/Base', '']

        for tz in invalid_timezones:
            with self.subTest(tz=tz):
                with self.assertRaises(ValueError):
                    util.check_timezone(tz)

    def test_check_iso8601_timestamp_valid(self):
        """Tests valid ISO 8601 timestamps with and without timezone."""
        naive_timestamps = [
            "2025-10-01T00:00:00",
            "2026-01-01T12:30:45.123456",
            "2026-01-01",
        ]
        aware_timestamps = [
            "2025-10-01T00:00:00Z",
            "2025-10-01T00:00:00+00:00",
            "2025-10-01T00:00:00-05:00",
            "2025-10-01T00:00:00+0200",
        ]

        for ts in naive_timestamps:
            with self.subTest(ts=ts, allow_timezone=True):
                util.check_iso8601_timestamp(ts, allow_timezone=True)
            with self.subTest(ts=ts, allow_timezone=False):
                util.check_iso8601_timestamp(ts, allow_timezone=False)

        for ts in aware_timestamps:
            with self.subTest(ts=ts, allow_timezone=True):
                util.check_iso8601_timestamp(ts, allow_timezone=True)

    def test_check_iso8601_timestamp_rejects_timezone_when_disallowed(self):
        """Tests timezone suffixes/offsets are rejected when allow_timezone=False."""
        aware_timestamps = [
            "2025-10-01T00:00:00Z",
            "2025-10-01T00:00:00+00:00",
            "2025-10-01T00:00:00-05:00",
            "2025-10-01T00:00:00+0200",
        ]
        for ts in aware_timestamps:
            with self.subTest(ts=ts):
                with self.assertRaisesRegex(
                    ValueError, "must not include a timezone suffix or offset"
                ):
                    util.check_iso8601_timestamp(ts, allow_timezone=False)

    def test_check_iso8601_timestamp_invalid(self):
        """Tests invalid ISO 8601 timestamp strings raise ValueError."""
        invalid_timestamps = [
            "2025/01/01",
            "2025-10-01T00:00:00z",
            "not-a-timestamp",
            "",
        ]
        for ts in invalid_timestamps:
            with self.subTest(ts=ts):
                with self.assertRaises(ValueError):
                    util.check_iso8601_timestamp(ts)

    def test_check_iso8601_timestamp_type_error(self):
        """Tests non-string timestamp inputs raise TypeError."""
        for val in [123, None, []]:
            with self.subTest(val=val):
                with self.assertRaises(TypeError):
                    util.check_iso8601_timestamp(val)

    def test_check_duration_valid(self):
        """Tests that check duration accepts shorthand and ISO 8601 durations."""
        valid_durations = [
            '10s',
            '5m',
            '2h',
            '1d',
            '4w',  # Simple units
            '1h30m',
            '1w 2d 3h',  # Combined units
            ' 10m ',
            '1h 30m ',  # Leading/trailing spaces
            '1H 30M',
            '10S',  # Case sensitivity
            'PT30M',
            'PT1H',
            'PT45S',
            'PT1H30M',
            'PT1H30M15S',
            'P1D',
            'P1DT2H30M',
            'P1DT2H30M15S',
            'P2W',
            'PT0.5H',
            'PT1H30.5M',
            'PT30.5S',
            'PT0.000001S',
            'pt30m',
            ' PT30M ',
        ]

        for dur in valid_durations:
            with self.subTest(dur=dur):
                util.check_duration(dur)

    def test_check_duration_invalid(self):
        """Tests that check duration rejects invalid shorthand and ISO 8601 durations."""
        invalid_durations = {
            'invalid unit': '10x',
            'missing unit': '10',
            'just number': '100',
            'gibberish': 'abc',
            'unsupported combined': '1h 30z',
            'empty string': '',
            'only spaces': '   ',
            'zero seconds': '0s',
            'zero combined': '0h 0m',
            'iso bare P': 'P',
            'iso bare PT': 'PT',
            'iso trailing T': 'P1DT',
            'iso months unsupported': 'P1M',
            'iso years unsupported': 'P1Y',
            'iso year month day': 'P1Y2M3D',
            'iso zero seconds': 'PT0S',
            'iso zero minutes': 'PT0M',
            'iso zero days': 'P0D',
            'iso zero weeks': 'P0W',
            'iso zero fractional': 'PT0.0S',
            'iso sub-microsecond rounds to zero': 'PT0.0000001S',
            'iso overflow weeks': 'P200000000W',
            'iso overflow days': 'P1000000000D',
            'iso negative': '-PT30M',
            'iso wrong order': 'PT30M1H',
            'iso weeks with days': 'P1W2D',
            'iso fractional non-final unit': 'PT1.5H30M',
            'iso internal space': 'PT 30M',
            'iso invalid unit': 'PT30X',
        }

        for name, dur in invalid_durations.items():
            with self.subTest(name=name, dur=dur):
                with self.assertRaises(ValueError):
                    util.check_duration(dur)

    def test_check_duration_type_error(self):
        """Tests that check duration type error."""
        invalid_types = [123, None, [], {"key": "val"}]

        for val in invalid_types:
            with self.subTest(val=val):
                with self.assertRaises(TypeError):
                    util.check_duration(val)


if __name__ == '__main__':
    unittest.main()
