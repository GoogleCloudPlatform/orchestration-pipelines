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
"""Unit tests for the duration_utils module."""
import unittest
from datetime import timedelta
from orchestration_pipelines_lib.utils.duration_utils import duration_to_timedelta


class TestDurationUtils(unittest.TestCase):
    """Test suite for duration utility functions."""

    def test_single_units(self):
        """Test basic single unit conversions."""
        cases = [
            ("10s", 10.0),
            ("5m", 300.0),
            ("2h", 7200.0),
            ("1d", 86400.0),
            ("1w", 604800.0),
        ]

        for val, expected in cases:
            with self.subTest(val=val):
                self.assertEqual(duration_to_timedelta(val),
                                 timedelta(seconds=expected))

    def test_combined_durations(self):
        """Test strings containing multiple units."""
        cases = [
            ("1h 30m", 5400.0),
            ("1d 12h", 129600.0),
            ("1w 2d 3h 4m 5s", 788645.0),
        ]

        for val, expected in cases:
            with self.subTest(val=val):
                self.assertEqual(duration_to_timedelta(val),
                                 timedelta(seconds=expected))

    def test_decimals_and_formatting(self):
        """Test floating point amounts and varying string formats."""
        cases = [
            ("1.5h", 5400.0),
            (".5m", 30.0),
            ("2.25d", 194400.0),
            (" 1H 30M ", 5400.0),  # Case sensitivity and whitespace
            ("1h30m", 5400.0),  # No spaces between pairs
        ]

        for val, expected in cases:
            with self.subTest(val=val):
                self.assertEqual(duration_to_timedelta(val),
                                 timedelta(seconds=expected))

    def test_edge_cases(self):
        """Test unusual or empty inputs."""
        cases = [
            ("", 0.0),  # Empty string
            ("invalid", 0.0),  # No matches
            ("10x", 0.0),  # Valid number, invalid unit
            ("h", 0.0),  # Unit only
        ]

        for val, expected in cases:
            with self.subTest(val=val):
                self.assertEqual(duration_to_timedelta(val),
                                 timedelta(seconds=expected))


if __name__ == '__main__':
    unittest.main()
