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
# limitations under the License.
#
"""Configuration and fixtures for pytest.

This module provides custom pytest hooks used across the test suite,
including logic to skip tests based on the installed Airflow version.
"""

import importlib.metadata

from packaging.version import parse

airflow_version = parse(importlib.metadata.version("apache-airflow"))
IS_AIRFLOW_2 = airflow_version.major == 2


def pytest_ignore_collect(collection_path, config) -> bool:
    """Skips test collection for specific Airflow versions.

    This hook ignores test directories specific to Airflow 2 or Airflow 3
    if the currently installed version of Airflow does not match.

    Args:
        collection_path: The path to the file or directory being collected.
        config: The pytest config object.

    Returns:
        True if the path should be ignored during collection, False otherwise.
    """
    major_version = airflow_version.major

    path_parts = collection_path.parts

    if "airflow_2" in path_parts and major_version != 2:
        return True

    if "airflow_3" in path_parts and major_version != 3:
        return True

    return False
