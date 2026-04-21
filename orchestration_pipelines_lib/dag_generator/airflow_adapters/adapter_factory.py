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

import importlib
import os
from distutils.version import LooseVersion


def get_adapter(version):
    """Dynamically loads the most appropriate Airflow adapter version.

    It chooses the closest, lowest version available.

    Args:
        version: The target Airflow version string.

    Returns:
        The imported core module of the matching Airflow adapter.

    Raises:
        ValueError: If no suitable adapter is found or the import fails.
    """
    current_dir = os.path.dirname(os.path.abspath(__file__))
    available_versions = []
    for dir_name in os.listdir(current_dir):
        if os.path.isdir(os.path.join(
                current_dir, dir_name)) and dir_name.startswith("airflow_"):
            version_str = dir_name.replace("airflow_", "").replace("_", ".")
            available_versions.append((LooseVersion(version_str), dir_name))

    available_versions.sort(key=lambda x: x[0], reverse=True)

    input_version = LooseVersion(version.replace("_", "."))

    best_match = None
    for version_obj, dir_name in available_versions:
        if version_obj <= input_version:
            best_match = dir_name.replace("airflow_", "")
            break

    if best_match:
        try:
            module_path = (
                f"orchestration_pipelines_lib.dag_generator.airflow_adapters."
                f"airflow_{best_match}.core")
            adapter_module = importlib.import_module(module_path)
            return adapter_module
        except ImportError as e:
            raise ValueError(
                f"Unsupported Airflow version: {version}. Error: {e}")
    else:
        raise ValueError(
            f"No suitable adapter found for Airflow version: {version}")
