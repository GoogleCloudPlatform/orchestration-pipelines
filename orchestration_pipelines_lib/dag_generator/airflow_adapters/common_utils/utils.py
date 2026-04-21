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
"""Module with uncommon util methods."""
import os
import sys
from typing import Any
import importlib
import logging


def import_callable(path: str, function: str) -> Any:
    """Dynamically imports a callable from a Python module.

    Args:
        path: The relative path to the Python module file.
        function: The name of the callable function to import.

    Returns:
        The imported callable object, or None if an error occurs during import.
    """
    try:
        dags_folder = os.environ.get("DAGS_FOLDER", "/home/airflow/gcs/dags")
        module_path = os.path.join(dags_folder, path)
        if not os.path.exists(module_path):
            raise ImportError(f"Module file not found at: {module_path}")
        module_spec = importlib.util.spec_from_file_location(
            "test_script_1", module_path)
        if not module_spec:
            raise ImportError(f"Cannot find spec for module at {module_path}.")
        module = importlib.util.module_from_spec(module_spec)
        sys.modules["test_script_1"] = module
        module_spec.loader.exec_module(module)
        if not hasattr(module, function):
            raise AttributeError(
                f"Module {module_path} has no attribute '{function}'")
        return getattr(module, function)
    except (ImportError, AttributeError) as e:
        logging.error("Failed to import callable '%s' from '%s': %s", function,
                      path, e)
    except Exception as e:
        logging.error("Unexpected error importing callable '%s' from '%s': %s",
                      function, path, e)
