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
"""Unit tests for the Airflow adapter factory."""

from unittest.mock import MagicMock, patch

import pytest
from packaging.version import Version

from orchestration_pipelines_lib.dag_generator.airflow_adapters.adapter_factory import (  # noqa: E501
    _parse_version,
    get_adapter,
)

ADAPTER_FACTORY_MODULE = (
    "orchestration_pipelines_lib.dag_generator.airflow_adapters.adapter_factory"
)


@pytest.mark.parametrize(
    "version_str,expected",
    [
        ("airflow_2", Version("2")),
        ("airflow_3", Version("3")),
        ("2_9_0", Version("2.9.0")),
        ("2.10.2", Version("2.10.2")),
        ("2.9.0rc1", Version("2.9.0rc1")),
        ("3.1.0rc1", Version("3.1.0rc1")),
        ("2_9_0_dev0", Version("2.9.0.dev0")),
        ("3.1.0.dev0", Version("3.1.0.dev0")),
        ("2.9.0+composer", Version("2.9.0+composer")),
        ("2_9_0+composer", Version("2.9.0+composer")),
    ],
)
def test_parse_version_handles_various_version_formats(version_str, expected):
    """Test that _parse_version parses standard, pre-release, and local
    version strings into Version objects.
    """
    assert _parse_version(version_str) == expected


@pytest.mark.parametrize(
    "input_version,expected_adapter",
    [
        ("2_9_0", "airflow_2"),
        ("2.10.0+composer", "airflow_2"),
        ("2.9.0rc1", "airflow_2"),
        ("2.9.0.dev0", "airflow_2"),
        ("3_0_0", "airflow_3"),
        ("3.1.0rc1", "airflow_3"),
        ("3.1.0.dev0", "airflow_3"),
        ("3.0.0+composer", "airflow_3"),
    ],
)
@patch(f"{ADAPTER_FACTORY_MODULE}.importlib.import_module")
@patch(f"{ADAPTER_FACTORY_MODULE}.os.path.isdir", return_value=True)
@patch(
    f"{ADAPTER_FACTORY_MODULE}.os.listdir",
    return_value=["airflow_2", "airflow_3"],
)
def test_get_adapter_with_valid_version_returns_expected_module(
    mock_listdir,
    mock_isdir,
    mock_import_module,
    input_version,
    expected_adapter,
):
    """Test that get_adapter selects and imports the appropriate adapter module
    including for pre-release and local version strings.
    """
    mock_module = MagicMock()
    mock_import_module.return_value = mock_module

    result = get_adapter(input_version)

    assert result == mock_module
    mock_import_module.assert_called_once_with(
        f"orchestration_pipelines_lib.dag_generator.airflow_adapters."
        f"{expected_adapter}.core"
    )


@patch(
    f"{ADAPTER_FACTORY_MODULE}.os.path.isdir",
    return_value=True,
)
@patch(
    f"{ADAPTER_FACTORY_MODULE}.os.listdir",
    return_value=["airflow_2", "airflow_3"],
)
def test_get_adapter_with_lower_unsupported_version_raises_value_error(
    mock_listdir, mock_isdir
):
    """Test that get_adapter raises a ValueError when no adapter is <= the
    requested version.
    """
    with pytest.raises(
        ValueError,
        match="No suitable adapter found for Airflow version: 1.10.15",
    ):
        get_adapter("1.10.15")


@patch(
    f"{ADAPTER_FACTORY_MODULE}.importlib.import_module",
    side_effect=ImportError("Module not found"),
)
@patch(f"{ADAPTER_FACTORY_MODULE}.os.path.isdir", return_value=True)
@patch(
    f"{ADAPTER_FACTORY_MODULE}.os.listdir",
    return_value=["airflow_2", "airflow_3"],
)
def test_get_adapter_with_import_error_raises_value_error(
    mock_listdir, mock_isdir, mock_import_module
):
    """Test that get_adapter wraps an ImportError in a ValueError."""
    with pytest.raises(
        ValueError,
        match="Unsupported Airflow version: 2. Error: Module not found",
    ):
        get_adapter("2.9.0")
