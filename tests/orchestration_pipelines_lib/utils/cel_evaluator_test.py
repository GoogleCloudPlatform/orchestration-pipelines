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
"""Unit tests for runtime CEL evaluation and lazy `celpy` loading."""

from __future__ import annotations

import decimal
import subprocess
import sys

import pytest

from orchestration_pipelines_lib.utils import cel_evaluator

_CEL_CTX = {
    "parameters": {
        "env": "prod",
        "threshold": 10.5,
        "int_threshold": 10,
        "regions": ["US", "EU"],
    },
    "run": {
        "id": "manual__2026-10-22T12:00:00+00:00",
        "trigger_time": "2026-10-22T12:00:00Z",
        "trigger_date": "2026-10-22",
    },
    "outputs": {
        "step1": {
            "value": {
                "rows": decimal.Decimal("25"),
                "tag": "batch_a",
                "cursor": None,
                "nan_val": float("nan"),
            },
            "metadata": {"rows_written": 25},
        },
        "none_step": {"value": None, "metadata": {"run_id": None}},
        "my-step": {"value": {"cnt": 7}, "metadata": {}},
    },
}


@pytest.mark.parametrize(
    ("expr", "expected"),
    [
        ("${true}", True),
        ('outputs["my-step"].value.cnt', 7),
        ("outputs.my-step.value.cnt", 7),
        ("has(outputs.my-step.value)", True),
        ('has(outputs["my-step"].value)', True),
        ('has(outputs["missing-step"])', False),
        ("parameters.env", "prod"),
        ("parameters.regions", ["US", "EU"]),
        ("parameters.int_threshold + 0.5", 10.5),
        ("outputs.step1.metadata.rows_written >= parameters.threshold", True),
        ("parameters.threshold > -parameters.int_threshold", True),
        ("parameters.int_threshold + 0.5 == 10.5", True),
        ("10 in [10.0, 20.5]", True),
        ("30 in [10, 20.5]", False),
        ("'US' in parameters.regions", True),
        ("outputs.step1.value.tag.startsWith('batch_')", True),
        ("parameters.env != '${parameters.other}'", True),
        ("has(outputs.missing_step.value)", False),
        ("has(outputs.none_step)", True),
        ("has(outputs.none_step.value)", False),
        ("outputs.none_step.metadata.run_id == null", True),
        ("has(outputs.step1.value.cursor)", True),
        ("outputs.step1.value.cursor == null", True),
        ("outputs.step1.value.nan_val == null", True),
        ("outputs.step1.metadata.rows_written == null", False),
        ("outputs.step1.metadata.rows_written != null", True),
        (
            "has(outputs.missing_step.value) "
            "? outputs.missing_step.value.msg : 'fallback'",
            "fallback",
        ),
        (
            "${'t_' + run.trigger_date + '_' + "
            "string(parameters.int_threshold)}",
            "t_2026-10-22_10",
        ),
        (
            "outputs.step1.value",
            {"rows": 25.0, "tag": "batch_a", "cursor": None, "nan_val": None},
        ),
    ],
)
def test_evaluate_cel_expression_features_and_numeric_promotion(expr, expected):
    assert cel_evaluator.evaluate_cel_expression(expr, _CEL_CTX) == expected


@pytest.mark.parametrize(
    "expr",
    [
        "outputs.none_step.value != null",
        "outputs.missing_step.value == null",
        "(1 / 0) != null",
    ],
)
def test_evaluate_cel_expression_propagates_errors_with_null_comparisons(expr):
    import celpy

    with pytest.raises(celpy.CELEvalError):
        cel_evaluator.evaluate_cel_expression(expr, _CEL_CTX)


def test_parse_time_imports_do_not_load_celpy():
    code = (
        "import sys; "
        "import orchestration_pipelines_lib.utils.dict_utils; "
        "import orchestration_pipelines_lib.utils.runtime_resolver; "
        "assert 'celpy' not in sys.modules"
    )
    subprocess.run([sys.executable, "-c", code], check=True)
