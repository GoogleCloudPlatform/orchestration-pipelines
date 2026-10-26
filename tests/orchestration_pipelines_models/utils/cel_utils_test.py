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
"""Unit tests for orchestration_pipelines_models.utils.cel_utils."""

import warnings

import pytest

from orchestration_pipelines_models.utils import cel_utils


@pytest.mark.parametrize(
    ("raw_value", "require_root", "expected", "has_ref"),
    [
        ("static_string", True, None, False),
        ("${ parameters.env }", True, "parameters.env", True),
        ("  ${ parameters.count + 1 }  ", True, "parameters.count + 1", True),
        ("${ {'k': parameters.val} }", True, "{'k': parameters.val}", True),
        ("${ true }", True, None, False),
        ("${ true }", False, "true", False),
        ("${HOME}/data", True, None, False),
        ("echo ${AIP_MODEL_DIR} && echo ${USER:-default}", True, None, False),
        ("echo ${spark.yarn.app.container.log.dir}", True, None, False),
    ],
)
def test_extract_and_has_cel_reference(
    raw_value, require_root, expected, has_ref
):
    extracted = cel_utils.extract_cel_expression(
        raw_value, require_cel_root=require_root
    )
    has_reference = cel_utils.has_cel_reference(raw_value)

    assert extracted == expected
    assert has_reference is has_ref


@pytest.mark.parametrize(
    "invalid_value,error_pattern",
    [
        ("gs://${parameters.bucket}/data", "Mid-string interpolation"),
        ("gs://${ 'p-' + parameters.bucket }/data", "Mid-string interpolation"),
        ("${parameters.a}/${parameters.b}", "Mid-string interpolation"),
        ("${parameters.a}/suffix", "Mid-string interpolation"),
        ("SELECT '${parameters.table}'", "Mid-string interpolation"),
        ("${ parameters.unclosed", r"Unclosed '\$\{\.\.\.\}'"),
        ("${ 'unterminated } + parameters.env }", r"Unclosed '\$\{\.\.\.\}'"),
        ("${   }", "Empty CEL expression"),
    ],
)
def test_extract_cel_expression_invalid(invalid_value, error_pattern):
    with pytest.raises(ValueError, match=error_pattern):
        cel_utils.extract_cel_expression(invalid_value)


@pytest.mark.parametrize(
    ("expr", "params", "run_fields", "outputs", "guarded"),
    [
        (
            "${ 'gs://' + parameters.bucket + '/' + run.trigger_date }",
            {"bucket"},
            {"trigger_date"},
            set(),
            set(),
        ),
        (
            "outputs.extract.value.count > 0 && !parameters.dry_run",
            {"dry_run"},
            set(),
            {"extract"},
            set(),
        ),
        (
            "has(outputs.task_a.value) ? outputs.task_a.value[0].id : run.id",
            set(),
            {"id"},
            {"task_a"},
            {"task_a"},
        ),
        ("has(outputs.task_a)", set(), set(), {"task_a"}, {"task_a"}),
        (
            "size(outputs.bq.metadata.job_id) > 0 "
            "&& run.trigger_time.startsWith('2026')",
            set(),
            {"trigger_time"},
            {"bq"},
            set(),
        ),
        (
            "parameters.env in ['dev', 'prod'] "
            "&& {'a': parameters.retries}['a'] == 1",
            {"env", "retries"},
            set(),
            set(),
            set(),
        ),
        (
            "parameters.env != '${FOO}' && parameters.threshold >= 1.5e-3 "
            "&& 10u > 1e2",
            {"env", "threshold"},
            set(),
            set(),
            set(),
        ),
        (
            "outputs['action.v1'].value['my-key'] == parameters['env']",
            {"env"},
            set(),
            {"action.v1"},
            set(),
        ),
        (
            "outputs ['action.v1'] . value ['my-key'] == parameters . env "
            "&& has( outputs . task_a . value ) && parameters . env in ['dev']",
            {"env"},
            set(),
            {"action.v1", "task_a"},
            {"task_a"},
        ),
        (
            'has(outputs["action-name"].value) '
            '? outputs["action-name"].value.default : "fallback"',
            set(),
            set(),
            {"action-name"},
            {"action-name"},
        ),
        (
            "'outputs.literal-str.value' + outputs.real_step.value.x",
            set(),
            set(),
            {"real_step"},
            set(),
        ),
        ("eval(parameters.a)", {"a"}, set(), set(), set()),
        ("secrets.api_key", set(), set(), set(), set()),
        ("'a[\"]' + secrets.api_key + '\"]'", set(), set(), set(), set()),
        (
            "parameters.regions.exists(r, r == 'US' && has(r.tag)) "
            "&& math.greatest(1, outputs.t1.value.n) > 0",
            {"regions"},
            set(),
            {"t1"},
            set(),
        ),
        (
            "has(secrets.api_key) && has(run.trigger_date) "
            "&& has(parameters.env)",
            {"env"},
            {"trigger_date"},
            set(),
            set(),
        ),
    ],
)
def test_validate_and_analyze_cel_valid(
    expr, params, run_fields, outputs, guarded
):
    expected = cel_utils.CelExpressionAnalysis(
        parameters=frozenset(params),
        run_fields=frozenset(run_fields),
        output_actions=frozenset(outputs),
        guarded_output_actions=frozenset(guarded),
    )

    with warnings.catch_warnings():
        warnings.simplefilter("error", UserWarning)
        analysis = cel_utils.validate_and_analyze_cel(expr)

    assert analysis == expected


@pytest.mark.parametrize(
    ("expr", "outputs", "guarded"),
    [
        (
            "${ outputs . action-name . value . default }",
            {"action-name"},
            set(),
        ),
        ("outputs.1step.value.cnt > 0", {"1step"}, set()),
        (
            "has(outputs.action-name) "
            "? outputs.action-name.value.default : 'fallback'",
            {"action-name"},
            {"action-name"},
        ),
        (
            "has(outputs.action-name.value) "
            "? outputs.action-name.value.default : 'fallback'",
            {"action-name"},
            {"action-name"},
        ),
        (
            "outputs.step-1.value.count - outputs.step-2.value.count",
            {"step-1", "step-2"},
            set(),
        ),
        (
            "'outputs.literal-str.value' + outputs.real-step.value.x",
            {"real-step"},
            set(),
        ),
    ],
)
def test_validate_and_analyze_cel_warns_on_non_identifier_outputs(
    expr, outputs, guarded
):
    with pytest.warns(UserWarning) as record:
        analysis = cel_utils.validate_and_analyze_cel(expr)

    messages = " ".join(str(w.message) for w in record)
    for action_id in outputs:
        assert f'outputs["{action_id}"]' in messages
    assert "literal-str" not in messages
    assert analysis.output_actions == outputs
    assert analysis.guarded_output_actions == guarded


@pytest.mark.parametrize(
    ("invalid_expr", "error_pattern"),
    [
        ("", "CEL expression must be a non-empty string"),
        ("${ 'unterminated }", r"Unclosed '\$\{\.\.\.\}'"),
        ("parameters", "Invalid or unknown reference 'parameters'"),
        ('parameters[""]', "Invalid or unknown reference"),
        ("run", "Invalid or unknown reference 'run'"),
        ("run.bad", "Invalid or unknown reference 'run.bad'"),
        ("outputs", "Invalid or unknown reference 'outputs'"),
        ("outputs.task_a", "Invalid or unknown reference 'outputs.task_a'"),
        (
            "outputs.action-name",
            "Invalid or unknown reference 'outputs.action-name'",
        ),
        ("outputs.action-name.other", "Invalid or unknown reference"),
        ('outputs[""].value', "Invalid or unknown reference"),
        ('outputs.task_a.value[""]', "Invalid or unknown reference"),
        ("outputs.task_a.other", "Invalid or unknown reference"),
        ('has(outputs.task_a.value[""])', r"Invalid 'has\(\)' argument"),
        ("has(outputs.task_a.invalid)", r"Invalid 'has\(\)' argument"),
        ("has(outputs.action-name.invalid)", r"Invalid 'has\(\)' argument"),
        ("has(run.bad)", r"Invalid 'has\(\)' argument"),
    ],
)
def test_validate_and_analyze_cel_invalid(invalid_expr, error_pattern):
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)

        with pytest.raises(ValueError, match=error_pattern):
            cel_utils.validate_and_analyze_cel(invalid_expr)
