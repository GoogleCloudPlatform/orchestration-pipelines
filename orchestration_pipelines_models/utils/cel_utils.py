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
"""Lightweight standard-library CEL validator for orchestration pipelines."""

import ast
import re
import warnings
from dataclasses import dataclass

ALLOWED_RUN_FIELDS = frozenset({"id", "trigger_time", "trigger_date"})
ALLOWED_ACTION_OUTPUT_FIELDS = frozenset({"value", "metadata"})
ALLOWED_PIPELINE_ROOTS = frozenset({"parameters", "run", "outputs"})

# Matches a closed single- or double-quoted string literal (including escapes).
_STR_LIT = r"'(?:\\.|[^'\\])*'|\"(?:\\.|[^\"\\])*\""
_CLOSED_STRING_RE = re.compile(_STR_LIT)
# Matches closed or unclosed string literals to mask/skip characters in quotes.
_SPAN_MASK_RE = re.compile(
    r"'(?:\\.|[^'\\])*(?:'|$)|\"(?:\\.|[^\"\\])*(?:\"|$)"
)
# Matches bracket key access like `obj["k"]` (excluding `in [...]` lists).
# Uses _SPAN_MASK_RE as an OR branch to ignore matches inside string literals.
_BRACKET_OR_STR_RE = re.compile(
    rf"(?<!\bin)(?<=[\w\]}}])\s*\[\s*({_STR_LIT})\s*\]"
    rf"|{_SPAN_MASK_RE.pattern}"
)
# Matches top-level CEL roots (`parameters`, `outputs`, `run`) or `has(`.
_CEL_ROOT_RE = re.compile(
    r"(?<![.\w])(?:parameters|outputs|run)\b|(?<!\.)\bhas\s*\("
)
# Matches `outputs.action_id` where the ID might contain hyphens
# (e.g., `outputs.my-task`). Uses _SPAN_MASK_RE as an OR branch to ignore
# matches inside string literals.
_OUTPUTS_DOT_REF_RE = re.compile(
    rf"{_SPAN_MASK_RE.pattern}|(?<![.\w])outputs\s*\.\s*([a-zA-Z0-9_-]+)"
)
# Matches top-level `has(...)` calls and captures the inner argument.
_HAS_CALL_RE = re.compile(r"(?<!\.)\bhas\s*\(([^()]*)\)")
# Matches a valid dotted field path inside `has(...)` (e.g., `outputs.a.value`).
_HAS_ARG_RE = re.compile(r"^\s*([a-zA-Z_]\w*(?:\s*\.\s*[a-zA-Z_]\w*)+)\s*$")
# Matches dotted identifier chains (e.g., `parameters.env`, `run.id`).
_CHAIN_RE = re.compile(r"(?<![.\w])([a-zA-Z_]\w*(?:\s*\.\s*[a-zA-Z_]\w*)*)")


@dataclass(frozen=True)
class CelExpressionAnalysis:
    """Structured analysis result for a validated CEL expression."""

    parameters: frozenset[str]
    run_fields: frozenset[str]
    output_actions: frozenset[str]
    guarded_output_actions: frozenset[str]


def _check_outputs_dot_ref(match: re.Match[str]) -> str:
    action_id = match.group(1)
    if not action_id or action_id.isidentifier():
        return match.group(0)
    warnings.warn(
        f"Non-standard CEL syntax 'outputs.{action_id}'; "
        f'use outputs["{action_id}"] instead.',
        UserWarning,
        stacklevel=3,
    )
    return f'outputs["{action_id}"]'


def _iter_dollar_spans(value: str) -> list[tuple[int, int, bool, str]]:
    """Scans '${...}' spans, returning (start, end, closed, inner)."""
    spans: list[tuple[int, int, bool, str]] = []
    idx = 0
    while (start := value.find("${", idx)) != -1:
        masked_tail = _SPAN_MASK_RE.sub(
            lambda m: " " * len(m.group(0)), value[start + 2 :]
        )
        depth, offset = 1, 0
        while offset < len(masked_tail) and depth > 0:
            char = masked_tail[offset]
            depth += (char == "{") - (char == "}")
            offset += 1
        pos, closed = start + 2 + offset, depth == 0
        spans.append((start, pos, closed, value[start + 2 : pos - closed]))
        idx = pos
    return spans


def _span_has_cel_root(inner: str) -> bool:
    return bool(_CEL_ROOT_RE.search(_CLOSED_STRING_RE.sub("''", inner)))


def has_cel_reference(value: str) -> bool:
    """Returns True if `value` has a '${...}' span referencing a CEL root."""
    return (
        isinstance(value, str)
        and "${" in value
        and any(
            _span_has_cel_root(inner) for *_, inner in _iter_dollar_spans(value)
        )
    )


def extract_cel_expression(
    value: str, *, require_cel_root: bool = True
) -> str | None:
    """Extracts the inner CEL expression from a full-string '${ ... }' value."""
    if not isinstance(value, str) or "${" not in value:
        return None
    stripped = value.strip()
    spans = _iter_dollar_spans(stripped)
    if len(spans) == 1:
        start, end, closed, inner = spans[0]
        if start == 0 and end == len(stripped) and closed:
            if not (inner_stripped := inner.strip()):
                raise ValueError(f"Empty CEL expression in '{value}'.")
            if not require_cel_root or _span_has_cel_root(inner_stripped):
                return inner_stripped
            return None

    relevant = [
        closed
        for _, _, closed, inner in spans
        if not require_cel_root or _span_has_cel_root(inner)
    ]
    if not relevant:
        return None
    if not all(relevant):
        raise ValueError(f"Unclosed '${{...}}' expression in '{value}'.")
    raise ValueError(
        f"Mid-string interpolation is not supported in '{value}'. Wrap the "
        "entire value in '${ ... }' and use CEL '+' concatenation."
    )


def validate_and_analyze_cel(raw_expr: str) -> CelExpressionAnalysis:
    """Validates pipeline root references and extracts referenced symbols."""
    if not isinstance(raw_expr, str) or not (expr := raw_expr.strip()):
        raise ValueError("CEL expression must be a non-empty string.")
    if expr.startswith("${"):
        expr = extract_cel_expression(expr, require_cel_root=False) or ""
    normalized = _OUTPUTS_DOT_REF_RE.sub(_check_outputs_dot_ref, expr)

    key_table: dict[str, str] = {}

    def _intern_or_mask(match: re.Match[str]) -> str:
        if (lit := match.group(1)) is None:
            return "''"
        token = f"__op_key_{len(key_table)}__"
        key_table[token] = ast.literal_eval(lit)
        return f".{token}"

    indexed = re.sub(
        r"\s*\.\s*", ".", _BRACKET_OR_STR_RE.sub(_intern_or_mask, normalized)
    )
    params, run_fields, outputs, guarded = set(), set(), set(), set()

    items: list[tuple[list[str], bool]] = [
        (arg_match.group(1).split("."), True)
        for raw_arg in _HAS_CALL_RE.findall(indexed)
        if (arg_match := _HAS_ARG_RE.match(raw_arg))
    ]
    # Replace `has(...)` calls and strip `<func>(` prefixes so `_CHAIN_RE`
    # inspects function arguments without matching function names.
    cleaned = re.sub(
        r"\.?[a-zA-Z_]\w*\s*\(", "(", _HAS_CALL_RE.sub("true", indexed)
    )
    for chain_match in _CHAIN_RE.finditer(cleaned):
        items.append((chain_match.group(1).split("."), False))

    for raw_parts, in_has in items:
        parts = [key_table.get(p, p) for p in raw_parts]
        root = parts[0]
        if root not in ALLOWED_PIPELINE_ROOTS:
            continue
        if all(parts) and len(parts) >= 2:
            if root == "parameters":
                params.add(parts[1])
                continue
            if (
                root == "run"
                and len(parts) == 2
                and parts[1] in ALLOWED_RUN_FIELDS
            ):
                run_fields.add(parts[1])
                continue
            if root == "outputs" and (
                (in_has and len(parts) == 2)
                or (
                    len(parts) >= 3 and parts[2] in ALLOWED_ACTION_OUTPUT_FIELDS
                )
            ):
                outputs.add(parts[1])
                if in_has:
                    guarded.add(parts[1])
                continue
        ref = ".".join(parts)
        raise ValueError(
            f"Invalid 'has()' argument '{ref}' in '{expr}'."
            if in_has
            else f"Invalid or unknown reference '{ref}' in CEL: '{expr}'."
        )

    return CelExpressionAnalysis(
        parameters=frozenset(params),
        run_fields=frozenset(run_fields),
        output_actions=frozenset(outputs),
        guarded_output_actions=frozenset(guarded),
    )
