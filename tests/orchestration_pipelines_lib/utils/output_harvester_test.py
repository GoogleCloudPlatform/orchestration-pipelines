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
"""Unit tests for orchestration_pipelines_lib.utils.output_harvester."""

from __future__ import annotations

import dataclasses
import datetime
import decimal
import json
import os
import pathlib
import types
from typing import Any
from unittest import mock

import pytest
from google.api_core import exceptions as gcp_exceptions

from orchestration_pipelines_lib.utils import file_manager, output_harvester


@dataclasses.dataclass
class _FakeBlob:
    data: bytes | None = None
    reloaded_size: int | None = None
    size: int | None = None
    error: Exception | None = None

    def exists(self) -> bool:
        return self.data is not None or self.reloaded_size is not None

    def reload(self) -> None:
        self.size = self.reloaded_size or (
            len(self.data) if self.data is not None else None
        )

    def upload_from_string(self, data: bytes | str, **_: Any) -> None:
        self.data = data.encode() if isinstance(data, str) else data
        self.size = len(self.data)

    def download_as_bytes(self) -> bytes:
        if self.error is not None:
            raise self.error
        if self.data is None:
            raise gcp_exceptions.NotFound("Blob not found")
        return self.data


def _make_op(
    action_type: str | None = None,
    *,
    task_id: str = "step_a",
    dag_id: str = "dag_a",
    action_attrs: dict[str, Any] | None = None,
    **op_attrs: Any,
) -> types.SimpleNamespace:
    model = (
        types.SimpleNamespace(type=action_type, **(action_attrs or {}))
        if action_type
        else None
    )
    return types.SimpleNamespace(
        task_id=task_id, dag_id=dag_id, _op_action_model=model, **op_attrs
    )


@pytest.fixture
def fake_gcs(monkeypatch):
    blobs: dict[str, _FakeBlob] = {}
    client = types.SimpleNamespace(
        bucket=lambda b: types.SimpleNamespace(
            blob=lambda n: blobs.setdefault(f"gs://{b}/{n}", _FakeBlob())
        )
    )
    monkeypatch.setattr(file_manager, "_get_gcs_client", lambda: client)
    return blobs


def test_normalize_json_value_converts_primitives_and_non_finite_numbers():
    raw = {
        1: decimal.Decimal("42.00"),
        "ratio": decimal.Decimal("3.25"),
        "nan_dec": decimal.Decimal("NaN"),
        "inf_dec": decimal.Decimal("Infinity"),
        "nan_flt": float("nan"),
        "inf_flt": float("-inf"),
        "ts": datetime.datetime(2026, 10, 22, 12, 30, tzinfo=datetime.UTC),
        "dt": datetime.date(2026, 10, 22),
        "tm": datetime.time(8, 15),
        "raw_bytes": b"hello",
        "items": (1, True, None),
        "tags": frozenset({"b", "a"}),
        "custom": pathlib.PurePosixPath("/a/b"),
    }

    normalized = output_harvester.normalize_json_value(raw)

    assert normalized == {
        "1": 42,
        "ratio": 3.25,
        "nan_dec": None,
        "inf_dec": None,
        "nan_flt": None,
        "inf_flt": None,
        "ts": "2026-10-22T12:30:00+00:00",
        "dt": "2026-10-22",
        "tm": "08:15:00",
        "raw_bytes": "hello",
        "items": [1, True, None],
        "tags": ["a", "b"],
        "custom": "/a/b",
    }
    assert type(normalized["1"]) is int


def test_pack_and_unpack_inline_and_legacy_envelopes(monkeypatch):
    monkeypatch.delenv("GCS_BUCKET", raising=False)
    operator = _make_op(dag_id="pipe_1")
    user_dict_with_extra = {"value": 1, "metadata": {}, "extra": True}

    inline = output_harvester.pack_output_envelope(
        {"count": 5}, {"job_id": "j1"}, {}, operator
    )
    large_no_bucket = output_harvester.pack_output_envelope(
        {"blob": "x" * 11_000}, {}, {}, operator
    )

    assert inline == {"value": {"count": 5}, "metadata": {"job_id": "j1"}}
    assert output_harvester.unpack_output_envelope(inline) == inline
    assert "value" in large_no_bucket
    assert output_harvester.unpack_output_envelope(None) is None
    assert output_harvester.unpack_output_envelope("plain") == {
        "value": "plain",
        "metadata": {},
    }
    assert output_harvester.unpack_output_envelope(user_dict_with_extra) == {
        "value": user_dict_with_extra,
        "metadata": {},
    }


def test_pack_and_unpack_gcs_offload_and_missing_blob(monkeypatch, fake_gcs):
    monkeypatch.setenv("GCS_BUCKET", "gs://env-bucket/")
    operator = _make_op(dag_id="pipe_1")
    ctx = {"run_id": "manual/2026", "ti": types.SimpleNamespace(try_number=2)}
    overhead = len('{"value":"","metadata":{}}')
    below_10kb = "x" * (output_harvester.XCOM_DB_THRESHOLD_BYTES - overhead - 1)
    exact_10kb = "x" * (output_harvester.XCOM_DB_THRESHOLD_BYTES - overhead)

    inline = output_harvester.pack_output_envelope(
        below_10kb, {}, ctx, operator
    )
    pointer = output_harvester.pack_output_envelope(
        exact_10kb, {"big_meta": "m" * 200}, ctx, operator
    )

    expected_uri = (
        "gs://env-bucket/data/task_outputs/"
        "pipe_1/manual_2026/step_a/2/xcom_offload.json"
    )
    assert inline == {"value": below_10kb, "metadata": {}}
    assert pointer == {output_harvester.OFFLOAD_URI_KEY: expected_uri}
    assert output_harvester.unpack_output_envelope(pointer) == {
        "value": exact_10kb,
        "metadata": {"big_meta": "m" * 200},
    }
    missing = {output_harvester.OFFLOAD_URI_KEY: "gs://env-bucket/missing.json"}
    assert output_harvester.unpack_output_envelope(missing) is None

    unavailable_uri = "gs://env-bucket/down.json"
    fake_gcs[unavailable_uri] = _FakeBlob(
        error=gcp_exceptions.ServiceUnavailable("GCS down")
    )
    with pytest.raises(gcp_exceptions.ServiceUnavailable, match="GCS down"):
        output_harvester.unpack_output_envelope(
            {output_harvester.OFFLOAD_URI_KEY: unavailable_uri}
        )


def test_pack_output_envelope_enforces_1mb_boundary():
    operator = _make_op(task_id="huge_task", dag_id="pipe_1")
    overhead = len('{"value":"","metadata":{}}')
    exact_1mb = "a" * (output_harvester.XCOM_MAX_LIMIT_BYTES - overhead)

    assert (
        output_harvester.pack_output_envelope(exact_1mb, {}, {}, operator)[
            "value"
        ]
        == exact_1mb
    )
    with pytest.raises(ValueError, match="exceeds the 1 MB limit"):
        output_harvester.pack_output_envelope(exact_1mb + "a", {}, {}, operator)


@pytest.mark.parametrize(
    ("deps_bucket", "filename", "expected_bucket"),
    [
        ("gs://staging-bkt/sub/dir/", "gs://file-bkt/job.py", "staging-bkt"),
        (None, "gs://file-bkt/scripts/job.py", "file-bkt"),
        (None, "local_job.py", "env-bkt"),
    ],
)
def test_inject_dataproc_output_uri_serverless_and_gce(
    monkeypatch, deps_bucket, filename, expected_bucket
):
    monkeypatch.setenv("GCS_BUCKET", "env-bkt")
    ctx = {"run_id": "r1", "ti": types.SimpleNamespace(try_number=1)}
    shared_props = {"spark.app.name": "demo"}
    attrs = {"depsBucket": deps_bucket, "filename": filename}
    batch_op = _make_op(
        "pyspark",
        task_id="spark_task",
        action_attrs=attrs,
        batch={"runtime_config": {"properties": shared_props}},
    )
    gce_op = _make_op(
        "pyspark",
        task_id="spark_task",
        action_attrs=attrs,
        job={"pyspark_job": None},
    )

    output_harvester.inject_dataproc_output_uri(batch_op, ctx)
    output_harvester.inject_dataproc_output_uri(gce_op, ctx)

    expected_uri = (
        f"gs://{expected_bucket}/data/task_outputs/"
        "dag_a/r1/spark_task/1/output.json"
    )
    expected_props = {
        "spark.dataproc.driverEnv.PIPELINE_OUTPUT_URI": expected_uri,
        "spark.driverEnv.PIPELINE_OUTPUT_URI": expected_uri,
        "spark.yarn.appMasterEnv.PIPELINE_OUTPUT_URI": expected_uri,
    }
    assert shared_props == {"spark.app.name": "demo"}
    assert batch_op.batch["runtime_config"]["properties"] == {
        "spark.app.name": "demo",
        **expected_props,
    }
    assert gce_op.job["pyspark_job"]["properties"] == expected_props


@pytest.mark.parametrize(
    ("action_type", "op_attrs"),
    [
        ("sql", {"job": {"spark_sql_job": {}}}),
        ("pyspark", {"_op_is_output_task": False, "batch": {}}),
    ],
)
def test_inject_dataproc_output_uri_skips_ineligible_tasks(
    monkeypatch, action_type, op_attrs
):
    monkeypatch.setenv("GCS_BUCKET", "env-bkt")
    op = _make_op(action_type, **op_attrs)

    output_harvester.inject_dataproc_output_uri(op, {})

    assert not hasattr(op, "_op_pipeline_output_uri")


@pytest.mark.parametrize(
    ("fetch_value", "rows", "failed", "expected_value"),
    [
        (True, [{"cnt": 7}], False, {"cnt": 7}),
        (True, [], False, {}),
        (False, [{"cnt": 7}], False, None),
        (True, [{"cnt": 7}], True, None),
    ],
)
def test_extract_bigquery_output_respects_fetch_flag_and_empty_rows(
    fetch_value, rows, failed, expected_value
):
    fake_job = types.SimpleNamespace(
        num_dml_affected_rows=12,
        total_bytes_billed=2048,
        result=lambda max_results=1: rows[:max_results],
    )
    operator = _make_op(
        "operation",
        action_attrs={"fetchOutputValue": fetch_value},
        project_id="proj",
        location="US",
        _op_engine="BIGQUERY",
        hook=types.SimpleNamespace(get_job=lambda **_: fake_job),
    )

    envelope = output_harvester.extract_and_pack_output(
        operator, {}, "bq_job_99", failed=failed
    )

    assert envelope == {
        "value": expected_value,
        "metadata": {
            "job_id": "bq_job_99",
            "rows_written": 12,
            "bytes_billed": 2048,
        },
    }


def test_extract_bigquery_output_defaults_fetch_false_and_handles_hook_error():
    operator = _make_op(
        "airflow_task",
        _op_engine="BIGQUERY",
        hook=types.SimpleNamespace(
            get_job=mock.Mock(side_effect=RuntimeError("API error"))
        ),
    )

    assert output_harvester.extract_and_pack_output(
        operator, {}, "bq_job_err"
    ) == {
        "value": None,
        "metadata": {
            "job_id": "bq_job_err",
            "rows_written": 0,
            "bytes_billed": 0,
        },
    }


def test_extract_dataproc_serverless_and_gce_outputs(fake_gcs):
    ok_uri = "gs://bkt/data/task_outputs/d/r/t/1/output.json"
    fake_gcs[ok_uri] = _FakeBlob(json.dumps({"rows": 99}).encode())
    fake_gcs["gs://bkt/bad.json"] = _FakeBlob(b"{not-json")
    batch_op = _make_op(
        "pyspark",
        batch_id="init-batch",
        _op_engine="DATAPROC",
        _op_pipeline_output_uri=ok_uri,
    )
    gce_op = _make_op(
        "pyspark",
        job_id="gce-job-1",
        _op_pipeline_output_uri="gs://bkt/bad.json",
    )

    batch_env = output_harvester.extract_and_pack_output(
        batch_op,
        {},
        {"name": "projects/p/locations/l/batches/final-batch-1"},
    )
    gce_env = output_harvester.extract_and_pack_output(gce_op, {}, "gce-job-2")

    assert batch_env == {
        "value": {"rows": 99},
        "metadata": {"batch_id": "final-batch-1"},
    }
    assert gce_env == {"value": None, "metadata": {"job_id": "gce-job-2"}}


def test_extract_dataproc_output_rejects_oversized_blob_after_reload(fake_gcs):
    fake_gcs["gs://bkt/oversized.json"] = _FakeBlob(
        reloaded_size=output_harvester.XCOM_MAX_LIMIT_BYTES + 10
    )
    batch_op = _make_op(
        batch_id="b1",
        _op_engine="DATAPROC",
        _op_pipeline_output_uri="gs://bkt/oversized.json",
    )

    with pytest.raises(ValueError, match="exceeds the 1 MB limit"):
        output_harvester.extract_and_pack_output(batch_op, {}, {})


def test_extract_dataform_metadata_on_failure_via_xcom():
    actions = [
        types.SimpleNamespace(
            target=types.SimpleNamespace(name="tbl_ok"), state=4
        ),
        types.SimpleNamespace(
            target=types.SimpleNamespace(name="tbl_err"), state="FAILED"
        ),
    ]
    dataform_op = _make_op(
        "dataform_pipeline",
        project_id="p",
        region="us-central1",
        repository_id="repo",
        hook=types.SimpleNamespace(
            query_workflow_invocation_actions=lambda **_: actions
        ),
    )
    ti = types.SimpleNamespace(
        xcom_pull=lambda **_: {"workflow_invocation_id": "inv_failed"}
    )

    assert output_harvester.extract_and_pack_output(
        dataform_op, {"ti": ti}, None, failed=True
    ) == {
        "value": None,
        "metadata": {
            "invocation_id": (
                "projects/p/locations/us-central1/repositories/"
                "repo/workflowInvocations/inv_failed"
            ),
            "failed_actions": ["tbl_err"],
            "successful_actions": ["tbl_ok"],
        },
    }


def test_extract_dbt_metadata_fresh_and_stale_run_results(tmp_path):
    target_dir = tmp_path / "target"
    target_dir.mkdir()
    results_file = target_dir / "run_results.json"
    results_file.write_text(
        json.dumps(
            {
                "metadata": {"invocation_id": "dbt-inv-7"},
                "results": [
                    {"unique_id": "model.pkg.m1", "status": "success"},
                    {"unique_id": "model.pkg.m2", "status": "runtime error"},
                ],
            }
        ),
        encoding="utf-8",
    )
    dbt_op = _make_op("dbt_pipeline", op_kwargs={"project_dir": str(tmp_path)})

    fresh = output_harvester.extract_and_pack_output(
        dbt_op, {"run_id": None}, None
    )
    os.utime(results_file, (1_000_000, 1_000_000))
    later_ti = types.SimpleNamespace(
        start_date=datetime.datetime(2026, 10, 22, tzinfo=datetime.UTC)
    )
    stale = output_harvester.extract_and_pack_output(
        dbt_op, {"run_id": "cur_run", "ti": later_ti}, None, failed=True
    )

    assert fresh["metadata"] == {
        "run_id": "dbt-inv-7",
        "failed_actions": ["model.pkg.m2"],
        "successful_actions": ["model.pkg.m1"],
    }
    assert stale["metadata"] == {
        "run_id": "cur_run",
        "failed_actions": [],
        "successful_actions": [],
    }


def test_extract_dts_metadata_via_lazy_hook_and_unrendered_template(
    monkeypatch,
):
    from airflow.providers.google.cloud.hooks import bigquery_dts

    transfer_run = types.SimpleNamespace(
        state=4,
        start_time="2026-10-22T01:00:00Z",
        end_time="2026-10-22T01:05:00Z",
    )
    monkeypatch.setattr(
        bigquery_dts,
        "BiqQueryDataTransferServiceHook",
        mock.Mock(
            return_value=types.SimpleNamespace(
                get_transfer_run=lambda **_: transfer_run
            )
        ),
    )
    dts_sensor = _make_op(
        run_id="dts_run_1",
        transfer_config_id="cfg_1",
        project_id="proj",
        gcp_cloud_conn_id="google_cloud_default",
        _op_action_type="DATA_INGESTION",
        _op_engine="BIGQUERY",
    )
    unrendered_dts = _make_op(
        "data_ingestion", run_id="{{ task_instance.xcom_pull('x') }}"
    )

    assert output_harvester.extract_and_pack_output(dts_sensor, {}, None) == {
        "value": None,
        "metadata": {
            "run_id": "dts_run_1",
            "run_state": "SUCCEEDED",
            "start_time": "2026-10-22T01:00:00Z",
            "end_time": "2026-10-22T01:05:00Z",
        },
    }
    assert output_harvester.extract_and_pack_output(
        unrendered_dts, {}, None, failed=True
    )["metadata"] == {
        "run_id": None,
        "run_state": "FAILED",
        "start_time": None,
        "end_time": None,
    }


def test_extract_python_output_on_success_and_failure():
    py_op = _make_op("script")

    assert output_harvester.extract_and_pack_output(
        py_op, {}, {"batch_tag": "b1"}
    ) == {"value": {"batch_tag": "b1"}, "metadata": {}}
    assert output_harvester.extract_and_pack_output(
        py_op, {}, {"batch_tag": "b1"}, failed=True
    ) == {"value": None, "metadata": {}}
