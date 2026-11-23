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
"""Unit tests for runtime context, dynamic binding resolution, and mixin."""

from __future__ import annotations

import copy
import dataclasses
import datetime
import json
import types
from typing import Any

import pytest
import yaml
from google.auth import credentials as auth_credentials
from google.cloud import storage

from orchestration_pipelines_lib.internal_models import (
    pipeline as internal_pipeline,
)
from orchestration_pipelines_lib.internal_models.converters import (
    v1_model_converter,
)
from orchestration_pipelines_lib.utils import (
    dict_utils,
    file_manager,
    runtime_resolver,
    versioned_file_manager,
)
from orchestration_pipelines_lib.utils.runtime_resolver import (
    AirflowSkipException,
)
from orchestration_pipelines_models.pipeline_v1_model import (
    orchestration_pipeline_builder,
)
from orchestration_pipelines_models.pipeline_v1_model.protos import (
    orchestration_pipeline_pb2,
)


def _make_ctx(
    conf: dict[str, Any] | None = None,
    xcom_map: dict[str, Any] | None = None,
    **extra: Any,
) -> dict[str, Any]:
    dag_run = types.SimpleNamespace(
        run_id="run_2026_01",
        logical_date=datetime.datetime(
            2026, 10, 22, 12, 0, 0, tzinfo=datetime.UTC
        ),
        conf=conf or {},
    )
    data = xcom_map or {}
    ti = types.SimpleNamespace(
        xcom_pull=lambda task_ids=None, key=None: data.get(task_ids)
    )
    return {"dag_run": dag_run, "ti": ti, "params": {}, **extra}


def _build_pipeline(
    raw_yaml: str,
    data_root: str = ".",
    fm: file_manager.FileManager | None = None,
) -> internal_pipeline.PipelineModel:
    spec = {
        "modelVersion": "1.0",
        "pipelineId": "test_pipe",
        "owner": "test-owner",
        "runner": "airflow",
        "defaults": {"projectId": "p", "location": "us-central1"},
        **yaml.safe_load(raw_yaml),
    }
    proto = orchestration_pipeline_builder.OrchestrationPipelineBuilder.build(
        spec
    )
    mgr = fm or file_manager.FileManager(
        gcs_client=storage.Client(
            project="p", credentials=auth_credentials.AnonymousCredentials()
        ),
        data_root=data_root,
    )
    return v1_model_converter.ConverterV1ToInternal(
        mgr
    ).convert_to_internal_model(proto)


def _sample_pipeline() -> Any:
    return types.SimpleNamespace(
        parameters={
            "num": internal_pipeline.ParameterModel(type="number", default=5),
            "flag": internal_pipeline.ParameterModel(
                type="boolean", default=False
            ),
            "tags": internal_pipeline.ParameterModel(
                type="array", default=["a"]
            ),
        }
    )


def test_coerce_to_engine_str_and_indexed_proto_path():
    assert dict_utils.coerce_to_engine_str(True) == "true"
    assert dict_utils.coerce_to_engine_str(False) == "false"
    assert dict_utils.coerce_to_engine_str(["a", 2]) == '["a", 2]'
    assert dict_utils.coerce_to_engine_str(42) == "42"
    with pytest.raises(TypeError, match="null"):
        dict_utils.coerce_to_engine_str(None)

    dbt_msg = orchestration_pipeline_pb2.DbtFrameworkSpec(
        invocations=[
            orchestration_pipeline_pb2.DbtInvocation(
                command="run", target="dev"
            )
        ]
    )
    dict_utils.set_proto_path(dbt_msg, "invocations[0].target", "prod")
    assert dbt_msg.invocations[0].target == "prod"


def test_build_cel_eval_context_merges_sources_and_pulls_selective_xcoms():
    pulled: list[Any] = []
    dag = types.SimpleNamespace(
        task_dict={
            "skipped_up": 1,
            "dp_step.dp_step_submit_job": 1,
            "dts_up.dts_up_start": 1,
            "dts_up.dts_up_sensor": 1,
            "unused_dep": 1,
        }
    )
    op = types.SimpleNamespace(
        dag=dag,
        _op_pipeline_model=_sample_pipeline(),
        _op_action_model=types.SimpleNamespace(
            dependsOn=["skipped_up", "dp_step", "dts_up", "unused_dep"],
            runIf="has(outputs.skipped_up) || outputs.dp_step.value.rows > 0",
            dynamicBindings={"params.x": "outputs.dts_up.metadata.run_state"},
        ),
    )
    ev1 = types.SimpleNamespace(timestamp=None, extra={"tags": ["old"]})
    ev2 = types.SimpleNamespace(
        timestamp=datetime.datetime(2026, 10, 22, 11, tzinfo=datetime.UTC),
        extra={"params": {"tags": ("x", 2.0)}},
    )
    xcoms = {
        "skipped_up": None,
        "dp_step.dp_step_submit_job": {"value": {"rows": 3}, "metadata": {}},
        "dts_up.dts_up_sensor": {
            "value": None,
            "metadata": {"run_state": "SUCCEEDED"},
        },
        "dts_up.dts_up_start": "must_not_be_pulled",
        "unused_dep": {"value": "must_not_be_pulled", "metadata": {}},
    }
    ctx = _make_ctx(
        conf={"num": 12.0, "flag": True, "undeclared_key": "ignored"},
        triggering_asset_events={"gs://bkt/in": [ev2, ev1]},
    )
    ctx["ti"].xcom_pull = lambda task_ids=None, key=None: (
        pulled.append(task_ids) or xcoms.get(task_ids)
    )
    eval_ctx = runtime_resolver.build_cel_eval_context(op, ctx)

    assert set(pulled) == {
        "skipped_up",
        "dp_step.dp_step_submit_job",
        "dts_up.dts_up_sensor",
    }
    assert eval_ctx["parameters"] == {"num": 12, "flag": True, "tags": ["x", 2]}
    assert type(eval_ctx["parameters"]["num"]) is int
    assert eval_ctx["run"] == {
        "id": "run_2026_01",
        "trigger_time": "2026-10-22T12:00:00Z",
        "trigger_date": "2026-10-22",
    }
    assert eval_ctx["outputs"] == {
        "dp_step": {"value": {"rows": 3}, "metadata": {}},
        "dts_up": {"value": None, "metadata": {"run_state": "SUCCEEDED"}},
    }


@pytest.mark.parametrize(
    "bad_conf",
    [{"num": True}, {"num": None}, {"num": float("nan")}, {"tags": [{"k": 1}]}],
)
def test_build_cel_eval_context_rejects_invalid_param_types(bad_conf):
    op = types.SimpleNamespace(_op_pipeline_model=_sample_pipeline())
    with pytest.raises(TypeError):
        runtime_resolver.build_cel_eval_context(op, _make_ctx(conf=bad_conf))


def test_internal_models_stay_pure_and_deepcopy_safe(tmp_path):
    internal = _build_pipeline(
        """
        defaults: {projectId: "${parameters.proj}", location: us-central1}
        parameters: {proj: {type: string, default: p1}}
        actions:
          - sql:
              name: q1
              query: {inline: "SELECT 1"}
              engine: {bigquery: {location: US}}
        """,
        data_root=str(tmp_path),
    )
    as_dict = dataclasses.asdict(internal)
    assert not any(k.startswith("_") for k in as_dict)
    assert not any(k.startswith("_") for k in as_dict["actions"][0])
    assert copy.deepcopy(internal) == internal


def test_merge_rendered_sync_kwargs_and_filter_airflow_params():
    op = types.SimpleNamespace(
        location="EU",
        config={
            "extra_key": "keep-me",
            "labels": {"env": "rendered-prod", "run": ""},
            "items": ["rendered-ds", ""],
        },
    )
    base_kw = {
        "location": "{{ var.value.loc }}",
        "config": {
            "labels": {"env": "{{ var.value.env }}", "run": ""},
            "items": ["{{ ds }}", ""],
        },
    }
    resolved_kw = {
        "location": "{{ var.value.loc }}",
        "config": {
            "labels": {"run": "r-1", "env": "{{ var.value.env }}"},
            "items": ["{{ ds }}", "dyn-item"],
        },
    }
    runtime_resolver.sync_operator_kwargs(op, base_kw, resolved_kw)
    assert op.location == "EU"
    assert op.config == {
        "extra_key": "keep-me",
        "labels": {"env": "rendered-prod", "run": "r-1"},
        "items": ["rendered-ds", "dyn-item"],
    }
    assert list(op.config["labels"]) == ["env", "run"]

    filtered = runtime_resolver.filter_airflow_task_params(
        {"task_id": "bad", "trigger_rule": "always", "_priv": 1, "cmd": "ok"}
    )
    assert filtered == {"cmd": "ok"}


def test_apply_dynamic_bindings_defaults_filtering_and_python_sync(tmp_path):
    (tmp_path / "helper.py").write_text("def fn(): pass\n", encoding="utf-8")
    pipeline = _build_pipeline(
        """
        defaults:
          projectId: "${parameters.proj}"
          location: "${parameters.loc}"
          retryPolicy: {maxRetries: "${parameters.retries}"}
        parameters:
          proj: {type: string, default: dyn-proj}
          loc: {type: string, default: europe-west1}
          retries: {type: number, default: 3}
          batch_sz: {type: number, default: 50}
        actions:
          - python:
              name: py_step
              mainFilePath: helper.py
              pythonCallable: fn
              opKwargs:
                static_dt: "{{ ds }}"
                size: "${parameters.batch_sz}"
              engine: {local: {}}
          - sql:
              name: bq_step
              query: {inline: "SELECT 1"}
              engine: {bigquery: {location: "{{ var.value.bq_loc }}"}}
          - dataIngestion:
              name: dts_step
              bigqueryDts:
                transferConfigId: cfg_1
        """,
        data_root=str(tmp_path),
    )
    r_ctx = runtime_resolver.build_runtime_pipeline_context(pipeline)
    py_act, bq_act, dts_act = pipeline.actions

    # Local Python consumes retry defaults, not GCP defaults; DTS_SENSOR skips retry.
    assert [
        k
        for k, _ in runtime_resolver._applicable_defaults_bindings(
            r_ctx, py_act
        )
    ] == ["defaults.retry_policy.max_retries"]
    assert {
        k
        for k, _ in runtime_resolver._applicable_defaults_bindings(
            r_ctx, dts_act, runtime_resolver.TASK_ROLE_DTS_SENSOR
        )
    } == {"defaults.project_id", "defaults.location"}

    def _builder(op, act, dflt, **_):
        if act.name == "py_step":
            return {
                "op_kwargs": dict(act.config.opKwargs or {}),
                "retries": dflt.retryPolicy.maxRetries,
            }
        return {
            "project_id": dflt.cloudDefault.project,
            "location": act.config.location,
        }

    py_op = types.SimpleNamespace(
        task_id="py_step",
        dag=types.SimpleNamespace(
            template_searchpath=[str(tmp_path)], doc_md="{}", task_dict={}
        ),
        op_kwargs={"static_dt": "2026-10-22", "size": ""},
        retries=0,
        hook="stale-hook",
        _op_action_model=copy.deepcopy(py_act),
        _op_pipeline_model=r_ctx,
        _op_kwargs_builder=_builder,
    )
    ctx = _make_ctx(conf={"batch_sz": 120, "retries": 4})
    runtime_resolver.apply_dynamic_bindings(
        py_op, runtime_resolver.build_cel_eval_context(py_op, ctx)
    )
    assert py_op.op_kwargs == {"static_dt": "2026-10-22", "size": 120}
    assert py_op._op_action_model.config.opKwargs == {
        "static_dt": "2026-10-22",
        "size": 120,
    }
    assert py_op.retries == 4
    assert "hook" not in py_op.__dict__

    bq_op = types.SimpleNamespace(
        task_id="bq_step",
        dag=py_op.dag,
        project_id="",
        location="EU",
        _op_action_model=copy.deepcopy(bq_act),
        _op_pipeline_model=r_ctx,
        _op_kwargs_builder=_builder,
    )
    runtime_resolver.apply_dynamic_bindings(
        bq_op, runtime_resolver.build_cel_eval_context(bq_op, ctx)
    )
    assert (bq_op.project_id, bq_op.location) == ("dyn-proj", "EU")

    with pytest.raises(ValueError, match="project_id"):
        bad_ctx = _make_ctx(conf={"proj": ""})
        runtime_resolver.apply_dynamic_bindings(
            bq_op, runtime_resolver.build_cel_eval_context(bq_op, bad_ctx)
        )


def test_reconvert_action_versioned_profiles_and_static_reuse(
    tmp_path, monkeypatch, caplog
):
    monkeypatch.setattr(runtime_resolver, "_default_kwargs_builder", None)
    monkeypatch.setattr(
        file_manager.FileManager, "get_blob_reference", lambda _self, p: p
    )
    bundle_dir = tmp_path / "bundle1" / "versions" / "v1"
    bundle_dir.mkdir(parents=True)
    (bundle_dir / "prof_a.yaml").write_text(
        "definition: {runtimeConfig: {version: '2.2'}}",
        encoding="utf-8",
    )
    (bundle_dir / "prof_b.yaml").write_text(
        "definition: {runtimeConfig: {version: '2.3'}}",
        encoding="utf-8",
    )
    vfm = versioned_file_manager.VersionedFileManager(
        pipeline_id="test_pipe",
        current_version="v1",
        bundle_id="bundle1",
        local_data_root=str(tmp_path),
    )
    pipeline = _build_pipeline(
        """
        parameters:
          prof: {type: string, default: prof_b.yaml}
          reg: {type: string, default: us-east1}
        actions:
          - notebook:
              name: nb_dyn_prof
              mainFilePath: gs://code-bkt/job.ipynb
              stagingBucket: stage-bkt
              engine:
                dataprocServerless:
                  resourceProfile: {path: "${parameters.prof}"}
          - notebook:
              name: nb_static_prof
              mainFilePath: gs://code-bkt/job.ipynb
              stagingBucket: stage-bkt
              params: {resource_profile_name: "${parameters.reg}"}
              engine:
                dataprocServerless:
                  location: "${parameters.reg}"
                  resourceProfile: {path: prof_a.yaml}
        """,
        data_root=str(tmp_path),
        fm=vfm,
    )
    doc = json.dumps(
        {"op_bundle": "bundle1", "op_version": "v1", "op_pipeline": "test_pipe"}
    )
    r_ctx = runtime_resolver.build_runtime_pipeline_context(pipeline)
    dyn_op = types.SimpleNamespace(
        task_id="nb_dyn_prof",
        dag=types.SimpleNamespace(
            template_searchpath=[str(tmp_path)], doc_md=doc, task_dict={}
        ),
        _op_action_model=copy.deepcopy(pipeline.actions[0]),
        _op_pipeline_model=r_ctx,
    )
    with caplog.at_level("WARNING"):
        runtime_resolver.apply_dynamic_bindings(
            dyn_op, runtime_resolver.build_cel_eval_context(dyn_op, _make_ctx())
        )
    assert "No operator kwargs builder registered" in caplog.text
    assert (
        dyn_op._op_action_model.config.resourceProfile.runtimeConfig["version"]
        == "2.3"
    )

    # Delete prof_a.yaml on disk to prove static resourceProfile is reused
    # without I/O even when an unrelated param key contains "resource_profile".
    (bundle_dir / "prof_a.yaml").unlink()
    stat_op = types.SimpleNamespace(
        task_id="nb_static_prof",
        dag=dyn_op.dag,
        _op_action_model=copy.deepcopy(pipeline.actions[1]),
        _op_pipeline_model=r_ctx,
    )
    runtime_resolver.apply_dynamic_bindings(
        stat_op, runtime_resolver.build_cel_eval_context(stat_op, _make_ctx())
    )
    assert stat_op._op_action_model.region == "us-east1"
    assert stat_op._op_action_model.params == {
        "resource_profile_name": "us-east1"
    }
    assert (
        stat_op._op_action_model.config.resourceProfile.runtimeConfig["version"]
        == "2.2"
    )


def test_reconvert_action_rejects_resource_profile_path_traversal(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(runtime_resolver, "_default_kwargs_builder", None)
    monkeypatch.setattr(
        file_manager.FileManager, "get_blob_reference", lambda _self, p: p
    )
    monkeypatch.delenv("GCS_BUCKET", raising=False)
    root = tmp_path / "dags"
    root.mkdir()
    outside = tmp_path / "outside.yaml"
    outside.write_text(
        "definition: {runtimeConfig: {version: '9.9'}}", encoding="utf-8"
    )
    (root / "escape_link.yaml").symlink_to(outside)

    pipeline = _build_pipeline(
        """
        parameters: {prof: {type: string, default: prof.yaml}}
        actions:
          - notebook:
              name: nb_path
              mainFilePath: gs://code-bkt/job.ipynb
              stagingBucket: stage-bkt
              engine: {dataprocServerless: {resourceProfile: {path: "${parameters.prof}"}}}
          - sql:
              name: gce_ext
              query: {inline: "SELECT 1"}
              engine:
                dataprocOnGce:
                  ephemeralCluster:
                    clusterName: c1
                    resourceProfile: {externalConfigPath: "${parameters.prof}"}
        """,
        data_root=str(root),
    )
    r_ctx = runtime_resolver.build_runtime_pipeline_context(pipeline)
    dag = types.SimpleNamespace(
        template_searchpath=str(root), doc_md="{}", task_dict={}
    )

    def _apply(act: Any, prof: str) -> Any:
        op = types.SimpleNamespace(
            task_id=act.name,
            dag=dag,
            _op_action_model=copy.deepcopy(act),
            _op_pipeline_model=r_ctx,
        )
        runtime_resolver.apply_dynamic_bindings(
            op,
            runtime_resolver.build_cel_eval_context(
                op, _make_ctx(conf={"prof": prof})
            ),
        )
        return op

    for act in pipeline.actions:
        for bad in (
            "../outside.yaml",
            "/etc/passwd",
            "escape_link.yaml",
            "gs://any-bucket/prof.yaml",
        ):
            with pytest.raises(
                file_manager.OrchestrationPipelinesInvalidPathError
            ):
                _apply(act, bad)

    monkeypatch.setenv("GCS_BUCKET", "allowed-bkt")
    for bad_gcs in (
        "gs://other-bkt/prof.yaml",
        "gs://allowed-bkt/../secret.yaml",
    ):
        with pytest.raises(file_manager.OrchestrationPipelinesInvalidPathError):
            _apply(pipeline.actions[0], bad_gcs)

    monkeypatch.setattr(
        file_manager.FileManager,
        "read",
        lambda _self, _p: "definition: {runtimeConfig: {version: '2.4'}}",
    )
    ok_op = _apply(pipeline.actions[0], "gs://allowed-bkt/profiles/ok.yaml")
    assert (
        ok_op._op_action_model.config.resourceProfile.runtimeConfig["version"]
        == "2.4"
    )
    for sp in (str(root), iter([str(root)])):
        fm = runtime_resolver._build_runtime_file_manager(
            types.SimpleNamespace(
                dag=types.SimpleNamespace(template_searchpath=sp, doc_md=None)
            )
        )
        assert fm._data_root == str(root)


def test_apply_bindings_idempotent_preserves_static_retry_and_deferred_resume(
    tmp_path,
):
    (tmp_path / "job.py").write_text("def run(): pass\n", encoding="utf-8")
    pipeline = _build_pipeline(
        """
        defaults: {projectId: p, location: us-central1, retryPolicy: {maxRetries: 3}}
        parameters:
          should_run: {type: boolean, default: true}
          batch_sz: {type: number, default: 10}
        actions:
          - python:
              name: py_def
              mainFilePath: job.py
              pythonCallable: run
              runIf: "${parameters.should_run}"
              opKwargs: {static_dt: "{{ ds }}", size: "${parameters.batch_sz}"}
              engine: {local: {}}
        """,
        data_root=str(tmp_path),
    )
    r_ctx = runtime_resolver.build_runtime_pipeline_context(pipeline)
    builder_calls = 0

    def _builder(_op, act, _dflt, **_):
        nonlocal builder_calls
        builder_calls += 1
        return {"op_kwargs": dict(act.config.opKwargs or {})}

    class _Base:
        def resume_execution(self, next_method, next_kwargs, context):
            return {**(next_kwargs or {}), "op_kwargs": dict(self.op_kwargs)}

    class _Op(runtime_resolver.RuntimeResolutionMixin, _Base):
        task_id = "py_def"

        def __init__(self) -> None:
            self.dag = types.SimpleNamespace(
                template_searchpath=str(tmp_path), doc_md="{}", task_dict={}
            )
            self.op_kwargs = {"static_dt": "2026-10-22", "size": ""}
            self._op_pipeline_model = r_ctx
            self._op_action_model = copy.deepcopy(pipeline.actions[0])
            self._op_kwargs_builder = _builder

    # Direct re-application preserves Jinja-rendered op_kwargs and static retryPolicy.
    op = _Op()
    eval_ctx = runtime_resolver.build_cel_eval_context(
        op, _make_ctx(conf={"batch_sz": 42})
    )
    runtime_resolver.apply_dynamic_bindings(op, eval_ctx)
    runtime_resolver.apply_dynamic_bindings(op, eval_ctx)
    assert op.op_kwargs == {"static_dt": "2026-10-22", "size": 42}
    assert op._op_action_model.retryPolicy.maxRetries == 3

    # AF2 (ti.next_method) and AF3 (ti._ti_context_from_server.next_method):
    # pre_execute skips run_if on resume and resume_execution skips re-binding.
    for ti_attr, ti_val in (
        ("next_method", "execute_complete"),
        (
            "_ti_context_from_server",
            types.SimpleNamespace(next_method="execute_complete"),
        ),
    ):
        builder_calls = 0
        resumed_op = _Op()
        ctx = _make_ctx(conf={"should_run": False, "batch_sz": 99})
        setattr(ctx["ti"], ti_attr, ti_val)
        resumed_op.pre_execute(ctx)
        res = resumed_op.resume_execution(
            "execute_complete", {"event": "ok"}, ctx
        )
        assert builder_calls == 2
        assert res == {
            "value": {
                "event": "ok",
                "op_kwargs": {"static_dt": "2026-10-22", "size": 99},
            },
            "metadata": {},
        }


def test_runtime_resolution_mixin_run_if_and_output_wrapping(caplog):
    pushed: dict[str, Any] = {}
    pre_calls: list[Any] = []
    defer_exc = type("TaskDeferred", (Exception,), {})

    class _BaseOp:
        mode = "ok"
        task_id = "py_task"
        doc: str | None = None

        def pre_execute(self, context: Any) -> None:
            pre_calls.append(self.doc)

        def execute(self, context: Any) -> Any:
            if self.mode == "fail":
                raise RuntimeError("boom")
            if self.mode == "skip":
                raise AirflowSkipException("inner skip")
            if self.mode == "trigger":
                raise type("DagRunTriggerException", (Exception,), {})()
            return {"ok": True}

        def execute_complete(self, context: Any, event: Any = None) -> Any:
            if self.mode == "fail":
                raise RuntimeError("resume boom")
            if self.mode == "defer":
                raise defer_exc()
            return {"resumed": event, "doc": self.doc}

        def resume_execution(
            self, next_method: str, next_kwargs: Any, context: Any
        ) -> Any:
            return getattr(self, next_method)(
                context=context, **(next_kwargs or {})
            )

    class _WrappedOp(runtime_resolver.RuntimeResolutionMixin, _BaseOp):
        pass

    bare_op = _WrappedOp()
    bare_op.pre_execute(_make_ctx())
    assert bare_op.execute(_make_ctx()) == {"ok": True}
    assert pre_calls == [None]
    pre_calls.clear()

    static_op = _WrappedOp()
    static_op._op_pipeline_model = _sample_pipeline()
    static_op._op_action_model = types.SimpleNamespace(
        name="static_task", type="script", dependsOn=["up_task"]
    )
    static_ctx = _make_ctx(conf={"num": 8.0})
    static_ctx["ti"].xcom_pull = lambda **_: (_ for _ in ()).throw(
        AssertionError("xcom_pull should not be called for static tasks")
    )
    static_op.pre_execute(static_ctx)
    assert pre_calls == [None]
    pre_calls.clear()

    op = _WrappedOp()
    op._op_pipeline_model = _sample_pipeline()
    op._op_action_model = types.SimpleNamespace(
        name="py_task", type="script", runIf="parameters.flag"
    )

    with pytest.raises(AirflowSkipException, match="run_if"):
        op.pre_execute(_make_ctx(conf={"flag": False}))
    assert pre_calls == []

    op._op_action_model.runIf = "parameters.num"
    with pytest.raises(TypeError, match="must evaluate to bool"):
        op.pre_execute(_make_ctx())

    op._op_action_model.runIf = "parameters.flag"
    run_ctx = _make_ctx(conf={"flag": True})
    run_ctx["ti"].xcom_push = lambda key, value: pushed.__setitem__(key, value)
    op.pre_execute(run_ctx)
    assert op.execute(run_ctx) == {"value": {"ok": True}, "metadata": {}}

    for mode, exc_match in (("skip", "inner skip"), ("trigger", None)):
        op.mode = mode
        with pytest.raises(Exception, match=exc_match):
            op.execute(run_ctx)
        assert not pushed

    op.mode = "fail"
    with pytest.raises(RuntimeError, match="boom"):
        op.execute(run_ctx)
    assert pushed["return_value"] == {"value": None, "metadata": {}}
    pushed.clear()

    dyn_pipeline = _build_pipeline(
        """
        parameters: {doc_val: {type: string, default: restored-doc}}
        actions:
          - airflowTask:
              name: af_def
              operatorClass: airflow.operators.empty.EmptyOperator
              params: {doc: "${parameters.doc_val}"}
        """
    )
    prev_builder = runtime_resolver._default_kwargs_builder
    runtime_resolver.register_operator_kwargs_builder(
        lambda _op, act, _dflt, **_: (
            runtime_resolver.filter_airflow_task_params(act.params)
        )
    )
    try:
        fresh_op = _WrappedOp()
        fresh_op._op_pipeline_model = (
            runtime_resolver.build_runtime_pipeline_context(dyn_pipeline)
        )
        fresh_op._op_action_model = dyn_pipeline.actions[0]
        assert fresh_op.resume_execution(
            "execute_complete", {"event": 7}, run_ctx
        ) == {"value": {"resumed": 7, "doc": "restored-doc"}, "metadata": {}}

        fresh_op.mode = "defer"
        with pytest.raises(defer_exc):
            fresh_op.resume_execution("execute_complete", {"event": 7}, run_ctx)
        assert not pushed

        fresh_op.mode = "fail"
        with pytest.raises(RuntimeError, match="resume boom"):
            fresh_op.resume_execution("execute_complete", {"event": 7}, run_ctx)
        assert pushed["return_value"] == {"value": None, "metadata": {}}

        def _broken_push(**_: Any) -> None:
            raise OSError("db down")

        run_ctx["ti"].xcom_push = _broken_push
        with pytest.raises(RuntimeError, match="resume boom"):
            fresh_op.resume_execution("execute_complete", {"event": 7}, run_ctx)
        assert "Failed to push failure metadata XCom" in caplog.text
    finally:
        runtime_resolver.register_operator_kwargs_builder(prev_builder)
