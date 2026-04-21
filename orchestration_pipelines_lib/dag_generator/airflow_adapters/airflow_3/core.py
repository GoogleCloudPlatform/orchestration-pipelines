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
"""Module to validate and build pipeline from YAML in Airflow 3."""
import json
from functools import partial
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from . import task_factory
from . import airflow_client_utils
from . import email_utils
import airflow_client.client
from airflow_client.client.rest import ApiException
from airflow_client.client.exceptions import ServiceException
from tenacity import retry, stop_after_attempt, wait_random, retry_if_exception_type
from orchestration_pipelines_lib.dag_generator.airflow_adapters.common_utils import action_handler_registry
from orchestration_pipelines_lib.internal_models.pipeline import PipelineModel
from orchestration_pipelines_lib.internal_models.triggers import ScheduleTriggerModel


@retry(
    stop=stop_after_attempt(3),
    wait=wait_random(min=1, max=10),
    retry=retry_if_exception_type(ServiceException),
)
def _update_metadata(dag_run, dag, additional_notes, dag_run_api,
                     task_instance_api):
    """Updates the DAG run and task instance metadata notes with retry mechanism."""
    # 1. Update/Insert DAG RUN Note
    dag_run_api.patch_dag_run(dag_id=dag_run.dag_id,
                              dag_run_id=dag_run.run_id,
                              dag_run_patch_body={"note": additional_notes},
                              update_mask=["note"])

    # 2. Update/Insert TASK INSTANCE Note
    task_instances = task_instance_api.get_task_instances(
        dag_id=dag_run.dag_id, dag_run_id=dag_run.run_id).task_instances

    existing_notes_map = {
        (ti.task_id, ti.map_index): ti.note
        for ti in task_instances
    }

    doc_md_map = {t.task_id: t.doc_md for t in dag.tasks}
    entities = []

    for task_instance in task_instances:
        new_content = doc_md_map.get(task_instance.task_id, "")
        if not new_content:
            continue

        existing_note = existing_notes_map.get(
            (task_instance.task_id, task_instance.map_index))

        if existing_note and existing_note == new_content:
            continue

        entities.append({
            'task_id': task_instance.task_id,
            'note': new_content
        })

    if entities:
        batch_body = airflow_client.client.BulkBodyBulkTaskInstanceBody.from_dict(
            {
                "actions": [{
                    "action": "update",
                    "action_on_non_existence": "skip",
                    "entities": entities
                }]
            })
        task_instance_api.bulk_task_instances(
            dag_id=dag_run.dag_id,
            dag_run_id=dag_run.run_id,
            bulk_body_bulk_task_instance_body=batch_body)


def init_orchestration_pipeline_context(note_content: str, **context):
    """Initializes the orchestration pipeline context for a DAG run.

    Extracts specific metadata from the provided notes content and applies
    it to the DAG Run and its Task Instances via the Airflow API.

    Args:
        note_content: The JSON string containing the DAG documentation and metadata.
        **context: The Airflow task execution context.

    Raises:
        ApiException: If the Airflow API client fails to update the metadata.
    """
    dag_run = context.get("dag_run")
    dag = context.get("dag")

    # Filter note_content to keep only specific fields
    additional_notes = ""
    if note_content:
        notes_data = json.loads(note_content)
        if isinstance(notes_data, dict):
            allowed_keys = [
                "op_bundle",
                "op_version",
                "op_pipeline",
                "op_owner",
                "op_origination",
                "op_deployment_details",
                "op_repository",
                "op_branch",
                "op_commit_sha",
                "op_is_current",
            ]
            notes_dict = {
                k: v
                for k, v in notes_data.items() if k in allowed_keys
            }
            if notes_dict:
                additional_notes = json.dumps(notes_dict, indent=4)

    api_client = airflow_client_utils.get_airflow_api_client()
    dag_run_api = airflow_client.client.DagRunApi(api_client)
    task_instance_api = airflow_client.client.TaskInstanceApi(api_client)

    try:
        _update_metadata(dag_run, dag, additional_notes, dag_run_api,
                         task_instance_api)
    except ApiException as e:
        print(
            f"Failed when calling Airflow Python Client API during metadata application: {e}"
        )
        raise


def generate(pipeline: PipelineModel, tags: list[str], dag_notes: str,
             data_root: str) -> DAG:
    """Generates the Airflow DAG for the given pipeline model.

    Args:
        pipeline: The parsed pipeline model.
        tags: A list of tags to apply to the generated DAG.
        dag_notes: The markdown documentation/notes for the DAG.
        data_root: The root directory for the pipeline data used for template search.

    Returns:
        The fully constructed Airflow DAG.

    Raises:
        ValueError: If a task dependency cannot be resolved.
    """
    # Defines list of non-relative path to the additional folders where jinja will look for
    # templates. For example, .txt file format is treated as jinja template. By default,
    # it searches in /home/airflow/gcs/dags folder first.
    template_searchpath = []
    if data_root:
        template_searchpath.append(data_root)

    action_handlers = action_handler_registry.get_action_handlers(task_factory)

    schedule_trigger = next(
        (t for t in pipeline.triggers if isinstance(t, ScheduleTriggerModel)),
        None)

    dag_kwargs = {
        "dag_id": pipeline.metadata.pipelineId,
        "description": pipeline.metadata.description,
        "default_args": {
            "owner": pipeline.metadata.owner,
            "retries": pipeline.defaults.executionConfigDefault.retries,
        },
        "tags": tags,
        "template_searchpath": template_searchpath,
    }

    if pipeline.notifications and pipeline.notifications.onPipelineFailure:
        emails = pipeline.notifications.onPipelineFailure.email
        on_failure_callback = partial(
            email_utils.send_failure_notification_email, emails)
        dag_kwargs["on_failure_callback"] = on_failure_callback

    if schedule_trigger:
        task_factory.create_schedule_trigger_task(dag_kwargs, schedule_trigger)
    else:
        dag_kwargs["schedule"] = None

    dag_kwargs["doc_md"] = dag_notes

    dag = DAG(**dag_kwargs)

    _ = PythonOperator(task_id="init_orchestration_pipeline_context",
                       python_callable=init_orchestration_pipeline_context,
                       op_args=[dag_notes],
                       dag=dag)

    tasks = {}
    # 2. Create tasks in a task group and explicitly associate them with the dag
    for action in pipeline.actions:
        handler = action_handlers.get(type(action))
        if handler:
            # IMPORTANT: Ensure your handler passes 'dag=dag' to the Operator constructor
            task_obj = handler(action, pipeline, dag=dag)
            tasks[action.name] = task_obj

    # 3. Add cross-task dependencies
    for action in pipeline.actions:
        if action.dependsOn and action.name in tasks:
            current_task = tasks[action.name]
            for dep_name in action.dependsOn:
                if dep_name not in tasks:
                    raise ValueError(
                        f"Task {dep_name} being upstream dependency for "
                        f"{action.name} does not exist.")

                upstream_task = tasks[dep_name]
                # Relationships are safely set on the objects directly
                current_task.set_upstream(upstream_task)
    return dag


def get_actively_running_versions(pipeline_id: str,
                                  bundle_id: str) -> list[str]:
    """Retrieves a list of actively running versions for a given pipeline.

    Queries the Airflow API to find any DAG runs currently in 'running' or
    'queued' states that match the bundle and pipeline ID pattern.
    """
    active_states = ["running", "queued"]
    prefix = f"{bundle_id}__v__"
    suffix = f"__{pipeline_id}"

    version_ids = set()

    api_client = airflow_client_utils.get_airflow_api_client()
    dag_run_api = airflow_client.client.DagRunApi(api_client)

    try:
        response = dag_run_api.get_dag_runs(
            dag_id="~",
            state=active_states,
        )

        if response.dag_runs:
            for run in response.dag_runs:
                dag_id = run.dag_id

                if dag_id.startswith(prefix) and dag_id.endswith(suffix):
                    version = dag_id.removeprefix(prefix).removesuffix(suffix)
                    version_ids.add(version)
    except ApiException as e:
        print(f"Exception when calling DagRunApi->get_dag_runs: {e}")

    return list(version_ids)


def get_previous_default_versions(pipeline_id: str,
                                  bundle_id: str) -> list[str]:
    """Retrieves a list of previous default versions for a given pipeline.

    Queries the Airflow API for DAGs tagged as current for the specific
    bundle and pipeline.
    """
    api_client = airflow_client_utils.get_airflow_api_client()
    dag_api = airflow_client.client.DAGApi(api_client)

    versions = set()
    try:
        response = dag_api.get_dags(tags=[
            "op:is_current", f"op:bundle:{bundle_id}",
            f"op:pipeline:{pipeline_id}"
        ],
                                    tags_match_mode="all")

        if response.dags:
            for dag in response.dags:
                if dag.tags:
                    for tag in dag.tags:
                        if hasattr(
                                tag,
                                "name") and tag.name.startswith("op:version:"):
                            version_id = tag.name.split("op:version:")[1]
                            if version_id:
                                versions.add(version_id)
    except ApiException as e:
        print(f"Exception when calling DAGApi->get_dags: {e}")

    return list(versions)
