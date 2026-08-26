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

from typing import TYPE_CHECKING

from orchestration_pipelines_lib.dag_generator.airflow_adapters.common_utils import (  # noqa: E501
    dag_utils,
)

from . import airflow_client_utils, email_utils, task_factory

if TYPE_CHECKING:
    from airflow.models import DagRun
    from airflow_client.client import (
        DagRunApi,
        TaskInstanceApi,
        TaskInstanceResponse,
    )


def get_deps() -> dag_utils.AirflowVersionedDependencies:
    """Returns Airflow 3 specific dependencies for DAG generation."""
    from airflow.providers.standard.operators.python import PythonOperator

    return dag_utils.AirflowVersionedDependencies(
        task_factory=task_factory,
        emails_callback=email_utils.send_notification_email,
        init_pipeline_context=init_orchestration_pipeline_context,
        init_task_operator=PythonOperator,
    )


def _update_metadata(
    dag_run, dag, additional_notes, dag_run_api, task_instance_api
):
    """Updates DAG run and task instance metadata notes with retry."""
    from airflow_client.client.exceptions import ServiceException
    from tenacity import (
        retry,
        retry_if_exception_type,
        stop_after_attempt,
        wait_random,
    )

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_random(min=1, max=10),
        retry=retry_if_exception_type(ServiceException),
    )
    def _do_update():
        _set_additional_notes(dag_run, additional_notes, dag_run_api)
        task_instances = _get_task_instances(dag_run, task_instance_api)

        doc_md_map = {t.task_id: t.doc_md for t in dag.tasks}
        entities = _calculate_updates(task_instances, doc_md_map)

        if entities:
            _update_task_instances(entities, dag_run, task_instance_api)

    _do_update()


def _set_additional_notes(
    dag_run: "DagRun",
    additional_notes: str,
    dag_run_api: "DagRunApi",
):
    dag_run_api.patch_dag_run(
        dag_id=dag_run.dag_id,
        dag_run_id=dag_run.run_id,
        dag_run_patch_body={"note": additional_notes},
        update_mask=["note"],
    )


def _get_task_instances(
    dag_run: "DagRun", task_instance_api: "TaskInstanceApi"
):
    return task_instance_api.get_task_instances(
        dag_id=dag_run.dag_id, dag_run_id=dag_run.run_id
    ).task_instances


def _calculate_updates(
    task_instances: list["TaskInstanceResponse"], doc_md_map: dict[str, str]
) -> list[dict]:
    existing_notes_map = {
        (ti.task_id, ti.map_index): ti.note for ti in task_instances
    }

    entities = []

    for task_instance in task_instances:
        new_content = doc_md_map.get(task_instance.task_id, "")
        if not new_content:
            continue

        existing_note = existing_notes_map.get(
            (task_instance.task_id, task_instance.map_index)
        )

        if existing_note and existing_note == new_content:
            continue

        entities.append({"task_id": task_instance.task_id, "note": new_content})

    return entities


def _update_task_instances(
    entities: list[dict],
    dag_run: "DagRun",
    task_instance_api: "TaskInstanceApi",
):
    import airflow_client.client

    batch_body = airflow_client.client.BulkBodyBulkTaskInstanceBody.from_dict(
        {
            "actions": [
                {
                    "action": "update",
                    "action_on_non_existence": "skip",
                    "entities": entities,
                }
            ]
        }
    )
    task_instance_api.bulk_task_instances(
        dag_id=dag_run.dag_id,
        dag_run_id=dag_run.run_id,
        bulk_body_bulk_task_instance_body=batch_body,
    )


def init_orchestration_pipeline_context(note_content: str, **context):
    """Initializes the orchestration pipeline context for a DAG run.

    Extracts specific metadata from the provided notes content and applies
    it to the DAG Run and its Task Instances via the Airflow API.

    Args:
        note_content: JSON string containing the DAG documentation.
        **context: The Airflow task execution context.

    Raises:
        ApiException: If the Airflow API client fails to update the metadata.
    """
    import airflow_client.client
    from airflow_client.client.rest import ApiException

    dag_run = context.get("dag_run")
    dag = context.get("dag")

    # Filter note_content to keep only specific fields
    additional_notes = dag_utils.extract_additional_notes(note_content)

    api_client = airflow_client_utils.get_airflow_api_client()
    dag_run_api = airflow_client.client.DagRunApi(api_client)
    task_instance_api = airflow_client.client.TaskInstanceApi(api_client)

    try:
        _update_metadata(
            dag_run, dag, additional_notes, dag_run_api, task_instance_api
        )
    except ApiException as e:
        print(
            "Failed when calling Airflow Python Client API during "
            f"metadata application: {e}"
        )
        raise


def get_actively_running_versions(
    pipeline_id: str, bundle_id: str
) -> list[str]:
    """Retrieves a list of actively running versions for a given pipeline.

    Queries the Airflow API to find any DAG runs currently in 'running' or
    'queued' states that match the bundle and pipeline ID pattern.
    """
    import airflow_client.client
    from airflow_client.client.rest import ApiException

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


def get_previous_default_versions(
    pipeline_id: str, bundle_id: str
) -> list[str]:
    """Retrieves a list of previous default versions for a given pipeline.

    Queries the Airflow API for DAGs tagged as current for the specific
    bundle and pipeline.
    """
    import airflow_client.client
    from airflow_client.client.rest import ApiException

    api_client = airflow_client_utils.get_airflow_api_client()
    dag_api = airflow_client.client.DAGApi(api_client)

    versions = set()
    try:
        response = dag_api.get_dags(
            tags=[
                "op:is_current",
                f"op:bundle:{bundle_id}",
                f"op:pipeline:{pipeline_id}",
            ],
            tags_match_mode="all",
        )

        if response.dags:
            for dag in response.dags:
                if dag.tags:
                    for tag in dag.tags:
                        if hasattr(tag, "name") and tag.name.startswith(
                            "op:version:"
                        ):
                            version_id = tag.name.split("op:version:")[1]
                            if version_id:
                                versions.add(version_id)
    except ApiException as e:
        print(f"Exception when calling DAGApi->get_dags: {e}")

    return list(versions)
