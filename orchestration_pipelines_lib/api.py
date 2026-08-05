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
"""Module with api methods."""

from __future__ import annotations

import logging
import os
import time
import traceback
from typing import TYPE_CHECKING, Any

import yaml
from airflow.exceptions import AirflowException

if TYPE_CHECKING:
    from orchestration_pipelines_lib.utils.file_manager import FileManager
    from orchestration_pipelines_lib.utils.pipeline_metadata import (
        PipelineMetadata,
    )
    from orchestration_pipelines_lib.utils.pipeline_repository import (
        PipelineRepository,
    )
    from orchestration_pipelines_lib.utils.versioned_file_manager import (
        VersionedFileManager,
    )
    from orchestration_pipelines_models.manifest.manifest import Manifest


def validate(pipeline_definition_file: str) -> None:
    """Validates the input pipeline.

    Args:
        pipeline_definition_file (str): The path to the pipeline
            definition file.
    """
    from orchestration_pipelines_lib.utils.file_manager import FileManager
    from orchestration_pipelines_lib.utils.pipeline_repository import (
        PipelineRepository,
    )

    dag_id = os.path.splitext(os.path.basename(pipeline_definition_file))[0]

    repository = PipelineRepository(data_root="")
    file_manager = FileManager()

    _get_and_convert_pipeline(
        repository=repository,
        file_manager=file_manager,
        pipeline_definition_path=pipeline_definition_file,
        bundle_id=None,
        pipeline_id=dag_id,
    )


def generate(
    pipeline_definition_file: str, globals_dict: dict[str, Any] = None
) -> None:
    """Generates the DAG based on the input pipeline.

    Args:
        pipeline_definition_file (str): The path to the pipeline
            definition file.
        globals_dict (Dict[str, Any], optional): The global dictionary to
            register the DAG in. Defaults to None.
    """
    from orchestration_pipelines_lib.utils.file_manager import FileManager
    from orchestration_pipelines_lib.utils.pipeline_metadata import (
        PipelineMetadata,
    )
    from orchestration_pipelines_lib.utils.pipeline_repository import (
        PipelineRepository,
    )

    dag_id = os.path.splitext(os.path.basename(pipeline_definition_file))[0]
    repository = PipelineRepository(data_root="")
    pipeline_id = dag_id

    file_manager = FileManager()
    source_filepath = file_manager.get_blob_reference(
        file_manager.resolve_path(pipeline_definition_file)
    )
    _generate_dag(
        file_manager,
        pipeline_definition_file,
        repository,
        dag_id=dag_id,
        metadata=PipelineMetadata(
            pipeline_id=pipeline_id,
            manifest=None,
            version_id="",
            source_filepath=source_filepath,
        ),
        data_root=None,
        globals_dict=globals_dict,
        bundle_id=None,
        pipeline_id=pipeline_id,
    )


def generate_dags(
    data_root: str,
    bundle_id: str,
    pipeline_id: str,
    globals_dict: dict[str, Any] = None,
):
    """Validates and generates DAGs for all versions of a pipeline from a bundle.

    Args:
        data_root (str): The root directory containing the data.
        bundle_id (str): The ID of the bundle.
        pipeline_id (str): The ID of the pipeline.
        globals_dict (Dict[str, Any], optional): The global dictionary to
            register the DAGs in. Defaults to None.
    """  # noqa: E501
    from orchestration_pipelines_lib.utils.file_manager import FileManager
    from orchestration_pipelines_lib.utils.pipeline_repository import (
        PipelineRepository,
    )
    from orchestration_pipelines_lib.utils.versioned_file_manager import (
        VersionedFileManager,
    )
    from orchestration_pipelines_lib.utils.versions_utils import (
        get_versions_to_parse,
    )

    base_file_manager = FileManager()
    repository = PipelineRepository(
        data_root=data_root, file_manager=base_file_manager
    )
    manifest = repository.get_manifest(bundle_id)

    versions_to_parse = get_versions_to_parse(pipeline_id, manifest)
    logging.info("Versions to parse: %s", versions_to_parse)

    versioned_file_manager = None
    for version in versions_to_parse:
        if manifest.is_pipeline_in_bundle(version, pipeline_id):
            if versioned_file_manager is None:
                versioned_file_manager = VersionedFileManager.from_file_manager(
                    base_file_manager,
                    pipeline_id=pipeline_id,
                    current_version=version,
                    bundle_id=bundle_id,
                    local_data_root=data_root,
                )
            else:
                versioned_file_manager.set_version(version)

            _generate_dag_for_version(
                data_root,
                repository,
                manifest,
                bundle_id=bundle_id,
                version_id=version,
                pipeline_id=pipeline_id,
                globals_dict=globals_dict,
                file_manager=versioned_file_manager,
            )


def _get_and_convert_pipeline(
    repository: PipelineRepository,
    file_manager: FileManager,
    pipeline_definition_path: str,
    bundle_id: str | None,
    pipeline_id: str,
    version_id: str | None = None,
):
    """Reads the pipeline and converts to its internal representation."""
    from orchestration_pipelines_lib.internal_models.converters import (
        converter as pipeline_converter,
    )

    if version_id:
        parsed_pipeline = repository.get_versioned_pipeline(
            bundle_id=bundle_id,
            pipeline_id=pipeline_id,
            version_id=version_id,
            file_manager=file_manager,
        )
    else:
        parsed_pipeline = repository.get_pipeline(
            pipeline_definition_path,
            file_manager=file_manager,
        )

    return pipeline_converter.convert(parsed_pipeline, file_manager)


def _generate_dag(
    file_manager: FileManager,
    pipeline_definition_path: str,
    repository: PipelineRepository,
    dag_id: str,
    metadata: PipelineMetadata,
    data_root: str,
    globals_dict: dict[str, Any],
    bundle_id: str | None,
    pipeline_id: str,
    version_id: str | None = None,
):
    """Generates a single DAG based on the provided pipeline definition.

    Args:
        file_manager (FileManager): The file manager instance.
        pipeline_definition_path (str): The path to the pipeline
            definition file.
        repository (PipelineRepository): The pipeline repository instance.
        dag_id (str): The ID to assign to the generated DAG.
        metadata (PipelineMetadata): The pipeline metadata.
        data_root (str): The root directory containing the data.
        globals_dict (Dict[str, Any]): The global dictionary to register the
            DAG in.
        bundle_id (Optional[str]): The ID of the bundle.
        pipeline_id (str): The ID of the pipeline.
        version_id (Optional[str]): The version ID.
    """
    from orchestration_pipelines_lib.dag_generator import core
    from orchestration_pipelines_lib.internal_models.triggers import (
        ScheduleTriggerModel,
    )
    from orchestration_pipelines_lib.utils.dummy_dag import (
        create as create_dummy_dag,
    )
    from orchestration_pipelines_lib.utils.file_manager import (
        OrchestrationPipelinesFileNotFoundError,
        OrchestrationPipelinesFileReadError,
        OrchestrationPipelinesInitializationError,
        OrchestrationPipelinesInvalidPathError,
    )
    from orchestration_pipelines_lib.utils.metrics import (
        ParsingStatus,
        report_parsing,
    )

    # Initial tags and metadata
    tags = ["op:orchestration_pipeline"]
    doc_md = ""
    internal_pipeline = None

    status = ParsingStatus.SUCCESS
    generate_time_start = time.perf_counter()

    try:
        internal_pipeline = _get_and_convert_pipeline(
            repository=repository,
            file_manager=file_manager,
            pipeline_definition_path=pipeline_definition_path,
            bundle_id=bundle_id,
            pipeline_id=pipeline_id,
            version_id=version_id,
        )

        # Override dag_id to desired form
        internal_pipeline.metadata.pipelineId = dag_id

        # Step 2: Prepare metadata
        schedule_trigger = next(
            (
                t
                for t in internal_pipeline.triggers
                if isinstance(t, ScheduleTriggerModel)
            ),
            None,
        )

        if metadata.is_paused() or not metadata.is_current():
            internal_pipeline.triggers = []

        tags = metadata.generate_tags(
            owner=internal_pipeline.metadata.owner,
            customer_tags=internal_pipeline.metadata.tags,
        )
        doc_md = metadata.generate_doc_md(
            owner=internal_pipeline.metadata.owner,
            schedule_trigger=schedule_trigger,
        )

        # Step 3: Generate DAG
        dag = core.generate(
            internal_pipeline,
            tags,
            doc_md,
            data_root,
            bundle_id,
            pipeline_id,
        )

        # Step 4: Validate DAG (TODO: Should be Airflow version specific)
        if hasattr(dag, "validate"):
            dag.validate()

        from airflow.utils.dag_cycle_tester import check_cycle

        check_cycle(dag)
        try:
            from airflow.serialization.serialized_objects import (
                DagSerialization as DagSerializer,
            )
        except ImportError:
            from airflow.serialization.serialized_objects import (
                SerializedDAG as DagSerializer,
            )

        DagSerializer.to_dict(dag)

        # Step 5: Register DAG
        if globals_dict is not None:
            globals_dict[dag_id] = dag
        else:
            with dag:
                pass
    except Exception as err:  # pylint: disable=broad-exception-caught
        if isinstance(
            err,
            (
                OrchestrationPipelinesFileReadError,
                OrchestrationPipelinesInitializationError,
                OrchestrationPipelinesInvalidPathError,
                OrchestrationPipelinesFileNotFoundError,
                ImportError,
            ),
        ):
            status = ParsingStatus.MISSING_FILE
        elif isinstance(err, (ValueError, TypeError, yaml.YAMLError)):
            status = ParsingStatus.PARSING_ERROR
        elif isinstance(err, AirflowException):
            status = ParsingStatus.AIRFLOW_ERROR
        else:
            status = ParsingStatus.INTERNAL

        # If a DAG with this ID was already put in globals by core.generate,
        # remove it first to avoid duplicates/ghosts.
        if globals_dict is not None and dag_id in globals_dict:
            del globals_dict[dag_id]
        error_message = traceback.format_exc()
        logging.warning(error_message)
        owner = internal_pipeline.metadata.owner if internal_pipeline else None

        # Re-initialize tags and doc_md for the error DAG to avoid using
        # partially modified state from the try block.
        error_tags = metadata.generate_tags(owner, customer_tags=None)
        error_doc_md = metadata.generate_doc_md(
            owner=owner,
            schedule_trigger=None,
        )
        if internal_pipeline and internal_pipeline.metadata.tags:
            error_tags.extend(internal_pipeline.metadata.tags)

        dummy_dag = create_dummy_dag(
            dag_id, error_message, error_tags, error_doc_md
        )
        if globals_dict is not None:
            globals_dict[dummy_dag.dag_id] = dummy_dag
        else:
            with dummy_dag:
                pass

    duration_ms = (time.perf_counter() - generate_time_start) * 1000
    report_parsing(bundle_id, pipeline_id, status, duration_ms)


def _generate_dag_for_version(
    data_root: str,
    repository: PipelineRepository,
    manifest: Manifest,
    bundle_id: str,
    version_id: str,
    pipeline_id: str,
    globals_dict: dict[str, Any],
    file_manager: VersionedFileManager,
):
    """Validates and generates the DAG based on the bundle, version, and pipeline ID.

    Args:
        data_root (str): The root directory containing the data.
        repository (PipelineRepository): The pipeline repository
            instance.
        manifest (Manifest): The manifest object.
        bundle_id (str): The ID of the bundle.
        version_id (str): The version ID.
        pipeline_id (str): The ID of the pipeline.
        globals_dict (Dict[str, Any]): The global dictionary to register the
            DAG in.
        file_manager (VersionedFileManager): The shared file manager instance.
    """  # noqa: E501
    from orchestration_pipelines_lib.utils.pipeline_metadata import (
        PipelineMetadata,
    )

    pipeline_definition_path = f"{pipeline_id}.yml"

    metadata = PipelineMetadata(
        pipeline_id=pipeline_id,
        manifest=manifest,
        version_id=version_id,
        source_filepath=file_manager.get_blob_reference(
            file_manager.resolve_path(pipeline_definition_path)
        ),
    )
    pipeline_filename = f"{pipeline_id}.yml"

    _generate_dag(
        file_manager,
        pipeline_filename,
        repository,
        dag_id=f"{bundle_id}__v__{version_id}__{pipeline_id}",
        metadata=metadata,
        data_root=data_root,
        globals_dict=globals_dict,
        bundle_id=bundle_id,
        pipeline_id=pipeline_id,
        version_id=version_id,
    )
