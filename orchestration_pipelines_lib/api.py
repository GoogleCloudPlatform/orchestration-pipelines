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
import os
import logging
import traceback
from typing import Optional, Dict, Any

import yaml
from airflow.models import DAG
from airflow.serialization.serialized_objects import SerializedDAG
from airflow.utils.dag_cycle_tester import check_cycle

from orchestration_pipelines_lib.dag_generator import core
from orchestration_pipelines_lib.internal_models.converters import converter
from orchestration_pipelines_lib.internal_models.triggers import ScheduleTriggerModel
from orchestration_pipelines_lib.utils.dummy_dag import create as create_dummy_dag
from orchestration_pipelines_lib.utils.file_manager import FileManager
from orchestration_pipelines_lib.utils.path_utils import get_manifest_path
from orchestration_pipelines_lib.utils.pipeline_metadata import PipelineMetadata
from orchestration_pipelines_lib.utils.versioned_file_manager import VersionedFileManager
from orchestration_pipelines_lib.utils.versions_utils import get_versions_to_parse
from orchestration_pipelines_models.manifest.manifest import Manifest
from orchestration_pipelines_models.orchestration_pipelines_model import OrchestrationPipelinesModel


def validate(pipeline_definition_file: str) -> None:
    """Validates the input pipeline.

    Args:
        pipeline_definition_file (str): The path to the pipeline definition file.
    """
    _read_parse_and_convert_pipeline(FileManager(), pipeline_definition_file)


def generate(pipeline_definition_file: str,
             globals_dict: Dict[str, Any] = None):
    """Generates the DAG based on the input pipeline.

    Args:
        pipeline_definition_file (str): The path to the pipeline definition file.
        globals_dict (Dict[str, Any], optional): The global dictionary to register the DAG in. Defaults to None.
    """
    dag_id = os.path.splitext(os.path.basename(pipeline_definition_file))[0]
    _generate_dag(FileManager(), pipeline_definition_file, dag_id, None, None,
                  globals_dict)


def generate_dags(data_root: str,
                  bundle_id: str,
                  pipeline_id: str,
                  globals_dict: Dict[str, Any] = None):
    """Validates and generates DAGs for all versions of a pipeline from a bundle.

    Args:
        data_root (str): The root directory containing the data.
        bundle_id (str): The ID of the bundle.
        pipeline_id (str): The ID of the pipeline.
        globals_dict (Dict[str, Any], optional): The global dictionary to register the DAGs in. Defaults to None.
    """
    manifest = get_manifest(data_root, bundle_id)
    versions_to_parse = get_versions_to_parse(pipeline_id, manifest)
    logging.info("Versions to parse: %s", versions_to_parse)
    for version in versions_to_parse:
        if manifest.is_pipeline_in_bundle(version, pipeline_id):
            _generate_dag_for_version(data_root, manifest, bundle_id, version,
                                      pipeline_id, globals_dict)


def _read_parse_and_convert_pipeline(file_manager: FileManager,
                                     pipeline_definition_path: str):
    """Reads, parses, and converts a pipeline definition to an internal model.

    Args:
        file_manager (FileManager): The file manager instance to read the file.
        pipeline_definition_path (str): The path to the pipeline definition file.

    Returns:
        The internal pipeline model object.
    """
    # Step 1: Read pipeline definition
    definition_content = file_manager.read(pipeline_definition_path)
    pipeline_definition = yaml.safe_load(definition_content)

    # Step 2: Parse pipeline and convert to internal model
    parsed_pipeline = OrchestrationPipelinesModel.build(pipeline_definition)
    internal_pipeline_model = converter.convert(parsed_pipeline, file_manager)
    return internal_pipeline_model


def _generate_dag(file_manager: FileManager, pipeline_definition_path: str,
                  dag_id: str, metadata: Optional[PipelineMetadata],
                  data_root: str, globals_dict: Dict[str, Any]):
    """Generates a single DAG based on the provided pipeline definition.

    Args:
        file_manager (FileManager): The file manager instance.
        pipeline_definition_path (str): The path to the pipeline definition file.
        dag_id (str): The ID to assign to the generated DAG.
        metadata (Optional[PipelineMetadata]): The pipeline metadata, if any.
        data_root (str): The root directory containing the data.
        globals_dict (Dict[str, Any]): The global dictionary to register the DAG in.
    """
    # Initial tags and metadata
    tags = ["op:orchestration_pipeline"]
    doc_md = ""
    internal_pipeline = None

    try:
        # Step 1: Read, parse and convert pipeline to internal model
        internal_pipeline = _read_parse_and_convert_pipeline(
            file_manager, pipeline_definition_path)

        # Override dag_id to desired form
        internal_pipeline.metadata.pipelineId = dag_id

        # Step 2: Prepare metadata
        schedule_trigger = next((t for t in internal_pipeline.triggers
                                 if isinstance(t, ScheduleTriggerModel)), None)

        if metadata:
            if metadata.is_paused() or not metadata.is_current():
                internal_pipeline.triggers = []
            tags = metadata.generate_tags(
                owner=internal_pipeline.metadata.owner,
                customer_tags=internal_pipeline.metadata.tags)
            doc_md = metadata.generate_doc_md(
                owner=internal_pipeline.metadata.owner,
                schedule_trigger=schedule_trigger)
        else:
            if internal_pipeline.metadata.tags:
                tags.extend(internal_pipeline.metadata.tags)

        # Step 3: Generate DAG
        dag = core.generate(internal_pipeline, tags, doc_md, data_root)

        # Step 4: Validate DAG (TODO: Should be Airflow version specific)
        if hasattr(dag, "validate"):
            dag.validate()
        check_cycle(dag)
        SerializedDAG.to_dict(dag)

        # Step 5: Register DAG
        if globals_dict:
            globals_dict[dag_id] = dag
        else:
            with dag:
                pass
    except Exception:
        # If a DAG with this ID was already put in globals by core.generate,
        # remove it first to avoid duplicates/ghosts.
        if dag_id in globals():
            del globals_dict[dag_id]
        error_message = traceback.format_exc()
        logging.warning(error_message)
        owner = internal_pipeline.metadata.owner if internal_pipeline else None

        # Re-initialize tags and doc_md for the error DAG to avoid using
        # partially modified state from the try block.
        error_tags = ["op:orchestration_pipeline"]
        error_doc_md = ""
        if metadata:
            error_tags = metadata.generate_tags(owner, customer_tags=None)
            error_doc_md = metadata.generate_doc_md(owner,
                                                    schedule_trigger=None)
        elif internal_pipeline and internal_pipeline.metadata.tags:
            error_tags.extend(internal_pipeline.metadata.tags)

        dummy_dag = create_dummy_dag(dag_id, error_message, error_tags,
                                     error_doc_md)
        if globals_dict:
            globals_dict[dummy_dag.dag_id] = dummy_dag
        else:
            with dummy_dag:
                pass


def _generate_dag_for_version(data_root: str, manifest: Manifest,
                              bundle_id: str, version_id: str,
                              pipeline_id: str, globals_dict: Dict[str, Any]):
    """Validates and generates the DAG based on the bundle, version, and pipeline ID.

    Args:
        data_root (str): The root directory containing the data.
        manifest (Manifest): The manifest object.
        bundle_id (str): The ID of the bundle.
        version_id (str): The version ID.
        pipeline_id (str): The ID of the pipeline.
        globals_dict (Dict[str, Any]): The global dictionary to register the DAG in.
    """
    versioned_file_manager = VersionedFileManager(pipeline_id,
                                                  version_id,
                                                  bundle_id,
                                                  local_data_root=data_root)
    metadata = PipelineMetadata(pipeline_id=pipeline_id,
                                manifest=manifest,
                                version_id=version_id)
    _generate_dag(versioned_file_manager, f"{pipeline_id}.yml",
                  f"{bundle_id}__v__{version_id}__{pipeline_id}", metadata,
                  data_root, globals_dict)


def get_manifest(data_root: str, bundle_id: str):
    """Retrieves and parses the manifest for a given bundle.

    Args:
        data_root (str): The root directory containing the data.
        bundle_id (str): The ID of the bundle.

    Returns:
        Manifest: The parsed manifest object.
    """
    manifest_path = get_manifest_path(data_root, bundle_id)
    file_manager = FileManager()
    manifest_content = file_manager.read(manifest_path)
    parsed_manifest = yaml.safe_load(manifest_content)
    return Manifest.from_dict(parsed_manifest)
