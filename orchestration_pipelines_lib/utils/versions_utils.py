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
"""Module to manipulate versions of a bundle"""

import logging
from orchestration_pipelines_models.manifest.manifest import Manifest
from orchestration_pipelines_lib.dag_generator import core


def get_versions_to_parse(pipeline_id: str, manifest: Manifest):
    """Gets the set of pipeline versions that need to be parsed.

    Args:
        pipeline_id: The ID of the pipeline.
        manifest: The manifest object containing bundle information.

    Returns:
        A set of versions to parse, including the default version, actively
        running versions, and the previous default versions.
    """
    default_version = manifest.get_default_version()
    active_dag_versions = core.get_actively_running_versions(
        pipeline_id, manifest.get_bundle_id())
    previous_default_versions = core.get_previous_default_versions(
        pipeline_id, manifest.get_bundle_id())
    logging.info("Previous Default version: %s", previous_default_versions)
    logging.info("Active dag versions: %s", active_dag_versions)
    logging.info("Default version: %s", default_version)
    versions = {default_version} | set(active_dag_versions)
    if previous_default_versions:
        versions.update(previous_default_versions)
    return versions
