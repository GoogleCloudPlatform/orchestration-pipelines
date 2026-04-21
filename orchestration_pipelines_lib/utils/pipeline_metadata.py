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
"""Module for encapsulating pipeline metadata."""

import json
from typing import List, Optional

from orchestration_pipelines_lib.internal_models.triggers import \
    ScheduleTriggerModel
from orchestration_pipelines_models.manifest.manifest import Manifest
from orchestration_pipelines_models.manifest.manifest_pb2 import DeploymentOrigination


class PipelineMetadata:
    """A class to encapsulate pipeline metadata for generating DAG tags and documentation."""

    def __init__(self, pipeline_id: str, manifest: Manifest, version_id: str):
        """Initializes the PipelineMetadata object.

        Args:
            pipeline_id: The ID of the pipeline.
            manifest: The bundle manifest.
            version_id: The version ID of the pipeline.
        """
        self._manifest = manifest
        self._version_id = version_id
        self._bundle_id = manifest.get_bundle_id()
        self._pipeline_id = pipeline_id

        self._is_paused = self._manifest.is_paused(self._pipeline_id)
        self._is_current = self._manifest.is_current(self._version_id)

        self._extract_deployment_details()

    def _extract_deployment_details(self):
        """Extracts deployment details from the manifest and sets instance attributes."""
        deployment_details = self._manifest.get_deployment_details(
            self._version_id)

        self._origination = ""
        self._repo = ""
        self._branch = ""
        self._commit = ""

        if deployment_details:
            if deployment_details.origination is not None:
                self._origination = DeploymentOrigination.Name(
                    deployment_details.origination)
            else:
                self._origination = ""
            self._repo = deployment_details.git_repo or ""
            self._branch = deployment_details.git_branch or ""
            self._commit = deployment_details.commit_sha or ""

    def is_paused(self):
        """Checks if the pipeline is currently paused.

        Returns:
            True if the pipeline is paused, False otherwise.
        """
        return self._is_paused

    def is_current(self):
        """Checks if the pipeline version is the current default version.

        Returns:
            True if this is the current version, False otherwise.
        """
        return self._is_current

    def generate_tags(self, owner: Optional[str],
                      customer_tags: Optional[List[str]]) -> List[str]:
        """Generates a list of tags for the DAG.

        Args:
            owner: The owner of the pipeline.
            customer_tags: A list of customer-defined tags.

        Returns:
            A list of combined customer and orchestration tags.
        """
        owner = owner or ""
        customer_tags = customer_tags or []
        registered_tag_prefixes = ("op:bundle", "op:version", "op:pipeline",
                                   "op:owner", "op:origination",
                                   "op:is_paused", "op:is_current",
                                   "op:orchestration_pipeline")

        # Filter out registered tags from customer tags
        filtered_customer_tags = [
            tag for tag in customer_tags
            if not tag.startswith(registered_tag_prefixes)
        ]

        orchestration_tags = [
            "op:orchestration_pipeline",
            f"op:bundle:{self._bundle_id}",
            f"op:version:{self._version_id}",
            f"op:pipeline:{self._pipeline_id}",
            f"op:owner:{owner}",
            f"op:origination:{self._origination}",
        ]

        if self._is_current:
            orchestration_tags.append("op:is_current")
        if self._is_paused:
            orchestration_tags.append("op:is_paused")

        return filtered_customer_tags + orchestration_tags

    def generate_doc_md(
            self, owner: Optional[str],
            schedule_trigger: Optional[ScheduleTriggerModel]) -> str:
        """Generates a JSON string for the DAG's doc_md.

        Args:
            owner: The owner of the pipeline.
            schedule_trigger: The schedule trigger model containing schedule details.

        Returns:
            A JSON-formatted string representing the DAG documentation metadata.
        """
        owner = owner or ""
        doc_data = {
            "op_bundle": self._bundle_id,
            "op_version": self._version_id,
            "op_pipeline": self._pipeline_id,
            "op_owner": owner,
            "op_origination": self._origination,
            "op_is_paused": self._is_paused,
            "op_is_current": self._is_current,
        }

        # Create and filter deployment_details dict concisely.
        deployment_details = {
            "op_repository": self._repo,
            "op_branch": self._branch,
            "op_commit_sha": self._commit,
        }
        filtered_details = {k: v for k, v in deployment_details.items() if v}

        if filtered_details:
            doc_data["op_deployment_details"] = filtered_details

        if schedule_trigger:
            schedule_info = {
                "scheduleInterval": schedule_trigger.scheduleInterval,
                "startTime": schedule_trigger.startTime,
                "endTime": schedule_trigger.endTime,
                "catchup": schedule_trigger.catchup,
                "timezone": schedule_trigger.timezone,
            }
            doc_data["op_schedule"] = schedule_info

        return json.dumps(doc_data)
