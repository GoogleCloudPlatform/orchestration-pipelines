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

"""Declarative configuration action models."""

# pylint: disable=invalid-name,missing-class-docstring

from dataclasses import dataclass
from typing import Any, Literal


@dataclass
class FixedDelayStrategyModel:
    """Configuration model for Fixed Delay retry strategy."""

    retryDelay: str


@dataclass
class RetryPolicyModel:
    """Model representing retry policy configuration."""

    maxRetries: int
    fixedDelay: FixedDelayStrategyModel | None = None


@dataclass(kw_only=True)
class ActionBaseModel:
    """Base model for all action configurations."""

    name: str
    dependsOn: list[str] | None
    executionTimeout: str | None
    triggerRule: str | None
    retryPolicy: RetryPolicyModel | None = None


@dataclass
class PythonScriptConfigurationModel:
    """Configuration for Python script actions."""

    pythonCallable: str
    opKwargs: dict[str, Any] | None = None


@dataclass
class PythonScriptActionModel(ActionBaseModel):
    """Action model for executing Python scripts."""

    type: Literal["script"]
    filename: str
    config: PythonScriptConfigurationModel


@dataclass
class PythonVirtualenvConfigurationModel(PythonScriptConfigurationModel):
    """Configuration for Python virtualenv actions."""

    requirementsPath: str | None = None
    requirements: list[str] | None = None
    systemSitePackages: bool | None = None


@dataclass
class PythonVirtualenvActionModel(ActionBaseModel):
    """Action model for executing Python in a virtualenv."""

    type: Literal["python-virtual-env"]
    filename: str
    config: PythonVirtualenvConfigurationModel


@dataclass
class ResourceProfile:
    """Resource profile settings for runtime and environment."""

    runtimeConfig: dict[str, Any] | None = None
    environmentConfig: dict[str, Any] | None = None


@dataclass
class DataprocCreateBatchOperatorConfigurationModel:
    """Configuration for Dataproc Create Batch operator."""

    resourceProfile: ResourceProfile


@dataclass
class BqOperationConfigurationModel:
    """Configuration for BigQuery operation actions."""

    location: str
    destinationTable: str | None = None


@dataclass
class BqOperationActionModel(ActionBaseModel):
    """Action model for BigQuery operations."""

    type: Literal["operation"]
    engine: Literal["bq"]
    config: BqOperationConfigurationModel
    query: str | None = None
    filename: str | None = None
    labels: dict[str, str] | None = None
    impersonationChain: str | list[str] | None = None
    params: dict[str, str] | None = None


@dataclass
class DataprocEphemeralConfigurationModel:
    """Configuration for ephemeral Dataproc clusters."""

    region: str
    project_id: str
    cluster_name: str
    cluster_config: dict[str, Any] | None = None
    properties: dict[str, str] | None = None


@dataclass
class DataprocGceExistingClusterConfigurationModel:
    """Configuration for existing Dataproc on GCE clusters."""

    cluster_name: str
    project_id: str | None = None
    properties: dict[str, str] | None = None


@dataclass
class EngineModel:
    """Execution engine model for Dataproc actions."""

    engineType: Literal["dataproc-gce", "dataproc-serverless"]
    clusterMode: Literal["existing", "ephemeral"] | None = None


@dataclass
class DataprocOperatorActionModel(ActionBaseModel):
    """Action model for Dataproc operations."""

    type: Literal["notebook", "pyspark", "sql"]
    region: str
    engine: EngineModel
    filename: str | None = None
    pyFiles: list[str] | None = None
    query: str | None = None
    archives: list[str] | None = None
    depsBucket: str | None = None
    labels: dict[str, str] | None = None
    params: dict[str, str] | None = None
    impersonationChain: str | list[str] | None = None
    config: (
        DataprocGceExistingClusterConfigurationModel
        | DataprocEphemeralConfigurationModel
        | DataprocCreateBatchOperatorConfigurationModel
        | None
    ) = None


@dataclass
class DbtLocalExecutionModel:
    """Configuration for local DBT execution."""

    path: str


@dataclass
class DBTActionModel(ActionBaseModel):
    """Action model for DBT pipelines."""

    type: Literal["dbt_pipeline"]
    engine: Literal["dbt"]
    executionMode: Literal["local"]
    source: DbtLocalExecutionModel
    select_models: list[str] | None = None
    params: dict[str, str] | None = None


@dataclass
class DataformServiceModel:
    """Configuration for executing on Dataform Service."""

    workflow_invocation: dict[str, Any]
    project_id: str | None = None
    region: str | None = None
    repository_id: str | None = None


@dataclass
class DataformActionModel(ActionBaseModel):
    """Internal model representing a Dataform action."""

    type: Literal["dataform_pipeline"]
    executionMode: Literal["local", "service"]
    dataform_project_path: str | None = None
    dataformServiceConfig: DataformServiceModel | None = None
    labels: dict[str, str] | None = None
    params: dict[str, str] | None = None


@dataclass
class BigQueryDtsSpecModel:
    """BigQuery DTS spec model."""

    transferConfigId: str
    runtimeParams: dict[str, Any] | None = None
    requestedRunTime: str | None = None
    requestedTimeRange: dict[str, str] | None = None
    impersonationChain: str | list[str] | None = None
    projectId: str | None = None
    location: str | None = None


@dataclass
class DataIngestionActionModel(ActionBaseModel):
    """Internal model representing a Data Ingestion action."""

    type: Literal["data_ingestion"]
    config: BigQueryDtsSpecModel
    labels: dict[str, str] | None = None


@dataclass
class OrchestrationPipelineActionModel(ActionBaseModel):
    """Internal model for triggering another orchestration pipeline."""

    type: Literal["orchestration_pipeline"]
    pipeline_id: str
    bundle_id: str | None = None
    wait_for_completion: bool | None = None


@dataclass
class AgentPlatformModelUploadSpecModel:
    """Agent Platform (Vertex AI) Model Upload spec model."""

    model_name: str
    model_artifact_uri: str
    serving_container_image_uri: str
    description: str | None = None
    project_id: str | None = None
    location: str | None = None


@dataclass
class AgentPlatformBatchInferenceSpecModel:
    """Agent Platform (Vertex AI) Batch Inference spec model."""

    job_display_name: str
    model_name: str
    instances_format: str | None = None
    predictions_format: str | None = None
    bigquery_source: str | None = None
    gcs_source: str | list[str] | None = None
    bigquery_destination_prefix: str | None = None
    gcs_destination_prefix: str | None = None
    project_id: str | None = None
    location: str | None = None
    impersonation_chain: str | list[str] | None = None


@dataclass
class AIActionModel(ActionBaseModel):
    """Internal model representing an AI action."""

    type: Literal["ai"]
    provider: Literal["agent_platform"]
    ai_action_type: Literal[
        "model_upload",
        "batch_inference",
    ]
    config: (
        AgentPlatformModelUploadSpecModel | AgentPlatformBatchInferenceSpecModel
    )
    labels: dict[str, str] | None = None
