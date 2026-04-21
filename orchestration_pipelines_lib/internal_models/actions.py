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

from typing import Literal, Optional, List, Any, Dict, Union
from dataclasses import dataclass


@dataclass
class ActionBaseModel:
    name: str
    dependsOn: Optional[List[str]]
    executionTimeout: Optional[str]


@dataclass
class PythonScriptConfigurationModel:
    pythonCallable: str
    opKwargs: Optional[Dict[str, Any]] = None


@dataclass
class PythonScriptActionModel(ActionBaseModel):
    type: Literal["script"]
    filename: str
    config: PythonScriptConfigurationModel


@dataclass
class PythonVirtualenvConfigurationModel(PythonScriptConfigurationModel):
    requirementsPath: Optional[str] = None
    requirements: Optional[List[str]] = None
    systemSitePackages: Optional[bool] = None


@dataclass
class PythonVirtualenvActionModel(ActionBaseModel):
    type: Literal["python-virtual-env"]
    filename: str
    config: PythonVirtualenvConfigurationModel


@dataclass
class ResourceProfile:
    runtimeConfig: Optional[Dict[str, Any]] = None
    environmentConfig: Optional[Dict[str, Any]] = None


@dataclass
class DataprocCreateBatchOperatorConfigurationModel:
    resourceProfile: ResourceProfile


@dataclass
class BqOperationConfigurationModel:
    location: str
    destinationTable: Optional[str] = None


@dataclass
class BqOperationActionModel(ActionBaseModel):
    type: Literal["operation"]
    engine: Literal["bq"]
    config: BqOperationConfigurationModel
    query: Optional[str] = None
    filename: Optional[str] = None
    labels: Optional[Dict[str, str]] = None
    impersonationChain: Optional[Union[str, list[str]]] = None


@dataclass
class DataprocEphemeralConfigurationModel:
    region: str
    project_id: str
    cluster_name: str
    cluster_config: Optional[Dict[str, Any]] = None
    properties: Optional[Dict[str, str]] = None


@dataclass
class DataprocGceExistingClusterConfigurationModel:
    cluster_name: str
    project_id: Optional[str] = None
    properties: Optional[Dict[str, str]] = None


@dataclass
class EngineModel:
    engineType: Literal["dataproc-gce", "dataproc-serverless"]
    clusterMode: Optional[Literal["existing", "ephemeral"]] = None


@dataclass
class DataprocOperatorActionModel(ActionBaseModel):
    type: Literal["notebook", "pyspark", "sql"]
    region: str
    engine: EngineModel
    filename: Optional[str] = None
    pyFiles: Optional[List[str]] = None
    query: Optional[str] = None
    archives: Optional[List[str]] = None
    depsBucket: Optional[str] = None
    labels: Optional[Dict[str, str]] = None
    impersonationChain: Optional[Union[str, list[str]]] = None
    config: Union[DataprocGceExistingClusterConfigurationModel,
                  DataprocEphemeralConfigurationModel,
                  DataprocCreateBatchOperatorConfigurationModel, None] = None


@dataclass
class DbtLocalExecutionModel:
    path: str


@dataclass
class DBTActionModel(ActionBaseModel):
    type: Literal["dbt_pipeline"]
    engine: Literal["dbt"]
    executionMode: Literal["local"]
    source: DbtLocalExecutionModel
    select_models: Optional[List[str]] = None


@dataclass
class DataformServiceModel:
    workflow_invocation: Dict[str, Any]
    project_id: Optional[str] = None
    region: Optional[str] = None
    repository_id: Optional[str] = None


@dataclass
class DataformActionModel(ActionBaseModel):
    type: Literal['dataform_pipeline']
    executionMode: Literal['local', 'service']
    dataform_project_path: Optional[str] = None
    dataformServiceConfig: Optional[DataformServiceModel] = None
