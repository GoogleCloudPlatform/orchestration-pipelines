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
"""Defines the internal data models for an orchestration pipeline."""

# pylint: disable=invalid-name,missing-class-docstring

from dataclasses import dataclass
from enum import Enum
from typing import List, Optional, Union

from orchestration_pipelines_lib.internal_models.actions import (
    BqOperationActionModel,
    DataformActionModel,
    DataprocOperatorActionModel,
    DBTActionModel,
    PythonScriptActionModel,
    PythonVirtualenvActionModel,
)
from orchestration_pipelines_lib.internal_models.triggers import (
    ScheduleTriggerModel,
)

# Define a Union of all possible action models for this version.
# In the future, if you add a NewActionModel, just add it to this Union.
# e.g., AnyAction = Union[PapermillActionModel, NewActionModel]
AnyAction = Union[
    PythonScriptActionModel,
    DataprocOperatorActionModel,
    BqOperationActionModel,
    PythonVirtualenvActionModel,
    DBTActionModel,
    DataformActionModel,
]
AnyScheduleTrigger = Union[ScheduleTriggerModel]


class RunnerType(str, Enum):
    """Enumeration for types of runners that execute the pipeline."""

    CORE = "core"
    AIRFLOW = "airflow"


@dataclass
class ExecutionConfigDefaultsModel:
    """Default execution configuration for actions within the pipeline."""
    retries: int


@dataclass
class CloudDefaultsModel:
    """Default cloud provider settings for the pipeline."""
    project: str
    region: str


@dataclass
class DefaultsModel:
    """Encapsulates all default settings for the pipeline."""
    cloudDefault: CloudDefaultsModel
    executionConfigDefault: ExecutionConfigDefaultsModel


@dataclass
class MetaDataModel:
    """Metadata for the pipeline."""
    pipelineId: str
    description: str
    owner: str
    tags: Optional[List[str]] = None


@dataclass
class EmailNotificationModel:
    """Model for email notifications."""
    email: List[str]


@dataclass
class NotificationModel:
    """Model containing various notification configurations."""
    onPipelineFailure: Optional[EmailNotificationModel] = None
    onPipelineSuccess: Optional[EmailNotificationModel] = None
    onPipelineComplete: Optional[EmailNotificationModel] = None


@dataclass
class PipelineModel:
    """The root model representing a full orchestration pipeline."""
    defaults: DefaultsModel
    metadata: MetaDataModel
    runner: RunnerType
    triggers: List[AnyScheduleTrigger]
    actions: List[AnyAction]
    notifications: Optional[NotificationModel] = None
