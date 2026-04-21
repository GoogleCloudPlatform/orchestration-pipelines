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
"""Converts v1 protobuf pipeline models to internal pydantic models."""

import yaml
from typing import Dict
from google.protobuf import struct_pb2
from google.protobuf.message import Message
from google.cloud.dataproc_v1.types.shared import (EnvironmentConfig,
                                                   RuntimeConfig)
from google.cloud.dataproc_v1.types.clusters import ClusterConfig
from google.cloud.dataform_v1.types.dataform import WorkflowInvocation
from orchestration_pipelines_lib.internal_models import (
    actions as internal_actions, pipeline as internal_pipeline, triggers as
    internal_triggers)
from orchestration_pipelines_lib.utils.dict_utils import normalize_struct, struct_to_dict, dict_to_struct
from orchestration_pipelines_lib.utils.file_manager import FileManager
from orchestration_pipelines_models.pipeline_v1_model.protos import \
    orchestration_pipeline_pb2 as v1_pipeline_protos


class ConverterV1ToInternal:
    """Converts v1 pipeline models (protobufs) to internal models."""

    def __init__(self, file_manager: FileManager):
        self.file_manager = file_manager

    def _normalize_environment_config(
            self, environment_config_msg: Message) -> EnvironmentConfig:
        return normalize_struct(environment_config_msg, EnvironmentConfig)

    def _normalize_runtime_config(
            self, runtime_config_msg: Message) -> RuntimeConfig:
        return normalize_struct(runtime_config_msg, RuntimeConfig)

    def _normalize_cluster_config(
            self, cluster_config_msg: Message) -> ClusterConfig:
        return normalize_struct(cluster_config_msg, ClusterConfig)

    def _normalize_workflow_invocation(
            self, workflow_invocation_msg: Message) -> WorkflowInvocation:
        return normalize_struct(workflow_invocation_msg, WorkflowInvocation)

    def _get_gce_cluster_config(self, resource_profile_msg: Message) -> Dict:
        """Parses a resource profile message for Dataproc on GCE.

        Args:
            resource_profile_msg: The resource profile protobuf message to parse.

        Returns:
            A dictionary containing the parsed cluster configuration.
        """
        cluster_config = None
        config_type = resource_profile_msg.WhichOneof("config")

        if config_type == "inline":
            inline_config = resource_profile_msg.inline
            cluster_config_msg = (inline_config.config if inline_config.config
                                  else inline_config.cluster_config)
            cluster_config = self._normalize_cluster_config(cluster_config_msg)
        elif config_type in ["path", "external_config_path"]:
            path = (resource_profile_msg.path if config_type == "path" else
                    resource_profile_msg.external_config_path)
            path_to_read = self.file_manager.resolve_path(path)
            cluster_config_dict = yaml.safe_load(
                self.file_manager.read(path_to_read))
            config_data = {}
            if (isinstance(cluster_config_dict, dict)
                    and "definition" in cluster_config_dict
                    and isinstance(cluster_config_dict["definition"], dict)
                    and "config" in cluster_config_dict["definition"]):
                config_data = cluster_config_dict["definition"]["config"]

            cluster_config = dict_to_struct(config_data, ClusterConfig)
        overrides = resource_profile_msg.overrides
        if cluster_config and overrides:
            overrides_struct = (overrides.config if overrides.config else
                                overrides.cluster_config)
            normalized_config = self._normalize_cluster_config(
                overrides_struct)
            if normalized_config:
                cluster_config._pb.MergeFrom(normalized_config._pb)

        return struct_to_dict(cluster_config._pb) if cluster_config else {}

    def _get_serverless_resource_profile(
            self,
            resource_profile_msg: Message) -> internal_actions.ResourceProfile:
        """Parses a resource profile message for Dataproc Serverless.

        Args:
            resource_profile_msg: The resource profile protobuf message to parse.

        Returns:
            An internal ResourceProfile model populated with the configuration.
        """
        runtime_config_msg = struct_pb2.Struct()
        environment_config_msg = struct_pb2.Struct()
        config_type = resource_profile_msg.WhichOneof("config")

        if config_type == "inline":
            inline_config = resource_profile_msg.inline
            runtime_config_msg = inline_config.runtime_config
            environment_config_msg = inline_config.environment_config
        elif config_type in ["path", "external_config_path"]:
            path = (resource_profile_msg.path if config_type == "path" else
                    resource_profile_msg.external_config_path)
            resolved_path = self.file_manager.resolve_path(path)
            resolved_config = yaml.safe_load(
                self.file_manager.read(resolved_path))
            config_definition = resolved_config.get("definition") or {}
            runtime_config_struct = struct_pb2.Struct()
            runtime_config_struct.update(
                config_definition.get("runtimeConfig")
                or config_definition.get("runtime_config") or {})
            runtime_config_msg = runtime_config_struct

            environment_config_struct = struct_pb2.Struct()
            environment_config_struct.update(
                config_definition.get("environmentConfig")
                or config_definition.get("environment_config") or {})
            environment_config_msg = environment_config_struct

        runtime_config = self._normalize_runtime_config(runtime_config_msg)
        environment_config = self._normalize_environment_config(
            environment_config_msg)

        overrides = resource_profile_msg.overrides
        if overrides:
            runtime_overrides = overrides.runtime_config
            environment_overrides = overrides.environment_config
            if runtime_overrides and runtime_config:
                normalized_runtime_overrides = self._normalize_runtime_config(
                    runtime_overrides)
                if normalized_runtime_overrides:
                    runtime_config._pb.MergeFrom(
                        normalized_runtime_overrides._pb)
            if environment_overrides and environment_config:
                normalized_environment_overrides = (
                    self._normalize_environment_config(environment_overrides))
                if normalized_environment_overrides:
                    environment_config._pb.MergeFrom(
                        normalized_environment_overrides._pb)

        return internal_actions.ResourceProfile(
            runtimeConfig=struct_to_dict(runtime_config._pb)
            if runtime_config else {},
            environmentConfig=struct_to_dict(environment_config._pb)
            if environment_config else {})

    def convert_to_internal_model(
        self, v1_pipeline_model: v1_pipeline_protos.OrchestrationPipeline
    ) -> internal_pipeline.PipelineModel:
        """Converts a v1 protobuf pipeline model to an internal pipeline model.

        Args:
            v1_pipeline_model: The v1 pipeline model to convert.

        Returns:
            The converted internal pipeline model.
        """

        internal_triggers_list = [
            self.convert_trigger(t) for t in v1_pipeline_model.triggers
        ]
        labels = self._get_labels(v1_pipeline_model.tags)
        internal_actions_list = [
            self.convert_action(a, v1_pipeline_model.defaults, labels)
            for a in v1_pipeline_model.actions
        ]

        v1_defaults = v1_pipeline_model.defaults
        internal_cloud_defaults = internal_pipeline.CloudDefaultsModel(
            project=v1_defaults.project_id, region=v1_defaults.location)

        internal_exec_config_defaults = (
            internal_pipeline.ExecutionConfigDefaultsModel(
                retries=v1_defaults.execution_config.retries))

        internal_defaults = internal_pipeline.DefaultsModel(
            cloudDefault=internal_cloud_defaults,
            executionConfigDefault=internal_exec_config_defaults)

        internal_metadata = internal_pipeline.MetaDataModel(
            pipelineId=v1_pipeline_model.pipeline_id,
            description=v1_pipeline_model.description,
            owner=v1_pipeline_model.owner,
            tags=list(v1_pipeline_model.tags),
        )

        internal_notifications = self.convert_notifications(
            v1_pipeline_model.notifications)

        runner_name = v1_pipeline_protos.PipelineRunner.Name(
            v1_pipeline_model.runner).lower()

        return internal_pipeline.PipelineModel(
            metadata=internal_metadata,
            defaults=internal_defaults,
            runner=runner_name,
            triggers=internal_triggers_list,
            actions=internal_actions_list,
            notifications=internal_notifications)

    def convert_notifications(
        self, notifications: v1_pipeline_protos.Notification
    ) -> internal_pipeline.NotificationModel:
        if not notifications:
            return None
        on_pipeline_failure = None
        if notifications.HasField("on_pipeline_failure"):
            on_pipeline_failure = internal_pipeline.EmailNotificationModel(
                email=list(notifications.on_pipeline_failure.email))
        return internal_pipeline.NotificationModel(
            onPipelineFailure=on_pipeline_failure)

    def convert_trigger(
        self, trigger: v1_pipeline_protos.Trigger
    ) -> internal_pipeline.AnyScheduleTrigger:
        trigger_type = trigger.WhichOneof("trigger")
        if trigger_type == "schedule":
            schedule = trigger.schedule
            return internal_triggers.ScheduleTriggerModel(
                type="schedule",
                scheduleInterval=schedule.interval,
                startTime=schedule.start_time,
                endTime=schedule.end_time,
                catchup=schedule.catchup,
                timezone=schedule.timezone)
        raise TypeError(f"Unknown trigger type: {trigger_type}")

    def convert_action(self, action: v1_pipeline_protos.Action,
                       defaults: v1_pipeline_protos.Defaults,
                       labels: Dict[str, str]) -> internal_pipeline.AnyAction:
        action_type = action.WhichOneof("action")
        if action_type == "python":
            return self._convert_python_action(action.python)
        if action_type == "pyspark":
            return self._convert_dataproc_action(action.pyspark, "pyspark",
                                                 defaults, labels)
        if action_type == "notebook":
            return self._convert_dataproc_action(action.notebook, "notebook",
                                                 defaults, labels)
        if action_type == "sql":
            return self._convert_sql_action(action.sql, defaults, labels)
        if action_type == "pipeline":
            return self._convert_pipeline_action(action.pipeline, defaults)
        raise TypeError(f"Unknown action type: {action_type}")

    def _convert_python_action(
        self, action: v1_pipeline_protos.PythonAction
    ) -> internal_pipeline.AnyAction:
        op_kwargs = struct_to_dict(action.op_kwargs) if action.HasField(
            "op_kwargs") and action.op_kwargs else None

        if action.HasField("environment"):
            env = action.environment
            requirements = None
            requirements_path = None
            if env.HasField("requirements"):
                reqs_wrapper = env.requirements
                req_type = reqs_wrapper.WhichOneof("requirements")
                if req_type == "inline":
                    requirements = list(reqs_wrapper.inline.list)
                elif req_type == "path":
                    requirements_path = self.file_manager.extract_relative_path(
                        self.file_manager.resolve_path(reqs_wrapper.path))

            return internal_actions.PythonVirtualenvActionModel(
                name=action.name,
                type="python-virtual-env",
                filename=self.file_manager.resolve_path(action.main_file_path),
                executionTimeout=action.execution_timeout or None,
                dependsOn=list(action.depends_on),
                config=internal_actions.PythonVirtualenvConfigurationModel(
                    pythonCallable=action.python_callable,
                    opKwargs=op_kwargs,
                    requirementsPath=requirements_path,
                    requirements=requirements,
                    systemSitePackages=env.system_site_packages,
                ))
        else:
            return internal_actions.PythonScriptActionModel(
                name=action.name,
                type="script",
                filename=self.file_manager.resolve_path(action.main_file_path),
                executionTimeout=action.execution_timeout or None,
                dependsOn=list(action.depends_on),
                config=internal_actions.PythonScriptConfigurationModel(
                    pythonCallable=action.python_callable,
                    opKwargs=op_kwargs,
                ))

    def _convert_dataproc_action(
        self, action, action_type: str, defaults: v1_pipeline_protos.Defaults,
        labels: Dict[str,
                     str]) -> internal_actions.DataprocOperatorActionModel:
        engine_type = action.engine.WhichOneof("engine")
        internal_engine = None
        internal_config = None
        region = None
        impersonation_chain = None

        if engine_type == "dataproc_on_gce":
            gce_engine = action.engine.dataproc_on_gce
            config_type = gce_engine.WhichOneof("config")
            if config_type == "existing_cluster":
                config = gce_engine.existing_cluster
                region = config.location or defaults.location
                impersonation_chain = list(config.impersonation_chain)
                internal_engine = internal_actions.EngineModel(
                    engineType="dataproc-gce", clusterMode="existing")
                internal_config = (
                    internal_actions.
                    DataprocGceExistingClusterConfigurationModel(
                        cluster_name=config.cluster_name,
                        project_id=config.project_id or defaults.project_id,
                        properties=config.properties))
            elif config_type == "ephemeral_cluster":
                config = gce_engine.ephemeral_cluster
                region = config.location or defaults.location
                impersonation_chain = list(config.impersonation_chain)
                internal_engine = internal_actions.EngineModel(
                    engineType="dataproc-gce", clusterMode="ephemeral")
                cluster_config = self._get_gce_cluster_config(
                    config.resource_profile)

                internal_config = (
                    internal_actions.DataprocEphemeralConfigurationModel(
                        region=region,
                        project_id=config.project_id or defaults.project_id,
                        cluster_name=config.cluster_name,
                        cluster_config=cluster_config,
                        properties=config.properties))
        elif engine_type == "dataproc_serverless":
            config = action.engine.dataproc_serverless
            region = config.location or defaults.location
            impersonation_chain = list(config.impersonation_chain)
            internal_engine = internal_actions.EngineModel(
                engineType="dataproc-serverless")
            resource_profile = self._get_serverless_resource_profile(
                config.resource_profile)

            internal_config = (
                internal_actions.DataprocCreateBatchOperatorConfigurationModel(
                    resourceProfile=resource_profile))

        return internal_actions.DataprocOperatorActionModel(
            name=action.name,
            type=action_type,
            filename=self.file_manager.resolve_path(action.main_file_path),
            pyFiles=[
                self.file_manager.resolve_path(p)
                for p in getattr(action, "py_files", [])
            ],
            query=None,
            executionTimeout=action.execution_timeout or None,
            dependsOn=list(action.depends_on),
            region=region,
            labels=labels,
            impersonationChain=impersonation_chain,
            archives=list(action.archive_uris),
            depsBucket=action.staging_bucket,
            engine=internal_engine,
            config=internal_config)

    def _convert_sql_action(
            self, action: v1_pipeline_protos.SqlAction,
            defaults: v1_pipeline_protos.Defaults,
            labels: Dict[str, str]) -> internal_pipeline.AnyAction:
        engine_type = action.engine.WhichOneof("engine")
        query_type = action.query.WhichOneof("query")
        query = None
        filename = None
        if query_type == "inline":
            query = action.query.inline
        elif query_type == "path":
            resolved_path = self.file_manager.resolve_path(action.query.path)
            if engine_type in ["dataproc_serverless", "dataproc_on_gce"]:
                filename = self.file_manager.get_blob_reference(resolved_path)
            else:
                filename = resolved_path

        if engine_type == "bigquery":
            bq_engine = action.engine.bigquery
            return internal_actions.BqOperationActionModel(
                name=action.name,
                type="operation",
                engine="bq",
                query=query,
                filename=filename,
                labels=labels,
                executionTimeout=action.execution_timeout or None,
                dependsOn=list(action.depends_on),
                impersonationChain=list(bq_engine.impersonation_chain),
                config=internal_actions.BqOperationConfigurationModel(
                    location=bq_engine.location or defaults.location,
                    destinationTable=bq_engine.destination_table,
                ))
        if engine_type == "dataproc_serverless":
            config = action.engine.dataproc_serverless
            region = config.location or defaults.location
            impersonation_chain = list(config.impersonation_chain)
            internal_engine = internal_actions.EngineModel(
                engineType="dataproc-serverless")
            resource_profile = self._get_serverless_resource_profile(
                config.resource_profile)
            internal_config = (
                internal_actions.DataprocCreateBatchOperatorConfigurationModel(
                    resourceProfile=resource_profile))

            return internal_actions.DataprocOperatorActionModel(
                name=action.name,
                type="sql",
                filename=filename,
                query=query,
                labels=labels,
                executionTimeout=action.execution_timeout or None,
                dependsOn=list(action.depends_on),
                region=region,
                impersonationChain=impersonation_chain,
                engine=internal_engine,
                config=internal_config)

        if engine_type == "dataproc_on_gce":
            gce_engine = action.engine.dataproc_on_gce
            config_type = gce_engine.WhichOneof("config")
            region = None
            impersonation_chain = None
            internal_engine = None
            internal_config = None

            if config_type == "existing_cluster":
                config = gce_engine.existing_cluster
                region = config.location or defaults.location
                impersonation_chain = list(config.impersonation_chain)
                internal_engine = internal_actions.EngineModel(
                    engineType="dataproc-gce", clusterMode="existing")
                internal_config = (
                    internal_actions.
                    DataprocGceExistingClusterConfigurationModel(
                        cluster_name=config.cluster_name,
                        project_id=config.project_id or defaults.project_id,
                        properties=config.properties))
            elif config_type == "ephemeral_cluster":
                config = gce_engine.ephemeral_cluster
                region = config.location or defaults.location
                impersonation_chain = list(config.impersonation_chain)
                internal_engine = internal_actions.EngineModel(
                    engineType="dataproc-gce", clusterMode="ephemeral")
                cluster_config = self._get_gce_cluster_config(
                    config.resource_profile)

                internal_config = (
                    internal_actions.DataprocEphemeralConfigurationModel(
                        region=region,
                        project_id=config.project_id or defaults.project_id,
                        cluster_name=config.cluster_name,
                        cluster_config=cluster_config,
                        properties=config.properties))

            return internal_actions.DataprocOperatorActionModel(
                name=action.name,
                type="sql",
                filename=filename,
                query=query,
                labels=labels,
                executionTimeout=action.execution_timeout or None,
                dependsOn=list(action.depends_on),
                region=region,
                impersonationChain=impersonation_chain,
                engine=internal_engine,
                config=internal_config)

        raise TypeError(f"Unknown SQL engine type: {engine_type}")

    def _convert_pipeline_action(
            self, action: v1_pipeline_protos.PipelineAction,
            defaults: v1_pipeline_protos.Defaults
    ) -> internal_pipeline.AnyAction:
        framework_type = action.framework.WhichOneof("framework")
        if framework_type == "dbt":
            dbt = action.framework.dbt
            execution_type = dbt.WhichOneof("execution")
            if execution_type == "airflow_worker":
                airflow_worker = dbt.airflow_worker
                return internal_actions.DBTActionModel(
                    name=action.name,
                    executionTimeout=action.execution_timeout or None,
                    dependsOn=list(action.depends_on),
                    type="dbt_pipeline",
                    engine="dbt",
                    executionMode="local",
                    source=internal_actions.DbtLocalExecutionModel(
                        path=self.file_manager.resolve_path(
                            airflow_worker.project_directory_path)),
                    select_models=list(airflow_worker.select_models))
        elif framework_type == "dataform":
            dataform = action.framework.dataform
            execution_type = dataform.WhichOneof("execution")
            if execution_type == "airflow_worker":
                airflow_worker = dataform.airflow_worker
                return internal_actions.DataformActionModel(
                    name=action.name,
                    executionTimeout=action.execution_timeout or None,
                    dependsOn=list(action.depends_on),
                    type="dataform_pipeline",
                    executionMode="local",
                    dataform_project_path=self.file_manager.get_blob_reference(
                        self.file_manager.resolve_path(
                            airflow_worker.project_directory_path)),
                )
            if execution_type == "dataform_service":
                service = dataform.dataform_service
                workflow_invocation = self._normalize_workflow_invocation(
                    service.workflow_invocation)
                return internal_actions.DataformActionModel(
                    name=action.name,
                    executionTimeout=action.execution_timeout or None,
                    dependsOn=list(action.depends_on),
                    type="dataform_pipeline",
                    executionMode="service",
                    dataformServiceConfig=internal_actions.
                    DataformServiceModel(project_id=service.project_id
                                         or defaults.project_id,
                                         region=service.location
                                         or defaults.location,
                                         repository_id=service.repository_id,
                                         workflow_invocation=struct_to_dict(
                                             workflow_invocation._pb)))
        raise TypeError(f"Unknown pipeline framework: {framework_type}")

    def _get_labels(self, tags: list[str]):
        labels = {"orchestration_pipeline": "true"}
        for tag in tags:
            if tag.startswith("job:"):
                key, sep, value = tag[4:].partition(":")
                labels[key] = value if sep else "true"
        return labels
