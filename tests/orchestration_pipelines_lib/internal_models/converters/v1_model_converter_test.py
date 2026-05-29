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
"""Unit tests for the v1_model_converter module."""

import unittest
from unittest.mock import MagicMock

from google.protobuf import struct_pb2

from orchestration_pipelines_lib.internal_models import (
    actions as internal_actions,
)
from orchestration_pipelines_lib.internal_models import (
    pipeline as internal_pipeline,
)
from orchestration_pipelines_lib.internal_models import (
    triggers as internal_triggers,
)
from orchestration_pipelines_lib.internal_models.converters.v1_model_converter import (
    ConverterV1ToInternal,
)
from orchestration_pipelines_models.pipeline_v1_model.protos import (
    orchestration_pipeline_pb2 as v1_protos,
)


class TestConverterV1ToInternal(unittest.TestCase):
    """Test suite for the v1 to internal model converter."""

    def setUp(self):
        """Set up the test case."""
        self.file_manager = MagicMock()
        self.file_manager.resolve_path.side_effect = (
            lambda path: f"resolved/{path}" if path else None)
        self.file_manager.get_blob_reference.side_effect = (
            lambda path: f"gs://bucket/{path}" if path else None)
        self.file_manager.extract_relative_path.side_effect = (
            lambda path: path.removeprefix("resolved/") if path else None)
        self.converter = ConverterV1ToInternal(self.file_manager)
        self.defaults = v1_protos.Defaults(
            project_id="default-project",
            location="default-location",
        )
        self.labels = {"op:orchestration_pipeline": "true"}

    def test_convert_to_internal_model_full(self):
        """Tests conversion of a full v1 pipeline model."""
        v1_pipeline = v1_protos.OrchestrationPipeline(
            pipeline_id="test-pipeline",
            description="A test pipeline",
            owner="test-owner",
            runner=v1_protos.PipelineRunner.airflow,
            tags=["tag1", "job:test-job"],
            defaults=v1_protos.Defaults(
                project_id="example-project",
                location="us-central1",
                execution_config=v1_protos.ExecutionConfig(retries=3),
            ),
            triggers=[
                v1_protos.Trigger(schedule=v1_protos.ScheduleTrigger(
                    interval="0 0 * * *",
                    start_time="2023-01-01T00:00:00Z",
                    end_time="2024-01-01T00:00:00Z",
                    catchup=False,
                    timezone="UTC",
                ))
            ],
            actions=[
                v1_protos.Action(python=v1_protos.PythonAction(
                    name="python-task",
                    main_file_path="path/to/script.py",
                    python_callable="my_func",
                ))
            ],
            notifications=v1_protos.Notification(
                on_pipeline_failure=v1_protos.OnPipelineFailure(
                    email=["test@example.com"])),
        )

        internal_model = self.converter.convert_to_internal_model(v1_pipeline)

        self.assertIsInstance(internal_model, internal_pipeline.PipelineModel)
        self.assertEqual(internal_model.metadata.pipelineId, "test-pipeline")
        self.assertEqual(internal_model.defaults.cloudDefault.project,
                         "example-project")
        self.assertEqual(internal_model.runner, "airflow")
        self.assertEqual(len(internal_model.triggers), 1)
        self.assertEqual(len(internal_model.actions), 1)
        self.assertIsNotNone(internal_model.notifications.onPipelineFailure)
        self.assertEqual(internal_model.actions[0].filename,
                         "resolved/path/to/script.py")

    def test_convert_notifications(self):
        """Tests conversion of notification settings."""
        # Test with failure notification
        v1_notification = v1_protos.Notification(
            on_pipeline_failure=v1_protos.OnPipelineFailure(
                email=["test@example.com"]))
        internal_notification = self.converter.convert_notifications(
            v1_notification)
        self.assertEqual(internal_notification.onPipelineFailure.email,
                         ["test@example.com"])

        # Test with no failure notification
        v1_notification_empty = v1_protos.Notification()
        internal_notification_empty = (
            self.converter.convert_notifications(v1_notification_empty))
        self.assertIsNone(internal_notification_empty.onPipelineFailure)

        # Test with None input
        self.assertIsNone(self.converter.convert_notifications(None))

    def test_convert_trigger(self):
        """Tests conversion of trigger settings."""
        v1_trigger = v1_protos.Trigger(schedule=v1_protos.ScheduleTrigger(
            interval="* * * * *",
            start_time="2023-01-01T00:00:00Z",
            end_time="2024-01-01T00:00:00Z",
            catchup=True,
            timezone="America/New_York",
        ))
        internal_trigger = self.converter.convert_trigger(v1_trigger)
        self.assertIsInstance(internal_trigger,
                              internal_triggers.ScheduleTriggerModel)
        self.assertEqual(internal_trigger.scheduleInterval, "* * * * *")
        self.assertEqual(internal_trigger.timezone, "America/New_York")

    def test_convert_trigger_unknown_type(self):
        """Tests that an unknown trigger type raises a TypeError."""
        v1_trigger = v1_protos.Trigger()  # Empty trigger
        with self.assertRaisesRegex(TypeError, "Unknown trigger type"):
            self.converter.convert_trigger(v1_trigger)

    def test_convert_action_unknown_type(self):
        """Tests that an unknown action type raises a TypeError."""
        v1_action = v1_protos.Action()  # Empty action
        defaults = v1_protos.Defaults()
        with self.assertRaisesRegex(TypeError, "Unknown action type"):
            self.converter.convert_action(v1_action,
                                          defaults,
                                          labels=self.labels)

    def test_convert_orchestration_pipeline_action(self):
        """Tests conversion of an orchestration pipeline action."""
        v1_action = v1_protos.OrchestrationPipelineAction(
            name="orchestration-task",
            pipeline_id="target-pipeline",
            execution_timeout="120s",
            depends_on=["dep1"],
            wait_for_completion=True,
            bundle_id="bundle-id",
        )

        internal_action = self.converter._convert_orchestration_pipeline_action(
            v1_action, self.defaults
        )

        self.assertIsInstance(
            internal_action, internal_actions.OrchestrationPipelineActionModel
        )
        self.assertEqual(internal_action.name, "orchestration-task")
        self.assertEqual(internal_action.type, "orchestration_pipeline")
        self.assertEqual(internal_action.pipeline_id, "target-pipeline")
        self.assertEqual(internal_action.wait_for_completion, True)
        self.assertEqual(internal_action.bundle_id, "bundle-id")
        self.assertEqual(internal_action.executionTimeout, "120s")
        self.assertEqual(internal_action.dependsOn, ["dep1"])

    def test_convert_python_action_script(self):
        """Tests conversion of a simple Python script action."""
        v1_action_script = v1_protos.PythonAction(name="py-script",
                                                  main_file_path="s.py",
                                                  python_callable="f",
                                                  depends_on=["dep1"])
        internal_script = self.converter._convert_python_action(
            v1_action_script)
        self.assertIsInstance(internal_script,
                              internal_actions.PythonScriptActionModel)
        self.assertEqual(internal_script.name, "py-script")
        self.assertEqual(internal_script.filename, "resolved/s.py")
        self.assertEqual(internal_script.dependsOn, ["dep1"])

    def test_convert_python_action_virtualenv_inline_reqs(self):
        """Tests conversion of a Python venv action with inline requirements."""
        op_kwargs_struct = struct_pb2.Struct()
        op_kwargs_struct.update({
            "someKey": "value",
            "nestedDict": {
                "anotherKey": 1
            }
        })
        v1_action_venv_reqs = v1_protos.PythonAction(
            name="py-venv",
            main_file_path="v.py",
            python_callable="f",
            op_kwargs=op_kwargs_struct,
            environment=v1_protos.PythonEnvironment(
                requirements=v1_protos.PythonEnvironment.Requirements(
                    inline=v1_protos.PythonEnvironment.InlineRequirements(
                        list=["lib==1.0"])),
                system_site_packages=True,
            ),
        )
        internal_venv = self.converter._convert_python_action(
            v1_action_venv_reqs)
        self.assertIsInstance(internal_venv,
                              internal_actions.PythonVirtualenvActionModel)
        self.assertEqual(internal_venv.filename, "resolved/v.py")
        self.assertEqual(internal_venv.config.requirements, ["lib==1.0"])
        self.assertTrue(internal_venv.config.systemSitePackages)
        self.assertEqual(internal_venv.config.opKwargs, {
            "someKey": "value",
            "nestedDict": {
                "anotherKey": 1
            },
        })

    def test_convert_python_action_virtualenv_path_reqs(self):
        """Tests conversion of a Python venv action with a requirements path."""
        v1_action_venv_path = v1_protos.PythonAction(
            name="py-venv-path",
            main_file_path="v.py",
            python_callable="f",
            environment=v1_protos.PythonEnvironment(
                requirements=v1_protos.PythonEnvironment.Requirements(
                    path="path/to/reqs.txt")),
        )
        internal_venv_path = self.converter._convert_python_action(
            v1_action_venv_path)
        self.assertIsInstance(internal_venv_path,
                              internal_actions.PythonVirtualenvActionModel)
        self.assertEqual(internal_venv_path.filename, "resolved/v.py")
        self.assertEqual(internal_venv_path.config.requirementsPath,
                         "path/to/reqs.txt")

    def test_convert_dataproc_gce_existing_cluster_action(self):
        """Tests converting a Dataproc GCE existing cluster action."""
        pyspark_gce_existing = v1_protos.PysparkAction(
            name="dp-gce-existing",
            main_file_path="main.py",
            execution_timeout="3600s",
            engine=v1_protos.PysparkEngine(
                dataproc_on_gce=v1_protos.DataprocOnGceEngine(
                    existing_cluster=(
                        v1_protos.DataprocExistingClusterConfiguration(
                            cluster_name="my-cluster",
                            location="us-west1",
                            project_id="action-project",
                            properties={"propA": "valA"},
                            impersonation_chain=["sa@impersonate.com"],
                        )))))
        internal_gce_existing = self.converter._convert_dataproc_action(
            pyspark_gce_existing, "pyspark", self.defaults, self.labels)
        self.assertEqual(internal_gce_existing.filename, "resolved/main.py")
        self.assertEqual(internal_gce_existing.engine.engineType,
                         "dataproc-gce")
        self.assertEqual(internal_gce_existing.engine.clusterMode, "existing")
        self.assertEqual(internal_gce_existing.config.cluster_name,
                         "my-cluster")
        self.assertEqual(internal_gce_existing.region, "us-west1")
        self.assertEqual(internal_gce_existing.config.project_id,
                         "action-project")
        self.assertEqual(internal_gce_existing.config.properties,
                         {"propA": "valA"})
        self.assertEqual(internal_gce_existing.impersonationChain,
                         ["sa@impersonate.com"])
        self.assertEqual(internal_gce_existing.executionTimeout, "3600s")
        self.assertEqual(internal_gce_existing.labels, self.labels)

    def test_convert_dataproc_gce_existing_cluster_action_with_defaults(self):
        """Tests converting a Dataproc GCE existing cluster action using defaults."""
        pyspark_gce_existing_defaults = v1_protos.PysparkAction(
            name="dp-gce-existing-defaults",
            main_file_path="main.py",
            engine=v1_protos.PysparkEngine(
                dataproc_on_gce=v1_protos.DataprocOnGceEngine(
                    existing_cluster=(
                        v1_protos.DataprocExistingClusterConfiguration(
                            cluster_name="my-cluster",
                            # No location or project_id, should use defaults
                        )))))
        internal_gce_existing_defaults = self.converter._convert_dataproc_action(
            pyspark_gce_existing_defaults, "pyspark", self.defaults,
            self.labels)
        self.assertEqual(internal_gce_existing_defaults.filename,
                         "resolved/main.py")
        self.assertEqual(internal_gce_existing_defaults.region,
                         "default-location")
        self.assertEqual(internal_gce_existing_defaults.config.project_id,
                         "default-project")
        self.assertEqual(internal_gce_existing_defaults.labels, self.labels)

    def test_convert_dataproc_gce_ephemeral_inline_config_action(self):
        """Tests converting a Dataproc GCE ephemeral cluster with inline config."""
        cluster_config_struct = struct_pb2.Struct()
        cluster_config_struct.update({"config_bucket": "some-bucket"})
        notebook_gce_ephemeral = v1_protos.NotebookAction(
            name="dp-gce-ephemeral",
            main_file_path="main.ipynb",
            engine=v1_protos.
            NotebookEngine(dataproc_on_gce=v1_protos.DataprocOnGceEngine(
                ephemeral_cluster=(v1_protos.DataprocEphemeralConfiguration(
                    cluster_name="temp-cluster",
                    location="us-west1",
                    project_id="proj",
                    properties={"propB": "valB"},
                    resource_profile=(v1_protos.DataprocClusterResourceProfile(
                        inline=v1_protos.DataprocClusterResourceProfile.
                        InlineConfig(
                            cluster_config=cluster_config_struct))))))))
        internal_gce_eph = self.converter._convert_dataproc_action(
            notebook_gce_ephemeral, "notebook", self.defaults, self.labels)
        self.assertEqual(internal_gce_eph.filename, "resolved/main.ipynb")
        self.assertEqual(internal_gce_eph.type, "notebook")
        self.assertEqual(internal_gce_eph.engine.clusterMode, "ephemeral")
        self.assertEqual(internal_gce_eph.config.cluster_name, "temp-cluster")
        self.assertEqual(internal_gce_eph.region, "us-west1")
        self.assertEqual(internal_gce_eph.config.project_id, "proj")
        self.assertEqual(internal_gce_eph.config.properties, {"propB": "valB"})
        self.assertEqual(internal_gce_eph.config.cluster_config,
                         {"config_bucket": "some-bucket"})
        self.assertEqual(internal_gce_eph.labels, self.labels)

    def test_convert_dataproc_gce_ephemeral_path_config_action(self):
        """Tests converting a Dataproc GCE ephemeral cluster with path config."""
        self.file_manager.read.return_value = "definition:\n  config:\n    clusterTier: CLUSTER_TIER_STANDARD"
        notebook_gce_ephemeral_path = v1_protos.NotebookAction(
            name="dp-gce-ephemeral-path",
            main_file_path="main.ipynb",
            engine=v1_protos.NotebookEngine(
                dataproc_on_gce=v1_protos.DataprocOnGceEngine(
                    ephemeral_cluster=(
                        v1_protos.DataprocEphemeralConfiguration(
                            cluster_name="temp-cluster",
                            location="us-west1",
                            project_id="proj",
                            resource_profile=(
                                v1_protos.DataprocClusterResourceProfile(
                                    path="gs://bucket/cluster.json")))))))
        internal_gce_eph_path = self.converter._convert_dataproc_action(
            notebook_gce_ephemeral_path, "notebook", self.defaults,
            self.labels)
        self.assertEqual(internal_gce_eph_path.filename, "resolved/main.ipynb")
        self.assertEqual(internal_gce_eph_path.config.cluster_config,
                         {"cluster_tier": "CLUSTER_TIER_STANDARD"})
        self.assertEqual(internal_gce_eph_path.labels, self.labels)
        self.assertIsNone(
            getattr(internal_gce_eph_path.config, "cluster_config_path", None))

    def test_convert_dataproc_gce_ephemeral_inline_config_with_overrides(self):
        """Tests GCE ephemeral cluster with inline config and overrides."""
        base_config_struct = struct_pb2.Struct()
        base_config_struct.update({
            "gceClusterConfig": {
                "metadata": {
                    "test": "true",
                    "override": "true"
                }
            }
        })
        override_config_struct = struct_pb2.Struct()
        override_config_struct.update({
            "gceClusterConfig": {
                "metadata": {
                    "override": "false",
                    "overridden": "true"
                },
            }
        })

        action = v1_protos.NotebookAction(
            name="dp-gce-eph-inline-override",
            main_file_path="main.ipynb",
            engine=v1_protos.
            NotebookEngine(dataproc_on_gce=v1_protos.DataprocOnGceEngine(
                ephemeral_cluster=v1_protos.DataprocEphemeralConfiguration(
                    resource_profile=v1_protos.DataprocClusterResourceProfile(
                        inline=v1_protos.DataprocClusterResourceProfile.
                        InlineConfig(config=base_config_struct),
                        overrides=v1_protos.DataprocClusterResourceProfile.
                        InlineConfig(config=override_config_struct))))))

        internal_action = self.converter._convert_dataproc_action(
            action, "notebook", self.defaults, self.labels)

        expected_config = {
            "gce_cluster_config": {
                "metadata": {
                    "test": "true",
                    "override": "false",
                    "overridden": "true"
                },
            }
        }
        self.assertEqual(internal_action.config.cluster_config,
                         expected_config)
        self.assertEqual(internal_action.labels, self.labels)

    def test_convert_dataproc_gce_ephemeral_path_config_with_overrides(self):
        """Tests GCE ephemeral cluster with path config and overrides."""
        self.file_manager.read.return_value = "definition:\n  config:\n    gceClusterConfig:\n      metadata:\n        test: 'true'\n        override: 'true'"
        override_config_struct = struct_pb2.Struct()
        override_config_struct.update({
            "gceClusterConfig": {
                "metadata": {
                    "override": "false",
                    "overridden": "true"
                }
            }
        })

        action = v1_protos.NotebookAction(
            name="dp-gce-eph-path-override",
            main_file_path="main.ipynb",
            engine=v1_protos.
            NotebookEngine(dataproc_on_gce=v1_protos.DataprocOnGceEngine(
                ephemeral_cluster=v1_protos.DataprocEphemeralConfiguration(
                    resource_profile=v1_protos.DataprocClusterResourceProfile(
                        path="gs://bucket/cluster.yaml",
                        overrides=v1_protos.DataprocClusterResourceProfile.
                        InlineConfig(config=override_config_struct))))))

        internal_action = self.converter._convert_dataproc_action(
            action, "notebook", self.defaults, self.labels)

        expected_config = {
            "gce_cluster_config": {
                "metadata": {
                    "test": "true",
                    "override": "false",
                    "overridden": "true"
                },
            }
        }
        self.assertEqual(internal_action.config.cluster_config,
                         expected_config)
        self.assertEqual(internal_action.labels, self.labels)

    def test_convert_dataproc_serverless_inline_config_action(self):
        """Tests converting a Dataproc Serverless action with inline config."""
        runtime_config = struct_pb2.Struct()
        runtime_config.update({
            "version": "2.1",
            "properties": {
                "spark.executor.instances": "2"
            }
        })
        env_config = struct_pb2.Struct()
        env_config.update({"executionConfig": {"subnetworkUri": "some-uri"}})
        pyspark_serverless_inline = v1_protos.PysparkAction(
            name="dp-serverless-inline",
            main_file_path="main.py",
            engine=v1_protos.PysparkEngine(
                dataproc_serverless=v1_protos.DataprocServerlessBatchEngine(
                    location="us-west1",
                    resource_profile=v1_protos.DataprocBatchResourceProfile(
                        inline=(v1_protos.DataprocBatchResourceProfile.
                                InlineConfig(
                                    runtime_config=runtime_config,
                                    environment_config=env_config,
                                ))))))
        internal_serverless_inline = self.converter._convert_dataproc_action(
            pyspark_serverless_inline, "pyspark", self.defaults, self.labels)
        self.assertEqual(internal_serverless_inline.filename,
                         "resolved/main.py")
        self.assertEqual(internal_serverless_inline.engine.engineType,
                         "dataproc-serverless")
        self.assertEqual(internal_serverless_inline.region, "us-west1")
        self.assertEqual(
            internal_serverless_inline.config.resourceProfile.runtimeConfig, {
                "version": "2.1",
                "properties": {
                    "spark.executor.instances": "2"
                }
            })
        self.assertEqual(
            internal_serverless_inline.config.resourceProfile.
            environmentConfig,
            {"execution_config": {
                "subnetwork_uri": "some-uri"
            }})
        self.assertEqual(internal_serverless_inline.labels, self.labels)

    def test_convert_dataproc_serverless_with_defaults_action(self):
        """Tests converting a Dataproc Serverless action using defaults."""
        self.file_manager.read.return_value = "{}"

        pyspark_serverless_defaults = v1_protos.PysparkAction(
            name="dp-serverless-defaults",
            main_file_path="main.py",
            engine=v1_protos.PysparkEngine(
                dataproc_serverless=v1_protos.DataprocServerlessBatchEngine(
                    # No location, should use default
                    resource_profile=v1_protos.DataprocBatchResourceProfile(
                        path="gs://bucket/config.json"))))
        internal_serverless_defaults = self.converter._convert_dataproc_action(
            pyspark_serverless_defaults, "pyspark", self.defaults, self.labels)
        self.assertEqual(internal_serverless_defaults.filename,
                         "resolved/main.py")
        self.assertEqual(internal_serverless_defaults.region,
                         "default-location")
        self.assertEqual(internal_serverless_defaults.labels, self.labels)

    def test_convert_dataproc_serverless_path_config_action(self):
        """Tests converting a Dataproc Serverless action with path config."""
        self.file_manager.read.return_value = (
            "definition:\n"
            "  runtimeConfig:\n"
            "    version: '2.2'\n"
            "    properties:\n"
            "      spark.some.prop: 'value'\n"
            "  environmentConfig:\n"
            "    executionConfig:\n"
            "      subnetworkUri: 'some-uri'")
        pyspark_serverless_path = v1_protos.PysparkAction(
            name="dp-serverless-path",
            main_file_path="main.py",
            engine=v1_protos.PysparkEngine(
                dataproc_serverless=v1_protos.DataprocServerlessBatchEngine(
                    location="us-west1",
                    resource_profile=v1_protos.DataprocBatchResourceProfile(
                        path="gs://bucket/config.json"))))
        internal_serverless_path = self.converter._convert_dataproc_action(
            pyspark_serverless_path, "pyspark", self.defaults, self.labels)
        self.assertEqual(internal_serverless_path.filename, "resolved/main.py")
        self.assertEqual(
            internal_serverless_path.config.resourceProfile.runtimeConfig, {
                "version": "2.2",
                "properties": {
                    "spark.some.prop": "value"
                }
            })
        self.assertEqual(
            internal_serverless_path.config.resourceProfile.environmentConfig,
            {"execution_config": {
                "subnetwork_uri": "some-uri"
            }})
        self.assertEqual(internal_serverless_path.labels, self.labels)

    def test_convert_dataproc_serverless_inline_config_with_overrides(self):
        """Tests a Dataproc Serverless action with inline config and overrides."""
        base_runtime_config = struct_pb2.Struct()
        base_runtime_config.update({
            "version": "2.1",
            "properties": {
                "test": "true",
                "override": "true"
            }
        })
        base_env_config = struct_pb2.Struct()
        base_env_config.update(
            {"executionConfig": {
                "subnetworkUri": "base-uri"
            }})

        override_env_config = struct_pb2.Struct()
        override_env_config.update(
            {"executionConfig": {
                "subnetworkUri": "override-uri"
            }})

        override_runtime_config = struct_pb2.Struct()
        override_runtime_config.update({
            "version": "2.1",
            "properties": {
                "test": "false",
                "overridden": "true"
            }
        })

        action = v1_protos.PysparkAction(
            name="dp-serverless-inline-override",
            main_file_path="main.py",
            engine=v1_protos.PysparkEngine(
                dataproc_serverless=v1_protos.DataprocServerlessBatchEngine(
                    resource_profile=v1_protos.DataprocBatchResourceProfile(
                        inline=v1_protos.DataprocBatchResourceProfile.
                        InlineConfig(runtime_config=base_runtime_config,
                                     environment_config=base_env_config),
                        overrides=v1_protos.DataprocBatchResourceProfile.
                        InlineConfig(
                            runtime_config=override_runtime_config,
                            environment_config=override_env_config)))))

        internal_action = self.converter._convert_dataproc_action(
            action, "pyspark", self.defaults, self.labels)

        expected_runtime = {
            "version": "2.1",
            "properties": {
                "test": "false",
                "override": "true",
                "overridden": "true"
            }
        }
        expected_env = {
            "execution_config": {
                "subnetwork_uri": "override-uri",
            }
        }
        self.assertEqual(internal_action.config.resourceProfile.runtimeConfig,
                         expected_runtime)
        self.assertEqual(
            internal_action.config.resourceProfile.environmentConfig,
            expected_env)
        self.assertEqual(internal_action.labels, self.labels)

    def test_convert_dataproc_serverless_path_config_with_overrides(self):
        """Tests a Dataproc Serverless action with path config and overrides."""
        self.file_manager.read.return_value = ("definition:\n"
                                               "  runtimeConfig:\n"
                                               "    version: '2.1'\n"
                                               "    properties:\n"
                                               "      test: 'true'\n")
        override_runtime_config = struct_pb2.Struct()
        override_runtime_config.update(
            {"properties": {
                "test": "false",
                "overridden": "true"
            }})

        action = v1_protos.PysparkAction(
            name="dp-serverless-path-override",
            main_file_path="main.py",
            engine=v1_protos.PysparkEngine(
                dataproc_serverless=v1_protos.DataprocServerlessBatchEngine(
                    resource_profile=v1_protos.DataprocBatchResourceProfile(
                        path="gs://bucket/config.yaml",
                        overrides=v1_protos.DataprocBatchResourceProfile.
                        InlineConfig(
                            runtime_config=override_runtime_config)))))

        internal_action = self.converter._convert_dataproc_action(
            action, "pyspark", self.defaults, self.labels)

        expected_runtime = {
            "version": "2.1",
            "properties": {
                "test": "false",
                "overridden": "true"
            }
        }
        self.assertEqual(internal_action.config.resourceProfile.runtimeConfig,
                         expected_runtime)
        self.assertEqual(internal_action.labels, self.labels)

    def test_convert_sql_action_bq_inline(self):
        """Tests conversion of a BigQuery action with an inline query."""
        sql_bq_inline = v1_protos.SqlAction(
            name="bq-inline",
            execution_timeout="1h",
            query=v1_protos.Query(inline="SELECT 1"),
            engine=v1_protos.SqlEngine(bigquery=v1_protos.BigQueryEngine(
                location="US",
                destination_table="p.d.t",
                impersonation_chain=["sa@impersonate.com"],
            )))
        internal_bq_inline = self.converter._convert_sql_action(
            sql_bq_inline, self.defaults, self.labels)
        self.assertIsInstance(internal_bq_inline,
                              internal_actions.BqOperationActionModel)
        self.assertEqual(internal_bq_inline.query, "SELECT 1")
        self.assertIsNone(internal_bq_inline.filename)
        self.assertEqual(internal_bq_inline.config.location, "US")
        self.assertEqual(internal_bq_inline.config.destinationTable, "p.d.t")
        self.assertEqual(internal_bq_inline.executionTimeout, "1h")
        self.assertEqual(internal_bq_inline.labels, self.labels)

    def test_convert_sql_action_bq_with_defaults(self):
        """Tests conversion of a BigQuery action using default location."""
        sql_bq_defaults = v1_protos.SqlAction(
            name="bq-defaults",
            query=v1_protos.Query(inline="SELECT 1"),
            engine=v1_protos.SqlEngine(bigquery=v1_protos.BigQueryEngine(
                # No location, should use default
                destination_table="p.d.t", )))
        internal_bq_defaults = self.converter._convert_sql_action(
            sql_bq_defaults, self.defaults, self.labels)
        self.assertEqual(internal_bq_defaults.config.location,
                         "default-location")
        self.assertEqual(internal_bq_defaults.labels, self.labels)

    def test_convert_sql_action_bq_path(self):
        """Tests conversion of a BigQuery action with a query path."""
        sql_bq_path = v1_protos.SqlAction(
            name="bq-path",
            query=v1_protos.Query(path="path/to/query.sql"),
            engine=v1_protos.SqlEngine(bigquery=v1_protos.BigQueryEngine(
                location="US")))
        internal_bq_path = self.converter._convert_sql_action(
            sql_bq_path, self.defaults, self.labels)
        self.assertEqual(internal_bq_path.filename,
                         "resolved/path/to/query.sql")
        self.assertEqual(internal_bq_path.labels, self.labels)

    def test_convert_sql_action_dataproc_serverless(self):
        """Tests conversion of a Dataproc Serverless SQL action."""
        runtime_config = struct_pb2.Struct()
        runtime_config.update({"version": "2.1"})
        sql_dp_serverless_inline = v1_protos.SqlAction(
            name="dp-sql-inline",
            query=v1_protos.Query(inline="SELECT 1"),
            engine=v1_protos.SqlEngine(
                dataproc_serverless=v1_protos.DataprocServerlessBatchEngine(
                    location="us-west1",
                    impersonation_chain=["sa@impersonate.com"],
                    resource_profile=v1_protos.DataprocBatchResourceProfile(
                        inline=(v1_protos.DataprocBatchResourceProfile.
                                InlineConfig(
                                    runtime_config=runtime_config))))))
        internal_dp_sql_inline = self.converter._convert_sql_action(
            sql_dp_serverless_inline, self.defaults, self.labels)

        self.assertIsInstance(internal_dp_sql_inline,
                              internal_actions.DataprocOperatorActionModel)
        self.assertEqual(internal_dp_sql_inline.name, "dp-sql-inline")
        self.assertEqual(internal_dp_sql_inline.type, "sql")
        self.assertEqual(internal_dp_sql_inline.query, "SELECT 1")
        self.assertIsNone(internal_dp_sql_inline.filename)
        self.assertEqual(internal_dp_sql_inline.engine.engineType,
                         "dataproc-serverless")
        self.assertEqual(internal_dp_sql_inline.region, "us-west1")
        self.assertEqual(internal_dp_sql_inline.impersonationChain,
                         ["sa@impersonate.com"])
        self.assertEqual(
            internal_dp_sql_inline.config.resourceProfile.runtimeConfig,
            {"version": "2.1"})
        self.assertEqual(internal_dp_sql_inline.labels, self.labels)

    def test_convert_sql_action_dataproc_gce_existing(self):
        """Tests conversion of a Dataproc GCE Existing Cluster SQL action."""
        sql_dp_gce_existing = v1_protos.SqlAction(
            name="dp-gce-existing-sql",
            query=v1_protos.Query(inline="SELECT 1"),
            engine=v1_protos.SqlEngine(
                dataproc_on_gce=v1_protos.DataprocOnGceEngine(
                    existing_cluster=v1_protos.DataprocExistingClusterConfiguration(
                        cluster_name="my-cluster",
                        location="us-central1",
                        project_id="my-project",
                    )
                )
            )
        )
        internal_dp_sql = self.converter._convert_sql_action(
            sql_dp_gce_existing, self.defaults, self.labels)

        self.assertIsInstance(internal_dp_sql,
                              internal_actions.DataprocOperatorActionModel)
        self.assertEqual(internal_dp_sql.name, "dp-gce-existing-sql")
        self.assertEqual(internal_dp_sql.type, "sql")
        self.assertEqual(internal_dp_sql.query, "SELECT 1")
        self.assertEqual(internal_dp_sql.engine.engineType, "dataproc-gce")
        self.assertEqual(internal_dp_sql.engine.clusterMode, "existing")
        self.assertEqual(internal_dp_sql.region, "us-central1")
        self.assertEqual(internal_dp_sql.config.cluster_name, "my-cluster")

    def test_convert_sql_action_dataproc_gce_ephemeral(self):
        """Tests conversion of a Dataproc GCE Ephemeral Cluster SQL action."""
        cluster_config_struct = struct_pb2.Struct()
        cluster_config_struct.update({"config_bucket": "some-bucket"})
        sql_dp_gce_ephemeral = v1_protos.SqlAction(
            name="dp-gce-eph-sql",
            query=v1_protos.Query(inline="SELECT 1"),
            engine=v1_protos.SqlEngine(
                dataproc_on_gce=v1_protos.DataprocOnGceEngine(
                    ephemeral_cluster=v1_protos.DataprocEphemeralConfiguration(
                        cluster_name="temp-cluster",
                        location="us-central1",
                        project_id="my-project",
                        resource_profile=v1_protos.DataprocClusterResourceProfile(
                            inline=v1_protos.DataprocClusterResourceProfile.InlineConfig(
                                cluster_config=cluster_config_struct
                            )
                        )
                    )
                )
            )
        )
        internal_dp_sql = self.converter._convert_sql_action(
            sql_dp_gce_ephemeral, self.defaults, self.labels)

        self.assertIsInstance(internal_dp_sql,
                              internal_actions.DataprocOperatorActionModel)
        self.assertEqual(internal_dp_sql.name, "dp-gce-eph-sql")
        self.assertEqual(internal_dp_sql.type, "sql")
        self.assertEqual(internal_dp_sql.query, "SELECT 1")
        self.assertEqual(internal_dp_sql.engine.engineType, "dataproc-gce")
        self.assertEqual(internal_dp_sql.engine.clusterMode, "ephemeral")
        self.assertEqual(internal_dp_sql.region, "us-central1")
        self.assertEqual(internal_dp_sql.config.cluster_name, "temp-cluster")
        self.assertEqual(internal_dp_sql.config.cluster_config, {"config_bucket": "some-bucket"})

    def test_convert_sql_action_unknown_engine(self):
        """Tests that an unknown SQL engine type raises a TypeError."""
        sql_unknown_engine = v1_protos.SqlAction(
            name="unknown-sql",
            query=v1_protos.Query(inline="SELECT 1"),
            engine=v1_protos.SqlEngine())
        with self.assertRaisesRegex(TypeError, "Unknown SQL engine type"):
            self.converter._convert_sql_action(sql_unknown_engine,
                                               self.defaults, self.labels)

    def test_convert_pipeline_action_dbt_local(self):
        """Tests conversion of a dbt local execution action."""
        dbt_action = v1_protos.PipelineAction(
            name="dbt-task",
            depends_on=["dep1"],
            framework=v1_protos.PipelineFramework(
                dbt=v1_protos.DbtFrameworkSpec(
                    airflow_worker=v1_protos.DbtAirflowExecution(
                        project_directory_path="dbt/project",
                        select_models=["model1"],
                    ))))
        internal_dbt = self.converter._convert_pipeline_action(
            dbt_action, self.defaults)
        self.assertIsInstance(internal_dbt, internal_actions.DBTActionModel)
        self.assertEqual(internal_dbt.source.path, "resolved/dbt/project")
        self.assertEqual(internal_dbt.select_models, ["model1"])

    def test_convert_pipeline_action_dataform_local(self):
        """Tests conversion of a Dataform local execution action."""
        dataform_local_action = v1_protos.PipelineAction(
            name="dataform-local-task",
            framework=v1_protos.PipelineFramework(
                dataform=v1_protos.DataformFrameworkSpec(
                    airflow_worker=v1_protos.DataformAirflowExecution(
                        project_directory_path="dataform/project"))))
        internal_df_local = self.converter._convert_pipeline_action(
            dataform_local_action, self.defaults)
        self.assertIsInstance(internal_df_local,
                              internal_actions.DataformActionModel)
        self.assertEqual(internal_df_local.executionMode, "local")
        self.assertEqual(internal_df_local.dataform_project_path,
                         "gs://bucket/resolved/dataform/project")

    def test_convert_pipeline_action_dataform_service(self):
        """Tests conversion of a Dataform service execution action."""
        workflow_invocation_struct = struct_pb2.Struct()
        workflow_invocation_struct.update({
            "compilationResult": "some-compilation-result",
            "invocationConfig": {
                "includedTags": ["daily"]
            }
        })
        dataform_service_action = v1_protos.PipelineAction(
            name="dataform-service-task",
            framework=v1_protos.PipelineFramework(
                dataform=v1_protos.DataformFrameworkSpec(
                    dataform_service=v1_protos.DataformServiceExecution(
                        project_id="p",
                        location="r",
                        repository_id="repo",
                        workflow_invocation=workflow_invocation_struct,
                    ))))
        internal_df_service = self.converter._convert_pipeline_action(
            dataform_service_action, self.defaults)
        self.assertIsInstance(internal_df_service,
                              internal_actions.DataformActionModel)
        self.assertEqual(internal_df_service.executionMode, "service")
        self.assertEqual(internal_df_service.dataformServiceConfig.project_id,
                         "p")
        self.assertEqual(internal_df_service.dataformServiceConfig.region, "r")
        self.assertEqual(
            internal_df_service.dataformServiceConfig.workflow_invocation, {
                "compilation_result": "some-compilation-result",
                "invocation_config": {
                    "included_tags": ["daily"]
                }
            })

    def test_convert_pipeline_action_dataform_service_with_defaults(self):
        """Tests conversion of a Dataform service action using defaults."""
        workflow_invocation_struct = struct_pb2.Struct()
        workflow_invocation_struct.update({
            "compilationResult": "some-compilation-result",
            "invocationConfig": {
                "includedTags": ["daily"]
            }
        })

        dataform_service_defaults = v1_protos.PipelineAction(
            name="dataform-service-task-defaults",
            framework=v1_protos.PipelineFramework(
                dataform=v1_protos.DataformFrameworkSpec(
                    dataform_service=v1_protos.DataformServiceExecution(
                        # No project_id or location, should use defaults
                        repository_id="repo",
                        workflow_invocation=workflow_invocation_struct,
                    ))))
        internal_df_service_defaults = (
            self.converter._convert_pipeline_action(dataform_service_defaults,
                                                    self.defaults))
        df_config = internal_df_service_defaults.dataformServiceConfig
        self.assertEqual(df_config.project_id, "default-project")
        self.assertEqual(df_config.region, "default-location")

    def test_convert_pipeline_action_unknown_framework(self):
        """Tests that an unknown pipeline framework raises a TypeError."""
        unknown_framework_action = v1_protos.PipelineAction(
            name="unknown-framework", framework=v1_protos.PipelineFramework())
        with self.assertRaisesRegex(TypeError, "Unknown pipeline framework"):
            self.converter._convert_pipeline_action(unknown_framework_action,
                                                    self.defaults)

    def test_get_labels(self):
        """Tests the _get_labels method."""
        tags1 = ["tag1", "job:my-job", "job:another:value"]
        expected1 = {
            "orchestration_pipeline": "true",
            "my-job": "true",
            "another": "value",
        }
        self.assertEqual(self.converter._get_labels(tags1), expected1)

        tags2 = ["no-job-tags"]
        expected2 = {"orchestration_pipeline": "true"}
        self.assertEqual(self.converter._get_labels(tags2), expected2)

        tags3 = []
        expected3 = {"orchestration_pipeline": "true"}
        self.assertEqual(self.converter._get_labels(tags3), expected3)

    def test_convert_data_ingestion_action(self):
        """Tests conversion of a DataIngestion action."""
        data_ingestion_action = v1_protos.DataIngestionAction(
            name="test-dts",
            execution_timeout="600s",
            depends_on=["dep1"],
        )
        spec = data_ingestion_action.bigquery_dts
        spec.project_id = "proj"
        spec.location = "loc"
        spec.transfer_config_id = "config-123"
        spec.impersonation_chain.append("sa@impersonate.com")
        spec.runtime_params.update({"requested_run_time": {"seconds": 12345}})

        internal_dts = self.converter._convert_data_ingestion_action(
            data_ingestion_action, self.defaults, self.labels
        )

        self.assertIsInstance(
            internal_dts, internal_actions.DataIngestionActionModel
        )
        self.assertEqual(internal_dts.name, "test-dts")
        self.assertEqual(internal_dts.type, "data_ingestion")
        self.assertEqual(internal_dts.executionTimeout, "600s")
        self.assertEqual(internal_dts.dependsOn, ["dep1"])
        self.assertEqual(internal_dts.config.projectId, "proj")
        self.assertEqual(internal_dts.config.location, "loc")
        self.assertEqual(internal_dts.config.transferConfigId, "config-123")
        self.assertEqual(
            internal_dts.config.impersonationChain, ["sa@impersonate.com"]
        )
        self.assertEqual(
            internal_dts.config.runtimeParams,
            {"requested_run_time": {"seconds": 12345}},
        )

    def test_convert_data_ingestion_action_defaults(self):
        """Tests DataIngestion action conversion falling back to defaults."""
        data_ingestion_action = v1_protos.DataIngestionAction(
            name="test-dts-defaults",
        )
        spec = data_ingestion_action.bigquery_dts
        spec.transfer_config_id = "config-456"

        internal_dts = self.converter._convert_data_ingestion_action(
            data_ingestion_action, self.defaults, self.labels
        )

        self.assertEqual(internal_dts.config.projectId, "default-project")
        self.assertEqual(internal_dts.config.location, "default-location")
        self.assertIsNone(internal_dts.config.runtimeParams)
        self.assertIsNone(internal_dts.config.impersonationChain)

    def test_convert_data_ingestion_action_missing_transfer_config(self):
        """Tests that missing transfer_config raises ValueError."""
        data_ingestion_action = v1_protos.DataIngestionAction(
            name="test-missing",
        )
        # Populating an inner field sets the config oneof while leaving
        # transfer_config unset
        data_ingestion_action.bigquery_dts.impersonation_chain.append(
            "sa@test.com"
        )

        with self.assertRaises(ValueError):
            self.converter._convert_data_ingestion_action(
                data_ingestion_action, self.defaults, self.labels
            )

    def test_convert_data_ingestion_action_missing_config(self):
        """Tests that missing config type raises TypeError."""
        data_ingestion_action = v1_protos.DataIngestionAction(
            name="test-missing-config",
        )
        with self.assertRaisesRegex(
            TypeError, "Unknown DataIngestionAction config type: None"
        ):
            self.converter._convert_data_ingestion_action(
                data_ingestion_action, self.defaults, self.labels
            )

    def test_convert_data_ingestion_action_unknown_config(self):
        """Tests that unknown config type raises TypeError."""
        data_ingestion_action = MagicMock()
        data_ingestion_action.WhichOneof.return_value = "unknown_config"

        with self.assertRaises(TypeError):
            self.converter._convert_data_ingestion_action(
                data_ingestion_action, self.defaults, self.labels
            )


if __name__ == "__main__":
    unittest.main()
