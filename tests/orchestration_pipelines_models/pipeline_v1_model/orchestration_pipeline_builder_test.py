# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""Unit tests for the OrchestrationPipelineBuilder."""

import unittest
import os
import yaml

from orchestration_pipelines_models.pipeline_v1_model.orchestration_pipeline_builder import (
    OrchestrationPipelineBuilder, )
from orchestration_pipelines_models.pipeline_v1_model.protos.orchestration_pipeline_pb2 import (
    OrchestrationPipeline,
    PipelineRunner,
)


class TestOrchestrationPipelineBuilder(unittest.TestCase):
    """Test suite for the OrchestrationPipelineBuilder."""

    def _get_valid_pipeline_def(self, **kwargs):
        """Returns a minimal valid pipeline definition, overrideable with kwargs."""
        base = {
            "model_version": "1.0",
            "pipeline_id": "test_pipeline",
            "runner": "airflow",
            "owner": "example-owner",
            "defaults": {
                "project_id": "example-project",
                "location": "us-central1",
                "execution_config": {
                    "retries": 1
                }
            },
            "actions": [
                {
                    "python": {
                        "name": "dummy_action",
                        "main_file_path": "dummy.py",
                        "python_callable": "dummy",
                        "engine": {"local": {}}
                    }
                }
            ]
        }
        base.update(kwargs)
        return base

    def test_build_sets_metadata(self):
        """Tests that basic metadata is set correctly."""
        pipeline_def = self._get_valid_pipeline_def(
            pipeline_id="test_pipeline_1",
            description="A test pipeline",
            tags=["test", "example"]
        )

        pipeline = OrchestrationPipelineBuilder.build(pipeline_def)

        self.assertIsInstance(pipeline, OrchestrationPipeline)
        self.assertEqual(pipeline.model_version, "1.0")
        self.assertEqual(pipeline.pipeline_id, "test_pipeline_1")
        self.assertEqual(pipeline.description, "A test pipeline")
        self.assertEqual(pipeline.runner, PipelineRunner.airflow)
        self.assertEqual(pipeline.owner, "example-owner")
        self.assertListEqual(list(pipeline.tags), ["test", "example"])

    def test_build_sets_defaults(self):
        """Tests that defaults are set correctly."""
        pipeline_def = self._get_valid_pipeline_def(
            defaults={
                "project_id": "example-project",
                "location": "us-central1",
                "execution_config": {
                    "retries": 1
                }
            }
        )

        pipeline = OrchestrationPipelineBuilder.build(pipeline_def)

        self.assertTrue(pipeline.HasField("defaults"))
        self.assertEqual(pipeline.defaults.project_id, "example-project")
        self.assertEqual(pipeline.defaults.location, "us-central1")
        self.assertTrue(pipeline.defaults.HasField("execution_config"))
        self.assertEqual(pipeline.defaults.execution_config.retries, 1)

    def test_build_sets_notifications(self):
        """Tests that notifications are set correctly."""
        pipeline_def = self._get_valid_pipeline_def(
            notifications={
                "on_pipeline_failure": {
                    "email": ["user1@example.com", "user2@example.com"]
                }
            }
        )

        pipeline = OrchestrationPipelineBuilder.build(pipeline_def)

        self.assertTrue(pipeline.HasField("notifications"))
        self.assertTrue(pipeline.notifications.HasField("on_pipeline_failure"))
        self.assertListEqual(
            list(pipeline.notifications.on_pipeline_failure.email),
            ["user1@example.com", "user2@example.com"],
        )

    def test_build_sets_triggers(self):
        """Tests that triggers are set correctly."""
        pipeline_def = self._get_valid_pipeline_def(
            triggers=[
                {
                    "schedule": {
                        "interval": "0 4 * * *",
                        "start_time": "2025-10-01T00:00:00",
                        "end_time": "2026-10-01T00:00:00",
                        "catchup": False,
                        "timezone": "UTC"
                    }
                }
            ]
        )

        pipeline = OrchestrationPipelineBuilder.build(pipeline_def)

        self.assertEqual(len(pipeline.triggers), 1)
        trigger = pipeline.triggers[0]
        self.assertTrue(trigger.HasField("schedule"))
        schedule = trigger.schedule
        self.assertEqual(schedule.interval, "0 4 * * *")
        self.assertEqual(schedule.start_time, "2025-10-01T00:00:00")
        self.assertEqual(schedule.end_time, "2026-10-01T00:00:00")
        self.assertFalse(schedule.catchup)
        self.assertEqual(schedule.timezone, "UTC")

    def test_build_action_bq_create(self):
        """Tests BQ inline action."""
        pipeline_def = self._get_valid_pipeline_def(
            actions=[
                {
                    "sql": {
                        "name": "run_bigquery_insert_job_create",
                        "query": {
                            "inline": "CREATE TABLE IF NOT EXISTS `example-project.my_dataset.my_table` (id INT64, name STRING, timestamp TIMESTAMP );"
                        },
                        "execution_timeout": "1h 30m",
                        "engine": {
                            "bigquery": {
                                "location": "US",
                                "impersonation_chain": ["test-sa@example-project.iam.gserviceaccount.com"]
                            }
                        }
                    }
                }
            ]
        )

        pipeline = OrchestrationPipelineBuilder.build(pipeline_def)

        self.assertEqual(len(pipeline.actions), 1)
        action = pipeline.actions[0]
        self.assertTrue(action.HasField("sql"))
        sql_action = action.sql
        self.assertEqual(sql_action.name, "run_bigquery_insert_job_create")
        self.assertTrue(sql_action.query.HasField("inline"))
        self.assertEqual(
            sql_action.query.inline,
            ("CREATE TABLE IF NOT EXISTS `example-project.my_dataset.my_table` "
             "(id INT64, name STRING, timestamp TIMESTAMP );"),
        )
        self.assertEqual(sql_action.execution_timeout, "1h 30m")
        self.assertTrue(sql_action.engine.HasField("bigquery"))
        bq_engine = sql_action.engine.bigquery
        self.assertEqual(bq_engine.location, "US")
        self.assertListEqual(
            list(bq_engine.impersonation_chain),
            ["test-sa@example-project.iam.gserviceaccount.com"],
        )

    def test_build_action_bq_select(self):
        """Tests BQ Select action."""
        pipeline_def = self._get_valid_pipeline_def(
            actions=[
                {
                    "sql": {
                        "name": "run_bigquery_insert_job_create",
                        "query": {"inline": "CREATE TABLE ..."},
                        "engine": {"bigquery": {}}
                    }
                },
                {
                    "sql": {
                        "name": "run_bigquery_insert_job_select",
                        "depends_on": ["run_bigquery_insert_job_create"],
                        "query": {
                            "inline": "SELECT COUNT(*) AS row_count FROM `example-project.my_dataset.my_table` LIMIT 1;"
                        },
                        "engine": {
                            "bigquery": {
                                "location": "US",
                                "destination_table": "example-project.my_dataset.my_table_query_results",
                                "impersonation_chain": ["test-sa@example-project.iam.gserviceaccount.com"]
                            }
                        }
                    }
                }
            ]
        )

        pipeline = OrchestrationPipelineBuilder.build(pipeline_def)

        self.assertEqual(len(pipeline.actions), 2)
        action = pipeline.actions[1]
        self.assertTrue(action.HasField("sql"))
        sql_action = action.sql
        self.assertEqual(sql_action.name, "run_bigquery_insert_job_select")
        self.assertListEqual(list(sql_action.depends_on),
                             ["run_bigquery_insert_job_create"])
        self.assertTrue(sql_action.query.HasField("inline"))
        self.assertEqual(
            sql_action.query.inline,
            ("SELECT COUNT(*) AS row_count FROM "
             "`example-project.my_dataset.my_table` LIMIT 1;"),
        )
        self.assertTrue(sql_action.engine.HasField("bigquery"))
        bq_engine = sql_action.engine.bigquery
        self.assertEqual(bq_engine.location, "US")
        self.assertEqual(
            bq_engine.destination_table,
            ("example-project.my_dataset.my_table_query_results"),
        )
        self.assertListEqual(
            list(bq_engine.impersonation_chain),
            ["test-sa@example-project.iam.gserviceaccount.com"],
        )

    def test_build_action_bq_select_file(self):
        """Tests BQ Select File action."""
        pipeline_def = self._get_valid_pipeline_def(
            actions=[
                {
                    "sql": {
                        "name": "run_bigquery_insert_job_create",
                        "query": {"inline": "CREATE TABLE ..."},
                        "engine": {"bigquery": {}}
                    }
                },
                {
                    "sql": {
                        "name": "run_bigquery_insert_job_select_file",
                        "depends_on": ["run_bigquery_insert_job_create"],
                        "query": {
                            "path": "count_rows.sql"
                        },
                        "engine": {
                            "bigquery": {
                                "location": "US",
                                "destination_table": "example-project.my_dataset.my_table_query_results2",
                                "impersonation_chain": ["test-sa@example-project.iam.gserviceaccount.com"]
                            }
                        }
                    }
                }
            ]
        )

        pipeline = OrchestrationPipelineBuilder.build(pipeline_def)

        self.assertEqual(len(pipeline.actions), 2)
        action = pipeline.actions[1]
        self.assertTrue(action.HasField("sql"))
        sql_action = action.sql
        self.assertEqual(sql_action.name,
                         "run_bigquery_insert_job_select_file")
        self.assertListEqual(list(sql_action.depends_on),
                             ["run_bigquery_insert_job_create"])
        self.assertTrue(sql_action.query.HasField("path"))
        self.assertEqual(sql_action.query.path, "count_rows.sql")
        self.assertTrue(sql_action.engine.HasField("bigquery"))
        bq_engine = sql_action.engine.bigquery
        self.assertEqual(bq_engine.location, "US")
        self.assertEqual(bq_engine.destination_table,
                         "example-project.my_dataset.my_table_query_results2")
        self.assertListEqual(
            list(bq_engine.impersonation_chain),
            ["test-sa@example-project.iam.gserviceaccount.com"],
        )

    def test_build_action_dbt(self):
        """Tests DBT action."""
        pipeline_def = self._get_valid_pipeline_def(
            actions=[
                {
                    "sql": {
                        "name": "run_bigquery_insert_job_select_file",
                        "query": {"inline": "SELECT 1"},
                        "engine": {"bigquery": {}}
                    }
                },
                {
                    "pipeline": {
                        "name": "dbt_first_script_run",
                        "depends_on": ["run_bigquery_insert_job_select_file"],
                        "execution_timeout": "30m",
                        "framework": {
                            "dbt": {
                                "airflow_worker": {
                                    "project_directory_path": "/home/airflow/gcs/dags/dbt_project",
                                    "select_models": ["model_1_stg", "model_2_vip"],
                                    "tags": ["tag1"]
                                }
                            }
                        }
                    }
                }
            ]
        )

        pipeline = OrchestrationPipelineBuilder.build(pipeline_def)

        self.assertEqual(len(pipeline.actions), 2)
        action = pipeline.actions[1]
        self.assertTrue(action.HasField("pipeline"))
        pipeline_action = action.pipeline
        self.assertEqual(pipeline_action.name, "dbt_first_script_run")
        self.assertListEqual(list(pipeline_action.depends_on),
                             ["run_bigquery_insert_job_select_file"])
        self.assertEqual(pipeline_action.execution_timeout, "30m")
        self.assertTrue(pipeline_action.framework.HasField("dbt"))
        dbt = pipeline_action.framework.dbt
        self.assertTrue(dbt.HasField("airflow_worker"))
        airflow_worker = dbt.airflow_worker
        self.assertEqual(
            airflow_worker.project_directory_path,
            "/home/airflow/gcs/dags/dbt_project",
        )
        self.assertListEqual(list(airflow_worker.select_models),
                             ["model_1_stg", "model_2_vip"])
        self.assertListEqual(list(airflow_worker.tags), ["tag1"])

    def test_build_action_dataform(self):
        """Tests Dataform action."""
        pipeline_def = self._get_valid_pipeline_def(
            actions=[
                {
                    "pipeline": {
                        "name": "run_dataform",
                        "framework": {
                            "dataform": {
                                "dataform_service": {
                                    "location": "us-central1",
                                    "project_id": "example-project",
                                    "repository_id": "example-dataform-repo",
                                    "workflow_invocation": {
                                        "workspaceId": "example-dataform-workspace",
                                        "workflowSettingsYaml": "example_workflow_settings",
                                        "path": "definitions/example.sqlx",
                                        "fileContents": "config { type: 'table' } select 1"
                                    }
                                }
                            }
                        }
                    }
                }
            ]
        )

        pipeline = OrchestrationPipelineBuilder.build(pipeline_def)

        self.assertEqual(len(pipeline.actions), 1)
        action = pipeline.actions[0]
        self.assertTrue(action.HasField("pipeline"))
        pipeline_action = action.pipeline
        self.assertEqual(pipeline_action.name, "run_dataform")
        self.assertTrue(pipeline_action.framework.HasField("dataform"))
        dataform = pipeline_action.framework.dataform
        self.assertTrue(dataform.HasField("dataform_service"))
        dataform_service = dataform.dataform_service
        self.assertEqual(dataform_service.location, "us-central1")
        self.assertEqual(dataform_service.project_id, "example-project")
        self.assertEqual(dataform_service.repository_id, "example-dataform-repo")
        self.assertTrue(dataform_service.HasField("workflow_invocation"))
        workflow_invocation = dataform_service.workflow_invocation
        self.assertEqual(workflow_invocation["workspaceId"],
                         "example-dataform-workspace")
        self.assertEqual(workflow_invocation["workflowSettingsYaml"],
                         "example_workflow_settings")
        self.assertEqual(workflow_invocation["path"],
                         "definitions/example.sqlx")
        self.assertEqual(
            workflow_invocation["fileContents"],
            "config { type: 'table' } select 1",
        )

    def test_build_action_dataproc_ephemeral_notebook(self):
        """Tests Dataproc Ephemeral Notebook action."""
        pipeline_def = self._get_valid_pipeline_def(
            actions=[
                {
                    "notebook": {
                        "name": "run_dataproc_ephemeral_notebook",
                        "main_file_path": "gs://example-bucket/data/trivialNotebook.ipynb",
                        "engine": {
                            "dataproc_on_gce": {
                                "ephemeral_cluster": {
                                    "location": "us-central1",
                                    "project_id": "example-project",
                                    "cluster_name": "ephemeral-cluster-test",
                                    "resource_profile": {
                                        "inline": {
                                            "cluster_config": {
                                                "masterConfig": {
                                                    "numInstances": 1,
                                                    "machineTypeUri": "n1-standard-4",
                                                    "diskConfig": {
                                                        "bootDiskType": "pd-standard",
                                                        "bootDiskSizeGb": 1024
                                                    }
                                                },
                                                "workerConfig": {
                                                    "numInstances": 2,
                                                    "machineTypeUri": "n1-standard-4",
                                                    "diskConfig": {
                                                        "bootDiskType": "pd-standard",
                                                        "bootDiskSizeGb": 1024
                                                    }
                                                }
                                            }
                                        }
                                    },
                                    "impersonation_chain": ["test-sa@example-project.iam.gserviceaccount.com"]
                                }
                            }
                        }
                    }
                }
            ]
        )

        pipeline = OrchestrationPipelineBuilder.build(pipeline_def)

        self.assertEqual(len(pipeline.actions), 1)
        action = pipeline.actions[0]
        self.assertTrue(action.HasField("notebook"))
        notebook_action = action.notebook
        self.assertEqual(notebook_action.name,
                         "run_dataproc_ephemeral_notebook")
        self.assertEqual(
            notebook_action.main_file_path,
            "gs://example-bucket/data/trivialNotebook.ipynb",
        )
        self.assertTrue(notebook_action.engine.HasField("dataproc_on_gce"))
        dataproc_on_gce = notebook_action.engine.dataproc_on_gce
        self.assertTrue(dataproc_on_gce.HasField("ephemeral_cluster"))
        ephemeral_cluster = dataproc_on_gce.ephemeral_cluster
        self.assertEqual(ephemeral_cluster.location, "us-central1")
        self.assertEqual(ephemeral_cluster.project_id, "example-project")
        self.assertEqual(ephemeral_cluster.cluster_name,
                         "ephemeral-cluster-test")
        self.assertTrue(ephemeral_cluster.resource_profile.HasField("inline"))
        inline_profile = ephemeral_cluster.resource_profile.inline
        cluster_config = inline_profile.cluster_config
        master_config = cluster_config["masterConfig"]
        self.assertEqual(master_config["numInstances"], 1)
        self.assertEqual(master_config["machineTypeUri"], "n1-standard-4")
        self.assertEqual(master_config["diskConfig"]["bootDiskType"],
                         "pd-standard")
        self.assertEqual(master_config["diskConfig"]["bootDiskSizeGb"], 1024)
        worker_config = cluster_config["workerConfig"]
        self.assertEqual(worker_config["numInstances"], 2)
        self.assertEqual(worker_config["machineTypeUri"], "n1-standard-4")
        self.assertEqual(worker_config["diskConfig"]["bootDiskType"],
                         "pd-standard")
        self.assertEqual(worker_config["diskConfig"]["bootDiskSizeGb"], 1024)
        self.assertListEqual(
            list(ephemeral_cluster.impersonation_chain),
            ["test-sa@example-project.iam.gserviceaccount.com"],
        )

    def test_build_action_dataproc_serverless_notebook(self):
        """Tests Dataproc Serverless Notebook action."""
        pipeline_def = self._get_valid_pipeline_def(
            actions=[
                {
                    "notebook": {
                        "name": "run_dataproc_ephemeral_notebook",
                        "main_file_path": "gs://example-bucket/notebook.ipynb",
                        "engine": {
                            "dataproc_serverless": {
                                "resource_profile": {
                                    "path": "dataproc_config.yml"
                                }
                            }
                        }
                    }
                },
                {
                    "notebook": {
                        "name": "run-notebook-on-dataproc-serverless",
                        "depends_on": ["run_dataproc_ephemeral_notebook"],
                        "execution_timeout": "45m",
                        "main_file_path": "gs://example-bucket/notebookWithArchivesCheck.ipynb",
                        "staging_bucket": "example-bucket",
                        "archive_uris": ["gs://example-bucket/custom_venv.tar.gz"],
                        "engine": {
                            "dataproc_serverless": {
                                "location": "us-central1",
                                "resource_profile": {
                                    "inline": {
                                        "environment_config": {
                                            "executionConfig": {
                                                "serviceAccount": "service-account@example-project.iam.gserviceaccount.com",
                                                "networkUri": "projects/example-project/global/networks/default",
                                                "subnetworkUri": "projects/example-project/regions/us-central1/subnetworks/default"
                                            }
                                        },
                                        "runtime_config": {
                                            "version": "2.3",
                                            "properties": {
                                                "spark.app.name": "run-notebook-on-dataproc-serverless",
                                                "spark.executor.instances": "2",
                                                "spark.driver.cores": "4"
                                            }
                                        }
                                    }
                                },
                                "impersonation_chain": ["test-sa@example-project.iam.gserviceaccount.com"]
                            }
                        }
                    }
                }
            ]
        )

        pipeline = OrchestrationPipelineBuilder.build(pipeline_def)

        self.assertEqual(len(pipeline.actions), 2)
        action = pipeline.actions[1]
        self.assertTrue(action.HasField("notebook"))
        notebook_action = action.notebook
        self.assertEqual(notebook_action.name,
                         "run-notebook-on-dataproc-serverless")
        self.assertListEqual(list(notebook_action.depends_on),
                             ["run_dataproc_ephemeral_notebook"])
        self.assertEqual(notebook_action.execution_timeout, "45m")
        self.assertEqual(
            notebook_action.main_file_path,
            ("gs://example-bucket/notebookWithArchivesCheck.ipynb"),
        )
        self.assertEqual(notebook_action.staging_bucket, "example-bucket")
        self.assertListEqual(list(notebook_action.archive_uris),
                             ["gs://example-bucket/custom_venv.tar.gz"])
        self.assertTrue(notebook_action.engine.HasField("dataproc_serverless"))
        serverless = notebook_action.engine.dataproc_serverless
        self.assertEqual(serverless.location, "us-central1")
        self.assertTrue(serverless.resource_profile.HasField("inline"))
        inline_profile = serverless.resource_profile.inline
        environment_config = inline_profile.environment_config
        execution_config = environment_config["executionConfig"]
        self.assertEqual(
            execution_config["serviceAccount"],
            "service-account@example-project.iam.gserviceaccount.com",
        )
        self.assertEqual(execution_config["networkUri"],
                         "projects/example-project/global/networks/default")
        self.assertEqual(
            execution_config["subnetworkUri"],
            ("projects/example-project/regions/us-central1/subnetworks/default"),
        )
        runtime_config = inline_profile.runtime_config
        self.assertEqual(runtime_config["version"], "2.3")
        self.assertEqual(
            runtime_config["properties"]["spark.app.name"],
            ("run-notebook-on-dataproc-serverless"),
        )
        self.assertEqual(
            runtime_config["properties"]["spark.executor.instances"], "2")
        self.assertEqual(runtime_config["properties"]["spark.driver.cores"],
                         "4")
        self.assertListEqual(
            list(serverless.impersonation_chain),
            ["test-sa@example-project.iam.gserviceaccount.com"],
        )

    def test_build_action_dataproc_existing_cluster_notebook(self):
        """Tests Dataproc Existing Cluster Notebook action."""
        pipeline_def = self._get_valid_pipeline_def(
            actions=[
                {
                    "notebook": {
                        "name": "run-notebook-on-existing-cluster",
                        "main_file_path": "gs://example-bucket/data/trivialNotebook.ipynb",
                        "engine": {
                            "dataproc_on_gce": {
                                "existing_cluster": {
                                    "cluster_name": "test-cluster",
                                    "impersonation_chain": ["test-sa@example-project.iam.gserviceaccount.com"]
                                }
                            }
                        }
                    }
                }
            ]
        )

        pipeline = OrchestrationPipelineBuilder.build(pipeline_def)

        self.assertEqual(len(pipeline.actions), 1)
        action = pipeline.actions[0]
        self.assertTrue(action.HasField("notebook"))
        notebook_action = action.notebook
        self.assertEqual(notebook_action.name,
                         "run-notebook-on-existing-cluster")
        self.assertEqual(
            notebook_action.main_file_path,
            "gs://example-bucket/data/trivialNotebook.ipynb",
        )
        self.assertTrue(notebook_action.engine.HasField("dataproc_on_gce"))
        dataproc_on_gce = notebook_action.engine.dataproc_on_gce
        self.assertTrue(dataproc_on_gce.HasField("existing_cluster"))
        existing_cluster = dataproc_on_gce.existing_cluster
        self.assertEqual(existing_cluster.cluster_name, "test-cluster")
        self.assertListEqual(
            list(existing_cluster.impersonation_chain),
            ["test-sa@example-project.iam.gserviceaccount.com"],
        )

    def test_build_action_dataproc_serverless_pyspark_gcs(self):
        """Tests Dataproc Serverless PySpark (GCS) action."""
        pipeline_def = self._get_valid_pipeline_def(
            actions=[
                {
                    "pyspark": {
                        "name": "run-pyspark-on-dataproc-serverless-gcs",
                        "main_file_path": "gs://example-bucket/data/my_spark_job.py",
                        "engine": {
                            "dataproc_serverless": {
                                "location": "us-central1",
                                "resource_profile": {
                                    "path": "dataproc_config.yml"
                                },
                                "impersonation_chain": ["test-sa@example-project.iam.gserviceaccount.com"]
                            }
                        }
                    }
                }
            ]
        )

        pipeline = OrchestrationPipelineBuilder.build(pipeline_def)

        self.assertEqual(len(pipeline.actions), 1)
        action = pipeline.actions[0]
        self.assertTrue(action.HasField("pyspark"))
        pyspark_action = action.pyspark
        self.assertEqual(
            pyspark_action.name,
            "run-pyspark-on-dataproc-serverless-gcs",
        )
        self.assertEqual(pyspark_action.main_file_path,
                         "gs://example-bucket/data/my_spark_job.py")
        self.assertTrue(pyspark_action.engine.HasField("dataproc_serverless"))
        serverless = pyspark_action.engine.dataproc_serverless
        self.assertEqual(serverless.location, "us-central1")
        self.assertTrue(serverless.resource_profile.HasField("path"))
        self.assertEqual(serverless.resource_profile.path,
                         "dataproc_config.yml")
        self.assertListEqual(
            list(serverless.impersonation_chain),
            ["test-sa@example-project.iam.gserviceaccount.com"],
        )

    def test_build_action_dataproc_serverless_pyspark_inline(self):
        """Tests Dataproc Serverless PySpark (Inline) action."""
        pipeline_def = self._get_valid_pipeline_def(
            actions=[
                {
                    "pyspark": {
                        "name": "run-pyspark-on-dataproc-serverless-inline",
                        "main_file_path": "gs://example-bucket/data/my_spark_job.py",
                        "engine": {
                            "dataproc_serverless": {
                                "location": "us-central1",
                                "resource_profile": {
                                    "inline": {
                                        "environment_config": {
                                            "executionConfig": {
                                                "serviceAccount": "service-account@example-project.iam.gserviceaccount.com"
                                            }
                                        },
                                        "runtime_config": {
                                            "version": "2.3"
                                        }
                                    }
                                },
                                "impersonation_chain": ["test-sa@example-project.iam.gserviceaccount.com"]
                            }
                        }
                    }
                }
            ]
        )

        pipeline = OrchestrationPipelineBuilder.build(pipeline_def)

        self.assertEqual(len(pipeline.actions), 1)
        action = pipeline.actions[0]
        self.assertTrue(action.HasField("pyspark"))
        pyspark_action = action.pyspark
        self.assertEqual(
            pyspark_action.name,
            "run-pyspark-on-dataproc-serverless-inline",
        )
        self.assertEqual(pyspark_action.main_file_path,
                         "gs://example-bucket/data/my_spark_job.py")
        self.assertTrue(pyspark_action.engine.HasField("dataproc_serverless"))
        serverless = pyspark_action.engine.dataproc_serverless
        self.assertEqual(serverless.location, "us-central1")
        self.assertTrue(serverless.resource_profile.HasField("inline"))
        inline_profile = serverless.resource_profile.inline
        self.assertEqual(
            inline_profile.environment_config["executionConfig"]
            ["serviceAccount"],
            "service-account@example-project.iam.gserviceaccount.com",
        )
        self.assertEqual(inline_profile.runtime_config["version"], "2.3")
        self.assertListEqual(
            list(serverless.impersonation_chain),
            ["test-sa@example-project.iam.gserviceaccount.com"],
        )

    def test_build_action_dataproc_ephemeral_pyspark(self):
        """Tests Dataproc Ephemeral PySpark action."""
        pipeline_def = self._get_valid_pipeline_def(
            actions=[
                {
                    "pyspark": {
                        "name": "run-pyspark-on-dataproc-serverless-inline",
                        "main_file_path": "gs://example-bucket/data/my_spark_job.py",
                        "engine": {
                            "dataproc_serverless": {
                                "resource_profile": {
                                    "path": "dataproc_config.yml"
                                }
                            }
                        }
                    }
                },
                {
                    "pyspark": {
                        "name": "run_dataproc_ephemeral_pyspark",
                        "depends_on": ["run-pyspark-on-dataproc-serverless-inline"],
                        "main_file_path": "gs://example-bucket/data/my_spark_job.py",
                        "staging_bucket": "example-bucket",
                        "archive_uris": ["gs://example-bucket/lib.zip"],
                        "execution_timeout": "1h",
                        "engine": {
                            "dataproc_on_gce": {
                                "ephemeral_cluster": {
                                    "location": "us-central1",
                                    "project_id": "example-project",
                                    "cluster_name": "ephemeral-cluster-test",
                                    "resource_profile": {
                                        "inline": {
                                            "cluster_config": {
                                                "masterConfig": {
                                                    "numInstances": 1
                                                }
                                            }
                                        }
                                    },
                                    "impersonation_chain": ["test-sa@example-project.iam.gserviceaccount.com"],
                                    "properties": {
                                        "foo": "bar"
                                    }
                                }
                            }
                        }
                    }
                }
            ]
        )

        pipeline = OrchestrationPipelineBuilder.build(pipeline_def)

        self.assertEqual(len(pipeline.actions), 2)
        action = pipeline.actions[1]
        self.assertTrue(action.HasField("pyspark"))
        pyspark_action = action.pyspark
        self.assertEqual(pyspark_action.name, "run_dataproc_ephemeral_pyspark")
        self.assertListEqual(
            list(pyspark_action.depends_on),
            ["run-pyspark-on-dataproc-serverless-inline"],
        )
        self.assertEqual(pyspark_action.main_file_path,
                         "gs://example-bucket/data/my_spark_job.py")
        self.assertEqual(pyspark_action.staging_bucket, "example-bucket")
        self.assertListEqual(list(pyspark_action.archive_uris),
                             ["gs://example-bucket/lib.zip"])
        self.assertEqual(pyspark_action.execution_timeout, "1h")
        self.assertTrue(pyspark_action.engine.HasField("dataproc_on_gce"))
        dataproc_on_gce = pyspark_action.engine.dataproc_on_gce
        self.assertTrue(dataproc_on_gce.HasField("ephemeral_cluster"))
        ephemeral_cluster = dataproc_on_gce.ephemeral_cluster
        self.assertEqual(ephemeral_cluster.location, "us-central1")
        self.assertEqual(ephemeral_cluster.project_id, "example-project")
        self.assertEqual(ephemeral_cluster.cluster_name,
                         "ephemeral-cluster-test")
        self.assertTrue(ephemeral_cluster.resource_profile.HasField("inline"))
        self.assertEqual(
            ephemeral_cluster.resource_profile.inline.
            cluster_config["masterConfig"]["numInstances"], 1)
        self.assertListEqual(
            list(ephemeral_cluster.impersonation_chain),
            ["test-sa@example-project.iam.gserviceaccount.com"],
        )
        self.assertEqual(ephemeral_cluster.properties["foo"], "bar")

    def test_build_action_dataproc_existing_cluster_pyspark(self):
        """Tests Dataproc Existing Cluster PySpark action."""
        pipeline_def = self._get_valid_pipeline_def(
            actions=[
                {
                    "pyspark": {
                        "name": "run-pyspark-on-existing-cluster",
                        "main_file_path": "gs://example-bucket/data/my_spark_job.py",
                        "engine": {
                            "dataproc_on_gce": {
                                "existing_cluster": {
                                    "cluster_name": "test-cluster",
                                    "impersonation_chain": ["test-sa@example-project.iam.gserviceaccount.com"],
                                    "properties": {
                                        "propA": "valA"
                                    }
                                }
                            }
                        }
                    }
                }
            ]
        )

        pipeline = OrchestrationPipelineBuilder.build(pipeline_def)

        self.assertEqual(len(pipeline.actions), 1)
        action = pipeline.actions[0]
        self.assertTrue(action.HasField("pyspark"))
        pyspark_action = action.pyspark
        self.assertEqual(
            pyspark_action.name,
            "run-pyspark-on-existing-cluster",
        )
        self.assertEqual(pyspark_action.main_file_path,
                         "gs://example-bucket/data/my_spark_job.py")
        self.assertTrue(pyspark_action.engine.HasField("dataproc_on_gce"))
        dataproc_on_gce = pyspark_action.engine.dataproc_on_gce
        self.assertTrue(dataproc_on_gce.HasField("existing_cluster"))
        existing_cluster = dataproc_on_gce.existing_cluster
        self.assertEqual(existing_cluster.cluster_name, "test-cluster")
        self.assertListEqual(
            list(existing_cluster.impersonation_chain),
            ["test-sa@example-project.iam.gserviceaccount.com"],
        )
        self.assertEqual(existing_cluster.properties["propA"], "valA")

    def test_build_action_python_script(self):
        """Tests Python Script action."""
        pipeline_def = self._get_valid_pipeline_def(
            actions=[
                {
                    "python": {
                        "name": "python_first_script_run",
                        "execution_timeout": "10m",
                        "main_file_path": "scripts/test_script_1.py",
                        "python_callable": "main",
                        "op_kwargs": {
                            "apiEndpoint": "https://api.my-vendor.com/v1/status"
                        },
                        "engine": {
                            "local": {}
                        }
                    }
                }
            ]
        )

        pipeline = OrchestrationPipelineBuilder.build(pipeline_def)

        self.assertEqual(len(pipeline.actions), 1)
        action = pipeline.actions[0]
        self.assertTrue(action.HasField("python"))
        python_action = action.python
        self.assertEqual(python_action.name, "python_first_script_run")
        self.assertEqual(python_action.execution_timeout, "10m")
        self.assertEqual(python_action.main_file_path,
                         "scripts/test_script_1.py")
        self.assertEqual(python_action.python_callable, "main")
        self.assertEqual(
            python_action.op_kwargs["apiEndpoint"],
            "https://api.my-vendor.com/v1/status",
        )
        self.assertTrue(python_action.engine.HasField("local"))

    def test_build_action_venv_requirements_path(self):
        """Tests Venv with requirements_path action."""
        pipeline_def = self._get_valid_pipeline_def(
            actions=[
                {
                    "python": {
                        "name": "venv_first_script_run",
                        "main_file_path": "scripts/venv_test_script_1.py",
                        "python_callable": "main",
                        "environment": {
                            "requirements": {
                                "path": "scripts/requirements.txt"
                            },
                            "system_site_packages": True
                        },
                        "engine": {
                            "local": {}
                        }
                    }
                }
            ]
        )

        pipeline = OrchestrationPipelineBuilder.build(pipeline_def)

        self.assertEqual(len(pipeline.actions), 1)
        action = pipeline.actions[0]
        self.assertTrue(action.HasField("python"))
        python_action = action.python
        self.assertEqual(python_action.name, "venv_first_script_run")
        self.assertEqual(python_action.main_file_path,
                         "scripts/venv_test_script_1.py")
        self.assertEqual(python_action.python_callable, "main")
        self.assertTrue(python_action.HasField("environment"))
        self.assertEqual(python_action.environment.requirements.path,
                         "scripts/requirements.txt")
        self.assertTrue(python_action.environment.system_site_packages)
        self.assertTrue(python_action.engine.HasField("local"))

    def test_build_action_venv_requirements_inline(self):
        """Tests Venv with list of requirements action."""
        pipeline_def = self._get_valid_pipeline_def(
            actions=[
                {
                    "python": {
                        "name": "venv_first_script_run",
                        "main_file_path": "scripts/venv_test_script_1.py",
                        "python_callable": "main",
                        "engine": {"local": {}}
                    }
                },
                {
                    "python": {
                        "name": "venv_second_script_run",
                        "depends_on": ["venv_first_script_run"],
                        "main_file_path": "scripts/venv_test_script_1.py",
                        "python_callable": "main",
                        "environment": {
                            "requirements": {
                                "inline": {
                                    "list": ["pandas>=2.0.0"]
                                }
                            },
                            "system_site_packages": True
                        },
                        "engine": {
                            "local": {}
                        }
                    }
                }
            ]
        )

        pipeline = OrchestrationPipelineBuilder.build(pipeline_def)

        self.assertEqual(len(pipeline.actions), 2)
        action = pipeline.actions[1]
        self.assertTrue(action.HasField("python"))
        python_action = action.python
        self.assertEqual(python_action.name, "venv_second_script_run")
        self.assertListEqual(list(python_action.depends_on),
                             ["venv_first_script_run"])
        self.assertEqual(python_action.main_file_path,
                         "scripts/venv_test_script_1.py")
        self.assertEqual(python_action.python_callable, "main")
        self.assertTrue(python_action.HasField("environment"))
        self.assertListEqual(
            list(python_action.environment.requirements.inline.list),
            ["pandas>=2.0.0"])
        self.assertTrue(python_action.environment.system_site_packages)
        self.assertTrue(python_action.engine.HasField("local"))

    def test_build_action_sql_dataproc_serverless(self):
        """Tests SQL Dataproc Serverless action."""
        pipeline_def = self._get_valid_pipeline_def(
            actions=[
                {
                    "sql": {
                        "name": "run_sql_dataproc_serverless",
                        "query": {
                            "inline": "SELECT 1"
                        },
                        "engine": {
                            "dataproc_serverless": {
                                "resource_profile": {
                                    "inline": {
                                        "runtime_config": {
                                            "version": "2.3"
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            ]
        )

        pipeline = OrchestrationPipelineBuilder.build(pipeline_def)

        self.assertEqual(len(pipeline.actions), 1)
        action = pipeline.actions[0]
        self.assertTrue(action.HasField("sql"))
        sql_action = action.sql
        self.assertEqual(sql_action.name, "run_sql_dataproc_serverless")
        self.assertTrue(sql_action.query.HasField("inline"))
        self.assertEqual(sql_action.query.inline, "SELECT 1")
        self.assertTrue(sql_action.engine.HasField("dataproc_serverless"))
        serverless = sql_action.engine.dataproc_serverless
        self.assertTrue(serverless.resource_profile.HasField("inline"))
        self.assertEqual(
            serverless.resource_profile.inline.runtime_config["version"],
            "2.3")

    def test_build_action_dataform_airflow_worker(self):
        """Tests Dataform Airflow Worker action."""
        pipeline_def = self._get_valid_pipeline_def(
            actions=[
                {
                    "pipeline": {
                        "name": "run_dataform_airflow",
                        "framework": {
                            "dataform": {
                                "airflow_worker": {
                                    "project_directory_path": "/home/airflow/gcs/dags/dataform"
                                }
                            }
                        }
                    }
                }
            ]
        )

        pipeline = OrchestrationPipelineBuilder.build(pipeline_def)

        self.assertEqual(len(pipeline.actions), 1)
        action = pipeline.actions[0]
        self.assertTrue(action.HasField("pipeline"))
        pipeline_action = action.pipeline
        self.assertEqual(pipeline_action.name, "run_dataform_airflow")
        self.assertTrue(pipeline_action.framework.HasField("dataform"))
        dataform = pipeline_action.framework.dataform
        self.assertTrue(dataform.HasField("airflow_worker"))
        self.assertEqual(
            dataform.airflow_worker.project_directory_path,
            "/home/airflow/gcs/dags/dataform",
        )

    def test_build_action_ephemeral_cluster_gcs_config(self):
        """Tests Ephemeral Cluster with GCS Config action."""
        pipeline_def = self._get_valid_pipeline_def(
            actions=[
                {
                    "pyspark": {
                        "name": "run_ephemeral_with_gcs_config",
                        "main_file_path": "gs://example-bucket/job.py",
                        "engine": {
                            "dataproc_on_gce": {
                                "ephemeral_cluster": {
                                    "location": "us-central1",
                                    "project_id": "example-project",
                                    "cluster_name": "ephemeral-gcs-test",
                                    "resource_profile": {
                                        "path": "gs://example-bucket/cluster-config.yaml"
                                    }
                                }
                            }
                        }
                    }
                }
            ]
        )

        pipeline = OrchestrationPipelineBuilder.build(pipeline_def)

        self.assertEqual(len(pipeline.actions), 1)
        action = pipeline.actions[0]
        self.assertTrue(action.HasField("pyspark"))
        pyspark_action = action.pyspark
        self.assertEqual(pyspark_action.name, "run_ephemeral_with_gcs_config")
        self.assertEqual(pyspark_action.main_file_path,
                         "gs://example-bucket/job.py")
        self.assertTrue(pyspark_action.engine.HasField("dataproc_on_gce"))
        dataproc_on_gce = pyspark_action.engine.dataproc_on_gce
        self.assertTrue(dataproc_on_gce.HasField("ephemeral_cluster"))
        ephemeral_cluster = dataproc_on_gce.ephemeral_cluster
        self.assertEqual(ephemeral_cluster.location, "us-central1")
        self.assertEqual(ephemeral_cluster.project_id, "example-project")
        self.assertEqual(ephemeral_cluster.cluster_name, "ephemeral-gcs-test")
        self.assertTrue(ephemeral_cluster.resource_profile.HasField("path"))
        self.assertEqual(
            ephemeral_cluster.resource_profile.path,
            "gs://example-bucket/cluster-config.yaml",
        )

    def test_build_with_pyspark_environment_requirements(self):
        """Tests that PysparkAction with environment requirements is parsed correctly."""
        pipeline_def = self._get_valid_pipeline_def(
            actions=[
                {
                    "pyspark": {
                        "name": "run-pyspark-with-env",
                        "main_file_path": "gs://example-bucket/data/my_spark_job.py",
                        "engine": {
                            "dataproc_serverless": {
                                "location": "us-central1",
                                "resource_profile": {
                                    "path": "dataproc_config.yml"
                                }
                            }
                        },
                        "environment": {
                            "requirements": {
                                "path": "scripts/requirements.txt"
                            }
                        }
                    }
                }
            ]
        )

        pipeline = OrchestrationPipelineBuilder.build(pipeline_def)

        self.assertEqual(len(pipeline.actions), 1)
        action = pipeline.actions[0]
        self.assertTrue(action.HasField("pyspark"))
        pyspark_action = action.pyspark

        self.assertTrue(pyspark_action.HasField("environment"))
        self.assertTrue(pyspark_action.environment.HasField("requirements"))
        self.assertEqual(pyspark_action.environment.requirements.path,
                         "scripts/requirements.txt")

    def test_build_with_invalid_data_fails(self):
        """Tests that building with invalid data raises a ValueError."""
        invalid_pipeline_yaml_content = """
model_version: 1.0"
pipeline_id: "" # Invalid: empty required field
runner: "airflow"
owner: "valid-owner"
defaults:
  project_id: "example-project"
  location: "us-central1"
actions:
  - python:
      name: "valid-action-name"
      main_file_path: "/path/to/script.py"
      python_callable: "main"
      engine:
        local: {}
"""
        pipeline_def = yaml.safe_load(invalid_pipeline_yaml_content)

        with self.assertRaisesRegex(
                ValueError,
            ("Error for field 'pipeline_id': field is required and cannot be "
             "an empty string."),
        ):
            OrchestrationPipelineBuilder.build(pipeline_def)


if __name__ == "__main__":
    unittest.main()
