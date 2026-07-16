# Orchestration Pipelines (Declarative Pipelines) - Codebase Documentation

This document provides a clean and extensive documentation of the current status of the `orchestration-pipelines` package, focusing on the data models defined in the protocol buffers.

Reference Codebase Path: `../orchestration_pipelines_models/pipeline_v1_model/protos/`

**Links:** [Home (README)](../README.md) | [Changelog](../CHANGELOG.md)

---

## 1. Overview

Orchestration Pipelines (formerly known as Declarative Pipelines) provide a **YAML-based DSL** (Domain Specific Language) for defining orchestration workflows on Google Cloud. It is designed to:
*   **Abstract Underlying Infrastructure**: Users don't need to manage Dataproc clusters, or Dataform service details directly.
*   **No-Code/Low-Code**: Define complex pipelines without writing Python/Java orchestration code (like Apache Airflow DAGs).
*   **CI/CD Friendly**: YAML files can be easily versioned, tracked, and deployed via Git-based workflows.
*   **Agent-Friendly**: Easy for LLMs/AI Agents to generate and modify.

---

## 2. Core Concepts

*   **Pipeline (`OrchestrationPipeline`)**: The top-level definition containing metadata, defaults, triggers, and a list of actions.
*   **Runner (`PipelineRunner`)**: The execution engine. Currently, Apache Airflow (`airflow`) is the only supported runner.
*   **Defaults**: Common configurations shared across actions (e.g., GCP Project, Region).
*   **Triggers**: Define how the pipeline is started (e.g., on a schedule).
*   **Actions**: The individual tasks in the pipeline. Actions can depend on other actions, forming a Directed Acyclic Graph (DAG).
*   **Engines**: The compute resources where actions are executed (e.g., BigQuery, Dataproc, Dataproc Serverless, Local Airflow Worker).

---

## 3. Schema Reference (`orchestration_pipeline.proto`)

### 3.1. OrchestrationPipeline (Root Message)

This is the entry point for a pipeline definition.

| Field Name | Type | Required | Validation Rules | Description |
| :--- | :--- | :--- | :--- | :--- |
| `model_version` | `string` | **Yes** | | Version of the declarative model (e.g., `v1`). |
| `pipeline_id` | `string` | **Yes** | `regex: ^[a-zA-Z0-9_-]+$`<br>`min_len: 1`, `max_len: 64` | Unique identifier for the pipeline. |
| `description` | `string` | No | | Human-readable description. |
| `runner` | `PipelineRunner` | **Yes** | `disallow_zero_enum: true` | The runner engine (must be `airflow`). |
| `owner` | `string` | **Yes** | `regex: ^[a-zA-Z0-9_#-]+$`<br>`min_len: 1`, `max_len: 32` | Owner identifier (e.g. team name). |
| `defaults` | `Defaults` | **Yes** | | Default GCP settings for the pipeline. |
| `triggers` | `repeated Trigger` | No | | List of triggers (runs manually if empty). |
| `actions` | `repeated Action` | **Yes** | `min_items: 1` | The tasks that make up the pipeline. |
| `tags` | `repeated string` | No | `regex: ^[a-zA-Z0-9_-]{1,32}$` (implicit) | Tags for metadata and filtering. |
| `notifications`| `Notification` | No | | Notification settings for failures. |

### 3.2. Defaults & Execution Config

*   **`Defaults`**:
    *   `project_id` (string, **Required**): Default GCP Project ID.
    *   `location` (string, **Required**): Default GCP Region (e.g., `us-central1`).
    *   `execution_config` (`ExecutionConfig`, **Required**): Execution-related defaults.
*   **`ExecutionConfig`**:
    *   `retries` (int32): Number of retries for actions (must be >= 0, default is 0).

### 3.3. Triggers

A pipeline can be triggered by external events or schedules.

*   **`Trigger`**: A wrapper message using `oneof` to support different trigger types. Currently supports `ScheduleTrigger`.
*   **`ScheduleTrigger`**:
    *   `interval` (string, **Required**): Cron expression defining the schedule.
    *   `start_time` (string, **Required**): ISO 8601 timestamp when the schedule starts.
    *   `end_time` (string): ISO 8601 timestamp when the schedule ends.
    *   `catchup` (bool): If true, backfills missed runs. Default is `false`.
    *   `timezone` (string): IANA Timezone (e.g., `America/New_York`). Defaults to `UTC`.

### 3.4. Notifications

Configuration for pipeline notifications on failure.

*   **`Notification`**:
    *   `on_pipeline_failure` (`OnPipelineFailure`): Settings for failure notifications.
*   **`OnPipelineFailure`**:
    *   `email` (`repeated string`): List of email addresses to notify.
*   **Examples**:
    *   [pipeline-email-notification.yml](../examples/pipeline-email-notification.yml)

---

## 4. Actions Reference

Actions are the building blocks of the pipeline. Every action has a `name` (unique within the pipeline), optional `depends_on` list (for ordering), and an `execution_timeout` (ISO 8601 duration).

All actions support a `trigger_rule` (enum `TriggerRule`, defaults to `all_success`):
*   `all_success`: All direct parent tasks must have succeeded.
*   `all_failed`: All direct parent tasks must be in a failed state.
*   `all_done`: All direct parent tasks are done (regardless of success/failure).
*   `one_failed`: At least one parent task has failed.
*   `one_success`: At least one parent task has succeeded.
*   `always`: The task runs regardless of parent states.

### 4.1. PythonAction

Runs a Python function on the local Airflow worker.

*   **Engine**: `local` (`LocalEngine`) - runs in the Airflow worker environment.
*   **Key Fields**:
    *   `main_file_path` (string, **Required**): Path to the Python file.
    *   `python_callable` (string, **Required**): Name of the function to execute.
    *   `op_kwargs` (`google.protobuf.Struct`): Keyword arguments to pass to the function.
    *   `environment` (`PythonEnvironment`): Virtualenv requirements.
        *   Can be `inline` (list of pip packages) or `path` to a `requirements.txt`.
        *   `system_site_packages` (bool): Allow access to system packages. Default is `false`.
*   **Examples**:
    *   [pipeline-python-script.yml](../examples/pipeline-python-script.yml)
    *   [pipeline-python-script-with-pypi-dependencies.yml](../examples/pipeline-python-script-with-pypi-dependencies.yml)

### 4.2. PysparkAction

Runs a PySpark script on Dataproc.

*   **Engine**: `PysparkEngine` (**Required**, `oneof`):
    *   `dataproc_on_gce`: Existing or ephemeral Dataproc GCE Cluster.
    *   `dataproc_serverless`: Dataproc Serverless Batch.
*   **Key Fields**:
    *   `main_file_path` (string, **Required**): Path to the main `.py` script.
    *   `archive_uris` (`repeated string`): Tarballs/zips to unpack in the executor.
    *   `staging_bucket` (string): GCS bucket for staging files.
    *   `py_files` (`repeated string`): Additional Python files to put on PYTHONPATH.
    *   `environment` (`PysparkEnvironment`): Environment requirements.
        *   Can be `inline` (list of pip packages) or `path` to a `requirements.txt`.
    *   `params` (`map<string, string>`): Parameters passed to the PySpark job.
    *   `labels` (`map<string, string>`): Labels to apply to the PySpark job.
*   **Examples**:
    *   [pipeline-dataproc-create-batch-pyspark.yml (Serverless)](../examples/pipeline-dataproc-create-batch-pyspark.yml)
    *   [pipeline-dataproc-existing-cluster-script.yml (GCE Existing Basic)](../examples/pipeline-dataproc-existing-cluster-script.yml)
    *   [pipeline-dataproc-existing-cluster-script-pyfiles.yml (GCE Existing with pyFiles)](../examples/pipeline-dataproc-existing-cluster-script-pyfiles.yml)

### 4.3. NotebookAction

Executes a Jupyter Notebook (`.ipynb`) on Dataproc.

*   **Engine**: `NotebookEngine` (**Required**, `oneof`):
    *   `dataproc_on_gce`: Existing or ephemeral Dataproc GCE Cluster.
    *   `dataproc_serverless`: Dataproc Serverless Batch.
*   **Key Fields**:
    *   `main_file_path` (string, **Required**): Path to the `.ipynb` file.
    *   `archive_uris` (`repeated string`): Resources to unpack.
    *   `staging_bucket` (string): GCS bucket for staging.
    *   `environment` (`NotebookEnvironment`): Environment requirements.
        *   Can be `inline` (list of pip packages) or `path` to a `requirements.txt`.
    *   `params` (`map<string, string>`): Parameters passed to the Notebook.
    *   `labels` (`map<string, string>`): Labels to apply to the Notebook.
*   **Examples**:
    *   [pipeline-dataproc-create-batch.yml (Serverless)](../examples/pipeline-dataproc-create-batch.yml)

### 4.4. SqlAction

Executes SQL queries on BigQuery or Spark.

*   **Engine**: `SqlEngine` (**Required**, `oneof`):
    *   `bigquery`: Runs on BigQuery.
    *   `dataproc_serverless`: Runs on Spark SQL (Serverless).
    *   `dataproc_on_gce`: Runs on Spark SQL (GCE Cluster).
*   **Key Fields**:
    *   `query` (`Query`, **Required**, `oneof`):
        *   `inline`: The SQL query string directly in YAML.
        *   `path`: Path to a `.sql` file.
    *   `params` (`map<string, string>`): Parameters passed to the SQL job.
    *   `labels` (`map<string, string>`): Labels to apply to the SQL job.
*   **Examples**:
    *   [pipeline-sql-on-bigquery.yml (BigQuery)](../examples/pipeline-sql-on-bigquery.yml)
    *   [pipeline-sql-on-dataproc-serverless.yml (Spark SQL Serverless)](../examples/pipeline-sql-on-dataproc-serverless.yml)
    *   [pipeline-sql-on-dataproc-existing-cluster.yml (Spark SQL GCE Existing)](../examples/pipeline-sql-on-dataproc-existing-cluster.yml)
    *   [pipeline-sql-on-dataproc-ephemeral-cluster.yml (Spark SQL GCE Ephemeral)](../examples/pipeline-sql-on-dataproc-ephemeral-cluster.yml)

### 4.5. PipelineAction (dbt / Dataform)

Orchestrates third-party transformation frameworks.

*   **Framework**: `PipelineFramework` (**Required**, `oneof`):
    *   `dbt`: Runs dbt Core.
        *   `airflow_worker` (`DbtAirflowExecution`): Executes locally on the Airflow worker.
            *   `project_directory_path` (string, **Required**): Path to dbt project.
            *   `select_models` (`repeated string`): Models to run (equivalent to `--select`).
            *   `tags` (`repeated string`): dbt tags to filter.
    *   `dataform`: Runs Dataform.
        *   `dataform_service` (`DataformServiceExecution`): Executes via GCP Dataform API.
            *   `repository_id` (string, **Required**): Dataform repository resource name.
            *   `project_id` / `location` (string): Optional overrides.
            *   `workflow_invocation` (`google.protobuf.Struct`): Additional invocation configs.
        *   `airflow_worker` (`DataformAirflowExecution`): Runs Dataform CLI on Airflow worker.
            *   `project_directory_path` (string, **Required**): Path to Dataform project.
*   **Key Fields**:
    *   `params` (`map<string, string>`): Parameters passed to the pipeline execution. Note: currently not supported when executing Dataform using Dataform Service.
    *   `labels` (`map<string, string>`): Labels to apply. Note: currently not supported when executing DBT or Dataform using Dataform Service.
*   **Examples**:
    *   [pipeline-dbt.yml (dbt)](../examples/pipeline-dbt.yml)
    *   [pipeline-dataform-service.yml (Dataform Service)](../examples/pipeline-dataform-service.yml)
    *   [pipeline-dataform-local.yml (Dataform Local)](../examples/pipeline-dataform-local.yml)

### 4.6. DataIngestionAction

Handles data ingestion tasks. Currently supports BigQuery Data Transfer Service (DTS).

*   **Config**: `BigQueryDtsSpec` (`oneof`):
    *   `transfer_config_id` (string, **Required**): Resource name of the DTS transfer config.
    *   `requested_time_range` (`TimeRange`): Time range (`start_time` and `end_time` ISO 8601 timestamps) for the transfer run.
    *   `requested_run_time` (string, ISO 8601 timestamp): Requested run time.
    *   `runtime_params` (`google.protobuf.Struct`): **[DEPRECATED]** Parameters for the transfer run.
    *   `impersonation_chain` (`repeated string`): Service accounts to impersonate.
    *   `project_id` / `location` (string): Target project/location.
*   **Examples**:
    *   [pipeline-bigquery-dts.yml (BQ DTS)](../examples/pipeline-bigquery-dts.yml)

### 4.7. OrchestrationPipelineAction

Triggers another Orchestration Pipeline.

*   **Key Fields**:
    *   `pipeline_id` (string, **Required**): ID of the target pipeline to trigger.
    *   `bundle_id` (string): Version/Bundle ID of the target pipeline. Defaults to current bundle.
    *   `wait_for_completion` (bool): If true, this action waits until the triggered pipeline finishes. Default is `false`.
*   **Examples**:
    *   [pipeline-trigger-another-orchestration-pipeline.yml](../examples/pipeline-trigger-another-orchestration-pipeline.yml)

---

## 5. Engines Configuration Detail

### 5.1. BigQueryEngine
Used for `SqlAction` running on BigQuery.
*   `location` (string): BigQuery dataset location override.
*   `destination_table` (string): Table to write results to (if applicable).
*   `impersonation_chain` (`repeated string`): Service accounts to impersonate.

### 5.2. Dataproc Serverless Batch Engine
Used for PySpark, Notebook, and Spark SQL actions.
*   `location` (string): Region override.
*   `resource_profile` (`DataprocBatchResourceProfile`, **Required**): Defines compute resources.
    *   Supports `inline` config (using GCP `RuntimeConfig` and `EnvironmentConfig` structures), `path` to config file, or `external_config_path`.
    *   Supports `overrides` (deep merged onto the config).
*   `impersonation_chain` (`repeated string`): Service accounts to impersonate.
*   **Examples**:
    *   [pipeline-dataproc-create-batch-resource-profile-gcs.yml (GCS Config Path)](../examples/pipeline-dataproc-create-batch-resource-profile-gcs.yml)
    *   [pipeline-dataproc-create-batch-resource-profile-gcs-overrides.yml (GCS Config with Overrides)](../examples/pipeline-dataproc-create-batch-resource-profile-gcs-overrides.yml)
    *   [pipeline-dataproc-create-batch-resource-profile-relative.yml (Relative Config Path)](../examples/pipeline-dataproc-create-batch-resource-profile-relative.yml)

### 5.3. Dataproc on GCE Engine
Used for existing or ephemeral Dataproc GCE clusters.
*   `existing_cluster` (`DataprocExistingClusterConfiguration`):
    *   `cluster_name` (string, **Required**).
    *   `project_id` / `location` (string): Optional overrides.
    *   `properties` (map<string, string>): Dataproc properties.
    *   `impersonation_chain` (`repeated string`): Service accounts to impersonate.
*   `ephemeral_cluster` (`DataprocEphemeralConfiguration`):
    *   `cluster_name` (string, **Required**).
    *   `project_id` / `location` (string): Optional overrides.
    *   `resource_profile` (`DataprocClusterResourceProfile`, **Required**): Ephemeral cluster configuration.
    *   `properties` (map<string, string>): Dataproc properties.
    *   `impersonation_chain` (`repeated string`): Service accounts to impersonate.
*   **Examples**:
    *   [pipeline-dataproc-ephemeral-inline.yml (Ephemeral Inline Config)](../examples/pipeline-dataproc-ephemeral-inline.yml)
    *   [pipeline-dataproc-ephemeral-gcs-resource-profile.yml (Ephemeral GCS Config Path)](../examples/pipeline-dataproc-ephemeral-gcs-resource-profile.yml)
    *   [pipeline-dataproc-ephemeral-gcs-resource-profile-overrides.yml (Ephemeral GCS Config with Overrides)](../examples/pipeline-dataproc-ephemeral-gcs-resource-profile-overrides.yml)
    *   [pipeline-dataproc-ephemeral-relative-resource-profile.yml (Ephemeral Relative Config Path)](../examples/pipeline-dataproc-ephemeral-relative-resource-profile.yml)

---

## 6. Validation Rules (`validation.proto`)

The framework enforces strict validation using custom Proto annotations.

*   `is_required` (bool): Field must not be empty (strings/repeated) or must be set (messages).
*   `regex` (string): String must match the regex pattern.
*   `min_value` (double): Numeric value must be >= specified value.
*   `min_items` (uint32): Repeated field must have at least N items.
*   `disallow_zero_enum` (bool): Enum field cannot use the default `0` (undefined) value.
*   `min_len` / `max_len` (uint32): Min/Max character length for strings.
*   `is_cron_expression` (bool): String must be a valid cron schedule.
*   `is_iso8601_timestamp` (bool): String must be an ISO 8601 timestamp.
*   `is_iso8601_duration` (bool): String must be an ISO 8601 duration (e.g., `PT1H`).
*   `is_iana_timezone` (bool): String must be a valid IANA timezone (e.g., `UTC`).
*   `map_key_regex` (string): Map key must match the regex pattern.
*   `map_value_regex` (string): Map value must match the regex pattern.
