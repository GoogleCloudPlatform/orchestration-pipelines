# Orchestration Pipelines (Declarative Pipelines) - Codebase Documentation

This document provides a clean and extensive documentation of the current status
of the `orchestration-pipelines` package, focusing on the data models defined in
the protocol buffers.

Reference Codebase Path:
`../orchestration_pipelines_models/pipeline_v1_model/protos/`

**Links:** [Home (README)](../README.md) | [Changelog](../CHANGELOG.md) |
[Limitations](./limitations.md)

---

## 1. Overview

Orchestration Pipelines (formerly known as Declarative Pipelines) provide a
**YAML-based DSL** (Domain Specific Language) for defining orchestration
workflows on Google Cloud. It is designed to:

- **Abstract Underlying Infrastructure**: Users don't need to manage Dataproc
  clusters, or Dataform service details directly.
- **No-Code/Low-Code**: Define complex pipelines without writing Python/Java
  orchestration code (like Apache Airflow DAGs).
- **CI/CD Friendly**: YAML files can be easily versioned, tracked, and deployed
  via Git-based workflows.
- **Agent-Friendly**: Easy for LLMs/AI Agents to generate and modify.

---

## 2. Core Concepts

- **Pipeline (`OrchestrationPipeline`)**: The top-level definition containing
  metadata, defaults, triggers, and a list of actions.
- **Runner (`PipelineRunner`)**: The execution engine. Currently, Apache Airflow
  (`airflow`) is the only supported runner.
- **Defaults**: Common configurations shared across actions (e.g., GCP Project,
  Region).
- **Triggers**: Define how the pipeline is started (e.g., on a schedule).
- **Actions**: The individual tasks in the pipeline. Actions can depend on other
  actions, forming a Directed Acyclic Graph (DAG).
- **Engines**: The compute resources where actions are executed (e.g., BigQuery,
  Dataproc, Dataproc Serverless, Local Airflow Worker).

---

## 3. Schema Reference (`orchestration_pipeline.proto`)

### 3.1. OrchestrationPipeline (Root Message)

This is the entry point for a pipeline definition.

| Field Name | Type | Required | Validation Rules | Description |
| :--- | :--- | :--- | :--- | :--- |
| `model_version` | `string` | **Yes** | | Version of the declarative model (e.g., `v1`). |
| `pipeline_id` | `string` | **Yes** | `regex: ^[a-zA-Z0-9_-]+$`, `min_len: 1`, `max_len: 64` | Unique identifier for the pipeline. |
| `description` | `string` | No | | Human-readable description. |
| `runner` | `PipelineRunner` | **Yes** | `disallow_zero_enum: true` | The runner engine (must be `airflow`). |
| `owner` | `string` | **Yes** | `regex: ^[a-zA-Z0-9_#-]+$`, `min_len: 1`, `max_len: 32` | Owner identifier (e.g. team name). |
| `defaults` | `Defaults` | **Yes** | | Default GCP settings for the pipeline. |
| `triggers` | `repeated Trigger` | No | | List of triggers (runs manually if empty). |
| `actions` | `repeated Action` | **Yes** | `min_items: 1` | The tasks that make up the pipeline. |
| `tags` | `repeated string` | No | `regex: ^[a-zA-Z0-9_-]{1,32}$` (implicit) | Tags for metadata and filtering. |
| `notifications`| `Notification` | No | | Notification settings for success or failures. |
| `parameters` | `map<string, Parameter>` | No | `map_key_regex: ^[a-zA-Z_][a-zA-Z0-9_]*$` | Runtime parameters (`string`, `number`, `boolean`, `array`) with `default`. |

### 3.2. Defaults & Execution Config

- **`Defaults`**:
  - `project_id` (string, **Required**): Default GCP Project ID.
  - `location` (string, **Required**): Default GCP Region (e.g., `us-central1`).
  - `execution_config` (`ExecutionConfig`, **Required**): Execution-related
    defaults.
- **`ExecutionConfig`**:
  - `retries` (int32): Number of retries for actions (must be >= 0, default is
    0).

### 3.3. Triggers

A pipeline can be triggered by external events or schedules.

- **`Trigger`**: A wrapper message using `oneof` to support different trigger
  types. Currently supports `ScheduleTrigger`.
- **`ScheduleTrigger`**:
  - `interval` (string, **Required**): Cron expression defining the schedule.
  - `start_time` (string, **Required**): Naive ISO 8601 timestamp without a
    timezone suffix or offset (e.g., `2026-01-01T00:00:00`); use `timezone` to
    specify the timezone.
  - `end_time` (string): Naive ISO 8601 timestamp without a timezone suffix or
    offset (e.g., `2026-12-31T23:59:59`); use `timezone` to specify the
    timezone.
  - `catchup` (bool): If true, backfills missed runs. Default is `false`.
  - `timezone` (string): IANA Timezone (e.g., `America/New_York`). Defaults to
    `UTC`.

### 3.4. Notifications

Configuration for pipeline notifications on failure.

- **`Notification`**:
  - `on_pipeline_failure` (`OnPipelineFailure`): Settings for failure
    notifications.
  - `on_pipeline_success` (`OnPipelineSuccess`): Settings for success
    notifications.
- **`OnPipelineFailure`**:
  - `email` (`repeated string`): List of email addresses to notify.
- **`OnPipelineSuccess`**:
  - `email` (`repeated string`): List of email addresses to notify.
- **Examples**:
  - [pipeline-email-notification.yml](../examples/pipeline-email-notification.yml)

### 3.5. Retry Policy

Pipelines and actions support automated retries for transient failures. A retry
policy can be defined globally in `defaults.retryPolicy` or per action in
`action.retryPolicy`. An action-level policy overrides the global default.

- **`RetryPolicy`**:
  - `max_retries` (int32, **Required**): Maximum number of retry attempts
    (must be >= 0).
  - Strategy (`oneof`):
    - `fixed_delay` (`FixedDelayStrategy`): Retries with a constant delay.
    - `exponential_backoff` (`ExponentialBackoffStrategy`): Retries with an
      increasing exponential delay.
- **`FixedDelayStrategy`**:
  - `retry_delay` (string, **Required**): Delay between retries (e.g., `"30s"`,
    `"2m"`).
- **`ExponentialBackoffStrategy`**:
  - `initial_delay` (string, **Required**): Initial delay for the first retry
    (e.g., `"10s"`).
  - `max_delay` (string): Maximum upper bound for the delay (e.g., `"10m"`).
  - `multiplier` (double): Exponential scaling factor (Default: `2.0`).
    *Note: Custom multipliers are supported on Airflow 3.3+. Airflow 2 defaults
    to 2.0.*
  - `randomize_jitter` (bool): Whether to introduce random jitter (Default:
    `false`).
    *Note: Custom jitter configuration is supported on Airflow 3.3+. Airflow 2
    uses built-in hash-based jitter.*

---

## 4. Actions Reference

Actions are the building blocks of the pipeline. Every action has a `name`
(unique within the pipeline), optional `depends_on` list (for ordering),
optional `run_if` CEL boolean condition (skips the task when `false`), and an
`execution_timeout` (ISO 8601 duration).

All actions support a `trigger_rule` (enum `TriggerRule`, defaults to
`all_success`):

- `all_success`: All direct parent tasks must have succeeded.
- `all_failed`: All direct parent tasks must be in a failed state.
- `all_done`: All direct parent tasks are done (regardless of success/failure).
- `one_failed`: At least one parent task has failed.
- `one_success`: At least one parent task has succeeded.
- `always`: The task runs regardless of parent states.

### 4.1. PythonAction

Runs a Python function on the local Airflow worker.

- **Engine**: `local` (`LocalEngine`) - runs in the Airflow worker environment.
- **Key Fields**:
  - `main_file_path` (string, **Required**): Path to the Python file.
  - `python_callable` (string, **Required**): Name of the function to execute.
  - `op_kwargs` (`google.protobuf.Struct`): Keyword arguments to pass to the
    function.
  - `environment` (`PythonEnvironment`): Virtualenv requirements.
    - Can be `inline` (list of pip packages) or `path` to a `requirements.txt`.
    - `system_site_packages` (bool): Allow access to system packages. Default is
      `false`.
- **Examples**:
  - [pipeline-python-script.yml](../examples/pipeline-python-script.yml)
  - [pipeline-python-script-with-pypi-dependencies.yml](../examples/pipeline-python-script-with-pypi-dependencies.yml)

### 4.2. PysparkAction

Runs a PySpark script on Dataproc.

- **Engine**: `PysparkEngine` (**Required**, `oneof`):
  - `dataproc_on_gce`: Existing or ephemeral Dataproc GCE Cluster.
  - `dataproc_serverless`: Dataproc Serverless Batch.
- **Key Fields**:
  - `main_file_path` (string, **Required**): Path to the main `.py` script.
  - `archive_uris` (`repeated string`): Tarballs/zips to unpack in the executor.
  - `staging_bucket` (string): GCS bucket for staging files.
  - `py_files` (`repeated string`): Additional Python files to put on
    PYTHONPATH.
  - `environment` (`PysparkEnvironment`): Environment requirements.
    - Can be `inline` (list of pip packages) or `path` to a `requirements.txt`.
  - `params` (`map<string, string>`): Parameters passed to the PySpark job.
  - `labels` (`map<string, string>`): Labels to apply to the PySpark job.
- **Examples**:
  - [pipeline-dataproc-create-batch-pyspark.yml (Serverless)](../examples/pipeline-dataproc-create-batch-pyspark.yml)
  - [pipeline-dataproc-existing-cluster-script.yml (GCE Existing Basic)](../examples/pipeline-dataproc-existing-cluster-script.yml)
  - [pipeline-dataproc-existing-cluster-script-pyfiles.yml (GCE Existing with pyFiles)](../examples/pipeline-dataproc-existing-cluster-script-pyfiles.yml)

### 4.3. NotebookAction

Executes a Jupyter Notebook (`.ipynb`) on Dataproc.

- **Engine**: `NotebookEngine` (**Required**, `oneof`):
  - `dataproc_on_gce`: Existing or ephemeral Dataproc GCE Cluster.
  - `dataproc_serverless`: Dataproc Serverless Batch.
- **Key Fields**:
  - `main_file_path` (string, **Required**): Path to the `.ipynb` file.
  - `archive_uris` (`repeated string`): Resources to unpack.
  - `staging_bucket` (string): GCS bucket for staging.
  - `environment` (`NotebookEnvironment`): Environment requirements.
    - Can be `inline` (list of pip packages) or `path` to a `requirements.txt`.
  - `params` (`map<string, string>`): Parameters passed to the Notebook.
  - `labels` (`map<string, string>`): Labels to apply to the Notebook.
  - `py_files` (`repeated string`): Additional Python files to put on
    PYTHONPATH.
  - `output_uri_prefix` (string, optional): GCS URI prefix (`gs://...`) for
    archiving executed `.ipynb` files (defaults to
    `gs://<GCS_BUCKET>/composer_orchestration_pipelines_resources/`).
- **Examples**:
  - [pipeline-dataproc-create-batch.yml (Serverless)](../examples/pipeline-dataproc-create-batch.yml)

### 4.4. SqlAction

Executes SQL queries on BigQuery or Spark.

- **Engine**: `SqlEngine` (**Required**, `oneof`):
  - `bigquery`: Runs on BigQuery.
  - `dataproc_serverless`: Runs on Spark SQL (Serverless).
  - `dataproc_on_gce`: Runs on Spark SQL (GCE Cluster).
- **Key Fields**:
  - `query` (`Query`, **Required**, `oneof`):
    - `inline`: The SQL query string directly in YAML.
    - `path`: Path to a `.sql` file.
  - `params` (`map<string, string>`): Parameters passed to the SQL job:
    - **BigQuery (`bigquery`)**: Interpolates `${param}` placeholders in SQL
      text (e.g., `FROM`/`JOIN` table or dataset identifiers; static keys also
      support legacy `{{ param }}` at parse time, while dynamic `${ ... }` keys
      interpolate `${param}` at runtime), and binds all entries as `@param`
      BigQuery `STRING` query parameters (use `CAST(@param AS ...)` for
      non-string comparisons).
    - **Dataproc (`dataproc_serverless` / `dataproc_on_gce`)**: Passed via
      `query_variables` / `script_variables` and substituted using `${param}`
      for both `WHERE` value filters and SQL identifiers (`@param` and
      `{{ param }}` do not apply).
  - `labels` (`map<string, string>`): Labels to apply to the SQL job.
- **Examples**:
  - [pipeline-sql-on-bigquery.yml (BigQuery)](../examples/pipeline-sql-on-bigquery.yml)
  - [pipeline-sql-on-dataproc-serverless.yml (Spark SQL Serverless)](../examples/pipeline-sql-on-dataproc-serverless.yml)
  - [pipeline-sql-on-dataproc-existing-cluster.yml (Spark SQL GCE Existing)](../examples/pipeline-sql-on-dataproc-existing-cluster.yml)
  - [pipeline-sql-on-dataproc-ephemeral-cluster.yml (Spark SQL GCE Ephemeral)](../examples/pipeline-sql-on-dataproc-ephemeral-cluster.yml)

### 4.5. PipelineAction (dbt / Dataform)

Orchestrates third-party transformation frameworks.

- **Framework**: `PipelineFramework` (**Required**, `oneof`):
  - `dbt`: Runs dbt Core.
    - `airflow_worker` (`DbtAirflowExecution`): Executes locally on the Airflow
      worker.
      - `project_directory_path` (string, **Required**): Path to dbt project.
      - `select_models` (`repeated string`): **[DEPRECATED]** Models to run
        (equivalent to `--select`). Use `invocations` instead.
      - `tags` (`repeated string`): **[DEPRECATED]** dbt tags to filter.
    - `output_gcs_uri` (string, optional): GCS URI (`gs://...`) where dbt
      `target/` and `logs/` artifacts are uploaded after execution. Defaults
      to the Composer environment's bucket under
      `orchestration_pipelines_outputs/` when `GCS_BUCKET` is set.
    - `invocations` (`repeated DbtInvocation`, optional): Ordered sequence of
      structured dbt commands executed sequentially. Defaults to a single `run`
      invocation if omitted.
      - `command` (string, **Required**): Base dbt command (`build`, `run`,
        `test`, `snapshot`, `freshness`, `source freshness`, `seed`,
        `compile`, `deps`, `run-operation`).
      - `select` (`repeated string`, optional): Node selection (`--select`).
      - `exclude` (`repeated string`, optional): Node exclusion (`--exclude`).
      - `vars` (`map<string, string>`, optional): Variables passed via `--vars`.
      - `full_refresh` (bool, optional): Pass `--full-refresh`.
      - `fail_fast` (bool, optional): Pass `--fail-fast`.
      - `threads` (int32, optional): Number of threads (`--threads`, >= 0).
      - `target` (string, optional): Target profile name (`--target`).
      - `extra_args` (`repeated string`, optional): Additional CLI flags.
      - `macro` (string, optional): Macro name to invoke when `command` is
        `run-operation` (**Required** for `run-operation`).
      - `args` (`google.protobuf.Struct`, optional): Keyword arguments passed
        via `--args` when `command` is `run-operation`.
    - `knowledge_catalog` (`DbtKnowledgeCatalogConfig`, optional): Ingests dbt
      artifacts from `target/` into Dataplex Knowledge Catalog via
      `gcloud dataplex dbt metadata-jobs create` (requires
      `gcloud-cli >= 587.0.0` in the Airflow worker environment; see
      [limitations.md](limitations.md)).
      - `entry_group` (string, **Required**): Existing Dataplex entry group
        short ID. Use a dedicated entry group per dbt project because full-sync
        imports (`aspects_only: false`) delete entries not present in the
        run's artifacts.
      - `location` (string, optional): Dataplex resource location (defaults to
        pipeline `defaults.location`).
      - `project_id` (string, optional): Dataplex project ID (defaults to
        pipeline `defaults.project_id`).
      - `storage_uri` (string, optional): GCS URI prefix (`gs://...`) for
        staging `dbt_metadata.jsonl` (defaults to
        `<output_gcs_uri>/dataplex_metadata/`). Ensure the Dataplex service
        agent (`gcp-sa-dataplex`) has read access to `storage_uri`—especially
        when relying on the default Composer `GCS_BUCKET` or targeting a
        different `project_id`—and the worker service account has write access
        plus `roles/dataplex.metadataJobOwner` along with entry-group /
        connector-type permissions (e.g., `roles/dataplex.entryGroupOwner` or
        `roles/dataplex.catalogAdmin`, or least-privilege
        `roles/dataplex.entryGroupImporter` and `roles/dataplex.entryOwner`),
        or `roles/dataplex.admin`.
      - `aspects_only` (bool, optional): Pass `--aspects-only` to update only
        observed metadata without creating or deleting entries.
      - `skip_bigquery_link` (bool, optional): Pass `--skip-bigquery-link` to
        skip physical BigQuery table reference links.
      - `include_entry_links` (bool, optional): Emit EntryLink records
        (defaults to `true`; set to `false` to pass
        `--no-include-entry-links`).
      - `async_mode` (bool, optional): Pass `--async` to return once Dataplex
        accepts the job (defaults to `true`; set to `false` to wait for
        completion).
  - `dataform`: Runs Dataform.
    - `dataform_service` (`DataformServiceExecution`): Executes via GCP Dataform
      API.
      - `repository_id` (string, **Required**): Dataform repository resource
        name.
      - `project_id` / `location` (string): Optional overrides.
      - `workflow_invocation` (`google.protobuf.Struct`): Additional invocation
        configs.
    - `airflow_worker` (`DataformAirflowExecution`): Runs Dataform CLI on
      Airflow worker.
      - `project_directory_path` (string, **Required**): Path to Dataform
        project.
- **Key Fields**:
  - `params` (`map<string, string>`): Parameters passed to the pipeline
    execution. Note: currently not supported when executing Dataform using
    Dataform Service.
  - `labels` (`map<string, string>`): Labels to apply. Note: currently not
    supported when executing DBT or Dataform using Dataform Service.
- **Examples**:
  - [pipeline-dbt.yml (dbt)](../examples/pipeline-dbt.yml)
  - [pipeline-dataform-service.yml (Dataform Service)](../examples/pipeline-dataform-service.yml)
  - [pipeline-dataform-local.yml (Dataform Local)](../examples/pipeline-dataform-local.yml)

### 4.6. DataIngestionAction

Handles data ingestion tasks. Currently supports BigQuery Data Transfer Service
(DTS).

- **Config**: `BigQueryDtsSpec` (`oneof`):
  - `transfer_config_id` (string, **Required**): Resource name of the DTS
    transfer config.
  - `requested_time_range` (`TimeRange`): Time range (`start_time` and
    `end_time` ISO 8601 timestamps) for the transfer run.
  - `requested_run_time` (string, ISO 8601 timestamp): Requested run time.
  - `runtime_params` (`google.protobuf.Struct`): **[DEPRECATED]** Parameters for
    the transfer run.
  - `impersonation_chain` (`repeated string`): Service accounts to impersonate.
  - `project_id` / `location` (string): Target project/location.
- **Examples**:
  - [pipeline-bigquery-dts.yml (BQ DTS)](../examples/pipeline-bigquery-dts.yml)

### 4.7. OrchestrationPipelineAction

Triggers another Orchestration Pipeline.

- **Key Fields**:
  - `pipeline_id` (string, **Required**): ID of the target pipeline to trigger.
  - `bundle_id` (string): Version/Bundle ID of the target pipeline. Defaults to
    current bundle.
  - `wait_for_completion` (bool): If true, this action waits until the triggered
    pipeline finishes. Default is `false`.
  - `params` (`google.protobuf.Struct`): Runtime parameters forwarded to the
    triggered pipeline as `dag_run.conf`.
- **Examples**:
  - [pipeline-trigger-another-orchestration-pipeline.yml](../examples/pipeline-trigger-another-orchestration-pipeline.yml)

### 4.8. AIAction

Handles machine learning and AI tasks. Supports model uploading and batch
inference on Vertex AI via `AgentPlatform`.

- `labels` (`map<string, string>`, optional): Custom labels for the AI action.
  For model upload, passed to the Airflow operator via the model definition; for
  batch inference, passed to the batch prediction job.
- **Provider**: `agent_platform` (`oneof provider`):
  - `project_id` / `location` (string, optional): GCP project ID and region
    override (defaults to pipeline defaults).
  - `model_upload` (`AgentPlatformModelUpload`, `oneof type`):
    - `model_name` (string, **Required**): Display name for the model in Vertex
      AI.
    - `model_artifact_uri` (string, **Required**): Cloud Storage URI where the
      model artifacts are stored (e.g. `gs://bucket/models/spark_rf_model`).
    - `serving_container_image_uri` (string, **Required**): Container image URI
      used for model serving/prediction.
    - `description` (string, optional): Description of the model.
  - `batch_inference` (`AgentPlatformBatchInference`, `oneof type`):
    - `job_display_name` (string, **Required**): Display name for the batch
      prediction job in Vertex AI.
    - `model_name` (string, **Required**): Fully-qualified Vertex AI model
      resource name or ID.
    - `instances_format` (string, optional): Format of the input instances (e.g.
      `bigquery`, `jsonl`, `csv`). Defaults to `jsonl`.
    - `predictions_format` (string, optional): Format of the prediction output
      (e.g. `bigquery`, `jsonl`, `csv`). Defaults to `jsonl`.
    - `bigquery_source` (string, optional): BigQuery table URI for input data
      (e.g. `bq://project.dataset.table`).
    - `gcs_source` (`repeated string`, optional): Cloud Storage URIs for input
      data.
    - `bigquery_destination_prefix` (string, optional): BigQuery dataset URI
      prefix for output (e.g. `bq://project.dataset`).
    - `gcs_destination_prefix` (string, optional): Cloud Storage URI prefix for
      output.
    - `impersonation_chain` (`repeated string`, optional): Service account
      impersonation chain for the batch prediction job.
  - `create_and_run_custom_job` (`AgentPlatformCreateAndRunCustomJob`,
    `oneof type`):
    *Note: This action is only supported on Airflow versions compatible with
    `apache-airflow-providers-google>=22.5.0`. Check details in
    [Limitations](./limitations.md).*
    - `custom_job` (`google.protobuf.Struct`): Agent Platform [`CustomJob`](https://cloud.google.com/python/docs/reference/aiplatform/latest/google.cloud.aiplatform_v1.types.CustomJob)
      configuration (`display_name`, `job_spec`, etc.).
    - `execution_timeout` (string, optional): ISO 8601 duration string for
      maximum execution time of the custom job task.
    - `impersonation_chain` (`repeated string`, optional): Service account
      impersonation chain for the custom job.
- **Examples**:
  - [pipeline-vertex-ai-upload-model.yml](../examples/pipeline-vertex-ai-upload-model.yml)
  - [pipeline-vertex-ai-batch-inference.yml](../examples/pipeline-vertex-ai-batch-inference.yml)
  - [pipeline-vertex-ai-custom-job.yml](../examples/pipeline-vertex-ai-custom-job.yml)

### 4.9. AirflowTaskAction

Instantiates and executes an arbitrary Apache Airflow operator within the
pipeline.

- **Key Fields**:
  - `operator_class` (string, **Required**): Fully qualified Python path to the
    Airflow operator class (e.g., `airflow.operators.bash.BashOperator`,
    `airflow.providers.http.operators.http.HttpOperator`).
  - `params` (mapping/dictionary): Operator-specific parameters passed directly
    to the operator's constructor as keyword arguments (`**kwargs`). Supports
    arbitrarily nested structures (primitives, lists, maps) to configure
    operators with complex requirements (e.g., BigQuery job configurations,
    HTTP headers/payloads).
- **Examples**:
  - [pipeline-airflow-task-action.yml](../examples/pipeline-airflow-task-action.yml)

Only operators that accept configuration purely via JSON/YAML-serializable data
structures are supported. Check details in [Limitations](./limitations.md).

---

## 5. Engines Configuration Detail

### 5.1. BigQueryEngine

Used for `SqlAction` running on BigQuery.

- `location` (string): BigQuery dataset location override.
- `destination_table` (string): Table to write results to (if applicable).
- `impersonation_chain` (`repeated string`): Service accounts to impersonate.

### 5.2. Dataproc Serverless Batch Engine

Used for PySpark, Notebook, and Spark SQL actions.

- `location` (string): Region override.
- `resource_profile` (`DataprocBatchResourceProfile`, **Required**): Defines
  compute resources.
  - Supports `inline` config (using GCP `RuntimeConfig` and `EnvironmentConfig`
    structures), `path` to config file, or `external_config_path`.
  - Supports `overrides` (deep merged onto the config).
- `impersonation_chain` (`repeated string`): Service accounts to impersonate.
- **Examples**:
  - [pipeline-dataproc-create-batch-resource-profile-gcs.yml (GCS Config Path)](../examples/pipeline-dataproc-create-batch-resource-profile-gcs.yml)
  - [pipeline-dataproc-create-batch-resource-profile-gcs-overrides.yml (GCS Config with Overrides)](../examples/pipeline-dataproc-create-batch-resource-profile-gcs-overrides.yml)
  - [pipeline-dataproc-create-batch-resource-profile-relative.yml (Relative Config Path)](../examples/pipeline-dataproc-create-batch-resource-profile-relative.yml)

### 5.3. Dataproc on GCE Engine

Used for existing or ephemeral Dataproc GCE clusters.

- `existing_cluster` (`DataprocExistingClusterConfiguration`):
  - `cluster_name` (string, **Required**).
  - `project_id` / `location` (string): Optional overrides.
  - `properties` (map\<string, string>): Dataproc properties.
  - `impersonation_chain` (`repeated string`): Service accounts to impersonate.
- `ephemeral_cluster` (`DataprocEphemeralConfiguration`):
  - `cluster_name` (string, **Required**).
  - `project_id` / `location` (string): Optional overrides.
  - `resource_profile` (`DataprocClusterResourceProfile`, **Required**):
    Ephemeral cluster configuration.
  - `properties` (map\<string, string>): Dataproc properties.
  - `impersonation_chain` (`repeated string`): Service accounts to impersonate.
- **Examples**:
  - [pipeline-dataproc-ephemeral-inline.yml (Ephemeral Inline Config)](../examples/pipeline-dataproc-ephemeral-inline.yml)
  - [pipeline-dataproc-ephemeral-gcs-resource-profile.yml (Ephemeral GCS Config Path)](../examples/pipeline-dataproc-ephemeral-gcs-resource-profile.yml)
  - [pipeline-dataproc-ephemeral-gcs-resource-profile-overrides.yml (Ephemeral GCS Config with Overrides)](../examples/pipeline-dataproc-ephemeral-gcs-resource-profile-overrides.yml)
  - [pipeline-dataproc-ephemeral-relative-resource-profile.yml (Ephemeral Relative Config Path)](../examples/pipeline-dataproc-ephemeral-relative-resource-profile.yml)

---

## 6. Validation Rules (`validation.proto`)

The framework enforces strict validation using custom Proto annotations.

- `is_required` (bool): Field must not be empty (strings/repeated) or must be
  set (messages).
- `regex` (string): String must match the regex pattern.
- `min_value` (double): Numeric value must be >= specified value.
- `min_items` (uint32): Repeated field must have at least N items.
- `disallow_zero_enum` (bool): Enum field cannot use the default `0` (undefined)
  value.
- `min_len` / `max_len` (uint32): Min/Max character length for strings.
- `is_cron_expression` (bool): String must be a valid cron schedule.
- `is_iso8601_timestamp` (bool): String must be an ISO 8601 timestamp (naive
  timestamp without `Z` or UTC offset on `ScheduleTrigger` fields).
- `is_iso8601_duration` (bool): String must be an ISO 8601 duration (e.g.,
  `PT30M`, `PT1H`) or shorthand duration (e.g., `30m`, `1h 30m`).
- `is_iana_timezone` (bool): String must be a valid IANA timezone (e.g., `UTC`).
- `map_key_regex` (string): Map key must match the regex pattern.
- `map_value_regex` (string): Map value must match the regex pattern.
- `is_parameterizable` (bool): Field accepts runtime `${ ... }` CEL expressions.

---

## 7. Parameters, Cross-Task Data Sharing & Conditions

Parameterizable fields and `run_if` (`runIf` in YAML) conditions accept
[Common Expression Language (CEL)](https://github.com/google/cel-spec)
`${ ... }` expressions evaluated at runtime against three namespaces. Because
task IDs and map keys may contain hyphens or other non-identifier characters,
prefer bracket indexing (`["..."]`) over `.` for map/action lookups:

- **`parameters["<name>"]`**: Pipeline parameters declared under `parameters:`
  (`string`, `number`, `boolean`, `array` of primitives), resolved in precedence
  order: `default` < Airflow `params` < asset events < `dag_run.conf`.
- **`run`**: Execution metadata (`run["id"]` / `run.id`, UTC ISO 8601
  `run["trigger_time"]` / `run.trigger_time`, and `YYYY-MM-DD`
  `run["trigger_date"]` / `run.trigger_date`).
- **`outputs["<action_name>"]`**: Harvested upstream output envelope containing
  `.value` and `.metadata` (`map<string, dyn>`; requires `<action_name>` in
  `depends_on`).

### 7.1. Harvested Task Output Schema (`outputs["<action_name>"]`)

| Action / Engine | `.value` (CEL Type) | `.metadata` Fields (`map<string, dyn>`) |
| :--- | :--- | :--- |
| **`python`** (`local` / virtualenv) | JSON-normalized return value of `python_callable` (`dyn`, `null` on failure) | `{}` |
| **`sql`** (`bigquery`) | First result row as `map<string, dyn>` when `.value` is referenced in CEL (`{}` if 0 rows; `null` otherwise) | `job_id` (`string \| null`), `rows_written` (`int`), `bytes_billed` (`int`) |
| **`pyspark` / `notebook` / `sql`** (`dataproc_serverless`) | JSON payload uploaded to the `gs://...` URI in `os.environ["PIPELINE_OUTPUT_URI"]` (`dyn`; `null` for `sql` or if unwritten) | `batch_id` (`string \| null`) |
| **`pyspark` / `notebook` / `sql`** (`dataproc_on_gce`) | JSON payload uploaded to the `gs://...` URI in `os.environ["PIPELINE_OUTPUT_URI"]` (`dyn`; `null` for `sql` or if unwritten) | `job_id` (`string \| null`) |
| **`pipeline`** (`dataform.dataform_service` / `dataform.airflow_worker`) | `null` | `invocation_id` (`string \| null`; `null` on `airflow_worker`), `failed_actions` (`list<string>`), `successful_actions` (`list<string>`) |
| **`pipeline`** (`dbt.airflow_worker`) | `null` | `run_id` (`string`), `failed_actions` (`list<string>`), `successful_actions` (`list<string>`) |
| **`data_ingestion`** (`bigquery_dts`) | `null` | `run_id` (`string \| null`), `run_state` (`string`), `start_time` (`string \| null`), `end_time` (`string \| null`) |
| **`orchestration_pipeline` / `ai` / `airflow_task`** | Operator XCom return value, e.g., triggered run ID (`dyn`, `null` on failure) | `{}` |

- **Dataproc `PIPELINE_OUTPUT_URI`**: For referenced `pyspark` and `notebook`
  actions, `os.environ["PIPELINE_OUTPUT_URI"]` is a `gs://...` Cloud Storage
  URI (not a local filesystem path—upload JSON via the Cloud Storage SDK rather
  than local `open()`).
- **XCom Offloading & Size Limits**: Serialized `{value, metadata}` envelopes
  >= 10 KB are automatically offloaded to
  `gs://<GCS_BUCKET>/data/task_outputs/...` when `GCS_BUCKET` is configured
  (configure a bucket lifecycle TTL rule on `data/task_outputs/` if automatic
  expiration is desired). Envelopes exceeding **1 MB** fail the task; write
  larger datasets to a table or GCS object and pass its identifier instead.

### 7.2. Conditional Execution (`run_if`)

`run_if: "${ ... }"` skips the action when the boolean CEL expression evaluates
to `false`. Downstream `trigger_rule: all_done` actions can guard against
skipped or failed upstreams with `has(outputs["<action_name>"].value)`. See
[pipeline-cross-task-data-sharing.yml](../examples/pipeline-cross-task-data-sharing.yml).
