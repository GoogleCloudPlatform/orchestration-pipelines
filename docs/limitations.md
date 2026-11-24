# Known Limitations & Constraints

This document outlines the known architectural boundaries, environment
constraints, and temporary limitations of `orchestration-pipelines`.

## 1. Compatibility

- **Airflow Compatibility:** Versions of the package 0.4.0 and older are not
  compatible with Airflow 3.2.0+.
- **Vertex AI CustomJob Action (`create_and_run_custom_job`):** Only valid with
  Airflow versions compatible with `apache-airflow-providers-google>=22.5.0`
  (requires Airflow 2.11.0+; not supported on Airflow 2.10.x).
- **Python Versions:** Supported only on Python 3.11+. Older Python runtimes
  (e.g., 3.10) are not supported.
- **dbt Knowledge Catalog Metadata Ingestion (`knowledge_catalog` on Airflow
  worker):** Requires `gcloud-cli` (`google-cloud-cli`) version `587.0.0+`
  installed in the worker environment, as
  `gcloud dataplex dbt metadata-jobs create` graduated from `alpha` in Google
  Cloud SDK `587.0.0`. On Cloud Composer, this is bundled in newer image builds
  (for example, `composer-3-airflow-2.11.1-build.23`,
  `composer-3-airflow-3.2.2-build.10`, `composer-3-airflow-3.3.1-build.6`, and
  `composer-2.17.17-airflow-2.11.1` or later); verify `gcloud --version` when
  targeting earlier builds. The target Dataplex `entry_group` must already
  exist in the project and location (use a dedicated entry group per dbt
  project when `aspects_only` is `false`), and the Dataplex service agent
  (`gcp-sa-dataplex`) must have read access to `storage_uri` (especially when
  using the default Composer `GCS_BUCKET` or targeting another `project_id`).

## 2. Feature Limitations

- **Parameter Types:** Parameters provided to actions (e.g., in SQL queries,
  scripts) are always passed as strings. Users must handle type casting within
  their scripts, queries, or notebooks if different data types are required
  (e.g., using `CAST` in SQL).
- **YAML Keys:** Avoid using `n` or `y` (as well as `yes`, `no`, `on`, or `off`)
  as unquoted keys in your YAML definitions. Due to YAML 1.1 parsing
  specifications, these unquoted words are implicitly evaluated as boolean
  `false`/`true` rather than strings. If you must use them as strings, they must
  be wrapped in explicit quotes (e.g. `'n'` or `"y"`).
- **Event-driven scheduling** The library currently do not support enabling
  pipeline to be triggered based on anything else than a schedule and manual
  trigger.
- **Exponential Backoff Retry Strategy:** Configuring a custom `multiplier` (other
  than 2.0) or disabling jitter (`randomizeJitter: false`) is not supported on
  Airflow 2. Full customization of multiplier and deterministic backoff without
  jitter is supported on Airflow 3.3+.

## 3. Managed Airflow on GCP limitations

- **Lack of support of per-folder role auto registration**

## 4. AirflowTaskAction Operator Support Limitations

The current implementation of `AirflowTaskAction` instantiates operators
dynamically by unpacking the `params` field directly as keyword arguments
(`**kwargs`) into the target class constructor. Consequently, only operators
that accept configuration purely via JSON/YAML-serializable data structures are
supported.

---

### 4.1. Unsupported Operators & Current Limitations

The current engine cannot dynamically resolve custom Python objects or execute
code during DAG parsing. Therefore, the following patterns are not currently
supported:

1. **Python Callables & Callbacks**
   - Operators requiring executable Python functions or callbacks (e.g.,
     `python_callable` in `PythonOperator`, `BranchPythonOperator`, or
     `ShortCircuitOperator`) cannot be passed via YAML.
   - *Limitation:* The action cannot resolve a dotted string path (e.g.,
     `"my_module.my_func"`) into an executable Python callable.

2. **Complex SDK Objects & Data Models**
   - Operators requiring specialized client objects, typed models, or
     third-party SDK classes in their constructor parameters cannot be
     initialized directly.
   - *Examples:*
     - `KubernetesPodOperator` when configured with instantiated Kubernetes
       client models (`k8s.V1Volume`, `k8s.V1EnvVar`,
       `k8s.V1ResourceRequirements`).
     - `DockerOperator` when requiring `docker.types.Mount` objects.
     - Sensors expecting `datetime.timedelta` objects rather than numeric
       seconds or string templates.

---

### 4.2. Workarounds & Best Practices

- **Prefer REST/Job Configuration Schemas:** When working with cloud providers
  (GCP, AWS, Snowflake), use operators that accept standard dictionary configs
  (such as `BigQueryInsertJobOperator`) rather than operators requiring
  pre-built Python client objects.
- **Use Existing Dedicated Actions:** For workflows requiring custom Python
  logic, use existing specialized pipeline actions rather than attempting to
  embed arbitrary code references inside `AirflowTaskAction`.
