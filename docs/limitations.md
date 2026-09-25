# Known Limitations & Constraints

This document outlines the known architectural boundaries, environment
constraints, and temporary limitations of `orchestration-pipelines`.

## 1. Compatibility

- **Airflow Compatibility:** Versions of the package 0.4.0 and older are not
  compatible with Airflow 3.2.0+.
- **Python Versions:** Supported only on Python 3.10+. Older Python runtimes
  (e.g., 3.9) are not supported.

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

## 3. Managed Airflow on GCP limitations

* **Lack of support of per-folder role auto registration**

## 4. AirflowTaskAction Operator Support Limitations

The current implementation of `AirflowTaskAction` instantiates operators dynamically by unpacking the `params` field directly as keyword arguments (`**kwargs`) into the target class constructor. Consequently, only operators that accept configuration purely via JSON/YAML-serializable data structures are supported.

---

### 4.1. Unsupported Operators & Current Limitations

The current engine cannot dynamically resolve custom Python objects or execute code during DAG parsing. Therefore, the following patterns are not currently supported:

1. **Python Callables & Callbacks**
   * Operators requiring executable Python functions or callbacks (e.g., `python_callable` in `PythonOperator`, `BranchPythonOperator`, or `ShortCircuitOperator`) cannot be passed via YAML.
   * *Limitation:* The action cannot resolve a dotted string path (e.g., `"my_module.my_func"`) into an executable Python callable.

2. **Complex SDK Objects & Data Models**
   * Operators requiring specialized client objects, typed models, or third-party SDK classes in their constructor parameters cannot be initialized directly.
   * *Examples:*
     * `KubernetesPodOperator` when configured with instantiated Kubernetes client models (`k8s.V1Volume`, `k8s.V1EnvVar`, `k8s.V1ResourceRequirements`).
     * `DockerOperator` when requiring `docker.types.Mount` objects.
     * Sensors expecting `datetime.timedelta` objects rather than numeric seconds or string templates.

---

### 4.2. Workarounds & Best Practices

* **Prefer REST/Job Configuration Schemas:** When working with cloud providers (GCP, AWS, Snowflake), use operators that accept standard dictionary configs (such as `BigQueryInsertJobOperator`) rather than operators requiring pre-built Python client objects.
* **Use Existing Dedicated Actions:** For workflows requiring custom Python logic, use existing specialized pipeline actions rather than attempting to embed arbitrary code references inside `AirflowTaskAction`.
