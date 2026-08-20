# Known Limitations & Constraints

This document outlines the known architectural boundaries, environment constraints, and temporary limitations of `orchestration-pipelines`.

## 1. Compatibility

* **Airflow Compatibility:** Versions of the package 0.4.0 and older are not compatible with Airflow 3.2.0+.
* **Python Versions:** Supported only on Python 3.9+. Older Python runtimes (e.g., 3.8) are not supported.

## 2. Feature Limitations

* **Parameter Types:** Parameters provided to actions (e.g., in SQL queries, scripts) are always passed as strings. Users must handle type casting within their scripts, queries, or notebooks if different data types are required (e.g., using `CAST` in SQL).
* **YAML Keys:** Avoid using `n` or `y` (as well as `yes`, `no`, `on`, or `off`) as unquoted keys in your YAML definitions. Due to YAML 1.1 parsing specifications, these unquoted words are implicitly evaluated as boolean `false`/`true` rather than strings. If you must use them as strings, they must be wrapped in explicit quotes (e.g. `'n'` or `"y"`).
* **Event-driven scheduling** The library currently do not support enabling pipeline to be triggered based on anything else than a schedule and manual trigger.

## 3. Managed Airflow on GCP limitations

* **Lack of support of per-folder role auto registration**
