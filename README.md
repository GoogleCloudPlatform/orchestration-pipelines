# Orchestration-pipelines

[![PyPI version](https://badge.fury.io/py/orchestration-pipelines.svg)](https://badge.fury.io/py/orchestration-pipelines)
[![Python Versions](https://img.shields.io/pypi/pyversions/orchestration-pipelines.svg)](https://pypi.org/project/orchestration-pipelines/)
[![Support Status](https://img.shields.io/badge/support-preview-orange.svg)](https://github.com/GoogleCloudPlatform/orchestration-pipelines)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

A library for defining and generating Apache Airflow DAGs declaratively using YAML. Currently focused on orchestration of GCP resources (Dataproc, BigQuery, Dataform) and DBT...

> [!NOTE]
> This library is currently in **Preview**.

## Overview

`orchestration-pipelines` allows you to define complex data workflows in simple, human-readable YAML files. It abstracts away the boilerplate of writing Airflow DAGs in Python, making it easier for non-Python experts to create and manage pipelines.

- **Documentation**: [docs/doc.md](https://github.com/GoogleCloudPlatform/orchestration-pipelines/blob/main/docs/doc.md)
- **Changelog**: [CHANGELOG.md](https://github.com/GoogleCloudPlatform/orchestration-pipelines/blob/main/CHANGELOG.md)

## Supported Python Versions

Python >= 3.9

## Features

-   **Declarative DAGs**: Define your pipeline structure, triggers, and actions in YAML.
-   **Rich Actions Support**: Built-in support for:
    -   Python Scripts
    -   Google Cloud BigQuery
    -   Google Cloud Dataproc (Serverless, Ephemeral and existing clusters)
    -   Google Cloud Dataform
    -   DBT
-   **Automatic Generation**: A simple Python call generates the full Airflow DAG.
-   **Versioning**: Supports versioning of pipelines via a manifest file(as of Preview, on Google Cloud Composer).

## Installation

You can install `orchestration-pipelines` from PyPI:

```bash
pip install orchestration-pipelines
```

> [!IMPORTANT]
> Ensure your `apache-airflow-client` version is fully compatible with Airflow 3 to prevent critical DAG parsing or runtime errors. This package utilizes Airflow Client API calls to interact with the metadata database; `apache-airflow-client` library introduces significant architectural shifts in newer versions, a version mismatch will likely break communication and disrupt your pipelines. Always verify that your client version aligns with your Airflow environment to ensure stability.

## Quick Start

### 1. Define your pipeline in YAML

Create a file named `my_pipeline.yml`:

```yaml
modelVersion: "1.0"
pipelineId: "my_pipeline"
description: "A simple example pipeline"
runner: "airflow"

defaults:
  projectId: "your-gcp-project"
  location: "us-central1"

triggers:
  - schedule:
      interval: "0 4 * * *"
      startTime: "2026-01-01T00:00:00"
      catchup: false

actions:
  - sql:
      name: "create_table"
      query:
        inline: "CREATE TABLE IF NOT EXISTS `your-gcp-project.my_dataset.my_table` (id INT64, name STRING);"
      engine:
        bigquery:
          location: "US"
```

> **Known Limitations**
>
> Parameters provided to actions (e.g., in SQL queries, scripts) are always passed as strings.
Users must handle type casting within their scripts, queries, or notebooks if different data types
are required (e.g., using `CAST` in SQL).
>
> Avoid using `n` or `y` (as well as `yes`, `no`, `on`, or `off`) as unquoted keys in your YAML definitions.
> Due to YAML 1.1 parsing specifications, these unquoted words are implicitly evaluated as boolean `false`/`true` rather than strings.
> *If you must use them as strings*, they **must** be wrapped in explicit quotes (e.g. `'n'` or `"y"`).


### 2. Generate the Airflow DAG

Create a Python file named `my_pipeline.py` in your Airflow DAGs folder:

```python
from orchestration_pipelines_lib.api import generate

# Generate Airflow DAG from pipeline definition file
# airflow | dag
# Root is "dags" directory in Composer bucket
generate("dataform-pipeline-local.yml")
```

Airflow will parse this Python file and automatically generate the DAG based on your YAML definition.

## Advanced Features

### Versioning and Manifests

You can manage multiple versions of your pipelines using a `manifest.yml` file. This allows you to specify which version of a pipeline should be active.

See the `examples/` directory for a sample `manifest.yml` and how to use it.

## Contributing

Contributions are welcome! Please see [contributing.md](https://github.com/GoogleCloudPlatform/orchestration-pipelines/blob/main/contributing.md) for guidelines.

## License

This project is licensed under the Apache 2.0 License - see the [LICENSE](https://github.com/GoogleCloudPlatform/orchestration-pipelines/blob/main/LICENSE) file for details.
