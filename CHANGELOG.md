# Change Log

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]  - yyyy-mm-dd

...

## [1.1.0] - 2026-09-02

### Added

- Support for `@once` trigger schedule interval.

### Fixed

- Output overwriting on historical runs for Dataproc notebook actions by properly resolving template variables.
- Schedule triggers population for unversioned pipelines.

## [1.0.0] - 2026-08-24

### Added

- Observability metrics for pipeline runs via DAG callbacks (success/failure counters).
- Observability metrics for action execution using a custom operator.
- Observability metrics for internal pipeline metadata population.
- The library limitations have been documented and can be found under `docs/limitations.md`.

### Fixed

- Dummy DAG indicating parsing errors now fails by default with an `AirflowFailException` showing the parsing error message.
- Actions running on Dataproc Ephemeral Cluster as engine are properly marked as success/failed based on the job status, abstracting the cleanup step result.

## [0.4.1] - 2026-08-18

### Added

- A separate document for known limitations and compatibility constraints (`docs/limitations.md`).

### Fixed

- Serialization compatibility with Airflow 3.2+.

## [0.4.0] - 2026-08-12

### Added

- Support for `.yaml` extension for pipeline definition files in addition to
  `.yml`.
- `doc_md` documentation generation for unversioned pipeline workflows.
- Source pipeline definition file path included in DAG `doc_md` metadata.
- Support for Agent Platform upload model in `AIAction`.
- Support for triggering batch inference on Agent Platform in `AIAction`.
- Emiting metrics with respect to count and duration of parsing pipeline.

### Changed

- Migrated local Dataform file staging command from `gsutil` to
  `gcloud storage`.

### Fixed

- Detection of circular dependencies between actions during pipeline validation.
- Path resolution in `FileManager` for non-versioned pipelines.
- `dag_id` verification check in `globals()`.
- PyPI documentation links in `README.md`.

## [0.3.0] - 2026-07-08

### Added

- Params support for SQL, Pyspark and Notebook actions.
- Params support for DBT pipeline and Dataform pipeline local (on Airflow) executions.
- Support for custom labels in Dataproc, SQL, and Dataform local execution.
- Inline requirements support for Notebook/PySpark run on Dataproc.
- Support for trigger rules in all actions.
- New action, supporting triggering another orchestration pipeline.

### Changed

- Replaced `runtime_params` from BigQuery DTS action with two specific fields: `requested_run_time` and `requested_time_range`.

### Fixed

- Improved error message for datetime/cron/duration/timezone validation.
- Adjusted examples to comply with validation rules.

## [0.2.0] - 2026-05-15

### Added

- Action for triggering existing BigQuery DTS configuration.

### Changed

- Performance improvements.

### Fixed

- Inline query option for SQL action with Dataproc engine selected.

## [0.1.2] - 2026-04-17

### Changed

- The Dataform local docker image has been updated. Now users should not provide the dataform-core version in the workflow_settings.yaml.
- Performance improvements.

### Fixed

- Auto-generated batch_id for actions using dataprocServerless is now resolved properly in both Airflow 2 and Airflow 3.

[Unreleased]: https://github.com/GoogleCloudPlatform/orchestration-pipelines/compare/v1.1.0...main
[1.1.0]: https://github.com/GoogleCloudPlatform/orchestration-pipelines/compare/v1.0.0...v1.1.0
[1.0.0]: https://github.com/GoogleCloudPlatform/orchestration-pipelines/compare/v0.4.1...v1.0.0
[0.4.1]: https://github.com/GoogleCloudPlatform/orchestration-pipelines/compare/v0.4.0...v0.4.1
[0.4.0]: https://github.com/GoogleCloudPlatform/orchestration-pipelines/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/GoogleCloudPlatform/orchestration-pipelines/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/GoogleCloudPlatform/orchestration-pipelines/compare/v0.1.2...v0.2.0
[0.1.2]: https://github.com/GoogleCloudPlatform/orchestration-pipelines/releases/tag/v0.1.2

