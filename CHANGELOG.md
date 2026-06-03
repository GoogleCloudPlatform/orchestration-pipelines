# Change Log

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased] - yyyy-mm-dd

...

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


[Unreleased]: https://github.com/GoogleCloudPlatform/orchestration-pipelines/compare/v0.2.0...HEAD