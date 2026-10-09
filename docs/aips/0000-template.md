# AIP-XXXX: [Short Descriptive Title]

| Field | Value |
| :--- | :--- |
| **AIP** | XXXX (assigned when the proposal PR is opened) |
| **Title** | [Short Descriptive Title] |
| **Author(s)** | [Name (@github-handle)] |
| **Status** | Draft \| Under Review \| Approved \| Rejected \| Implemented |
| **Created** | YYYY-MM-DD |

## Abstract

Provide a brief (2-4 sentence) technical summary of the proposed change or
feature and what it achieves.

## Motivation

Explain why this change is needed:

- What user problem, limitation, or architectural gap does it solve?
- Why are existing capabilities in `orchestration-pipelines` insufficient?

## Detailed Design

Describe the proposed solution in detail:

- **YAML Schema / Model Changes**: Show any additions or modifications to the
  declarative pipeline YAML specification and protobuf/Pydantic models.
- **DAG Generation & Operator Behavior**: Explain how the pipeline generator and
  underlying Airflow operators will execute the change across Airflow 2 and
  Airflow 3.
- **Backward Compatibility**: State whether existing pipeline definitions remain
  valid without modification, or describe the migration path.

## Security & Multi-Tenancy Implications

Describe any security considerations, including credential handling, parameter
rendering, IAM permissions, or isolation in shared Airflow environments.

## Alternatives Considered

List alternative designs or workarounds that were evaluated and explain why they
were not chosen.

## Test Plan

Describe how the feature will be verified:

- Unit test coverage across both `airflow2` and `airflow3` test environments.
- Schema validation and error-handling test cases.
- Example pipeline definitions (`examples/`) to add or update.
