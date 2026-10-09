# How to Contribute

We would love to accept your patches and contributions to this project. This
document outlines the end-to-end process for contributing to
`orchestration-pipelines`.

## Before You Begin

### 1. Sign our Contributor License Agreement (CLA)

Contributions to this project must be accompanied by a
[Contributor License Agreement](https://cla.developers.google.com/about) (CLA).
You (or your employer) retain the copyright to your contribution; this simply
gives us permission to use and redistribute your contributions as part of the
project.

If you or your current employer have already signed the Google CLA (even if it
was for a different project), you probably do not need to do it again.

Visit <https://cla.developers.google.com/> to see your current agreements or to
sign a new one. Every pull request is automatically checked by the `google-cla`
bot (`cla/google`).

### 2. Review our Community Guidelines

This project follows
[Google's Open Source Community Guidelines](https://opensource.google/conduct/).
Please also review our local [Code of Conduct](docs/code-of-conduct.md).

## Architecture & Improvement Proposals (AIPs)

Minor bug fixes, documentation updates, and test additions do not require an
Architecture & Improvement Proposal (AIP); you may open a pull request directly.

For **new features, declarative YAML schema additions, or major architectural
changes**, you must submit an AIP before writing the implementation code:

1. **Draft Proposal**: Copy `docs/aips/0000-template.md` to
   `docs/aips/XXXX-your-feature-name.md` (where `XXXX` is the next sequential
   number or the PR number).
2. **Submit AIP Pull Request**: Open a Pull Request on GitHub containing **only
   the markdown proposal**. Prefix your PR title with `[AIP]`.
3. **Review & Approval**: Maintainers and the community will review the design
   directly on the PR. Once consensus is reached, the AIP PR is merged into
   `main`.
4. **Implementation**: Only after the AIP is merged should you open a separate
   PR with the implementation code referencing the approved AIP.

## Local Development & Code Preparation

### Prerequisites

- **Python**: `>= 3.10` (CI runs on Python `3.11.9`)
- **uv**: Used for dependency management, lockfile verification, and running
  test/lint environments.

### Running Checks Locally

All pull requests are validated by GitHub Actions CI. Before pushing your
branch, ensure the following checks pass locally:

1. **Verify `uv.lock` consistency**:

   ```bash
   uv lock --check --no-config --default-index https://pypi.org/simple
   ```

2. **Run Ruff linter & formatter checks**:

   ```bash
   uv run --no-config --default-index https://pypi.org/simple --frozen --group dev \
     ruff check orchestration_pipelines_lib orchestration_pipelines_models examples
   ```

3. **Run `pre-commit` hooks (formatting, YAML checks, markdownlint)**:

   ```bash
   PRE_COMMIT_USE_UV=1 PIP_EXTRA_INDEX_URL=https://pypi.org/simple \
     uv run --no-config --default-index https://pypi.org/simple --frozen --group dev \
     pre-commit run --config=.pre-commit-config.yaml --all-files --show-diff-on-failure
   ```

4. **Run unit tests for both Airflow 2 and Airflow 3 (minimum 85% coverage)**:

   ```bash
   UV_PROJECT_ENVIRONMENT=.venv-a2 uv sync --no-config --default-index https://pypi.org/simple --frozen --group dev --group airflow2
   PYTHONPATH=. .venv-a2/bin/pytest tests/ \
     --cov=orchestration_pipelines_lib --cov=orchestration_pipelines_models \
     --cov-report=term-missing --cov-report=term:skip-covered --cov-append

   UV_PROJECT_ENVIRONMENT=.venv-a3 uv sync --no-config --default-index https://pypi.org/simple --frozen --group dev --group airflow3
   PYTHONPATH=. .venv-a3/bin/pytest tests/ \
     --cov=orchestration_pipelines_lib --cov=orchestration_pipelines_models \
     --cov-report=term-missing --cov-report=term:skip-covered --cov-append

   .venv-a2/bin/coverage report --fail-under=85 -m
   ```

### Coding Style & Git Hygiene

- Format and lint your Python code to follow the
  [Google Python Style Guide](https://google.github.io/styleguide/pyguide.html).
- Include the standard Apache 2.0 license header at the top of any newly created
  source files.
- Keep your pull request branch rebased on top of the latest `main` branch with
  a clean commit history.

## Pull Request Review & Merge Process

All submissions require code review via
[GitHub pull requests](https://docs.github.com/articles/about-pull-requests):

1. **Pull Request & CI**: Open a Pull Request against the `main` branch on
   GitHub. The `google-cla` bot and GitHub Actions CI will automatically
   validate your changes.
2. **Maintainer Review**: A project maintainer will review your code on GitHub.
   If changes are requested, push new commits to your PR branch.
3. **Merge & Sync**: This repository is synchronized using
   [Copybara](https://github.com/google/copybara). Once all checks pass and a
   maintainer approves your PR, Copybara imports and merges the change into the
   `main` branch, automatically closing the PR and preserving your Git `Author`
   attribution on the final commit.
