# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""Module to invoke dbt commands programmatically using dbtRunner."""

import json
import logging


def invoke_dbt_run(
    project_dir: str,
    profiles_dir: str,
    select_models: list = None,
    params: dict = None,
):
    """Wraps dbtRunner to execute dbt commands programmatically.

    Args:
        project_dir: The directory containing the dbt project.
        profiles_dir: The directory containing the dbt profiles.yml file.
        select_models: An optional list of specific dbt models to run.
        params: An optional dictionary of dbt parameters/variables.

    Raises:
        RuntimeError: If the dbt run fails or encounters a system exception.
    """
    from dbt.cli.main import dbtRunner, dbtRunnerResult

    logger = logging.getLogger("airflow.task")
    logger.info("🚀 Initializing dbtRunner for project: %s", project_dir)

    # Initialize dbt Runner
    dbt = dbtRunner()

    # Build Arguments
    cli_args = [
        "run",
        "--project-dir",
        project_dir,
        "--profiles-dir",
        profiles_dir,
    ]

    if select_models:
        cli_args.extend(["--select", " ".join(select_models)])

    if params:
        cli_args.extend(["--vars", json.dumps(params)])

    logger.info("Running command args: %s", cli_args)

    # Execute
    res: dbtRunnerResult = dbt.invoke(cli_args)

    # Handle Results
    if res.success:
        logger.info("✅ dbt run finished successfully!")
        for r in res.result:
            logger.info("   • %s: %s", r.node.name, r.status)
    else:
        if res.exception:
            logger.error("❌ System Exception: %s", res.exception)
        if res.result:
            for r in res.result:
                if r.status != "success":
                    logger.error("❌ Failed: %s - %s", r.node.name, r.message)
        raise RuntimeError("dbt run failed. Check logs for details.")
