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

import logging


def invoke_dbt_run(project_dir: str,
                   profiles_dir: str,
                   select_models: list = None):
    """Wraps dbtRunner to execute dbt commands programmatically.

    Args:
        project_dir: The directory containing the dbt project.
        profiles_dir: The directory containing the dbt profiles.yml file.
        select_models: An optional list of specific dbt models to run.

    Raises:
        Exception: If the dbt run fails or encounters a system exception.
    """
    from dbt.cli.main import dbtRunner, dbtRunnerResult

    logger = logging.getLogger("airflow.task")
    logger.info(f"🚀 Initializing dbtRunner for project: {project_dir}")

    # Initialize dbt Runner
    dbt = dbtRunner()

    # Build Arguments
    cli_args = [
        "run", "--project-dir", project_dir, "--profiles-dir", profiles_dir
    ]

    if select_models:
        cli_args.extend(["--select", " ".join(select_models)])

    logger.info(f"Running command args: {cli_args}")

    # Execute
    res: dbtRunnerResult = dbt.invoke(cli_args)

    # Handle Results
    if res.success:
        logger.info("✅ dbt run finished successfully!")
        for r in res.result:
            logger.info(f"   • {r.node.name}: {r.status}")
    else:
        if res.exception:
            logger.error(f"❌ System Exception: {res.exception}")
        if res.result:
            for r in res.result:
                if r.status != 'success':
                    logger.error(f"❌ Failed: {r.node.name} - {r.message}")
        raise Exception("dbt run failed. Check logs for details.")
