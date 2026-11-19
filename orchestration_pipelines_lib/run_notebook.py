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
"""Wrapper script for executing Jupyter notebooks with papermill."""

import json
import os
import sys

DEFAULT_OUTPUT_SUBDIR = "composer_orchestration_pipelines_resources"


def build_output_path(
    input_notebook: str, output_target: str, run_id: str
) -> str:
    """Builds the destination GCS URI for the executed notebook.

    Args:
        input_notebook: Path or GCS URI of the input notebook.
        output_target: Either a GCS URI prefix (starting with ``gs://``) or a
            bare GCS bucket name (legacy fallback writing under
            ``composer_orchestration_pipelines_resources/``).
        run_id: Airflow run ID appended to the output filename.

    Returns:
        The full GCS URI where the executed notebook should be written.

    Raises:
        ValueError: If a ``gs://`` ``output_target`` has no valid bucket
            segment (e.g., ``"gs://"`` or ``"gs:///"``).
    """
    file_name = os.path.basename(input_notebook)
    file_name, file_extension = os.path.splitext(file_name)
    output_filename = f"{file_name}-output-{run_id}{file_extension}"

    if output_target.startswith("gs://"):
        remainder = output_target.removeprefix("gs://").rstrip("/")
        bucket = remainder.split("/", 1)[0]
        if not bucket or any(ch.isspace() for ch in bucket):
            raise ValueError(
                f"Invalid GCS output URI prefix: {output_target!r}"
            )
        return f"gs://{remainder}/{output_filename}"

    return f"gs://{output_target}/{DEFAULT_OUTPUT_SUBDIR}/{output_filename}"


def main(argv: list[str] | None = None) -> None:
    """Executes the input notebook and writes the archived output to GCS."""
    import papermill as pm

    args = sys.argv[1:] if argv is None else argv
    print("Starting run_notebook.py")
    input_notebook = args[0]
    output_target = args[1]
    run_id = args[2]

    # Parse additional arguments as papermill parameters
    parameters = {}
    if len(args) > 3:
        parameters = json.loads(args[3])

    output_path = build_output_path(input_notebook, output_target, run_id)

    print(f"Starting notebook execution for {input_notebook}")
    print(f"Output path: {output_path}")
    try:
        pm.execute_notebook(
            input_notebook,
            output_path,
            parameters=parameters,
            kernel_name="python3",
        )
        print(f"Finished notebook execution for {input_notebook}")
    except Exception as e:
        print(f"Error executing notebook {input_notebook}: {e}")
        raise e


if __name__ == "__main__":
    main()
