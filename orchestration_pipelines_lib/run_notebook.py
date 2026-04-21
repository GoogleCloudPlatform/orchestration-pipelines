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
"""Wrapper script for pyspark script."""

import sys
import os
import papermill as pm

print("Starting run_notebook.py")
input_notebook = sys.argv[1]
output_bucket = sys.argv[2]
run_id = sys.argv[3]

file_name = os.path.basename(input_notebook)
file_name, file_extension = os.path.splitext(file_name)

OUTPUT_PATH = (
    f"gs://{output_bucket}/composer_orchestration_pipelines_resources/{file_name}-output-{run_id}{file_extension}"
)

print(f"Starting notebook execution for {input_notebook}")
print(f"Output path: {OUTPUT_PATH}")
try:
    pm.execute_notebook(
        input_notebook,
        OUTPUT_PATH,
        kernel_name='python3',
    )
    print(f"Finished notebook execution for {input_notebook}")
except Exception as e:
    print(f"Error executing notebook {input_notebook}: {e}")
    raise e
