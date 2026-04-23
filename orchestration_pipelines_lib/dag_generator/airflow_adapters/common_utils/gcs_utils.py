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
"""Module with various file util methods."""
import os
import importlib
import importlib.resources

BLOB_NAME_CLUSTER = "data/run_notebook.py"


def upload_run_notebook_if_needed(gcs_path: str):
    """Checks buckets for the existence of run_notebook.py and uploads it if missing.

    Args:
        gcs_path: The GCS path where the notebook should be uploaded.
    """
    path_parts = gcs_path.replace("gs://", "").split("/", 1)
    bucket_name = path_parts[0]
    blob_name = path_parts[1]

    from google.cloud import storage
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    if blob.exists():
        print(f"File already exists at {gcs_path}.")
        return

    # File does not exist, so we need to upload it.
    try:
        # Using importlib.resources.files is the modern approach (Python 3.9+)
        files = importlib.resources.files("orchestration_pipelines_lib")
        notebook_path = files.joinpath("run_notebook.py")
        with importlib.resources.as_file(notebook_path) as local_path:
            blob.upload_from_filename(str(local_path))
            print(f"File {local_path} uploaded to {gcs_path}.")

    except (ImportError, AttributeError):
        print(f"Exception while uploading to {gcs_path}.")


def get_run_notebook_gcs_path() -> str:
    """Returns the GCS path for the run_notebook.py script.

    Returns:
        The full GCS path to the notebook wrapper script.

    Raises:
        ValueError: If the GCS_BUCKET environment variable is not set.
    """
    bucket_name = os.environ.get("GCS_BUCKET")
    if not bucket_name:
        raise ValueError(
            "GCS_BUCKET environment variable not set. " \
            "This is expected to be set in a Cloud Composer environment."
        )
    gcs_path = f"gs://{bucket_name}/{BLOB_NAME_CLUSTER}"

    return gcs_path


def get_gcs_file_content(gcs_path: str) -> str:
    """Downloads a file from GCS and returns its content as a string.

    Args:
        gcs_path: The full GCS path of the file to download.

    Returns:
        The content of the GCS file as a string.
    """
    path_parts = gcs_path.replace("gs://", "").split("/", 1)
    bucket_name = path_parts[0]
    blob_name = path_parts[1]
    from google.cloud import storage
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    return blob.download_as_string().decode("utf-8")


def read_local_file_content(relative_path: str) -> str:
    """Reads a local file from the DAGS_FOLDER and returns its content as a string.

    Args:
        relative_path: The relative path to the file from the DAGS_FOLDER.

    Returns:
        The content of the local file as a string.
    """
    dags_folder = os.environ.get("DAGS_FOLDER", "/home/airflow/gcs/dags")
    full_path = os.path.join(dags_folder, relative_path)
    with open(full_path, "r", encoding="utf-8") as f:
        return f.read()


def read_local_file_content_from_path(file_path: str) -> str:
    """Reads a local file from a specific path and returns its content as a string.

    Args:
        file_path: The absolute or relative path to the file.

    Returns:
        The content of the local file as a string.
    """
    with open(file_path, "r", encoding="utf-8") as f:
        return f.read()
