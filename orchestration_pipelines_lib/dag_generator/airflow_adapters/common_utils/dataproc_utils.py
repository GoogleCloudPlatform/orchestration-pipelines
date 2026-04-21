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
"""Module with all dataproc related client methods."""
import os
from typing import Any, Dict

from google.api_core.client_options import ClientOptions
from google.cloud.dataproc_v1 import SessionTemplateControllerClient, GetSessionTemplateRequest
from google.cloud.dataproc_v1.types import session_templates


def get_session_template(
        region: str,
        session_template_name: str) -> session_templates.SessionTemplate:
    """Retrieves the session template using the Dataproc client.

    Args:
        region: The Google Cloud region to use for the API endpoint.
        session_template_name: The fully qualified name of the session template.

    Returns:
        The retrieved SessionTemplate object.
    """
    client_options = ClientOptions(
        api_endpoint=f"{region}-dataproc.googleapis.com:443")
    client = SessionTemplateControllerClient(client_options=client_options)
    request = GetSessionTemplateRequest(name=session_template_name)
    return client.get_session_template(request=request)


def sanitized(
    session_template: session_templates.SessionTemplate
) -> session_templates.SessionTemplate:
    """Removes unsupported fields for Batch Jobs.

    Args:
        session_template: The original session template to sanitize.

    Returns:
        The sanitized session template with unsupported fields removed.
    """
    if session_template.runtime_config and hasattr(
            session_template.runtime_config, "repository_config"):
        session_template.runtime_config.repository_config = None
    return session_template


def get_pyspark_batch_config(action: Dict[str, Any],
                             wrapper_uri: str) -> Dict[str, Any]:
    """Returns the pyspark_batch configuration.

    Args:
        action: A dictionary containing the action's configuration.
        wrapper_uri: The GCS URI for the notebook wrapper script.

    Returns:
        A dictionary representing the PySpark batch configuration.
    """
    pyspark_job = {}
    if action.type == "notebook":
        bucket_name = os.environ.get("GCS_BUCKET")
        # Using run_notebook.py as a wrapper
        pyspark_job = {
            "main_python_file_uri": wrapper_uri,
            "args": [action.filename, bucket_name, "{{ run_id }}"],
            "python_file_uris": action.pyFiles or [],
        }
    else:
        pyspark_job = {
            "main_python_file_uri": action.filename,
            "python_file_uris": action.pyFiles or [],
        }

    pyspark_job["archive_uris"] = action.archives or []
    return pyspark_job
