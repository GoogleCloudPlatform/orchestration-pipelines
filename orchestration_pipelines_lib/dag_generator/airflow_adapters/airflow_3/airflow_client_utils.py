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
"""Module with Airflow 3 Python Client package methods."""
import os

_API_CLIENT = None
_CREDENTIALS = None


def get_airflow_api_client():
    """Gets an authenticated Airflow API client.

    Returns:
        An instance of ApiClient configured for the current Airflow environment.
    """
    global _API_CLIENT, _CREDENTIALS
    import google.auth
    from google.auth.transport.requests import Request
    from airflow.configuration import conf
    import airflow_client.client

    if _CREDENTIALS is None:
        _CREDENTIALS, _ = google.auth.default(
            scopes=["https://www.googleapis.com/auth/cloud-platform"])
        _CREDENTIALS.refresh(Request())
    elif not _CREDENTIALS.valid:
        _CREDENTIALS.refresh(Request())

    if _API_CLIENT is None:
        webserver_url = os.environ.get("AIRFLOW__WEBSERVER__BASE_URL") or conf.get(
            'api', 'base_url')

        if not webserver_url:
            raise ValueError("Could not determine Airflow webserver URL.")

        configuration = airflow_client.client.Configuration(host=webserver_url)
        configuration.access_token = _CREDENTIALS.token
        _API_CLIENT = airflow_client.client.ApiClient(configuration)
    else:
        _API_CLIENT.configuration.access_token = _CREDENTIALS.token

    return _API_CLIENT
