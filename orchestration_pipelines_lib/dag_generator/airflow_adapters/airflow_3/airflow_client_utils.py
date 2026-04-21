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
import airflow_client.client
import google.auth
from airflow.configuration import conf
from google.auth.transport.requests import Request


def get_airflow_client_configuration():
    """Gets the configuration for the Airflow Python client.

    Retrieves default Google Cloud credentials and configures the Airflow
    client with the webserver URL and access token. For now, it requires a
    GCP environment or pre-configured Application Default Credentials (ADC).

    Returns:
        The configured Airflow client Configuration object.

    Raises:
        ValueError: If the Airflow webserver URL cannot be determined.
    """
    # TODO: Parameterize auth configuration based on environment variable,
    # to accomodate open source solutions too.
    credentials, _ = google.auth.default(
        scopes=["https://www.googleapis.com/auth/cloud-platform"])
    webserver_url = os.environ.get("AIRFLOW__WEBSERVER__BASE_URL") or conf.get(
        'api', 'base_url')

    if not webserver_url:
        raise ValueError("Could not determine Airflow webserver URL.")

    configuration = airflow_client.client.Configuration(host=webserver_url)
    credentials.refresh(Request())
    configuration.access_token = credentials.token

    return configuration


def get_airflow_api_client():
    """Gets an authenticated Airflow API client.

    Returns:
        An instance of ApiClient configured for the current Airflow environment.
    """
    configuration = get_airflow_client_configuration()
    return airflow_client.client.ApiClient(configuration)
