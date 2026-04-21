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
"""Email utilities for Airflow DAGs."""
from airflow.utils.email import send_email
from airflow.sdk import get_current_context


def send_failure_notification_email(emails, context=None):
    """Sends an email when a DAG run fails.

    Args:
        emails: A list of email addresses to send the notification to.
        context: The Airflow context dictionary. If not present, the current
            context can be retrieved from Task Context.
    """
    if not context:
        context = get_current_context()

    dag_run = context.get('dag_run')
    logical_date = context.get('logical_date')

    ti = context.get('task_instance')

    subject = f"DAG Failed: {dag_run.dag_id}"
    html_content = f"""
    <h3>DAG Failure Alert</h3>
    <p><b>DAG:</b> {dag_run.dag_id}</p>
    <p><b>Run ID:</b> {dag_run.run_id}</p>
    <p><b>Logical Date:</b> {logical_date.isoformat() if logical_date else 'N/A'}</p>
    <p><b>Failed Task (Trigger):</b> {ti.task_id if ti else 'Unknown'}</p>
    <p><b>Log URL:</b> <a href="{ti.log_url if ti else '#'}">View Logs</a></p>
    """

    send_email(to=emails, subject=subject, html_content=html_content)
