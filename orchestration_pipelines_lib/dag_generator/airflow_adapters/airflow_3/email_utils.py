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


def send_notification_email(emails, is_success, context=None):
    """Sends an email when a DAG run is completed.

    Args:
        emails: A list of email addresses to send the notification to.
        is_success: A boolean representing if the DAG run has completed
                    successfully or has failed.
        context: The Airflow context dictionary. If not present, the current
            context can be retrieved from Task Context.
    """
    from airflow.sdk import get_current_context
    from airflow.utils.email import send_email

    if not context:
        context = get_current_context()

    dag_run = context.get("dag_run")
    task_instance = context.get("task_instance")

    if is_success:
        subject = f"DAG Success: {dag_run.dag_id}"
        h3_topic = "DAG Success"
        tasks_paragraph = ""
    else:
        subject = f"DAG Failed: {dag_run.dag_id}"
        h3_topic = "DAG Failure"
        task_id = task_instance.task_id if task_instance else "N/A"
        tasks_paragraph = (
            f"<p><b>Failed Task:</b> {task_id}</p>"
        )

    href = task_instance.log_url if task_instance else "#"

    logical_date = context.get("logical_date")
    logical_date_format = logical_date.isoformat() if logical_date else "N/A"

    html_content = f"""
    <h3>{h3_topic}</h3>
    <p><b>DAG:</b> {dag_run.dag_id}</p>
    <p><b>Run ID:</b> {dag_run.run_id}</p>
    <p><b>Logical Date:</b> {logical_date_format}</p>
    {tasks_paragraph}
    <p><b>Log URL:</b> <a href="{href}">View Logs</a></p>
    """

    send_email(to=emails, subject=subject, html_content=html_content)
