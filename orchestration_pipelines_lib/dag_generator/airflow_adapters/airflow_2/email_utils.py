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

def send_failure_notification_email(emails, context):
    """Sends an email when a DAG run fails.

    Args:
        emails: A list of email addresses to send the notification to.
        context: The Airflow context dictionary.
    """
    from airflow.utils.email import send_email
    dag_run = context.get('dag_run')
    task_instances = dag_run.get_task_instances()

    failed_tasks = [
        ti.task_id for ti in task_instances if ti.state == 'failed'
    ]

    subject = f"DAG Failed: {dag_run.dag_id}"
    html_content = f"""
    <h3>DAG Failure Alert</h3>
    <p><b>DAG:</b> {dag_run.dag_id}</p>
    <p><b>Run ID:</b> {dag_run.run_id}</p>
    <p><b>Execution Date:</b> {context.get('execution_date')}</p>
    <p><b>Failed Tasks:</b> {', '.join(failed_tasks)}</p>
    <p><b>Log URL:</b> {task_instances[0].log_url if task_instances else 'Not available'}</p>
    """

    send_email(to=emails, subject=subject, html_content=html_content)
