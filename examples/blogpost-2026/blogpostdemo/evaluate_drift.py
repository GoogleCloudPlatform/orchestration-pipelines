def check_drift():
    from google.cloud import bigquery
    from airflow.exceptions import AirflowSkipException
    
    client = bigquery.Client()
    query = "SELECT trigger_retraining FROM `your-project-id.mlops.drift_metrics` LIMIT 1"
    
    results = list(client.query(query))
    
    if not results or not results[0].trigger_retraining:
        raise AirflowSkipException("Drift not detected. Skipping retraining.")
