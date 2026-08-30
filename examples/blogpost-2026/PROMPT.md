Act as an expert GCP MLOps Engineer. Generate 3 YAML Orchestration pipelines and their necessary side files (SQL, Python, dbt) for a package transit day predictor.

### 1. Training Pipeline (training-pipeline.yml) & Files:

- **extract_training_data (BQ SQL)**: Run `blogpostdemo/training_query.sql`. Output: `projectId.mlops.training_dataset`. BQ Location: `US`.
  SQL logic: Pure SELECT joining `bigquery-public-data.thelook_ecommerce` tables (specifically `order_items` joined with `orders` for `num_of_item`, `users` for `longitude`/`latitude`, `inventory_items`, and `distribution_centers` for their `longitude`/`latitude`). Filter on 'Complete' state before the year 2023, only valid dates. Extract the `shipping_month`, `shipping_day`, `num_of_item`, Haversine distance (km). Target: `transit_days` (delivery minus shipping).

- **train_model_dataproc (PySpark Dataproc Serverless)**: Run `blogpostdemo/train_model.py`. Input: BQ dataset. Output: `gs://your-bucket-name/models/tf_transit_days_model`.
  CRITICAL Configs: `runtimeConfig.version: "2.3"`.
  Python script logic: Split 70/30, train a standard Keras NN (Normalization/Dense/Dropout) on the 4 features.

- **upload_model_vertex**: Register as `transit_days_predictor` from Dataproc output path using image `us-docker.pkg.dev/vertex-ai/prediction/tf2-cpu.2-14:latest`.

### 2. Inference Pipeline (inference-pipeline.yml) & Files:

- **extract_inference_data (BQ SQL)**: Run `blogpostdemo/inference_query.sql`. Output: `projectId.mlops.inference_dataset`. BQ Location: `US`.
  SQL logic: Pure SELECT with same features as training, but filter for 'Shipped' between 2023-2024 (no `delivered_at`, no target).

- **run_vertex_batch_prediction**: Predict on BQ dataset using model `projects/projectId/locations/us-central1/models/your-model-id`. Destination prefix: `bq://projectId.mlops`.

### 3. Evaluation Pipeline (evaluation-pipeline.yml) & Files:

- **run_dbt_models**: Run local project `blogpostdemo/dbt_project`. Target dataset: `mlops`. Materialize as tables.
  CRITICAL: Generate `profiles.yml` (OAuth, target: `projectId/mlops`). Do NOT use `+dataset` in `dbt_project.yml`.

- **dbt Model 1 (model_accuracy.sql)**: Join latest `predictions_*` with actuals. Extract prediction using `CAST(prediction AS FLOAT64)`. Flag `is_sla_breached` if the absolute error is superior to 2 days.
  CRITICAL: In the actuals CTE, recalculate `shipping_month`, `shipping_day`, and `haversine_distance` from raw `thelook_ecommerce` tables exactly like the training query.

- **dbt Model 2 (drift_metrics.sql)**: Aggregate MAE, total/pct breaches. Create boolean `trigger_retraining`.

- **check_retraining_condition (Python action)**: Run `blogpostdemo/evaluate_drift.py` to fetch the latest `trigger_retraining` from `mlops.drift_metrics`. If false, raise `AirflowSkipException`.
  CRITICAL: Omit the `environment:` block in the YAML so it inherits host packages. In the script, place all import statements inside the function, and use `list(client.query)` instead of `.to_dataframe()`.

- **trigger_retraining_pipeline**: If drift is True, trigger `training-pipeline` (bundle `my-first-bundle`, `waitForCompletion: false`).



