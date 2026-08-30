SELECT
    AVG(absolute_error) AS mae,
    COUNT(*) AS total_predictions,
    SUM(CASE WHEN is_sla_breached THEN 1 ELSE 0 END) AS total_breaches,
    SUM(CASE WHEN is_sla_breached THEN 1 ELSE 0 END) / COUNT(*) AS pct_breaches,
    (AVG(absolute_error) > 1.5) AS trigger_retraining
FROM
    {{ ref('model_accuracy') }}
