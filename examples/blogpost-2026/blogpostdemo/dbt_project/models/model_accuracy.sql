WITH actuals AS (
    SELECT
        EXTRACT(MONTH FROM o.shipped_at) AS shipping_month,
        EXTRACT(DAY FROM o.shipped_at) AS shipping_day,
        oi.num_of_item,
        ST_DISTANCE(
            ST_GEOGPOINT(u.longitude, u.latitude),
            ST_GEOGPOINT(dc.longitude, dc.latitude)
        ) / 1000 AS haversine_distance,
        DATE_DIFF(CAST(o.delivered_at AS DATE), CAST(o.shipped_at AS DATE), DAY) AS transit_days
    FROM
        `bigquery-public-data.thelook_ecommerce.orders` o
    JOIN
        `bigquery-public-data.thelook_ecommerce.users` u ON o.user_id = u.id
    JOIN
        (
            SELECT order_id, COUNT(id) AS num_of_item, MAX(inventory_item_id) AS inventory_item_id
            FROM `bigquery-public-data.thelook_ecommerce.order_items`
            GROUP BY order_id
        ) oi ON o.order_id = oi.order_id
    JOIN
        `bigquery-public-data.thelook_ecommerce.inventory_items` ii ON oi.inventory_item_id = ii.id
    JOIN
        `bigquery-public-data.thelook_ecommerce.distribution_centers` dc ON ii.product_distribution_center_id = dc.id
    WHERE
        o.status = 'Complete'
        AND o.shipped_at IS NOT NULL
        AND o.delivered_at IS NOT NULL
),
latest_predictions AS (
    SELECT *
    FROM `your-project-id.mlops.predictions_*`
    WHERE _TABLE_SUFFIX = (
        SELECT MAX(_TABLE_SUFFIX)
        FROM `your-project-id.mlops.predictions_*`
    )
)

SELECT
    p.shipping_month,
    p.shipping_day,
    p.num_of_item,
    p.haversine_distance,
    a.transit_days AS actual_transit_days,
    CAST(p.prediction AS FLOAT64) AS predicted_transit_days,
    ABS(a.transit_days - CAST(p.prediction AS FLOAT64)) AS absolute_error,
    ABS(a.transit_days - CAST(p.prediction AS FLOAT64)) > 2 AS is_sla_breached
FROM
    latest_predictions p
JOIN
    actuals a
ON
    p.shipping_month = a.shipping_month
    AND p.shipping_day = a.shipping_day
    AND p.num_of_item = a.num_of_item
    AND ROUND(p.haversine_distance, 4) = ROUND(a.haversine_distance, 4)
