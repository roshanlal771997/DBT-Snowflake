WITH orders_agg AS (
    SELECT
        customer_id,
        COUNT(*) AS total_orders,
        SUM(order_total) AS total_amount,
        AVG(order_total) AS avg_order_value,
        MAX(order_date) AS last_order_date
    FROM {{ source('demo', 'orders') }}
    WHERE status = 'Completed'
    GROUP BY customer_id
),

customers_data AS (
    SELECT
        customer_id,
        first_name,
        last_name,
        email,
        city,
        state,
        signup_date
    FROM {{ source('demo', 'customers') }}
)

SELECT
    c.customer_id,
    c.first_name,
    c.last_name,
    c.email,
    c.city,
    c.state,
    c.signup_date,
    o.total_orders,
    o.total_amount,
    o.avg_order_value,
    o.last_order_date
FROM customers_data c
LEFT JOIN orders_agg o ON c.customer_id = o.customer_id
