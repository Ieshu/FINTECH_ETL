-- models/transformations.sql

WITH base AS (
    SELECT
        transaction_id,
        amount,
        timestamp,
        category,
        status,
        discount,
        amount * (1 - discount) AS discounted_amount,
        EXTRACT(YEAR FROM timestamp) AS year,
        EXTRACT(MONTH FROM timestamp) AS month
    FROM {{ ref('raw_transactions') }}
)

SELECT
    *,
    RANK() OVER (PARTITION BY category ORDER BY amount) AS rank
FROM base
WHERE amount > 0
