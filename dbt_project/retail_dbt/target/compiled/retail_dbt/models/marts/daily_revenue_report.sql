

WITH base AS (
    SELECT * FROM RETAIL_DB.GOLD.DAILY_REVENUE
)
SELECT 
    DATE AS transaction_date,
    TOTAL_REVENUE as total_amount,
    TRANSACTION_COUNT as total_transactions,
    AVG_ORDER_VALUE as aov
FROM base