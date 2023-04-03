WITH summary AS (
SELECT
    dfr.courier_id,
    dc.courier_name,
    DATE_PART('year', order_ts) AS settlement_year,
    DATE_PART('month', order_ts) AS settlement_month,
    COUNT(order_id) AS orders_count,
    SUM("sum") AS orders_total_sum,
    AVG(rate) AS rate_avg,
    SUM("sum") * 0.25 AS order_processing_fee,
    CASE
        WHEN AVG(rate) < 4 THEN GREATEST(SUM("sum") * 0.05, 100)
        WHEN AVG(rate) >= 4 AND AVG(rate) < 4.5 THEN GREATEST(SUM("sum") * 0.07, 150)
        WHEN AVG(rate) >= 4.5 AND AVG(rate) < 4.9 THEN GREATEST(SUM("sum") * 0.08, 175)
        WHEN AVG(rate) >= 4.9 THEN GREATEST(SUM("sum") * 0.10, 200)
    END courier_order_sum,
    SUM(tip_sum) AS courier_tips_sum    
FROM dds.fct_delivery_reports AS dfr
LEFT JOIN dds.dm_couriers AS dc ON
    dfr.courier_id = dc.id
WHERE
    DATE_PART('year', order_ts) = DATE_PART('year', CURRENT_DATE) AND
    DATE_PART('month', order_ts) = DATE_PART('month', CURRENT_DATE) - 1   
GROUP BY 
    dfr.courier_id,
    dc.courier_name,
    settlement_year,
    settlement_month
ORDER BY dfr.courier_id ASC
)
INSERT INTO cdm.dm_courier_ledger (
    courier_id,
    courier_name,
    settlement_year,
    settlement_month,
    orders_count,
    orders_total_sum,
    rate_avg,
    order_processing_fee,
    courier_order_sum,
    courier_tips_sum,
    courier_reward_sum 
)
SELECT 
    *,
    courier_order_sum + courier_tips_sum * 0.95 AS courier_reward_sum
FROM summary

