--1
SELECT 
    DATE_TRUNC('day', start_time::TIMESTAMP) AS trading_day, 
    nd,
    CASE
        -- We cast to TIMESTAMP here so EXTRACT can read the day of the week
        WHEN EXTRACT(ISODOW FROM start_time::TIMESTAMP) IN (6, 7) THEN 'weekend'
        ELSE 'work_day'
    END AS day_type
FROM neso_demand;







