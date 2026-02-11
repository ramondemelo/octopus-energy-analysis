---1
SELECT
    *
FROM octopus_prices;

---2
SELECT
    valid_FROM,
    value_inc_vat
FROM octopus_prices;

---3
SELECT
    count(*) AS zero_price_counts
FROM octopus_prices
WHERE value_inc_vat = 0;

---4
SELECT temperature_2m
FROM uk_weather
WHERE temperature_2m BETWEEN 0 AND 2;

---5
SELECT
    settlement_date,
    nd
FROM neso_demand
WHERE settlement_date = '2025-01-01';

---6
SELECT
    wind_speed_10m
FROM uk_weather
ORDER BY wind_speed_10m DESC
LIMIT 10;

---7
SELECT
    count(settlement_period)
FROM neso_demand;

---8
SELECT
    min(value_inc_vat) as min_price,
    max(value_inc_vat) as max_price
FROM octopus_prices;

---9
SELECT
    date('month', temperature_2m),
    avg(temperature_2m)
FROM
    weather_live
GROUP BY;

---10
SELECT
    DATE_TRUNC('month', valid_from) AS month_start, AVG(value_inc_vat) AS avg_below_0
FROM octopus_prices
GROUP BY month_start
HAVING AVG(value_inc_vat) < 0;

--11. Basic Inner Join: Join octopus_prices and neso_demand on their timestamps. 
-- Select valid_from, value_inc_vat, and nd_demand. (Tables: octopus_prices, neso_demand)

SELECT 
    valid_from, value_inc_vat, nd
FROM octopus_prices
INNER JOIN neso_demand 
ON octopus_prices.valid_from = neso_demand.start_time;

-- 12. Join with Filter: Join prices and demand, but only show rows where nd_demand > 40,000 MW. 
-- (Tables: octopus_prices, neso_demand)

SELECT 
    valid_from, value_inc_vat, nd
FROM octopus_prices 
INNER JOIN neso_demand
ON octopus_prices.valid_from = neso_demand.start_time
WHERE nd > 40000;

-- 13. Three-Table Join: Join prices, demand, and weather. Use date_trunc('hour', ...) 
-- to match 30-min price data to hourly weather data. (Tables: all three)

SELECT 
    value_inc_vat, nd, temperature_2m
FROM octopus_prices
JOIN neso_demand
ON octopus_prices.valid_from = neso_demand.start_time
JOIN uk_weather
ON neso_demand.start_time = uk_weather.observation_time;

-- 14. Left Join Check: Left Join octopus_prices (Left) with uk_weather (Right). 
-- Filter for rows where temperature_2m is NULL to find missing weather data. (Tables: octopus_prices, uk_weather)

SELECT *
FROM octopus_prices
LEFT JOIN uk_weather
ON octopus_prices.valid_from = uk_weather.observation_time
WHERE temperature_2m = NULL;


-- 15. Compare Tables: Join prices and demand. Calculate the revenue potential (value_inc_vat * nd_demand) 
-- for each row. (Tables: octopus_prices, neso_demand)

SELECT 
    settlement_date, value_inc_vat * nd AS revenue_potential
FROM octopus_prices
JOIN neso_demand 
ON octopus_prices.valid_from = neso_demand.start_time;


-- 16. Self-Join: Join octopus_prices to itself to compare the price at valid_from with 
-- the price 24 hours later. (Table: octopus_prices)

SELECT
    p1.valid_from as ORGINAL_TIME,
    p1.value_inc_vat as ORIGINAL_VALUE,
    p2.valid_from as NEXT_DAY_TIME,
    p2.value_inc_vat as next_day_price,
    (p2.value_inc_vat - p1.value_inc_vat) as price_diff
FROM octopus_prices p1
JOIN octopus_prices p2
    ON p2.valid_from = p1.valid_from + INTERVAL '24 hours'
LIMIT 10;


-- 17. Aggregated Join: Join weather and prices. Calculate the average price for every 
-- distinct temperature (rounded to nearest integer). (Tables: uk_weather, octopus_prices)

SELECT temperature_2m, AVG(value_inc_vat) average_price
FROM uk_weather
JOIN octopus_prices
ON uk_weather.observation_time = octopus_prices.valid_from
GROUP BY temperature_2m
ORDER BY temperature_2m ASC;


-- 18. Filtered Join Analysis: Join demand and weather. Find the average nd_demand when 
-- wind_speed_10m is > 10 m/s. (Tables: neso_demand, uk_weather)

SELECT 
    AVG(nd), wind_speed_10m
FROM neso_demand
JOIN uk_weather
    ON uk_weather.observation_time = neso_demand.start_time
GROUP BY (wind_speed_10m)
HAVING wind_speed_10m > 10
ORDER BY wind_speed_10m ASC;



-- 19. Date Trunc Join: Join demand and weather matching strictly on the Day (not hour). Calculate average daily demand vs average daily temp. (Tables: neso_demand, uk_weather)



-- 20. Complex Filter: Find the valid_from times where Price was Negative AND Wind Speed was > 12 m/s. (Tables: octopus_prices, uk_weather)







-- SELECT 
--     NOW() AS right_now,
--     NOW() + INTERVAL '2 hours 30 minutes' AS future,
--     NOW() - INTERVAL '1 year' AS last_year;
