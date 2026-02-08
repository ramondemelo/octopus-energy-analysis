CREATE VIEW v_master_energy_weather AS
SELECT 
    n.start_time,
    n.nd,
    n.embedded_wind_generation,
    o.value_inc_vat as price,
    w.temperature_2m,
    w.wind_speed_10m
FROM neso_demand n
JOIN octopus_prices o ON n.start_time = o.valid_from
JOIN uk_weather w ON date_trunc('hour', n.start_time) = w.observation_time;



SELECT * FROM v_master_energy_weather WHERE price < 0;

---1
SELECT valid_from,
value_inc_vat
FROM octopus_prices
WHERE value_inc_vat < 0
ORDER BY value_inc_vat ASC;

---2
