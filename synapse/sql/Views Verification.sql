/* ============================================================
   VIEWS VERIFICATION
   ============================================================ */

SELECT
    TABLE_SCHEMA,
    TABLE_NAME
FROM INFORMATION_SCHEMA.VIEWS
WHERE TABLE_SCHEMA = 'dbo'
ORDER BY TABLE_NAME;

--- ROW COUNTS ---

SELECT 'vw_dim_mote' AS view_name, COUNT(*) AS row_count FROM dbo.vw_dim_mote
UNION ALL
SELECT 'vw_dim_date', COUNT(*) FROM dbo.vw_dim_date
UNION ALL
SELECT 'vw_dim_time', COUNT(*) FROM dbo.vw_dim_time
UNION ALL
SELECT 'vw_fact_sensor_reading', COUNT(*) FROM dbo.vw_fact_sensor_reading
UNION ALL
SELECT 'vw_fact_sensor_hourly', COUNT(*) FROM dbo.vw_fact_sensor_hourly
UNION ALL
SELECT 'vw_fact_sensor_daily', COUNT(*) FROM dbo.vw_fact_sensor_daily
UNION ALL
SELECT 'vw_fact_sensor_health', COUNT(*) FROM dbo.vw_fact_sensor_health;