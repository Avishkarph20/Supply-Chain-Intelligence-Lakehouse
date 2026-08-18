USE iot_berkeley_lab_sql;
GO

/* ============================================================
   Drop existing fact views if they already exist
   ============================================================ */

DROP VIEW IF EXISTS dbo.vw_fact_sensor_health;
GO

DROP VIEW IF EXISTS dbo.vw_fact_sensor_daily;
GO

DROP VIEW IF EXISTS dbo.vw_fact_sensor_hourly;
GO

DROP VIEW IF EXISTS dbo.vw_fact_sensor_reading;
GO


/* ============================================================
   Fact View: Sensor Reading
   Grain: one row per valid sensor reading
   ============================================================ */

CREATE VIEW dbo.vw_fact_sensor_reading AS
SELECT
    src.reading_id,
    src.date_key,
    src.time_key,
    src.mote_id,
    src.event_timestamp,
    src.epoch,
    src.temperature_celsius,
    src.humidity_percent,
    src.light,
    src.voltage,
    TRY_CONVERT(DATE, src.ingest_date) AS ingest_date
FROM OPENROWSET(
    BULK 'gold/fact_sensor_reading/',
    DATA_SOURCE = 'iot_berkeley_lake',
    FORMAT = 'DELTA'
)
WITH (
    reading_id VARCHAR(64),
    date_key INT,
    time_key INT,
    mote_id INT,
    event_timestamp DATETIME2,
    epoch BIGINT,
    temperature_celsius FLOAT,
    humidity_percent FLOAT,
    light FLOAT,
    voltage FLOAT,
    ingest_date VARCHAR(20)
) AS src;
GO


/* ============================================================
   Fact View: Sensor Hourly
   Grain: one row per date and hour
   ============================================================ */

CREATE VIEW dbo.vw_fact_sensor_hourly AS
SELECT
    src.date_key,
    src.event_hour,
    src.reading_count,
    src.active_sensor_count,
    src.avg_temperature_celsius,
    src.min_temperature_celsius,
    src.max_temperature_celsius,
    src.avg_humidity_percent,
    src.avg_light,
    src.avg_voltage,
    src.min_voltage,
    src.low_voltage_reading_count,
    TRY_CONVERT(DATE, src.ingest_date) AS ingest_date
FROM OPENROWSET(
    BULK 'gold/fact_sensor_hourly/',
    DATA_SOURCE = 'iot_berkeley_lake',
    FORMAT = 'DELTA'
)
WITH (
    date_key INT,
    event_hour INT,
    reading_count BIGINT,
    active_sensor_count BIGINT,
    avg_temperature_celsius FLOAT,
    min_temperature_celsius FLOAT,
    max_temperature_celsius FLOAT,
    avg_humidity_percent FLOAT,
    avg_light FLOAT,
    avg_voltage FLOAT,
    min_voltage FLOAT,
    low_voltage_reading_count BIGINT,
    ingest_date VARCHAR(20)
) AS src;
GO


/* ============================================================
   Fact View: Sensor Daily
   Grain: one row per date
   ============================================================ */

CREATE VIEW dbo.vw_fact_sensor_daily AS
SELECT
    src.date_key,
    src.reading_count,
    src.active_sensor_count,
    src.avg_temperature_celsius,
    src.min_temperature_celsius,
    src.max_temperature_celsius,
    src.avg_humidity_percent,
    src.avg_light,
    src.avg_voltage,
    src.min_voltage,
    src.low_voltage_reading_count,
    TRY_CONVERT(DATE, src.ingest_date) AS ingest_date
FROM OPENROWSET(
    BULK 'gold/fact_sensor_daily/',
    DATA_SOURCE = 'iot_berkeley_lake',
    FORMAT = 'DELTA'
)
WITH (
    date_key INT,
    reading_count BIGINT,
    active_sensor_count BIGINT,
    avg_temperature_celsius FLOAT,
    min_temperature_celsius FLOAT,
    max_temperature_celsius FLOAT,
    avg_humidity_percent FLOAT,
    avg_light FLOAT,
    avg_voltage FLOAT,
    min_voltage FLOAT,
    low_voltage_reading_count BIGINT,
    ingest_date VARCHAR(20)
) AS src;
GO


/* ============================================================
   Fact View: Sensor Health
   Grain: one row per sensor and date
   ============================================================ */

CREATE VIEW dbo.vw_fact_sensor_health AS
SELECT
    src.date_key,
    src.mote_id,
    src.reading_count,
    src.first_reading_timestamp,
    src.last_reading_timestamp,
    src.avg_voltage,
    src.min_voltage,
    src.low_voltage_reading_count,
    src.low_voltage_detected,
    TRY_CONVERT(DATE, src.ingest_date) AS ingest_date
FROM OPENROWSET(
    BULK 'gold/fact_sensor_health/',
    DATA_SOURCE = 'iot_berkeley_lake',
    FORMAT = 'DELTA'
)
WITH (
    date_key INT,
    mote_id INT,
    reading_count BIGINT,
    first_reading_timestamp DATETIME2,
    last_reading_timestamp DATETIME2,
    avg_voltage FLOAT,
    min_voltage FLOAT,
    low_voltage_reading_count BIGINT,
    low_voltage_detected BIT,
    ingest_date VARCHAR(20)
) AS src;
GO