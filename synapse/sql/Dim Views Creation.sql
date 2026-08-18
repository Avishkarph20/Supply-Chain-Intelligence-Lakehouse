USE iot_berkeley_lab_sql;
GO

/* ============================================================
   Drop existing views if they already exist
   ============================================================ */

DROP VIEW IF EXISTS dbo.vw_dim_time;
GO

DROP VIEW IF EXISTS dbo.vw_dim_date;
GO

DROP VIEW IF EXISTS dbo.vw_dim_mote;
GO

/* ============================================================
   Dimension View: Mote
   Grain: one row per sensor/mote
   ============================================================ */

CREATE VIEW dbo.vw_dim_mote AS
SELECT
    mote_id,
    x_position,
    y_position,
    metadata_available
FROM OPENROWSET(
    BULK 'gold/dim_mote/',
    DATA_SOURCE = 'iot_berkeley_lake',
    FORMAT = 'DELTA'
)
WITH (
    mote_id INT,
    x_position FLOAT,
    y_position FLOAT,
    metadata_available BIT
) AS rows;
GO

/* ============================================================
   Dimension View: Date
   Grain: one row per date
   ============================================================ */

CREATE VIEW dbo.vw_dim_date AS
SELECT
    date_key,
    full_date,
    calendar_year,
    calendar_quarter,
    month_number,
    month_name,
    day_of_month,
    day_of_week_number,
    day_name,
    is_weekend
FROM OPENROWSET(
    BULK 'gold/dim_date/',
    DATA_SOURCE = 'iot_berkeley_lake',
    FORMAT = 'DELTA'
)
WITH (
    date_key INT,
    full_date DATE,
    calendar_year INT,
    calendar_quarter INT,
    month_number INT,
    month_name VARCHAR(20),
    day_of_month INT,
    day_of_week_number INT,
    day_name VARCHAR(20),
    is_weekend BIT
) AS rows;
GO

/* ============================================================
   Dimension View: Time
   Grain: one row per hour/minute combination
   ============================================================ */

CREATE VIEW dbo.vw_dim_time AS
SELECT
    time_key,
    event_hour,
    event_minute,
    time_label,
    day_period
FROM OPENROWSET(
    BULK 'gold/dim_time/',
    DATA_SOURCE = 'iot_berkeley_lake',
    FORMAT = 'DELTA'
)
WITH (
    time_key INT,
    event_hour INT,
    event_minute INT,
    time_label VARCHAR(10),
    day_period VARCHAR(20)
) AS rows;
GO
