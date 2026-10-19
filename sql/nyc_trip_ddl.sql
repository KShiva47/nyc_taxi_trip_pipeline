SELECT
    current_database() AS database_name,
    current_user AS username,
    current_schema() AS schema_name;

SELECT version();

CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS analytics;

SELECT schema_name
FROM information_schema.schemata
WHERE schema_name IN ('staging', 'analytics')
ORDER BY schema_name;


CREATE TABLE staging.staging_trip_data (
    vendor_id                 SMALLINT,
    tpep_pickup_datetime      TIMESTAMP,
    tpep_dropoff_datetime     TIMESTAMP,
    passenger_count           SMALLINT,
    trip_distance             DOUBLE PRECISION,
    rate_code_id              SMALLINT,
    store_and_fwd_flag        VARCHAR(3),
    pu_location_id            SMALLINT,
    do_location_id            SMALLINT,
    payment_type              SMALLINT,
    fare_amount               DOUBLE PRECISION,
    extra                     DOUBLE PRECISION,
    mta_tax                   DOUBLE PRECISION,
    tip_amount                DOUBLE PRECISION,
    toll_amount               DOUBLE PRECISION,
    improvement_surcharge     DOUBLE PRECISION,
    total_amount              DOUBLE PRECISION,
    congestion_surcharge      DOUBLE PRECISION,
    airport_fee               DOUBLE PRECISION,
    trip_duration_minutes     DOUBLE PRECISION,
    pickup_hour               INTEGER,
    trip_speed_mph            DOUBLE PRECISION,
    pickup_zone               VARCHAR(256),
    pickup_borough            VARCHAR(64),
    pickup_service_zone       VARCHAR(64),
    drop_zone                 VARCHAR(256),
    drop_borough              VARCHAR(64),
    drop_service_zone         VARCHAR(64),
    is_airport_trip           BOOLEAN
);
-- DROP TABLE staging.staging_trip_data;

COPY staging.staging_trip_data
FROM 's3://nyc-raw-yellow-taxi-trip/curated/trips/'
IAM_ROLE 'arn:aws:iam::635390850465:role/nyc-taxi-space-redshift'
FORMAT AS PARQUET;

CREATE TABLE staging.staging_borough_daily_revenue (
    pickup_borough VARCHAR(64),
    revenue        DOUBLE PRECISION,
    trip_count     BIGINT
);

-- drop TABLE staging.staging_borough_daily_revenue;

COPY staging.staging_borough_daily_revenue
FROM 's3://nyc-raw-yellow-taxi-trip/curated/borough_daily_revenue/'
IAM_ROLE 'arn:aws:iam::635390850465:role/nyc-taxi-space-redshift'
FORMAT AS PARQUET;

CREATE TABLE staging.staging_zone_daily_revenue (
    pickup_zone VARCHAR(256),
    revenue DOUBLE PRECISION,
    rank INTEGER
);

COPY staging.staging_zone_daily_revenue
FROM 's3://nyc-raw-yellow-taxi-trip/curated/zone_daily_revenue/'
IAM_ROLE 'arn:aws:iam::635390850465:role/nyc-taxi-space-redshift'
FORMAT AS PARQUET;

CREATE TABLE analytics.trips AS
SELECT
    vendor_id,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    CAST(tpep_pickup_datetime AS DATE) AS pickup_date,
    passenger_count,
    trip_distance,
    rate_code_id,
    store_and_fwd_flag,
    pu_location_id,
    do_location_id,
    payment_type,
    CAST(fare_amount AS DECIMAL(12,2)) AS fare_amount,
    CAST(extra AS DECIMAL(12,2)) AS extra,
    CAST(mta_tax AS DECIMAL(12,2)) AS mta_tax,
    CAST(tip_amount AS DECIMAL(12,2)) AS tip_amount,
    CAST(toll_amount AS DECIMAL(12,2)) AS toll_amount,
    CAST(improvement_surcharge AS DECIMAL(12,2))
        AS improvement_surcharge,
    CAST(total_amount AS DECIMAL(12,2)) AS total_amount,
    CAST(congestion_surcharge AS DECIMAL(12,2))
        AS congestion_surcharge,
    CAST(airport_fee AS DECIMAL(12,2)) AS airport_fee,
    trip_duration_minutes,
    pickup_hour,
    trip_speed_mph,
    pickup_zone,
    pickup_borough,
    pickup_service_zone,
    drop_zone,
    drop_borough,
    drop_service_zone,
    is_airport_trip
FROM staging.staging_trip_data;


CREATE TABLE analytics.borough_daily_revenue AS
SELECT
    pickup_borough,
    CAST(revenue AS DECIMAL(18,2)) AS revenue,
    trip_count
FROM staging.staging_borough_daily_revenue;


CREATE TABLE analytics.zone_daily_revenue AS
SELECT
    pickup_zone,
    CAST(revenue AS DECIMAL(19,2)) AS revenue,
    rank
FROM staging.staging_zone_daily_revenue;

