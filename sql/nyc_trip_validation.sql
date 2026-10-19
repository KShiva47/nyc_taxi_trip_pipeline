-- Null Validation

SELECT
    COUNT(*) AS total_rows,
    SUM(CASE WHEN tpep_pickup_datetime IS NULL THEN 1 ELSE 0 END)
        AS null_pickup_datetime,
    SUM(CASE WHEN pu_location_id IS NULL THEN 1 ELSE 0 END)
        AS null_pu_location_id,
    SUM(CASE WHEN fare_amount IS NULL THEN 1 ELSE 0 END)
        AS null_fare_amount
FROM analytics.trips;
-- total_rows	null_pickup_datetime	null_pu_location_id	null_fare_amount
-- 9176860	0	0	0

-- Duplicate Validation

SELECT COUNT(*) AS exact_duplicate_groups,
       SUM(row_count - 1) AS excess_duplicate_rows
FROM (
    SELECT COUNT(*) AS row_count
    FROM analytics.trips
    GROUP BY
        vendor_id,
        tpep_pickup_datetime,
        tpep_dropoff_datetime,
        pickup_date,
        passenger_count,
        trip_distance,
        rate_code_id,
        store_and_fwd_flag,
        pu_location_id,
        do_location_id,
        payment_type,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        toll_amount,
        improvement_surcharge,
        total_amount,
        congestion_surcharge,
        airport_fee,
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
    HAVING COUNT(*) > 1
) duplicates;

-- Schema validation

SELECT
    column_name,
    data_type,
    character_maximum_length,
    numeric_precision,
    numeric_scale
FROM information_schema.columns
WHERE table_schema = 'analytics'
  AND table_name = 'trips'
ORDER BY ordinal_position;


SELECT
    vendor_id,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    passenger_count,
    trip_distance,
    pu_location_id,
    do_location_id,
    fare_amount,
    total_amount,
    trip_duration_minutes,
    pickup_hour,
    trip_speed_mph,
    pickup_zone,
    pickup_borough,
    drop_zone,
    drop_borough,
    is_airport_trip
FROM analytics.trips
ORDER BY RANDOM()
LIMIT 5;