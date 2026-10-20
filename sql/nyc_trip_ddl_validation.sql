--Staging Trip Table count
select count(*) from staging.staging_trip_data as trp; --9176860

select rate_code_id,count(*) from staging.staging_trip_data as trp group by trp.rate_code_id;

SELECT
    column_name,
    data_type
FROM information_schema.columns
WHERE table_schema = 'staging'
  AND table_name = 'staging_trip_data'
ORDER BY ordinal_position;

SELECT *
FROM sys_load_error_detail
ORDER BY start_time DESC
LIMIT 20;

SELECT
    query_id,
    status,
    start_time
FROM sys_load_history
WHERE query_id >= 1210836
ORDER BY start_time DESC
LIMIT 10;

--Visual validation of Analytics trips
select * from analytics.trips limit 10;

SELECT
    location_id,
    COUNT(*) AS row_count
FROM staging.staging_zone_lookup
GROUP BY location_id
HAVING COUNT(*) > 1;

-- staging.staging_borough_daily_revenue count
select count(*) from staging.staging_borough_daily_revenue; --704

-- Visual validation of staging_borough_daily_revenue
select * from staging.staging_borough_daily_revenue;

-- Visual validation of borough_daily_revenue
select * from analytics.borough_daily_revenue;

--borough_daily_revenue count
select count(*) from analytics.borough_daily_revenue; -- 704

-- borough based validation
with daily_revenue as (SELECT
    CAST(tpep_pickup_datetime AS DATE) AS pickup_date,
    pickup_borough,
    SUM(total_amount) AS revenue,
    COUNT(*) AS trip_count
FROM analytics.trips
WHERE pickup_borough = 'Manhattan'
GROUP BY
    CAST(tpep_pickup_datetime AS DATE),
    pickup_borough
ORDER BY pickup_date) select pickup_borough, sum(revenue) as revenue, sum(trip_count) as trip_count from daily_revenue group by pickup_borough;

SELECT
    SUM(revenue) AS revenue,
    SUM(trip_count) AS trip_count
FROM analytics.borough_daily_revenue
WHERE pickup_borough = 'Manhattan';

-- Manhattan	189366802.2	8189873


SELECT
    SUM(total_amount) AS total_revenue,
    COUNT(*) AS total_trips
FROM analytics.trips;

SELECT
    SUM(revenue) AS total_revenue,
    SUM(trip_count) AS total_trips
FROM analytics.borough_daily_revenue;
-- 253624100.54	9176860


SELECT COUNT(*)
FROM staging.staging_zone_daily_revenue; --19192

SELECT COUNT(*)
FROM analytics.zone_daily_revenue; --19192

SELECT
    COUNT(*) AS row_count,
    SUM(revenue) AS total_revenue,
    MIN(rank) AS min_rank,
    MAX(rank) AS max_rank,
    COUNT(DISTINCT pickup_zone) AS distinct_zones
FROM analytics.zone_daily_revenue;
-- row_count	total_revenue	min_rank	max_rank	distinct_zones
-- 19192	253624100.54	1	228	261

SELECT
    pickup_zone,
    SUM(revenue) AS total_revenue,
    COUNT(*) AS daily_records
FROM analytics.zone_daily_revenue
GROUP BY pickup_zone
ORDER BY total_revenue DESC
LIMIT 10;

SELECT
    SUM(total_amount) AS total_revenue,
    COUNT(*) AS trip_count
FROM analytics.trips
WHERE pickup_zone = 'JFK Airport';

-- total_revenue	trip_count
-- 35347566.67	445570
