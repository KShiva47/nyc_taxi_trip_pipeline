-- 1. What is the total revenue and trip count per day, and what is the 7-day rolling average revenue?
WITH trip_date_count AS (
    SELECT
        pickup_date,
        COUNT(*) AS trip_count,
        SUM(total_amount) AS total_revenue
    FROM analytics.trips
    GROUP BY pickup_date
)
SELECT
    pickup_date,
    trip_count,
    total_revenue,
    AVG(total_revenue) OVER (
        ORDER BY pickup_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS rolling_avg
FROM trip_date_count
ORDER BY pickup_date;

--2. Which pickup zone generates the highest average tip percentage (tip_amount / fare_amount),
--among zones with at least 100trips? (aggregation + HAVING + join to zone lookup)
SELECT
    pickup_zone,
    COUNT(*) AS trip_count,
    ROUND(
        AVG(
            (tip_amount / NULLIF(fare_amount, 0)) * 100
        ),
        2
    ) AS tip_percentage
FROM analytics.trips
WHERE fare_amount IS NOT NULL
  AND fare_amount > 0
  AND tip_amount IS NOT NULL
GROUP BY pickup_zone
HAVING COUNT(*) >= 100
ORDER BY tip_percentage DESC
LIMIT 100;

-- 3. For each borough, what share of trips are "long trips" (> 30 minutes) vs "short trips", broken out with CASE? (CASE+aggregation)
WITH pickup_trip_details AS (
    SELECT
        pickup_borough,
        trip_duration_minutes AS duration
    FROM analytics.trips
),
pickup_type_details AS (
    SELECT
        *,
        CASE
            WHEN duration > 30 THEN 'Long'
            WHEN duration > 0 AND duration <= 30 THEN 'Short'
        END AS trip_type
    FROM pickup_trip_details
)
SELECT
    pickup_borough,
    SUM(CASE WHEN trip_type = 'Long' THEN 1 ELSE 0 END)
        AS long_trip_count,
    100.0 * SUM(CASE WHEN trip_type = 'Long' THEN 1 ELSE 0 END)
        / COUNT(*) AS long_trip_share_pct,
    100.0 * SUM(CASE WHEN trip_type = 'Short' THEN 1 ELSE 0 END)
        / COUNT(*) AS short_trip_share_pct
FROM pickup_type_details
WHERE duration > 0
GROUP BY pickup_borough
ORDER BY long_trip_count DESC;

--4.Using a CTE, find the top 3 revenue-generating pickup zones for each day (window RANK() partitioned by date).

WITH taxi_revenue AS (
    SELECT
        pickup_date,
        SUM(total_amount) AS revenue,
        pickup_zone,
        RANK() OVER (
            PARTITION BY pickup_date
            ORDER BY SUM(total_amount) DESC
        ) AS revenue_rank
    FROM analytics.trips
    GROUP BY pickup_date, pickup_zone
)
SELECT *
FROM taxi_revenue
WHERE revenue_rank <= 3;

--5. What is the week-over-week change in trip volume, using DATE_TRUNC/date functions and a self-join or LAG() windowfunction?


WITH trip_week_agg AS (
    SELECT
        COUNT(*) AS trip_count,
        DATE_TRUNC('week', tpep_pickup_datetime) AS trip_week
    FROM analytics.trips
    GROUP BY DATE_TRUNC('week', tpep_pickup_datetime)
),
week_details AS (
    SELECT
        *,
        trip_count - LAG(trip_count) OVER (
            ORDER BY trip_week
        ) AS volume_change_by_week
    FROM trip_week_agg
)
SELECT *
FROM week_details
ORDER BY trip_week;

--7. What percentage of trips each month are flagged is_airport_trip, and how does their average fare compare to non-airport
--trips?

WITH airport_trip_agg AS (
    SELECT
        COUNT(*) AS airport_trip_count,
        AVG(total_amount) AS airport_fare,
        DATE_TRUNC('month', tpep_pickup_datetime) AS airport_trip_month
    FROM analytics.trips
    WHERE is_airport_trip = TRUE
    GROUP BY DATE_TRUNC('month', tpep_pickup_datetime)
),
trip_count_agg AS (
    SELECT
        COUNT(*) AS trip_count,
        DATE_TRUNC('month', tpep_pickup_datetime) AS trip_month
    FROM analytics.trips
    GROUP BY DATE_TRUNC('month', tpep_pickup_datetime)
),
trip_fare_agg AS (
    SELECT
        AVG(total_amount) AS local_avg_trip_fare,
        DATE_TRUNC('month', tpep_pickup_datetime) AS local_trip_month
    FROM analytics.trips
    WHERE is_airport_trip = FALSE
    GROUP BY DATE_TRUNC('month', tpep_pickup_datetime)
)
SELECT
    100.0 * airport_trip_count / trip_count AS airport_trip_percentage,
    trip_month,
    local_avg_trip_fare,
    airport_fare
FROM airport_trip_agg
LEFT JOIN trip_count_agg
    ON trip_month = airport_trip_month
LEFT JOIN trip_fare_agg
    ON trip_fare_agg.local_trip_month = trip_count_agg.trip_month
ORDER BY trip_month;

--7.Find pickup zones where average trip distance is in the top 10% (subquery/percentile)
--but average fare is belowtheoverall median — a potential pricing/efficiency question.
WITH trip_zone_agg AS (
    SELECT
        pickup_zone,
        AVG(trip_distance) AS trip_avg,
        PERCENT_RANK() OVER (ORDER BY AVG(trip_distance)) AS p_rank,
        AVG(total_amount) AS avg_amount
    FROM analytics.trips
    GROUP BY pickup_zone
),
median_amount AS (
    SELECT
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY total_amount) AS median_amount
    FROM analytics.trips
)
SELECT *
FROM trip_zone_agg, median_amount
WHERE p_rank >= 0.90
  AND avg_amount < median_amount;

