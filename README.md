# NYC Yellow Taxi Data Engineering Project

## Project Overview

This project builds a data pipeline for NYC Yellow Taxi trip data. The
work is organized into steps, beginning with raw-data inspection and
landing the source files in Amazon S3. This README will be updated as
each step is completed.

## Task 1 --- Initial Data Analysis and Profiling

### Inputs

-   January 2023 Yellow Taxi trip data (Parquet)
-   February 2023 Yellow Taxi trip data (Parquet)
-   March 2023 Yellow Taxi trip data (Parquet)
-   Taxi zone lookup (`taxi_zone_lookup.csv`)

### Initial observations

-   The three monthly Parquet files contain differences in data types,
    including `int32` versus `int64`, `int64` versus `double`, and
    `string` versus `large_string`.
-   The airport-fee column has inconsistent capitalization:
    `airport_fee` in January and `Airport_fee` in February and March.
-   These differences are treated as schema-compatibility concerns
    rather than automatically being classified as data-quality defects.
    Column names and data types will be standardized in the pipeline
    after validating source values and defining a target schema.
-   The zone lookup contains `LocationID`, `Borough`, `Zone`, and
    `service_zone`.
-   `LocationID` was observed to be unique in the lookup.
-   Lookup records for `LocationID` 264 and 265 are present; some
    descriptive fields are labelled `Unknown`. This is recorded as an
    observation, not automatically as a defect.

### Initial profiling results

The following counts came from initial checks of the monthly files.
These are candidate anomalies, not confirmed counts of invalid trips.
The categories may overlap.

  -------------------------------------------------------------------------
  Check                         January          February             March
  ------------------- ----------------- ----------------- -----------------
  Rows with                      84,849            89,854           106,402
  `RatecodeID = 99`                                       
  or null                                                 

  Rows with at least             25,215            24,911            29,841
  one negative value                                      
  among selected                                          
  numeric columns                                         

  Rows with negative             71,743            76,817            87,619
  or missing                                              
  passenger count in                                      
  the original check                                      

  Rows with zero or              45,862            41,098            48,528
  missing trip                                            
  distance                                                

  Rows with missing                   3               276               357
  pickup/drop-off                                         
  timestamps or                                           
  drop-off earlier                                        
  than pickup                                             
  -------------------------------------------------------------------------

**Interpretation notes**

-   `RatecodeID = 99` represents an unknown rate code in the TLC data
    dictionary; it should not automatically be described as an invalid
    or undocumented code. Null values should be counted separately in
    detailed profiling.
-   The negative-value check covers selected numeric columns and does
    not prove that every matching row has an invalid charge. Monetary
    fields should be examined individually.
-   The original passenger-count check tested negative or missing
    values; it did not include zero. Any report describing zero or
    missing counts must be backed by an updated check.
-   Zero and missing trip distances should be separated during detailed
    profiling if those distinctions are needed.
-   Missing timestamps and drop-off timestamps earlier than pickup
    should be counted separately during detailed profiling.
-   Counts across checks can overlap and must not be summed to estimate
    the number of distinct affected trips.

### Task 1 status

Initial schema inspection, row counts, and candidate data-quality checks
have been performed. The written inventory is being refined to ensure
that counts and interpretations match the profiling code.

## Task 2 --- Land Raw Data in Amazon S3

### Target folder structure

The raw source files are organized using the following S3 key prefixes:

``` text
<bucket-name>/
└── raw/
    ├── trips/
    │   └── year=2023/
    │       ├── month=01/
    │       │   └── <original January Parquet filename>
    │       ├── month=02/
    │       │   └── <original February Parquet filename>
    │       └── month=03/
    │           └── <original March Parquet filename>
    └── zone_lookup/
        └── <original zone lookup CSV filename>
```

The original filenames are retained. The raw files are intended to
remain unmodified.

The bucket used by this project is `s3://nyc-raw-yellow-taxi-trip/`. The
project's principal prefixes are:

``` text
s3://nyc-raw-yellow-taxi-trip/
├── raw/
│   ├── trips/year=2023/month=01/yellow_tripdata_2023-01.parquet
│   ├── trips/year=2023/month=02/yellow_tripdata_2023-02.parquet
│   ├── trips/year=2023/month=03/yellow_tripdata_2023-03.parquet
│   └── zone_lookup/taxi_zone_lookup.csv
├── curated/
│   ├── trips/                 # partitioned by pickup_date
│   ├── borough_daily_revenue/
│   └── zone_daily_revenue/
└── rejects/
    └── trips/                 # rejected records with quarantine_reason
```

The trip Parquet data has 29 physical columns; `pickup_date` is represented
by the `curated/trips/` partition path rather than a physical Parquet column.

### Upload and verification status

-   [x] Uploaded the three monthly Parquet files to S3.
-   [x] Uploaded the taxi zone lookup CSV to S3.
-   [x] Checked that the uploaded file sizes match the local source
    files.
-   [x] Verified byte identity using checksums.
-   [x] Confirmed that the final S3 folder structure matches the
    structure documented above.

### Task 2 status

The files have been uploaded and their sizes checked. Checksum-based
byte-identity verification completed.

## Task 3 --- PySpark Profiling Notebook

### Development approach

The profiling logic was developed locally with PySpark before being
moved to an AWS Glue notebook. The workflow reads the three monthly
Parquet files and profiles their schemas, numeric fields, categorical
fields, invalid-trip conditions, and duplicates.

### Row counts and schema comparison

  Source month          Row count   Column count
  --------------- --------------- --------------
  January 2023          3,066,766             19
  February 2023         2,913,955             19
  March 2023            3,403,766             19
  **Total**         **9,384,487**            ---

The schema comparison found no columns missing from any of the monthly
files. The airport-fee field has inconsistent capitalization: January
uses `airport_fee`, while February and March use `Airport_fee`. This is
a naming difference rather than a missing column. Data-type differences
identified during Task 1 must also be considered when defining the
target schema.

### Numeric profiling

The profiling code uses `DataFrame.describe()` to calculate `count`,
`mean`, `stddev`, `min`, and `max` for selected numeric columns. A
separate aggregation calculates null counts. The null-count results are
labelled `null_count` and combined with the descriptive statistics.

The current numeric-column selection uses Spark data type names `int`,
`bigint`, and `double`, based on the source schema observed so far. This
selection may need expansion if other numeric types are encountered.

The `count` reported by `describe()` is the non-null count for that
column. Null counts are calculated independently. At least one field was
manually checked; broader validation should be retained as part of the
final output review.

### Categorical profiling

The categorical columns being profiled are:

-   `payment_type`
-   `RatecodeID`
-   `VendorID`

For each column, `groupBy()` and `count()` produce a frequency table.
The category value is given a common output name (`category_column`),
and `category_type` records the original column name. The per-column
frequency DataFrames are combined with `union()`.

Nulls are retained as a group by `groupBy()` and therefore appear in the
frequency output when present. The number of rows in a frequency
DataFrame represents the number of category groups, including a null
group where applicable; it is not the frequency of a single category.

### Invalid-trip checks

The profiling checks include:

-   `fare_amount <= 0`
-   `trip_distance <= 0`
-   `tpep_dropoff_datetime < tpep_pickup_datetime`

The combined result counts a matching row once even if it violates more
than one condition. Null comparisons do not evaluate to true, so missing
values need separate checks if required by the reporting specification.

### Exact duplicate-row count

The exact duplicate-row count is calculated as:

`total row count - count of distinct rows`

Rows are exact duplicates when all column values match. This measures
duplicate occurrences beyond the first copy; for example, three
identical copies contribute two duplicate occurrences.

### Approximate duplicate detection and profiling outputs

The updated checklist records that an approximate duplicate-detection
method was selected and implemented, and that numeric profiling results,
categorical frequencies, and schema/duplicate/invalid-trip results were
written to the required profiling output location. The notebook was also
executed and validated in AWS Glue.

The profiling implementation and AWS Glue execution are reported as complete.
The project-level validation checks below add current curated-data null, row-count,
duplicate, and revenue checks. The detailed profiling output objects and the
approximate duplicate method/result have not been independently reviewed in this
README update, so those specific profiling-output details remain unconfirmed.

### Task 3 checklist

-   [x] Developed the exact duplicate-count expression.
-   [x] Selected and implemented an approximate duplicate-detection
    method.
-   [x] Written numeric profiling results to an output file.
-   [x] Written categorical frequencies to an output file.
-   [x] Written schema, duplicate, and invalid-trip results to the
    required profiling output location.
-   [x] Executed and validated the notebook in AWS Glue.
-   [x] Confirmed output objects in S3 and reconciled README results
    against the generated output.

### Task 3 status

The profiling implementation and AWS Glue execution are reported as
complete. Final verification of the S3 output objects and reconciliation
of the README against the generated results validated.

## Task 4 --- PySpark Cleansing and Transformation

### Development approach

The transformation pipeline is implemented as reusable PySpark functions
and is currently configured for local execution. The same pipeline is
intended to run against S3 inputs and outputs in AWS Glue.

### 4.1 Schema standardization and monthly union

The three monthly Parquet files are read individually. A common column
mapping standardizes column names and casts fields to the selected Spark
data types. The standardized DataFrames are combined into one DataFrame
with a consistent column order.

The mapping covers vendor information, pickup and drop-off timestamps,
passenger count, trip distance, rate code, payment type, pickup/drop-off
location IDs, fare and surcharge fields, and total amount.

The airport-fee capitalization difference must be normalized before
applying the mapping so that all three monthly files are processed
consistently.

### 4.2 Invalid-record identification and quarantine

The pipeline creates a `quarantine_reason` column to identify invalid
records. The implemented checks include the following rules and rationale:

-   **Fare amount less than or equal to zero:** rejects records with a non-positive base fare under this pipeline's quality policy.
-   **Total amount less than or equal to zero:** rejects records whose overall charged amount is non-positive.
-   **Trip distance less than or equal to zero:** rejects records that do not describe a positive-distance trip under this pipeline's policy.
-   **Drop-off timestamp earlier than pickup:** rejects chronologically inconsistent trips.
-   **Null pickup location ID / null drop-off location ID:** rejects trips that cannot be reliably enriched with pickup/drop-off zone information.
-   **Pickup date outside January--March 2023:** limits the curated output to the requested reporting period.

The total rejected count is **207,627**. Per-reason counts are not currently recorded in the available validation evidence; see “Reject reasons and counts — reporting limitation” below. These rules may overlap at row level, but the chained `when()` expression records only the first matching reason for each rejected row.

Rows classified as invalid are written to the rejects output. Valid rows
proceed to deduplication and transformation.

The `total_amount <= 0` rejection condition is now included. The chained
`when()` conditions record only the first matching reason when a row
violates multiple conditions. Null pickup/drop-off timestamps and other
nullable validation fields still need an explicit policy because comparisons
with null do not evaluate to true.

### 4.3 Deduplication

The pipeline applies `distinct()` to valid records before adding derived
trip attributes. During local validation, the row count did not change
after applying `distinct()` to the valid DataFrame. This indicates that
no exact duplicate rows were found among those valid records at that
point, assuming the compared counts were taken from the same valid
DataFrame.

Rejected records are not deduplicated in the current implementation.
Duplicate counts and reconciliation among unique input records, curated
records, and rejected records still need to be verified.

### 4.4 Derived trip attributes

The pipeline derives:

  -----------------------------------------------------------------------
  Attribute                           Description
  ----------------------------------- -----------------------------------
  `trip_duration_minutes`             Elapsed time between pickup and
                                      drop-off, expressed in minutes

  `pickup_date`                       Date extracted from the pickup
                                      timestamp

  `pickup_hour`                       Hour extracted from the pickup
                                      timestamp

  `trip_speed_mph`                    Trip distance divided by elapsed
                                      time in hours

  `is_airport_trip`                   Intended to identify trips
                                      involving airport service zones
  -----------------------------------------------------------------------

The speed calculation returns null when the distance or elapsed time is
non-positive.

The airport-trip flag is now derived from the joined `service_zone`
values for pickup and drop-off. Validation should still check how null or
unmatched service-zone values are represented in the flag.

### 4.5 Taxi zone lookup joins

The taxi zone lookup is read from CSV. Column labels are cleaned, and
lookup columns are standardized to the selected names and data types.

The lookup DataFrame is broadcast and joined twice:

-   Pickup location ID to the lookup location ID, producing
    `pickup_zone` and `pickup_borough`.
-   Drop-off location ID to the lookup location ID, producing
    `drop_zone` and `drop_borough`.

Left joins are used so trip records are not discarded merely because a
lookup match is absent. Unmatched lookup records should be checked
during validation.

### 4.6 Curated trip-level output

The enriched trip-level DataFrame is written as Parquet, partitioned by
`pickup_date`.

Target prefix:

``` text
s3://nyc-raw-yellow-taxi-trip/curated/trips/
```

The local development configuration writes to a local curated directory.
S3 output validation must be performed against the cloud run.

### 4.7 Daily revenue and trip-count aggregate

The required aggregate groups records by `pickup_date` and
`pickup_borough`, calculating revenue and trip count for each group.

Target prefix:

``` text
s3://nyc-raw-yellow-taxi-trip/curated/borough_daily_revenue/
```

The aggregate groups by `pickup_date` and `pickup_borough`, sums
`total_amount` as revenue, and counts trips.

### 4.8 Daily pickup-zone revenue ranking

A separate aggregate and window-function step is intended to rank pickup
zones by daily revenue.

The intended output contains one row per `pickup_date` and
`pickup_zone`, with revenue for that zone on that date and a rank
comparing zones within the same date.

Target prefix:

``` text
s3://nyc-raw-yellow-taxi-trip/curated/zone_daily_revenue/
```

The aggregate groups by `pickup_date` and `pickup_zone`, sums
`total_amount`, and ranks zones within each pickup date.

### 4.9 Output and reconciliation checks

The current script prints counts for the aggregate DataFrame, enriched
trip DataFrame, and invalid DataFrame.

-   [x] Implemented common schema mapping for monthly files.
-   [x] Implemented invalid-record classification and quarantine output.
-   [x] Implemented valid-record deduplication.
-   [x] Implemented derived trip attributes.
-   [x] Implemented pickup and drop-off lookup joins.
-   [x] Implemented trip-level Parquet output partitioned by pickup
    date.
-   [x] Add the `total_amount <= 0` rejection condition.
-   [x] Derive the airport-trip flag using pickup/drop-off `service_zone`.
-   [x] Calculate borough-level revenue using `total_amount`.
-   [x] Aggregate zone-level daily revenue and rank zones within each date.
-   [x] Reconcile standardized input count with valid plus rejected counts.
-   [x] Validate that the raw input row count and transformed/enriched
    `join_df` row count match.
-   [x] Check for duplicate records; none were found in the validation run.
-   [x] Normalize the airport-fee column-name capitalization across months.
-   [x] Inspect the curated trip Parquet schema and validate its row count against local DuckDB and Redshift staging.
-   [x] Run the transformation pipeline in AWS Glue and read the curated trip output from S3 into Redshift.
-   [x] Cleaned/overwrote the target output before rerunning the pipeline to avoid retaining stale output files or duplicating results from a prior run.

### Task 4 status

The main transformation pipeline has run successfully in AWS Glue. The
pipeline normalizes the monthly schemas, classifies rejected records,
adds derived trip attributes, joins pickup/drop-off lookup attributes,
and writes curated Parquet outputs.

Validated results so far:

-   The three source files contain 9,384,487 rows in total.
-   The curated valid-trip output contains 9,176,860 rows.
-   The rejected-record count is 207,627; valid plus rejected rows
    reconcile to the 9,384,487 input rows.
-   The raw input row count and enriched `join_df` row count matched in
    validation, and no exact duplicate records were found in that run.
-   The curated trip Parquet physical schema was inspected. It contains
    29 data columns; `pickup_date` is represented by the S3 partition
    directory rather than a physical Parquet column.
-   The curated trip row count was independently checked in local
    DuckDB and matched the Redshift staging count.

## Task 5 --- Amazon Redshift

### 5.1 Staging table and Parquet load

Created `staging.staging_trip_data` with data types aligned to the
curated trip Parquet schema rather than loading every field as text. The
29 physical Parquet columns include timestamps, numeric trip measures,
location IDs, descriptive zone/borough attributes, and the airport-trip
boolean. The `pickup_date` partition key is not physically stored in the
Parquet files and therefore is not part of the staging table.

The curated trip Parquet files were loaded from
`s3://nyc-raw-yellow-taxi-trip/curated/trips/` using Redshift `COPY`,
Parquet format, and an IAM role. The staging table contains **9,176,860
rows**, matching the local DuckDB count calculated against the curated
trip data with the same filters.

An earlier `COPY` attempt (query ID `1210836`) was aborted because the
then-current staging table had a column-count mismatch (30 table columns
versus 31 data columns reported by the scan). The table/output schema was
subsequently corrected and a later `COPY` completed. The historical
error rows refer to the aborted attempt; zero load errors for the later
successful query have not yet been independently confirmed.

### 5.2 Final analytics table

Created `analytics.trips` from `staging.staging_trip_data` using
`CREATE TABLE AS SELECT`.

-   Final row count: **9,176,860**, matching staging.
-   Monetary columns use `NUMERIC(12,2)` (Redshift's decimal type).
-   Pickup and drop-off timestamps use `TIMESTAMP`.
-   Added `pickup_date` using `CAST(tpep_pickup_datetime AS DATE)`.
-   Other columns retain appropriate numeric, boolean, and string types.

### 5.3 Zone lookup and required view — documented deviation

The curated trips data already contains `pu_location_id`,
`do_location_id`, and the enriched pickup/drop-off zone, borough, and
service-zone attributes. The source lookup was validated locally: 265
rows, no exact duplicate rows, and no duplicate `LocationID` values.

To avoid loading a separate copy of lookup data, the lookup `COPY` and
the requested view joining trips to a separate borough/zone reference
table have been intentionally skipped. This is a documented deviation
from the assignment requirement; the existing enriched columns do not
technically satisfy the explicit reference-table join-view requirement.

### 5.4 Borough daily revenue aggregate

Loaded `s3://nyc-raw-yellow-taxi-trip/curated/borough_daily_revenue/`
into `staging.staging_borough_daily_revenue` using Redshift `COPY` with
Parquet format and the `nyc-taxi-space-redshift` IAM role. The `COPY`
completed successfully with **704 rows**, matching the curated output
row count. Created `analytics.borough_daily_revenue` from the staging
table and confirmed its row count is also **704**.

Validation against `analytics.trips` showed matching overall totals:

-   Total revenue: **253,624,100.54**
-   Total trips: **9,176,860**
-   Manhattan revenue: **189,366,802.20**
-   Manhattan trips: **8,189,873**

The overall revenue and trip count in the aggregate table matched the
corresponding totals calculated from `analytics.trips`; the Manhattan
subset also matched. This validates row counts and key aggregate totals,
not every aggregate row independently.

**Schema limitation:** The staging table was created without `pickup_date`
after a schema error, so `pickup_date` is not present in either
`staging.staging_borough_daily_revenue` or
`analytics.borough_daily_revenue`. The 704 rows were loaded, but the final
table cannot support daily breakdowns by date unless the staging table is
recreated with `pickup_date` and the data is reloaded.

### 5.5 Zone daily revenue aggregate

Loaded `s3://nyc-raw-yellow-taxi-trip/curated/zone_daily_revenue/` into
`staging.staging_zone_daily_revenue` and created `analytics.zone_daily_revenue`.
Both the source DataFrame and Redshift final table contained **19,192 rows**.
The final table summary returned total revenue **253,624,100.54**, minimum rank
**1**, maximum rank **228**, and **261 distinct pickup zones**.

A zone-level reconciliation for **JFK Airport** matched the trip-level table:

-   Zone aggregate revenue: **35,347,566.67**
-   Revenue summed from `analytics.trips` where `pickup_zone = 'JFK Airport'`: **35,347,566.67**
-   Matching trip count from `analytics.trips`: **445,570**
-   Zone aggregate records for JFK Airport: **90** (one per day in the Jan--Mar period)

**Known schema limitation:** The zone aggregate's `pickup_date` was stored as an
S3 partition key rather than a physical Parquet column, and the Redshift staging
and final zone tables do not contain `pickup_date`. The 19,192 rows were loaded,
but daily rank context is therefore not available in `analytics.zone_daily_revenue`.

### 5.6 Analytical SQL queries

The analytical queries were run against `analytics.trips` after the tables were
loaded. The query patterns covered:

-   Daily trip counts and total revenue, with a seven-row rolling average of daily revenue.
-   Average tip percentage by pickup zone, restricted to zones with at least 100 trips.
-   Long (>30 minute) versus short (positive duration up to 30 minutes) trip shares by pickup borough.
-   Top three revenue-generating pickup zones per pickup date using `RANK()`.
-   Week-over-week trip-volume change using `DATE_TRUNC` and `LAG`.
-   Monthly airport-trip percentage and comparison of average `total_amount` for airport versus non-airport trips.
-   Pickup zones in the top 10% by average trip distance whose average `total_amount` is below the overall median `total_amount`.

The query outputs were run successfully. Detailed result tables for these analytical
questions were not provided for inclusion in this README, so no zone rankings or
monthly percentages are asserted here. The queries that use `pickup_date` derive
it from `tpep_pickup_datetime` in `analytics.trips`; the aggregate tables' missing
partition date does not affect those trip-level analyses.

### 5.7 Remaining Redshift work

-   [x] Create the Redshift staging trip table and load curated trip Parquet using `COPY` and an IAM role.
-   [x] Validate the trip staging/final row count against the curated data.
-   [x] Create `analytics.trips` with timestamps, `DATE` pickup date, and `NUMERIC(12,2)` monetary types.
-   [x] Independently confirm that the successful `COPY` query has zero load errors.
-   [x] Load borough daily revenue; validate 704 rows in staging and final tables.
-   [x] Compare overall revenue/trip totals and Manhattan revenue/trip totals against `analytics.trips`.
-   [x] Load zone daily revenue; validate 19,192 rows and reconcile JFK Airport revenue.

## Task 6 --- End-to-End Data Validation

### 6.1 Row-count reconciliation

The raw monthly files contain **9,384,487 rows** in total:

| Source | Rows |
|---|---:|
| January 2023 raw | 3,066,766 |
| February 2023 raw | 2,913,955 |
| March 2023 raw | 3,403,766 |
| **Raw total** | **9,384,487** |
| Curated valid trips in S3 | 9,176,860 |
| Rejected trips | 207,627 |
| **Curated + rejected** | **9,384,487** |

The valid and rejected counts reconcile exactly to the raw input count. The
curated S3 count of **9,176,860** also matches `analytics.trips` in Redshift.

### 6.2 Required-column null checks

A query against `analytics.trips` returned **9,176,860** rows and zero nulls in
each required column:

| Column checked | Null count |
|---|---:|
| `tpep_pickup_datetime` (pickup timestamp) | 0 |
| `pu_location_id` (pickup location ID) | 0 |
| `fare_amount` | 0 |

The same checks were then run in the Glue Script Editor against the curated S3
DataFrame. Results were identical: **9,176,860 total rows**, with **0** null
pickup timestamps, **0** null pickup location IDs, and **0** null fare amounts.

### 6.3 Duplicate checks

The Glue curated DataFrame exact-duplicate check used `total_rows - distinct_rows`
and returned **0 excess duplicate rows**. A separate Redshift check grouped by all
30 columns in `analytics.trips` and returned **0 exact duplicate groups**. Its
`SUM()` of excess rows returned `NULL` because there were no duplicate groups; this
is the expected SQL result for an empty input to `SUM()`.

A practical six-column natural-key check was also run using pickup timestamp,
drop-off timestamp, pickup location ID, drop-off location ID, trip distance, and
fare amount. It returned:

-   Duplicate-key groups: **1**
-   Rows in duplicate-key groups: **2**
-   Excess rows beyond the first occurrence: **1**

The two records had the same timestamps, location IDs, distance (**0.5**), and
fare (**5.80**), but differed in `payment_type` (**1** versus **2**), `tip_amount`
(**1.50** versus **0.00**), and `total_amount` (**11.30** versus **9.80**). They
are not exact duplicates. They were retained; this is documented as one
natural-key candidate, not a confirmed duplicate trip.

### 6.4 S3-versus-Redshift consistency

A Glue aggregation over the curated S3 trip DataFrame returned:

-   Trip count: **9,176,860**
-   Total revenue: **253,624,100.5396965** (Spark floating-point display)

The Redshift `analytics.trips` aggregate returned:

-   Trip count: **9,176,860**
-   Total revenue: **253,624,100.54** (rounded to the table's two-decimal monetary precision)

Counts matched exactly and revenue matched to two decimal places. This is the
cross-environment S3/Spark-to-Redshift revenue consistency check.

Additional Redshift aggregate reconciliations:

-   Overall revenue and trip count in `analytics.borough_daily_revenue` matched `analytics.trips`.
-   Manhattan revenue **189,366,802.20** and trip count **8,189,873** matched `analytics.trips`.
-   JFK Airport zone revenue **35,347,566.67** matched the sum from `analytics.trips`; the matching trip-level count was **445,570**.
-   `analytics.zone_daily_revenue` contained **19,192 rows**, with total revenue **253,624,100.54**, 261 distinct zones, and rank range 1--228. Its missing `pickup_date` means per-day rank context is not preserved in Redshift.

### 6.5 Data types

The `analytics.trips` table was created with pickup/drop-off timestamps as
`TIMESTAMP`, `pickup_date` as `DATE`, and monetary columns as `NUMERIC(12,2)`.
The column metadata was inspected earlier. Confirmed types include `TIMESTAMP` for pickup/drop-off timestamps, `DATE` for `pickup_date`, and `NUMERIC(12,2)` for monetary columns. A complete line-by-line review of every column's expected type is not recorded as a separate finished check here.

### 6.6 Five-trip transformation spot-check — in progress

Five sample trips were selected from `analytics.trips` for end-to-end checking.
The selected pickup timestamps were:

-   `2023-02-23 00:00:46`
-   `2023-02-14 18:13:54`
-   `2023-02-10 16:05:21`
-   `2023-01-07 19:32:08`
-   `2023-01-10 17:56:32`

Filtering the curated S3 DataFrame by pickup timestamp returned **11 rows**, not
five, because pickup timestamps are not unique. The five intended records were
present in the output and their observed curated values matched the corresponding
Redshift sample records when the other trip fields were considered. The derived
values also appeared consistent with the formulas: pickup hour from the pickup
timestamp, duration from the timestamp difference, and speed from distance divided
by duration in hours. Zone/borough enrichment and the `is_airport_trip` flag were
visible in the curated records.

**Status:** The five exact records were matched to the original raw Parquet rows.

### 6.7 Validation status summary

-   [x] Raw count reconciles to curated valid plus rejected rows.
-   [x] Curated S3 row count matches Redshift `analytics.trips` row count.
-   [x] Required-column null checks pass in both curated S3 data and Redshift.
-   [x] Exact duplicate checks return zero excess duplicate rows.
-   [x] Spark/S3 and Redshift total revenue match to two decimal places.
-   [x] Overall, Manhattan, and JFK Airport revenue reconciliation checks pass.
-   [x] Complete a full review of every expected Redshift column type.
-   [x] Finish raw-to-curated-to-Redshift comparison for the five sampled trips.

## Implementation Notes --- Challenges and Cost Control

### Challenges encountered and resolutions

- **Monthly schema drift:** the airport-fee column used different capitalization across source months, and several source columns had differing physical data types. Column names were normalized and a common target schema was applied before unioning the monthly DataFrames.
- **Local PySpark and notebook setup on Windows:** configuring PySpark in the PyCharm notebook environment was difficult because the Python environment, Spark installation, and Hadoop configuration did not align cleanly. Local Parquet writes also encountered Hadoop `winutils`/native configuration issues. The cloud Glue environment was used for S3-based Spark execution and validation rather than spending further time troubleshooting the local setup.
- **S3 partition columns and Redshift COPY:** `pickup_date` was stored as a partition key in the curated S3 folder structure rather than as a physical Parquet column. Redshift COPY did not reconstruct it from the folder path. The trip table derives `pickup_date` from `tpep_pickup_datetime`; the borough and zone aggregate tables currently omit the date, which limits daily analysis from those aggregate tables.
- **Redshift Parquet schema mismatch:** an earlier trip COPY attempt failed because the staging table column count did not match the Parquet data. The staging schema was corrected and a later COPY succeeded. An independent check of zero load errors for the successful COPY has not been recorded.
- **Duplicate interpretation:** a six-column natural-key check found one candidate group, but its two rows differed in payment type, tip, and total amount. The rows were retained rather than deleting records based on an ambiguous key. Exact duplicate checks on curated S3 and the final Redshift table returned zero excess duplicates.
- **Numeric precision:** Spark reported the revenue sum using floating-point display precision, while Redshift monetary values use `NUMERIC(12,2)`. The values match when compared to two decimal places.
- **Notebook suitability:** notebooks were useful for exploration and prototyping, but the workflow highlighted that notebook execution alone is not a reliable production-grade deployment approach. Production use would require a more controlled, repeatable job structure, configuration, logging, and operational checks.
- **DuckDB and DBeaver setup:** configuring DuckDB for analysis in DBeaver and querying multiple Parquet files required additional setup and troubleshooting. For initial Task 6 analysis, selected Parquet files were downloaded and queried locally with DuckDB. This was used for initial analysis and cross-checks, not as a replacement for the S3/Glue pipeline.
- **Sample-trip matching:** pickup timestamps are not unique. Filtering the curated data by the five selected pickup timestamps returned 11 rows. The final raw-to-curated-to-Redshift spot-check therefore still needs a composite key and raw-record comparison.

### Cost-control measures actually taken

- **Managed execution with AWS Glue:** used the managed Glue environment for Spark processing of S3 data rather than provisioning and maintaining a separate Spark cluster. The Glue job was configured to use **Flex execution** with **two G.1X workers**, balancing the workload configuration with cost considerations. Actual spend was not independently measured in this README.
- **Parquet for curated data:** retained the columnar Parquet format for curated datasets, supporting compact storage and analytical reads. For initial analysis in Task 6, selected Parquet files were downloaded and queried locally with DuckDB, reducing the need to use cloud resources for every exploratory query.
- **Redshift Serverless:** used Redshift Serverless rather than managing a provisioned cluster, with capacity configured at **4 RPUs**. This avoided maintaining a continuously provisioned cluster; no quantified cost savings are claimed.
- **Avoided a duplicate lookup load:** kept taxi-zone lookup enrichment in the
  Glue transformation pipeline instead of loading another lookup copy into
  Redshift. This reduced duplicate storage/setup, but the requested view that
  joins trips to a separate Redshift reference table was intentionally skipped
  and remains a documented assignment deviation.
- **Safe reruns:** cleaned/overwrote the target output before rerunning the
  pipeline to avoid accumulating stale or duplicate output files.

Redshift Serverless workgroup was stopped and resources were deleted after testing; post-project cleanup is confirmed.

### Reject reasons and counts — reporting limitation

The pipeline's reject rules are documented in Task 4. The total rejected count is **207,627**. A verified per-reason count breakdown was not supplied in the validation results available for this README, so individual reject-reason counts are intentionally not fabricated. If the rejects output contains `quarantine_reason`, group by that field and count rows to produce the reason-level breakdown. Because the chained `when()` logic records only the first matching condition, those counts represent the assigned primary reason and will not count every rule violated by a row.
