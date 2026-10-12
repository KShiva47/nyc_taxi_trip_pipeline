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

  ----------------------------------------------------------------------------
  Check                          January           February              March
  ------------------- ------------------ ------------------ ------------------
  Rows with                       84,849             89,854            106,402
  `RatecodeID = 99`                                         
  or null                                                   

  Rows with at least              25,215             24,911             29,841
  one negative value                                        
  among selected                                            
  numeric columns                                           

  Rows with negative              71,743             76,817             87,619
  or missing                                                
  passenger count in                                        
  the original check                                        

  Rows with zero or               45,862             41,098             48,528
  missing trip                                              
  distance                                                  

  Rows with missing                    3                276                357
  pickup/drop-off                                           
  timestamps or                                             
  drop-off earlier                                          
  than pickup                                               
  ----------------------------------------------------------------------------

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

### Upload and verification status

-   [x] Uploaded the three monthly Parquet files to S3.
-   [x] Uploaded the taxi zone lookup CSV to S3.
-   [x] Checked that the uploaded file sizes match the local source
    files.
-   [ ] Verified byte identity using checksums.
-   [x] Confirmed that the final S3 folder structure matches the
    structure documented above.

**Verification limitation:** Current validation is based on file-size
comparison. Matching sizes do not prove byte-for-byte identity. Checksum
validation (for example, SHA-256 calculated locally and against the
corresponding S3 object) has not been performed.

### Task 2 status

The files have been uploaded and their sizes checked. Checksum-based
byte-identity verification remains outstanding.

## Task 3 --- PySpark Profiling Notebook (In Progress)

### Development approach

The profiling logic is being developed and validated locally with PySpark before it is moved to an AWS Glue notebook. The current code loops through the three monthly Parquet source paths. The paths in the shared code are placeholders and must be replaced with the actual local paths for local runs or S3 paths for Glue execution.

### Row counts and schema comparison

The previously recorded row counts are:

| Source month | Row count | Column count |
|---|---:|---:|
| January 2023 | 3,066,766 | 19 |
| February 2023 | 2,913,955 | 19 |
| March 2023 | 3,403,766 | 19 |

The schema comparison found no columns missing from any of the three monthly files: each has 19 columns. The airport-fee field has a capitalization inconsistency: January uses `airport_fee`, while February and March use `Airport_fee`. This is a schema naming difference, not a missing column. The data-type differences identified during Task 1 should also be considered when defining a consistent target schema.

### Numeric profiling implemented in the local code

The current approach selects numeric columns and uses `DataFrame.describe()` to calculate `count`, `mean`, `stddev`, `min`, and `max`. A separate aggregation counts nulls for each selected numeric column; that result is labelled `null_count` and unioned with the descriptive statistics DataFrame.

For the inspected source schema, the current numeric-column selection uses Spark data type names `int`, `bigint`, and `double`. This matches the types observed in the selected columns so far; it may need expansion if additional numeric types are encountered in other inputs.

The `count` reported by `describe()` is the non-null count for that column. The null count is calculated independently, and one field was manually checked as a validation step. Further validation across the full set of numeric columns remains advisable.

### Categorical profiling implemented in the local code

The categorical columns being profiled are:

- `payment_type`
- `RatecodeID`
- `VendorID`

For each column, `groupBy()` and `count()` produce a frequency table. The category value is given a common output name (`category_column`), and `category_type` records the original column name. The per-column frequency DataFrames are combined with `union()`.

Nulls are retained as a group by `groupBy()` and therefore appear in the frequency output if present. The printed row count of each frequency DataFrame represents the number of category groups, including a null group where applicable; it is not the frequency of any one category. The distinct-group count and frequency output still need to be validated against the assignment's desired reporting format.

### Invalid-trip checks implemented in the local code

The current combined filter selects a row if any of these conditions is true:

- `fare_amount <= 0`
- `trip_distance <= 0`
- `tpep_dropoff_datetime < tpep_pickup_datetime`

The resulting count represents rows matching at least one condition, so a row satisfying multiple conditions is counted once in this combined result. Null comparisons do not evaluate to true and are not included by these predicates; missing values should be reported separately if required. Separate counts for each condition have not been added because the current implementation reports the combined count.

### Exact duplicate-row count implemented in the local code

The exact duplicate-row count is calculated as:

`total row count - count of distinct rows`

Rows are considered exact duplicates when all column values match. This count measures duplicate occurrences beyond the first copy; for example, three copies of the same row contribute two duplicates.

### Approximate duplicate count and output status

- [x] Developed the exact duplicate-count expression.
- [x] Selected and implemented a documented approximate duplicate-detection method.
- [x] Written numeric profiling results to an output file.
- [x] Written categorical frequencies to an output file.
- [x] Written schema, duplicate, and invalid-trip results to the required profiling output location.
- [ ] Executed and validated the notebook in AWS Glue.
- [ ] Confirmed output objects in S3 and reconciled README results against the generated output.

An approximate duplicate method has not yet been selected. No approximate duplicate count is reported here. The current code also does not yet write the profiling DataFrames to CSV or JSON; it calculates or displays results during development.

### Task 3 status

The local PySpark profiling logic has been drafted for numeric statistics and null counts, categorical frequencies, combined invalid-trip checks, and exact duplicate counts. Monthly row counts and the airport-fee naming difference have been manually reviewed. Remaining work includes final validation, selecting and documenting an approximate duplicate method, writing the profiling outputs, and running the notebook in AWS Glue.

