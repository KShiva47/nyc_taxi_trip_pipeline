# Initial Data Analysis and Data Quality Report

## 1. Overview

An initial analysis was performed on the January, February, and March 2023 Yellow Taxi Parquet files and the taxi zone lookup CSV before implementing the data pipeline. The analysis covered row counts, schemas, data types, and potential data-quality issues.

## 2. Observations

### 2.1 Schema Differences Across Monthly Files

The monthly Parquet files contain differences in data types and column-name capitalization. Observed differences include integer-width variations (`int32` and `int64`), numeric representation differences (`int64` and `double`), string representation differences (`string` and `large_string`), and inconsistent capitalization of the airport fee column (`airport_fee` and `Airport_fee`).

These differences do not necessarily indicate data-quality defects. However, they may cause compatibility issues when combining the monthly datasets and loading the data into the target database.

**Planned resolution:** Standardize column names and data types during ingestion or transformation, after validating the source values and defining an appropriate target schema.

### 2.2 Potential Data-Quality Issues

The following suspicious values were identified during the initial profiling:

| Potential issue                                                              | January | February |   March |
| ---------------------------------------------------------------------------- | ------: | -------: | ------: |
| Rows with `RatecodeID = 99` or null                                          |  84,849 |   89,854 | 106,402 |
| Rows with at least one negative value among the selected numeric columns     |  25,215 |   24,911 |  29,841 |
| Rows with zero or missing passenger count                                    | 122,907 |  124,094 | 145,984 |
| Rows with zero or missing trip distance                                      |  45,862 |   41,098 |  48,528 |
| Rows with missing pickup/drop-off timestamps or drop-off earlier than pickup |       3 |      276 |     357 |

**Finding 1: Unknown or unspecified rate codes**

Records with `RatecodeID = 99` or a null value were identified in all three monthly files. These records require separate investigation of the unknown code and missing values. Code `99` should not automatically be classified as invalid; it represents an unknown rate code in the TLC data dictionary.

**Finding 2: Negative numeric values**

Records containing at least one negative value among the selected numeric columns were identified in all three files. These records require further validation, particularly for monetary fields such as `fare_amount` and `total_amount`. Negative values may be legitimate in certain circumstances, such as adjustments or refunds, and should not automatically be treated as invalid.

**Finding 3: Zero or missing passenger counts**

Records with zero or missing passenger counts were identified across all three months. These values may indicate incomplete or suspicious trip records and should be investigated before deciding whether to retain, correct, or exclude them.

**Finding 4: Zero or missing trip distances**

Records with zero or missing trip distances were identified across all three months. These records require further validation because zero distance may occur in some legitimate scenarios, while missing distances indicate incomplete information.

**Finding 5: Potentially invalid timestamps**

Records with missing pickup or drop-off timestamps, or with drop-off occurring before pickup, were identified. Missing timestamps and chronologically inconsistent timestamps should be evaluated separately during detailed profiling.

**Finding 6: Extremely Large Trip distance**
Maximum Trip Distance recorded were found to be 258928.15 in Jan parquet file

### 2.3 Zone Lookup Observations

The zone lookup CSV contains `LocationID`, `Borough`, `Zone`, and `service_zone`. The `LocationID` field was observed to be unique. Records for location IDs 264 and 265 are present, with some descriptive fields labelled `Unknown`.

The presence of `Unknown` does not, by itself, establish a data-quality defect. Cross-validation of pickup and drop-off location IDs against the lookup has not yet been performed.

## 3. Limitations and Next Steps

This analysis is an initial profiling exercise. The reported counts represent records matching the specified checks, not necessarily confirmed invalid records. Categories may overlap, so their counts must not be added to calculate the total number of affected trips.

Further validation will determine which records require correction, retention, or exclusion. The pipeline will also standardize the schema across monthly files to ensure consistent downstream processing.
