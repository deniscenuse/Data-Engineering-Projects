-- Create dataset
CREATE SCHEMA IF NOT EXISTS `{project_id}.energy_analytics`
OPTIONS(
  location="EU",
  default_table_expiration_days=0
);

-- Raw table (loaded from CSV)
CREATE TABLE IF NOT EXISTS `{project_id}.energy_analytics.raw_usage` (
  hh_id STRING,
  year INT64,
  month INT64,
  zipcode STRING,
  mozip STRING,
  lusage FLOAT64,
  lusage1 FLOAT64,
  lusage2 FLOAT64,
  lusage3 FLOAT64,
  lusage4 FLOAT64,
  lusage5 FLOAT64,
  lusage6 FLOAT64,
  children BOOL,
  hhsize2 BOOL,
  hhsize3 BOOL,
  hhsize4 BOOL,
  hhsize5plus BOOL,
  income2 BOOL,
  income3 BOOL,
  income4 BOOL,
  income5 BOOL,
  income6 BOOL,
  income7 BOOL,
  income8 BOOL,
  income9 BOOL,
  owner BOOL,
  size_sqft INT64
)
PARTITION BY DATE_TRUNC(DATE(year, month, 1), MONTH)
CLUSTER BY zipcode, mozip, hh_id;

-- Staging table with standardization + data quality filters
CREATE OR REPLACE TABLE `{project_id}.energy_analytics.stg_usage` AS
SELECT
  hh_id,
  year,
  month,
  zipcode,
  mozip,
  CAST(lusage AS FLOAT64) AS lusage,
  CAST(lusage1 AS FLOAT64) AS lusage1,
  CAST(lusage2 AS FLOAT64) AS lusage2,
  CAST(lusage3 AS FLOAT64) AS lusage3,
  CAST(lusage4 AS FLOAT64) AS lusage4,
  CAST(lusage5 AS FLOAT64) AS lusage5,
  CAST(lusage6 AS FLOAT64) AS lusage6,
  CAST(children AS BOOL) AS children,
  CAST(hhsize2 AS BOOL) AS hhsize2,
  CAST(hhsize3 AS BOOL) AS hhsize3,
  CAST(hhsize4 AS BOOL) AS hhsize4,
  CAST(hhsize5plus AS BOOL) AS hhsize5plus,
  CAST(income2 AS BOOL) AS income2,
  CAST(income3 AS BOOL) AS income3,
  CAST(income4 AS BOOL) AS income4,
  CAST(income5 AS BOOL) AS income5,
  CAST(income6 AS BOOL) AS income6,
  CAST(income7 AS BOOL) AS income7,
  CAST(income8 AS BOOL) AS income8,
  CAST(income9 AS BOOL) AS income9,
  CAST(owner AS BOOL) AS owner,
  CAST(size_sqft AS INT64) AS size_sqft,
  DATE(year, month, 1) AS period_date,
  CURRENT_TIMESTAMP() AS ingestion_ts
FROM `{project_id}.energy_analytics.raw_usage`
WHERE
  year IN (2010, 2011)
  AND month BETWEEN 4 AND 8
  AND zipcode IS NOT NULL
  AND mozip IS NOT NULL;

-- Curated table shell (data will be inserted by imputation scripts)
CREATE TABLE IF NOT EXISTS `{project_id}.energy_analytics.curated_usage` (
  hh_id STRING NOT NULL,
  year INT64 NOT NULL,
  month INT64 NOT NULL,
  zipcode STRING NOT NULL,
  mozip STRING NOT NULL,
  lusage FLOAT64,
  lusage1 FLOAT64,
  lusage2 FLOAT64,
  lusage3 FLOAT64,
  lusage4 FLOAT64,
  lusage5 FLOAT64,
  lusage6 FLOAT64,
  children BOOL,
  hhsize2 BOOL,
  hhsize3 BOOL,
  hhsize4 BOOL,
  hhsize5plus BOOL,
  income2 BOOL,
  income3 BOOL,
  income4 BOOL,
  income5 BOOL,
  income6 BOOL,
  income7 BOOL,
  income8 BOOL,
  income9 BOOL,
  owner BOOL,
  size_sqft INT64,
  period_date DATE NOT NULL,
  ingestion_ts TIMESTAMP NOT NULL,
  was_imputed BOOL,
  impute_strategy STRING
)
PARTITION BY period_date
CLUSTER BY zipcode, mozip, hh_id;