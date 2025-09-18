-- Count total rows
SELECT COUNT(*) AS n
FROM `{{project_id}}.energy_analytics.stg_usage`;

-- Check missingness
SELECT
  COUNTIF(lusage IS NULL) AS miss_lusage,
  COUNTIF(children IS NULL) AS miss_children,
  COUNTIF(size_sqft IS NULL) AS miss_size
FROM `{{project_id}}.energy_analytics.stg_usage`;

-- Range checks
SELECT
  COUNTIF(month < 4 OR month > 8) AS bad_month,
  COUNTIF(year NOT IN (2010,2011)) AS bad_year
FROM `{{project_id}}.energy_analytics.stg_usage`;

-- hhsize one-hot check
SELECT COUNT(*) AS bad_hhsize
FROM `{{project_id}}.energy_analytics.stg_usage`
WHERE (COALESCE(CAST(hhsize2 AS INT64),0)
     + COALESCE(CAST(hhsize3 AS INT64),0)
     + COALESCE(CAST(hhsize4 AS INT64),0)
     + COALESCE(CAST(hhsize5plus AS INT64),0)) > 1;

-- income one-hot check
SELECT COUNT(*) AS bad_income
FROM `{{project_id}}.energy_analytics.stg_usage`
WHERE (COALESCE(CAST(income2 AS INT64),0)
     + COALESCE(CAST(income3 AS INT64),0)
     + COALESCE(CAST(income4 AS INT64),0)
     + COALESCE(CAST(income5 AS INT64),0)
     + COALESCE(CAST(income6 AS INT64),0)
     + COALESCE(CAST(income7 AS INT64),0)
     + COALESCE(CAST(income8 AS INT64),0)
     + COALESCE(CAST(income9 AS INT64),0)) > 1;

-- Simple descriptive stats
SELECT
  MIN(lusage) AS min_lusage,
  MAX(lusage) AS max_lusage,
  AVG(lusage) AS avg_lusage
FROM `{{project_id}}.energy_analytics.stg_usage`;
