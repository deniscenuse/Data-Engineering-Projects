-- 1) Key ranges
SELECT
  SUM(CASE WHEN year NOT IN (2010, 2011) THEN 1 ELSE 0 END) AS bad_years,
  SUM(CASE WHEN month < 4 OR month > 8 THEN 1 ELSE 0 END)   AS bad_months
FROM `{{project_id}}.energy_analytics.stg_usage`;

-- 2) Required identifiers
SELECT COUNT(*) AS missing_ids
FROM `{{project_id}}.energy_analytics.stg_usage`
WHERE hh_id IS NULL OR zipcode IS NULL OR mozip IS NULL;

-- 3) One-hot hygiene for hhsize (baseline category likely size=1 not present)
SELECT COUNT(*) AS bad_hhsize
FROM `{{project_id}}.energy_analytics.stg_usage`
WHERE (COALESCE(hhsize2,0)+COALESCE(hhsize3,0)+COALESCE(hhsize4,0)+COALESCE(hhsize5plus,0)) > 1;

-- 4) One-hot hygiene for income (baseline likely income1 omitted)
SELECT COUNT(*) AS bad_income
FROM `{{project_id}}.energy_analytics.stg_usage`
WHERE (COALESCE(income2,0)+COALESCE(income3,0)+COALESCE(income4,0)+COALESCE(income5,0)+
       COALESCE(income6,0)+COALESCE(income7,0)+COALESCE(income8,0)+COALESCE(income9,0)) > 1;

-- 5) Size sanity
SELECT
  COUNTIF(size_sqft IS NOT NULL AND (size_sqft < 100 OR size_sqft > 10000)) AS size_out_of_range
FROM `{{project_id}}.energy_analytics.stg_usage`;

-- 6) Lusage sanity (log kWh ~ usually between ~3 and 8; flag outliers)
SELECT
  COUNTIF(lusage IS NOT NULL AND (lusage < 2 OR lusage > 9)) AS lusage_outliers
FROM `{{project_id}}.energy_analytics.stg_usage`;

-- 7) Missingness inventory
SELECT
  COUNTIF(lusage IS NULL) AS miss_lusage,
  COUNTIF(size_sqft IS NULL) AS miss_size,
  COUNTIF(children IS NULL) AS miss_children
FROM `{{project_id}}.energy_analytics.stg_usage`;