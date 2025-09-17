-- Compare pre vs post imputation distributions of lusage
WITH pre AS (
  SELECT 'pre' AS stage, APPROX_QUANTILES(lusage, 100)[OFFSET(50)] AS median, AVG(lusage) AS avg, COUNT(*) AS n,
         COUNTIF(lusage IS NULL) AS miss
  FROM `{{project_id}}.energy_analytics.stg_usage`
),
post AS (
  SELECT 'post' AS stage, APPROX_QUANTILES(lusage, 100)[OFFSET(50)] AS median, AVG(lusage) AS avg, COUNT(*) AS n,
         COUNTIF(was_imputed) AS imputed_n
  FROM `{{project_id}}.energy_analytics.curated_usage`
)
SELECT * FROM pre UNION ALL SELECT * FROM post;

-- Sanity: simple predictive check (BigQuery built-in ML alternative if enabled)
-- Example: correlation-ish insight
SELECT
  month, AVG(lusage) AS avg_lusage, COUNT(*) n
FROM `{{project_id}}.energy_analytics.curated_usage`
GROUP BY month
ORDER BY month;

-- Check that lagged lusage correlates positively with current usage
SELECT
  CORR(lusage, lusage1) AS corr_l1, CORR(lusage, lusage2) AS corr_l2, CORR(lusage, lusage3) AS corr_l3
FROM `{{project_id}}.energy_analytics.curated_usage`;