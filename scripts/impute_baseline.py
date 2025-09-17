"""
Baseline imputation: fill missing lusage within (year, month, mozip) median;
fallback to zipcode-month median; fallback to global median.
Writes to energy_analytics.imputed_baseline and appends to curated_usage.
Requires: google-cloud-bigquery, pandas, pandas-gbq (or use DataFrame -> temp table)
"""
import argparse, json
import pandas as pd
from google.cloud import bigquery

def read_staging_df(bq, project):
    sql = f"SELECT * FROM `{project}.energy_analytics.stg_usage`"
    return bq.query(sql).to_dataframe(create_bqstorage_client=True)

def compute_baseline(df: pd.DataFrame):
    out = df.copy()
    # Track imputation flags
    out['was_imputed'] = False
    out['impute_strategy'] = None

    # Group medians
    med1 = out.groupby(['year','month','mozip'])['lusage'].median()
    med2 = out.groupby(['zipcode','month'])['lusage'].median()
    global_med = out['lusage'].median()

    def fill_row(row):
        if pd.notnull(row['lusage']):
            return row['lusage'], row['was_imputed'], row['impute_strategy']
        # Try group medians
        key1 = (row['year'], row['month'], row['mozip'])
        val = med1.get(key1, None)
        if pd.isnull(val):
            key2 = (row['zipcode'], row['month'])
            val = med2.get(key2, None)
        if pd.isnull(val):
            val = global_med
        return val, True, 'median_by_mozip_zipcode_month'

    out[['lusage','was_imputed','impute_strategy']] = out.apply(lambda r: pd.Series(fill_row(r)), axis=1)
    return out

def write_to_bq(bq, project, df):
    table_id = f"{project}.energy_analytics.imputed_baseline"
    job = bq.load_table_from_dataframe(df, table_id, job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE"))
    job.result()
    # Append to curated
    curated_cols = [c.name for c in bq.get_table(f"{project}.energy_analytics.curated_usage").schema]
    job2 = bq.load_table_from_dataframe(df[curated_cols], f"{project}.energy_analytics.curated_usage", job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND"))
    job2.result()

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--project", required=True)
    args = ap.parse_args()
    bq = bigquery.Client(project=args.project)
    df = read_staging_df(bq, args.project)
    out = compute_baseline(df)
    write_to_bq(bq, args.project, out)
    print("Baseline imputation complete.")