"""
Ingest CSV from GCS into BigQuery with schema + basic validation.
Requires: google-cloud-bigquery, google-cloud-storage, pandas (optional)
Usage:
  python scripts/ingest_to_bq.py --project <PROJECT_ID> --bucket <BUCKET> --object imputation/data_test.csv
"""
import argparse, sys, json, datetime
from google.cloud import bigquery, storage

def ensure_dataset(bq, project_id):
    ds_id = f"{project_id}.energy_analytics"
    ds = bigquery.Dataset(ds_id)
    ds.location = "EU"
    bq.create_dataset(ds, exists_ok=True)
    return ds_id

def create_raw_table(bq, project_id):
    table_id = f"{project_id}.energy_analytics.raw_usage"
    schema = json.load(open("bq/schema.raw.json"))
    table = bigquery.Table(table_id, schema=[bigquery.SchemaField(**s) for s in schema])
    table.clustering_fields = ["zipcode","mozip","hh_id"]
    table.time_partitioning = bigquery.TimePartitioning(type_=bigquery.TimePartitioningType.MONTH, field=None)  # partition by ingestion-time
    bq.create_table(table, exists_ok=True)
    return table_id

def load_csv_to_raw(bq, project_id, gcs_uri):
    table_id = f"{project_id}.energy_analytics.raw_usage"
    job_config = bigquery.LoadJobConfig(
        schema=[bigquery.SchemaField(**s) for s in json.load(open("bq/schema.raw.json"))],
        skip_leading_rows=1,
        source_format=bigquery.SourceFormat.CSV,
        allow_quoted_newlines=True,
        allow_jagged_rows=True,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        field_delimiter=",",
        encoding="UTF-8",
    )
    load_job = bq.load_table_from_uri(gcs_uri, table_id, job_config=job_config)
    result = load_job.result()
    print(f"Loaded {result.output_rows} rows into {table_id}")

def build_staging(bq, project_id):
    ddl = open("bq/ddl.sql").read().replace("{project_id}", project_id)
    for stmt in ddl.split(";"):
        s = stmt.strip()
        if s:
            bq.query(s).result()
    print("Staging and curated tables are ready.")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--project", required=True)
    ap.add_argument("--bucket", required=True, help="GCS bucket name (no gs:// prefix)")
    ap.add_argument("--object", required=True, help="Object path inside bucket, e.g. imputation/data_test.csv")
    args = ap.parse_args()

    bq = bigquery.Client(project=args.project)
    ensure_dataset(bq, args.project)
    create_raw_table(bq, args.project)
    gcs_uri = f"gs://{args.bucket}/{args.object}"
    load_csv_to_raw(bq, args.project, gcs_uri)
    build_staging(bq, args.project)