import argparse
import json
from google.cloud import bigquery

def create_raw_table(bq, project_id):
    dataset_id = f"{project_id}.energy_analytics"
    table_id = f"{dataset_id}.raw_usage"

    # Load schema spec from JSON
    spec = json.load(open("bq/schema.raw.json"))

    schema = [bigquery.SchemaField(s["name"], s["type"]) for s in spec]

    table = bigquery.Table(table_id, schema=schema)
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.MONTH,
        field="period_date",
    )
    table.clustering_fields = ["zipcode", "mozip", "hh_id"]

    bq.create_table(table, exists_ok=True)
    print(f"Created table {table_id}")

def load_csv_to_raw(bq, project_id, gcs_uri):
    dataset_id = f"{project_id}.energy_analytics"
    table_id = f"{dataset_id}.raw_usage"

    spec = json.load(open("bq/schema.raw.json"))
    schema = [bigquery.SchemaField(s["name"], s["type"]) for s in spec]

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        skip_leading_rows=1,
        source_format=bigquery.SourceFormat.CSV,
        allow_quoted_newlines=True,
        allow_jagged_rows=True,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        field_delimiter=",",
        encoding="UTF-8",
    )

    job = bq.load_table_from_uri(gcs_uri, table_id, job_config=job_config)
    result = job.result()
    print(f"Loaded {result.output_rows} rows into {table_id}")

def build_staging(bq, project_id):
    dataset_id = f"{project_id}.energy_analytics"
    raw = f"{dataset_id}.raw_usage"
    stg = f"{dataset_id}.stg_usage"

    stmt = f"""
    CREATE OR REPLACE TABLE `{stg}` AS
    SELECT
      *,
      DATE(CONCAT(CAST(year AS STRING), '-', CAST(month AS STRING), '-01')) AS period_date
    FROM `{raw}`;
    """

    bq.query(stmt).result()
    print(f"Created staging table {stg}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", required=True)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--object", required=True)
    args = parser.parse_args()

    bq = bigquery.Client(project=args.project)

    create_raw_table(bq, args.project)

    gcs_uri = f"gs://{args.bucket}/{args.object}"
    load_csv_to_raw(bq, args.project, gcs_uri)

    build_staging(bq, args.project)
