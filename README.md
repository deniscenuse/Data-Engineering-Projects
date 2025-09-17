# Energy Analytics Mini‑Project (GCP • BQ • GCS)

This package gives you a tidy, reproducible pipeline:

**Deliverables covered**
1) Ingestion + schema to BigQuery with DQ checks  
2) Two imputation strategies (baseline + model) and comparison  
3) Insights SQL + predictive sanity checks  
4) Security & IAM samples (least‑privilege, column/row scoping)  
5) Reproducibility (Terraform stub + scripts)  

## 0) Prereqs
- GCP project (BQ Sandbox OK), `gcloud` & `bq` CLIs
- A bucket: `gs://YOUR_BUCKET/imputation/` with `data_test.csv`
- Python 3.10+, `pip install -r requirements.txt`

## 1) Create infra
Option A — **Terraform (stub)**:
```bash
cd terraform
terraform init
terraform apply -var="project_id=YOUR_PROJECT" -var="bucket_name=YOUR_BUCKET"
```

Option B — **CLI**:
```bash
gcloud config set project YOUR_PROJECT
bq --location=EU mk -d energy_analytics
gsutil mb -l EU gs://YOUR_BUCKET
```

## 2) Load raw CSV → BigQuery
```bash
# Upload the file
gsutil cp ./data/data_test.csv gs://YOUR_BUCKET/imputation/data_test.csv

# Use Python helper (creates tables and staging)
python scripts/ingest_to_bq.py --project YOUR_PROJECT --bucket YOUR_BUCKET --object imputation/data_test.csv
```

Alternatively via CLI:
```bash
bq load --autodetect=false --replace   --source_format=CSV --skip_leading_rows=1   YOUR_PROJECT:energy_analytics.raw_usage   gs://YOUR_BUCKET/imputation/data_test.csv   ./bq/schema.raw.json

# Build staging/curated shells
cat bq/ddl.sql | sed "s/{project_id}/YOUR_PROJECT/g" | bq query --use_legacy_sql=false
```

## 3) Data Quality (run a few assertions)
```bash
cat dq/dq_checks.sql | sed "s/{project_id}/YOUR_PROJECT/g" | bq query --use_legacy_sql=false
```

## 4) Imputation
- **Baseline** (group median):  
```bash
python scripts/impute_baseline.py --project YOUR_PROJECT
```
- **Model‑based** (Ridge regression):  
```bash
python scripts/impute_model.py --project YOUR_PROJECT
```

Outputs:
- `energy_analytics.imputed_baseline`
- `energy_analytics.imputed_model`
- Appended rows in `energy_analytics.curated_usage`
- Optional metrics in `energy_analytics.model_metrics`

## 5) Insights & sanity
- SQL snippets in `bq/insights.sql` (median/avg before vs after, seasonality, correlations).
```bash
cat bq/insights.sql | sed "s/{project_id}/YOUR_PROJECT/g" | bq query --use_legacy_sql=false
```
- Notebooks in `notebooks/` for EDA and comparison.

## 6) Security & IAM
- Bind least privilege (examples in `iam/*.json`):
  - `etl-sa`: Storage Object Viewer (bucket), BigQuery Data Editor + Job User (dataset)
  - Analysts group: BigQuery Data Viewer
- **Row‑level security**: see `iam/row_access_policy.sql`
- **Column‑level security** (policy tags) for `income*`: see `iam/README_column_level_security.md` & `iam/attach_policy_tags.sql`

**Rationale**: ETL only needs to read from GCS and write/query dataset. Analysts shouldn’t see sensitive income columns by default and may have zipcode‑scoped access.

## 7) Concerns & notes
- Demographic fields come from a 3rd‑party aggregator: watch for **missing‑not‑at‑random** patterns; imputation can encode bias.
- `mozip` stands in for weather; ensure its granularity is stable across time.
- Partitioning by `period_date` supports month‑level pruning; clustering by `zipcode`,`mozip`,`hh_id` helps point‑lookups.
- Keep an **audit trail**: the scripts mark `was_imputed` and `impute_strategy`.
- Sanity bounds for `lusage` and `size_sqft` are included in DQ SQL; tune thresholds with domain input.

## 8) Requirements
See `requirements.txt`

## 9) File map
```
bq/
  ddl.sql
  schema.raw.json
  schema.curated.json
  insights.sql
dq/
  dq_checks.sql
iam/
  bucket_policy.bindings.json
  dataset_policy.bindings.json
  row_access_policy.sql
  attach_policy_tags.sql
  README_column_level_security.md
notebooks/
  01_eda.ipynb
  02_imputation_compare.ipynb
  03_insights_and_sanity.ipynb
scripts/
  ingest_to_bq.py
  impute_baseline.py
  impute_model.py
terraform/
  main.tf
```