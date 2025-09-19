# Household Electricity Analytics 
## Data Engineering Mini Project

## Project Overview

This project demonstrates a complete but lightweight **data engineering workflow** using Google Cloud Platform (GCP).  

- The project starts with a CSV of household electricity consumption and build a pipeline that ingests, validates, imputes, and analyzes the data, the goal being to showcase the thought process, the tools used, and of course, the steps needed to reproduce the solution.

---

## Workflow Summary

1. **Ingestion & Schema**  
   - Input CSV (`data_test.csv` in our case) should be uploaded to a GCS bucket.  
   - The Python script (`scripts/ingest_to_bq.py`) loads data into BigQuery.  
   - Tables follow the raw, staging and curated pattern with partitioning and clustering.  

2. **Data Quality (DQ)**  
   - SQL checks in `dq/dq_checks.sql` validate row counts, missing values, and categorical consistency.  
   - As this process is required to ensure that the dataset is suitable for downstream imputation and modeling.  

3. **Imputation**  
   - **Baseline strategy**: median imputation (BigQuery SQL).  
   - **Model-based strategy**: linear regression (BigQuery ML).  
   - Both of these approaches populate the curated table with clean data.  

4. **Insights & Sanity**  
   - Queries in `bq/insights.sql` compute medians, averages, seasonal usage, and correlations.  
   - As well, a BQML predictive model checks whether features explain electricity usage.  

5. **Security & Reproducibility**  
   - Example IAM JSONs show least-privilege roles (bucket read, BQ read, column scoping).  
   - Terraform stub provisions bucket + dataset.  

---

## Reproduction Guide

### 0. Prerequisites

- GCP project 
- APIs enabled: BigQuery, Cloud Storage.  
- Cloud Shell (recommended).  
- Clone my repo:  
  ```bash
  git clone https://github.com/deniscenuse/Data-Engineering-Projects
  cd Data-Engineering-Projects
  ```

---

### 1. Provision Infra (Terraform)

```bash
cd terraform
terraform init
terraform apply -var="project_id=<PROJECT_ID>" -var="bucket_name=<BUCKET_NAME>" -var="location=EU"
```

This creates:  
- A bucket (`gs://<BUCKET_NAME>`)  
- A BigQuery dataset (`energy_analytics`)  

Check with:  
```bash
gsutil ls
bq ls <PROJECT_ID>:energy_analytics
```

*(If Terraform not used, this can be created manually with the UI on the Google Cloud Console  or via CLI with `gsutil mb` and `bq mk`.)*

---

### 2. Upload the CSV to GCS

Place `data_test.csv` into the bucket, ideally inside `imputation/`.  

#### 2.1 First option would be through the web console  
Go to Storage → bucket → `imputation/` → Upload file.  

#### 2.2 The second option would be by using the CLI. 
```bash
gsutil cp data_test.csv gs://<BUCKET_NAME>/imputation/
```

Verify:  
```bash
gsutil ls gs://<BUCKET_NAME>/imputation/
```

---

### 3. Ingest into BigQuery

Install Python deps (Cloud Shell):  
```bash
pip install -r requirements.txt
```

Run ingestion:  
```bash
python scripts/ingest_to_bq.py   --project <PROJECT_ID>   --bucket  <BUCKET_NAME>   --object  imputation/data_test.csv
```

This creates:  
- `raw_usage` (partitioned, clustered)  
- `stg_usage` (cleaned types + derived date)  

Check row counts:  
```bash
bq query --use_legacy_sql=false --location=EU 'SELECT COUNT(*) FROM `<PROJECT_ID>.energy_analytics.stg_usage`'
```

---

### 4. Data Quality Checks

Run:  
```bash
cat dq/dq_checks.sql | sed "s/{{project_id}}/<PROJECT_ID>/g"   | bq query --use_legacy_sql=false --location=EU
```

This reports:  
- Missing values  
- Year/month validity  
- One-hot encoding consistency  
- Value ranges  

---

### 5. Imputation

#### 5.1 Baseline (SQL)

```bash
bq query --use_legacy_sql=false --location=EU < bq/baseline_impute.sql
```

This inserts into `curated_usage`.  

#### 5.2 Model-based (BQML)

Train:  
```bash
bq query --use_legacy_sql=false --location=EU '
CREATE OR REPLACE MODEL `<PROJECT_ID>.energy_analytics.lusage_linreg`
OPTIONS(model_type="linear_reg") AS
SELECT
  lusage AS label,
  mozip, zipcode, month, size_sqft, children, owner,
  hhsize2, hhsize3, hhsize4, hhsize5plus,
  income2, income3, income4, income5, income6, income7, income8, income9,
  lusage1, lusage2, lusage3, lusage4, lusage5, lusage6
FROM `<PROJECT_ID>.energy_analytics.stg_usage`;'
```

Evaluate:  
```bash
bq query --use_legacy_sql=false --location=EU '
SELECT * FROM ML.EVALUATE(MODEL `<PROJECT_ID>.energy_analytics.lusage_linreg`,
  (SELECT * FROM `<PROJECT_ID>.energy_analytics.stg_usage`));'
```

---

### 6. Insights

```bash
cat bq/insights.sql | sed "s/{{project_id}}/<PROJECT_ID>/g"   | bq query --use_legacy_sql=false --location=EU
```

Outputs:  
- Pre vs Post medians/averages  
- Usage by month  
- Correlation with historical lags  

Optional: run notebooks in `notebooks/` for plots.  

---

### 7. Security & IAM

See `iam/` for sample JSONs:  
- Bucket read-only role  
- BigQuery read role on curated table  
- Example column/row restriction policies  

These illustrate **least privilege** principles.  

---

### 8. Wrap-up

At this point, the pipeline has:  
- Raw data ingested into BigQuery  
- Data quality validated  
- Missing values imputed (two ways)  
- Insights generated  
- IAM + reproducibility considerations provided  

---

## Notes & Concerns

- The dataset had very few missing values, so imputation impact is minor.  
- BQML linear regression showed weak predictive performance (likely overfitting).  
- For real production, monitoring, cost controls, and lifecycle rules for raw files would be added.  

---

## Author

### Denis-Ionuț Cenușe  
Technical Interview Project
