# Energy Analytics Mini Project

This mini-project was created as part of a learning exercise in **data engineering on Google Cloud Platform (GCP)**.  
The goal was to work with a household electricity consumption dataset and practice a typical data pipeline:  
from **raw CSV in Cloud Storage** to **structured tables in BigQuery**, with **data quality checks, imputation, insights, and reproducibility**.  

---

## Project Structure

```
Data-Engineering-Projects/
├── bq/                 # BigQuery SQL scripts (insights, transformations)
├── dq/                 # Data quality checks in SQL
├── notebooks/          # Jupyter notebooks for EDA, imputation, insights
├── scripts/            # Python scripts for ingestion and imputation
├── terraform/          # Stub for reproducibility (bucket + dataset)
├── iam/                # Example IAM JSON policies (least privilege)
└── README.md           # This file
```

---

## Workflow

### 1. Data ingestion
- Input data: `data_test.csv` (household electricity usage, 2010–2011).  
- Uploaded to **Google Cloud Storage** bucket (`gs://mybucket8787/`).  
- Ingestion done with **Python script** (`scripts/ingest_to_bq.py`), using the **BigQuery Python client**.  
- Schema defined explicitly → loaded into **raw_usage** table.  
- Tables organized as:
  - **raw_usage** → as ingested  
  - **stg_usage** → cleaned/structured  
  - **curated_usage** → ready for analysis  

### 2. Data quality checks
- SQL queries stored in `dq/dq_checks.sql`.  
- Checks include:
  - Missing values in important columns  
  - Distribution of household size and income categories  
  - Consistency of year/month ranges  

### 3. Imputation strategies
Two approaches were tried to fill missing values in `lusage` (log of electricity consumption):
1. **Baseline** → median imputation (by mozip, zipcode, or global level).  
   - Implemented in **SQL** (`baseline_impute.sql`).  
2. **Model-based** → linear regression model trained with **BQML**.  
   - Features: zipcode, mozip, size of house, past consumption (`lusage1-6`), demographics.  
   - Implemented via **Python script** + **BigQuery ML**.

### 4. Insights and sanity check
- Used queries from `bq/insights.sql`.  
- Compared **pre- and post-imputation statistics** (mean, median).  
- Correlation between current usage and lagged variables (`lusage1-3`) checked as a sanity step.  
- Built a simple **predictive model** in BigQuery ML (`linear_reg`) and evaluated metrics.  

### 5. Security & IAM
- Simulated **least-privilege policies** in JSON (`iam/`).  
- Examples:
  - Storage Object Viewer → only read access to bucket.  
  - BigQuery Data Viewer → only read access to curated dataset.  
  - Column/row scoping for sensitive demographics.  

### 6. Reproducibility
- Small **Terraform stub** in `terraform/` creates:  
  - GCS bucket  
  - BigQuery dataset  
- Scripts are runnable via **Cloud Shell**.  
- README explains arguments (`--project`, `--bucket`, `--object`).  

---

## Thought Process

- Start from **raw CSV in GCS** to simulate real-world landing zone.  
- Build step-by-step: **raw → staging → curated** to keep transformations traceable.  
- Add **data quality checks** to validate assumptions.  
- Apply **two imputation methods** to practice both a simple and a model-based approach.  
- Run **insights SQL + BQML model** for sanity checking.  
- Document **IAM and reproducibility** because they are always expected in real data engineering projects.  

---

## Visualizations (see notebooks)

1. Histogram of `lusage` (distribution of electricity usage).  
2. Line plot of average `lusage` by month (seasonality).  
3. Scatter plot of `lusage` vs. `size_sqft`.  
4. Count of imputed vs. original records.  
5. Predicted vs. actual `lusage` from regression model.  

---

## Concerns & Notes
- Real pipelines would separate **raw vs. processed** data in different folders (e.g., `raw/`, `processed/`).  
- Lifecycle rules in GCS could automatically delete/move files after processing.  
- Model performance (R² very low) shows the dataset may not be strong for prediction, but it is fine for practice.  

---

## How to Run

1. Upload CSV into your bucket:
   ```bash
   gsutil cp data_test.csv gs://<your-bucket>/imputation/
   ```
2. Run ingestion:
   ```bash
   python scripts/ingest_to_bq.py --project <PROJECT_ID> --bucket <BUCKET_NAME> --object imputation/data_test.csv
   ```
3. Run DQ checks:
   ```bash
   cat dq/dq_checks.sql | sed "s/{{project_id}}/<PROJECT_ID>/g" | bq query --use_legacy_sql=false
   ```
4. Run imputation (baseline and model):
   ```bash
   python scripts/impute_baseline.py --project <PROJECT_ID>
   python scripts/impute_model.py --project <PROJECT_ID>
   ```
5. Explore notebooks in `notebooks/` for EDA, imputation comparison, and insights.

---

## Conclusion

This project demonstrates a **complete but simple pipeline**:  
- Ingestion from GCS to BigQuery  
- Data validation  
- Imputation with two strategies  
- Insights and a sanity predictive check  
- IAM + reproducibility considerations  


## Thank you for your time!