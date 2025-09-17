"""
Model-based imputation: predict lusage using linear regression with regularization (Ridge)
Features: mozip, zipcode, month, size_sqft, children, owner, hhsize*, income*, lusage1-6
Train only on rows with non-null lusage; impute for nulls; evaluate on 20% holdout.
Writes to energy_analytics.imputed_model and appends to curated_usage.
Requires: google-cloud-bigquery, scikit-learn, pandas
"""
import argparse, json
import pandas as pd
import numpy as np
from google.cloud import bigquery
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder
from sklearn.linear_model import Ridge
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.model_selection import train_test_split
from sklearn.metrics import r2_score, mean_absolute_error

def read_staging_df(bq, project):
    sql = f"SELECT * FROM `{project}.energy_analytics.stg_usage`"
    return bq.query(sql).to_dataframe(create_bqstorage_client=True)

def train_and_impute(df: pd.DataFrame):
    df = df.copy()
    target = "lusage"
    features = ['mozip','zipcode','month','size_sqft','children','owner','hhsize2','hhsize3','hhsize4','hhsize5plus',
                'income2','income3','income4','income5','income6','income7','income8','income9',
                'lusage1','lusage2','lusage3','lusage4','lusage5','lusage6']

    X = df[features]
    y = df[target]

    num_cols = ['month','size_sqft','lusage1','lusage2','lusage3','lusage4','lusage5','lusage6']
    cat_cols = ['mozip','zipcode','children','owner','hhsize2','hhsize3','hhsize4','hhsize5plus',
                'income2','income3','income4','income5','income6','income7','income8','income9']

    preproc = ColumnTransformer([
        ("num", SimpleImputer(strategy="median"), num_cols),
        ("cat", Pipeline([("imp", SimpleImputer(strategy="most_frequent")), ("ohe", OneHotEncoder(handle_unknown="ignore"))]), cat_cols),
    ])

    model = Ridge(alpha=1.0, random_state=0)
    pipe = Pipeline([("prep", preproc), ("model", model)])

    # Train on non-null target
    mask_train = y.notnull()
    X_train = X[mask_train]
    y_train = y[mask_train]
    X_miss  = X[~mask_train]

    # Eval split
    Xtr, Xte, ytr, yte = train_test_split(X_train, y_train, test_size=0.2, random_state=42)
    pipe.fit(Xtr, ytr)
    pred = pipe.predict(Xte)
    metrics = {"r2": float(r2_score(yte, pred)), "mae": float(mean_absolute_error(yte, pred)), "n_train": int(len(Xtr)), "n_test": int(len(Xte))}

    # Impute
    imputed = df.copy()
    imputed['was_imputed'] = False
    imputed['impute_strategy'] = None
    if len(X_miss) > 0:
        preds_missing = pipe.predict(X_miss)
        imputed.loc[~mask_train, 'lusage'] = preds_missing
        imputed.loc[~mask_train, 'was_imputed'] = True
        imputed.loc[~mask_train, 'impute_strategy'] = "ridge_linear_model"

    return imputed, metrics

def write_to_bq(bq, project, df, metrics):
    table_id = f"{project}.energy_analytics.imputed_model"
    job = bq.load_table_from_dataframe(df, table_id, job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE"))
    job.result()
    # Append to curated
    curated_cols = [c.name for c in bq.get_table(f"{project}.energy_analytics.curated_usage").schema]
    job2 = bq.load_table_from_dataframe(df[curated_cols], f"{project}.energy_analytics.curated_usage", job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND"))
    job2.result()
    # Store metrics table
    mdf = pd.DataFrame([metrics])
    bq.load_table_from_dataframe(mdf, f"{project}.energy_analytics.model_metrics", job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")).result()

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--project", required=True)
    args = ap.parse_args()
    bq = bigquery.Client(project=args.project)
    df = read_staging_df(bq, args.project)
    out, m = train_and_impute(df)
    write_to_bq(bq, args.project, out, m)
    print("Model-based imputation complete. Metrics:", m)