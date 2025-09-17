terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = "europe-west1"
}

variable "project_id" {}
variable "bucket_name" {}

resource "google_storage_bucket" "imputation" {
  name          = var.bucket_name
  location      = "EU"
  force_destroy = true
  uniform_bucket_level_access = true
  labels = { env = "demo", app = "energy-analytics" }
}

resource "google_bigquery_dataset" "energy_analytics" {
  dataset_id  = "energy_analytics"
  location    = "EU"
  description = "Analytics dataset for household electricity usage"
  labels = { env = "demo", app = "energy-analytics" }
}

# Raw table (schema-only; data loaded via script/CLI)
resource "google_bigquery_table" "raw_usage" {
  dataset_id = google_bigquery_dataset.energy_analytics.dataset_id
  table_id   = "raw_usage"
  deletion_protection = false
  schema = file("${path.module}/../bq/schema.raw.json")
}

# Curated table (schema controlled by ddl.sql, created at runtime)
# (Optional) create an empty table resource if you want infra-as-code parity.