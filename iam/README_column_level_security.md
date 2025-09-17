# Column-level security (Policy Tags) - Rationale & How-To

We treat income* as sensitive. Steps:
1) Create a Data Catalog taxonomy (manually or via Terraform) e.g., taxonomy "EnergyAnalyticsSensitivity" with policy tag "PII_Low".
2) Attach the policy tag to columns income2...income9 in table curated_usage.
3) Grant fine-grained access to that policy tag (Data Catalog) only to roles/groups that should see income.

Terraform/resource names vary per project. See attach_policy_tags.sql for a template.