-- Example Row Access Policy: restrict analysts to only a subset of zipcodes
CREATE OR REPLACE ROW ACCESS POLICY zip_scope
ON `{{project_id}}.energy_analytics.curated_usage`
GRANT TO ("group:analysts@yourcompany.com")
FILTER USING (zipcode IN ("Z001","Z002","Z003"));