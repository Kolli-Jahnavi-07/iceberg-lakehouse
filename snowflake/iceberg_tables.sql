-- Read the lakehouse's gold tables from Snowflake without copying them.
-- Snowflake reads the same Iceberg metadata and Parquet files through a catalog integration
-- (AWS Glue here) and an external volume on the S3 warehouse bucket.

CREATE EXTERNAL VOLUME IF NOT EXISTS LAKEHOUSE_S3
  STORAGE_LOCATIONS = ((
    NAME = 'lakehouse-us-east-2'
    STORAGE_PROVIDER = 'S3'
    STORAGE_BASE_URL = 's3://acme-lakehouse/warehouse/'
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::<account-id>:role/snowflake-lakehouse-reader'
  ));

CREATE CATALOG INTEGRATION IF NOT EXISTS LAKEHOUSE_GLUE
  CATALOG_SOURCE = GLUE
  CATALOG_NAMESPACE = 'gold'
  TABLE_FORMAT = ICEBERG
  GLUE_AWS_ROLE_ARN = 'arn:aws:iam::<account-id>:role/snowflake-glue-reader'
  GLUE_CATALOG_ID = '<account-id>'
  GLUE_REGION = 'us-east-2'
  ENABLED = TRUE;

CREATE ICEBERG TABLE IF NOT EXISTS ANALYTICS.LEASING.RESIDUAL_RISK
  EXTERNAL_VOLUME = 'LAKEHOUSE_S3'
  CATALOG = 'LAKEHOUSE_GLUE'
  CATALOG_TABLE_NAME = 'residual_risk'
  AUTO_REFRESH = TRUE;   -- picks up each new gold snapshot

-- Same table, same snapshot, queried from Snowflake:
SELECT risk_tier, COUNT(*) AS leases, SUM(residual_at_risk) AS residual_at_risk
FROM ANALYTICS.LEASING.RESIDUAL_RISK
GROUP BY risk_tier;
