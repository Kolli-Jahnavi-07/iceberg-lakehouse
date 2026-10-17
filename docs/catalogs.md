# Running against a real catalog

The code only ever calls `catalog()` in `src/lakehouse/tables.py`. Locally that's a SQLite-backed
`SqlCatalog`; set PyIceberg's standard environment variables to use a shared catalog instead.

**AWS Glue + S3**

```bash
export PYICEBERG_CATALOG__LAKEHOUSE__TYPE=glue
export PYICEBERG_CATALOG__LAKEHOUSE__WAREHOUSE=s3://acme-lakehouse/warehouse
export AWS_REGION=us-east-2          # credentials from the instance / task role
python -m lakehouse demo
```

**Iceberg REST catalog** (Snowflake Open Catalog / Apache Polaris, Tabular-style services)

```bash
export PYICEBERG_CATALOG__LAKEHOUSE__TYPE=rest
export PYICEBERG_CATALOG__LAKEHOUSE__URI=https://<account>.snowflakecomputing.com/polaris/api/catalog
export PYICEBERG_CATALOG__LAKEHOUSE__CREDENTIAL=<client_id>:<client_secret>
export PYICEBERG_CATALOG__LAKEHOUSE__WAREHOUSE=lakehouse
```

Because the tables are plain Iceberg, the same data is readable by Spark, Trino/Athena, Flink and
Snowflake (`snowflake/iceberg_tables.sql`) without exporting anything.
