# Glue Jobs

Code-first ETL for the `aws_project2` CSV data pipeline. Replaces the original Glue Studio Visual ETL job with a versioned, reviewable PySpark script.

## Files

| File | Purpose |
|---|---|
| `csv_data_transformation.py` | PySpark Glue job. Reads from the Data Catalog, drops configured columns, writes to S3. |

## Why code over Visual ETL

- **Reviewable** - real diffs in pull requests instead of opaque visual graphs.
- **Reproducible** - the same script deploys to dev, staging, and prod.
- **Testable** - PySpark logic can be unit-tested locally with `pytest` and `pyspark`.
- **Extensible** - adding a transformation is editing a file, not clicking through a UI.

## Deploy

1. Upload the script to a scripts bucket:
   ```
   aws s3 cp glue_jobs/csv_data_transformation.py \
     s3://<your-scripts-bucket>/glue/csv_data_transformation.py
   ```

2. Create a Glue job (or update the existing `CSVDataTransformation` job):
   - **Type:** Spark
   - **Glue version:** 4.0 (Spark 3.3, Python 3)
   - **Script path:** the S3 path from step 1
   - **IAM role:** existing Glue service role with read on the processed bucket and write on the final bucket
   - **Worker type:** `G.1X`, 2 workers (cheapest viable for this scale)

3. Set job parameters:

| Key | Example value |
|---|---|
| `--DATABASE_NAME` | `csv_data_pipeline_catalog` |
| `--TABLE_NAME` | `csv_processed_data` |
| `--OUTPUT_PATH` | `s3://csv-final-data/` |
| `--DROP_COLUMNS` | `icon` |
| `--OUTPUT_FORMAT` | `csv` (or `parquet` - recommended) |

4. Save and run.

## Recommended next steps

- **Switch `OUTPUT_FORMAT` to `parquet`** and reconfigure the QuickSight dataset. Files become roughly 5x smaller and column-pruned scans are dramatically faster.
- **Add data-quality assertions** before the write step - row count > 0, required columns present, null-rate thresholds. Fail fast on bad data instead of silently corrupting downstream dashboards.
- **Wrap deployment in Terraform** so the job, the IAM role, and the script upload are one `terraform apply` instead of console clicks.
- **Orchestrate with Step Functions** so this job runs as part of a state machine with retries, error branches, and a DLQ - rather than relying on chained S3 events.
