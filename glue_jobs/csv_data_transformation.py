"""
CSV Data Transformation - AWS Glue ETL Job
==========================================

Reads CSV files from the processed bucket via the Glue Data Catalog,
drops configured columns, and writes the result to the final bucket.

This is the PySpark equivalent of the Visual ETL job originally built
in Glue Studio. Code-first ETL is the production-grade approach:
  - Version controlled in Git
  - Reviewable in pull requests
  - Reproducible across environments (dev / staging / prod)
  - Unit-testable with pytest + pyspark

Deploy as a Glue 4.0 Spark job. See glue_jobs/README.md for details.

Required job parameters:
    --JOB_NAME         (auto-supplied by Glue)
    --DATABASE_NAME    e.g. csv_data_pipeline_catalog
    --TABLE_NAME       e.g. csv_processed_data
    --OUTPUT_PATH      e.g. s3://csv-final-data/
    --DROP_COLUMNS     comma-separated list, e.g. icon
    --OUTPUT_FORMAT    csv | parquet (parquet recommended)
"""

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext


# -----------------------------------------------------------------------------
# 0. Job initialization
# -----------------------------------------------------------------------------
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "DATABASE_NAME",
        "TABLE_NAME",
        "OUTPUT_PATH",
        "DROP_COLUMNS",
        "OUTPUT_FORMAT",
    ],
)

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
logger = glue_context.get_logger()

job = Job(glue_context)
job.init(args["JOB_NAME"], args)

logger.info(f"Starting job: {args['JOB_NAME']}")
logger.info(f"Source       : {args['DATABASE_NAME']}.{args['TABLE_NAME']}")
logger.info(f"Target       : {args['OUTPUT_PATH']}")
logger.info(f"Format       : {args['OUTPUT_FORMAT']}")
logger.info(f"Drop columns : {args['DROP_COLUMNS']}")


# -----------------------------------------------------------------------------
# 1. SOURCE - read from the Glue Data Catalog
# -----------------------------------------------------------------------------
# Reading via the Catalog (rather than a raw S3 path) means the schema is
# managed centrally. Schema evolution picked up by the Crawler flows here
# without any code change.
source_dyf = glue_context.create_dynamic_frame.from_catalog(
    database=args["DATABASE_NAME"],
    table_name=args["TABLE_NAME"],
    transformation_ctx="source_dyf",
)

input_count = source_dyf.count()
logger.info(f"Read {input_count} rows from source")

if input_count == 0:
    logger.warn("Source contained zero rows. Exiting cleanly without writing output.")
    job.commit()
    sys.exit(0)


# -----------------------------------------------------------------------------
# 2. TRANSFORM - drop unwanted columns
# -----------------------------------------------------------------------------
# DROP_COLUMNS is a comma-separated list. Parsing once at the top keeps the
# transformation declarative and easy to extend later (e.g. add filtering,
# type casts, derived columns).
columns_to_drop = [c.strip() for c in args["DROP_COLUMNS"].split(",") if c.strip()]

transformed_dyf = source_dyf.drop_fields(
    paths=columns_to_drop,
    transformation_ctx="drop_columns",
)

logger.info(f"Dropped columns: {columns_to_drop}")
logger.info("Output schema:")
transformed_dyf.printSchema()


# -----------------------------------------------------------------------------
# 3. TARGET - write to S3
# -----------------------------------------------------------------------------
# Two output formats supported:
#   - csv     : matches the original Visual ETL output (CSV + GZIP)
#   - parquet : recommended - columnar, smaller files, much faster scans
#               from QuickSight and Athena. Requires reconfiguring the
#               QuickSight dataset to read Parquet.
output_format = args["OUTPUT_FORMAT"].lower()

if output_format == "parquet":
    glue_context.write_dynamic_frame.from_options(
        frame=transformed_dyf,
        connection_type="s3",
        connection_options={"path": args["OUTPUT_PATH"]},
        format="parquet",
        format_options={"compression": "snappy"},
        transformation_ctx="write_parquet",
    )
elif output_format == "csv":
    glue_context.write_dynamic_frame.from_options(
        frame=transformed_dyf,
        connection_type="s3",
        connection_options={"path": args["OUTPUT_PATH"]},
        format="csv",
        format_options={"writeHeader": True, "compression": "gzip"},
        transformation_ctx="write_csv",
    )
else:
    raise ValueError(
        f"Unsupported OUTPUT_FORMAT '{output_format}'. Use 'csv' or 'parquet'."
    )

logger.info(f"Wrote output to {args['OUTPUT_PATH']} as {output_format}")


# -----------------------------------------------------------------------------
# 4. Commit
# -----------------------------------------------------------------------------
# job.commit() flushes Glue bookmarks - required so incremental processing
# (if enabled) only picks up new files on the next run.
job.commit()
logger.info("Job completed successfully")
