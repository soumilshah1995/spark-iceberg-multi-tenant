import os, sys, time, json
from pyspark.sql import SparkSession
import boto3
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor
from prometheus_client import start_http_server, Summary, Gauge

# Prometheus Metrics
MERGE_DURATION = Summary('iceberg_merge_duration_seconds', 'Time spent merging data for a tenant', ['tenant'])
TOTAL_PROCESS_TIME = Summary('iceberg_total_process_time_seconds', 'Total time for all tenants to process')
FILES_PROCESSED = Gauge('iceberg_files_processed_count', 'Number of files processed in the manifest')

# Spark and Env Setup
SPARK_VERSION = '3.4'
ICEBERG_VERSION = '1.3.0'

SUBMIT_ARGS = f"--packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:{ICEBERG_VERSION},software.amazon.awssdk:bundle:2.20.160,software.amazon.awssdk:url-connection-client:2.20.160,org.apache.hadoop:hadoop-aws:3.3.4 pyspark-shell"
os.environ["PYSPARK_SUBMIT_ARGS"] = SUBMIT_ARGS
os.environ['PYSPARK_PYTHON'] = sys.executable
s3 = boto3.client("s3")

spark = SparkSession.builder \
    .appName("IcebergReadExample") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.dev", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.dev.type", "hadoop") \
    .config("spark.sql.catalog.dev.warehouse", "s3a://soumil-dev-bucket-1995/warehouse/") \
    .config("spark.sql.catalog.dev.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config("spark.sql.catalog.dev.s3.endpoint", "https://s3.amazonaws.com") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    .getOrCreate()


# Manifest creation
def create_pending_manifest(s3_client, bucket_name, raw_prefix, pending_prefix, uri_scheme, max_files):
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket_name, Prefix=raw_prefix)

    files = []
    for page in pages:
        for obj in page.get('Contents', []):
            files.append(f"{uri_scheme}{bucket_name}/{obj['Key']}")
            if len(files) >= max_files:
                break
        if len(files) >= max_files:
            break

    if not files:
        return None

    manifest_content = '\n'.join(files)
    unix_ts = int(time.time())
    manifest_key = f"{pending_prefix}{unix_ts}.pending"
    s3_client.put_object(Bucket=bucket_name, Key=manifest_key, Body=manifest_content)
    FILES_PROCESSED.set(len(files))
    return f"{uri_scheme}{bucket_name}/{manifest_key}"


def process_data_with_spark(spark, manifest_path, file_type="parquet"):
    manifest_df = spark.read.text(manifest_path)
    file_paths = manifest_df.rdd.map(lambda r: r[0]).collect()
    if file_type == "parquet":
        data_df = spark.read.parquet(*file_paths)
    elif file_type == "csv":
        data_df = spark.read.csv(*file_paths, sep='\t', header=True, inferSchema=True)
    else:
        raise Exception("invalid file format")
    return data_df


def parse_s3_path(s3_uri):
    parsed = urlparse(s3_uri)
    return parsed.netloc, parsed.path.lstrip('/')


def archive_file(s3_client, bucket_name, old_key, new_key):
    try:
        s3_client.copy_object(
            Bucket=bucket_name,
            CopySource={'Bucket': bucket_name, 'Key': old_key},
            Key=new_key
        )
        s3_client.delete_object(Bucket=bucket_name, Key=old_key)
    except Exception as e:
        print(f"Error archiving {old_key}: {str(e)}")


def archive_processed_files(s3_client, bucket_name, manifest_path, archived_prefix, max_threads=10):
    _, manifest_key = parse_s3_path(manifest_path)
    response = s3_client.get_object(Bucket=bucket_name, Key=manifest_key)
    content = response['Body'].read().decode('utf-8')
    file_paths = [path.strip() for path in content.split('\n') if path.strip()]
    with ThreadPoolExecutor(max_threads) as executor:
        futures = []
        for file_path in file_paths:
            _, old_key = parse_s3_path(file_path)
            new_key = f"{archived_prefix}{old_key}"
            futures.append(executor.submit(archive_file, s3_client, bucket_name, old_key, new_key))
        for future in futures:
            future.result()
    s3_client.delete_object(Bucket=bucket_name, Key=manifest_key)


def create_iceberg_table_if_not_exists(spark, table_name):
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        tenant STRING,
        id INT,
        name STRING,
        amount DOUBLE,
        event_time TIMESTAMP
    )
    USING iceberg
    """)


# ❌ Removed incorrect decorator
def merge_into_iceberg_table(spark, tenant, tenant_df, table_name):
    temp_view_name = f"staging_data_{tenant}"
    tenant_df.createOrReplaceTempView(temp_view_name)
    merge_sql = f"""
        MERGE INTO {table_name} AS target
        USING (
            SELECT *
            FROM (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY event_time DESC) AS row_num
                FROM {temp_view_name}
            ) AS deduped
            WHERE row_num = 1
        ) AS source
        ON target.id = source.id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """
    spark.sql(merge_sql)
    spark.catalog.dropTempView(temp_view_name)


def create_iceberg_table_if_not_exists_and_perform_merge(spark, table_name, tenant, spark_df):
    tenant_df = spark_df.filter(spark_df.tenant == tenant)
    create_iceberg_table_if_not_exists(spark, table_name=table_name)
    with MERGE_DURATION.labels(tenant).time():
        merge_into_iceberg_table(spark, tenant, tenant_df, table_name)


def process_tenant(spark, spark_df, tenant):
    table_name = f"dev.default.table_{tenant}"
    create_iceberg_table_if_not_exists_and_perform_merge(spark, table_name, tenant, spark_df)


@TOTAL_PROCESS_TIME.time()
def main_threaded():
    manifest_path = create_pending_manifest(
        s3,
        bucket_name="XX",
        raw_prefix="data/",
        pending_prefix="pending/",
        uri_scheme="s3a://",
        max_files=1000
    )

    if manifest_path is None:
        print("No files to process. Exiting.")
        return

    spark_df = process_data_with_spark(spark=spark, manifest_path=manifest_path, file_type="parquet")
    spark_df.cache()
    unique_tenants_list = [row["tenant"] for row in spark_df.select("tenant").distinct().collect()]

    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = [
            executor.submit(process_tenant, spark, spark_df, tenant)
            for tenant in unique_tenants_list
        ]
        for future in futures:
            future.result()

    archive_processed_files(
        s3,
        bucket_name="XX",
        manifest_path=manifest_path,
        archived_prefix="archived/"
    )


if __name__ == "__main__":
    # Start Prometheus metrics server
    start_http_server(8000, addr="0.0.0.0")
    print("Prometheus metrics available at http://localhost:8000/metrics")
    main_threaded()
    print("PROCESS COMPLETE")
    time.sleep(40)
