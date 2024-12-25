-- CREATING SECRET to connect with MinIO Bucket

CREATE SECRET minio_secrets (
    TYPE S3,
    KEY_ID 'admin', -- MINIO_USER
    SECRET 'password', -- MINIO_PASSWORD
    REGION 'us-east-1',
    ENDPOINT '127.0.0.1:9000', -- MINIO_ENDPOINT
    USE_SSL false,
    URL_STYLE 'path'
);

-- Querying all the columns
FROM read_parquet('s3a://warehouse/logging/*/*.parquet', hive_partitioning = true);

-- Querying Specific columns
SELECT cluster_id, error, status, time_taken  FROM read_parquet('s3a://warehouse/logging/*/*.parquet', hive_partitioning = true);

-- Querying to get full Exception Stacktrace
SELECT error_desc FROM read_parquet('s3a://warehouse/logging/*/*.parquet', hive_partitioning = true);
