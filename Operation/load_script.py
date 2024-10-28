def write_csv_to_s3(df, bucket, key):
    """Write a Spark DataFrame to CSV in S3."""
    s3_path = f"s3a://{bucket}/{key}"
    try:
        df.write.csv(s3_path, header=True, mode="overwrite")
        print(f"Data written to {s3_path}")
    except Exception as e:
        print(f"Error writing {key} to S3: {e}")