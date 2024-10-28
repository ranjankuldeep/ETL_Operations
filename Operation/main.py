from pyspark.sql import SparkSession
from extract_script import extract_data  
from transform_script import transform_confirmed, transform_recovered, transform_deaths  
from load_script import write_csv_to_s3  

from dotenv import load_dotenv
import os

# AWS S3 configuration
output_bucket = 'apache-output'
confirmed_output_key = 'transformed_confirmed_data.csv'
recovered_output_key = 'transformed_recovered_data.csv'
deaths_output_key = 'transformed_deaths_data.csv'

load_dotenv()
aws_access_key=os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_key=os.getenv("AWS_SECRET_ACCESS_KEY")

if __name__ == "__main__":
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("CovidDataPipeline") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.access.key", aws_access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", aws_secret_key) \
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.12.115") \
        .getOrCreate()

    # Extract data
    confirmed_df, recovered_df, deaths_df = extract_data(spark)

    # Apply transformations
    transformed_confirmed_df = transform_confirmed(confirmed_df)
    transformed_recovered_df = transform_recovered(recovered_df)
    transformed_deaths_df = transform_deaths(deaths_df)

    # Load the transformed DataFrames to S3
    write_csv_to_s3(transformed_confirmed_df, output_bucket, confirmed_output_key)
    write_csv_to_s3(transformed_recovered_df, output_bucket, recovered_output_key)
    write_csv_to_s3(transformed_deaths_df, output_bucket, deaths_output_key)

    # Stop the Spark session
    spark.stop()
