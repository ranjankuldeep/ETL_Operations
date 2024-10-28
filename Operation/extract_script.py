from pyspark.sql import SparkSession
import boto3

# AWS S3 configuration
input_bucket = 'apachekuldeep'                   
output_bucket = 'apache-output'                    

"""All the input csv to be used"""
confirmed_key = 'time_series_covid19_confirmed_global.csv'
recovered_key = 'time_series_covid19_recovered_global.csv'
deaths_key = 'time_series_covid19_deaths_global.csv'

def read_csv_from_s3(spark, bucket, key):
    """Read a CSV file from S3 using Spark and return a DataFrame."""
    s3_path = f"s3a://{bucket}/{key}"
    try:
        df = spark.read.csv(s3_path, header=True, inferSchema=True)
        print(f"Data loaded from {s3_path}:")
        return df
    except Exception as e:
        print(f"Error reading {key} from S3: {e}")

        """returns an empty data frame"""
        return spark.createDataFrame([], schema="") 

def extract_data(spark):
    # Extract data from S3
    confirmed_data_df = read_csv_from_s3(spark, input_bucket, confirmed_key)
    recovered_data_df = read_csv_from_s3(spark, input_bucket, recovered_key)
    deaths_data_df = read_csv_from_s3(spark, input_bucket, deaths_key)

    return confirmed_data_df, recovered_data_df, deaths_data_df
