
from pyspark.sql import SparkSession
from pyspark.sql.functions import rand, randn

S3_DATA_SOURCE_PATH = 's3://big-data-demo-bucket-emr-primuslearning/data-source/large_dataset.csv'
S3_DATA_OUTPUT_PATH = 's3://big-data-demo-bucket-emr-primuslearning/data-output/output.parquet'

# S3 credentials
AWS_ACCESS_KEY_ID = 'put your access key here'
AWS_SECRET_ACCESS_KEY = 'put your secret access key here'

# Create a SparkSession
print("Creating SparkSession...")
spark = SparkSession.builder.appName('GenerateAndProcessData') \
    .config('spark.hadoop.fs.s3a.access.key', AWS_ACCESS_KEY_ID) \
    .config('spark.hadoop.fs.s3a.secret.key', AWS_SECRET_ACCESS_KEY) \
    .getOrCreate()

# Generate a large dataset with random values
print("Generating data...")
data = spark.range(0, 10000000).withColumn('value1', rand(seed=42)).withColumn('value2', randn(seed=42))

# Save the dataset to an S3 bucket
print("Writing data to S3...")
data.write.csv(S3_DATA_SOURCE_PATH, header=True)

# Read the data from the S3 bucket
print("Reading data from S3...")
data = spark.read.csv(S3_DATA_SOURCE_PATH, header=True)

# Process the data
print("Processing data...")
processed_data = data.filter(data['value1'] > 0.5)

# Save the processed data to an S3 bucket
print("Writing processed data to S3...")
processed_data.write.mode('overwrite').parquet(S3_DATA_OUTPUT_PATH)

print("Done!")

