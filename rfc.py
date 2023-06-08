# import redis
# import json
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, struct
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Create SparkSession
spark = SparkSession.builder.appName("API Joining").getOrCreate()

# Define API endpoints
account_url = "https://xloop-dummy.herokuapp.com/account"
councillor_url = "https://xloop-dummy.herokuapp.com/councillor"
patient_url = "https://xloop-dummy.herokuapp.com/patient"

# Fetch data from API endpoints
account_data = requests.get(account_url).json()
councillor_data = requests.get(councillor_url).json()
patient_data = requests.get(patient_url).json()

# Define schema for account DataFrame
schema = StructType([
    StructField("id", IntegerType(), nullable=False),
    StructField("created", StringType(), nullable=False),
    StructField("updated", StringType(), nullable=False),
    StructField("email", StringType(), nullable=False),
    StructField("password", StringType(), nullable=False),
    StructField("first_name", StringType(), nullable=False),
    StructField("last_name", StringType(), nullable=False),
    StructField("gender", StringType(), nullable=False),
    StructField("phone_number", StringType(), nullable=False),
    StructField("address", StructType([
        StructField("address", StringType(), nullable=False),
        StructField("location", StructType([
            StructField("lat", DoubleType(), nullable=False),
            StructField("lng", DoubleType(), nullable=False)
        ])),
        StructField("placeId", StringType(), nullable=False),
        StructField("region", StringType(), nullable=False)
    ])),
    StructField("national_identity", StringType(), nullable=False),
    StructField("role", StringType(), nullable=False),
    StructField("is_active", StringType(), nullable=False)
])

# Create DataFrames from API data
account_df = spark.createDataFrame(account_data, schema)
councillor_df = spark.createDataFrame(councillor_data)
patient_df = spark.createDataFrame(patient_data)

# Select necessary columns from the account DataFrame
selected_df = account_df.select(
    col("id").alias("account_id"),
    col("address.region"),
    col("address.location.lat"),
    col("address.location.lng")
)
# Join selected_df with patient_df
joined_df = selected_df.join(patient_df, selected_df.account_id == patient_df.user_id)
# Join selected_df with councillor_df
joined_df_1 = selected_df.join(councillor_df, selected_df.account_id == councillor_df.user_id)

# # Create Redis client
# redis_host = 'localhost'  # Replace with your Redis host
# redis_port = 6380  # Replace with your Redis port
# redis_client = redis.Redis(host=redis_host, port=redis_port)

# # Store joined_df in Redis
# joined_df_json = joined_df.toJSON().collect()
# for row in joined_df_json:
#     json_data = json.loads(row)
#     account_id = json_data['account_id']
#     redis_client.hset('joined_table', account_id, json.dumps(json_data))

# # Store joined_df_1 in Redis
# joined_df_1_json = joined_df_1.toJSON().collect()
# for row in joined_df_1_json:
#     json_data = json.loads(row)
#     account_id = json_data['account_id']
#     redis_client.hset('joined_table_1', account_id, json.dumps(json_data))