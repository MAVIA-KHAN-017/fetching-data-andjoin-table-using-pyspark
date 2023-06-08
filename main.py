# import redis

# # Create a Redis client
# redis_host = 'localhost'  # Replace with your Redis host
# redis_port = 6379  # Replace with your Redis port
# redis_client = redis.Redis(host=redis_host, port=redis_port)

# # Set a key-value pair in Redis
# redis_client.set('my_key', 'my_value')

# # Get the value for a key from Redis
# value = redis_client.get('my_key')
# print(value)  # Output: b'my_value'

# # Increment a counter in Redis
# redis_client.incr('my_counter')
# counter_value = redis_client.get('my_counter')
# print(counter_value)  # Output: b'1'

# # Store a list in Redis
# redis_client.rpush('my_list', 'item1')
# redis_client.rpush('my_list', 'item2')
# redis_client.rpush('my_list', 'item3')

# # Retrieve the list from Redis
# list_values = redis_client.lrange('my_list', 0, -1)
# print(list_values)  # Output: [b'item1', b'item2', b'item3']

# # Delete a key from Redis
# # redis_client.delete('my_key')
import redis
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, struct
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, TimestampType, DoubleType

spark = SparkSession.builder.getOrCreate()
spark = SparkSession.builder.appName("API Joining").getOrCreate()

account_url = "https://xloop-dummy.herokuapp.com/account"
councillor_url = "https://xloop-dummy.herokuapp.com/councillor"
patient_url = "https://xloop-dummy.herokuapp.com/patient"

account_data = requests.get(account_url).json()
councillor_data = requests.get(councillor_url).json()
patient_data = requests.get(patient_url).json()


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


account_df = spark.createDataFrame(account_data,schema)
councillor_df = spark.createDataFrame(councillor_data)
patient_df = spark.createDataFrame(patient_data)

location_schema = struct(
    col("address.location.lat").alias("lat"),
    col("address.location.lng").alias("lng")
)
selected_df = account_df.select("id", "address.location", "address.region")
selected_df.show(truncate=False)
selected_df = account_df.select(col("id"), col("address.region"),col("address.location.lat"), col("address.location.lng"))

# Show the selected data
selected_df.show(truncate=False)

selected_df = account_df.select(col("id").alias("account_id"), col("address.region"), col("address.location.lat"), col("address.location.lng"))
patient_selected_df = patient_df.select("user_id", "id")
selected_df.show()

joined_df = selected_df.join(patient_selected_df, selected_df.account_id == patient_selected_df.user_id)
joined_df.show()

councillor_selected_df = councillor_df.select("user_id", "id")

joined_df_1 = selected_df.join(councillor_selected_df, selected_df.account_id == councillor_selected_df.user_id)
joined_df_1.show()


