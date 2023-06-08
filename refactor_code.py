from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructType, StructField, BooleanType, DoubleType
from pyspark.sql.functions import col
import requests
import pandas as pd



def create_spark_session(app_name):
    return SparkSession.builder.appName(app_name).getOrCreate()


def get_data_from_url(url):
    response = requests.get(url)
    return response.json()


def create_dataframe(data, columns, schema=None):
    spark = create_spark_session("MyApp")
    if schema is None:
        df = spark.createDataFrame(data)
    else:
        df = spark.createDataFrame(data, schema)
    df = df.select(columns)
    return df


def join_data(account_df, user_df, join_column):
    joined_df = account_df.join(user_df, col("account_id") == col(join_column), how="inner")
    return joined_df


def get_location(patient_id=None,councillor_id=None):
    if councillor_id == None and patient_id != None:
        location = patient_join_df.filter(patient_join_df.patient_id == patient_id).select("latitude","longitude")
        return location
    elif patient_id == None and councillor_id != None :
        location = councillor_join_df.filter(councillor_join_df.councillor_id == councillor_id).select("latitude","longitude")       
        return location
api_key="HamdIRQ6ZeNJZj69Gy2Rru0VCqe04EoK"
if __name__ == "__main__":
    # Make HTTP requests and get data
    patient_url = "https://xloop-dummy.herokuapp.com/patient"
    patient_data = get_data_from_url(patient_url)

    councillor_url = "https://xloop-dummy.herokuapp.com/councillor"
    councillor_data = get_data_from_url(councillor_url)

    account_url = "https://xloop-dummy.herokuapp.com/account"
    account_data = get_data_from_url(account_url)

    # Define schema for account data
    schema = StructType([
        StructField("id", StringType(), nullable=False),
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
            ]), nullable=False),
            StructField("placeId", StringType(), nullable=False),
            StructField("region", StringType(), nullable=False)
        ]), nullable=False),
        StructField("national_identity", StringType(), nullable=False),
        StructField("role", StringType(), nullable=False),
        StructField("is_active", BooleanType(), nullable=False)
    ])

    # Create SparkSession
    spark = create_spark_session("MyApp")

    # Create DataFrames
    account_df = create_dataframe(account_data,[
        col("id").alias("account_id"),
        col("address.region").alias("region"),
        col("address.location.lat").alias("latitude"),
        col("address.location.lng").alias("longitude")
    ], schema)

    patient_df = create_dataframe(patient_data, [
        col("id").alias("patient_id"),
        col("user_id").alias("patient_user_id")
    ])

    councillor_df = create_dataframe(councillor_data, [
        col("id").alias("councillor_id"),
        col("user_id").alias("councillor_user_id")
    ])

    # Join DataFrames
    councillor_join_df = join_data(account_df, councillor_df, "councillor_user_id")
    patient_join_df = join_data(account_df, patient_df, "patient_user_id")

    get_location(patient_id="41").show()
    get_location(councillor_id="31").show()

