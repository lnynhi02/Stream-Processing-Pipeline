from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructField, StructType, StringType
import configparser
import logging
import os

logging.basicConfig(level=logging.INFO)

config = configparser.ConfigParser()
config_path = os.path.join(os.path.dirname(__file__), "config", "config.ini")
config.read(config_path)


DB_NAME = config['database']['database']
PWD = config['database']['password']
USER = config['database']['user']
HOST = config['database']['host']

def create_sparksession() -> SparkSession:
    spark = SparkSession.builder \
            .appName("KafkaToPostgres") \
            .config("spark.sql.shuffle.partitions", "9") \
            .config("spark.sql.warehouse.dir", "tmp/sql_warehouse") \
            .config("spark.local.dir", "tmp/local_dir") \
            .getOrCreate()
    
    return spark

def read_kafka_stream(spark):
    """ Reads the streaming data from Kafka """

    kafka_host = config['kafka']['host']
    kafka_port = config['kafka']['port']
    kafka_topic = config['kafka']['topic']
    kafka_bootstrap_servers = f"{kafka_host}:{kafka_port}"

    try:
        df_stream = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
            .option("subscribe", kafka_topic) \
            .option("startingOffsets", "earliest") \
            .load()
        
        logging.info("Read stream from Kafka successfully!")
    except Exception as e:
        logging.warning(f"Cannot read stream from Kafka due to {e}")
        raise
        
    return df_stream

# Apply schema to the data stream
def create_schema(streaming_df):
    """ Modify the data schema of streaming data """

    schema = StructType([
        StructField('VendorID', StringType(), True),
        StructField('tpep_pickup_datetime', StringType(), True),
        StructField('tpep_dropoff_datetime', StringType(), True),
        StructField('passenger_count', StringType(), True),
        StructField('trip_distance', StringType(), True),
        StructField('RatecodeID', StringType(), True),
        StructField('store_and_fwd_flag', StringType(), True),
        StructField('PULocationID', StringType(), True),
        StructField('DOLocationID', StringType(), True),
        StructField('payment_type', StringType(), True),
        StructField('fare_amount', StringType(), True),
        StructField('extra', StringType(), True),
        StructField('mta_tax', StringType(), True),
        StructField('tip_amount', StringType(), True),
        StructField('tolls_amount', StringType(), True),
        StructField('improvement_surcharge', StringType(), True),
        StructField('total_amount', StringType(), True),
        StructField('congestion_surcharge', StringType(), True),
        StructField('Airport_fee', StringType(), True)
    ])


    df = streaming_df.selectExpr("CAST(value AS String)")\
        .select(from_json(col('value'), schema).alias('data')) \
        .select('data.*')
    
    df_stream = df \
            .withColumnRenamed('VendorID', 'vendor_id') \
            .withColumnRenamed('tpep_pickup_datetime', 'pickup_datetime') \
            .withColumnRenamed('tpep_dropoff_datetime', 'dropoff_datetime') \
            .withColumnRenamed('RatecodeID', 'ratecode_id') \
            .withColumnRenamed('PULocationID', 'pu_location_id') \
            .withColumnRenamed('DOLocationID', 'do_location_id') \
            .withColumnRenamed('Airport_fee', 'airport_fee') \

    return df_stream

def select_columns(df_stream):
    streaming_main = df_stream.select(
        to_timestamp(col("pickup_datetime")).alias("pickup_datetime"),
        to_timestamp(col("dropoff_datetime")).alias("dropoff_datetime"),
        col("total_amount").cast("double").alias("total_amount"),
        col("payment_type").cast("int").alias("payment_type")
    )

    streaming_extra = df_stream.select(
    "pickup_datetime", "dropoff_datetime", "total_amount", "payment_type",
    "pu_location_id", "do_location_id", "fare_amount", "extra", "mta_tax", 
    "tip_amount", "tolls_amount", "improvement_surcharge", "airport_fee"
    )

    return streaming_main, streaming_extra
    
def foreach_batch_function(df, epoch_id, table_name):
    jdbc_url = f"jdbc:postgresql://{HOST}:5432/{DB_NAME}"
    connection_properties = {
        "user": f"{USER}",
        "password": f"{PWD}",
        "driver": "org.postgresql.Driver"
    }

    df.write.jdbc(jdbc_url, table_name, mode="append", properties=connection_properties)

def write_raw_data(streaming_raw):
    query = streaming_raw \
        .drop("store_and_fwd_flag") \
        .withColumn("pickup_datetime", to_timestamp("pickup_datetime")) \
        .withColumn("dropoff_datetime", to_timestamp("dropoff_datetime")) \
        .writeStream \
        .foreachBatch(lambda df, epoch_id: foreach_batch_function(df, epoch_id, "yellow_tripdata")) \
        .outputMode("append") \
        .option("checkpointLocation", "tmp/checkpoint/raw_data") \
        .start()
    
    return query

def detetect_abnormal_duration(streaming_extra):
    abnormal_duration = streaming_extra \
        .withColumn("pickup_datetime", to_timestamp("pickup_datetime")) \
        .withColumn("dropoff_datetime", to_timestamp("dropoff_datetime")) \
        .withColumn(
            "trip_duration_minutes", 
            (col("dropoff_datetime").cast("long") - col("pickup_datetime").cast("long")) / 60
        ) \
        .filter((col("trip_duration_minutes") < 1) | (col("trip_duration_minutes") > 120)) \
        .selectExpr(
            "pickup_datetime", 
            "dropoff_datetime", 
            "pu_location_id", 
            "do_location_id", 
            "round(trip_duration_minutes, 2) AS trip_duration_minutes"
        )
    
    query = abnormal_duration \
        .writeStream \
        .foreachBatch(lambda df, epoch_id: foreach_batch_function(df, epoch_id, "abnormal_duration")) \
        .outputMode("append") \
        .option("checkpointLocation", "tmp/checkpoint/abnormal_duration") \
        .start()
    
    return query

def detect_abnormal_fee(streaming_extra):
    abnormal_fee = streaming_extra \
        .withColumn("pickup_datetime", to_timestamp("pickup_datetime")) \
        .withColumn("dropoff_datetime", to_timestamp("dropoff_datetime")) \
        .withColumn("caculated_total_amount", 
                    col("fare_amount").cast("double") +
                    col("extra").cast("double") +
                    col("mta_tax").cast("double") +
                    col("tip_amount").cast("double") +
                    col("tolls_amount").cast("double") +
                    col("improvement_surcharge").cast("double") +
                    col("airport_fee").cast("double")) \
        .filter((abs(col("total_amount") - col("caculated_total_amount")) > 1) | (col("total_amount").isNull())) \
        .selectExpr(
            "pickup_datetime", 
            "dropoff_datetime", 
            "pu_location_id", 
            "do_location_id", 
            "round(abs(total_amount - caculated_total_amount), 2) AS amount_discrepancy"
        )
    
    query = abnormal_fee \
        .writeStream \
        .foreachBatch(lambda df, epoch_id: foreach_batch_function(df, epoch_id, "abnormal_fee")) \
        .outputMode("append") \
        .option("checkpointLocation", "tmp/checkpoint/abnormal_fee") \
        .start()
    
    return query

def avg_revenue_per_hour(streaming_main):
    avg_revenue_per_hour = streaming_main \
        .filter(col("total_amount").isNotNull()) \
        .withWatermark("pickup_datetime", "60 minutes") \
        .groupBy(window(col("pickup_datetime"), "60 minutes")) \
        .agg(
            avg("total_amount").alias("total_amount"),
            avg(expr("CASE WHEN payment_type = 1 THEN total_amount END")).alias("credit_card"),
            avg(expr("CASE WHEN payment_type = 2 THEN total_amount END")).alias("cash")
        ) \
        .selectExpr(
            "cast(date_format(window.start, 'yyyy-MM-dd') as date) AS date",
            "date_format(window.start, 'HH:mm:ss') AS start_time",
            "date_format(window.end, 'HH:mm:ss') AS end_time",
            "round(total_amount, 2) AS total_amount",
            "round(credit_card, 2) AS credit_card",
            "round(cash, 2) AS cash"
        ) \
        .drop("window")
    
    query = avg_revenue_per_hour \
        .writeStream \
        .foreachBatch(lambda df, epoch_id: foreach_batch_function(df, epoch_id, "avg_revenue_per_hour")) \
        .outputMode("append") \
        .option("checkpointLocation", "tmp/checkpoint/avg_revenue") \
        .start()
    
    return query

def trip_count_per_hour(streaming_main):
    trip_count_per_hour = streaming_main \
        .withWatermark("pickup_datetime", "60 minutes") \
        .groupBy(window(col("pickup_datetime"), "60 minutes")) \
        .agg(
            count("*").alias("total_trip"),
            count(expr("CASE WHEN payment_type = 1 then 1 END")).alias("credit_card"),                            
            count(expr("CASE WHEN payment_type = 2 then 1 END")).alias("cash")                            
        ) \
        .selectExpr(
            "cast(date_format(window.start, 'yyyy-MM-dd') as date) AS date",
            "date_format(window.start, 'HH:mm:ss') AS start_time",
            "date_format(window.end, 'HH:mm:ss') AS end_time",
            "total_trip",
            "credit_card",
            "cash"
        ) \
        .drop("window")

    query = trip_count_per_hour \
        .writeStream \
        .foreachBatch(lambda df, epoch_id: foreach_batch_function(df, epoch_id, "trip_count_per_hour")) \
        .outputMode("append") \
        .option("checkpointLocation", "tmp/checkpoint/trip_count") \
        .start()
    
    return query

def caculate_trip_count_per_zone(spark, streaming_extra):
    taxi_zone_lookup_path = os.path.join(os.path.dirname(__file__), "data", "taxi_zone_lookup.csv")

    lookup_df = spark.read.csv(taxi_zone_lookup_path, header=True, inferSchema=True)
    streaming_extra = streaming_extra \
    .selectExpr(
        "CAST(pickup_datetime AS timestamp) AS pickup_datetime",
        "CAST(dropoff_datetime AS timestamp) AS dropoff_datetime",
        "CAST(pu_location_id AS int) AS pu_location_id"
    )

    streaming_extra_with_location = streaming_extra \
        .join(broadcast(lookup_df), streaming_extra.pu_location_id == lookup_df.LocationID, "left") \
        .select(streaming_extra["*"], lookup_df["LocationID"], lookup_df["Borough"])

    trip_count_per_borough = streaming_extra_with_location \
        .withWatermark("pickup_datetime", "60 minutes") \
        .groupBy(
            window(col("pickup_datetime"), "60 minutes"),
            "Borough"
        ) \
        .agg(count("pu_location_id").alias("total_trip")) \
        .selectExpr(
            "cast(date_format(window.start, 'yyyy-MM-dd') as date) AS date",
            "date_format(window.start, 'HH:mm:ss') AS start_time",
            "date_format(window.end, 'HH:mm:ss') AS end_time",
            "Borough AS borough",
            "total_trip"
        ) \
        .drop("window")

    query = trip_count_per_borough \
        .writeStream \
        .outputMode("append") \
        .foreachBatch(lambda df, epoch_id: foreach_batch_function(df, epoch_id, "trip_count_by_borough")) \
        .option("checkpointLocation", "tmp/checkpoint/trip_count_by_borough") \
        .start()

    return query

def write_to_postgres(spark, streaming_raw, streaming_main, streaming_extra):
    raw_data_stream = write_raw_data(streaming_raw)
    abnormal_duration = detetect_abnormal_duration(streaming_extra)
    abnormal_fee = detect_abnormal_fee(streaming_extra)
    trip_count_stream = trip_count_per_hour(streaming_main)
    avg_revenue_stream = avg_revenue_per_hour(streaming_main)
    trip_count_per_zone = caculate_trip_count_per_zone(spark, streaming_extra)

    raw_data_stream.awaitTermination()
    abnormal_duration.awaitTermination()
    abnormal_fee.awaitTermination()
    trip_count_stream.awaitTermination()
    avg_revenue_stream.awaitTermination()
    trip_count_per_zone.awaitTermination()
                          

if __name__ == '__main__':
    spark = create_sparksession()
    df_initial = read_kafka_stream(spark)
    df_raw = create_schema(df_initial)
    df_main, df_extra = select_columns(df_raw)
    write_to_postgres(spark, df_raw, df_main, df_extra)