from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructField, StructType, StringType
from utils import send_email
import configparser
import logging

# === Logging setup ===
logging.basicConfig(
    filename='logs.log',
    level=logging.WARNING,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# === ConfigParser ===
config = configparser.ConfigParser()
config_path = "config/config.ini"
config.read(config_path)

# === DB Info from config ===
DB_NAME = config['database']['database']
PWD = config['database']['password']
USER = config['database']['user']
HOST = config['database']['host']

# === SparkSession setup ===
def create_sparksession() -> SparkSession:
    spark = SparkSession.builder \
            .appName("KafkaToPostgres") \
            .config("spark.sql.shuffle.partitions", "9") \
            .config("spark.sql.warehouse.dir", "tmp/sql_warehouse") \
            .config("spark.local.dir", "tmp/local_dir") \
            .getOrCreate()

    return spark

# === Read data from Kafka ===
def read_kafka_stream(spark):
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
        logging.error(f"Cannot read stream from Kafka due to {e}")
        raise

    return df_stream

# === Apply schema ===
def create_schema(streaming_df):
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

    df_raw = streaming_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')) \
        .select('data.*')
    
    df_raw.printSchema()

    return df_raw

# === Change column types ===
def column_types(df_stream):
    df = df_stream.select(
        when(col("VendorID") == "", None).otherwise(col("VendorID").cast("int")).alias("vendor_id"),
        when(col("tpep_pickup_datetime") == "", None).otherwise(to_timestamp(col("tpep_pickup_datetime"))).alias("pickup_datetime"),
        when(col("tpep_dropoff_datetime") == "", None).otherwise(to_timestamp(col("tpep_dropoff_datetime"))).alias("dropoff_datetime"),
        when(col("passenger_count") == "", None).otherwise(col("passenger_count").cast("int")).alias("passenger_count"),
        when(col("trip_distance") == "", None).otherwise(col("trip_distance").cast("double")).alias("trip_distance"),
        when(col("RatecodeID") == "", None).otherwise(col("RatecodeID").cast("int")).alias("ratecode_id"),
        when(col("PULocationID") == "", None).otherwise(col("PULocationID").cast("int")).alias("pu_location_id"),
        when(col("DOLocationID") == "", None).otherwise(col("DOLocationID").cast("int")).alias("do_location_id"),
        when(col("payment_type") == "", None).otherwise(col("payment_type").cast("int")).alias("payment_type"),
        when(col("fare_amount") == "", None).otherwise(col("fare_amount").cast("double")).alias("fare_amount"),
        when(col("extra") == "", None).otherwise(col("extra").cast("double")).alias("extra"),
        when(col("mta_tax") == "", None).otherwise(col("mta_tax").cast("double")).alias("mta_tax"),
        when(col("tip_amount") == "", None).otherwise(col("tip_amount").cast("double")).alias("tip_amount"),
        when(col("tolls_amount") == "", None).otherwise(col("tolls_amount").cast("double")).alias("tolls_amount"),
        when(col("improvement_surcharge") == "", None).otherwise(col("improvement_surcharge").cast("double")).alias("improvement_surcharge"),
        when(col("total_amount") == "", None).otherwise(col("total_amount").cast("double")).alias("total_amount"),
        when(col("congestion_surcharge") == "", None).otherwise(col("congestion_surcharge").cast("double")).alias("congestion_surcharge"),
        when(col("Airport_fee") == "", None).otherwise(col("Airport_fee").cast("double")).alias("airport_fee")
    )
    df.printSchema()

    return df

def foreach_batch_jdbc_writer(df, epoch_id, table_name):
    jdbc_url = f"jdbc:postgresql://{HOST}:5432/{DB_NAME}"
    connection_properties = {
        "user": f"{USER}",
        "password": f"{PWD}",
        "driver": "org.postgresql.Driver"
    }

    df.write.jdbc(jdbc_url, table_name, mode="append", properties=connection_properties)

# === Write full table to PostgreSQL  ===
def write_full_table(df_parsed):
    query = df_parsed \
        .drop("store_and_fwd_flag") \
        .withColumn("pickup_datetime", to_timestamp("pickup_datetime")) \
        .withColumn("dropoff_datetime", to_timestamp("dropoff_datetime")) \
        .writeStream \
        .foreachBatch(lambda df, epoch_id: foreach_batch_jdbc_writer(df, epoch_id, "yellow_tripdata")) \
        .outputMode("append") \
        .option("checkpointLocation", "tmp/checkpoint/full_table_data") \
        .start()
    
    return query

# === Select just necessary columns for better performance  ===
def select_column(df_parsed):
    df_main = df_parsed.select(
        "pickup_datetime", "dropoff_datetime", "total_amount", "payment_type"
    )

    df_extra = df_parsed.select(
        "pickup_datetime", "dropoff_datetime", "total_amount", "payment_type",
        "pu_location_id", "do_location_id", "fare_amount", "extra", "mta_tax", 
        "tip_amount", "tolls_amount", "improvement_surcharge", "airport_fee"
    )

    return df_main, df_extra

# === Detect trips with abnormal duration (e.g., duration less than 1) ===
def detect_abnormal_duration(df_extra):
    def process_abnormal_duration(batch_df, epoch_id):
        abnormal_duration = batch_df \
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

        # === Adding alert ===
        abnormal_duration_count = abnormal_duration.count()
        if abnormal_duration_count > 0:
            alert_message = f"⚠️ {abnormal_duration_count} trips with abnormal duration detected."

            send_email(
                subject="Alert: Abnormal Trip Duration Detected",
                body=alert_message,
                to_email=config["email"]["to_email"]
            )

            logging.warning(alert_message)

        foreach_batch_jdbc_writer(abnormal_duration, epoch_id, "abnormal_duration")

    query = df_extra \
        .writeStream \
        .foreachBatch(process_abnormal_duration) \
        .outputMode("append") \
        .option("checkpointLocation", "tmp/checkpoint/abnormal_duration") \
        .start()

    return query

# === Detect trips with abnormal fee ===
def detect_abnormal_fee(df_extra):
    def process_abnormal_fee(batch_df, epoch_id):
        abnormal_fee = batch_df \
            .withColumn("caculated_total_amount", 
                        col("fare_amount") +
                        col("extra") +
                        col("mta_tax") +
                        col("tip_amount") +
                        col("tolls_amount") +
                        col("improvement_surcharge") +
                        col("airport_fee")) \
            .filter((abs(col("total_amount") - col("caculated_total_amount")) > 1) | (col("total_amount").isNull())) \
            .selectExpr(
                "pickup_datetime", 
                "dropoff_datetime", 
                "pu_location_id", 
                "do_location_id", 
                "round(abs(total_amount - caculated_total_amount), 2) AS amount_discrepancy"
            )
        
        # === Adding alert ===
        abnormal_fee_count = abnormal_fee.count()
        if abnormal_fee_count > 0:
            alert_message = f"⚠️ {abnormal_fee_count} trips with abnormal fee detected."

            send_email(
                subject="Alert: Abnormal Trip Fee Detected",
                body=alert_message,
                to_email=config["email"]["to_email"]
            )

            logging.warning(alert_message)

        foreach_batch_jdbc_writer(abnormal_fee, epoch_id, "abnormal_fee")

    
    query = df_extra \
        .writeStream \
        .foreachBatch(process_abnormal_fee) \
        .outputMode("append") \
        .option("checkpointLocation", "tmp/checkpoint/abnormal_fee") \
        .start()
    
    return query

# === Average revenue per hour ===
def avg_revenue_per_hour(df_main):    
    avg_revenue_per_hour = df_main \
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
        .foreachBatch(lambda df, epoch_id: foreach_batch_jdbc_writer(df, epoch_id, "avg_revenue_per_hour")) \
        .outputMode("append") \
        .option("checkpointLocation", "tmp/checkpoint/avg_revenue") \
        .start()
    
    return query

# === Count trips per hour  ===
def trip_count_per_hour(df_main):    
    trip_count_per_hour = df_main \
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
        .foreachBatch(lambda df, epoch_id: foreach_batch_jdbc_writer(df, epoch_id, "trip_count_per_hour")) \
        .outputMode("append") \
        .option("checkpointLocation", "tmp/checkpoint/trip_count") \
        .start()
    
    return query

# === Count hourly trips per zone ===
def caculate_trip_count_per_zone(spark, df_extra):
    taxi_zone_lookup_path = ("data/taxi_zone_lookup.csv")

    lookup_df = spark.read.csv(taxi_zone_lookup_path, header=True, inferSchema=True)
    df_extra = df_extra \
    .select(
        "pickup_datetime", "dropoff_datetime", "pu_location_id"
    )

    df_extra_with_location = df_extra \
        .join(broadcast(lookup_df), df_extra.pu_location_id == lookup_df.LocationID, "left") \
        .select(df_extra["*"], lookup_df["LocationID"], lookup_df["Borough"])

    trip_count_per_borough = df_extra_with_location \
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
        .foreachBatch(lambda df, epoch_id: foreach_batch_jdbc_writer(df, epoch_id, "trip_count_by_borough")) \
        .option("checkpointLocation", "tmp/checkpoint/trip_count_by_borough") \
        .start()

    return query

# === Main workflow ===
def main():
    spark = create_sparksession()
    
    kafka_df = read_kafka_stream(spark)
    raw_df = create_schema(kafka_df)
    parsed_df = column_types(raw_df)
    df_main, df_extra = select_column(parsed_df)
    
    full_data_to_postgres = write_full_table(parsed_df)
    abnormal_duration = detect_abnormal_duration(df_extra)
    abnormal_fee = detect_abnormal_fee(df_extra)
    avg_revenue_hourly = avg_revenue_per_hour(df_main)
    trip_hourly = trip_count_per_hour(df_main)
    trip_per_zone_hourly = caculate_trip_count_per_zone(spark, df_extra)

    full_data_to_postgres.awaitTermination()
    abnormal_duration.awaitTermination()
    abnormal_fee.awaitTermination()
    avg_revenue_hourly.awaitTermination()
    trip_hourly.awaitTermination()
    trip_per_zone_hourly.awaitTermination()

if __name__ == '__main__':
    main()