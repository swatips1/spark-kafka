import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf
import time


"""Schema for incoming resources"""
schema = StructType([
        StructField("crime_id", StringType(), False),
        StructField("original_crime_type_name", StringType(), True),
        StructField("report_date", TimestampType(), True),
        StructField("call_date", TimestampType(), True),
        StructField("offense_date", TimestampType(), True),
        StructField("call_time", StringType(), True),
        StructField("call_date_time", TimestampType(), True),
        StructField("disposition", StringType(), True),
        StructField("address", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("agency_id", StringType(), True),
        StructField("address_type", StringType(), True),
        StructField("common_location", StringType(), True),
    ])

def run_spark_job(spark):

    """Spark Configuration
    # Set max offset of 200 per trigger
    # set up correct bootstrap server and port
    """
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "service.calls") \
        .option("startingOffsets", "earliest") \
        .option("spark.streaming.ui.retainedBatches","100")\
        .option("spark.streaming.ui.retainedStages","100")\
        .option("stopGracefullyOnShutdown", "true") \
        .load()
    
    """What does the schema look like?"""

    print("***********+++++Schema Def+++++*************")
    df.printSchema()
    print("***********+++++Schema Def+++++*************")
    
    """ Process inputs
    # Extract the correct column from the kafka input resources
    # Take only value and convert it to String
    """
    print("***********+++++df+++++*************")
    print(df)
    print("******************************")
    
    kafka_df = df.selectExpr("CAST(value AS STRING)")

    print("***********+++++kafka_df+++++*************")
    print(kafka_df)
    print("******************************")
    
    service_table = kafka_df\
        .select(psf.from_json(psf.col('value'), schema).alias("DF"))\
        .select("DF.*")
    
    print("***********+++++service_table+++++*************")
    print(service_table)
    print("******************************")
    
    distinct_table = service_table \
        .select('original_crime_type_name', 'disposition', 'call_date_time') \
        .distinct() \
        .withWatermark('call_date_time', "1 minute")
    
    print("***********+++++distinct_table+++++*************")
    print(distinct_table)
    print("******************************")
    
    """ How many distinct types of crimes can we find?"""
    
    agg_df = distinct_table\
        .select(psf.col("original_crime_type_name"), psf.col("call_date_time"), psf.col("disposition"))\
        .withWatermark("call_date_time", "60 minutes")\
        .groupBy(psf.window(distinct_table.call_date_time, "10 minutes", "5 minutes"),
                 psf.col("original_crime_type_name")).count()
    
    print("***********+++++distinct_table+++++*************")
    print(agg_df)
    print("******************************")


    """Write output stream"""
    query = agg_df \
            .writeStream \
            .format('console') \
            .outputMode('complete') \
            .trigger(processingTime="30 seconds") \
            .option("truncate", "false") \
            .start()
    
    """Get the right radio code json path"""
    time.sleep(30)
    query.awaitTermination()
    
    radio_code_json_filepath = "radio_code.json"
    radio_code_df = spark.read.json(radio_code_json_filepath, multiLine=True )

    """Clean the data to match the column names with radio radio_code_df and agg_df
     to facilitate oin on the disposition code"""

    """Rename disposition_code column to disposition"""
    radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")

    """Join on disposition column"""
    
    join_query = agg_df \
        .join(radio_code_df, col('agg_df.disposition') == col('radio_code_df.disposition'), 'left_outer')

    time.sleep(30)
    join_query.awaitTermination()


if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    """Create Spark in Standalone mode"""
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("KafkaSparkStructuredStreaming") \
        .config("spark.ui.port", 3000)\
        .getOrCreate()

    logger.info("Spark started")
    print("Running job now...")
    run_spark_job(spark)
    print("Ending job now...")
    spark.stop()
