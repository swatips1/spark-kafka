import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf


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
        .option("kafka.bootstrap.servers", "localhost:9093") \
        .option("subscribe", "service.calls") \
        .option("spark.streaming.ui.retainedBatches","100000")\
        .option("spark.streaming.ui.retainedStages","100000")\
        .option("stopGracefullyOnShutdown", "true") \
        .load()
#         .option("autoBroadcastJoinThreshold", "10000")\
#         .option("spark.sql.shuffle.partitions", 1000)\
#         .option("stopGracefullyOnShutdown", "true") \
#         .load()

#     df = spark \
#         .readStream \
#         .format("kafka") \
#         .option("kafka.bootstrap.servers", "localhost:9093") \
#         .option("subscribe", "service.calls") \
#         #.option("spark.streaming.ui.retainedBatches","1000")\
#         #.option("startingOffsets", "earliest") \
#         #.option("maxOffsetsPerTrigger", 200) \
#         #.option("maxRatePerPartition", 1000)\
#         #.option("spark.sql.inMemoryColumnarStorage.batchSize", 100000)\
#         #.option("spark.sql.shuffle.partitions", 1000)\
#         .option("stopGracefullyOnShutdown", "true") \
#         .load()
    
    """What does the schema look like?"""
    df.printSchema()

    """ Process inputs
    # Extract the correct column from the kafka input resources
    # Take only value and convert it to String
    """
    kafka_df = df.selectExpr("CAST(value AS STRING)")

    service_table = kafka_df\
        .select(psf.from_json(psf.col('value'), schema).alias("DF"))\
        .select("DF.*")

    """select original_crime_type_name and disposition"""
    distinct_table = service_table.select('original_crime_type_name',
                                          'disposition','call_date_time').distinct()

    """ How many distinct types of crimes can we find?"""
    agg_df =  distinct_table \
        .dropna() \
        .select('original_crime_type_name') \
        .groupby('original_crime_type_name') \
        .agg({'original_crime_type_name':'count'})

    """Write output stream"""
    query = agg_df \
        .writeStream \
        .format('console') \
        .outputMode('Complete') \
        .trigger(processingTime="15 seconds")\
        .start()

    query.awaitTermination()

    """Get the right radio code json path"""
    radio_code_json_filepath = "radio_code.json"
    radio_code_df = spark.read.json(radio_code_json_filepath)

    """Clean the data to match the column names with radio radio_code_df and agg_df
    to facilitate oin on the disposition code"""

    """Rename disposition_code column to disposition"""
    radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")

    """Join on disposition column"""
    join_query = agg_df.join(radio_code_df, col('agg_df.disposition') == col("radio_code_df.disposition"),  "inner")
    

    join_query.awaitTermination()


if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    # TODO Create Spark in Standalone mode
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("KafkaSparkStructuredStreaming") \
        .config("spark.ui.port", 3000)\
        .getOrCreate()

    logger.info("Spark started")

    run_spark_job(spark)

    spark.stop()
