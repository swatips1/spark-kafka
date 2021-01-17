1. How did changing values on the SparkSession property parameters affect the throughput and latency of the data?
The total time it took for the queries to executed changed based on the values or presence/absense of different parameters

2. What were the 2-3 most efficient SparkSession property key/value pairs? Through testing multiple variations on values, how can you tell these were the most optimal? 
# I played with following settings:
## "spark.streaming.ui.retainedBatches" : How many finished batches the Spark UI and status APIs remember before garbage collecting. With multiple experiment, it showed that setting the value to 100000 resulted into fastest query processing.

## "spark.streaming.ui.retainedBatches" :How many batches the Spark Streaming UI and status APIs remember before garbage collecting. With multiple experiment, it showed that setting the value to 100000 along with other settings resulted into fastest query processing.


# References: https://spark.apache.org/docs/latest/configuration.html