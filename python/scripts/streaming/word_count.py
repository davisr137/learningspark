import argparse
import logging
import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

def _main():
    # Parse input arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", "-port", type=int, default=9999)
    parser.add_argument("--log_level", "-log_level", type=str, default='INFO', choices=['INFO','DEBUG','WARNING','ERROR'])
    args = parser.parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level))
    # Initialize Spark Session
    spark = SparkSession.builder.appName("StructuredNetworkWordCount").getOrCreate()
    # Create DataFrame representing stream of input
    # lines from connection to localhost:9999
    lines = spark.readStream.format("socket").option("host", "localhost").option("port", args.port).load()
    # Split the lines into words
    words = lines.select(explode(split(lines.value, " ")).alias("word"))
    # Generate running word count
    wordCounts = words.groupBy("word").count()
    # Start running query that prints running counts to console
    query = wordCounts.writeStream.outputMode("complete").format("console").start()
    query.awaitTermination()

if __name__ == "__main__":
    _main()
