import argparse
import logging
import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

def _main():
    # Parse input arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", "-port", type=int, default=9999)
    parser.add_argument("--log_level", "-log_level", type=str, default='INFO', choices=['INFO','DEBUG','WARNING','ERROR'])
    parser.add_argument("--read_path", "-read_path", type=str, default='/Users/ryandavis/repos/learnspark/data/streaming')
    args = parser.parse_args()
    # Create Spark session
    spark = SparkSession.builder.getOrCreate()
    socket_df = spark.readStream.format("socket").option("host", "localhost").option("port", args.port).load()
    # Define schema
    user_schema = StructType().add("name", "string").add("age", "integer")
    # Read dataframe
    df = spark.readStream.option("sep", ",").schema(user_schema).csv(args.read_path)
    # Count names
    name_count = df.groupBy('name').count()
    # Print to console
    query = name_count.writeStream.outputMode("complete").format("console").start()
    query.awaitTermination()

if __name__ == "__main__":
    _main()
