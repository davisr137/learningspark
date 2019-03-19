import argparse
import logging
import time
import findspark
findspark.init()
import pandas as pd
from scipy import stats
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, rand
from pyspark.sql.functions import udf, pandas_udf, PandasUDFType

def _main():
    # Parse input arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", "-rows", type=int, default=10**5)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    # Initilaize Spark session
    conf = SparkConf()
    conf.set('spark.sql.execution.arrow.enabled', True)
    conf.set('spark.sql.execution.arrow.fallback.enabled', False)
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    # Build dataframe
    logging.info('Build Spark DataFrame with %s rows.' % args.rows)
    df = spark.range(0, args.rows).withColumn('id', (col('id') / 10000).cast('integer')).withColumn('v', rand())
    df.cache()
    # Row-at-a-time UDF
    @udf('double')
    def cdf(v):
        return float(stats.norm.cdf(v))
    logging.info('Applying row-at-a-time UDF.')
    t=time.time()
    df.withColumn('cumulative_probability', cdf(df.v)).collect()
    el=time.time()-t
    logging.info('Row-at-a-time UDF time elapsed: %s seconds' % el)
    # Pandas UDF
    @pandas_udf('double', PandasUDFType.SCALAR)
    def pandas_cdf(v):
        return pd.Series(stats.norm.cdf(v))
    logging.info('Applying Pandas UDF.')
    t=time.time()
    df.withColumn('cumulative_probability', pandas_cdf(df.v)).collect()
    el=time.time()-t
    logging.info('Pandas UDF time elapsed: %s seconds' % el)
    # Release dataframe from cache
    df.unpersist()
    spark.stop()

if __name__ == "__main__":
    _main()
