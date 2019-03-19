import argparse
import logging
import time
import findspark
findspark.init()
import pandas as pd
from scipy import stats
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import *
from pyspark.sql.functions import col, count, rand, collect_list, explode, struct, count, lit
from pyspark.sql.functions import udf, pandas_udf, PandasUDFType

def _main():
    # Parse input arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", "-rows", type=int, default=10**5)
    parser.add_argument("--log_level", "-log_level", type=str, default='INFO', choices=['INFO','DEBUG','WARNING','ERROR'])
    args = parser.parse_args()
    logging.basicConfig(level=args.log_level)
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
    @udf(ArrayType(df.schema))
    def substract_mean(rows):
        vs = pd.Series([r.v for r in rows])
        vs = vs - vs.mean()
        return [Row(id=rows[i]['id'], v=float(vs[i])) for i in range(len(rows))]
    logging.info('Applying row-at-a-time UDF.')
    t = time.time()
    df.groupby('id').agg(collect_list(struct(df['id'], df['v'])).alias('rows')).withColumn('new_rows', substract_mean(col('rows'))).withColumn('new_row', explode(col('new_rows'))).withColumn('id', col('new_row.id')).withColumn('v', col('new_row.v')).agg(count(col('v'))).show()
    el = time.time()-t 
    logging.info('Row-at-a-time UDF time elapsed: %s seconds' % el)
    # Pandas UDF
    @pandas_udf(df.schema, PandasUDFType.GROUPED_MAP)
    def pandas_subtract_mean(pdf):
        return pdf.assign(v=pdf.v - pdf.v.mean())
    t = time.time()
    df.groupby('id').apply(pandas_subtract_mean).agg(count(col('v'))).show()
    el = time.time()-t
    logging.info('Pandas UDF time elapsed: %s seconds' % el)
    df.unpersist()
    spark.stop()

if __name__ == "__main__":
    _main()
