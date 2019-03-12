import os
import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

HOME = '/usr/local/Cellar/apache-spark/2.4.0/'
SPARK = SparkSession.builder.appName("SimpleApp").getOrCreate()

def read_readme_file():
    textFile = SPARK.read.text(os.path.join(HOME, "README.md"))
    return textFile
