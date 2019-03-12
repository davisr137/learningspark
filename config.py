import findspark
findspark.init()
from pyspark.sql import SparkSession

HOME = '/usr/local/Cellar/apache-spark/2.4.0/'
spark = SparkSession.builder.appName("SparkExercises").getOrCreate()
