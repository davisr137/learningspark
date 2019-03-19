import unittest2 as unittest
import findspark
findspark.init()
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

class TestCase(unittest.TestCase):
    """
    Shell for unit test cases. Includes set up and tear 
    down of Spark context and session.
    """
    @classmethod
    def setUpClass(cls):
        cls.conf = SparkConf().setAppName("UnitTestApp")
        cls.sc = SparkContext(conf=cls.conf)
        cls.spark = SparkSession.builder.appName("UnitTestApp").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.sc.stop()
        cls.spark.stop()
