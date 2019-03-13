import unittest2 as unittest
import findspark
findspark.init()
from pyspark import SparkContext, SparkConf

CONF = SparkConf().setAppName("TestRDDApp")
SC = SparkContext(conf=CONF)

class TestRDD(unittest.TestCase):
    """
    Test Spark Resilient Distributed Dataset (RDD) functionality.
    """
    def test_map_reduce(self):
        """
        Compute total letters in list of words using map/reduce.
        """
        data=['the','quick','brown','fox','jumped']
        distData = SC.parallelize(data)
        letters = distData.map(lambda s: len(s)).reduce(lambda a, b: a + b)
        self.assertTrue(letters, 22)
