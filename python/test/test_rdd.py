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
        data = ['the', 'quick', 'brown', 'fox', 'jumped']
        dist_data = SC.parallelize(data)
        letters = dist_data.map(lambda s: len(s)).reduce(lambda a, b: a + b)
        self.assertTrue(letters, 22)

    def test_reduce_by_key(self):
        """
        Aggregate elements in RDD by key.
        """
        letters = SC.parallelize(['a', 'b', 'a', 'b', 'a'])
        pairs = letters.map(lambda s: (s, 1))
        counts = pairs.reduceByKey(lambda a, b: a + b)
        self.assertEqual(counts.collect(), [('b', 2), ('a', 3)])

    def test_accumulator(self):
        """
        Initialize an accumulator and increment it.
        """
        accum = SC.accumulator(0)
        SC.parallelize([1, 2, 3, 4, 5]).foreach(lambda x: accum.add(x))
        self.assertEqual(accum.value, 15)
