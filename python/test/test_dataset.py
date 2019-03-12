import unittest2 as unittest
import config
from pyspark.sql.functions import *

class TestDataset(unittest.TestCase):
    """
    Test basic Spark Dataset functionality.
    """
    def test_split(self):
        """
        Test 'split' function.
        """
        # Split on numeric characters
        df = config.spark.createDataFrame([('abc127def',)], ['s',])
        words = df.select(split(df.s, '[0-9]+').alias('s')).collect()[0]
        self.assertEqual(words.s, ['abc', 'def'])
        # Split on spaces
        df = config.spark.createDataFrame([('the quick brown fox',)], ['s',])
        words = df.select(split(df.s, '\s+').alias('s')).collect()[0]
        self.assertEqual(words.s, ['the', 'quick', 'brown', 'fox'])
