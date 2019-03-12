import unittest2 as unittest
from config import spark
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
        df = spark.createDataFrame([('abc127def',)], ['s',])
        words = df.select(split(df.s, '[0-9]+').alias('s')).collect()[0]
        self.assertEqual(words.s, ['abc', 'def'])
        # Split on spaces
        df = spark.createDataFrame([('the quick brown fox',)], ['s',])
        words = df.select(split(df.s, '\s+').alias('s')).collect()[0]
        self.assertEqual(words.s, ['the', 'quick', 'brown', 'fox'])
    
    def test_size(self):
        """
        Test 'size' function.
        """
        df = spark.createDataFrame([([1, 2],),([1],),([7, 8],)], ['data'])
        rows = df.select(size(df.data).name("Len")).collect()
        rl = [r.Len for r in rows]
        self.assertEqual(rl, [2, 1, 2])
