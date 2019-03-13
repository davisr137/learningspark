import unittest2 as unittest
import pandas as pd
from config import spark
from pyspark.sql import Row
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

    def test_explode(self):
        """
        Return a new row for each element in a given array or map.
        """
        df = spark.createDataFrame([Row(a=1, intlist=[1,5,17], mapfield={"a": "b"})])
        l = df.select(explode(df.intlist).alias("anInt")).collect()
        values = [row.anInt for row in l]
        self.assertEqual(values, [1,5,17])

    def test_groupBy(self):
        """
        Count instances of word.
        """
        df = spark.createDataFrame([('hello',),('bonjour',),('hello',)], ['word'])
        df_ct = df.groupBy("word").count().toPandas()
        df_expected = pd.DataFrame(index=[0, 1], columns=['word', 'count'], data=[['hello', 2], ['bonjour', 1]])
        pd.testing.assert_frame_equal(df_ct, df_expected)
