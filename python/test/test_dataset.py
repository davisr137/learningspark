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
        df = config.spark.createDataFrame([('abc127def',)], ['s',])
        row = df.select(split(df.s, '[0-9]+').alias('s')).collect()
        row = row[0]
        self.assertEqual(row.s, ['abc', 'def'])
