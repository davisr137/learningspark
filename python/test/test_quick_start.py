import pandas as pd
import findspark
findspark.init()
from pyspark.sql import SparkSession

import helpers.unit_test as hut
import python.quick_start as qs

class TestQuickStart(hut.TestCase):
    """
    Test code from 'Quick Start' section exercises.
    """
    @classmethod
    def setUpClass(cls):
        """
        Initialize Spark context and read README.md file.
        """
        cls.spark = SparkSession.builder.appName("UnitTestQuickStart").getOrCreate()
        cls.text_file = qs.read_readme_file(cls.spark)

    def test_count(self):
        """
        Test row count.
        """
        self.assertEqual(self.text_file.count(), 105)
    
    def test_lines_with_spark(self):
        """
        Check how many lines contain 'Spark'.
        """
        lines_with_spark = self.text_file.filter(self.text_file.value.contains("Spark")) 
        self.assertEqual(lines_with_spark.count(), 20)

    def test_to_pandas(self):
        """
        Test toPandas() function.
        """
        df = self.text_file.toPandas()
        self.assertIsInstance(df, pd.DataFrame)

    @classmethod
    def tearDownClass(cls):
        """
        Stop Spark context.
        """
        cls.spark.stop()
