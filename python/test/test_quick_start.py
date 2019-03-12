import unittest2 as unittest
import pandas as pd

import python.quick_start as qs

class TestQuickStart(unittest.TestCase):
    """
    Test code from 'Quick Start' section exercises.
    """
    @classmethod
    def setUpClass(cls):
        """
        Read README.md file.
        """ 
        cls.textFile = qs.read_readme_file()

    def test_count(self):
        """
        Test row count.
        """
        self.assertEqual(self.textFile.count(), 105)
    
    def test_lines_with_spark(self):
        """
        Check how many lines contain 'Spark'.
        """
        linesWithSpark = self.textFile.filter(self.textFile.value.contains("Spark")) 
        self.assertEqual(linesWithSpark.count(), 20)

    def test_to_pandas(self):
        """
        Test toPandas() function.
        """
        df = self.textFile.toPandas()
        self.assertIsInstance(df, pd.DataFrame)
