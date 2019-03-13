import unittest2 as unittest
import pandas as pd
from config import spark

class TestSQL(unittest.TestCase):
    """
    Test Spark SQL.
    """
    @classmethod
    def setUpClass(cls):
        """
        Create a Spark DataFrame and enable SQL queries.
        """
        df = spark.createDataFrame([('Bob', 15), ('Bill', 20), ('Joe', 25)], ['name', 'age'])
        df.createOrReplaceTempView("people")

    def test_query(self):
        """
        Test SQL query on DataFrame.
        """
        df_sql = spark.sql("SELECT * FROM people WHERE age > 20")
        df_expected = pd.DataFrame(index=[0], columns=['name', 'age'], data=[['Joe', 25]])
        pd.testing.assert_frame_equal(df_sql.toPandas(), df_expected)
