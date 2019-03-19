import os
import shutil
import numpy as np
import pandas as pd
import findspark
findspark.init()
from pyspark.sql import Row
from config import REPO_PATH

import python.helpers.unit_test as hut

class TestParquet(hut.TestCase):
    """
    Test Parquet utilities.
    """
    @staticmethod
    def _format_df(df):
        """
        Utility for formatting dataframe
        """
        df = df.sort_values('single')
        df.index = range(len(df))
        return df

    def test_schema_merge(self):
        """
        Merge schemas of two Parquet tables.
        """
        if os.path.isdir("%s/data/test_table" % REPO_PATH):
            shutil.rmtree("%s/data/test_table" % REPO_PATH)
            #os.mkdir("%s/data/test_table" % REPO_PATH)
        # Squares
        df_squares = self.spark.createDataFrame(self.sc.parallelize(range(1, 4)).map(lambda i: Row(single=i, double=i**2)))
        df_squares.write.parquet("%s/data/test_table/key=1" % REPO_PATH)
        # Cubes
        df_cubes = self.spark.createDataFrame(self.sc.parallelize(range(4, 7)).map(lambda i: Row(single=i, triple=i**3)))
        df_cubes.write.parquet("%s/data/test_table/key=2" % REPO_PATH)
        # Merge two schemas into a single table
        df_merged = self.spark.read.option("mergeSchema", "true").parquet("%s/data/test_table" % REPO_PATH)
        # Check against expected result
        data_dict = {
            'double' : [np.nan, np.nan, np.nan, 4.0, 9.0, 1.0],
            'single' : [5, 6, 4, 2, 3, 1],
            'triple' : [125.0, 216.0, 64.0, np.nan, np.nan, np.nan],
            'key' : [2, 2, 2, 1, 1, 1],
        }
        df_expected = pd.DataFrame.from_dict(data_dict)
        df_expected = TestParquet._format_df(df_expected)
        df_merged_pd = TestParquet._format_df(df_merged.toPandas())
        pd.testing.assert_frame_equal(df_merged_pd, df_expected, check_dtype=False)
        # Remove directory with test data
        shutil.rmtree("%s/data/test_table" % REPO_PATH)
