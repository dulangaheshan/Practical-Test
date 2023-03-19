# Author: Dulanga Heshan
# test_main.py
import unittest
from src import main
from pyspark.sql import SparkSession

from src.utils import read_csv_file


class TestMain(unittest.TestCase):
    """
    @ todo - need to configure test files for the test cases
    """
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName("practical_test").getOrCreate()
        cls.df = read_csv_file(cls.spark, "test_csv.py")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def task2(self):
        result_df = main.task2(self.df)
        expected_df = read_csv_file(self.df, "expected_results_csv.py")
        self.assertEqual(result_df.collect(), expected_df.collect())


if __name__ == '__main__':
    unittest.main()
