# Author: Dulanga Heshan
# test_main.py
import unittest
from src import main
from pyspark.sql import SparkSession


class TestMain(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName("test").getOrCreate()
        cls.df = cls.spark.createDataFrame([(1, 2), (3, 4)], ["col1", "col2"])

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def task2(self):
        result_df = main.task2(self.df, "col1", "col2", "output")
        expected_df = self.spark.createDataFrame([(1, 2, 2), (3, 4, 12)], ["col1", "col2", "output"])
        self.assertEqual(result_df.collect(), expected_df.collect())


if __name__ == '__main__':
    unittest.main()
