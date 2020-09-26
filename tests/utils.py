from unittest import TestCase

from pyspark.sql import SparkSession


def sort_dataframe(dataframe, cols):
    return dataframe.sort_values(cols).reset_index(drop=True)


class SparkTestCase(TestCase):
    def setUp(self):
        self.spark = SparkSession.builder.appName(
            "Test"

        ).master("local").getOrCreate()

    def tearDown(self):
        self.spark.stop()
