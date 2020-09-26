from unittest import TestCase

import pandas as pd

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

    @staticmethod
    def assert_frame_equal(left, right):
        pd.testing.assert_frame_equal(
            left.toPandas(),
            right.toPandas()
        )

    @staticmethod
    def assert_frame_equal_sorted(left, left_cols, right, right_cols):
        pd.testing.assert_frame_equal(
            sort_dataframe(left.toPandas(), left_cols),
            sort_dataframe(right.toPandas(), right_cols),
            check_like=True
        )
