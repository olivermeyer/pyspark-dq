import pandas as pd

from pytest import raises

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    BooleanType
)
from pyspark.sql.utils import AnalysisException

from pysparkdq.checks.is_unique import ColumnSetIsUniqueCheck

from tests.utils import (
    SparkTestCase,
    sort_dataframe
)


class ColumnSetIsUniqueCheckTest(SparkTestCase):
    def setUp(self):
        SparkTestCase.setUp(self)
        self.check = ColumnSetIsUniqueCheck(["foo", "bar"])

    def test_validation_column(self):
        """Test the name of the validation column"""
        assert self.check.validation_column == "foo_bar_is_unique_identifier"

    def test_run(self):
        """Test expected execution"""
        input_df = self.spark.createDataFrame(
            data=[
                ("a", 1),
                ("a", 2),
                ("b", 3),
                ("b", 3),
            ],
            schema=StructType([
                StructField("foo", StringType()),
                StructField("bar", StringType()),
            ])
        )
        expected_df = self.spark.createDataFrame(
            data=[
                ("a", 1, True),
                ("a", 2, True),
                ("b", 3, False),
                ("b", 3, False),
            ],
            schema=StructType([
                StructField("foo", StringType()),
                StructField("bar", StringType()),
                StructField(
                    "foo_bar_is_unique_identifier", BooleanType(), False
                ),
            ])
        ).toPandas()
        output_df = self.check.run(input_df).toPandas()
        pd.testing.assert_frame_equal(
            sort_dataframe(expected_df, ["foo", "bar"]),
            sort_dataframe(output_df, ["foo", "bar"]),
            check_like=True
        )

    def test_run_missing_column(self):
        """Test that running on a missing field throws an error"""
        input_df = self.spark.createDataFrame(
            data=[
                ("a",),
            ],
            schema=StructType([
                StructField("baz", StringType()),
            ])
        )
        with raises(AnalysisException):
            self.check.run(input_df)
