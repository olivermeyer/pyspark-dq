from pytest import raises

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    BooleanType
)
from pyspark.sql.utils import AnalysisException

from pysparkdq.checks.is_in_values import ColumnIsInValuesCheck

from tests.utils import SparkTestCase


class ColumnIsInValuesCheckTest(SparkTestCase):
    def setUp(self):
        SparkTestCase.setUp(self)
        self.check = ColumnIsInValuesCheck("foo", ["a"])

    def test_validation_column(self):
        """Test the name of the validation column"""
        assert self.check.validation_column == "foo_is_in_allowed_values"

    def test_run(self):
        """Test expected execution"""
        input_df = self.spark.createDataFrame(
            data=[
                ("a",),
                ("b",),
            ],
            schema=StructType([
                StructField("foo", StringType()),
            ])
        )
        expected_df = self.spark.createDataFrame(
            data=[
                ("a", True),
                ("b", False),
            ],
            schema=StructType([
                StructField("foo", StringType()),
                StructField("foo_is_in_allowed_values", BooleanType(), False),
            ])
        )
        output_df = self.check.run(input_df)
        self.assert_frame_equal(expected_df, output_df)

    def test_run_missing_column(self):
        """Test that running on a missing field throws an error"""
        input_df = self.spark.createDataFrame(
            data=[
                ("a",),
            ],
            schema=StructType([
                StructField("bar", StringType()),
            ])
        )
        with raises(AnalysisException):
            self.check.run(input_df)
