from pytest import raises

from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    BooleanType
)
from pyspark.sql.utils import AnalysisException

from pysparkdq.checks.is_in_range import ColumnIsInRangeCheck

from tests.utils import SparkTestCase


class ColumnIsInRangeCheckTest(SparkTestCase):
    def setUp(self):
        SparkTestCase.setUp(self)
        self.check = ColumnIsInRangeCheck("foo", 1, 3)

    def test_validation_column(self):
        """Test the name of the validation column"""
        assert self.check.validation_column == "foo_is_in_range"

    def test_run(self):
        """Test expected execution"""
        input_df = self.spark.createDataFrame(
            data=[
                (0,),
                (1,),
                (2,),
                (3,),
                (4,)
            ],
            schema=StructType([
                StructField("foo", IntegerType()),
            ])
        )
        expected_df = self.spark.createDataFrame(
            data=[
                (0, False),
                (1, True),
                (2, True),
                (3, True),
                (4, False)
            ],
            schema=StructType([
                StructField("foo", IntegerType()),
                StructField("foo_is_in_range", BooleanType(), False),
            ])
        )
        output_df = self.check.run(input_df)
        self.assert_frame_equal(expected_df, output_df)

    def test_run_missing_column(self):
        """Test that running on a missing field throws an error"""
        input_df = self.spark.createDataFrame(
            data=[
                (1,),
            ],
            schema=StructType([
                StructField("bar", IntegerType()),
            ])
        )
        with raises(AnalysisException):
            self.check.run(input_df)
