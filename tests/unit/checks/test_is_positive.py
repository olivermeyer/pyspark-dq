from pytest import raises

from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    BooleanType
)
from pyspark.sql.utils import AnalysisException

from pysparkdq.checks.is_positive import ColumnIsPositiveCheck

from tests.utils import SparkTestCase


class ColumnIsPositiveCheckTest(SparkTestCase):
    def setUp(self):
        SparkTestCase.setUp(self)
        self.check = ColumnIsPositiveCheck("foo")

    def test_validation_column(self):
        """Test the name of the validation column"""
        assert self.check.validation_column == "foo_is_positive"

    def test_run(self):
        """Test expected execution"""
        input_df = self.spark.createDataFrame(
            data=[
                (-1,),
                (0,),
                (1,),
                (None,)
            ],
            schema=StructType([
                StructField("foo", IntegerType()),
            ])
        )
        expected_df = self.spark.createDataFrame(
            data=[
                (-1, False),
                (0, True),
                (1, True),
                (None, False)
            ],
            schema=StructType([
                StructField("foo", IntegerType()),
                StructField("foo_is_positive", BooleanType(), False),
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
