from pysparkdq.operator import CheckOperator
from pysparkdq.checks.is_in_range import ColumnIsInRangeCheck
from pysparkdq.checks.is_in_values import ColumnIsInValuesCheck
from pysparkdq.checks.is_not_null import ColumnIsNotNullCheck
from pysparkdq.checks.is_positive import ColumnIsPositiveCheck
from pysparkdq.checks.is_unique import ColumnSetIsUniqueCheck


from tests.utils import SparkTestCase


class IntegrationTest(SparkTestCase):
    def test(self):
        columns = [
                "id", "age", "country", "weight"
            ]
        input_df = self.spark.createDataFrame(
            data=[
                ("valid", 10, "DE", 10),
                (None, 6, "DE", 30),
                ("invalid_age", -1, "GB", 20),
                ("invalid_country", 35, "US", 13),
                ("invalid_weight", 4, "DE", 3),
                ("non_unique", 4, "GB", 13),
                ("non_unique", 6, "GB", 22),
                (None, -3, "CH", 45)
            ],
            schema=columns
        )
        check_operator = CheckOperator(
            dataframe=input_df
        ).add_check(
            ColumnIsNotNullCheck("id"),
        ).add_check(
            ColumnIsPositiveCheck("age")
        ).add_check(
            ColumnIsInValuesCheck(
                "country", ["DE", "GB"]
            )
        ).add_check(
            ColumnIsInRangeCheck(
                "weight", 10, 30
            )
        ).add_check(
            ColumnSetIsUniqueCheck(
                ["id", "country"]
            )
        )
        output_valid_df, output_invalid_df = check_operator.run()
        print(output_invalid_df.show())

        expected_valid_df = self.spark.createDataFrame(
            data=[
                ("valid", 10, "DE", 10)
            ],
            schema=columns
        )
        self.assert_frame_equal_sorted(
            output_valid_df, columns,
            expected_valid_df, columns
        )

        expected_invalid_df = self.spark.createDataFrame(
            data=[
                (None, 6, "DE", 30, False, True, True, True, True),
                ("invalid_age", -1, "GB", 20, True, False, True, True, True),
                ("invalid_country", 35, "US", 13, True, True, False, True, True),
                ("invalid_weight", 4, "DE", 3, True, True, True, False, True),
                ("non_unique", 4, "GB", 13, True, True, True, True, False),
                ("non_unique", 6, "GB", 22, True, True, True, True, False),
                (None, -3, "CH", 45, False, False, False, False, True)
            ],
            schema=columns + [
                "id_is_not_null",
                "age_is_positive",
                "country_is_in_allowed_values",
                "weight_is_in_range",
                "id_country_is_unique_identifier"
            ]
        )
        print(expected_invalid_df.show())
        self.assert_frame_equal_sorted(
            output_invalid_df, columns,
            expected_invalid_df, columns
        )

