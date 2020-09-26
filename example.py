from pyspark.sql import SparkSession

from pysparkdq.operator import CheckOperator
from pysparkdq.checks.is_not_null import ColumnIsNotNullCheck
from pysparkdq.checks.is_not_negative import ColumnIsNotNegativeCheck
from pysparkdq.checks.is_in_values import ColumnIsInValuesCheck
from pysparkdq.checks.is_in_range import ColumnIsInRangeCheck
from pysparkdq.checks.is_unique import ColumnSetIsUniqueCheck


if __name__ == "__main__":
    spark = SparkSession.builder.appName(
        "PySpark-DQ-Check"
    ).getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    df = spark.createDataFrame(
        data=[
            ("valid", 10, "DE", 10),
            (None, 6, "DE", 30),
            ("invalid_age", -1, "GB", 20),
            ("invalid_country", 35, "US", 13),
            ("invalid_weight", 4, "DE", 3),
            ("non_unique", 4, "GB", 13),
            ("non_unique", 6, "GB", 22),
            (None, -3, "CH", 45)  # all wrong
        ],
        schema=[
            "id", "age", "country", "weight"
        ]
    )
    check_operator = CheckOperator(
        dataframe=df
    ).add_check(
        ColumnIsNotNullCheck("id"),
    ).add_check(
        ColumnIsNotNegativeCheck("age")
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
    valid_df, invalid_df = check_operator.run()
    # do something with valid_df and invalid_df
    print(valid_df.show())
    print(invalid_df.show())
    spark.stop()
