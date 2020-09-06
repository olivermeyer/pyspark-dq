from pyspark.sql.functions import when
from pyspark.sql.dataframe import DataFrame

from pysparkdq.checks.base import BaseCheck


class ColumnIsInRangeCheck(BaseCheck):
    def __init__(self, column: str, lower: float, upper: float) -> None:
        self.column = column
        self.lower = lower
        self.upper = upper

    @property
    def validation_column(self) -> str:
        return f"{self.column}_is_in_range"

    def run(self, dataframe: DataFrame) -> DataFrame:
        return dataframe.withColumn(
            self.validation_column,
            when(
                dataframe[self.column].between(self.lower, self.upper),
                True
            ).otherwise(False)
        )
