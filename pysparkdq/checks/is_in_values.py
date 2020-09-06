from pyspark.sql.functions import when
from pyspark.sql.dataframe import DataFrame

from pysparkdq.checks.base import BaseCheck


class ColumnIsInValuesCheck(BaseCheck):
    def __init__(self, column: str, allowed_values: list) -> None:
        self.column = column
        self.allowed_values = allowed_values

    @property
    def validation_column(self) -> str:
        return f"{self.column}_is_in_allowed_values"

    def run(self, dataframe: DataFrame) -> DataFrame:
        return dataframe.withColumn(
            self.validation_column,
            when(
                dataframe[self.column].isin(self.allowed_values),
                True
            ).otherwise(False)
        )
