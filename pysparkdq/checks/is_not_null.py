from pyspark.sql.functions import when
from pyspark.sql.dataframe import DataFrame

from pysparkdq.checks.base import BaseCheck


class ColumnIsNotNullCheck(BaseCheck):
    def __init__(self, column: str) -> None:
        self.column = column

    @property
    def validation_column(self) -> str:
        return f"{self.column}_is_not_null"

    def run(self, dataframe: DataFrame) -> DataFrame:
        dataframe = dataframe.withColumn(
            self.validation_column,
            when(
                dataframe[self.column].isNotNull(),
                True
            ).otherwise(False)
        )
        return dataframe
