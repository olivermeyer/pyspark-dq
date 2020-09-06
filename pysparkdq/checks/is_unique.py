from typing import List

from pyspark.sql.functions import when
from pyspark.sql.dataframe import DataFrame

from pysparkdq.checks.base import BaseCheck


class ColumnSetIsUniqueCheck(BaseCheck):
    def __init__(self, columns: List[str]) -> None:
        self.columns = columns

    @property
    def validation_column(self) -> str:
        return f"{'_'.join(self.columns)}_is_unique_identifier"

    def run(self, dataframe: DataFrame) -> DataFrame:
        incoming_columns = dataframe.columns
        non_unique = dataframe.groupBy(
            *self.columns
        ).count().where("count > 1")
        dataframe = dataframe.join(
            other=non_unique,
            on=self.columns,
            how="left"
        )
        return dataframe.withColumn(
            self.validation_column,
            when(
                dataframe["count"].isNull(),
                True
            ).otherwise(False)
        ).select(
            incoming_columns + [self.validation_column]
        )
