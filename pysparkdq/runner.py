from typing import (
    List,
    Tuple
)

from pyspark.sql.dataframe import DataFrame

from pysparkdq.checks.base import BaseCheck


class DataFrameValidator:
    def __init__(self, dataframe: DataFrame, checks: List[BaseCheck]):
        self.dataframe = dataframe
        self.checks = checks
        self.input_columns = dataframe.columns
        self.validation_columns = [
            check.validation_column for check in checks
        ]

    @property
    def invalid_rows(self) -> DataFrame:
        invalid_constraint_string = " or ".join(
            ["not {}".format(n) for n in self.validation_columns]
        )
        return self.dataframe.where(invalid_constraint_string)

    @property
    def valid_rows(self) -> DataFrame:
        valid_constraint_string = " and ".join(
            self.validation_columns
        )
        return self.dataframe.where(valid_constraint_string)

    def validate(self) -> Tuple[DataFrame, DataFrame]:
        for check in self.checks:
            self.dataframe = check.run(self.dataframe)
        return (
            self.valid_rows.select(self.input_columns),
            self.invalid_rows
        )
