from typing import Tuple

from pyspark.sql.dataframe import DataFrame

from pysparkdq.validator import DataFrameValidator
from pysparkdq.checks.base import BaseCheck


class CheckOperator:
    def __init__(self, dataframe: DataFrame) -> None:
        self.dataframe = dataframe
        self.checks = []

    def add_check(self, check: BaseCheck) -> None:
        self.checks.append(check)

    def run(self) -> Tuple[DataFrame, DataFrame]:
        return DataFrameValidator(
            dataframe=self.dataframe,
            checks=self.checks
        ).validate()
