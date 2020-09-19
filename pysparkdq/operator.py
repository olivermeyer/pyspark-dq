from __future__ import annotations

from typing import Tuple

from pyspark.sql.dataframe import DataFrame

from pysparkdq.validator import DataFrameValidator
from pysparkdq.checks.base import BaseCheck


class CheckOperator:
    def __init__(self, dataframe: DataFrame) -> None:
        self.dataframe = dataframe
        self.checks = []

    def add_check(self, check: BaseCheck) -> CheckOperator:
        self.checks.append(check)
        return self

    def run(self) -> Tuple[DataFrame, DataFrame]:
        return DataFrameValidator(
            dataframe=self.dataframe,
            checks=self.checks
        ).validate()
