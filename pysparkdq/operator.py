from __future__ import annotations

from typing import Tuple

from pyspark.sql.dataframe import DataFrame

from pysparkdq.validator import DataFrameValidator
from pysparkdq.checks.base import BaseCheck
from pysparkdq.exceptions import (
    InvalidCheckTypeException,
    InvalidDataFrameException
)


class CheckOperator:
    def __init__(self, dataframe: DataFrame) -> None:
        if not isinstance(dataframe, DataFrame):
            raise InvalidDataFrameException(
                "Not a pyspark.sql.dataframe.DataFrame: {}".format(
                    dataframe
                )
            )
        self.dataframe = dataframe
        self.checks = []

    def add_check(self, check: BaseCheck) -> CheckOperator:
        if not isinstance(check, BaseCheck):
            raise InvalidCheckTypeException(
                "Not a child of pysparkdq.checks.base.BaseCheck: {}".format(
                    type(check)
                )
            )
        self.checks.append(check)
        return self

    def run(self) -> Tuple[DataFrame, DataFrame]:
        return DataFrameValidator(
            dataframe=self.dataframe,
            checks=self.checks
        ).validate()
