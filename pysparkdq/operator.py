from typing import Tuple

from pyspark.sql.dataframe import DataFrame

from pysparkdq.checks.base import BaseCheck


class CheckOperator:
    def __init__(
        self,
        dataframe: DataFrame,
    ) -> None:
        self.dataframe = dataframe
        self.checks = []
        self.validation_columns = []
        self.validated = False

    @property
    def invalid_rows(self) -> DataFrame:
        if not self.validated:
            raise RuntimeError(
                "Dataset has not yet been validated"
            )
        invalid_constraint_string = " or ".join(
            ["not {}".format(n) for n in self.validation_columns]
        )
        return self.dataframe.where(invalid_constraint_string)

    @property
    def valid_rows(self) -> DataFrame:
        if not self.validated:
            raise RuntimeError(
                "Dataset has not yet been validated"
            )
        valid_constraint_string = " and ".join(
            self.validation_columns
        )
        return self.dataframe.where(valid_constraint_string)

    def add_check(self, check: BaseCheck) -> None:
        self.checks.append(check)
        self.validation_columns.append(check.validation_column)

    def run_and_return(self) -> Tuple[DataFrame, DataFrame]:
        self.run_checks()
        return (
            self.valid_rows.drop(*self.validation_columns),
            self.invalid_rows
        )

    def run_checks(self) -> None:
        for check in self.checks:
            self.dataframe = check.run(self.dataframe)
        self.validated = True
