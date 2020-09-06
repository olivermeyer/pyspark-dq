from abc import (
    ABC,
    abstractmethod
)

from pyspark.sql.dataframe import DataFrame


class BaseCheck(ABC):
    @property
    @abstractmethod
    def validation_column(self) -> str:
        raise NotImplementedError

    @abstractmethod
    def run(self, dataframe: DataFrame) -> DataFrame:
        raise NotImplementedError
