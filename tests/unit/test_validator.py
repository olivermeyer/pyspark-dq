import pytest

from pytest_mock import mock

from pyspark.sql import DataFrame

from pysparkdq.validator import DataFrameValidator
from pysparkdq.checks.base import BaseCheck


class FooCheck(BaseCheck):
    @property
    def validation_column(self) -> str:
        return "foo"

    def run(self, dataframe: DataFrame) -> DataFrame:
        pass


class BarCheck(BaseCheck):
    @property
    def validation_column(self) -> str:
        return "bar"

    def run(self, dataframe: DataFrame) -> DataFrame:
        pass


@pytest.fixture
def mock_dataframe():
    dataframe = mock.Mock()
    dataframe.columns.return_value = None
    return dataframe


@pytest.fixture
def validator(mock_dataframe):
    return DataFrameValidator(mock_dataframe, [FooCheck(), BarCheck()])


def test_valid_constraint_string(validator):
    assert validator._build_valid_constraint_string() == "foo and bar"


def test_invalid_constraint_string(validator):
    assert validator._build_invalid_constraint_string() == \
           "not foo or not bar"
