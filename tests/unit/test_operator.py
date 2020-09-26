import pytest

from pyspark.sql import DataFrame
from pysparkdq.operator import CheckOperator
from pysparkdq.checks.is_not_null import ColumnIsNotNullCheck
from pysparkdq.checks.is_not_negative import ColumnIsNotNegativeCheck
from pysparkdq.exceptions import (
    InvalidCheckTypeException,
    InvalidDataFrameException
)


def test_create_invalid_dataframe():
    with pytest.raises(InvalidDataFrameException):
        CheckOperator(dataframe="foo")


@pytest.fixture
def check_operator():
    return CheckOperator(dataframe=DataFrame(None, None))


def test_add_check(check_operator):
    check_operator.add_check(ColumnIsNotNullCheck("foo"))
    check_operator.add_check(ColumnIsNotNegativeCheck("bar"))
    assert len(check_operator.checks) == 2


def test_add_invalid_check_type(check_operator):
    with pytest.raises(InvalidCheckTypeException):
        check_operator.add_check("foo")
