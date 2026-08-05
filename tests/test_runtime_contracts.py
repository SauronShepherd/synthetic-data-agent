import pytest

from sda.runtime.errors import InvalidRequestError
from sda.runtime.identifiers import QualifiedName


def test_qualified_name_is_validated_and_quoted() -> None:
    name = QualifiedName.parse("main.sales.orders")
    assert name.quoted == "`main`.`sales`.`orders`"


def test_qualified_name_rejects_injection_and_wildcards() -> None:
    with pytest.raises(InvalidRequestError):
        QualifiedName.parse("main.sales.orders;DROP TABLE x")
    with pytest.raises(InvalidRequestError):
        QualifiedName.parse("main.*.orders")
