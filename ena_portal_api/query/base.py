from datetime import date
from typing import Union, TypeVar
from pydantic import BaseModel, Field, computed_field
from typing_extensions import Self

from .operators import ENAQueryOperators


class ENAQueryClause(BaseModel):
    search_field: str
    value: Union[str, int, date]
    is_not: bool = Field(default=False)

    def __str__(self):
        value = self.value
        if isinstance(value, date):
            value = value.strftime("%Y-%m-%d")
        return f"{ENAQueryOperators.NOT if self.is_not else ''} {self.search_field}={value}".strip()

    def __or__(self, other: Union["Self", "ENAQueryPair"]) -> "ENAQueryPair":
        return ENAQueryPair(left=self, operator=ENAQueryOperators.OR, right=other)

    def __and__(self, other: Union["Self", "ENAQueryPair"]) -> "ENAQueryPair":
        return ENAQueryPair(left=self, operator=ENAQueryOperators.AND, right=other)

    def __invert__(self) -> "Self":
        return ENAQueryClause(
            search_field=self.search_field, value=self.value, is_not=not self.is_not
        )


QueryType = TypeVar("QueryType", bound="BaseENAQueryConditions")


class ENAQueryPair(BaseModel):
    operator: ENAQueryOperators = Field(ENAQueryOperators.AND)
    left: Union[ENAQueryClause, "ENAQueryPair", QueryType]
    right: Union[ENAQueryClause, "ENAQueryPair", QueryType]
    is_not: bool = Field(default=False)

    def __str__(self):
        return f"{ENAQueryOperators.NOT + ' ' if self.is_not else ''}({str(self.left)} {self.operator.value} {str(self.right)})"

    def __or__(
        self, other: Union[ENAQueryClause, "ENAQueryPair", QueryType]
    ) -> "ENAQueryPair":
        return ENAQueryPair(left=self, operator=ENAQueryOperators.OR, right=other)

    def __and__(
        self, other: Union[ENAQueryClause, "ENAQueryPair", QueryType]
    ) -> "ENAQueryPair":
        return ENAQueryPair(left=self, operator=ENAQueryOperators.AND, right=other)

    def __invert__(self) -> "ENAQueryPair":
        return ENAQueryPair(
            left=self.left,
            operator=self.operator,
            right=self.right,
            is_not=not self.is_not,
        )


class BaseENAQueryConditions(BaseModel):
    """Base class for all ENA query condition models"""

    is_not: bool = Field(default=False)

    @computed_field
    @property
    def queries(self) -> Union[ENAQueryPair, ENAQueryClause]:
        clauses = None
        for search_field, value in self.model_dump(
            exclude={"is_not", "queries"}
        ).items():
            if value is None:
                continue
            if clauses:
                clauses &= ENAQueryClause(search_field=search_field, value=value)
            else:
                clauses = ENAQueryClause(search_field=search_field, value=value)
        return clauses

    def __str__(self):
        return str(self.queries)

    def __or__(
        self, other: Union[ENAQueryClause, ENAQueryPair, "BaseENAQueryConditions"]
    ) -> ENAQueryPair:
        return ENAQueryPair(left=self, operator=ENAQueryOperators.OR, right=other)

    def __and__(
        self, other: Union[ENAQueryClause, ENAQueryPair, "BaseENAQueryConditions"]
    ) -> ENAQueryPair:
        return ENAQueryPair(left=self, operator=ENAQueryOperators.AND, right=other)

    def __invert__(self) -> "BaseENAQueryConditions":
        already_set = self.model_dump(exclude={"is_not"})
        return self.__class__(**already_set, is_not=not self.is_not)
