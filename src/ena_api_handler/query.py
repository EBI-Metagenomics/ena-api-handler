from __future__ import annotations

from enum import Enum

from pydantic import BaseModel


class ENAQueryOperator(str, Enum):
    AND = "AND"
    OR = "OR"


class ENAQueryClause:
    """Base for composable ENA Portal API query elements."""

    def to_query_string(self) -> str:
        raise NotImplementedError

    def leaves(self) -> list[ENAQueryClause]:
        return [self]

    def __and__(self, other: ENAQueryClause) -> ENAQueryPair:
        return ENAQueryPair(self, other, ENAQueryOperator.AND)

    def __or__(self, other: ENAQueryClause) -> ENAQueryPair:
        return ENAQueryPair(self, other, ENAQueryOperator.OR)

    def __invert__(self) -> ENAQueryNot:
        return ENAQueryNot(self)


class ENAQueryPair(ENAQueryClause):
    """Two query clauses joined by AND or OR."""

    def __init__(
        self,
        left: ENAQueryClause,
        right: ENAQueryClause,
        operator: ENAQueryOperator,
    ) -> None:
        self.left = left
        self.right = right
        self.operator = operator

    def leaves(self) -> list[ENAQueryClause]:
        return self.left.leaves() + self.right.leaves()

    def to_query_string(self) -> str:
        return (
            f"({self.left.to_query_string()}"
            f" {self.operator.value}"
            f" {self.right.to_query_string()})"
        )


class ENAQueryNot(ENAQueryClause):
    """Negation of a query clause."""

    def __init__(self, clause: ENAQueryClause) -> None:
        self.clause = clause

    def leaves(self) -> list[ENAQueryClause]:
        return self.clause.leaves()

    def to_query_string(self) -> str:
        return f"NOT {self.clause.to_query_string()}"


class ENARawQuery(ENAQueryClause):
    """
    A raw ENA Portal API query string, bypassing the typed query model system.

    Use this for query expressions not supported by ``ENABaseQuery``, such as
    date-range comparisons or complex hand-written queries::

        ENARawQuery("last_updated>=2024-01-01")
        ENARawQuery('assembly_type="primary metagenome"') & ENAReadRunQuery(study_accession="PRJEB1234")
    """

    def __init__(self, raw: str) -> None:
        self._raw = raw

    def to_query_string(self) -> str:
        return self._raw


class ENABaseQuery(BaseModel, ENAQueryClause):
    """
    Base class for generated per-portal Query models.

    All fields set to a non-None value are combined with AND into a query
    string suitable for the ENA Portal API ``query`` parameter.

    Supports ``&`` (AND), ``|`` (OR), and ``~`` (NOT) operators for
    composing complex queries::

        q = ENAReadRunQuery(study_accession="PRJEB1234") & ENAReadRunQuery(library_strategy="AMPLICON")
        q = ENAReadRunQuery(study_accession="PRJEB1234") | ENAReadRunQuery(secondary_study_accession="ERP123")
        q = ~ENAReadRunQuery(library_strategy="AMPLICON")
    """

    def to_query_string(self) -> str:
        parts = [
            f'{field}="{value}"'
            for field, value in self.model_dump(exclude_none=True).items()
        ]
        return " AND ".join(parts)


# TODO: Add a 'timedelta' based query class
