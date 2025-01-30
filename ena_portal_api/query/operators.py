from enum import Enum

class ENAQueryOperators(str, Enum):
    OR = "OR"
    AND = "AND"
    NOT = "NOT"