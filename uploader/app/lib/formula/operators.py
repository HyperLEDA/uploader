from dataclasses import dataclass
from typing import final


@final
@dataclass(frozen=True)
class OperatorDef:
    name: str
    detail: str


OPERATORS: tuple[OperatorDef, ...] = (
    OperatorDef("+", "Addition; also concatenates strings"),
    OperatorDef("-", "Subtraction"),
    OperatorDef("*", "Multiplication"),
    OperatorDef("/", "Division"),
    OperatorDef("**", "Exponentiation"),
    OperatorDef("%", 'Modulo; divisor must carry units (e.g. col("pa") % (180 * deg))'),
    OperatorDef("==", "Equal"),
    OperatorDef("!=", "Not equal"),
    OperatorDef("<", "Less than"),
    OperatorDef("<=", "Less than or equal"),
    OperatorDef(">", "Greater than"),
    OperatorDef(">=", "Greater than or equal"),
)
