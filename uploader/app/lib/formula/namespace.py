from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Literal, TypedDict, final

import astropy.constants as const
import astropy.units as u
import numpy as np
from astropy.coordinates import Angle

from uploader.app.lib.formula.values import TextValue, Value


@final
@dataclass(frozen=True)
class FunctionDef:
    name: str
    detail: str
    impl: object | None = None
    placeholder: str = "${1:x}"

    @property
    def insert(self) -> str:
        return f"{self.name}({self.placeholder})"

    @property
    def signature(self) -> str:
        args = self.placeholder.replace("${1:", "").replace("}", "")
        return f"{self.name}({args})"


@final
@dataclass(frozen=True)
class ConstantDef:
    name: str
    value: u.Quantity
    detail: str

    @property
    def insert(self) -> str:
        return self.name


@final
@dataclass(frozen=True)
class OperatorDef:
    name: str
    detail: str


NAMED_CONSTANTS: tuple[ConstantDef, ...] = (
    ConstantDef("pi", np.pi * u.dimensionless_unscaled, "Pi"),
    ConstantDef("c", const.c, "Speed of light"),
    ConstantDef("deg", 1 * u.deg, "Degree"),
    ConstantDef("rad", 1 * u.rad, "Radian"),
    ConstantDef("arcmin", 1 * u.arcmin, "Arcminute"),
    ConstantDef("arcsec", 1 * u.arcsec, "Arcsecond"),
    ConstantDef("mag", 1 * u.mag, "Magnitude"),
)


OPERATORS: tuple[OperatorDef, ...] = (
    OperatorDef("+", "Addition; also concatenates strings"),
    OperatorDef("-", "Subtraction"),
    OperatorDef("*", "Multiplication"),
    OperatorDef("/", "Division"),
    OperatorDef("**", "Exponentiation"),
    OperatorDef("%", 'Modulo; divisor must carry units (e.g. col("pa") % (180 * deg))'),
)


def _scalar_to_str(value: float | int | np.number) -> str:
    numeric = float(value)
    if numeric.is_integer():
        return str(int(numeric))
    return str(numeric)


def _formula_str(value: Value) -> str | np.ndarray:
    if isinstance(value, TextValue):
        return value.data
    if isinstance(value, str):
        return value
    if isinstance(value, u.Quantity):
        scalar = value.value
        if isinstance(scalar, np.ndarray):
            return np.asarray([_scalar_to_str(x) for x in scalar])
        return _scalar_to_str(scalar)
    return np.asarray([_scalar_to_str(x) for x in value])


def _to_deg(value: object) -> u.Quantity:
    if isinstance(value, TextValue):
        angle = Angle(value.data, unit=u.Unit(value.unit)) if value.unit else Angle(value.data)
        return angle.to(u.deg)
    if isinstance(value, str):
        return Angle(value).to(u.deg)
    if isinstance(value, u.Quantity):
        return value.to(u.deg)
    if isinstance(value, np.ndarray):
        return u.Quantity([_to_deg(item).value for item in value], unit=u.deg)
    raise TypeError(f"to_deg() expected angle or coordinate string, got {type(value).__name__}")


def _unit(name: object) -> u.Quantity:
    if not isinstance(name, str):
        raise TypeError(f"unit() expected a unit name string, got {type(name).__name__}")
    return 1 * u.Unit(name)


COL_FUNCTION = FunctionDef("col", "Rawdata column", placeholder='"${1:name}"')

FUNCTIONS: tuple[FunctionDef, ...] = (
    FunctionDef("sin", "Sine (argument must be an angle)", np.sin),
    FunctionDef("cos", "Cosine (argument must be an angle)", np.cos),
    FunctionDef("sqrt", "Square root", np.sqrt),
    FunctionDef("str", "Convert to text", _formula_str),
    FunctionDef(
        "to_deg",
        'Convert to degrees; e.g. "00 02 08.4" (hourangle), "+16 35 13" (deg), "00h02m08.4s"',
        _to_deg,
    ),
    FunctionDef(
        "unit",
        'Astropy unit from a name string; e.g. "Mpc", "km/s", "Jy"',
        _unit,
        placeholder='"${1:name}"',
    ),
)


class ExpressionToken(TypedDict):
    label: str
    insert: str
    kind: Literal["function", "constant"]
    detail: str


def expression_tokens() -> list[ExpressionToken]:
    tokens: list[ExpressionToken] = []
    for fn in (COL_FUNCTION, *FUNCTIONS):
        tokens.append(
            {
                "label": fn.name,
                "insert": fn.insert,
                "kind": "function",
                "detail": fn.detail,
            },
        )
    for constant in NAMED_CONSTANTS:
        tokens.append(
            {
                "label": constant.name,
                "insert": constant.insert,
                "kind": "constant",
                "detail": constant.detail,
            },
        )
    return tokens


def expression_json_schema_extra() -> dict[str, Any]:
    return {
        "ui:options": {"tokens": expression_tokens()},
    }


def build_namespace(columns: Mapping[str, Value]) -> dict[str, object]:
    return {
        "__builtins__": {},
        COL_FUNCTION.name: lambda name: columns[name],
        **{constant.name: constant.value for constant in NAMED_CONSTANTS},
        **{fn.name: fn.impl for fn in FUNCTIONS if fn.impl is not None},
    }


def expression_syntax_help() -> str:
    constants = ", ".join(f"`{c.name}` ({c.detail})" for c in NAMED_CONSTANTS)
    functions = ", ".join(f"`{fn.signature}` ({fn.detail})" for fn in (COL_FUNCTION, *FUNCTIONS))
    operators = ", ".join(f"`{op.name}` ({op.detail})" for op in OPERATORS)
    return f"""\
## Expression syntax

Expressions are unit-aware and units are taken from column metadata.

Mathematical operations:
- Operators: {operators}
- Functions: {functions}
- Numbers are dimensionless
- String literals are supported
- Log columns (`mag`/`dex`) yield the bare exponent; multiply by the scale yourself

Available constants: {constants}.

## Examples

- Fill a column with a constant: 
    - `1.5`
    - `180 * deg`
    - `"G"` - fills the column with a text "G"
- Copy another column: `col("ra")`
- Mathematical expression: `3 * 10 ** col("logd25") * arcsec`"""
