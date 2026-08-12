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


NAMED_CONSTANTS: dict[str, u.Quantity] = {
    "pi": np.pi * u.dimensionless_unscaled,
    "c": const.c,
    "deg": 1 * u.deg,
    "rad": 1 * u.rad,
    "arcmin": 1 * u.arcmin,
    "arcsec": 1 * u.arcsec,
    "mag": 1 * u.mag,
}


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


COL_FUNCTION = FunctionDef("col", "Rawdata column", placeholder='"${1:name}"')

FUNCTIONS: dict[str, FunctionDef] = {
    fn.name: fn
    for fn in (
        FunctionDef("sin", "Sine (argument must be an angle)", np.sin),
        FunctionDef("cos", "Cosine (argument must be an angle)", np.cos),
        FunctionDef("str", "Convert to text", _formula_str),
        FunctionDef("to_deg", "Convert to degrees; parses coordinate strings or angle quantities", _to_deg),
    )
}


class ExpressionToken(TypedDict):
    label: str
    insert: str
    kind: Literal["function", "constant"]
    detail: str


def expression_tokens() -> list[ExpressionToken]:
    tokens: list[ExpressionToken] = []
    for fn in (COL_FUNCTION, *FUNCTIONS.values()):
        tokens.append(
            {
                "label": fn.name,
                "insert": fn.insert,
                "kind": "function",
                "detail": fn.detail,
            },
        )
    for name in NAMED_CONSTANTS:
        tokens.append(
            {
                "label": name,
                "insert": name,
                "kind": "constant",
                "detail": "Named constant",
            },
        )
    return tokens


def expression_json_schema_extra() -> dict[str, Any]:
    return {
        "ui:widget": "expression",
        "ui:options": {"tokens": expression_tokens()},
    }


def build_namespace(columns: Mapping[str, Value]) -> dict[str, object]:
    return {
        "__builtins__": {},
        COL_FUNCTION.name: lambda name: columns[name],
        **NAMED_CONSTANTS,
        **{name: fn.impl for name, fn in FUNCTIONS.items() if fn.impl is not None},
    }


def expression_syntax_help() -> str:
    constants = ", ".join(f"`{name}`" for name in sorted(NAMED_CONSTANTS))
    functions = ", ".join(f"`{fn.signature}` ({fn.detail})" for fn in (COL_FUNCTION, *FUNCTIONS.values()))
    return f"""\
## Expression syntax

Expressions are unit-aware and units are taken from column metadata.

Mathematical operations:
- Operators: `+` `-` `*` `/` `**` `%`
- Functions: {functions}
- Numbers are dimensionless
- String literals and `+` concatenation are supported
- Modulo divisors must carry units (e.g. `col("pa") % (180 * deg)`)
- Log columns (`mag`/`dex`) yield the bare exponent; multiply by the scale yourself

Available constants: {constants}.

## Examples

- Fill a column with a constant: 
    - `1.5`
    - `180 * deg`
    - `"G"` - fills the column with a text "G"
- Copy another column: `col("ra")`
- Sexagesimal coordinates: `to_deg(col("RAJ2000"))`
- Mathematical expression: `3 * 10 ** col("logd25") * arcsec`"""
