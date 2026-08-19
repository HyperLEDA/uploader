import re
from collections.abc import Callable, Mapping
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
        args = re.sub(r"\$\{\d+:([^}]+)\}", r"\1", self.placeholder)
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
    ConstantDef("G", const.G, "Gravitational constant"),
    ConstantDef("h", const.h, "Planck constant"),
    ConstantDef("k_B", const.k_B, "Boltzmann constant"),
    ConstantDef("sigma", const.sigma_sb, "Stefan-Boltzmann constant"),
    ConstantDef("m_e", const.m_e, "Electron mass"),
    ConstantDef("m_p", const.m_p, "Proton mass"),
    ConstantDef("au", 1 * u.au, "Astronomical unit"),
    ConstantDef("pc", 1 * u.pc, "Parsec"),
    ConstantDef("ly", 1 * u.lyr, "Light year"),
    ConstantDef("eV", 1 * u.eV, "Electronvolt"),
    ConstantDef("Jy", 1 * u.Jy, "Jansky"),
    ConstantDef("M_sun", const.M_sun, "Solar mass"),
    ConstantDef("R_sun", const.R_sun, "Nominal solar radius"),
    ConstantDef("L_sun", const.L_sun, "Nominal solar luminosity"),
    ConstantDef("M_earth", const.M_earth, "Earth mass"),
    ConstantDef("M_jup", const.M_jup, "Jupiter mass"),
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
    OperatorDef("==", "Equal"),
    OperatorDef("!=", "Not equal"),
    OperatorDef("<", "Less than"),
    OperatorDef("<=", "Less than or equal"),
    OperatorDef(">", "Greater than"),
    OperatorDef(">=", "Greater than or equal"),
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


def _to_quantity(value: object) -> u.Quantity:
    if isinstance(value, u.Quantity):
        return value
    return np.asarray(value, dtype=float) * u.dimensionless_unscaled


def _math(fn: Callable[..., u.Quantity]) -> Callable[..., u.Quantity]:
    def impl(*args: object) -> u.Quantity:
        return fn(*(_to_quantity(arg) for arg in args))

    return impl


def _as_angle(value: object, default_unit: u.Unit) -> u.Quantity:
    if isinstance(value, u.Quantity):
        if value.unit.is_equivalent(u.rad):
            return value
        if value.unit.is_equivalent(u.dimensionless_unscaled):
            return value.to_value(u.dimensionless_unscaled) * default_unit
        raise TypeError(f"expected an angle or dimensionless value, got unit {value.unit}")
    return np.asarray(value, dtype=float) * default_unit


def _deg2rad(value: object) -> u.Quantity:
    return _as_angle(value, u.deg).to(u.rad)


def _rad2deg(value: object) -> u.Quantity:
    return _as_angle(value, u.rad).to(u.deg)


def _wrap360(value: object) -> u.Quantity:
    return _as_angle(value, u.deg).to(u.deg) % (360 * u.deg)


def _mask(cond: object) -> np.ndarray:
    if isinstance(cond, u.Quantity):
        return np.asarray(cond.value)
    return np.asarray(cond)


def _where(cond: object, then: object, otherwise: object) -> object:
    mask = _mask(cond)
    if mask.shape == ():
        return then if bool(mask) else otherwise
    if isinstance(then, u.Quantity) or isinstance(otherwise, u.Quantity):
        then_q = then if isinstance(then, u.Quantity) else _to_quantity(then)
        else_q = otherwise if isinstance(otherwise, u.Quantity) else _to_quantity(otherwise)
        aligned = else_q.to(then_q.unit)
        return np.where(mask, np.asarray(then_q.value), np.asarray(aligned.value)) * then_q.unit
    then_v = then.data if isinstance(then, TextValue) else then
    else_v = otherwise.data if isinstance(otherwise, TextValue) else otherwise
    return np.where(mask, np.asarray(then_v), np.asarray(else_v))


COL_FUNCTION = FunctionDef("col", "Rawdata column", placeholder='"${1:name}"')

FUNCTIONS: tuple[FunctionDef, ...] = (
    FunctionDef("sin", "Sine (argument must be an angle)", np.sin),
    FunctionDef("cos", "Cosine (argument must be an angle)", np.cos),
    FunctionDef("tan", "Tangent (argument must be an angle)", np.tan),
    FunctionDef("asin", "Arcsine (returns radians)", _math(np.arcsin)),
    FunctionDef("acos", "Arccosine (returns radians)", _math(np.arccos)),
    FunctionDef("atan", "Arctangent (returns radians)", _math(np.arctan)),
    FunctionDef(
        "atan2",
        "Two-argument arctangent (returns radians)",
        _math(np.arctan2),
        placeholder="${1:y}, ${2:x}",
    ),
    FunctionDef("deg2rad", "Convert degrees to radians", _deg2rad, placeholder="${1:deg}"),
    FunctionDef("rad2deg", "Convert radians to degrees", _rad2deg, placeholder="${1:rad}"),
    FunctionDef("wrap360", "Wrap angle to [0, 360) degrees", _wrap360, placeholder="${1:deg}"),
    FunctionDef("sqrt", "Square root", _math(np.sqrt)),
    FunctionDef("exp", "Exponential", _math(np.exp)),
    FunctionDef("log10", "Base-10 logarithm", _math(np.log10)),
    FunctionDef("ln", "Natural logarithm", _math(np.log)),
    FunctionDef("pow", "Raise x to the power y", _math(np.power), placeholder="${1:x}, ${2:y}"),
    FunctionDef("str", "Convert to text", _formula_str),
    FunctionDef(
        "where",
        "Pick then if cond is true, otherwise the third argument; nest for extra branches",
        _where,
        placeholder="${1:cond}, ${2:then}, ${3:else}",
    ),
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
- Mathematical expression: `3 * 10 ** col("logd25") * arcsec`
- Conditional: `where(col("v") > 0, col("v"), 0)`"""
