import re
from collections.abc import Callable
from dataclasses import dataclass
from typing import final

import astropy.units as u
import numpy as np
from astropy.coordinates import Angle

from uploader.app.lib.formula.values import TextValue, Value


@final
@dataclass(frozen=True)
class ArgumentDef:
    type: str
    detail: str


@final
@dataclass(frozen=True)
class FunctionDef:
    name: str
    summary: str
    args: tuple[ArgumentDef, ...]
    returns: ArgumentDef
    impl: object | None = None
    placeholder: str = "${1:x}"

    @property
    def detail(self) -> str:
        lines = [f"- `arg{i}` ({arg.type}): {arg.detail}" for i, arg in enumerate(self.args, start=1)]
        lines.append(f"- `return` ({self.returns.type}): {self.returns.detail}")
        return f"{self.summary}\n\n" + "\n".join(lines)

    @property
    def insert(self) -> str:
        return f"{self.name}({self.placeholder})"

    @property
    def signature(self) -> str:
        args = re.sub(r"\$\{\d+:([^}]+)\}", r"\1", self.placeholder)
        return f"{self.name}({args})"


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


def _extremum(fn: Callable[..., u.Quantity]) -> Callable[[object, object], u.Quantity]:
    def impl(left: object, right: object) -> u.Quantity:
        left_q = left if isinstance(left, u.Quantity) else _to_quantity(left)
        right_q = right if isinstance(right, u.Quantity) else _to_quantity(right)
        aligned = right_q.to(left_q.unit)
        return fn(left_q, aligned)

    return impl


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


_ANGLE = ArgumentDef("angle", "Must be an angle")
_NUMBER = ArgumentDef("number", "Numeric value")
_TEXT = ArgumentDef("text | number", "Value to convert to text")

_RET_NUMBER = ArgumentDef("number", "Numeric result")
_RET_ANGLE = ArgumentDef("angle", "Angle result")
_RET_RADIANS = ArgumentDef("number", "Angle in radians")
_RET_TEXT = ArgumentDef("text", "Text result")
_RET_ANY = ArgumentDef("any", "Result value")

COL_FUNCTION = FunctionDef(
    "col",
    "Rawdata column",
    (ArgumentDef("string", "Column name from rawdata"),),
    _RET_ANY,
    placeholder='"${1:name}"',
)

FUNCTIONS: tuple[FunctionDef, ...] = (
    FunctionDef("sin", "Sine", (_ANGLE,), _RET_NUMBER, np.sin),
    FunctionDef("cos", "Cosine", (_ANGLE,), _RET_NUMBER, np.cos),
    FunctionDef("tan", "Tangent", (_ANGLE,), _RET_NUMBER, np.tan),
    FunctionDef(
        "asin",
        "Arcsine",
        (ArgumentDef("number", "Value in [-1, 1]"),),
        _RET_RADIANS,
        _math(np.arcsin),
    ),
    FunctionDef(
        "acos",
        "Arccosine",
        (ArgumentDef("number", "Value in [-1, 1]"),),
        _RET_RADIANS,
        _math(np.arccos),
    ),
    FunctionDef(
        "atan",
        "Arctangent",
        (ArgumentDef("number", "Numeric value"),),
        _RET_RADIANS,
        _math(np.arctan),
    ),
    FunctionDef(
        "atan2",
        "Two-argument arctangent",
        (
            ArgumentDef("number", "Y coordinate"),
            ArgumentDef("number", "X coordinate"),
        ),
        _RET_RADIANS,
        _math(np.arctan2),
        placeholder="${1:y}, ${2:x}",
    ),
    FunctionDef(
        "deg2rad",
        "Convert degrees to radians",
        (ArgumentDef("angle", "Angle in degrees"),),
        ArgumentDef("angle", "Angle in radians"),
        _deg2rad,
        placeholder="${1:deg}",
    ),
    FunctionDef(
        "rad2deg",
        "Convert radians to degrees",
        (ArgumentDef("angle", "Angle in radians"),),
        ArgumentDef("angle", "Angle in degrees"),
        _rad2deg,
        placeholder="${1:rad}",
    ),
    FunctionDef(
        "wrap360",
        "Wrap angle to [0, 360) degrees",
        (ArgumentDef("angle", "Angle in degrees"),),
        ArgumentDef("angle", "Angle in degrees, wrapped to [0, 360)"),
        _wrap360,
        placeholder="${1:deg}",
    ),
    FunctionDef("sqrt", "Square root", (_NUMBER,), _RET_NUMBER, _math(np.sqrt)),
    FunctionDef("exp", "Exponential", (_NUMBER,), _RET_NUMBER, _math(np.exp)),
    FunctionDef("log10", "Base-10 logarithm", (_NUMBER,), _RET_NUMBER, _math(np.log10)),
    FunctionDef("ln", "Natural logarithm", (_NUMBER,), _RET_NUMBER, _math(np.log)),
    FunctionDef(
        "pow",
        "Raise x to the power y",
        (
            ArgumentDef("number", "Base"),
            ArgumentDef("number", "Exponent"),
        ),
        _RET_NUMBER,
        _math(np.power),
        placeholder="${1:x}, ${2:y}",
    ),
    FunctionDef(
        "max",
        "Larger of two values",
        (
            ArgumentDef("number", "First value"),
            ArgumentDef("number", "Second value"),
        ),
        ArgumentDef("number", "Larger of the two inputs, preserving units"),
        _extremum(np.maximum),
        placeholder="${1:x}, ${2:y}",
    ),
    FunctionDef(
        "min",
        "Smaller of two values",
        (
            ArgumentDef("number", "First value"),
            ArgumentDef("number", "Second value"),
        ),
        ArgumentDef("number", "Smaller of the two inputs, preserving units"),
        _extremum(np.minimum),
        placeholder="${1:x}, ${2:y}",
    ),
    FunctionDef("str", "Convert to text", (_TEXT,), _RET_TEXT, _formula_str),
    FunctionDef(
        "where",
        "Conditional value selection",
        (
            ArgumentDef("boolean", "Condition to test"),
            ArgumentDef("any", "Value when cond is true"),
            ArgumentDef("any", "Value when cond is false; nest for extra branches"),
        ),
        _RET_ANY,
        _where,
        placeholder="${1:cond}, ${2:then}, ${3:else}",
    ),
    FunctionDef(
        "to_deg",
        "Convert to degrees",
        (
            ArgumentDef(
                "angle | string",
                'Angle or coordinate string; e.g. "00 02 08.4" (hourangle), "+16 35 13" (deg), "00h02m08.4s"',
            ),
        ),
        ArgumentDef("angle", "Angle in degrees"),
        _to_deg,
    ),
    FunctionDef(
        "unit",
        "Astropy unit from a name string",
        (
            ArgumentDef(
                "string",
                'Unit name; e.g. "Mpc", "km/s", "Jy"',
            ),
        ),
        ArgumentDef("number", "Scalar with the named unit"),
        _unit,
        placeholder='"${1:name}"',
    ),
)
