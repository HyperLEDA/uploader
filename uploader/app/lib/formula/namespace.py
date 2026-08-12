from collections.abc import Mapping

import astropy.constants as const
import astropy.units as u
import numpy as np
from astropy.coordinates import Angle

from uploader.app.lib.formula.values import TextValue, Value

COL_FUNCTION = "col"

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


FUNCTIONS: dict[str, object] = {
    "sin": np.sin,
    "cos": np.cos,
    "str": _formula_str,
    "to_deg": _to_deg,
}


def build_namespace(columns: Mapping[str, Value]) -> dict[str, object]:
    return {
        "__builtins__": {},
        COL_FUNCTION: lambda name: columns[name],
        **NAMED_CONSTANTS,
        **FUNCTIONS,
    }


def expression_syntax_help() -> str:
    constants = ", ".join(f"`{name}`" for name in sorted(NAMED_CONSTANTS))
    return f"""\
## Expression syntax

Use `{COL_FUNCTION}("name")` to refer to rawdata columns (e.g. `col("a")`).
Expressions are unit-aware and units are taken from column metadata.

Mathematical operations:
- Operators: `+` `-` `*` `/` `**` `%`
- Functions: `sin(x)`, `cos(x)` (argument must be an angle), `str(x)`, `to_deg(x)`
- `to_deg(x)` parses coordinate strings or converts angle quantities to degrees
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
- Copy another column: `{COL_FUNCTION}("ra")`
- Sexagesimal coordinates: `to_deg({COL_FUNCTION}("RAJ2000"))`
- Mathematical expression: `3 * 10 ** {COL_FUNCTION}("logd25") * arcsec`"""
