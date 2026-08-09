from collections.abc import Mapping

import astropy.constants as const
import astropy.units as u
import numpy as np

from uploader.app.lib.formula.values import Value

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
    if isinstance(value, str):
        return value
    if isinstance(value, u.Quantity):
        scalar = value.value
        if isinstance(scalar, np.ndarray):
            return np.asarray([_scalar_to_str(x) for x in scalar])
        return _scalar_to_str(scalar)
    return np.asarray([_scalar_to_str(x) for x in value])


FUNCTIONS: dict[str, object] = {
    "sin": np.sin,
    "cos": np.cos,
    "str": _formula_str,
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
- Functions: `sin(x)`, `cos(x)` (argument must be an angle), `str(x)`
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
- Mathematical expression: `3 * 10 ** {COL_FUNCTION}("logd25") * arcsec`"""
