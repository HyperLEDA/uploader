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

FUNCTIONS: dict[str, object] = {
    "sin": np.sin,
    "cos": np.cos,
}


def build_namespace(columns: Mapping[str, Value]) -> dict[str, object]:
    bare = {name: value for name, value in columns.items() if name.isidentifier()}
    return {
        "__builtins__": {},
        COL_FUNCTION: lambda name: columns[name],
        **bare,
        **NAMED_CONSTANTS,
        **FUNCTIONS,
    }


def expression_syntax_help() -> str:
    constants = ", ".join(sorted(NAMED_CONSTANTS))
    return (
        f'Use {COL_FUNCTION}("name") or bare identifiers to refer to rawdata columns '
        '(e.g. col("a"), e_logd25).\n'
        "Bare identifiers that match predefined constants use those constants.\n"
        "Operators: + - * / ** %.\n"
        "Functions: sin(x), cos(x) (argument must be an angle).\n"
        "Numbers are dimensionless.\n"
        "String literals and + concatenation are supported.\n"
        'Modulo divisors must carry units (e.g. col("pa") % (180 * deg)).\n'
        "Log columns (mag/dex) yield the bare exponent; multiply by the scale yourself.\n"
        f"Available constants: {constants}."
    )
