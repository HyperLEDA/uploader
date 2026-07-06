from collections.abc import Sequence

import astropy.units as u
import numpy as np
from astropy.units.function.core import FunctionUnitBase

type Value = u.Quantity | str | np.ndarray


def _is_logarithmic_column_unit(unit: u.Unit) -> bool:
    return unit == u.mag or unit == u.dex or isinstance(unit, FunctionUnitBase)


def column_quantity(value: float | str | Sequence[float] | Sequence[str], unit: str) -> Value:
    if isinstance(value, str):
        return value
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        if all(isinstance(x, str) for x in value):
            return np.asarray(value)
    numeric = np.asarray(value, dtype=float)
    if not unit:
        return numeric * u.dimensionless_unscaled
    parsed = u.Unit(unit)
    if _is_logarithmic_column_unit(parsed):
        return numeric * u.dimensionless_unscaled
    return numeric * parsed
