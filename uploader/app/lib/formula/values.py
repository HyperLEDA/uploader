import astropy.units as u
from astropy.units.function.core import FunctionUnitBase

type Value = u.Quantity | str


def _is_logarithmic_column_unit(unit: u.Unit) -> bool:
    return unit == u.mag or unit == u.dex or isinstance(unit, FunctionUnitBase)


def column_quantity(value: float | str, unit: str) -> Value:
    if isinstance(value, str):
        return value
    if not unit:
        return float(value) * u.dimensionless_unscaled
    parsed = u.Unit(unit)
    if _is_logarithmic_column_unit(parsed):
        return float(value) * u.dimensionless_unscaled
    return float(value) * parsed
