from dataclasses import dataclass

import astropy.units as u
import numpy as np
import pytest

from uploader.app.lib.formula import ExpressionEvaluationError, column_quantity, evaluate, parse


@dataclass
class Col:
    value: float | str
    unit: str = ""


@dataclass
class EvalCase:
    expression: str
    columns: dict[str, Col]
    result_val: str | float | None = None
    result_unit: u.Unit | None = None
    error: bool = False
    name: str = ""


def evaluate_expr(source: str, columns: dict[str, Col]) -> object:
    built = {name: column_quantity(col.value, col.unit) for name, col in columns.items()}
    return evaluate(parse(source), built)


_COLUMNS: dict[str, Col] = {
    "logd25": Col(1.5),
    "logr25": Col(0.3),
    "e_logd25": Col(0.05),
    "e_logr25": Col(0.04),
    "pa": Col(190.0, "deg"),
    "logd25_mag": Col(1.5, "mag"),
    "logr25_mag": Col(0.3, "mag"),
    "e_logd25_mag": Col(0.05, "mag"),
    "e_logr25_mag": Col(0.04, "mag"),
    "logd25_dex": Col(1.5, "dex"),
    "logr25_dex": Col(0.3, "dex"),
    "e_logd25_dex": Col(0.05, "dex"),
    "e_logr25_dex": Col(0.04, "dex"),
    "logd25_hl": Col(0.697, "dex(0.1 arcmin)"),
    "logr25_hl": Col(0.13, "dex"),
    "e_logd25_hl": Col(0.079, "dex(0.1 arcmin)"),
    "e_logr25_hl": Col(0.028, "dex"),
    "pa_hl": Col(161.14, "deg"),
    "bri25": Col(23.162, "mag / arcsec2"),
    "designation": Col("NGC 123"),
    "str_a": Col("M"),
    "str_b": Col("82"),
    "id": Col("82"),
    "ngc_a": Col("NGC"),
    "ngc_b": Col("905"),
    "x_str": Col("hello"),
    "x_dim": Col(1.5),
    "x_deg": Col(1.5, "deg"),
    "x_mag": Col(1.5, "mag"),
    "x_dex": Col(1.5, "dex"),
    "x_dex_fn": Col(0.697, "dex(0.1 arcmin)"),
    "pi": Col(1.0),
    "pa_trig": Col(30.0, "deg"),
    "err_name": Col("x"),
    "x_dimless": Col(0.5),
}


EVAL_CASES: list[EvalCase] = [
    EvalCase(
        name="isophotal_major_axis",
        expression='3 * 10 ** col("logd25") * arcsec',
        columns=_COLUMNS,
        result_val=94.8683,
        result_unit=u.arcsec,
    ),
    EvalCase(
        name="isophotal_major_axis_error",
        expression='3 * 10 ** col("logd25") * 2.302585093 * e_logd25 * arcsec',
        columns=_COLUMNS,
        result_val=10.9221,
        result_unit=u.arcsec,
    ),
    EvalCase(
        name="isophotal_minor_axis",
        expression='3 * 10 ** (col("logd25") - col("logr25")) * arcsec',
        columns=_COLUMNS,
        result_val=47.5468,
        result_unit=u.arcsec,
    ),
    EvalCase(
        name="isophotal_minor_axis_error",
        expression=(
            '3 * 10 ** (col("logd25") - col("logr25")) * 2.302585093 '
            '* (col("e_logd25") ** 2 + col("e_logr25") ** 2) ** 0.5 * arcsec'
        ),
        columns=_COLUMNS,
        result_val=7.0102,
        result_unit=u.arcsec,
    ),
    EvalCase(
        name="position_angle_modulo",
        expression='col("pa") % (180.0 * deg)',
        columns=_COLUMNS,
        result_val=10.0,
        result_unit=u.deg,
    ),
    EvalCase(
        name="isophotal_major_axis_mag_units",
        expression='3 * 10 ** col("logd25_mag") * arcsec',
        columns=_COLUMNS,
        result_val=94.8683,
        result_unit=u.arcsec,
    ),
    EvalCase(
        name="isophotal_major_axis_error_mag_units",
        expression='3 * 10 ** col("logd25_mag") * 2.302585093 * e_logd25_mag * arcsec',
        columns=_COLUMNS,
        result_val=10.9221,
        result_unit=u.arcsec,
    ),
    EvalCase(
        name="isophotal_minor_axis_error_mag_units",
        expression=(
            '3 * 10 ** (col("logd25_mag") - col("logr25_mag")) * 2.302585093 '
            '* (col("e_logd25_mag") ** 2 + col("e_logr25_mag") ** 2) ** 0.5 * arcsec'
        ),
        columns=_COLUMNS,
        result_val=7.0102,
        result_unit=u.arcsec,
    ),
    EvalCase(
        name="isophotal_major_axis_dex_units",
        expression='3 * 10 ** col("logd25_dex") * arcsec',
        columns=_COLUMNS,
        result_val=94.8683,
        result_unit=u.arcsec,
    ),
    EvalCase(
        name="isophotal_major_axis_error_dex_units",
        expression='3 * 10 ** col("logd25_dex") * 2.302585093 * e_logd25_dex * arcsec',
        columns=_COLUMNS,
        result_val=10.9221,
        result_unit=u.arcsec,
    ),
    EvalCase(
        name="isophotal_minor_axis_error_dex_units",
        expression=(
            '3 * 10 ** (col("logd25_dex") - col("logr25_dex")) * 2.302585093 '
            '* (col("e_logd25_dex") ** 2 + col("e_logr25_dex") ** 2) ** 0.5 * arcsec'
        ),
        columns=_COLUMNS,
        result_val=7.0102,
        result_unit=u.arcsec,
    ),
    EvalCase(
        name="hyperleda_major_axis",
        expression='3 * 10 ** col("logd25_hl") * arcsec',
        columns=_COLUMNS,
        result_val=14.9321,
        result_unit=u.arcsec,
    ),
    EvalCase(
        name="hyperleda_major_axis_error",
        expression='3 * 10 ** col("logd25_hl") * 2.302585093 * e_logd25_hl * arcsec',
        columns=_COLUMNS,
        result_val=2.7162,
        result_unit=u.arcsec,
    ),
    EvalCase(
        name="hyperleda_minor_axis",
        expression='3 * 10 ** (col("logd25_hl") - col("logr25_hl")) * arcsec',
        columns=_COLUMNS,
        result_val=11.0693,
        result_unit=u.arcsec,
    ),
    EvalCase(
        name="hyperleda_minor_axis_error",
        expression=(
            '3 * 10 ** (col("logd25_hl") - col("logr25_hl")) * 2.302585093 '
            '* (col("e_logd25_hl") ** 2 + col("e_logr25_hl") ** 2) ** 0.5 * arcsec'
        ),
        columns=_COLUMNS,
        result_val=2.1363,
        result_unit=u.arcsec,
    ),
    EvalCase(
        name="surface_brightness_column",
        expression='col("bri25")',
        columns=_COLUMNS,
        result_val=23.162,
        result_unit=u.Unit("mag/arcsec2"),
    ),
    EvalCase(name="string_literal", expression='"abc"', columns={}, result_val="abc"),
    EvalCase(
        name="string_column",
        expression='col("designation")',
        columns=_COLUMNS,
        result_val="NGC 123",
    ),
    EvalCase(
        name="string_concat_columns",
        expression='col("str_a") + col("str_b")',
        columns=_COLUMNS,
        result_val="M82",
    ),
    EvalCase(
        name="string_concat_literal_prefix",
        expression='"M " + col("id")',
        columns=_COLUMNS,
        result_val="M 82",
    ),
    EvalCase(
        name="string_concat_literal_separator",
        expression='col("ngc_a") + " " + col("ngc_b")',
        columns=_COLUMNS,
        result_val="NGC 905",
    ),
    EvalCase(
        name="column_string_value",
        expression='col("x_str")',
        columns=_COLUMNS,
        result_val="hello",
    ),
    EvalCase(
        name="column_dimensionless",
        expression='col("x_dim")',
        columns=_COLUMNS,
        result_val=1.5,
        result_unit=u.dimensionless_unscaled,
    ),
    EvalCase(
        name="column_angle_unit",
        expression='col("x_deg")',
        columns=_COLUMNS,
        result_val=1.5,
        result_unit=u.deg,
    ),
    EvalCase(
        name="column_mag_unit",
        expression='col("x_mag")',
        columns=_COLUMNS,
        result_val=1.5,
        result_unit=u.dimensionless_unscaled,
    ),
    EvalCase(
        name="column_dex_unit",
        expression='col("x_dex")',
        columns=_COLUMNS,
        result_val=1.5,
        result_unit=u.dimensionless_unscaled,
    ),
    EvalCase(
        name="column_dex_function_unit",
        expression='col("x_dex_fn")',
        columns=_COLUMNS,
        result_val=0.697,
        result_unit=u.dimensionless_unscaled,
    ),
    EvalCase(
        name="constant_over_column",
        expression="pi",
        columns=_COLUMNS,
        result_val=3.1416,
        result_unit=u.dimensionless_unscaled,
    ),
    EvalCase(
        name="column_over_constant",
        expression='col("pi")',
        columns=_COLUMNS,
        result_val=1.0,
        result_unit=u.dimensionless_unscaled,
    ),
    EvalCase(
        name="trig_on_angle_column",
        expression='sin(col("pa_trig"))',
        columns=_COLUMNS,
        result_val=0.5,
        result_unit=u.dimensionless_unscaled,
    ),
    EvalCase(
        name="constant_pi",
        expression="pi",
        columns={},
        result_val=3.1416,
        result_unit=u.dimensionless_unscaled,
    ),
    EvalCase(name="constant_c", expression="c", columns={}, result_val=299792458.0, result_unit=u.Unit("m/s")),
    EvalCase(name="constant_deg", expression="deg", columns={}, result_val=1.0, result_unit=u.deg),
    EvalCase(name="constant_rad", expression="rad", columns={}, result_val=1.0, result_unit=u.rad),
    EvalCase(name="constant_arcmin", expression="arcmin", columns={}, result_val=1.0, result_unit=u.arcmin),
    EvalCase(name="constant_arcsec", expression="arcsec", columns={}, result_val=1.0, result_unit=u.arcsec),
    EvalCase(name="constant_mag", expression="mag", columns={}, result_val=1.0, result_unit=u.mag),
    EvalCase(
        name="function_sin",
        expression="sin(30 * deg)",
        columns={},
        result_val=0.5,
        result_unit=u.dimensionless_unscaled,
    ),
    EvalCase(
        name="function_cos", expression="cos(0 * rad)", columns={}, result_val=1.0, result_unit=u.dimensionless_unscaled
    ),
    EvalCase(name="error_missing_column_bare", expression="missing_col", columns={}, error=True),
    EvalCase(name="error_missing_column_call", expression='col("missing")', columns={}, error=True),
    EvalCase(name="error_incompatible_units", expression="arcsec + mag", columns={}, error=True),
    EvalCase(
        name="error_string_plus_number",
        expression='col("err_name") + col("logd25")',
        columns=_COLUMNS,
        error=True,
    ),
    EvalCase(
        name="error_modulo_dimensionless_divisor",
        expression='col("pa") % 180.0',
        columns=_COLUMNS,
        error=True,
    ),
    EvalCase(
        name="error_trig_on_dimensionless",
        expression='sin(col("x_dimless"))',
        columns=_COLUMNS,
        error=True,
    ),
]


@pytest.mark.parametrize("case", EVAL_CASES, ids=[case.name for case in EVAL_CASES])
def test_evaluate(case: EvalCase) -> None:
    if case.error:
        with pytest.raises(ExpressionEvaluationError):
            evaluate_expr(case.expression, case.columns)
        return

    result = evaluate_expr(case.expression, case.columns)

    if case.result_unit is None:
        assert result == case.result_val
        return

    assert isinstance(result, u.Quantity)
    assert result.unit == case.result_unit
    np.testing.assert_almost_equal(result.value, case.result_val, decimal=4)
