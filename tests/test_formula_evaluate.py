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


def _sample_columns() -> dict[str, Col]:
    return {
        "logd25": Col(1.5),
        "logr25": Col(0.3),
        "e_logd25": Col(0.05),
        "e_logr25": Col(0.04),
        "pa": Col(190.0, "deg"),
    }


def _log_columns(log_unit: str) -> dict[str, Col]:
    return {
        **_sample_columns(),
        "logd25": Col(1.5, log_unit),
        "logr25": Col(0.3, log_unit),
        "e_logd25": Col(0.05, log_unit),
        "e_logr25": Col(0.04, log_unit),
    }


def _hyperleda_columns() -> dict[str, Col]:
    return {
        "logd25": Col(0.697, "dex(0.1 arcmin)"),
        "logr25": Col(0.13, "dex"),
        "e_logd25": Col(0.079, "dex(0.1 arcmin)"),
        "e_logr25": Col(0.028, "dex"),
        "pa": Col(161.14, "deg"),
    }


EVAL_CASES: list[EvalCase] = [
    EvalCase(
        name="isophotal_major_axis",
        expression='3 * 10 ** col("logd25") * arcsec',
        columns=_sample_columns(),
        result_val=94.8683,
        result_unit=u.arcsec,
    ),
    EvalCase(
        name="isophotal_major_axis_error",
        expression='3 * 10 ** col("logd25") * 2.302585093 * e_logd25 * arcsec',
        columns=_sample_columns(),
        result_val=10.9221,
        result_unit=u.arcsec,
    ),
    EvalCase(
        name="isophotal_minor_axis",
        expression='3 * 10 ** (col("logd25") - col("logr25")) * arcsec',
        columns=_sample_columns(),
        result_val=47.5468,
        result_unit=u.arcsec,
    ),
    EvalCase(
        name="isophotal_minor_axis_error",
        expression=(
            '3 * 10 ** (col("logd25") - col("logr25")) * 2.302585093 '
            '* (col("e_logd25") ** 2 + col("e_logr25") ** 2) ** 0.5 * arcsec'
        ),
        columns=_sample_columns(),
        result_val=7.0102,
        result_unit=u.arcsec,
    ),
    EvalCase(
        name="position_angle_modulo",
        expression='col("pa") % (180.0 * deg)',
        columns=_sample_columns(),
        result_val=10.0,
        result_unit=u.deg,
    ),
    EvalCase(
        name="isophotal_major_axis_mag_units",
        expression='3 * 10 ** col("logd25") * arcsec',
        columns=_log_columns("mag"),
        result_val=94.8683,
        result_unit=u.arcsec,
    ),
    EvalCase(
        name="isophotal_major_axis_error_mag_units",
        expression='3 * 10 ** col("logd25") * 2.302585093 * e_logd25 * arcsec',
        columns=_log_columns("mag"),
        result_val=10.9221,
        result_unit=u.arcsec,
    ),
    EvalCase(
        name="isophotal_minor_axis_error_mag_units",
        expression=(
            '3 * 10 ** (col("logd25") - col("logr25")) * 2.302585093 '
            '* (col("e_logd25") ** 2 + col("e_logr25") ** 2) ** 0.5 * arcsec'
        ),
        columns=_log_columns("mag"),
        result_val=7.0102,
        result_unit=u.arcsec,
    ),
    EvalCase(
        name="isophotal_major_axis_dex_units",
        expression='3 * 10 ** col("logd25") * arcsec',
        columns=_log_columns("dex"),
        result_val=94.8683,
        result_unit=u.arcsec,
    ),
    EvalCase(
        name="isophotal_major_axis_error_dex_units",
        expression='3 * 10 ** col("logd25") * 2.302585093 * e_logd25 * arcsec',
        columns=_log_columns("dex"),
        result_val=10.9221,
        result_unit=u.arcsec,
    ),
    EvalCase(
        name="isophotal_minor_axis_error_dex_units",
        expression=(
            '3 * 10 ** (col("logd25") - col("logr25")) * 2.302585093 '
            '* (col("e_logd25") ** 2 + col("e_logr25") ** 2) ** 0.5 * arcsec'
        ),
        columns=_log_columns("dex"),
        result_val=7.0102,
        result_unit=u.arcsec,
    ),
    EvalCase(
        name="hyperleda_major_axis",
        expression='3 * 10 ** col("logd25") * arcsec',
        columns=_hyperleda_columns(),
        result_val=14.9321,
        result_unit=u.arcsec,
    ),
    EvalCase(
        name="hyperleda_major_axis_error",
        expression='3 * 10 ** col("logd25") * 2.302585093 * e_logd25 * arcsec',
        columns=_hyperleda_columns(),
        result_val=2.7162,
        result_unit=u.arcsec,
    ),
    EvalCase(
        name="hyperleda_minor_axis",
        expression='3 * 10 ** (col("logd25") - col("logr25")) * arcsec',
        columns=_hyperleda_columns(),
        result_val=11.0693,
        result_unit=u.arcsec,
    ),
    EvalCase(
        name="hyperleda_minor_axis_error",
        expression=(
            '3 * 10 ** (col("logd25") - col("logr25")) * 2.302585093 '
            '* (col("e_logd25") ** 2 + col("e_logr25") ** 2) ** 0.5 * arcsec'
        ),
        columns=_hyperleda_columns(),
        result_val=2.1363,
        result_unit=u.arcsec,
    ),
    EvalCase(
        name="surface_brightness_column",
        expression='col("bri25")',
        columns={"bri25": Col(23.162, "mag / arcsec2")},
        result_val=23.162,
        result_unit=u.Unit("mag/arcsec2"),
    ),
    EvalCase(name="string_literal", expression='"abc"', columns={}, result_val="abc"),
    EvalCase(
        name="string_column",
        expression='col("name")',
        columns={"name": Col("NGC 123")},
        result_val="NGC 123",
    ),
    EvalCase(
        name="string_concat_columns",
        expression='col("a") + col("b")',
        columns={"a": Col("M"), "b": Col("82")},
        result_val="M82",
    ),
    EvalCase(
        name="string_concat_literal_prefix",
        expression='"M " + col("id")',
        columns={"id": Col("82")},
        result_val="M 82",
    ),
    EvalCase(
        name="string_concat_literal_separator",
        expression='col("a") + " " + col("b")',
        columns={"a": Col("NGC"), "b": Col("905")},
        result_val="NGC 905",
    ),
    EvalCase(
        name="column_string_value",
        expression='col("x")',
        columns={"x": Col("hello")},
        result_val="hello",
    ),
    EvalCase(
        name="column_dimensionless",
        expression='col("x")',
        columns={"x": Col(1.5)},
        result_val=1.5,
        result_unit=u.dimensionless_unscaled,
    ),
    EvalCase(
        name="column_angle_unit",
        expression='col("x")',
        columns={"x": Col(1.5, "deg")},
        result_val=1.5,
        result_unit=u.deg,
    ),
    EvalCase(
        name="column_mag_unit",
        expression='col("x")',
        columns={"x": Col(1.5, "mag")},
        result_val=1.5,
        result_unit=u.dimensionless_unscaled,
    ),
    EvalCase(
        name="column_dex_unit",
        expression='col("x")',
        columns={"x": Col(1.5, "dex")},
        result_val=1.5,
        result_unit=u.dimensionless_unscaled,
    ),
    EvalCase(
        name="column_dex_function_unit",
        expression='col("x")',
        columns={"x": Col(0.697, "dex(0.1 arcmin)")},
        result_val=0.697,
        result_unit=u.dimensionless_unscaled,
    ),
    EvalCase(
        name="constant_over_column",
        expression="pi",
        columns={"pi": Col(1.0)},
        result_val=3.1416,
        result_unit=u.dimensionless_unscaled,
    ),
    EvalCase(
        name="column_over_constant",
        expression='col("pi")',
        columns={"pi": Col(1.0)},
        result_val=1.0,
        result_unit=u.dimensionless_unscaled,
    ),
    EvalCase(
        name="trig_on_angle_column",
        expression='sin(col("pa"))',
        columns={"pa": Col(30.0, "deg")},
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
        expression='col("name") + col("logd25")',
        columns={"name": Col("x"), "logd25": Col(1.5)},
        error=True,
    ),
    EvalCase(
        name="error_modulo_dimensionless_divisor",
        expression='col("pa") % 180.0',
        columns={"pa": Col(190.0, "deg")},
        error=True,
    ),
    EvalCase(
        name="error_trig_on_dimensionless",
        expression='sin(col("x"))',
        columns={"x": Col(0.5)},
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
