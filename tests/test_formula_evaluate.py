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
    "float_col_1": Col(1.5),
    "float_col_2": Col(0.3),
    "float_col_3": Col(0.05),
    "float_col_4": Col(0.04),
    "angle_col_1": Col(190.0, "deg"),
    "float_col_1_mag": Col(1.5, "mag"),
    "float_col_2_mag": Col(0.3, "mag"),
    "float_col_3_mag": Col(0.05, "mag"),
    "float_col_4_mag": Col(0.04, "mag"),
    "float_col_1_dex": Col(0.697, "dex(0.1 arcmin)"),
    "float_col_2_dex": Col(0.13, "dex"),
    "float_col_3_dex": Col(0.079, "dex(0.1 arcmin)"),
    "float_col_4_dex": Col(0.028, "dex"),
    "brightness_col_1": Col(23.162, "mag / arcsec2"),
    "string_col_1": Col("NGC 123"),
    "string_col_2": Col("M"),
    "string_col_3": Col("82"),
    "string_col_4": Col("NGC"),
    "string_col_5": Col("905"),
    "angle_col_2": Col(1.5, "deg"),
    "const_col_1": Col(1.0),
    "angle_col_3": Col(30.0, "deg"),
    "string_col_6": Col("x"),
    "float_col_dimless": Col(0.5),
}


EVAL_CASES: list[EvalCase] = [
    EvalCase(
        name="power_ten_scale_length",
        expression='3 * 10 ** col("float_col_1") * arcsec',
        columns=_COLUMNS,
        result_val=94.8683,
        result_unit=u.arcsec,
    ),
    EvalCase(
        name="power_ten_scale_length_error",
        expression='3 * 10 ** col("float_col_1") * 2.302585093 * float_col_3 * arcsec',
        columns=_COLUMNS,
        result_val=10.9221,
        result_unit=u.arcsec,
    ),
    EvalCase(
        name="power_ten_scale_length_diff",
        expression='3 * 10 ** (col("float_col_1") - col("float_col_2")) * arcsec',
        columns=_COLUMNS,
        result_val=47.5468,
        result_unit=u.arcsec,
    ),
    EvalCase(
        name="power_ten_scale_length_diff_error",
        expression=(
            '3 * 10 ** (col("float_col_1") - col("float_col_2")) * 2.302585093 '
            '* (col("float_col_3") ** 2 + col("float_col_4") ** 2) ** 0.5 * arcsec'
        ),
        columns=_COLUMNS,
        result_val=7.0102,
        result_unit=u.arcsec,
    ),
    EvalCase(
        name="angle_modulo",
        expression='col("angle_col_1") % (180.0 * deg)',
        columns=_COLUMNS,
        result_val=10.0,
        result_unit=u.deg,
    ),
    EvalCase(
        name="power_ten_scale_length_mag_units",
        expression='3 * 10 ** col("float_col_1_mag") * arcsec',
        columns=_COLUMNS,
        result_val=94.8683,
        result_unit=u.arcsec,
    ),
    EvalCase(
        name="power_ten_scale_length_error_mag_units",
        expression='3 * 10 ** col("float_col_1_mag") * 2.302585093 * float_col_3_mag * arcsec',
        columns=_COLUMNS,
        result_val=10.9221,
        result_unit=u.arcsec,
    ),
    EvalCase(
        name="power_ten_scale_length_diff_error_mag_units",
        expression=(
            '3 * 10 ** (col("float_col_1_mag") - col("float_col_2_mag")) * 2.302585093 '
            '* (col("float_col_3_mag") ** 2 + col("float_col_4_mag") ** 2) ** 0.5 * arcsec'
        ),
        columns=_COLUMNS,
        result_val=7.0102,
        result_unit=u.arcsec,
    ),
    EvalCase(
        name="dex_scale_length",
        expression='3 * 10 ** col("float_col_1_dex") * arcsec',
        columns=_COLUMNS,
        result_val=14.9321,
        result_unit=u.arcsec,
    ),
    EvalCase(
        name="dex_scale_length_error",
        expression='3 * 10 ** col("float_col_1_dex") * 2.302585093 * float_col_3_dex * arcsec',
        columns=_COLUMNS,
        result_val=2.7162,
        result_unit=u.arcsec,
    ),
    EvalCase(
        name="dex_scale_length_diff",
        expression='3 * 10 ** (col("float_col_1_dex") - col("float_col_2_dex")) * arcsec',
        columns=_COLUMNS,
        result_val=11.0693,
        result_unit=u.arcsec,
    ),
    EvalCase(
        name="dex_scale_length_diff_error",
        expression=(
            '3 * 10 ** (col("float_col_1_dex") - col("float_col_2_dex")) * 2.302585093 '
            '* (col("float_col_3_dex") ** 2 + col("float_col_4_dex") ** 2) ** 0.5 * arcsec'
        ),
        columns=_COLUMNS,
        result_val=2.1363,
        result_unit=u.arcsec,
    ),
    EvalCase(
        name="brightness_column",
        expression='col("brightness_col_1")',
        columns=_COLUMNS,
        result_val=23.162,
        result_unit=u.Unit("mag/arcsec2"),
    ),
    EvalCase(name="string_literal", expression='"abc"', columns={}, result_val="abc"),
    EvalCase(
        name="string_column",
        expression='col("string_col_1")',
        columns=_COLUMNS,
        result_val="NGC 123",
    ),
    EvalCase(
        name="string_concat_columns",
        expression='col("string_col_2") + col("string_col_3")',
        columns=_COLUMNS,
        result_val="M82",
    ),
    EvalCase(
        name="string_concat_literal_prefix",
        expression='"M " + col("string_col_3")',
        columns=_COLUMNS,
        result_val="M 82",
    ),
    EvalCase(
        name="string_concat_literal_separator",
        expression='col("string_col_4") + " " + col("string_col_5")',
        columns=_COLUMNS,
        result_val="NGC 905",
    ),
    EvalCase(
        name="column_dimensionless",
        expression='col("float_col_1")',
        columns=_COLUMNS,
        result_val=1.5,
        result_unit=u.dimensionless_unscaled,
    ),
    EvalCase(
        name="column_angle_unit",
        expression='col("angle_col_2")',
        columns=_COLUMNS,
        result_val=1.5,
        result_unit=u.deg,
    ),
    EvalCase(
        name="column_mag_unit",
        expression='col("float_col_1_mag")',
        columns=_COLUMNS,
        result_val=1.5,
        result_unit=u.dimensionless_unscaled,
    ),
    EvalCase(
        name="column_dex_function_unit",
        expression='col("float_col_1_dex")',
        columns=_COLUMNS,
        result_val=0.697,
        result_unit=u.dimensionless_unscaled,
    ),
    EvalCase(
        name="column_over_constant",
        expression='col("const_col_1")',
        columns=_COLUMNS,
        result_val=1.0,
        result_unit=u.dimensionless_unscaled,
    ),
    EvalCase(
        name="trig_on_angle_column",
        expression='sin(col("angle_col_3"))',
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
    EvalCase(name="constant_arcmin", expression="arcmin", columns={}, result_val=1.0, result_unit=u.arcmin),
    EvalCase(
        name="function_sin",
        expression="sin(30 * deg)",
        columns={},
        result_val=0.5,
        result_unit=u.dimensionless_unscaled,
    ),
    EvalCase(name="error_missing_column_bare", expression="missing_col", columns={}, error=True),
    EvalCase(name="error_missing_column_call", expression='col("missing")', columns={}, error=True),
    EvalCase(name="error_incompatible_units", expression="arcsec + mag", columns={}, error=True),
    EvalCase(
        name="error_string_plus_number",
        expression='col("string_col_6") + col("float_col_1")',
        columns=_COLUMNS,
        error=True,
    ),
    EvalCase(
        name="error_modulo_dimensionless_divisor",
        expression='col("angle_col_1") % 180.0',
        columns=_COLUMNS,
        error=True,
    ),
    EvalCase(
        name="error_trig_on_dimensionless",
        expression='sin(col("float_col_dimless"))',
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
