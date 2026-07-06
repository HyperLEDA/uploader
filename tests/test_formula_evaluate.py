from dataclasses import dataclass

import astropy.units as u
import numpy as np
import pytest

from uploader.app.lib.formula import ExpressionEvaluationError, column_quantity, evaluate, parse


@dataclass
class Col:
    value: float | str | list[float] | list[str]
    unit: str = ""


@dataclass
class EvalCase:
    expression: str
    columns: dict[str, Col]
    result_val: str | float | list[float] | list[str] | None = None
    result_unit: u.Unit | None = None
    error: bool = False
    name: str = ""


def evaluate_expr(source: str, columns: dict[str, Col]) -> object:
    built = {name: column_quantity(col.value, col.unit) for name, col in columns.items()}
    return evaluate(parse(source), built)


_COLUMNS: dict[str, Col] = {
    "float_col": Col(1.5),
    "angle_col": Col(190.0, "deg"),
    "float_col_mag": Col(1.5, "mag"),
    "float_col_dex": Col(0.697, "dex(0.1 arcmin)"),
    "brightness_col": Col(23.162, "mag / arcsec2"),
    "string_col_1": Col("NGC 123"),
    "string_col_2": Col("M"),
    "float_col_dimless": Col(0.5),
    "vec_col": Col([1.0, 2.0, 3.0]),
    "vec_angle_col": Col([0.0, 90.0, 180.0], "deg"),
    "vec_string_a": Col(["NGC", "IC", "M"]),
    "vec_string_b": Col(["123", "456", "789"]),
}


EVAL_CASES: list[EvalCase] = [
    EvalCase(
        name="dimentionless_expr",
        expression='3 * 10 ** col("float_col") * arcsec',
        columns=_COLUMNS,
        result_val=94.8683,
        result_unit=u.arcsec,
    ),
    EvalCase(
        name="modulo_edgecase",
        expression='col("angle_col") % (180.0 * deg)',
        columns=_COLUMNS,
        result_val=10.0,
        result_unit=u.deg,
    ),
    EvalCase(
        name="single_column",
        expression='col("brightness_col")',
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
        expression='col("string_col_2") + " " + col("string_col_1")',
        columns=_COLUMNS,
        result_val="M NGC 123",
    ),
    EvalCase(
        name="function_unit",
        expression='col("float_col_dex")',
        columns=_COLUMNS,
        result_val=0.697,
        result_unit=u.dimensionless_unscaled,
    ),
    EvalCase(
        name="nested_functions",
        expression='sin(col("angle_col"))',
        columns=_COLUMNS,
        result_val=-0.1736,
        result_unit=u.dimensionless_unscaled,
    ),
    EvalCase(
        name="constant",
        expression="pi",
        columns={},
        result_val=3.1416,
        result_unit=u.dimensionless_unscaled,
    ),
    EvalCase(name="error_missing_column_call", expression='col("missing")', columns={}, error=True),
    EvalCase(name="error_incompatible_units", expression="arcsec + mag", columns={}, error=True),
    EvalCase(
        name="error_string_plus_number",
        expression='col("string_col_1") + col("float_col")',
        columns=_COLUMNS,
        error=True,
    ),
    EvalCase(
        name="error_modulo_dimensionless_divisor",
        expression='col("angle_col") % 180.0',
        columns=_COLUMNS,
        error=True,
    ),
    EvalCase(
        name="error_trig_on_dimensionless",
        expression='sin(col("float_col_dimless"))',
        columns=_COLUMNS,
        error=True,
    ),
    EvalCase(
        name="vector_column_with_unit",
        expression='col("vec_angle_col")',
        columns=_COLUMNS,
        result_val=[0.0, 90.0, 180.0],
        result_unit=u.deg,
    ),
    EvalCase(
        name="vector_arithmetic",
        expression='3 * 10 ** col("vec_col") * arcsec',
        columns=_COLUMNS,
        result_val=[30.0, 300.0, 3000.0],
        result_unit=u.arcsec,
    ),
    EvalCase(
        name="vector_trig",
        expression='sin(col("vec_angle_col"))',
        columns=_COLUMNS,
        result_val=[0.0, 1.0, 0.0],
        result_unit=u.dimensionless_unscaled,
    ),
    EvalCase(
        name="scalar_vector_broadcast",
        expression='col("float_col") + col("vec_col")',
        columns=_COLUMNS,
        result_val=[2.5, 3.5, 4.5],
        result_unit=u.dimensionless_unscaled,
    ),
    EvalCase(
        name="vector_string_concat",
        expression='col("vec_string_a") + " " + col("vec_string_b")',
        columns=_COLUMNS,
        result_val=["NGC 123", "IC 456", "M 789"],
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
        if isinstance(case.result_val, list):
            assert isinstance(result, np.ndarray)
            np.testing.assert_array_equal(result, case.result_val)
        else:
            assert result == case.result_val
        return

    assert isinstance(result, u.Quantity)
    assert result.unit == case.result_unit
    if isinstance(case.result_val, list):
        np.testing.assert_allclose(
            np.asarray(result.value),
            np.asarray(case.result_val),
            rtol=1e-4,
            atol=1e-10,
        )
    else:
        np.testing.assert_almost_equal(result.value, case.result_val, decimal=4)
