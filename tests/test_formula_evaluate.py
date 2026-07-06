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
    "float_col": Col(1.5),
    "angle_col": Col(190.0, "deg"),
    "float_col_mag": Col(1.5, "mag"),
    "float_col_dex": Col(0.697, "dex(0.1 arcmin)"),
    "brightness_col": Col(23.162, "mag / arcsec2"),
    "string_col_1": Col("NGC 123"),
    "string_col_2": Col("M"),
    "float_col_dimless": Col(0.5),
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
