from dataclasses import dataclass

import astropy.constants as const
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
        name="string_concat_with_str",
        expression='col("string_col_2") + " " + str(col("float_col"))',
        columns=_COLUMNS,
        result_val="M 1.5",
    ),
    EvalCase(
        name="str_on_string_column",
        expression='str(col("string_col_1"))',
        columns=_COLUMNS,
        result_val="NGC 123",
    ),
    EvalCase(
        name="str_integer",
        expression='str(col("num"))',
        columns={"num": Col(495444.0)},
        result_val="495444",
    ),
    EvalCase(
        name="str_keeps_fraction",
        expression='str(col("float_col"))',
        columns=_COLUMNS,
        result_val="1.5",
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
        name="sqrt",
        expression='sqrt(col("float_col") ** 2)',
        columns=_COLUMNS,
        result_val=1.5,
        result_unit=u.dimensionless_unscaled,
    ),
    EvalCase(
        name="tan",
        expression="tan(45 * deg)",
        columns={},
        result_val=1.0,
        result_unit=u.dimensionless_unscaled,
    ),
    EvalCase(
        name="asin",
        expression='asin(col("float_col_dimless"))',
        columns=_COLUMNS,
        result_val=0.5236,
        result_unit=u.rad,
    ),
    EvalCase(
        name="acos",
        expression="acos(1)",
        columns={},
        result_val=0.0,
        result_unit=u.rad,
    ),
    EvalCase(
        name="atan",
        expression="atan(1)",
        columns={},
        result_val=0.7854,
        result_unit=u.rad,
    ),
    EvalCase(
        name="atan2",
        expression="atan2(0, 1)",
        columns={},
        result_val=0.0,
        result_unit=u.rad,
    ),
    EvalCase(
        name="deg2rad",
        expression="deg2rad(180)",
        columns={},
        result_val=np.pi,
        result_unit=u.rad,
    ),
    EvalCase(
        name="rad2deg",
        expression="rad2deg(pi)",
        columns={},
        result_val=180.0,
        result_unit=u.deg,
    ),
    EvalCase(
        name="wrap360",
        expression="wrap360(370 * deg)",
        columns={},
        result_val=10.0,
        result_unit=u.deg,
    ),
    EvalCase(
        name="wrap360_negative",
        expression="wrap360(-10 * deg)",
        columns={},
        result_val=350.0,
        result_unit=u.deg,
    ),
    EvalCase(
        name="exp",
        expression="exp(0)",
        columns={},
        result_val=1.0,
        result_unit=u.dimensionless_unscaled,
    ),
    EvalCase(
        name="log10",
        expression="log10(100)",
        columns={},
        result_val=2.0,
        result_unit=u.dimensionless_unscaled,
    ),
    EvalCase(
        name="ln",
        expression="ln(1)",
        columns={},
        result_val=0.0,
        result_unit=u.dimensionless_unscaled,
    ),
    EvalCase(
        name="pow",
        expression="pow(2, 3)",
        columns={},
        result_val=8.0,
        result_unit=u.dimensionless_unscaled,
    ),
    EvalCase(
        name="max",
        expression="max(2, 3)",
        columns={},
        result_val=3.0,
        result_unit=u.dimensionless_unscaled,
    ),
    EvalCase(
        name="min",
        expression="min(2, 3)",
        columns={},
        result_val=2.0,
        result_unit=u.dimensionless_unscaled,
    ),
    EvalCase(
        name="max_equivalent_units",
        expression="max(10 * deg, 1 * rad)",
        columns={},
        result_val=57.2958,
        result_unit=u.deg,
    ),
    EvalCase(
        name="vector_max",
        expression='max(col("vec_col"), 2)',
        columns=_COLUMNS,
        result_val=[2.0, 2.0, 3.0],
        result_unit=u.dimensionless_unscaled,
    ),
    EvalCase(
        name="error_max_incompatible_units",
        expression="max(1 * deg, 1)",
        columns={},
        error=True,
    ),
    EvalCase(
        name="vector_sqrt",
        expression='sqrt(col("vec_col"))',
        columns=_COLUMNS,
        result_val=[1.0, 1.4142, 1.7321],
        result_unit=u.dimensionless_unscaled,
    ),
    EvalCase(
        name="constant",
        expression="pi",
        columns={},
        result_val=3.1416,
        result_unit=u.dimensionless_unscaled,
    ),
    EvalCase(
        name="speed_of_light",
        expression="c",
        columns={},
        result_val=299792458.0,
        result_unit=u.m / u.s,
    ),
    EvalCase(
        name="solar_mass",
        expression="M_sun",
        columns={},
        result_val=const.M_sun.value,
        result_unit=u.kg,
    ),
    EvalCase(
        name="parsec",
        expression="pc",
        columns={},
        result_val=1.0,
        result_unit=u.pc,
    ),
    EvalCase(
        name="jansky",
        expression="Jy",
        columns={},
        result_val=1.0,
        result_unit=u.Jy,
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
    EvalCase(
        name="to_deg_sexagesimal_hourangle",
        expression='to_deg(col("ra"))',
        columns={"ra": Col("00 02 08.4", "hourangle")},
        result_val=0.535,
        result_unit=u.deg,
    ),
    EvalCase(
        name="to_deg_sexagesimal_deg",
        expression='to_deg(col("dec"))',
        columns={"dec": Col("+16 35 13", "deg")},
        result_val=16.5869,
        result_unit=u.deg,
    ),
    EvalCase(
        name="to_deg_embedded_unit_string",
        expression='to_deg(col("ra"))',
        columns={"ra": Col("00h02m08.4s")},
        result_val=0.535,
        result_unit=u.deg,
    ),
    EvalCase(
        name="to_deg_quantity",
        expression='to_deg(col("angle_col"))',
        columns=_COLUMNS,
        result_val=190.0,
        result_unit=u.deg,
    ),
    EvalCase(
        name="to_deg_hourangle_quantity",
        expression='to_deg(col("ra"))',
        columns={"ra": Col(1.0, "hourangle")},
        result_val=15.0,
        result_unit=u.deg,
    ),
    EvalCase(
        name="string_column_with_unit_concat",
        expression='col("ra") + "x"',
        columns={"ra": Col("00 02 08.4", "hourangle")},
        result_val="00 02 08.4x",
    ),
    EvalCase(
        name="error_to_deg_bare_string",
        expression='to_deg(col("ra"))',
        columns={"ra": Col("00 02 08.4")},
        error=True,
    ),
    EvalCase(
        name="unit_from_string",
        expression='unit("km")',
        columns={},
        result_val=1.0,
        result_unit=u.km,
    ),
    EvalCase(
        name="unit_multiplied",
        expression='col("float_col") * unit("Mpc")',
        columns=_COLUMNS,
        result_val=1.5,
        result_unit=u.Mpc,
    ),
    EvalCase(
        name="unit_composite",
        expression='col("float_col") * unit("km/s")',
        columns=_COLUMNS,
        result_val=1.5,
        result_unit=u.km / u.s,
    ),
    EvalCase(
        name="where_true",
        expression='where(col("float_col") > 1, col("float_col"), 0)',
        columns=_COLUMNS,
        result_val=1.5,
        result_unit=u.dimensionless_unscaled,
    ),
    EvalCase(
        name="where_false",
        expression='where(col("float_col") > 2, 10, col("float_col"))',
        columns=_COLUMNS,
        result_val=1.5,
        result_unit=u.dimensionless_unscaled,
    ),
    EvalCase(
        name="where_nested",
        expression='where(col("float_col") > 2, 1, where(col("float_col") > 1, 2, 3))',
        columns=_COLUMNS,
        result_val=2,
    ),
    EvalCase(
        name="where_angle",
        expression='where(col("angle_col") > 180 * deg, col("angle_col") - 360 * deg, col("angle_col"))',
        columns=_COLUMNS,
        result_val=-170.0,
        result_unit=u.deg,
    ),
    EvalCase(
        name="where_string",
        expression='where(col("string_col_2") == "M", col("string_col_1"), "x")',
        columns=_COLUMNS,
        result_val="NGC 123",
    ),
    EvalCase(
        name="where_vector",
        expression='where(col("vec_col") > 1.5, col("vec_col"), 0)',
        columns=_COLUMNS,
        result_val=[0.0, 2.0, 3.0],
        result_unit=u.dimensionless_unscaled,
    ),
    EvalCase(
        name="where_vector_angle",
        expression='where(col("vec_angle_col") > 90 * deg, col("vec_angle_col"), 0 * deg)',
        columns=_COLUMNS,
        result_val=[0.0, 0.0, 180.0],
        result_unit=u.deg,
    ),
    EvalCase(
        name="where_vector_string",
        expression='where(col("vec_string_a") == "IC", col("vec_string_b"), "x")',
        columns=_COLUMNS,
        result_val=["x", "456", "x"],
    ),
    EvalCase(name="error_unknown_unit", expression='unit("not_a_unit")', columns={}, error=True),
    EvalCase(name="error_unit_non_string", expression="unit(1)", columns={}, error=True),
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
