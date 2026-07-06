from dataclasses import dataclass

import astropy.units as u
import numpy as np
import pytest

from uploader.app.lib import expression as legacy_expression
from uploader.app.lib.formula import (
    ExpressionEvaluationError,
    Value,
    column_quantity,
    evaluate,
    parse,
)
from uploader.app.lib.formula.namespace import FUNCTIONS, NAMED_CONSTANTS


@dataclass
class Col:
    value: float | str
    unit: str = ""


def evaluate_expr(source: str, columns: dict[str, Col]) -> Value:
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


def _legacy_values_units(columns: dict[str, Col]) -> tuple[dict[str, float], dict[str, str]]:
    values = {name: float(col.value) for name, col in columns.items() if isinstance(col.value, int | float)}
    units = {name: col.unit for name, col in columns.items()}
    return values, units


def test_isophotal_axis_expressions() -> None:
    columns = _sample_columns()
    a = evaluate_expr('3 * 10 ** col("logd25") * arcsec', columns)
    assert isinstance(a, u.Quantity)
    assert a.unit == u.arcsec
    assert abs(a.value - 94.86832980505137) < 1e-6

    e_a = evaluate_expr('3 * 10 ** col("logd25") * 2.302585093 * e_logd25 * arcsec', columns)
    assert isinstance(e_a, u.Quantity)
    assert e_a.unit == u.arcsec
    assert e_a.value > 0

    b = evaluate_expr('3 * 10 ** (col("logd25") - col("logr25")) * arcsec', columns)
    assert isinstance(b, u.Quantity)
    assert b.unit == u.arcsec
    assert b.value > 0

    e_b = evaluate_expr(
        '3 * 10 ** (col("logd25") - col("logr25")) * 2.302585093 '
        '* (col("e_logd25") ** 2 + col("e_logr25") ** 2) ** 0.5 * arcsec',
        columns,
    )
    assert isinstance(e_b, u.Quantity)
    assert e_b.unit == u.arcsec
    assert e_b.value > 0


def test_unit_aware_position_angle_modulo() -> None:
    columns = _sample_columns()
    pa = evaluate_expr('col("pa") % (180.0 * deg)', columns)
    assert isinstance(pa, u.Quantity)
    assert pa.unit == u.deg
    assert pa.value == 10.0


def test_isophotal_axis_expressions_with_logarithmic_column_units() -> None:
    columns = _sample_columns()
    for log_unit in ("mag", "dex"):
        columns_with_log = {
            **columns,
            "logd25": Col(1.5, log_unit),
            "logr25": Col(0.3, log_unit),
            "e_logd25": Col(0.05, log_unit),
            "e_logr25": Col(0.04, log_unit),
        }
        a = evaluate_expr('3 * 10 ** col("logd25") * arcsec', columns_with_log)
        assert isinstance(a, u.Quantity)
        assert a.unit == u.arcsec
        assert abs(a.value - 94.86832980505137) < 1e-6

        e_a = evaluate_expr('3 * 10 ** col("logd25") * 2.302585093 * e_logd25 * arcsec', columns_with_log)
        assert isinstance(e_a, u.Quantity)
        assert e_a.unit == u.arcsec
        assert e_a.value > 0

        e_b = evaluate_expr(
            '3 * 10 ** (col("logd25") - col("logr25")) * 2.302585093 '
            '* (col("e_logd25") ** 2 + col("e_logr25") ** 2) ** 0.5 * arcsec',
            columns_with_log,
        )
        assert isinstance(e_b, u.Quantity)
        assert e_b.unit == u.arcsec
        assert e_b.value > 0


def test_isophotal_axis_expressions_with_hyperleda_units() -> None:
    columns = {
        "logd25": Col(0.697, "dex(0.1 arcmin)"),
        "logr25": Col(0.13, "dex"),
        "e_logd25": Col(0.079, "dex(0.1 arcmin)"),
        "e_logr25": Col(0.028, "dex"),
        "pa": Col(161.14, "deg"),
    }
    a = evaluate_expr('3 * 10 ** col("logd25") * arcsec', columns)
    assert isinstance(a, u.Quantity)
    assert a.unit == u.arcsec
    assert a.value > 0

    e_a = evaluate_expr('3 * 10 ** col("logd25") * 2.302585093 * e_logd25 * arcsec', columns)
    assert isinstance(e_a, u.Quantity)
    assert e_a.unit == u.arcsec
    assert e_a.value > 0

    b = evaluate_expr('3 * 10 ** (col("logd25") - col("logr25")) * arcsec', columns)
    assert isinstance(b, u.Quantity)
    assert b.unit == u.arcsec
    assert b.value > 0

    e_b = evaluate_expr(
        '3 * 10 ** (col("logd25") - col("logr25")) * 2.302585093 '
        '* (col("e_logd25") ** 2 + col("e_logr25") ** 2) ** 0.5 * arcsec',
        columns,
    )
    assert isinstance(e_b, u.Quantity)
    assert e_b.unit == u.arcsec
    assert e_b.value > 0


def test_surface_brightness_column_keeps_units() -> None:
    columns = {"bri25": Col(23.162, "mag / arcsec2")}
    bri25 = evaluate_expr('col("bri25")', columns)
    assert isinstance(bri25, u.Quantity)
    assert bri25.unit == u.Unit("mag/arcsec2")


STRING_CASES: list[tuple[str, dict[str, Col], str]] = [
    ('"abc"', {}, "abc"),
    ('col("name")', {"name": Col("NGC 123")}, "NGC 123"),
    ('col("a") + col("b")', {"a": Col("M"), "b": Col("82")}, "M82"),
    ('"M " + col("id")', {"id": Col("82")}, "M 82"),
    ('col("a") + " " + col("b")', {"a": Col("NGC"), "b": Col("905")}, "NGC 905"),
]


@pytest.mark.parametrize("source,columns,expected", STRING_CASES)
def test_string_evaluation(source: str, columns: dict[str, Col], expected: str) -> None:
    assert evaluate_expr(source, columns) == expected


COERCION_CASES: list[tuple[float | str, str, object]] = [
    ("hello", "", "hello"),
    (1.5, "", 1.5),
    (1.5, "deg", u.Quantity(1.5, u.deg)),
    (1.5, "mag", 1.5),
    (1.5, "dex", 1.5),
    (0.697, "dex(0.1 arcmin)", 0.697),
    (23.162, "mag / arcsec2", u.Quantity(23.162, u.Unit("mag/arcsec2"))),
]


@pytest.mark.parametrize("value,unit,expected", COERCION_CASES)
def test_column_quantity(value: float | str, unit: str, expected: object) -> None:
    result = column_quantity(value, unit)
    if isinstance(expected, u.Quantity):
        assert isinstance(result, u.Quantity)
        assert result.unit == expected.unit
        assert result.value == pytest.approx(expected.value)
    else:
        assert result == expected


def test_name_precedence_constant_over_column() -> None:
    columns = {"pi": Col(1.0)}
    result = evaluate_expr("pi", columns)
    assert isinstance(result, u.Quantity)
    assert result.value == pytest.approx(np.pi)

    col_result = evaluate_expr('col("pi")', columns)
    assert isinstance(col_result, u.Quantity)
    assert col_result.value == 1.0
    assert col_result.unit == u.dimensionless_unscaled


def test_trig_on_angle_column() -> None:
    columns = {"pa": Col(30.0, "deg")}
    result = evaluate_expr('sin(col("pa"))', columns)
    assert isinstance(result, u.Quantity)
    assert result.value == pytest.approx(0.5)


EVAL_ERROR_CASES: list[tuple[str, dict[str, Col]]] = [
    ("missing_col", {}),
    ('col("missing")', {}),
    ("arcsec + mag", {}),
    ('col("name") + col("logd25")', {"name": Col("x"), "logd25": Col(1.5)}),
    ('col("pa") % 180.0', {"pa": Col(190.0, "deg")}),
    ('sin(col("x"))', {"x": Col(0.5)}),
]


@pytest.mark.parametrize("source,columns", EVAL_ERROR_CASES)
def test_evaluation_errors(source: str, columns: dict[str, Col]) -> None:
    with pytest.raises(ExpressionEvaluationError):
        evaluate_expr(source, columns)


PARITY_CASES: list[tuple[str, dict[str, Col]]] = [
    ('3 * 10 ** col("logd25") * arcsec', _sample_columns()),
    ('3 * 10 ** col("logd25") * 2.302585093 * e_logd25 * arcsec', _sample_columns()),
    ('3 * 10 ** (col("logd25") - col("logr25")) * arcsec', _sample_columns()),
    (
        '3 * 10 ** (col("logd25") - col("logr25")) * 2.302585093 '
        '* (col("e_logd25") ** 2 + col("e_logr25") ** 2) ** 0.5 * arcsec',
        _sample_columns(),
    ),
    ('col("bri25")', {"bri25": Col(23.162, "mag / arcsec2")}),
]


@pytest.mark.parametrize("source,columns", PARITY_CASES)
def test_parity_with_legacy_expression(source: str, columns: dict[str, Col]) -> None:
    formula_result = evaluate_expr(source, columns)
    values, units = _legacy_values_units(columns)
    legacy_result = legacy_expression.parse(source).evaluate(values, units)
    assert isinstance(formula_result, u.Quantity)
    assert formula_result.unit == legacy_result.unit
    assert formula_result.value == pytest.approx(legacy_result.value)


@pytest.mark.parametrize(
    "source,columns",
    [
        (
            '3 * 10 ** col("logd25") * arcsec',
            {
                **{k: Col(v.value, v.unit) for k, v in _sample_columns().items()},
                "logd25": Col(1.5, "mag"),
                "logr25": Col(0.3, "mag"),
                "e_logd25": Col(0.05, "mag"),
                "e_logr25": Col(0.04, "mag"),
            },
        ),
        (
            '3 * 10 ** col("logd25") * arcsec',
            {
                **{k: Col(v.value, v.unit) for k, v in _sample_columns().items()},
                "logd25": Col(1.5, "dex"),
                "logr25": Col(0.3, "dex"),
                "e_logd25": Col(0.05, "dex"),
                "e_logr25": Col(0.04, "dex"),
            },
        ),
        (
            '3 * 10 ** col("logd25") * arcsec',
            {
                "logd25": Col(0.697, "dex(0.1 arcmin)"),
                "logr25": Col(0.13, "dex"),
                "e_logd25": Col(0.079, "dex(0.1 arcmin)"),
                "e_logr25": Col(0.028, "dex"),
                "pa": Col(161.14, "deg"),
            },
        ),
    ],
)
def test_parity_with_logarithmic_units(source: str, columns: dict[str, Col]) -> None:
    formula_result = evaluate_expr(source, columns)
    values, units = _legacy_values_units(columns)
    legacy_result = legacy_expression.parse(source).evaluate(values, units)
    assert isinstance(formula_result, u.Quantity)
    assert formula_result.unit == legacy_result.unit
    assert formula_result.value == pytest.approx(legacy_result.value)


CONSTANT_GUARD_CASES = [(name, f"{name}") for name in NAMED_CONSTANTS]


@pytest.mark.parametrize("name,source", CONSTANT_GUARD_CASES)
def test_named_constants_are_usable(name: str, source: str) -> None:
    result = evaluate_expr(source, {})
    assert isinstance(result, u.Quantity)


FUNCTION_GUARD_CASES = [
    ("sin", "sin(30 * deg)"),
    ("cos", "cos(0 * rad)"),
]


@pytest.mark.parametrize("name,source", FUNCTION_GUARD_CASES)
def test_functions_are_usable(name: str, source: str) -> None:
    assert name in FUNCTIONS
    result = evaluate_expr(source, {})
    assert isinstance(result, u.Quantity)
