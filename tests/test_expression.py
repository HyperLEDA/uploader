import astropy.units as u

from uploader.app.lib.expression import parse


def _sample_values() -> tuple[dict[str, float], dict[str, str]]:
    values = {
        "logd25": 1.5,
        "logr25": 0.3,
        "e_logd25": 0.05,
        "e_logr25": 0.04,
        "pa": 190.0,
    }
    units = {
        "logd25": "",
        "logr25": "",
        "e_logd25": "",
        "e_logr25": "",
        "pa": "deg",
    }
    return values, units


def test_isophotal_axis_expressions() -> None:
    values, units = _sample_values()
    a = parse('3 * 10 ** col("logd25") * arcsec').evaluate(values, units)
    assert a.unit == u.arcsec
    assert abs(a.value - 94.86832980505137) < 1e-6

    e_a = parse('3 * 10 ** col("logd25") * 2.302585093 * e_logd25 * arcsec').evaluate(values, units)
    assert e_a.unit == u.arcsec
    assert e_a.value > 0

    b = parse('3 * 10 ** (col("logd25") - col("logr25")) * arcsec').evaluate(values, units)
    assert b.unit == u.arcsec
    assert b.value > 0

    e_b = parse(
        '3 * 10 ** (col("logd25") - col("logr25")) * 2.302585093 '
        '* (col("e_logd25") ** 2 + col("e_logr25") ** 2) ** 0.5 * arcsec',
    ).evaluate(values, units)
    assert e_b.unit == u.arcsec
    assert e_b.value > 0


def test_position_angle_modulo() -> None:
    values, units = _sample_values()
    pa = parse('col("pa") % 180.0').evaluate(values, units)
    assert pa.unit == u.deg
    assert pa.value == 10.0


def test_isophotal_axis_expressions_with_logarithmic_column_units() -> None:
    values, units = _sample_values()
    for log_unit in ("mag", "dex"):
        units_with_log = {**units, "logd25": log_unit, "logr25": log_unit, "e_logd25": log_unit, "e_logr25": log_unit}
        a = parse('3 * 10 ** col("logd25") * arcsec').evaluate(values, units_with_log)
        assert a.unit == u.arcsec
        assert abs(a.value - 94.86832980505137) < 1e-6

        e_a = parse('3 * 10 ** col("logd25") * 2.302585093 * e_logd25 * arcsec').evaluate(values, units_with_log)
        assert e_a.unit == u.arcsec
        assert e_a.value > 0

        e_b = parse(
            '3 * 10 ** (col("logd25") - col("logr25")) * 2.302585093 '
            '* (col("e_logd25") ** 2 + col("e_logr25") ** 2) ** 0.5 * arcsec',
        ).evaluate(values, units_with_log)
        assert e_b.unit == u.arcsec
        assert e_b.value > 0


def test_isophotal_axis_expressions_with_hyperleda_units() -> None:
    values = {
        "logd25": 0.697,
        "logr25": 0.13,
        "e_logd25": 0.079,
        "e_logr25": 0.028,
        "pa": 161.14,
    }
    units = {
        "logd25": "dex(0.1 arcmin)",
        "logr25": "dex",
        "e_logd25": "dex(0.1 arcmin)",
        "e_logr25": "dex",
        "pa": "deg",
    }
    a = parse('3 * 10 ** col("logd25") * arcsec').evaluate(values, units)
    assert a.unit == u.arcsec
    assert a.value > 0

    e_a = parse('3 * 10 ** col("logd25") * 2.302585093 * e_logd25 * arcsec').evaluate(values, units)
    assert e_a.unit == u.arcsec
    assert e_a.value > 0

    b = parse('3 * 10 ** (col("logd25") - col("logr25")) * arcsec').evaluate(values, units)
    assert b.unit == u.arcsec
    assert b.value > 0

    e_b = parse(
        '3 * 10 ** (col("logd25") - col("logr25")) * 2.302585093 '
        '* (col("e_logd25") ** 2 + col("e_logr25") ** 2) ** 0.5 * arcsec',
    ).evaluate(values, units)
    assert e_b.unit == u.arcsec
    assert e_b.value > 0


def test_surface_brightness_column_keeps_units() -> None:
    values = {"bri25": 23.162}
    units = {"bri25": "mag / arcsec2"}
    bri25 = parse('col("bri25")').evaluate(values, units)
    assert bri25.unit == u.Unit("mag/arcsec2")


def test_bare_column_referenced_in_parse() -> None:
    expr = parse("3 * 10 ** col('logd25') * 2.302585093 * e_logd25 * arcsec")
    assert expr.referenced_columns == {"logd25", "e_logd25"}
