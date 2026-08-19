import pytest

from uploader.app.lib.formula import ExpressionSyntaxError, parse

PARSE_CASES: list[tuple[str, set[str] | type[ExpressionSyntaxError]]] = [
    ('col("a") + col("b")', {"a", "b"}),
    ('col("weird name")', {"weird name"}),
    ('sin(col("pa")) + pi', {"pa"}),
    ('to_deg(col("RAJ2000"))', {"RAJ2000"}),
    ('col("dist") * unit("Mpc")', {"dist"}),
    ('3 * 10 ** col("logd25") * col("e_logd25") * arcsec', {"logd25", "e_logd25"}),
    ('"M " + col("id")', {"id"}),
    ("1 + 2", set()),
    ("pi + deg", set()),
    ("M_sun * G / c ** 2", set()),
    ("", ExpressionSyntaxError),
    ("1 +", ExpressionSyntaxError),
    ("col(", ExpressionSyntaxError),
    ("* 2", ExpressionSyntaxError),
    ("a = 1", ExpressionSyntaxError),
    ("col()", ExpressionSyntaxError),
    ("col(x)", ExpressionSyntaxError),
    ('col("a", "b")', ExpressionSyntaxError),
    ("col(1)", ExpressionSyntaxError),
    ("e_logd25 + logd25", ExpressionSyntaxError),
    ("logd25 + logd25", ExpressionSyntaxError),
    ('3 * 10 ** col("logd25") * e_logd25 * arcsec', ExpressionSyntaxError),
    ("atan2(1, 0) + wrap360(10 * deg)", set()),
    ("unknown(1)", ExpressionSyntaxError),
    ("col", ExpressionSyntaxError),
]


@pytest.mark.parametrize("source,expected", PARSE_CASES)
def test_parse(source: str, expected: set[str] | type[ExpressionSyntaxError]) -> None:
    if isinstance(expected, type):
        with pytest.raises(expected):
            parse(source)
    else:
        assert parse(source).referenced_columns == expected
