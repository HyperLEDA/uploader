import pytest

from app.name_checker.rules import RULES


@pytest.mark.parametrize(
    "input_name,expected",
    [
        ("NGC 905", "NGC 905"),
        ("NGC905", "NGC 905"),
        ("NGC00905", "NGC 905"),
        ("NGC  905", "NGC 905"),
        ("ngc 905", "NGC 905"),
        ("NGC", None),
        ("NGC ", None),
        ("M 31", None),
        ("UGC 123", None),
        ("", None),
    ],
)
def test_ngc_rule(input_name: str, expected: str | None) -> None:
    ngc_rule = next(r for r in RULES if r.name == "NGC")
    if expected is None:
        assert ngc_rule.match(input_name) is None
    else:
        assert ngc_rule.match(input_name) == expected


def test_unmatched_name_returns_none_from_all_rules() -> None:
    for rule in RULES:
        assert rule.match("unknown catalog XYZ 123") is None


@pytest.mark.parametrize(
    "input_name,expected",
    [
        ("SDSSJ121551.62+573421.6", "SDSS J121551.62+573421.6"),
        ("SDSSJ121552.44+294932.9", "SDSS J121552.44+294932.9"),
        ("SDSSJ121553.17+202452.4", "SDSS J121553.17+202452.4"),
        ("sdssj121551.62+573421.6", "SDSS J121551.62+573421.6"),
        ("SDSSJ121551.62-573421.6", "SDSS J121551.62-573421.6"),
        ("SDSSJ121551.62+57342.6", None),
        ("SDSS J121551.62+573421.6", None),
        ("", None),
    ],
)
def test_sdss_rule(input_name: str, expected: str | None) -> None:
    sdss_rule = next(r for r in RULES if r.name == "SDSS")
    if expected is None:
        assert sdss_rule.match(input_name) is None
    else:
        assert sdss_rule.match(input_name) == expected


@pytest.mark.parametrize(
    "input_name,expected",
    [
        ("PGC1191069", "PGC 1191069"),
        ("PGC1119121", "PGC 1119121"),
        ("PGC1425552", "PGC 1425552"),
        ("PGC 1191069", "PGC 1191069"),
        ("PGC001191069", "PGC 1191069"),
        ("pgc1191069", "PGC 1191069"),
        ("PGC", None),
        ("PGC ", None),
        ("UGC 123", None),
        ("", None),
    ],
)
def test_pgc_rule(input_name: str, expected: str | None) -> None:
    pgc_rule = next(r for r in RULES if r.name == "PGC")
    if expected is None:
        assert pgc_rule.match(input_name) is None
    else:
        assert pgc_rule.match(input_name) == expected
