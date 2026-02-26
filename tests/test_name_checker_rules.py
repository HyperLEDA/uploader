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
    ],
)
def test_ngc_rule_matches_and_normalizes(input_name: str, expected: str) -> None:
    ngc_rule = next(r for r in RULES if r.name == "NGC")
    assert ngc_rule.match(input_name) == expected


@pytest.mark.parametrize(
    "input_name",
    [
        "NGC",
        "NGC ",
        "M 31",
        "UGC 123",
        "",
    ],
)
def test_ngc_rule_does_not_match(input_name: str) -> None:
    ngc_rule = next(r for r in RULES if r.name == "NGC")
    assert ngc_rule.match(input_name) is None


def test_unmatched_name_returns_none_from_all_rules() -> None:
    for rule in RULES:
        assert rule.match("unknown catalog XYZ 123") is None
