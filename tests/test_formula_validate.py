from dataclasses import asdict

from uploader.app.lib.formula import validate_expression


def test_validate_expression_accepts_valid() -> None:
    assert validate_expression('to_deg(col("RAJ2000"))') == []
    assert validate_expression("sin(pi) + 1.5 * deg") == []
    assert validate_expression("") == []


def test_validate_expression_reports_python_syntax_errors() -> None:
    unclosed_string = validate_expression('col("a)')
    assert len(unclosed_string) == 1
    assert "string" in unclosed_string[0].message.lower()
    assert unclosed_string[0].start_line == 1
    assert unclosed_string[0].start_column >= 1

    unclosed_paren = validate_expression('col("a"')
    assert len(unclosed_paren) == 1
    assert unclosed_paren[0].message == "'(' was never closed"
    assert asdict(unclosed_paren[0])["start_column"] == 4

    unmatched = validate_expression("1)")
    assert len(unmatched) == 1
    assert unmatched[0].message == "unmatched ')'"


def test_validate_expression_reports_unknown_names() -> None:
    diagnostics = validate_expression('foo + col("a")')
    assert len(diagnostics) == 1
    assert "foo" in diagnostics[0].message
    assert diagnostics[0].start_column == 1
    assert diagnostics[0].end_column == 4


def test_validate_expression_ignores_names_inside_strings() -> None:
    assert validate_expression('col("foo_bar")') == []
