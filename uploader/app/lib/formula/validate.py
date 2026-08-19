import ast
from dataclasses import asdict, dataclass
from typing import Annotated, final

from pydantic import AfterValidator, ValidationError
from pydantic_core import PydanticCustomError

from uploader.app.lib.formula.constants import NAMED_CONSTANTS
from uploader.app.lib.formula.functions import COL_FUNCTION, FUNCTIONS


@final
@dataclass(frozen=True)
class ExpressionDiagnostic:
    message: str
    start_line: int
    start_column: int
    end_line: int
    end_column: int


@final
@dataclass(frozen=True)
class FormError:
    path: tuple[str, ...]
    message: str
    start_line: int
    start_column: int
    end_line: int
    end_column: int


def _validate_expression_str(source: str) -> str:
    diagnostics = validate_expression(source)
    if not diagnostics:
        return source
    raise PydanticCustomError(
        "expression",
        "invalid expression",
        {"diagnostics": [asdict(item) for item in diagnostics]},
    )


ExpressionStr = Annotated[str, AfterValidator(_validate_expression_str)]


def _from_syntax_error(error: SyntaxError, source: str) -> ExpressionDiagnostic:
    start_line = error.lineno if error.lineno and error.lineno > 0 else 1
    start_column = error.offset if error.offset and error.offset > 0 else 1
    end_line = error.end_lineno if error.end_lineno and error.end_lineno > 0 else start_line
    end_column = error.end_offset if error.end_offset and error.end_offset > start_column else start_column + 1
    lines = source.splitlines() or [source]
    if start_line == end_line and start_line <= len(lines):
        end_column = min(max(end_column, start_column + 1), len(lines[start_line - 1]) + 1)
    return ExpressionDiagnostic(
        message=error.msg or "invalid syntax",
        start_line=start_line,
        start_column=start_column,
        end_line=end_line,
        end_column=end_column,
    )


def _from_node(node: ast.AST, message: str) -> ExpressionDiagnostic:
    start_line = getattr(node, "lineno", 1) or 1
    start_column = (getattr(node, "col_offset", 0) or 0) + 1
    end_line = getattr(node, "end_lineno", None) or start_line
    end_offset = getattr(node, "end_col_offset", None)
    end_column = end_offset + 1 if end_offset is not None else start_column + 1
    if end_line == start_line and end_column <= start_column:
        end_column = start_column + 1
    return ExpressionDiagnostic(
        message=message,
        start_line=start_line,
        start_column=start_column,
        end_line=end_line,
        end_column=end_column,
    )


def _column_from_call(node: ast.Call) -> str | None:
    if not isinstance(node.func, ast.Name) or node.func.id != COL_FUNCTION.name:
        return None
    if node.keywords or len(node.args) != 1:
        raise ValueError(f"{COL_FUNCTION.name}() takes exactly one string argument")
    arg = node.args[0]
    if not isinstance(arg, ast.Constant) or not isinstance(arg.value, str):
        raise ValueError(f"{COL_FUNCTION.name}() argument must be a string literal")
    return arg.value


@final
class _DiagnosticCollector(ast.NodeVisitor):
    def __init__(self) -> None:
        self.columns: set[str] = set()
        self.diagnostics: list[ExpressionDiagnostic] = []

    def visit_Call(self, node: ast.Call) -> None:
        try:
            column = _column_from_call(node)
        except ValueError as e:
            self.diagnostics.append(_from_node(node, str(e)))
            return
        if column is not None:
            self.columns.add(column)
            return
        if not isinstance(node.func, ast.Name):
            self.diagnostics.append(_from_node(node.func, "only simple function calls are allowed"))
            self.generic_visit(node)
            return
        if all(fn.name != node.func.id for fn in FUNCTIONS):
            self.diagnostics.append(_from_node(node.func, f"unknown function: {node.func.id}"))
        if node.keywords:
            self.diagnostics.append(_from_node(node, "keyword arguments are not allowed"))
        for arg in node.args:
            self.visit(arg)

    def visit_Name(self, node: ast.Name) -> None:
        if any(c.name == node.id for c in NAMED_CONSTANTS) or any(fn.name == node.id for fn in FUNCTIONS):
            return
        self.diagnostics.append(
            _from_node(
                node,
                f"unknown name {node.id!r}; use {COL_FUNCTION.name}() for columns or a predefined constant/function",
            ),
        )


def diagnose_expression(source: str) -> tuple[ast.AST | None, frozenset[str], list[ExpressionDiagnostic]]:
    try:
        tree = ast.parse(source, mode="eval")
    except SyntaxError as e:
        return None, frozenset(), [_from_syntax_error(e, source)]
    collector = _DiagnosticCollector()
    collector.visit(tree.body)
    return tree, frozenset(collector.columns), collector.diagnostics


def validate_expression(source: str) -> list[ExpressionDiagnostic]:
    if not source.strip():
        return []
    _, _, diagnostics = diagnose_expression(source)
    return diagnostics


def expression_form_errors(error: ValidationError) -> list[FormError]:
    result: list[FormError] = []
    for item in error.errors():
        if item["type"] != "expression":
            continue
        ctx = item.get("ctx") or {}
        diagnostics = ctx.get("diagnostics")
        if not isinstance(diagnostics, list):
            continue
        path = tuple(str(part) for part in item["loc"])
        for diagnostic in diagnostics:
            if not isinstance(diagnostic, dict):
                continue
            result.append(
                FormError(
                    path=path,
                    message=str(diagnostic["message"]),
                    start_line=int(diagnostic["start_line"]),
                    start_column=int(diagnostic["start_column"]),
                    end_line=int(diagnostic["end_line"]),
                    end_column=int(diagnostic["end_column"]),
                ),
            )
    return result
