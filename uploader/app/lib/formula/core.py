import ast
from collections.abc import Mapping
from dataclasses import dataclass
from types import CodeType
from typing import final

import astropy.units as u

from uploader.app.lib.formula.errors import ExpressionEvaluationError, ExpressionSyntaxError
from uploader.app.lib.formula.namespace import COL_FUNCTION, FUNCTIONS, NAMED_CONSTANTS, build_namespace
from uploader.app.lib.formula.values import Value


def _column_from_call(node: ast.Call) -> str | None:
    if not isinstance(node.func, ast.Name) or node.func.id != COL_FUNCTION:
        return None
    if node.keywords or len(node.args) != 1:
        raise ValueError(f"{COL_FUNCTION}() takes exactly one string argument")
    arg = node.args[0]
    if not isinstance(arg, ast.Constant) or not isinstance(arg.value, str):
        raise ValueError(f"{COL_FUNCTION}() argument must be a string literal")
    return arg.value


@final
@dataclass(frozen=True)
class Expression:
    referenced_columns: frozenset[str]
    code: CodeType


def parse(source: str) -> Expression:
    try:
        tree = ast.parse(source.strip(), mode="eval")
    except SyntaxError as e:
        raise ExpressionSyntaxError(str(e)) from e
    try:
        referenced_columns = frozenset(_ColumnCollector().collect(tree.body))
    except ValueError as e:
        raise ExpressionSyntaxError(str(e)) from e
    code = compile(tree, "<expression>", "eval")
    return Expression(referenced_columns=referenced_columns, code=code)


def evaluate(expression: Expression, columns: Mapping[str, Value]) -> Value:
    try:
        return eval(expression.code, build_namespace(columns))  # noqa: S307
    except (KeyError, NameError, TypeError, ValueError, ZeroDivisionError, u.UnitsError) as e:
        raise ExpressionEvaluationError(str(e)) from e


@final
class _ColumnCollector(ast.NodeVisitor):
    def __init__(self) -> None:
        self.columns: set[str] = set()

    def collect(self, node: ast.AST) -> set[str]:
        self.visit(node)
        return self.columns

    def visit_Call(self, node: ast.Call) -> None:
        column = _column_from_call(node)
        if column is not None:
            self.columns.add(column)
            return
        for arg in node.args:
            self.visit(arg)

    def visit_Name(self, node: ast.Name) -> None:
        if node.id in NAMED_CONSTANTS or node.id in FUNCTIONS:
            return
        self.columns.add(node.id)
