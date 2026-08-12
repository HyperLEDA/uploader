from collections.abc import Mapping
from dataclasses import dataclass
from types import CodeType
from typing import final

import astropy.units as u

from uploader.app.lib.formula.errors import ExpressionEvaluationError, ExpressionSyntaxError
from uploader.app.lib.formula.namespace import build_namespace
from uploader.app.lib.formula.validate import diagnose_expression
from uploader.app.lib.formula.values import Value


@final
@dataclass(frozen=True)
class Expression:
    referenced_columns: frozenset[str]
    code: CodeType


def parse(source: str) -> Expression:
    tree, referenced_columns, diagnostics = diagnose_expression(source.strip())
    if tree is None or diagnostics:
        message = diagnostics[0].message if diagnostics else "invalid expression"
        raise ExpressionSyntaxError(message)
    code = compile(tree, "<expression>", "eval")
    return Expression(referenced_columns=referenced_columns, code=code)


def evaluate(expression: Expression, columns: Mapping[str, Value]) -> Value:
    try:
        return eval(expression.code, build_namespace(columns))  # noqa: S307
    except (KeyError, NameError, TypeError, ValueError, ZeroDivisionError, u.UnitsError) as e:
        raise ExpressionEvaluationError(str(e)) from e
