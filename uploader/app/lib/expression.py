import ast
import operator
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import final

import astropy.constants as const
import astropy.units as u
import numpy as np

COL_FUNCTION = "col"

NAMED_CONSTANTS: dict[str, u.Quantity] = {
    "pi": np.pi * u.dimensionless_unscaled,
    "c": const.c,
    "deg": 1 * u.deg,
    "rad": 1 * u.rad,
    "arcmin": 1 * u.arcmin,
    "arcsec": 1 * u.arcsec,
    "mag": 1 * u.mag,
}


def expression_syntax_help() -> str:
    constants = ", ".join(sorted(NAMED_CONSTANTS))
    return (
        f'Use {COL_FUNCTION}("name") to refer to rawdata columns '
        '(e.g. col("a"), col("SMASB22.5"), col("PA-LEDA")).\n'
        "Bare identifiers refer to predefined constants.\n"
        "Operators: + - * /.\n"
        "Functions: sin(x), cos(x) (argument must be an angle).\n"
        "Numbers are dimensionless.\n"
        f"Available constants: {constants}."
    )


type _QuantityBinOp = Callable[[u.Quantity, u.Quantity], u.Quantity]
type _QuantityUnaryOp = Callable[[u.Quantity], u.Quantity]
type _QuantityFunc = Callable[[u.Quantity], u.Quantity | float]

_BINOPS: dict[type[ast.operator], _QuantityBinOp] = {
    ast.Add: operator.add,
    ast.Sub: operator.sub,
    ast.Mult: operator.mul,
    ast.Div: operator.truediv,
}

_UNARYOPS: dict[type[ast.unaryop], _QuantityUnaryOp] = {
    ast.UAdd: operator.pos,
    ast.USub: operator.neg,
}

_FUNCTIONS: dict[str, _QuantityFunc] = {
    "sin": np.sin,
    "cos": np.cos,
}


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
@dataclass
class Expression:
    _tree: ast.Expression
    referenced_columns: set[str] = field(default_factory=set)

    def evaluate(self, values: dict[str, float], units: dict[str, str]) -> u.Quantity:
        return _Evaluator(values, units).visit(self._tree.body)


def parse(source: str) -> Expression:
    tree = ast.parse(source.strip(), mode="eval")
    referenced_columns = _collect_columns(tree.body)
    return Expression(_tree=tree, referenced_columns=referenced_columns)


def _collect_columns(node: ast.AST) -> set[str]:
    return _ColumnCollector().collect(node)


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


@final
class _Evaluator(ast.NodeVisitor):
    def __init__(self, values: dict[str, float], units: dict[str, str]) -> None:
        self._values = values
        self._units = units

    def visit(self, node: ast.AST) -> u.Quantity:
        match node:
            case ast.BinOp(left=left, op=op, right=right):
                return self._binop(left, op, right)
            case ast.UnaryOp(op=op, operand=operand):
                return self._unaryop(op, operand)
            case ast.Call() as call:
                return self._call(call)
            case ast.Name(id=name):
                return self._lookup_constant(name)
            case ast.Constant(value=value):
                return self._constant(value)
            case _:
                raise ValueError(f"unsupported expression node: {type(node).__name__}")

    def _binop(self, left: ast.AST, op: ast.operator, right: ast.AST) -> u.Quantity:
        op_type = type(op)
        if op_type not in _BINOPS:
            raise ValueError(f"unsupported operator: {op_type.__name__}")
        return _BINOPS[op_type](self.visit(left), self.visit(right))

    def _unaryop(self, op: ast.unaryop, operand: ast.AST) -> u.Quantity:
        op_type = type(op)
        if op_type not in _UNARYOPS:
            raise ValueError(f"unsupported unary operator: {op_type.__name__}")
        return _UNARYOPS[op_type](self.visit(operand))

    def _call(self, node: ast.Call) -> u.Quantity:
        column = _column_from_call(node)
        if column is not None:
            return self._lookup_column(column)
        if node.keywords:
            raise ValueError("keyword arguments are not allowed")
        if not isinstance(node.func, ast.Name):
            raise ValueError("only simple function calls are allowed")
        fn = _FUNCTIONS.get(node.func.id)
        if fn is None:
            raise ValueError(f"unknown function: {node.func.id}")
        if len(node.args) != 1:
            raise ValueError(f"{node.func.id}() takes exactly one argument")
        arg = self.visit(node.args[0]).to(u.rad)
        result = fn(arg)
        if isinstance(result, u.Quantity):
            return result
        return float(result) * u.dimensionless_unscaled

    def _lookup_constant(self, name: str) -> u.Quantity:
        constant = NAMED_CONSTANTS.get(name)
        if constant is None:
            raise ValueError(f"unknown constant {name!r}")
        return constant

    def _lookup_column(self, name: str) -> u.Quantity:
        if name not in self._values:
            raise ValueError(f"unknown column {name!r}")
        unit_str = self._units.get(name, "")
        unit = u.Unit(unit_str) if unit_str else u.dimensionless_unscaled
        return self._values[name] * unit

    def _constant(self, value: object) -> u.Quantity:
        if isinstance(value, bool):
            raise ValueError("boolean constants are not allowed")
        if isinstance(value, int | float):
            return float(value) * u.dimensionless_unscaled
        raise ValueError(f"unsupported constant type: {type(value).__name__}")
