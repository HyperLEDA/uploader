from collections.abc import Mapping
from typing import Any, Literal, TypedDict

from uploader.app.lib.formula.constants import NAMED_CONSTANTS
from uploader.app.lib.formula.functions import COL_FUNCTION, FUNCTIONS
from uploader.app.lib.formula.operators import OPERATORS
from uploader.app.lib.formula.values import Value


class ExpressionToken(TypedDict):
    label: str
    insert: str
    kind: Literal["function", "constant"]
    detail: str


def expression_tokens() -> list[ExpressionToken]:
    tokens: list[ExpressionToken] = []
    for fn in (COL_FUNCTION, *FUNCTIONS):
        tokens.append(
            {
                "label": fn.name,
                "insert": fn.insert,
                "kind": "function",
                "detail": fn.detail,
            },
        )
    for constant in NAMED_CONSTANTS:
        tokens.append(
            {
                "label": constant.name,
                "insert": constant.insert,
                "kind": "constant",
                "detail": constant.detail,
            },
        )
    return tokens


def expression_json_schema_extra() -> dict[str, Any]:
    return {
        "ui:options": {"tokens": expression_tokens()},
    }


def build_namespace(columns: Mapping[str, Value]) -> dict[str, object]:
    return {
        "__builtins__": {},
        COL_FUNCTION.name: lambda name: columns[name],
        **{constant.name: constant.value for constant in NAMED_CONSTANTS},
        **{fn.name: fn.impl for fn in FUNCTIONS if fn.impl is not None},
    }


def expression_syntax_help() -> str:
    constants = ", ".join(f"`{c.name}`" for c in NAMED_CONSTANTS)
    functions = ", ".join(f"`{fn.signature}`" for fn in (COL_FUNCTION, *FUNCTIONS))
    operators = ", ".join(f"`{op.name}`" for op in OPERATORS)
    return f"""\
## Expression syntax

Expressions are unit-aware and units are taken from column metadata.

Mathematical operations:
- Operators: {operators}
- Functions: {functions}
- Numbers are dimensionless
- String literals are supported
- Log columns (`mag`/`dex`) yield the bare exponent; multiply by the scale yourself

Available constants: {constants}.

## Examples

- Fill a column with a constant: 
    - `1.5`
    - `180 * deg`
    - `"G"` - fills the column with a text "G"
- Copy another column: `col("ra")`
- Mathematical expression: `3 * 10 ** col("logd25") * arcsec`
- Conditional: `where(col("v") > 0, col("v"), 0)`"""
