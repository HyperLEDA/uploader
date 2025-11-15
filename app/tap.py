from dataclasses import dataclass

from astropy import table
from pyvo import registry

import app

TAP_ENDPOINT = "https://tapvizier.cds.unistra.fr/TAPVizieR/tap/sync"


@dataclass
class Constraint:
    column: str
    operator: str
    value: str


class TAPRepository:
    def __init__(self, endpoint: str = TAP_ENDPOINT):
        self.tap_endpoint = endpoint

    def _quote_column(self, column: str) -> str:
        if any(char in column for char in "()[]."):
            return f'"{column}"'
        return column

    def _build_where_clause(self, constraints: list[Constraint]) -> str:
        if not constraints:
            return ""

        conditions = []
        for constraint in constraints:
            quoted_column = self._quote_column(constraint.column)
            conditions.append(f"{quoted_column} {constraint.operator} {constraint.value}")

        return " WHERE " + " AND ".join(conditions)

    def _build_order_by_clause(self, order_by: str | None) -> str:
        if not order_by:
            return ""
        return f" ORDER BY {order_by}"

    def query(
        self,
        table_name: str,
        constraints: list[Constraint] | None = None,
        order_by: str | None = None,
    ) -> table.Table:
        where_clause = self._build_where_clause(constraints) if constraints else ""
        order_by_clause = self._build_order_by_clause(order_by)

        query = f'SELECT *\nFROM "{table_name}"{where_clause}{order_by_clause}'

        app.logger.info("Running TAP query", query=query)
        data = registry.regtap.RegistryQuery(self.tap_endpoint, query)
        result = data.execute()
        return result.to_table()
