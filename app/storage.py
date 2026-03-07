from collections.abc import Sequence
from typing import Any

from psycopg import sql
from psycopg.connection import Connection
from psycopg.rows import dict_row


class PgStorage:
    def __init__(self, conn: Connection) -> None:
        self._conn = conn

    def query(
        self,
        query: str | sql.Composed | sql.SQL,
        params: Sequence[Any] | None = None,
    ) -> list[dict[str, Any]]:
        with self._conn.cursor(row_factory=dict_row) as cur:
            cur.execute(query, params)
            return list(cur.fetchall())
