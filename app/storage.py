from collections.abc import Sequence
from typing import Any, LiteralString, cast

from psycopg import sql
from psycopg.connection import Connection
from psycopg.rows import dict_row

from app import log


class PgStorage:
    def __init__(self, conn: Connection) -> None:
        self._conn = conn

    def query(
        self,
        query: str | sql.Composed | sql.SQL,
        params: Sequence[Any] | None = None,
    ) -> list[dict[str, Any]]:
        query_str = query if isinstance(query, str) else query.as_string(self._conn)
        log.logger.info("Started query", query=query_str)
        if isinstance(query, str):
            query_exec: sql.SQL | sql.Composed = sql.SQL(cast(LiteralString, query))
        else:
            query_exec = query
        with self._conn.cursor(row_factory=dict_row) as cur:
            cur.execute(query_exec, params)
            rows = list(cur.fetchall())
        log.logger.info("Finished query", rows=len(rows))
        return rows
