from collections.abc import Iterator, Sequence
from typing import Any

from psycopg import sql

from app import log
from app.storage import PgStorage


def rawdata_batches(
    storage: PgStorage,
    table_name: str,
    columns: Sequence[str],
    batch_size: int,
) -> Iterator[list[dict[str, Any]]]:
    id_col = sql.Identifier("hyperleda_internal_id")
    select_cols: list[sql.Identifier] = [id_col]
    for col in columns:
        select_cols.append(sql.Identifier(col))
    table = sql.SQL("rawdata.") + sql.Identifier(table_name)
    select_list = sql.SQL(", ").join(select_cols)
    query = sql.SQL(
        "SELECT {cols} FROM {t} WHERE {id_col} > %s ORDER BY {id_col} ASC LIMIT %s"
    ).format(cols=select_list, t=table, id_col=id_col)

    last_id = ""
    total = 0
    while True:
        rows = storage.query(query, (last_id, batch_size))
        if not rows:
            break
        total += len(rows)
        log.logger.debug(
            "processed batch",
            rows=len(rows),
            last_id=rows[-1]["hyperleda_internal_id"],
            total=total,
        )
        yield rows
        last_id = rows[-1]["hyperleda_internal_id"]
