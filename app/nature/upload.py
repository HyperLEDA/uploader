from collections import Counter

from psycopg import sql

from app import log
from app.display import print_table
from app.gen.client import adminapi
from app.gen.client.adminapi.api.default import save_structured_data
from app.gen.client.adminapi.models.save_structured_data_request import (
    SaveStructuredDataRequest,
)
from app.storage import PgStorage
from app.upload import handle_call

NATURE_COLUMNS = ["type_name"]


def upload_nature(
    storage: PgStorage,
    table_name: str,
    column_name: str | None,
    type_mapping: dict[str, str],
    default_type: str | None,
    batch_size: int,
    client: adminapi.AuthenticatedClient,
    *,
    write: bool = False,
) -> None:
    id_col = sql.Identifier("hyperleda_internal_id")
    table = sql.SQL("rawdata.") + sql.Identifier(table_name)

    constant_type = column_name is None
    if constant_type:
        query = sql.SQL("SELECT {id_col} FROM {t} WHERE {id_col} > %s ORDER BY {id_col} ASC LIMIT %s").format(
            id_col=id_col, t=table
        )
    else:
        col = sql.Identifier(column_name)
        query = sql.SQL("SELECT {id_col}, {col} FROM {t} WHERE {id_col} > %s ORDER BY {id_col} ASC LIMIT %s").format(
            id_col=id_col, col=col, t=table
        )

    total_uploaded = 0
    type_counts: Counter[str] = Counter()

    last_id = ""
    while True:
        rows = storage.query(query, (last_id, batch_size))
        if not rows:
            break

        batch_ids: list[str] = []
        batch_data: list[list[str]] = []

        for row in rows:
            last_id = row["hyperleda_internal_id"]
            leda_type: str | None = default_type
            if not constant_type:
                raw_val = row[column_name]
                raw_key = str(raw_val).strip() if raw_val is not None else ""
                leda_type = type_mapping.get(raw_key, default_type if default_type is not None else raw_key)

            assert leda_type is not None
            batch_ids.append(last_id)
            batch_data.append([leda_type])
            type_counts[leda_type] += 1
            total_uploaded += 1

        if write and batch_ids:
            handle_call(
                save_structured_data.sync_detailed(
                    client=client,
                    body=SaveStructuredDataRequest(
                        catalog="nature",
                        columns=NATURE_COLUMNS,
                        ids=batch_ids,
                        data=batch_data,
                    ),
                )
            )

        log.logger.debug(
            "processed batch",
            rows=len(rows),
            last_id=last_id,
            total=total_uploaded,
        )

    table_rows: list[tuple[str, int, str]] = [
        (
            leda_type,
            count,
            f"{100.0 * count / total_uploaded:.1f}%" if total_uploaded else "-",
        )
        for leda_type, count in sorted(type_counts.items())
    ]
    print_table(
        ("LEDA type", "Count", "%"),
        table_rows,
        title=f"Total rows: {total_uploaded}\n",
        percent_last_column=True,
    )
