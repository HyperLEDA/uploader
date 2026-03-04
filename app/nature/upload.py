from collections import Counter

from psycopg import connect, sql

from app import log
from app.display import print_table
from app.gen.client import adminapi
from app.gen.client.adminapi.api.default import save_structured_data
from app.gen.client.adminapi.models.save_structured_data_request import (
    SaveStructuredDataRequest,
)
from app.upload import handle_call

NATURE_COLUMNS = ["type_name"]


def upload_nature(
    dsn: str,
    table_name: str,
    column_name: str,
    class_mapping: dict[str, str],
    default_class: str,
    batch_size: int,
    client: adminapi.AuthenticatedClient,
    *,
    write: bool = False,
) -> None:
    id_col = sql.Identifier("hyperleda_internal_id")
    table = sql.SQL("rawdata.") + sql.Identifier(table_name)
    col = sql.Identifier(column_name)
    query = sql.SQL("SELECT {id_col}, {col} FROM {t} WHERE {id_col} > %s ORDER BY {id_col} ASC LIMIT %s").format(
        id_col=id_col, col=col, t=table
    )

    total_uploaded = 0
    class_counts: Counter[str] = Counter()

    with connect(dsn) as conn:
        last_id = ""
        while True:
            with conn.cursor() as cur:
                cur.execute(query, (last_id, batch_size))
                rows = cur.fetchall()
            if not rows:
                break

            batch_ids: list[str] = []
            batch_data: list[list[str]] = []

            for row in rows:
                last_id = row[0]
                raw_val = row[1]
                raw_key = (str(raw_val).strip() if raw_val is not None else "")
                leda_class = class_mapping.get(raw_key, default_class)
                batch_ids.append(last_id)
                batch_data.append([leda_class])
                class_counts[leda_class] += 1
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
            leda_class,
            count,
            f"{100.0 * count / total_uploaded:.1f}%" if total_uploaded else "-",
        )
        for leda_class, count in sorted(class_counts.items())
    ]
    print_table(
        ("LEDA class", "Count", "%"),
        table_rows,
        title=f"Total rows: {total_uploaded}\n",
        percent_last_column=True,
    )
