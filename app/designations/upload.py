import click
from psycopg import connect, sql

from app import log
from app.designations.rules import RULES
from app.display import print_table
from app.gen.client import adminapi
from app.gen.client.adminapi.api.default import save_structured_data
from app.gen.client.adminapi.models.save_structured_data_request import (
    SaveStructuredDataRequest,
)
from app.upload import handle_call


def upload_designations(
    dsn: str,
    table_name: str,
    column_name: str,
    batch_size: int,
    client: adminapi.AuthenticatedClient,
    *,
    write: bool = False,
    print_unmatched: bool = False,
) -> None:
    table_parts = table_name.split(".", 1)
    quoted_table = (
        sql.SQL(".").join(sql.Identifier(p) for p in table_parts)
        if len(table_parts) > 1
        else sql.Identifier(table_name)
    )
    id_col = sql.Identifier("hyperleda_internal_id")
    name_col = sql.Identifier(column_name)

    query = sql.SQL(
        "SELECT {id_col}, {name_col} FROM rawdata.{t} WHERE {id_col} > %s ORDER BY {id_col} ASC LIMIT %s"
    ).format(
        id_col=id_col,
        name_col=name_col,
        t=quoted_table,
    )

    rule_counts: dict[str, int] = {r.name: 0 for r in RULES}
    unmatched = 0

    with connect(dsn) as conn:
        last_id = ""
        while True:
            with conn.cursor() as cur:
                cur.execute(query, (last_id, batch_size))
                rows = cur.fetchall()
            if not rows:
                break

            batch_ids: list[str] = []
            batch_names: list[list[str]] = []

            for row in rows:
                internal_id, name_val = row
                last_id = internal_id
                if name_val is None or (isinstance(name_val, str) and not name_val.strip()):
                    unmatched += 1
                    continue
                name_str = str(name_val).strip()
                transformed: str | None = None
                for rule in RULES:
                    transformed = rule.match(name_str)
                    if transformed is not None:
                        rule_counts[rule.name] += 1
                        break
                if transformed is None:
                    unmatched += 1
                    transformed = name_str
                    if print_unmatched:
                        click.echo(name_str)
                batch_ids.append(internal_id)
                batch_names.append([transformed])

            if write and batch_ids:
                handle_call(
                    save_structured_data.sync_detailed(
                        client=client,
                        body=SaveStructuredDataRequest(
                            catalog="designation",
                            columns=["design"],
                            ids=batch_ids,
                            data=batch_names,
                        ),
                    )
                )

            batch_size_actual = len(rows)
            total_matched_so_far = sum(rule_counts.values())
            total_so_far = total_matched_so_far + unmatched

            def total_pct(n: int, total: int = total_so_far) -> float:
                return (100.0 * n / total) if total else 0.0

            log.logger.debug(
                "processed batch",
                rows=batch_size_actual,
                last_id=last_id,
                total=total_so_far,
                matched=total_matched_so_far,
                matched_pct=round(total_pct(total_matched_so_far), 1),
                unmatched=unmatched,
                unmatched_pct=round(total_pct(unmatched), 1),
            )

    total = sum(rule_counts.values()) + unmatched

    def pct(n: int) -> float:
        return (100.0 * n / total) if total else 0.0

    table_rows = [
        (name, rule_counts[name], pct(rule_counts[name]))
        for name in sorted(rule_counts.keys(), key=lambda n: (-rule_counts[n], n))
        if rule_counts[name] > 0
    ]
    table_rows.append(("(no rule matched)", unmatched, pct(unmatched)))
    print_table(
        ("Rule", "Count", "%"),
        table_rows,
        title=f"Total names: {total}\n",
    )
