import click
from psycopg import sql

from app import log
from app.name_checker.rules import RULES


def run_checker(
    dsn: str,
    table_name: str,
    column_name: str,
    batch_size: int,
    *,
    print_unmatched: bool = False,
) -> None:
    from psycopg import connect

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
            for row in rows:
                internal_id, name_val = row
                last_id = internal_id
                if name_val is None or (isinstance(name_val, str) and not name_val.strip()):
                    unmatched += 1
                    continue
                name_str = str(name_val).strip()
                matched = False
                for rule in RULES:
                    if rule.match(name_str) is not None:
                        rule_counts[rule.name] += 1
                        matched = True
                        break
                if not matched:
                    unmatched += 1
                    if print_unmatched:
                        click.echo(name_str)
            batch_size_actual = len(rows)
            total_matched_so_far = sum(rule_counts.values())
            total_so_far = total_matched_so_far + unmatched

            def total_pct(n: int) -> float:
                return (100.0 * n / total_so_far) if total_so_far else 0.0

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

    click.echo(f"Total names: {total}")
    click.echo("Rule match counts (absolute / %):")
    for name in sorted(rule_counts.keys(), key=lambda n: (-rule_counts[n], n)):
        n = rule_counts[name]
        if n > 0:
            click.echo(f"  {name}: {n} ({pct(n):.1f}%)")
    click.echo(f"  (no rule matched): {unmatched} ({pct(unmatched):.1f}%)")
