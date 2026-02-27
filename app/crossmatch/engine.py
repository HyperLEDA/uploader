import math
from collections import defaultdict

import click
from psycopg import connect, sql

from app import log
from app.crossmatch.models import (
    CrossmatchResult,
    CrossmatchStatus,
    Neighbor,
    RecordEvidence,
    TriageStatus,
)
from app.crossmatch.resolver import resolve


def angular_distance_deg(ra1: float, dec1: float, ra2: float, dec2: float) -> float:
    d_dec = dec1 - dec2
    d_ra = (ra1 - ra2) * math.cos(math.radians((dec1 + dec2) / 2))
    return math.sqrt(d_dec**2 + d_ra**2)


def run_crossmatch(
    dsn: str,
    table_name: str,
    radius_arcsec: float,
    batch_size: int,
    *,
    pgc_column: str | None = None,
    print_pending: bool = False,
) -> None:
    radius_deg = radius_arcsec / 3600.0

    batch_query = sql.SQL("""
        WITH batch AS (
            SELECT rec.id
            FROM layer0.records rec
            WHERE rec.table_id = %s AND rec.id > %s
            ORDER BY rec.id ASC
            LIMIT %s
        )
        SELECT
            b.id AS new_id,
            nc.ra AS new_ra,
            nc.dec AS new_dec,
            new_desig.design AS new_design,
            l2.pgc AS existing_pgc,
            l2.ra AS existing_ra,
            l2.dec AS existing_dec,
            l2_desig.design AS existing_design
        FROM batch b
        LEFT JOIN icrs.data nc ON b.id = nc.record_id
        LEFT JOIN designation.data new_desig ON b.id = new_desig.record_id
        LEFT JOIN layer2.icrs l2
            ON nc.record_id IS NOT NULL
            AND ST_DWithin(
                ST_MakePoint(nc.dec, nc.ra - 180),
                ST_MakePoint(l2.dec, l2.ra - 180),
                %s / GREATEST(COS(RADIANS(nc.dec)), 0.01)
            )
        LEFT JOIN layer2.designation l2_desig ON l2.pgc = l2_desig.pgc
        ORDER BY b.id ASC
    """)

    with connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id FROM layer0.tables WHERE table_name = %s",
                (table_name,),
            )
            row = cur.fetchone()
            if row is None:
                raise RuntimeError(f"Table not found: {table_name}")
            table_id = row[0]

        counts: dict[tuple[CrossmatchStatus, TriageStatus], int] = defaultdict(int)
        total = 0
        last_id = ""

        while True:
            with conn.cursor() as cur:
                cur.execute(
                    batch_query,
                    (table_id, last_id, batch_size, radius_deg),
                )
                rows = cur.fetchall()

            if not rows:
                break

            by_record: dict[str, dict] = {}
            for r in rows:
                (
                    new_id,
                    new_ra,
                    new_dec,
                    new_design,
                    existing_pgc,
                    existing_ra,
                    existing_dec,
                    existing_design,
                ) = r
                last_id = new_id
                if new_id not in by_record:
                    by_record[new_id] = {
                        "new_ra": None,
                        "new_dec": None,
                        "new_design": None,
                        "candidates": [],
                    }
                rec_data = by_record[new_id]
                if new_ra is not None:
                    rec_data["new_ra"] = new_ra
                    rec_data["new_dec"] = new_dec
                if new_design is not None:
                    rec_data["new_design"] = new_design
                if existing_pgc is not None and existing_ra is not None and existing_dec is not None:
                    rec_data["candidates"].append((existing_ra, existing_dec, existing_pgc, existing_design))

            record_pgc_by_id: dict[str, int | None] = {}
            if pgc_column is not None:
                raw_pgc_query = sql.SQL(
                    "SELECT hyperleda_internal_id, {col} FROM rawdata.{t} WHERE hyperleda_internal_id = ANY(%s)"
                ).format(
                    col=sql.Identifier(pgc_column),
                    t=sql.Identifier(table_name),
                )
                batch_ids = list(by_record.keys())
                with conn.cursor() as cur:
                    cur.execute(raw_pgc_query, (batch_ids,))
                    for record_id, pgc_val in cur.fetchall():
                        record_pgc_by_id[record_id] = int(pgc_val) if pgc_val is not None else None

            claimed_pgcs = {p for p in record_pgc_by_id.values() if p is not None}
            existing_pgcs: set[int] = set()
            if claimed_pgcs:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT pgc FROM layer2.icrs WHERE pgc = ANY(%s)",
                        (list(claimed_pgcs),),
                    )
                    existing_pgcs = {row[0] for row in cur.fetchall()}

            designations_in_batch = {
                rec_data["new_design"] for rec_data in by_record.values() if rec_data["new_design"] is not None
            }
            design_to_pgcs: dict[str, frozenset[int]] = {}
            if designations_in_batch:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT design, pgc FROM layer2.designation WHERE design = ANY(%s)",
                        (list(designations_in_batch),),
                    )
                    pgcs_by_design: dict[str, set[int]] = {}
                    for design, pgc in cur.fetchall():
                        pgcs_by_design.setdefault(design, set()).add(pgc)
                    design_to_pgcs = {d: frozenset(s) for d, s in pgcs_by_design.items()}
                for design in designations_in_batch:
                    if design not in design_to_pgcs:
                        design_to_pgcs[design] = frozenset()

            for record_id, rec_data in by_record.items():
                new_ra = rec_data["new_ra"]
                new_dec = rec_data["new_dec"]
                record_designation = rec_data["new_design"]
                candidates = rec_data["candidates"]
                global_pgcs = (
                    design_to_pgcs.get(record_designation, frozenset())
                    if record_designation is not None
                    else frozenset()
                )
                neighbors: list[Neighbor] = []
                if new_ra is not None and new_dec is not None:
                    for existing_ra, existing_dec, pgc, existing_design in candidates:
                        dist = angular_distance_deg(new_ra, new_dec, existing_ra, existing_dec)
                        if dist <= radius_deg:
                            neighbors.append(
                                Neighbor(
                                    pgc=pgc,
                                    ra=existing_ra,
                                    dec=existing_dec,
                                    distance_deg=dist,
                                    design=existing_design,
                                ),
                            )
                record_pgc = record_pgc_by_id.get(record_id) if record_pgc_by_id else None
                claimed_pgc_exists = record_pgc is not None and record_pgc in existing_pgcs
                evidence = RecordEvidence(
                    record_id=record_id,
                    neighbors=neighbors,
                    record_designation=record_designation,
                    global_pgcs_with_same_design=global_pgcs or None,
                    record_pgc=record_pgc,
                    claimed_pgc_exists_in_layer2=claimed_pgc_exists,
                )
                result: CrossmatchResult = resolve(evidence)
                counts[(result.status, result.triage_status)] += 1
                total += 1
                if print_pending and result.triage_status == TriageStatus.PENDING:
                    line = record_id
                    if result.colliding_pgcs:
                        line += " pgcs: " + ",".join(str(p) for p in sorted(result.colliding_pgcs))
                    elif result.matched_pgc is not None:
                        line += " pgc: " + str(result.matched_pgc)
                    click.echo(line)

            log.logger.debug(
                "processed batch",
                rows=len(rows),
                last_id=last_id,
                total=total,
            )

        def pct(n: int) -> float:
            return (100.0 * n / total) if total else 0.0

        click.echo(f"Total records: {total}\n")
        rows = [
            (status.value, triage.value, counts[(status, triage)], pct(counts[(status, triage)]))
            for status, triage in sorted(
                counts.keys(),
                key=lambda k: (-counts[k], k[0].value, k[1].value),
            )
            if counts[(status, triage)] > 0
        ]
        if not rows:
            click.echo("Status   Triage   Count      %")
            click.echo("-" * 26)
            return
        col_status = max(len(r[0]) for r in rows)
        col_triage = max(len(r[1]) for r in rows)
        col_count = max(len(str(r[2])) for r in rows)
        click.echo(f"{'Status':<{col_status}}  {'Triage':<{col_triage}}  {'Count':>{col_count}}  {'%':>6}")
        click.echo("-" * (col_status + col_triage + col_count + 14))
        for status, triage, n, p in rows:
            click.echo(f"{status:<{col_status}}  {triage:<{col_triage}}  {n:>{col_count}}  {p:>5.1f}%")
