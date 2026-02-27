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
            l2.pgc AS existing_pgc,
            l2.ra AS existing_ra,
            l2.dec AS existing_dec
        FROM batch b
        LEFT JOIN icrs.data nc ON b.id = nc.record_id
        LEFT JOIN layer2.icrs l2
            ON nc.record_id IS NOT NULL
            AND ST_DWithin(
                ST_MakePoint(nc.dec, nc.ra - 180),
                ST_MakePoint(l2.dec, l2.ra - 180),
                %s
            )
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
                (new_id, new_ra, new_dec, existing_pgc, existing_ra, existing_dec) = r
                last_id = new_id
                if new_id not in by_record:
                    by_record[new_id] = {"new_ra": None, "new_dec": None, "candidates": []}
                rec = by_record[new_id]
                if new_ra is not None:
                    rec["new_ra"] = new_ra
                    rec["new_dec"] = new_dec
                if existing_pgc is not None and existing_ra is not None and existing_dec is not None:
                    rec["candidates"].append((existing_ra, existing_dec, existing_pgc))

            for record_id, rec in by_record.items():
                new_ra = rec["new_ra"]
                new_dec = rec["new_dec"]
                candidates = rec["candidates"]
                neighbors: list[Neighbor] = []
                if new_ra is not None and new_dec is not None:
                    for existing_ra, existing_dec, pgc in candidates:
                        dist = angular_distance_deg(new_ra, new_dec, existing_ra, existing_dec)
                        if dist <= radius_deg:
                            neighbors.append(
                                Neighbor(
                                    pgc=pgc,
                                    ra=existing_ra,
                                    dec=existing_dec,
                                    distance_deg=dist,
                                ),
                            )
                evidence = RecordEvidence(record_id=record_id, neighbors=neighbors)
                result: CrossmatchResult = resolve(evidence)
                counts[(result.status, result.triage_status)] += 1
                total += 1

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
