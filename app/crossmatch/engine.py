import math
from collections import defaultdict
from typing import cast

import click
from psycopg import connect, sql

from app import log
from app.crossmatch.models import (
    CrossmatchResult,
    CrossmatchStatus,
    Neighbor,
    PendingReason,
    RecordEvidence,
    TriageStatus,
)
from app.crossmatch.resolver import Resolver
from app.display import print_table
from app.gen.client import adminapi
from app.gen.client.adminapi.api.default import set_crossmatch_results
from app.gen.client.adminapi.models.collided_status_payload import CollidedStatusPayload
from app.gen.client.adminapi.models.existing_status_payload import ExistingStatusPayload
from app.gen.client.adminapi.models.new_status_payload import NewStatusPayload
from app.gen.client.adminapi.models.record_triage_status import RecordTriageStatus
from app.gen.client.adminapi.models.set_crossmatch_results_request import (
    SetCrossmatchResultsRequest,
)
from app.gen.client.adminapi.models.statuses_payload import StatusesPayload
from app.gen.client.adminapi.types import UNSET, Unset
from app.upload import handle_call

BATCH_QUERY = sql.SQL("""
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


def angular_distance_deg(ra1: float, dec1: float, ra2: float, dec2: float) -> float:
    d_dec = dec1 - dec2
    d_ra = (ra1 - ra2) * math.cos(math.radians((dec1 + dec2) / 2))
    return math.sqrt(d_dec**2 + d_ra**2)


def _fetch_batch(
    conn,
    table_id: str,
    last_id: str,
    batch_size: int,
    radius_deg: float,
) -> tuple[dict[str, dict], str]:
    with conn.cursor() as cur:
        cur.execute(
            BATCH_QUERY,
            (table_id, last_id, batch_size, radius_deg),
        )
        rows = cur.fetchall()

    if not rows:
        return {}, last_id

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

    return by_record, last_id


def _enrich_batch(
    conn,
    table_name: str,
    by_record: dict[str, dict],
    pgc_column: str | None,
) -> tuple[dict[str, int | None], set[int], dict[str, frozenset[int]]]:
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

    return record_pgc_by_id, existing_pgcs, design_to_pgcs


def _resolve_batch(
    by_record: dict[str, dict],
    record_pgc_by_id: dict[str, int | None],
    existing_pgcs: set[int],
    design_to_pgcs: dict[str, frozenset[int]],
    resolver: Resolver,
    print_pending: bool,
) -> list[CrossmatchResult]:
    results: list[CrossmatchResult] = []
    radius_deg = resolver.search_radius_deg
    for record_id, rec_data in by_record.items():
        new_ra = rec_data["new_ra"]
        new_dec = rec_data["new_dec"]
        record_designation = rec_data["new_design"]
        candidates = rec_data["candidates"]
        global_pgcs = (
            design_to_pgcs.get(record_designation, frozenset()) if record_designation is not None else frozenset()
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
        result = resolver.resolve(evidence)
        results.append(result)
        if print_pending and result.triage_status == TriageStatus.PENDING:
            line = record_id
            if result.pending_reason is not None:
                line += " " + result.pending_reason.value
            if result.colliding_pgcs:
                line += " pgcs: " + ",".join(str(p) for p in sorted(result.colliding_pgcs))
            elif result.matched_pgc is not None:
                line += " pgc: " + str(result.matched_pgc)
            click.echo(line)
    return results


def _write_crossmatch_results(
    client: adminapi.AuthenticatedClient,
    results: list[CrossmatchResult],
) -> None:
    new_record_ids_list: list[str] = []
    new_triage_list: list[RecordTriageStatus] = []
    existing_record_ids_list: list[str] = []
    existing_pgc_list: list[int] = []
    existing_triage_list: list[RecordTriageStatus] = []
    collided_record_ids_list: list[str] = []
    collided_matches_list: list[list[int]] = []
    collided_triage_list: list[RecordTriageStatus] = []
    for r in results:
        triage = RecordTriageStatus(r.triage_status.value)
        if r.status == CrossmatchStatus.NEW:
            new_record_ids_list.append(r.record_id)
            new_triage_list.append(triage)
        elif r.status == CrossmatchStatus.EXISTING and r.matched_pgc is not None:
            existing_record_ids_list.append(r.record_id)
            existing_pgc_list.append(r.matched_pgc)
            existing_triage_list.append(triage)
        elif r.status == CrossmatchStatus.COLLIDING and r.colliding_pgcs is not None:
            collided_record_ids_list.append(r.record_id)
            collided_matches_list.append(sorted(r.colliding_pgcs))
            collided_triage_list.append(triage)
    new_pl = (
        NewStatusPayload(
            record_ids=new_record_ids_list,
            triage_statuses=cast("list[RecordTriageStatus | None] | Unset", new_triage_list),
        )
        if new_record_ids_list
        else None
    )
    existing_pl = (
        ExistingStatusPayload(
            record_ids=existing_record_ids_list,
            pgcs=existing_pgc_list,
            triage_statuses=cast("list[RecordTriageStatus | None] | Unset", existing_triage_list),
        )
        if existing_record_ids_list
        else None
    )
    collided_pl = (
        CollidedStatusPayload(
            record_ids=collided_record_ids_list,
            possible_matches=collided_matches_list,
            triage_statuses=cast("list[RecordTriageStatus | None] | Unset", collided_triage_list),
        )
        if collided_record_ids_list
        else None
    )
    if new_pl is not None or existing_pl is not None or collided_pl is not None:
        handle_call(
            set_crossmatch_results.sync_detailed(
                client=client,
                body=SetCrossmatchResultsRequest(
                    statuses=StatusesPayload(
                        new=new_pl if new_pl is not None else UNSET,
                        existing=existing_pl if existing_pl is not None else UNSET,
                        collided=collided_pl if collided_pl is not None else UNSET,
                    ),
                ),
            )
        )


def run_crossmatch(
    dsn: str,
    table_name: str,
    batch_size: int,
    client: adminapi.AuthenticatedClient,
    resolver: Resolver,
    *,
    print_pending: bool = False,
    write: bool = False,
) -> None:
    radius_deg = resolver.search_radius_deg
    pgc_column = resolver.pgc_column

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

        counts: dict[tuple[CrossmatchStatus, TriageStatus, PendingReason | None], int] = defaultdict(int)
        total = 0
        last_id = ""

        while True:
            by_record, last_id = _fetch_batch(conn, table_id, last_id, batch_size, radius_deg)
            if not by_record:
                break

            record_pgc_by_id, existing_pgcs, design_to_pgcs = _enrich_batch(conn, table_name, by_record, pgc_column)
            batch_results = _resolve_batch(
                by_record,
                record_pgc_by_id,
                existing_pgcs,
                design_to_pgcs,
                resolver,
                print_pending,
            )

            for result in batch_results:
                counts[(result.status, result.triage_status, result.pending_reason)] += 1
                total += 1

            if write and client and batch_results:
                _write_crossmatch_results(client, batch_results)

            log.logger.debug(
                "processed batch",
                rows=len(by_record),
                last_id=last_id,
                total=total,
            )

        def pct(n: int) -> float:
            return (100.0 * n / total) if total else 0.0

        summary_rows = [
            (
                status.value,
                triage.value,
                reason.value if reason is not None else "",
                counts[(status, triage, reason)],
                pct(counts[(status, triage, reason)]),
            )
            for status, triage, reason in sorted(
                counts.keys(),
                key=lambda k: (-counts[k], k[0].value, k[1].value, k[2].value if k[2] is not None else ""),
            )
            if counts[(status, triage, reason)] > 0
        ]
        print_table(
            ("Status", "Triage", "Reason", "Count", "%"),
            summary_rows,
            title=f"Total records: {total}\n",
        )
