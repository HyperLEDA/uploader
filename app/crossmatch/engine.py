import math
from collections import defaultdict
from typing import cast

import click
from psycopg import sql

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
from app.storage import PgStorage
from app.upload import handle_call

C_M_S = 299792458

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
        new_cz.cz AS new_cz,
        l2.pgc AS existing_pgc,
        l2.ra AS existing_ra,
        l2.dec AS existing_dec,
        l2_desig.design AS existing_design,
        l2_cz.cz AS existing_cz
    FROM batch b
    LEFT JOIN icrs.data nc ON b.id = nc.record_id
    LEFT JOIN designation.data new_desig ON b.id = new_desig.record_id
    LEFT JOIN cz.data new_cz ON b.id = new_cz.record_id
    LEFT JOIN layer2.icrs l2
        ON nc.record_id IS NOT NULL
        AND ST_DWithin(
            ST_MakePoint(nc.dec, nc.ra - 180),
            ST_MakePoint(l2.dec, l2.ra - 180),
            %s / GREATEST(COS(RADIANS(nc.dec)), 0.01)
        )
    LEFT JOIN layer2.designation l2_desig ON l2.pgc = l2_desig.pgc
    LEFT JOIN layer2.cz l2_cz ON l2.pgc = l2_cz.pgc
    ORDER BY b.id ASC
""")


def angular_distance_deg(ra1: float, dec1: float, ra2: float, dec2: float) -> float:
    d_dec = dec1 - dec2
    d_ra = (ra1 - ra2) * math.cos(math.radians((dec1 + dec2) / 2))
    return math.sqrt(d_dec**2 + d_ra**2)


def _fetch_batch(
    storage: PgStorage,
    table_id: str,
    last_id: str,
    batch_size: int,
    radius_deg: float,
) -> tuple[dict[str, dict], str]:
    rows = storage.query(BATCH_QUERY, (table_id, last_id, batch_size, radius_deg))

    if not rows:
        return {}, last_id

    by_record: dict[str, dict] = {}
    for r in rows:
        new_id = r["new_id"]
        new_ra = r["new_ra"]
        new_dec = r["new_dec"]
        new_design = r["new_design"]
        new_cz = r["new_cz"]
        existing_pgc = r["existing_pgc"]
        existing_ra = r["existing_ra"]
        existing_dec = r["existing_dec"]
        existing_design = r["existing_design"]
        existing_cz = r["existing_cz"]
        last_id = new_id
        if new_id not in by_record:
            by_record[new_id] = {
                "new_ra": None,
                "new_dec": None,
                "new_design": None,
                "new_redshift": None,
                "candidates": [],
            }
        rec_data = by_record[new_id]
        if new_ra is not None:
            rec_data["new_ra"] = new_ra
            rec_data["new_dec"] = new_dec
        if new_design is not None:
            rec_data["new_design"] = new_design
        if new_cz is not None:
            rec_data["new_redshift"] = float(new_cz) / C_M_S
        if existing_pgc is not None and existing_ra is not None and existing_dec is not None:
            existing_redshift = float(existing_cz) / C_M_S if existing_cz is not None else None
            rec_data["candidates"].append((existing_ra, existing_dec, existing_pgc, existing_design, existing_redshift))

    return by_record, last_id


def _enrich_batch(
    storage: PgStorage,
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
        for row in storage.query(raw_pgc_query, (batch_ids,)):
            record_id = row["hyperleda_internal_id"]
            pgc_val = row[pgc_column]
            record_pgc_by_id[record_id] = int(pgc_val) if pgc_val is not None else None

    claimed_pgcs = {p for p in record_pgc_by_id.values() if p is not None}
    existing_pgcs: set[int] = set()
    if claimed_pgcs:
        for row in storage.query(
            "SELECT pgc FROM layer2.icrs WHERE pgc = ANY(%s)",
            (list(claimed_pgcs),),
        ):
            existing_pgcs.add(row["pgc"])

    designations_in_batch = {
        rec_data["new_design"] for rec_data in by_record.values() if rec_data["new_design"] is not None
    }
    design_to_pgcs: dict[str, frozenset[int]] = {}
    if designations_in_batch:
        pgcs_by_design: dict[str, set[int]] = {}
        for row in storage.query(
            "SELECT design, pgc FROM layer2.designation WHERE design = ANY(%s)",
            (list(designations_in_batch),),
        ):
            pgcs_by_design.setdefault(row["design"], set()).add(row["pgc"])
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
) -> list[tuple[str, CrossmatchResult]]:
    results: list[tuple[str, CrossmatchResult]] = []
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
            for existing_ra, existing_dec, pgc, existing_design, existing_redshift in candidates:
                dist = angular_distance_deg(new_ra, new_dec, existing_ra, existing_dec)
                if dist <= radius_deg:
                    neighbors.append(
                        Neighbor(
                            pgc=pgc,
                            ra=existing_ra,
                            dec=existing_dec,
                            distance_deg=dist,
                            design=existing_design,
                            redshift=existing_redshift,
                        ),
                    )
        record_pgc = record_pgc_by_id.get(record_id) if record_pgc_by_id else None
        claimed_pgc_exists = record_pgc is not None and record_pgc in existing_pgcs
        record_redshift = rec_data.get("new_redshift")
        evidence = RecordEvidence(
            neighbors=neighbors,
            record_designation=record_designation,
            global_pgcs_with_same_design=global_pgcs or None,
            record_pgc=record_pgc,
            claimed_pgc_exists_in_layer2=claimed_pgc_exists,
            record_redshift=record_redshift,
        )
        result = resolver.resolve(evidence)
        results.append((record_id, result))
        if print_pending and result.triage_status == TriageStatus.PENDING:
            line = record_id + f"({result.status})"
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
    results: list[tuple[str, CrossmatchResult]],
) -> None:
    new_record_ids_list: list[str] = []
    new_triage_list: list[RecordTriageStatus] = []
    existing_record_ids_list: list[str] = []
    existing_pgc_list: list[int] = []
    existing_triage_list: list[RecordTriageStatus] = []
    collided_record_ids_list: list[str] = []
    collided_matches_list: list[list[int]] = []
    collided_triage_list: list[RecordTriageStatus] = []
    for record_id, r in results:
        triage = RecordTriageStatus(r.triage_status.value)
        if r.status == CrossmatchStatus.NEW:
            new_record_ids_list.append(record_id)
            new_triage_list.append(triage)
        elif r.status == CrossmatchStatus.EXISTING and r.matched_pgc is not None:
            existing_record_ids_list.append(record_id)
            existing_pgc_list.append(r.matched_pgc)
            existing_triage_list.append(triage)
        elif r.status == CrossmatchStatus.COLLIDING and r.colliding_pgcs is not None:
            collided_record_ids_list.append(record_id)
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
    storage: PgStorage,
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

    rows = storage.query(
        "SELECT id FROM layer0.tables WHERE table_name = %s",
        (table_name,),
    )
    if not rows:
        raise RuntimeError(f"Table not found: {table_name}")
    table_id = rows[0]["id"]

    counts: dict[tuple[CrossmatchStatus, TriageStatus, PendingReason | None], int] = defaultdict(int)
    total = 0
    last_id = ""

    try:
        while True:
            by_record, last_id = _fetch_batch(storage, table_id, last_id, batch_size, radius_deg)
            if not by_record:
                break

            record_pgc_by_id, existing_pgcs, design_to_pgcs = _enrich_batch(storage, table_name, by_record, pgc_column)
            batch_results = _resolve_batch(
                by_record,
                record_pgc_by_id,
                existing_pgcs,
                design_to_pgcs,
                resolver,
                print_pending,
            )

            for _record_id, result in batch_results:
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
    finally:

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
