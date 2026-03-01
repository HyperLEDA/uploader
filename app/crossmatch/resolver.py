"""Crossmatch decision tree.

Overview:

  - "Neighbors" = layer2 objects within the search radius (after angular-distance
    post-filter). "Preferred" = neighbor matches by PGC (record_pgc == neighbor.pgc)
    or by designation (record_designation matches neighbor.design).

  - ONE NEIGHBOR
    - PGC mismatch (record has claimed PGC and it differs from neighbor)
      → EXISTING, PENDING (PGC_MISMATCH).
    - Preferred (PGC or name match) or no PGC column
      → EXISTING, RESOLVED.
    - Else (one neighbor, no identity match, PGC column used)
      → EXISTING, PENDING (SINGLE_NEIGHBOR_NO_IDENTITY_MATCH).

  - MULTIPLE NEIGHBORS
    - Exactly one preferred neighbor
      - PGC match or no PGC column → EXISTING, RESOLVED.
      - PGC mismatch → EXISTING, PENDING (PGC_MISMATCH).
    - Zero or more-than-one preferred
      → COLLIDING, PENDING (MULTIPLE_OBJECTS_MATCHED).

  - ZERO NEIGHBORS
    - Exactly one PGC elsewhere (by name and/or claimed PGC in layer2)
      → EXISTING, PENDING (MATCHED_NAME_OUTSIDE_CIRCLE or
        MATCHED_PGC_OUTSIDE_CIRCLE).
    - Else
      → NEW, RESOLVED.
"""

from typing import Protocol

from app.crossmatch.models import (
    CrossmatchResult,
    CrossmatchStatus,
    Neighbor,
    PendingReason,
    RecordEvidence,
    TriageStatus,
)


def _designations_match(a: str | None, b: str | None) -> bool:
    if a is None or b is None:
        return False
    return a.strip().upper() == b.strip().upper()


def _preferred_neighbor(
    evidence: RecordEvidence,
    neighbor: Neighbor,
) -> bool:
    if evidence.record_pgc is not None and neighbor.pgc == evidence.record_pgc:
        return True
    return _designations_match(evidence.record_designation, neighbor.design)


def resolve(evidence: RecordEvidence) -> CrossmatchResult:
    neighbors = evidence.neighbors
    record_pgc = evidence.record_pgc
    claimed_pgc_exists = evidence.claimed_pgc_exists_in_layer2
    global_pgcs = evidence.global_pgcs_with_same_design or frozenset()

    if len(neighbors) == 1:
        n = neighbors[0]

        if record_pgc is not None and n.pgc != record_pgc:
            return CrossmatchResult(
                record_id=evidence.record_id,
                status=CrossmatchStatus.EXISTING,
                triage_status=TriageStatus.PENDING,
                matched_pgc=n.pgc,
                pending_reason=PendingReason.PGC_MISMATCH,
            )

        if _preferred_neighbor(evidence, n) or record_pgc is None:
            return CrossmatchResult(
                record_id=evidence.record_id,
                status=CrossmatchStatus.EXISTING,
                triage_status=TriageStatus.RESOLVED,
                matched_pgc=n.pgc,
            )

        return CrossmatchResult(
            record_id=evidence.record_id,
            status=CrossmatchStatus.EXISTING,
            triage_status=TriageStatus.PENDING,
            matched_pgc=n.pgc,
            pending_reason=PendingReason.SINGLE_NEIGHBOR_NO_IDENTITY_MATCH,
        )

    if len(neighbors) > 1:
        preferred = [n for n in neighbors if _preferred_neighbor(evidence, n)]

        if len(preferred) == 1:
            p = preferred[0]
            triage = TriageStatus.RESOLVED if record_pgc is None or p.pgc == record_pgc else TriageStatus.PENDING
            return CrossmatchResult(
                record_id=evidence.record_id,
                status=CrossmatchStatus.EXISTING,
                triage_status=triage,
                matched_pgc=p.pgc,
                pending_reason=PendingReason.PGC_MISMATCH if triage == TriageStatus.PENDING else None,
            )

        return CrossmatchResult(
            record_id=evidence.record_id,
            status=CrossmatchStatus.COLLIDING,
            triage_status=TriageStatus.PENDING,
            matched_pgc=None,
            colliding_pgcs=[n.pgc for n in neighbors],
            pending_reason=PendingReason.MULTIPLE_OBJECTS_MATCHED,
        )

    pgcs_elsewhere = set(global_pgcs)

    if record_pgc is not None and claimed_pgc_exists:
        pgcs_elsewhere.add(record_pgc)

    if len(pgcs_elsewhere) == 1:
        matched_pgc = next(iter(pgcs_elsewhere))
        reason = (
            PendingReason.MATCHED_NAME_OUTSIDE_CIRCLE
            if matched_pgc in global_pgcs
            else PendingReason.MATCHED_PGC_OUTSIDE_CIRCLE
        )
        return CrossmatchResult(
            record_id=evidence.record_id,
            status=CrossmatchStatus.EXISTING,
            triage_status=TriageStatus.PENDING,
            matched_pgc=matched_pgc,
            pending_reason=reason,
        )

    return CrossmatchResult(
        record_id=evidence.record_id,
        status=CrossmatchStatus.NEW,
        triage_status=TriageStatus.RESOLVED,
        matched_pgc=None,
    )


def _resolve_by_radius_coordinate(
    evidence: RecordEvidence,
    r1_deg: float,
    r2_deg: float,
) -> CrossmatchResult:
    """Coordinate-only two-radii check. No redshift. Same rules as doc below."""
    inner = [n for n in evidence.neighbors if n.distance_deg <= r1_deg]
    outer = [n for n in evidence.neighbors if r1_deg < n.distance_deg <= r2_deg]

    if len(inner) > 1:
        return CrossmatchResult(
            record_id=evidence.record_id,
            status=CrossmatchStatus.COLLIDING,
            triage_status=TriageStatus.PENDING,
            matched_pgc=None,
            colliding_pgcs=[n.pgc for n in inner],
            pending_reason=PendingReason.MULTIPLE_IN_INNER_RADIUS,
        )

    if len(inner) == 1 and len(outer) >= 1:
        return CrossmatchResult(
            record_id=evidence.record_id,
            status=CrossmatchStatus.COLLIDING,
            triage_status=TriageStatus.PENDING,
            matched_pgc=inner[0].pgc,
            pending_reason=PendingReason.SINGLE_IN_INNER_WITH_OUTER_NEIGHBORS,
        )

    if len(inner) == 1 and len(outer) == 0:
        return CrossmatchResult(
            record_id=evidence.record_id,
            status=CrossmatchStatus.EXISTING,
            triage_status=TriageStatus.RESOLVED,
            matched_pgc=inner[0].pgc,
        )

    if len(inner) == 0 and len(outer) == 1:
        return CrossmatchResult(
            record_id=evidence.record_id,
            status=CrossmatchStatus.EXISTING,
            triage_status=TriageStatus.PENDING,
            matched_pgc=outer[0].pgc,
            pending_reason=PendingReason.SINGLE_IN_OUTER_RADIUS_ONLY,
        )

    if len(inner) == 0 and len(outer) > 1:
        return CrossmatchResult(
            record_id=evidence.record_id,
            status=CrossmatchStatus.COLLIDING,
            triage_status=TriageStatus.PENDING,
            matched_pgc=None,
            colliding_pgcs=[n.pgc for n in outer],
            pending_reason=PendingReason.MULTIPLE_IN_OUTER_RADIUS,
        )

    return CrossmatchResult(
        record_id=evidence.record_id,
        status=CrossmatchStatus.NEW,
        triage_status=TriageStatus.RESOLVED,
        matched_pgc=None,
    )


def _redshift_close(record_z: float, neighbor_z: float | None, tolerance: float) -> bool:
    if neighbor_z is None:
        return False
    return abs(neighbor_z - record_z) < tolerance


def _apply_redshift_check(
    coord_result: CrossmatchResult,
    evidence: RecordEvidence,
    redshift_tolerance: float,
) -> CrossmatchResult:
    """Refine coordinate result using redshift when record and involved neighbors have redshift."""
    record_id = coord_result.record_id
    record_z = evidence.record_redshift

    if record_z is None:
        return coord_result

    if coord_result.status == CrossmatchStatus.NEW:
        return coord_result

    if coord_result.status == CrossmatchStatus.EXISTING:
        matched_pgc = coord_result.matched_pgc
        if matched_pgc is None:
            return coord_result

        neighbor = next((n for n in evidence.neighbors if n.pgc == matched_pgc), None)
        if neighbor is None or neighbor.redshift is None:
            return coord_result

        if _redshift_close(record_z, neighbor.redshift, redshift_tolerance):
            return CrossmatchResult(
                record_id=record_id,
                status=CrossmatchStatus.EXISTING,
                triage_status=TriageStatus.RESOLVED,
                matched_pgc=matched_pgc,
            )

        return CrossmatchResult(
            record_id=record_id,
            status=CrossmatchStatus.EXISTING,
            triage_status=TriageStatus.PENDING,
            matched_pgc=matched_pgc,
            pending_reason=PendingReason.REDSHIFT_MISMATCH,
        )

    if coord_result.status == CrossmatchStatus.COLLIDING:
        involved_pgcs = (
            coord_result.colliding_pgcs if coord_result.colliding_pgcs else [n.pgc for n in evidence.neighbors]
        )
        neighbors_involved = [n for n in evidence.neighbors if n.pgc in involved_pgcs]
        if any(n.redshift is None for n in neighbors_involved):
            return coord_result
        close = [n for n in neighbors_involved if _redshift_close(record_z, n.redshift, redshift_tolerance)]
        if len(close) == 1:
            return CrossmatchResult(
                record_id=record_id,
                status=CrossmatchStatus.EXISTING,
                triage_status=TriageStatus.RESOLVED,
                matched_pgc=close[0].pgc,
            )
        return coord_result

    return coord_result


def resolve_by_radius(
    evidence: RecordEvidence,
    r1_deg: float,
    r2_deg: float,
    redshift_tolerance: float,
) -> CrossmatchResult:
    coord_result = _resolve_by_radius_coordinate(evidence, r1_deg, r2_deg)
    return _apply_redshift_check(coord_result, evidence, redshift_tolerance)


class Resolver(Protocol):
    @property
    def search_radius_deg(self) -> float: ...

    @property
    def pgc_column(self) -> str | None: ...

    def resolve(self, evidence: RecordEvidence) -> CrossmatchResult: ...


class DefaultResolver:
    def __init__(self, radius_deg: float, pgc_column: str | None = None) -> None:
        self._radius_deg = radius_deg
        self._pgc_column = pgc_column

    @property
    def search_radius_deg(self) -> float:
        return self._radius_deg

    @property
    def pgc_column(self) -> str | None:
        return self._pgc_column

    def resolve(self, evidence: RecordEvidence) -> CrossmatchResult:
        return resolve(evidence)


class TwoRadiiResolver:
    def __init__(
        self,
        r1_deg: float,
        r2_deg: float,
        redshift_tolerance: float = 0.0003,
    ) -> None:
        self._r1_deg = r1_deg
        self._r2_deg = r2_deg
        self._redshift_tolerance = redshift_tolerance

    @property
    def search_radius_deg(self) -> float:
        return self._r2_deg

    @property
    def pgc_column(self) -> str | None:
        return None

    def resolve(self, evidence: RecordEvidence) -> CrossmatchResult:
        return resolve_by_radius(
            evidence,
            self._r1_deg,
            self._r2_deg,
            self._redshift_tolerance,
        )
