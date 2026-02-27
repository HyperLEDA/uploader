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
