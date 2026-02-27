from app.crossmatch.models import (
    CrossmatchResult,
    CrossmatchStatus,
    RecordEvidence,
    TriageStatus,
)


def _designations_match(a: str | None, b: str | None) -> bool:
    if a is None or b is None:
        return False
    return a.strip().upper() == b.strip().upper()


def resolve(evidence: RecordEvidence) -> CrossmatchResult:
    neighbors = evidence.neighbors
    record_designation = evidence.record_designation
    global_pgcs = evidence.global_pgcs_with_same_design or frozenset()
    neighbor_pgcs = {n.pgc for n in neighbors}

    matching_by_name = [n for n in neighbors if _designations_match(record_designation, n.design)]
    if record_designation and len(matching_by_name) == 1:
        return CrossmatchResult(
            record_id=evidence.record_id,
            status=CrossmatchStatus.EXISTING,
            triage_status=TriageStatus.RESOLVED,
            matched_pgc=matching_by_name[0].pgc,
        )

    pgcs_outside_circle = global_pgcs - neighbor_pgcs
    if record_designation and len(pgcs_outside_circle) == 1:
        return CrossmatchResult(
            record_id=evidence.record_id,
            status=CrossmatchStatus.EXISTING,
            triage_status=TriageStatus.PENDING,
            matched_pgc=next(iter(pgcs_outside_circle)),
        )

    if len(neighbors) == 1:
        return CrossmatchResult(
            record_id=evidence.record_id,
            status=CrossmatchStatus.EXISTING,
            triage_status=TriageStatus.RESOLVED,
            matched_pgc=neighbors[0].pgc,
        )

    if len(neighbors) > 1:
        return CrossmatchResult(
            record_id=evidence.record_id,
            status=CrossmatchStatus.COLLIDING,
            triage_status=TriageStatus.PENDING,
            matched_pgc=None,
            colliding_pgcs=[n.pgc for n in neighbors],
        )

    return CrossmatchResult(
        record_id=evidence.record_id,
        status=CrossmatchStatus.NEW,
        triage_status=TriageStatus.RESOLVED,
        matched_pgc=None,
    )
