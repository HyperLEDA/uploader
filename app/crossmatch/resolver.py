from app.crossmatch.models import (
    CrossmatchResult,
    CrossmatchStatus,
    Neighbor,
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
            )

        return CrossmatchResult(
            record_id=evidence.record_id,
            status=CrossmatchStatus.COLLIDING,
            triage_status=TriageStatus.PENDING,
            matched_pgc=None,
            colliding_pgcs=[n.pgc for n in neighbors],
        )

    pgcs_elsewhere = set(global_pgcs)

    if record_pgc is not None and claimed_pgc_exists:
        pgcs_elsewhere.add(record_pgc)

    if len(pgcs_elsewhere) == 1:
        return CrossmatchResult(
            record_id=evidence.record_id,
            status=CrossmatchStatus.EXISTING,
            triage_status=TriageStatus.PENDING,
            matched_pgc=next(iter(pgcs_elsewhere)),
        )

    return CrossmatchResult(
        record_id=evidence.record_id,
        status=CrossmatchStatus.NEW,
        triage_status=TriageStatus.RESOLVED,
        matched_pgc=None,
    )
