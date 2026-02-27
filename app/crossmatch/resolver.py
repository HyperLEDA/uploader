from app.crossmatch.models import (
    CrossmatchResult,
    CrossmatchStatus,
    RecordEvidence,
    TriageStatus,
)


def resolve(evidence: RecordEvidence) -> CrossmatchResult:
    n = len(evidence.neighbors)
    if n == 0:
        return CrossmatchResult(
            record_id=evidence.record_id,
            status=CrossmatchStatus.NEW,
            triage_status=TriageStatus.RESOLVED,
            matched_pgc=None,
        )
    if n == 1:
        return CrossmatchResult(
            record_id=evidence.record_id,
            status=CrossmatchStatus.EXISTING,
            triage_status=TriageStatus.RESOLVED,
            matched_pgc=evidence.neighbors[0].pgc,
        )
    return CrossmatchResult(
        record_id=evidence.record_id,
        status=CrossmatchStatus.COLLIDING,
        triage_status=TriageStatus.PENDING,
        matched_pgc=None,
    )
