from dataclasses import dataclass

from app.crossmatch.models import CrossmatchResult, CrossmatchStatus, PendingReason, RecordEvidence, TriageStatus


@dataclass
class PreliminaryCrossmatchStatusNew:
    pass


@dataclass
class PreliminaryCrossmatchStatusExisting:
    pgc: int


@dataclass
class PreliminaryCrossmatchStatusColliding:
    pgcs: set[int]


PreliminaryCrossmatchStatus = (
    PreliminaryCrossmatchStatusNew | PreliminaryCrossmatchStatusExisting | PreliminaryCrossmatchStatusColliding
)


def icrs_simple_resolver(evidence: RecordEvidence) -> tuple[PreliminaryCrossmatchStatus, PendingReason | None]:
    if len(evidence.neighbors) == 0:
        return PreliminaryCrossmatchStatusNew(), None

    if len(evidence.neighbors) == 1:
        return PreliminaryCrossmatchStatusExisting(evidence.neighbors[0].pgc), None

    return PreliminaryCrossmatchStatusColliding({n.pgc for n in evidence.neighbors}), None


def name_resolver(
    evidence: RecordEvidence, previous_result: PreliminaryCrossmatchStatus
) -> tuple[PreliminaryCrossmatchStatus, PendingReason | None]:
    name_pgcs = set(evidence.same_name_pgcs or [])

    if isinstance(previous_result, PreliminaryCrossmatchStatusNew):
        if len(name_pgcs) == 0:
            return previous_result, None

        if len(name_pgcs) == 1:
            objs = evidence.same_name_pgcs or []
            return PreliminaryCrossmatchStatusExisting(objs[0]), PendingReason.MATCHED_NAME_OUTSIDE_CIRCLE

        return PreliminaryCrossmatchStatusColliding(name_pgcs), PendingReason.MATCHED_NAME_OUTSIDE_CIRCLE

    if isinstance(previous_result, PreliminaryCrossmatchStatusExisting):
        record_pgc = previous_result.pgc

        if len(name_pgcs) == 0:
            return previous_result, None

        if len(name_pgcs) == 1:
            name_pgc = next(iter(name_pgcs))
            if name_pgc == record_pgc:
                return previous_result, None

            return PreliminaryCrossmatchStatusColliding(
                {name_pgc, record_pgc}
            ), PendingReason.MATCHED_NAME_OUTSIDE_CIRCLE

        return PreliminaryCrossmatchStatusColliding(name_pgcs | {record_pgc}), PendingReason.MATCHED_NAME_OUTSIDE_CIRCLE

    if len(name_pgcs) == 0:
        return previous_result, None

    if len(name_pgcs) == 1:
        name_pgc = next(iter(name_pgcs))

        for matched_pgc in previous_result.pgcs:
            if matched_pgc == name_pgc:
                return PreliminaryCrossmatchStatusExisting(name_pgc), None

        return PreliminaryCrossmatchStatusColliding(
            previous_result.pgcs | {name_pgc}
        ), PendingReason.MATCHED_NAME_OUTSIDE_CIRCLE

    common_pgcs = name_pgcs.intersection(previous_result.pgcs)
    if len(common_pgcs) == 1:
        return PreliminaryCrossmatchStatusExisting(next(iter(common_pgcs))), None

    return PreliminaryCrossmatchStatusColliding(
        previous_result.pgcs | name_pgcs
    ), PendingReason.MATCHED_NAME_OUTSIDE_CIRCLE


def _preliminary_to_final(results: PreliminaryCrossmatchStatus, pending_reason: PendingReason) -> CrossmatchResult:
    if isinstance(results, PreliminaryCrossmatchStatusNew):
        return CrossmatchResult(
            status=CrossmatchStatus.NEW, triage_status=TriageStatus.PENDING, pending_reason=pending_reason
        )
    if isinstance(results, PreliminaryCrossmatchStatusExisting):
        return CrossmatchResult(
            status=CrossmatchStatus.EXISTING,
            triage_status=TriageStatus.PENDING,
            matched_pgc=results.pgc,
            pending_reason=pending_reason,
        )
    return CrossmatchResult(
        status=CrossmatchStatus.COLLIDING,
        triage_status=TriageStatus.PENDING,
        colliding_pgcs=list(results.pgcs),
        pending_reason=pending_reason,
    )


def resolver(evidence: RecordEvidence) -> CrossmatchResult:
    icrs_result, pending_reason = icrs_simple_resolver(evidence)
    if pending_reason is not None:
        return _preliminary_to_final(icrs_result, pending_reason)

    name_result, pending_reason = name_resolver(evidence, icrs_result)
    if pending_reason is not None:
        return _preliminary_to_final(icrs_result, pending_reason)

    final_result = name_result

    if isinstance(final_result, PreliminaryCrossmatchStatusNew):
        return CrossmatchResult(status=CrossmatchStatus.NEW, triage_status=TriageStatus.RESOLVED)

    if isinstance(final_result, PreliminaryCrossmatchStatusExisting):
        return CrossmatchResult(
            status=CrossmatchStatus.EXISTING, triage_status=TriageStatus.RESOLVED, matched_pgc=final_result.pgc
        )

    return CrossmatchResult(
        status=CrossmatchStatus.COLLIDING, triage_status=TriageStatus.PENDING, colliding_pgcs=list(final_result.pgcs)
    )
