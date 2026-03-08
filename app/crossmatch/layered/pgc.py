from app.crossmatch.layered.models import (
    PreliminaryCrossmatchStatus,
    PreliminaryCrossmatchStatusColliding,
    PreliminaryCrossmatchStatusExisting,
    PreliminaryCrossmatchStatusNew,
)
from app.crossmatch.models import PendingReason, RecordEvidence


def pgc_resolver(
    evidence: RecordEvidence, previous_result: PreliminaryCrossmatchStatus
) -> tuple[PreliminaryCrossmatchStatus, PendingReason | None]:
    if evidence.record_pgc is None:
        return previous_result, None

    if evidence.record_pgc is not None and not evidence.claimed_pgc_exists_in_layer2:
        return previous_result, PendingReason.UNKNOWN_PGC

    pgc = evidence.record_pgc

    if isinstance(previous_result, PreliminaryCrossmatchStatusNew):
        return PreliminaryCrossmatchStatusExisting(pgc), PendingReason.MATCHED_PGC_OUTSIDE_CIRCLE

    if isinstance(previous_result, PreliminaryCrossmatchStatusExisting):
        if pgc == previous_result.pgc:
            return previous_result, None

        return PreliminaryCrossmatchStatusColliding(
            {pgc, previous_result.pgc}
        ), PendingReason.MATCHED_PGC_OUTSIDE_CIRCLE

    if pgc in previous_result.pgcs:
        return PreliminaryCrossmatchStatusExisting(pgc), None

    return PreliminaryCrossmatchStatusColliding(previous_result.pgcs | {pgc}), PendingReason.MATCHED_PGC_OUTSIDE_CIRCLE
