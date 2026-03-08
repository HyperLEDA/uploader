from app.crossmatch.layered.models import (
    PreliminaryCrossmatchStatus,
    PreliminaryCrossmatchStatusColliding,
    PreliminaryCrossmatchStatusExisting,
    PreliminaryCrossmatchStatusNew,
)
from app.crossmatch.models import PendingReason, RecordEvidence


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
