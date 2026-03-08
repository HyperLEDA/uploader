from app.crossmatch.layered.models import (
    PreliminaryCrossmatchStatus,
    PreliminaryCrossmatchStatusExisting,
    PreliminaryCrossmatchStatusNew,
)
from app.crossmatch.models import PendingReason, RecordEvidence


def object_type_resolver(
    evidence: RecordEvidence, previous_result: PreliminaryCrossmatchStatus
) -> tuple[PreliminaryCrossmatchStatus, PendingReason | None]:
    if evidence.record_type_name is None:
        return previous_result, None

    if isinstance(previous_result, PreliminaryCrossmatchStatusNew):
        return previous_result, None

    record_type = evidence.record_type_name

    if isinstance(previous_result, PreliminaryCrossmatchStatusExisting):
        neighbor = next((n for n in evidence.neighbors if n.pgc == previous_result.pgc), None)
        existing_type = neighbor.type_name if neighbor is not None else None

        if existing_type is not None and record_type != existing_type:
            return previous_result, PendingReason.TYPE_MISMATCH

        return previous_result, None

    same_type_neighbors = [
        n
        for n in evidence.neighbors
        if n.pgc in previous_result.pgcs and record_type is not None and n.type_name == record_type
    ]
    if len(same_type_neighbors) == 1:
        return PreliminaryCrossmatchStatusExisting(same_type_neighbors[0].pgc), None

    return previous_result, None
