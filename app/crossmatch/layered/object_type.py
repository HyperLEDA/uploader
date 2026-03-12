from app.crossmatch.layered.models import (
    PreliminaryCrossmatchStatus,
    PreliminaryCrossmatchStatusExisting,
    PreliminaryCrossmatchStatusNew,
)
from app.crossmatch.models import PendingReason, RecordEvidence

SIMILAR_TYPE_MAP = {
    "G": {"ext", "?", "QSO"},
    "ext": {"G", "?", "QSO"},
}


def _types_match(record_type: str, other_type: str) -> bool:
    return record_type == other_type or other_type in SIMILAR_TYPE_MAP.get(record_type, set())


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

        if existing_type is not None and not _types_match(record_type, existing_type):
            return previous_result, PendingReason.TYPE_MISMATCH

        return previous_result, None

    same_type_neighbors = [
        n
        for n in evidence.neighbors
        if n.pgc in previous_result.pgcs
        and n.type_name is not None
        and _types_match(record_type, n.type_name)
    ]
    if len(same_type_neighbors) == 1:
        return PreliminaryCrossmatchStatusExisting(same_type_neighbors[0].pgc), None

    return previous_result, None
