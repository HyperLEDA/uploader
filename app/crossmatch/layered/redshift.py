from app.crossmatch.layered.models import (
    PreliminaryCrossmatchStatus,
    PreliminaryCrossmatchStatusExisting,
    PreliminaryCrossmatchStatusNew,
)
from app.crossmatch.models import PendingReason, RecordEvidence


def redshift_resolver(
    evidence: RecordEvidence,
    previous_result: PreliminaryCrossmatchStatus,
    redshift_tolerance: float | None,
) -> tuple[PreliminaryCrossmatchStatus, PendingReason | None]:
    if redshift_tolerance is None or evidence.record_redshift is None:
        return previous_result, None

    record_z = evidence.record_redshift

    if isinstance(previous_result, PreliminaryCrossmatchStatusNew):
        return previous_result, None

    if isinstance(previous_result, PreliminaryCrossmatchStatusExisting):
        neighbor = next((n for n in evidence.neighbors if n.pgc == previous_result.pgc), None)

        if neighbor is None or neighbor.redshift is None:
            return previous_result, None

        if abs(neighbor.redshift - record_z) < redshift_tolerance:
            return previous_result, None

        return previous_result, PendingReason.REDSHIFT_MISMATCH

    neighbors_involved = [n for n in evidence.neighbors if n.pgc in previous_result.pgcs]
    if any(n.redshift is None for n in neighbors_involved):
        return previous_result, None

    close = [
        n for n in neighbors_involved if n.redshift is not None and abs(n.redshift - record_z) < redshift_tolerance
    ]
    if len(close) == 1:
        return PreliminaryCrossmatchStatusExisting(close[0].pgc), None

    return previous_result, None
