from app.crossmatch.layered.models import (
    PreliminaryCrossmatchStatus,
    PreliminaryCrossmatchStatusColliding,
    PreliminaryCrossmatchStatusExisting,
    PreliminaryCrossmatchStatusNew,
)
from app.crossmatch.models import PendingReason, RecordEvidence


def icrs_simple_resolver(
    evidence: RecordEvidence, radius_deg: float
) -> tuple[PreliminaryCrossmatchStatus, PendingReason | None]:
    neighbors_within_radius = [n for n in evidence.neighbors if n.distance_deg <= radius_deg]

    if len(neighbors_within_radius) == 0:
        return PreliminaryCrossmatchStatusNew(), None

    if len(neighbors_within_radius) == 1:
        return PreliminaryCrossmatchStatusExisting(neighbors_within_radius[0].pgc), None

    return PreliminaryCrossmatchStatusColliding({n.pgc for n in neighbors_within_radius}), None
