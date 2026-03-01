import enum
from dataclasses import dataclass


@dataclass
class Neighbor:
    pgc: int
    ra: float
    dec: float
    distance_deg: float
    design: str | None = None


@dataclass
class RecordEvidence:
    record_id: str
    neighbors: list[Neighbor]
    record_designation: str | None = None
    global_pgcs_with_same_design: frozenset[int] | None = None
    record_pgc: int | None = None
    claimed_pgc_exists_in_layer2: bool = False


class CrossmatchStatus(enum.Enum):
    NEW = "new"
    EXISTING = "existing"
    COLLIDING = "colliding"


class TriageStatus(enum.Enum):
    RESOLVED = "resolved"
    PENDING = "pending"


class PendingReason(enum.Enum):
    # default
    PGC_MISMATCH = "PGC_MISMATCH"
    MULTIPLE_OBJECTS_MATCHED = "MULTIPLE_OBJECTS_MATCHED"
    MATCHED_NAME_OUTSIDE_CIRCLE = "MATCHED_NAME_OUTSIDE_CIRCLE"
    MATCHED_PGC_OUTSIDE_CIRCLE = "MATCHED_PGC_OUTSIDE_CIRCLE"
    SINGLE_NEIGHBOR_NO_IDENTITY_MATCH = "SINGLE_NEIGHBOR_NO_IDENTITY_MATCH"

    # two radii
    MULTIPLE_IN_INNER_RADIUS = "MULTIPLE_IN_INNER_RADIUS"
    MULTIPLE_IN_OUTER_RADIUS = "MULTIPLE_IN_OUTER_RADIUS"
    SINGLE_IN_INNER_WITH_OUTER_NEIGHBORS = "SINGLE_IN_INNER_WITH_OUTER_NEIGHBORS"
    SINGLE_IN_OUTER_RADIUS_ONLY = "SINGLE_IN_OUTER_RADIUS_ONLY"


@dataclass
class CrossmatchResult:
    record_id: str
    status: CrossmatchStatus
    triage_status: TriageStatus
    matched_pgc: int | None
    colliding_pgcs: list[int] | None = None
    pending_reason: PendingReason | None = None
