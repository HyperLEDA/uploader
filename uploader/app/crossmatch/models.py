import enum
from dataclasses import dataclass


@dataclass
class Neighbor:
    pgc: int
    ra: float
    dec: float
    distance_deg: float
    design: str | None = None
    redshift: float | None = None
    type_name: str | None = None


@dataclass
class RecordEvidence:
    neighbors: list[Neighbor]
    record_designation: str | None = None
    same_name_pgcs: list[int] | None = None
    record_pgc: int | None = None
    claimed_pgc_exists_in_layer2: bool = False
    record_redshift: float | None = None
    record_type_name: str | None = None


class CrossmatchStatus(enum.Enum):
    NEW = "new"
    EXISTING = "existing"
    COLLIDING = "colliding"


class TriageStatus(enum.Enum):
    RESOLVED = "resolved"
    PENDING = "pending"


class PendingReason(enum.Enum):
    MULTIPLE_OBJECTS_MATCHED = "MULTIPLE_OBJECTS_MATCHED"
    MATCHED_NAME_OUTSIDE_CIRCLE = "MATCHED_NAME_OUTSIDE_CIRCLE"
    MATCHED_PGC_OUTSIDE_CIRCLE = "MATCHED_PGC_OUTSIDE_CIRCLE"
    UNKNOWN_PGC = "UNKNOWN_PGC"
    REDSHIFT_MISMATCH = "REDSHIFT_MISMATCH"
    TYPE_MISMATCH = "TYPE_MISMATCH"


@dataclass
class CrossmatchResult:
    status: CrossmatchStatus
    triage_status: TriageStatus
    matched_pgc: int | None = None
    colliding_pgcs: list[int] | None = None
    pending_reason: PendingReason | None = None
