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


@dataclass
class CrossmatchResult:
    record_id: str
    status: CrossmatchStatus
    triage_status: TriageStatus
    matched_pgc: int | None
    colliding_pgcs: list[int] | None = None
