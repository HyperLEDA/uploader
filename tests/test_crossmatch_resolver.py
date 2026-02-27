from app.crossmatch.models import (
    CrossmatchStatus,
    Neighbor,
    RecordEvidence,
    TriageStatus,
)
from app.crossmatch.resolver import resolve


def test_resolve_zero_neighbors() -> None:
    evidence = RecordEvidence(record_id="rec-1", neighbors=[])
    result = resolve(evidence)
    assert result.record_id == "rec-1"
    assert result.status == CrossmatchStatus.NEW
    assert result.triage_status == TriageStatus.RESOLVED
    assert result.matched_pgc is None


def test_resolve_one_neighbor() -> None:
    evidence = RecordEvidence(
        record_id="rec-2",
        neighbors=[Neighbor(pgc=42, ra=10.0, dec=20.0, distance_deg=0.001)],
    )
    result = resolve(evidence)
    assert result.record_id == "rec-2"
    assert result.status == CrossmatchStatus.EXISTING
    assert result.triage_status == TriageStatus.RESOLVED
    assert result.matched_pgc == 42


def test_resolve_multiple_neighbors() -> None:
    evidence = RecordEvidence(
        record_id="rec-3",
        neighbors=[
            Neighbor(pgc=1, ra=10.0, dec=20.0, distance_deg=0.001),
            Neighbor(pgc=2, ra=10.01, dec=20.01, distance_deg=0.002),
        ],
    )
    result = resolve(evidence)
    assert result.record_id == "rec-3"
    assert result.status == CrossmatchStatus.COLLIDING
    assert result.triage_status == TriageStatus.PENDING
    assert result.matched_pgc is None
