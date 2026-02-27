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
    assert result.colliding_pgcs == [1, 2]


def test_resolve_name_match_in_circle() -> None:
    evidence = RecordEvidence(
        record_id="rec-4",
        neighbors=[
            Neighbor(pgc=1, ra=10.0, dec=20.0, distance_deg=0.001, design="NGC 123"),
            Neighbor(pgc=2, ra=10.01, dec=20.01, distance_deg=0.002, design="PGC 456"),
        ],
        record_designation="NGC 123",
        global_pgcs_with_same_design=frozenset({1}),
    )
    result = resolve(evidence)
    assert result.status == CrossmatchStatus.EXISTING
    assert result.triage_status == TriageStatus.RESOLVED
    assert result.matched_pgc == 1


def test_resolve_name_match_outside_circle() -> None:
    evidence = RecordEvidence(
        record_id="rec-5",
        neighbors=[],
        record_designation="NGC 999",
        global_pgcs_with_same_design=frozenset({100}),
    )
    result = resolve(evidence)
    assert result.status == CrossmatchStatus.EXISTING
    assert result.triage_status == TriageStatus.PENDING
    assert result.matched_pgc == 100


def test_resolve_name_match_in_circle_ambiguous_two_matching() -> None:
    evidence = RecordEvidence(
        record_id="rec-6",
        neighbors=[
            Neighbor(pgc=1, ra=10.0, dec=20.0, distance_deg=0.001, design="NGC 123"),
            Neighbor(pgc=2, ra=10.01, dec=20.01, distance_deg=0.002, design="NGC 123"),
        ],
        record_designation="NGC 123",
        global_pgcs_with_same_design=frozenset({1, 2}),
    )
    result = resolve(evidence)
    assert result.status == CrossmatchStatus.COLLIDING
    assert result.triage_status == TriageStatus.PENDING
    assert result.matched_pgc is None
    assert result.colliding_pgcs == [1, 2]


def test_resolve_name_match_outside_circle_ambiguous_multiple_pgcs() -> None:
    evidence = RecordEvidence(
        record_id="rec-7",
        neighbors=[],
        record_designation="NGC 999",
        global_pgcs_with_same_design=frozenset({100, 101}),
    )
    result = resolve(evidence)
    assert result.status == CrossmatchStatus.NEW
    assert result.triage_status == TriageStatus.RESOLVED
    assert result.matched_pgc is None


def test_resolve_one_neighbor_matching_pgc() -> None:
    evidence = RecordEvidence(
        record_id="rec-8",
        neighbors=[Neighbor(pgc=42, ra=10.0, dec=20.0, distance_deg=0.001)],
        record_pgc=42,
        claimed_pgc_exists_in_layer2=True,
    )
    result = resolve(evidence)
    assert result.status == CrossmatchStatus.EXISTING
    assert result.triage_status == TriageStatus.RESOLVED
    assert result.matched_pgc == 42


def test_resolve_one_neighbor_different_pgc() -> None:
    evidence = RecordEvidence(
        record_id="rec-9",
        neighbors=[Neighbor(pgc=100, ra=10.0, dec=20.0, distance_deg=0.001)],
        record_pgc=42,
        claimed_pgc_exists_in_layer2=True,
    )
    result = resolve(evidence)
    assert result.status == CrossmatchStatus.EXISTING
    assert result.triage_status == TriageStatus.PENDING
    assert result.matched_pgc == 100


def test_resolve_no_neighbors_claimed_pgc_exists() -> None:
    evidence = RecordEvidence(
        record_id="rec-10",
        neighbors=[],
        record_pgc=42,
        claimed_pgc_exists_in_layer2=True,
    )
    result = resolve(evidence)
    assert result.status == CrossmatchStatus.EXISTING
    assert result.triage_status == TriageStatus.PENDING
    assert result.matched_pgc == 42


def test_resolve_one_neighbor_name_match_pgc_mismatch() -> None:
    evidence = RecordEvidence(
        record_id="rec-11",
        neighbors=[Neighbor(pgc=100, ra=10.0, dec=20.0, distance_deg=0.001, design="NGC 123")],
        record_designation="NGC 123",
        record_pgc=42,
        claimed_pgc_exists_in_layer2=True,
    )
    result = resolve(evidence)
    assert result.status == CrossmatchStatus.EXISTING
    assert result.triage_status == TriageStatus.PENDING
    assert result.matched_pgc == 100
