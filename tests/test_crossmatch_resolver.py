from app.crossmatch.models import (
    CrossmatchResult,
    CrossmatchStatus,
    Neighbor,
    PendingReason,
    RecordEvidence,
    TriageStatus,
)
from app.crossmatch.resolver import (
    _apply_redshift_check,
    _resolve_by_radius_coordinate,
    resolve,
    resolve_by_radius,
)


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
    assert result.pending_reason == PendingReason.MULTIPLE_OBJECTS_MATCHED


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
    assert result.pending_reason == PendingReason.MATCHED_NAME_OUTSIDE_CIRCLE


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
    assert result.pending_reason == PendingReason.PGC_MISMATCH


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
    assert result.pending_reason == PendingReason.MATCHED_PGC_OUTSIDE_CIRCLE


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
    assert result.pending_reason == PendingReason.PGC_MISMATCH


R1, R2 = 0.0005, 0.002
TOL = 0.0003


def test_resolve_by_radius_no_neighbors_new_resolved() -> None:
    evidence = RecordEvidence(record_id="r1", neighbors=[])
    result = resolve_by_radius(evidence, R1, R2, TOL)
    assert result.status == CrossmatchStatus.NEW
    assert result.triage_status == TriageStatus.RESOLVED
    assert result.matched_pgc is None


def test_resolve_by_radius_multiple_in_inner_colliding() -> None:
    evidence = RecordEvidence(
        record_id="r2",
        neighbors=[
            Neighbor(pgc=1, ra=10.0, dec=20.0, distance_deg=0.0003),
            Neighbor(pgc=2, ra=10.0, dec=20.0, distance_deg=0.0004),
        ],
    )
    result = resolve_by_radius(evidence, R1, R2, TOL)
    assert result.status == CrossmatchStatus.COLLIDING
    assert result.triage_status == TriageStatus.PENDING
    assert result.colliding_pgcs == [1, 2]
    assert result.pending_reason == PendingReason.MULTIPLE_IN_INNER_RADIUS


def test_resolve_by_radius_single_in_inner_none_in_outer_existing_resolved() -> None:
    evidence = RecordEvidence(
        record_id="r3",
        neighbors=[
            Neighbor(pgc=10, ra=10.0, dec=20.0, distance_deg=0.0003),
        ],
    )
    result = resolve_by_radius(evidence, R1, R2, TOL)
    assert result.status == CrossmatchStatus.EXISTING
    assert result.triage_status == TriageStatus.RESOLVED
    assert result.matched_pgc == 10


def test_resolve_by_radius_single_in_inner_none_in_outer_redshift_not_close_pending() -> None:
    evidence = RecordEvidence(
        record_id="r4",
        neighbors=[
            Neighbor(pgc=10, ra=10.0, dec=20.0, distance_deg=0.0003, redshift=0.1),
        ],
        record_redshift=0.5,
    )
    result = resolve_by_radius(evidence, R1, R2, TOL)
    assert result.status == CrossmatchStatus.EXISTING
    assert result.triage_status == TriageStatus.PENDING
    assert result.matched_pgc == 10
    assert result.pending_reason == PendingReason.REDSHIFT_MISMATCH


def test_resolve_by_radius_single_in_inner_none_in_outer_redshift_close_stays_resolved() -> None:
    evidence = RecordEvidence(
        record_id="r5",
        neighbors=[
            Neighbor(pgc=10, ra=10.0, dec=20.0, distance_deg=0.0003, redshift=0.05),
        ],
        record_redshift=0.0502,
    )
    result = resolve_by_radius(evidence, R1, R2, TOL)
    assert result.status == CrossmatchStatus.EXISTING
    assert result.triage_status == TriageStatus.RESOLVED
    assert result.matched_pgc == 10


def test_resolve_by_radius_single_in_outer_only_no_redshift_proxy_pending() -> None:
    evidence = RecordEvidence(
        record_id="r6",
        neighbors=[
            Neighbor(pgc=10, ra=10.0, dec=20.0, distance_deg=0.001),
        ],
    )
    result = resolve_by_radius(evidence, R1, R2, TOL)
    assert result.status == CrossmatchStatus.EXISTING
    assert result.triage_status == TriageStatus.PENDING
    assert result.matched_pgc == 10
    assert result.pending_reason == PendingReason.SINGLE_IN_OUTER_RADIUS_ONLY


def test_resolve_by_radius_single_in_outer_only_record_no_redshift_proxy() -> None:
    evidence = RecordEvidence(
        record_id="r7",
        neighbors=[
            Neighbor(pgc=10, ra=10.0, dec=20.0, distance_deg=0.001, redshift=0.05),
        ],
        record_redshift=None,
    )
    result = resolve_by_radius(evidence, R1, R2, TOL)
    assert result.status == CrossmatchStatus.EXISTING
    assert result.triage_status == TriageStatus.PENDING
    assert result.pending_reason == PendingReason.SINGLE_IN_OUTER_RADIUS_ONLY


def test_resolve_by_radius_single_in_outer_only_redshift_close_resolved() -> None:
    evidence = RecordEvidence(
        record_id="r8",
        neighbors=[
            Neighbor(pgc=10, ra=10.0, dec=20.0, distance_deg=0.001, redshift=0.05),
        ],
        record_redshift=0.0501,
    )
    result = resolve_by_radius(evidence, R1, R2, TOL)
    assert result.status == CrossmatchStatus.EXISTING
    assert result.triage_status == TriageStatus.RESOLVED
    assert result.matched_pgc == 10


def test_resolve_by_radius_single_in_outer_only_redshift_not_close_pending() -> None:
    evidence = RecordEvidence(
        record_id="r9",
        neighbors=[
            Neighbor(pgc=10, ra=10.0, dec=20.0, distance_deg=0.001, redshift=0.05),
        ],
        record_redshift=0.2,
    )
    result = resolve_by_radius(evidence, R1, R2, TOL)
    assert result.status == CrossmatchStatus.EXISTING
    assert result.triage_status == TriageStatus.PENDING
    assert result.matched_pgc == 10
    assert result.pending_reason == PendingReason.REDSHIFT_MISMATCH


def test_resolve_by_radius_multiple_in_outer_any_no_redshift_colliding_proxy() -> None:
    evidence = RecordEvidence(
        record_id="r10",
        neighbors=[
            Neighbor(pgc=10, ra=10.0, dec=20.0, distance_deg=0.001, redshift=0.05),
            Neighbor(pgc=20, ra=10.01, dec=20.01, distance_deg=0.0015, redshift=None),
        ],
        record_redshift=0.05,
    )
    result = resolve_by_radius(evidence, R1, R2, TOL)
    assert result.status == CrossmatchStatus.COLLIDING
    assert result.triage_status == TriageStatus.PENDING
    assert result.colliding_pgcs == [10, 20]
    assert result.pending_reason == PendingReason.MULTIPLE_IN_OUTER_RADIUS


def test_resolve_by_radius_multiple_in_outer_one_close_redshift_resolved() -> None:
    evidence = RecordEvidence(
        record_id="r11",
        neighbors=[
            Neighbor(pgc=10, ra=10.0, dec=20.0, distance_deg=0.001, redshift=0.05),
            Neighbor(pgc=20, ra=10.01, dec=20.01, distance_deg=0.0015, redshift=0.2),
        ],
        record_redshift=0.0502,
    )
    result = resolve_by_radius(evidence, R1, R2, TOL)
    assert result.status == CrossmatchStatus.EXISTING
    assert result.triage_status == TriageStatus.RESOLVED
    assert result.matched_pgc == 10


def test_resolve_by_radius_multiple_in_outer_multiple_close_redshift_colliding() -> None:
    evidence = RecordEvidence(
        record_id="r12",
        neighbors=[
            Neighbor(pgc=10, ra=10.0, dec=20.0, distance_deg=0.001, redshift=0.05),
            Neighbor(pgc=20, ra=10.01, dec=20.01, distance_deg=0.0015, redshift=0.0501),
        ],
        record_redshift=0.05,
    )
    result = resolve_by_radius(evidence, R1, R2, TOL)
    assert result.status == CrossmatchStatus.COLLIDING
    assert result.triage_status == TriageStatus.PENDING
    assert result.colliding_pgcs == [10, 20]
    assert result.pending_reason == PendingReason.MULTIPLE_IN_OUTER_RADIUS


def test_resolve_by_radius_multiple_in_outer_zero_close_redshift_colliding() -> None:
    evidence = RecordEvidence(
        record_id="r13",
        neighbors=[
            Neighbor(pgc=10, ra=10.0, dec=20.0, distance_deg=0.001, redshift=0.05),
            Neighbor(pgc=20, ra=10.01, dec=20.01, distance_deg=0.0015, redshift=0.1),
        ],
        record_redshift=0.5,
    )
    result = resolve_by_radius(evidence, R1, R2, TOL)
    assert result.status == CrossmatchStatus.COLLIDING
    assert result.triage_status == TriageStatus.PENDING
    assert result.colliding_pgcs == [10, 20]


def test_resolve_by_radius_single_in_inner_with_outer_colliding() -> None:
    evidence = RecordEvidence(
        record_id="r14",
        neighbors=[
            Neighbor(pgc=1, ra=10.0, dec=20.0, distance_deg=0.0003),
            Neighbor(pgc=2, ra=10.01, dec=20.01, distance_deg=0.001),
        ],
    )
    result = resolve_by_radius(evidence, R1, R2, TOL)
    assert result.status == CrossmatchStatus.COLLIDING
    assert result.triage_status == TriageStatus.PENDING
    assert result.matched_pgc == 1
    assert result.pending_reason == PendingReason.SINGLE_IN_INNER_WITH_OUTER_NEIGHBORS


def test_apply_redshift_check_new_proxy() -> None:
    coord = CrossmatchResult(
        record_id="a",
        status=CrossmatchStatus.NEW,
        triage_status=TriageStatus.RESOLVED,
        matched_pgc=None,
    )
    evidence = RecordEvidence(record_id="a", neighbors=[], record_redshift=0.05)
    result = _apply_redshift_check(coord, evidence, TOL)
    assert result.status == CrossmatchStatus.NEW
    assert result.triage_status == TriageStatus.RESOLVED


def test_apply_redshift_check_no_record_redshift_proxy() -> None:
    coord = CrossmatchResult(
        record_id="a",
        status=CrossmatchStatus.EXISTING,
        triage_status=TriageStatus.PENDING,
        matched_pgc=10,
        pending_reason=PendingReason.SINGLE_IN_OUTER_RADIUS_ONLY,
    )
    evidence = RecordEvidence(
        record_id="a",
        neighbors=[Neighbor(pgc=10, ra=0, dec=0, distance_deg=0.001, redshift=0.05)],
        record_redshift=None,
    )
    result = _apply_redshift_check(coord, evidence, TOL)
    assert result.status == CrossmatchStatus.EXISTING
    assert result.pending_reason == PendingReason.SINGLE_IN_OUTER_RADIUS_ONLY


def test_resolve_by_radius_coordinate_single_in_outer_only() -> None:
    evidence = RecordEvidence(
        record_id="c1",
        neighbors=[
            Neighbor(pgc=10, ra=10.0, dec=20.0, distance_deg=0.001),
        ],
    )
    result = _resolve_by_radius_coordinate(evidence, R1, R2)
    assert result.status == CrossmatchStatus.EXISTING
    assert result.triage_status == TriageStatus.PENDING
    assert result.matched_pgc == 10
    assert result.pending_reason == PendingReason.SINGLE_IN_OUTER_RADIUS_ONLY
