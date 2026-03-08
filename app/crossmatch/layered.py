from dataclasses import dataclass

from app.crossmatch.models import CrossmatchResult, CrossmatchStatus, PendingReason, RecordEvidence, TriageStatus


@dataclass
class PreliminaryCrossmatchStatusNew:
    pass


@dataclass
class PreliminaryCrossmatchStatusExisting:
    pgc: int


@dataclass
class PreliminaryCrossmatchStatusColliding:
    pgcs: set[int]


PreliminaryCrossmatchStatus = (
    PreliminaryCrossmatchStatusNew | PreliminaryCrossmatchStatusExisting | PreliminaryCrossmatchStatusColliding
)


def icrs_simple_resolver(
    evidence: RecordEvidence, radius_deg: float
) -> tuple[PreliminaryCrossmatchStatus, PendingReason | None]:
    neighbors_within_radius = [n for n in evidence.neighbors if n.distance_deg <= radius_deg]

    if len(neighbors_within_radius) == 0:
        return PreliminaryCrossmatchStatusNew(), None

    if len(neighbors_within_radius) == 1:
        return PreliminaryCrossmatchStatusExisting(neighbors_within_radius[0].pgc), None

    return PreliminaryCrossmatchStatusColliding({n.pgc for n in neighbors_within_radius}), None


def name_resolver(
    evidence: RecordEvidence, previous_result: PreliminaryCrossmatchStatus
) -> tuple[PreliminaryCrossmatchStatus, PendingReason | None]:
    name_pgcs = set(evidence.same_name_pgcs or [])

    if isinstance(previous_result, PreliminaryCrossmatchStatusNew):
        if len(name_pgcs) == 0:
            return previous_result, None

        if len(name_pgcs) == 1:
            objs = evidence.same_name_pgcs or []
            return PreliminaryCrossmatchStatusExisting(objs[0]), PendingReason.MATCHED_NAME_OUTSIDE_CIRCLE

        return PreliminaryCrossmatchStatusColliding(name_pgcs), PendingReason.MATCHED_NAME_OUTSIDE_CIRCLE

    if isinstance(previous_result, PreliminaryCrossmatchStatusExisting):
        record_pgc = previous_result.pgc

        if len(name_pgcs) == 0:
            return previous_result, None

        if len(name_pgcs) == 1:
            name_pgc = next(iter(name_pgcs))
            if name_pgc == record_pgc:
                return previous_result, None

            return PreliminaryCrossmatchStatusColliding(
                {name_pgc, record_pgc}
            ), PendingReason.MATCHED_NAME_OUTSIDE_CIRCLE

        return PreliminaryCrossmatchStatusColliding(name_pgcs | {record_pgc}), PendingReason.MATCHED_NAME_OUTSIDE_CIRCLE

    if len(name_pgcs) == 0:
        return previous_result, None

    if len(name_pgcs) == 1:
        name_pgc = next(iter(name_pgcs))

        for matched_pgc in previous_result.pgcs:
            if matched_pgc == name_pgc:
                return PreliminaryCrossmatchStatusExisting(name_pgc), None

        return PreliminaryCrossmatchStatusColliding(
            previous_result.pgcs | {name_pgc}
        ), PendingReason.MATCHED_NAME_OUTSIDE_CIRCLE

    common_pgcs = name_pgcs.intersection(previous_result.pgcs)
    if len(common_pgcs) == 1:
        return PreliminaryCrossmatchStatusExisting(next(iter(common_pgcs))), None

    return PreliminaryCrossmatchStatusColliding(
        previous_result.pgcs | name_pgcs
    ), PendingReason.MATCHED_NAME_OUTSIDE_CIRCLE


def pgc_resolver(
    evidence: RecordEvidence, previous_result: PreliminaryCrossmatchStatus
) -> tuple[PreliminaryCrossmatchStatus, PendingReason | None]:
    if evidence.record_pgc is None:
        return previous_result, None

    if evidence.record_pgc is not None and not evidence.claimed_pgc_exists_in_layer2:
        return previous_result, PendingReason.UNKNOWN_PGC

    pgc = evidence.record_pgc

    if isinstance(previous_result, PreliminaryCrossmatchStatusNew):
        return PreliminaryCrossmatchStatusExisting(pgc), PendingReason.MATCHED_PGC_OUTSIDE_CIRCLE

    if isinstance(previous_result, PreliminaryCrossmatchStatusExisting):
        if pgc == previous_result.pgc:
            return previous_result, None

        return PreliminaryCrossmatchStatusColliding(
            {pgc, previous_result.pgc}
        ), PendingReason.MATCHED_PGC_OUTSIDE_CIRCLE

    if pgc in previous_result.pgcs:
        return PreliminaryCrossmatchStatusExisting(pgc), None

    return PreliminaryCrossmatchStatusColliding(previous_result.pgcs | {pgc}), PendingReason.MATCHED_PGC_OUTSIDE_CIRCLE


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
        # across neigbours, find me a neighbour with the matched PGC
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


def _preliminary_to_final(results: PreliminaryCrossmatchStatus, pending_reason: PendingReason) -> CrossmatchResult:
    if isinstance(results, PreliminaryCrossmatchStatusNew):
        return CrossmatchResult(
            status=CrossmatchStatus.NEW, triage_status=TriageStatus.PENDING, pending_reason=pending_reason
        )
    if isinstance(results, PreliminaryCrossmatchStatusExisting):
        return CrossmatchResult(
            status=CrossmatchStatus.EXISTING,
            triage_status=TriageStatus.PENDING,
            matched_pgc=results.pgc,
            pending_reason=pending_reason,
        )
    return CrossmatchResult(
        status=CrossmatchStatus.COLLIDING,
        triage_status=TriageStatus.PENDING,
        colliding_pgcs=list(results.pgcs),
        pending_reason=pending_reason,
    )


class LayeredResolver:
    def __init__(
        self,
        radius_deg: float,
        pgc_column: str | None = None,
        redshift_tolerance: float | None = None,
    ) -> None:
        self._radius_deg = radius_deg
        self._pgc_column = pgc_column
        self._redshift_tolerance = redshift_tolerance

    @property
    def search_radius_deg(self) -> float:
        return self._radius_deg

    @property
    def pgc_column(self) -> str | None:
        return self._pgc_column

    def resolve(self, evidence: RecordEvidence) -> CrossmatchResult:
        icrs_result, pending_reason = icrs_simple_resolver(evidence, self._radius_deg)
        if pending_reason is not None:
            return _preliminary_to_final(icrs_result, pending_reason)

        pgc_result, pending_reason = pgc_resolver(evidence, icrs_result)
        if pending_reason is not None:
            return _preliminary_to_final(pgc_result, pending_reason)

        name_result, pending_reason = name_resolver(evidence, pgc_result)
        if pending_reason is not None:
            return _preliminary_to_final(name_result, pending_reason)

        redshift_result, pending_reason = redshift_resolver(evidence, name_result, self._redshift_tolerance)
        if pending_reason is not None:
            return _preliminary_to_final(redshift_result, pending_reason)

        type_result, pending_reason = object_type_resolver(evidence, redshift_result)
        if pending_reason is not None:
            return _preliminary_to_final(type_result, pending_reason)
        final_result = type_result

        if isinstance(final_result, PreliminaryCrossmatchStatusNew):
            return CrossmatchResult(status=CrossmatchStatus.NEW, triage_status=TriageStatus.RESOLVED)

        if isinstance(final_result, PreliminaryCrossmatchStatusExisting):
            return CrossmatchResult(
                status=CrossmatchStatus.EXISTING, triage_status=TriageStatus.RESOLVED, matched_pgc=final_result.pgc
            )

        return CrossmatchResult(
            status=CrossmatchStatus.COLLIDING,
            triage_status=TriageStatus.PENDING,
            colliding_pgcs=list(final_result.pgcs),
            pending_reason=PendingReason.MULTIPLE_OBJECTS_MATCHED,
        )
