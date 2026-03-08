from app.crossmatch.layered import icrs, name, object_type, pgc, redshift
from app.crossmatch.layered.models import (
    PreliminaryCrossmatchStatus,
    PreliminaryCrossmatchStatusExisting,
    PreliminaryCrossmatchStatusNew,
)
from app.crossmatch.models import CrossmatchResult, CrossmatchStatus, PendingReason, RecordEvidence, TriageStatus


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
        icrs_result, pending_reason = icrs.icrs_simple_resolver(evidence, self._radius_deg)
        if pending_reason is not None:
            return _preliminary_to_final(icrs_result, pending_reason)

        pgc_result, pending_reason = pgc.pgc_resolver(evidence, icrs_result)
        if pending_reason is not None:
            return _preliminary_to_final(pgc_result, pending_reason)

        name_result, pending_reason = name.name_resolver(evidence, pgc_result)
        if pending_reason is not None:
            return _preliminary_to_final(name_result, pending_reason)

        redshift_result, pending_reason = redshift.redshift_resolver(evidence, name_result, self._redshift_tolerance)
        if pending_reason is not None:
            return _preliminary_to_final(redshift_result, pending_reason)

        type_result, pending_reason = object_type.object_type_resolver(evidence, redshift_result)
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
