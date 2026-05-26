from typing import Protocol

from uploader.app.crossmatch import layered
from uploader.app.crossmatch.models import (
    CrossmatchResult,
    RecordEvidence,
)


class Resolver(Protocol):
    @property
    def search_radius_deg(self) -> float: ...

    @property
    def pgc_column(self) -> str | None: ...

    def resolve(self, evidence: RecordEvidence) -> CrossmatchResult: ...


LayeredResolver = layered.LayeredResolver
