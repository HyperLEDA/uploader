from dataclasses import dataclass


@dataclass(frozen=True)
class ReportLog:
    message: str


@dataclass(frozen=True)
class ReportProgress:
    percent: int


@dataclass(frozen=True)
class ReportDone:
    total_rows: int


@dataclass(frozen=True)
class ReportError:
    message: str


ReportEvent = ReportLog | ReportProgress | ReportDone | ReportError
