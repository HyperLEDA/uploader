from dataclasses import dataclass


@dataclass(frozen=True)
class LogEvent:
    message: str


@dataclass(frozen=True)
class ProgressEvent:
    percent: float


@dataclass(frozen=True)
class DoneEvent:
    message: str


@dataclass(frozen=True)
class ErrorEvent:
    message: str


Event = LogEvent | ProgressEvent | DoneEvent | ErrorEvent
