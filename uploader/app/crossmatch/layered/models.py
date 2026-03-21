from dataclasses import dataclass


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
