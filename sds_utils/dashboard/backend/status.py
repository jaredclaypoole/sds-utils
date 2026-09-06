from typing import NamedTuple


class StatusCounts(NamedTuple):
    materialized: int = 0
    materializing: int = 0
    failed: int = 0
    skipped: int = 0
    not_run: int = 0
    not_found: int = 0

    def __add__(self, other: object) -> StatusCounts:
        if not isinstance(other, StatusCounts):
            return NotImplemented
        sum_tup = tuple(a + b for a, b in zip(self, other, strict=True))
        return type(self)(*sum_tup)
