from typing import TypeVar

T = TypeVar("T")


def dedup_ordered(items: list[T]) -> list[T]:
    """Return a copy of *items* with duplicates removed, preserving first-seen order."""
    return list(dict.fromkeys(items))
