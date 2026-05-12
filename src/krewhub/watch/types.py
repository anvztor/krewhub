from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class WatchEvent:
    """A single event delivered to watch subscribers."""

    event_type: str  # ADDED, MODIFIED, DELETED
    resource_type: str
    resource_id: str
    resource_version: int
    object: dict[str, Any]
    cookbook_id: str | None = None
    seq: int = 0
    channel: str = ""  # Typed channel: task:completed, etc.


@dataclass(frozen=True)
class WatchOptions:
    """Filtering options for watch subscriptions."""

    resource_type: str | None = None
    cookbook_id: str | None = None
    since: int = 0  # replay from this seq (exclusive)
    resource_types: list[str] = field(default_factory=list)
    channel_prefixes: list[str] = field(default_factory=list)
