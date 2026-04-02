from __future__ import annotations

from krewhub.watch.service import WatchService

_watch_service: WatchService | None = None


def set_watch_service(service: WatchService) -> None:
    global _watch_service
    _watch_service = service


def clear_watch_service() -> None:
    global _watch_service
    _watch_service = None


def get_watch_service() -> WatchService:
    if _watch_service is None:
        raise RuntimeError("WatchService not initialized. App lifespan not started.")
    return _watch_service
