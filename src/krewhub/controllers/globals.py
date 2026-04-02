from __future__ import annotations

from krewhub.controllers.manager import ControllerManager

_manager: ControllerManager | None = None


def set_controller_manager(manager: ControllerManager) -> None:
    global _manager
    _manager = manager


def clear_controller_manager() -> None:
    global _manager
    _manager = None


def get_controller_manager() -> ControllerManager | None:
    return _manager
