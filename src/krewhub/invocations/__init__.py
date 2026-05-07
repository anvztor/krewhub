"""Invocation Contract — runtime infrastructure for the three Hands.

Contains:
- protocol: `Hand`, `TapeWriter`, `CancelToken` Protocols (contract §4).
"""

from krewhub.invocations.protocol import (
    CancelToken,
    Hand,
    TapeWriter,
)

__all__ = ["Hand", "TapeWriter", "CancelToken"]
