"""Failure types raised by the ingestion and the exit code each one maps to.

``RateLimitExhausted`` and ``SourceError`` are raised while calling the APIs
(retries already exhausted); ``die`` prints a message to stderr and exits with
the code from the CLI contract: 2 validation, 3 rate limit, 4 source failed.
"""

from __future__ import annotations

import sys
from typing import NoReturn


class RateLimitExhausted(Exception):
    """429 until the retries ran out (exit 3)."""


class SourceError(Exception):
    """A source failed after the retries ran out (exit 4)."""


# It lives here, and not in main.py, because the ingestion module also aborts
# with it and importing main from there would create a cycle.
def die(code: int, message: str) -> NoReturn:
    """Message to stderr and exit with ``code``: 2 validation, 3 rate limit, 4 partial."""
    print(message, file=sys.stderr)
    raise SystemExit(code)
