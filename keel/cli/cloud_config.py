"""Cloud configuration loading from ``{keel_dir}/config.json``.

Configuration errors NEVER cause CLI failure.  Every edge case falls back
to sensible defaults and (optionally) logs a warning to stderr.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DEFAULT_QUEUE_TTL_HOURS: int = 24
MAX_QUEUE_SIZE: int = 1000


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def load_queue_ttl_hours(keel_dir: str) -> int:
    """Load ``cloud.queue_ttl_hours`` from ``{keel_dir}/config.json``.

    Fallback rules (build plan v2.2.1 / v2.2.2):

    * File missing → default (24 h), no warning.
    * Invalid JSON → default, warning to stderr.
    * ``cloud`` key absent → default, no warning.
    * ``cloud.queue_ttl_hours`` absent → default, no warning.
    * Non-numeric / zero / negative → default, warning to stderr.
    """
    config_path = Path(keel_dir) / "config.json"

    if not config_path.exists():
        return DEFAULT_QUEUE_TTL_HOURS

    try:
        with open(config_path, "r") as fh:
            data = json.load(fh)
    except (json.JSONDecodeError, OSError):
        print(
            "[keel] Warning: config.json parse error, using defaults.",
            file=sys.stderr,
        )
        return DEFAULT_QUEUE_TTL_HOURS

    if not isinstance(data, dict):
        return DEFAULT_QUEUE_TTL_HOURS

    cloud_section = data.get("cloud")
    if not isinstance(cloud_section, dict):
        return DEFAULT_QUEUE_TTL_HOURS

    if "queue_ttl_hours" not in cloud_section:
        return DEFAULT_QUEUE_TTL_HOURS

    ttl = cloud_section["queue_ttl_hours"]

    if not isinstance(ttl, (int, float)) or ttl <= 0:
        print(
            "[keel] Warning: invalid queue_ttl_hours value, using 24h default.",
            file=sys.stderr,
        )
        return DEFAULT_QUEUE_TTL_HOURS

    return int(ttl)
