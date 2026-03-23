"""JSONL-based fallback queue for cloud write operations.

When a cloud API call fails due to a network error, the operation is
executed locally and enqueued here for later replay.  Queue drain is
piggybacked on the next successful cloud API call (no background thread).

Directory layout::

    {keel_dir}/.cloud_queue/
        pending.jsonl       ← FIFO queue (one JSON object per line)
        abandoned.jsonl     ← Dead-letter file (append-only)
"""

from __future__ import annotations

import json
import os
import sys
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

from keel.cli.cloud_config import (
    DEFAULT_QUEUE_TTL_HOURS,
    MAX_QUEUE_SIZE,
    load_queue_ttl_hours,
)


def _utcnow() -> datetime:
    """Current UTC time (mockable in tests)."""
    return datetime.now(timezone.utc)


class CloudQueue:
    """Fallback queue for write operations that fail due to network errors."""

    def __init__(self, keel_dir: str) -> None:
        self._queue_dir = Path(keel_dir) / ".cloud_queue"
        self._pending_path = self._queue_dir / "pending.jsonl"
        self._abandoned_path = self._queue_dir / "abandoned.jsonl"
        self._ttl_hours = load_queue_ttl_hours(keel_dir)

    # -- Public API ---------------------------------------------------------

    def ensure_dir(self) -> None:
        """Create the queue directory if it does not exist."""
        self._queue_dir.mkdir(parents=True, exist_ok=True)

    def enqueue(
        self,
        operation: str,
        method: str,
        path: str,
        body: dict | None,
        idempotency_key: str,
    ) -> bool:
        """Add a write operation to the pending queue.

        Returns ``True`` on success, ``False`` if the queue is full
        (>= ``MAX_QUEUE_SIZE``).  When full, a warning is printed to
        stderr but the CLI operation still succeeds via local execution.
        """
        if self.pending_count() >= MAX_QUEUE_SIZE:
            print(
                f"[keel] Cloud queue full ({MAX_QUEUE_SIZE} items). "
                "Operating in local-only mode.",
                file=sys.stderr,
            )
            return False

        now = _utcnow()
        ttl_expires = now + timedelta(hours=self._ttl_hours)

        item: dict[str, Any] = {
            "enqueued_at": now.isoformat(),
            "idempotency_key": idempotency_key,
            "ttl_expires_at": ttl_expires.isoformat(),
            "method": method,
            "path": path,
            "body": body,
            "operation": operation,
        }

        self.ensure_dir()
        with open(self._pending_path, "a") as fh:
            fh.write(json.dumps(item, default=str) + "\n")

        return True

    def drain(self, replay_fn: Callable) -> dict:
        """Replay pending operations and age-out expired items.

        Parameters
        ----------
        replay_fn : callable
            ``(method, path, body, idempotency_key) -> int``
            Returns the HTTP status code.  Raises on network failure
            (caller should catch ``URLError`` / ``OSError``).

        Returns
        -------
        dict
            ``{"replayed": N, "abandoned": [item, ...], "remaining": N}``
        """
        items = self._load_pending()
        if not items:
            return {"replayed": 0, "abandoned": [], "remaining": 0}

        now = _utcnow()
        abandoned: list[dict] = []
        remaining: list[dict] = []
        replayed = 0

        # Phase 1 — age-out expired items.
        active: list[dict] = []
        for item in items:
            expires = item.get("ttl_expires_at", "")
            try:
                expires_dt = datetime.fromisoformat(expires)
            except (ValueError, TypeError):
                expires_dt = now  # malformed → treat as expired
            if expires_dt <= now:
                item["abandoned_at"] = now.isoformat()
                item["reason"] = "ttl_expired"
                abandoned.append(item)
            else:
                active.append(item)

        # Phase 2 — replay active items FIFO.
        stop_index: int | None = None
        for i, item in enumerate(active):
            try:
                status = replay_fn(
                    item["method"],
                    item["path"],
                    item.get("body"),
                    item["idempotency_key"],
                )
            except Exception:
                # Network failure — stop replay, keep remaining.
                stop_index = i
                break

            if 200 <= status < 300:
                replayed += 1
            elif status == 409:
                item["abandoned_at"] = now.isoformat()
                item["reason"] = "conflict_409"
                abandoned.append(item)
            else:
                # Other HTTP error — abandon and continue.
                item["abandoned_at"] = now.isoformat()
                item["reason"] = f"http_{status}"
                abandoned.append(item)

        if stop_index is not None:
            remaining = active[stop_index:]
        else:
            # All active items processed (replayed or abandoned).
            remaining = []

        # Write abandoned items to dead-letter file.
        if abandoned:
            self.ensure_dir()
            with open(self._abandoned_path, "a") as fh:
                for item in abandoned:
                    fh.write(json.dumps(item, default=str) + "\n")

        # Phase 3 — atomic rewrite of pending queue.
        self._save_pending(remaining)

        return {
            "replayed": replayed,
            "abandoned": abandoned,
            "remaining": len(remaining),
        }

    def pending_count(self) -> int:
        """Number of items in the pending queue."""
        return len(self._load_pending())

    def abandoned_count(self) -> int:
        """Number of items in the dead-letter file."""
        if not self._abandoned_path.exists():
            return 0
        count = 0
        with open(self._abandoned_path, "r") as fh:
            for line in fh:
                if line.strip():
                    count += 1
        return count

    # -- Internals ----------------------------------------------------------

    def _load_pending(self) -> list[dict]:
        """Read all items from ``pending.jsonl``."""
        if not self._pending_path.exists():
            return []
        items: list[dict] = []
        with open(self._pending_path, "r") as fh:
            for line in fh:
                line = line.strip()
                if line:
                    try:
                        items.append(json.loads(line))
                    except json.JSONDecodeError:
                        pass  # skip malformed lines
        return items

    def _save_pending(self, items: list[dict]) -> None:
        """Atomic rewrite of ``pending.jsonl``."""
        self.ensure_dir()
        tmp_path = self._pending_path.with_suffix(".tmp")
        with open(tmp_path, "w") as fh:
            for item in items:
                fh.write(json.dumps(item, default=str) + "\n")
        tmp_path.replace(self._pending_path)
