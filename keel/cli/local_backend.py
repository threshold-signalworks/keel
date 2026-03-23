"""Local (offline) implementation of the CLI backend.

Each method call is stateless from the caller's perspective:

1. Load ``PolicyStore`` from ``{keel_dir}/store.json``.
2. Load ``WAL`` from ``{keel_dir}/wal/{agent}.wal.jsonl``.
3. Construct ``ContextManager``, ``FidelityVerifier``, etc. as needed.
4. Perform operation.
5. ``PolicyStore`` auto-saves on mutation (``_auto_save``).

Quarantine state is reconstructed from WAL events
(``QUARANTINED`` / ``QUARANTINE_RELEASED``).
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

from keel.actuation.quarantine import QuarantineManager
from keel.actuation.validator import Validator
from keel.actuation.wal import WAL
from keel.cli.backend import Backend
from keel.core.context_manager import ContextManager
from keel.core.fidelity import FidelityVerifier
from keel.core.policy_store import PolicyStore
from keel.core.schemas import (
    ActionSpec,
    ContextConfig,
    WALEventType,
)


class LocalBackend(Backend):
    """Offline backend using the keel library directly."""

    # -- Path helpers -------------------------------------------------------

    def _store_path(self) -> Path:
        return Path(self.keel_dir) / "store.json"

    def _wal_path(self) -> Path:
        return Path(self.keel_dir) / "wal" / f"{self.agent}.wal.jsonl"

    # -- Component loading --------------------------------------------------

    def _load_components(self):
        """Load the full component graph from disk.

        Returns ``(policy_store, wal, context_manager, fidelity_verifier)``.
        """
        store_path = self._store_path()
        wal_path = self._wal_path()
        wal_path.parent.mkdir(parents=True, exist_ok=True)

        policy_store = PolicyStore(store_path=store_path)
        wal = WAL(wal_path=wal_path, session_id=self.agent)
        config = ContextConfig()
        context_manager = ContextManager(policy_store, config, wal=wal)
        fidelity_verifier = FidelityVerifier(
            policy_store, context_manager, wal=wal,
        )
        return policy_store, wal, context_manager, fidelity_verifier

    def _load_wal(self) -> WAL:
        """Load WAL only (no PolicyStore / ContextManager)."""
        wal_path = self._wal_path()
        wal_path.parent.mkdir(parents=True, exist_ok=True)
        return WAL(wal_path=wal_path, session_id=self.agent)

    def _load_store(self) -> PolicyStore:
        """Load PolicyStore only."""
        return PolicyStore(store_path=self._store_path())

    # -- Quarantine reconstruction ------------------------------------------

    @staticmethod
    def _reconstruct_quarantine(wal: WAL) -> QuarantineManager:
        """Rebuild quarantine state from WAL events.

        Reads ``QUARANTINED`` and ``QUARANTINE_RELEASED`` events and replays
        them into a fresh ``QuarantineManager``.

        .. note::
           ``quarantine-add`` is deferred to a future phase.  This method is
           read-only — it does not write to the WAL.
        """
        # wal=None so the reconstruction itself doesn't log again.
        qm = QuarantineManager(min_delay=300, wal=None)

        quarantined_events = wal.read_by_type("QUARANTINED")
        released_events = wal.read_by_type("QUARANTINE_RELEASED")
        released_ids = {
            e.payload.get("item_id") for e in released_events
        }

        for event in quarantined_events:
            p = event.payload
            item_id = p.get("item_id", "")
            if not item_id:
                continue
            if item_id not in released_ids:
                qm.quarantine(
                    item_id=item_id,
                    surface=p.get("surface", "unknown"),
                    original_labels=p.get("original_labels", []),
                    reason=p.get("reason", ""),
                )

        # Mark released items (so list_all shows them correctly).
        for event in released_events:
            item_id = event.payload.get("item_id", "")
            if item_id:
                try:
                    qm.release(item_id)
                except Exception:
                    pass  # Already released or not found — skip.

        return qm

    # -- Command implementations --------------------------------------------

    def init(self) -> dict:
        keel_path = Path(self.keel_dir)
        created: list[str] = []

        # Create keel directory.
        keel_path.mkdir(parents=True, exist_ok=True)

        # Create WAL directory.
        wal_dir = keel_path / "wal"
        wal_dir.mkdir(exist_ok=True)
        created.append(str(wal_dir))

        # Create store.json with defaults if it doesn't already exist.
        store_path = self._store_path()
        if not store_path.exists():
            ps = PolicyStore(store_path=store_path)
            ps.save()
            created.append(str(store_path))

        return {
            "initialized": True,
            "keel_dir": str(keel_path),
            "agent": self.agent,
            "created": created,
        }

    def check_policy(self, action_spec: dict) -> dict:
        ps, _wal, _cm, fv = self._load_components()
        action = ActionSpec.from_dict(action_spec)
        validator = Validator(ps, fv)
        result = validator.validate(action)
        return result.to_dict()

    def wal_append(self, event_type: str, payload: dict) -> dict:
        wal = self._load_wal()
        event = wal.log(event_type, payload)
        return event.to_dict()

    def wal_query(
        self,
        event_type: str | None = None,
        since: str | None = None,
        last: int | None = None,
    ) -> dict:
        wal = self._load_wal()

        # Validate ISO-8601 timestamp if provided.
        if since is not None:
            try:
                datetime.fromisoformat(since.replace("Z", "+00:00"))
            except (ValueError, AttributeError) as exc:
                raise ValueError(
                    f"Invalid ISO-8601 timestamp for --since: {since!r}"
                ) from exc

        if event_type:
            events = wal.read_by_type(event_type)
        elif since:
            events = wal.read_since(since)
        else:
            events = wal.read_all()

        if last is not None and last > 0:
            events = events[-last:]

        return {
            "events": [e.to_dict() for e in events],
            "count": len(events),
        }

    def verify_chain(self) -> dict:
        wal = self._load_wal()
        chain_valid = wal.verify_chain()
        return {
            "chain_valid": chain_valid,
            "event_count": wal.event_count,
            "last_hash": wal.last_hash,
        }

    def status(self) -> dict:
        ps, wal, cm, _fv = self._load_components()
        return {
            "policy_count": len(ps),
            "tier0_count": len(ps.get_tier0_policies()),
            "snapshot_hash": ps.get_snapshot_hash(),
            "wal_event_count": wal.event_count,
            "chain_valid": wal.verify_chain(),
            "context_pressure": cm.get_pressure(),
            "context_usage": cm.get_current_usage(),
            "agent": self.agent,
        }

    def fidelity(self) -> dict:
        """Run fidelity verification.

        Side-effect: the ``FidelityVerifier.verify()`` method appends a
        ``FIDELITY_CHECK`` event to the WAL when a WAL instance is provided.
        """
        _ps, _wal, _cm, fv = self._load_components()
        result = fv.verify()
        return result.to_dict()

    def policies(
        self,
        scope: str | None = None,
        show_inactive: bool = False,
    ) -> dict:
        ps = self._load_store()
        active_filter = None if show_inactive else True
        policy_list = ps.list_policies(scope=scope, active=active_filter)
        return {
            "policies": [p.to_dict() for p in policy_list],
            "count": len(policy_list),
            "snapshot_hash": ps.get_snapshot_hash(),
        }

    def add_policy(
        self,
        content: str,
        scope: str = "global",
        policy_type: str = "constraint",
        priority: int = 0,
    ) -> dict:
        # PolicyStore._auto_save() persists on mutation when store_path is set.
        # No explicit save() call needed.
        ps, wal, _cm, _fv = self._load_components()

        policy = ps.make_policy(
            content=content,
            scope=scope,
            policy_type=policy_type,
            priority=priority,
        )
        ps.add_policy(policy)  # auto-saves

        wal.log(WALEventType.POLICY_ADDED.value, {
            "policy_id": policy.id,
            "content": content,
            "scope": scope,
            "type": policy_type,
            "priority": priority,
        })

        return policy.to_dict()

    def remove_policy(self, policy_id: str) -> dict:
        # PolicyStore._auto_save() persists on mutation when store_path is set.
        # No explicit save() call needed.
        ps, wal, _cm, _fv = self._load_components()

        ps.deactivate_policy(policy_id)  # auto-saves; raises KeyError if missing

        wal.log(WALEventType.POLICY_DEACTIVATED.value, {
            "policy_id": policy_id,
        })

        return {
            "deactivated": True,
            "policy_id": policy_id,
        }

    def quarantine(self) -> dict:
        """List quarantine records reconstructed from WAL.

        .. note::
           ``quarantine-add`` is deferred to a future phase.  This command is
           read-only — state is derived from ``QUARANTINED`` /
           ``QUARANTINE_RELEASED`` WAL events appended by agents.
        """
        wal = self._load_wal()
        qm = self._reconstruct_quarantine(wal)
        return {
            "items": [r.to_dict() for r in qm.list_all()],
            "active_count": qm.active_count,
        }

    def restore(self, item_id: str) -> dict:
        wal = self._load_wal()
        qm = self._reconstruct_quarantine(wal)
        record = qm.release(item_id)
        if record is None:
            raise ValueError(
                f"Cannot restore '{item_id}': not found or not active in quarantine"
            )

        # Persist the release into WAL so future reconstructions see it.
        wal.log("QUARANTINE_RELEASED", {
            "item_id": item_id,
            "original_labels": record.original_labels,
        })

        return {
            "released": True,
            "item_id": item_id,
            "original_labels": record.original_labels,
        }
