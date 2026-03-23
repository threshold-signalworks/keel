"""Abstract backend protocol for CLI commands.

Both ``LocalBackend`` and ``CloudClient`` extend this class.  All methods
return Python dicts matching the API response schema shapes (schema-equivalent).
"""

from __future__ import annotations


class Backend:
    """Backend protocol.  Constructor receives context; methods are stateless."""

    def __init__(self, keel_dir: str, agent: str) -> None:
        self.keel_dir = keel_dir
        self.agent = agent

    # -- Commands -----------------------------------------------------------

    def init(self) -> dict:
        """Initialise keel directory structure.

        Returns ``{initialized, keel_dir, agent, created}``.
        """
        raise NotImplementedError

    def check_policy(self, action_spec: dict) -> dict:
        """Validate an action against the policy store.

        Returns ``ValidationResult.to_dict()``.
        """
        raise NotImplementedError

    def wal_append(self, event_type: str, payload: dict) -> dict:
        """Append an event to the WAL.

        Returns ``WALEvent.to_dict()``.
        """
        raise NotImplementedError

    def wal_query(
        self,
        event_type: str | None = None,
        since: str | None = None,
        last: int | None = None,
    ) -> dict:
        """Query WAL events with optional filters.

        Returns ``{events: [...], count: int}``.
        """
        raise NotImplementedError

    def verify_chain(self) -> dict:
        """Verify WAL hash chain integrity.

        Returns ``{chain_valid, event_count, last_hash}``.
        """
        raise NotImplementedError

    def status(self) -> dict:
        """System status overview.

        Returns ``{policy_count, tier0_count, snapshot_hash,
        wal_event_count, chain_valid, context_pressure, context_usage, agent}``.
        """
        raise NotImplementedError

    def fidelity(self) -> dict:
        """Run fidelity verification.

        Returns ``FidelityResult.to_dict()``.
        """
        raise NotImplementedError

    def policies(
        self,
        scope: str | None = None,
        show_inactive: bool = False,
    ) -> dict:
        """List policies with optional filters.

        Returns ``{policies: [...], count, snapshot_hash}``.
        """
        raise NotImplementedError

    def add_policy(
        self,
        content: str,
        scope: str = "global",
        policy_type: str = "constraint",
        priority: int = 0,
    ) -> dict:
        """Add a new policy.

        Returns ``Policy.to_dict()``.
        """
        raise NotImplementedError

    def remove_policy(self, policy_id: str) -> dict:
        """Deactivate (soft-delete) a policy.

        Returns ``{deactivated: True, policy_id}``.
        """
        raise NotImplementedError

    def quarantine(self) -> dict:
        """List quarantine records reconstructed from WAL.

        Returns ``{items: [...], active_count}``.

        .. note::
           ``quarantine-add`` is deferred to a future phase.  This command is
           read-only — state is reconstructed from WAL events.
        """
        raise NotImplementedError

    def restore(self, item_id: str) -> dict:
        """Release an item from quarantine.

        Returns ``{released: True, item_id, original_labels}``.
        """
        raise NotImplementedError
