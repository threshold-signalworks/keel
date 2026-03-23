"""
Keel Context Manager

Manages the three-tier context structure. This is where the tiered
preservation model becomes real.

Tier 0: Verbatim from PolicyStore. Never compacted. The keel.
Tier 1: Structured records. Summarised under pressure, structure preserved.
Tier 2: Conversation history. Aggressively compactable.

The Context Manager assembles prompts, compacts under pressure, promotes
constraints to Tier 0, and demotes superseded records. Every compaction
event is logged to the WAL with full provenance.

Design properties:
- Tier 0 is ALWAYS injected verbatim from PolicyStore (never from memory)
- Compaction order: Tier 2 first, then Tier 1, never Tier 0
- Constraint-like language in user messages is flagged for preservation
- Compaction is deterministic: same state → same result
- v0.1 compaction: truncation of oldest turns (not summarisation)
"""

from __future__ import annotations

import re
import uuid
from typing import Dict, List, Optional, Tuple

from keel.core.schemas import (
    AssembledPrompt,
    CompactionEvent,
    ContextConfig,
    ConversationTurn,
    Policy,
    PolicySource,
    PolicyType,
    Tier,
    TierRecord,
    WALEventType,
    canonical_hash,
    _now,
)
from keel.core.policy_store import PolicyStore


# ---------------------------------------------------------------------------
# Token estimation
# ---------------------------------------------------------------------------

def _estimate_tokens(text: str) -> int:
    """Characters / 4 approximation. Exact counting via tiktoken is a v0.2 enhancement."""
    return max(1, len(text) // 4)


# ---------------------------------------------------------------------------
# Context Manager
# ---------------------------------------------------------------------------

class ContextManager:
    """
    Manages the three-tier context structure for Keel.
    
    The structural persistence guarantee lives here: Tier 0 content
    is always sourced fresh from the PolicyStore on every assembly,
    never from cached memory. Compaction can never touch Tier 0
    because Tier 0 was never part of the compactable state.
    """

    def __init__(
        self,
        policy_store: PolicyStore,
        config: Optional[ContextConfig] = None,
        wal=None,
    ):
        self._policy_store = policy_store
        self._config = config or ContextConfig()
        self._wal = wal

        # Tier 1: structured records
        self._tier1_records: Dict[str, TierRecord] = {}

        # Tier 2: conversation turns
        self._tier2_turns: List[ConversationTurn] = []
        self._turn_counter: int = 0

        # Compaction tracking
        self._compaction_count: int = 0

    # -----------------------------------------------------------------
    # Assembly — the core operation
    # -----------------------------------------------------------------

    def assemble_prompt(self, user_message: str = "") -> AssembledPrompt:
        """
        Assemble the full prompt from all three tiers plus the user message.
        
        Tier 0 is ALWAYS sourced fresh from the PolicyStore.
        This is the structural guarantee: even if everything else is lost,
        Tier 0 is intact because it was never in the compactable context.
        """
        # Tier 0: always fresh from PolicyStore
        tier0_text = self._policy_store.get_tier0_injection_text()
        tier0_hash = self._policy_store.get_snapshot_hash()
        tier0_tokens = _estimate_tokens(tier0_text)

        # Tier 1: structured records
        tier1_text = self._format_tier1()
        tier1_tokens = _estimate_tokens(tier1_text) if tier1_text else 0
        tier1_count = len(self._get_active_tier1_records())

        # Tier 2: conversation history
        tier2_text = self._format_tier2()
        tier2_tokens = _estimate_tokens(tier2_text) if tier2_text else 0
        tier2_count = len(self._tier2_turns)

        # User message
        user_tokens = _estimate_tokens(user_message) if user_message else 0

        total = tier0_tokens + tier1_tokens + tier2_tokens + user_tokens

        return AssembledPrompt(
            tier0_text=tier0_text,
            tier0_hash=tier0_hash,
            tier1_text=tier1_text,
            tier1_record_count=tier1_count,
            tier2_text=tier2_text,
            tier2_turn_count=tier2_count,
            user_message=user_message,
            total_tokens=total,
            tier0_tokens=tier0_tokens,
            tier1_tokens=tier1_tokens,
            tier2_tokens=tier2_tokens,
            user_tokens=user_tokens,
        )

    # -----------------------------------------------------------------
    # Tier 1 management
    # -----------------------------------------------------------------

    def add_to_tier1(self, record: TierRecord) -> str:
        """Add a structured record to Tier 1. Returns the record ID."""
        self._tier1_records[record.id] = record
        return record.id

    def get_tier1_record(self, record_id: str) -> TierRecord:
        """Get a Tier 1 record by ID."""
        if record_id not in self._tier1_records:
            raise KeyError(f"Tier 1 record not found: {record_id}")
        return self._tier1_records[record_id]

    def _get_active_tier1_records(self) -> List[TierRecord]:
        """All non-superseded Tier 1 records."""
        return [r for r in self._tier1_records.values() if not r.superseded]

    def make_tier1_record(
        self,
        content: str,
        category: str = "",
        policy_references: Optional[List[str]] = None,
    ) -> TierRecord:
        """Convenience factory for Tier 1 records."""
        return TierRecord(
            id=f"t1_{uuid.uuid4().hex[:12]}",
            tier=Tier.COARSE.value,
            content=content,
            category=category,
            source_turn=self._turn_counter,
            policy_references=policy_references or [],
        )

    # -----------------------------------------------------------------
    # Tier 2 management
    # -----------------------------------------------------------------

    def add_to_tier2(self, message: str, role: str, turn: Optional[int] = None) -> ConversationTurn:
        """
        Add a conversation turn to Tier 2.
        
        Scans user messages for constraint-like language and flags them
        for preservation during compaction.
        """
        self._turn_counter += 1
        turn_num = turn if turn is not None else self._turn_counter

        has_constraint = False
        if role == "user":
            has_constraint = self._detect_constraint_language(message)

        ct = ConversationTurn(
            turn_number=turn_num,
            role=role,
            content=message,
            has_constraint=has_constraint,
            preserved=has_constraint,  # Constraint turns are preserved by default
        )
        self._tier2_turns.append(ct)
        return ct

    def _detect_constraint_language(self, text: str) -> bool:
        """
        Check if text contains constraint-like language.
        
        This is the ingestion-time flag that marks turns for preservation
        during Tier 2 compaction and surfaces them for potential Tier 0
        promotion.
        """
        text_lower = text.lower()
        for keyword in self._config.constraint_keywords:
            # Match as whole word or at word boundary to reduce false positives
            pattern = r'\b' + re.escape(keyword) + r'\b'
            if re.search(pattern, text_lower):
                return True
        return False

    # -----------------------------------------------------------------
    # Promotion / Demotion
    # -----------------------------------------------------------------

    def promote_to_tier0(self, content: str, source: str = PolicySource.USER_EXPLICIT.value) -> Policy:
        """
        Promote content to Tier 0 by creating a new policy in the PolicyStore.
        
        This is how a user's in-conversation constraint becomes permanent:
        "never touch emails from legal" → new Tier 0 policy.
        
        Returns the created Policy. Raises ValueError if Tier 0 budget
        would be exceeded.
        """
        policy = self._policy_store.make_policy(
            content=content,
            source=source,
        )
        self._policy_store.add_policy(policy)

        self._log_wal(WALEventType.CONSTRAINT_PROMOTED.value, {
            "policy_id": policy.id,
            "from_tier": 2,
            "to_tier": 0,
            "content": content,
        })

        return policy

    def demote_to_tier2(self, tier1_record_id: str):
        """
        Mark a Tier 1 record as superseded. It remains in the record set
        but will be dropped during the next compaction cycle.
        
        Records that reference Tier 0 policies cannot be demoted.
        """
        record = self.get_tier1_record(tier1_record_id)
        if record.references_tier0:
            raise ValueError(
                f"Cannot demote Tier 1 record {tier1_record_id}: "
                f"it references Tier 0 policies {record.policy_references}"
            )
        record.superseded = True

        self._log_wal(WALEventType.CONSTRAINT_DEMOTED.value, {
            "record_id": tier1_record_id,
            "from_tier": 1,
            "to_tier": 2,
        })

    # -----------------------------------------------------------------
    # Compaction
    # -----------------------------------------------------------------

    def compact(self) -> List[CompactionEvent]:
        """
        Compact context to fit within budget.
        
        Order: Tier 2 first, then Tier 1 if needed. NEVER Tier 0.
        
        Returns a list of CompactionEvents (one per tier compacted).
        """
        events = []

        # Check if compaction is needed
        pressure = self.get_pressure()
        if pressure < self._config.compaction_threshold and self._get_total_tokens() <= self._config.total_budget:
            return events

        # Phase 1: compact Tier 2
        tier2_event = self._compact_tier2()
        if tier2_event:
            events.append(tier2_event)

        # Check again — Tier 2 compaction may have been sufficient
        pressure = self.get_pressure()
        if pressure < self._config.compaction_threshold and self._get_total_tokens() <= self._config.total_budget:
            return events

        # Phase 2: compact Tier 1 (only if Tier 2 is fully compacted)
        tier1_event = self._compact_tier1()
        if tier1_event:
            events.append(tier1_event)

        return events

    def _compact_tier2(self) -> Optional[CompactionEvent]:
        """
        Compact Tier 2: truncate oldest non-preserved turns.
        
        Preserved turns (those with constraint-like language) survive
        compaction. Everything else is fair game, oldest first.
        """
        if not self._tier2_turns:
            return None

        tokens_before = sum(t.token_estimate for t in self._tier2_turns)
        target_tokens = self._config.tier2_budget
        items_dropped = 0

        # Calculate how many tokens we need to drop to get below threshold
        current_total = self._get_total_tokens()
        # Target: get below compaction threshold (with headroom)
        target_total = int(self._config.total_budget * (self._config.compaction_threshold - 0.1))
        overage = current_total - target_total
        if overage <= 0:
            return None

        # Drop oldest non-preserved turns until we're below threshold
        new_turns = []
        tokens_dropped = 0
        # Sort: droppable turns first (oldest), then preserved turns
        droppable = [t for t in self._tier2_turns if not t.preserved]
        preserved = [t for t in self._tier2_turns if t.preserved]

        # Drop oldest droppable turns
        for turn in droppable:
            if tokens_dropped < overage:
                tokens_dropped += turn.token_estimate
                items_dropped += 1
            else:
                new_turns.append(turn)

        # Preserved turns always survive
        new_turns.extend(preserved)

        # Sort back into turn order
        new_turns.sort(key=lambda t: t.turn_number)
        self._tier2_turns = new_turns

        tokens_after = sum(t.token_estimate for t in self._tier2_turns)

        event = CompactionEvent(
            tier=Tier.FINE.value,
            tokens_before=tokens_before,
            tokens_after=tokens_after,
            items_dropped=items_dropped,
            details=f"Truncated {items_dropped} oldest non-preserved turns",
        )

        self._log_wal(WALEventType.CONTEXT_COMPACTION.value, event.to_dict())

        return event

    def _compact_tier1(self) -> Optional[CompactionEvent]:
        """
        Compact Tier 1: drop superseded records that don't reference Tier 0.
        
        Records with Tier 0 policy references are NEVER dropped.
        """
        active_records = self._get_active_tier1_records()
        if not active_records:
            return None

        tokens_before = sum(_estimate_tokens(r.content) for r in self._tier1_records.values() if not r.superseded)

        # Find droppable records: superseded AND no Tier 0 references
        droppable = [
            rid for rid, r in self._tier1_records.items()
            if r.superseded and not r.references_tier0
        ]

        items_dropped = len(droppable)
        if items_dropped == 0:
            # Nothing superseded to drop — try dropping oldest non-referencing records
            # only if we're still over budget
            non_ref = [
                (rid, r) for rid, r in self._tier1_records.items()
                if not r.references_tier0 and not r.superseded
            ]
            # Sort by source turn (oldest first)
            non_ref.sort(key=lambda x: x[1].source_turn)

            current_total = self._get_total_tokens()
            overage = current_total - self._config.total_budget
            tokens_freed = 0
            for rid, record in non_ref:
                if tokens_freed >= overage:
                    break
                tokens_freed += _estimate_tokens(record.content)
                droppable.append(rid)
                items_dropped += 1

        for rid in droppable:
            del self._tier1_records[rid]

        tokens_after = sum(_estimate_tokens(r.content) for r in self._tier1_records.values() if not r.superseded)

        event = CompactionEvent(
            tier=Tier.COARSE.value,
            tokens_before=tokens_before,
            tokens_after=tokens_after,
            items_dropped=items_dropped,
            details=f"Dropped {items_dropped} Tier 1 records (superseded or non-referencing)",
        )

        self._log_wal(WALEventType.CONTEXT_COMPACTION.value, event.to_dict())

        return event

    # -----------------------------------------------------------------
    # Pressure / Token accounting
    # -----------------------------------------------------------------

    def get_pressure(self) -> float:
        """Current window usage as fraction of total budget."""
        total = self._get_total_tokens()
        return total / self._config.total_budget if self._config.total_budget > 0 else 0.0

    def _get_total_tokens(self) -> int:
        """Total token usage across all tiers."""
        tier0 = _estimate_tokens(self._policy_store.get_tier0_injection_text())
        tier1 = sum(
            _estimate_tokens(r.content)
            for r in self._tier1_records.values()
            if not r.superseded
        )
        tier2 = sum(t.token_estimate for t in self._tier2_turns)
        return tier0 + tier1 + tier2

    def get_current_usage(self) -> Dict[str, int]:
        """Token usage per tier."""
        tier0 = _estimate_tokens(self._policy_store.get_tier0_injection_text())
        tier1 = sum(
            _estimate_tokens(r.content)
            for r in self._tier1_records.values()
            if not r.superseded
        )
        tier2 = sum(t.token_estimate for t in self._tier2_turns)
        return {
            "tier0": tier0,
            "tier1": tier1,
            "tier2": tier2,
            "total": tier0 + tier1 + tier2,
        }

    def get_tier_budgets(self) -> Dict[str, int]:
        """Configured token budgets per tier."""
        return {
            "tier0": self._config.tier0_budget,
            "tier1": self._config.tier1_budget,
            "tier2": self._config.tier2_budget,
            "total": self._config.total_budget,
        }

    # -----------------------------------------------------------------
    # Formatting
    # -----------------------------------------------------------------

    def _format_tier1(self) -> str:
        """Format active Tier 1 records as structured text."""
        records = self._get_active_tier1_records()
        if not records:
            return ""

        lines = ["[KEEL TIER 1 — OPERATIONAL STATE]", ""]
        for r in records:
            cat_tag = f"[{r.category.upper()}]" if r.category else ""
            ref_tag = f" (refs: {','.join(r.policy_references)})" if r.policy_references else ""
            lines.append(f"{cat_tag} {r.content}{ref_tag}")
        lines.append("[END TIER 1]")
        return "\n".join(lines)

    def _format_tier2(self) -> str:
        """Format Tier 2 conversation turns."""
        if not self._tier2_turns:
            return ""

        lines = []
        for turn in self._tier2_turns:
            role_label = turn.role.upper()
            lines.append(f"[{role_label}] {turn.content}")
        return "\n".join(lines)

    # -----------------------------------------------------------------
    # State inspection
    # -----------------------------------------------------------------

    @property
    def turn_count(self) -> int:
        return self._turn_counter

    @property
    def tier1_record_count(self) -> int:
        return len(self._get_active_tier1_records())

    @property
    def tier2_turn_count(self) -> int:
        return len(self._tier2_turns)

    @property
    def compaction_count(self) -> int:
        return self._compaction_count

    def get_constraint_flagged_turns(self) -> List[ConversationTurn]:
        """All turns flagged as containing constraint-like language."""
        return [t for t in self._tier2_turns if t.has_constraint]

    # -----------------------------------------------------------------
    # WAL integration
    # -----------------------------------------------------------------

    def _log_wal(self, event_type: str, payload: dict):
        """Log to WAL if available."""
        if self._wal:
            self._wal.log(event_type, payload)
