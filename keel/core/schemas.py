"""
Keel Core Schemas

Canonical data structures for the structural persistence guarantee.
Every component in Keel speaks this language. All types are JSON-serialisable,
hashable, and round-trip through dict representation.

These schemas are the contract. Change them deliberately.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class PolicyScope(str, Enum):
    GLOBAL = "global"
    # Surface-specific scopes added as adapters are implemented
    GMAIL = "gmail"
    GCAL = "gcal"
    GITHUB = "github"
    FILESYSTEM = "filesystem"


class PolicyType(str, Enum):
    CONSTRAINT = "constraint"
    PERMISSION = "permission"
    LIMIT = "limit"
    PREFERENCE = "preference"


class PolicySource(str, Enum):
    USER_EXPLICIT = "user_explicit"
    USER_INFERRED = "user_inferred"
    SYSTEM_DEFAULT = "system_default"


class Tier(int, Enum):
    FROZEN = 0      # Never compacted. The keel.
    COARSE = 1      # Summarised, structure-preserved.
    FINE = 2        # Aggressively compactable.


class Reversibility(str, Enum):
    REVERSIBLE = "reversible"
    REVERSIBLE_WITHIN_WINDOW = "reversible_within_window"
    IRREVERSIBLE = "irreversible"


class WALEventType(str, Enum):
    PROPOSED = "PROPOSED"
    VALIDATED = "VALIDATED"
    FIDELITY_CHECK = "FIDELITY_CHECK"
    APPROVED = "APPROVED"
    APPROVAL_REJECTED = "APPROVAL_REJECTED"
    EXEC_STARTED = "EXEC_STARTED"
    EXEC_RESULT = "EXEC_RESULT"
    ROLLBACK = "ROLLBACK"
    CONTEXT_COMPACTION = "CONTEXT_COMPACTION"
    CONSTRAINT_PROMOTED = "CONSTRAINT_PROMOTED"
    CONSTRAINT_DEMOTED = "CONSTRAINT_DEMOTED"
    POLICY_ADDED = "POLICY_ADDED"
    POLICY_DEACTIVATED = "POLICY_DEACTIVATED"


# ---------------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------------

def _now() -> str:
    """ISO 8601 timestamp in UTC."""
    return datetime.now(timezone.utc).isoformat()


def canonical_hash(obj: dict) -> str:
    """SHA-256 of deterministically serialised dict."""
    canonical = json.dumps(obj, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Policy
# ---------------------------------------------------------------------------

@dataclass
class Policy:
    """
    A single policy in the Keel policy store.
    
    Tier 0 policies are the structural core — the keel.
    They live outside the context window and are injected verbatim
    into every LLM call.
    """
    id: str
    scope: str                      # PolicyScope value or surface name
    type: str                       # PolicyType value
    priority: int                   # Tier: 0 = frozen, 1 = coarse, 2 = fine
    content: str                    # The actual instruction/constraint text
    source: str                     # PolicySource value
    active: bool = True
    created_at: str = field(default_factory=_now)
    deactivated_at: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> Policy:
        return cls(**d)

    def canonical_dict(self) -> dict:
        """Fields that contribute to identity hashing. Excludes timestamps and metadata."""
        return {
            "id": self.id,
            "scope": self.scope,
            "type": self.type,
            "priority": self.priority,
            "content": self.content,
            "source": self.source,
            "active": self.active,
        }


# ---------------------------------------------------------------------------
# ActionSpec
# ---------------------------------------------------------------------------

@dataclass
class ActionSpec:
    """
    A proposed action. The LLM generates these; the Validator checks them
    against the PolicyStore (not against context).
    """
    surface: str                            # "gmail", "gcal", "none" (planning mode), etc.
    action_type: str                        # "label_add", "archive", "delete_soft", etc.
    target_ids: List[str]                   # message IDs, event IDs, etc.
    params: Dict[str, Any] = field(default_factory=dict)
    preconditions: List[str] = field(default_factory=list)
    expected_effect: str = ""
    reversibility: str = Reversibility.REVERSIBLE.value
    undo_plan: str = ""
    rationale: str = ""
    risk_level: int = 0                     # Computed by Validator, not by LLM
    action_id: str = ""                     # Set by compute_action_id()

    def __post_init__(self):
        if not self.action_id:
            self.action_id = self.compute_action_id()

    def compute_action_id(self) -> str:
        """Stable hash of the canonical action spec."""
        spec = {
            "surface": self.surface,
            "action_type": self.action_type,
            "target_ids": sorted(self.target_ids),
            "params": self.params,
        }
        return canonical_hash(spec)

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> ActionSpec:
        return cls(**d)


# ---------------------------------------------------------------------------
# Receipt
# ---------------------------------------------------------------------------

@dataclass
class Receipt:
    """
    Structured approval bundle. Contains everything needed to verify
    that an action was proposed, validated, and approved under known
    policy conditions.
    
    The context_fidelity_hash proves what Tier 0 policies were active
    at the time this receipt was generated.
    """
    batch_id: str
    policy_snapshot_hash: str               # SHA-256 of PolicyStore at generation time
    context_fidelity_hash: str              # SHA-256 of Tier 0 content at generation time
    actions: List[ActionSpec]
    human_summary: str = ""
    risk_summary: str = ""
    caps_applied: Dict[str, Any] = field(default_factory=dict)
    approval_instructions: str = ""         # Exact syntax the user must provide
    created_at: str = field(default_factory=_now)

    @property
    def action_ids(self) -> List[str]:
        return [a.action_id for a in self.actions]

    def to_dict(self) -> dict:
        d = asdict(self)
        return d

    @classmethod
    def from_dict(cls, d: dict) -> Receipt:
        d = dict(d)
        d["actions"] = [ActionSpec.from_dict(a) for a in d.get("actions", [])]
        return cls(**d)


# ---------------------------------------------------------------------------
# WAL Event
# ---------------------------------------------------------------------------

@dataclass
class WALEvent:
    """
    Write-ahead log event. Every state change in Keel produces one.
    
    Hash-chained: each event includes the hash of the previous event,
    matching the provenance chain pattern.
    """
    event_type: str                         # WALEventType value
    payload: Dict[str, Any]
    timestamp: str = field(default_factory=_now)
    session_id: str = ""
    prev_hash: str = ""                     # SHA-256 of previous WAL event
    event_hash: str = ""                    # Computed after creation

    def compute_hash(self) -> str:
        """Hash this event including the previous hash (chain link)."""
        hashable = {
            "event_type": self.event_type,
            "payload": self.payload,
            "timestamp": self.timestamp,
            "session_id": self.session_id,
            "prev_hash": self.prev_hash,
        }
        return canonical_hash(hashable)

    def __post_init__(self):
        if not self.event_hash:
            self.event_hash = self.compute_hash()

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> WALEvent:
        return cls(**d)


# ---------------------------------------------------------------------------
# Conversation Turn (Tier 2 content unit)
# ---------------------------------------------------------------------------

@dataclass
class ConversationTurn:
    """A single turn in the conversation history (Tier 2)."""
    turn_number: int
    role: str                               # "user", "assistant", "system"
    content: str
    timestamp: str = field(default_factory=_now)
    has_constraint: bool = False             # Flagged during ingestion if constraint-like language detected
    preserved: bool = False                  # If True, survives Tier 2 compaction
    token_estimate: int = 0

    def __post_init__(self):
        if self.token_estimate == 0:
            self.token_estimate = max(1, len(self.content) // 4)

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> ConversationTurn:
        return cls(**d)


# ---------------------------------------------------------------------------
# Assembled Prompt (output of Context Manager)
# ---------------------------------------------------------------------------

@dataclass
class AssembledPrompt:
    """
    The structured output of the Context Manager's assembly step.
    
    Not a string — tracks which tokens came from which tier.
    This allows fidelity verification to confirm Tier 0 is present
    and to attribute token usage per tier.
    """
    tier0_text: str                         # Verbatim from PolicyStore — the keel
    tier0_hash: str                         # SHA-256 of tier0_text
    tier1_text: str                         # Structured Tier 1 summaries
    tier1_record_count: int = 0
    tier2_text: str = ""                    # Conversation history
    tier2_turn_count: int = 0
    user_message: str = ""                  # Current user message
    total_tokens: int = 0
    tier0_tokens: int = 0
    tier1_tokens: int = 0
    tier2_tokens: int = 0
    user_tokens: int = 0

    def to_prompt_string(self) -> str:
        """Flatten to a single string for LLM consumption."""
        parts = [self.tier0_text]
        if self.tier1_text:
            parts.append(self.tier1_text)
        if self.tier2_text:
            parts.append(self.tier2_text)
        if self.user_message:
            parts.append(self.user_message)
        return "\n\n".join(parts)

    def to_dict(self) -> dict:
        return asdict(self)


# ---------------------------------------------------------------------------
# Tier Record (for Tier 1 structured context)
# ---------------------------------------------------------------------------

@dataclass
class TierRecord:
    """
    A structured record in Tier 1 context. Not prose — a key-value record
    with provenance tracking and policy references.
    """
    id: str
    tier: int                               # Should be 1 for Tier 1 records
    content: str
    category: str = ""                      # "decision", "fact", "preference", "tool_state", "task"
    source_turn: int = 0
    confidence: float = 1.0
    policy_references: List[str] = field(default_factory=list)  # Policy IDs this record relates to
    created_at: str = field(default_factory=_now)
    superseded: bool = False
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> TierRecord:
        return cls(**d)

    @property
    def references_tier0(self) -> bool:
        """Does this record reference any Tier 0 policy? If so, it must not be dropped during compaction."""
        return len(self.policy_references) > 0


# ---------------------------------------------------------------------------
# Compaction Event
# ---------------------------------------------------------------------------

@dataclass
class CompactionEvent:
    """Record of a context compaction operation."""
    tier: int
    tokens_before: int
    tokens_after: int
    items_dropped: int
    items_merged: int = 0
    fidelity_result_passed: bool = True
    timestamp: str = field(default_factory=_now)
    details: str = ""

    def to_dict(self) -> dict:
        return asdict(self)


# ---------------------------------------------------------------------------
# Fidelity Result
# ---------------------------------------------------------------------------

@dataclass
class FidelityResult:
    """Result of a fidelity verification check."""
    passed: bool
    tier0_hash_ok: bool
    tier0_hash_expected: str = ""
    tier0_hash_actual: str = ""
    constraints_present: bool = True
    missing_constraints: List[str] = field(default_factory=list)
    consistency_ok: bool = True
    consistency_issues: List[str] = field(default_factory=list)
    timestamp: str = field(default_factory=_now)

    def to_dict(self) -> dict:
        return asdict(self)


# ---------------------------------------------------------------------------
# Context Config
# ---------------------------------------------------------------------------

@dataclass
class ContextConfig:
    """Configuration for the Context Manager's token budgets and behaviour."""
    tier0_budget: int = 2000        # Hard cap, tokens
    tier1_budget: int = 4000        # Soft cap, tokens
    total_budget: int = 16000       # Total context window (conservative default)
    compaction_threshold: float = 0.85  # Trigger compaction at 85% usage
    constraint_keywords: List[str] = field(default_factory=lambda: [
        "never", "always", "must not", "do not", "don't",
        "forbidden", "prohibited", "required", "mandatory",
    ])

    @property
    def tier2_budget(self) -> int:
        return self.total_budget - self.tier0_budget - self.tier1_budget


# ---------------------------------------------------------------------------
# Approval Result
# ---------------------------------------------------------------------------

@dataclass
class ApprovalResult:
    """Result of parsing a user's approval attempt."""
    accepted: bool
    batch_id: str = ""
    approved_action_ids: List[str] = field(default_factory=list)
    rejection_reason: str = ""
    raw_input: str = ""

    def to_dict(self) -> dict:
        return asdict(self)


# ---------------------------------------------------------------------------
# Validation Result
# ---------------------------------------------------------------------------

@dataclass
class ValidationResult:
    """Result of validating an action against the PolicyStore."""
    passed: bool
    action_id: str = ""
    risk_level: int = 0
    violations: List[str] = field(default_factory=list)
    fidelity_ok: bool = True
    reasons: List[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return asdict(self)


# ---------------------------------------------------------------------------
# Telemetry Event (telemetry integration surface)
# ---------------------------------------------------------------------------

@dataclass
class KeelTelemetryEvent:
    """
    Structured telemetry event.

    Follows a standard envelope pattern for external consumption.
    Provenance hash is compatible with the provenance chain format.
    """
    event_type: str
    session_id: str
    payload: Dict[str, Any]
    timestamp: str = field(default_factory=_now)
    provenance_hash: str = ""               # SHA-256 chain hash

    def to_dict(self) -> dict:
        return asdict(self)
