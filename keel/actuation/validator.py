"""
Keel Validator

Validates proposed actions against the PolicyStore (never against context).
Computes deterministic risk scores. Enforces batch size limits.

The Validator is the enforcement point: it sits between the LLM's proposals
and the approval pipeline, catching policy violations that the LLM may have
generated because it lost the constraint from context, or because it's
ignoring it.

Risk scoring is a pure deterministic function — a lookup table, not a model.
Anyone can inspect it, anyone can audit it, and it does not drift.
"""

from __future__ import annotations

import re
from typing import List, Optional

from keel.core.schemas import (
    ActionSpec,
    Receipt,
    Reversibility,
    ValidationResult,
    WALEventType,
    _now,
)
from keel.core.policy_store import PolicyStore
from keel.core.fidelity import FidelityVerifier
try:
    from keel.integration.helmsman import HelmsmanBridge
except ImportError:
    HelmsmanBridge = None  # type: ignore[assignment,misc]


# ---------------------------------------------------------------------------
# Risk scoring constants
# ---------------------------------------------------------------------------

# Base risk by reversibility class
_REVERSIBILITY_RISK = {
    Reversibility.REVERSIBLE.value: 0,
    Reversibility.REVERSIBLE_WITHIN_WINDOW.value: 1,
    Reversibility.IRREVERSIBLE.value: 2,
}

# Risk bump by action type
_ACTION_TYPE_RISK = {
    "delete_hard": 3,
    "delete_soft": 1,
    "send": 2,
    "merge": 2,
    "archive": 0,
    "label_add": 0,
    "label_remove": 0,
    "move": 0,
    "quarantine": 0,
}

# Batch size thresholds for risk escalation
_BATCH_RISK_THRESHOLDS = [
    (10, 0),   # <=10 items: no additional risk
    (20, 1),   # 11-20 items: +1 risk
    (50, 2),   # 21-50 items: +2 risk
]

# Default max batch size (from default policies)
DEFAULT_MAX_BATCH_SIZE = 20


# ---------------------------------------------------------------------------
# Validator
# ---------------------------------------------------------------------------

class Validator:
    """
    Validates proposed actions against the PolicyStore and computes
    deterministic risk scores.
    
    The Validator ALWAYS checks the PolicyStore directly, never the context.
    Even if total context loss has occurred, the PolicyStore still holds
    the full constraint set.
    """

    def __init__(
        self,
        policy_store: PolicyStore,
        fidelity_verifier: FidelityVerifier,
        helmsman_bridge: Optional[HelmsmanBridge] = None,
        wal=None,
    ):
        self._policy_store = policy_store
        self._fidelity = fidelity_verifier
        self._helmsman = helmsman_bridge
        self._wal = wal

    # -----------------------------------------------------------------
    # Single action validation
    # -----------------------------------------------------------------

    def validate(self, action: ActionSpec) -> ValidationResult:
        """
        Validate a single action. Runs fidelity check, policy compliance,
        and risk scoring.
        
        If fidelity fails, the action is blocked at maximum risk regardless
        of its inherent properties.
        """
        # Fidelity + compliance via the verifier
        result = self._fidelity.verify_and_check_action(action)

        if not result.fidelity_ok:
            # Hard stop. Risk is maximum.
            result.risk_level = 3
            self._log_event(action, result)
            return result

        # Compute risk
        risk = self.compute_risk(action)
        result.risk_level = risk

        self._log_event(action, result)
        return result

    # -----------------------------------------------------------------
    # Batch validation
    # -----------------------------------------------------------------

    def validate_batch(self, actions: List[ActionSpec]) -> List[ValidationResult]:
        """
        Validate a batch of actions. Checks batch size limits first,
        then validates each action individually.
        """
        results = []

        # Check batch size against policy
        max_batch = self._get_max_batch_size()
        if len(actions) > max_batch:
            # Block the entire batch — too large
            for action in actions:
                results.append(ValidationResult(
                    passed=False,
                    action_id=action.action_id,
                    risk_level=3,
                    violations=[
                        f"Batch size {len(actions)} exceeds maximum {max_batch}. "
                        f"Split into smaller batches."
                    ],
                    fidelity_ok=True,
                    reasons=[f"Batch size limit: {max_batch}"],
                ))
            return results

        # Validate each action
        for action in actions:
            results.append(self.validate(action))

        return results

    def split_batch(self, actions: List[ActionSpec]) -> List[List[ActionSpec]]:
        """
        Split an oversized batch into compliant sub-batches.
        Each sub-batch is at most max_batch_size actions.
        """
        max_batch = self._get_max_batch_size()
        batches = []
        for i in range(0, len(actions), max_batch):
            batches.append(actions[i:i + max_batch])
        return batches

    # -----------------------------------------------------------------
    # Risk scoring — deterministic, auditable
    # -----------------------------------------------------------------

    def compute_risk(self, action: ActionSpec) -> int:
        """
        Compute risk level (0-3) for an action.
        
        Pure deterministic function. Inputs:
        - Action reversibility class
        - Action type
        - Number of targets
        - Helmsman signals (if available)
        
        This is a lookup table, not a neural network.
        """
        risk = 0

        # 1. Reversibility class
        risk += _REVERSIBILITY_RISK.get(action.reversibility, 1)

        # 2. Action type
        action_base = action.action_type.lower().split("_")[0] if "_" in action.action_type else action.action_type.lower()
        risk += _ACTION_TYPE_RISK.get(action.action_type.lower(), 0)
        # Also check base type for partial matches
        if action.action_type.lower() not in _ACTION_TYPE_RISK:
            for atype, arisk in _ACTION_TYPE_RISK.items():
                if atype in action.action_type.lower():
                    risk += arisk
                    break

        # 3. Target count
        n_targets = len(action.target_ids)
        for threshold, bump in _BATCH_RISK_THRESHOLDS:
            if n_targets <= threshold:
                risk += bump
                break
        else:
            risk += 3  # Over all thresholds

        # 4. Helmsman signals (optional)
        if self._helmsman:
            risk += self._helmsman.get_risk_adjustment()

        # Clamp to 0-3
        return min(max(risk, 0), 3)

    # -----------------------------------------------------------------
    # Helpers
    # -----------------------------------------------------------------

    def _get_max_batch_size(self) -> int:
        """
        Extract max batch size from active policies.
        Looks for a limit-type policy containing 'batch size'.
        Falls back to DEFAULT_MAX_BATCH_SIZE.
        """
        policies = self._policy_store.list_policies(active=True)
        for p in policies:
            if p.type == "limit" and "batch size" in p.content.lower():
                # Extract number from content like "Maximum batch size: 20 actions"
                match = re.search(r'(\d+)', p.content)
                if match:
                    return int(match.group(1))
        return DEFAULT_MAX_BATCH_SIZE

    def _log_event(self, action: ActionSpec, result: ValidationResult):
        """Log validation result to WAL."""
        if self._wal:
            self._wal.log(WALEventType.VALIDATED.value, {
                "action_id": action.action_id,
                "passed": result.passed,
                "risk_level": result.risk_level,
                "violations": result.violations,
                "fidelity_ok": result.fidelity_ok,
            })
