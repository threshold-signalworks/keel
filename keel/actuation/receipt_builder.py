"""
Keel Receipt Builder

Builds structured approval bundles (Receipts) from validated actions.
Each receipt includes the policy snapshot hash and context fidelity hash,
creating an audit trail proving what policies were active when the
actions were proposed.

Receipts generate the exact approval syntax the user must provide.
This is deliberately friction-ful — the friction is the safety feature.
"""

from __future__ import annotations

import uuid
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
from keel.actuation.validator import Validator


class ReceiptBuilder:
    """
    Builds Receipts from validated action batches.
    
    Each receipt is a self-contained approval bundle: it records
    the exact policy state, fidelity state, proposed actions, risk
    summary, and the exact syntax required for approval.
    """

    def __init__(
        self,
        policy_store: PolicyStore,
        fidelity_verifier: FidelityVerifier,
        validator: Validator,
        wal=None,
    ):
        self._policy_store = policy_store
        self._fidelity = fidelity_verifier
        self._validator = validator
        self._wal = wal

    def build(self, actions: List[ActionSpec]) -> Receipt:
        """
        Build a receipt for a batch of actions.
        
        Validates all actions, computes risk, generates approval syntax.
        If any action fails validation, it's still included in the receipt
        but marked with its violations — the user sees everything.
        """
        # Generate batch ID
        batch_id = f"B{uuid.uuid4().hex[:8].upper()}"

        # Validate each action
        results = self._validator.validate_batch(actions)

        # Annotate actions with computed risk levels
        for action, result in zip(actions, results):
            action.risk_level = result.risk_level

        # Policy and fidelity snapshots
        policy_hash = self._policy_store.get_snapshot_hash()
        fidelity_result = self._fidelity.verify()
        fidelity_hash = fidelity_result.tier0_hash_actual

        # Build summaries
        human_summary = self._build_human_summary(actions, results)
        risk_summary = self._build_risk_summary(actions, results)

        # Build approval instructions
        valid_actions = [
            a for a, r in zip(actions, results) if r.passed
        ]
        blocked_actions = [
            (a, r) for a, r in zip(actions, results) if not r.passed
        ]

        approval_instructions = self._build_approval_instructions(
            batch_id, valid_actions, blocked_actions
        )

        # Caps applied
        max_batch = self._validator._get_max_batch_size()
        caps = {
            "max_batch_size": max_batch,
            "batch_size": len(actions),
            "actions_approved": len(valid_actions),
            "actions_blocked": len(blocked_actions),
        }

        receipt = Receipt(
            batch_id=batch_id,
            policy_snapshot_hash=policy_hash,
            context_fidelity_hash=fidelity_hash,
            actions=actions,
            human_summary=human_summary,
            risk_summary=risk_summary,
            caps_applied=caps,
            approval_instructions=approval_instructions,
        )

        # Log
        if self._wal:
            self._wal.log(WALEventType.PROPOSED.value, {
                "batch_id": batch_id,
                "action_count": len(actions),
                "approved_count": len(valid_actions),
                "blocked_count": len(blocked_actions),
                "policy_snapshot_hash": policy_hash,
                "fidelity_hash": fidelity_hash,
            })

        return receipt

    def build_split(self, actions: List[ActionSpec]) -> List[Receipt]:
        """
        Build receipts for an oversized batch by splitting it first.
        Each sub-batch gets its own receipt requiring separate approval.
        """
        batches = self._validator.split_batch(actions)
        return [self.build(batch) for batch in batches]

    # -----------------------------------------------------------------
    # Summary builders
    # -----------------------------------------------------------------

    def _build_human_summary(
        self, actions: List[ActionSpec], results: List[ValidationResult]
    ) -> str:
        """Build a human-readable summary of the proposed actions."""
        lines = []
        for i, (action, result) in enumerate(zip(actions, results), 1):
            status = "OK" if result.passed else "BLOCKED"
            risk_label = ["LOW", "MEDIUM", "HIGH", "CRITICAL"][min(action.risk_level, 3)]
            rev_label = action.reversibility.replace("_", " ").title()

            line = (
                f"  {i}. [{status}] [{risk_label}] "
                f"{action.action_type} on {action.surface} "
                f"({len(action.target_ids)} target{'s' if len(action.target_ids) != 1 else ''}) "
                f"[{rev_label}]"
            )
            if action.rationale:
                line += f"\n     Reason: {action.rationale}"
            if not result.passed:
                line += f"\n     Violations: {'; '.join(result.violations[:3])}"

            lines.append(line)

        return "\n".join(lines)

    def _build_risk_summary(
        self, actions: List[ActionSpec], results: List[ValidationResult]
    ) -> str:
        """Build a risk summary for the batch."""
        total = len(actions)
        blocked = sum(1 for r in results if not r.passed)
        risk_counts = {0: 0, 1: 0, 2: 0, 3: 0}
        for a in actions:
            risk_counts[min(a.risk_level, 3)] += 1

        irreversible = sum(
            1 for a in actions
            if a.reversibility == Reversibility.IRREVERSIBLE.value
        )

        parts = [
            f"{total} actions proposed",
            f"{blocked} blocked by policy",
            f"Risk: {risk_counts[0]} low, {risk_counts[1]} medium, "
            f"{risk_counts[2]} high, {risk_counts[3]} critical",
        ]
        if irreversible:
            parts.append(f"{irreversible} IRREVERSIBLE")

        return " | ".join(parts)

    def _build_approval_instructions(
        self,
        batch_id: str,
        valid_actions: List[ActionSpec],
        blocked_actions: list,
    ) -> str:
        """
        Build the exact approval syntax the user must provide.
        
        This is deliberately strict. "yes" and "go ahead" will be rejected.
        The friction is the safety feature.
        """
        lines = []

        if not valid_actions:
            lines.append("NO ACTIONS AVAILABLE FOR APPROVAL.")
            lines.append("All proposed actions were blocked by active policies.")
            if blocked_actions:
                lines.append(f"Blocked actions: {len(blocked_actions)}")
                for action, result in blocked_actions[:5]:
                    lines.append(f"  - {action.action_type}: {'; '.join(result.violations[:1])}")
            return "\n".join(lines)

        # Build the approval command
        action_ids_short = [a.action_id[:8] for a in valid_actions]
        action_ids_str = ",".join(action_ids_short)

        lines.append(f"To approve, type exactly:")
        lines.append(f"  APPROVE batch {batch_id} actions {action_ids_str}")
        lines.append("")

        if blocked_actions:
            lines.append(
                f"Note: {len(blocked_actions)} action(s) were blocked by policy "
                f"and cannot be approved."
            )

        lines.append("Any other response will be rejected.")

        return "\n".join(lines)
