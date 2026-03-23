"""
Keel Fidelity Verifier

The detection layer. It answers one question: is the structural core
still intact?

Runs periodically and mandatorily before any high-risk action.
A fidelity check failure is a HARD STOP, not a warning. If Tier 0
integrity cannot be verified, no actions proceed.

Verification checks:
1. Hash check: SHA-256 of injected Tier 0 matches PolicyStore hash
2. Constraint presence: all active Tier 0 constraints present in assembled prompt
3. Consistency check: Tier 1 summaries don't contradict Tier 0 constraints
4. Action compliance: would a proposed action violate any Tier 0 constraint

v0.1 consistency checking is keyword-based.
v0.2 will add LLM-based semantic consistency.
"""

from __future__ import annotations

import re
from typing import Dict, List, Optional

from keel.core.schemas import (
    ActionSpec,
    AssembledPrompt,
    FidelityResult,
    Tier,
    ValidationResult,
    WALEventType,
    _now,
)
from keel.core.policy_store import PolicyStore
from keel.core.context_manager import ContextManager


# ---------------------------------------------------------------------------
# Contradiction detection (v0.1: keyword-based)
# ---------------------------------------------------------------------------

# Maps constraint keywords to action keywords that contradict them.
# If a Tier 0 constraint contains a key phrase and a Tier 1 record
# contains its contradiction, that's a consistency issue.
_CONTRADICTION_PAIRS: List[Dict[str, List[str]]] = [
    {
        "constraint_patterns": [r"\bnever\s+delete\b", r"\bdo\s+not\s+delete\b", r"\bdon't\s+delete\b"],
        "contradiction_patterns": [r"\bdeleted\b", r"\bremoved\b", r"\bpurged\b"],
    },
    {
        "constraint_patterns": [r"\bnever\s+send\b", r"\bdo\s+not\s+send\b"],
        "contradiction_patterns": [r"\bsent\b", r"\bdelivered\b", r"\bemailed\b"],
    },
    {
        "constraint_patterns": [r"\bnever\s+modify\b", r"\bdo\s+not\s+modify\b", r"\bdo\s+not\s+change\b"],
        "contradiction_patterns": [r"\bmodified\b", r"\bchanged\b", r"\bupdated\b", r"\bedited\b"],
    },
    {
        "constraint_patterns": [r"\brequire.*approval\b", r"\brequire.*confirmation\b"],
        "contradiction_patterns": [r"\bautomatically\b", r"\bwithout\s+approval\b", r"\bwithout\s+confirmation\b"],
    },
]

# Maps action types to constraint keywords that would prohibit them.
_ACTION_CONSTRAINT_MAP: Dict[str, List[str]] = {
    "delete_hard": ["never delete", "do not delete", "don't delete", "no deletion", "no irreversible"],
    "delete_soft": ["never delete", "do not delete", "don't delete", "no deletion"],
    "send": ["never send", "do not send", "don't send"],
    "modify": ["never modify", "do not modify", "don't modify", "do not change"],
    "merge": ["no irreversible"],
}


def _text_matches_any(text: str, patterns: List[str]) -> bool:
    """Check if text matches any of the regex patterns."""
    text_lower = text.lower()
    for pattern in patterns:
        if re.search(pattern, text_lower):
            return True
    return False


# ---------------------------------------------------------------------------
# Fidelity Verifier
# ---------------------------------------------------------------------------

class FidelityVerifier:
    """
    Verifies the structural persistence guarantee is intact.
    
    This is the detection half of Keel's prevent → detect → enforce
    architecture. It proves that:
    - Tier 0 content has not been tampered with or lost
    - All constraints are present in the assembled prompt
    - Tier 1 state doesn't contradict Tier 0 constraints
    - Proposed actions don't violate active policies
    
    A fidelity failure is a hard stop. The system cannot prove its own
    safety properties hold, and the only correct response is to halt.
    """

    def __init__(
        self,
        policy_store: PolicyStore,
        context_manager: ContextManager,
        wal=None,
    ):
        self._policy_store = policy_store
        self._context_manager = context_manager
        self._wal = wal

    # -----------------------------------------------------------------
    # Full verification
    # -----------------------------------------------------------------

    def verify(self, assembled: Optional[AssembledPrompt] = None) -> FidelityResult:
        """
        Run all fidelity checks. Returns a FidelityResult.
        
        If assembled is None, assembles a fresh prompt to verify against.
        """
        if assembled is None:
            assembled = self._context_manager.assemble_prompt()

        # 1. Hash check
        tier0_hash_ok, expected_hash, actual_hash = self._check_tier0_hash(assembled)

        # 2. Constraint presence
        constraints_present, missing = self._check_constraint_presence(assembled)

        # 3. Tier 1 consistency
        consistency_ok, consistency_issues = self._check_tier1_consistency()

        # Overall pass: all three must pass
        passed = tier0_hash_ok and constraints_present and consistency_ok

        result = FidelityResult(
            passed=passed,
            tier0_hash_ok=tier0_hash_ok,
            tier0_hash_expected=expected_hash,
            tier0_hash_actual=actual_hash,
            constraints_present=constraints_present,
            missing_constraints=missing,
            consistency_ok=consistency_ok,
            consistency_issues=consistency_issues,
        )

        # Log to WAL
        self._log_wal(WALEventType.FIDELITY_CHECK.value, {
            "passed": result.passed,
            "tier0_hash_ok": tier0_hash_ok,
            "constraints_present": constraints_present,
            "consistency_ok": consistency_ok,
            "missing_constraints": missing,
            "consistency_issues": consistency_issues,
        })

        return result

    # -----------------------------------------------------------------
    # Individual checks
    # -----------------------------------------------------------------

    def check_tier0_hash(self, assembled: Optional[AssembledPrompt] = None) -> bool:
        """
        Quick hash check: does the assembled Tier 0 hash match the PolicyStore?
        
        This is the fast path. If it passes, Tier 0 is structurally intact.
        """
        if assembled is None:
            assembled = self._context_manager.assemble_prompt()
        ok, _, _ = self._check_tier0_hash(assembled)
        return ok

    def _check_tier0_hash(self, assembled: AssembledPrompt) -> tuple:
        """Returns (ok, expected_hash, actual_hash)."""
        expected = self._policy_store.get_snapshot_hash()
        actual = assembled.tier0_hash
        return (expected == actual, expected, actual)

    def check_constraint_presence(self, assembled: Optional[AssembledPrompt] = None) -> bool:
        """
        Paranoia check: every active Tier 0 constraint appears in the
        assembled prompt text.
        
        Even if the hash matches, verify each constraint is findable.
        """
        if assembled is None:
            assembled = self._context_manager.assemble_prompt()
        present, _ = self._check_constraint_presence(assembled)
        return present

    def _check_constraint_presence(self, assembled: AssembledPrompt) -> tuple:
        """Returns (all_present, list_of_missing_policy_ids)."""
        tier0_policies = self._policy_store.get_tier0_policies()
        prompt_text = assembled.tier0_text.lower()
        missing = []

        for policy in tier0_policies:
            # Check that the policy content appears in the Tier 0 section
            if policy.content.lower() not in prompt_text:
                missing.append(policy.id)

        return (len(missing) == 0, missing)

    def check_tier1_consistency(self) -> bool:
        """
        Check that Tier 1 records don't contradict Tier 0 constraints.
        
        v0.1: keyword-based contradiction detection.
        v0.2: LLM-based semantic consistency.
        """
        ok, _ = self._check_tier1_consistency()
        return ok

    def _check_tier1_consistency(self) -> tuple:
        """Returns (consistent, list_of_issues)."""
        tier0_policies = self._policy_store.get_tier0_policies()
        tier1_records = self._context_manager._get_active_tier1_records()

        if not tier0_policies or not tier1_records:
            return (True, [])

        issues = []

        for policy in tier0_policies:
            policy_text = policy.content.lower()

            for pair in _CONTRADICTION_PAIRS:
                # Does this policy match any constraint pattern?
                policy_matches = _text_matches_any(policy_text, pair["constraint_patterns"])
                if not policy_matches:
                    continue

                # Check each Tier 1 record for contradictions
                for record in tier1_records:
                    record_text = record.content.lower()
                    if _text_matches_any(record_text, pair["contradiction_patterns"]):
                        issues.append(
                            f"Tier 1 record '{record.id}' ({record.content[:60]}...) "
                            f"may contradict Tier 0 policy '{policy.id}' ({policy.content[:60]}...)"
                        )

        return (len(issues) == 0, issues)

    # -----------------------------------------------------------------
    # Action compliance
    # -----------------------------------------------------------------

    def check_action_compliance(self, action: ActionSpec) -> ValidationResult:
        """
        Check whether a proposed action would violate any Tier 0 constraint.
        
        This checks against the PolicyStore directly, NOT against context.
        Even if context has been compacted to nothing, the PolicyStore
        still has the full constraint set.
        """
        tier0_policies = self._policy_store.get_tier0_policies()
        violations = []

        # Check action type against constraint map
        action_type = action.action_type.lower()
        for mapped_type, constraint_keywords in _ACTION_CONSTRAINT_MAP.items():
            if mapped_type not in action_type:
                continue
            for policy in tier0_policies:
                policy_lower = policy.content.lower()
                for keyword in constraint_keywords:
                    if keyword in policy_lower:
                        violations.append(
                            f"Action '{action.action_type}' violates policy '{policy.id}': {policy.content}"
                        )

        # Check target-specific constraints
        for policy in tier0_policies:
            policy_lower = policy.content.lower()
            # Look for target-specific mentions in policy (e.g., "never touch emails from boss@")
            for target_id in action.target_ids:
                if target_id.lower() in policy_lower:
                    violations.append(
                        f"Action targets '{target_id}' which is referenced in policy '{policy.id}': {policy.content}"
                    )

        # Scope-based filtering
        #
        # Enforcement model:
        #   scope == 'global'     — advisory. The agent reads the content and follows
        #                           it as an instruction. Enforcement relies on
        #                           _ACTION_CONSTRAINT_MAP above, which covers the
        #                           hardcoded high-risk action types.
        #   scope != 'global'     — structural enforcement. Any active constraint
        #                           whose scope matches the action's surface blocks
        #                           the action unconditionally, regardless of action
        #                           type. The policy content is returned verbatim so
        #                           the agent and user can see which rule fired.
        #
        # This distinction keeps the five system-default global constraints advisory
        # while allowing user-defined surface constraints (financial, email,
        # filesystem, etc.) to act as hard blocks.
        action_scope = action.surface.lower()
        scoped_policies = self._policy_store.list_policies(
            scope=action_scope, active=True, priority=Tier.FROZEN.value
        )
        for policy in scoped_policies:
            if policy.scope.lower() == "global":
                # Global constraints: advisory only at this path.
                # Enforcement handled above via _ACTION_CONSTRAINT_MAP.
                policy_lower = policy.content.lower()
                for mapped_type, constraint_keywords in _ACTION_CONSTRAINT_MAP.items():
                    if mapped_type in action_type:
                        for keyword in constraint_keywords:
                            if keyword in policy_lower:
                                violations.append(
                                    f"Action '{action.action_type}' violates global policy "
                                    f"'{policy.id}': {policy.content}"
                                )
            else:
                # Non-global scoped constraint: structural enforcement.
                # Scope matched surface — block unconditionally.
                if policy.type == "constraint":
                    violations.append(
                        f"Action '{action.action_type}' on surface '{action_scope}' blocked by "
                        f"policy '{policy.id}': {policy.content}"
                    )

        # Deduplicate
        violations = list(dict.fromkeys(violations))

        passed = len(violations) == 0
        return ValidationResult(
            passed=passed,
            action_id=action.action_id,
            violations=violations,
            fidelity_ok=True,  # Compliance check doesn't imply fidelity check
            reasons=violations if not passed else [],
        )

    # -----------------------------------------------------------------
    # Combined: fidelity + compliance
    # -----------------------------------------------------------------

    def verify_and_check_action(self, action: ActionSpec) -> ValidationResult:
        """
        Full verification before an action: fidelity check + action compliance.
        
        If fidelity fails, the action is blocked regardless of compliance.
        This is the hard stop.
        """
        # Fidelity first
        fidelity = self.verify()
        if not fidelity.passed:
            return ValidationResult(
                passed=False,
                action_id=action.action_id,
                risk_level=3,  # Maximum risk
                violations=[
                    f"FIDELITY CHECK FAILED: {'; '.join(fidelity.missing_constraints + fidelity.consistency_issues)}"
                ],
                fidelity_ok=False,
                reasons=["Tier 0 integrity cannot be verified. Hard stop."],
            )

        # Then compliance
        compliance = self.check_action_compliance(action)
        compliance.fidelity_ok = True
        return compliance

    # -----------------------------------------------------------------
    # WAL integration
    # -----------------------------------------------------------------

    def _log_wal(self, event_type: str, payload: dict):
        """Log to WAL if available."""
        if self._wal:
            self._wal.log(event_type, payload)
