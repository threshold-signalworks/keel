"""
Keel Actuation Engine

Orchestrates the full pipeline:
  propose → validate → receipt → approve → execute → log

This is the top-level coordinator. It wires together the Validator,
ReceiptBuilder, ApprovalParser, and Adapter into the complete
actuation pipeline defined in the design document.
"""

from __future__ import annotations

from typing import Dict, List, Optional

from keel.core.schemas import (
    ActionSpec,
    ApprovalResult,
    Receipt,
    WALEventType,
    _now,
)
from keel.core.policy_store import PolicyStore
from keel.core.context_manager import ContextManager
from keel.core.fidelity import FidelityVerifier
from keel.actuation.validator import Validator
from keel.actuation.receipt_builder import ReceiptBuilder
from keel.actuation.approval_parser import ApprovalParser
from keel.actuation.wal import WAL
from keel.adapters.base import BaseAdapter, PlanningAdapter, ToolResult
try:
    from keel.integration.helmsman import HelmsmanBridge
except ImportError:
    HelmsmanBridge = None  # type: ignore[assignment,misc]
from keel.telemetry.events import KeelTelemetry


class ActuationEngine:
    """
    The complete Keel actuation pipeline.
    
    Lifecycle:
    1. propose() — LLM (or user) provides ActionSpecs
    2. Engine validates against PolicyStore, builds receipt
    3. User provides structured approval
    4. approve() — Engine parses approval, executes if accepted
    5. Everything logged to WAL, emitted to telemetry
    
    In planning mode (v0.1 default), step 4 simulates execution.
    """

    def __init__(
        self,
        policy_store: PolicyStore,
        context_manager: ContextManager,
        wal: Optional[WAL] = None,
        adapter: Optional[BaseAdapter] = None,
        helmsman: Optional[HelmsmanBridge] = None,
        telemetry: Optional[KeelTelemetry] = None,
    ):
        self._policy_store = policy_store
        self._context_manager = context_manager
        self._wal = wal or WAL(session_id="default")
        self._adapter = adapter or PlanningAdapter()
        self._telemetry = telemetry

        # Build the internal pipeline
        self._fidelity = FidelityVerifier(policy_store, context_manager, wal=self._wal)
        self._validator = Validator(
            policy_store, self._fidelity,
            helmsman_bridge=helmsman, wal=self._wal,
        )
        self._receipt_builder = ReceiptBuilder(
            policy_store, self._fidelity,
            self._validator, wal=self._wal,
        )
        self._approval_parser = ApprovalParser(wal=self._wal)

        # Pending receipt (only one at a time)
        self._pending_receipt: Optional[Receipt] = None

    # -----------------------------------------------------------------
    # Step 1: Propose actions
    # -----------------------------------------------------------------

    def propose(self, actions: List[ActionSpec]) -> Receipt:
        """
        Propose a batch of actions for approval.
        
        Validates each action against the PolicyStore, computes risk,
        builds a receipt with approval instructions.
        
        Returns the Receipt. The user must then call approve() with
        the exact approval syntax.
        """
        # If batch is oversized, build split receipts
        max_batch = self._validator._get_max_batch_size()
        if len(actions) > max_batch:
            # Return the first sub-batch receipt. Caller should use
            # propose_split() for full handling of oversized batches.
            batches = self._validator.split_batch(actions)
            receipt = self._receipt_builder.build(batches[0])
            receipt.caps_applied["total_actions"] = len(actions)
            receipt.caps_applied["batches_remaining"] = len(batches) - 1
        else:
            receipt = self._receipt_builder.build(actions)

        self._pending_receipt = receipt

        # Telemetry
        if self._telemetry:
            self._telemetry.approval_requested(
                receipt.batch_id,
                len(actions),
                receipt.risk_summary,
            )

        return receipt

    def propose_split(self, actions: List[ActionSpec]) -> List[Receipt]:
        """
        Propose an oversized batch by splitting into sub-batches.
        Each sub-batch gets its own receipt requiring separate approval.
        Returns all receipts. Only the first becomes the pending receipt.
        """
        receipts = self._receipt_builder.build_split(actions)
        if receipts:
            self._pending_receipt = receipts[0]
        return receipts

    # -----------------------------------------------------------------
    # Step 2: Approve
    # -----------------------------------------------------------------

    def approve(self, user_input: str) -> ApprovalResult:
        """
        Parse user approval input against the pending receipt.
        
        If accepted, executes the approved actions via the adapter.
        If rejected, returns the rejection reason with the correct syntax.
        """
        if self._pending_receipt is None:
            return ApprovalResult(
                accepted=False,
                rejection_reason="No pending receipt. Propose actions first.",
                raw_input=user_input,
            )

        result = self._approval_parser.parse(user_input, self._pending_receipt)

        if result.accepted:
            # Execute approved actions
            self._execute_approved(self._pending_receipt, result)
            self._pending_receipt = None
        else:
            # Log rejection
            if self._wal:
                self._wal.log(WALEventType.APPROVAL_REJECTED.value, {
                    "batch_id": self._pending_receipt.batch_id,
                    "reason": result.rejection_reason,
                    "raw_input": user_input[:200],
                })
            if self._telemetry:
                self._telemetry.approval_result(
                    self._pending_receipt.batch_id,
                    False,
                    result.rejection_reason,
                )

        return result

    # -----------------------------------------------------------------
    # Execution
    # -----------------------------------------------------------------

    def _execute_approved(self, receipt: Receipt, approval: ApprovalResult):
        """Execute approved actions via the adapter."""
        approved_short_ids = set(approval.approved_action_ids)

        for action in receipt.actions:
            short_id = action.action_id[:8]
            if short_id not in approved_short_ids and action.action_id not in approved_short_ids:
                continue

            # Log start
            if self._wal:
                self._wal.log(WALEventType.EXEC_STARTED.value, {
                    "action_id": action.action_id,
                    "batch_id": receipt.batch_id,
                })

            # Execute
            tool_result = self._adapter.execute(action)

            # Log result
            if self._wal:
                self._wal.log(WALEventType.EXEC_RESULT.value, {
                    "action_id": action.action_id,
                    "status": tool_result.status,
                    "details": tool_result.details,
                })

            # Update Tier 1 context with the result
            self._context_manager.add_to_tier1(
                self._context_manager.make_tier1_record(
                    f"{action.action_type} on {action.surface}: {tool_result.status} "
                    f"({len(action.target_ids)} targets)",
                    category="tool_state",
                )
            )

        # Telemetry
        if self._telemetry:
            self._telemetry.approval_result(receipt.batch_id, True)

    # -----------------------------------------------------------------
    # State
    # -----------------------------------------------------------------

    @property
    def pending_receipt(self) -> Optional[Receipt]:
        return self._pending_receipt

    @property
    def has_pending(self) -> bool:
        return self._pending_receipt is not None

    def cancel_pending(self):
        """Cancel the pending receipt without executing."""
        if self._pending_receipt and self._wal:
            self._wal.log(WALEventType.APPROVAL_REJECTED.value, {
                "batch_id": self._pending_receipt.batch_id,
                "reason": "Cancelled by user",
            })
        self._pending_receipt = None
