"""
Keel Tool Adapters

Base adapter interface and the planning mode (no-op) adapter.

Each adapter declares:
- Action types supported
- Reversibility level per action type
- Preview/diff capability
- Idempotency strategy

The planning adapter produces receipts but never executes.
This is the v0.1 default — tool execution comes in Phase 3.
"""

from __future__ import annotations

from dataclasses import dataclass, field, asdict
from typing import Any, Dict, List

from keel.core.schemas import ActionSpec, _now


@dataclass
class ToolResult:
    """Result of a tool adapter execution."""
    action_id: str = ""
    status: str = ""            # "success", "error", "simulated", "blocked"
    details: str = ""
    tool_receipt: Dict[str, Any] = field(default_factory=dict)
    timestamp: str = field(default_factory=_now)

    def to_dict(self) -> dict:
        return asdict(self)


class BaseAdapter:
    """
    Base class for tool adapters.
    
    Each adapter declares what it supports and how to execute actions.
    """

    @property
    def surface_name(self) -> str:
        """The surface this adapter serves (e.g., 'gmail', 'gcal')."""
        raise NotImplementedError

    @property
    def supported_actions(self) -> Dict[str, str]:
        """Map of action_type -> reversibility level."""
        raise NotImplementedError

    def execute(self, action: ActionSpec) -> ToolResult:
        """Execute an action. Returns a ToolResult."""
        raise NotImplementedError

    def preview(self, action: ActionSpec) -> str:
        """Generate a preview/diff of what the action would do."""
        return f"[{self.surface_name}] {action.action_type} on {len(action.target_ids)} target(s)"

    def supports(self, action_type: str) -> bool:
        """Does this adapter support the given action type?"""
        return action_type in self.supported_actions


class PlanningAdapter(BaseAdapter):
    """
    No-op adapter for planning mode (chat-only sessions).
    
    Produces receipts and records everything, but never touches
    any external system. This is the default for v0.1.
    
    Design principle: the same pipeline runs for chat and agentic modes.
    Even without tools, the system produces receipts and requires
    structured approval. This trains user behaviour and ensures
    consistent safety properties.
    """

    @property
    def surface_name(self) -> str:
        return "none"

    @property
    def supported_actions(self) -> Dict[str, str]:
        # Planning mode "supports" everything — it just doesn't execute
        return {
            "label_add": "reversible",
            "label_remove": "reversible",
            "archive": "reversible",
            "quarantine": "reversible",
            "delete_soft": "reversible_within_window",
            "delete_hard": "irreversible",
            "send": "irreversible",
            "move": "reversible",
        }

    def execute(self, action: ActionSpec) -> ToolResult:
        return ToolResult(
            action_id=action.action_id,
            status="simulated",
            details=f"Planning mode — no tool execution. "
                    f"Would {action.action_type} on {len(action.target_ids)} target(s).",
            tool_receipt={
                "surface": "none",
                "action_type": action.action_type,
                "target_count": len(action.target_ids),
                "simulated": True,
            },
        )

    def preview(self, action: ActionSpec) -> str:
        targets = ", ".join(action.target_ids[:5])
        if len(action.target_ids) > 5:
            targets += f" ... (+{len(action.target_ids) - 5} more)"
        return f"[PLAN] {action.action_type}: {targets}"
