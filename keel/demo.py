"""
Keel Interactive CLI Demo

A terminal REPL that walks through the full Keel pipeline:
policies, proposals, receipts, approvals, rejections, compaction,
fidelity checks, WAL inspection.

Designed for:
- Handing to a reviewer who wants to see the system work
- Quick smoke tests during development

Run: python -m keel.demo [--run-dir DIR]
"""

from __future__ import annotations

import json
import os
import readline  # enables arrow keys / history in input()
import shutil
import sys
import textwrap
from pathlib import Path
from typing import List, Optional

from keel.core.policy_store import PolicyStore
from keel.core.context_manager import ContextManager
from keel.core.fidelity import FidelityVerifier
from keel.core.schemas import (
    ActionSpec,
    ContextConfig,
    Reversibility,
    _now,
)
from keel.actuation.engine import ActuationEngine
from keel.actuation.quarantine import QuarantineManager
from keel.actuation.rollback import RollbackManager
from keel.actuation.wal import WAL
from keel.adapters.base import PlanningAdapter
from keel.adapters.gmail import GmailAdapter
from keel.adapters.gmail_client import MockGmailClient, GmailMessage
from keel.telemetry.events import KeelTelemetry, FileEmitter


# ---------------------------------------------------------------------------
# Terminal formatting
# ---------------------------------------------------------------------------

_TERM_WIDTH = min(shutil.get_terminal_size().columns, 80)

def _c(text: str, code: str) -> str:
    """ANSI colour wrapper. Degrades gracefully if piped."""
    if not sys.stdout.isatty():
        return text
    codes = {
        "bold": "1", "dim": "2",
        "red": "31", "green": "32", "yellow": "33",
        "blue": "34", "magenta": "35", "cyan": "36", "white": "37",
        "bg_red": "41", "bg_green": "42", "bg_yellow": "43",
    }
    c = codes.get(code, "0")
    return f"\033[{c}m{text}\033[0m"


def _banner(text: str, char: str = "="):
    print()
    print(_c(char * _TERM_WIDTH, "cyan"))
    print(_c(f"  {text}", "bold"))
    print(_c(char * _TERM_WIDTH, "cyan"))


def _section(text: str):
    print()
    print(_c(f"--- {text} ---", "yellow"))


def _ok(text: str):
    print(f"  {_c('✓', 'green')} {text}")


def _fail(text: str):
    print(f"  {_c('✗', 'red')} {text}")


def _info(text: str):
    print(f"  {_c('·', 'dim')} {text}")


def _warn(text: str):
    print(f"  {_c('!', 'yellow')} {text}")


def _prompt(text: str = "keel") -> str:
    try:
        return input(f"\n{_c(text, 'cyan')}> ").strip()
    except (EOFError, KeyboardInterrupt):
        print()
        return "quit"


def _print_wrapped(text: str, indent: int = 4):
    for line in text.split("\n"):
        wrapped = textwrap.fill(line, width=_TERM_WIDTH - indent,
                                initial_indent=" " * indent,
                                subsequent_indent=" " * indent)
        print(wrapped)


# ---------------------------------------------------------------------------
# Scenario library — prebuilt action sets for quick demos
# ---------------------------------------------------------------------------

SCENARIOS = {
    "triage": {
        "name": "Email Triage (planning)",
        "description": "Archive newsletters, label important, quarantine suspicious (planning mode)",
        "actions": [
            ActionSpec(surface="gmail", action_type="archive", target_ids=["newsletter_001", "newsletter_002", "newsletter_003"],
                       rationale="Archive old newsletters", reversibility=Reversibility.REVERSIBLE.value),
            ActionSpec(surface="gmail", action_type="label_add", target_ids=["msg_from_boss"],
                       params={"label": "important"}, rationale="Label boss's email", reversibility=Reversibility.REVERSIBLE.value),
            ActionSpec(surface="gmail", action_type="label_add", target_ids=["msg_from_client"],
                       params={"label": "urgent"}, rationale="Label client email urgent", reversibility=Reversibility.REVERSIBLE.value),
            ActionSpec(surface="gmail", action_type="quarantine", target_ids=["suspicious_msg_001"],
                       rationale="Quarantine suspicious email", reversibility=Reversibility.REVERSIBLE.value),
        ],
    },
    "gmail": {
        "name": "Gmail Inbox Triage (live mock)",
        "description": "Archive, label, quarantine against a mock inbox — state actually changes",
        "actions": [
            ActionSpec(surface="gmail", action_type="archive", target_ids=["msg_001"],
                       rationale="Archive old newsletter"),
            ActionSpec(surface="gmail", action_type="label_add", target_ids=["msg_003"],
                       params={"label": "follow-up"}, rationale="Label for follow-up"),
            ActionSpec(surface="gmail", action_type="quarantine", target_ids=["msg_005"],
                       rationale="Quarantine suspicious email"),
        ],
    },
    "gmail-boss": {
        "name": "Touch the Boss's Email",
        "description": "Try to archive boss's email — blocked by 'never touch' policy",
        "actions": [
            ActionSpec(surface="gmail", action_type="archive", target_ids=["msg_002"],
                       rationale="Archive boss's email"),
        ],
    },
    "dangerous": {
        "name": "Dangerous Batch",
        "description": "Mix of safe and unsafe actions — deletions should be blocked",
        "actions": [
            ActionSpec(surface="gmail", action_type="archive", target_ids=["msg_001"],
                       rationale="Archive read message", reversibility=Reversibility.REVERSIBLE.value),
            ActionSpec(surface="gmail", action_type="delete_hard", target_ids=["msg_002"],
                       rationale="Permanently delete spam", reversibility=Reversibility.IRREVERSIBLE.value),
            ActionSpec(surface="gmail", action_type="send", target_ids=["draft_001"],
                       params={"to": "all_staff@company.com"}, rationale="Send company-wide email",
                       reversibility=Reversibility.IRREVERSIBLE.value),
            ActionSpec(surface="gmail", action_type="label_add", target_ids=["msg_003"],
                       params={"label": "follow-up"}, rationale="Label for follow-up", reversibility=Reversibility.REVERSIBLE.value),
        ],
    },
    "oversized": {
        "name": "Oversized Batch",
        "description": "30 actions — exceeds batch limit, triggers split",
        "actions": [
            ActionSpec(surface="gmail", action_type="archive", target_ids=[f"msg_{i:03d}"],
                       rationale=f"Archive message {i}")
            for i in range(30)
        ],
    },
}


# ---------------------------------------------------------------------------
# Demo session
# ---------------------------------------------------------------------------

class DemoSession:
    def __init__(self, run_dir: Path):
        self._run_dir = run_dir
        self._run_dir.mkdir(parents=True, exist_ok=True)

        self._wal = WAL(
            wal_path=run_dir / "demo.wal.jsonl",
            session_id="demo",
        )
        self._telemetry = KeelTelemetry(
            session_id="demo",
            emitter=FileEmitter(run_dir / "demo.telemetry.jsonl"),
        )
        self._policy_store = PolicyStore()
        self._config = ContextConfig(total_budget=8000, compaction_threshold=0.7)
        self._context_manager = ContextManager(
            self._policy_store, self._config, wal=self._wal,
        )

        # Mock Gmail inbox
        self._gmail_client = MockGmailClient([
            GmailMessage(id="msg_001", subject="Weekly Newsletter", sender="news@site.com",
                          snippet="This week in tech...", labels={"INBOX", "UNREAD"}),
            GmailMessage(id="msg_002", subject="Q3 Report", sender="boss@company.com",
                          snippet="Please review the attached...", labels={"INBOX", "IMPORTANT"}),
            GmailMessage(id="msg_003", subject="Meeting Tomorrow", sender="colleague@company.com",
                          snippet="Can we reschedule?", labels={"INBOX"}),
            GmailMessage(id="msg_004", subject="Invoice #4521", sender="billing@vendor.com",
                          snippet="Your invoice is ready", labels={"INBOX"}),
            GmailMessage(id="msg_005", subject="Suspicious Link", sender="unknown@sketchy.com",
                          snippet="Click here to win...", labels={"INBOX", "SPAM"}),
            GmailMessage(id="msg_006", subject="Legal Notice", sender="legal@company.com",
                          snippet="Please do not delete", labels={"INBOX", "IMPORTANT"}),
        ])
        self._gmail_adapter = GmailAdapter(self._gmail_client)

        # Engine starts in planning mode; switches to Gmail for gmail scenarios
        self._engine = ActuationEngine(
            self._policy_store,
            self._context_manager,
            wal=self._wal,
            adapter=PlanningAdapter(),
            telemetry=self._telemetry,
        )
        self._gmail_engine = ActuationEngine(
            self._policy_store,
            self._context_manager,
            wal=self._wal,
            adapter=self._gmail_adapter,
            telemetry=self._telemetry,
        )
        self._fidelity = FidelityVerifier(
            self._policy_store, self._context_manager, wal=self._wal,
        )
        self._active_engine = self._engine  # current engine

        # Quarantine + rollback (Phase 4)
        self._quarantine = QuarantineManager(min_delay=10, wal=self._wal)  # 10s for demo
        self._rollback = RollbackManager(
            self._quarantine, gmail_client=self._gmail_client, wal=self._wal,
        )
        self._pre_quarantine_labels = {}  # {msg_id: original_labels} captured at propose time

    def run(self):
        _banner("KEEL — Interactive Demo")
        print()
        _info("Structural persistence guarantee for LLM agent systems.")
        _info(f"Run directory: {self._run_dir}")
        _info(f"WAL: {self._run_dir / 'demo.wal.jsonl'}")
        print()
        self._show_help()

        while True:
            cmd = _prompt("keel")
            if not cmd:
                continue
            parts = cmd.split(None, 1)
            verb = parts[0].lower()
            arg = parts[1] if len(parts) > 1 else ""

            if verb in ("quit", "exit", "q"):
                self._do_quit()
                break
            elif verb == "help":
                self._show_help()
            elif verb == "policies":
                self._do_policies()
            elif verb == "add-policy":
                self._do_add_policy(arg)
            elif verb == "propose":
                self._do_propose(arg)
            elif verb == "approve":
                self._do_approve(arg)
            elif verb == "receipt":
                self._do_receipt()
            elif verb == "fidelity":
                self._do_fidelity()
            elif verb == "talk":
                self._do_talk(arg)
            elif verb == "compact":
                self._do_compact()
            elif verb == "pressure":
                self._do_pressure()
            elif verb == "wal":
                self._do_wal(arg)
            elif verb == "scenarios":
                self._do_scenarios()
            elif verb == "status":
                self._do_status()
            elif verb == "suite":
                self._do_suite()
            elif verb == "inbox":
                self._do_inbox()
            elif verb == "quarantine":
                self._do_quarantine_status()
            elif verb == "rollback":
                self._do_rollback(arg)
            elif verb == "undo":
                self._do_undo(arg)
            elif verb.startswith("approve"):
                # Handle "APPROVE batch ..." directly
                self._do_approve(cmd)
            else:
                _warn(f"Unknown command: {verb}. Type 'help' for commands.")

    # -----------------------------------------------------------------
    # Commands
    # -----------------------------------------------------------------

    def _show_help(self):
        _section("Commands")
        cmds = [
            ("policies", "Show all active policies"),
            ("add-policy <text>", "Add a new Tier 0 constraint"),
            ("scenarios", "List prebuilt action scenarios"),
            ("propose <scenario>", "Propose actions from a scenario (triage/dangerous/oversized)"),
            ("receipt", "Show the pending receipt"),
            ("approve <text>", "Submit approval (or paste the APPROVE syntax)"),
            ("fidelity", "Run fidelity verification"),
            ("talk <message>", "Add a conversation turn (simulates user message)"),
            ("compact", "Trigger context compaction"),
            ("pressure", "Show context pressure and token usage"),
            ("wal [n]", "Show last n WAL entries (default 10)"),
            ("status", "Session status overview"),
            ("inbox", "Show mock Gmail inbox state"),
            ("quarantine", "Show quarantine status"),
            ("rollback <msg_id>", "Unquarantine a message (restore original state)"),
            ("undo <action> <msg_id>", "Undo an action (archive/label/trash)"),
            ("suite", "Run the fidelity test suite"),
            ("help", "Show this help"),
            ("quit", "Exit"),
        ]
        for cmd, desc in cmds:
            print(f"  {_c(cmd, 'bold'):42s} {desc}")

    def _do_quit(self):
        _section("Session Summary")
        _info(f"WAL events: {self._wal.event_count}")
        _info(f"WAL chain intact: {self._wal.verify_chain()}")
        _info(f"Turns: {self._context_manager.turn_count}")
        _info(f"Policies: {len(self._policy_store)}")
        _info(f"Run directory: {self._run_dir}")
        print()
        print(_c("Session ended.", "dim"))

    def _do_policies(self):
        _section("Active Policies")
        policies = self._policy_store.list_policies(active=True)
        for i, p in enumerate(policies, 1):
            tier = f"Tier {p.priority}"
            source = p.source
            icon = _c("■", "green") if p.priority == 0 else _c("□", "dim")
            print(f"  {icon} {i}. [{tier}] [{source}] {p.content}")
        print()
        _info(f"Snapshot hash: {self._policy_store.get_snapshot_hash()[:24]}...")

    def _do_add_policy(self, text: str):
        if not text:
            _warn("Usage: add-policy <constraint text>")
            _warn("Example: add-policy Never delete emails from legal@company.com")
            return
        try:
            policy = self._policy_store.make_policy(text)
            self._policy_store.add_policy(policy)
            _ok(f"Policy added: {policy.id[:12]}...")
            _info(f"Content: {text}")
            _info(f"New snapshot hash: {self._policy_store.get_snapshot_hash()[:24]}...")
        except ValueError as e:
            _fail(f"Failed: {e}")

    def _do_scenarios(self):
        _section("Available Scenarios")
        for key, scenario in SCENARIOS.items():
            n = len(scenario["actions"])
            print(f"  {_c(key, 'bold'):20s} {scenario['name']} ({n} actions)")
            _print_wrapped(scenario["description"], indent=22)

    def _do_propose(self, scenario_name: str):
        if not scenario_name:
            _warn("Usage: propose <scenario>")
            _warn("Available: " + ", ".join(SCENARIOS.keys()))
            return

        scenario = SCENARIOS.get(scenario_name.lower())
        if not scenario:
            _warn(f"Unknown scenario: {scenario_name}")
            _warn("Available: " + ", ".join(SCENARIOS.keys()))
            return

        _section(f"Proposing: {scenario['name']}")
        _info(scenario["description"])

        actions = scenario["actions"]

        # Switch engine based on scenario
        is_gmail = scenario_name.lower().startswith("gmail")
        self._active_engine = self._gmail_engine if is_gmail else self._engine
        if is_gmail:
            _info("Using Gmail adapter (mock inbox)")
            # Pre-capture original labels for quarantine targets (Phase 4)
            self._pre_quarantine_labels = {}
            for action in actions:
                if action.action_type == "quarantine":
                    for tid in action.target_ids:
                        msg = self._gmail_client.get_message(tid)
                        if msg:
                            self._pre_quarantine_labels[tid] = list(msg.labels)

        if len(actions) > 20:
            _warn(f"Batch has {len(actions)} actions (limit 20). Will be split.")
            receipts = self._active_engine.propose_split(actions)
            _info(f"Split into {len(receipts)} sub-batches.")
            receipt = receipts[0]
            _info(f"Showing first batch ({len(receipt.actions)} actions):")
        else:
            receipt = self._active_engine.propose(actions)

        self._show_receipt(receipt)

    def _do_receipt(self):
        if not self._active_engine.has_pending:
            _warn("No pending receipt. Use 'propose <scenario>' first.")
            return
        self._show_receipt(self._active_engine.pending_receipt)

    def _show_receipt(self, receipt):
        _section(f"Receipt: {receipt.batch_id}")

        print()
        print(_c("  Actions:", "bold"))
        print(receipt.human_summary)

        print()
        print(_c("  Risk Summary:", "bold"))
        _print_wrapped(receipt.risk_summary)

        print()
        print(_c("  Policy Hash:", "bold"))
        _info(f"{receipt.policy_snapshot_hash[:32]}...")

        print()
        print(_c("  Fidelity Hash:", "bold"))
        _info(f"{receipt.context_fidelity_hash[:32]}...")

        print()
        print(_c("  Approval Instructions:", "bold"))
        for line in receipt.approval_instructions.split("\n"):
            if "APPROVE batch" in line:
                print(f"    {_c(line.strip(), 'green')}")
            else:
                print(f"    {line}")

    def _do_approve(self, text: str):
        if not text:
            _warn("Usage: approve <approval text>")
            _warn("Paste the exact APPROVE syntax from the receipt.")
            return

        # Handle the case where user types "approve APPROVE batch..."
        if text.lower().startswith("approve ") and "batch" in text.lower():
            # They typed "approve APPROVE batch..." — strip the leading "approve "
            pass
        elif not text.upper().startswith("APPROVE"):
            # Wrap it — they might have typed "approve yes" meaning the approval text is "yes"
            pass

        result = self._active_engine.approve(text)
        if result.accepted:
            _ok(f"Approved! Batch {result.batch_id} executed.")
            _info(f"Actions: {', '.join(result.approved_action_ids[:5])}")
            if len(result.approved_action_ids) > 5:
                _info(f"  ... +{len(result.approved_action_ids) - 5} more")
            if self._active_engine is self._gmail_engine:
                _info("Gmail inbox state updated. Use 'inbox' to see changes.")
                # Record quarantine actions using pre-captured labels
                if self._pre_quarantine_labels:
                    for tid, labels in self._pre_quarantine_labels.items():
                        self._quarantine.quarantine(tid, "gmail", labels,
                                                    reason="Quarantined via Keel")
                    self._pre_quarantine_labels = {}
        else:
            _fail("Rejected.")
            for line in result.rejection_reason.split("\n"):
                _print_wrapped(line, indent=6)

    def _do_fidelity(self):
        _section("Fidelity Verification")
        result = self._fidelity.verify()
        if result.passed:
            _ok("All checks passed")
        else:
            _fail("FIDELITY CHECK FAILED")

        print()
        status = _c("PASS", "green") if result.tier0_hash_ok else _c("FAIL", "red")
        _info(f"Tier 0 hash:        {status}")
        if result.tier0_hash_ok:
            _info(f"  Hash: {result.tier0_hash_expected[:24]}...")
        else:
            _info(f"  Expected: {result.tier0_hash_expected[:24]}...")
            _info(f"  Actual:   {result.tier0_hash_actual[:24]}...")

        status = _c("PASS", "green") if result.constraints_present else _c("FAIL", "red")
        _info(f"Constraints present: {status}")
        if result.missing_constraints:
            for mc in result.missing_constraints:
                _fail(f"  Missing: {mc}")

        status = _c("PASS", "green") if result.consistency_ok else _c("FAIL", "red")
        _info(f"Tier 1 consistency: {status}")
        if result.consistency_issues:
            for ci in result.consistency_issues:
                _warn(f"  {ci}")

    def _do_talk(self, message: str):
        if not message:
            _warn("Usage: talk <message>")
            _warn("Example: talk Never delete emails from my boss")
            return

        turn = self._context_manager.add_to_tier2(message, "user")
        _info(f"Turn {turn.turn_number}: {message[:60]}{'...' if len(message) > 60 else ''}")

        if turn.has_constraint:
            _warn("Constraint-like language detected!")
            _warn(f"This turn is flagged for preservation during compaction.")
            _info("Consider promoting to Tier 0: add-policy <text>")

    def _do_compact(self):
        _section("Context Compaction")
        pressure_before = self._context_manager.get_pressure()
        events = self._context_manager.compact()
        pressure_after = self._context_manager.get_pressure()

        if not events:
            _info(f"No compaction needed (pressure: {pressure_before:.1%})")
            return

        for event in events:
            tier_name = {0: "Tier 0 (FROZEN)", 1: "Tier 1 (COARSE)", 2: "Tier 2 (FINE)"}.get(event.tier, f"Tier {event.tier}")
            _info(f"{tier_name}: {event.tokens_before} → {event.tokens_after} tokens ({event.items_dropped} items dropped)")

        _info(f"Pressure: {pressure_before:.1%} → {pressure_after:.1%}")

    def _do_pressure(self):
        _section("Context Pressure")
        usage = self._context_manager.get_current_usage()
        budgets = self._context_manager.get_tier_budgets()
        pressure = self._context_manager.get_pressure()

        bar_width = 30
        filled = int(pressure * bar_width)
        bar = "█" * filled + "░" * (bar_width - filled)
        colour = "green" if pressure < 0.5 else "yellow" if pressure < 0.85 else "red"

        print(f"  [{_c(bar, colour)}] {pressure:.1%}")
        print()
        _info(f"Tier 0 (FROZEN):  {usage['tier0']:>6} / {budgets['tier0']:>6} tokens")
        _info(f"Tier 1 (COARSE):  {usage['tier1']:>6} / {budgets['tier1']:>6} tokens")
        _info(f"Tier 2 (FINE):    {usage['tier2']:>6} / {budgets['tier2']:>6} tokens")
        _info(f"Total:            {usage['total']:>6} / {budgets['total']:>6} tokens")
        print()
        _info(f"Tier 1 records: {self._context_manager.tier1_record_count}")
        _info(f"Tier 2 turns:   {self._context_manager.tier2_turn_count}")
        _info(f"Compaction threshold: {self._config.compaction_threshold:.0%}")

    def _do_wal(self, arg: str):
        n = int(arg) if arg.isdigit() else 10
        events = self._wal.read_all()
        if not events:
            _info("WAL is empty.")
            return

        _section(f"WAL (last {min(n, len(events))} of {len(events)} events)")
        for event in events[-n:]:
            ts = event.timestamp[11:19] if len(event.timestamp) > 19 else event.timestamp
            etype = event.event_type
            colour = "green" if "PASS" in str(event.payload.get("passed", "")) else "white"
            if etype in ("FIDELITY_CHECK",) and not event.payload.get("passed", True):
                colour = "red"
            print(f"  {_c(ts, 'dim')} {_c(etype, colour):30s} {_summary(event.payload)}")

        print()
        chain_ok = self._wal.verify_chain()
        if chain_ok:
            _ok(f"Chain integrity: verified ({len(events)} events)")
        else:
            _fail("Chain integrity: BROKEN")

    def _do_status(self):
        _banner("Session Status", char="-")
        _info(f"Policies:         {len(self._policy_store)} ({len(self._policy_store.get_tier0_policies())} Tier 0)")
        _info(f"Turns:            {self._context_manager.turn_count}")
        _info(f"Tier 1 records:   {self._context_manager.tier1_record_count}")
        _info(f"Context pressure: {self._context_manager.get_pressure():.1%}")
        _info(f"WAL events:       {self._wal.event_count}")
        _info(f"WAL chain intact: {self._wal.verify_chain()}")
        _info(f"Pending receipt:  {'Yes' if self._active_engine.has_pending else 'No'}")
        _info(f"Policy hash:      {self._policy_store.get_snapshot_hash()[:24]}...")
        _info(f"Gmail adapter:    {'paused' if self._gmail_adapter.is_paused else 'active'}")

    def _do_inbox(self):
        _section("Mock Gmail Inbox")
        for mid in sorted(self._gmail_client._messages.keys()):
            msg = self._gmail_client.get_message(mid)
            labels = sorted(msg.labels)
            status_parts = []
            if msg.is_archived:
                status_parts.append(_c("ARCHIVED", "dim"))
            if msg.is_trashed:
                status_parts.append(_c("TRASH", "red"))
            if msg.is_quarantined:
                status_parts.append(_c("QUARANTINE", "yellow"))
            status_str = " ".join(status_parts) if status_parts else _c("INBOX", "green")

            print(f"  {_c(mid, 'bold')}  {status_str}")
            print(f"    From: {msg.sender}  Subject: {msg.subject}")
            print(f"    Labels: {', '.join(labels)}")

        # Operation count
        ops = self._gmail_client.operation_log
        if ops:
            print()
            _info(f"Operations: {len(ops)} ({self._gmail_client.noop_count} no-ops)")
            _info(f"Rate limit: {self._gmail_adapter._rate_limit.used}/{self._gmail_adapter._rate_limit.max_per_hour} per hour")

    def _do_quarantine_status(self):
        _section("Quarantine Status")
        active = self._quarantine.list_active()
        if not active:
            _info("No items in quarantine.")
            return
        _info(f"{len(active)} item(s) in quarantine (delay: {self._quarantine.min_delay:.0f}s):")
        print()
        for rec in active:
            age = rec.age_seconds
            can_del, reason = self._quarantine.can_delete(rec.item_id)
            msg = self._gmail_client.get_message(rec.item_id)
            subject = msg.subject if msg else "(unknown)"
            status = _c("ELIGIBLE FOR DELETE", "red") if can_del else _c(f"{age:.0f}s / {self._quarantine.min_delay:.0f}s", "yellow")
            print(f"  {_c(rec.item_id, 'bold')}  {subject}")
            print(f"    Original labels: {rec.original_labels}")
            print(f"    Reason: {rec.reason or '(none)'}")
            print(f"    Age: {status}")
            print()

    def _do_rollback(self, arg: str):
        if not arg:
            _warn("Usage: rollback <msg_id>")
            _warn("Unquarantines a message and restores its original labels.")
            return
        msg_id = arg.strip()
        result = self._rollback.unquarantine(msg_id)
        if result.success:
            _ok(f"Rolled back {msg_id}")
            _info(f"Labels restored: {result.labels_after}")
        else:
            _fail(f"Rollback failed: {result.details}")

    def _do_undo(self, arg: str):
        if not arg:
            _warn("Usage: undo <action> <msg_id> [label]")
            _warn("Actions: archive, label-add, label-remove, trash")
            return
        parts = arg.split()
        action = parts[0].lower()
        msg_id = parts[1] if len(parts) > 1 else ""
        label = parts[2] if len(parts) > 2 else ""

        if not msg_id:
            _warn("Missing msg_id. Usage: undo <action> <msg_id>")
            return

        if action == "archive":
            result = self._rollback.undo_archive(msg_id)
        elif action == "label-add" or action == "label_add":
            if not label:
                _warn("Missing label. Usage: undo label-add <msg_id> <label>")
                return
            result = self._rollback.undo_label_add(msg_id, label)
        elif action == "label-remove" or action == "label_remove":
            if not label:
                _warn("Missing label. Usage: undo label-remove <msg_id> <label>")
                return
            result = self._rollback.undo_label_remove(msg_id, label)
        elif action == "trash":
            result = self._rollback.undo_trash(msg_id)
        else:
            _warn(f"Unknown undo action: {action}")
            _warn("Available: archive, label-add, label-remove, trash")
            return

        if result.success:
            _ok(f"Undone: {result.action} on {msg_id}")
            _info(f"Labels now: {result.labels_after}")
        else:
            _fail(f"Undo failed: {result.details}")

    def _do_suite(self):
        _section("Running Fidelity Test Suite")
        _info("This runs all 9 design doc test cases in isolated sessions...")
        print()

        from keel.integration.fidelity_suite import run_fidelity_suite
        report = run_fidelity_suite(run_dir=self._run_dir / "fidelity_suite")

        for result in report.results:
            if result["passed"]:
                _ok(f"{result['name']} ({result['duration_ms']}ms)")
            else:
                _fail(f"{result['name']} ({result['duration_ms']}ms)")
                for ev in result["evidence"]:
                    _print_wrapped(ev, indent=8)

        print()
        if report.all_passed:
            _ok(f"All {report.total_tests} tests passed")
        else:
            _fail(f"{report.failed}/{report.total_tests} tests failed")

        report_path = self._run_dir / "fidelity_suite" / "suite_report.json"
        _info(f"Report: {report_path}")


def _summary(payload: dict) -> str:
    """One-line summary of a WAL event payload."""
    parts = []
    if "passed" in payload:
        parts.append("pass" if payload["passed"] else "FAIL")
    if "action_id" in payload:
        parts.append(f"action={payload['action_id'][:8]}")
    if "batch_id" in payload:
        parts.append(f"batch={payload['batch_id']}")
    if "status" in payload:
        parts.append(payload["status"])
    if "risk_level" in payload:
        parts.append(f"risk={payload['risk_level']}")
    if "items_dropped" in payload:
        parts.append(f"dropped={payload['items_dropped']}")
    if "policy_id" in payload:
        parts.append(f"policy={payload['policy_id'][:12]}")
    if "content" in payload:
        parts.append(payload["content"][:40])
    if not parts:
        keys = list(payload.keys())[:3]
        parts = [f"{k}={str(payload[k])[:20]}" for k in keys]
    return " | ".join(parts)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    run_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("runs/demo")
    session = DemoSession(run_dir)
    session.run()


if __name__ == "__main__":
    main()
