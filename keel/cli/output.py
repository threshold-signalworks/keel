"""CLI output formatting — JSON and human-readable output.

Single module responsible for all CLI output formatting. Both LocalBackend
and CloudClient return Python dicts; this module converts them to JSON or
human-readable strings.
"""

from __future__ import annotations

import json


# ---------------------------------------------------------------------------
# JSON output
# ---------------------------------------------------------------------------

def json_output(data: dict) -> str:
    """Compact JSON serialisation.  No enforced key ordering."""
    return json.dumps(data, default=str)


# ---------------------------------------------------------------------------
# API response normalisation (used by CloudClient in Phase 2)
# ---------------------------------------------------------------------------

def normalise_api_response(http_status: int, body: dict) -> dict:
    """Wrap an API response into the CLI envelope.

    Returns::

        {
            "exit_code": 0 | 1 | 2,
            "http_status": <int>,
            "data": <body on success | None>,
            "api_error": <body on error | None>,
        }

    Exit code logic:
        * 2xx with ``requires_approval: true`` → exit 2
        * 2xx without ``requires_approval`` → exit 0
        * 4xx / 5xx → exit 1
    """
    if 200 <= http_status < 300:
        if body.get("requires_approval", False):
            return {
                "exit_code": 2,
                "http_status": http_status,
                "data": body,
                "api_error": None,
            }
        return {
            "exit_code": 0,
            "http_status": http_status,
            "data": body,
            "api_error": None,
        }
    return {
        "exit_code": 1,
        "http_status": http_status,
        "data": None,
        "api_error": body,
    }


# ---------------------------------------------------------------------------
# Human-readable output
# ---------------------------------------------------------------------------

def human_output(data: dict, command: str) -> str:
    """Format a backend result dict for human-readable terminal display.

    Parameters
    ----------
    data : dict
        The normalised CLI envelope (``exit_code``, ``data``, ``api_error``).
    command : str
        CLI command name (e.g. ``"status"``, ``"policies"``).
    """
    if data.get("api_error"):
        err = data["api_error"]
        msg = err.get("error", err.get("detail", str(err)))
        return f"Error: {msg}"

    payload = data.get("data") or {}
    formatter = _FORMATTERS.get(command, _format_generic)
    return formatter(payload)


# -- Per-command formatters --------------------------------------------------

def _format_init(d: dict) -> str:
    lines = [f"Initialized: {d.get('keel_dir', '?')}"]
    lines.append(f"Agent: {d.get('agent', 'default')}")
    for item in d.get("created", []):
        lines.append(f"  + {item}")
    return "\n".join(lines)


def _format_check_policy(d: dict) -> str:
    passed = d.get("passed", False)
    risk = d.get("risk_level", 0)
    tag = "PASS" if passed else "FAIL"
    lines = [f"Policy check: {tag}  (risk_level={risk})"]
    for v in d.get("violations", []):
        lines.append(f"  violation: {v}")
    if not d.get("fidelity_ok", True):
        lines.append("  fidelity: FAILED")
    for r in d.get("reasons", []):
        lines.append(f"  reason: {r}")
    return "\n".join(lines)


def _format_wal_append(d: dict) -> str:
    etype = d.get("event_type", "?")
    ehash = d.get("event_hash", "")[:16]
    sid = d.get("session_id", "")
    return f"Appended: {etype}  hash={ehash}...  session={sid}"


def _format_wal_query(d: dict) -> str:
    events = d.get("events", [])
    count = d.get("count", len(events))
    lines = [f"Events: {count}"]
    for e in events:
        ts = e.get("timestamp", "?")
        etype = e.get("event_type", "?")
        sid = e.get("session_id", "")
        ehash = e.get("event_hash", "")[:12]
        lines.append(f"  {ts}  {etype:<24s}  session={sid}  hash={ehash}...")
    return "\n".join(lines)


def _format_verify_chain(d: dict) -> str:
    valid = d.get("chain_valid", False)
    count = d.get("event_count", 0)
    status = "intact" if valid else "BROKEN"
    return f"Chain {status}  ({count} events)"


def _format_status(d: dict) -> str:
    # Fix #3: handle None for cloud-mode fields that are unavailable.
    tier0 = d.get("tier0_count")
    tier0_str = "n/a" if tier0 is None else str(tier0)
    lines = [
        f"Policies: {d.get('policy_count', 0)} active  ({tier0_str} Tier 0)",
        f"Snapshot hash: {d.get('snapshot_hash', '?')[:16]}...",
        f"WAL events: {d.get('wal_event_count', 0)}",
    ]

    chain_valid = d.get("chain_valid")
    if chain_valid is None:
        lines.append("Chain valid: n/a")
    else:
        lines.append(f"Chain valid: {chain_valid}")

    pressure = d.get("context_pressure")
    if pressure is not None:
        lines.append(f"Context pressure: {pressure:.1%}")

    lines.append(f"Agent: {d.get('agent', 'default')}")

    # Cloud queue status (only present in cloud mode).
    pending = d.get("cloud_queue_pending_count")
    if pending is not None:
        lines.append(f"Cloud queue pending: {pending}")
    abandoned = d.get("cloud_queue_abandoned_count")
    if abandoned and abandoned > 0:
        path = d.get(
            "cloud_queue_abandoned_path",
            "<keel_dir>/.cloud_queue/abandoned.jsonl",
        )
        lines.append(
            f"Cloud sync: {abandoned} operations abandoned (see {path})"
        )

    return "\n".join(lines)


def _format_fidelity(d: dict) -> str:
    passed = d.get("passed", False)
    tag = "PASS" if passed else "FAIL"
    lines = [f"Fidelity: {tag}"]
    lines.append(f"  tier0_hash_ok: {d.get('tier0_hash_ok', '?')}")
    lines.append(f"  constraints_present: {d.get('constraints_present', '?')}")
    lines.append(f"  consistency_ok: {d.get('consistency_ok', '?')}")
    for m in d.get("missing_constraints", []):
        lines.append(f"  missing: {m}")
    for i in d.get("consistency_issues", []):
        lines.append(f"  issue: {i}")
    return "\n".join(lines)


def _format_policies(d: dict) -> str:
    policies = d.get("policies", [])
    count = d.get("count", len(policies))
    shash = d.get("snapshot_hash", "")[:16]
    lines = [f"Policies: {count}  (snapshot={shash}...)"]
    for p in policies:
        tier = p.get("priority", "?")
        scope = p.get("scope", "?")
        source = p.get("source", "?")
        active = "active" if p.get("active", True) else "inactive"
        content = p.get("content", "")
        content_trunc = (content[:57] + "...") if len(content) > 60 else content
        lines.append(f"  T{tier} [{scope}] ({source}) {active}: {content_trunc}")
    return "\n".join(lines)


def _format_add_policy(d: dict) -> str:
    pid = d.get("id", "?")
    content = d.get("content", "")
    content_trunc = (content[:57] + "...") if len(content) > 60 else content
    return f"Added policy {pid}: {content_trunc}"


def _format_remove_policy(d: dict) -> str:
    pid = d.get("policy_id", "?")
    return f"Deactivated policy {pid}"


def _format_quarantine(d: dict) -> str:
    items = d.get("items", [])
    active = d.get("active_count", 0)
    lines = [f"Quarantine: {active} active items"]
    # NOTE: quarantine-add is deferred to a future phase.
    # This command is read-only — shows state reconstructed from WAL events.
    for item in items:
        iid = item.get("item_id", "?")
        surface = item.get("surface", "?")
        reason = item.get("reason", "")
        is_active = item.get("is_active", not item.get("released", False))
        tag = "active" if is_active else "released"
        lines.append(f"  [{tag}] {iid} ({surface}): {reason}")
    return "\n".join(lines)


def _format_restore(d: dict) -> str:
    iid = d.get("item_id", "?")
    labels = d.get("original_labels", [])
    return f"Released {iid}  (original_labels={labels})"


def _format_generic(d: dict) -> str:
    """Fallback for unknown commands."""
    return json.dumps(d, indent=2, default=str)


_FORMATTERS = {
    "init": _format_init,
    "check-policy": _format_check_policy,
    "wal-append": _format_wal_append,
    "wal-query": _format_wal_query,
    "verify-chain": _format_verify_chain,
    "status": _format_status,
    "fidelity": _format_fidelity,
    "policies": _format_policies,
    "add-policy": _format_add_policy,
    "remove-policy": _format_remove_policy,
    "quarantine": _format_quarantine,
    "restore": _format_restore,
}
