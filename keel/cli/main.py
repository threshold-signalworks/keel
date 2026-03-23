"""CLI entry point — argument parsing, backend dispatch, output formatting."""

from __future__ import annotations

import argparse
import json
import os
import sys

from keel.cli.cloud_client import CloudAPIError, CloudClient
from keel.cli.local_backend import LocalBackend
from keel.cli.output import json_output, human_output, normalise_api_response


# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------

DEFAULT_KEEL_DIR = os.path.join(os.path.expanduser("~"), ".keel")
DEFAULT_AGENT = "default"

# Canonical list of the 12 Phase 1 subcommands.
COMMANDS = [
    "init",
    "check-policy",
    "wal-append",
    "wal-query",
    "verify-chain",
    "status",
    "fidelity",
    "policies",
    "add-policy",
    "remove-policy",
    "quarantine",
    "restore",
]


# ---------------------------------------------------------------------------
# Argument parser
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    """Build the argparse parser with all 12 subcommands."""

    def _add_mode_flags(
        target: argparse.ArgumentParser,
        *,
        human_dest: str,
        local_dest: str,
    ) -> None:
        target.add_argument(
            "--human", dest=human_dest, action="store_true", default=False,
            help="Human-readable output (default: JSON)",
        )
        target.add_argument(
            "--local", dest=local_dest, action="store_true", default=False,
            help="Force local mode even if cloud credentials are set",
        )

    parser = argparse.ArgumentParser(
        prog="keel",
        description="Keel - Structural persistence guarantee for LLM agent systems",
    )
    _add_mode_flags(parser, human_dest="human_root", local_dest="local_root")

    # Global flags.
    parser.add_argument(
        "--keel-dir", default=None,
        help=f"Keel data directory (default: {DEFAULT_KEEL_DIR})",
    )
    parser.add_argument(
        "--agent", default=None,
        help=f"Agent session ID (default: {DEFAULT_AGENT})",
    )

    sub = parser.add_subparsers(dest="command")

    # -- init ---------------------------------------------------------------
    init_parser = sub.add_parser("init", help="Initialise keel directory")
    _add_mode_flags(init_parser, human_dest="human_sub", local_dest="local_sub")

    # -- check-policy -------------------------------------------------------
    cp = sub.add_parser("check-policy", help="Validate an action against policies")
    _add_mode_flags(cp, human_dest="human_sub", local_dest="local_sub")
    cp_input = cp.add_mutually_exclusive_group(required=True)
    cp_input.add_argument(
        "--action-json",
        help="JSON string or @filepath with action spec",
    )
    cp_input.add_argument(
        "--action-file",
        help="Path to a UTF-8 JSON file containing the action spec",
    )

    # -- wal-append ---------------------------------------------------------
    wa = sub.add_parser("wal-append", help="Append event to WAL")
    _add_mode_flags(wa, human_dest="human_sub", local_dest="local_sub")
    wa.add_argument("--event-type", required=True, help="WAL event type")
    wa.add_argument(
        "--payload", required=True,
        help="JSON string or @filepath with event payload",
    )

    # -- wal-query ----------------------------------------------------------
    wq = sub.add_parser("wal-query", help="Query WAL events")
    _add_mode_flags(wq, human_dest="human_sub", local_dest="local_sub")
    wq.add_argument("--type", dest="event_type", default=None, help="Filter by event type")
    wq.add_argument("--since", default=None, help="Filter events since ISO-8601 timestamp")
    wq.add_argument("--last", type=int, default=None, help="Return only the last N events")

    # -- verify-chain -------------------------------------------------------
    verify_parser = sub.add_parser("verify-chain", help="Verify WAL hash chain integrity")
    _add_mode_flags(verify_parser, human_dest="human_sub", local_dest="local_sub")

    # -- status -------------------------------------------------------------
    status_parser = sub.add_parser("status", help="Show system status")
    _add_mode_flags(status_parser, human_dest="human_sub", local_dest="local_sub")

    # -- fidelity -----------------------------------------------------------
    fidelity_parser = sub.add_parser("fidelity", help="Run fidelity verification")
    _add_mode_flags(fidelity_parser, human_dest="human_sub", local_dest="local_sub")

    # -- policies -----------------------------------------------------------
    pol = sub.add_parser("policies", help="List policies")
    _add_mode_flags(pol, human_dest="human_sub", local_dest="local_sub")
    pol.add_argument("--scope", default=None, help="Filter by scope")
    pol.add_argument("--show-inactive", action="store_true", help="Include deactivated policies")

    # -- add-policy ---------------------------------------------------------
    ap = sub.add_parser("add-policy", help="Add a new policy")
    _add_mode_flags(ap, human_dest="human_sub", local_dest="local_sub")
    ap.add_argument("--content", required=True, help="Policy content text")
    ap.add_argument("--scope", default="global", help="Policy scope")
    ap.add_argument(
        "--type", dest="policy_type", default="constraint",
        help="Policy type (constraint/permission/limit/preference)",
    )
    ap.add_argument("--priority", type=int, default=0, help="Policy tier (0=frozen, 1=coarse, 2=fine)")

    # -- remove-policy ------------------------------------------------------
    rp = sub.add_parser("remove-policy", help="Deactivate a policy")
    _add_mode_flags(rp, human_dest="human_sub", local_dest="local_sub")
    rp.add_argument("--id", dest="policy_id", required=True, help="Policy ID to deactivate")

    # -- quarantine ---------------------------------------------------------
    quarantine_parser = sub.add_parser("quarantine", help="List quarantine records")
    _add_mode_flags(quarantine_parser, human_dest="human_sub", local_dest="local_sub")

    # -- restore ------------------------------------------------------------
    rs = sub.add_parser("restore", help="Release an item from quarantine")
    _add_mode_flags(rs, human_dest="human_sub", local_dest="local_sub")
    rs.add_argument("--item-id", required=True, help="Item ID to restore")

    return parser


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _resolve_json_arg(value: str) -> dict:
    """Parse a JSON argument.

    Supports:
    - Direct JSON string: ``'{"key": "value"}'``
    - File reference:     ``@path/to/file.json``
    - Stdin:              ``-``
    """
    if value == "-":
        return json.load(sys.stdin)
    if value.startswith("@"):
        filepath = value[1:]
        # utf-8-sig strips the BOM (U+FEFF) written by Windows tools such as
        # Out-File and Notepad; it is a no-op for BOM-free UTF-8 files.
        with open(filepath, "r", encoding="utf-8-sig") as fh:
            return json.load(fh)
    return json.loads(value)


def _resolve_action_spec_arg(args) -> dict:
    """Resolve check-policy action input from JSON text or file path."""
    if getattr(args, "action_json", None):
        return _resolve_json_arg(args.action_json)
    if getattr(args, "action_file", None):
        with open(args.action_file, "r", encoding="utf-8") as fh:
            return json.loads(fh.read().rstrip())
    raise ValueError("Missing action spec.")


def _get_backend(args, keel_dir: str, agent: str):
    """Select backend based on environment and flags.

    Cloud mode activates when ``KEEL_CLOUD_API_KEY`` is set and ``--local``
    is not passed.  Otherwise, local mode is used.
    """
    api_key = os.environ.get("KEEL_CLOUD_API_KEY")
    if api_key and not args.local:
        base_url = os.environ.get(
            "KEEL_CLOUD_BASE_URL", "https://api.thresholdsignalworks.com",
        )
        from keel.cli.cloud_client import CloudClient
        return CloudClient(keel_dir, agent, base_url, api_key)
    return LocalBackend(keel_dir, agent)


def _policy_check_wal_payload(action_spec: dict, result: dict) -> dict:
    """Build the policy-check audit payload from input and result data."""
    return {
        "action_type": action_spec.get("action_type", ""),
        "target_ids": action_spec.get("target_ids", []),
        "surface": action_spec.get("surface", ""),
        "reversibility": action_spec.get("reversibility", ""),
        "passed": result.get("passed", False),
        "risk_level": result.get("risk_level", 0),
        "violations": result.get("violations", []),
        "action_id": result.get("action_id", ""),
    }


def _record_policy_check(backend, action_spec: dict, result: dict) -> None:
    """Append a policy_check WAL event without affecting command output."""
    payload = _policy_check_wal_payload(action_spec, result)
    try:
        if isinstance(backend, CloudClient):
            backend.wal_append_local_queue("policy_check", payload)
        else:
            backend.wal_append("policy_check", payload)
    except Exception:
        # WAL recording is a side effect; keep check-policy output stable.
        pass


# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------

def _dispatch(backend, args) -> dict:
    """Route subcommand to the appropriate backend method."""

    cmd = args.command

    if cmd == "init":
        return backend.init()

    if cmd == "check-policy":
        action_spec = _resolve_action_spec_arg(args)
        result = backend.check_policy(action_spec)
        _record_policy_check(backend, action_spec, result)
        return result

    if cmd == "wal-append":
        payload = _resolve_json_arg(args.payload)
        return backend.wal_append(args.event_type, payload)

    if cmd == "wal-query":
        return backend.wal_query(
            event_type=args.event_type,
            since=args.since,
            last=args.last,
        )

    if cmd == "verify-chain":
        return backend.verify_chain()

    if cmd == "status":
        return backend.status()

    if cmd == "fidelity":
        return backend.fidelity()

    if cmd == "policies":
        return backend.policies(
            scope=args.scope,
            show_inactive=args.show_inactive,
        )

    if cmd == "add-policy":
        return backend.add_policy(
            content=args.content,
            scope=args.scope,
            policy_type=args.policy_type,
            priority=args.priority,
        )

    if cmd == "remove-policy":
        return backend.remove_policy(args.policy_id)

    if cmd == "quarantine":
        return backend.quarantine()

    if cmd == "restore":
        return backend.restore(args.item_id)

    raise ValueError(f"Unknown command: {cmd}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main(argv: list[str] | None = None) -> None:
    """CLI entry point.

    Parameters
    ----------
    argv : list[str] | None
        Command-line arguments (default: ``sys.argv[1:]``).
        Exposed for testability.
    """
    parser = build_parser()
    args = parser.parse_args(argv)
    args.human = bool(getattr(args, "human_root", False) or getattr(args, "human_sub", False))
    args.local = bool(getattr(args, "local_root", False) or getattr(args, "local_sub", False))

    if not args.command:
        parser.print_help()
        sys.exit(1)

    keel_dir = args.keel_dir or os.environ.get("KEEL_DIR", DEFAULT_KEEL_DIR)
    agent = args.agent or os.environ.get("KEEL_AGENT", DEFAULT_AGENT)

    backend = _get_backend(args, keel_dir, agent)

    exit_code = 0
    result: dict | None = None
    _policy_block = False  # True when exit_code=1 is a policy block, not an error

    try:
        result = _dispatch(backend, args)
    except CloudAPIError as exc:
        # Cloud API returned a non-2xx status — use normalise_api_response.
        envelope = normalise_api_response(exc.status_code, exc.body)
        if args.human:
            print(human_output(envelope, args.command))
        else:
            print(json_output(envelope))
        sys.exit(envelope["exit_code"])
    except KeyError as exc:
        result = {"error": str(exc)}
        exit_code = 1
    except ValueError as exc:
        result = {"error": str(exc)}
        exit_code = 1
    except FileNotFoundError as exc:
        result = {"error": str(exc)}
        exit_code = 1
    except NotImplementedError as exc:
        result = {"error": str(exc)}
        exit_code = 1
    except Exception as exc:
        result = {"error": f"Unexpected error: {exc}"}
        exit_code = 1

    # Check for requires_approval or policy violations in successful results.
    if exit_code == 0 and isinstance(result, dict):
        if result.get("requires_approval"):
            exit_code = 2
        if not result.get("passed", True):
            # Covers check-policy, fidelity, verify-chain -- any result with
            # an explicit passed: false should exit 1 (blocked/failed), not 0.
            exit_code = 1
            _policy_block = True

    # Build normalised envelope.
    http_status = getattr(backend, "_last_http_status", None)
    envelope: dict = {
        "exit_code": exit_code,
        "http_status": http_status,
        # Policy blocks (passed: false) go to data, not api_error.
        # api_error is reserved for exceptions and unexpected failures.
        "data": result if exit_code in (0, 2) or _policy_block else None,
        "api_error": result if exit_code == 1 and not _policy_block else None,
    }

    # Format and print.
    if args.human:
        print(human_output(envelope, args.command))
    else:
        print(json_output(envelope))

    sys.exit(exit_code)

