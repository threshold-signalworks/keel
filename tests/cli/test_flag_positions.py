from __future__ import annotations

import pytest

from keel.cli import main as cli_main


class _DummyBackend:
    _last_http_status = None


def _run_cli(argv: list[str], monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> tuple[int, str]:
    """Run CLI main() and return (exit_code, stdout)."""

    def _fake_get_backend(args, keel_dir: str, agent: str):
        return _DummyBackend()

    def _fake_dispatch(backend, args) -> dict:
        # Deterministic status payload for output comparison.
        return {
            "policy_count": 1,
            "tier0_count": 0,
            "snapshot_hash": "abc123",
            "wal_event_count": 0,
            "chain_valid": True,
            "context_pressure": 0.0,
            "context_usage": 0,
            "agent": "test-agent",
        }

    monkeypatch.setattr(cli_main, "_get_backend", _fake_get_backend)
    monkeypatch.setattr(cli_main, "_dispatch", _fake_dispatch)

    with pytest.raises(SystemExit) as excinfo:
        cli_main.main(argv)

    output = capsys.readouterr().out.strip()
    return int(excinfo.value.code), output


def test_status_human_flag_before_and_after_subcommand_identical(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`keel --human status` and `keel status --human` should be equivalent."""

    exit_before, out_before = _run_cli(["--human", "status"], monkeypatch, capsys)
    exit_after, out_after = _run_cli(["status", "--human"], monkeypatch, capsys)

    assert exit_before == 0
    assert exit_after == 0
    assert out_before == out_after


def test_all_subcommands_accept_human_and_local_after_subcommand() -> None:
    """Every subcommand parser accepts --human/--local after command name."""

    parser = cli_main.build_parser()
    required_args: dict[str, list[str]] = {
        "init": [],
        "check-policy": ["--action-json", "{}"],
        "wal-append": ["--event-type", "PROPOSED", "--payload", "{}"],
        "wal-query": [],
        "verify-chain": [],
        "status": [],
        "fidelity": [],
        "policies": [],
        "add-policy": ["--content", "policy text"],
        "remove-policy": ["--id", "p1"],
        "quarantine": [],
        "restore": ["--item-id", "item-1"],
    }

    for cmd in cli_main.COMMANDS:
        args = parser.parse_args([cmd, "--human", "--local", *required_args[cmd]])
        assert args.command == cmd
        assert getattr(args, "human_sub", False) is True
        assert getattr(args, "local_sub", False) is True
