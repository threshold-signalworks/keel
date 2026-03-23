from __future__ import annotations

import shutil
import tempfile
import uuid
from pathlib import Path

import pytest

from keel.cli import main as cli_main


class _FakeLocalBackend:
    def __init__(self) -> None:
        self.append_calls: list[tuple[str, dict]] = []

    def check_policy(self, action_spec: dict) -> dict:
        return {
            "passed": True,
            "risk_level": 0,
            "violations": [],
            "action_id": "action-local-1",
        }

    def wal_append(self, event_type: str, payload: dict) -> dict:
        self.append_calls.append((event_type, payload))
        return {"event_type": event_type, "payload": payload}


def test_check_policy_dispatch_appends_wal_and_returns_original_result() -> None:
    """check-policy should append a side-effect WAL event without changing output."""

    backend = _FakeLocalBackend()
    parser = cli_main.build_parser()
    args = parser.parse_args(
        [
            "check-policy",
            "--action-json",
            '{"surface":"gmail","action_type":"archive","target_ids":["msg-1"],"reversibility":"reversible"}',
        ]
    )

    result = cli_main._dispatch(backend, args)

    assert result == {
        "passed": True,
        "risk_level": 0,
        "violations": [],
        "action_id": "action-local-1",
    }
    assert backend.append_calls == [
        (
            "policy_check",
            {
                "action_type": "archive",
                "target_ids": ["msg-1"],
                "surface": "gmail",
                "reversibility": "reversible",
                "passed": True,
                "risk_level": 0,
                "violations": [],
                "action_id": "action-local-1",
            },
        )
    ]


class _FakeCloudBackend:
    def __init__(self) -> None:
        self.append_calls: list[tuple[str, dict]] = []

    def check_policy(self, action_spec: dict) -> dict:
        return {
            "passed": False,
            "risk_level": 3,
            "violations": ["blocked"],
            "action_id": "action-123",
        }

    def wal_append(self, event_type: str, payload: dict) -> dict:
        raise AssertionError("cloud policy-check side effect should not use wal_append() directly")

    def wal_append_local_queue(self, event_type: str, payload: dict) -> dict:
        self.append_calls.append((event_type, payload))
        return {"queued": True}


def test_check_policy_cloud_path_uses_local_queue(monkeypatch) -> None:
    """Cloud mode should record locally and enqueue via the cloud queue path."""

    monkeypatch.setattr(cli_main, "CloudClient", _FakeCloudBackend)
    backend = _FakeCloudBackend()

    parser = cli_main.build_parser()
    args = parser.parse_args(
        [
            "check-policy",
            "--action-json",
            '{"surface":"gmail","action_type":"delete_soft","target_ids":["msg-9"],"reversibility":"reversible_within_window"}',
        ]
    )

    result = cli_main._dispatch(backend, args)

    assert result == {
        "passed": False,
        "risk_level": 3,
        "violations": ["blocked"],
        "action_id": "action-123",
    }
    assert backend.append_calls == [
        (
            "policy_check",
            {
                "action_type": "delete_soft",
                "target_ids": ["msg-9"],
                "surface": "gmail",
                "reversibility": "reversible_within_window",
                "passed": False,
                "risk_level": 3,
                "violations": ["blocked"],
                "action_id": "action-123",
            },
        )
    ]


@pytest.fixture
def workspace_tmp_dir() -> Path:
    try:
        tmp_dir = Path(tempfile.mkdtemp(prefix=f"keel-{uuid.uuid4()}-"))
    except PermissionError:
        pytest.skip("sandbox prevents temp directory creation")
    try:
        yield tmp_dir
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


def test_check_policy_action_file_matches_action_json(workspace_tmp_dir: Path) -> None:
    """--action-file should resolve identically to --action-json."""

    action_json = (
        '{"surface":"gmail","action_type":"archive","target_ids":["msg-1"],'
        '"reversibility":"reversible"}'
    )
    action_file = workspace_tmp_dir / "action.json"
    try:
        action_file.write_text(action_json + "\n", encoding="utf-8")
    except PermissionError:
        pytest.skip("sandbox prevents temp file writes")

    parser = cli_main.build_parser()
    args_json = parser.parse_args(["check-policy", "--action-json", action_json])
    args_file = parser.parse_args(["check-policy", "--action-file", str(action_file)])

    assert cli_main._resolve_action_spec_arg(args_json) == cli_main._resolve_action_spec_arg(args_file)
