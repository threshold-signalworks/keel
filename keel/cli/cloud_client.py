"""Keel Cloud Client — HTTP backend with fallback queue.

Implements the ``Backend`` protocol via HTTP calls to the Keel Cloud API.
On **network failure** (``URLError``, ``TimeoutError``, ``OSError``), write
operations fall back to the local backend and are enqueued for later replay.
Read operations fall back to local without queuing.

Cloud mode activates when ``KEEL_CLOUD_API_KEY`` is set and ``--local``
is not passed.
"""

from __future__ import annotations

import json
import sys
import uuid
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Callable

from keel.cli.backend import Backend
from keel.cli.cloud_queue import CloudQueue
from keel.cli.local_backend import LocalBackend


# ---------------------------------------------------------------------------
# Exception
# ---------------------------------------------------------------------------

class CloudAPIError(Exception):
    """Raised when the cloud API returns a non-2xx HTTP status.

    Attributes
    ----------
    status_code : int
        HTTP status code (e.g. 400, 401, 409, 500).
    body : dict
        Parsed JSON error body (always a dict; raw text is wrapped as
        ``{"detail": "<text>"}``).
    """

    def __init__(self, status_code: int, body: dict) -> None:
        self.status_code = status_code
        self.body = body
        super().__init__(f"Cloud API error {status_code}: {body}")


# ---------------------------------------------------------------------------
# Cloud Backend
# ---------------------------------------------------------------------------

class CloudClient(Backend):
    """Cloud implementation of the CLI backend.

    Holds a ``LocalBackend`` for fallback and a ``CloudQueue`` for
    enqueuing write operations on network failure.
    """

    def __init__(
        self,
        keel_dir: str,
        agent: str,
        base_url: str,
        api_key: str,
    ) -> None:
        super().__init__(keel_dir, agent)
        self._base_url = base_url.rstrip("/")
        self._api_key = api_key
        self._local = LocalBackend(keel_dir, agent)
        self._queue = CloudQueue(keel_dir)
        self._last_http_status: int | None = None

    # -- HTTP layer ---------------------------------------------------------

    def _request(
        self,
        method: str,
        path: str,
        *,
        body: dict | None = None,
        params: dict | None = None,
        idempotency_key: str | None = None,
    ) -> tuple[int, Any]:
        """Make an HTTP request to the cloud API.

        Returns ``(status_code, parsed_json_body)``.

        Raises
        ------
        CloudAPIError
            On HTTP 4xx/5xx responses.
        URLError / TimeoutError / OSError
            On network failures (caught by ``_call_or_fallback``).
        """
        url = self._base_url + path
        if params:
            qs = urllib.parse.urlencode(
                {k: v for k, v in params.items() if v is not None}
            )
            if qs:
                url += "?" + qs

        headers: dict[str, str] = {
            "Authorization": f"Bearer {self._api_key}",
        }
        if idempotency_key:
            headers["Idempotency-Key"] = idempotency_key

        data: bytes | None = None
        if body is not None:
            headers["Content-Type"] = "application/json"
            data = json.dumps(body, default=str).encode("utf-8")

        req = urllib.request.Request(
            url, data=data, headers=headers, method=method,
        )

        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                resp_body = resp.read()
                try:
                    parsed = json.loads(resp_body)
                except (json.JSONDecodeError, ValueError):
                    parsed = {}
                return resp.status, parsed
        except urllib.error.HTTPError as exc:
            # Read error body and normalise to dict (fix #5).
            try:
                err_text = exc.read().decode("utf-8", errors="replace")
            except Exception:
                err_text = ""
            try:
                err_body = json.loads(err_text)
                if not isinstance(err_body, dict):
                    err_body = {"detail": str(err_body)}
            except (json.JSONDecodeError, ValueError):
                err_body = {"detail": err_text or str(exc)}
            raise CloudAPIError(exc.code, err_body) from exc

    # -- Fallback orchestration ---------------------------------------------

    def _call_or_fallback(
        self,
        *,
        operation: str,
        method: str,
        path: str,
        body: dict | None = None,
        params: dict | None = None,
        is_write: bool = False,
        local_fn: Callable[[], dict],
        transform_fn: Callable[[Any], dict] | None = None,
        idempotency_key: str | None = None,
    ) -> dict:
        """Try cloud API call; fall back to local on network failure.

        Parameters
        ----------
        operation : str
            Operation name for queue logging (e.g. ``"wal_append"``).
        method : str
            HTTP method (GET, POST, DELETE).
        path : str
            API path (e.g. ``"/api/v1/wal"``).
        body : dict | None
            JSON request body.
        params : dict | None
            URL query parameters.
        is_write : bool
            If ``True``, enqueue on network failure.
        local_fn : callable
            Fallback function returning the Backend-protocol dict.
        transform_fn : callable | None
            Transforms cloud response to Backend-protocol shape.
        idempotency_key : str | None
            Idempotency key for the request (fix #4).
        """
        try:
            status, resp = self._request(
                method, path, body=body, params=params,
                idempotency_key=idempotency_key,
            )
            self._last_http_status = status
            self._try_drain()
            return transform_fn(resp) if transform_fn else resp
        except (urllib.error.URLError, TimeoutError, OSError):
            # Network failure → fallback to local.
            self._last_http_status = None
            result = local_fn()
            if is_write:
                self._queue.enqueue(
                    operation, method, path, body,
                    idempotency_key=idempotency_key or str(uuid.uuid4()),
                )
                print(
                    "[keel] Network error. Executed locally and queued "
                    "for cloud sync.",
                    file=sys.stderr,
                )
            else:
                print(
                    "[keel] Network error. Using local data.",
                    file=sys.stderr,
                )
            return result
        # CloudAPIError propagates to main.py.

    def _try_drain(self) -> None:
        """Attempt queue drain (piggybacked on successful cloud call)."""
        if self._queue.pending_count() == 0:
            return
        drain_result = self._queue.drain(self._replay_call)
        # Log abandoned items to local WAL.
        for item in drain_result.get("abandoned", []):
            try:
                wal = self._local._load_wal()
                wal.log("cloud_sync_abandoned", {
                    "operation": item.get("operation", ""),
                    "path": item.get("path", ""),
                    "reason": item.get("reason", ""),
                    "enqueued_at": item.get("enqueued_at", ""),
                    "idempotency_key": item.get("idempotency_key", ""),
                })
            except Exception:
                pass  # Best-effort WAL logging.

    def _replay_call(
        self,
        method: str,
        path: str,
        body: dict | None,
        idempotency_key: str,
    ) -> int:
        """Replay a queued operation.  Returns HTTP status code.

        Called by ``CloudQueue.drain()``.  Raises on network failure
        (causes drain to stop).
        """
        try:
            status, _resp = self._request(
                method, path, body=body,
                idempotency_key=idempotency_key,
            )
            return status
        except CloudAPIError as exc:
            return exc.status_code
        # URLError / TimeoutError / OSError propagate → drain stops.

    # -- Response transforms ------------------------------------------------

    @staticmethod
    def _transform_wal_query(body: Any) -> dict:
        """Wrap flat event list into Backend protocol shape."""
        events = body if isinstance(body, list) else []
        return {"events": events, "count": len(events)}

    @staticmethod
    def _transform_verify_chain(body: dict) -> dict:
        """Rename server fields to Backend protocol fields."""
        return {
            "chain_valid": body.get("valid"),
            "event_count": body.get("event_count", 0),
            "last_hash": body.get("chain_tip", ""),
        }

    def _transform_status(self, body: dict) -> dict:
        """Map server status shape to Backend protocol shape."""
        return {
            "policy_count": body.get(
                "policy_count_active", body.get("policy_count", 0),
            ),
            "tier0_count": None,
            "snapshot_hash": body.get("policy_snapshot_hash", ""),
            "wal_event_count": body.get("wal_event_count", 0),
            "chain_valid": None,
            "context_pressure": None,
            "context_usage": None,
            "agent": self.agent,
            "cloud_queue_pending_count": self._queue.pending_count(),
            "cloud_queue_abandoned_count": self._queue.abandoned_count(),
            "cloud_queue_abandoned_path": str(self._queue._abandoned_path),
        }

    @staticmethod
    def _transform_policies(body: list, snapshot_hash: str) -> dict:
        """Wrap flat policy list into Backend protocol shape."""
        policies = body if isinstance(body, list) else []
        return {
            "policies": policies,
            "count": len(policies),
            "snapshot_hash": snapshot_hash,
        }

    @staticmethod
    def _transform_remove_policy(body: dict, policy_id: str) -> dict:
        """Add ``policy_id`` to server response."""
        return {**body, "policy_id": policy_id}

    @staticmethod
    def _transform_quarantine(body: Any) -> dict:
        """Wrap flat quarantine list into Backend protocol shape."""
        items = body if isinstance(body, list) else []
        active_count = sum(
            1 for r in items if not r.get("released", False)
        )
        return {"items": items, "active_count": active_count}

    @staticmethod
    def _transform_restore(body: dict, item_id: str) -> dict:
        """Add ``item_id`` and ``original_labels`` to server response."""
        return {
            "released": body.get("released", True),
            "item_id": item_id,
            "original_labels": [],  # Not returned by server release endpoint.
        }

    # -- Backend protocol methods -------------------------------------------

    def init(self) -> dict:
        """Initialise local dirs, create queue dir, test cloud connectivity."""
        result = self._local.init()
        self._queue.ensure_dir()
        result["created"].append(str(self._queue._queue_dir))

        # Test cloud connectivity.
        try:
            status, _body = self._request("GET", "/api/v1/status")
            result["cloud_connected"] = True
            self._last_http_status = status
        except (urllib.error.URLError, TimeoutError, OSError):
            result["cloud_connected"] = False
            self._last_http_status = None
            print(
                "[keel] Warning: cloud API unreachable during init.",
                file=sys.stderr,
            )
        except CloudAPIError as exc:
            result["cloud_connected"] = False
            self._last_http_status = exc.status_code

        return result

    def check_policy(self, action_spec: dict) -> dict:
        return self._call_or_fallback(
            operation="check_policy",
            method="POST",
            path="/api/v1/policies/check",
            body=action_spec,
            is_write=False,
            local_fn=lambda: self._local.check_policy(action_spec),
        )

    def wal_append(self, event_type: str, payload: dict) -> dict:
        body = {
            "event_type": event_type,
            "payload": payload,
            "session_id": self.agent,
        }
        idem_key = str(uuid.uuid4())
        return self._call_or_fallback(
            operation="wal_append",
            method="POST",
            path="/api/v1/wal",
            body=body,
            is_write=True,
            local_fn=lambda: self._local.wal_append(event_type, payload),
            idempotency_key=idem_key,
        )

    def wal_append_local_queue(self, event_type: str, payload: dict) -> dict:
        """Append to the local WAL immediately and queue cloud replay."""
        body = {
            "event_type": event_type,
            "payload": payload,
            "session_id": self.agent,
        }
        idem_key = str(uuid.uuid4())
        result = self._local.wal_append(event_type, payload)
        self._queue.enqueue(
            "wal_append",
            "POST",
            "/api/v1/wal",
            body,
            idempotency_key=idem_key,
        )
        return result

    def wal_query(
        self,
        event_type: str | None = None,
        since: str | None = None,
        last: int | None = None,
    ) -> dict:
        # Fix #1: CLI --type maps to server param "event_type".
        params: dict[str, Any] = {}
        if event_type is not None:
            params["event_type"] = event_type
        if since is not None:
            params["since"] = since
        if last is not None:
            params["limit"] = last

        return self._call_or_fallback(
            operation="wal_query",
            method="GET",
            path="/api/v1/wal",
            params=params,
            is_write=False,
            local_fn=lambda: self._local.wal_query(event_type, since, last),
            transform_fn=self._transform_wal_query,
        )

    def verify_chain(self) -> dict:
        return self._call_or_fallback(
            operation="verify_chain",
            method="GET",
            path="/api/v1/wal/verify",
            is_write=False,
            local_fn=lambda: self._local.verify_chain(),
            transform_fn=self._transform_verify_chain,
        )

    def status(self) -> dict:
        return self._call_or_fallback(
            operation="status",
            method="GET",
            path="/api/v1/status",
            is_write=False,
            local_fn=lambda: self._local.status(),
            transform_fn=self._transform_status,
        )

    def fidelity(self) -> dict:
        """Fidelity is inherently a local operation."""
        return self._local.fidelity()

    def policies(
        self,
        scope: str | None = None,
        show_inactive: bool = False,
    ) -> dict:
        """List policies.  Requires two HTTP calls (policies + snapshot).

        Fix #6: If the snapshot call fails independently, return policies
        with an empty ``snapshot_hash`` rather than falling back entirely.
        """
        params: dict[str, Any] = {}
        if scope is not None:
            params["scope"] = scope
        active_val = None if show_inactive else True
        if active_val is not None:
            params["active"] = str(active_val).lower()

        try:
            status, body = self._request("GET", "/api/v1/policies", params=params)
            self._last_http_status = status
        except (urllib.error.URLError, TimeoutError, OSError):
            self._last_http_status = None
            print(
                "[keel] Network error. Using local data.",
                file=sys.stderr,
            )
            return self._local.policies(scope, show_inactive)

        # Best-effort snapshot fetch (fix #6).
        snapshot_hash = ""
        try:
            _, snap_body = self._request("GET", "/api/v1/policies/snapshot")
            snapshot_hash = snap_body.get("snapshot_hash", "")
        except (CloudAPIError, urllib.error.URLError, TimeoutError, OSError):
            pass  # Snapshot unavailable — use empty string.

        self._try_drain()
        return self._transform_policies(body, snapshot_hash)

    def add_policy(
        self,
        content: str = "",
        scope: str = "global",
        policy_type: str = "constraint",
        priority: int = 0,
    ) -> dict:
        # Server POST /api/v1/policies uses client-provided ID (verified).
        body = {
            "id": str(uuid.uuid4()),
            "scope": scope,
            "type": policy_type,
            "priority": priority,
            "content": content,
            "source": "user_explicit",
            "active": True,
        }
        idem_key = str(uuid.uuid4())
        return self._call_or_fallback(
            operation="add_policy",
            method="POST",
            path="/api/v1/policies",
            body=body,
            is_write=True,
            local_fn=lambda: self._local.add_policy(
                content, scope, policy_type, priority,
            ),
            idempotency_key=idem_key,
        )

    def remove_policy(self, policy_id: str) -> dict:
        idem_key = str(uuid.uuid4())
        return self._call_or_fallback(
            operation="remove_policy",
            method="DELETE",
            path=f"/api/v1/policies/{policy_id}",
            is_write=True,
            local_fn=lambda: self._local.remove_policy(policy_id),
            transform_fn=lambda body: self._transform_remove_policy(
                body, policy_id,
            ),
            idempotency_key=idem_key,
        )

    def quarantine(self) -> dict:
        return self._call_or_fallback(
            operation="quarantine",
            method="GET",
            path="/api/v1/quarantine",
            is_write=False,
            local_fn=lambda: self._local.quarantine(),
            transform_fn=self._transform_quarantine,
        )

    def restore(self, item_id: str) -> dict:
        idem_key = str(uuid.uuid4())
        return self._call_or_fallback(
            operation="restore",
            method="POST",
            path=f"/api/v1/quarantine/{item_id}/release",
            is_write=True,
            local_fn=lambda: self._local.restore(item_id),
            transform_fn=lambda body: self._transform_restore(body, item_id),
            idempotency_key=idem_key,
        )
