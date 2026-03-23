# Security

## Reporting a vulnerability

If you believe you have found a security vulnerability in Keel or Keel Cloud, please report it privately. Do not open a public GitHub issue.

Email: **security@threshold.systems**

We will acknowledge receipt within 48 hours and aim to provide an initial assessment within 5 working days. Critical issues (authentication bypass, tenant isolation failure, WAL chain integrity compromise) are triaged immediately.

We ask that you give us reasonable time to investigate and remediate before any public disclosure.

## Security model

Keel is a safety and audit layer for tool-using agents. The security model has four priorities, in order:

1. **Tenant isolation.** Each tenant's policies, WAL events, and agent metadata are strictly separated. No cross-tenant data access is possible through the API.

2. **WAL chain integrity.** The write-ahead log is append-only with SHA-256 hash chaining. Each event contains a cryptographic hash of the previous event, making the log tamper-evident. Chain verification is available through both the CLI and the API.

3. **Authentication and access control.** All API and MCP endpoints require bearer-token authentication. Admin operations require a separate credential. Authentication comparisons are constant-time. Failed authentication attempts are rate-limited with automatic lockout.

4. **Abuse resistance.** Request body sizes, query result limits, and verification scan depths are all bounded. Plan-based quotas constrain resource consumption per tenant.

## Deployment assumptions

Keel Cloud is designed to run behind a TLS-terminating reverse proxy (Caddy, nginx, or equivalent). The application server binds to loopback only. TLS, certificate management, and IP-layer rate limiting are the responsibility of the reverse proxy, not the application.

API keys are bearer secrets and must be treated as credentials. They should not be committed to version control, logged in plaintext, or transmitted over unencrypted channels.

## Audit status

A structured internal security review was completed, covering authentication paths, input validation, resource bounding, WAL integrity, tenant isolation, and operational hardening. All findings were remediated and have corresponding regression tests. The audit scope covered both the REST API and MCP transport layers.

## Secure defaults

The Keel CLI operates fully offline with no network access required. Cloud sync activates only when an API key is explicitly configured. Local safety guarantees are never degraded by network conditions.

The policy store and WAL live on disk in the user's home directory (`~/.keel/`). No data is transmitted externally unless Cloud mode is enabled.

## Dependencies

The Keel CLI and core library have zero runtime dependencies beyond the Python standard library. The server component uses asyncpg, FastAPI, and uvicorn, pinned to specific versions.

---

*Threshold Signalworks Ltd, Limerick, Ireland.*
