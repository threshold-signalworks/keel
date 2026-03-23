# Keel

Persistent safety policies and cryptographic audit trails for tool-using agents.

Keel keeps constraints on disk, not in the prompt. Policies survive context compaction.
Every action is logged to a tamper-evident SHA-256 hash chain. Policy enforcement is
deterministic -- no LLM in the enforcement path.

## Install

```
pip install threshold-keel
```

Requires Python 3.10 or later. Zero runtime dependencies.

## Quick start

```bash
# Initialise (creates ~/.keel/ with default safety policies)
keel init

# Check status
keel --human status

# List active policies
keel --human policies

# Add a policy
keel add-policy --content "Block all financial transactions" --scope financial --priority 0

# Check a proposed action against policies
keel check-policy --action-json '{"action_type":"payment","target_ids":["vendor@example.com"],"surface":"financial","reversibility":"irreversible"}'

# Log an action to the WAL
keel wal-append --event-type PROPOSED --payload '{"action_type":"send_email","target_ids":["user@example.com"]}'

# Verify hash chain integrity
keel verify-chain

# Run a full fidelity self-check
keel fidelity
```

## What it does

Keel is a safety layer for autonomous agents that use tools. It provides three things:

**Persistent policies.** Rules live on disk in `~/.keel/store.json`, not in the chat
prompt. They survive context compaction, session restarts, and machine changes. The
agent checks policies before every action. A blocked action stays blocked -- the agent
cannot override it.

**Cryptographic audit trail.** Every action the agent takes is logged to an append-only
write-ahead log (WAL) with SHA-256 hash chaining. Each entry contains a cryptographic
hash of the previous entry, making the log tamper-evident. Chain integrity can be
verified at any time with `keel verify-chain`.

**Deterministic policy evaluation.** The CLI checks policies using structural matching,
not language model interpretation. A constraint scoped to `financial` blocks all actions
on the `financial` surface. No ambiguity, no "I think the user meant...".

## CLI commands

| Command | What it does |
|---------|-------------|
| `keel init` | Create `~/.keel/` directory with default safety policies |
| `keel check-policy` | Check a proposed action against active policies |
| `keel wal-append` | Log an event to the write-ahead log |
| `keel wal-query` | Query recent WAL entries |
| `keel verify-chain` | Verify WAL hash chain integrity |
| `keel --human status` | Show session overview |
| `keel fidelity` | Run a full self-check (policies, WAL, chain) |
| `keel --human policies` | List active policies |
| `keel add-policy` | Add a new policy |
| `keel remove-policy` | Deactivate a policy |
| `keel quarantine` | List quarantined items |
| `keel restore` | Restore an item from quarantine |

All commands output JSON by default. Add `--human` before the subcommand for
human-readable output. Add `--local` to force local mode when cloud credentials
are set.

## Exit codes

| Code | Meaning |
|------|---------|
| 0 | Success / action allowed |
| 1 | Blocked by policy or error |
| 2 | Requires human approval (T2/T3 action) |

## Risk tiers

Keel classifies actions into four risk tiers:

| Tier | Risk | Examples |
|------|------|----------|
| T0 | Read-only | Fetch email, list files, search |
| T1 | Reversible | Create files, add labels |
| T2 | Reversible within window | Archive, move to bin |
| T3 | Irreversible | Send email, permanent delete, publish, pay |

## Cloud sync (optional)

Threshold Cloud adds persistent policy sync across multiple agents, a shared
WAL with web dashboard, compliance-ready audit exports, and real-time monitoring.

| Plan | Price | Includes |
|------|-------|----------|
| **Pro** | EUR 29/mo | Single user, up to 10 agents, dashboard, API access |
| **Team** | COMING SOON | Multi-user, higher agent limit, shared policies, role-based access |

To get started, visit [thresholdsignalworks.com/cloud](https://thresholdsignalworks.com/cloud)
and subscribe. Your API key will be provided on registration.

```bash
export KEEL_CLOUD_API_KEY=sk-keel-your-key-here
keel --human status
```

Local safety continues uninterrupted if the cloud is unreachable. Use `--local`
to force local mode when a cloud key is set.

## Agent integration

Keel ships with a SKILL.md that works as both an OpenClaw skill and a Claude Code
skill (both follow the AgentSkills open standard). In instructions-only mode, the
agent follows Keel's safety rules using file tools directly. When the CLI is
installed, the agent uses it automatically for cryptographic hashing and
deterministic policy checks.

After installing the skill, start a new agent session for Keel to load.

## Security

See [SECURITY.md](SECURITY.md) for the security model and vulnerability
reporting process.

## Licence

Keel is licensed under the Business Source License 1.1 (BSL 1.1) and converts to Apache 2.0 on 2030-03-03.


If you want to offer Keel (or a fork/derivative) as a hosted service, managed service, or embedded commercial product, that is production use and requires a commercial licence. See [LICENSING.md](LICENSING.md)

## Author

Threshold Signalworks Ltd, Limerick, Ireland.
[thresholdsignalworks.com](https://thresholdsignalworks.com)
