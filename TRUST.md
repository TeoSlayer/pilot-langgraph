# Trust + security model

`pilot-langgraph` deploys a two-tier security model: **Pilot Protocol's mutual trust** at the network layer, plus **per-handler ACL** at the plugin's application layer. This document is explicit about what each tier guarantees, what it doesn't, and how to configure them safely.

## Tier 1 — Pilot Protocol (network layer)

**What it does**

- Every Pilot daemon has a long-term Ed25519 identity (`~/.pilot/identity.json`).
- Two daemons can only exchange data after a **mutual trust handshake** (`pilotctl handshake <peer>` or `pilot_langgraph.ensure_trust(node_id)` from each side, OR `--trust-auto-approve` on one side).
- Once mutual trust is established, every packet between the two daemons is **AES-256-GCM encrypted** under per-session keys derived from X25519 ECDH, signed by Ed25519 identities.
- Hostname resolution and the broker (port 1002) require mutual trust.

**What it guarantees**

- An untrusted peer (no mutual handshake) cannot talk to your daemon at all — `dial`, `find`, `subscribe` all fail.
- A passive observer on the network cannot read packet contents (encrypted) or impersonate a peer (signed).
- A peer cannot lie about its node_id or address — those are bound to its Ed25519 keypair.

**What it does NOT guarantee**

- Once trust exists, the peer can call **every handler** the worker exposes by default. Trust is binary, not capability-scoped.
- A daemon's identity file (`identity.json`) is a single point of failure — anyone with read access to it on disk can impersonate that node from anywhere.
- The rendezvous server (e.g. `34.71.57.205:9000`) sees discovery + NAT-traversal control packets but never your data. Trust the rendezvous operator's discretion accordingly, or run your own.

## Tier 2 — Per-handler ACL (plugin layer)

**What it does**

- Each handler can be registered with `allow=[node_id, ...]` (`@pilot_handler("name", allow=[...])` or `WorkerServer.register(..., allow=[...])`).
- Default is open — any mutually-trusted peer can call the handler.
- Calls from disallowed peers receive a typed `PilotUnauthorizedError(error_type="unauthorized")` and the handler is **not** executed.

**What it guarantees**

- A handler with `allow=[42]` cannot be invoked by anyone except node_id 42, even if that other peer is otherwise mutually trusted.
- The check happens before the handler runs and before any pydantic input validation, so malformed payloads from unauthorized callers waste no compute.

**What it does NOT guarantee**

- The plugin trusts what the daemon reports as the caller's node_id. If the daemon itself is compromised, ACL doesn't help. (But Pilot's tunnel encryption + identity binding makes daemon impersonation hard.)
- ACL is per-handler, not per-payload. If `do_thing` accepts a `target` field in payload, two trusted callers can both invoke it on each other's data unless the handler implements its own authorization.

## Defense in depth — typical pattern

```python
from pilot_langgraph import pilot_handler, Context

@pilot_handler(
    "delete_account",
    allow=[ALICE, BOB],            # Pilot-level: only Alice or Bob can call
    rate_per_caller=5, rate_window_secs=60,   # back-pressure
    timeout_secs=10,                # hard cap on runtime
)
async def delete_account(payload, ctx: Context):
    # App-level authorization on top of Pilot-level ACL
    if payload["account_id"] not in OWNED_BY[ctx.caller_node_id]:
        raise PermissionError(f"caller {ctx.caller_node_id} cannot touch {payload['account_id']!r}")
    # ...
```

Notice the four layers protecting one handler:
1. **Pilot mutual trust** — caller is a known peer
2. **ACL** — caller is in the explicit allowlist
3. **Rate limit** — caller hasn't burst more than N calls
4. **Per-payload check** — caller owns the targeted resource

## Operational best practices

| | |
|---|---|
| **Secrets in payload** | Pilot tunnels are encrypted, so payloads are protected on the wire. They are NOT protected in the worker's logs by default — JSON-format log lines include payload metadata (size, request_id) but not the payload itself. If your handler logs the payload (e.g. via `logger.info("got %r", payload)`), redact secrets explicitly. |
| **Identity file rotation** | If a worker's `~/.pilot/identity.json` is ever exposed (backup leak, compromised disk), revoke trust from all peers that trusted that node_id and start the daemon with a fresh identity. The worker's node_id will change. |
| **`--trust-auto-approve`** | Convenient for unattended deployments; equivalent to "any peer that handshakes me is trusted." Combine with strict per-handler ACLs to limit blast radius. |
| **Handlers that take payload->action** | Always validate that the caller is authorized for the specific *thing* they're acting on (per-payload check), not just the handler. ACL alone is coarse. |
| **Public broker (port 1002)** | Anyone mutually trusted can subscribe to any topic. Don't put secrets in topic payloads. Treat broker traffic like internal HTTP — encrypted in transit, but visible to all subscribers. |

## Threat scenarios

| Threat | Mitigated by | Residual risk |
|---|---|---|
| Network observer reads payload | Pilot's AES-256-GCM tunnel encryption | None |
| Peer impersonates another node | Ed25519 identity binding | Disk/backup leak of `identity.json` |
| Untrusted peer invokes handler | Mutual trust requirement | None |
| Trusted peer invokes a privileged handler | ACL `allow=[...]` | Handler bug allowing payload manipulation |
| Trusted peer floods worker | `rate_per_caller` + `max_concurrent` | Multiple trusted peers colluding |
| Worker handler runs forever | `timeout_secs` | None for the affected call |
| Compromised local daemon impersonates user | None — daemon access = identity | Run daemon as a dedicated user; restrict socket perms |

## What this is NOT

- **Not end-to-end encrypted from agent code to agent code.** It's daemon-to-daemon E2E. The payload sits in plaintext inside both worker processes.
- **Not authenticated within the user-facing application.** The plugin treats Pilot trust as the only identity. If you need user-level auth (JWT, OAuth, etc.), validate it inside the handler from `ctx.caller_node_id` + payload.
- **Not zero-trust.** Mutual trust gives the peer access to all open handlers. Restrict explicitly with ACL when needed.

For Pilot Protocol's full security spec, see [the IETF draft](https://www.ietf.org/archive/id/draft-teodor-pilot-protocol-01.html) and `docs/SPEC.md` in the pilotprotocol repo.
