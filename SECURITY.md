# Security policy

## Reporting a vulnerability

If you've found a security issue in `pilot-langgraph`, please **do not open a public issue**. Email **founders@pilotprotocol.network** with:

- A description of the issue
- A minimal reproduction (script, network setup, or test case)
- Your assessment of impact + suggested mitigation if any

Expect an acknowledgement within 5 business days. Coordinated disclosure: fix lands on `main`, security release ships, then public disclosure 30 days later (or sooner if you prefer).

## What's in scope

| | |
|---|---|
| Bugs in this repo's Python code | **In scope** |
| Bugs in the Pilot Protocol daemon itself | Report to [pilotprotocol/security](https://github.com/TeoSlayer/pilotprotocol) per its own policy |
| Misconfiguration in user code consuming this plugin | Out of scope (we're happy to add docs) |
| Issues in upstream dependencies (`langchain-core`, `langgraph`, `pydantic`, `opentelemetry-api`) | Report upstream; we'll bump our pin |
| Cosmetic logging that reveals payloads in user-installed apps | Out of scope (handler authors control what they log) |

## What's covered by the threat model

See **`TRUST.md`** for the full security model. Briefly:

- **Pilot Protocol layer** guarantees mutual authentication + AES-256-GCM tunnel encryption between trusted daemons. We trust this layer.
- **Plugin layer** adds per-handler ACL, rate limits, concurrency caps, timeouts, pydantic validation, typed errors.
- **Out of band**: anything requiring access to the local daemon's identity file, the Unix socket, or the worker process's environment.

## Hardening recommendations

For deployments handling sensitive data:

- Run the worker as a **dedicated unprivileged user** (`pilot` user in the reference Dockerfile).
- Restrict the Pilot socket's filesystem permissions (`chmod 600 /tmp/pilot.sock`).
- Use `@pilot_handler("name", allow=[node_ids])` for any handler that does more than read public data.
- Validate **per-payload authorization** inside the handler — ACL is per-handler, not per-resource.
- Enable JSON logging (`--log-format json`) and ship to a SIEM. The `request_id` field correlates client+server events.
- Consider running your own [Pilot rendezvous](https://github.com/TeoSlayer/pilotprotocol#deploying-a-rendezvous) instead of the public one for full data-path control.

## Dependency vulnerabilities

We monitor:

- `langchain-core`, `langgraph` — pinned `>=` so security updates flow automatically with `pip install -U`
- `pydantic` — same
- `opentelemetry-api` (optional `[otel]` extra) — same

If you spot a CVE in a pinned dependency, file an issue and we'll bump.

## Past advisories

None to date. This file will list any issued CVEs once we've published any.
