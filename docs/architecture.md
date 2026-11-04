# Architecture (phase 1)

This document records how the private cluster MVP is built and why. The
product-level research and the decisions behind it (dual executor, fiat-only
billing, honest confidentiality tiers) live in the research doc shared with
the project; this file covers the code.

## Crates

| Crate | Role |
| --- | --- |
| `pumasi-proto` | Job spec (`job.toml`), wire messages, framing, tickets, endpoint helpers. No business logic. |
| `pumasi-agent` | `WasmExecutor` (Wasmtime sandbox) and the `Agent` session loop. |
| `pumasi-cluster` | `State` (membership, queue, scheduler), `Store` (redb), `Cluster` (iroh server), `AdminClient`. |
| `pumasi-cli` | The `pumasi` binary. Thin: parses args, calls the crates above, prints. |

The marketplace hub is not in this repository; it will be a separate,
closed-source service. The agent and control plane stay Apache-2.0.

## Networking

Every process is an iroh endpoint with a persistent ed25519 key
(`secret.key` in its state directory). Connections are QUIC, end-to-end
encrypted and mutually authenticated by key, with hole punching and relay
fallback in `public` mode. `local` mode disables relays and address lookup
for LANs and tests.

Two ALPNs:

- `pumasi/agent/0`: the agent dials the control plane and opens one
  bidirectional stream. Handshake: `AgentHello` -> `HelloReply`. Then the
  control plane sends `ToAgent::{Assign, Cancel}` and the agent sends
  `FromAgent::{Heartbeat, Done}`. Agents always dial out, so they work from
  behind NAT without port forwarding.
- `pumasi/admin/0`: one `AdminRequest` / `AdminResponse` per stream, each
  request carrying the admin secret.

Frames are a 4-byte big-endian length plus a postcard body, capped at 64 MiB.

### Tickets

A ticket is `prefix + base32(postcard({cluster EndpointAddr, secret}))`.
`pumasijoin...` admits a new node; `pumasiadmin...` grants admin access. Join
tokens have an expiry and an optional use count, are stored in redb, and are
compared in constant time. After a node joins, its endpoint id is a member
and reconnects need no secret.

## Scheduling

- Tasks enter a FIFO queue at submission. Retried tasks go to the front.
- `schedule()` runs after every event (submit, connect, disconnect, report,
  tick). It places each pending task on the online node with the most free
  slots whose memory is at least the task's memory limit.
- The module is sent with the first assignment per connection and cached by
  BLAKE3 hash on the agent; later assignments carry only the hash. The agent
  compiles modules in message order so a later hash-only assignment always
  finds its module.
- Each placement increments `attempts`, and `TaskRef` carries it, so a late
  report from a node that was already presumed dead is ignored.

### Failure handling

| Event | Effect |
| --- | --- |
| Connection closes | Node offline; its running tasks requeue. |
| No message for `node_timeout` (10 s) | Same as above; the session is killed. |
| Task runs past `timeout + 30 s` | Cancel sent; task requeued elsewhere. |
| Task attempts reach `max_attempts` | Task fails with the reason. |
| Trap, non-zero exit, out of fuel, timeout | Task fails, no retry (deterministic). |
| Control plane restarts | Running tasks become pending; agents reconnect with backoff. |

## Executor

`WasmExecutor` wraps one Wasmtime `Engine` configured with fuel metering,
epoch interruption and NaN canonicalization. Each task gets a fresh `Store`:

- WASI preview 1 with only args, env and stdin; no preopened directories, no
  sockets.
- Memory cap via `StoreLimits` (trap on growth failure).
- Fuel budget: reported back as `fuel_used`, a hardware-independent work
  measure intended for future work-based billing and verification.
- Wall-clock cap and cancellation via a 10 ms epoch ticker and an epoch
  deadline callback.
- stdout/stderr captured in memory, 4 MiB each.

NaN canonicalization plus a sandbox without clocks-as-inputs (future
deterministic mode) is what makes BOINC-style redundant execution possible in
phase 2: run a sample of tasks twice on different nodes and compare outputs.

## Persistence

`state.redb` in the cluster directory holds: `meta` (admin secret, cluster
name), `members`, `tokens`, `jobs` (metadata), `tasks` keyed by
`(job, index)` so a completion rewrites only that task, and `modules` keyed by
hash.

## Not yet

- High availability: one control plane; Raft (openraft) planned.
- Usage receipts signed by node and cluster (phase 2, for billing).
- Deterministic mode (fixed clock and random seed) for verification.
- WASI preview 2 components, output files, job garbage collection.
