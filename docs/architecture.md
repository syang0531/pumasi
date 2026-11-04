# Architecture (phase 1)

This document records how the private cluster MVP is built and why. The
product-level research and the decisions behind it (dual executor, fiat-only
billing, honest confidentiality tiers) live in the research doc shared with
the project; this file covers the code.

## Crates

| Crate | Role |
| --- | --- |
| `pumasi-proto` | Job spec (`job.toml`), wire messages, framing, tickets, endpoint helpers. No business logic. |
| `pumasi-agent` | `WasmExecutor` (Wasmtime sandbox), `DockerExecutor` (Docker CLI) and the `Agent` session loop. |
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
  slots that offers the job's executor (`NodeInfo.executors`) and whose
  memory is at least the task's memory limit. A docker job waits in the
  queue while no docker-capable node is online.
- Content blobs (the WASM module, and the postcard bundle of a job's shared
  input files) are sent with the first assignment per connection and cached
  by BLAKE3 hash on the agent; later assignments carry only the hash. The
  agent handles assignments in message order so a later hash-only assignment
  always finds its blob. Per-task files travel with each assignment.
- Each placement increments `attempts`, and `TaskRef` carries it, so a late
  report from a node that was already presumed dead is ignored.

### Failure handling

| Event | Effect |
| --- | --- |
| Connection closes | Node offline; its running tasks requeue. |
| No message for `node_timeout` (10 s) | Same as above; the session is killed. |
| Task runs past `timeout + 30 s` | Cancel sent; task requeued elsewhere. |
| Task attempts reach `max_attempts` | Task fails with the reason. |
| Trap, non-zero exit, out of fuel or memory, timeout | Task fails, no retry. |
| Control plane restarts | Running tasks become pending; agents reconnect with backoff. |

## Executors

A job names one executor; everything around it (job file, scheduling, files,
reports, CLI) is shared. Both executors see the same file layout: the job's
shared files plus the task's own files read-only at `/input`, and an empty
writable `/output` whose contents (capped at 32 MiB) are returned in the
report and stored with the task.

### WASM

`WasmExecutor` wraps one Wasmtime `Engine` configured with fuel metering,
epoch interruption and NaN canonicalization. Each task gets a fresh `Store`:

- WASI preview 1 with args, env, stdin and two preopened directories:
  `/input` (read-only rights) and `/output`, both backed by a per-task
  temporary directory. No sockets.
- Memory cap via `StoreLimits` (trap on growth failure).
- Fuel budget: reported back as `fuel_used`, a hardware-independent work
  measure intended for future work-based billing and verification.
- Wall-clock cap and cancellation via a 10 ms epoch ticker and an epoch
  deadline callback.
- stdout/stderr captured in memory, 4 MiB each.

NaN canonicalization plus a sandbox without clocks-as-inputs (future
deterministic mode) is what makes BOINC-style redundant execution possible in
phase 2: run a sample of tasks twice on different nodes and compare outputs.

### Docker

`DockerExecutor` drives the Docker CLI (or a compatible one such as Podman
via `--docker-bin`). It is enabled only when the agent is started with
`--allow-docker` and the daemon answers; the node then advertises
`executors = [wasm, docker]`. Per task:

1. Pull the image if missing (serialized per agent, retried with backoff).
2. `docker create` with `--network none` (unless the job sets
   `network = true`), `--memory`/`--memory-swap`, `--cpus`, `--pids-limit`,
   `--security-opt no-new-privileges`, `--init`, a `pumasi.agent` label, and
   no bind mounts.
3. `docker cp` the input tree into `/input` and an empty `/output`.
4. `docker start -a -i` with stdin, under the task's wall-clock limit;
   cancellation or timeout kills the container.
5. `docker cp` `/output` back, inspect for OOM, `docker rm -f` (also on
   drop, and stale labeled containers are removed at agent start).

Copying instead of mounting keeps host paths out of the container and works
the same with remote daemons and Docker Desktop. Containers share the host
kernel, so this executor is for machines and people the owner trusts; the
phase 3 microVM executor will run the same images with VM isolation.

## Persistence

`state.redb` in the cluster directory holds: `meta` (admin secret, cluster
name), `members`, `tokens`, `jobs` (metadata), `tasks` keyed by
`(job, index)` so a completion rewrites only that task (including its output
files), and `modules` keyed by hash, which holds both WASM modules and input
file bundles.

## Not yet

- High availability: one control plane; Raft (openraft) planned.
- Usage receipts signed by node and cluster (phase 2, for billing).
- Deterministic mode (fixed clock and random seed) for verification.
- WASI preview 2 components, job garbage collection.
- Large files: inputs and outputs travel through the control plane and are
  capped (48 MiB in per job, 32 MiB out per task). Direct node-to-node
  transfer with iroh-blobs would lift that.
