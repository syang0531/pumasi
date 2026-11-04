# Architecture

This document records how the cluster is built and why: the private
cluster of phase 1, the invited providers, receipts, credit ledger and
result checks of phase 2, and the microVM executor of phase 3. The
product-level research and the decisions behind it (dual executor, fiat-only
billing, honest confidentiality tiers) live in the research doc shared with
the project; this file covers the code.

## Crates

| Crate | Role |
| --- | --- |
| `pumasi-proto` | Job spec (`job.toml`), wire messages, framing, tickets, endpoint helpers, receipts (signing and verification), price list and policy. No business logic. |
| `pumasi-guest` | `pumasi-guest`, the init process inside every microVM (a static musl binary), and the disk protocol the agent shares with it. |
| `pumasi-agent` | `WasmExecutor` (Wasmtime sandbox, deterministic mode), `DockerExecutor` (Docker CLI), `MicrovmExecutor` (registry client, Firecracker/QEMU) and the `Agent` session loop, which signs usage. |
| `pumasi-cluster` | `State` (membership, queue, scheduler), `ledger.rs` (accounts, holds), `check.rs` (result checks), `Store` (redb), `Cluster` (iroh server), `AdminClient`. |
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
  bidirectional stream. Handshake: `AgentHello` -> `HelloReply` (which tells
  the node its role and account). Then the
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
  Check runs (below) share the queue.
- `schedule()` runs after every event (submit, connect, disconnect, report,
  tick, deposit). A node is eligible for a task when it is online, not
  suspended, has a free slot, enough memory, and the job's executor among
  the executors the cluster allows it (provider machines: WASM and microVM,
  plus Docker only with an invite that allows it). A node of another account than the
  job's is eligible only while that account's balance is positive. Among
  eligible nodes, the job account's own machines come first (they cost
  nothing), then the most free slots. A docker job waits in the queue while
  no docker-capable node is online.
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
reports, CLI) is shared. All executors see the same file layout: the job's
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
  measure; WASM runs are priced by it and checks compare it.
- Wall-clock cap and cancellation via a 10 ms epoch ticker and an epoch
  deadline callback.
- stdout/stderr captured in memory, 4 MiB each.

NaN canonicalization plus deterministic mode (below) is what makes
BOINC-style redundant execution possible: run a sample of tasks again on
another node and compare the results.

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
microVM executor runs the same images with VM isolation.

### MicroVM

`MicrovmExecutor` runs docker-style jobs (`executor = "microvm"`, same
`DockerSpec`) with one VM per task. It is enabled with `--allow-microvm`
once `pumasi vm setup` has installed a pinned Firecracker CI guest kernel
(Linux 6.1: virtio-mmio, ext4, overlayfs, initramfs), Firecracker on Linux,
and the guest init. Backends:

| Backend | Where | Notes |
| --- | --- | --- |
| Firecracker | Linux with `/dev/kvm` | default when installed; `--no-api --config-file`, its own seccomp filters |
| QEMU | Linux, macOS | `-M microvm` on x86_64, `-M virt` on ARM64; KVM or HVF when available, else TCG (slow; adds `tsc_early_khz` so boot never stalls calibrating the TSC) |

The research doc names libkrun for macOS; QEMU came first because the same
command line covers Linux and macOS and CI can test it. libkrun stays an
option once a Mac runner can exercise it.

Nothing crosses the VM boundary except virtio block devices, which the
guest identifies by content (VMMs attach them in different orders):

| Disk | Contents | Mode |
| --- | --- | --- |
| base | the image root filesystem, ext4 labeled `pumasi-base` | read-only, shared by every task of the image |
| scratch | empty ext4 labeled `pumasi-scratch`, 10 GiB sparse | read-write, one per task |
| input | tar: role entry, `spec.json` (argv, env, workdir, user), `stdin`, `input/...` | read-only |
| output | starts with a marker; the guest overwrites it with a tar: `stdout`, `stderr`, `output/...`, `result.json` | read-write |
| layers | tar of the image's layer blobs (build mode only) | read-only |

Per image, once: the agent resolves the reference with its own OCI
distribution client (anonymous bearer tokens, manifest lists resolved to
`linux/<arch>`, every manifest, config and layer checked against its
SHA-256), makes an empty sparse ext4 (`mkfs.ext4`, no journal), and boots a
*build* VM. The guest unpacks each layer (gzip, zstd or plain tar) in a
child chrooted into the new filesystem, so absolute symlinks resolve inside
it, honoring OCI whiteouts and opaque directories and keeping ownership and
modes. The host never unpacks image tarballs. The disk is cached under
`<vm dir>/images/<digest>/` with the image config; `refs/` maps the
reference to the digest, so a cached image is used without contacting the
registry (like `docker run`).

Per task: the guest mounts base read-only and scratch, stacks an overlay
(upper on scratch) as the new root, copies `/input`, makes `/output` owned
by the task user, mounts `proc`, `sys` (read-only), `devtmpfs`, `devpts`,
`/dev/shm`, writes `/etc/hosts` and `/etc/hostname`, brings up loopback,
resolves the image's `User` against its `/etc/passwd` and `/etc/group`, and
forks the command (entrypoint + job command and task args, or the image's
`Cmd`) with stdout and stderr going to files on scratch, outside the
container's view. PID 1 reaps everything, kills what is left when the
command exits, detects the OOM killer through `/proc/vmstat`, writes the
output disk (capped like the other executors) and reboots, which ends the
VMM. The host kills the VMM at the deadline or on cancellation, then reads
the output disk as data: entries are checked with `validate_rel_path` and
only kept in memory.

Outcomes map like Docker's: exit code as is, death by signal N as exit
`128 + N`, OOM as a trap "out of memory". The VM gets the task's memory
limit plus 96 MiB for the kernel, and `ceil(cpus)` vCPUs. There is no
network device at all, so `network = true` and `gpus` are rejected for
microvm jobs at submission.

### Deterministic mode

With `deterministic = true` (the default for WASM jobs) the assignment
carries a seed, `blake3("pumasi/seed/v1" || job || index)`, the same for
every run of a task. The executor then replaces the WASI clocks with a
virtual clock (starts at 2000-01-01, advances 1 ms per read, so "wait N
seconds" loops still end) and seeds the secure and insecure random sources
from the seed. Together with NaN canonicalization this makes the stdout,
output files, exit status and fuel of an honest run identical on every
machine, which is what checks compare. The job opts out with
`deterministic = false`; its provider runs are then not checked.

## Phase 2: other people's machines

### Roles and invites

A member is either `Owner` (one of the cluster owner's machines, admitted
with `pumasi token`) or `Provider` (someone else's, admitted with
`pumasi invite <account>`). An invite is a join token whose record carries
`Invite { account, allow_docker }`; the role and account are fixed when the
node first joins and kept in its member record. `owner` is the owner's
account and cannot be handed out.

| | Owner machine | Provider machine |
| --- | --- | --- |
| Executors | whatever it offers | WASM, microVM if the agent passes `--allow-microvm`; Docker only if the invite allowed it and the agent passes `--allow-docker` |
| Results | trusted, settle checks | sampled for checks, never settle one |
| Paid | no (same account as the owner's jobs) | yes, held then released |
| Suspended | never | after `max_strikes` failed checks |

Jobs carry an account (`owner` unless `pumasi run --account` says
otherwise). A run on a machine of the job's own account is free and
unchecked.

### Usage receipts

After a run the agent builds `Usage { cluster key, node key, task ref
(job, index, attempt, check), executor, outcome, fuel, wall ms, memory
limit, peak memory (WASM), cpu limit, result digest, finished }` and signs
`"pumasi/receipt/node/v1\0" || postcard(usage)` with its endpoint key. The
result digest is BLAKE3 over the outcome, stdout and output files (sorted by
path), length-prefixed. stderr is left out because the agent appends notes.

The control plane rejects a report unless the signature verifies, the keys
are this node's and its own, the task ref is the run it placed, the digest
matches the stdout and files it received, and the numbers match the report
and the job's limits. A rejected report counts as a strike and the run is
requeued. Otherwise it adds `Billing { consumer, provider, billed wall ms,
amount, check, recorded }` and countersigns
`"pumasi/receipt/cluster/v1\0" || postcard(usage, node sig, billing)`.
`billed wall ms` is the node's figure capped by how long the control plane
saw the run, so a node cannot inflate time. Receipts are stored in the
`receipts` table keyed by run and exported by `pumasi receipts --export` as
JSON rows with readable fields plus a `pumasireceipt...` blob; `pumasi
verify-receipts` checks both signatures and that the readable fields match
the blob.

### Prices and the ledger

Amounts are integer micro-credits. The fixed price list:

- WASM: `fuel × wasm_per_gfuel / 10^9`. Fuel is hardware independent and a
  check run reproduces it, so slow machines are paid the same for the same
  work and a provider cannot pad it unnoticed.
- Docker: `cpus × wall hours × docker_cpu_hour` (one core when unset).
- MicroVM: `vCPUs × wall hours × microvm_cpu_hour`, vCPUs being `cpus`
  rounded up (one when unset). Wall time starts at boot; the first pull of an
  image is not billed.
- All: `memory limit GiB × wall hours × memory_gib_hour`.

Every receipt with a non-zero amount writes, in one redb transaction with
the receipt: the consumer's `Charge` (balance down), the provider's `Hold`
(held up) and a hold record that releases at `recorded + hold_secs`. The
ticker releases due holds (`Release`: held down, balance up) unless the
task is still being checked. A rejected run's hold is `Forfeit`ed and the
consumer gets a `Refund`. `Deposit` and `Payout` are admin bookkeeping.
Balances can go slightly negative: the credit check happens at placement,
and runs already placed are charged when they finish.

### Result checks

When a provider run of a deterministic WASM task finishes with a
reproducible outcome (exit, out of fuel, trap; not timeout or agent error),
the control plane samples it with the node's rate: 0 if checks are off,
1000‰ after any strike, at least 500‰ until the node has passed 10 checks,
else `verify_permille` (default 100‰). A sampled task becomes `Verifying`
and a check run is queued. It goes to a node of a different account than
every earlier run, a trusted (owner) node when one is free.

Runs agree when their digests match and, between equal agent versions,
their fuel matches. A trusted run settles the check. Otherwise two agreeing
untrusted runs settle it, two disagreeing ones call for a third, and three
without a majority make the check inconclusive (task failed, every
untrusted hold refunded, no strikes since nobody can be blamed). Runs that
disagree with the accepted one are forfeited and struck; agreeing provider
runs count as passed checks. If a check run's result wins, it replaces the
task's output. Check runs are billed like any run. A check whose runs keep
failing, or whose job is cancelled, is skipped and the first result kept.

Cheating is caught statistically, not prevented: with rate p a provider
that fakes n results escapes with probability (1-p)^n, and the hold period
keeps its earnings reclaimable meanwhile.

## Persistence

`state.redb` in the cluster directory holds: `meta` (admin secret, cluster
name, policy, layout version), `members`, `tokens`, `jobs` (metadata),
`tasks` keyed by `(job, index)` so a completion rewrites only that task
(including its output files and check record), `modules` keyed by hash,
which holds both WASM modules and input file bundles, `accounts`, `ledger`
keyed by sequence number, and `holds` and `receipts` keyed by run
`(job, index, attempt, check)`.

Postcard is not self-describing, so `meta.schema` versions the record
layout, and older layouts are migrated in place on first open. A phase 1
database (no version) becomes layout 2: members become owner machines, jobs
belong to `owner` and are not deterministic. Layout 3 (phase 3) adds
`microvm_cpu_hour` to the stored policy, starting at the Docker price.
`Executor::Microvm` was appended to the enum, so older records decode
unchanged. `PROTOCOL_VERSION` is 4: agents and the control plane must both
be phase 3 builds.

## Not yet

- High availability: one control plane; Raft (openraft) planned.
- Accounts other than `owner` exist only as ledger entries; consumer
  tickets that let other people submit (and see only their jobs) are next.
- Credits are not connected to a payment provider, and there is no
  platform fee.
- Benchmarks at join and "honeypot" tasks with known answers (needed for
  non-deterministic Docker and microVM work, which checks cannot cover).
- MicroVM: private registries, network (a tap device with egress rules),
  GPU passthrough (VFIO), a snapshot pool for faster boots, and libkrun on
  macOS.
- WASI preview 2 components, job garbage collection.
- Large files: inputs and outputs travel through the control plane and are
  capped (48 MiB in per job, 32 MiB out per task). Direct node-to-node
  transfer with iroh-blobs would lift that.
