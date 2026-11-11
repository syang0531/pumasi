# Architecture

This document records how the cluster is built and why: the private
cluster of phase 1, the invited providers, receipts, ledger and
result checks of phase 2, the microVM executor of phase 3, the
releases, OS services and signed agent upgrades of phase 4, the
Windows support of phase 5, the Mac support and availability policy of
phase 6, the benchmarks and compute units of phase 7, the cluster
file store of phase 8, the GPUs and microVM devices of phase 9, the
cluster management of phase 10, the monitoring of phase 11, the web
dashboard of phase 12, the control plane replicas of phase 13 and the
WebGPU executor of phase 14. The
product-level research and the decisions behind it (dual executor, fiat-only
billing, honest confidentiality tiers) live in the research doc shared with
the project; this file covers the code.

## Crates

| Crate | Role |
| --- | --- |
| `pumasi-proto` | Job spec (`job.toml`), wire messages, framing, tickets, endpoint helpers, receipts (signing and verification), price list and policy, the OCI registry client, and `blobs.rs` (iroh-blobs helpers: collections from files and back, resumable push and fetch). No business logic. |
| `pumasi-guest` | `pumasi-guest`, the init process inside every microVM (a static musl binary), and the disk protocol the agent shares with it. |
| `pumasi-agent` | `WasmExecutor` (Wasmtime sandbox, deterministic mode), `DockerExecutor` (Docker CLI), `NodeStore` (the node's file store cache), `MicrovmExecutor` (ext4 formatter, Firecracker/QEMU/Virtualization.framework), `availability` (power, idle and hours policy), `webgpu` (WebGPU adapters and the `pumasi_webgpu` host API), `platform` (sparse files, Windows Job Objects, the WHPX check) and the `Agent` session loop, which signs usage. |
| `pumasi-cluster` | `State` (membership, queue, scheduler), `ledger.rs` (accounts, holds), `check.rs` (result checks), `datasets.rs` (datasets, quotas, secrets, storage billing), `files.rs` (the blob store, its access rules and tags), `transfer.rs` (the CLI's uploads and downloads), `Store` (redb), `Cluster` (iroh server), `AdminClient`. |
| `pumasi-dashboard` | The web dashboard and JSON API behind `pumasi ui` (axum, maud templates, vendored htmx): one admin connection to the control plane, browser sessions holding a ticket each, pages, downloads as zip, live task output as server-sent events. |
| `pumasi-cli` | The `pumasi` binary. Thin: parses args, calls the crates above, prints. `service.rs` installs the agent as an OS service, `upgrade.rs` drives rollouts. |

The marketplace hub, Madang (마당), is not in this repository; it is a
separate, closed-source service in the private `madang` repository. The agent and control plane stay Apache-2.0.

## Networking

Every process is an iroh endpoint with a persistent ed25519 key
(`secret.key` in its state directory). Connections are QUIC, end-to-end
encrypted and mutually authenticated by key, with hole punching and relay
fallback in `public` mode. `local` mode disables relays and address lookup
for LANs and tests.

The relays are n0's public ones unless the control plane runs with
`--relay URL` (an iroh-relay server). Its address, and so every ticket,
then names that relay; `custom_relays` in `pumasi-proto` picks the non-n0
relays out of a ticket, and agents, admin clients (the CLI, `pumasi ui`)
and replicas bind with them as their own relay map. Dialing works through
any relay a peer's address names, so only the control plane needs the
option.

Three ALPNs:

- `pumasi/agent/0`: the agent dials the control plane and opens one
  bidirectional stream. Handshake: `AgentHello` -> `HelloReply` (which tells
  the node its role and account). Then the
  control plane sends `ToAgent::{Assign, Cancel, Upgrade}` and the agent
  sends `FromAgent::{Heartbeat, Done, UpgradeFailed}`. Further streams the
  agent opens on the same connection each carry one `FetchRequest` (a
  chunk of a release binary) and its `FetchReply`. Agents always dial out,
  so they work from behind NAT without port forwarding.
- `pumasi/admin/0`: one `AdminRequest` / `AdminResponse` per stream, each
  request carrying the admin secret.
- `/iroh-bytes/4` (iroh-blobs): the file store, served by the control
  plane and, read-only, by every agent of protocol 8 for its cache (see
  phase 8 below).

Frames are a 4-byte big-endian length plus a postcard body, capped at 64 MiB.

### Tickets

A ticket is `prefix + base32(postcard({cluster EndpointAddr, secret}))`.
`pumasijoin...` admits a new node; `pumasiadmin...` grants admin access
(or, since phase 10, the role of an access ticket). Join
tokens have an expiry and an optional use count, are stored in redb, and are
compared in constant time. After a node joins, its endpoint id is a member
and reconnects need no secret.

## Scheduling

- Tasks enter a queue ordered by job priority, first in, first out within
  a priority (phase 10). Retried tasks go to the front of their priority.
  Check runs (below) share the queue.
- `schedule()` runs after every event (submit, connect, disconnect, report,
  tick, deposit, availability change). A node is eligible for a task when
  it is online, not suspended, not paused by its availability policy, has a
  free slot, enough memory, at least the job's vCPUs (image jobs; a WASM
  task needs one), and the job's executor among
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
`--allow-docker` and the daemon answers with Linux containers (Docker
Desktop in Windows-containers mode is refused with a hint); the node then
advertises `executors = [wasm, docker]`. Per task:

1. Pull the image if missing (serialized per agent, retried with backoff).
2. `docker create` with `--network none` (unless the job sets
   `network = true`), `--memory`/`--memory-swap`, `--cpus`, `--pids-limit`,
   `--security-opt no-new-privileges`, `--init`, a `pumasi.agent` label, and
   no bind mounts, with one exception: NVIDIA tasks that ask for
   `graphics` on an agent with `--docker-wsl-libs` (the default on
   Windows) get `--volume /usr/lib/wsl:/usr/lib/wsl:ro` (see
   [GPU drawing](#gpu-drawing)).
3. `docker cp - <name>:/` streams a tar built in memory: `/input` with the
   job's and the task's files, and an empty `/output` (mode 0777, so any
   image user can write it).
4. `docker start -a -i` with stdin, under the task's wall-clock limit;
   cancellation or timeout kills the container.
5. `docker cp <name>:/output -` streams `/output` back as a tar, read in
   memory up to twice the output cap and parsed like the microVM output
   disk (regular files only, `validate_rel_path`, capped); inspect for
   OOM; `docker rm -f` (also on drop, and stale labeled containers are
   removed at agent start).

Streaming tars instead of mounting or copying directories keeps host paths
out of the container and off the host (no Windows path translation, no
long-path or symlink trouble on NTFS) and works the same with remote
daemons and Docker Desktop. Containers share the host
kernel, so this executor is for machines and people the owner trusts; the
microVM executor runs the same images with VM isolation.

### MicroVM

`MicrovmExecutor` runs docker-style jobs (`executor = "microvm"`, same
`DockerSpec`) with one VM per task. It is enabled with `--allow-microvm`
once `pumasi vm setup` has installed a pinned Firecracker CI guest kernel
(Linux 6.1: virtio-mmio, ext4, overlayfs, initramfs), Firecracker on Linux
or QEMU on Windows, and the guest init (release binaries embed it). Backends:

| Backend | Where | Notes |
| --- | --- | --- |
| Firecracker | Linux with `/dev/kvm` | default when installed; `--no-api --config-file`, its own seccomp filters |
| QEMU | Linux, macOS, Windows | `-M microvm,pic=off,pit=off` on x86_64 (the guest kernel never programs the legacy 8259, so a PIT tick would arrive as vector 0; HVF keeps the PIT to calibrate the TSC), `-M virt` on ARM64; KVM, HVF or WHPX when available, else TCG (slow; adds `tsc_early_khz` so boot never stalls calibrating the TSC) |
| vz | macOS 11+ | Apple's Virtualization.framework in a signed helper process; the default on macOS (phase 6, below) |

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

Disks are sparse files. The ext4 ones come from a built-in formatter
(`microvm/ext4.rs`) rather than `mkfs.ext4`, which Windows lacks and macOS
has only through Homebrew: 4 KiB blocks, 32768-block groups each with its
own bitmaps and inode table, `sparse_super` backups, `filetype`,
`extents`, `ext_attr`, `large_file`, `dir_nlink` and `extra_isize` with
256-byte inodes, no journal and no checksums. Only metadata is written
(zeroed inode tables are valid without `uninit_bg`), so a 10 GiB scratch
disk costs a few hundred KiB. Unit tests run `e2fsck -fn` on it, before
and after `debugfs` writes files; CI boots it with every backend.

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
cpu score, exact µCU, amount (mCU), check, recorded }` and countersigns
`"pumasi/receipt/cluster/v2\0" || postcard(usage, node sig, billing)`.
`billed wall ms` is the node's figure capped by how long the control plane
saw the run, so a node cannot inflate time; `cpu score` is the score the
run was billed at (see "Benchmarks and the CPU score"). Receipts are stored
in the `receipts` table keyed by run and exported by `pumasi receipts
--export` as JSON rows with readable fields plus a `pumasicureceipt...`
blob; `pumasi verify-receipts` checks both signatures and that the readable
fields match the blob. Phase 2 credit receipts (`pumasireceipt...`, domain
v1) are converted when the database is migrated, see "Persistence".

### Compute units and the ledger

Everything inside a cluster is priced in compute units (CU). One CU is one
reference vCPU (score 1000, see below) busy for an hour. The engine knows
nothing about money; a hub may later sell CU, but nothing here converts
them back.

Rates are integer micro-CU (`CuRates`, set with `pumasi prices set`):

- CPU: always 1 CU per reference vCPU-hour, so it is not a setting.
- `memory_gib_hour`: 250 000 µCU (0.25 CU) per GiB of the memory limit per
  reference hour, so 1 vCPU with 2 GiB costs 1.5 CU an hour and with 4 GiB
  2 CU.
- `storage_gib_month` (0.5 CU) and `egress_gib` (0.1 CU): stored in the
  policy now and charged once the file store (phase 8) meters them.

A run's reference time, in nanoseconds, is what it would have taken on the
reference vCPU:

- WASM: `fuel / REFERENCE_FUEL_PER_SEC`. Fuel is hardware independent and a
  check run reproduces it, so slow machines are paid the same for the same
  work and a provider cannot pad it unnoticed.
- Docker and microVM: `billed wall × cpu score / 1000`. A node twice as fast
  as the reference finishes in half the time and earns the same. Wall time
  starts at boot; the first pull of an image is not billed.

The charge is `vCPUs × reference hours` CU plus `memory limit GiB ×
reference hours × memory_gib_hour`, where vCPUs are the Docker CPU limit in
thousandths (one when unset), the microVM's whole vCPUs (`cpus` rounded up),
or one for WASM. `CuRates::charge` returns the exact amount in µCU, which
goes on the receipt. The ledger keeps integer milli-CU: each
(consumer, provider) pair has a `carry` of the sub-milli remainder, so many
tiny runs add up to what one long run would cost instead of rounding to
zero each time.

Every receipt with a non-zero amount writes, in one redb transaction with
the receipt and the carry: the consumer's `Charge` (balance down), the
provider's `Hold` (held up) and a hold record that releases at `recorded +
hold_secs`. The ticker releases due holds (`Release`: held down, balance up)
unless the task is still being checked. A rejected run's hold is
`Forfeit`ed and the consumer gets a `Refund`. Balances can go slightly past
the floor: the balance check happens at placement, and runs already placed
are charged when they finish.

`Policy::ledger` picks how accounts get CU:

- `Prepaid` (default): the owner grants CU with `pumasi cu add` (`Deposit`)
  and takes them out with `Payout`; an account needs a positive balance to
  use other accounts' machines.
- `Mutual { credit_limit }`: money-free pooling (품앗이). Nobody grants CU;
  lending a machine earns them and using others' machines spends them, and
  a balance may go down to `-credit_limit`. Deposits and payouts are
  refused, so balances add up to zero (plus whatever was granted before the
  switch).

### Benchmarks and the CPU score

Each agent measures itself when it starts (unless `perf.json` in its data
directory is younger than a day and of the current `BENCH_VERSION`) and
again every day while idle (`--bench-interval`, `--no-benchmark` turns it
off), and on `pumasi node <id> --rebenchmark`. `pumasi bench` runs the same
measurements locally.

- CPU: a seeded integer kernel (`perf::KERNEL_WAT`: 64-bit multiply, rotate,
  xor and a 4 KiB table) runs in Wasmtime with fuel on, as tasks do, first
  on one thread and then on every offered CPU. The score is iterations per
  second relative to `REFERENCE_ITERS_PER_SEC` (150 M, one hardware thread
  of a 2.5 GHz Skylake-class server core), bounded to 100..5000 for
  billing. A vCPU's score is `min(multi / threads, single)`, so SMT and
  thermal limits count.
- Memory: copy bandwidth over 256 MiB.
- Disk, under the agent's work directory, fio-style with the page cache
  bypassed (O_DIRECT, F_NOCACHE, `FILE_FLAG_NO_BUFFERING`): sequential write
  and read in 1 MiB blocks, 4 KiB random reads at queue depth 1 (p50 and
  p99 latency) and 8 (IOPS). Without direct I/O the class is `unknown`;
  otherwise a p50 of 1.5 ms or more is `hdd`, 700 MB/s sequential or more
  is `nvme`, and the rest `ssd`. The OS's rotational flag is recorded
  beside it.
- SMART: health, wear (percentage used or remaining life), power-on hours
  and media errors from `smartctl -j -a` when installed and permitted.

The profile is signed with the node key (`"pumasi/perf/node/v1\0" ||
postcard(profile)`) and sent as `FromAgent::Perf`; the control plane keeps
it on the member. The round-trip time of each node's selected iroh path,
and whether it is relayed, is read on every heartbeat. `pumasi status`
shows score, disk class and RTT; `pumasi node <id>` the whole profile.

A profile is only a claim, so the control plane times it: when an untrusted
node's profile arrives (or its WASM runs go much slower than its score
says) and the node is idle, it sends `ToAgent::Benchmark { nonce, seed,
iters }` with iterations sized for half a second at the claimed
single-thread score, and no tasks are placed there meanwhile. The agent
runs the kernel and answers with the result; the control plane takes the
elapsed time minus the RTT, recomputes the answer natively
(`perf::kernel_native`) and records `PerfCheck { claimed, measured,
correct, attempts }`. Billing then uses the profile's score capped at
1.25 × the best timing of that claim; a wrong answer bills at the minimum
and counts as a strike. A claim more than twice the timed score is timed
again (at least 5 s later, in case the machine was just busy with its
owner's work) and struck if it still is. Until a node has
been timed, a profile above the reference is billed at 1000. A node with
no profile (older agent or benchmarks off) is billed at 1000. Owner nodes
are trusted and billed at their own score.

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

## Phase 4: distribution and upgrades

### Upgrading to a published release (1.3)

`AdminOp::UpgradeToLatest` (`pumasi upgrade latest`, the dashboard's
Upgrade button, `POST /api/v1/upgrade`) runs on the leader in the
background and reports through `AdminOp::LatestRelease`
(`LatestReleaseView`: the newest tag, the control plane's version and a
`FetchState`). It resolves the tag from the redirect of
`<release source>/latest` (default
`https://github.com/syang0531/pumasi/releases`), reads
`download/<tag>/SHA256SUMS` (at most 64 KiB), and for every platform of a
member not already on that version or newer downloads
`download/<tag>/pumasi-<os>-<arch>[.exe]` (at most 256 MiB, 20 minutes).
A binary is used only if its SHA-256 matches `SHA256SUMS`, its executable
header names that platform and its build marker carries the tag's
version; it then goes into the release store and `start_rollout` signs it
as for `pumasi upgrade rollout`, so agents still check the cluster's
signature, size and hash. A binary already in the store under that hash
is not downloaded again. The tag is checked before it goes into a URL
(letters, digits, `.-_+`).

The control plane and `pumasi ui` remember the path they started from and
`stat` it every 30 seconds (`crates/cli/src/follow.rs`). When a binary
whose build marker is newer appears there (the local agent's upgrade or
the install script) and it answers `--version`, `pumasi ui` restarts at
once and the control plane restarts once `Cluster::rollout_settled` (no
download running, no node waiting, draining or upgrading), the same way
the agent does: `exec` on Unix, exit code 75 under the Windows service
manager.

### Node names (1.3)

`AdminOp::RenameNode` stores `node id -> (admin's name, reported name)`
under `meta.node_names` and puts the admin's name into the member's and
the live node's `NodeInfo` whenever the agent's hello arrives
(`State::apply_node_name`), so every view, event, metric and the built-in
`name` label use it. Names given by an admin are unique; `resolve_node`
(name first, then id prefix) is used by `RemoveNode` too.

### Release builds

`.github/workflows/release.yml` runs on a `v*` tag. It builds `pumasi-cli`
in release mode for six targets: `x86_64`/`aarch64-unknown-linux-gnu`
with cargo-zigbuild against glibc 2.28 (one binary for every distribution
since 2018; until phase 14 these were static musl binaries, but a static
binary cannot `dlopen` the GPU drivers, Vulkan's loader, NVML or CUDA, so
WebGPU and NVIDIA detection never worked in them; the release checks that
no symbol newer than 2.28 is linked and that the binary finds lavapipe),
`x86_64`/`aarch64-apple-darwin` with `MACOSX_DEPLOYMENT_TARGET=10.15` (Apple Silicon builds start at 11.0), and
`x86_64`/`aarch64-pc-windows-msvc`. Assets are raw binaries named
`pumasi-<os>-<arch>[.exe]` (what `pumasi upgrade rollout` takes as is),
`SHA256SUMS`, `install.sh` and `install.ps1`. With a public repository the
job also attests build provenance (`actions/attest-build-provenance`);
GitHub offers attestations for private repositories only on Enterprise
Cloud, so the step is skipped while the repository is private. Pull
requests that touch the release files run the six builds without
publishing.

The version comes from the tag (`PUMASI_VERSION`, read by
`crates/proto/build.rs`); other builds use the crate version, plus
`+<PUMASI_BUILD_ID>` when set. Every binary carries the marker
`pumasi-build-info:v=<version>;p=<protocol>;` (`pumasi_proto::BUILD_INFO`),
so a rollout can read the version and protocol of a binary for another OS
without running it. The platform comes from the executable header (ELF,
Mach-O or PE machine field).

### Services

`pumasi service install` writes a service that runs
`pumasi --home <home> agent start <options>` and starts it:

| OS | System (root) | User |
| --- | --- | --- |
| Linux | `/etc/systemd/system/pumasi-agent.service`, `WantedBy=multi-user.target` | `~/.config/systemd/user/`, plus `loginctl enable-linger` |
| macOS | `/Library/LaunchDaemons/pumasi.agent.plist` in the `system` domain | `~/Library/LaunchAgents/`, `gui/<uid>` domain |
| Windows | SCM service `pumasi-agent`, LocalSystem, automatic start | - |

All restart the agent when it exits (`Restart=always`, `KeepAlive`, SCM
failure actions with `failure_actions_on_non_crash_failures`). On Windows
the service runs `pumasi ... service run -- <options>`, which hands the
main thread to the service dispatcher (`windows-service`) and runs the
agent on its own runtime, logging to `<home>/logs/agent.log`.
`--ticket` stores the ticket as `<home>/agent/join.ticket` (mode 0600);
`agent start` joins with it while there is no membership yet, and deletes
it afterwards.

### Signed rollouts

1. `pumasi upgrade rollout FILE...` reads target, version and protocol of
   each file, refuses a protocol other than its own without `--force`, and
   uploads each binary with `AdminOp::UploadRelease` in 8 MiB chunks. The
   control plane appends them to `<cluster dir>/releases/<sha256>.part`
   and renames the file once its SHA-256 matches (a binary already there
   is not sent again).
2. `AdminOp::Rollout` makes the control plane sign, per binary,
   `"pumasi/release/v1\0" || postcard(Release { cluster key, ReleaseFile
   { target, version, protocol, sha256, size }, created })` with its
   endpoint key, the key agents dialed when they joined. The record is
   stored under `meta.rollout`; binaries of earlier rollouts are deleted.
3. Each agent tells the control plane in its hello whether it can upgrade
   itself: the SHA-256 of its binary and its opt-in (`UpgradeHello`).
   Owner machines accept by default, providers only with
   `--auto-upgrade`. A node is due when the rollout has a binary for its
   platform with another hash, it accepts, and it has not failed with
   that binary. Every scheduling pass sends `ToAgent::Upgrade` to due nodes
   with no running task; of the due nodes that are busy, one at a time is
   drained (gets no new tasks). A node sent a release gets no tasks until
   it reconnects, reports a failure, or 15 minutes pass.
4. The agent checks the release (signature against its cluster's key,
   target, opt-in, not failed before) before downloading anything, then
   fetches the binary in chunks over its own connection and checks size and
   SHA-256. It copies it next to its binary (same filesystem), runs
   `<new> --version` (must exit 0 and print the release version), backs the
   running binary up to `<data dir>/upgrade/previous`, writes
   `upgrade/pending.json` and renames the new file over the binary
   (Windows: the running file is renamed to `<exe>.old` first, since a
   running executable can be renamed but not overwritten). `serve` returns
   `AgentExit::Upgraded`; the CLI `exec`s the binary again with the same
   arguments on Unix (same PID, so systemd and launchd keep tracking it),
   spawns it on Windows, or under the Windows service exits with code 75,
   which the failure actions turn into a restart.
5. The restarted agent sees `pending.json` with its own hash: it is on
   trial. The first `Welcome` commits the upgrade (deletes the file). A
   `Rejected` reply, 5 minutes without reaching the cluster, or a fourth
   start without a welcome (a crash loop under a service manager) puts
   `previous` back, records the release in `upgrade/failed.json` and
   restarts again. Refusals and failures go to the control plane as
   `FromAgent::UpgradeFailed` and show in `pumasi upgrade status`.

The cluster key is the trust root, so the admin secret (which can start a
rollout) is as powerful as root on every machine that accepts upgrades;
providers keep the choice with `--auto-upgrade`. The control plane itself
is upgraded by hand: replace its binary and restart it before rolling out
agents of a newer protocol.

## Phase 5: Windows

Windows was already a release target, a service host and a WASM executor.
Phase 5 makes Docker and microVM jobs work there too.

### Docker Desktop

Docker Desktop runs Linux containers in a WSL 2 VM and exposes the usual
API on `//./pipe/docker_engine`, which the Docker CLI uses as is. What
changed in the executor is everything that touched host paths: inputs and
outputs travel as tar streams (above), so nothing depends on how Windows
paths, ownership, symlinks or long paths map into the VM. `detect` refuses
a daemon in Windows-containers mode and, for a service whose `PATH` does
not have Docker yet, also looks in `C:\Program Files\Docker\Docker\resources\bin`.
The attached `docker start` CLI is put in a Job Object (below). Docker
Desktop runs only in a signed-in session, so a headless machine offers
WASM and microVM only.

### QEMU with WHPX

The research doc's choice: QEMU with the Windows Hypervisor Platform reuses
the phase 3 QEMU code; Hyper-V's HCS would be faster to boot but is a much
larger integration, deferred. The QEMU command differs from Linux in four
places:

| | Windows | Why |
| --- | --- | --- |
| Accelerator | `-accel whpx,kernel-irqchip=off -accel tcg` | QEMU's own PIC/IOAPIC/LAPIC; WHPX's in-kernel APIC trips up Linux guests on some builds. TCG is the fallback when WHPX cannot start. |
| CPU model | `-cpu Nehalem` under WHPX, `max` otherwise | WHPX takes CPUID leaf 1 from QEMU's model, and `max` there is TCG's list; Nehalem is x86-64-v2, which every WHPX host has |
| TSC | `tsc_early_khz=<host rate>` | no kvm-clock under WHPX; the agent measures the host TSC against the system clock (50 ms) and the guest reads the same TSC |
| Console | `-serial file:<dir>/serial.log` | QEMU's stdio console polls a console input handle that services and NUL stdin lack |

`PUMASI_QEMU_ACCEL` (e.g. `tcg`, `whpx`) and `PUMASI_QEMU_CPU` override the
first two for trying a new machine. Whether WHPX is usable is checked by
loading `WinHvPlatform.dll` from System32 at run time (it exists only with
the feature on) and asking `WHvGetCapability(HypervisorPresent)`; `pumasi
vm status` prints the reason when it is not.

WHPX being present does not mean it works. Nested under Hyper-V (cloud
Windows VMs, GitHub's hosted runners) QEMU's virtio-mmio disks break under
it: the device sees garbage ring indices ("Guest says index 12855 is
available") and the guest never gets its disks. So when the microVM executor
starts on Windows with WHPX it boots one probe VM (guest `probe` mode: copy
a 4 MiB file from the input disk to the output disk) and compares the copy.
If that fails it logs why and runs every microVM in TCG instead, which is
slow but correct. `PUMASI_QEMU_ACCEL` skips the probe. Two more WHPX
quirks are handled in the command: a guest reset is a triple fault that
leaves WHPX stuck ("Unexpected VP exit code 4"), so guests reboot through
ACPI (`reboot=a`), and the host also watches the serial log and stops a
QEMU still running 3 s after the guest said it was restarting.

`pumasi vm setup` on Windows x86_64 downloads a pinned QEMU installer
(Stefan Weil's build, the one qemu.org links to; it has `microvm` and
`whpx`), checks size and SHA-256 while streaming it to disk, and runs it
silently (`/S /D=<vm dir>\qemu`). The installer registers itself for all
users, so this needs an administrator shell; a plain shell gets
`ERROR_ELEVATION_REQUIRED` and a message saying so. A marker file names the
installer, so a newer pin replaces the install. Windows on ARM gets no
QEMU (there is no upstream build); a QEMU on `PATH` would run guests in TCG.

### Job Objects, sparse files, long paths

- Each VMM and each attached Docker CLI goes into its own Job Object with
  `KILL_ON_JOB_CLOSE`. The handle lives as long as the run; if the agent
  dies, Windows closes it and the process dies with it. (Only the
  executor's children are in jobs, not the agent, so the upgrade path that
  spawns the new binary is unaffected.)
- NTFS files are not sparse unless asked: every disk is created with
  `FSCTL_SET_SPARSE` before it is extended.
- Rust's standard library handles paths over 260 characters; QEMU does not,
  so the agent warns when the VM directory path is long enough for task
  disks to cross that limit.

### Firewall

`pumasi service install` adds an inbound UDP rule for the binary on private
and domain networks (`netsh advfirewall`), since a service never gets the
firewall prompt and its direct QUIC paths would be dropped (the relay still
works). Public networks are left closed. `service uninstall` removes it.

### CI

The `microvm (windows, qemu)` job builds the guest init on Linux, embeds
it (`PUMASI_EMBED_GUEST`), runs `pumasi vm setup` (kernel and the pinned
QEMU installer, cached between runs), boots an image with `pumasi vm run`,
and runs the microVM end-to-end test. Hosted runners have nested WHPX with
the broken disks described above, so the probe VM fails there and the runs
go through the TCG fallback; WHPX itself is verified on a real PC. The release workflow builds
the guest for x86_64 and aarch64 and embeds the matching one in all six
binaries.

## Phase 6: Macs and availability

### Old Intel Macs

The x86_64 macOS binary is built with `MACOSX_DEPLOYMENT_TARGET=10.15`
(Rust supports 10.12), the last macOS of the oldest 2013-2014 Macs. CI
builds it on the `macos-15-intel` runner, checks the Mach-O minimum OS
with `vtool` and that Virtualization.framework is not among the linked
libraries, and runs the tests and the service/upgrade script there.
Catalina Macs run WASM jobs; Docker Desktop and current QEMU builds do not
support these macOS versions, so Docker stays off on old Macs.

### Virtualization.framework

The `vz` backend replaces the untested macOS QEMU path as the default from
macOS 11 (QEMU stays selectable). Two constraints shape it:

- The framework serves only processes signed with the
  `com.apple.security.virtualization` entitlement; an ad-hoc signature is
  enough. `pumasi vm setup` (and the agent when it starts) writes a copy of
  the running binary to `<vm dir>/pumasi-vz` and signs it with
  `/usr/bin/codesign`; `pumasi-vz.source` holds the BLAKE3 of the binary
  it came from, so an upgraded agent re-signs a fresh copy. Each VM runs as
  `pumasi-vz __vz-run <dir>/vz.json`, which `main` intercepts before
  parsing arguments: a VMM process like Firecracker or QEMU, killed at the
  deadline. The agent's own binary is never re-signed, because upgrades
  compare its SHA-256.
- macOS 10.15 lacks the framework, and a linked framework that is missing
  stops a binary from starting. So `microvm/vz.rs` loads it with `dlopen`
  and talks to it through the Objective-C runtime (`objc2` for messages,
  `block2` for the start completion handler); classes are looked up by
  name.

The helper builds a `VZVirtualMachineConfiguration`: `VZLinuxBootLoader`
with the kernel, the initramfs and the command line (`console=hvc0`), one
`VZVirtioBlockDeviceConfiguration` over a `VZDiskImageStorageDeviceAttachment`
per disk (raw images; the agent already pads them to 4 KiB), a virtio
console on its stdout (the task's `console.log`) and an entropy device,
with CPUs and memory clamped to what the framework allows. It validates
the configuration, starts the VM on the main queue and runs the main run
loop until the VM stops; a guest reboot stops a VZ VM. The agent also
watches `console.log` for the kernel's "reboot:" line, as it does for
WHPX, in case a VM lingers.

VZ devices are virtio-pci; the Firecracker 1.10 kernels used elsewhere have
virtio-mmio only. Firecracker's 1.15 CI configuration for 6.1 has PCI and
virtio-console, so Apple Silicon boots its arm64 `Image` (6.1.155) as is.
On Intel, `VZLinuxBootLoader` boots a bzImage, which Firecracker does not
publish: `scripts/kernel/build.sh` builds one from the same configuration
(pinned source and config hashes, fixed build timestamp, user and host), and
`.github/workflows/guest-kernel.yml` runs it on every change to the script.
On main, the first build whose SHA-256 equals the pin in `setup.rs` is
published as the prerelease `guest-kernel-6.1.155-1` (not "latest", so the
install scripts keep finding the pumasi release); later builds are compared
with it. The kernel file is `vz-kernel` on macOS so a phase 3 `vmlinux`
there is not used by mistake. Kernels after 6.1 have an ACPI regression
under Virtualization.framework on x86_64, one more reason to stay on 6.1.

Hosted Mac runners are VMs without nested virtualization, so CI checks the
helper up to the framework's answer (signing, entitlement, the
`__vz-run` path through the Objective-C calls); guests are booted on real
Macs.

### Availability policy

`availability.rs` holds the owner's policy (`--only-on-ac`,
`--idle-minutes`, `--hours`) and a `Probe` that reads the machine:

| | Power | Idle time |
| --- | --- | --- |
| macOS | `pmset -g batt` | `HIDIdleTime` of `IOHIDSystem` via `ioreg`: system-wide, so a LaunchDaemon sees it |
| Linux | `/sys/class/power_supply`: on battery when a battery discharges and no mains/USB supply is online | most recent of terminal access times (`/dev/tty<N>`, `/dev/pts/*`, as `w` shows) and logind's idle hint for graphical sessions; nobody logged in is idle |
| Windows | `GetSystemPowerStatus` | `GetLastInputInfo`, own session only; a service in session 0 reports that it cannot tell |

No battery counts as AC power; idle time that cannot be read pauses the
node with the reason instead of guessing. The agent evaluates the policy
every 15 s on a blocking thread and keeps the answer in a `watch` channel.
The hello carries the state at connect time, so a node that joins on
battery is never placed a task, and `FromAgent::Availability` reports each
change. The control plane keeps it as `NodeRuntime::paused`: paused nodes
get no new tasks, running tasks finish (a laptop that then sleeps is
handled like any node that goes silent), and `NodeView::paused` shows the
reason in `pumasi status`.

Two related defaults: `--max-cpu-percent` makes the node report that share
of its CPUs as `NodeInfo::cpus`, which the scheduler now compares with an
image job's vCPUs (and Docker containers of jobs without a CPU limit get
`--cpus` at the cap); and default slots are one per offered CPU but at most
one per GiB of memory beyond the first two, so a 4 GiB laptop runs two
tasks at once.

## Phase 8: the cluster file store

### Store and tags

The control plane runs an iroh-blobs file store in `<cluster dir>/blobs`.
Everything in it is content-addressed by BLAKE3 and verified in 16 KiB
chunk groups on the way in, so a transfer can stop anywhere and resume
from what the receiver already has. A set of files is a *collection* (a
hash sequence of the names blob and the files), and a dataset version,
a task's output, a job input and a private image are each one collection.

Tags are the references garbage collection keeps; everything without a
tag is deleted by the store's GC (every `gc_interval`, 10 minutes):

| Tag | Keeps |
| --- | --- |
| `ds/<name>/<version>` | a dataset version |
| `out/<job>/<index>`, `chk/<job>/<index>/<attempt>` | a task's (or check run's) stored output until the job's output dataset is made |
| `in/<job>/<root>` | a collection an unfinished job mounts |
| `up/<root>` | a committed upload no job has used yet (24 hours) |
| `img/<job>/<arch>/<digest>` | a private image an unfinished job runs |
| `job/<id>` | a job's combined outputs while they become a dataset |

The control plane's state (redb) is the source of truth; a worker task
applies tag changes the state machine sends it, and on start it reconciles
the store's tags with what the state says should exist. Data pushed but not
yet named by a report or commit is kept for two hours by a protect
callback, so the GC cannot delete an upload between its push and its
commit. Removing a dataset deletes its tag; the space comes back at the
next GC once no other tag shares the blobs.

### Datasets, uploads and downloads

`pumasi store put` imports the files into a temporary local store (by
reference, so nothing is copied), asks the control plane what it lacks of
the collection (`StoreUpload`, which also lets the CLI's endpoint push that
one collection for an hour), pushes only the missing chunk ranges and then
commits (`StoreCommit`): a new version of the dataset, unless the root is
the same as the latest version's. The quota check (account quota or the
default) happens at commit. An iroh-blobs push returns once the data is
written to the stream, so the pusher then observes the receiver's bitfield
of the last blob it sent until it is complete before closing the
connection.

`pumasi store get` (`StoreOpen`) lets the CLI's endpoint read the named
collections for an hour, fetches them into `<dest>/.pumasi-partial` (an
iroh-blobs store, so a second run continues where the first stopped) and
then moves the files out. Bytes the control plane serves to the CLI are
charged as egress to the dataset's (or job's) account.

### Jobs

`files = ["store:NAME[@VERSION][/PATH]"]` becomes a `MountRequest`; at
submission the control plane resolves it to a `Mount` (collection root,
selected path, place under `/input`) and fixes the version, so a job reads
the same data on every retry. `{shard}` in a reference makes per-task
mounts. Local `files` over 16 MiB are uploaded first and mounted from the
upload. `output = "store:NAME"` sets the job's output dataset.

Tasks with mounts, an output dataset or a private image go only to agents
of protocol 8 or later. The assignment carries the mounts, the output mode
(`Inline` for old agents, `Auto`: in the report up to 32 MiB, else stored,
or `Store`), the private image and up to four peers that have the
collections cached. The agent fetches each mount into its cache from the
peers first and from the control plane otherwise (four attempts, keeping
partial data), places the selected files in a staging directory that
becomes `/input` (preopened for WASM, streamed as a tar into Docker,
written to the input disk of a microVM), and pushes a stored output as a
collection before it reports `FromAgent::Report { report, stored }`. The
control plane checks that the collection is complete in its store and that
its size matches before it accepts the result; `output_digest` (result
digest v2) includes the stored root, so a check run that stored different
bytes disagrees. Leases of store tasks get 10 minutes plus one second per
4 MiB mounted on top of the task timeout.

When a job ends, the stored outputs of its tasks are combined into one
collection (`task-<index>/...`) and become a version of the output dataset,
or of `job-<id>`; the per-task tags are then dropped.

### Node caches

Each agent of protocol 8 keeps its own iroh-blobs store in
`<agent dir>/store` with a `cache/<root>` tag per cached collection and a
size cap (`--store-cache`, 20 GiB). Least recently used collections not in
use by a running task are untagged when it is over the cap, and the
store's GC reclaims them. The agent tells the control plane what it has
(`FromAgent::StoreState`, a full list at connect and changes after), which
the scheduler uses for the peer list. Agents serve their cache read-only
on the blobs ALPN; a collection's hash works as the read capability, and
nodes learn only the hashes of what they are given.

### Private images

`pumasi secret set registry/<host> user:password` stores a login in the
`secrets` table (it is never sent to nodes and `secret ls` shows only
names). A Docker or microVM job whose image's registry has a login is
*private*: before its tasks can run, the control plane resolves the image
for each architecture among the nodes that could run it, downloads the
manifest, config and layers with the login, and imports them as one
collection (`image.json`, `config.json`, `layers/<digest>`). Nodes fetch it
like a mount, from peers or the control plane. Docker nodes turn it into a
`docker save` archive streamed into `docker load` as
`pumasi-image:<digest>`; microVM nodes build their read-only root disk from
the layer files the same way as from a registry pull. A node that already
has the digest loaded skips the download. If the image cannot be pulled
on any architecture, the job's pending tasks fail with the reason, shown in
`pumasi job` as *preparing*.

### Billing and quotas

Every `storage_bill_every` (an hour) the control plane charges each
account `bytes x seconds x rate / (GiB x 30 days)` for its dataset
versions, uploads and the stored outputs of unfinished jobs, as `Storage`
ledger entries paid to the owner. The rate depends on the control plane's
disk class (`cluster start --storage-class`): `StoragePolicy` holds the
HDD and NVMe rates, and `CuRates::storage_gib_month` is the SSD (and
unknown) rate. Sub-milli-CU amounts carry over like task charges.
Downloads are `Egress` entries at `CuRates::egress_gib`. Accounts have an
optional quota in bytes, with a default in the storage policy.

## Phase 9: GPUs and microVM devices

### Device profiles

Besides the performance profile, an agent of protocol 9 sends a
`DeviceProfile` signed with its node key (`pumasi/devices/node/v1`): every
GPU with its vendor, PCI address, memory, driver, compute capability,
benchmark (FP32 GFLOPS, memory bandwidth, challenge hash rate), whether it
drives a display, and which executors may use it (`docker`, `vfio`), plus
whether its microVMs can have a network card. The control plane verifies
the signature, keeps the latest profile per member (`member_devices`) and
shows it in the status and `pumasi node`. Detection (`crates/agent/src/gpu`)
never fails an agent: what it cannot find goes into the profile's notes.

- NVIDIA: NVML is loaded at run time (`libnvidia-ml.so.1`, `nvml.dll`), so
  machines without the driver simply have no NVIDIA GPUs. The benchmark
  uses the CUDA driver API (also loaded at run time) with PTX generated by
  the agent: an FMA loop for FP32, a device-to-device copy for bandwidth,
  and the challenge kernel for the hash rate. The PTX hash kernel is
  generated from a small instruction list that a unit test runs on the CPU
  against `gpu_hash`, so the GPU and CPU agree on every answer.
- AMD (Linux): sysfs gives the PCI devices, memory size and render node;
  the KFD topology gives SIMD count and clock, from which FP32 is
  estimated (16 lanes per SIMD on GCN/CDNA, 32 on RDNA, an FMA per lane).
- macOS lists GPUs from `system_profiler` for information only.
- A GPU is `docker` when Docker can reach it (NVIDIA Container Toolkit:
  an `nvidia` runtime, a CDI spec or `nvidia-container-cli`; Docker
  Desktop on Windows; AMD needs `/dev/kfd` and the render node), and
  `vfio` when it is bound to `vfio-pci`, every other member of its IOMMU
  group is bound to `vfio-pci`, driverless or a bridge, `/dev/vfio/<group>`
  opens, it drives no display, and the GPU guest bundle and QEMU with KVM
  are installed.

### Requests and scheduling

`JobSubmission::gpu` is a `GpuRequest`: a count per task (0 for all of the
node's GPUs), an optional vendor and a minimum memory. job.toml's `gpus`
and `gpu = {vendor, min_vram_gb}` fill it; the old `docker.gpus` string is
refused in new submissions, and stored jobs that had it become "all NVIDIA
GPUs" when loaded. GPU requests are for image jobs only.

The scheduler treats GPUs like slots: a node is eligible only when it
speaks protocol 9, is not being timed (below), and `GpuRequest::pick`
finds enough free GPUs that match and that the job's executor may use.
It picks the smallest GPUs that fit, so big ones stay free for jobs that
need them, records them as busy for that task (`NodeRuntime::gpu.busy`),
and names them in `TaskAssignment::gpus` (trailing field). A node without
GPUs, or with too few free ones, never gets the task. The agent maps the
indices back to its profile: Docker gets `--gpus "device=<uuid>,..."`
(NVIDIA) or `--device /dev/kfd --device <render node>` with the groups that
own those nodes (AMD), and `PUMASI_GPUS` lists the indices; microVMs get
the GPU and its IOMMU group passed through.

### Billing

`Metered` carries the summed GFLOPS and memory of the run's GPUs, and
`CuRates` two new rates: `gpu_tflops_hour` (0.1 CU per TFLOPS-hour) and
`gpu_vram_gib_hour` (0.3 CU per GiB-hour), charged on the billed wall time
like memory. The receipt gets a trailing `GpuBilling` (GPU names, GFLOPS,
memory and the GPU part of the exact price) and is countersigned under a
new domain (`pumasi/receipt/cluster/v3`) when it has one; receipts without
GPUs keep the v2 message, so old receipts verify unchanged and
`Receipt::decode` reads both layouts.

The GFLOPS billed for a GPU are the profile's for owner machines. For
providers, a GPU is timed by the control plane before it gets work: a
`GpuChallenge` asks for 64 values of `x` with `gpu_hash(seed, x) < target`,
where the target is sized for half a second at the claimed hash rate. The
control plane checks every answer and divides the expected work by the
round-trip-corrected time. A wrong answer is a strike and bills 0; a GPU
slower than claimed (with 25% slack) is billed in proportion, and one
claiming more than twice what was timed is timed again and struck the
second time. A GPU that cannot run the challenge (no CUDA, hash rate 0) is
billed at `REFERENCE_GPU_GFLOPS` (10 TFLOPS) at most. After GPU runs on a
provider, its GPUs are timed again at the result-check rate (at most every
10 minutes per GPU). This keeps the billed throughput honest; it does not
check results, which for GPU work are not deterministic (see "Not yet").

### MicroVM network

A microVM task with `network = true` boots on QEMU with a virtio-net card
attached to `-netdev socket,connect=127.0.0.1:<port>`. The agent listens
on that port (`microvm::net::Gateway`, one per task, closed with the VM)
and runs a user-space TCP/IP stack (smoltcp) as the guest's gateway
`10.0.2.2`; the guest is `10.0.2.15/24` (the guest init configures `eth0`,
the default route and `resolv.conf` from `RunSpec::network`). smoltcp runs
in any-IP mode: when the guest sends a SYN to an allowed destination, the
gateway adds a listening socket for exactly that address and port before
the frame is processed, then connects from the host to the same address
and copies bytes both ways with bounded buffers (256 KiB per direction).
Refused destinations have no socket, so smoltcp answers with a reset.
Allowed means public unicast IPv4: not RFC 1918, CGNAT, loopback,
link-local, multicast, broadcast, documentation, benchmarking or reserved
ranges, not an address this host can bind (its own), and not port 25. DNS
queries to the gateway are answered from the host's resolver (A records,
filtered the same way). Other UDP, ICMP and IPv6 are dropped. Firecracker
cards need TAP devices and root, so Firecracker nodes run network tasks on
QEMU when it is installed, and report `microvm_network` accordingly; the
scheduler sends network tasks only to nodes that report it.

### GPU passthrough (experimental)

With GPUs in the assignment, the microVM executor boots QEMU's `q35`
machine (a PCI bus; disks and the network card become virtio-pci) with a
`vfio-pci` device for every function in the GPU's set, the GPU guest
kernel (`<vm dir>/gpu/vmlinux`: Firecracker's 6.1 configuration with
loadable modules) and a read-only driver disk labeled `pumasi-gpu`
(`drivers.ext4`). The guest init mounts it, loads the modules in
`modules.order` with `finit_module`, creates `/dev/nvidiactl`,
`/dev/nvidia<N>` and `/dev/nvidia-uvm*`, bind-mounts the driver's
libraries and tools at `/usr/local/nvidia/{lib64,bin}` (where CUDA images
look) and adds them to `LD_LIBRARY_PATH` and `PATH`.
`scripts/kernel/build-gpu.sh` builds the bundle from pinned kernel sources
and an NVIDIA driver release (its open kernel modules, so Turing and
newer). It has not run in CI: GPU hosts are checked by hand.

## Phase 10: cluster management

Everything in this phase lives in the control plane (`manage.rs`,
`access.rs`); agents and the agent protocol are unchanged.

### Labels, taints and selectors

A node's labels are its built-in labels, computed from what it reports
(`name`, `os`, `arch`, `role`, `account`, `executor` and `gpu` with one
value per executor or GPU vendor, `gpus`, `disk`), plus the user labels in
its `NodeConfig`. Label sets are multi-valued, so `executor=docker` matches
a node that offers WASM and Docker. User labels cannot reuse a built-in
key. A `Selector` is a list of terms, all of which must hold: `key=a|b`
(In), `key!=a` (NotIn, also true when the key is missing), `key`
(Exists) and `!key` (NotExists); job.toml writes it as a table of
strings, `"!value"`, arrays or booleans. A `Taint` is `key` or
`key=value`; a node with taints is eligible only for jobs whose
`tolerations` cover every one (`key` tolerates any value, `*` everything).
`NodeConfig` also holds `cordoned`. Configs are keyed by endpoint id and
deleted with the member.

### Priorities and preemption

The queue is ordered by job priority (higher first), first in, first out
within a priority; retries and check runs go to the front of their
priority band. `schedule()` walks it in order. For each run it first looks
for an eligible node as before (now also: not cordoned, taints tolerated,
selector matches, account within quota). If there is none, `preempt_for`
looks for a node that would be eligible with some runs of strictly lower
priority stopped, and picks the one that needs the fewest victims and the
lowest priorities, taking the newest run first among equals. The victims
are cancelled on the agent and requeued at the front of their band; the
run takes their place in the same pass. Preempted and drained runs are
counted in `preempted` per run and subtracted from `attempts` when
comparing with `max_attempts`, so being moved never fails a job. `drain`
cordons the node and evicts every run on it the same way.

### Quotas

`AccountQuota` caps an account's running tasks (`slots`), GPUs in use,
CU spent in any 24 hours, the highest priority its jobs may ask for, and
store bytes (the phase 8 storage quota, folded into the same view). Slot
and GPU usage is counted from the nodes' running runs; a run over either
cap stays queued (`within_quota`), and later runs of other accounts are
still placed. Spending is noted on every `Charge` and `Transfer` the
ledger writes (`note_spend`, rebuilt from the last day of ledger entries at
start); an account at its daily cap can still use its own machines, which
cost nothing. `max_priority` is checked at submit: admin tickets are
unlimited without one, submitter tickets get 0.

### Access tickets

`pumasi access create` makes an `AccessRecord` (secret, name, role, expiry)
and returns a ticket in the admin ticket format (`pumasiadmin...`) with
that secret, so `pumasi login` takes either. The admin handler maps the
secret to a `Caller`: the cluster admin secret is `Admin`, a live access
record is its role, anything else is refused. `access::admit` checks each
call against the role before it runs: viewers get read-only calls;
submitters get their own account's jobs, receipts, ledger, datasets and
schedules, with `account` fields pinned to theirs and anything of another
account refused (including mounting another account's dataset);
everything else needs an admin. `access::scope` then filters replies
(status jobs, accounts, datasets, store usage, quotas, schedules) to the
submitter's account. Revoking deletes the record; the next call with it
fails. Access ids are a short BLAKE3 digest of the secret, never the
secret itself, but the records hold the secret, so `state.redb` is as
sensitive as `admin.ticket`.

### Schedules and dependencies

A `ScheduleSpec` keeps a whole `JobSubmission` (with its inline files, up
to 16 MiB) and a cron expression: five fields with lists, ranges and
steps, or `@hourly`, `@daily`, `@weekly`, `@monthly`, evaluated in the
control plane's local time (day of month and weekday are OR'ed when both
are restricted, as in cron). `tick()` submits due schedules; a run whose
previous job has not finished is skipped (and recorded as the last error)
unless `overlap` is set. The next time is computed from now, so a control
plane that was down submits one catch-up run, not one per missed slot.

`after` names jobs that must exist at submit. A job with unfinished
dependencies is kept out of the queue in `waiting`; when a dependency
ends, `release_waiting` queues the job once all have succeeded, or fails
its tasks with a `not run: ...` error naming the dependency (a
cancelled dependency counts as failed). Waiting is recomputed from the jobs at start.

### Apply

`pumasi apply -f cluster.toml` resolves the file on the CLI (job files are
read relative to it, like `pumasi run`) into an `ApplySpec` and sends it
in one call. The control plane compares each listed node's config, each
listed account's quota and each schedule with what it has, writes only
the differences and returns one line per change; `--dry-run` returns the
lines without writing. Nodes and accounts the file does not mention are
untouched, schedules too unless `--prune`. A node the cluster does not
know is reported as skipped. Applying the same file twice returns no
changes the second time.

## Phase 11: monitoring

### Node metrics

Agents of protocol 10 (`MONITOR_PROTOCOL`) sample their machine every
`METRICS_EVERY_SECS` (5 s, `agent/src/metrics.rs`) and send
`FromAgent::Metrics(NodeMetrics)`: CPU use and the one-minute load,
memory used (total minus available), the disk holding the data directory
(the longest mount point that is a prefix of it), network byte counters
summed over non-loopback interfaces, the hottest plausible temperature
sensor, the running task count, and for each GPU of the device profile its
use, VRAM, temperature and power (NVML through the same `libloading`
handle as detection for NVIDIA, the amdgpu sysfs files for AMD on Linux;
GPUs bound to `vfio-pci` are skipped). sysinfo needs two refreshes to
measure CPU, so the sampler lives as long as the agent.

The control plane (`cluster/src/monitor.rs`) keeps per node the latest
sample, the network rates derived from consecutive counters, and three
tiers of averaged points: 10 s steps for the last hour (memory only),
1 min steps for 24 hours and 10 min steps for the retention period
(`--metrics-retention`, 7 days by default), each capped by the retention.
A point averages its samples, except temperature, which keeps the
maximum. Finished 1 min and 10 min points are written to the `metrics`
table keyed by (node, tier, step start) every 15 s and pruned hourly.
`AdminOp::Metrics { node, since_secs }` returns `MetricsView`: every node's
latest sample, and with a node the finest tier that covers `since_secs`
(`step_secs` says which), plus `ClusterMetrics` (queued runs per priority,
running runs, waiting jobs, runs held by quotas, run outcomes, preemptions
and check mismatches since start, firing alerts).

### Events

`Monitor::event` appends an `EventView` (sequence number, time, level,
kind, node, job, account, message) to a ring of the last 10,000 and to the
`events` table, flushed with the metrics and pruned by the same
retention. Kinds: `node.joined`, `node.online`, `node.offline` (with the
runs it lost), `node.paused`, `node.resumed`, `node.suspended`,
`node.removed`, `job.submitted`, `job.succeeded`, `job.failed`,
`job.cancelled`, `task.failed` (a run that will not be retried, or a
failed attempt), `task.preempted`, `check.mismatch`, `upgrade.failed`,
`alert.firing` and `alert.resolved`. Job ends are found by checking the
jobs that were active once a second. `AdminOp::Events { after,
since_unix, limit }` is cursor-based: `pumasi events -f` passes the last
sequence number it printed.

### Live output

For WASM and Docker runs (not checks), the agent gives the executor a
`LiveOutput` buffer: Docker's stdout and stderr readers copy into it as
they read (`read_capped_live`), WASM attaches its memory pipes. A
streamer task sends what is new every 200 ms as `FromAgent::Logs { task,
stdout, stderr }`, up to the same cap as the final output. The control
plane accepts it only from the node running that attempt (generation
checked) and keeps it until the run's report arrives; then the stored
output takes over. `AdminOp::Logs { job, index, attempt, stdout_from,
stderr_from }` returns a `LogChunk` with the bytes from those offsets (at
most 1 MiB each), the task state, `done` and `live` (false when the output
comes only at the end, as for microVM runs, whose guest writes it to the
output disk). `pumasi logs -f` polls every 200 ms, so output reaches the
terminal within about 0.4 s plus one round trip.

### Alerts and webhooks

`AlertPolicy` holds the rules: node offline (not cordoned) for
`node_offline_secs`, failure rate over a window with a minimum run count,
disk percent (each node's and the control plane's own data directory,
measured every minute), temperature, benchmark drop, SMART warnings from
the node's profile and check mismatches. Each `Option` is a threshold or
off. `evaluate_alerts` runs every tick; stateful rules are kept in
`firing` keyed by (rule, subject) and send a notice when they start and
when they clear; benchmark drops and check mismatches are one-off notices.
A node counts as offline from its last heartbeat, or from the control
plane's start for nodes that were already offline then, so a restart does
not page for machines that were off.

Webhooks (`webhooks` table) have a name, URL and format: Slack (also
Mattermost) and Discord payloads with the message text, or JSON with
every `Notice` field (`cluster`, `status`, `rule`, `subject`, `level`,
`message`, `unix`, `since_unix`). Notices go through a channel to a worker
that posts each one with up to three attempts (2 s and 10 s apart; a 4xx
other than 429 stops), and records sent, failed and the last error per
webhook. URLs are secrets in effect: views show only `scheme://host/…`,
and errors are stripped of the URL. `pumasi webhook add` sends a test
notice (`AdminOp::TestWebhook`) and reports the result. The policy is in
`meta.alert_policy`; `apply` can set both from `[alerts]` and
`[webhooks.NAME]` (`url_env` reads the URL from the environment of the
machine running `apply`).

### Prometheus

`--metrics-addr` starts a small HTTP/1.1 server (`prometheus.rs`) that
answers `GET /metrics` with the text exposition format 0.0.4, `/healthz`
with `ok`, and anything else with 404 or 405; `--metrics-token` requires
a bearer token (compared in constant time). Families: `pumasi_build_info`;
per node (`node`, `node_id` labels) `node_up`, `node_slots`,
`node_running_tasks`, `node_cpu_score`, `node_last_seen_timestamp_seconds`,
`node_rtt_seconds`, `node_cpu_usage_ratio`, `node_load1`, memory and disk
used and total bytes, network receive and transmit byte counters,
`node_temperature_celsius`, `node_metrics_age_seconds`; per GPU (plus a
`gpu` label) utilization, memory, temperature and power; and for the
cluster `queued_runs{priority}`, `running_runs`, `waiting_jobs`,
`quota_blocked_runs`, `jobs{state}`, `jobs_finished_total`,
`runs_total{outcome}`, `preemptions_total`, `check_mismatches_total`,
`events_total{level}`, `alerts_firing{rule}`, `account_balance_cu` and
`account_held_cu`, `store_bytes` and the control plane's disk. Families
with no samples are left out. A strict parser in
`pumasi_proto::monitor::lint_exposition` checks the output in tests, and
CI also runs `promtool check metrics` on it.

### Roles

Viewers may call `Metrics`, `Events`, `Logs` and `Alerts`. Submitters may
read metrics and alerts, read `Logs` of their own account's jobs only, and
see only events with no account or their own. Changing the policy and
webhooks needs an admin.

## Phase 12: the web dashboard

### Where it runs

`pumasi ui` (`crates/dashboard`, started by `cli/src/ui.rs`) is an HTTP
server on the admin's own machine, 127.0.0.1:7400 by default. Browsers
cannot speak iroh QUIC, so the server is the bridge: it keeps one
`AdminClient` connection to the control plane (reopened when it closes,
`backend.rs`) and turns every page, button and API call into admin calls.
The control plane serves nothing new and opens no port; the dashboard
reaches any cluster the CLI can. The Prometheus server of phase 11 stays
in the control plane, because scrapers want a stable address there.

### Sign-in and roles

There are no dashboard accounts. A session holds the secret of a Pumasi
ticket, and `AdminClient::with_secret` makes each call over the shared
connection with that session's secret, so `access::admit` and
`access::scope` apply unchanged: a submitter sees and changes only its
account's jobs and datasets, a viewer only reads, refused calls come back
as the control plane's message. The pages hide what the role cannot do
(from `WhoAmI` at sign-in), but nothing depends on that.

- The URL `pumasi ui` prints has a random 160-bit token; opening it starts
  a session with the ticket `pumasi ui` runs with (`--admin`, or the saved
  one). `--login-only` drops the token, and listening on anything but
  loopback implies it.
- `/login` takes any ticket of the same cluster (the ticket's cluster id
  must match), checked with `WhoAmI`.
- The JSON API also takes `Authorization: Bearer <ticket>` per request.

Sessions live in memory (12 hours idle) behind an `HttpOnly`,
`SameSite=Strict` cookie. Requests other than GET need the session's CSRF
token in `X-Pumasi-CSRF` (htmx sends it from `hx-headers` on `<body>`); the
sign-in form checks `Origin`. On loopback the server refuses Host headers
other than its own address (DNS rebinding). Every response carries a
strict Content-Security-Policy (scripts, styles and connections from the
server only; no inline script or style, htmx's eval off), `X-Frame-Options:
DENY`, `nosniff`, `Referrer-Policy: same-origin` (the token never leaves
in a referrer) and `Cache-Control: no-store`. Downloads are always
`attachment` and `application/octet-stream`, so task output never renders
as a page of the dashboard's origin.

### Pages

Server-rendered HTML (maud, compiled into the binary), Korean or English
from the `pumasi_lang` cookie or `Accept-Language`; strings sit side by
side where they are used (`l.t("Nodes", "노드")`). Each page has an
`#live` region that reloads itself with htmx every few seconds and after
a button worked (`HX-Trigger: refresh`); buttons post with htmx and show
the result or the control plane's error in a flash area. Charts are SVG
drawn on the server from `MetricsView` series (`charts.rs`); times are
UTC in `<time>` elements that `app.js` shows in the browser's zone.

- Submitting: the form's job.toml and files (with their relative paths, so
  a folder upload keeps its layout) are written to a temporary directory
  and go through `JobFile::load_split` and `AdminClient::submit`, the same
  path as `pumasi run`, including large inputs through the file store.
  Dataset uploads go through `store_put` the same way.
- Live output: `/jobs/{id}/tasks/{i}/stream` is a server-sent event stream.
  The server polls `AdminOp::Logs` every 250 ms like `pumasi logs -f` and
  sends what is new as JSON (attempt changes reset the view; a UTF-8
  character cut between two chunks waits for the rest).
- Downloads: one inline output file straight from `TaskFiles`; all of a
  job's outputs (inline and stored) or a dataset as a zip (`zip.rs`, stored
  entries, no ZIP64) built in a temporary directory and streamed, the
  directory removed when the response ends. Receipts download in the
  `pumasi receipts --export` format (`ReceiptRow`, now in `pumasi-proto`).

### JSON API

`/api/v1` returns the protocol's own views as JSON, the response enum's
tag dropped. `JobId` now serializes as its hex string in human-readable
formats (and as the same u128 in postcard, so nothing stored or on the
wire changes), because JSON numbers lose precision past 2^53. `POST
/api/v1/op` takes any `AdminOp` in its serde JSON form. The routes are
listed in `assets/openapi.json` (OpenAPI 3.1), and a unit test checks that
the document and the router agree.

### Tests

Unit tests cover the session store, cookies, the language choice, the
zip writer, UTF-8 splitting, chart axes and the OpenAPI listing.
`crates/dashboard/e2e` holds the Playwright tests the `dashboard
(playwright)` CI job runs: a global setup starts a control plane, two
WASM agents and `pumasi ui --url-file` from the debug build, and the tests
sign in with the printed link, refuse a bad token, a bad ticket, a POST
without the CSRF token and a foreign Host header, submit a job of
`fixtures/ticker.wasm` (a WAT module that prints once a second and writes
an output file; a unit test checks it matches `ticker.wat`), follow its
output live, download one file and the zip, drain the node running a
25-second task and see it finish on the other node, create a viewer
ticket and check it cannot drain (in the page or the API), upload and
download a dataset, call the JSON API with a bearer ticket, and switch to
Korean.

## Phase 13: control plane replicas

`crates/cluster/src/ha.rs` replicates the control plane over Raft
([openraft](https://github.com/databendlabs/openraft) 0.9, `storage-v2`).
What is replicated is the state database itself, not commands:

- Every write `Store` makes is a `Batch` of `Op::Put` and `Op::Del`
  (table number, key and value in redb's own encoding; the 30 tables are
  listed in a fixed order by the `tables!` macro in `store.rs`). A single
  control plane applies a batch in one transaction (`Sink::Direct`); with
  replicas, the leader's store sends it to Raft (`Sink::Replicate`), which
  packs pending batches into entries of up to 16 MiB.
- Every replica's state machine applies committed entries to its own
  `state.redb`, writing the applied log id and the membership (table
  `raft_sm`) in the same transaction. So the database holds exactly a
  committed prefix, and a ledger move (`write_ledger`, one batch) is on
  every replica or on none: a failover cannot pay twice.
- The log, vote and purge point are in `raft.redb`. A snapshot is a
  `store::Dump` of every row (`snapshot.bin`), taken after 5000 entries,
  keeping 1000; installing one wipes and reloads the tables. Migrations
  run outside the log, on every replica, when it opens the database.

Only the leader runs the cluster. `server::lead` follows the Raft metrics:
on becoming leader it waits for `ensure_linearizable` (everything
committed is applied), loads `State` from the database and starts the
services of that term (agent and admin handling, the scheduler, timers),
held in `Inner`; on losing leadership, or when `get_read_log_id` (a quorum
round) has failed for `LEASE` (3 s), it stops them, which ends every agent
and admin session. The in-memory state runs ahead of the database by the
batches not committed yet; whatever a dead leader had not committed is
lost exactly as on a single control plane's crash (running tasks are
placed again; their receipts never existed). Reads of the database on the
leader (receipts, the ledger) first wait, up to 5 s, for the writes sent
before them to be applied (`store::InFlight`), so the leader reads what it
wrote.

Followers answer agents with `HelloReply::NotLeader { leader, replicas }`
and admin calls with `AdminResponse::NotLeader { leader }`. `AdminClient`
keeps the candidate list from the ticket, follows the hint, and retries
for up to 30 s a call that was refused or never sent; a call whose answer
was lost is not resent (it may have run). The dashboard uses it, so it
follows the leader too. Agents rotate through their candidates, follow
hints, and back off only after a full round; the leader sends protocol 11
agents `ToAgent::Replicas` whenever the set changes, and they save it with
their membership.

Identity: the cluster key (`secret.key`) is the same on every replica:
receipts and releases are signed with it and agents know the cluster by
its public key. The replica that created the cluster also uses it as its
endpoint key, so agents from before phase 13 keep reaching it; every
other replica has its own `endpoint.key`. Tickets append `replicas` to the
payload (`PayloadV2`); a ticket without it decodes as before.

Joining: `pumasi ha invite` turns replication on the first time
(`enable_ha`: initialize Raft with this replica alone, switch the store's
sink, snapshot and purge so newcomers start from a snapshot) and returns a
`ReplicaTicket` (prefix `pumasireplica`) with a one-use secret. `pumasi
cluster replica` binds its endpoint, sends `RaftReq::Join` over
`pumasi/raft/0` (redirected to the leader if needed), gets the cluster key
back, starts Raft empty, and the leader adds it as a learner, waits for it
to catch up, then makes it a voter. `ha remove` takes a replica out of the
membership. Raft traffic is length-prefixed postcard frames, one stream
per call; only members (and a peer holding a valid invite) are served.

The file store's data is not in the log: every `mirror_every` (30 s by
default) a follower asks the leader for its blob tags (`RaftReq::Tags`)
and fetches what it lacks over the blobs protocol, so after a failover
outputs and datasets are there up to that lag. Uploaded release binaries
are not mirrored.

`pumasi backup` (`AdminOp::Backup { offset }`, chunked with a BLAKE3
digest of the whole file, made in one read transaction and cached for 10
minutes) writes `PUMASI-BACKUP` + postcard of the version, time, cluster
key and `Dump`. `pumasi cluster restore` writes `state.redb` and
`secret.key` into an empty cluster directory; replication starts off.

`crates/cluster/tests/ha.rs` runs three control planes in one process with
a WASM agent: it checks that a replica ticket works once, runs a job,
kills the leader without a clean shutdown in the middle of a 6-task job,
and checks that a new leader finishes it with every task paid once, the
sum of all balances and held amounts unchanged and the owner charged
exactly the job's cost; then the old leader returns as a follower, catches
up, and a third job runs. A second test restores a backup and compares
every row.

## Phase 14: the WebGPU executor (experimental)

A wasm task that asked for a GPU (`gpus = 1`, a `GpuRequest` on a wasm
job) runs WGSL compute shaders on one WebGPU adapter. It is the way to use
GPUs that have no CUDA (Intel and AMD integrated GPUs, old Intel Macs'
GPUs, Apple Silicon) and works on NVIDIA and AMD cards too.

### Host API

`crates/agent/src/webgpu.rs` adds nine imports in the `pumasi_webgpu`
module to the executor's linker, so every module links whether or not its
task has a GPU (they fail with "this task has no GPU" when it does not):
`adapter`, `shader`, `buffer`, `write`, `read`, `dispatch`, `wait`,
`release` and `error`. They are plain core-module functions over the
module's exported `memory`, because the executor runs WASI preview 1
modules; each is one or two WebGPU calls (`createShaderModule` plus
`createComputePipeline` with `layout: "auto"`, `createBuffer` with
`STORAGE | UNIFORM | COPY_SRC | COPY_DST`, `queue.writeBuffer`, a compute
pass with one bind group, `copyBufferToBuffer` plus `mapAsync`), so a
module written against them moves to the component model's `wasi:webgpu`
(wasi-gfx) without changing its shaders once Wasmtime runs that. Handles
are small integers into a per-task table (4096 objects at most).
Failures return -1 and keep the message for `error`; wgpu's validation and
out-of-memory errors are caught with error scopes, everything else with an
uncaptured-error handler and the device-lost callback, and a panic inside
wgpu or a driver drops the device and fails the call.

Limits: the job's `memory_mb` caps the sum of its GPU buffers (separately
from its linear memory), and a buffer must fit the device's
`max_buffer_size`. Waiting on the GPU (`read`, `wait`) polls the device in
50 ms steps and stops at the task's deadline (a timeout) or when it is
cancelled, like the epoch callback does for wasm code. Shaders themselves
cannot be interrupted: a shader that never ends is stopped by the
driver's watchdog (TDR on Windows, the kernel's hang check on Linux,
macOS's GPU timeout). Fuel counts only wasm instructions. The device is
opened at the task's first call with the adapter's own limits and no
optional features, and dropped with the task. When the module used the
GPU, a line on its stderr says how many dispatches it ran and how long it
waited for the GPU.

### Adapters and profiles

`WebGpu::detect` enumerates adapters on Vulkan, Metal and DX12 (plus
OpenGL on Linux, for GPUs without Vulkan; `WGPU_BACKEND` overrides it),
drops those that cannot run compute shaders, keeps one entry per physical
GPU on its best backend (Metal or DX12, then Vulkan, then OpenGL only when
nothing else reaches the GPU), and leaves out software rasterizers
(lavapipe, llvmpipe, WARP) unless `--webgpu-software` is given. Each
adapter is matched to the device profile's GPU with the same PCI address
(or the only GPU of its vendor with a matching name) and benchmarked: a
WGSL shader of independent FMA chains, grown until a dispatch takes about
150 ms, gives FP32 GFLOPS of the same kind as the CUDA benchmark's.

The agent sends `FromAgent::WebGpu(SignedWebGpu)` after the welcome
(protocol 12), signed with its node key, every time it connects, with no
adapters when it was started without `--allow-webgpu`. The control plane
checks the signature, that no two adapters claim one GPU, and stores it in
the `member_webgpu` table (replicated like every other write).

### Scheduling and billing

An adapter's slot is the index of its GPU when it is one of the node's
listed GPUs, otherwise `WEBGPU_SLOT_BASE + index` (65536 + index). The
busy set of a node holds slots, so a Docker job and a WebGPU job never get
the same GPU. A wasm task with a GPU request goes to a node whose WebGPU
profile has a free adapter of the right vendor (hardware before software,
then the fastest), and the assignment names the slot; the agent maps it
back to its adapter. Wasm GPU requests take exactly one GPU and no memory
minimum (WebGPU does not report VRAM). Nodes with adapters get the label
`webgpu=<count>`.

The run's receipt carries a `GpuBilling` naming the adapter
(`<name> (WebGPU, <backend>)`) at the GFLOPS it is billed at: trusted
nodes at what they measured; untrusted ones at the CUDA GPU's checked
figure when the adapter is a GPU the cluster timed (a wrong answer bills
nothing), else at most `UNCHECKED_WEBGPU_GFLOPS` (1 TFLOPS), since no
challenge runs over WebGPU yet (the challenge hash uses only 32-bit
integer operations, so a WGSL version is the obvious next step). GPU
float results differ between GPUs, so a wasm job with a GPU is not
deterministic (`deterministic` defaults to off for it and cannot be
turned on) and is never re-run to check it.

### Examples and CI

`examples/webgpu` builds `matmul.wasm` (a 16 x 16 tiled matrix
multiplication) and `blur.wasm` (a two-pass Gaussian blur of a PPM image)
for `wasm32-wasip1`, with `src/lib.rs` as a small safe wrapper of the
imports. Both compute the same thing on the CPU inside the sandbox, where
Wasmtime's canonical NaNs make the result bit-identical on every machine,
compare the GPU's answer within a relative tolerance of 1e-3 and print both
times. CI's `webgpu` job runs them on Linux (lavapipe), Windows and macOS
(whatever adapter the hosted runner has; a runner without one is noted and
skipped), and `webgpu-compare` checks that the CPU checksums are identical
across the OSes and the GPU ones within the tolerance, with a table of the
speeds in the job summary. The unit tests run a WAT module through the
imports on the first adapter, and the end-to-end test runs a wasm GPU job
through a real control plane and agent (`PUMASI_TEST_WEBGPU=require` makes
a missing adapter a failure; CI sets it on Linux). Old Intel Macs (Metal
on macOS 10.15, Haswell-era Intel and Kepler GPUs) are checked by hand.

## Phase 15: remote API clients

Programs on other machines use the cluster through `pumasi ui
--api-only` over HTTPS (a tunnel, a proxy or `--tls-cert`) with a
submitter ticket. Phase 15 makes that safe to hand out and useful to
automate.

### Account job policies

`AccountPolicy` (`crates/proto/src/manage.rs`) sits next to an
account's quota: image patterns (`*` and `?`, matched against the name as
given and with Docker's defaults filled in, so `python:3.12` and
`docker.io/library/python:3.12` are the same), `require_digest`,
executors, `deny_network`, a selector appended to every job, the
tolerations a job may carry (`None` = any), the longest task timeout and
the most GPUs per task. `access::admit` calls `State::admit_policy` for
a submitter's `Submit` and `SetSchedule`, so the refusal (`403` in the
API, with the reason) happens before anything is stored, and a schedule
cannot smuggle in what a submission may not. Admin tickets are not bound
by it. Policies are set with `AdminOp::SetAccountPolicy` (`pumasi quota
ACCOUNT --images ...`) or the account's entry in `cluster.toml`, kept in
the table `account_policies`, and shown by `pumasi quota` and `describe
account`.

Each job also records its origin in the table `job_origin`: the name of
the ticket that submitted it (`cluster admin`, an access ticket's name,
or `schedule NAME`), its queue timeout and GPU capabilities, and the image
digests its tasks ran. Agents report a digest per task
(`FromAgent::Image`, protocol 13) after pulling or loading the image
(`docker image inspect`'s repo digest of that repository, else the image
id); at most 16 distinct digests are kept per job.

### Queue timeouts and capacity

`queue_timeout` in job.toml is `JobSubmission::queue_timeout_ms`. The
clock starts when the job could run (its `after` jobs succeeded), and
`tick_queue_timeouts` fails the tasks that are still pending and were
never started once it runs out, with an error that lists every node and
why it did not take the task (`State::why_unplaced`). Tasks that already
started are left alone: the timeout is about waiting for a node, not
about running long.

The reasons come from `State::refusal`, which `fits` now wraps: it
returns a `Refusal` (offline, paused, cordoned, executor missing, cannot
pay, memory, CPUs, image architecture, taint, selector, GPUs, cannot
draw, full...) instead of a bool, so the scheduler, the queue timeout
and `capacity` share one set of rules. `ClusterStatus::capacity`
(`State::capacity`, for the caller's account: a submitter's own, the
owner for others) builds a small synthetic task per executor the account
may use and counts the online nodes that would take it now, plus those
with a free GPU and those whose GPUs can draw, and gives each node its
executors, free slots and GPUs and the reason it would refuse. The quota
is checked once for the account.

### Docker that comes and goes

An agent whose Docker is not usable at startup (Docker Desktop not
started yet) no longer fails: it starts without the executor and a
`DockerWatch` checks `docker version` every 30 seconds. When Docker
appears or disappears it re-marks which GPUs Docker can use, signs a new
device profile, and sends `FromAgent::Executors` (`ExecutorReport`:
executors now, unavailable ones with a short reason such as "Docker
Desktop is not running", and whether its Docker GPUs can draw). The
control plane updates the node, emits `node.executor_unavailable` or
`node.executor_available`, and schedules again. `pumasi status` and
`NodeView::unavailable` show the reason.

### The API over the internet

`UiConfig::api_only` routes only `/api/v1`, `/api/openapi.json` and
`/healthz`, takes bearer tickets only (no cookies, so no CSRF), and drops
the Host check, which guarded browser sessions against DNS rebinding and
would refuse a tunnel's public host name. A bearer ticket is now
resolved with `WhoAmI` (cached for a minute per secret) so handlers know
its name and role; the control plane still checks the secret on every
call. `limit::AuthLimiter` counts failed sign-ins and wrong bearer
tickets per client address (10 in a minute block it for 5 minutes, `429`
with `Retry-After`); the address is the TCP peer, or, for a connection
from loopback, the header named by `--real-ip-header` (a client that
reaches the port directly cannot choose its own address). The peer comes
from `ConnectInfo<ClientAddr>`, which the router is served with, plain
or TLS.

`--tls-cert` and `--tls-key` load PEM files into a rustls `ServerConfig`
(ring provider, TLS 1.2 and 1.3, ALPN `http/1.1`). `tls::TlsListener`
implements axum's `Listener`: an accept task hands each connection to its
own handshake task (10 s limit, 256 at once) and queues finished streams,
so a silent client blocks nobody. `pumasi ui --api-only` refuses plain
HTTP on an address other than loopback unless `--insecure-http`.

`GET /api/v1/jobs/{id}?wait=D` asks the control plane for the job, and
while it is not finished sleeps (250 ms doubling to 2 s) and asks again,
until it finishes or `D` (at most 90 s, under Cloudflare's 100 s) runs
out; the answer is the same job view either way. `docs/api.md` is
written as a walkthrough whose `sh` blocks (and hidden `<!-- ci: -->`
lines) `scripts/ci/api-doc.sh` runs in order against a real control
plane, agent and `pumasi ui --api-only`.

### GPU drawing

`gpu.capabilities` in job.toml becomes `JobSubmission::gpu_capabilities`
(`graphics`, `video`; `compute` and `utility` are implied). The agent
passes them as `NVIDIA_DRIVER_CAPABILITIES` (always with `compute,utility`)
to `docker create`, which is what makes the NVIDIA Container Toolkit
mount the EGL, GLX, Vulkan and OptiX libraries on Linux. On WSL 2 (Docker
Desktop) drawing goes through Mesa's d3d12 gallium driver in the image,
which loads the Windows driver's `libd3d12.so` from `/usr/lib/wsl/lib`
and talks to the GPU through `/dev/dxg`. Docker Desktop (seen with 4.80)
gives `--gpus` containers `/dev/dxg` and the CUDA libraries but not
`/usr/lib/wsl/lib`, even with `NVIDIA_DRIVER_CAPABILITIES=all`, although
its VM has the folder. So the agent lends it: with `--docker-wsl-libs`
(`auto`: on Windows hosts, where Docker is Docker Desktop) NVIDIA tasks
that ask for `graphics` get `--volume /usr/lib/wsl:/usr/lib/wsl:ro`.
This is the only host folder a container gets, and the exception is kept
narrow on purpose: read-only, a fixed path in Docker Desktop's VM (not on
the Windows file system), only graphics tasks, and only the GPU driver's
user-mode files, which drive the same `/dev/dxg` every `--gpus` container
already has. `--volume`
rather than `--mount` means a VM without the folder gives an empty one,
so drawing falls back to the CPU instead of the task failing. The image
still has to point Mesa at it (`LD_LIBRARY_PATH=/usr/lib/wsl/lib`,
`GALLIUM_DRIVER=d3d12`), as `examples/blender-render/render.sh` does only
when `libd3d12.so` is there. OptiX does not start in Docker Desktop
containers (Blender: error 7805) with or without the folder, so the probe
reports it as missing on WSL 2. Whether drawing works on a machine depends on
the driver, the toolkit and the image, so it is measured rather than
guessed: `pumasi gpu check --docker-run [--image IMAGE] [--wsl-libs]`
(`gpu::docker_check`) starts the image with every capability (and the
WSL folder, as graphics tasks get it), looks for
`nvidia-smi`, the NVIDIA EGL/GLX/Vulkan files, OptiX, `/dev/dxg` and
`libd3d12`, Mesa's `d3d12_dri.so`, and asks `eglinfo`/`vulkaninfo` (when
the image has them) which renderer they get, treating llvmpipe and
lavapipe as a failure. The answer is saved as `docker-gpu.json` in the
agent's data directory; the agent reads it (or `--gpu-graphics yes|no`)
at each Docker check and reports it in `ExecutorReport::gpu_graphics`.
The node gets the built-in label `gpu.graphics=true`, and jobs that ask
for `graphics` are refused by `Refusal::GpuGraphics` elsewhere.
`examples/blender-render` (Cycles and EEVEE, with an image built on
Ubuntu's Mesa) is checked on an RTX 3080 under Windows by hand.

### Compatibility

`PROTOCOL_VERSION` is 13: `TaskAssignment` gains `gpu_capabilities` at
the end (older agents ignore it, and graphics jobs only go to nodes that
reported `gpu_graphics`, which only protocol 13 agents do), and
`FromAgent::Executors` and `FromAgent::Image` are new (the control
plane refuses agents newer than itself, so an agent that sends them talks
to one that knows them). GPU jobs with capabilities go only to agents of
`REMOTE_PROTOCOL` (13) or later.
`JobSubmission` gains `queue_timeout_ms` and `gpu_capabilities` at the
end, which changes the stored schedules that embed it: layout 7 migrates
them (see Persistence). The admin protocol gains
`AdminOp::SetAccountPolicy` and new fields at the end of `ClusterStatus`,
`NodeView`, `JobView` and `QuotaView`, so the CLI and `pumasi ui` must
match the control plane's version, as before.

## Phase 16: the confidential tier

Jobs carry a `Tier` (`crates/proto/src/job.rs`). `open` runs anywhere.
`trusted` runs only on a *trusted host*: an owner machine (role `Owner`)
or a provider an admin trusts (`pumasi trust NODE`, the `trusted_nodes`
table; `Refusal::Untrusted` otherwise). `confidential` is a microVM job
(no GPUs, no network: `JobSubmission::validate`) that runs in a SEV-SNP
or TDX guest and is hidden from the node's owner. The rest of this
section is about that tier.

### Sealing

The control plane is the key broker. A job's key is
`blake3::derive_key("pumasi 2026 confidential job key v1", cluster key ||
job id)` (`confidential::job_key`), so every replica derives the same key
and nothing new is stored. `pumasi_guest::seal` derives the image key and
each task's key from it with HKDF-SHA256 and seals streams with
XChaCha20-Poly1305 in 64 KiB chunks (the STREAM construction: a random
19-byte prefix, a chunk counter and a last-chunk flag make each nonce; the
header, with its length field zeroed, and a caller's context are the
associated data, so chunks cannot be reordered, dropped or cut off, and a
stream of one kind or run cannot pass for another).

A confidential job always goes through the private image path:
`prepare_store` sets `private_image`, `want_images` asks only for `amd64`,
and the store worker pulls the image and replaces the plain collection
under the same tag with `config.sealed` (the image manifest with its
config, sealed with the image key) and `image.sealed` (the layers tar the
guest unpacks, sealed). Each task's input is sealed on demand, at most
four at a time (`StoreCmd::SealInput`, `confidential::seal_input`): the
worker opens the sealed config, builds the run spec as an agent would
(`vmdisk::run_spec`, shared with the agent), places the mounted datasets'
files, and writes the input tar (run spec, stdin, job and task files,
mounts) through a `SealWriter` with the task key into `input.sealed`,
tagged under the job's `in/` tags. Sealed inputs are kept in memory; after
a restart they are sealed again. `Need::confidential` holds a run back
with `Refusal::Sealing` until both are there.

### Placement and attestation

A node reports the TEEs it starts VMs in with `FromAgent::Tee` (protocol
14): `sev-snp`, `tdx`, or the test stand-in `test`, which counts only on a
control plane started with `--insecure-test-tee`. `refusal` sends a
confidential run only to protocol 14 agents with a usable TEE. `place`
gives the run a *lease* (node, TEE, a random 32-byte nonce, used flag) and
sends an assignment with the image, command, environment, files and
mounts left out: `TaskAssignment::confidential` names the TEE, the nonce,
the sealed image and input collections, the output disk's size and the
memory the VM needs on top of the task (the root file system, input and
output live in the guest's encrypted memory).

The agent fetches the two sealed files and boots QEMU with
`confidential-guest-support` (`sev-snp-guest` with `kernel-hashes=on`, or
`tdx-guest` with the host's quote generation socket) and OVMF built for
it, so the firmware measures the kernel, initramfs (with the guest init)
and command line. Its disks are the sealed image, the sealed input, an
output disk and a 1 MiB *attest disk* whose first sector holds the TEE and
nonce. The guest init (mode `sealed`) makes an X25519 key pair, asks the
kernel's TSM interface (`/sys/kernel/config/tsm/report`) for a report
whose 64 bytes of report data are `SHA-512("pumasi/attest/v1\0" || nonce
|| public key)`, and writes it to the attest disk at 4 KiB (with `O_DIRECT`,
so the host sees it). The agent passes it on as `FromAgent::Attest`.

`State::begin_attest` uses the lease up (one try per run): it must exist,
be this node's, for a run the node is running, and the evidence must be
from the TEE it was placed for. Off the state lock, `attest::Verifier`
checks it:

- SEV-SNP (`check_snp`): the VCEK comes from the report's extended
  certificate table, or from AMD's key distribution service by chip id
  and reported TCB (cached). It must chain to AMD's ARK and ASK for its
  generation (Milan, Genoa, Turin; built into the `sev` crate), its TCB
  and hardware id extensions must match the report, the report's ECDSA
  P-384 signature must check out with it, the guest policy must not allow
  debugging, and the report data must be this run's. The measurement is
  the report's `MEASUREMENT`.
- TDX (`check_tdx`): `dcap-qvl` checks the quote with collateral the
  control plane fetches from Intel's PCS (PCK CRL, TCB info and QE
  identity for the quote's FMSPC, Intel's root CRL; cached for six hours):
  the PCK chain to Intel's root, the QE report, the quote's signature and
  the TCB status. Only `UpToDate`, `SWHardeningNeeded` and the configuration
  statuses pass; the TD must not be debuggable, and its report data must
  be this run's. The measurement is SHA-384 of MRTD, RTMR1 and RTMR2.
- Test (`check_test`): only with `--insecure-test-tee`; the "measurement"
  is the guest init's SHA-384, so the trust flow is the same.

`finish_attest` records the measurement (`measurements` table: TEE,
node, agent version, vCPUs, first and last seen, count, details) and
releases the keys only if an admin trusts it (`pumasi confidential
trust`, at least 8 characters of its id): the image and task keys,
wrapped for the guest's public key (an ephemeral X25519 exchange, HKDF
bound to the nonce, XChaCha20-Poly1305). Anything else gets
`ToAgent::Keys` with the reason, which the agent writes back to the attest
disk; the guest then reports only that it failed. Each outcome is a
`confidential.*` event. Recorded SEV-SNP reports and TDX quotes, with
forgeries (another run's report data, flipped bytes, the wrong
generation's VCEK, a self-signed VCEK, an out-of-date platform), are unit
tests in `attest.rs`; the lease rules, replays, untrusted and changed
measurements are state tests.

### Results

With its keys, the guest unpacks the sealed layers into a tmpfs root,
reads the sealed input, runs the command and writes its output tar
through a `SealWriter` with the task key and the run's nonce as context.
The agent copies the sealed stream from the output disk and uploads it as
`output.sealed` (confidential runs always use the store for output). The
control plane checks the receipt against the report as the node sent it,
then opens the stream (`confidential::open_output`) and records the
guest's own outcome, stdout, stderr and files instead
(`State::on_report_opened`); with an output dataset, the opened files are
imported into the store. A sealed output that does not open (the wrong
key or nonce, or a changed byte) strikes the node and the run is done
again. A run with no sealed output failed before it had keys and can only
be an error. Live output is not sent for confidential runs.

### What it does not protect

The control plane holds every key, so it, its replicas, backups and the
admin ticket can read confidential jobs, and datasets in its store are
stored in the clear (members of the cluster can fetch a collection whose
hash they know). The node sees sizes, timings and memory use, and can
stop a run or withhold its result. Leases live in memory, so runs in
flight are done again after a restart or failover. The SEV-SNP check
does not enforce a minimum TCB beyond what AMD still certifies; RTMR0
(the TDX firmware's own configuration) is not pinned; there is no
encrypted scratch disk (tasks are limited by the VM's memory); NVIDIA
confidential computing (H100 and later) is not supported, so confidential
jobs have no GPUs.

## Phase 17: self-hosting 1.0

What an organization needs before it trusts the cluster with its people
and its data: knowing who did what, signing people in with the accounts
they already have, limits on whatever reaches the internet, and evidence
that the code holds up. The [threat model](threat-model.md) and the
[operations guide](operations.md) are part of this phase.

### Audit log

Every mutating admin call, every refusal, node joins, sign-ins and their
failures, secret reads, admin rotations, job expiry and control plane
starts append an `AuditEntry` to the `audit` table: sequence number, time,
actor (ticket name, role, iroh peer), action (`job.submit`,
`access.revoke`, `call.Status` for a refused read, ...), target, detail,
success, and two hashes. `hash = BLAKE3("pumasi/audit/v1\0" ||
postcard(entry without hash))` covers the previous entry's hash, so the
log is a chain (`pumasi_proto::audit`). `audit::describe` maps each
`AdminOp` to an action, or to nothing for reads; refused calls get one
from `refused_action`. Unknown secrets are refused before anything is
written, so strangers cannot add to the log. The tip (next sequence
number and last hash) is kept in memory and reloaded from the last row.

`AdminOp::AuditVerify` walks the whole chain, checks it ends at the tip,
and returns an `AuditHead` (cluster id, entry count, last sequence and
hash, time) signed with the cluster key over `"pumasi/audit-head/v1\0" ||
seq || hash || unix`. `pumasi audit verify --against head.json` checks
the signature of a head saved earlier and that the log still contains it
at that sequence number. `pumasi audit export` writes RFC 5424 syslog
(`<85>1 TIME HOST pumasi - audit [pumasi@32473 seq=".." action=".."]
{json}`, facility authpriv, severity notice or warning for failures) to
standard output, UDP, or TCP with octet counting (RFC 6587), resuming
from a state file.

### Admin secret rotation

`AdminOp::RotateAdminSecret` makes a new admin secret, stores it under
`meta.admin_secret`, swaps it in memory (`Inner::admin_secret` is behind a
mutex now), rewrites `admin.ticket`, and returns the new ticket once.
Access tickets and SSO sessions are separate secrets and keep working.

### SSO

Settings (`SsoConfig`: OIDC provider, LDAP directory, rules, session
length) live in `meta.sso`; secrets in them are `Hidden` (redacted in
`Debug` and never in views). Sign-in calls are the only admin calls
accepted without a secret (`AdminOp::is_sign_in`; clients send
`SignInTicket`s, which are admin tickets with a zero secret and their own
`pumasisignin` prefix). Each successful sign-in becomes an ordinary access
ticket named after the person (their verified email, else the subject or
LDAP user) with the most powerful role their rules give
(`sso::role_for`: admin > submitter > viewer), valid for the session
length; at most 10 sessions per person are kept. No matching rule, no
ticket.

- **OIDC browser flow.** `SsoStart { redirect_uri }` (the dashboard's
  `--public-url` + `/sso/callback`, HTTPS or loopback) fetches the
  issuer's discovery document (cached 1 hour), makes `state`, `nonce` and
  a PKCE verifier, and returns the authorization URL; the dashboard keeps
  `state` in a `SameSite=Lax` cookie scoped to `/sso`. `SsoFinish { state,
  code }` takes the pending login (10 minutes, used once, at most 1000),
  exchanges the code with the client secret, and checks the ID token with
  `jsonwebtoken` against the JWKS (fetched again for an unknown key id, at
  most once a minute):
  asymmetric algorithms only, `iss`, `aud`, `azp`, `exp` and `nonce`.
  Groups come from `--groups-claim`; emails count only when
  `email_verified`.
- **OIDC device flow** (RFC 8628) for the CLI: `SsoDeviceStart` asks the
  provider for a device code and returns the user code with a handle;
  `SsoDevicePoll { handle }` polls the token endpoint for the client,
  honouring `slow_down`, so the device code never leaves the control
  plane.
- **LDAP.** `LdapLogin { user, password }` binds as the service account,
  finds the user with the filter (`{user}` escaped per RFC 4515), binds
  as the user (empty passwords refused), and reads groups from
  `memberOf` or a group search. Five wrong passwords in five minutes lock
  that user out of Pumasi for the rest of the window; at most four binds
  run at once. LDAPS or StartTLS with the Mozilla roots; plain LDAP only
  to loopback.

The control plane makes these outbound calls itself, from the leader; the
runtime state (caches, pending logins) is per leader term.

### Request limits

`pumasi ui` gives every ticket a token bucket (120 requests, refilled at
20 per second; `429` with `Retry-After`) and at most 8 concurrent
`?wait=` polls, and every client address a sign-in bucket (10, then 10 a
minute). Bodies are capped per route: 1 MiB by default, 32 MiB for
`/api/v1/op`, `--max-upload` only for job submissions and file store
uploads. The session cookie is `Secure` when the dashboard's public URL
is HTTPS.

### Job retention

Jobs, their tasks (with stdout, stderr and small outputs) and their
modules stayed in memory and in `state.redb` forever. Every 10 minutes the
leader deletes up to 2000 finished jobs submitted more than `--keep-jobs`
ago (30 days by default; 0 keeps everything), skipping jobs a waiting job
depends on, jobs with earnings on hold, and jobs whose outputs are still
being stored (`retention.rs`). Their rows go in one batch with the
modules and file bundles no remaining job uses; receipts, holds, the
ledger and the audit log stay. One `jobs.expire` audit entry records each
sweep.

### Supply chain, fuzzing, soak

- `deny.toml` (advisories, licenses, sources, wildcards; every workspace
  crate is `publish = false`) and `.cargo/audit.toml`, both run in CI's
  `supply-chain` job. Each ignored advisory has its reason.
- `fuzz/` is a cargo-fuzz crate outside the workspace with ten targets:
  `admin_wire`, `agent_wire`, `tickets`, `config_files`, `sealed_stream`
  (also checks that any flipped byte of a sealed stream is detected),
  `attestation`, `receipt`, `guest_output`, `audit_sso`,
  `release_binary`. CI's `fuzz` job runs each for 60 seconds.
- `scripts/soak.sh` (CI's `soak` job runs it for 10 minutes; the Long
  checks workflow, `.github/workflows/long-checks.yml`, for 5h20m on
  Linux and macOS every Sunday and before a release, with 20 minutes per
  fuzz target).
- `scripts/sso-e2e.sh` (CI's `sso-e2e` job): the OIDC browser and device
  flows against Keycloak and LDAP against OpenLDAP, in Docker, with the
  realm and directory in `scripts/sso-e2e/`. It found that Keycloak's
  "PKCE Method" client setting also demands PKCE on the device flow, so
  operations.md says to leave it empty.
- CodeQL (`.github/workflows/codeql.yml`) for Rust and the workflows.

### Compatibility

Protocol stays 14: agents see nothing new. Admin ops and responses are
appended at the end of their enums, so older CLIs keep working against
this control plane (and get an unknown-response error for the new
calls). Storage layout stays 7.

## Persistence

`state.redb` in the cluster directory holds: `meta` (admin secret, cluster
name, policy, layout version), `members`, `tokens`, `jobs` (metadata),
`tasks` keyed by `(job, index)` so a completion rewrites only that task
(including its output files and check record), `modules` keyed by hash,
which holds both WASM modules and input file bundles, `accounts`, `ledger`
keyed by sequence number, `holds` and `receipts` keyed by run
`(job, index, attempt, check)`, and `carry` keyed by (consumer, provider).

Postcard is not self-describing, so `meta.schema` versions the record
layout, and older layouts are migrated in place on first open. A phase 1
database (no version) becomes layout 2: members become owner machines, jobs
belong to `owner` and are not deterministic. Layout 3 (phase 3) adds
`microvm_cpu_hour` to the stored policy, starting at the Docker price.
`Executor::Microvm` was appended to the enum, so older records decode
unchanged. Phase 4 adds no new record layout: the rollout lives under the
new `meta.rollout` key and uploaded binaries are files in
`<cluster dir>/releases/`. Phase 6 adds none either.

Layout 4 (phase 7) moves the ledger from credits to milli-CU at the price
the cluster charged for a Docker core-hour (`docker_cpu_hour`, 0.05 credits
by default, so one credit becomes 20 CU). Balances and held amounts are
converted with largest-remainder rounding so the total stays exact, open
holds keep their release time, the credit ledger and receipts are kept in
`credit_ledger` and `credit_receipts`, and the old rate is in
`meta.credit_cpu_hour`. The converted ledger starts with one `Convert`
entry per account with a balance, and task costs are converted too. Members gain the profile and the last benchmark check. On the
next start the control plane re-countersigns each credit receipt as a CU
receipt at score 1000 (the node signature still holds because `Usage` is
unchanged) and marks it done in `meta.credit_receipts_converted`.

Phase 8 adds tables without changing a layout (still 4): `job_ext` and
`task_ext` (a job's mounts, output dataset, private image state; a task's
mounts and stored outputs, including check runs), `datasets` keyed by
(name, version), `quotas`, `secrets` and `uploads`, and the meta keys
`storage_policy` and `storage_billed`. Blob data lives in `blobs/`.

Layout 5 (phase 9) adds the two GPU rates to the stored policy (migrated
from the layout 4 policy with the defaults) and the tables
`member_devices` (each member's signed device profile and its GPU checks)
and `job_gpu` (a job's GPU request, kept apart so job metadata keeps its
layout). Receipts with GPU billing use the new receipt layout; older ones
decode with the previous one.

Layout 6 (phase 10) adds tables and changes no record, so the migration
only writes the version: `job_sched` (a job's priority, selector,
tolerations, dependencies and schedule, apart from job metadata like
`job_gpu`), `node_config` (labels, taints, cordon per member),
`account_quotas`, `preempted` keyed by run, `access` and `schedules`.
`JobSubmission` gained fields at the end, which matters for stored
schedules: a later change to it needs a migration of `schedules`.

Phase 11 adds tables and keeps layout 6: `events` keyed by sequence
number, `metrics` keyed by (node, tier, step start), `webhooks` by name,
and the meta key `alert_policy`. An older control plane opening the file
ignores them.

Phase 13 keeps layout 6 and adds the table `raft_sm` (applied log id and
membership) to `state.redb`, and `raft.redb`, `snapshot.bin`,
`endpoint.key` and `join.peers` beside it on replicas.

Phase 15 moves to layout 7: schedules embed a `JobSubmission`, which
gained two fields, so `migrate_v6` re-encodes each stored schedule
(without a queue timeout or GPU capabilities). The tables
`account_policies` (by account) and `job_origin` (by job id) are new;
like every table they come last in the `tables!` list, so Raft batches
from replicas of the same version agree on table numbers.

Phase 16 keeps layout 7. `Tier::Confidential` was appended to the enum,
so stored jobs decode unchanged; the tables `measurements` (by
measurement id) and `trusted_nodes` (by node id) come last. Job keys are
derived, and sealed inputs and leases are not stored.

Phase 17 keeps layout 7 and adds the table `audit` (by sequence number,
last in `tables!`) and the meta key `sso`. Expired jobs are deleted from
`jobs`, `job_ext`, `job_gpu`, `job_sched`, `job_origin`, `tasks`,
`task_ext`, `preempted` and `modules`.

`PROTOCOL_VERSION` is 14 (phase 16, above): protocol 5 (phase 4) added `UpgradeHello` and the
upgrade messages, protocol 6 appends `AgentHello::availability` and
`FromAgent::Availability`, protocol 7 adds `FromAgent::Perf` and
`BenchResult` and `ToAgent::Benchmark` and `Rebenchmark`, which the control
plane sends only to agents of protocol 7 or later. Protocol 8 appends the
mount, output, image and peer fields to `TaskAssignment` (postcard ignores
trailing fields, and only protocol 8 agents get store tasks) and adds
`FromAgent::Report` and `StoreState`. Protocol 9 appends
`TaskAssignment::gpus` and adds `FromAgent::Devices` and `GpuResult` and
`ToAgent::GpuChallenge`; only protocol 9 agents get GPU or microVM network
tasks and challenges. Protocol 10 adds `FromAgent::Metrics` and
`FromAgent::Logs`; since the control plane refuses agents newer than
itself, an agent sends them only to a control plane that knows them. The hello is unchanged. Protocol 11 adds `HelloReply::NotLeader` and
`ToAgent::Replicas`; a protocol 10 agent talking to a follower sees an
unknown reply and retries, so it reaches the cluster only while the replica
it joined leads. Protocol 12 adds `FromAgent::WebGpu`; protocol 11 agents
get no wasm jobs with GPUs. Protocol 13 is described under phase 15.
Protocol 14 (phase 16) appends `TaskAssignment::confidential` and adds
`FromAgent::Tee`, `FromAgent::Attest` and `ToAgent::Keys`; confidential
runs go only to protocol 14 agents. The control plane accepts hellos from
`MIN_AGENT_PROTOCOL` (5) up: `AgentHello::decode` tries the current layout
and falls back to the protocol 5 one (available), so phase 4 and 5 agents
keep working and can take a rollout. Upgrade the control plane first; it
rejects agents newer than itself. Phase 3 agents cannot be upgraded
remotely; install a current binary on them once.

## Not yet

- Remote API: no iroh proxy for the CLI behind HTTP-only networks, no
  ACME (certificates come from certbot, a proxy or a tunnel), failed
  sign-in counts live in each `pumasi ui` process, `?wait=` polls the
  control plane, and policies cannot limit CPU, memory or CU per job
  (quotas cover slots, GPUs and CU per day).

- File store: replicas on owner nodes (the control plane is the only full
  copy, so back up `<cluster dir>/blobs`), an S3-compatible gateway, and
  per-node quotas for caches beyond the size cap. Private images are pulled
  from the registry again for each job (nodes that already loaded the
  digest skip the download from the store).
- GPUs: Intel and Apple GPUs are used only through WebGPU; AMD GPUs are not benchmarked, only
  estimated, and cannot take the GPU challenge (billed at the reference at
  most on providers); passthrough is NVIDIA-only and untested in CI; MIG
  and vGPU slices are not scheduled separately.
- MicroVM network: TCP and DNS only (no other UDP, ICMP or IPv6), no
  per-job allow lists or bandwidth caps, egress is not billed, and the vz
  backend and Firecracker without QEMU have no network.
- Cluster management: no affinity or anti-affinity between jobs, no
  gang scheduling (all tasks of a job at once), no per-node or per-label
  quotas, quotas count tasks rather than CPU or memory, preemption stops a
  task instead of letting it checkpoint, and schedules use the control
  plane's time zone only.
- Monitoring: microVM output is not streamed while the task runs; Intel
  and Apple GPUs report no metrics; Windows nodes report no temperature
  (sysinfo reads it through WMI, which is not safe across threads); the 10 s tier is lost on a control
  plane restart; alert rules are cluster-wide (no per-node or per-label
  thresholds, no silences or repeat notices); node-to-node latency is not
  measured.
- Dashboard: prices, alert rules, webhooks, quotas, schedules and
  rollouts are shown (or left to the CLI) but not edited in the browser;
  sessions are in memory only; charts are server-drawn snapshots
  that refresh every 10 seconds rather than streaming.
- WebGPU: compute shaders only (no textures, samplers or render
  passes), one bind group, no optional features such as `shader-f16` or
  subgroups, one adapter per task, no GPU challenge over WebGPU (so an
  untrusted node's adapter is billed at 1 TFLOPS at most), shaders that
  never end are stopped by the driver's watchdog rather than the job's
  timeout, no GPU metrics for adapters that are not listed GPUs, and core
  modules only (no `wasi:webgpu` components until Wasmtime runs them).
- Confidential tier: no NVIDIA confidential computing (no GPUs in
  confidential jobs), no network, x86-64 only, no encrypted scratch disk,
  no minimum SEV-SNP TCB policy or RTMR0 pinning, leases lost on a
  restart or failover, and real SEV-SNP and TDX hosts checked by hand only
  (CI uses the test stand-in and recorded reports).
- Remaining roadmap phases (18 and 19): the hub and buying CU. See the
  roadmap in the README.
- Phase 17 leftovers: no per-peer limit on unauthenticated sign-in calls
  at the control plane, the audit log is never pruned (pruning behind a
  signed checkpoint), no SCIM provisioning (people appear when they first
  sign in and their sessions simply expire), no SAML (1.2 added the
  control plane's service installer, `pumasi service install cluster`). 1.0 was tagged after a green
  long-checks run on the tagged commit. The internal security review is done (threat
  model, "Internal review for 1.0"); no third-party audit yet.
- Windows: microVMs on ARM64, Hyper-V HCS as a faster backend, a Docker
  path for machines nobody is signed into (dockerd inside a WSL distro).
- Rollouts go to every node; there is no canary group or per-node
  selection yet, and the control plane does not upgrade itself.
- High availability: release binaries are not mirrored to followers, the
  file store's data reaches them only every 30 seconds, a replica's
  address in the membership is the one it joined with (a replica that
  moves must be removed and invited again), and replicas take no reads
  (followers serve nothing; the leader does all the work).
- CU cannot be bought or exchanged, and there is no platform fee; both
  belong to the hub.
- "Honeypot" tasks with known answers (needed for non-deterministic Docker
  and microVM work, which checks cannot cover). The CPU benchmark
  challenge keeps the billed speed honest, not the results. Disk and
  memory figures are not challenged yet, and no scheduling uses them.
- MicroVM: a snapshot pool for faster boots.
- Availability: running tasks are not stopped when a node pauses, and
  idle detection under a Windows service is not possible yet.
- WASI preview 2 components.
