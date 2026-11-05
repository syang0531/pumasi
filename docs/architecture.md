# Architecture

This document records how the cluster is built and why: the private
cluster of phase 1, the invited providers, receipts, ledger and
result checks of phase 2, the microVM executor of phase 3, the
releases, OS services and signed agent upgrades of phase 4, the
Windows support of phase 5, the Mac support and availability policy of
phase 6, the benchmarks and compute units of phase 7, the cluster
file store of phase 8, the GPUs and microVM devices of phase 9, and the
cluster management of phase 10. The
product-level research and the decisions behind it (dual executor, fiat-only
billing, honest confidentiality tiers) live in the research doc shared with
the project; this file covers the code.

## Crates

| Crate | Role |
| --- | --- |
| `pumasi-proto` | Job spec (`job.toml`), wire messages, framing, tickets, endpoint helpers, receipts (signing and verification), price list and policy, the OCI registry client, and `blobs.rs` (iroh-blobs helpers: collections from files and back, resumable push and fetch). No business logic. |
| `pumasi-guest` | `pumasi-guest`, the init process inside every microVM (a static musl binary), and the disk protocol the agent shares with it. |
| `pumasi-agent` | `WasmExecutor` (Wasmtime sandbox, deterministic mode), `DockerExecutor` (Docker CLI), `NodeStore` (the node's file store cache), `MicrovmExecutor` (ext4 formatter, Firecracker/QEMU/Virtualization.framework), `availability` (power, idle and hours policy), `platform` (sparse files, Windows Job Objects, the WHPX check) and the `Agent` session loop, which signs usage. |
| `pumasi-cluster` | `State` (membership, queue, scheduler), `ledger.rs` (accounts, holds), `check.rs` (result checks), `datasets.rs` (datasets, quotas, secrets, storage billing), `files.rs` (the blob store, its access rules and tags), `transfer.rs` (the CLI's uploads and downloads), `Store` (redb), `Cluster` (iroh server), `AdminClient`. |
| `pumasi-cli` | The `pumasi` binary. Thin: parses args, calls the crates above, prints. `service.rs` installs the agent as an OS service, `upgrade.rs` drives rollouts. |

The marketplace hub, Madang (마당), is not in this repository; it is a
separate, closed-source service in the private `madang` repository. The agent and control plane stay Apache-2.0.

## Networking

Every process is an iroh endpoint with a persistent ed25519 key
(`secret.key` in its state directory). Connections are QUIC, end-to-end
encrypted and mutually authenticated by key, with hole punching and relay
fallback in `public` mode. `local` mode disables relays and address lookup
for LANs and tests.

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
   no bind mounts.
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
| QEMU | Linux, macOS, Windows | `-M microvm` on x86_64, `-M virt` on ARM64; KVM, HVF or WHPX when available, else TCG (slow; adds `tsc_early_khz` so boot never stalls calibrating the TSC) |
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

### Release builds

`.github/workflows/release.yml` runs on a `v*` tag. It builds `pumasi-cli`
in release mode for six targets: `x86_64`/`aarch64-unknown-linux-musl`
with cargo-zigbuild (static, so one binary covers every distribution),
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

`PROTOCOL_VERSION` is 10: protocol 5 (phase 4) added `UpgradeHello` and the
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
itself, an agent sends them only to a control plane that knows them. The hello is unchanged. The control plane accepts hellos from
`MIN_AGENT_PROTOCOL` (5) up: `AgentHello::decode` tries the current layout
and falls back to the protocol 5 one (available), so phase 4 and 5 agents
keep working and can take a rollout. Upgrade the control plane first; it
rejects agents newer than itself. Phase 3 agents cannot be upgraded
remotely; install a current binary on them once.

## Not yet

- File store: replicas on owner nodes (the control plane is the only full
  copy, so back up `<cluster dir>/blobs`), an S3-compatible gateway, and
  per-node quotas for caches beyond the size cap. Private images are pulled
  from the registry again for each job (nodes that already loaded the
  digest skip the download from the store).
- GPUs: Intel and Apple GPUs are listed but no executor uses them (the
  WebGPU executor of phase 14 will); AMD GPUs are not benchmarked, only
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
  and Apple GPUs report no metrics; the 10 s tier is lost on a control
  plane restart; alert rules are cluster-wide (no per-node or per-label
  thresholds, no silences or repeat notices); node-to-node latency is not
  measured.
- Remaining roadmap phases (12 to 18): a web dashboard, Raft, WebGPU, the confidential
  tier, self-hosting 1.0 (SSO, audit log, external security review), the
  hub, buying CU. See the roadmap in the README.
- Windows: microVMs on ARM64, Hyper-V HCS as a faster backend, a Docker
  path for machines nobody is signed into (dockerd inside a WSL distro).
- Rollouts go to every node; there is no canary group or per-node
  selection yet, and the control plane does not upgrade itself.
- High availability: one control plane; Raft (openraft) planned.
- Access tickets are handed out by the admin; signing in with SSO and
  mapping groups to roles is phase 16.
- CU cannot be bought or exchanged, and there is no platform fee; both
  belong to the hub.
- "Honeypot" tasks with known answers (needed for non-deterministic Docker
  and microVM work, which checks cannot cover). The CPU benchmark
  challenge keeps the billed speed honest, not the results. Disk and
  memory figures are not challenged yet, and no scheduling uses them.
- MicroVM: a snapshot pool for faster boots.
- Availability: running tasks are not stopped when a node pauses, and
  idle detection under a Windows service is not possible yet.
- WASI preview 2 components, job garbage collection.
