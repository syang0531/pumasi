# Architecture

This document records how the cluster is built and why: the private
cluster of phase 1, the invited providers, receipts, credit ledger and
result checks of phase 2, the microVM executor of phase 3, the
releases, OS services and signed agent upgrades of phase 4, the
Windows support of phase 5, and the Mac support and availability policy of
phase 6. The
product-level research and the decisions behind it (dual executor, fiat-only
billing, honest confidentiality tiers) live in the research doc shared with
the project; this file covers the code.

## Crates

| Crate | Role |
| --- | --- |
| `pumasi-proto` | Job spec (`job.toml`), wire messages, framing, tickets, endpoint helpers, receipts (signing and verification), price list and policy. No business logic. |
| `pumasi-guest` | `pumasi-guest`, the init process inside every microVM (a static musl binary), and the disk protocol the agent shares with it. |
| `pumasi-agent` | `WasmExecutor` (Wasmtime sandbox, deterministic mode), `DockerExecutor` (Docker CLI), `MicrovmExecutor` (registry client, ext4 formatter, Firecracker/QEMU/Virtualization.framework), `availability` (power, idle and hours policy), `platform` (sparse files, Windows Job Objects, the WHPX check) and the `Agent` session loop, which signs usage. |
| `pumasi-cluster` | `State` (membership, queue, scheduler), `ledger.rs` (accounts, holds), `check.rs` (result checks), `Store` (redb), `Cluster` (iroh server), `AdminClient`. |
| `pumasi-cli` | The `pumasi` binary. Thin: parses args, calls the crates above, prints. `service.rs` installs the agent as an OS service, `upgrade.rs` drives rollouts. |

The marketplace hub, Madang (마당), is not in this repository; it is a
separate, closed-source service in the private `madang` repository. The agent and control plane stay Apache-2.0.

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
  control plane sends `ToAgent::{Assign, Cancel, Upgrade}` and the agent
  sends `FromAgent::{Heartbeat, Done, UpgradeFailed}`. Further streams the
  agent opens on the same connection each carry one `FetchRequest` (a
  chunk of a release binary) and its `FetchReply`. Agents always dial out,
  so they work from behind NAT without port forwarding.
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
unchanged. Phase 4 adds no new record layout: the rollout lives under the
new `meta.rollout` key and uploaded binaries are files in
`<cluster dir>/releases/`. Phase 6 adds none either. `PROTOCOL_VERSION`
is 6: protocol 5 (phase 4) added `UpgradeHello` and the upgrade messages,
protocol 6 appends `AgentHello::availability` and
`FromAgent::Availability`. The control plane accepts hellos from
`MIN_AGENT_PROTOCOL` (5) up: `AgentHello::decode` tries the current layout
and falls back to the protocol 5 one (available), so phase 4 and 5 agents
keep working and can take a rollout. Upgrade the control plane first; it
rejects agents newer than itself. Phase 3 agents cannot be upgraded
remotely; install a current binary on them once.

## Not yet

- Remaining roadmap phases (7 to 17): benchmarks and compute units (CU),
  a cluster file store, GPUs, cluster management, monitoring, a web
  dashboard, Raft, WebGPU, the confidential tier, the hub, buying CU. See
  the roadmap in the README.
- Windows: microVMs on ARM64, Hyper-V HCS as a faster backend, a Docker
  path for machines nobody is signed into (dockerd inside a WSL distro).
- Rollouts go to every node; there is no canary group or per-node
  selection yet, and the control plane does not upgrade itself.
- High availability: one control plane; Raft (openraft) planned.
- Accounts other than `owner` exist only as ledger entries; consumer
  tickets that let other people submit (and see only their jobs) are next.
- Credits are not connected to a payment provider, and there is no
  platform fee.
- Benchmarks at join and "honeypot" tasks with known answers (needed for
  non-deterministic Docker and microVM work, which checks cannot cover).
- MicroVM: private registries, network (a tap device with egress rules),
  GPU passthrough (VFIO), a snapshot pool for faster boots.
- Availability: running tasks are not stopped when a node pauses, and
  idle detection under a Windows service is not possible yet.
- WASI preview 2 components, job garbage collection.
- Large files: inputs and outputs travel through the control plane and are
  capped (48 MiB in per job, 32 MiB out per task). Direct node-to-node
  transfer with iroh-blobs would lift that.
