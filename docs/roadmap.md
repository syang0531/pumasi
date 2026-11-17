# Roadmap after 1.3

Phases 1 to 17 (self-hosting 1.0) are done, and the 1.1 to 1.3 releases
fixed what the first weeks of real use found: a five-machine home cluster
of Windows PCs (one with an RTX 3080), a control-plane-only PC, a 2014
iMac on Big Sur, and Claude's cloud sessions using it through the HTTP
API. Phases 18 and 19 (the hub, Madang, and buying CU) are **on hold**
until the project owner asks for them; nothing below depends on them.

Phases 20 to 28 (1.4 to 1.12) covered what was missing in the scope the
project set itself: GPUs, Windows, old Intel Macs, the file store,
k8s-style cluster management, the dashboard and monitoring. Most items
came from the issues filed from real use, the "Not yet" list in
[architecture.md](architecture.md) and the README's known gaps; the order
put first what the home cluster runs into every day.

Phases 29 to 36 (1.13 to 1.20) take what those phases left over, in the
same scope, plus the parts of the research that are still open: result
checks for work that is not deterministic ("honeypot" tasks), disk and
memory figures that are challenged like the CPU's, and upgrades that
reach the control plane itself. There were no open issues when they were
planned (2026-11-15); the sources are each phase's "what differs from the
plan", the "Not yet" list, the handoffs' leftovers (two CI flakes, the
HCS measurement, signing) and the research's recommendations. The order
again starts with what the home cluster meets every day (upgrading the
control plane by hand, the iMac's slow microVMs) and ends with what
matters once outside providers are invited.

Each phase is one pull request (or a few), ends in a minor release, and
has a gate: what CI checks and what the owner checks by hand on real
machines.

| Phase | Release | Scope | Status |
| --- | --- | --- | --- |
| 20 | 1.4 | Issues from real use: account policies and agent options without a terminal, task timings and names | done |
| 21 | 1.5 | Machines that sleep and machines without Docker | done |
| 22 | 1.6 | Monitoring: live microVM output, every GPU and temperature, node-to-node network | done |
| 23 | 1.7 | Dashboard: change settings in the browser, sessions that survive restarts, canary rollouts | done |
| 24 | 1.8 | File store on several machines and an S3-compatible gateway | done |
| 25 | 1.9 | Scheduling: gangs, affinity, CPU and memory quotas, checkpoints | done |
| 26 | 1.10 | GPUs: WebGPU timeouts and checks, AMD, shared GPUs | done |
| 27 | 1.11 | Windows: Docker without Docker Desktop, ARM64 microVMs | done |
| 28 | 1.12 | Operations: limits, audit log pruning, SCIM, the CLI behind an HTTPS proxy | done |
| 29 | 1.13 | Upgrades that reach the control plane, and the flaky checks | done |
| 30 | 1.14 | Old Intel Macs: faster microVMs, microVM checkpoints, Mac sensors | done |
| 31 | 1.15 | Monitoring and the dashboard: what 1.6 and 1.7 left in the CLI | done |
| 32 | 1.16 | GPUs: enforced memory for parts of GPUs, drawing over WebGPU, several adapters | planned |
| 33 | 1.17 | Windows: HCS if it is faster, signed binaries, a cleaner WSL Docker | planned |
| 34 | 1.18 | File store and microVM networks: node caches, images from the store, UDP and allow lists | planned |
| 35 | 1.19 | High availability and the remote API: reads from followers, limits that survive a failover, ACME | planned |
| 36 | 1.20 | Result checks: honeypot tasks, disk and memory challenges, re-benchmarks | planned |
| 18 | | Hub (Madang): provider discovery, latency probes, trust scores, cross-group CU ledger | on hold |
| 19 | | Buying CU with won through the hub (Toss Payments) | on hold |

## 20. Issues from real use: account policies and agent options without a terminal, task timings and names (1.4)

Done in 1.4.0 ([release notes](releases/v1.4.0.md)).

Why: the open GitHub issues, all filed while running the home cluster and
using it from a cloud sandbox over the HTTP API. They are small next to
the phases after this one, and phase 21 adds agent options that need #42
to be practical.

- **Agent options without reinstalling the service** ([#42](https://github.com/syang0531/pumasi/issues/42)).
  The service keeps its agent options in `agent.toml` in the state
  directory; `pumasi service status` prints them; `pumasi service set
  [ROLE] -- --allow-webgpu --no-allow-docker` changes only the given ones
  and restarts the service. Services installed by older releases are
  moved to the file on their first `service set`. The control plane and
  the dashboard services work the same way (`control-plane.toml`,
  `ui.toml`).
- **Account quotas and job policies from anywhere** ([#43](https://github.com/syang0531/pumasi/issues/43)).
  A documented `GET`/`PATCH /api/v1/accounts/{name}/policy` in
  `openapi.json`, where PATCH changes only the given fields and adds or
  removes list entries (`{"images": {"add": [...], "remove": [...]}}`); the
  CLI's `--add-images`/`--remove-images` (and the same for executors and
  tolerations) next to the replacing `--images`; an Accounts page on the
  dashboard where admins see and edit each account's quota and policy.
- **Submitters can read their own policy** ([#38](https://github.com/syang0531/pumasi/issues/38)):
  `whoami` includes it, `POST /api/v1/jobs?validate=1` runs the policy,
  quota and capacity checks without creating the job, and `capacity` in
  `/api/v1/status` counts only nodes that the caller's policy can use.
- **Task timings** ([#40](https://github.com/syang0531/pumasi/issues/40)):
  `queued_ms`, `fetch_ms`, `pull_ms` (0 when the image was cached),
  `run_ms` and `upload_ms` per task in the API, `pumasi job` and the
  dashboard, and the docs say which of them `wall_ms` and the receipt
  cover: `run_ms` only, for Docker as it already was for microVMs.
- **Node names in task views** ([#41](https://github.com/syang0531/pumasi/issues/41)):
  `node_name` next to `node` in the API, and names in `pumasi job`.

Gate: unit and end-to-end tests for `service set` on all three service
managers (CI's service job), the PATCH semantics and its OpenAPI schema,
`validate=1` answering 200 and 403 like a real submission, timings whose
`run_ms` is the task's `wall_ms` with the pull told apart, and a
Playwright test of the Accounts page; the curl
examples in docs/api.md run in CI. By hand: turn on WebGPU on every agent
with one `service set` each, and add an image to the cloud account's
policy from the dashboard through the tunnel.

## 21. Machines that sleep and machines without Docker (1.5)

Done in 1.5.0 ([release notes](releases/v1.5.0.md)). Two things differ
from the plan below: on Windows the blocker is a power request
(`PowerCreateRequest`), and the idle helper is started by the agent
service in the user's session and read through an anonymous pipe, not a
scheduled task with a named pipe
([why](architecture.md#idle-time-under-a-windows-service)).

Why: home machines sleep. When the iMac sleeps its node goes offline
after 10 seconds and its running tasks start again elsewhere from the
beginning; nothing can wake it when work is waiting for it. And old Macs,
or Windows PCs without Docker Desktop, cannot take Docker jobs even though
the same image would run in a microVM there.

- **Stay awake while working.** The agent holds the OS's sleep blocker
  while it runs tasks (IOKit power assertion on macOS,
  `SetThreadExecutionState` on Windows, a logind inhibitor on Linux) and
  lets go when it is idle, so the display still sleeps and the machine
  sleeps on its normal schedule afterwards. `--allow-sleep` opts out.
- **Say goodbye before sleeping.** The agent tells the control plane when
  the OS announces sleep (macOS sleep notifications, Windows
  `PBT_APMSUSPEND`, logind `PrepareForSleep`): its tasks are requeued at
  once instead of after the heartbeat timeout, the node shows as `asleep`
  rather than `offline`, and the node-offline alert skips it.
- **Wake-on-LAN.** Agents report their wired MAC addresses and LAN
  subnets. When queued work fits only sleeping nodes (or has waited longer
  than `--wake-after`), the control plane asks an online agent on the same
  subnet to send the magic packet. `pumasi node wake NODE` and a Wake
  button on the dashboard's node page do it by hand. Per node, opt-in
  (`--wake-on-lan`); the README says which macOS, Windows and BIOS settings
  it needs.
- **`executor = "image"`.** An image job that runs as Docker where the
  node has Docker and as a microVM where it does not (old Macs, Windows
  PCs without Docker Desktop). Jobs that need what only one of them offers
  (`gpus` through Docker, or the confidential tier) are placed
  accordingly; `pumasi job` shows which one each task used.
- **Idle detection under a Windows service.** A small helper started at
  sign-in (`pumasi idle-helper`, a scheduled task) reports the last input
  time to the service over a named pipe, so `--idle-minutes` works for the
  agent service too.
- **`--on-pause requeue`.** When a paused machine's owner comes back (on
  battery, input, outside the hours), stop its running tasks and run them
  elsewhere instead of finishing them (the default stays `finish`).
- A guide for putting Linux on a 2013-2014 Mac (Docker, KVM microVMs and
  WebGPU over Vulkan instead of macOS's limits).

Gate: unit and end-to-end tests with stand-in power and wake backends, the
magic packet's format, and an `image` job placed on a Docker node and on a
microVM-only node in CI. By hand: the iMac does not sleep during a
30-minute task and sleeps after it; a job selected to the sleeping iMac
wakes it from another machine on the LAN and runs; putting it to sleep
mid-task requeues the task within two seconds without an offline alert;
an `image` job runs as Docker on a Windows PC and as a microVM on the
iMac; a Windows agent service with `--idle-minutes 5` pauses while someone
types.

## 22. Monitoring: live microVM output, every GPU and temperature, node-to-node network (1.6)

Done in 1.6.0 ([release notes](releases/v1.6.0.md)). What differs from
the plan below: the guest streams its output over the console it already
has (Firecracker has no virtio console), as framed lines with an offset
and a checksum, and only the first MiB of each stream is live; per-node
thresholds and silences are set with the CLI, the API and the dashboard,
not in `cluster.toml` (`apply` keeps the cluster-wide rules), and the
dashboard edits silences but not yet the rules themselves (phase 23).
WebGPU adapters are the same GPUs, so they report through the vendor
paths ([details](architecture.md#phase-22-monitoring-everywhere-16)).

- **Live microVM output.** The guest streams stdout and stderr over a
  virtio console while the task runs (Firecracker, QEMU, WHPX and vz), so
  `pumasi logs -f` and the dashboard follow microVM tasks as they do
  Docker and wasm ones.
- **Temperatures on Windows** (WMI on its own thread: ACPI thermal zones,
  and LibreHardwareMonitor's sensors when it is running) and **GPU metrics
  for every vendor**: utilization and memory for Intel, AMD and Apple GPUs
  (IOAccelerator statistics on macOS, sysfs and DRM fdinfo on Linux, the
  GPU Engine performance counters on Windows), WebGPU adapters included.
- **The 10-second tier survives a control plane restart** (the last hour
  is written with the rest of the state).
- **Node-to-node latency and bandwidth.** Agents probe each other over
  iroh now and then (small, rate-limited); `pumasi net` and a dashboard
  matrix show it, and the file store picks the closest node cache.
- **Alerts:** silences, repeat and resolved notices, thresholds per node
  or label, and a test button for webhooks.

Gate: CI checks the live stream on Linux microVMs (KVM), the restart, the
probe matrix between local agents and silences. By hand: `logs -f` shows
a microVM task's output within a second on the iMac and a Windows PC;
Windows PCs show a temperature; the iMac's AMD GPU and an Intel GPU show
utilization.

## 23. Dashboard: change settings in the browser, sessions that survive restarts, canary rollouts (1.7)

Done in 1.7.0 ([release notes](releases/v1.7.0.md)). What differs from
the plan below: node labels and taints were already editable, so the
`cluster.toml` editor covers them with the alert rules; it refuses
schedules and `url_env` and never prunes (both would read files or the
environment on the dashboard's machine); per-node alert thresholds stay
in the CLI; and the Upgrade panel picks canaries only for a published
release (your own binaries take `pumasi upgrade rollout --canary`)
([details](architecture.md#phase-23-the-dashboard-changes-settings-17)).

- **Sessions kept in the control plane's state**, so restarting or
  upgrading `pumasi ui` (which 1.3's one-button upgrade does) no longer
  signs everyone out; a list of signed-in sessions with Revoke.
- **Edit in the browser** what is shown read-only now: alert rules,
  webhooks and silences, prices, cron schedules, node labels and taints, and each node's availability (slots,
  CPU cap, hours, AC power, idle minutes), written to the agent's
  `agent.toml` (phase 20) and applied without a manual restart. A `cluster.toml` editor shows the `apply` diff
  before applying it.
- **Canary rollouts:** `pumasi upgrade latest --canary NODE|SELECTOR`
  upgrades some nodes first and the rest after they have run tasks
  cleanly for a while; per-node selection in the Upgrade panel.
- **Streaming charts** (server-sent events) instead of 10-second
  snapshots, and a layout that works on a phone.

Gate: Playwright covers every new form, a `pumasi ui` restart that keeps
the sign-in, and a canary rollout between two local agents. By hand: the
owner changes an alert rule and a node's hours from a phone through the
tunnel, and upgrades the cluster with a canary.

## 24. File store on several machines and an S3-compatible gateway (1.8)

Done in 1.8.0 ([release notes](releases/v1.8.0.md)). What differs from
the plan below: a storage node is an owner node with the `storage` label,
whose value is its quota (`storage=200G`, or `yes` for 90% of its disk);
the gateway also offers CopyObject, DeleteObjects, ListObjects v1,
presigned URLs and streamed uploads, and each gateway write makes a
dataset version that replaces the gateway's previous one; damage in the
control plane's own store is reported, and repaired by restarting it
without its `blobs` directory
([details](architecture.md#phase-24-the-file-store-on-several-machines-18)).

Why: the store's only full copy lives on the control plane's machine; on
a single control plane a lost disk loses every dataset.

- **Replicas on owner nodes:** a replication factor per dataset (default
  2), placed on nodes labelled for storage within a disk quota each,
  repaired when a node is removed or stays offline, and scrubbed (hashes
  checked) now and then.
- **Writes acknowledged once they are on enough copies**, control plane
  replicas included, which closes the 30-second window after a failover;
  release binaries uploaded for a rollout are copied to the other control
  planes too.
- **S3-compatible gateway** in `pumasi ui` (`/s3`): GetObject, PutObject,
  HeadObject, ListObjectsV2, DeleteObject and multipart uploads, signed
  with SigV4 keys issued from access tickets, so `aws s3`, `rclone` and
  boto3 work against datasets.
- The dashboard's storage page shows versions, replicas and quotas.

Gate: CI deletes the control plane's blob directory and gets every dataset
back from node replicas, re-replicates after a node is removed, and runs
`aws s3 cp`, `rclone sync` and a multipart upload against the gateway. By
hand: a dataset survives moving the control plane to another PC without
copying `blobs`.

## 25. Scheduling: gangs, affinity, CPU and memory quotas, checkpoints (1.9)

Done in 1.9.0 ([release notes](releases/v1.9.0.md)). What differs from
the plan below: `spread` caps a group with `max` or only prefers the
emptiest without it; affinity names jobs of the same account by name or
id; a machine going to sleep gives checkpointing runs 10 seconds at most;
microVM tasks cannot checkpoint yet, and a WASM task is stopped at once
with what it saved so far (it cannot catch a signal); a checkpoint is
saved when a run is moved, not periodically
([details](architecture.md#phase-25-scheduling-19)).

- **Gang scheduling** (`gang = true`): all tasks of a job start together
  or none do.
- **Affinity and anti-affinity**: spread tasks over nodes or labels, run
  next to or away from another job's tasks.
- **Quotas in CPU and memory** (not only task counts), and quotas per
  node label.
- **Checkpoints:** preemption, drain, `--on-pause requeue` and sleep send
  the task a stop signal with a grace period; whatever it writes to its
  checkpoint directory goes to the store and comes back when the task runs
  again. An example resumes a long computation after being preempted.
- **Cron schedules in any time zone** (`tz = "Asia/Seoul"`).

Gate: end-to-end tests for each, including a checkpointed task moved off
a drained node that finishes without starting over.

## 26. GPUs: WebGPU timeouts and checks, AMD, shared GPUs (1.10)

Done in 1.10.0 ([release notes](releases/v1.10.0.md)). What differs from
the plan below: the timeout kills a worker process that holds the task's
device (dispatches are the bounded pieces, at most 16 queued), rather
than dropping the device inside the agent; AMD GPUs (and every other GPU
the CUDA benchmark cannot run on) are benchmarked and challenged over
WebGPU rather than with ROCm; parts of GPUs are Docker-only, in
hundredths of one GPU, share memory on trust, and can be turned off per
node (`gpu.share=off`); MIG slices are estimated, not benchmarked
([details](architecture.md#phase-26-gpus-110)).

- **WebGPU:** the job's timeout stops a runaway shader (work is split into
  bounded submissions and the device is dropped on timeout), a GPU
  challenge over WebGPU so untrusted adapters are billed at their real
  speed, and optional features (`shader-f16`, subgroups) where the
  adapter has them.
- **AMD:** a benchmark, the GPU challenge and Docker GPU jobs through
  `/dev/kfd` and `/dev/dri` (ROCm images).
- **Shared GPUs:** `gpus = 0.5` style fractions on time-sliced GPUs, and
  NVIDIA MIG slices scheduled as separate devices.

Gate: CI checks the timeout and the challenge on software rasterizers.
By hand: a never-ending shader stops at its timeout on the RTX 3080 and
the iMac; two half-GPU jobs share the RTX 3080; AMD Docker jobs only where
the owner has an AMD Linux machine.

## 27. Windows: Docker without Docker Desktop, ARM64 microVMs (1.11)

Done in 1.11.0 ([release notes](releases/v1.11.0.md)). The distro is
started at boot by a scheduled task that logs a hidden account on (WSL
does not run as the agent service's LocalSystem), and the agent starts
that task again when Docker stops answering. HCS was not adopted: the
cost is a second guest kernel and disk format, and CI cannot time it
against WHPX, so `scripts/windows/boot-times.ps1` measures it on real PCs
first. Signing still waits for SignPath Foundation.

- **Docker for machines nobody signs into:** `pumasi docker setup`
  installs dockerd in a small WSL distro that Pumasi manages, started by
  the agent service at boot, NVIDIA GPUs included through WSL's CUDA
  support; Docker Desktop stays supported.
- **MicroVMs on Windows on ARM** with QEMU (WHPX where QEMU supports it on
  ARM64, emulation otherwise).
- A measured look at Hyper-V's compute service (HCS) as a faster backend
  than QEMU with WHPX, adopted only if it is clearly faster.
- Windows binaries signed once SignPath Foundation approves the project
  (the workflow is ready; [signing.md](signing.md)).

Gate: CI installs the WSL distro on a Windows runner (or checks the steps
up to what nested virtualization allows). By hand: a fresh Windows PC
without Docker Desktop runs Docker and GPU jobs as a service after a
reboot, before anyone signs in.

## 28. Operations: limits, audit log pruning, SCIM, the CLI behind an HTTPS proxy (1.12)

Done in 1.12.0 ([release notes](releases/v1.12.0.md),
[details](architecture.md#phase-28-operations-112)). Shared limits apply
to dashboards run with the admin ticket and live in the leader's memory
(a failover resets them). Keycloak has no SCIM client of its own, so CI
sends the requests Entra ID and Okta send, and the Keycloak end-to-end
test plays the directory with curl for a user signed in through
Keycloak. Followers answer the dashboard's status, events and current
metrics for admins and viewers; the API and the CLI still read from the
leader. Proxy settings apply to agents and control planes too
(`PUMASI_PROXY=off` opts out).

- Per-peer limits on unauthenticated sign-in calls at the control plane,
  and request limits and failed sign-in counts shared by every `pumasi ui`
  process (kept in the control plane).
- **Audit log pruning** behind a signed checkpoint, so `audit verify`
  still proves nothing before the checkpoint changed.
- **SCIM provisioning** (Entra ID, Okta, Keycloak): people removed in the
  identity provider lose their sessions at once.
- **The CLI behind an HTTPS proxy** (`HTTPS_PROXY` with CONNECT and an
  extra CA for the relay), for clients such as cloud sandboxes that only
  have a proxy; the HTTP API stays the way for clients that keep keys in
  headers.
- Followers answer reads (status, events, metrics), so a busy leader does
  less.

Gate: CI tests the limits across two `pumasi ui` processes, a pruned log
that still verifies, SCIM against Keycloak, and the CLI through a local
CONNECT proxy.

## 29. Upgrades that reach the control plane, and the flaky checks (1.13)

Done in 1.13.0 ([release notes](releases/v1.13.0.md),
[details](architecture.md#phase-29-upgrades-that-reach-the-control-plane-113)).
What differs from the plan: openraft 0.9 has no leadership transfer, so
the leader steps down by restarting last, once every other replica runs
the new version, and the others elect a new leader (one that comes back
within the election timeout may win again, already upgraded). Replicas
before 1.13 cannot take part, so a cluster with replicas goes to 1.13 by
hand once. A dashboard alone on its machine installs the rollout's binary
itself; one sharing the binary follows it, down too after a rollback.
CI's "previous release" is the latest published one (1.11.0 when this
was written, which installs and restarts without a trial) plus a build of
this commit for the trial and rollback. The three flaky tests each had a
real cause (the purge point read back with the wrong type, a failed start
keeping its databases open, the agent cache's garbage collection removing
a download in progress, and control plane stalls counted against nodes'
heartbeats). The Intel Mac flake is not explained: CI now records what
the client sees when it happens.

Why: every release since 1.3 says "upgrade the control plane first (stop
it, replace the binary, start it)", by hand on its machine, and on three
replicas that is three machines. Two CI flakes have been rerun since phase
20 without a cause, and a third (a restarted replica that cannot read its
own log) showed up while this roadmap was written; one of them (the local control plane answering only
after a minute on the Intel Mac runner) may be a real slowness on old
Macs.

- **The control plane upgrades itself.** `pumasi upgrade latest` and
  `upgrade rollout` (and the dashboard's Upgrade button) upgrade the
  control plane after the agents, through its service manager, with the
  same signed-binary check and a rollback when the new binary does not
  come back healthy within a timeout. With replicas: followers one at a
  time, then a leadership transfer, then the old leader, never losing the
  quorum; `pumasi upgrade status` shows each replica's version.
- **The dashboard service** (`service install ui`) is upgraded with its
  control plane, so the cluster ends up on one version without a terminal.
- **The Intel Mac flake:** find why `pumasi status` fails for 1 to 2
  minutes after the service starts on `macos-15-intel` (the service logs
  `control plane started` within a second), and fix the cause rather than
  the wait.
- **The Windows heartbeat flake** in `manage::selectors_drain_and_preemption`
  (two agents missing heartbeats 5 seconds apart): find whether the test
  or the agent is at fault; a real agent should not miss a heartbeat while
  idle.
- **The HA restart failure** seen on this roadmap's own pull request
  (#60, `ubuntu-latest`, `three_replicas_survive_losing_the_leader`):
  the crashed leader would not start again because openraft asked for a
  log entry at index 31975955732986110, far past anything written, and
  every retry after that failed with the file store's database still
  locked by the failed start. Find where the bogus index comes from (the
  log, the committed or purged id, or the applied id in `state.redb`)
  and release everything a failed `Cluster::start` opened.
- The long checks also soak on Windows (today Linux and macOS only).

Gate: CI upgrades a service-installed control plane from the previous
release on Linux, macOS and Windows, rolls a three-replica cluster to a
new binary without failing a running job, and rolls back a binary that
exits at start; the three flaky tests pass 50 runs in a row on their
runners.
By hand: the owner upgrades the home cluster, control plane included, from
the dashboard through the tunnel.

## 30. Old Intel Macs: faster microVMs, microVM checkpoints, Mac sensors (1.14)

Done in 1.14.0 ([release notes](releases/v1.14.0.md),
[details](architecture.md#phase-30-old-intel-macs-114)).
What differs from the plan: unpacked image disks were already kept by
digest since phase 3, so the gain is the warm guest, which skips the VMM
start, the kernel boot and the mounts. The pool keeps at most one spare
guest per image, CPU count and memory size, two in all by default
(`--warm-vms`), within a quarter of the machine's memory rather than the
node's slots (a waiting guest runs nothing, and a task that takes it uses
its slot as before); tasks with a network card or GPUs boot cold. A
microVM's checkpoint crosses the VM's existing input and output disks as
`checkpoint/` entries rather than a disk of its own, so the agent stores
it exactly like a Docker task's. The Mac's CPU package power is read
(`pumasi sensors`) but not sent to the control plane yet, since that
needs a new metrics field. Release builds of `x86_64-apple-darwin` were
already made on Apple Silicon; CI now also builds it there, checks its
minimum OS, and runs the Intel tests under Rosetta 2. The recorded SMC and
IOReport fixtures are written in the formats `smc -l` and IOReport print
on a 2014 iMac, a 2017 MacBook Pro and an M1, not captured from the
owner's iMac; `pumasi sensors --raw` prints the same format, so a real
dump can replace them. Warm guests under Virtualization.framework and
WHPX are not checked in CI (no runner can boot them): a guest that does
not see its task within 15 seconds falls back to a cold boot and turns
the pool off on that node with a warning.

Why: the 2014 iMac takes image jobs only as microVMs (no Docker on its
macOS), and every task pays a full boot and image unpack; microVM tasks
cannot checkpoint, so when it sleeps or its owner comes back they start
over elsewhere; and Macs report no temperature or GPU power, so the
temperature alert never fires for them. GitHub's Intel Mac runner
(`macos-15-intel`) is announced until autumn 2027.

- **Warm microVMs:** keep a small pool of booted, idle guests per image
  (bounded by memory and the node's slots, off on battery) and hand a task
  to one; reuse unpacked image disks between tasks of the same digest.
  On every backend, not only vz.
- **MicroVM checkpoints:** the guest gets the same stop signal and grace
  period as Docker tasks, and its checkpoint directory is a disk the agent
  stores like a Docker task's (phase 25), so a microVM task moved by sleep,
  drain or `--on-pause requeue` resumes.
- **Mac sensors:** CPU and GPU temperatures and package power through the
  SMC on Intel Macs and the IOReport/HID sensors on Apple Silicon, in
  `top`, the dashboard and the temperature alert.
- **After the Intel runner:** build `x86_64-apple-darwin` on an Apple
  Silicon runner, keep the minimum-OS check on the binary, and run the
  Intel tests under Rosetta 2 where they can; the README says what is then
  checked on real Intel Macs only.

Gate: CI times a second microVM task of the same image on Linux (KVM)
against the first and expects it well under half; a checkpointed microVM
task moved off a drained node finishes without starting over; sensor
parsing is tested against recorded SMC and IOReport output. By hand: on
the iMac, the second `vm run` of an image starts in seconds, a microVM
task resumes after the iMac sleeps, and the dashboard shows its CPU
temperature.

## 31. Monitoring and the dashboard: what 1.6 and 1.7 left in the CLI (1.15)

Done in 1.15.0 ([release notes](releases/v1.15.0.md),
[details](architecture.md#phase-31-monitoring-and-the-dashboard-115)).
What differs from the plan: postcard keeps existing messages as they are,
so the new data travels in new calls rather than new fields: the alert
tuning and silences of a `cluster.toml` go in `ApplyMore`, each event's
message key in `EventsKeyed`, the firing alerts' keys in `AlertTexts`, and
the extra sensor readings in a `MetricsExtra` message beside each sample
(which also carries the Mac's and RAPL's CPU package power to the
dashboard, left over from phase 30). Pushed charts are a long poll
(`MetricsWait`) that the control plane answers when the node's next sample
arrives, not a new stream; the dashboard's server-sent events follow it,
and fall back to the 2-second poll against an older control plane. A
silence in the file gets its id from its name, so `apply` finds it again,
and only `apply --prune` removes one the file no longer lists (the
dashboard never prunes); the editor shows the thresholds and schedules
(without their jobs, which `apply` then keeps) but not the silences, whose
names the control plane does not keep. The guest had no ring buffer: it
stopped printing after 1 MiB, and now a separate thread reads the output
files behind the task and streams up to the 4 MiB the result keeps. The
serial console of Firecracker and QEMU turned out too slow for that (a
few KiB a second, with the vCPU held up while it prints), so on those
backends the output goes to a region at the end of the output disk that
the agent reads from the disk file; vz keeps its virtio console. Windows reports a GPU's temperature
(from `D3DKMTQueryAdapterInfo`, WDDM 2.4 drivers) but not its power, which
the driver gives only as a share of a limit it does not name. Intel's
integrated GPUs have no sensors of their own and report the CPU package's
temperature and RAPL's `uncore` power. RAPL counters are readable by root
only on most distributions, so an agent running as another user reports no
CPU power on Linux. Per-node thresholds name nodes by the `name` label,
which cannot hold spaces. The by-hand checks (a phone, a Linux machine with
an Intel or AMD GPU) are in the phase's QA list.

Why: phases 22 and 23 left several things CLI-only or partial, and the
Korean dashboard still shows events in English.

- **Everything in `cluster.toml` and the browser:** per-node and
  per-label alert thresholds and silences in `cluster.toml` (`apply` and
  its diff), and the dashboard's editor takes schedules too; secrets are
  listed, added and rotated from the dashboard (values never shown again).
- **Charts pushed by the control plane** over the stream instead of
  polled every 2 seconds behind it.
- **Live microVM output past the first MiB** (the guest's ring buffer
  is drained continuously instead of stopping).
- **GPU temperature and power for Intel and AMD on Linux** (hwmon) and
  on Windows where the driver exposes them; Macs come in phase 30.
- **The network to the file store:** agents measure latency and
  throughput to the control plane's store, not only to each other, and
  the matrix shows it.
- **Events and alert notices in Korean** on the Korean dashboard (a
  message key and arguments with each event instead of English text).

Gate: Playwright covers the new forms and a chart that moves without a
poll; CI checks a 5 MiB live microVM stream on Linux and an `apply` of
per-node thresholds. By hand: the owner silences one node and edits a
schedule from a phone; a Linux machine with an Intel or AMD GPU shows its
temperature.

## 32. GPUs: enforced memory for parts of GPUs, drawing over WebGPU, several adapters (1.16)

Why: parts of GPUs (`gpus = 0.5`) share memory on trust, so one task can
take all of the RTX 3080's memory from the other; WebGPU offers compute
shaders only, so the iMac's and other CUDA-less GPUs cannot draw; and a
WebGPU task gets one adapter.

- **Memory limits for parts of NVIDIA GPUs** with the CUDA MPS server
  (`CUDA_MPS_PINNED_DEVICE_MEM_LIMIT` and an active-thread percentage per
  task), started by the agent where the driver allows it; time slicing
  without limits stays the fallback and is shown as such.
- **MIG slices benchmarked** like whole GPUs instead of estimated.
- **WebGPU textures, samplers and render passes** (and more than one bind
  group), with a rendering example checked against the CPU.
- **Several adapters per task** (`gpus = 2` on wasm jobs) where the node
  has them.
- **WASI preview 2 components** for wasm jobs, and `wasi:webgpu` once
  Wasmtime runs it, next to today's core modules.

Gate: CI runs the drawing example on software rasterizers on Linux,
Windows and macOS, and a component job everywhere; the MPS limit is tested
against a stand-in. By hand: two half-GPU jobs on the RTX 3080, where the
one that asks for more than its half fails with an out-of-memory error
and the other keeps running; the drawing example on the iMac's AMD GPU.

## 33. Windows: HCS if it is faster, signed binaries, a cleaner WSL Docker (1.17)

Why: phase 27 left HCS waiting for `scripts/windows/boot-times.ps1` on
real PCs, signing waiting for SignPath Foundation, and `docker remove`
leaving the hidden account's profile until a reboot. This phase comes
after the measurement and the approval have had time to arrive.

- **HCS backend, only if the measurement says so:** when
  `boot-times.ps1` shows HCS booting clearly faster than QEMU with WHPX
  on the owner's PCs, add it as a microVM backend (its own guest kernel
  build and VHD), picked automatically where it works; otherwise record
  the numbers and drop it from the roadmap.
- **Signed Windows binaries** once SignPath approves (the workflow's
  `sign` job is ready); Smart App Control then stops blocking installs and
  upgrades.
- **`pumasi docker remove` finishes in one go** (stop the WSL VM, then
  remove the profile), and `docker setup` repairs a distro whose task or
  account was changed by hand.
- Windows desktops without LibreHardwareMonitor: a documented, opt-in
  way to get CPU temperatures (no kernel driver ships with Pumasi).

Gate: CI runs the HCS backend's microVM tests where the runner allows it
(or its unit tests and a recorded boot otherwise), and `docker remove`
then `setup` on a Windows runner. By hand: `boot-times.ps1` numbers in the
release notes; a signed binary installs with Smart App Control on.

## 34. File store and microVM networks: node caches, images from the store, UDP and allow lists (1.18)

Why: node caches have only a size cap, private images are pulled from the
registry again for each job, and microVM networks forward TCP and DNS
only, with no per-job allow list and no billing for egress.

- **Node cache quotas** per node (like the storage label's quota) and
  per account, with eviction that keeps what queued jobs need.
- **Images through the store:** a private image's layers are pulled once
  into the store and nodes fetch them from the closest copy, for Docker
  and microVMs, so the registry is not asked again for each job.
- **MicroVM networks:** UDP (and so QUIC), IPv6 where the host has it,
  per-job egress allow lists (`network = ["pypi.org:443", ...]`) and
  bandwidth caps, with the same private-range blocking as today; egress
  counted per task and billed in CU at the price list's rate.
- Firecracker without QEMU gets a network where the agent may create TAP
  devices (as root on Linux).

Gate: CI pulls a private image once for a 4-task job over two nodes, runs
UDP and an allow list in Linux microVMs (a denied host fails, an allowed
one answers, a private address stays blocked) and checks the egress
figure on the receipt. By hand: a microVM job on the iMac reaches only
its allow list.

## 35. High availability and the remote API: reads from followers, limits that survive a failover, ACME (1.19)

Why: phase 28 left the API and the CLI reading from the leader, shared
limits in the leader's memory, and replicas that must be removed and
invited again when they move; the remote API still needs certbot, a proxy
or a tunnel for its certificate, and `?wait=` polls.

- **The API and the CLI read from followers** (status, jobs, events,
  metrics history) with the same freshness rule as the dashboard;
  submitters too, for their own jobs.
- **Shared limits and sign-in blocks replicated** (compacted, not one
  log entry per request), so a failover keeps them.
- **Replicas that move** update their address in the membership
  themselves, signed by their key.
- **`?wait=` and `logs -f` pushed** by the control plane instead of
  polled.
- **Policies per job:** an account policy can cap CPUs, memory and CU per
  job, not only per account.
- **ACME** in `pumasi ui` (`--acme DOMAIN`, HTTP-01 and TLS-ALPN-01) for
  machines that face the internet without a proxy.

Gate: CI kills the leader during a burst of failed sign-ins and the limit
holds on the new leader; a replica restarted on another address rejoins
without an invite; ACME against a local Pebble server; `?wait=` answers
within 100 ms of a job finishing. By hand: the cloud sessions keep working
through the tunnel while the owner restarts the leader.

## 36. Result checks: honeypot tasks, disk and memory challenges, re-benchmarks (1.20)

Why: the research's answer for work that is not deterministic (Docker,
microVMs, GPUs) is tasks with known answers, and it is still open: the CPU
and GPU challenges keep billed speed honest, not results, and disk and
memory figures are taken on trust. This matters once outside providers
are invited, which is why it comes last.

- **Honeypot tasks:** the control plane slips tasks with known answers
  (built-in ones per executor, and an account's own via `pumasi honeypot
  add`) into providers' queues at a rate set per trust level; a wrong
  answer holds that provider's receipts, lowers its score and raises its
  re-run rate, like a check mismatch today. Providers cannot tell them
  from real tasks by their image, size or timing.
- **Disk and memory challenges** next to the CPU one, and **periodic
  re-benchmarks** (not only at join), with a drop raising the existing
  benchmark alert.
- **A score per provider** (uptime, check and honeypot results,
  completed tasks) in `pumasi node`, the dashboard and placement, the
  in-cluster half of the hub's trust score.
- **Labels from checked figures:** the automatic `disk=nvme|ssd|hdd`
  label follows the disk challenge, so a node that claims NVMe and
  measures like an HDD loses it, and selectors can trust it.

Gate: CI runs a provider that returns wrong answers to a honeypot and
gets its receipts held, one that inflates its disk benchmark and gets
caught, and placement that prefers the higher score. By hand: an invited
provider's score in the dashboard after a day of work.

## Not planned

- Long-running services (Deployments, Services, ingress): Pumasi stays a
  batch system, as decided in the research.
- MicroVMs on macOS 10.15 (Virtualization.framework needs macOS 11);
  Catalina Macs stay WASM-only, or run Linux (phase 21's guide).
- CUDA on Intel, AMD or Apple GPUs; those use WebGPU or, for AMD on Linux,
  ROCm images.
- NVIDIA confidential computing (GPUs in confidential jobs): it needs
  H100-class GPUs that neither the owner nor CI has.
- A paid third-party security audit and the 72-hour soak in the
  research's 1.0 gate: both need the owner's budget or machines (hosted
  runners stop at 6 hours), and are done when the owner asks.
