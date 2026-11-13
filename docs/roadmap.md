# Roadmap after 1.3

Phases 1 to 17 (self-hosting 1.0) are done, and the 1.1 to 1.3 releases
fixed what the first weeks of real use found: a five-machine home cluster
of Windows PCs (one with an RTX 3080), a control-plane-only PC, a 2014
iMac on Big Sur, and Claude's cloud sessions using it through the HTTP
API. Phases 18 and 19 (the hub, Madang, and buying CU) are **on hold**
until the project owner asks for them; nothing below depends on them.

Phases 20 to 28 cover what is still missing in the scope the project set
itself: GPUs, Windows, old Intel Macs, the file store, k8s-style cluster
management, the dashboard and monitoring. Most items come from the
issues filed from that use, the "What is not done yet" list in
[architecture.md](architecture.md) and the README's known gaps; the order
puts first what the home cluster runs into every day. Each phase is one pull request (or a few), ends in a minor
release, and has a gate: what CI checks and what the owner checks by hand
on real machines.

| Phase | Release | Scope | Status |
| --- | --- | --- | --- |
| 20 | 1.4 | Issues from real use: account policies and agent options without a terminal, task timings and names | done |
| 21 | 1.5 | Machines that sleep and machines without Docker | done |
| 22 | 1.6 | Monitoring: live microVM output, every GPU and temperature, node-to-node network | done |
| 23 | 1.7 | Dashboard: change settings in the browser, sessions that survive restarts, canary rollouts | planned |
| 24 | 1.8 | File store on several machines and an S3-compatible gateway | planned |
| 25 | 1.9 | Scheduling: gangs, affinity, CPU and memory quotas, checkpoints | planned |
| 26 | 1.10 | GPUs: WebGPU timeouts and checks, AMD, shared GPUs | planned |
| 27 | 1.11 | Windows: Docker without Docker Desktop, ARM64 microVMs | planned |
| 28 | 1.12 | Operations: limits, audit log pruning, SCIM, the CLI behind an HTTPS proxy | planned |
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

## Not planned

- Long-running services (Deployments, Services, ingress): Pumasi stays a
  batch system, as decided in the research.
- MicroVMs on macOS 10.15 (Virtualization.framework needs macOS 11);
  Catalina Macs stay WASM-only, or run Linux (phase 21's guide).
- CUDA on Intel, AMD or Apple GPUs; those use WebGPU or, for AMD on Linux,
  ROCm images.
