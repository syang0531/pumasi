# Pumasi

Pool the machines you already own into one private cluster and run batch
jobs across them: sandboxed WebAssembly modules on every machine, ordinary
Docker images (Python, any packages, optional network) on the machines you
allow, or the same images inside microVMs, each task with its own kernel,
which is safe enough for machines other people lend you. One small Rust
binary, the same on Linux, macOS and Windows, x86_64 and ARM64.

> [한국어 README](README.ko.md)

Pumasi grows in phases: a private cluster tool that is useful on its own
(phase 1), then other people's machines joining by invite, with signed
usage receipts, a prepaid ledger and spot checks of their results
(phase 2), then a microVM executor that runs container images on those
machines with VM isolation (phase 3), then release binaries, OS services
and signed cluster-wide upgrades (phase 4), then Windows as a first-class
host for all three executors (phase 5), old Intel Macs and availability
policies (phase 6), then benchmarks and compute units (phase 7, this
release), and later storage, GPUs, cluster management, monitoring, a
dashboard and a marketplace. See [docs/architecture.md](docs/architecture.md)
and the roadmap below.

## Install

Every release has a `pumasi` binary for Linux (x86_64, aarch64; static),
macOS (x86_64 from 10.15, Apple Silicon) and Windows (x86_64, ARM64), a
`SHA256SUMS` file, and two install scripts that download the right binary
and check its checksum:

```sh
# Linux, macOS: installs to /usr/local/bin as root, else ~/.local/bin
curl -fsSL https://github.com/syang0531/pumasi/releases/latest/download/install.sh | sh
```

```powershell
# Windows: installs to Program Files\Pumasi (administrator) or %LOCALAPPDATA%, adds it to PATH
irm https://github.com/syang0531/pumasi/releases/latest/download/install.ps1 | iex
```

Options: `--version v0.4.0` / `-Version`, `--dir` / `-Dir`, and a join
ticket to also install the agent as a service (next section):
`curl ... | sudo sh -s -- --ticket pumasijoin... -- --allow-microvm`, or
`& ([scriptblock]::Create((irm .../install.ps1))) -Ticket pumasijoin...`.
While the repository is private, set `GITHUB_TOKEN` to a token that can
read it (the scripts then go through the GitHub API). Or build from source:
`cargo build --release -p pumasi-cli`.

## Quick start

```sh
# Machine 1: start the control plane. It prints a join command.
pumasi cluster start --name homelab

# Machines 2..N: paste the join command.
pumasi agent join pumasijoin...

# Anywhere the admin ticket is available (machine 1 by default):
pumasi status
```

Run the sample job (16 shards of a Monte Carlo estimate of pi):

```sh
rustup target add wasm32-wasip1
cargo build --release --target wasm32-wasip1 --manifest-path examples/montecarlo/Cargo.toml
pumasi run examples/montecarlo/job.toml --output
```

```
submitted job 1b6ed8ca8300baee (16 tasks)
running: 0/16 done, 6 running, 0 failed
succeeded: 16/16 done, 0 running, 0 failed
job 1b6ed8ca8300baee "estimate-pi": succeeded
  tasks 16  succeeded 16  failed 0  fuel 6000109645
  [0] ok node=a57fd372f9 attempts=1 63ms
0 3926363 5000000
...
```

Reduce the shard outputs locally with the same module:

```sh
pumasi run examples/montecarlo/job.toml -o | grep -E '^[0-9]+ ' \
  | pumasi exec examples/montecarlo/target/wasm32-wasip1/release/montecarlo.wasm reduce
# pi ~= 3.141691 from 80000000 samples in 16 shards
```

### Docker jobs: Python, files and more

Start the agents that may run containers with `--allow-docker` (Docker or a
compatible CLI must be installed there; on Windows, Docker Desktop with the
WSL 2 backend in Linux containers mode):

```sh
pumasi agent join pumasijoin... --allow-docker
```

Then run the Python word count sample. It ships a script and a folder of
text files to 4 containers and brings back each one's `/output`:

```sh
pumasi run examples/python-wordcount/job.toml --output
python3 examples/python-wordcount/merge.py pumasi-out/<job id>
```

```
submitted job 6d916b0dc63f63ea (4 tasks)
succeeded: 4/4 done, 0 running, 0 failed
job 6d916b0dc63f63ea "wordcount": succeeded
  executor docker (python:3.12-slim)
  tasks 4  succeeded 4  failed 0  output files 4
  [0] ok node=a8bce3f98f attempts=1 734ms
shard 0/4: 3 files, 2164 words
...
saved 4 output files to pumasi-out/6d916b0dc63f63ea
```

### MicroVM jobs: containers on machines you do not own

A container shares the host's kernel, so Docker jobs are for your own
machines. The microVM executor runs the same OCI images, but boots every
task in its own lightweight VM ([Firecracker](https://firecracker-microvm.github.io/)
on Linux with KVM, Apple's Virtualization.framework on macOS 11+, QEMU with
WHPX on Windows, or QEMU elsewhere) with its
own Linux kernel and no network. The job file only adds
`executor = "microvm"`:

```sh
# Once per machine (Linux with KVM, macOS 11+, Windows 10/11 x86_64; on
# Windows run it from an administrator terminal, since it installs QEMU).
pumasi vm setup
pumasi vm run python:3.12-slim -- python -c 'import platform; print(platform.release())'
# 6.1.102   <- the guest kernel, not the host's

pumasi agent join pumasijoin... --allow-microvm
pumasi run examples/python-wordcount/job-microvm.toml --output
```

`pumasi vm setup` downloads a pinned guest kernel (plus Firecracker on Linux,
QEMU on Windows) and checks their SHA-256; `pumasi vm status` shows what is
installed and which VM monitor will be used. Release binaries carry the
guest init (the VM's PID 1); a source build needs `--guest`, a static
Linux build of `pumasi-guest`
(`cargo build --release -p pumasi-guest --target x86_64-unknown-linux-musl`).
The agent pulls images itself (no Docker needed), formats the VM disks
itself (no e2fsprogs), and unpacks images inside a VM, so untrusted image
layers never touch the host's filesystem.

### Windows

Everything works on Windows 10 and 11 (x86_64): WASM jobs out of the box,
Docker jobs through Docker Desktop, microVM jobs through QEMU with the
Windows Hypervisor Platform (WHPX). From an administrator PowerShell:

```powershell
# Once: the hypervisor API QEMU uses, then reboot.
Enable-WindowsOptionalFeature -Online -FeatureName HypervisorPlatform

pumasi vm setup        # guest kernel + pinned QEMU (installed into ~\.pumasi\vm\qemu)
pumasi vm status       # "whpx usable" means hardware-accelerated microVMs
pumasi service install --ticket pumasijoin... -- --allow-microvm --allow-docker
```

- Without WHPX (feature off, or virtualization disabled in the firmware)
  microVMs still run, in QEMU's software emulation: correct but several
  times slower. `PUMASI_QEMU_ACCEL=tcg` forces that, e.g. to compare.
  The agent also boots one small probe VM at start and falls back to that
  emulation when WHPX is there but breaks the VM's disks, as it does nested
  in cloud VMs; the log says so.
- Docker jobs need Docker Desktop running with the WSL 2 backend and Linux
  containers. Docker Desktop starts with the signed-in user, so a machine
  nobody is logged into takes only WASM and microVM jobs (or set Docker
  Desktop to start at sign-in and enable automatic sign-in).
- `pumasi service install` adds a firewall rule letting the agent receive
  UDP on private and domain networks, so peers connect directly instead of
  through the relay; `service uninstall` removes it. On a network Windows
  calls Public, connections go through the relay unless you widen the rule:
  `netsh advfirewall firewall set rule name="Pumasi agent (UDP in)" new profile=any`.
- The VMs and Docker CLIs an agent starts are kept in Windows Job Objects,
  so they end with the agent even if it crashes.

### Macs, old Intel ones too

Any Mac from macOS 10.15 runs the agent, including the 2013-2014 Intel
models whose last macOS is 10.15, 11 or 12:

| Mac | Last macOS | WASM | microVM | Docker |
| --- | --- | --- | --- | --- |
| Mac mini (Late 2014), Mac Pro (Late 2013) | 12 Monterey | yes | yes | no |
| MacBook Air (Mid 2013, Early 2014), MacBook Pro (Late 2013, Mid 2014), iMac (Mid 2014, 5K Late 2014) | 11 Big Sur | yes | yes | no |
| iMac (Late 2013), MacBook Pro (Early 2013) | 10.15 Catalina | yes | no | no |
| Apple Silicon | current | yes | yes | with Docker Desktop |

From macOS 11, microVMs run on Apple's Virtualization.framework, built
into macOS: nothing to install, and `pumasi vm setup` only downloads the
guest kernel and signs a helper copy of the binary (the framework only
serves signed programs). Docker Desktop and Homebrew's QEMU support only
recent macOS releases, so old Macs take WASM and microVM jobs.

```sh
curl -fsSL https://github.com/syang0531/pumasi/releases/latest/download/install.sh | sudo sh
pumasi vm setup        # macOS 11+: guest kernel + signed VM helper
pumasi vm status       # "vz usable" means microVMs work
sudo pumasi service install --ticket pumasijoin... -- --allow-microvm --only-on-ac --idle-minutes 10
```

### When a machine takes work

Machines people use can lend themselves only when nobody notices. Agent
options (also after `--` in `pumasi service install`):

| Option | Effect |
| --- | --- |
| `--only-on-ac` | No new tasks while on battery |
| `--idle-minutes 10` | No new tasks until keyboard and mouse have been idle 10 minutes |
| `--hours 22:00-07:00` | No new tasks outside these local times (several: `00:00-08:00,12:00-13:00`) |
| `--max-cpu-percent 50` | Offer half the CPUs: image jobs that need more go elsewhere, slots follow |
| `--slots N` | Tasks at once (default: one per CPU offered, but at most one per GiB of memory beyond the first two) |

A paused machine gets no new tasks and finishes the ones it is running;
`pumasi status` shows it as `paused` with the reason (e.g. "on battery
power"). Idle time comes from the whole machine on macOS (so a
LaunchDaemon sees it) and from terminals and desktop sessions on Linux; on
Windows the agent must run in the user's session to see input (as a
service it reports that it cannot tell, and stays paused).

### Other people's machines: invites, compute units and checks

A friend can lend their PC to your cluster. Invite it for their account:

```sh
pumasi invite bob                  # prints `pumasi agent join pumasijoin...` for bob to run
pumasi cu add owner 100            # 100 CU for runs on other people's machines
pumasi run examples/montecarlo/job.toml
```

- Bob's machine joins as a **provider**: it runs WASM tasks, and microVM
  tasks if bob started his agent with `--allow-microvm` (Docker only if you
  invited it with `--allow-docker` *and* bob started his agent with
  `--allow-docker`), and only for jobs whose account has CU.
- Your own machines are used first and cost nothing; work overflows to
  providers. Each provider run is priced in **compute units** (below) and
  charged to the job's account. The provider's earning is **held**
  (default 1 hour), then becomes available.
- Every run produces a **usage receipt** (fuel, wall time, memory,
  executor, a hash of the result, the CPU score and CU it was billed at)
  signed by the node and countersigned by the control plane. `pumasi
  receipts <JOB> --export r.json` exports them; `pumasi verify-receipts
  r.json` checks the signatures offline.
- WASM jobs run in **deterministic mode** by default (virtual clock, seeded
  random numbers), so every honest node computes the same result. A sample
  of provider-run tasks (10%; 50% for new providers; all of them after a
  failed check) is **re-run on another node**, preferably one of yours. A
  provider whose result disagrees loses that earning (you are refunded), gets
  a strike, and after 3 strikes gets no more tasks. If a check run's result
  wins, it replaces the provider's.

```
$ pumasi job 7c1e04b2d9a8f310
job 7c1e04b2d9a8f310 "render-frames": succeeded
  executor microvm (python:3.12-slim)
  account owner  cost 0.81 CU
  tasks 8  succeeded 8  failed 0
  [0] ok node=a8020286f9 attempts=1 270114ms
  [1] ok node=46e7353324 attempts=1 268420ms cost=0.135
...
$ pumasi cu list
ACCOUNT            AVAILABLE CU           HELD          SPENT         EARNED
bob                        0.81              0              0           0.81
owner                     99.19              0           0.81              0
```

CU are bookkeeping only: the engine knows no money. `pumasi cu payout bob
10` records that 10 CU left the cluster's books (you settled with bob
outside Pumasi).

### Compute units and benchmarks

**1 CU is one reference vCPU for one hour.** The reference vCPU has CPU
score 1000 (one hardware thread of a 2.5 GHz Skylake-class server core).
Other resources have their own rates, which `pumasi prices set` changes:

| Resource | Default rate |
| --- | --- |
| CPU (Docker, microVM) | 1 CU per vCPU-hour at score 1000 |
| WASM | 1 CU per hour of reference time its fuel stands for |
| Memory | 0.25 CU per GiB-hour reserved |
| Storage (cluster file store) | 0.5 CU per GiB-month |
| Egress | 0.1 CU per GiB |

So 1 vCPU with 2 GiB for an hour is 1.5 CU, with 4 GiB 2 CU. Ledger
amounts are whole milli-CU; what a run costs below a milli-CU carries over
to the next run between the same two accounts, so many short runs add up
exactly.

Every agent **benchmarks** its machine when it starts (if its saved
profile is stale) and again every day while idle (`--bench-interval`,
`--no-benchmark`), and sends the result signed with its node key:

- **CPU**: a fixed integer kernel in the same WASM sandbox tasks run in,
  on one thread and on every offered CPU at once, as a score.
- **Memory**: copy bandwidth.
- **Disk** (under the agent's directory), fio-style with the page cache
  bypassed: sequential read and write, 4 KiB random reads one at a time
  (latency) and eight at a time (IOPS). Seek latency and speed tell a
  spinning disk (`hdd`), a SATA SSD (`ssd`) and `nvme` apart.
- **SMART** health, wear (share of rated endurance used), power-on hours
  and media errors, when `smartctl` is installed.
- **Latency**: the control plane measures the round trip on each node's
  connection itself (direct or through a relay).

CPU time is billed for **wall time x score / 1000**, so a job costs about
the same on a slow machine that takes longer as on a fast one, and WASM by
fuel, which is the same everywhere. A provider cannot simply claim a
faster CPU: until the control plane has timed it, it is billed at no more
than 1000; then the control plane sends it a fresh seed for the benchmark
kernel when it is idle, times the answer and recomputes it. The provider
is billed at no more than 1.25 times what the cluster timed, and a claim
over twice that (or a wrong answer) is a strike. Claiming shorter runs to
look fast does not pay either: the bill is time x score.

```
$ pumasi bench
cpu      Intel(R) Core(TM) i5-4250U CPU @ 1.30GHz (4 threads)
         score 811 single-thread, 2035 all threads, 508 per vCPU (reference vCPU = 1000)
memory   4.0 GiB, copy 9120 MB/s
disk     ssd (the OS says ssd)
         sequential 512 MB/s read, 431 MB/s write; 4 KiB random read 96 us median, 180 us p99, 31150 IOPS (8 in flight)
smart    APPLE SSD SM0128F, health ok, 7% of endurance used, 21530 h powered on, 0 media errors
```

`pumasi status` shows each node's billed score, disk class and latency;
`pumasi node <ID>` the whole profile and what the cluster's timing found,
and `pumasi node <ID> --rebenchmark` asks it to measure again.

**Mutual mode (품앗이).** For a group pooling machines without money:
`pumasi prices set --ledger mutual --credit-limit 20`. Nobody grants CU;
lending your machine earns them, using others' machines spends them, and
an account may go down to minus the credit limit. The balances always add
up to zero.

### Run the agent as a service

`pumasi agent join` runs in the foreground. To keep a machine in the
cluster across reboots, install the agent as a service instead:

```sh
sudo pumasi service install --ticket pumasijoin... -- --allow-microvm
pumasi service status
```

| OS | As root / administrator | Otherwise |
| --- | --- | --- |
| Linux | systemd unit `/etc/systemd/system/pumasi-agent.service`, starts at boot | `systemctl --user` unit; lingering is enabled so it also starts at boot |
| macOS | LaunchDaemon `/Library/LaunchDaemons/pumasi.agent.plist`, starts at boot | LaunchAgent, starts at login |
| Windows | Windows service `pumasi-agent` (automatic start, restarts on failure) | needs an administrator shell |

`--ticket` joins on the first start; without it the machine must have
joined before. Options after `--` are passed to `pumasi agent start`.
Logs go to the journal on Linux and to `~/.pumasi/logs/agent.log` on macOS
and Windows. `pumasi service uninstall` removes the service; the machine
stays a member (`pumasi remove-node` revokes it).

### Upgrading every agent at once

Download the new release's binaries, then roll them out from wherever the
admin ticket is:

```sh
pumasi upgrade rollout pumasi-linux-x86_64 pumasi-macos-aarch64 pumasi-windows-x86_64.exe --wait
pumasi upgrade status
```

```
PLATFORM         VERSION              SHA256
linux/x86_64     0.4.0                3f94db2f1a61...

NODE         NAME             PLATFORM         VERSION              BINARY         STATE
1634691880   desktop          linux/x86_64     0.4.0                3f94db2f1a61   current
8a1c0e55d2   bobs-pc          linux/x86_64     0.3.0                a82335740e93   opted out
```

The platform and version are read from each file. The control plane signs
each binary with the cluster key; an agent installs only a binary signed by
the cluster it joined, for its own platform, with the signed size and
SHA-256, and only after the new binary starts (`--version`). Your machines
accept upgrades by default (`--no-auto-upgrade` opts out); someone else's
machine only if its owner starts the agent with `--auto-upgrade`. Idle nodes
upgrade right away, busy ones stop taking new tasks one at a time. The agent
restarts in place; if the cluster rejects the new binary, it cannot reach
the cluster for 5 minutes, or it keeps crashing, the agent puts the
previous binary back and reports the failure in `pumasi upgrade status`.
Upgrade the control plane first (stop it, replace the binary, start it):
a binary with a newer protocol needs `--force` and its agents would roll
back.

## Commands

| Command | What it does |
| --- | --- |
| `pumasi cluster start [--name N] [--net public\|local] [--bind ADDR]` | Run the control plane. Creates the cluster on first run, keeps state in `~/.pumasi/cluster`. |
| `pumasi token [--ttl 24h] [--uses N]` | Create a join ticket for another machine of yours. |
| `pumasi invite <ACCOUNT> [--ttl 7d] [--uses 1] [--allow-docker]` | Invite someone else's machine as a provider earning for `ACCOUNT`. |
| `pumasi agent join <TICKET> [--name N] [--slots N] [--allow-docker] [--allow-microvm]` | Join a cluster and start running tasks. `--allow-docker` also accepts docker jobs, `--allow-microvm` microvm jobs (`--vm-backend firecracker\|qemu`, `--vm-dir DIR`). |
| `pumasi agent start [--allow-docker] [--allow-microvm] [--auto-upgrade\|--no-auto-upgrade]` | Reconnect to the cluster this machine joined before (no ticket needed). |
| `pumasi service install [--ticket T] [--user\|--system] [--dry-run] [-- AGENT OPTIONS]` | Run the agent as a service that starts at boot (systemd, launchd, Windows service). `status` and `uninstall` too. |
| `pumasi upgrade rollout FILE... [--wait] [--force]` | Roll new agent binaries out to the nodes, signed with the cluster key. `upgrade status` shows each node, `upgrade cancel` stops offering it. |
| `pumasi vm setup [--guest FILE] [--force]` | Install the guest kernel, Firecracker (Linux) or QEMU (Windows, as administrator) and the guest init into `~/.pumasi/vm`. |
| `pumasi vm status` | Show what is installed for microVMs and which VM monitor will be used. |
| `pumasi vm run IMAGE [--input DIR] [-e K=V] [--memory-mb N] [--cpus N] [--timeout T] [-- CMD...]` | Run an image in a microVM on this machine, the way an agent would (`/output` is saved to `pumasi-out/vm`). |
| `pumasi run job.toml [--account A] [--output] [--out DIR] [--detach]` | Submit a job and wait for it. Output files are saved to `DIR` (default `pumasi-out/<job id>`). Exit code 0 on success, 2 if any task failed. `--account` picks who pays for provider runs (default `owner`). |
| `pumasi status` | Nodes (role, online, executors, slots, running, checks, billed CPU score, disk class, latency) and recent jobs with their cost in CU. |
| `pumasi node <ID> [--rebenchmark]` | One node's benchmark profile, billed score, latency and checks; `--rebenchmark` asks it to measure again. |
| `pumasi bench [--quick] [--json] [--dir DIR]` | Benchmark this machine the way the agent does and print the profile. |
| `pumasi job <ID> [--output] [--out DIR]` | Per-task state, node, attempts, time, cost and check result; stdout/stderr with `--output`; downloads output files with `--out`. |
| `pumasi cu list \| add <ACCOUNT> <CU> \| payout <ACCOUNT> <CU> \| ledger [--account A]` | Balances in CU (available, held, spent, earned), grants, payouts, and the ledger (`pumasi credit` still works). |
| `pumasi prices [set --memory-gib-hour X --storage-gib-month X --egress-gib X --ledger prepaid\|mutual --credit-limit CU --hold 1h --verify-rate 10% --max-strikes 3]` | Show or change the CU rates, the ledger mode and the settlement policy. |
| `pumasi receipts <ID> [--export FILE]` | A job's signed usage receipts. |
| `pumasi verify-receipts FILE [--cluster ID]` | Check exported receipts offline. |
| `pumasi cancel <ID>` | Cancel queued and running tasks of a job. |
| `pumasi remove-node <ID>` | Revoke a node. It needs a new ticket to come back. |
| `pumasi login <ADMIN_TICKET>` | Manage a remote cluster from this machine. |
| `pumasi exec module.wasm [--input DIR] [--seed S] [ARGS...]` | Run a module locally in the agent's sandbox (stdin is passed through, `DIR` appears at `/input`, `/output` is saved to `pumasi-out/exec`, `--seed` for deterministic mode). |

`--home DIR` or `PUMASI_HOME` changes the state directory, which lets you run
a cluster and several agents on one machine for testing. `PUMASI_LOG=debug`
turns on verbose logs.

## Job file

A job runs on one of three executors with the same file format and commands:

| | `wasm` (default) | `docker` | `microvm` |
| --- | --- | --- | --- |
| Runs | a `wasm32-wasip1` module | any Linux OCI image | any public Linux OCI image |
| Where | every node | only nodes started with `--allow-docker` (providers also need an invite with `--allow-docker`) | nodes started with `--allow-microvm`, providers included |
| Isolation | Wasmtime sandbox, safe for untrusted code | container, only for machines and people you trust | a VM with its own kernel per task |
| Network | never | off by default, `network = true` to allow | never |
| GPUs | no | `gpus = "all"` | not yet |
| Files | `/input`, `/output` | `/input`, `/output` | `/input`, `/output` |

WASM job:

```toml
name = "estimate-pi"                  # optional, defaults to the module name
module = "montecarlo.wasm"            # a wasm32-wasip1 command module
shards = 16                           # N tasks; {shard} and {shards} are substituted
args = ["sample", "{shard}", "5000000"]
env = { MODE = "fast" }               # optional
stdin = "input.txt"                   # optional, fed to every task
files = ["data/", "lookup.csv"]       # optional, appear under /input
max_attempts = 3                      # placements before giving up on lost nodes
deterministic = true                  # default: virtual clock + seeded random, so results can be checked

[resources]
memory_mb = 32                        # memory cap per task (default 256)
timeout = "2m"                        # wall-clock cap per task (default 5m)
fuel = 10_000_000_000                 # instruction budget per task (wasm only, default unlimited)
```

Docker job:

```toml
name = "wordcount"
image = "python:3.12-slim"            # setting `image` selects the docker executor
command = ["python", "/input/count.py", "{shard}", "{shards}"]
files = ["count.py", "texts/"]        # copied into /input of every task
shards = 4
network = false                       # default; true allows outbound network
# gpus = "all"                        # passed to `docker run --gpus`

[resources]
memory_mb = 512
cpus = 1.5                            # CPU cap per task (docker only)
timeout = "10m"
```

MicroVM job (the same image and command, in a VM):

```toml
name = "wordcount-vm"
executor = "microvm"                  # required: `image` alone means docker
image = "python:3.12-slim"
command = ["python", "/input/count.py", "{shard}", "{shards}"]
files = ["count.py", "texts/"]
shards = 4

[resources]
memory_mb = 256                       # the VM gets this plus ~96 MiB for its kernel
cpus = 1                              # rounded up to whole vCPUs
timeout = "2m"                        # covers boot and run, not the first image pull
```

Paths in `files` are relative to the job file; directories are copied
recursively and keep their relative path under `/input`. All input files of a
job may total up to 48 MiB. Whatever a task writes to `/output` (up to
32 MiB) is stored by the control plane and downloaded by `pumasi run` into
`pumasi-out/<job id>/task-<N>/`. stdout and stderr are captured up to 4 MiB
each.

Containers run with `--network none` unless `network = true`, with memory,
CPU and process caps, `no-new-privileges`, and no host directories mounted
(files are copied in and out). Images are pulled on first use.

A microVM task runs the image's entrypoint and command (or the job's
`command`) as the image's user, with `/input` as the working directory,
like `docker run`. Its root filesystem is the image, read-only and shared by
all tasks, under a fresh writable layer that is thrown away afterwards. Each
task boots in well under a second with KVM.

Instead of `shards`, list tasks explicitly:

```toml
module = "convert.wasm"

[[task]]
args = ["a.png"]
stdin = "a.png"

[[task]]
args = ["b.png"]
stdin = "b.png"
```

Any language that targets `wasm32-wasip1` works for WASM jobs (Rust, C/C++,
Zig, Go, TinyGo, ...). A per-task `files = [...]` in a `[[task]]` adds files
only that task sees.

In deterministic mode (the default for WASM jobs) the clocks start at
2000-01-01 and advance 1 ms per read, and random numbers come from a seed
derived from the job and task index. A task that needs real time or fresh
randomness sets `deterministic = false`; its provider runs are then never
checked.

## How it works

```
             admin ticket                     join ticket
  pumasi CLI ───────────────►  control plane  ◄─────────── pumasi agent (laptop)
                               (scheduler,    ◄─────────── pumasi agent (desktop)
                                redb state)   ◄─────────── pumasi agent (Pi)
                  all links: iroh QUIC, dialed by public key, NAT traversal + relay fallback
```

- **Control plane** (`crates/cluster`): membership, join tokens, a FIFO task
  queue and a scheduler that places tasks on the node with the most free
  slots among those that offer the job's executor and enough memory. State
  (members, jobs, task results and output files, modules and input bundles)
  is persisted in redb, so a restart resumes queued and interrupted work.
- **Agent** (`crates/agent`): connects out to the control plane, so it works
  from behind NAT. It runs WASM tasks in Wasmtime (memory, fuel and
  wall-clock caps, only `/input` and `/output` visible) and, when allowed,
  docker tasks through the Docker CLI and microvm tasks in Firecracker or
  QEMU VMs whose init is `pumasi-guest` (`crates/guest`). It reports stdout, stderr, output
  files, exit status, fuel used and wall time. Modules and shared input files
  are sent to each node once per job, not once per task.
- **Protocol** (`crates/proto`): postcard frames over iroh QUIC streams.
  Every node is identified by its ed25519 key; nobody can impersonate it.
- **Failure handling**: an agent that disconnects or misses heartbeats loses
  its tasks to other nodes (up to `max_attempts`). Agents reconnect with
  backoff. Task-level failures (trap, non-zero exit, out of fuel or memory,
  timeout) are reported, not retried.
- **Receipts and settlement**: the agent signs each run's usage with its
  node key; the control plane checks it against the result it received,
  caps the wall time at what it observed itself, prices it in CU at the
  node's billed CPU score, countersigns it and stores it next to the ledger
  entries it caused, in one transaction.
- **Benchmarks**: agents send a performance profile signed with their node
  key; the control plane times providers' CPU benchmark itself with a
  seeded challenge before trusting a score above the reference.

## Security model

- Join tickets are bearer secrets with an expiry and optional use count.
  Once a node joins, its public key is remembered; the ticket is not needed
  again. `remove-node` revokes it.
- The admin ticket in `~/.pumasi/cluster/admin.ticket` grants full control.
  Treat it like a password.
- WASM tasks are sandboxed from the machine running them (only their own
  `/input` and `/output`, no network, memory and CPU caps).
- Docker tasks are only as isolated as a container: they share the host
  kernel. That is why an agent runs them only after its owner passes
  `--allow-docker`. Use it on your own machines and in clusters of people
  you trust.
- MicroVM tasks protect the provider's machine from the job: each task gets
  its own kernel behind KVM (Firecracker, which also confines itself with
  seccomp), no network device, only its own disks, and memory and vCPU caps.
  Image layers are unpacked inside a VM, never on the host. The provider
  still sees everything the task does (open tier).
- Provider machines (other people's, admitted with `pumasi invite`) get WASM
  and microVM tasks, Docker only if the invite allowed it, and never settle a
  check: only your own machines are trusted to arbitrate. MicroVM results are
  not deterministic, so they are not spot-checked.
- A provider can return made-up results; signatures prove who claimed what,
  not that it is true. Checks catch it statistically: with the default 10%
  rate, a provider faking 20 results is caught with 88% probability, loses
  every held earning that a check rejects, and is suspended after 3 strikes.
  Earnings are held for an hour so a later check can still claw them back.
- Tasks are **not** hidden from the machine's owner: only the `open`
  confidentiality tier exists. Do not send secrets to machines you do not
  trust.
- Agent upgrades are signed by the control plane's key, which agents learned
  when they joined, and checked against the signed SHA-256 before they run.
  Whoever holds the admin ticket can roll out a binary to your machines (and
  to providers that opted in), so the admin ticket is as sensitive as root
  on them. Release binaries come with `SHA256SUMS`, and with GitHub build
  provenance attestations once the repository is public
  (`gh attestation verify pumasi-linux-x86_64 -R syang0531/pumasi`).

## Roadmap

Phases 1 to 15 are enough to run Pumasi on your own machines, alone or as
an organization (phase 13 covers the essentials). Phase 16 finishes what an
organization needs to adopt it, and phases 17 and 18 are only for trading
with other clusters.

| Phase | Scope | Status |
| --- | --- | --- |
| 1 | Private cluster: agent, control plane, iroh networking, WASM and Docker executors, file input/output, CLI | done |
| 2 | Invited outside providers (open tier), usage receipts signed by node and cluster, fixed price list, prepaid credit ledger, sampled re-runs to check results | done |
| 3 | microVM executor: same OCI images as docker jobs in Firecracker or QEMU VMs, behind the same job format, open to providers | done |
| 4 | Distribution and install: release binaries for six platforms with SHA256SUMS, install scripts, OS services, signed cluster-driven upgrades with rollback | done |
| 5 | Windows: Docker Desktop (WSL 2), microVMs with QEMU + WHPX (TCG fallback), pinned QEMU install, Job Objects, built-in ext4 formatter and guest init | done |
| 6 | Old Intel Macs (macOS 10.15+), Virtualization.framework microVMs, availability policy (on AC power, idle, hours, CPU cap, memory-aware slots) | done |
| 7 | Benchmarks and compute units (CU): signed node performance profiles (CPU, memory bandwidth, disk class and speed with fio, SMART wear, latency), 1 CU = one reference vCPU for one hour with per-resource CU rates, ledger in integer milli-CU, group-local ledger for money-free pooling | this release |
| 8 | Cluster file store (iroh-blobs): datasets, large inputs and outputs, resumable transfers, storage billed in CU, private registry images | planned |
| 9 | GPUs and microVM devices: detection, GPU benchmarks in CU, scheduling, Docker GPU jobs, VFIO passthrough, microVM networking | planned |
| 10 | Cluster management: labels and selectors, drain, priorities, quotas, role tickets, `apply` | planned |
| 11 | Monitoring and alerts: node metrics, Prometheus endpoint, live logs, webhooks | planned |
| 12 | Web dashboard (built into the binary) | planned |
| 13 | Control plane high availability (Raft) | planned |
| 14 | WebGPU executor (experimental) | planned |
| 15 | Confidential tier on SEV-SNP / TDX servers with remote attestation | planned |
| 16 | Self-hosting 1.0: SSO (OIDC and LDAP, groups mapped to role tickets), tamper-evident audit log (hash chain, JSON and syslog export), threat model, cargo-audit, cargo-deny and fuzzing in CI, an external security review, operations guide, 72-hour soak test, then the 1.0 tag | planned |
| 17 | Hub, called Madang (마당, "the yard"; separate private repository `madang`): provider discovery, latency probes, trust scores, cross-group CU ledger with a hub fee | planned |
| 18 | Buying CU with won through the hub (Toss Payments); no cash-out, no transfers between users | planned |

Known gaps: single control plane (no Raft yet), no web console, accounts
exist only inside one cluster and only the admin can submit (with
`--account`), CU are not connected to real payments, GPUs have no CU rate
until phase 9 and storage and egress are not metered until phase 8,
node-to-node latency and bandwidth are not measured yet (only each node's
round trip to the control plane), input and output
files are size-capped (48 MiB in, 32 MiB out per task) because they travel
through the control plane. MicroVMs pull only public images, have no
network and no GPUs yet, and need KVM (Linux), Virtualization.framework
(macOS 11+) or WHPX (Windows x86_64) for speed (QEMU without them works but
is slow, and Windows on ARM has no microVMs yet). WHPX and
Virtualization.framework guests are checked on real machines only: CI
runners have WHPX only nested, where it breaks QEMU's disks and the agent
falls back to emulation, and hosted Mac runners cannot virtualize at all.
macOS 10.15 and 11 themselves have no CI runners either.

## Development

```sh
cargo test --workspace     # unit tests + end-to-end tests over real iroh connections
cargo clippy --workspace --all-targets -- -D warnings
```

`scripts/ci/service-upgrade.sh OLD NEW WORKDIR` (CI runs it on Linux,
macOS and Windows) installs the `OLD` binary as a real service with a join
ticket, restarts the service, rolls `NEW` out with `pumasi upgrade rollout
--wait` and checks that the node runs it; `--foreground` does the same
without a service manager. Build two different binaries with
`PUMASI_BUILD_ID`: `PUMASI_BUILD_ID=new cargo build -p pumasi-cli`.

The end-to-end tests in `crates/cluster/tests/e2e.rs` start a control plane
and agents on localhost and cover sharded jobs, node failure and rescheduling,
ticket rejection and reuse, reconnect without a ticket, cancellation, fuel
limits, bad admin credentials, restarting the control plane, docker jobs
with input/output files (skipped when no Linux Docker daemon is available),
an invited provider paid in CU with verifiable receipts, a dishonest
provider caught by check runs, refunded and suspended, signed benchmark
profiles and a provider with an inflated CPU score caught by the
cluster's timing, mutual-mode lending and borrowing that adds up to zero, microVM
jobs on an invited provider, and an agent upgrade rollout (opt-in, forged
signature and tampered binary refused). The microVM test runs when `PUMASI_TEST_VM_DIR`
points at a directory prepared with `pumasi vm setup` (CI does this on Linux
with KVM, for Firecracker and for QEMU, and on Windows with QEMU in software
emulation, using a binary built with the guest init embedded through
`PUMASI_EMBED_GUEST=<absolute path>`):

```sh
cargo build --release -p pumasi-guest --target x86_64-unknown-linux-musl
cargo run -p pumasi-cli -- vm setup --vm-dir /tmp/vm --guest target/x86_64-unknown-linux-musl/release/pumasi-guest
PUMASI_TEST_VM_DIR=/tmp/vm cargo test -p pumasi-cluster --test e2e microvm
```

## License

Apache-2.0.
