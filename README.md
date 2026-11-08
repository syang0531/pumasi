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

Every release has a `pumasi` binary for Linux (x86_64, aarch64; glibc 2.28
or later, so any distribution from 2018 on; on Alpine build from source with
`--target x86_64-unknown-linux-musl`, which has no GPUs),
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
own Linux kernel and no network unless the job asks for one. The job file
only adds `executor = "microvm"`:

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

With `network = true` a microVM gets a network card that reaches **public
addresses only**. Its other end is a gateway inside the agent, not a host
network device: the agent opens each connection itself after checking the
destination, and never connects to private ranges (10/8, 172.16/12,
192.168/16, 100.64/10), loopback, link-local (cloud metadata), multicast,
reserved ranges, any address of the host itself, or port 25. DNS goes to the
host's resolver; other UDP, ICMP and IPv6 are not forwarded. Network jobs
need QEMU on the node (Firecracker nodes use it when it is installed;
`pumasi vm status` says whether they can), and only go to nodes that can.

### GPU jobs

Ask for GPUs in the job file and the scheduler places each task on a node
with that many free GPUs, hands the task exactly those devices, and bills
their time on the receipt:

```toml
image = "pytorch/pytorch:2.5.1-cuda12.4-cudnn9-runtime"
command = ["python", "/input/train.py", "{shard}", "{shards}"]
files = ["train.py"]
shards = 4
gpus = 1                              # per task; "all" for every GPU of the node
gpu = { vendor = "nvidia", min_vram_gb = 8 }   # optional; implies gpus = 1
```

```sh
pumasi gpu list                       # this machine's GPUs, benchmarked
pumasi gpu check                      # what jobs could do with them, and how to fix what is missing
pumasi run examples/pytorch-cuda/job.toml --output
```

- **NVIDIA**: found through NVML (the driver's own library, loaded when
  present) and benchmarked with the CUDA driver (FP32 throughput, memory
  bandwidth, and a hash rate for the cluster's GPU check). Docker jobs get
  them with `--gpus device=...`, which needs the NVIDIA Container Toolkit on
  Linux; Docker Desktop on Windows has it built in (WSL 2).
- **AMD** (Linux): found in sysfs; FP32 is estimated from the compute
  units and clock. Docker jobs get `/dev/kfd` and the GPU's render node
  (ROCm images).
- **Intel and Apple GPUs**, and any GPU without CUDA, are used by wasm jobs
  through WebGPU (below).
- **MicroVMs (experimental)**: a GPU bound to `vfio-pci` on Linux, with a
  clean IOMMU group and no display attached, can be passed through to a
  microVM, which boots a GPU guest kernel and loads its own NVIDIA driver
  (`scripts/kernel/build-gpu.sh` builds the bundle, `pumasi vm setup
  --gpu-bundle DIR` installs it, `pumasi gpu check` prints the binding
  commands). This lets providers lend GPUs without sharing their kernel.

GPU time costs `gpu-tflops-hour` (default 0.1 CU per FP32 TFLOPS per hour)
plus `gpu-vram-gib-hour` (0.3 CU per GiB of GPU memory per hour) on top of
CPU and memory, so a 12 GiB, 12.7 TFLOPS card costs about 4.9 CU an hour.
Your own machines are billed at what they measured. A provider's GPU is
timed by the control plane first, with a challenge only that GPU can answer
quickly; a wrong answer is a strike and bills nothing, a GPU slower than it
claims is billed at what was timed, and one that cannot take the challenge
(no CUDA) is billed at 10 TFLOPS at most. Checks are repeated at the result
check rate. GPU results themselves are not deterministic, so they are not
re-run. `pumasi node <id>` shows each GPU and what it is billed at;
`--no-gpus` keeps an agent's GPUs out of the cluster.

### WebGPU: any GPU from a wasm job (experimental)

A wasm job that asks for `gpus = 1` gets one GPU through WebGPU: the agent
opens it with [wgpu](https://wgpu.rs) on whatever the machine has (Vulkan
on Linux, Metal on macOS, DX12 on Windows, OpenGL as a fallback), and the
module runs WGSL compute shaders on it through a small host API, the
`pumasi_webgpu` imports. It is the only way to use GPUs without CUDA:
Intel and AMD integrated GPUs, the GPUs of old Intel Macs, Apple Silicon.
The sandbox stays the same: no network, no files outside `/input` and
`/output`, and the job's memory limit and timeout cover the GPU too.

```sh
pumasi gpu webgpu                     # the adapters this machine offers, benchmarked
cargo build --release --target wasm32-wasip1 --manifest-path examples/webgpu/Cargo.toml
pumasi exec --gpu --webgpu-software examples/webgpu/target/wasm32-wasip1/release/matmul.wasm 256
# adapter: llvmpipe (LLVM 20.1.2, 256 bits) (vulkan)     <- a software rasterizer; real GPUs are far faster
# matmul 256x256: gpu 26.2 ms (1.3 GFLOPS; 172.7 ms with uploads and compile), cpu 47.9 ms (0.70 GFLOPS), speedup 1.8x
# checksum gpu 4.178840e6 cpu 4.178840e6; max relative error 0.00e0 (tolerance 1e-3): ok
pumasi agent start --allow-webgpu     # offer this machine's adapters to the cluster
pumasi run examples/webgpu/matmul.toml --output
```

```toml
module = "matmul.wasm"
gpus = 1                              # one WebGPU adapter per task
gpu = { vendor = "intel" }            # optional: nvidia, amd, intel, apple, any
```

The two examples in `examples/webgpu` (a tiled matrix multiplication and
a Gaussian blur of a PPM image) check the GPU's answer against the same
computation on the CPU and print both speeds; `src/lib.rs` there is a
small Rust wrapper of the host API to copy. The calls are:

| Import (`pumasi_webgpu`) | Does |
| --- | --- |
| `adapter(ptr, cap) -> len` | the task's GPU as JSON (name, backend, limits); -1 without one |
| `shader(src, len, entry, entry_len) -> handle` | compiles a WGSL compute entry point (automatic layout) |
| `buffer(size: i64) -> handle` | a zero-filled storage buffer (size a multiple of 4) |
| `write(buf, offset: i64, src, len)` / `read(buf, offset: i64, dst, len)` | copy memory to and from a buffer; `read` waits for earlier dispatches |
| `dispatch(shader, bufs, n, x, y, z)` | runs x*y*z workgroups with `n` buffers bound at `@group(0) @binding(0..n)` |
| `wait()` / `release(handle)` | wait for every dispatch; free a shader or buffer |
| `error(ptr, cap) -> len` | why the last call failed (WGSL compile errors included) |

Every call returns a negative number when it fails. They map one to one
onto WebGPU, so modules can move to the component model's `wasi:webgpu`
(wasi-gfx) once Wasmtime runs it. Agents offer WebGPU only with
`--allow-webgpu` (providers too: WebGPU validates every shader and bounds
checks every access, as browsers do, but it still reaches the GPU driver).
An adapter that is one of the node's listed GPUs shares its slot, so a
Docker job and a WebGPU job never use the same GPU at once. Its time is
billed like any GPU's, at the FP32 rate the agent measured with a WGSL
shader; there is no GPU challenge over WebGPU yet, so a provider's
adapter is billed at 1 TFLOPS at most unless it is a CUDA GPU the cluster
already timed. GPU results differ slightly from GPU to GPU, so these jobs
are not deterministic and are never re-run to check them. CI runs both
examples on Linux (lavapipe, a software Vulkan driver), Windows and macOS
and compares the results across them; real GPUs are checked by hand.

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

### The cluster file store

The control plane keeps a content-addressed file store (iroh-blobs, BLAKE3)
in `~/.pumasi/cluster/blobs`. Datasets are named and versioned; a version
that repeats files of another stores them once.

```sh
pumasi store put corpus texts/ extra.csv --account alice   # corpus@1; only missing data is sent
pumasi store ls                                            # latest version of each dataset
pumasi store get corpus@1 -o corpus/                       # resumes after an interruption
pumasi store rm corpus@1                                   # space comes back once no job uses it
pumasi store usage                                         # bytes per account, quotas, disk use
pumasi store quota alice 100G
```

A job reads datasets with `store:` entries in `files` and can write its
outputs to a dataset:

```toml
name = "tokenize"
module = "tokenize.wasm"
shards = 16
files = ["store:corpus/part-{shard:02}.txt", "store:vocab@2"]  # under /input/corpus/, /input/vocab/
args = ["/input/corpus/part-{shard:02}.txt"]
output = "store:tokens"            # every task's /output becomes dataset tokens@N
```

`store:NAME[@VERSION][/PATH]` mounts a dataset (or one file or directory of
it) at `/input/NAME/`; `{shard}` in it gives each task its own part, so a
large dataset is read in pieces by many nodes. Nodes fetch what their tasks
mount into a local cache (`--store-cache`, 20 GiB by default, least
recently used first), from other nodes that already have it when they can
and from the control plane otherwise, and pick up an interrupted download
where it stopped. Every piece is checked against its BLAKE3 hash.

Inputs and outputs no longer have to fit in a message: `pumasi run` sends
local `files` over 16 MiB through the store, and a task's `/output` over
32 MiB goes to the store instead of the report (all of it does with
`output = "store:..."`). When the job ends its stored outputs become dataset
`job-<id>` (or the named one), and `pumasi run` and `pumasi job --out`
download them; `pumasi store get --job <id>` does too.

Storage is billed in CU per GiB-month to the account that owns the data, at
the rate of the control plane's disk class (`cluster start --storage-class
hdd|ssd|nvme`; `prices set --storage-hdd-gib-month`,
`--storage-gib-month` for SSD, `--storage-nvme-gib-month`), and downloads
with `store get` as egress. `prices set --default-quota 50G` caps every
account without its own quota.

**Private images.** Store a registry login as a cluster secret and jobs can
use images from that registry, with Docker and in microVMs:

```sh
pumasi secret set registry/ghcr.io 'user:token'     # or read from stdin
pumasi secret ls                                    # names only, never values
```

The control plane pulls the image into the file store and nodes load it
from there, so the login never leaves the control plane, and nodes share
image layers with each other instead of each pulling them.

### Cluster management

Tell the scheduler where jobs may run, what matters most, and how much each
account may use, the way you would in Kubernetes, without leaving the CLI.

**Labels and selectors.** Every node has built-in labels (`name`, `os`,
`arch`, `role`, `account`, `executor`, `gpu`, `gpus`, `disk`) and any you
give it. A job's `selector` keeps it on nodes that match:

```sh
pumasi label desktop zone=home tier=fast      # `zone-` removes a label
pumasi get nodes -l 'os=linux,disk=nvme|ssd'
```

```toml
# job.toml
[selector]
os = "linux"
disk = ["nvme", "ssd"]           # any of these
gpu = "!amd"                     # anything but
zone = true                      # has the label at all (false: does not)
```

**Taints.** A tainted node takes only jobs that tolerate the taint, so you
can keep the GPU box for GPU work (`pumasi taint desktop gpu-only`, then
`tolerations = ["gpu-only"]` in the jobs that may go there; `key-`
removes a taint).

**Cordon and drain.** `pumasi cordon NODE` stops new work from going to a
node, `pumasi drain NODE` also moves its running tasks elsewhere (they do
not count against the job's attempts), and `pumasi uncordon NODE` brings it
back, for example after maintenance.

**Priorities and preemption.** `priority = 10` in job.toml (or `pumasi run
--priority 10`, from -1000 to 1000, default 0) puts a job ahead of lower
ones in the queue. When no node has room, a higher-priority task stops the
fewest and least important running tasks it needs; they go back to the
queue without losing an attempt.

**Quotas.** Each account can be capped in running tasks, GPUs, CU spent in
any 24 hours, the highest priority it may use and file store space. Work
over a quota waits in the queue until the account is back under it.

```sh
pumasi quota alice --slots 8 --gpus 1 --cu-per-day 50 --max-priority 10 --storage 100G
pumasi quota                                   # every account's limits and usage
```

**Role tickets.** Give people their own tickets instead of the admin one.
A `submitter` ticket works with one account's jobs, schedules and datasets
and sees only those; a `viewer` ticket only reads. Tickets can expire and
be revoked; they save it with `pumasi login`.

```sh
pumasi access create "alice laptop" --role submitter --account alice --ttl 90d
pumasi access ls
pumasi access revoke "alice laptop"
pumasi whoami
```

A submitter cannot remove nodes, change prices or quotas, read other
accounts' jobs or mount their datasets, and its jobs run at priority 0
unless its account's `max_priority` allows more.

**Schedules and dependencies.** A job can run on a cron schedule, in the
control plane's local time, and skips a run while the previous one is
still going (`--overlap` allows it). `after` holds a job until the jobs it
names have succeeded, and fails it if one of them fails.

```sh
pumasi schedule add nightly job.toml --cron "0 3 * * *" --account alice
pumasi schedule ls
pumasi run merge.toml --after JOB_ID      # or after = ["JOB_ID"] in job.toml
```

**Declarative config.** Keep labels, taints, cordons, quotas and schedules
in one file (see [`examples/cluster.toml`](examples/cluster.toml)) and apply
it; applying it again changes nothing. `--dry-run` shows the changes first,
`--prune` also removes schedules the file does not list.

```sh
pumasi apply -f cluster.toml --dry-run
pumasi apply -f cluster.toml
```

```
node desktop: labels {} -> {tier=fast,zone=home}, taints [] -> [gpu-only]
account alice: quota none -> slots 8, gpus 1, 50 CU/day, priority <= 10, storage 100.0 GiB
schedule nightly-wordcount: created (0 3 * * *)
```

`pumasi get nodes|jobs|accounts|schedules|access [-o json]` lists them and
`pumasi describe node|job|account|schedule NAME` shows one in detail.

### Monitoring and alerts

Every agent reports its CPU, memory, the disk under its data directory,
network traffic, the hottest temperature sensor and each GPU's use, VRAM,
temperature and power every 5 seconds. The control plane keeps them for 7
days (`--metrics-retention`): 10-second steps for the last hour, 1-minute
steps for a day, 10-minute steps after that.

```sh
pumasi top                     # every node, the queue and firing alerts, live
pumasi metrics desktop --since 24h
pumasi logs JOB_ID -t 3 -f     # a task's stdout and stderr while it runs
pumasi events -f               # nodes coming and going, jobs finishing, alerts
```

```
2026-11-05 14:02:11  runs: 5 running, 37 queued (p10: 5, p0: 32), 2 jobs waiting, 0 held by quotas   alerts: 1
since the control plane started: 412 runs succeeded, 3 failed, 2 preempted, 0 check mismatches

NODE             STATE    TASKS   CPU  LOAD              MEMORY  DISK            NET IN / OUT   TEMP            GPU
desktop          online     4/8   87%   6.1   21.3 GiB/31.2 GiB   62%  1.2 MiB/s / 88.0 KiB/s   71°C   97% 18.1 GiB
laptop           online     1/4   24%   1.0    9.8 GiB/16.0 GiB   41%  12.0 KiB/s / 3.1 KiB/s   52°C              -
old-mac          offline    0/2
```

Output of WASM and Docker tasks reaches `pumasi logs -f` within a fraction
of a second; microVM tasks show theirs when they finish.

**Prometheus and Grafana.** `pumasi cluster start --metrics-addr
127.0.0.1:9464` serves `/metrics` in the Prometheus text format: node
metrics, the queue by priority, runs and preemptions, check mismatches,
firing alerts, account balances and the file store's size. It is off
unless you give the address; `--metrics-token` (or
`PUMASI_METRICS_TOKEN`) requires `Authorization: Bearer TOKEN`. See
[`examples/prometheus.yml`](examples/prometheus.yml) and the Grafana
dashboard in [`examples/grafana`](examples/grafana).

**Alerts.** The control plane watches for a node offline for a minute, half
the runs of the last 15 minutes failing, a disk (a node's or its own) 90%
full, a sensor at 90 °C, a benchmark 25% slower than the node's last one, a
drive failing SMART and a check run disagreeing with a provider. Change the
thresholds or turn rules off, and send alerts to Slack, Mattermost, Discord
or any URL that takes JSON; each alert is sent when it starts and when it
clears.

```sh
pumasi alert set --node-offline 2m --disk 85% --temperature off
pumasi webhook add ops https://hooks.slack.com/services/...   # sends a test alert
pumasi alert ls
```

Both can also live in `cluster.toml` (`[alerts]` and `[webhooks.NAME]`,
with `url_env` to keep the URL out of the file).

### Control plane replicas and backups

One control plane is a single point of failure: while it is down, nothing
is scheduled and no CU move. Run it on three machines (or five) instead,
and the cluster keeps going when any one of them (two of five) dies:

```sh
pumasi ha invite                  # on the control plane: prints a replica ticket
pumasi cluster replica <TICKET>   # on each new control plane machine
pumasi ha status                  # who leads, who votes, how far each has the log
```

The replicas keep the control plane's whole state (members, the queue,
jobs and tasks, the CU ledger, receipts, the file store's metadata,
settings) in step over Raft. One of them leads and does the work; the
others follow, and when the leader dies they elect a new one within a few
seconds. It loads the state everything agreed on and carries on: nodes
and the CLI reconnect to it on their own, tasks that were running are
placed again, and nothing is paid twice, because a charge or a payout is
one entry that every replica applies or none does. A replica that comes
back catches up from the others. A leader cut off from the majority
stops scheduling within 3 seconds, so two leaders never run at once.

Join and admin tickets made after `ha invite` list every replica, so hand
out new ones (`pumasi token`, `pumasi access create`) once the replicas
are up. Agents learn about new replicas from the leader by themselves.
Restart a replica with `pumasi cluster start`; `pumasi ha remove ID`
takes one out for good. A majority must be up, so use 3 or 5 replicas,
not 2 or 4.

Back up the state at any time, from anywhere your admin ticket works:

```sh
pumasi backup pumasi-2026-11-05.backup
pumasi cluster restore pumasi-2026-11-05.backup   # on a fresh machine, then `pumasi cluster start`
```

The file is a consistent copy (taken in one transaction) of the state and
the cluster key, so nodes reconnect to the restored control plane when it
is reachable where the old one was. It holds the cluster key, the admin
secret and the cluster's secrets: keep it like the admin ticket. The
file store's data (`~/.pumasi/cluster/blobs`) is not in it; copy that
directory too. A restore makes one control plane; add replicas again with
`pumasi ha invite`.

### Web dashboard

```sh
pumasi ui            # opens http://127.0.0.1:7400/login?token=... in your browser
```

`pumasi ui` runs the dashboard on your own machine and talks to the
cluster over the same connection as the CLI, so the control plane opens no
port and it works from anywhere your ticket works (`pumasi login`). It is
part of the binary: no Node, no separate install. The pages:

- **Overview**: nodes online, running and queued tasks, active jobs, GPUs
  in use, firing alerts, recent jobs and events, refreshed every few
  seconds.
- **Nodes**: each node's state, CPU, memory, disk, temperature and GPUs;
  one node's charts over an hour, a day or a week, its benchmark and
  GPUs, its labels and taints, and buttons to cordon, drain, benchmark
  again or remove it.
- **Jobs**: submit a job (paste a job.toml, attach its files or a
  folder), follow each task's stdout and stderr live, cancel, download one
  output file or all of them as a zip, and the signed receipts (the
  `pumasi verify-receipts` format).
- **Storage**: datasets and their versions, usage against quotas, upload a
  folder as a new version, download one as a zip.
- **CU**: balances and the ledger; admins grant and pay out.
- **Invites**: join tickets with the install commands, provider invites,
  and access tickets for people.
- **Prices** and **Events** (with alert rules and webhooks).

Korean and English (the browser's language, or the switch in the header).

The URL `pumasi ui` prints signs the browser in with your ticket. Anyone
else signs in on the same page with an access ticket (`pumasi access
create`), and sees and may do what its role allows: the control plane
checks every call, as for the CLI. `--login-only` turns the link off;
`--listen 0.0.0.0:7400` serves other machines too (always sign-in only,
over plain HTTP, so put a TLS reverse proxy in front).

The same server has a JSON API under `/api/v1` (status, metrics, events,
jobs with submit, cancel, logs, outputs and receipts, node drain and
cordon, accounts, ledger, datasets, prices, invites, and `POST /api/v1/op`
for any admin call), described by `/api/openapi.json`:

```sh
curl -H "Authorization: Bearer $(cat ~/.pumasi/admin.ticket)" http://127.0.0.1:7400/api/v1/status
curl -H "Authorization: Bearer $TICKET" -F job=@job.toml -F files=@model.wasm http://127.0.0.1:7400/api/v1/jobs
```

### Using the cluster from anywhere (HTTP API)

Scripts on machines without Pumasi (a laptop at home, a CI job, another
team's service) can submit jobs and fetch results over HTTPS with `curl`.
The admin gives each client a submitter ticket and a job policy for its
account, serves the API alone, and puts it behind a Cloudflare Tunnel, a
reverse proxy or its own certificate:

```sh
pumasi access create "lab scripts" --role submitter --account lab
pumasi quota lab --executors docker --images 'ghcr.io/lab/*' --network deny \
  --max-timeout 30m --max-gpus-per-task 1
pumasi ui --api-only --listen 127.0.0.1:7400 --real-ip-header CF-Connecting-IP
cloudflared tunnel run pumasi          # https://pumasi.example.com -> 127.0.0.1:7400
```

and the client:

```sh
curl -H "Authorization: Bearer $PUMASI_TICKET" -F files=@job.toml -F files=@scene.py \
  https://pumasi.example.com/api/v1/jobs
curl -H "Authorization: Bearer $PUMASI_TICKET" "https://pumasi.example.com/api/v1/jobs/$JOB?wait=60s"
curl -H "Authorization: Bearer $PUMASI_TICKET" -o out.zip "https://pumasi.example.com/api/v1/jobs/$JOB/outputs.zip"
```

- **Job policies** per account: allowed images (patterns or pinned
  digests), executors, no network, a selector added to every job, allowed
  tolerations, the longest timeout and the most GPUs per task. Anything
  else is refused with `403` and the reason, whoever submits it. Every
  job records the ticket that submitted it and the digest of each image
  its tasks ran.
- **`--api-only`** serves `/api/v1` and `/api/openapi.json` only, with
  bearer tickets (no pages, no cookies). Ten wrong tickets in a minute
  from one address block it for five minutes (`429`). `--tls-cert` and
  `--tls-key` serve HTTPS directly; on an address other than loopback
  `--api-only` refuses plain HTTP.
- **`?wait=60s`** on a job answers as soon as it finishes (at most 90
  seconds, under Cloudflare's 100). **`queue_timeout = "10m"`** in
  job.toml fails a job no node has started in that time, with each node's
  reason, instead of leaving it queued. **`capacity`** in
  `/api/v1/status` (and `pumasi status`) says how many nodes would take
  the account's work right now, per executor and with GPUs, and why each
  other node would not.
- Agents re-check Docker every 30 seconds, so a Windows machine whose
  Docker Desktop starts late, or stops, is offered Docker work (or not)
  without a restart.

[docs/api.md](docs/api.md) walks through it with curl examples that CI
runs against a real cluster, including the Cloudflare Tunnel setup and
its limits (100 MB request bodies on the free plan).

**Blender and other GPU drawing.** CUDA jobs need only `gpus = 1`. Jobs
that draw (Blender EEVEE, OpenGL or Vulkan renderers) add `capabilities
= ["graphics"]` to `[gpu]` and go only to nodes whose Docker GPUs can
draw: `pumasi gpu check --docker-run --image IMAGE` on a node starts a
container and reports CUDA, OptiX and drawing (NVIDIA's EGL and Vulkan
drivers on Linux, Mesa's D3D12 driver on Windows with Docker Desktop),
and the node gets the label `gpu.graphics=true`. On Windows the probe and
graphics tasks get Docker Desktop's `/usr/lib/wsl` read-only, since Docker
Desktop does not put the D3D12 libraries in GPU containers itself
(`--wsl-libs` on the check, `--docker-wsl-libs` on the agent; `auto` means
Windows only). OptiX does not start in Docker Desktop containers, so
Cycles uses CUDA there.
[examples/blender-render](examples/blender-render) renders a scene with
Cycles and with EEVEE.

### Confidential jobs: hidden even from the machine's owner

Every job has a confidentiality tier (`tier` in job.toml):

| `tier` | Runs on | Who can read the job |
| --- | --- | --- |
| `open` (default) | any node that takes the executor | the machine's owner |
| `trusted` | your own machines, and providers you trust (`pumasi trust NODE`) | only owners you trust |
| `confidential` | microVMs in AMD SEV-SNP or Intel TDX on nodes started with `--allow-confidential` | nobody but the job: the node's owner sees only sealed bytes |

A confidential job is a microVM job whose image, command, environment,
input files and mounted datasets never reach the node in the clear. The
control plane pulls the image and seals it and each task's input with
keys only it has. The node boots a VM whose memory its CPU encrypts; the
guest asks the TEE for a signed report that measures the firmware, kernel
and guest init it booted and binds a fresh key and the run's nonce. The
control plane checks the report against AMD's or Intel's certificates,
and releases the keys, sealed to that guest, only when the report is
genuine, current, for this run, and its launch measurement is one an
admin trusts. The guest seals its result the same way; the control plane
opens it before it counts.

```toml
name = "payroll"
image = "ghcr.io/acme/payroll@sha256:..."
executor = "microvm"
tier = "confidential"
files = ["salaries.csv"]
```

```sh
# On a SEV-SNP or TDX server (bare metal: cloud confidential VMs cannot nest):
pumasi agent start --allow-microvm --allow-confidential
pumasi confidential ls                 # TEE nodes, and launch measurements seen
pumasi confidential trust 3f9a1c0b2e7d --note "agent 0.17, OVMF 2025.02, 4 vCPUs"
```

The first confidential task on a new guest (a new agent version, firmware
or vCPU count) fails with "launch measurement ... is not trusted" and the
measurement shows up in `pumasi confidential ls` and on the Nodes page;
trust it once you know it is your guest. The node needs SEV-SNP or TDX
turned on in the BIOS and KVM, QEMU 9.1 or later (TDX: 10.1), and in the
VM directory's `tee/` an OVMF built for it (`OVMF-snp.fd` from the
`AmdSevX64` build, or `OVMF-tdx.fd`) and a guest kernel with the
SEV-SNP or TDX guest drivers and `CONFIG_TSM_REPORTS` (`vmlinuz`).
Confidential tasks have no network and no GPUs, run on x86-64 only, and
keep their root file system, input and output in the VM's encrypted
memory, so the node needs memory for the image (about three times its
compressed size) on top of the task's. Their output is not streamed while
they run. See [docs/architecture.md](docs/architecture.md#phase-16-the-confidential-tier)
for the protocol and what it does not protect.

### Running it for an organization: SSO, audit log, operations

People sign in with the accounts they already have, through OpenID
Connect (Google Workspace, Microsoft Entra ID, Keycloak, Okta) or LDAP
and Active Directory. Rules map their groups, emails or domain to a role,
and each sign-in becomes an access ticket that lasts a session:

```sh
pumasi sso oidc --issuer https://login.microsoftonline.com/<tenant>/v2.0 \
  --client-id ... --client-secret-file oidc-secret --name "Acme SSO"
pumasi sso map group:<admins-group-id> admin
pumasi sso map group:<research-group-id> submitter:research
pumasi sso map domain:acme.com viewer
pumasi ui --public-url https://pumasi.acme.com     # "Sign in with Acme SSO"
pumasi sso device on                               # allow pumasi login --sso (off by default)
pumasi sso ticket                                  # for the CLI: pumasi login --sso <ticket>
```

Every change, refusal, sign-in and node join goes into a tamper-evident
audit log, a hash chain whose head the cluster key signs:

```sh
pumasi audit ls --follow
pumasi audit verify --json > head.json            # keep it elsewhere; later: --against head.json
pumasi audit export --syslog tcp://siem.acme.local:601 --state audit.state --follow
```

`pumasi admin rotate` retires a leaked admin ticket, `pumasi ui` limits
requests per ticket and per address, and finished jobs are deleted after
`--keep-jobs` (30 days by default). [docs/operations.md](docs/operations.md)
covers installing, upgrading, backups, the audit log and incident
response; [docs/threat-model.md](docs/threat-model.md) what Pumasi
defends against, including the API exposed to the internet.

## Commands

| Command | What it does |
| --- | --- |
| `pumasi cluster start [--name N] [--net public\|local] [--bind ADDR] [--metrics-addr ADDR] [--keep-jobs 30d]` | Run the control plane. Creates the cluster on first run, keeps state in `~/.pumasi/cluster`. `--metrics-addr` serves Prometheus metrics; `--keep-jobs` is how long finished jobs are kept (`0`: forever). |
| `pumasi token [--ttl 24h] [--uses N]` | Create a join ticket for another machine of yours. |
| `pumasi invite <ACCOUNT> [--ttl 7d] [--uses 1] [--allow-docker]` | Invite someone else's machine as a provider earning for `ACCOUNT`. |
| `pumasi agent join <TICKET> [--name N] [--slots N] [--allow-docker] [--allow-microvm] [--allow-confidential] [--allow-webgpu] [--no-gpus]` | Join a cluster and start running tasks. `--allow-docker` also accepts docker jobs, `--allow-microvm` microvm jobs (`--vm-backend firecracker\|qemu`, `--vm-dir DIR`), `--allow-confidential` confidential jobs in SEV-SNP or TDX microVMs, `--allow-webgpu` wasm jobs with GPUs (`--webgpu-software` adds a software rasterizer, for tests). GPUs are offered unless `--no-gpus`. |
| `pumasi agent start [--allow-docker] [--allow-microvm] [--auto-upgrade\|--no-auto-upgrade]` | Reconnect to the cluster this machine joined before (no ticket needed). |
| `pumasi service install [--ticket T] [--user\|--system] [--dry-run] [-- AGENT OPTIONS]` | Run the agent as a service that starts at boot (systemd, launchd, Windows service). `status` and `uninstall` too. |
| `pumasi upgrade rollout FILE... [--wait] [--force]` | Roll new agent binaries out to the nodes, signed with the cluster key. `upgrade status` shows each node, `upgrade cancel` stops offering it. |
| `pumasi vm setup [--guest FILE] [--force] [--gpu-bundle DIR]` | Install the guest kernel, Firecracker (Linux) or QEMU (Windows, as administrator) and the guest init into `~/.pumasi/vm`; `--gpu-bundle` adds the GPU guest kernel and driver for passthrough. |
| `pumasi vm status` | Show what is installed for microVMs and which VM monitor will be used. |
| `pumasi vm run IMAGE [--input DIR] [-e K=V] [--memory-mb N] [--cpus N] [--timeout T] [--network] [-- CMD...]` | Run an image in a microVM on this machine, the way an agent would (`/output` is saved to `pumasi-out/vm`). |
| `pumasi gpu list [--no-bench] [--json]` / `gpu check [GPU] [--docker-run [--image IMAGE] [--wsl-libs auto\|yes\|no]]` / `gpu webgpu [--software]` | This machine's GPUs and their benchmark; `check` says whether Docker and microVMs can use each one and runs the cluster's GPU check locally, and with `--docker-run` starts a container to see whether CUDA, OptiX and drawing (EGL, OpenGL, Vulkan) work in Docker (saved for the agent: `gpu.graphics`); `webgpu` lists the WebGPU adapters wasm jobs can use. |
| `pumasi store put NAME PATHS... [--account A]` / `get NAME[@V] \| --job ID [-o DIR]` / `ls [NAME]` / `rm NAME[@V]` / `usage` / `quota ACCOUNT SIZE\|none` | The cluster file store: versioned datasets, job outputs, resumable transfers, quotas. |
| `pumasi secret set NAME [VALUE]` / `ls` / `rm NAME` | Cluster secrets; `registry/<host>` logs in to a private registry. |
| `pumasi run job.toml [--account A] [--priority N] [--after JOB] [--output] [--out DIR] [--detach]` | Submit a job and wait for it. Output files are saved to `DIR` (default `pumasi-out/<job id>`). Exit code 0 on success, 2 if any task failed. `--account` picks who pays for provider runs (default `owner`). |
| `pumasi status` | Nodes (role, online, executors, slots, running, checks, billed CPU score, disk class, latency, GPUs) and recent jobs with their cost in CU. |
| `pumasi node <ID> [--rebenchmark]` | One node's benchmark profile, billed score, GPUs, latency and checks; `--rebenchmark` asks it to measure again. |
| `pumasi bench [--quick] [--json] [--dir DIR]` | Benchmark this machine the way the agent does and print the profile. |
| `pumasi job <ID> [--output] [--out DIR]` | Per-task state, node, attempts, time, cost and check result; stdout/stderr with `--output`; downloads output files with `--out`. |
| `pumasi cu list \| add <ACCOUNT> <CU> \| payout <ACCOUNT> <CU> \| ledger [--account A]` | Balances in CU (available, held, spent, earned), grants, payouts, and the ledger (`pumasi credit` still works). |
| `pumasi prices [set --memory-gib-hour X --storage-gib-month X --storage-hdd-gib-month X --storage-nvme-gib-month X --default-quota SIZE --egress-gib X --gpu-tflops-hour X --gpu-vram-gib-hour X --ledger prepaid\|mutual --credit-limit CU --hold 1h --verify-rate 10% --max-strikes 3]` | Show or change the CU rates, the ledger mode and the settlement policy. |
| `pumasi receipts <ID> [--export FILE]` | A job's signed usage receipts, with the GPU part of each price. |
| `pumasi verify-receipts FILE [--cluster ID]` | Check exported receipts offline. |
| `pumasi cancel <ID>` | Cancel queued and running tasks of a job. |
| `pumasi remove-node <ID>` | Revoke a node. It needs a new ticket to come back. |
| `pumasi login <TICKET> [--sso \| --ldap USER]` | Manage a remote cluster from this machine, with the admin ticket or an access ticket; `--sso` (device flow) and `--ldap` sign in with a sign-in ticket from `pumasi sso ticket`. |
| `pumasi sso status` / `sso oidc --issuer URL --client-id ID --client-secret-file F` / `sso ldap --url ldaps://... --user-base DN` / `sso map RULE ROLE` / `sso unmap RULE` / `sso session 12h` / `sso device on\|off` / `sso ticket` | Single sign-on: an OpenID Connect provider or an LDAP directory, rules (`group:`, `email:`, `domain:`, `anyone`) giving `admin`, `viewer` or `submitter:ACCOUNT`, how long a sign-in lasts, whether `pumasi login --sso` (device flow, off by default) is allowed, and the ticket people sign in with. `--off` turns a method off. |
| `pumasi audit ls [--after N] [--limit N] [--json] [-f]` / `audit verify [--json] [--against HEAD]` / `audit export [--syslog udp://\|tcp://HOST:PORT] [--state F] [-f]` | The audit log: entries, a check of the whole hash chain with a head signed by the cluster key, and syslog (RFC 5424) export. |
| `pumasi admin rotate` | Replace the admin secret: every copy of the old admin ticket stops working. |
| `pumasi confidential ls [--json]` / `confidential trust ID [--note N]` / `confidential untrust ID` | TEE nodes and the launch measurements their guests attested with; keys go only to guests whose measurement is trusted. |
| `pumasi trust \| untrust <NODE>` | Let a provider's node run `tier = "trusted"` jobs, or stop it (your own machines always do). |
| `pumasi cordon \| uncordon \| drain <NODE>` | Stop new tasks going to a node, take it back, or also move its running tasks elsewhere. |
| `pumasi label <NODE> k=v... \| k-` / `taint <NODE> key[=value]... \| key-` | Node labels for job selectors; taints keep away jobs that do not tolerate them. |
| `pumasi quota [ACCOUNT --slots N --gpus N --cu-per-day CU --max-priority N --storage SIZE --images P,.. --require-digest yes --executors E,.. --network deny --selector S --tolerations T,.. --max-timeout D --max-gpus-per-task N] [--clear]` | Show every account's limits, job policy and usage, or change one (`none` or `any` lifts a limit). |
| `pumasi access create NAME --role admin\|submitter\|viewer [--account A] [--ttl 30d]` / `ls` / `revoke ID\|NAME` | Tickets with a role for other people; `pumasi whoami` shows the one in use. |
| `pumasi schedule add NAME job.toml --cron "0 3 * * *" [--account A] [--overlap]` / `ls` / `rm NAME` / `run NAME` | Jobs submitted on a cron schedule. |
| `pumasi apply -f cluster.toml [--dry-run] [--prune]` | Make labels, taints, cordons, quotas, schedules, alert rules and webhooks match a file; applying twice changes nothing. |
| `pumasi get nodes\|jobs\|accounts\|schedules\|access [-l SELECTOR] [-o json]` / `describe node\|job\|account\|schedule NAME` | List things, or show one in detail. |
| `pumasi top [--interval 2s] [--once]` | Every node's CPU, memory, disk, network, temperature and GPUs, the queue and firing alerts, refreshed until Ctrl-C. |
| `pumasi metrics [NODE] [--since 1h] [-o json]` | The latest metrics of every node, or one node's history. |
| `pumasi logs <JOB> [-t TASK] [-f]` | A task's stdout and stderr; `-f` follows it while it runs. |
| `pumasi events [-f] [--since 1h] [-n 50] [-o json]` | Cluster events: nodes, jobs, checks, preemptions, alerts. |
| `pumasi alert ls` / `alert set [--node-offline 1m] [--failure-rate 50%] [--disk 90%] [--temperature 90] [--perf-drop 25%] [--smart on] [--check-mismatch on] [--defaults]` | Alert rules and what fires now; `off` turns a rule off. |
| `pumasi webhook add NAME URL [--format slack\|discord\|json]` / `ls` / `rm NAME` / `test NAME` | Where alerts go. |
| `pumasi ha status [-o json]` / `ha invite [--ttl 1h]` / `ha remove ID` | Control plane replicas: who leads and how far each has the log, a ticket for one more replica, take one out. |
| `pumasi cluster replica <TICKET> [--net public\|local] [--bind ADDR]` | Run this machine as a control plane replica (restart it later with `pumasi cluster start`). |
| `pumasi backup FILE` / `pumasi cluster restore FILE` | A consistent copy of the control plane's state and cluster key, and a control plane made from one. |
| `pumasi ui [--listen ADDR] [--login-only] [--no-open] [--url-file F] [--max-upload 4GiB] [--api-only] [--tls-cert F --tls-key F] [--real-ip-header H] [--public-url URL]` | The web dashboard and its JSON API, served from this machine (default 127.0.0.1:7400); `--api-only` for programs elsewhere (bearer tickets only), `--tls-cert` for HTTPS, `--public-url` the address browsers use (SSO sends them back to it). |
| `pumasi exec module.wasm [--input DIR] [--seed S] [--gpu] [ARGS...]` | Run a module locally in the agent's sandbox (stdin is passed through, `DIR` appears at `/input`, `/output` is saved to `pumasi-out/exec`, `--seed` for deterministic mode, `--gpu` gives it this machine's best WebGPU adapter). |

`--home DIR` or `PUMASI_HOME` changes the state directory, which lets you run
a cluster and several agents on one machine for testing. `PUMASI_LOG=debug`
turns on verbose logs.

## Job file

A job runs on one of three executors with the same file format and commands:

| | `wasm` (default) | `docker` | `microvm` |
| --- | --- | --- | --- |
| Runs | a `wasm32-wasip1` module | any Linux OCI image | any Linux OCI image |
| Where | every node | only nodes started with `--allow-docker` (providers also need an invite with `--allow-docker`) | nodes started with `--allow-microvm`, providers included |
| Isolation | Wasmtime sandbox, safe for untrusted code | container, only for machines and people you trust | a VM with its own kernel per task |
| Network | never | off by default, `network = true` to allow | off by default, `network = true` reaches public addresses only |
| GPUs | `gpus = 1`: any GPU through WebGPU (nodes with `--allow-webgpu`, experimental) | `gpus = N` or `gpu = {...}` (NVIDIA, AMD on Linux) | `gpus = N`, passed through with VFIO (Linux, experimental) |
| Files | `/input`, `/output` | `/input`, `/output` | `/input`, `/output` |

WASM job:

```toml
name = "estimate-pi"                  # optional, defaults to the module name
module = "montecarlo.wasm"            # a wasm32-wasip1 command module
shards = 16                           # N tasks; {shard} and {shards} are substituted
args = ["sample", "{shard}", "5000000"]
env = { MODE = "fast" }               # optional
stdin = "input.txt"                   # optional, fed to every task
files = ["data/", "lookup.csv"]       # optional, appear under /input; "store:NAME" mounts a dataset
max_attempts = 3                      # placements before giving up on lost nodes
deterministic = true                  # default (off with gpus): virtual clock + seeded random, so results can be checked
# gpus = 1                            # one GPU through WebGPU (the pumasi_webgpu imports)
priority = 0                          # -1000..1000; higher runs first and may preempt lower
after = []                            # job ids that must succeed before this job starts
tolerations = []                      # taints of nodes it may run on, e.g. ["gpu-only"]
# queue_timeout = "30m"               # fail (with each node's reason) if no node starts it by then

[selector]                            # only nodes whose labels match (see Cluster management)
os = "linux"

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
# gpus = 1                            # GPUs per task ("all": every GPU of the node)
# gpu = { vendor = "nvidia", min_vram_gb = 16 }  # which GPUs (vendor: nvidia, amd, any)
# gpu = { vendor = "nvidia", capabilities = ["graphics"] }  # drawing (EGL, OpenGL, Vulkan) or "video" (NVENC)

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

Any job may set `tier = "trusted"` (only your machines and providers you
trust run it) and a microVM job `tier = "confidential"` (see
[Confidential jobs](#confidential-jobs-hidden-even-from-the-machines-owner)).

Paths in `files` are relative to the job file; directories are copied
recursively and keep their relative path under `/input`. Larger inputs go
through the [file store](#the-cluster-file-store), as do outputs over
32 MiB. Whatever a task writes to `/output` is downloaded by `pumasi run`
into `pumasi-out/<job id>/task-<N>/`. stdout and stderr are captured up to
4 MiB each.

Containers run with `--network none` unless `network = true`, with memory,
CPU and process caps, `no-new-privileges`, and no host directories mounted
(files are copied in and out). The one exception: on Windows, NVIDIA tasks
that ask for `graphics` get Docker Desktop's `/usr/lib/wsl` read-only, the
GPU driver's Linux libraries, which Docker Desktop leaves out of GPU
containers (`--docker-wsl-libs no` on the agent turns it off). Images are pulled on first use; images from
a registry with a login in `pumasi secret` come through the file store.

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

- **Control plane** (`crates/cluster`): membership, join tokens, a task
  queue ordered by priority (first in, first out within one) and a
  scheduler that places tasks on the node with the most free slots among
  those that offer the job's executor, enough memory and GPUs, match its
  selector and tolerations and are not cordoned, within the account's
  quota, preempting lower-priority tasks when nothing has room. State
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
- **Devices**: agents also send a signed device profile (GPUs with their
  benchmark and what can use them, and whether microVMs can have a
  network). The scheduler hands out GPUs one by one, like slots, and
  providers' GPUs are timed with a challenge before they are billed above
  the reference.

## Security model

- Join tickets are bearer secrets with an expiry and optional use count.
  Once a node joins, its public key is remembered; the ticket is not needed
  again. `remove-node` revokes it.
- The admin ticket in `~/.pumasi/cluster/admin.ticket` grants full control.
  Treat it like a password. Give other people access tickets instead
  (`pumasi access create`): `submitter` tickets work with one account's
  jobs, schedules and datasets and see only those, `viewer` tickets only
  read. Every request is checked against the ticket's role on the control
  plane; revoked and expired tickets stop working at once.
- `pumasi ui --api-only` on the internet (through a tunnel, a proxy or
  `--tls-cert`) takes bearer tickets only and rate limits wrong ones per
  client address; refuses plain HTTP on other addresses than loopback;
  and gives each client only what its ticket's role and its account's job
  policy allow. A submitter ticket that leaks can still run whatever that
  account's policy allows (on Docker nodes, containers that share the
  host kernel), so keep policies narrow (pinned images, no network) and
  revoke tickets with `pumasi access revoke`.
- SSO sign-ins become access tickets with the role their rules give and
  the session length (`pumasi sso session`); `pumasi access revoke NAME`
  signs a person out everywhere. OIDC uses the code flow with PKCE, state
  and nonce and checks ID tokens against the provider's keys; LDAP binds
  over LDAPS or StartTLS. `pumasi admin rotate` retires a leaked admin
  ticket.
- Every change, refusal, sign-in and node join is in a hash-chained audit
  log (`pumasi audit verify` checks it and signs its head); export it to
  syslog to keep a copy out of the cluster's reach. The full threat model,
  including the API exposed to the internet, is in
  [docs/threat-model.md](docs/threat-model.md); report vulnerabilities as
  [SECURITY.md](SECURITY.md) says.
- WASM tasks are sandboxed from the machine running them (only their own
  `/input` and `/output`, no network, memory and CPU caps).
- Docker tasks are only as isolated as a container: they share the host
  kernel. That is why an agent runs them only after its owner passes
  `--allow-docker`. Use it on your own machines and in clusters of people
  you trust.
- MicroVM tasks protect the provider's machine from the job: each task gets
  its own kernel behind KVM (Firecracker, which also confines itself with
  seccomp), only its own disks, and memory and vCPU caps. It has no network
  device unless the job sets `network = true`, and then only a card whose
  traffic the agent forwards to public addresses (never the provider's own
  machine or network). A passed-through GPU is behind the IOMMU, and only
  GPUs whose whole IOMMU group can go with them are passed through.
  Image layers are unpacked inside a VM, never on the host. The provider
  still sees everything the task does (open tier) unless the job is
  confidential.
- Provider machines (other people's, admitted with `pumasi invite`) get WASM
  and microVM tasks, Docker only if the invite allowed it, and never settle a
  check: only your own machines are trusted to arbitrate. MicroVM results are
  not deterministic, so they are not spot-checked.
- A provider can return made-up results; signatures prove who claimed what,
  not that it is true. Checks catch it statistically: with the default 10%
  rate, a provider faking 20 results is caught with 88% probability, loses
  every held earning that a check rejects, and is suspended after 3 strikes.
  Earnings are held for an hour so a later check can still claw them back.
- Open and trusted tasks are **not** hidden from the machine's owner: do
  not send secrets to machines you do not trust (`tier = "trusted"` keeps a
  job on your own machines and providers you trust). Confidential tasks
  are: their image, inputs and outputs reach the node only sealed, and the
  keys go only to a SEV-SNP or TDX guest whose signed report the control
  plane checked against AMD's or Intel's certificates, for that very run,
  with a launch measurement the admin trusts (`pumasi confidential
  trust`). The node can still see that a task ran, how long, how much it
  read and wrote, and its memory size; it can stop it or return no
  result, but not read or change it. The control plane holds the keys, so
  it, its replicas and backups, and whoever holds the admin ticket can read
  confidential jobs: run the control plane on a machine you trust.
  `--insecure-test-tee` (tests only) turns all of this into a show.
- Agent upgrades are signed by the control plane's key, which agents learned
  when they joined, and checked against the signed SHA-256 before they run.
  Whoever holds the admin ticket can roll out a binary to your machines (and
  to providers that opted in), so the admin ticket is as sensitive as root
  on them. Release binaries come with `SHA256SUMS`, and with GitHub build
  provenance attestations once the repository is public
  (`gh attestation verify pumasi-linux-x86_64 -R syang0531/pumasi`).

## Roadmap

Phases 1 to 17 are what an organization needs to run Pumasi on its own
machines and use it from anywhere. The internal security review of the
scope in [docs/security-review.md](docs/security-review.md) is done
(findings and fixes in the [threat model](docs/threat-model.md#internal-review-for-10));
1.0 was tagged after a green run of the long checks (5-hour soaks on
Linux and macOS, 20 minutes per fuzz target) on the tagged commit
([release notes](docs/releases/v1.0.0.md); 1.0.1 fixes GPU drawing in Docker on
Windows and the Blender example: [notes](docs/releases/v1.0.1.md)). There has been no
paid third-party audit yet ([SECURITY.md](SECURITY.md)). Phases 18 and 19
are only for trading with other clusters.

| Phase | Scope | Status |
| --- | --- | --- |
| 1 | Private cluster: agent, control plane, iroh networking, WASM and Docker executors, file input/output, CLI | done |
| 2 | Invited outside providers (open tier), usage receipts signed by node and cluster, fixed price list, prepaid credit ledger, sampled re-runs to check results | done |
| 3 | microVM executor: same OCI images as docker jobs in Firecracker or QEMU VMs, behind the same job format, open to providers | done |
| 4 | Distribution and install: release binaries for six platforms with SHA256SUMS, install scripts, OS services, signed cluster-driven upgrades with rollback | done |
| 5 | Windows: Docker Desktop (WSL 2), microVMs with QEMU + WHPX (TCG fallback), pinned QEMU install, Job Objects, built-in ext4 formatter and guest init | done |
| 6 | Old Intel Macs (macOS 10.15+), Virtualization.framework microVMs, availability policy (on AC power, idle, hours, CPU cap, memory-aware slots) | done |
| 7 | Benchmarks and compute units (CU): signed node performance profiles (CPU, memory bandwidth, disk class and speed with fio, SMART wear, latency), 1 CU = one reference vCPU for one hour with per-resource CU rates, ledger in integer milli-CU, group-local ledger for money-free pooling | done |
| 8 | Cluster file store (iroh-blobs): versioned datasets, `store:` mounts and output datasets, inputs and outputs of any size, resumable hash-checked transfers, node caches shared between nodes, reference GC, quotas, storage and egress billed in CU, private registry images | done |
| 9 | GPUs and microVM devices: NVIDIA (NVML, CUDA benchmark) and AMD detection, signed device profiles, GPU time billed in CU, per-GPU scheduling, Docker GPU jobs, GPU checks for providers, VFIO passthrough (experimental), filtered microVM networking | done |
| 10 | Cluster management: node labels and taints, job selectors and tolerations, cordon and drain, priorities with preemption, per-account quotas (slots, GPUs, CU per day, priority, storage), role tickets (admin, submitter, viewer), declarative `apply` with `get` and `describe`, cron schedules, job dependencies | done |
| 11 | Monitoring and alerts: node metrics (CPU, memory, disk, network, temperature, GPUs) kept 7 days with downsampling, `top`, live `logs -f`, `events`, opt-in Prometheus endpoint with a Grafana dashboard, alert rules (node offline, failure rate, disk, temperature, benchmark drop, SMART, check mismatch) sent to Slack, Discord or JSON webhooks | done |
| 12 | Web dashboard built into the binary (`pumasi ui`): overview, nodes with charts, cordon and drain, jobs with submit, live output, cancel and output downloads, storage with uploads and downloads, CU and ledger, invites and access tickets, prices, events and alerts; sign-in with access tickets and their roles, Korean and English, JSON API with OpenAPI, Playwright tests in CI | done |
| 13 | Control plane high availability: state replicated over Raft (openraft) to 3 or 5 control planes, automatic failover with nodes, CLI and dashboard following the leader, tickets listing every replica, replicas catching up from snapshots, `pumasi backup` and `cluster restore` | done |
| 14 | WebGPU executor (experimental): wasm jobs with `gpus = 1` run WGSL compute shaders on any GPU through wgpu (Vulkan, Metal, DX12, GL) and the `pumasi_webgpu` imports, signed adapter profiles with a WGSL benchmark, adapters sharing slots with listed GPUs, GPU time billed in CU, matrix multiplication and blur examples checked against the CPU on Linux, Windows and macOS in CI, glibc Linux binaries that can load GPU drivers | done |
| 15 | Remote API clients: per-account job policies (images and digests, executors, no network, forced selector, tolerations, longest timeout, GPUs per task), who submitted each job and the image digests it ran, `queue_timeout`, the nodes that can take an account's work now (and why the others cannot), Docker re-checked by running agents, `pumasi ui --api-only` with HTTPS (`--tls-cert`) and failed sign-in limits, a Cloudflare Tunnel guide, `?wait=` long polling, curl examples run in CI, GPU driver capabilities for drawing (`gpu.graphics`, `gpu check --docker-run`) and a Blender example | done |
| 16 | Confidentiality tiers: `trusted` (owner machines and trusted providers) and `confidential` (SEV-SNP and TDX microVMs): images and inputs sealed on the control plane, remote attestation checked against AMD's and Intel's certificates (recorded reports and forgeries tested in CI), keys released only to a fresh guest with an admin-trusted launch measurement, sealed results, `pumasi confidential` and the dashboard's measurement list | done |
| 17 | Self-hosting 1.0: SSO (OIDC and LDAP, groups mapped to role tickets), tamper-evident audit log (hash chain, JSON and syslog export), threat model (including the API and tunnel exposed to the internet: leaked tickets, policy bypasses, request floods), cargo-audit, cargo-deny, CodeQL and fuzzing in CI, an SSO end-to-end test against Keycloak and OpenLDAP, an internal security review, operations guide, long soak tests on Linux and macOS, then the 1.0 tag. Also: job retention (`--keep-jobs`), admin secret rotation, per-ticket request limits | done (1.0) |
| 18 | Hub, called Madang (마당, "the yard"; separate private repository `madang`): provider discovery, latency probes, trust scores, cross-group CU ledger with a hub fee | planned |
| 19 | Buying CU with won through the hub (Toss Payments); no cash-out, no transfers between users | planned |

Known gaps: the file store's data is copied to the other control plane
replicas every 30 seconds rather than as part of each write (a file
uploaded just before the leader dies may be missing until it comes back),
agent binaries uploaded with `pumasi upgrade rollout` stay on the replica
that took them (roll out again after a failover), and agents older than
this release follow only the replica they joined. WebGPU is experimental: it is
checked on software rasterizers in CI and on real GPUs by hand, offers
compute shaders only (no textures or samplers, one bind group, no f16), a
runaway shader is stopped by the GPU driver's watchdog rather than the
job's timeout, and there is no GPU challenge over WebGPU yet. Accounts
exist only inside one cluster (SSO signs people in, but there is no SCIM
or SAML, and nobody is removed until their session ends or is revoked), CU are not connected to real payments, node-to-node latency and bandwidth are not measured yet
(only each node's round trip to the control plane), the file store lives
only on the control plane (no replicas on owner nodes yet, no S3-compatible
gateway). GPUs are checked on real hardware by hand, not in CI (hosted
runners have none); VFIO passthrough is experimental and NVIDIA-only
(Turing and newer, with the open kernel modules); Intel and Apple GPUs are
used only through WebGPU. MicroVM networks need QEMU (not Firecracker alone or
Virtualization.framework) and forward TCP and DNS only. MicroVMs need KVM (Linux), Virtualization.framework
(macOS 11+) or WHPX (Windows x86_64) for speed (QEMU without them works but
is slow, and Windows on ARM has no microVMs yet). WHPX and
Virtualization.framework guests are checked on real machines only: CI
runners have WHPX only nested, where it breaks QEMU's disks and the agent
falls back to emulation, and hosted Mac runners cannot virtualize at all.
macOS 10.15 and 11 themselves have no CI runners either.
MicroVM task output reaches `pumasi logs` only when the task ends (the
guest writes it to its output disk), Intel and Apple GPUs report no
metrics, Windows nodes report no temperature, and the 10-second metric steps of the last hour are not kept
across a control plane restart. The dashboard shows prices and alert rules
but does not change them (the CLI and `apply` do), and keeps its sign-ins
in memory (restarting `pumasi ui` signs everyone out). The remote API has
no built-in tunnel or certificate renewal (Cloudflare Tunnel, a reverse
proxy or certbot do that; there is no iroh-based proxy for the CLI yet),
its failed sign-in counts and request limits live in each `pumasi ui`
process (and the control plane itself does not limit unauthenticated
sign-in calls per peer), the audit log is never pruned, uploads
through Cloudflare stop at its plan's body limit, and `?wait=` follows a
job by polling the control plane. Drawing in Docker is checked on real
GPUs by hand; EEVEE on a Linux host runs under Xvfb and so on the CPU
unless the image brings its own GPU display (Cycles is not affected). Confidential VMs are checked in CI with a
test stand-in for the TEE (an ordinary microVM that cannot attest for
real) and against recorded SEV-SNP reports and TDX quotes; real SEV-SNP
and TDX servers are checked by hand, since hosted runners and cloud
confidential VMs cannot start them. Confidential tasks cannot use GPUs
(NVIDIA's confidential computing mode needs H100-class GPUs and is not
supported yet) or a network, run on x86-64 only, keep everything in the
VM's memory (no encrypted scratch disk yet), and are run again if the
control plane restarts or fails over while they run. The SEV-SNP check
does not yet enforce a minimum firmware (TCB) version beyond what AMD
still certifies, and TDX measurements cover MRTD, RTMR1 and RTMR2 (the
firmware's own RTMR0 is not pinned).

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
signature and tampered binary refused). `crates/cluster/tests/manage.rs`
covers selectors, drain and preemption, quotas, access tickets, `apply`
twice, dependent jobs across a control plane restart, and account job
policies, queue timeouts and capacity. The microVM test runs when `PUMASI_TEST_VM_DIR`
points at a directory prepared with `pumasi vm setup` (CI does this on Linux
with KVM, for Firecracker and for QEMU, and on Windows with QEMU in software
emulation, using a binary built with the guest init embedded through
`PUMASI_EMBED_GUEST=<absolute path>`):

```sh
cargo build --release -p pumasi-guest --target x86_64-unknown-linux-musl
cargo run -p pumasi-cli -- vm setup --vm-dir /tmp/vm --guest target/x86_64-unknown-linux-musl/release/pumasi-guest
PUMASI_TEST_VM_DIR=/tmp/vm cargo test -p pumasi-cluster --test e2e microvm
```

The dashboard's browser tests (`crates/dashboard/e2e`, Playwright) start a
control plane, two agents and `pumasi ui` from the debug build, then sign
in, submit a job, follow its output, download its outputs, drain the node
running a task, try a viewer ticket, upload and download a dataset, and
call the JSON API:

```sh
cargo build -p pumasi-cli
cd crates/dashboard/e2e && npm ci && npx playwright install chromium && npx playwright test
```

`scripts/ci/api-doc.sh` runs every `sh` example of
[docs/api.md](docs/api.md), in order, against a control plane, an agent
and `pumasi ui --api-only` from the debug build (CI runs it in the
dashboard job; it needs `curl`, `jq` and `unzip`). `crates/dashboard/tests/ui.rs`
covers `--api-only` over TLS, the failed sign-in limit and `?wait=`.

## License

Apache-2.0.
