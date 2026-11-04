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
usage receipts, a prepaid credit ledger and spot checks of their results
(phase 2), then a microVM executor that runs container images on those
machines with VM isolation (phase 3, this release), and later a marketplace
that bursts into idle machines with the same job format. See
[docs/architecture.md](docs/architecture.md) and the roadmap below.

## Quick start

```sh
cargo build --release -p pumasi-cli          # produces target/release/pumasi

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
compatible CLI must be installed there):

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
on Linux with KVM, or QEMU) with its own Linux kernel and no network. The
job file only adds `executor = "microvm"`:

```sh
# Once per machine (Linux x86_64/ARM64 with KVM; QEMU also works on macOS).
cargo build --release -p pumasi-guest --target x86_64-unknown-linux-musl
pumasi vm setup --guest target/x86_64-unknown-linux-musl/release/pumasi-guest
pumasi vm run python:3.12-slim -- python -c 'import platform; print(platform.release())'
# 6.1.102   <- the guest kernel, not the host's

pumasi agent join pumasijoin... --allow-microvm
pumasi run examples/python-wordcount/job-microvm.toml --output
```

`pumasi vm setup` downloads a pinned guest kernel (and Firecracker on Linux)
and checks their SHA-256; `pumasi vm status` shows what is installed and
which VM monitor will be used. The agent pulls images itself (no Docker
needed) and unpacks them inside a VM, so untrusted image layers never touch
the host's filesystem.

### Other people's machines: invites, credit and checks

A friend can lend their PC to your cluster. Invite it for their account:

```sh
pumasi invite bob                  # prints `pumasi agent join pumasijoin...` for bob to run
pumasi credit add owner 100        # prepaid credit for runs on other people's machines
pumasi run examples/montecarlo/job.toml
```

- Bob's machine joins as a **provider**: it runs WASM tasks, and microVM
  tasks if bob started his agent with `--allow-microvm` (Docker only if you
  invited it with `--allow-docker` *and* bob started his agent with
  `--allow-docker`), and only for jobs whose account has credit.
- Your own machines are used first and cost nothing; work overflows to
  providers. Each provider run is priced from the fixed price list
  (`pumasi prices`) and charged to the job's account. The provider's
  earning is **held** (default 1 hour), then becomes available.
- Every run produces a **usage receipt** (fuel, wall time, memory,
  executor, a hash of the result) signed by the node and countersigned by
  the control plane. `pumasi receipts <JOB> --export r.json` exports them;
  `pumasi verify-receipts r.json` checks the signatures offline.
- WASM jobs run in **deterministic mode** by default (virtual clock, seeded
  random numbers), so every honest node computes the same result. A sample
  of provider-run tasks (10%; 50% for new providers; all of them after a
  failed check) is **re-run on another node**, preferably one of yours. A
  provider whose result disagrees loses that earning (you are refunded), gets
  a strike, and after 3 strikes gets no more tasks. If a check run's result
  wins, it replaces the provider's.

```
$ pumasi job a21f288f39011a2d
job a21f288f39011a2d "estimate-pi": succeeded
  executor wasm
  account owner  cost 0.0375 credits  deterministic
  tasks 16  succeeded 16  failed 0  fuel 6000109645
  [0] ok node=a8020286f9 attempts=1 46ms
  [1] ok node=46e7353324 attempts=1 66ms cost=0.00375
  [2] ok node=46e7353324 attempts=1 42ms cost=0.00375 check=passed
...
$ pumasi credit list
ACCOUNT               AVAILABLE           HELD          SPENT         EARNED
bob                      0.0375              0              0         0.0375
owner                   99.9625              0         0.0375              0
```

Credits are bookkeeping only: no money moves. `pumasi credit payout bob 10`
records that you settled with bob outside Pumasi.

## Commands

| Command | What it does |
| --- | --- |
| `pumasi cluster start [--name N] [--net public\|local] [--bind ADDR]` | Run the control plane. Creates the cluster on first run, keeps state in `~/.pumasi/cluster`. |
| `pumasi token [--ttl 24h] [--uses N]` | Create a join ticket for another machine of yours. |
| `pumasi invite <ACCOUNT> [--ttl 7d] [--uses 1] [--allow-docker]` | Invite someone else's machine as a provider earning for `ACCOUNT`. |
| `pumasi agent join <TICKET> [--name N] [--slots N] [--allow-docker] [--allow-microvm]` | Join a cluster and start running tasks. `--allow-docker` also accepts docker jobs, `--allow-microvm` microvm jobs (`--vm-backend firecracker\|qemu`, `--vm-dir DIR`). |
| `pumasi agent start [--allow-docker] [--allow-microvm]` | Reconnect to the cluster this machine joined before (no ticket needed). |
| `pumasi vm setup [--guest FILE] [--force]` | Install the guest kernel, Firecracker (Linux) and the guest init into `~/.pumasi/vm`. |
| `pumasi vm status` | Show what is installed for microVMs and which VM monitor will be used. |
| `pumasi vm run IMAGE [--input DIR] [-e K=V] [--memory-mb N] [--cpus N] [--timeout T] [-- CMD...]` | Run an image in a microVM on this machine, the way an agent would (`/output` is saved to `pumasi-out/vm`). |
| `pumasi run job.toml [--account A] [--output] [--out DIR] [--detach]` | Submit a job and wait for it. Output files are saved to `DIR` (default `pumasi-out/<job id>`). Exit code 0 on success, 2 if any task failed. `--account` picks who pays for provider runs (default `owner`). |
| `pumasi status` | Nodes (role, online, executors, slots, running, checks) and recent jobs with their cost. |
| `pumasi job <ID> [--output] [--out DIR]` | Per-task state, node, attempts, time, cost and check result; stdout/stderr with `--output`; downloads output files with `--out`. |
| `pumasi credit list \| add <ACCOUNT> <AMOUNT> \| payout <ACCOUNT> <AMOUNT> \| ledger [--account A]` | Balances (available, held, spent, earned), prepaid deposits, payouts, and the ledger. |
| `pumasi prices [set --wasm-per-gfuel X --docker-cpu-hour X --microvm-cpu-hour X --memory-gib-hour X --hold 1h --verify-rate 10% --max-strikes 3]` | Show or change the price list and settlement policy. |
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
  caps the wall time at what it observed itself, prices it, countersigns it
  and stores it next to the ledger entries it caused, in one transaction.

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

## Roadmap

| Phase | Scope | Status |
| --- | --- | --- |
| 1 | Private cluster: agent, control plane, iroh networking, WASM and Docker executors, file input/output, CLI | done |
| 2 | Invited outside providers (open tier), usage receipts signed by node and cluster, fixed price list, prepaid credit ledger, sampled re-runs to check results | done |
| 3 | microVM executor: same OCI images as docker jobs in Firecracker or QEMU VMs, behind the same job format, open to providers | this release (GPU passthrough and WebGPU experiments not yet) |
| 4 | Confidential tier on SEV-SNP / TDX servers with remote attestation | planned |

Known gaps: single control plane (no Raft yet), no web console, accounts
exist only inside one cluster and only the admin can submit (with
`--account`), credits are not connected to real payments, input and output
files are size-capped (48 MiB in, 32 MiB out per task) because they travel
through the control plane. MicroVMs pull only public images, have no
network and no GPUs yet, and need Linux with KVM for speed (QEMU without KVM
works but is slow; the macOS path through QEMU is untested).

## Development

```sh
cargo test --workspace     # unit tests + end-to-end tests over real iroh connections
cargo clippy --workspace --all-targets -- -D warnings
```

The end-to-end tests in `crates/cluster/tests/e2e.rs` start a control plane
and agents on localhost and cover sharded jobs, node failure and rescheduling,
ticket rejection and reuse, reconnect without a ticket, cancellation, fuel
limits, bad admin credentials, restarting the control plane, docker jobs
with input/output files (skipped when no Linux Docker daemon is available),
an invited provider paid from prepaid credit with verifiable receipts, a
dishonest provider caught by check runs, refunded and suspended, and microVM
jobs on an invited provider. The microVM test runs when `PUMASI_TEST_VM_DIR`
points at a directory prepared with `pumasi vm setup` (CI does this on Linux
with KVM, for Firecracker and for QEMU):

```sh
cargo build --release -p pumasi-guest --target x86_64-unknown-linux-musl
cargo run -p pumasi-cli -- vm setup --vm-dir /tmp/vm --guest target/x86_64-unknown-linux-musl/release/pumasi-guest
PUMASI_TEST_VM_DIR=/tmp/vm cargo test -p pumasi-cluster --test e2e microvm
```

## License

Apache-2.0.
