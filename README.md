# Pumasi

Pool the machines you already own into one private cluster and run
WebAssembly batch jobs across them. One small Rust binary, the same on
Linux, macOS and Windows, x86_64 and ARM64.

> [한국어 README](README.ko.md)

Pumasi is phase 1 of a larger plan: first a private cluster tool that is
useful on its own, later invited outside providers, metered billing and a
marketplace that bursts into other people's idle machines with the same job
format. See [docs/architecture.md](docs/architecture.md) and the roadmap
below.

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

## Commands

| Command | What it does |
| --- | --- |
| `pumasi cluster start [--name N] [--net public\|local] [--bind ADDR]` | Run the control plane. Creates the cluster on first run, keeps state in `~/.pumasi/cluster`. |
| `pumasi token [--ttl 24h] [--uses N]` | Create a join ticket for another machine. |
| `pumasi agent join <TICKET> [--name N] [--slots N]` | Join a cluster and start running tasks. |
| `pumasi agent start` | Reconnect to the cluster this machine joined before (no ticket needed). |
| `pumasi run job.toml [--output] [--detach]` | Submit a job and wait for it. Exit code 0 on success, 2 if any task failed. |
| `pumasi status` | Nodes (online, slots, running) and recent jobs. |
| `pumasi job <ID> [--output]` | Per-task state, node, attempts, time; stdout/stderr with `--output`. |
| `pumasi cancel <ID>` | Cancel queued and running tasks of a job. |
| `pumasi remove-node <ID>` | Revoke a node. It needs a new ticket to come back. |
| `pumasi login <ADMIN_TICKET>` | Manage a remote cluster from this machine. |
| `pumasi exec module.wasm [ARGS...]` | Run a module locally in the agent's sandbox (stdin is passed through). |

`--home DIR` or `PUMASI_HOME` changes the state directory, which lets you run
a cluster and several agents on one machine for testing. `PUMASI_LOG=debug`
turns on verbose logs.

## Job file

```toml
name = "estimate-pi"                  # optional, defaults to the module name
module = "montecarlo.wasm"            # a wasm32-wasip1 command module
shards = 16                           # N tasks; {shard} and {shards} are substituted
args = ["sample", "{shard}", "5000000"]
env = { MODE = "fast" }               # optional
stdin = "input.txt"                   # optional, fed to every task
max_attempts = 3                      # placements before giving up on lost nodes

[resources]
memory_mb = 32                        # linear memory cap per task (default 256)
timeout = "2m"                        # wall-clock cap per task (default 5m)
fuel = 10_000_000_000                 # instruction budget per task (default unlimited)
```

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

Any language that targets `wasm32-wasip1` works (Rust, C/C++, Zig, Go,
TinyGo, ...). Tasks read stdin and write stdout; they have no filesystem and
no network.

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
  slots. State (members, jobs, task results, modules) is persisted in redb, so
  a restart resumes queued and interrupted work.
- **Agent** (`crates/agent`): connects out to the control plane, so it works
  from behind NAT. It runs tasks in Wasmtime with no filesystem or network,
  caps memory, fuel and wall-clock time, and reports stdout, stderr, exit
  status, fuel used and wall time.
- **Protocol** (`crates/proto`): postcard frames over iroh QUIC streams.
  Every node is identified by its ed25519 key; nobody can impersonate it.
- **Failure handling**: an agent that disconnects or misses heartbeats loses
  its tasks to other nodes (up to `max_attempts`). Agents reconnect with
  backoff. Task-level failures (trap, non-zero exit, out of fuel, timeout)
  are reported, not retried, because WASM execution is deterministic.

## Security model (phase 1)

- Join tickets are bearer secrets with an expiry and optional use count.
  Once a node joins, its public key is remembered; the ticket is not needed
  again. `remove-node` revokes it.
- The admin ticket in `~/.pumasi/cluster/admin.ticket` grants full control.
  Treat it like a password.
- Tasks are sandboxed from the machine running them (no files, no network,
  memory and CPU caps). They are **not** hidden from that machine's owner:
  phase 1 has only the `open` confidentiality tier. Do not send secrets to
  machines you do not trust.

## Roadmap

| Phase | Scope | Status |
| --- | --- | --- |
| 1 | Private cluster: agent, control plane, iroh networking, WASM executor, CLI | this release |
| 2 | Invited outside providers (open tier), usage receipts signed by node and cluster, fixed price list, prepaid ledger | next |
| 3 | microVM executor (OCI images, Linux GPU passthrough) behind the same job format; WebGPU experiments | planned |
| 4 | Confidential tier on SEV-SNP / TDX servers with remote attestation | planned |

Known gaps in phase 1: single control plane (no Raft yet), no web console,
no output files beyond stdout, no per-user accounts.

## Development

```sh
cargo test --workspace     # unit tests + end-to-end tests over real iroh connections
cargo clippy --workspace --all-targets -- -D warnings
```

The end-to-end tests in `crates/cluster/tests/e2e.rs` start a control plane
and agents on localhost and cover sharded jobs, node failure and rescheduling,
ticket rejection and reuse, reconnect without a ticket, cancellation, fuel
limits, bad admin credentials, and restarting the control plane.

## License

Apache-2.0.
