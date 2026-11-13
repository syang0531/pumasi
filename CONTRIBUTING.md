# Contributing

Thank you for trying Pumasi. The most useful thing you can send is a
report from real use: what machines you joined, what you ran and where it
went wrong or felt awkward. Most of the 1.1 to 1.5 releases came from
reports like that.

## Reports and questions

- **Bugs and requests**: open an [issue](https://github.com/syang0531/pumasi/issues/new/choose).
  Include `pumasi --version`, the OS of the machine involved and the
  command's output. Remove tickets first (`pumasijoin...`,
  `pumasiadmin...`): a ticket is a credential.
- **Questions** ("can it do this?", "is this setup sensible?") are fine as
  issues too.
- **Security vulnerabilities**: privately, as [SECURITY.md](SECURITY.md)
  describes, not in a public issue.

## Pull requests

Small fixes (a wrong sentence, a failing case with its fix) can come
straight as a pull request. For anything larger, open an issue first so
the scope is agreed before you spend time on it; the
[roadmap](docs/roadmap.md) shows what is planned and in what order.

Before you push:

```sh
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

The README's [Development](README.md#development) section explains the
end-to-end tests and the ones that need Docker, a microVM directory or a
browser; CI runs them all on Linux, macOS and Windows. A change that users
can see also changes the README (and `README.ko.md`, if you can; say so
if you cannot and a maintainer will).

Contributions are accepted under the project's license,
[Apache-2.0](LICENSE).

## Where things are

| Path | What |
| --- | --- |
| `crates/cli` | The `pumasi` command |
| `crates/cluster` | The control plane: scheduling, ledger, file store, Raft |
| `crates/agent` | The agent and the three executors |
| `crates/dashboard` | The web dashboard and the HTTP API |
| `crates/proto` | Messages, job files, tickets |
| `crates/guest` | The init that runs inside microVMs |
| `docs/architecture.md` | How the parts fit together |
