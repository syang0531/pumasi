# External security review: brief

What the reviewers of Pumasi 1.0 get: the scope, how to build and run
it, where the risky code is, and what we already know. Read it together
with the [threat model](threat-model.md).

## The system in one paragraph

Pumasi is a compute cluster in one Rust binary. A control plane
(optionally 3 or 5 replicas over Raft) schedules jobs (WASM modules,
Docker containers, microVMs, confidential SEV-SNP/TDX VMs) onto agents
running on the organization's machines and on invited providers' machines.
Everything talks over iroh (QUIC with endpoint keys). People manage it
with the CLI, a web dashboard (`pumasi ui`) and a JSON API that may be
exposed to the internet through a tunnel; they sign in with tickets or
SSO (OIDC, LDAP). Architecture: [architecture.md](architecture.md).

## Scope

In priority order:

1. **The internet-facing API and dashboard** (`crates/dashboard`): sign-in
   (tickets, SSO browser flow, LDAP form), sessions and CSRF, request and
   body limits, `/api/v1/*` and `/api/v1/op`, uploads and downloads.
2. **Authorization on the control plane** (`crates/cluster/src/access.rs`,
   `server.rs: admin_call`, `handle_sign_in`): can a submitter or viewer
   ticket do more than its role and account policy allow? Can anything
   be done without a secret besides the sign-in calls?
3. **SSO** (`crates/cluster/src/sso.rs`, `crates/proto/src/sso.rs`): OIDC
   code and device flows (PKCE, state, nonce, ID token validation, group
   mapping), LDAP (filter escaping, binds, lockout), session tickets.
4. **Audit log** (`crates/proto/src/audit.rs`, `crates/cluster/src/audit.rs`):
   is the chain tamper-evident as claimed, is anything important not
   logged, can it be flooded?
5. **Parsers of untrusted input** (all fuzzed in `fuzz/`): the wire
   protocol (`crates/proto/src/msg.rs`, `wire.rs`), tickets, job files,
   guest output tars (`vmdisk.rs`), sealed streams
   (`crates/guest/src/seal.rs`), attestation (`crates/cluster/src/attest.rs`),
   receipts, image layers (`crates/guest/src/init.rs`).
6. **Confidential tier** (`crates/cluster/src/confidential.rs`,
   `attest.rs`, `crates/guest`): key release only after attestation of
   that run with a trusted measurement.
7. **Job isolation** on agents (`crates/agent`): WASM limits, microVM
   setup, guest networking filter.

Out of scope: denial of service by volume against the hosting network,
iroh and wasmtime themselves (report upstream), and the Madang hub
(separate repository, later phases).

## Build and run

```sh
cargo build --release -p pumasi-cli          # current stable Rust
export PUMASI_HOME=$(mktemp -d)
target/release/pumasi cluster start --net local --name review &
target/release/pumasi --home "$PUMASI_HOME/node" agent join "$(target/release/pumasi token)" --net local &
target/release/pumasi ui --api-only --listen 127.0.0.1:7400 --no-open &
target/release/pumasi access create tester --role submitter --account lab
```

[api.md](api.md) walks through the API with curl. `cargo test
--workspace` runs the tests; `crates/cluster/tests/security.rs` has a
mock OIDC provider and a mock LDAP server that are easy to repurpose.
Fuzzing: `cd fuzz && cargo +nightly fuzz run <target>` (`cargo fuzz list`).
For a test IdP, Keycloak in Docker works with [operations.md](operations.md#2-sign-people-in).

## What we already know

The residual risks in the threat model, in particular: no per-peer limit
on unauthenticated sign-in calls at the control plane; limits per
`pumasi ui` process; the audit log is never pruned; the admin ticket can
roll out code to auto-upgrading nodes by design; RSA and P-256/P-384
timing advisories in verification-only code (`deny.toml`).

## What we would like back

- Findings with severity, a reproduction and a suggested fix.
- A read of `access.rs`'s role table against the list of `AdminOp`s.
- Whether the threat model misses a boundary or an attacker.
- Retest of the fixes before the 1.0 tag.

Contact and reporting: [SECURITY.md](../SECURITY.md).
