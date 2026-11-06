# Threat model

What Pumasi protects, from whom, where the trust boundaries are, and what
each one does about the threats that cross it. It covers 1.0 (phase 17)
and is the starting point of security reviews
([security-review.md](security-review.md)). The security model in the
README is the short version for users; this page is the long one for
reviewers and operators.

Each threat names the code that answers it, so a reviewer can check the
claim, and ends with what is left over. Residual risks are collected at
the end.

## What is worth protecting

| Asset | Where it lives | Why it matters |
| --- | --- | --- |
| Admin secret (admin ticket) | `~/.pumasi/cluster/admin.ticket`, the `admin_secret` meta row | Full control of the cluster, including rolling out signed binaries to every node: as sensitive as root on them |
| Cluster key (ed25519) | `cluster/secret.key`, backups | Signs receipts, agent upgrades and audit heads; identifies the control plane to nodes |
| Access tickets and SSO sessions | `access` table (secret, role, account, expiry) | A submitter ticket runs whatever its account's job policy allows |
| Join tickets and invites | `tokens` table | Let a machine join and receive tasks (and data) |
| Job inputs, outputs, logs | `jobs`/`tasks` tables, the file store, nodes running them | Customer data; confidential jobs must stay unreadable to the node |
| Cluster secrets | `secrets` table (registry passwords, ...) | Handed to tasks that ask for them |
| SSO settings | `sso` meta row (client secret, LDAP bind password) | Impersonating the cluster at the IdP, reading the directory |
| CU ledger, receipts, holds | `ledger`, `receipts`, `holds` tables | Money-like balances between accounts |
| Audit log | `audit` table, exported copies | Evidence of who did what; must not be rewritten unnoticed |
| Nodes (provider machines) | Everywhere agents run | Must not be harmed by the jobs they run |
| Availability | Control plane, `pumasi ui` | A cluster people depend on |

## Who might attack

- **Internet stranger.** Can reach `pumasi ui --api-only` (through a
  tunnel, a reverse proxy or `--tls-cert`) and, if they learn the
  control plane's endpoint id, its iroh endpoint. Has no ticket.
- **Holder of a leaked ticket.** Found a submitter, viewer or admin ticket
  in a chat log, a CI variable or a laptop backup.
- **Malicious member.** Has a legitimate submitter or viewer ticket (or an
  SSO account mapped to one) and tries to do more than the role allows:
  read other accounts' jobs, exceed quotas, escape the job policy.
- **Malicious job.** Code submitted as a job (by anyone allowed to
  submit) that attacks the node running it, or other jobs.
- **Malicious provider.** Owns a machine invited with `pumasi invite`.
  Sees whatever open and trusted tasks it runs, can return made-up
  results, can send anything over the agent protocol.
- **Compromised identity provider or directory.** Issues tokens or binds
  users it should not.
- **Network attacker.** Between any two parts (relay operators, Wi-Fi,
  a tunnel provider).
- **Supply chain.** A compromised dependency, release build or download.

Out of scope: someone with root on the control plane machine (they have
the database and the cluster key), and physical attacks on nodes beyond
what SEV-SNP and TDX defend for confidential jobs.

## Trust boundaries

```
 browser / script ──HTTPS──▶ pumasi ui ──iroh (admin ALPN)──▶ control plane ◀──iroh (Raft)──▶ replicas
                      ▲                       ▲                    │  ▲
         tunnel/proxy │          CLI ─────────┘           agent ALPN│  │ blobs ALPN
                                                                    ▼  │
                         IdP / LDAP ◀──HTTPS/LDAPS── control plane   agents ──▶ job sandboxes
```

1. **Internet → `pumasi ui`.** HTTP. Everything before authentication is
   attacker-controlled.
2. **`pumasi ui` → control plane.** iroh QUIC, admin ALPN. `pumasi ui`
   holds no privilege of its own: it forwards each caller's ticket secret,
   and the control plane decides. Sign-in calls (`SsoStart`,
   `SsoFinish`, `SsoDevice*`, `LdapLogin`, `SsoInfo`) are the only calls
   accepted without a secret.
3. **Anyone → control plane admin ALPN.** iroh authenticates the peer's
   endpoint key (not who they are); the request's secret decides the role
   (`server.rs: admin_call`, `access.rs: admit/scope`).
4. **Control plane ↔ agents.** Agent ALPN; a node is admitted once with a
   join ticket or invite and then known by its endpoint key.
5. **Agent → job.** WASM sandbox, container, microVM, or confidential VM.
6. **Control plane → IdP/LDAP.** HTTPS (rustls with the Mozilla roots) or
   LDAPS/StartTLS.
7. **Control plane ↔ replicas.** Raft over the Raft ALPN, between
   endpoints listed in the cluster's membership.

## The API and tunnel exposed to the internet

The scenario the 1.0 review cares most about: `pumasi ui --api-only`
behind Cloudflare Tunnel, a reverse proxy, or its own `--tls-cert`,
reachable by anyone, with scripts elsewhere holding submitter tickets
([api.md](api.md)).

### Leaked tickets

| Threat | What answers it | Left over |
| --- | --- | --- |
| A submitter or viewer ticket leaks | Tickets carry an expiry (`--ttl`, 30 days by default) and the role and account they were made for. `pumasi access revoke ID\|NAME` stops it at once: every request is checked against the `access` table, and `pumasi ui` keeps no session of its own for bearer tickets. The audit log records every call with the ticket's name and the iroh peer that made it (`pumasi audit ls`), so what the ticket did can be traced. | Until revoked, the holder can do what the role and the account's job policy allow. Narrow policies (pinned images, no network, short `--max-timeout`) limit the damage. |
| The admin ticket leaks | `pumasi admin rotate` replaces the admin secret: every copy of the old admin ticket stops working, access tickets and SSO sessions keep working, and the rotation itself is audited (`admin.rotate`). | The holder could already have rolled out a binary to the nodes (`upgrade.rollout` is audited) or read confidential jobs. Treat it as a compromise of every node that auto-upgrades: see [operations.md](operations.md#a-leaked-admin-ticket). |
| A browser session is stolen | Dashboard sessions are `HttpOnly`, `SameSite=Strict` cookies holding a random session id kept in the `pumasi ui` process, `Secure` when the dashboard is served over HTTPS or its `--public-url` is HTTPS (added in phase 17); SSO sessions are access tickets with a length (12 h by default, at most 30 days; `pumasi sso session`) and revocable like any other (`pumasi access revoke <name>` revokes every session of that person). | Restarting `pumasi ui` signs everyone out of the browser, but the underlying session ticket stays valid until it expires or is revoked. |
| A ticket is guessed | Secrets are 32 random bytes (`new_secret`); comparisons are constant-time (`secrets_eq`). | None. |
| An SSO sign-in is replayed or forged | OIDC: authorization code flow with PKCE (S256), `state` bound to a short-lived cookie on the browser that started it (`SameSite=Lax`, `Path=/sso`), `nonce` checked in the ID token, ID tokens verified against the issuer's JWKS with asymmetric algorithms only (no `none`, no HMAC), `iss`, `aud`, `azp` and `exp` checked, and email rules only for `email_verified` emails. Pending logins expire after 10 minutes and are used once. The redirect URI must be HTTPS (or loopback) and end in `/sso/callback`. Device flow (RFC 8628) for the CLI, with the device code held by the control plane, never the client. LDAP: user names are escaped in filters (RFC 4515), empty passwords are refused (no unauthenticated bind), and the bind is LDAPS or StartTLS (plain LDAP only to loopback). | Whoever controls the IdP or directory controls who signs in. Rules mapping `anyone` or a whole `domain:` give access to everyone the IdP vouches for. |

### Policy bypasses

| Threat | What answers it | Left over |
| --- | --- | --- |
| A submitter calls an operation its role does not allow | Every admin operation goes through `access::admit` on the control plane, which lists per role what is allowed (an allow list: new operations are refused for submitters and viewers until they are added). Responses go through `access::scope`, which removes other accounts' jobs, nodes' details and the like. The HTTP API has no checks of its own to get wrong: `/api/v1/op` and every route forward the caller's secret. Refusals are audited (`call.<Op>`, `audit.read`, `sso.read`). | A mistake in `admit`'s table (`crates/cluster/src/access.rs`), which `crates/cluster/tests/manage.rs` and `tests/security.rs` exercise per role for the main operations but not exhaustively; the review should read it line by line. |
| A submitter's job escapes the account's policy | Job policies (`pumasi quota ACCOUNT --images ... --executors ... --network deny ...`) are checked on the control plane at submission, whoever submits (including admins), and refusals answer `403` with the reasons. Images are resolved to digests and recorded with the job. | A policy that allows Docker lets jobs run containers sharing the host kernel. |
| A submitter reads or cancels another account's job | `admit` checks the job's account; `scope` filters lists. | None known. |
| Path traversal in job files, outputs or the file store | `validate_rel_path` (no absolute paths, `..`, `\` or `:`) on every file name from a job or a guest; guest output tars are parsed in memory with caps (`vmdisk::parse_output`). Both are fuzzed. A job file uploaded through the dashboard may only name files it was uploaded with (`JobFile::load_split_confined`): `module`, `stdin`, `files` and the tasks' own. | None known. Until the 1.0 review, an uploaded job file could name any file on the dashboard's machine (see "Internal review for 1.0"). |
| Cross-site request forgery on the dashboard | Session cookie `SameSite=Strict`; requests that change something must repeat the session's CSRF token in `X-Pumasi-CSRF`; the sign-in forms check the `Origin` header against the dashboard's host (`same_origin`); the API takes bearer tickets only (no cookies). | None known. |

### Request floods

| Threat | What answers it | Left over |
| --- | --- | --- |
| Password and ticket guessing | Wrong tickets and failed sign-ins are limited per client address in `pumasi ui` (10 attempts, then 10 a minute; `--real-ip-header` behind a proxy, of which only the last address of a list counts, the one the proxy added). LDAP: 5 wrong passwords per user name (whatever its capitalization) in 5 minutes, then that user is refused without asking the directory; at most 4 directory binds at once. | Limits live in each `pumasi ui` process; an attacker with many addresses gets more attempts (each still has to guess 32 random bytes). LDAP per-user lockout lets a stranger lock a known user out for 5 minutes. |
| Floods of valid requests from one ticket | A token bucket per ticket in `pumasi ui` (120 requests at once, then 20 per second), answered with `429` and `Retry-After`; at most 8 `?wait=` long polls per ticket at a time. | A leaked ticket can still use its share. |
| Huge request bodies | Body limits per route: 1 MiB by default (including the unauthenticated sign-in forms), 32 MiB for `/api/v1/op`, and `--max-upload` (4 GiB by default) only on job submission and file store uploads, which require a valid ticket. | Before phase 17 the upload limit applied to every route, including `/login`: fixed. |
| Floods of sign-in calls straight at the control plane | Sign-in calls take no secret, so anyone who knows the endpoint id can send them. Pending OIDC logins are capped (1000, 10 minutes each), device flows too, LDAP binds are limited as above, and unknown secrets are refused before anything is stored or audited. | No per-peer rate limit on the control plane itself (iroh endpoint ids are free to make); a stranger can keep the pending-login table full, which blocks new SSO sign-ins until entries expire. Keep the control plane's endpoint id to yourself (it is in tickets) and put `pumasi ui` in front of it. |
| TLS handshake and connection floods | Behind Cloudflare Tunnel or a reverse proxy, the tunnel or proxy absorbs them. | No connection limit or handshake timeout of its own inside `pumasi ui`; with `--tls-cert` directly on the internet, put a proxy or firewall rate limit in front. |
| Audit log growth | Unknown secrets are not audited, so a stranger cannot write to the log. Failed LDAP passwords are audited only for users that exist, within the per-user limit. | A member with a valid ticket can add refused calls to the log; the log is kept forever (no pruning yet). |
| Unbounded state from jobs | Finished jobs are deleted after `--keep-jobs` (30 days by default) with their logs and modules nothing else uses. Before phase 17 every job stayed in memory forever: fixed. | Quotas (`--slots`, `--cu-per-day`, `--storage`) limit how fast one account can fill the cluster. |

## The rest of the system (STRIDE)

### Control plane

- **Spoofing.** Nodes are identified by their iroh endpoint key after
  joining with a single-use, expiring ticket; replicas by keys in the Raft
  membership. The CLI and dashboard authenticate with a secret per call.
- **Tampering.** The audit log is a BLAKE3 hash chain
  (`pumasi_proto::audit`): each entry commits to the one before, and
  `pumasi audit verify` checks the whole chain and returns its head signed
  with the cluster key. Keep heads outside the cluster (`audit verify
  --json > head.json`, then `--against head.json` later): a log rewritten
  by someone with the database no longer reaches the saved head. Exports
  to syslog put a copy beyond the cluster's reach.
- **Repudiation.** Every mutating call, refusal, node join, sign-in,
  secret access, rotation and job expiry is audited with the ticket's
  name, role and the iroh peer.
- **Information disclosure.** `scope` filters responses per role; SSO
  settings are shown without secrets (`sso status`); secrets are shown
  only when set. The control plane holds confidential jobs' keys, so it,
  its replicas and backups can read them.
- **Denial of service.** See request floods above; agents and replicas
  are known peers.
- **Elevation of privilege.** `admit` and `scope`; SSO sessions get the
  most powerful role of the rules that match, and only rules the admin
  wrote.

### Agents and jobs

- WASM tasks: wasmtime with fuel and memory limits, WASI with only
  `/input` and `/output`, no network.
- Docker tasks: only with `--allow-docker`, sharing the host kernel.
- MicroVM tasks: their own kernel behind KVM, WHPX or
  Virtualization.framework, own disks, no network device unless asked,
  and then only to public addresses. Guest output is parsed with caps.
- Confidential tasks: sealed inputs and outputs (ChaCha20-Poly1305 with
  keys released only after SEV-SNP or TDX attestation of that run with a
  trusted measurement). The sealed stream format and the attestation
  parsers are fuzzed.
- Agent and admin protocol messages are postcard frames of at most
  64 MiB (`wire::MAX_FRAME`); decoding is fuzzed (`fuzz/fuzz_targets/agent_wire.rs`).
- Upgrades are signed by the cluster key and checked against the signed
  SHA-256 before running.

### Providers

A provider sees open and trusted tasks it runs; checks re-run a sample
(10% by default) on trusted machines and claw back held earnings on a
mismatch; three strikes suspend it. Providers never settle a check.

### Supply chain

- `cargo deny check` (advisories, licenses, sources: crates.io only) and
  `cargo audit` run on every pull request (`deny.toml`,
  `.cargo/audit.toml`; each ignored advisory says why it does not apply).
  Phase 17 found and fixed RUSTSEC-2026-0344 (a decompression bomb in
  ruzstd, which reads image layers from registries).
- Ten cargo-fuzz targets run for a minute each on every pull request
  (`fuzz/`).
- Releases ship `SHA256SUMS`, the install scripts check it, and releases
  carry GitHub build provenance once the repository is public.
- `Cargo.lock` is committed, so CI and releases build the reviewed
  versions.

## Residual risks

1. **Admin ticket = root on auto-upgrading nodes.** By design (signed
   rollouts). Rotate on any suspicion; consider `--no-auto-upgrade` on
   machines you do not want the admin to reach.
2. **The control plane can read confidential jobs.** It holds the keys.
3. **No per-peer limit on unauthenticated sign-in calls at the control
   plane**, so a stranger who knows the endpoint id can fill the pending
   SSO table (blocking new SSO sign-ins for up to 10 minutes at a time) or
   lock known LDAP users out for 5 minutes.
4. **Rate limits are per `pumasi ui` process** and reset when it restarts.
5. **The audit log is kept forever** and anyone with a valid ticket can
   add refused calls to it. Pruning behind a signed checkpoint is future
   work.
6. **Docker jobs share the host kernel**; only allow them for accounts you
   trust.
7. **Results from providers are checked statistically**, not proven.
8. **The SEV-SNP check does not enforce a minimum TCB**, and TDX does not
   pin RTMR0 (see the README's known gaps).
9. **RSA and P-256/P-384 verification use crates with timing advisories**
   (RUSTSEC-2023-0071, RUSTSEC-2026-0387) that concern private keys and
   secret integers; Pumasi only verifies signatures with public keys.
10. **A `group:NAME` rule also matches LDAP groups by their first `cn`**,
    wherever they are in the directory: `group:admins` matches
    `cn=admins,ou=contractors,...` as well. Where people other than the
    directory's admins can create groups, map roles (admin above all) to
    a group's full DN.
11. **Device sign-in can be phished.** As with any RFC 8628 flow, someone
    can start `pumasi login --sso` and talk a member into entering the
    code at the IdP; the session is then theirs, with the member's role.
    The IdP's consent screen names the application, every sign-in is
    audited with the IdP's name for the person, and an IdP that does not
    need the device flow can turn the device grant off for Pumasi's client.

## Internal review for 1.0

Done on 2026-11-06 over the scope of [security-review.md](security-review.md),
in its priority order: the dashboard and API, authorization on the
control plane (`access.rs` read against every `AdminOp`), SSO, the audit
log, the parsers of untrusted input, the confidential tier and job
isolation on agents. By the maintainers with AI assistance, so not
independent. Every finding below is fixed, with a test, in the pull
request that added this section; what was not fixed is in the residual risks
above (10 and 11 were added by this review).

| Severity | Finding | Fix |
| --- | --- | --- |
| High | **Files on the dashboard's machine could be read through a job upload.** The dashboard loaded an uploaded `job.toml` the way `pumasi run` loads one the user wrote, so `stdin`, `module` or a task's `stdin` could be an absolute path or climb out with `..`. A submitter (any ticket that may submit, over the internet-facing API too) could set `stdin = "/home/admin/.pumasi/…/admin.ticket"` and get the dashboard's own admin ticket back in the job's output. (`files` already had to stay inside.) | `JobFile::load_split_confined` refuses every local path that is absolute, has `..` or a drive or UNC prefix; the dashboard uses it for both the pages and `/api/v1/jobs`. `pumasi run` is unchanged. |
| Medium | **Every admin call formatted its whole request with `Debug`**, to name it in case it was refused. For a release chunk (`UploadRelease`, up to the 64 MiB frame) that is a string several times its size, built on every call, including by a viewer whose call is then refused: a cheap way to exhaust the control plane's memory, and wasted work on every legitimate rollout. | The call's name is taken from the start of the `Debug` output and formatting stops there. |
| Low | **`--real-ip-header X-Forwarded-For` counted clients under the first address in the list**, which the client writes itself. Anyone could pick the address their failed sign-ins counted against: unlimited guesses, or getting someone else's address blocked. | The last address counts, the one the proxy in front added. `CF-Connecting-IP` was not affected. |
| Low | **Open redirect in the language switch** (`/lang/en?next=`): browsers drop tabs and newlines from URLs, so `next=/%09/evil.example` sent the browser to another site. | `next` must be a path of visible ASCII. |
| Low | **The LDAP lockout counted user names as typed**, so `bob`, `Bob` and `BOB` each got five tries, while directories match names without case. | The count is per lowercased name. |
| Low | **Dashboard sessions were not bounded**: anyone with a valid ticket could keep signing in and grow the session table until sessions went idle (12 hours). | At most 10,000 sessions; the least recently used goes first. |
| Low | **Uploads and output zips went to world-readable temporary directories** (`/tmp/pumasi-ui-*`, mode 0755), so other users of the dashboard's machine could read jobs' inputs and outputs while they were there. | Created with mode 0700, and never reusing a directory that already exists. |
| Low | **Answers from the IdP were read whole before their size was checked.** Only a malicious or broken IdP could send one, but it could exhaust the control plane's memory. | Reading stops at 1 MiB. |

Read and found sound: the role table in `access.rs` (an allow list; every
submitter call on a job, dataset or schedule is checked against its
account, and replies go through `scope`); the sign-in calls are the only
ones that skip the secret check (`AdminOp::is_sign_in`), and unknown
secrets are refused before anything is audited; OIDC state, nonce, PKCE,
issuer, audience, `azp`, algorithm and key handling; LDAP filter
escaping and the refusal of empty passwords; the audit chain and its
signed head; the syslog export (each entry is JSON, so newlines in names
cannot forge entries); frame limits on the wire; SEV-SNP and TDX checks
(report data bound to the run's nonce and the guest's key, debug policies
refused, VCEK firmware versions matched to the report); key release once
per lease, to the node the run was placed on; the microVM network filter
(public unicast IPv4 only, not the host's own addresses).

## Changes made while writing this

- `pumasi ui` applied the upload limit (4 GiB) to every route, including
  the unauthenticated sign-in form; now 1 MiB by default.
- Per-ticket request limits and `?wait=` slots in `pumasi ui`.
- The dashboard's session cookie gets `Secure` over HTTPS.
- Finished jobs are deleted after `--keep-jobs`.
- ruzstd 0.8 → 0.9.1 (RUSTSEC-2026-0344).
- `SsoInfo` no longer contacts the IdP, so a slow or unreachable IdP
  cannot stall the sign-in page.
