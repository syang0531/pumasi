# Operations guide

Running Pumasi for an organization: installing the control plane,
signing people in, upgrading, backing up, watching it, and what to do
when something goes wrong. The README explains the features; this page
is the checklist for whoever keeps the cluster running. The
[threat model](threat-model.md) explains why each step matters.

## 1. Install

### The control plane

One machine you trust (it holds the cluster key, the admin secret and the
keys of confidential jobs), always on, with a disk for the file store.
Three or five such machines for high availability (step 5).

```sh
curl -fsSL https://github.com/syang0531/pumasi/releases/latest/download/install.sh | sudo sh
sudo useradd --system --create-home --home-dir /var/lib/pumasi pumasi
```

Run it as a service. `pumasi service install cluster` writes and starts
one (systemd, launchd or a Windows service) that runs as the owner of
the home it is given:

```sh
sudo chown pumasi /var/lib/pumasi
sudo pumasi --home /var/lib/pumasi service install cluster -- --name acme --bind 0.0.0.0:7480 --storage-class ssd --keep-jobs 90d
sudo pumasi --home /var/lib/pumasi service status cluster
```

It adds `--no-token` (no join ticket in the log at every start; make them
with `pumasi token`). The options are kept in `control-plane.toml` in the
home; `service status cluster` prints them and `service set` changes
them and restarts the service, keeping the ones it does not name:

```sh
sudo pumasi --home /var/lib/pumasi service set cluster -- --keep-jobs 180d
```

For a hardened unit like the one below, write it yourself:

```ini
# /etc/systemd/system/pumasi-cluster.service
[Unit]
Description=Pumasi control plane
After=network-online.target
Wants=network-online.target

[Service]
User=pumasi
Environment=PUMASI_HOME=/var/lib/pumasi
ExecStart=/usr/local/bin/pumasi cluster start --name acme --bind 0.0.0.0:7480 --storage-class ssd --keep-jobs 90d
Restart=always
RestartSec=5
NoNewPrivileges=true
ProtectSystem=strict
ReadWritePaths=/var/lib/pumasi
PrivateTmp=true

[Install]
WantedBy=multi-user.target
```

```sh
sudo systemctl enable --now pumasi-cluster
sudo -u pumasi PUMASI_HOME=/var/lib/pumasi pumasi status
```

- `--bind` fixes the UDP port, which lets you open it in the firewall and
  keeps the address in tickets stable across restarts.
- `--keep-jobs` sets how long finished jobs (with their logs and small
  outputs) are kept, counted from submission: 30 days by default, `0`
  keeps them forever. Receipts, the ledger and the audit log are kept
  regardless.
- `--metrics-addr 127.0.0.1:9464` serves Prometheus metrics (README,
  "Monitoring").

The admin ticket is in `/var/lib/pumasi/cluster/admin.ticket`. Copy it to
the admins' own machines with `pumasi login <TICKET>` and then keep the
file readable by the `pumasi` user only.

### Machines that run jobs

On each of your machines, with a join ticket from `pumasi token --ttl 1h`:

```sh
curl -fsSL https://github.com/syang0531/pumasi/releases/latest/download/install.sh \
  | sudo sh -s -- --ticket pumasijoin... -- --allow-microvm
```

The install script checks the binary's SHA-256 and installs the agent as
a service (README, "Running as a service"). Other people's machines join
with `pumasi invite ACCOUNT` instead.

### The dashboard and the API

`pumasi ui` runs wherever the admin ticket is (usually the control plane
machine), as its own service:

```ini
ExecStart=/usr/local/bin/pumasi ui --listen 127.0.0.1:7400 --no-open --login-only --public-url https://pumasi.example.com --real-ip-header CF-Connecting-IP
```

Put it on the internet only through Cloudflare Tunnel, a reverse proxy
with TLS, or `--tls-cert`/`--tls-key` ([api.md](api.md), "Put it on the
internet"). Behind a proxy, pass `--real-ip-header` so limits apply per
client and not to the proxy; when the proxy runs on another machine, also
`--trusted-proxy` with its address (the header is otherwise believed only
on connections from this machine). `pumasi ui --api-only` serves only
the JSON API for scripts.

Several dashboards may serve the same cluster (one per site, or two
behind a load balancer). Since 1.12, those that run with the admin
ticket share their limits through the control plane: a client that
fails ten sign-ins across them is blocked on all of them, and a ticket's
request budget is one budget. A dashboard started with any other ticket
keeps its own limits, as before.

### Behind an HTTPS proxy

On a network that reaches the internet only through a proxy, `pumasi`
(the CLI, agents and control planes, `--net public`) sends its relay
connections through it with HTTP CONNECT (1.12):

| Variable | Meaning |
| --- | --- |
| `PUMASI_PROXY` | The proxy, `http://[user:pass@]host:port` or `https://...`. Wins over the others; `off` (or empty) goes direct even if `HTTPS_PROXY` is set. |
| `HTTPS_PROXY`, `HTTP_PROXY`, `ALL_PROXY` | Read in that order (lower case too) when `PUMASI_PROXY` is unset. A value that is not an `http(s)://` URL is skipped with a warning. |
| `PUMASI_CA_FILE` | PEM certificates to trust beside the built-in roots, for a proxy that re-signs TLS. With a proxy set, the system's roots (and `SSL_CERT_FILE`) are trusted too. |

Only relay traffic goes through the proxy: direct QUIC paths need UDP,
which such proxies do not pass, so every byte goes through the relay.
That is slower than a direct path but works; `pumasi net` shows the
path. The relay must be reachable from the proxy (its host allowed),
and a proxy that refuses WebSocket upgrades after CONNECT cannot be
used. Pass `PUMASI_PROXY=off` to a service that should ignore a
machine-wide `HTTPS_PROXY`.

## 2. Sign people in

Give each person their own access: an SSO account mapped to a role, or an
access ticket (`pumasi access create NAME --role submitter --account lab
--ttl 30d`). Never hand out the admin ticket.

### OpenID Connect (Google Workspace, Microsoft Entra ID, Keycloak, Okta)

Register a **confidential web application** at the provider with
`https://pumasi.example.com/sso/callback` as its redirect URI (the
dashboard's `--public-url` plus `/sso/callback`), and allow the device
authorization grant if people will sign in from the CLI. Then:

```sh
umask 077; printf '%s' 'CLIENT-SECRET' > /tmp/oidc-secret
pumasi sso oidc --issuer ISSUER --client-id CLIENT-ID --client-secret-file /tmp/oidc-secret --name "Acme SSO"
rm /tmp/oidc-secret
```

| Provider | `--issuer` | Groups |
| --- | --- | --- |
| Google Workspace | `https://accounts.google.com` | Google's ID tokens carry no groups: map `domain:acme.com` or `email:` rules. Google does not offer the device flow for web clients; CLI users sign in with LDAP or an access ticket. |
| Microsoft Entra ID | `https://login.microsoftonline.com/<tenant-id>/v2.0` | Add the `groups` claim to the ID token (Token configuration → Add groups claim); groups arrive as object ids, so map `group:<object-id>`. Enable "Allow public client flows" for `pumasi login --sso`. |
| Keycloak | `https://keycloak.example.com/realms/<realm>` | Add a "Group Membership" mapper to the client (claim `groups`, full group path off). Enable "OAuth 2.0 Device Authorization Grant" in the client's capabilities. Leave the client's "PKCE Method" empty: Pumasi sends PKCE on the browser flow anyway, and with the setting on Keycloak also demands it on the device flow, which refuses `pumasi login --sso`. |
| Okta | `https://<org>.okta.com/oauth2/default` | Add a `groups` claim to the authorization server. |

`--groups-claim` changes the claim's name, `--scopes` adds scopes (some
providers need `groups`).

To try SSO before touching the organization's provider,
`scripts/sso-e2e.sh --keep` starts Keycloak and OpenLDAP in Docker with
a ready realm and directory (`scripts/sso-e2e/`), signs test users in
every way, and leaves both running (Keycloak admin console on
http://127.0.0.1:18080, admin/admin). CI runs the same script on every
pull request.

### LDAP and Active Directory

```sh
pumasi sso ldap --url ldaps://dc1.acme.local:636 \
  --bind-dn 'CN=pumasi,OU=Service,DC=acme,DC=local' --bind-password-file /root/ldap-password \
  --user-base 'OU=People,DC=acme,DC=local' --user-filter '(sAMAccountName={user})'
```

OpenLDAP: `--user-filter '(uid={user})'` (the default), and
`--group-base ou=groups,dc=example,dc=com` if the directory has no
`memberOf`. A `group:` rule names an LDAP group by its whole DN
(`group:CN=pumasi-admins,OU=Groups,DC=acme,DC=local`), or by its `cn`
alone (`group:pumasi-admins`) only when the group lies under
`--group-base`: a bare name never matches a group elsewhere in the
directory, where someone else may have made one of the same name. Use `ldaps://`, or `ldap://` with `--starttls`; plain LDAP is
accepted only to a loopback address.

### Rules and sessions

```sh
pumasi sso map group:pumasi-admins admin
pumasi sso map group:research submitter:research
pumasi sso map domain:acme.com viewer
pumasi sso session 12h
pumasi sso status
```

Someone several rules match gets the most powerful role; someone no rule
matches is refused (and the refusal is audited). Each sign-in creates an
access ticket named after the person, valid for the session length, so
`pumasi access ls` shows who is signed in and `pumasi access revoke
alice@acme.com` signs them out everywhere.

People sign in from the dashboard's sign-in page, or from a terminal:

```sh
pumasi sso ticket          # print once, share it: it holds no secret
pumasi login --sso pumasisignin...        # device flow: opens a code to confirm
pumasi login --ldap alice pumasisignin... # asks for the password
```

Sign-in from a terminal with `--sso` (the OpenID Connect device flow) is
off until you run `pumasi sso device on`. Anyone with the sign-in ticket
can start a device sign-in and send its code to a member; a member who
enters it at the provider signs that person in with their own role. Turn
it on only if people need it and know to enter only codes they asked for
themselves; the browser sign-in and `--ldap` do not have this problem.

### SCIM provisioning

With SCIM (1.12) the identity provider tells Pumasi when someone leaves
or changes groups, instead of waiting for their session to run out.
The dashboard answers SCIM 2.0 at `https://pumasi.example.com/scim/v2`
(its `--public-url` plus `/scim/v2`; also with `--api-only`):

```sh
pumasi sso scim token entra   # prints the token once
pumasi sso scim status        # tokens, people and groups the provider sent
pumasi sso scim revoke ID
```

| Provider | Where |
| --- | --- |
| Microsoft Entra ID | Enterprise application → Provisioning → Automatic: Tenant URL the address above, Secret Token the token. Map `userPrincipalName` (or `mail`) to `userName`, matching what the OIDC sign-in uses. |
| Okta | App → General → Provisioning: SCIM; base URL the address above, unique identifier `userName`, "Push New Users", "Push Profile Updates", "Push Groups", authentication "HTTP Header" with the token. |
| Keycloak, others | Keycloak has no SCIM client of its own; an extension or a script can send the same requests. `scripts/sso-e2e.sh` does it with curl. |

What it changes:

- Someone set inactive (`active: false`) or deleted loses every SSO
  ticket named after them (their `userName`, their e-mail addresses,
  and the LDAP name) and every dashboard session, at once, and cannot
  sign in again until set active.
- Someone removed from a group a `pumasi sso map group:...` rule names
  (by its display name, id or external id), or in a group that is
  deleted, has their sessions ended; their next sign-in gets the role
  the rules give them then.
- SCIM never creates access by itself: people still need a rule, and
  sign in as before. Every change is in the audit log as `scim.*` with
  the token's label.

Tokens are kept as hashes; a wrong token answers 401 and counts toward
the address's failed attempts. Filters support `eq` joined by `and`,
which is what the providers send.

## 3. Upgrade

1. Read the release notes (protocol and storage layout changes).
2. `pumasi backup` (step 4).
3. Roll out from the admin's machine: `pumasi upgrade latest --wait`
   (the control plane downloads the release and checks it against its
   SHA256SUMS), or with binaries you downloaded,
   `pumasi upgrade rollout pumasi-linux-x86_64 pumasi-macos-aarch64 pumasi-windows-x86_64.exe --wait`.
   The agents go first. Then the control plane (1.13): each replica
   installs the binary for its platform, followers restart one at a time
   with the leader's leave and only while a majority stays up, and the
   leader restarts last. Each new binary is on trial: if it does not
   become healthy within 5 minutes (or keeps crashing), the previous one
   is put back and `pumasi upgrade status` says why. The database is
   migrated in place on first open; there is no way back to an older
   binary after a migration except restoring the backup, so read the
   release notes for layout changes.
4. Dashboards restart into the new binary by themselves: one sharing
   the binary with the control plane follows it, and one alone on its
   machine installs the rollout's binary itself.
5. By hand only when the release notes say so: a newer agent protocol
   (the rollout refuses it without `--force`), or replicas from before
   1.13. Then replace the control plane's binary and restart it first,
   followers one at a time (`pumasi ha status` shows who leads), then
   the leader, before rolling out.

## 4. Back up and restore

```sh
pumasi backup /secure/pumasi-$(date +%F).backup
rsync -a /var/lib/pumasi/cluster/blobs/ backup-host:/pumasi/blobs/
```

A backup holds the cluster key, the admin secret, SSO settings and the
cluster's secrets: encrypt it and store it like the admin ticket. Back up
daily (a cron job or timer running `pumasi backup`), and keep the file
store's `blobs` directory with it, or (1.8) let storage nodes keep copies
of every dataset (below), in which case a restored control plane without
`blobs` fetches the datasets back from them. Restore on a fresh machine with
`pumasi cluster restore FILE` and then `pumasi cluster start`. Nodes,
admin and access tickets know the control plane by the cluster key, not
by its address, so in the default public network mode they find the
restored one on any machine (its relay is published under that key);
with `--net local` it needs the old IP address and `--bind` port.
Practice a restore once before you need it.

### Copies of the file store on other machines

Label one or more owner machines with spare disk for storage, each with
the most it may keep, and check where the copies are:

```sh
pumasi node label nas storage=2T          # or storage=yes: up to 90% of its disk
pumasi store copies --default 2           # control planes included (the default)
pumasi store replicas                     # copies per version; storage nodes, quotas, last scrub
```

Copies go to the storage nodes with the most room. A write is
acknowledged once it is on as many copies as there can be (the leader,
every control plane replica, and storage nodes with room, waiting up to
5 minutes for them). A storage node offline for longer than
`--replica-repair-after` (default 6h; `pumasi service set cluster --
--replica-repair-after 2h`) or removed has its copies made again on
other storage nodes. Every store is read back and checked against its
BLAKE3 hashes each `--scrub-every` (default 7d), or now with `pumasi
store scrub [NODE]`: a damaged copy on a node is dropped and fetched
again; damage in the control plane's own store raises a critical
`store.damaged` event. To repair that, stop the control plane, move its
`blobs` directory aside and start it again: it fetches every version back
from the storage nodes (`store.repaired` events; `store.missing` if no
copy is left). Taking the `storage` label off a node drops its copies once
others hold them.

### The S3 gateway

`pumasi ui` serves an S3-compatible API at `/s3` (also with `--api-only`
and `--tls-cert`). People make keys from their own tickets with `pumasi
s3 create-key` or on the Storage page; a key acts as its ticket and stops
working when the ticket is revoked, expires or is rotated. `pumasi s3
keys` with an admin ticket lists every key, and `pumasi s3 revoke-key`
revokes one (the dashboard notices within 4 minutes, or at once for a
signature of a new day or region). The gateway keeps uploads and what it
reads in its own file store under the temporary directory (removed when
it stops), so give that disk room for the largest object, and raise
`--max-upload` (4 GiB) for larger objects.

### Run only the control plane on a machine

To keep a machine as the control plane (and dashboard) but stop it from
running tasks, take its agent out; the cluster directory, the admin
ticket and the control plane's own service and firewall rule are not
touched (administrator PowerShell; `sudo` on Linux and macOS):

1. `pumasi drain NODE`: no new tasks, running ones move to other nodes.
2. `pumasi service uninstall agent`: removes the agent's service and its
   firewall rule. `<home>/agent` (the agent's key and cache) stays and
   may be deleted.
3. `pumasi remove-node NODE` (a 1.2 control plane takes the node id from
   `pumasi status`, not the name).

Upgrades from 1.3.1 on still reach that machine (the control plane
installs its own binary, see section 3).

### Move the control plane to another machine

To move a single control plane (and the dashboard) to another machine,
move its cluster directory. The cluster key goes with it, so nodes
reconnect on their own within seconds, including nodes that are off
during the move, and every admin, access and join ticket keeps working.
No node joins again and no ticket is made again. The steps on Windows
(administrator PowerShell; on Linux and macOS the same with `sudo`,
`cp -a` or `tar`, and the paths of step 1):

1. On the old machine, note what the services run with (`pumasi service
   status` prints each service's options; for a service installed before
   1.4 they are also in `sc.exe qc pumasi-cluster`, `sc.exe qc pumasi-ui`
   or the unit files) and
   take a backup for safety: `pumasi backup pumasi-before-move.backup`.
2. Stop it for good, so the two never run at once: `pumasi service
   uninstall ui`, `pumasi service uninstall cluster` (both keep the
   data), and stop anything else that serves the same address, such as
   a Cloudflare Tunnel connector (`cloudflared service uninstall`).
3. Pack the directory and set the old copy aside:
   `tar -cf pumasi-cluster.tar -C $env:USERPROFILE\.pumasi cluster`,
   then `Rename-Item $env:USERPROFILE\.pumasi\cluster cluster.moved`.
   The archive holds the cluster key and the admin secret: carry it on
   a disk or a share you control, and delete it afterwards.
4. On the new machine, with the same pumasi version:
   `tar -xf pumasi-cluster.tar -C $env:USERPROFILE\.pumasi`, then
   install the services with the options from step 1:
   `pumasi service install cluster -- ...` and
   `pumasi service install ui -- ...`. A machine that already runs an
   agent keeps it; the agent's state is in `agent`, beside `cluster`.
5. Check with `pumasi status` there: nodes come back online within a
   minute (nodes that were asleep, when they wake). Move the tunnel or
   reverse proxy to the new machine.

To go back, do the same in the other direction with the new machine's
directory (it has everything that happened since). Never start the old
copy beside the new one: two control planes with the same key split the
nodes between them and their ledgers diverge.

Replicas (step 5) are not the way to move: a replica has its own
endpoint key, so once the original machine is removed, nodes that were
off while the replica was added, and tickets made before it, cannot
reach the cluster any more.

## 5. High availability

`pumasi ha invite` on the running control plane gives a ticket for a
replica; run `pumasi cluster replica <TICKET> --bind 0.0.0.0:7480` on each
new machine (3 or 5 in total), then make new join and admin tickets so
they list every replica. Watch `pumasi ha status`. Since 1.8 a write to
the file store is acknowledged once the replicas have it too (they also
catch up every 30 seconds), and release binaries uploaded for a rollout
are copied to every replica, so a failover loses neither.

Since 1.12 followers answer the dashboard's reads (status, events and
node metrics) for admins and viewers from a copy the leader sends every
2 seconds, so a busy dashboard does not load the leader; anything older
than 6 seconds, any write, and any other caller go to the leader as
before. Rate limits shared between dashboards live in the leader's
memory, so a failover starts them afresh.

## 6. Watch it

- Alerts: `pumasi alert set --defaults` and a webhook (`pumasi webhook add
  ops https://hooks.slack.com/...`).
- Prometheus and Grafana: `--metrics-addr` and `examples/grafana`.
- `pumasi top`, `pumasi events -f`, and `pumasi net` for the round trip
  and bandwidth between nodes and (1.15) from each node to the control
  plane's file store: a slow `store` column explains slow inputs.
- Before you take a machine down on purpose, silence its alerts:
  `pumasi alert silence --on NODE --for 2h --comment "why"`. A laptop
  that comes and goes can have the offline rule off for good:
  `pumasi alert set --on laptop --node-offline off`. From 1.15 both can
  live in `cluster.toml` instead (`[alerts.nodes.laptop]` with
  `node_offline = "off"`, `[silences.NAME]` with an `until`), so the file
  under version control says why a node is quiet; `apply --dry-run` shows
  the change and `--prune` removes the silences the file no longer lists.
  Once a file sets any of `repeat`, `resolved`, `[alerts.nodes]` or
  `[alerts.labels]`, it owns all of them: a later `pumasi alert set --on`
  lasts only until the next `apply`.
- Disk: the control plane's database grows with jobs (bounded by
  `--keep-jobs`) and the audit log (kept forever unless pruned, below);
  the file store with datasets (bounded by quotas).
- Temperatures: `pumasi sensors --raw` on a machine shows what it reads
  (on Macs since 1.14, from the SMC or IOReport; Intel and AMD GPUs on
  Linux from hwmon and Windows GPUs from the driver since 1.15); a machine
  with nothing there never fires the temperature alert. The CPU package
  power on Linux comes from RAPL, which most distributions let only root
  read: an agent installed as a service runs as root, one started by hand
  as a user shows no CPU power.
- Secrets: the dashboard's Settings page (1.15) lists them with when each
  was set; rotate a registry password there (Rotate, new value) or with
  `pumasi secret set`. Values are never shown again, so keep the source of
  truth in your password manager.

### The audit log

Every change, refusal, sign-in and node join is in the audit log:

```sh
pumasi audit ls --limit 20
pumasi audit ls --follow --json
```

Send it to your SIEM as it happens, from a service that keeps its place:

```sh
pumasi audit export --syslog tcp://siem.acme.local:601 --state /var/lib/pumasi/audit.state --follow
```

(`udp://host:514` for plain syslog; messages are RFC 5424, facility
`authpriv`, one JSON entry each.) Then save a signed head somewhere the
cluster's admins cannot change, for example daily:

```sh
pumasi audit verify --json > /secure/audit-head-$(date +%F).json
pumasi audit verify --against /secure/audit-head-2026-11-01.json
```

`verify` walks the whole hash chain; `--against` also checks that the log
still contains a head saved earlier, which catches a log rewritten by
someone with the database.

The log grows forever unless you prune it (1.12). Pruning first checks
the whole chain, then replaces the old entries with a **checkpoint**
signed by the cluster key (the last pruned entry's number and hash), so
`verify` still proves that nothing after it changed:

```sh
pumasi audit prune --before 365d --save /secure/audit-2025.jsonl         # says what would go
pumasi audit prune --before 365d --save /secure/audit-2025.jsonl --yes   # prunes
pumasi audit verify --against /secure/audit-head-2025-06-01.json --pruned /secure/audit-2025.jsonl
```

`--save` keeps the removed entries (one JSON object per line) and the
checkpoint beside them, so a head saved before the prune can still be
checked with `--pruned`. Export them to your SIEM first anyway. The
newest entry is never pruned, and the prune itself is logged
(`audit.prune`). `pumasi cluster start --keep-audit 400d` prunes hourly
without saving: use it only when `audit export --follow` already keeps a
copy.

## 7. When something goes wrong

Start every incident with `pumasi audit ls` (who did what, from which
peer) and `pumasi events --since 24h`.

### A leaked access ticket or SSO session

```sh
pumasi access ls
pumasi access revoke NAME-OR-ID
pumasi audit ls --json | jq 'select(.actor == "NAME")'
```

It stops working at once. Check what it submitted (`pumasi get jobs`) and
cancel anything unexpected.

### A leaked admin ticket

1. `pumasi admin rotate` on a machine with the current admin ticket (or on
   the control plane machine). Every copy of the old ticket stops working;
   the new one is printed once and saved there. Log the other admins in
   again with it.
2. Read the audit log for calls by "cluster admin" from peers you do not
   know, especially `upgrade.rollout`, `access.create`, `secret.*`,
   `node.trust`, `measurement.trust` and `sso.*`.
3. If a rollout you did not start happened, treat every node that took
   it as compromised (below). `pumasi upgrade cancel` stops offering it.
4. Rotate the cluster secrets the attacker could read (`pumasi secret ls`)
   at their source (registry passwords and the like).

### A compromised node

```sh
pumasi remove-node NODE     # revokes it; it needs a new ticket to come back
```

Its running tasks are placed elsewhere. Re-image the machine before it
joins again. If it was trusted to settle checks, look at `pumasi events`
for the checks it settled. A provider node: also `pumasi untrust NODE`
and consider suspending its account's earnings (`pumasi cu`).

### A compromised identity provider or directory

`pumasi sso oidc --off` or `pumasi sso ldap --off` stops new sign-ins;
`pumasi access ls` and `pumasi access revoke` end the sessions it issued.

### A lost control plane

With replicas, the others elect a new leader within seconds. Without:
restore the latest backup on a new machine at the same address (step 4).

### A node is asleep, or does not wake

`pumasi status` shows a machine that said it was going to sleep as
`asleep`; its tasks have already moved and no alert fires. `pumasi node
NODE` shows what its agent does about sleep, its network cards, and when
the cluster last sent its magic packet. If `pumasi node wake NODE` says
no machine is on its LAN, another agent (1.5 or newer) or the control
plane has to be online on the same subnet. If the packet is sent and the
machine stays asleep (a `node.wake_failed` event after three tries),
Wake-on-LAN is off in its firmware or OS: the settings are in the
[README](../README.md#machines-that-sleep). A machine that should never
sleep while it lends itself needs nothing; one that must be free to
sleep mid-task takes `pumasi service set -- --allow-sleep`.

### Something is slow or full

`pumasi status`, `pumasi top`, `pumasi store usage`. A full disk on the
control plane: lower `--keep-jobs`, remove datasets, or move
`PUMASI_HOME` to a bigger disk while it is stopped.

A microVM node keeps up to two booted guests waiting for the next task
of an image it just ran (1.14), within a quarter of its memory. On a
machine short of memory, `pumasi service set -- --warm-vms 0` (or
`PUMASI_WARM_VMS=0`) turns that off; a guest that does not take its task
turns it off on that node by itself, with a warning in the agent's log.

## 8. Before a release

`scripts/soak.sh` runs a local cluster under a steady stream of jobs
through the API while it kills agents and the control plane, and fails on
lost or stuck jobs, an audit log that stops verifying, or memory and open
files that keep growing. It runs on Linux and macOS (bash 4 or later,
curl, jq). CI runs it for 10 minutes on every pull request.

A release needs, on the commit being tagged:

- CI green, including `supply-chain`, `fuzz`, `soak`, `sso-e2e` and
  CodeQL, and no open high or critical alerts under Security → Code
  scanning or Dependabot.
- A green run of the **Long checks** workflow
  (`.github/workflows/long-checks.yml`: Actions → Long checks → Run
  workflow). It soaks for 5h20m on Linux and on macOS, restarting an
  agent every 15 minutes and the control plane every hour, and runs every
  fuzz target for 20 minutes. It also runs every Sunday. The soak reports
  are the run's artifacts.

A longer soak on hardware you own is worth doing when you can, but no
longer blocks a release. Any Mac or Linux machine works; on a Mac, keep it
awake with `caffeinate`:

```sh
cargo build --release -p pumasi-cli
caffeinate -dims scripts/soak.sh --duration 72h --report soak-$(date +%F)   # Linux: drop caffeinate -dims
```

The report directory has `summary.txt`, `samples.csv` (RSS and open files
every 30 seconds) and `jobs.csv`. With no machine to spare, a Linux VM on
a Mac (UTM, Lima, OrbStack) or a free cloud VM (Oracle Cloud's Always
Free Ampere instances) can run it too.
