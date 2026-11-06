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

Run it as a service. Pumasi does not install one for the control plane
yet, so write the unit yourself (systemd shown; launchd and Windows
services work the same way):

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
ExecStart=/usr/local/bin/pumasi ui --listen 127.0.0.1:7400 --no-open --public-url https://pumasi.example.com
```

Put it on the internet only through Cloudflare Tunnel, a reverse proxy
with TLS, or `--tls-cert`/`--tls-key` ([api.md](api.md), "Put it on the
internet"). Behind a proxy, pass `--real-ip-header` so limits apply per
client and not to the proxy. `pumasi ui --api-only` serves only the JSON
API for scripts.

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
| Keycloak | `https://keycloak.example.com/realms/<realm>` | Add a "Group Membership" mapper to the client (claim `groups`, full group path off). Enable "OAuth 2.0 Device Authorization Grant" in the client's capabilities. |
| Okta | `https://<org>.okta.com/oauth2/default` | Add a `groups` claim to the authorization server. |

`--groups-claim` changes the claim's name, `--scopes` adds scopes (some
providers need `groups`).

### LDAP and Active Directory

```sh
pumasi sso ldap --url ldaps://dc1.acme.local:636 \
  --bind-dn 'CN=pumasi,OU=Service,DC=acme,DC=local' --bind-password-file /root/ldap-password \
  --user-base 'OU=People,DC=acme,DC=local' --user-filter '(sAMAccountName={user})'
```

OpenLDAP: `--user-filter '(uid={user})'` (the default), and
`--group-base ou=groups,dc=example,dc=com` if the directory has no
`memberOf`. Use `ldaps://`, or `ldap://` with `--starttls`; plain LDAP is
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

## 3. Upgrade

1. Read the release notes (protocol and storage layout changes).
2. `pumasi backup` (step 4).
3. Replace the control plane's binary and restart it. With replicas,
   upgrade the followers one at a time first (`pumasi ha status` shows
   who leads), then the leader, so a majority is always up. The database
   is migrated in place on first open; there is no way back to an older
   binary except restoring the backup.
4. Roll the agents out from the admin's machine:
   `pumasi upgrade rollout pumasi-linux-x86_64 pumasi-macos-aarch64 pumasi-windows-x86_64.exe --wait`.
   Older agents keep connecting as long as the control plane still
   supports their protocol version, and the rollout replaces them.
5. Restart `pumasi ui` with the new binary.

## 4. Back up and restore

```sh
pumasi backup /secure/pumasi-$(date +%F).backup
rsync -a /var/lib/pumasi/cluster/blobs/ backup-host:/pumasi/blobs/
```

A backup holds the cluster key, the admin secret, SSO settings and the
cluster's secrets: encrypt it and store it like the admin ticket. Back up
daily (a cron job or timer running `pumasi backup`), and keep the file
store's `blobs` directory with it. Restore on a fresh machine with
`pumasi cluster restore FILE` and then `pumasi cluster start`; nodes
reconnect when it is reachable where the old one was (same `--bind`
address or relay). Practice a restore once before you need it.

## 5. High availability

`pumasi ha invite` on the running control plane gives a ticket for a
replica; run `pumasi cluster replica <TICKET> --bind 0.0.0.0:7480` on each
new machine (3 or 5 in total), then make new join and admin tickets so
they list every replica. Watch `pumasi ha status`. The file store's data
reaches replicas every 30 seconds, not with each write (README, known
gaps).

## 6. Watch it

- Alerts: `pumasi alert set --defaults` and a webhook (`pumasi webhook add
  ops https://hooks.slack.com/...`).
- Prometheus and Grafana: `--metrics-addr` and `examples/grafana`.
- `pumasi top`, `pumasi events -f`.
- Disk: the control plane's database grows with jobs (bounded by
  `--keep-jobs`) and the audit log (kept forever); the file store with
  datasets (bounded by quotas).

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

### Something is slow or full

`pumasi status`, `pumasi top`, `pumasi store usage`. A full disk on the
control plane: lower `--keep-jobs`, remove datasets, or move
`PUMASI_HOME` to a bigger disk while it is stopped.

## 8. Before a release: the soak test

`scripts/soak.sh` runs a local cluster under a steady stream of jobs
through the API while it kills agents and the control plane, and fails on
lost or stuck jobs, an audit log that stops verifying, or memory and open
files that keep growing. CI runs it for 10 minutes on every pull request;
a release waits for 72 hours on a real machine:

```sh
cargo build --release -p pumasi-cli
scripts/soak.sh --duration 72h --report soak-$(date +%F)
```

The report directory has `summary.txt`, `samples.csv` (RSS and open files
every 30 seconds) and `jobs.csv`.
