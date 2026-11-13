# The HTTP API from anywhere

Scripts and programs on other machines (a laptop at home, a CI job, a
lab's render farm front end) can submit jobs to a Pumasi cluster and
fetch their results over plain HTTPS with `curl`, without installing
Pumasi or speaking iroh. `pumasi ui --api-only` serves the API; a
Cloudflare Tunnel (or a reverse proxy, or `--tls-cert`) puts it on the
internet; an access ticket and an account job policy decide what each
client may do.

<!--
The `sh` blocks on this page run in CI, top to bottom, in one shell
(scripts/ci/api-doc.sh), against a local cluster with `pumasi ui
--api-only` running and $PUMASI_URL pointing at it. `bash` blocks are
shown only. Lines like `<!- - ci: ... - ->` run in CI but are not shown.
-->

## On the admin's machine

### 1. A ticket and a policy for the client

Give each client a **submitter** ticket for its own account: it can
submit, follow and cancel that account's jobs and sees nothing else.

```sh
TICKET=$(pumasi access create "lab scripts" --role submitter --account lab --ttl 90d | cut -d' ' -f3)
echo "$TICKET"   # hand this to whoever runs the scripts
```

Then say what the account's jobs may do. The control plane refuses
anything else with `403`, whichever submitter ticket of the account sends
it (admin tickets are not held to the policy):

```sh
pumasi quota lab --slots 4 --executors wasm,docker \
  --images 'ghcr.io/lab/*,python:3.12-slim' --network deny --max-timeout 30m
pumasi quota lab
pumasi cu add lab 100   # CU its jobs pay the machines they run on
```

| Option | Refuses |
| --- | --- |
| `--executors wasm,docker` | other executors (microVMs here) |
| `--images 'ghcr.io/lab/*'` | other images (`*` matches any characters; `name@sha256:...` pins one digest) |
| `--require-digest yes` | images named by tag instead of digest |
| `--network deny` | jobs with `network = true` |
| `--selector gpu=nvidia` | nothing: it is added to every job's selector, so the account's jobs run only there |
| `--tolerations none` | jobs that tolerate taints (so they stay off tainted nodes) |
| `--max-timeout 30m`, `--max-gpus-per-task 1` | longer or bigger tasks |

`none` or `any` lifts one limit, `--clear` all of them. To change one
entry of a list and keep the rest, add or remove it:

```sh
pumasi quota lab --add-images 'node:22-slim'
pumasi quota lab --remove-images 'node:22-slim'
```
<!-- ci: pumasi quota lab 2>&1 | grep -q 'images ghcr.io/lab/\* python:3.12-slim,' -->

`--add-executors`, `--remove-executors`, `--add-tolerations` and
`--remove-tolerations` do the same for their lists. The same change over
HTTP is `PATCH /api/v1/accounts/lab/policy`
([below](#account-policies-admin-ticket)), and the dashboard's
**Accounts** page edits it in a form. The policy can also live in the
cluster file (`pumasi apply`):

```bash
cat > cluster.toml <<'EOF'
[accounts.lab]
slots = 4
executors = ["wasm", "docker"]
images = ["ghcr.io/lab/*", "python:3.12-slim"]
network = "deny"
max_timeout = "30m"
EOF
pumasi apply cluster.toml
```

Every job records who submitted it (the ticket's name) and the digest of
each image its tasks actually ran (`pumasi job ID`, and `submitted_by`
and `image_digests` in the API).

### 2. Serve the API

```bash
pumasi ui --api-only --listen 127.0.0.1:7400 --real-ip-header CF-Connecting-IP
```

`--api-only` serves `/api/v1`, `/api/openapi.json` and `/healthz` only:
no pages, no sign-in link, no cookies, and only
`Authorization: Bearer <ticket>`. After 10 wrong tickets within a minute
from one address, that address gets `429 Too Many Requests` for 5
minutes. Behind a tunnel every connection comes from 127.0.0.1, so
`--real-ip-header` names the header that carries the client's address
(trusted only on connections from this machine, and from a proxy on
another machine named with `--trusted-proxy`, below). For a list such as
`X-Forwarded-For` the last address counts, the one the proxy in front
added; earlier ones are whatever the client sent. Keep it running as a
service the same way as the agent (systemd, launchd or Task Scheduler).

What counts towards the 10: a bearer ticket that is not one, is for
another cluster, or that the control plane refuses (wrong, expired,
revoked), and on the dashboard a failed sign-in (ticket, LDAP password,
SSO, a sign-in link that is not valid). A request **without** a ticket
(a plain `401`, such as a scanner probing paths) does not count, and
neither does a valid ticket that lacks the role for a call (`403`).
While an address is blocked, its bearer requests and sign-ins get `429`
even with a valid ticket; browsers already signed in from it keep
working.

### 3. Put it on the internet

**Cloudflare Tunnel** (no open port, no certificate to renew, free):

```bash
cloudflared tunnel login
cloudflared tunnel create pumasi
cloudflared tunnel route dns pumasi pumasi.example.com
cat > ~/.cloudflared/config.yml <<'EOF'
tunnel: pumasi
credentials-file: /home/me/.cloudflared/<tunnel id>.json
ingress:
  - hostname: pumasi.example.com
    service: http://127.0.0.1:7400
  - service: http_status:404
EOF
cloudflared tunnel run pumasi      # or: sudo cloudflared service install
```

Or create the tunnel in the Cloudflare dashboard (Zero Trust → Networks
→ Tunnels → Create a tunnel, type Cloudflared): it shows a
`cloudflared service install <token>` command for the machine (on
Windows, from an administrator prompt) and takes the public host name
(`pumasi.example.com` → `HTTP` `127.0.0.1:7400`) there, with no config
file.

**The web dashboard through the same name.** `pumasi ui` without
`--api-only` serves the pages and the same `/api/v1` (bearer tickets work
there too), so one host name can serve browsers and scripts:

```bash
pumasi ui --listen 127.0.0.1:7400 --login-only \
  --public-url https://pumasi.example.com --real-ip-header CF-Connecting-IP
```

`--public-url` is the name browsers use: the dashboard accepts it as the
Host (on loopback it refuses other names, against DNS rebinding; before
1.2.3 that included the tunnel's name, so pages answered `421`), marks the
session cookie `Secure`, and sends SSO back to it. `--login-only` turns
off the sign-in link `pumasi ui` prints, so everyone signs in with their
own access ticket (`pumasi access create stan --role admin --ttl 30d`)
or SSO. Cloudflare Access can put an email check in front of the pages;
keep a Bypass rule for `/api/` so scripts with only a ticket still get
through.

For a quick try without a domain, `cloudflared tunnel --url
http://127.0.0.1:7400` prints a temporary `https://*.trycloudflare.com`
address. Things to know:

- Cloudflare limits request bodies by plan (100 MB on Free and Pro), so
  bigger inputs fail with `413`. Put large inputs in the cluster file
  store from a machine with the CLI (`pumasi store put NAME FILES`) and
  mount them with `store:NAME` in `job.toml`.
- A response has to start within 100 seconds, which is why `?wait=`
  stops at 90.
- Cloudflare Access can add a second check in front (a service token
  for scripts); the Pumasi ticket is still required.

**Your own TLS** (a reverse proxy such as Caddy or nginx on the same
machine works the same way as the tunnel; or serve HTTPS directly):

```bash
pumasi ui --api-only --listen 0.0.0.0:8443 \
  --tls-cert /etc/letsencrypt/live/pumasi.example.com/fullchain.pem \
  --tls-key  /etc/letsencrypt/live/pumasi.example.com/privkey.pem
```

`--api-only` refuses an address other than loopback over plain HTTP
(tickets would cross the network readable); `--insecure-http` overrides
that for a TLS proxy elsewhere on a private network.

**A reverse proxy on another machine** of a private network connects
from its own address, not from 127.0.0.1, so `--real-ip-header` alone is
not believed and every client counts as the proxy: one client's 10 wrong
tickets would block everyone behind it (`pumasi ui` warns about this at
start). Name the proxy with `--trusted-proxy`, by the address this
machine sees it connect from:

```bash
pumasi ui --listen 0.0.0.0:7400 --no-open --login-only \
  --public-url https://pumasi.example.com \
  --real-ip-header X-Forwarded-For --trusted-proxy 192.168.1.20
```

`--trusted-proxy` can be given several times and takes networks too
(`10.8.0.0/24`, `fd00:1::/64`). In `X-Forwarded-For`, addresses of
trusted proxies are skipped from the end, so a chain of them (a CDN's
proxy in front of yours, if you list its addresses) still yields the
client. Whatever can connect from a trusted address picks the address it
is counted under, so let only the proxy reach the port (a firewall rule
on this machine), and have the proxy set the header itself instead of
passing on what the client sent. Caddy does that by default:

```
pumasi.example.com {
    reverse_proxy 192.168.1.30:7400
}
```

For nginx: `proxy_set_header X-Forwarded-For $remote_addr;`.

## On the client

Two values, from the admin:

```bash
export PUMASI_URL=https://pumasi.example.com
export PUMASI_TICKET=pumasiadmin...
```
<!-- ci: export PUMASI_TICKET="$TICKET" -->

Who am I, what may my jobs ask for, and what can the cluster do for my
account right now:

```sh
curl -fsS -H "Authorization: Bearer $PUMASI_TICKET" "$PUMASI_URL/api/v1/whoami" \
  | tee whoami.json | jq '{name, role, account, policy, quota, usage}'
curl -fsS -H "Authorization: Bearer $PUMASI_TICKET" "$PUMASI_URL/api/v1/status" \
  | jq '.capacity | {ready, ready_gpu, nodes: [.nodes[] | {node, free_slots, reason}]}'
```
<!-- ci: test "$(jq -c '[.account, .policy.network, .policy.executors, .policy.images, .policy.max_timeout_ms, .quota.slots]' whoami.json)" = '["lab","deny",["docker","wasm"],["ghcr.io/lab/*","python:3.12-slim"],1800000,4]' -->

For a submitter ticket, `whoami` carries its account's job policy
(`images`, `require_digest`, `executors`, `network`, `selector`,
`tolerations`, `max_timeout_ms`, `max_gpus_per_task`), its `quota` and
what is in `usage` now, so a client can see what it may submit without an
admin ticket. An empty `images` or `executors` list allows any, and
`null` is no limit. `pumasi whoami` prints the same.

`capacity` counts the online nodes that would take this account's work
now, per executor, and says why each other node would not (offline,
paused, full, no Docker, the selector, a taint, the quota...). Nodes the
account's policy rules out (an executor it may not use, GPUs when
`max_gpus_per_task` is 0) are not counted as ready.

### Submit a job

A job is a `job.toml` plus its files, sent as `multipart/form-data`
(the files keep their names, so the job file goes as `job.toml`, or
`-F "files=@other.toml;filename=job.toml"`, or as a text field named
`job`):

```sh
cat > job.toml <<'EOF'
name = "from-curl"
module = "ticker.wasm"
args = ["1"]
shards = 2
deterministic = false
queue_timeout = "10m"
EOF
JOB=$(curl -fsS -H "Authorization: Bearer $PUMASI_TICKET" \
  -F files=@job.toml -F files=@ticker.wasm \
  "$PUMASI_URL/api/v1/jobs" | jq -r .id)
echo "$JOB"
```

`ticker.wasm` is any WASI module (this one is
`crates/dashboard/e2e/fixtures/ticker.wasm`). `queue_timeout` fails the
job, with the reasons, when no node has started it within that time,
instead of letting it wait forever.

### Wait for it

`?wait=` answers as soon as the job finishes, or when the wait runs out
(at most 90 seconds); then ask again:

```sh
while :; do
  STATE=$(curl -fsS -H "Authorization: Bearer $PUMASI_TICKET" \
    "$PUMASI_URL/api/v1/jobs/$JOB?wait=60s" | jq -r .summary.state)
  case "$STATE" in Succeeded|Failed|Cancelled) break ;; esac
done
echo "$STATE"
```
<!-- ci: test "$STATE" = Succeeded -->

### Where the time went

Each task says which node ran it, by id (`node`) and by name
(`node_name`), and how long each part took, in milliseconds:

```sh
curl -fsS -H "Authorization: Bearer $PUMASI_TICKET" "$PUMASI_URL/api/v1/jobs/$JOB" \
  | tee job.json \
  | jq '.tasks[] | {index, node_name, queued_ms, fetch_ms, pull_ms, run_ms, upload_ms, wall_ms}'
```
<!-- ci: test "$(jq -c '[.tasks[] | .node_name] | unique' job.json)" = '["node-a"]' -->
<!-- ci: jq -e '.tasks | length == 2 and all(.[]; .run_ms == .wall_ms and .pull_ms == 0 and (.queued_ms | type) == "number" and (.fetch_ms | type) == "number" and (.upload_ms | type) == "number")' job.json -->

| Field | Time spent |
| --- | --- |
| `queued_ms` | waiting in the queue for a node (for a task that ran again, the wait of its last run) |
| `fetch_ms` | on the node, fetching what the task mounts from the cluster's file store (datasets, large inputs, a private image) |
| `pull_ms` | pulling the image; `0` when the node already had it |
| `run_ms` | running: creating the container or VM, the program, copying its output out |
| `upload_ms` | storing the output in the file store; `0` when it is small enough to travel with the result |

A task of an `executor = "image"` job also has `ran_as` (1.5): `docker`
or `microvm`, whichever its node ran it with.

`wall_ms` is `run_ms`, and it is the only time the receipt bills: a task
that waited ten minutes for a cold image pays the same as one that found
the image cached. `pumasi job ID` and the dashboard show the same
numbers. Agents older than 1.4 report no `fetch_ms`, `pull_ms` or
`upload_ms` (they read `0`), and their Docker tasks still count the pull
in `wall_ms`.

### Logs and outputs

```sh
curl -fsS -H "Authorization: Bearer $PUMASI_TICKET" \
  "$PUMASI_URL/api/v1/jobs/$JOB/tasks/0/logs" | jq -r .stdout
curl -fsS -H "Authorization: Bearer $PUMASI_TICKET" \
  -o outputs.zip "$PUMASI_URL/api/v1/jobs/$JOB/outputs.zip"
unzip -o outputs.zip -d outputs && ls outputs/*
```
<!-- ci: grep -q "pumasi dashboard ok" outputs/task-0/result.txt -->

The zip has one folder per task (`task-0/`, `task-1/`, ...).

### When something is refused

Errors are JSON, `{"error": "..."}`, with the reason. A job the
account's policy does not allow:

```sh
cat > net-job.toml <<'EOF'
name = "needs-the-network"
image = "python:3.12-slim"
command = ["python", "-c", "print('hi')"]
network = true
EOF
curl -sS -H "Authorization: Bearer $PUMASI_TICKET" -F "files=@net-job.toml;filename=job.toml" \
  -o refused.json -w '%{http_code}\n' "$PUMASI_URL/api/v1/jobs"
jq -r .error refused.json
```
<!-- ci: grep -q "does not allow network access" refused.json -->

To ask first, add `?validate=1`: the job goes through the same checks
(the ticket, the account's policy and quotas, the files it names) and
nothing is submitted. The answer is `200`, or the status and error a real
submission would get:

```sh
CHECK=$(curl -sS -H "Authorization: Bearer $PUMASI_TICKET" \
  -F files=@job.toml -F files=@ticker.wasm \
  -o check.json -w '%{http_code}' "$PUMASI_URL/api/v1/jobs?validate=1")
echo "$CHECK"; jq . check.json
REFUSED=$(curl -sS -H "Authorization: Bearer $PUMASI_TICKET" \
  -F "files=@net-job.toml;filename=job.toml" \
  -o check-refused.json -w '%{http_code}' "$PUMASI_URL/api/v1/jobs?validate=1")
echo "$REFUSED"; jq -r .error check-refused.json
```
<!-- ci: test "$CHECK" = 200 -->
<!-- ci: test "$(jq -c '[.valid, .account, .tasks, .ready_nodes, .waiting]' check.json)" = '[true,"lab",2,["node-a"],null]' -->
<!-- ci: test "$REFUSED" = 403 -->
<!-- ci: test "$(jq -r .error check-refused.json)" = "$(jq -r .error refused.json)" -->

`ready_nodes` names the nodes that would start a task right now; when
there are none, `waiting` says why the job would sit in the queue (the
jobs it waits for, the quota, or each node's reason).

| Status | Means |
| --- | --- |
| `400` | the request or `job.toml` is wrong |
| `401` | no ticket, or a wrong, expired or revoked one |
| `403` | the ticket's role or the account's policy does not allow it |
| `404` | no such job (or not this account's) |
| `429` | too many wrong tickets from this address, or too many requests from this ticket (120 at once, then 20 a second); see `Retry-After` |
| `502` | `pumasi ui` cannot reach the control plane |

### The audit log

Every call that changes something, and every refusal (like the one
above), is in the cluster's audit log, with the ticket's name. Reading it
takes an admin ticket, so this client's ticket is refused:

```sh
curl -sS -H "Authorization: Bearer $PUMASI_TICKET" -o /dev/null -w '%{http_code}\n' \
  "$PUMASI_URL/api/v1/audit"
```
<!-- ci: test "$(curl -sS -H "Authorization: Bearer $PUMASI_TICKET" -o /dev/null -w '%{http_code}' "$PUMASI_URL/api/v1/audit")" = 403 -->

With an admin ticket, `GET /api/v1/audit?after=N&limit=N` returns the
entries (oldest first, with their hashes) and `GET /api/v1/audit/verify`
checks the whole chain and returns its head signed by the cluster key,
the same as `pumasi audit ls --json` and `pumasi audit verify --json`.

### Cancel

```sh
curl -fsS -X POST -H "Authorization: Bearer $PUMASI_TICKET" \
  "$PUMASI_URL/api/v1/jobs/$JOB/cancel"
```

### Account policies (admin ticket)

These take the cluster's admin ticket (or an admin-role access ticket),
not the client's. `GET /api/v1/accounts/NAME/policy` answers an account's
policy, quota and use (a submitter ticket may read its own account's);
`PATCH` changes only the fields in the body and answers the result. A
list takes the whole list, or what to add and remove, so allowing one
more image needs no read first:

<!-- ci: export PUMASI_ADMIN_TICKET=$(cat "$PUMASI_HOME/cluster/admin.ticket") -->
```sh
curl -fsS -X PATCH -H "Authorization: Bearer $PUMASI_ADMIN_TICKET" \
  -H 'Content-Type: application/json' \
  -d '{"images": {"add": ["node:22-slim"]}, "max_timeout_ms": 3600000}' \
  "$PUMASI_URL/api/v1/accounts/lab/policy" | tee policy.json | jq '{images, max_timeout_ms, executors, network}'
```
<!-- ci: test "$(jq -c '[.images, .max_timeout_ms, .executors, .network, .quota.slots]' policy.json)" = '[["ghcr.io/lab/*","python:3.12-slim","node:22-slim"],3600000,["docker","wasm"],"deny",4]' -->

```sh
curl -fsS -X PATCH -H "Authorization: Bearer $PUMASI_ADMIN_TICKET" \
  -H 'Content-Type: application/json' \
  -d '{"images": {"remove": ["node:22-slim"]}, "max_timeout_ms": 1800000, "quota": {"slots": 4}}' \
  "$PUMASI_URL/api/v1/accounts/lab/policy" >/dev/null
curl -fsS -H "Authorization: Bearer $PUMASI_TICKET" "$PUMASI_URL/api/v1/accounts/lab/policy" \
  | tee policy.json | jq '{account, images, quota}'
```
<!-- ci: test "$(jq -c '[.account, .images, .max_timeout_ms]' policy.json)" = '["lab",["ghcr.io/lab/*","python:3.12-slim"],1800000]' -->
<!-- ci: test "$(curl -sS -X PATCH -H "Authorization: Bearer $PUMASI_TICKET" -H 'Content-Type: application/json' -d '{"network": "allow"}' -o /dev/null -w '%{http_code}' "$PUMASI_URL/api/v1/accounts/lab/policy")" = 403 -->
<!-- ci: test "$(curl -sS -X PATCH -H "Authorization: Bearer $PUMASI_ADMIN_TICKET" -H 'Content-Type: application/json' -d '{"image": ["a"]}' -o /dev/null -w '%{http_code}' "$PUMASI_URL/api/v1/accounts/lab/policy")" = 400 -->
<!-- ci: test "$(curl -sS -H "Authorization: Bearer $PUMASI_TICKET" -o /dev/null -w '%{http_code}' "$PUMASI_URL/api/v1/accounts/other/policy")" = 403 -->

| In the body | Does |
| --- | --- |
| a field left out | nothing: it keeps its value |
| `"images": ["a", "b"]` | replaces the list (`[]` allows any image) |
| `"images": {"add": ["c"], "remove": ["a"]}` | changes the list; adding what is there or removing what is not changes nothing, so the call can be repeated. Removing the last entry is an error, because an empty list allows everything |
| `"executors"`, `"tolerations"` | lists like `images`; `"tolerations": null` allows any taint |
| `"network": "deny"`, `"require_digest": true`, `"selector": "gpu=nvidia"` | sets the value |
| `"max_timeout_ms": null`, `"max_gpus_per_task": null` | lifts the limit |
| `"quota": {"slots": 4, "gpus": null}` | sets or lifts the quotas it names (`slots`, `gpus`, `cu_per_day`, `max_priority`, `storage_bytes`) |

What `GET` answers can be sent back as it is, and an unknown field is a
`400`. The schema is in `/api/openapi.json` (`AccountPolicyPatch`).

### Node names and upgrades (admin ticket)

These also take an admin ticket. Give a node a display name; it shows in the status, the
dashboard and events, and works wherever a node name is taken. `null`
goes back to the name its agent reports:

```sh
curl -fsS -X POST -H "Authorization: Bearer $PUMASI_ADMIN_TICKET" \
  -H 'Content-Type: application/json' -d '{"name": "living-room"}' \
  "$PUMASI_URL/api/v1/nodes/node-a/rename"
```
<!-- ci: curl -fsS -H "Authorization: Bearer $PUMASI_ADMIN_TICKET" "$PUMASI_URL/api/v1/status" | grep -q '"living-room"' -->
<!-- ci: curl -fsS -X POST -H "Authorization: Bearer $PUMASI_ADMIN_TICKET" -H 'Content-Type: application/json' -d '{"name": null}' "$PUMASI_URL/api/v1/nodes/living-room/rename" -->

A node that told the cluster it was going to sleep has
`power.asleep_since_unix` in `GET /api/v1/status` (1.5), next to what its
agent reported: `power.report.wake_on_lan`, its LAN cards in
`power.report.lan`, and `power.can_wake` (a machine of the cluster is
online on its LAN). Send the Wake-on-LAN magic packet of an offline node
that runs with `--wake-on-lan`:

```sh
curl -sS -X POST -H "Authorization: Bearer $PUMASI_ADMIN_TICKET" \
  "$PUMASI_URL/api/v1/nodes/node-a/wake"
```
<!-- ci: curl -sS -X POST -H "Authorization: Bearer $PUMASI_ADMIN_TICKET" "$PUMASI_URL/api/v1/nodes/node-a/wake" | grep -q 'is online' -->
<!-- ci: curl -fsS -H "Authorization: Bearer $PUMASI_ADMIN_TICKET" "$PUMASI_URL/api/v1/status" | jq -e '.nodes[0].power | .asleep_since_unix == null and .report.wake_on_lan == false' -->

It answers with one line per machine that sent the packet, or an error
that says why none could (the node is online, did not opt in, or no
machine of the cluster is on its LAN).

`GET /api/v1/upgrade?check=true` shows the newest published release, and
`POST /api/v1/upgrade` has the control plane download it for every
platform in the cluster, check it against the release's SHA256SUMS and
roll it out (body `{"version": "v1.3.0"}` for another release). It answers
at once; poll `GET /api/v1/upgrade` until `fetch` is `Done` or `Failed`:

```sh
curl -fsS -H "Authorization: Bearer $PUMASI_ADMIN_TICKET" "$PUMASI_URL/api/v1/upgrade"
```
<!-- ci: curl -fsS -H "Authorization: Bearer $PUMASI_ADMIN_TICKET" "$PUMASI_URL/api/v1/upgrade" | grep -q '"fetch":"Idle"' -->

```json
{"source":"https://github.com/syang0531/pumasi/releases","latest":"v1.3.0","checked_unix":1762851000,"check_error":null,"control_plane":"1.2.3","fetch":{"Running":{"version":"v1.3.0","step":"downloading pumasi-windows-x86_64.exe (31/72 MiB)","started_unix":1762851004}}}
```

Every route, with its parameters, is in `$PUMASI_URL/api/openapi.json`
(OpenAPI 3.1), which code generators and tools such as Postman read.

## Clients behind an HTTPS proxy

Some clients reach the internet only through an HTTPS proxy: a Claude
Code cloud session, many company networks. That is all this API needs
(one HTTPS host and a bearer header). The CLI needs more: it speaks iroh,
QUIC over UDP directly or through relays over WebSocket, and such
proxies usually pass neither (see the end of this section).

### pumasi-remote

`examples/remote-client/pumasi-remote` does the calls above in one
script (bash, curl and jq). Put it on the client's `PATH`:

```bash
curl -fsSL -o ~/.local/bin/pumasi-remote \
  https://raw.githubusercontent.com/syang0531/pumasi/main/examples/remote-client/pumasi-remote
chmod +x ~/.local/bin/pumasi-remote
```

Then a folder with a `job.toml` and the files it names runs with one
command, which submits it, follows it, prints the logs of failed tasks and
unpacks the outputs:

```sh
mkdir -p hello && cp job.toml ticker.wasm hello/
pumasi-remote whoami
pumasi-remote check hello | tee hello-check.json
pumasi-remote run hello hello-out
ls hello-out
```
<!-- ci: jq -e '.valid and .tasks == 2' hello-check.json -->
<!-- ci: ! pumasi-remote check net-job.toml -->
<!-- ci: grep -q "pumasi dashboard ok" hello-out/task-0/result.txt -->

- It sends only the files `job.toml` names (`module`, `stdin`, `files`,
  the same in `[[tasks]]`; needs python3 3.11 or later), so a `.env` or
  `.git` next to it stays on the client.
- `PUMASI_TICKET` is optional: leave it unset when the network adds the
  `Authorization` header itself, as Claude Code network secrets do.
- It retries what cannot have changed anything (reads after network
  errors, `429` after `Retry-After`) and never resends a submission that
  may have arrived.
- When something in front of the API answers instead of Pumasi (a
  Cloudflare challenge page, a company proxy), it says so instead of
  failing on HTML.
- `whoami` shows what the account's jobs may ask for, and `check DIR`
  asks whether the cluster would take a job (`?validate=1`) without
  submitting it.
- `submit`, `wait`, `logs`, `outputs`, `cancel`, `status` and `jobs` do
  one step each; `pumasi-remote --help` lists them. `wait` and `run` exit
  0 only if the job succeeded, and 2 if `--timeout` ran out first (the
  job keeps running).

### Claude Code on the web

A Claude Code cloud session can use a home cluster as its build and test
farm. Its sandbox reaches the internet only through an HTTPS proxy that
re-signs TLS, sends no UDP, and keeps keys out of the session: a
**network secret** of the environment adds the header to requests for
the hosts it names, after they leave the session.

1. **On the admin's machine**, a ticket for its own account, with a
   narrow policy (Docker nodes share their kernel with containers):

   ```bash
   pumasi access create "claude cloud" --role submitter --account claude --ttl 90d
   pumasi quota claude --slots 4 --executors wasm,docker \
     --images 'python:3.12-slim,rust:1-slim,node:22-slim' --max-timeout 1h
   pumasi cu add claude 100
   ```

   Serve the API and put it behind Cloudflare Tunnel as above
   (`https://pumasi.example.com`).
2. **In the Claude Code environment settings** (the project's cloud
   environment):
   - Network secret: type Bearer, header `Authorization`, prefix
     `Bearer`, value the ticket, allowed website `pumasi.example.com`.
     The ticket never enters the session. A secret's host is reachable
     without adding it to the allowed domains.
   - Environment variable: `PUMASI_URL=https://pumasi.example.com`. Do
     not set `PUMASI_TICKET`.
   - Setup script: the `curl` lines above (pin a commit or tag instead
     of `main` if you prefer).
3. **Tell Claude** in the repository's `CLAUDE.md`, for example:

   ```text
   A Pumasi cluster is available through `pumasi-remote` (PUMASI_URL is
   set, the proxy adds the ticket). For heavy builds, tests or data jobs,
   write a job folder (job.toml plus the files it names; `pumasi-remote
   whoami` lists the images and executors the account may use) and run
   `pumasi-remote run DIR`; `pumasi-remote check DIR` says beforehand
   whether the job would be accepted. For jobs over a minute, `pumasi-remote submit
   DIR`, then `pumasi-remote wait ID` in the background. Check
   `pumasi-remote status` first; if no node is ready, run locally.
   ```

   Foreground commands in a cloud session stop after 2 minutes by
   default, which is why long jobs are submitted and then waited for in
   the background.

Things to know:

- **Cloudflare's bot protection.** Requests come from cloud addresses
  with a script's user agent. If `pumasi-remote` reports a web page
  instead of JSON, Security → Events in the Cloudflare dashboard names
  the rule. Bot Fight Mode cannot be skipped per host on the Free plan;
  turn it off for the zone, or put the API on a zone without it.
- **Cloudflare Access** needs two headers (`CF-Access-Client-Id` and
  `CF-Access-Client-Secret`), so two network secrets for the same host
  beside the Pumasi ticket. Whether the proxy sends all of them is not
  documented; not tried yet.
- **Bodies over 100 MB** fail at Cloudflare (Free and Pro). Keep job
  folders small; put big inputs in the store from a machine with the CLI
  (`pumasi store put`) or let the job download them (`network = true`,
  if the policy allows it).
- **Long polls.** `pumasi-remote` waits 50 seconds per request
  (`PUMASI_WAIT`, at most 90). How long the session proxy keeps a quiet
  request open is not documented; lower it if waits end with errors.

### Why the CLI does not go through the proxy

iroh 1.x can take an HTTP proxy (`HTTPS_PROXY`, CONNECT) and extra CA
roots for its relay connections, so the CLI could in principle reach a
relay from behind a company proxy. It would not help in a Claude Code
cloud session: relays speak WebSocket after the TLS handshake and that
proxy does not pass WebSocket upgrades, the relay host would have to be
allowed, and the CLI keeps its ticket in a file in the session, which
network secrets exist to avoid. The HTTP API is the way in from there.

## GPU drawing (EEVEE, OpenGL, Vulkan)

CUDA jobs need only `gpus = 1`. Jobs that draw (Blender EEVEE, Workbench,
OpenGL or Vulkan renderers) also ask for the graphics driver
capability, and then run only on nodes whose Docker GPUs can draw
(label `gpu.graphics=true`):

```toml
[gpu]
vendor = "nvidia"
capabilities = ["graphics"]
```

`pumasi gpu check --docker-run --image <image>` on a node starts a
container and says whether CUDA, OptiX and drawing work there; the agent
reads the answer. See `examples/blender-render`.
