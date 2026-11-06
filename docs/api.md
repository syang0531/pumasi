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
anything else with `403`, whoever submits it:

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

`none` or `any` lifts one limit, `--clear` all of them. The policy can
also live in the cluster file (`pumasi apply`):

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
(trusted only on connections from this machine). Keep it running as a
service the same way as the agent (systemd, launchd or Task Scheduler).

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

## On the client

Two values, from the admin:

```bash
export PUMASI_URL=https://pumasi.example.com
export PUMASI_TICKET=pumasiadmin...
```
<!-- ci: export PUMASI_TICKET="$TICKET" -->

Who am I, and what can the cluster do for my account right now:

```sh
curl -fsS -H "Authorization: Bearer $PUMASI_TICKET" "$PUMASI_URL/api/v1/whoami"
curl -fsS -H "Authorization: Bearer $PUMASI_TICKET" "$PUMASI_URL/api/v1/status" \
  | jq '.capacity | {ready, ready_gpu, nodes: [.nodes[] | {node, free_slots, reason}]}'
```

`capacity` counts the online nodes that would take this account's work
now, per executor, and says why each other node would not (offline,
paused, full, no Docker, the selector, a taint, the quota...).

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

| Status | Means |
| --- | --- |
| `400` | the request or `job.toml` is wrong |
| `401` | no ticket, or a wrong, expired or revoked one |
| `403` | the ticket's role or the account's policy does not allow it |
| `404` | no such job (or not this account's) |
| `429` | too many wrong tickets from this address; see `Retry-After` |
| `502` | `pumasi ui` cannot reach the control plane |

### Cancel

```sh
curl -fsS -X POST -H "Authorization: Bearer $PUMASI_TICKET" \
  "$PUMASI_URL/api/v1/jobs/$JOB/cancel"
```

Every route, with its parameters, is in `$PUMASI_URL/api/openapi.json`
(OpenAPI 3.1), which code generators and tools such as Postman read.

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
