#!/usr/bin/env bash
# Runs the examples of docs/api.md (phase 15): its `sh` blocks and its
# `<!-- ci: ... -->` lines, top to bottom in one shell, against a local
# cluster (control plane, one agent) with `pumasi ui --api-only` in front.
# A failing example fails CI, so the page cannot drift from the API.
#
#   scripts/ci/api-doc.sh [path/to/pumasi]
set -euo pipefail

root=$(cd "$(dirname "$0")/../.." && pwd)
bin=${1:-$root/target/debug/pumasi}
bin=$(cd "$(dirname "$bin")" && pwd)/$(basename "$bin")
work=$(mktemp -d)
pids=()
cleanup() {
  status=$?
  for p in "${pids[@]}"; do kill "$p" 2>/dev/null || true; done
  if [ "$status" != 0 ]; then
    for f in "$work"/*.log; do echo "=== $f"; tail -n 40 "$f"; done
  fi
  rm -rf "$work"
}
trap cleanup EXIT

wait_for() {
  local what=$1; shift
  for _ in $(seq 1 200); do
    if "$@" >/dev/null 2>&1; then return 0; fi
    sleep 0.3
  done
  echo "timed out waiting for $what"; return 1
}

# The admin's machine: control plane, one agent, the API.
export PUMASI_HOME=$work/admin
"$bin" cluster start --net local --no-token --name api-doc >"$work/cluster.log" 2>&1 &
pids+=($!)
wait_for "the control plane" test -s "$PUMASI_HOME/cluster/admin.ticket"
join=$("$bin" token --ttl 1h)
"$bin" --home "$work/node" agent join "$join" --net local --name node-a --slots 2 \
  >"$work/agent.log" 2>&1 &
pids+=($!)
wait_for "the agent" sh -c "'$bin' status | grep -q ' online'"
"$bin" ui --api-only --listen 127.0.0.1:0 --no-open --url-file "$work/ui.url" \
  --real-ip-header CF-Connecting-IP >"$work/ui.log" 2>&1 &
pids+=($!)
wait_for "the API" test -s "$work/ui.url"
url=$(cat "$work/ui.url")
export PUMASI_URL=${url%/}
wait_for "/healthz" curl -fsS "$PUMASI_URL/healthz"

# The page's examples, in a directory with the module they submit.
python3 - "$root/docs/api.md" >"$work/examples.sh" <<'PY'
import re, sys
text = open(sys.argv[1], encoding="utf-8").read()
out = ["set -euxo pipefail"]
for m in re.finditer(r"^```sh\n(.*?)^```$|^<!-- ci: (.*?) -->$", text, re.S | re.M):
    out.append(m.group(1) if m.group(1) is not None else m.group(2) + "\n")
print("\n".join(out))
PY
mkdir -p "$work/client"
cp "$root/crates/dashboard/e2e/fixtures/ticker.wasm" "$work/client/"
export PATH="$(dirname "$bin"):$root/examples/remote-client:$PATH"
(cd "$work/client" && timeout 600 bash "$work/examples.sh")
echo "docs/api.md: every example ran"
