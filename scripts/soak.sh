#!/usr/bin/env bash
# Soak test (phase 17): a local cluster (control plane, agents, the HTTP
# API) under a steady stream of jobs for a long time, with agents and the
# control plane killed and restarted along the way. It fails when a job
# is lost or stuck, when the audit log stops verifying, or when memory or
# open files keep growing after the warm-up.
#
#   scripts/soak.sh [--bin target/release/pumasi] [--duration 72h]
#                   [--agents 3] [--report DIR]
#                   [--agent-restart-every 30m] [--cluster-restart-every 6h]
#
# The 1.0 release waits for a 72-hour run (the default). CI runs a short
# one (`--duration 10m`) on every pull request. Linux and macOS; needs
# bash 4 or later, curl and jq. The report directory gets samples.csv (time, process, RSS
# in KiB, open files), jobs.csv and summary.txt.
set -euo pipefail

root=$(cd "$(dirname "$0")/.." && pwd)
bin=$root/target/release/pumasi
duration=72h
agents=3
report=
agent_every=30m
cluster_every=6h
while [ $# -gt 0 ]; do
  case $1 in
    --bin) bin=$2; shift 2 ;;
    --duration) duration=$2; shift 2 ;;
    --agents) agents=$2; shift 2 ;;
    --report) report=$2; shift 2 ;;
    --agent-restart-every) agent_every=$2; shift 2 ;;
    --cluster-restart-every) cluster_every=$2; shift 2 ;;
    -h|--help) sed -n '2,16p' "$0"; exit 0 ;;
    *) echo "unknown option $1"; exit 2 ;;
  esac
done

secs() {
  local v=$1
  case $v in
    *d) echo $(( ${v%d} * 86400 )) ;;
    *h) echo $(( ${v%h} * 3600 )) ;;
    *m) echo $(( ${v%m} * 60 )) ;;
    *s) echo "${v%s}" ;;
    *) echo "$v" ;;
  esac
}
total=$(secs "$duration")
agent_every=$(secs "$agent_every")
cluster_every=$(secs "$cluster_every")
bin=$(cd "$(dirname "$bin")" && pwd)/$(basename "$bin")
[ -x "$bin" ] || { echo "no pumasi binary at $bin (build it, or pass --bin)"; exit 2; }
command -v jq >/dev/null || { echo "soak.sh needs jq"; exit 2; }

work=$(mktemp -d)
report=${report:-$work/report}
mkdir -p "$report"
report=$(cd "$report" && pwd)
echo "time,process,rss_kib,open_files" >"$report/samples.csv"
echo "time,job,state" >"$report/jobs.csv"
declare -A pid
cleanup() {
  status=$?
  for p in "${pid[@]}"; do kill "$p" 2>/dev/null || true; done
  if [ "$status" != 0 ]; then
    for f in "$work"/*.log; do echo "=== $f"; tail -n 30 "$f"; done
  fi
  cp "$work"/*.log "$report/" 2>/dev/null || true
  rm -rf "$work/admin" "$work"/node-*
}
trap cleanup EXIT

wait_for() {
  local what=$1; shift
  for _ in $(seq 1 300); do
    if "$@" >/dev/null 2>&1; then return 0; fi
    sleep 0.3
  done
  echo "timed out waiting for $what"; return 1
}
log() { echo "[$(date -u +%FT%TZ)] $*" | tee -a "$report/summary.txt"; }

export PUMASI_HOME=$work/admin
# A fixed address, so agents find the control plane again after a restart.
port=$(( 20000 + RANDOM % 20000 ))
start_cluster() {
  "$bin" cluster start --net local --bind "127.0.0.1:$port" --no-token --name soak \
    >>"$work/cluster.log" 2>&1 &
  pid[cluster]=$!
  wait_for "the control plane" sh -c "'$bin' status"
}
start_agent() {
  local i=$1
  if [ -e "$work/node-$i/agent/membership.json" ]; then
    "$bin" --home "$work/node-$i" agent start --net local >>"$work/agent-$i.log" 2>&1 &
  else
    "$bin" --home "$work/node-$i" agent join "$join" --net local --name "soak-$i" --slots 2 \
      >>"$work/agent-$i.log" 2>&1 &
  fi
  pid[agent-$i]=$!
}

start_cluster
join=$("$bin" token --ttl 1h --uses "$agents")
for i in $(seq 1 "$agents"); do start_agent "$i"; done
wait_for "the agents" sh -c "[ \$('$bin' status | grep -c ' online') -ge $agents ]"
"$bin" ui --api-only --listen 127.0.0.1:0 --no-open --url-file "$work/ui.url" \
  >>"$work/ui.log" 2>&1 &
pid[api]=$!
wait_for "the API" test -s "$work/ui.url"
url=$(cat "$work/ui.url")
url=${url%/}
ticket=$("$bin" access create soak --role submitter --account owner --ttl 30d 2>/dev/null \
  | cut -d' ' -f3)
auth=(-H "Authorization: Bearer $ticket")

mkdir -p "$work/client"
cp "$root/crates/dashboard/e2e/fixtures/ticker.wasm" "$work/client/"
cat >"$work/client/job.toml" <<EOF
name = "soak"
module = "ticker.wasm"
args = ["1"]
shards = $((agents * 2))
deterministic = false
queue_timeout = "20m"
EOF

sample() {
  local now=$1 name p rss fds
  for name in "${!pid[@]}"; do
    p=${pid[$name]}
    rss=$(ps -o rss= -p "$p" 2>/dev/null | tr -d ' ') || continue
    [ -n "$rss" ] || continue
    if [ -d "/proc/$p/fd" ]; then
      fds=$(ls "/proc/$p/fd" 2>/dev/null | wc -l | tr -d ' ')
    else
      fds=$(lsof -p "$p" 2>/dev/null | wc -l | tr -d ' ')
    fi
    echo "$now,$name,$rss,$fds" >>"$report/samples.csv"
  done
}

# Jobs are submitted through the API and followed until they finish.
submitted=0 succeeded=0 failed=0 submit_errors=0
pending=()
submit() {
  local id
  if id=$(cd "$work/client" && curl -fsS -m 30 "${auth[@]}" \
      -F files=@job.toml -F files=@ticker.wasm "$url/api/v1/jobs" | jq -re .id); then
    pending+=("$1:$id")
    submitted=$((submitted + 1))
  else
    submit_errors=$((submit_errors + 1))
  fi
}
check_jobs() {
  local now=$1 left=() entry since id state
  for entry in "${pending[@]}"; do
    since=${entry%%:*} id=${entry#*:}
    state=$(curl -fsS -m 30 "${auth[@]}" "$url/api/v1/jobs/$id" 2>/dev/null \
      | jq -r .summary.state) || state=unknown
    case $state in
      Succeeded) succeeded=$((succeeded + 1)); echo "$now,$id,$state" >>"$report/jobs.csv" ;;
      Failed|Cancelled) failed=$((failed + 1)); echo "$now,$id,$state" >>"$report/jobs.csv"
        log "job $id $state" ;;
      *) if [ $((now - since)) -gt 1800 ]; then
           failed=$((failed + 1)); echo "$now,$id,stuck-$state" >>"$report/jobs.csv"
           log "job $id still $state after 30 minutes"
         else
           left+=("$entry")
         fi ;;
    esac
  done
  pending=("${left[@]+"${left[@]}"}")
}

start=$(date +%s)
end=$((start + total))
next_agent=$((start + agent_every))
next_cluster=$((start + cluster_every))
next_sample=$start
next_audit=$((start + 60))
restarts_agent=0 restarts_cluster=0 audit_ok=0 audit_bad=0
log "soak: $duration, $agents agents, binary $("$bin" --version)"
while :; do
  now=$(date +%s)
  [ "$now" -lt "$end" ] || break
  # About one job in flight per agent.
  if [ "${#pending[@]}" -lt "$agents" ]; then submit "$now"; fi
  if [ "$now" -ge "$next_sample" ]; then
    sample "$now"
    check_jobs "$now"
    next_sample=$((now + 30))
  fi
  if [ "$now" -ge "$next_audit" ]; then
    if "$bin" audit verify >/dev/null 2>>"$work/audit.log"; then
      audit_ok=$((audit_ok + 1))
    else
      audit_bad=$((audit_bad + 1)); log "audit log did not verify"
    fi
    next_audit=$((now + 600))
  fi
  if [ "$now" -ge "$next_agent" ]; then
    i=$(( (restarts_agent % agents) + 1 ))
    log "killing agent $i"
    kill -9 "${pid[agent-$i]}" 2>/dev/null || true
    wait "${pid[agent-$i]}" 2>/dev/null || true
    sleep 2
    start_agent "$i"
    restarts_agent=$((restarts_agent + 1))
    next_agent=$((now + agent_every))
  fi
  if [ "$now" -ge "$next_cluster" ]; then
    log "killing the control plane"
    kill -9 "${pid[cluster]}" 2>/dev/null || true
    wait "${pid[cluster]}" 2>/dev/null || true
    sleep 2
    start_cluster
    restarts_cluster=$((restarts_cluster + 1))
    next_cluster=$((now + cluster_every))
  fi
  sleep 2
done

# Let the last jobs finish.
for _ in $(seq 1 60); do
  [ "${#pending[@]}" -gt 0 ] || break
  sleep 5
  check_jobs "$(date +%s)"
done
for entry in "${pending[@]+"${pending[@]}"}"; do
  failed=$((failed + 1)); log "job ${entry#*:} never finished"
done
"$bin" audit verify >"$report/audit-verify.txt" 2>&1 || audit_bad=$((audit_bad + 1))

# Growth: the median of the last tenth of the run against the median of
# the second tenth (after the warm-up). Agents restart, so their samples
# restart too; the check uses the control plane and the API, which only
# restart with the control plane (the API never).
growth=$(awk -F, -v start="$start" -v total="$total" '
  NR > 1 {
    t = ($1 - start) / total
    if (t >= 0.1 && t < 0.2) { early[$2] = early[$2] " " $3; earlyf[$2] = earlyf[$2] " " $4 }
    if (t >= 0.9) { late[$2] = late[$2] " " $3; latef[$2] = latef[$2] " " $4 }
  }
  function median(s,   a, n, i, j, x) {
    n = split(s, a, " ")
    for (i = 2; i <= n; i++) { x = a[i]; for (j = i - 1; j >= 1 && a[j] > x; j--) a[j + 1] = a[j]; a[j + 1] = x }
    return n ? a[int((n + 1) / 2)] : 0
  }
  END {
    for (p in late) {
      if (p ~ /^agent/ || !(p in early)) continue
      e = median(early[p]); l = median(late[p]); ef = median(earlyf[p]); lf = median(latef[p])
      bad = (l > e * 1.5 + 65536) || (lf > ef * 2 + 64)
      printf "%s rss %d -> %d KiB, open files %d -> %d%s\n", p, e, l, ef, lf, bad ? "  GROWING" : ""
    }
  }' "$report/samples.csv")

{
  echo
  echo "jobs: $submitted submitted, $succeeded succeeded, $failed failed or stuck, $submit_errors submissions refused or timed out"
  echo "restarts: $restarts_agent agents, $restarts_cluster control plane"
  echo "audit verify: $audit_ok passed, $audit_bad failed"
  echo "$growth"
} | tee -a "$report/summary.txt"

ok=1
[ "$failed" = 0 ] || ok=0
[ "$audit_bad" = 0 ] || ok=0
[ "$succeeded" -gt 0 ] || ok=0
# Submissions fail while the control plane restarts; a few are expected.
[ "$submit_errors" -le $((restarts_cluster * 20 + 5)) ] || ok=0
if echo "$growth" | grep -q GROWING; then ok=0; fi
if [ "$ok" = 1 ]; then
  echo "soak passed; report in $report"
else
  echo "soak FAILED; report in $report"
  exit 1
fi
