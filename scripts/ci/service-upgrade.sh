#!/usr/bin/env bash
# End-to-end check of the agent as an OS service and of a cluster-driven
# upgrade with real binaries:
#
#   1. start a control plane (local network mode),
#   2. install the agent (the "old" binary) as a service with a join ticket,
#      wait until it is online, restart the service, wait again,
#   3. `pumasi upgrade rollout` the "new" binary and wait until the node runs
#      it (the agent downloads it, verifies it, swaps it in and restarts),
#   4. uninstall the service and check the node goes offline.
#
# Usage: service-upgrade.sh OLD_BINARY NEW_BINARY WORK_DIR [--foreground]
#
# --foreground runs the agent as a plain background process instead of a
# service (no root needed), which still exercises the restart in place.
# The service mode needs root (sudo) on Linux and macOS and an
# administrator shell on Windows.
set -euo pipefail

OLD=$1
NEW=$2
WORK=$3
MODE=${4:-service}

case "$(uname -s)" in
  Linux*) OS=linux ;;
  Darwin*) OS=macos ;;
  MINGW* | MSYS* | CYGWIN*) OS=windows ;;
  *) echo "unsupported OS"; exit 1 ;;
esac

# Paths as the native binary expects them.
nat() { if [ "$OS" = windows ]; then cygpath -w "$1"; else echo "$1"; fi; }
sha() {
  if command -v sha256sum >/dev/null; then sha256sum <"$1" | cut -d' ' -f1
  else shasum -a 256 <"$1" | cut -d' ' -f1; fi
}
as_root() { if [ "$OS" = windows ] || [ "$(id -u)" = 0 ]; then "$@"; else sudo "$@"; fi; }

EXT=""
[ "$OS" = windows ] && EXT=".exe"
rm -rf "$WORK"
mkdir -p "$WORK/bin"
WORK=$(cd "$WORK" && pwd)
AGENT_BIN="$WORK/bin/pumasi$EXT"
cp "$OLD" "$AGENT_BIN"
chmod +x "$AGENT_BIN"
CHOME="$WORK/cluster-home"
AHOME="$WORK/agent-home"
NODE=ci-node

cleanup() {
  set +e
  if [ "$MODE" = service ]; then
    as_root "$AGENT_BIN" --home "$(nat "$AHOME")" service uninstall >/dev/null 2>&1
  fi
  [ -n "${AGENT_PID:-}" ] && kill "$AGENT_PID" 2>/dev/null
  [ -n "${CLUSTER_PID:-}" ] && kill "$CLUSTER_PID" 2>/dev/null
}
trap cleanup EXIT

dump() {
  echo "=== cluster log ==="; tail -n 50 "$WORK/cluster.log" || true
  echo "=== agent log ==="
  tail -n 80 "$AHOME/logs/agent.log" 2>/dev/null || true
  tail -n 80 "$WORK/agent.log" 2>/dev/null || true
  if [ "$OS" = linux ] && [ "$MODE" = service ]; then
    sudo journalctl -u pumasi-agent --no-pager -n 80 || true
  fi
}

admin() { "$OLD" --home "$(nat "$CHOME")" "$@"; }

wait_until() { # description, command...
  local what=$1; shift
  for _ in $(seq 1 120); do
    if "$@" >/dev/null 2>&1; then return 0; fi
    sleep 1
  done
  echo "timed out waiting for: $what"
  admin status || true
  dump
  exit 1
}

node_online() { admin status | grep -E "^[0-9a-f]+ +$NODE +online"; }
node_offline() { ! node_online; }

echo "--- starting the control plane"
"$OLD" --home "$(nat "$CHOME")" cluster start --net local --no-token --name ci \
  >"$WORK/cluster.log" 2>&1 &
CLUSTER_PID=$!
wait_until "the control plane" test -s "$CHOME/cluster/admin.ticket"
TICKET=$(admin token --ttl 1h)

echo "--- installing the agent ($MODE)"
if [ "$MODE" = service ]; then
  as_root "$AGENT_BIN" --home "$(nat "$AHOME")" service install --ticket "$TICKET" \
    -- --net local --name "$NODE" --slots 1
  as_root "$AGENT_BIN" --home "$(nat "$AHOME")" service status
  if [ "$OS" = windows ]; then
    # The firewall rule for direct connections, with a plain program path
    # (netsh refuses \\?\ paths).
    powershell -NoProfile -Command "\$p = (Get-NetFirewallRule -DisplayName 'Pumasi agent (UDP in)' -ErrorAction Stop | Get-NetFirewallApplicationFilter).Program; \$p; if (-not \$p -or \$p.StartsWith('\\\\?')) { exit 1 }"
  fi
else
  "$AGENT_BIN" --home "$(nat "$AHOME")" agent join "$TICKET" --net local --name "$NODE" --slots 1 \
    >"$WORK/agent.log" 2>&1 &
  AGENT_PID=$!
fi
wait_until "the node to come online" node_online
admin status

if [ "$MODE" = service ]; then
  echo "--- restarting the service"
  case $OS in
    linux) sudo systemctl restart pumasi-agent ;;
    macos) sudo launchctl kickstart -k system/pumasi.agent ;;
    windows) powershell -NoProfile -Command "Restart-Service pumasi-agent" ;;
  esac
  sleep 2
  wait_until "the node to rejoin after a restart" node_online
fi

echo "--- rolling out the new binary"
NEW_SHA=$(sha "$NEW")
OLD_SHA=$(sha "$OLD")
[ "$NEW_SHA" != "$OLD_SHA" ] || { echo "old and new binaries are identical"; exit 1; }
if ! admin upgrade rollout "$(nat "$NEW")" --wait --timeout 5m; then
  dump
  exit 1
fi
admin upgrade status | tee "$WORK/rollout.txt"
grep -E " $NODE .* current" "$WORK/rollout.txt"
INSTALLED_SHA=$(sha "$AGENT_BIN")
if [ "$INSTALLED_SHA" != "$NEW_SHA" ]; then
  echo "installed binary $INSTALLED_SHA is not the new one $NEW_SHA"
  dump
  exit 1
fi
wait_until "the node to be online with the new binary" node_online
NEW_VERSION=$("$NEW" --version | awk '{print $2}')
admin status | grep -E "$NODE +online .*${NEW_VERSION//+/\\+}" || {
  echo "node does not report version $NEW_VERSION"; admin status; dump; exit 1;
}
if [ "$MODE" = service ]; then
  as_root "$AGENT_BIN" --home "$(nat "$AHOME")" service status
fi

if [ "$MODE" = service ]; then
  echo "--- uninstalling the service"
  as_root "$AGENT_BIN" --home "$(nat "$AHOME")" service uninstall
  wait_until "the node to go offline" node_offline
  if as_root "$AGENT_BIN" --home "$(nat "$AHOME")" service status; then
    echo "service still reported as running"; exit 1
  fi
fi
echo "--- ok"
