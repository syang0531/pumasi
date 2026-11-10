#!/usr/bin/env bash
# End-to-end check of the control plane and the remote API as OS services:
#
#   1. install the control plane as a system service (`service install
#      cluster`) in a home the invoking user owns, wait for its admin ticket
#      and read the status as that user,
#   2. install `pumasi ui --api-only` as a service and call the API with the
#      admin ticket,
#   3. restart the control plane's service and check both again,
#   4. uninstall both and check nothing is left running.
#
# Usage: service-cluster.sh BINARY WORK_DIR
#
# Needs root (sudo) on Linux and macOS and an administrator shell on
# Windows.
set -euo pipefail

BIN=$1
WORK=$2
PORT=7499

case "$(uname -s)" in
  Linux*) OS=linux ;;
  Darwin*) OS=macos ;;
  MINGW* | MSYS* | CYGWIN*) OS=windows ;;
  *) echo "unsupported OS"; exit 1 ;;
esac

nat() { if [ "$OS" = windows ]; then cygpath -w "$1"; else echo "$1"; fi; }
as_root() { if [ "$OS" = windows ] || [ "$(id -u)" = 0 ]; then "$@"; else sudo "$@"; fi; }

rm -rf "$WORK"
mkdir -p "$WORK/home"
WORK=$(cd "$WORK" && pwd)
H="$WORK/home"

svc() { as_root "$BIN" --home "$(nat "$H")" service "$@"; }
pumasi() { "$BIN" --home "$(nat "$H")" "$@"; }

cleanup() {
  set +e
  svc uninstall ui >/dev/null 2>&1
  svc uninstall cluster >/dev/null 2>&1
}
trap cleanup EXIT

dump() {
  if [ "$OS" = linux ]; then
    sudo journalctl -u pumasi-cluster -u pumasi-ui --no-pager -n 120 || true
  fi
  for f in "$H"/logs/*.log; do
    [ -f "$f" ] && { echo "=== $f"; tail -n 80 "$f"; }
  done
  ls -la "$H" "$H/cluster" 2>/dev/null || true
}

wait_until() { # description, command...
  local what=$1; shift
  for _ in $(seq 1 120); do
    if "$@" >/dev/null 2>&1; then return 0; fi
    sleep 1
  done
  echo "timed out waiting for: $what"
  dump
  exit 1
}

api() { # path
  curl -fsS -H "Authorization: Bearer $(tr -d '\r\n' <"$H/cluster/admin.ticket")" \
    "http://127.0.0.1:$PORT/api/v1$1"
}
cluster_up() { pumasi status | grep -q '^cluster ci-svc'; }
api_up() { api /status | grep -q '"cluster_name":"ci-svc"'; }

echo "--- installing the control plane as a service"
svc install cluster -- --net local --name ci-svc --bind 127.0.0.1:7481
wait_until "the admin ticket" test -r "$H/cluster/admin.ticket"
wait_until "the control plane to answer" cluster_up
pumasi status
# No join ticket is printed into a service's log at every start.
pumasi token ls | tee "$WORK/tokens.txt"
grep -q "no usable join tickets" "$WORK/tokens.txt"

echo "--- installing the remote API as a service"
svc install ui -- --api-only --listen "127.0.0.1:$PORT"
wait_until "the API" curl -fsS "http://127.0.0.1:$PORT/api/openapi.json"
wait_until "the API to reach the control plane" api_up
svc status

echo "--- restarting the control plane's service"
case $OS in
  linux) sudo systemctl restart pumasi-cluster ;;
  macos) sudo launchctl kickstart -k system/pumasi.cluster ;;
  windows) powershell -NoProfile -Command "Restart-Service pumasi-cluster" ;;
esac
sleep 2
wait_until "the control plane after a restart" cluster_up
wait_until "the API after the control plane restarted" api_up

echo "--- uninstalling"
svc uninstall ui
svc uninstall cluster
if svc status; then
  echo "a service is still reported as installed"; exit 1
fi
wait_until "the API to stop" sh -c "! curl -fsS http://127.0.0.1:$PORT/api/openapi.json"
echo "--- ok"
