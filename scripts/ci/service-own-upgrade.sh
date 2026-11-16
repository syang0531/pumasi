#!/usr/bin/env bash
# End-to-end check of the control plane's own upgrade (1.13) with real
# binaries, the control plane installed as an OS service and no agent on
# its machine:
#
#   1. install the "old" binary's control plane as a service, and the
#      dashboard from the same binary,
#   2. roll out the "new" binary, which this test makes exit right at
#      start (`<cluster>/upgrade/test-exit-at-start`): the control plane
#      installs it over its own, restarts into it, the service manager
#      starts it again until the fourth start puts the old binary back, and
#      `upgrade status` says the control plane rolled back; the dashboard
#      ends up back on the old binary,
#   3. take the test hook and the record of the failure away: the control
#      plane installs the new binary again, restarts into it, stays healthy
#      and commits (keeping the old binary as `previous`); `upgrade status`
#      shows it current on the new version, and the dashboard runs it too,
#   4. a dashboard alone on its machine (another binary, another home, no
#      control plane or agent there) installs the rollout's binary itself,
#   5. uninstall the services.
#
# Usage: service-own-upgrade.sh OLD_BINARY NEW_BINARY WORK_DIR [--foreground] [--from-release]
#
# NEW_BINARY must say a newer version than OLD_BINARY (`--version`).
# --foreground runs the control plane in a shell loop that starts it again
# whenever it exits, standing in for the service manager (no root needed),
# and no dashboard service. --from-release: OLD_BINARY is a published release from
# before 1.13, which installs and restarts into the new binary without a
# trial (nothing to roll back to): steps 2 and 4 are skipped.
# The service mode needs root (sudo) on Linux and macOS and an
# administrator shell on Windows.
set -euo pipefail

OLD=$1
NEW=$2
WORK=$3
shift 3
MODE=service
FROM_RELEASE=0
for a in "$@"; do
  case $a in
    --foreground) MODE=foreground ;;
    --from-release) FROM_RELEASE=1 ;;
    *) echo "unknown option $a"; exit 2 ;;
  esac
done
UI_PORT=7472
LONE_PORT=7473

case "$(uname -s)" in
  Linux*) OS=linux ;;
  Darwin*) OS=macos ;;
  MINGW* | MSYS* | CYGWIN*) OS=windows ;;
  *) echo "unsupported OS"; exit 1 ;;
esac

nat() { if [ "$OS" = windows ]; then cygpath -w "$1"; else echo "$1"; fi; }
sha() {
  if command -v sha256sum >/dev/null; then sha256sum <"$1" | cut -d' ' -f1
  else shasum -a 256 <"$1" | cut -d' ' -f1; fi
}
as_root() { if [ "$OS" = windows ] || [ "$(id -u)" = 0 ]; then "$@"; else sudo "$@"; fi; }

EXT=""
[ "$OS" = windows ] && EXT=".exe"
rm -rf "$WORK"
mkdir -p "$WORK/bin" "$WORK/home"
WORK=$(cd "$WORK" && pwd)
H="$WORK/home"
BIN="$WORK/bin/pumasi$EXT"
cp "$OLD" "$BIN"
chmod +x "$BIN"
UP="$H/cluster/upgrade"
OLD_VERSION=$("$OLD" --version | awk '{print $2}' | tr -d '\r')
NEW_VERSION=$("$NEW" --version | awk '{print $2}' | tr -d '\r')
OLD_SHA=$(sha "$OLD")
NEW_SHA=$(sha "$NEW")
echo "old: $OLD_VERSION ($OLD_SHA)"
echo "new: $NEW_VERSION ($NEW_SHA)"
[ "$OLD_SHA" != "$NEW_SHA" ] || { echo "old and new binaries are identical"; exit 1; }

# The admin commands use the new binary outside the work dir (the one at
# $BIN is replaced underneath; an old release does not list the control
# planes in `upgrade status`).
admin() { "$NEW" --home "$(nat "$H")" "$@"; }
svc() { as_root "$BIN" --home "$(nat "$H")" service "$@"; }

cleanup() {
  set +e
  if [ "$MODE" = service ]; then
    svc uninstall ui >/dev/null 2>&1
    svc uninstall cluster >/dev/null 2>&1
  else
    [ -n "${LOOP_PID:-}" ] && kill "$LOOP_PID" 2>/dev/null
    pkill -f "$BIN" 2>/dev/null
  fi
  [ -n "${LONE_PID:-}" ] && kill "$LONE_PID" 2>/dev/null
}
trap cleanup EXIT

dump() {
  if [ "$OS" = linux ] && [ "$MODE" = service ]; then
    sudo journalctl -u pumasi-cluster -u pumasi-ui --no-pager -n 200 || true
  fi
  for f in "$H"/logs/*.log "$WORK/cluster.log" "$WORK/lone-ui.log"; do
    [ -f "$f" ] && { echo "=== $f"; tail -n 150 "$f"; }
  done
  ls -la "$WORK/bin" "$UP" 2>/dev/null || true
  for f in "$UP"/*.json; do
    [ -f "$f" ] && { echo "=== $f"; cat "$f"; }
  done
}

wait_until() { # description, command...
  local what=$1; shift
  for _ in $(seq 1 300); do
    if "$@" >/dev/null 2>&1; then return 0; fi
    sleep 1
  done
  echo "timed out waiting for: $what"
  admin upgrade status || true
  dump
  exit 1
}

cluster_up() { admin status | grep -q '^cluster ci-own'; }
runs() { [ "$(sha "$BIN")" = "$1" ]; }
# The dashboard says its version in its stylesheet's URL.
ui_runs() { curl -fsS "http://127.0.0.1:${2:-$UI_PORT}/login" | grep -qF "app.css?v=$1\""; }
cp_state() { # version, state text
  admin upgrade status >"$WORK/cps.txt" 2>&1 || return 1
  grep -E "^[0-9a-f]+ +leader +[^ ]+ +${1//+/\\+} +$2" "$WORK/cps.txt"
}

echo "--- starting the old control plane ($MODE)"
OPTS=(--net local --name ci-own --bind 127.0.0.1:7471 --no-token)
if [ "$MODE" = service ]; then
  svc install cluster -- "${OPTS[@]}"
else
  (
    # A service manager: starts it again whenever it exits.
    while true; do
      "$BIN" --home "$(nat "$H")" cluster start "${OPTS[@]}" >>"$WORK/cluster.log" 2>&1 || true
      sleep 1
    done
  ) &
  LOOP_PID=$!
fi
wait_until "the admin ticket" test -r "$H/cluster/admin.ticket"
wait_until "the control plane to answer" cluster_up
if [ "$MODE" = service ]; then
  svc install ui -- --listen "127.0.0.1:$UI_PORT"
  wait_until "the dashboard" ui_runs "$OLD_VERSION"
fi

if [ "$FROM_RELEASE" = 0 ]; then
  echo "--- a dashboard alone on its machine"
  LONE="$WORK/lone/pumasi$EXT"
  mkdir -p "$WORK/lone" "$WORK/lone-home"
  cp "$OLD" "$LONE"
  chmod +x "$LONE"
  PUMASI_TEST_OWN_UPGRADE_SECS=5 "$LONE" --home "$(nat "$WORK/lone-home")" ui --no-open \
    --admin "$(tr -d '\r\n' <"$H/cluster/admin.ticket")" --listen "127.0.0.1:$LONE_PORT" \
    >"$WORK/lone-ui.log" 2>&1 &
  LONE_PID=$!
  wait_until "the lone dashboard" ui_runs "$OLD_VERSION" "$LONE_PORT"
fi

if [ "$FROM_RELEASE" = 0 ]; then
echo "--- rolling out a new binary that dies at start"
mkdir -p "$UP"
printf '%s' "$NEW_VERSION" >"$UP/test-exit-at-start"
admin upgrade rollout "$(nat "$NEW")"
# It installs the new binary within 30 s, restarts into it, and the fourth
# start (each a few seconds apart under the service manager) puts the old
# one back and records why.
wait_until "the rollback to be recorded" grep -q "$NEW_VERSION" "$UP/failed.json"
wait_until "the old binary back in place" runs "$OLD_SHA"
wait_until "the control plane after the rollback" cluster_up
wait_until "upgrade status to show the rollback" cp_state "$OLD_VERSION" "rolled back: .*started 3 times"
cat "$WORK/cps.txt"
if [ -e "$UP/pending.json" ]; then
  echo "a trial is still pending after the rollback"; dump; exit 1
fi
# A rolled-back binary is not installed again.
sleep 40
if ! runs "$OLD_SHA"; then
  echo "the rolled-back binary was installed again"; dump; exit 1
fi
if [ "$MODE" = service ]; then
  # It follows the binary (every 30 s): to the new one if it saw it in
  # time, and back.
  wait_until "the dashboard back on the old binary" ui_runs "$OLD_VERSION"
  if grep -q "restarting .*$NEW_VERSION\|into pumasi $NEW_VERSION" "$H/logs/ui.log"; then
    echo "(the dashboard went to the new binary and back)"
  fi
fi

echo "--- the same binary, working this time"
as_root rm -f "$UP/test-exit-at-start" "$UP/failed.json"
# The control plane running now started after the rollback and has not
# tried the binary itself: it installs it within 30 s.
else
echo "--- rolling out the new binary to a release from before 1.13"
admin upgrade rollout "$(nat "$NEW")"
fi
wait_until "the new binary in place" runs "$NEW_SHA"
wait_until "the control plane on the new binary" cluster_up
wait_until "the upgrade to commit" sh -c "! test -e '$UP/pending.json'"
wait_until "upgrade status to show it current" cp_state "$NEW_VERSION" "current"
cat "$WORK/cps.txt"
if [ "$FROM_RELEASE" = 0 ]; then
  test -s "$UP/previous$EXT"
  [ "$(sha "$UP/previous$EXT")" = "$OLD_SHA" ] || { echo "previous is not the old binary"; dump; exit 1; }
fi
if [ "$MODE" = service ]; then
  wait_until "the dashboard on the new binary" ui_runs "$NEW_VERSION"
fi
if [ "$FROM_RELEASE" = 0 ]; then
  # It installed the rollout's binary over its own and restarted into it.
  wait_until "the lone dashboard on the new binary" ui_runs "$NEW_VERSION" "$LONE_PORT"
  [ "$(sha "$LONE")" = "$NEW_SHA" ] || { echo "the lone dashboard's binary is not the new one"; dump; exit 1; }
  grep -q "installed pumasi $NEW_VERSION" "$WORK/lone-ui.log"
fi

if [ "$MODE" = service ]; then
  echo "--- uninstalling"
  svc uninstall ui
  svc uninstall cluster
  if svc status; then
    echo "the service is still reported as installed"; exit 1
  fi
fi
echo "--- ok"
