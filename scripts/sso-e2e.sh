#!/usr/bin/env bash
# SSO end-to-end test against real identity software instead of the mock
# providers in crates/cluster/tests/security.rs: Keycloak for OpenID
# Connect (browser flow with PKCE, device flow, group claims) and OpenLDAP
# for LDAP (service bind, user bind, group lookup), both in Docker.
#
#   scripts/sso-e2e.sh [--bin target/release/pumasi] [--keep]
#
# It starts a local control plane and dashboard, signs people in the way
# a browser and a terminal would, and checks the roles they get, the
# people who must be refused, and the audit log. CI runs it on every pull
# request; it also runs on a Mac with Docker Desktop, OrbStack or Colima.
# Needs bash 4 or later, docker, curl and jq. --keep leaves the
# containers running afterwards (Keycloak admin: admin/admin on
# http://127.0.0.1:18080).
set -euo pipefail

root=$(cd "$(dirname "$0")/.." && pwd)
bin=$root/target/release/pumasi
keep=
while [ $# -gt 0 ]; do
  case $1 in
    --bin) bin=$2; shift 2 ;;
    --keep) keep=1; shift ;;
    -h|--help) sed -n '2,15p' "$0"; exit 0 ;;
    *) echo "unknown option $1"; exit 2 ;;
  esac
done
bin=$(cd "$(dirname "$bin")" && pwd)/$(basename "$bin")
[ -x "$bin" ] || { echo "no pumasi binary at $bin (build it, or pass --bin)"; exit 2; }
for c in docker curl jq; do
  command -v "$c" >/dev/null || { echo "sso-e2e.sh needs $c"; exit 2; }
done

# Fixed ports: the redirect URI is registered in realm.json.
kc_image=keycloak/keycloak:26.0
ldap_image=osixia/openldap:1.5.0
kc=http://127.0.0.1:18080
issuer=$kc/realms/pumasi
ui=http://127.0.0.1:17400
ldap_port=11389
kc_name=pumasi-e2e-keycloak
ldap_name=pumasi-e2e-ldap

work=$(mktemp -d)
pids=()
cleanup() {
  status=$?
  for p in "${pids[@]}"; do kill "$p" 2>/dev/null || true; done
  if [ "$status" != 0 ]; then
    for f in "$work"/*.log; do [ -e "$f" ] && { echo "=== $f"; tail -n 40 "$f"; }; done
    docker logs --tail 40 "$kc_name" 2>&1 | sed 's/^/keycloak: /' || true
  fi
  if [ -z "$keep" ]; then
    docker rm -f "$kc_name" "$ldap_name" >/dev/null 2>&1 || true
  fi
  rm -rf "$work"
}
trap cleanup EXIT

passed=0
ok() { passed=$((passed + 1)); echo "ok   $*"; }
fail() { echo "FAIL $*"; exit 1; }
wait_for() {
  local what=$1; shift
  for _ in $(seq 1 200); do
    if "$@" >/dev/null 2>&1; then return 0; fi
    sleep 0.5
  done
  fail "timed out waiting for $what"
}

# --- Identity providers ---------------------------------------------------
docker rm -f "$kc_name" "$ldap_name" >/dev/null 2>&1 || true
docker run -d --name "$kc_name" -p 127.0.0.1:18080:8080 \
  -e KC_BOOTSTRAP_ADMIN_USERNAME=admin -e KC_BOOTSTRAP_ADMIN_PASSWORD=admin \
  -v "$root/scripts/sso-e2e/realm.json:/opt/keycloak/data/import/realm.json:ro" \
  "$kc_image" start-dev --import-realm >/dev/null
docker run -d --name "$ldap_name" -p "127.0.0.1:$ldap_port:389" \
  -e LDAP_DOMAIN=example.com -e LDAP_ADMIN_PASSWORD=admin-pass \
  "$ldap_image" >/dev/null
ldap_seed() {
  docker exec -i "$ldap_name" ldapadd -x -H ldap://localhost \
    -D cn=admin,dc=example,dc=com -w admin-pass <"$root/scripts/sso-e2e/users.ldif"
}
wait_for "OpenLDAP" ldap_seed
wait_for "Keycloak" curl -fsS "$issuer/.well-known/openid-configuration"
ok "Keycloak and OpenLDAP are up"

# --- Pumasi ---------------------------------------------------------------
export PUMASI_HOME=$work/admin
port=$(( 20000 + RANDOM % 20000 ))
"$bin" cluster start --net local --bind "127.0.0.1:$port" --no-token --name sso-e2e \
  >>"$work/cluster.log" 2>&1 &
pids+=($!)
wait_for "the control plane" "$bin" status
"$bin" ui --listen 127.0.0.1:17400 --public-url "$ui" --no-open >>"$work/ui.log" 2>&1 &
pids+=($!)
wait_for "the dashboard" curl -fsS -o /dev/null "$ui/login"

umask 077
printf '%s' pumasi-e2e-secret >"$work/oidc-secret"
printf '%s' admin-pass >"$work/ldap-secret"
umask 022
"$bin" sso oidc --issuer "$issuer" --client-id pumasi \
  --client-secret-file "$work/oidc-secret" --name "Keycloak" >/dev/null
"$bin" sso ldap --url "ldap://127.0.0.1:$ldap_port" \
  --bind-dn cn=admin,dc=example,dc=com --bind-password-file "$work/ldap-secret" \
  --user-base ou=people,dc=example,dc=com --user-filter '(uid={user})' \
  --group-base ou=groups,dc=example,dc=com >/dev/null
"$bin" sso map group:pumasi-admins admin >/dev/null
"$bin" sso map group:research submitter:research >/dev/null
# Sign-in from a terminal (the device flow) is off until turned on.
"$bin" sso status 2>/dev/null | grep -q 'pumasi login --sso): off' ||
  fail "pumasi sso status: the device flow should start off"
"$bin" sso device on >/dev/null
"$bin" sso status 2>/dev/null | grep -q 'pumasi login --sso): on' ||
  fail "pumasi sso device on did not turn the device flow on"
signin=$("$bin" sso ticket 2>/dev/null | grep -o 'pumasisignin[A-Za-z0-9]*' | head -n1)
[ -n "$signin" ] || fail "pumasi sso ticket printed no sign-in ticket"
ok "SSO configured (OIDC, LDAP, two group rules)"

# --- Helpers that act like a browser --------------------------------------
# The first <form> action in a page, with &amp; decoded.
form_action() {
  grep -o "<form[^>]*action=\"[^\"]*\"" "$1" | head -n1 \
    | sed 's/.*action="//; s/"$//; s/&amp;/\&/g'
}
# Fills Keycloak's sign-in form (page $1) for user $2, password $3, with
# cookie jar $4; prints the next response's Location (empty if none) and
# leaves its body in $1.next.
keycloak_sign_in() {
  local page=$1 user=$2 pass=$3 jar=$4 action
  action=$(form_action "$page")
  [ -n "$action" ] || fail "no sign-in form on Keycloak's page for $user"
  case $action in /*) action=$kc$action ;; esac
  curl -sS -c "$jar" -b "$jar" -o "$page.next" -w '%{redirect_url}' \
    --data-urlencode "username=$user" --data-urlencode "password=$pass" \
    --data credentialId= "$action"
}
# The dashboard's browser flow. Prints the callback's HTTP status; the
# cookie jar ends up holding the dashboard session on success.
browser_flow() {
  local user=$1 pass=$2 jar=$3 next status
  next=$(curl -sS -c "$jar" -b "$jar" -o /dev/null -w '%{redirect_url}' "$ui/sso/start")
  case $next in "$issuer"/*) ;; *) fail "/sso/start sent $user to ${next:-nowhere}" ;; esac
  curl -sS -c "$jar" -b "$jar" -o "$work/kc.html" "$next"
  next=$(keycloak_sign_in "$work/kc.html" "$user" "$pass" "$jar")
  case $next in "$ui/sso/callback?"*) ;; *) fail "Keycloak sent $user to ${next:-nowhere} after the password" ;; esac
  status=$(curl -sS -c "$jar" -b "$jar" -o "$work/callback.html" -w '%{http_code}' "$next")
  echo "$status"
}
# Whether the cookie jar holds a working dashboard session.
signed_in() {
  [ "$(curl -sS -b "$1" -o /dev/null -w '%{http_code}' "$ui/")" = 200 ]
}
# The role of the access ticket named $1, from pumasi access ls.
role_of() {
  "$bin" access ls | awk -v n="$1" '$0 ~ n { for (i = 1; i <= NF; i++) if ($i ~ /^(admin|viewer|submitter)/) { print $i; exit } }'
}

# --- OpenID Connect: browser flow -----------------------------------------
jar=$work/alice.jar
[ "$(browser_flow alice alice-pass "$jar")" = 200 ] || fail "alice's browser sign-in: $(sed 's/<[^>]*>/ /g' "$work/callback.html" | tr -s ' ' | head -c 300)"
signed_in "$jar" || fail "alice has no dashboard session after signing in"
ok "browser flow: alice signs in to the dashboard"

jar=$work/bob.jar
[ "$(browser_flow bob bob-pass "$jar")" = 200 ] || fail "bob's browser sign-in"
signed_in "$jar" || fail "bob has no dashboard session"
ok "browser flow: bob signs in to the dashboard"

jar=$work/carol.jar
status=$(browser_flow carol carol-pass "$jar")
[ "$status" = 401 ] || fail "carol (no matching rule) got HTTP $status, not 401"
signed_in "$jar" && fail "carol has a dashboard session"
ok "browser flow: carol (in no mapped group) is refused"

# A callback whose state did not start in this browser.
jar=$work/mallory.jar
next=$(curl -sS -c "$jar" -b "$jar" -o /dev/null -w '%{redirect_url}' "$ui/sso/start")
curl -sS -c "$jar" -b "$jar" -o "$work/kc.html" "$next"
next=$(keycloak_sign_in "$work/kc.html" alice alice-pass "$jar")
status=$(curl -sS -o /dev/null -w '%{http_code}' "$next")
[ "$status" = 400 ] || fail "a callback without the browser's state got HTTP $status, not 400"
ok "browser flow: a code brought back by another browser is refused"

# --- OpenID Connect: device flow (pumasi login --sso) ---------------------
mkdir -p "$work/alice-cli"
"$bin" --home "$work/alice-cli" login --sso "$signin" >"$work/device.log" 2>&1 &
device=$!
pids+=("$device")
wait_for "the device code" grep -q '^open ' "$work/device.log"
verify=$(sed -n 's/^open //p' "$work/device.log" | head -n1)
case $verify in "$issuer"/*) ;; *) fail "pumasi login --sso printed ${verify:-no URL}" ;; esac
case $verify in *user_code=*) ;; *)
  code=$(sed -n 's/.*enter the code  *//p' "$work/device.log" | head -n1)
  verify="$verify?user_code=$code" ;;
esac
jar=$work/device.jar
curl -sS -L -c "$jar" -b "$jar" -o "$work/kc.html" "$verify"
next=$(keycloak_sign_in "$work/kc.html" alice alice-pass "$jar")
if [ -n "$next" ]; then
  case $next in /*) next=$kc$next ;; esac
  curl -sS -L -c "$jar" -b "$jar" -o "$work/kc.html.next" "$next"
fi
# Keycloak asks for consent on device sign-ins.
if grep -q 'name="accept"' "$work/kc.html.next"; then
  action=$(form_action "$work/kc.html.next")
  case $action in /*) action=$kc$action ;; esac
  code=$(grep -o 'name="code" value="[^"]*"' "$work/kc.html.next" | sed 's/.*value="//; s/"$//')
  curl -sS -L -c "$jar" -b "$jar" -o /dev/null --data-urlencode "code=$code" \
    --data accept=Yes "$action"
fi
for _ in $(seq 1 60); do kill -0 "$device" 2>/dev/null || break; sleep 0.5; done
kill "$device" 2>/dev/null || true
wait "$device" || fail "pumasi login --sso failed: $(cat "$work/device.log")"
grep -q 'signed in as' "$work/device.log" || fail "pumasi login --sso: $(cat "$work/device.log")"
"$bin" --home "$work/alice-cli" status >/dev/null || fail "alice's session ticket from the device flow does not work"
ok "device flow: pumasi login --sso signs alice in from a terminal"

# --- LDAP -----------------------------------------------------------------
mkdir -p "$work/dave-cli"
PUMASI_PASSWORD=dave-pass "$bin" --home "$work/dave-cli" login --ldap dave "$signin" \
  >"$work/ldap.log" 2>&1 || fail "pumasi login --ldap dave: $(cat "$work/ldap.log")"
"$bin" --home "$work/dave-cli" status >/dev/null || fail "dave's session ticket does not work"
ok "LDAP: pumasi login --ldap signs dave in"

if PUMASI_PASSWORD=wrong "$bin" --home "$work/dave-cli" login --ldap dave "$signin" >/dev/null 2>&1; then
  fail "LDAP accepted a wrong password"
fi
ok "LDAP: a wrong password is refused"
if PUMASI_PASSWORD=erin-pass "$bin" --home "$work/dave-cli" login --ldap erin "$signin" >/dev/null 2>&1; then
  fail "LDAP signed in erin, who is in no mapped group"
fi
ok "LDAP: erin (in no mapped group) is refused"
if PUMASI_PASSWORD=x "$bin" --home "$work/dave-cli" login --ldap 'dave)(uid=*' "$signin" >/dev/null 2>&1; then
  fail "LDAP accepted a filter injection"
fi
ok "LDAP: a filter injection in the user name is refused"

jar=$work/dave.jar
status=$(curl -sS -c "$jar" -o /dev/null -w '%{http_code}' -H "Origin: $ui" \
  --data-urlencode user=dave --data-urlencode password=dave-pass "$ui/login/ldap")
[ "$status" = 303 ] || [ "$status" = 302 ] || fail "dashboard LDAP form gave HTTP $status"
signed_in "$jar" || fail "dave has no dashboard session after the LDAP form"
ok "LDAP: the dashboard form signs dave in"

# --- Roles, sessions and the audit log ------------------------------------
"$bin" access ls >"$work/access.txt"
for want in "alice:submitter" "bob:admin" "dave:submitter"; do
  who=${want%%:*} role=${want#*:}
  got=$(role_of "$who")
  case $got in "$role"*) ;; *) fail "$who has role '${got:-none}', expected $role ($(cat "$work/access.txt"))" ;; esac
done
grep -q carol "$work/access.txt" && fail "carol has an access ticket"
grep -q erin "$work/access.txt" && fail "erin has an access ticket"
ok "roles: alice and dave are submitters, bob is an admin, carol and erin have nothing"

"$bin" audit ls --json --limit 500 >"$work/audit.json"
for who in alice bob carol dave erin; do
  grep -q "$who" "$work/audit.json" || fail "the audit log does not mention $who"
done
"$bin" audit verify >/dev/null || fail "pumasi audit verify failed"
ok "audit: every sign-in and refusal is logged, and the chain verifies"

echo "sso e2e passed ($passed checks)"
