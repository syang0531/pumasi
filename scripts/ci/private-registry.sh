#!/usr/bin/env bash
# Starts a registry on localhost:5000 that needs a login, pushes busybox
# into it as a private image, and exports the variables the private image
# end-to-end tests read (to $GITHUB_ENV when set).
set -euo pipefail

user=pumasi
pass="ci-$(date +%s)-$RANDOM"
image=localhost:5000/private/busybox:1.36
auth_dir="$(mktemp -d)"

docker run --rm --entrypoint htpasswd httpd:2 -Bbn "$user" "$pass" > "$auth_dir/htpasswd"
docker rm -f pumasi-test-registry >/dev/null 2>&1 || true
docker run -d --name pumasi-test-registry -p 5000:5000 \
  -v "$auth_dir:/auth:ro" \
  -e REGISTRY_AUTH=htpasswd \
  -e REGISTRY_AUTH_HTPASSWD_REALM=pumasi-test \
  -e REGISTRY_AUTH_HTPASSWD_PATH=/auth/htpasswd \
  registry:2 >/dev/null
for _ in $(seq 1 30); do
  curl -s -o /dev/null http://localhost:5000/v2/ && break
  sleep 1
done

docker image inspect busybox:1.36 >/dev/null 2>&1 || docker pull busybox:1.36 >/dev/null
docker tag busybox:1.36 "$image"
echo "$pass" | docker login localhost:5000 -u "$user" --password-stdin >/dev/null
docker push "$image" >/dev/null
docker logout localhost:5000 >/dev/null
# Nothing local may stand in for the private image.
docker rmi "$image" >/dev/null

# Without the login the registry refuses.
code=$(curl -s -o /dev/null -w '%{http_code}' http://localhost:5000/v2/private/busybox/manifests/1.36)
test "$code" = 401

echo "private image $image ready"
if [ -n "${GITHUB_ENV:-}" ]; then
  echo "PUMASI_TEST_REGISTRY=$image" >> "$GITHUB_ENV"
  echo "PUMASI_TEST_REGISTRY_AUTH=$user:$pass" >> "$GITHUB_ENV"
else
  echo "export PUMASI_TEST_REGISTRY=$image PUMASI_TEST_REGISTRY_AUTH=$user:$pass"
fi
