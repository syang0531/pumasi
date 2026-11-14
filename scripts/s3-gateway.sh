#!/usr/bin/env bash
# The S3 gateway of `pumasi ui` (1.8) against real S3 clients: the AWS CLI
# (`aws s3 cp`, `aws s3 sync`, multipart uploads, s3api calls), rclone
# (`rclone sync`, `rclone check`) and boto3 (presigned URLs, checksums).
#
#   scripts/s3-gateway.sh [--bin target/release/pumasi]
#
# It starts a local control plane and dashboard, makes S3 keys from the
# admin ticket and from a submitter and a viewer ticket, and checks what
# each may do. Needs aws (v1 or v2), rclone, python3 with boto3, curl.
set -euo pipefail

root=$(cd "$(dirname "$0")/.." && pwd)
bin=$root/target/release/pumasi
while [ $# -gt 0 ]; do
  case $1 in
    --bin) bin=$2; shift 2 ;;
    -h|--help) sed -n '2,10p' "$0"; exit 0 ;;
    *) echo "unknown option $1"; exit 2 ;;
  esac
done
bin=$(cd "$(dirname "$bin")" && pwd)/$(basename "$bin")
[ -x "$bin" ] || { echo "no pumasi binary at $bin (build it, or pass --bin)"; exit 2; }
for c in aws rclone python3 curl; do
  command -v "$c" >/dev/null || { echo "s3-gateway.sh needs $c"; exit 2; }
done
python3 -c 'import boto3' 2>/dev/null || { echo "s3-gateway.sh needs boto3"; exit 2; }

work=$(mktemp -d)
pids=()
cleanup() {
  status=$?
  for p in "${pids[@]}"; do kill "$p" 2>/dev/null || true; done
  if [ "$status" != 0 ]; then
    for f in "$work"/*.log; do [ -e "$f" ] && { echo "=== $f"; tail -n 60 "$f"; }; done
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

export PUMASI_HOME=$work/admin
port=$(( 20000 + RANDOM % 20000 ))
uiport=$(( 40000 + RANDOM % 20000 ))
ep=http://127.0.0.1:$uiport/s3
"$bin" cluster start --net local --bind "127.0.0.1:$port" --no-token --name s3-e2e \
  >>"$work/cluster.log" 2>&1 &
pids+=($!)
wait_for "the control plane" "$bin" status
"$bin" ui --listen "127.0.0.1:$uiport" --no-open --max-upload 1GiB >>"$work/ui.log" 2>&1 &
pids+=($!)
wait_for "the dashboard" curl -fsS -o /dev/null "http://127.0.0.1:$uiport/healthz"

# A key: prints "ID SECRET".
make_key() {
  "$bin" s3 create-key --label "$1" "${@:2}" 2>/dev/null |
    awk '/access key id/ { id = $NF } /secret access key/ { s = $NF } END { print id, s }'
}
read -r admin_id admin_secret <<<"$(make_key admin)"
[ -n "$admin_secret" ] || fail "pumasi s3 create-key printed no key"
"$bin" s3 keys | grep -q "$admin_id" || fail "pumasi s3 keys does not list $admin_id"
ok "S3 key made from the admin ticket"

# The AWS CLI and boto3 read these (and nothing from the environment).
unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN AWS_PROFILE AWS_DEFAULT_PROFILE \
  AWS_REGION AWS_DEFAULT_REGION AWS_ENDPOINT_URL AWS_ENDPOINT_URL_S3 AWS_CA_BUNDLE
mkdir -p "$work/aws"
export AWS_CONFIG_FILE=$work/aws/config AWS_SHARED_CREDENTIALS_FILE=$work/aws/credentials
cat >"$AWS_CONFIG_FILE" <<EOF
[default]
region = us-east-1
s3 =
    addressing_style = path
    multipart_threshold = 8MB
    multipart_chunksize = 8MB
EOF
cat >"$AWS_SHARED_CREDENTIALS_FILE" <<EOF
[default]
aws_access_key_id = $admin_id
aws_secret_access_key = $admin_secret
EOF
s3() { aws --endpoint-url "$ep" s3 "$@"; }
s3api() { aws --endpoint-url "$ep" s3api "$@"; }

# Unsigned and wrongly signed requests are refused.
[ "$(curl -sS -o /dev/null -w '%{http_code}' "$ep/")" = 403 ] || fail "an anonymous request was not refused"
AWS_SECRET_ACCESS_KEY=wrong AWS_ACCESS_KEY_ID=$admin_id aws --endpoint-url "$ep" s3 ls \
  >"$work/wrong.out" 2>&1 && fail "a wrong secret was accepted"
grep -q SignatureDoesNotMatch "$work/wrong.out" || fail "a wrong secret: $(cat "$work/wrong.out")"
ok "unsigned and wrongly signed requests are refused"

s3 mb s3://photos >/dev/null || fail "aws s3 mb"
s3 ls | grep -q photos || fail "aws s3 ls does not list the new bucket"
"$bin" store ls | grep -q photos || fail "the bucket is not a dataset"
ok "aws s3 mb makes a dataset"

mkdir -p "$work/src/sub/deeper"
echo "hello s3" >"$work/src/hello.txt"
head -c 300000 /dev/urandom >"$work/src/sub/random.bin"
printf 'a b' >"$work/src/sub/deeper/with space+plus.txt"
: >"$work/src/empty.txt"
s3 cp "$work/src/hello.txt" s3://photos/hello.txt >/dev/null || fail "aws s3 cp (upload)"
s3 cp s3://photos/hello.txt "$work/hello.back" >/dev/null || fail "aws s3 cp (download)"
cmp -s "$work/src/hello.txt" "$work/hello.back" || fail "the file came back different"
etag=$(s3api head-object --bucket photos --key hello.txt --query ETag --output text)
[ "$etag" = "\"$(md5sum <"$work/src/hello.txt" | cut -d' ' -f1)\"" ] ||
  fail "the ETag $etag is not the file's MD5"
ok "aws s3 cp up and down, ETag is the MD5"

# A multipart upload (40 MB in 8 MB parts), read back whole and in ranges.
head -c $((40 * 1024 * 1024)) /dev/urandom >"$work/big.bin"
s3 cp "$work/big.bin" s3://photos/big/big.bin >/dev/null || fail "aws s3 cp (multipart)"
etag=$(s3api head-object --bucket photos --key big/big.bin --query ETag --output text)
case $etag in *-5\") ;; *) fail "the multipart upload's ETag $etag is not of 5 parts" ;; esac
s3 cp s3://photos/big/big.bin "$work/big.back" >/dev/null || fail "aws s3 cp (large download)"
cmp -s "$work/big.bin" "$work/big.back" || fail "the multipart upload came back different"
s3api get-object --bucket photos --key big/big.bin --range bytes=1000-1999 "$work/range.bin" >/dev/null
cmp -s "$work/range.bin" <(tail -c +1001 "$work/big.bin" | head -c 1000) || fail "a range read is wrong"
ok "multipart upload (5 parts), whole and ranged reads"

# Low-level multipart calls, including an abort.
upload=$(s3api create-multipart-upload --bucket photos --key manual.bin --query UploadId --output text)
head -c $((5 * 1024 * 1024)) /dev/urandom >"$work/p1"
head -c 1234 /dev/urandom >"$work/p2"
e1=$(s3api upload-part --bucket photos --key manual.bin --upload-id "$upload" --part-number 1 --body "$work/p1" --query ETag --output text)
e2=$(s3api upload-part --bucket photos --key manual.bin --upload-id "$upload" --part-number 2 --body "$work/p2" --query ETag --output text)
s3api list-parts --bucket photos --key manual.bin --upload-id "$upload" | grep -q '"PartNumber": 2' || fail "list-parts"
s3api complete-multipart-upload --bucket photos --key manual.bin --upload-id "$upload" \
  --multipart-upload "{\"Parts\":[{\"PartNumber\":1,\"ETag\":$e1},{\"PartNumber\":2,\"ETag\":$e2}]}" >/dev/null ||
  fail "complete-multipart-upload"
s3 cp s3://photos/manual.bin "$work/manual.back" >/dev/null
cmp -s "$work/manual.back" <(cat "$work/p1" "$work/p2") || fail "the joined parts are wrong"
upload=$(s3api create-multipart-upload --bucket photos --key aborted.bin --query UploadId --output text)
s3api upload-part --bucket photos --key aborted.bin --upload-id "$upload" --part-number 1 --body "$work/p2" >/dev/null
s3api abort-multipart-upload --bucket photos --key aborted.bin --upload-id "$upload"
s3api head-object --bucket photos --key aborted.bin >/dev/null 2>&1 && fail "an aborted upload left an object"
ok "create, upload-part, list-parts, complete and abort"

# Metadata, copies, listing with a delimiter, deletes.
s3api put-object --bucket photos --key meta.txt --body "$work/src/hello.txt" \
  --content-type text/plain --metadata color=blue >/dev/null
s3api head-object --bucket photos --key meta.txt | grep -q '"color": "blue"' || fail "user metadata did not come back"
s3api head-object --bucket photos --key meta.txt | grep -q '"ContentType": "text/plain"' || fail "Content-Type did not come back"
s3api copy-object --bucket photos --key copy.txt --copy-source photos/meta.txt >/dev/null || fail "copy-object"
s3api head-object --bucket photos --key copy.txt | grep -q '"color": "blue"' || fail "a copy lost its metadata"
s3api list-objects-v2 --bucket photos --delimiter / --query 'CommonPrefixes[].Prefix' --output text | grep -q 'big/' ||
  fail "list-objects-v2 with a delimiter shows no big/ prefix"
s3api list-objects-v2 --bucket photos --max-keys 2 --query 'KeyCount' --output text | grep -q '^2$' || fail "max-keys"
[ "$(s3api list-objects-v2 --bucket photos --page-size 2 --query 'Contents[].Key' --output text | wc -w)" = 5 ] ||
  fail "paging through list-objects-v2 does not see 5 objects"
s3 rm s3://photos/copy.txt >/dev/null
s3api delete-objects --bucket photos --delete '{"Objects":[{"Key":"meta.txt"},{"Key":"no-such-key"}]}' >/dev/null
[ "$(s3api list-objects-v2 --bucket photos --query 'length(Contents)' --output text)" = 3 ] || fail "deletes"
ok "metadata, copy-object, delimiter and paging, delete-object(s)"

# aws s3 sync both ways.
s3 sync "$work/src" s3://photos/synced >/dev/null || fail "aws s3 sync (up)"
s3 sync s3://photos/synced "$work/synced" >/dev/null || fail "aws s3 sync (down)"
diff -r "$work/src" "$work/synced" || fail "aws s3 sync round trip differs"
ok "aws s3 sync up and down"

# rclone, with modification times in metadata.
export RCLONE_CONFIG=$work/rclone.conf
cat >"$RCLONE_CONFIG" <<EOF
[pumasi]
type = s3
provider = Other
access_key_id = $admin_id
secret_access_key = $admin_secret
endpoint = $ep
region = us-east-1
force_path_style = true
EOF
rclone mkdir pumasi:mirror
rclone sync "$work/src" pumasi:mirror --s3-upload-cutoff 100k --s3-chunk-size 5M >"$work/rclone.log" 2>&1 ||
  fail "rclone sync: $(tail -n 5 "$work/rclone.log")"
rclone check "$work/src" pumasi:mirror >>"$work/rclone.log" 2>&1 || fail "rclone check after sync"
echo changed >>"$work/src/hello.txt"
rm "$work/src/empty.txt"
rclone sync "$work/src" pumasi:mirror >>"$work/rclone.log" 2>&1 || fail "rclone sync (second)"
rclone check "$work/src" pumasi:mirror >>"$work/rclone.log" 2>&1 || fail "rclone check after the second sync"
rclone lsl pumasi:mirror >"$work/lsl.out" 2>&1
grep -q 'sub/deeper/with space+plus.txt' "$work/lsl.out" || fail "rclone lsl: $(cat "$work/lsl.out")"
rclone copy pumasi:mirror "$work/rclone.back" >>"$work/rclone.log" 2>&1
diff -r "$work/src" "$work/rclone.back" || fail "rclone copy back differs"
ok "rclone sync, check, and copy back"

# boto3: a presigned GET, an upload with a CRC32 checksum, a streamed
# (aws-chunked) upload.
python3 - "$ep" "$admin_id" "$admin_secret" "$work" <<'EOF' || fail "boto3"
import sys, urllib.request, io
import boto3
from botocore.config import Config
ep, key, secret, work = sys.argv[1:]
c = boto3.client("s3", endpoint_url=ep, aws_access_key_id=key, aws_secret_access_key=secret,
                 region_name="us-east-1", config=Config(signature_version="s3v4", s3={"addressing_style": "path"}))
c.put_object(Bucket="photos", Key="boto/a.txt", Body=b"from boto3", ChecksumAlgorithm="CRC32")
assert c.get_object(Bucket="photos", Key="boto/a.txt")["Body"].read() == b"from boto3"
url = c.generate_presigned_url("get_object", Params={"Bucket": "photos", "Key": "boto/a.txt"}, ExpiresIn=300)
assert urllib.request.urlopen(url).read() == b"from boto3"
c.upload_fileobj(io.BytesIO(b"x" * (9 * 1024 * 1024)), "photos", "boto/big.bin")
assert c.head_object(Bucket="photos", Key="boto/big.bin")["ContentLength"] == 9 * 1024 * 1024
keys = [o["Key"] for p in c.get_paginator("list_objects_v2").paginate(Bucket="photos", Prefix="boto/") for o in p["Contents"]]
assert keys == ["boto/a.txt", "boto/big.bin"], keys
EOF
ok "boto3: CRC32 checksum, presigned GET, multipart upload_fileobj, paginator"

# Every write replaces the gateway's previous version: one version left.
versions=$("$bin" store ls photos | grep -c '^photos')
[ "$versions" -le 2 ] || fail "the gateway left $versions versions of photos"
ok "gateway writes replace the version they made ($versions listed)"

# A submitter's key writes only its own account; a viewer's only reads.
sub_ticket=$("$bin" access create s3-sub --role submitter --account lab 2>/dev/null | grep -o 'pumasiadmin[A-Za-z0-9]*' | head -n1)
view_ticket=$("$bin" access create s3-view --role viewer 2>/dev/null | grep -o 'pumasiadmin[A-Za-z0-9]*' | head -n1)
[ -n "$sub_ticket" ] && [ -n "$view_ticket" ] || fail "pumasi access create printed no tickets"
read -r sub_id sub_secret <<<"$(make_key lab --admin "$sub_ticket")"
read -r view_id view_secret <<<"$(make_key ro --admin "$view_ticket")"
[ -n "$sub_secret" ] && [ -n "$view_secret" ] || fail "keys from the submitter and viewer tickets"
as() { local id=$1 sec=$2; shift 2; AWS_ACCESS_KEY_ID=$id AWS_SECRET_ACCESS_KEY=$sec aws --endpoint-url "$ep" "$@"; }
as "$sub_id" "$sub_secret" s3 mb s3://lab-data >/dev/null || fail "the submitter's key cannot make a bucket"
as "$sub_id" "$sub_secret" s3 cp "$work/src/hello.txt" s3://lab-data/x.txt >/dev/null || fail "the submitter's key cannot write"
"$bin" store ls lab-data | grep -q lab || fail "the submitter's bucket is not in its account"
as "$sub_id" "$sub_secret" s3 cp "$work/src/hello.txt" s3://photos/intruder.txt >"$work/deny.out" 2>&1 &&
  fail "the submitter's key wrote to the admin's bucket"
grep -q AccessDenied "$work/deny.out" || fail "the submitter's refusal: $(cat "$work/deny.out")"
as "$view_id" "$view_secret" s3 cp s3://photos/hello.txt "$work/view.txt" >/dev/null || fail "the viewer's key cannot read"
as "$view_id" "$view_secret" s3 cp "$work/src/hello.txt" s3://photos/v.txt >"$work/deny.out" 2>&1 &&
  fail "the viewer's key wrote"
grep -q AccessDenied "$work/deny.out" || fail "the viewer's refusal: $(cat "$work/deny.out")"
ok "submitter keys write their own account only, viewer keys only read"

# A revoked key stops working (the gateway asks again within minutes;
# a new day's or region's signature asks at once).
"$bin" s3 revoke-key "$view_id" 2>/dev/null
AWS_DEFAULT_REGION=eu-west-1 as "$view_id" "$view_secret" --region eu-west-1 s3 ls >"$work/revoked.out" 2>&1 &&
  fail "a revoked key still works"
grep -q InvalidAccessKeyId "$work/revoked.out" || fail "the revoked key: $(cat "$work/revoked.out")"
ok "a revoked key is refused"

# Buckets go away only when empty.
s3 rb s3://lab-data >"$work/rb.out" 2>&1 && fail "a bucket with objects was removed"
grep -q BucketNotEmpty "$work/rb.out" || fail "removing a full bucket: $(cat "$work/rb.out")"
s3 rm s3://lab-data/x.txt >/dev/null
s3 rb s3://lab-data >/dev/null || fail "removing an empty bucket"
"$bin" store ls lab-data 2>/dev/null | grep -q '^lab-data' && fail "the removed bucket's dataset is still there"
ok "aws s3 rb: refused while full, removes the dataset once empty"

echo "all $passed S3 gateway checks passed"
