#!/bin/sh
# Installs the pumasi binary on Linux or macOS from a GitHub release,
# checks its SHA-256 against the release's SHA256SUMS, and optionally
# installs the agent as a service that joins a cluster.
#
#   curl -fsSL https://github.com/syang0531/pumasi/releases/latest/download/install.sh | sh
#   curl -fsSL .../install.sh | sudo sh -s -- --ticket pumasijoin... -- --allow-microvm
#
# Options:
#   --version TAG     release to install (default: latest)
#   --dir DIR         where to put the binary (default: /usr/local/bin as
#                     root, else ~/.local/bin)
#   --ticket TICKET   then run `pumasi service install --ticket TICKET`
#                     (a system service as root, else a user service)
#   --base-url URL    download from this URL or local directory instead
#                     of GitHub (a mirror holding the same release assets)
#   -- ARGS...        agent options for the service, e.g. --allow-microvm
#
# Environment: PUMASI_VERSION, PUMASI_BASE_URL, PUMASI_REPO (owner/name),
# PUMASI_TICKET, and GITHUB_TOKEN to download from a private repository.
set -eu

REPO="${PUMASI_REPO:-syang0531/pumasi}"
VERSION="${PUMASI_VERSION:-latest}"
BASE_URL="${PUMASI_BASE_URL:-}"
TICKET="${PUMASI_TICKET:-}"
DIR=""

say() { printf '%s\n' "$*" >&2; }
die() { say "error: $*"; exit 1; }

while [ $# -gt 0 ]; do
  case "$1" in
    --version) VERSION="$2"; shift 2 ;;
    --dir) DIR="$2"; shift 2 ;;
    --ticket) TICKET="$2"; shift 2 ;;
    --base-url) BASE_URL="$2"; shift 2 ;;
    --) shift; break ;;
    -h | --help) sed -n '2,24p' "$0" 2>/dev/null || true; exit 0 ;;
    *) die "unknown option $1 (agent options go after --)" ;;
  esac
done

case "$(uname -s)" in
  Linux) OS=linux ;;
  Darwin) OS=macos ;;
  *) die "unsupported OS $(uname -s); on Windows use install.ps1" ;;
esac
case "$(uname -m)" in
  x86_64 | amd64) ARCH=x86_64 ;;
  arm64 | aarch64) ARCH=aarch64 ;;
  *) die "unsupported CPU $(uname -m)" ;;
esac
# A shell running under Rosetta reports x86_64 on Apple Silicon.
if [ "$OS" = macos ] && [ "$ARCH" = x86_64 ] &&
  [ "$(sysctl -n hw.optional.arm64 2>/dev/null || echo 0)" = 1 ]; then
  ARCH=aarch64
fi
ASSET="pumasi-$OS-$ARCH"

if [ -z "$DIR" ]; then
  if [ "$(id -u)" = 0 ]; then DIR=/usr/local/bin; else DIR="$HOME/.local/bin"; fi
fi

TMP=$(mktemp -d 2>/dev/null || mktemp -d -t pumasi)
trap 'rm -rf "$TMP"' EXIT

http_get() { # url, output file, [extra header]
  if command -v curl >/dev/null 2>&1; then
    if [ -n "${3:-}" ]; then
      curl -fsSL --retry 3 -H "$3" -o "$2" "$1"
    else
      curl -fsSL --retry 3 -o "$2" "$1"
    fi
  elif command -v wget >/dev/null 2>&1; then
    if [ -n "${3:-}" ]; then
      wget -q --header="$3" -O "$2" "$1"
    else
      wget -q -O "$2" "$1"
    fi
  else
    die "need curl or wget"
  fi
}

# Private repositories: find the asset through the API.
api_asset_url() { # asset name
  rel="$TMP/release.json"
  if [ "$VERSION" = latest ]; then
    path="releases/latest"
  else
    path="releases/tags/$VERSION"
  fi
  [ -f "$rel" ] || http_get "https://api.github.com/repos/$REPO/$path" "$rel" \
    "Authorization: Bearer $GITHUB_TOKEN"
  # One key per line: an asset's "url" comes before its "name".
  awk -v want="$1" '
    /"url": *"https:\/\/api\.github\.com\/repos\/.*\/releases\/assets\// {
      u = $0; sub(/.*"url": *"/, "", u); sub(/".*/, "", u)
    }
    /"name": *"/ {
      n = $0; sub(/.*"name": *"/, "", n); sub(/".*/, "", n)
      if (n == want && u != "") { print u; exit }
    }' "$rel"
}

fetch() { # asset name, output file
  if [ -n "$BASE_URL" ]; then
    case "$BASE_URL" in
      *://*) http_get "$BASE_URL/$1" "$2" ;;
      *) cp "$BASE_URL/$1" "$2" ;;
    esac
  elif [ -n "${GITHUB_TOKEN:-}" ]; then
    command -v curl >/dev/null 2>&1 || die "downloading with GITHUB_TOKEN needs curl"
    url=$(api_asset_url "$1")
    [ -n "$url" ] || die "release $VERSION of $REPO has no asset $1"
    curl -fsSL --retry 3 -H "Authorization: Bearer $GITHUB_TOKEN" \
      -H "Accept: application/octet-stream" -o "$2" "$url"
  else
    if [ "$VERSION" = latest ]; then
      base="https://github.com/$REPO/releases/latest/download"
    else
      base="https://github.com/$REPO/releases/download/$VERSION"
    fi
    http_get "$base/$1" "$2" ||
      die "could not download $1 (for a private repository set GITHUB_TOKEN)"
  fi
}

sha256() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum <"$1" | cut -d' ' -f1
  else
    shasum -a 256 <"$1" | cut -d' ' -f1
  fi
}

say "downloading $ASSET ($VERSION)"
fetch SHA256SUMS "$TMP/SHA256SUMS"
fetch "$ASSET" "$TMP/pumasi"

want=$(awk -v f="$ASSET" '$2 == f || $2 == "*"f { print $1 }' "$TMP/SHA256SUMS")
[ -n "$want" ] || die "SHA256SUMS has no entry for $ASSET"
got=$(sha256 "$TMP/pumasi")
[ "$got" = "$want" ] || die "checksum mismatch for $ASSET: got $got, expected $want"
say "checksum ok ($got)"

mkdir -p "$DIR"
chmod 755 "$TMP/pumasi"
mv -f "$TMP/pumasi" "$DIR/pumasi" 2>/dev/null || cp -f "$TMP/pumasi" "$DIR/pumasi"
say "installed $DIR/pumasi ($("$DIR/pumasi" --version))"
case ":$PATH:" in
  *":$DIR:"*) ;;
  *) say "note: $DIR is not on your PATH" ;;
esac

if [ -n "$TICKET" ]; then
  say "installing the agent service"
  "$DIR/pumasi" service install --ticket "$TICKET" -- "$@"
fi
