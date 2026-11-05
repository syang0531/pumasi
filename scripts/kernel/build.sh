#!/bin/sh
# Builds the x86_64 guest kernel for Apple's Virtualization.framework on
# Intel Macs: Firecracker's CI configuration for Linux 6.1 (virtio-pci,
# virtio-blk, virtio-console, ext4, overlayfs, all built in, no modules) as
# a bzImage, the format VZLinuxBootLoader boots on Intel. Apple Silicon
# Macs use Firecracker's own arm64 Image and Linux and Windows hosts its ELF
# vmlinux, so only this one is built here.
#
# The build is meant to be reproducible: pinned sources checked against
# SHA-256, fixed timestamp, user and host. Usage: build.sh OUTDIR
set -eu

KERNEL_VERSION=6.1.155
# Checked against https://cdn.kernel.org/pub/linux/kernel/v6.x/sha256sums.asc
# (fetched over HTTPS) while empty.
KERNEL_SHA256=""
CONFIG_URL="https://s3.amazonaws.com/spec.ccfc.min/firecracker-ci/v1.15/x86_64/vmlinux-${KERNEL_VERSION}.config"
CONFIG_SHA256=024b2aae62fe7131f9a9ab80f27619c882c0a37265dd105adae01d2867bef7c3

mkdir -p "$1"
out=$(cd "$1" && pwd)
work=${WORK:-$(mktemp -d)}
cd "$work"

tarball=linux-${KERNEL_VERSION}.tar.xz
[ -f "$tarball" ] || curl -fsSLO "https://cdn.kernel.org/pub/linux/kernel/v6.x/$tarball"
if [ -z "$KERNEL_SHA256" ]; then
  KERNEL_SHA256=$(curl -fsSL https://cdn.kernel.org/pub/linux/kernel/v6.x/sha256sums.asc |
    awk -v f="$tarball" '$2 == f { print $1 }')
fi
echo "$KERNEL_SHA256  $tarball" | sha256sum -c -
curl -fsSL -o fc.config "$CONFIG_URL"
echo "$CONFIG_SHA256  fc.config" | sha256sum -c -

rm -rf "linux-$KERNEL_VERSION"
tar xf "$tarball"
cd "linux-$KERNEL_VERSION"
cp ../fc.config .config
make olddefconfig >/dev/null

export KBUILD_BUILD_TIMESTAMP="2025-01-01 00:00:00 UTC"
export KBUILD_BUILD_USER=pumasi KBUILD_BUILD_HOST=pumasi KBUILD_BUILD_VERSION=1
make -j"$(nproc)" bzImage

cp arch/x86/boot/bzImage "$out/bzImage-x86_64"
cp .config "$out/config-x86_64"
cd "$out"
sha256sum bzImage-x86_64 config-x86_64 > SHA256SUMS
cat SHA256SUMS
