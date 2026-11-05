#!/bin/sh
# Builds the GPU guest bundle for microVM GPU passthrough (experimental):
#
#   OUTDIR/vmlinux       the guest kernel: Firecracker's CI configuration for
#                        Linux 6.1 (what ordinary microVMs boot) with loadable
#                        modules turned on, as an ELF vmlinux QEMU boots
#   OUTDIR/drivers.ext4  ext4 labeled `pumasi-gpu`: NVIDIA's open kernel
#                        modules built for that kernel (modules/,
#                        modules.order), the driver's user-space libraries
#                        (lib/) and nvidia-smi (bin/)
#
# Install it on the GPU machine with `pumasi vm setup --gpu-bundle OUTDIR`.
# The guest loads the driver at boot, so the host keeps the GPU bound to
# vfio-pci and needs no NVIDIA driver of its own. NVIDIA's open modules
# support Turing (RTX 20 series) and newer GPUs.
#
# Pinned sources are checked against SHA-256 where a pin is set. Needs
# curl, make, gcc, flex, bison, bc, libelf-dev and e2fsprogs (mkfs.ext4 -d).
# Usage: build-gpu.sh OUTDIR
set -eu

KERNEL_VERSION=6.1.155
KERNEL_SHA256=""
CONFIG_URL="https://s3.amazonaws.com/spec.ccfc.min/firecracker-ci/v1.15/x86_64/vmlinux-${KERNEL_VERSION}.config"
CONFIG_SHA256=024b2aae62fe7131f9a9ab80f27619c882c0a37265dd105adae01d2867bef7c3
NVIDIA_VERSION=${NVIDIA_VERSION:-570.86.15}
NVIDIA_SHA256=${NVIDIA_SHA256:-}
NVIDIA_RUN=NVIDIA-Linux-x86_64-${NVIDIA_VERSION}.run
NVIDIA_URL="https://us.download.nvidia.com/XFree86/Linux-x86_64/${NVIDIA_VERSION}/${NVIDIA_RUN}"

mkdir -p "$1"
out=$(cd "$1" && pwd)
work=${WORK:-$(mktemp -d)}
cd "$work"

# The kernel.
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
linux=$work/linux-$KERNEL_VERSION
cd "$linux"
cp ../fc.config .config
# Loadable modules for the driver, and what NVIDIA's modules use.
scripts/config --enable MODULES --enable MODULE_UNLOAD \
  --disable MODULE_SIG --disable TRIM_UNUSED_KSYMS \
  --enable MMU_NOTIFIER --enable PCI_MSI --enable ACPI
make olddefconfig >/dev/null
export KBUILD_BUILD_TIMESTAMP="2025-01-01 00:00:00 UTC"
export KBUILD_BUILD_USER=pumasi KBUILD_BUILD_HOST=pumasi KBUILD_BUILD_VERSION=1
make -j"$(nproc)" vmlinux modules_prepare
cp vmlinux "$out/vmlinux"
cd "$work"

# The driver: kernel modules from its open sources, libraries as shipped.
[ -f "$NVIDIA_RUN" ] || curl -fsSLO "$NVIDIA_URL"
if [ -n "$NVIDIA_SHA256" ]; then
  echo "$NVIDIA_SHA256  $NVIDIA_RUN" | sha256sum -c -
fi
rm -rf nvidia
sh "$NVIDIA_RUN" --extract-only --target nvidia >/dev/null
make -C nvidia/kernel-open SYSSRC="$linux" SYSOUT="$linux" -j"$(nproc)" modules

stage=$work/stage
rm -rf "$stage"
mkdir -p "$stage/modules" "$stage/lib" "$stage/bin"
cp nvidia/kernel-open/nvidia.ko nvidia/kernel-open/nvidia-uvm.ko "$stage/modules/"
printf 'nvidia.ko\nnvidia-uvm.ko\n' > "$stage/modules.order"
for lib in libcuda libnvidia-ml libnvidia-ptxjitcompiler libnvidia-nvvm libnvidia-gpucomp; do
  f=nvidia/$lib.so.$NVIDIA_VERSION
  [ -f "$f" ] || continue
  cp "$f" "$stage/lib/"
done
cd "$stage/lib"
for f in *.so."$NVIDIA_VERSION"; do
  base=${f%.so.*}
  soname=$(readelf -d "$f" | sed -n 's/.*SONAME.*\[\(.*\)\]/\1/p')
  [ -n "$soname" ] && ln -sf "$f" "$soname"
  ln -sf "$f" "$base.so"
done
cd "$work"
cp nvidia/nvidia-smi "$stage/bin/"

size_mib=$(( $(du -sm "$stage" | cut -f1) + 64 ))
rm -f "$out/drivers.ext4"
mkfs.ext4 -q -L pumasi-gpu -d "$stage" -E root_owner=0:0 "$out/drivers.ext4" "${size_mib}M"

cd "$out"
sha256sum vmlinux drivers.ext4 > SHA256SUMS
cat SHA256SUMS
echo "NVIDIA driver $NVIDIA_VERSION, kernel $KERNEL_VERSION"
