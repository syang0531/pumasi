#!/bin/sh
# Renders frames of a Blender scene script with Cycles or EEVEE on the GPU,
# into /output (or $OUT). Run by the jobs in this directory:
#
#   render.sh SCENE.py cycles|eevee SHARD SHARDS
#
# It prints what Blender drew with, so a run that fell back to the CPU
# (llvmpipe) shows in the task's log.
set -eu
if [ "$#" -lt 2 ] || [ "$1" = "--help" ]; then
  echo "usage: render.sh SCENE.py cycles|eevee [SHARD SHARDS]"; exit 2
fi
scene=$1; engine=$2; shard=${3:-0}; shards=${4:-1}
out=${OUT:-/output}
mkdir -p "$out"

# WSL 2 (Docker Desktop on Windows): drawing goes through Mesa's d3d12
# driver onto the Windows GPU driver in /usr/lib/wsl/lib.
if [ -e /dev/dxg ]; then
  export LD_LIBRARY_PATH="/usr/lib/wsl/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
  export GALLIUM_DRIVER=${GALLIUM_DRIVER:-d3d12}
  export MESA_D3D12_DEFAULT_ADAPTER_NAME=${MESA_D3D12_DEFAULT_ADAPTER_NAME:-NVIDIA}
fi

run() {
  blender --background --factory-startup --python "$scene" -- \
    --engine "$engine" --shard "$shard" --shards "$shards" --out "$out"
}

case "$engine" in
  cycles)
    command -v nvidia-smi >/dev/null 2>&1 && nvidia-smi -L || true
    run ;;
  eevee)
    if command -v eglinfo >/dev/null 2>&1; then
      echo "EGL: $(eglinfo -B 2>/dev/null | grep -i -m1 'renderer' | sed 's/.*: *//')"
    fi
    if [ -n "${DISPLAY:-}" ]; then run; else
      # EEVEE opens a window system even in background mode.
      xvfb-run -a -s "-screen 0 1920x1080x24" blender --background --factory-startup \
        --python "$scene" -- --engine "$engine" --shard "$shard" --shards "$shards" --out "$out"
    fi ;;
  *) echo "engine must be cycles or eevee, not $engine"; exit 2 ;;
esac
ls -l "$out"
