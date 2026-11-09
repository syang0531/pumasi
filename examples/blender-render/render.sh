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
# driver onto the Windows GPU driver in /usr/lib/wsl/lib, which the Pumasi
# agent lends graphics tasks (Docker Desktop leaves it out). Without it
# d3d12 cannot start, so draw on the CPU (llvmpipe) instead of crashing.
if [ -e /dev/dxg ]; then
  if [ -e /usr/lib/wsl/lib/libd3d12.so ]; then
    export LD_LIBRARY_PATH="/usr/lib/wsl/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
    export GALLIUM_DRIVER=${GALLIUM_DRIVER:-d3d12}
    export MESA_D3D12_DEFAULT_ADAPTER_NAME=${MESA_D3D12_DEFAULT_ADAPTER_NAME:-NVIDIA}
  else
    # Only EEVEE draws; Cycles renders with CUDA and needs no GL.
    if [ "$engine" = eevee ]; then
      echo "note: WSL 2 without /usr/lib/wsl/lib/libd3d12.so: drawing on the CPU (llvmpipe)"
    fi
    export GALLIUM_DRIVER=${GALLIUM_DRIVER:-llvmpipe}
  fi
fi

# --python-exit-code: a Python error in the scene script fails the task
# (Blender itself exits 0 after one).
blender_run() {
  "$@" blender --background --factory-startup --python-exit-code 1 --python "$scene" -- \
    --engine "$engine" --shard "$shard" --shards "$shards" --out "$out"
}

case "$engine" in
  cycles)
    command -v nvidia-smi >/dev/null 2>&1 && nvidia-smi -L || true
    blender_run ;;
  eevee)
    if command -v eglinfo >/dev/null 2>&1; then
      # scene.py falls back to it when Blender cannot say what it drew with.
      PUMASI_EGL_RENDERER=$(eglinfo -B 2>/dev/null | grep -i -m1 'renderer' | sed 's/.*: *//')
      export PUMASI_EGL_RENDERER
      echo "EGL: $PUMASI_EGL_RENDERER"
    fi
    if [ -n "${DISPLAY:-}" ]; then blender_run; else
      # EEVEE opens a window system even in background mode.
      blender_run xvfb-run -a -s "-screen 0 1920x1080x24"
    fi ;;
  *) echo "engine must be cycles or eevee, not $engine"; exit 2 ;;
esac
ls -l "$out"
