# Blender on Pumasi GPUs: Cycles and EEVEE

Renders 24 frames of a small procedural scene (`scene.py`, no `.blend`
to download) on NVIDIA GPUs, split over 4 tasks:

- `job-cycles.toml`: Cycles on CUDA, or OptiX when the container has the
  driver's OptiX library. Needs only a GPU.
- `job-eevee.toml`: EEVEE, which draws with OpenGL. It asks for the
  `graphics` driver capability, so it only goes to nodes whose Docker
  GPUs can draw (`gpu.graphics=true`).

## 1. Build the image

```sh
docker build -t ghcr.io/YOU/pumasi-blender:4.2 examples/blender-render
docker push ghcr.io/YOU/pumasi-blender:4.2
```

and put that name in the two job files. A private registry works too
(`pumasi secret set registry/ghcr.io`). The image is Ubuntu 24.04 with
Blender 4.2 LTS (checked against Blender's published SHA-256 list; for
Blender 5.x add `--build-arg BLENDER_SERIES=5.2 --build-arg
BLENDER_VERSION=5.2.3`), the
glvnd and Vulkan loaders, Mesa (with its d3d12 driver for WSL 2),
`eglinfo`, `vulkaninfo` and Xvfb. The NVIDIA driver libraries come from
the host at run time, so the image works with any driver version.

## 2. Check the GPU nodes

On each machine with an NVIDIA GPU, with the image pulled:

```sh
pumasi gpu check --docker-run --image ghcr.io/YOU/pumasi-blender:4.2
```

It starts a container with every driver capability and reports:

```text
docker GPU tasks (ghcr.io/YOU/pumasi-blender:4.2, graphics tasks with /usr/lib/wsl read-only):
  CUDA: GPU 0: NVIDIA GeForce RTX 3080 (UUID: GPU-...)
  OptiX: no
  EGL/OpenGL: yes (WSL 2, through Mesa's D3D12 driver)
  EGL renderer: D3D12 (NVIDIA GeForce RTX 3080)
  Vulkan: CPU only (llvmpipe (LLVM 20.1.2, 256 bits))
  note: drawing works on the GPU (D3D12 (NVIDIA GeForce RTX 3080))
  note: Vulkan draws on the CPU here (llvmpipe (LLVM 20.1.2, 256 bits)): jobs that draw with Vulkan (such as Blender's Vulkan backend) will not use the GPU; EGL/OpenGL does
  note: OptiX does not start in Docker Desktop containers: Blender Cycles uses CUDA
```

That is Windows with Docker Desktop. Docker Desktop gives GPU containers
`/dev/dxg` and CUDA but not `/usr/lib/wsl/lib`, the Windows driver's
D3D12 libraries Mesa draws with, so on Windows the probe and the tasks
that ask for `graphics` get that folder read-only from Docker Desktop's
VM (`--wsl-libs` here and `--docker-wsl-libs` on the agent, both `auto`:
on Windows only). On a Linux host with the NVIDIA Container Toolkit,
OptiX and the NVIDIA EGL driver come from the toolkit instead.

The answer is saved with the agent's state, and a running agent picks it
up within 30 seconds: the node gets the label `gpu.graphics=true` when
drawing works. `--gpu-graphics yes|no` on the agent overrides the check.
`pumasi status` and the API's `capacity` show how many nodes can take
graphics work now.

## 3. Render

```sh
pumasi run examples/blender-render/job-cycles.toml --output
pumasi run examples/blender-render/job-eevee.toml --output
```

Each task writes `cycles-NNNN.png` or `eevee-NNNN.png` and a
`render.json` with the device and the seconds per frame, and its log
says `render device: ...`. Blender 5.x knows what EEVEE draws with only
once it has rendered, so EEVEE tasks print that line after the first
frame, falling back to the renderer `eglinfo` found (`render.sh` prints it
as `EGL: ...` first). Over the HTTP API (see
[docs/api.md](../../docs/api.md)):

```sh
curl -fsS -H "Authorization: Bearer $PUMASI_TICKET" \
  -F files=@examples/blender-render/job-eevee.toml\;filename=job.toml \
  -F files=@examples/blender-render/scene.py \
  "$PUMASI_URL/api/v1/jobs"
```

## How drawing reaches the GPU

| Host | CUDA, OptiX | EEVEE (OpenGL) |
| --- | --- | --- |
| Linux, NVIDIA Container Toolkit | the toolkit mounts `libcuda`; `libnvoptix` only with the `graphics` capability (add it to `job-cycles.toml` for OptiX) | the toolkit mounts NVIDIA's EGL and GLX drivers with `graphics` |
| Windows, Docker Desktop (WSL 2) | Docker Desktop mounts `libcuda`; OptiX does not start in its containers (Blender: error 7805), so Cycles uses CUDA | the agent lends graphics tasks Docker Desktop's `/usr/lib/wsl` read-only, and Mesa's d3d12 driver draws through `/dev/dxg` on the Windows driver (`render.sh` sets `GALLIUM_DRIVER=d3d12` when `/usr/lib/wsl/lib/libd3d12.so` is there, `llvmpipe` when not). Vulkan stays on the CPU (lavapipe/llvmpipe): keep Blender on its OpenGL backend |

EEVEE opens a window system even in background mode, so `render.sh` runs
it under Xvfb. On WSL 2 that still draws on the GPU (Mesa renders with
d3d12 and only copies the finished frames to the virtual screen). On a
Linux host, Xvfb's GLX is Mesa's, so EEVEE there falls back to the CPU
(`render device: EEVEE on llvmpipe`) unless the image adds VirtualGL or
a display server on the NVIDIA driver; Cycles is the better fit on Linux
hosts. The task log always says which it was.

Things that go wrong:

- `render device: Cycles CPU`: no CUDA in the container. `pumasi gpu
  check --docker-run` says why (toolkit missing, Docker Desktop without
  WSL 2, an old driver).
- `EEVEE on llvmpipe` on Windows: the image's Mesa has no d3d12 driver
  (Mesa 22 or newer needed; this image has it), or `/usr/lib/wsl/lib` is
  not in the container (the job left out `capabilities = ["graphics"]`,
  the agent runs with `--docker-wsl-libs no`, or Docker Desktop's WSL 2
  backend and GPU support are off). The task log then says `note: WSL 2
  without /usr/lib/wsl/lib/libd3d12.so`, and EEVEE still renders, slowly,
  on the CPU.
- `exec /opt/render.sh failed: No such file or directory`: `render.sh`
  has CRLF line ends, from a Windows checkout made before the
  repository's `.gitattributes`. The Dockerfile strips them; rebuild the
  image (or `git add --renormalize .` and check out again).
- A Python error in `scene.py` fails the task (`render.sh` runs Blender
  with `--python-exit-code 1`; Blender alone exits 0 after one).
- The job stays queued and then fails with "no node took it within the
  queue timeout": no online node has a free NVIDIA GPU that can draw. The
  failure lists each node's reason.
