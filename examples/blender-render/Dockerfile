# Blender for Pumasi GPU jobs: Cycles on CUDA or OptiX, and EEVEE drawing
# on the GPU through the NVIDIA EGL driver (Linux hosts) or Mesa's D3D12
# driver (Windows hosts, Docker Desktop on WSL 2). Blender 4.2 LTS by
# default; 5.x works too (--build-arg BLENDER_SERIES=5.2 --build-arg
# BLENDER_VERSION=5.2.3).
#
#   docker build -t ghcr.io/YOU/pumasi-blender:4.2 examples/blender-render
#   docker push ghcr.io/YOU/pumasi-blender:4.2
#
# The NVIDIA Container Toolkit (or Docker Desktop) mounts the driver's
# CUDA, OptiX, EGL and Vulkan libraries when a job asks for them, and on
# Windows the Pumasi agent lends graphics tasks Docker Desktop's
# /usr/lib/wsl (D3D12); the image brings only the vendor-neutral loaders
# (glvnd, the Vulkan loader) and Mesa, whose d3d12 driver is what WSL 2
# draws with.
FROM ubuntu:24.04

ARG BLENDER_SERIES=4.2
ARG BLENDER_VERSION=4.2.3

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates curl xz-utils \
      # GL and EGL through glvnd, and Mesa (with its d3d12 driver for WSL 2)
      libglvnd0 libgl1 libegl1 libgles2 libgl1-mesa-dri libegl-mesa0 \
      # Vulkan loader, Mesa's Vulkan drivers (dzn on WSL 2), and the probes
      libvulkan1 mesa-vulkan-drivers mesa-utils vulkan-tools \
      # what Blender's Linux build links against
      libx11-6 libxi6 libxxf86vm1 libxfixes3 libxrender1 libxkbcommon0 \
      libxkbcommon-x11-0 libsm6 libice6 libxext6 libxrandr2 libxinerama1 \
      libxcursor1 libwayland-client0 libwayland-egl1 libdecor-0-0 libdbus-1-3 \
      # a virtual display for EEVEE, which wants one even in background mode
      xvfb xauth \
    && rm -rf /var/lib/apt/lists/*

# Blender's official build, checked against the checksum list published
# next to it.
RUN set -eux; \
    base="https://download.blender.org/release/Blender${BLENDER_SERIES}"; \
    tarball="blender-${BLENDER_VERSION}-linux-x64.tar.xz"; \
    cd /tmp; \
    curl -fsSLO "$base/$tarball"; \
    curl -fsSL "$base/blender-${BLENDER_VERSION}.sha256" | grep " $tarball\$" > sums; \
    sha256sum -c sums; \
    mkdir -p /opt/blender; \
    tar -xJf "$tarball" -C /opt/blender --strip-components=1; \
    rm -f "$tarball" sums; \
    ln -s /opt/blender/blender /usr/local/bin/blender

COPY render.sh /opt/render.sh
# A checkout with CRLF line ends (Windows Git's core.autocrlf=true, before
# the repository's .gitattributes) would leave "#!/bin/sh\r", which does
# not start.
RUN sed -i 's/\r$//' /opt/render.sh && chmod +x /opt/render.sh
ENTRYPOINT []
CMD ["/opt/render.sh", "--help"]
