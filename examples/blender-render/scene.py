"""A small procedural scene for Pumasi's Blender example: no .blend file to
download, so the job is just this script and the image.

    blender -b --factory-startup --python scene.py -- \
        --engine cycles|eevee --shard 0 --shards 4 --out /output

The camera circles the objects over 24 frames; shard N of M renders
frames N, N+M, N+2M, ... Each task prints the device it rendered on and
writes `render.json` with the timings next to the frames.
"""

import argparse
import json
import math
import os
import sys
import time

import bpy

FRAMES = 24


def args():
    argv = sys.argv[sys.argv.index("--") + 1 :] if "--" in sys.argv else []
    p = argparse.ArgumentParser()
    p.add_argument("--engine", choices=["cycles", "eevee"], default="cycles")
    p.add_argument("--shard", type=int, default=0)
    p.add_argument("--shards", type=int, default=1)
    p.add_argument("--out", default="/output")
    p.add_argument("--samples", type=int, default=64)
    p.add_argument("--size", default="1280x720")
    return p.parse_args(argv)


def material(name, color, metallic=0.0, roughness=0.4, emission=0.0):
    m = bpy.data.materials.new(name)
    m.use_nodes = True
    bsdf = m.node_tree.nodes["Principled BSDF"]
    bsdf.inputs["Base Color"].default_value = (*color, 1.0)
    bsdf.inputs["Metallic"].default_value = metallic
    bsdf.inputs["Roughness"].default_value = roughness
    if emission:
        bsdf.inputs["Emission Color"].default_value = (*color, 1.0)
        bsdf.inputs["Emission Strength"].default_value = emission
    return m


def build():
    bpy.ops.wm.read_factory_settings(use_empty=True)
    scene = bpy.context.scene
    bpy.ops.mesh.primitive_plane_add(size=40)
    bpy.context.object.data.materials.append(material("floor", (0.8, 0.8, 0.8), roughness=0.2))
    colors = [(0.9, 0.2, 0.1), (0.1, 0.5, 0.9), (0.95, 0.75, 0.1), (0.2, 0.8, 0.3)]
    for i in range(12):
        a = i / 12 * 2 * math.pi
        r = 3 + (i % 3)
        if i % 2:
            bpy.ops.mesh.primitive_uv_sphere_add(radius=0.8, location=(r * math.cos(a), r * math.sin(a), 0.8), segments=64, ring_count=32)
            bpy.ops.object.shade_smooth()
        else:
            bpy.ops.mesh.primitive_cube_add(size=1.3, location=(r * math.cos(a), r * math.sin(a), 0.65), rotation=(0, 0, a))
        bpy.context.object.data.materials.append(
            material(f"m{i}", colors[i % 4], metallic=float(i % 3 == 0), emission=2.0 if i == 5 else 0.0)
        )
    bpy.ops.mesh.primitive_torus_add(location=(0, 0, 1.5), major_radius=1.5, minor_radius=0.4)
    bpy.ops.object.shade_smooth()
    bpy.context.object.data.materials.append(material("gold", (1.0, 0.77, 0.34), metallic=1.0, roughness=0.15))

    bpy.ops.object.light_add(type="SUN", rotation=(0.8, 0.2, 0.6))
    bpy.context.object.data.energy = 3.0
    bpy.ops.object.light_add(type="AREA", location=(0, -6, 8))
    bpy.context.object.data.energy = 800
    bpy.context.object.data.size = 5

    world = bpy.data.worlds.new("sky")
    world.use_nodes = True
    world.node_tree.nodes["Background"].inputs["Color"].default_value = (0.05, 0.07, 0.12, 1)
    scene.world = world

    bpy.ops.object.empty_add(location=(0, 0, 1))
    target = bpy.context.object
    bpy.ops.object.camera_add()
    cam = bpy.context.object
    scene.camera = cam
    track = cam.constraints.new("TRACK_TO")
    track.target = target
    for f in range(FRAMES):
        a = f / FRAMES * 2 * math.pi
        cam.location = (11 * math.cos(a), 11 * math.sin(a), 5)
        cam.keyframe_insert("location", frame=f + 1)
    scene.frame_start, scene.frame_end = 1, FRAMES
    return scene


def use_gpu_cycles(scene, samples):
    """OptiX if the driver's OptiX library is in the container, else CUDA."""
    scene.render.engine = "CYCLES"
    scene.cycles.samples = samples
    scene.cycles.use_denoising = False
    prefs = bpy.context.preferences.addons["cycles"].preferences
    for kind in ("OPTIX", "CUDA"):
        try:
            prefs.compute_device_type = kind
        except TypeError:
            continue
        prefs.get_devices()
        gpus = [d for d in prefs.devices if d.type == kind]
        if gpus:
            for d in prefs.devices:
                d.use = d.type == kind
            scene.cycles.device = "GPU"
            return f"Cycles {kind}: " + ", ".join(d.name for d in gpus)
    scene.cycles.device = "CPU"
    return "Cycles CPU (no CUDA or OptiX device in the container)"


def use_eevee(scene, samples):
    # EEVEE is BLENDER_EEVEE_NEXT from Blender 4.2 in the 4.x series and
    # BLENDER_EEVEE before it and in 5.x. Setting a name Blender does not
    # know raises TypeError.
    for name in ("BLENDER_EEVEE_NEXT", "BLENDER_EEVEE"):
        try:
            scene.render.engine = name
            break
        except TypeError:
            continue
    scene.eevee.taa_render_samples = samples
    return eevee_device()


def eevee_device():
    """What EEVEE draws with, or None before Blender has a GPU context
    (Blender 5.x in background mode makes it at the first render)."""
    import gpu

    try:
        return f"EEVEE on {gpu.platform.renderer_get()} ({gpu.platform.vendor_get()}, {gpu.platform.version_get()})"
    except Exception:
        return None


def report_device(device):
    print(f"render device: {device}", flush=True)
    if "llvmpipe" in device or "softpipe" in device:
        print("warning: drawing on the CPU (llvmpipe), not the GPU", flush=True)


def main():
    a = args()
    scene = build()
    w, h = (int(x) for x in a.size.split("x"))
    scene.render.resolution_x, scene.render.resolution_y = w, h
    scene.render.image_settings.file_format = "PNG"
    device = use_gpu_cycles(scene, a.samples) if a.engine == "cycles" else use_eevee(scene, a.samples)
    if device:
        report_device(device)
    os.makedirs(a.out, exist_ok=True)
    times = {}
    for f in range(a.shard + 1, FRAMES + 1, a.shards):
        scene.frame_set(f)
        scene.render.filepath = os.path.join(a.out, f"{a.engine}-{f:04d}.png")
        t = time.time()
        bpy.ops.render.render(write_still=True)
        times[f] = round(time.time() - t, 3)
        print(f"frame {f}: {times[f]}s", flush=True)
        if not device:
            # The GPU context exists now; else say what EGL (render.sh) found.
            egl = os.environ.get("PUMASI_EGL_RENDERER")
            device = eevee_device() or (f"EEVEE on {egl} (from EGL)" if egl else "EEVEE (device unknown)")
            report_device(device)
    with open(os.path.join(a.out, "render.json"), "w") as fh:
        json.dump({"engine": a.engine, "device": device, "size": a.size, "samples": a.samples, "seconds": times}, fh, indent=2)


main()
