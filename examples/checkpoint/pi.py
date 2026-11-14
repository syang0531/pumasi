"""Estimates pi with a seeded Monte Carlo walk of 200 million points,
saving where it is to $PUMASI_CHECKPOINT (/checkpoint) every few seconds
and when asked to stop (SIGTERM). A run started from a checkpoint goes on
from it: the result is the same as one uninterrupted run."""

import json
import os
import random
import signal
import sys
import time

TOTAL = 200_000_000
BATCH = 1_000_000
DIR = os.environ.get("PUMASI_CHECKPOINT", "/checkpoint")
STATE = os.path.join(DIR, "state.json")

stop = False


def on_term(signum, frame):
    global stop
    stop = True


signal.signal(signal.SIGTERM, on_term)


def load():
    try:
        with open(STATE) as f:
            s = json.load(f)
        print(f"resuming at {s['done']:,} points (run {s['runs'] + 1})", flush=True)
        s["runs"] += 1
        return s
    except FileNotFoundError:
        print("starting from the beginning", flush=True)
        return {"done": 0, "inside": 0, "runs": 1}


def save(s):
    tmp = STATE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(s, f)
    os.replace(tmp, STATE)  # never a half-written checkpoint


s = load()
last = time.monotonic()
while s["done"] < TOTAL:
    # Each batch has its own seed, so the result does not depend on where
    # the runs were cut.
    rng = random.Random(s["done"])
    inside = 0
    for _ in range(BATCH):
        x, y = rng.random(), rng.random()
        inside += x * x + y * y <= 1.0
    s["done"] += BATCH
    s["inside"] += inside
    if stop:
        save(s)
        print(f"stopping at {s['done']:,} points; checkpoint saved", flush=True)
        sys.exit(0)
    if time.monotonic() - last > 5:
        save(s)
        last = time.monotonic()

print(f"pi ~= {4 * s['inside'] / s['done']:.6f} after {s['done']:,} points in {s['runs']} run(s)")
