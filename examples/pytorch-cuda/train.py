"""Trains a small MLP on a synthetic two-spiral problem on the GPU.

Usage: train.py SHARD SHARDS. The shard number seeds the run, so shards are
independent trials. Writes /output/result.json and prints a summary.
"""

import json
import math
import os
import sys
import time

import torch

shard, shards = int(sys.argv[1]), int(sys.argv[2])
if not torch.cuda.is_available():
    sys.exit("CUDA is not available: this task needs a GPU (is the job asking for one?)")
device = torch.device("cuda")
torch.manual_seed(1000 + shard)
name = torch.cuda.get_device_name(0)
print(f"shard {shard}/{shards} on {name}", flush=True)


def spirals(n):
    t = torch.rand(n, device=device) * 3 * math.pi + 0.5
    label = torch.randint(0, 2, (n,), device=device)
    sign = label * 2 - 1
    x = torch.stack([t * torch.cos(t) * sign, t * torch.sin(t) * sign], dim=1)
    return x / 10 + torch.randn_like(x) * 0.02, label


train_x, train_y = spirals(200_000)
test_x, test_y = spirals(20_000)
width = 128 * (1 + shard % 2)
model = torch.nn.Sequential(
    torch.nn.Linear(2, width),
    torch.nn.ReLU(),
    torch.nn.Linear(width, width),
    torch.nn.ReLU(),
    torch.nn.Linear(width, 2),
).to(device)
opt = torch.optim.Adam(model.parameters(), lr=3e-3)
loss_fn = torch.nn.CrossEntropyLoss()

start = time.time()
losses = []
for epoch in range(20):
    perm = torch.randperm(train_x.shape[0], device=device)
    for i in range(0, train_x.shape[0], 4096):
        idx = perm[i : i + 4096]
        opt.zero_grad()
        loss = loss_fn(model(train_x[idx]), train_y[idx])
        loss.backward()
        opt.step()
    losses.append(round(loss.item(), 5))
torch.cuda.synchronize()
seconds = time.time() - start
with torch.no_grad():
    accuracy = (model(test_x).argmax(1) == test_y).float().mean().item()

os.makedirs("/output", exist_ok=True)
with open("/output/result.json", "w") as f:
    json.dump(
        {
            "shard": shard,
            "gpu": name,
            "width": width,
            "losses": losses,
            "accuracy": accuracy,
            "seconds": round(seconds, 2),
        },
        f,
        indent=2,
    )
print(f"shard {shard}: width {width}, accuracy {accuracy:.4f}, {seconds:.1f} s", flush=True)
