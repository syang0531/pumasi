"""Counts words in this shard's share of /input/texts and writes /output/counts.json."""

import collections
import json
import pathlib
import re
import sys

shard, shards = int(sys.argv[1]), int(sys.argv[2])

texts = sorted(pathlib.Path("/input/texts").glob("*.txt"))
mine = [p for i, p in enumerate(texts) if i % shards == shard]

counts = collections.Counter()
for path in mine:
    words = re.findall(r"[a-z']+", path.read_text(encoding="utf-8").lower())
    counts.update(words)

out = pathlib.Path("/output")
(out / "counts.json").write_text(json.dumps(dict(counts), sort_keys=True))
print(f"shard {shard}/{shards}: {len(mine)} files, {sum(counts.values())} words")
