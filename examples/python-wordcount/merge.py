"""Merges the counts.json files a wordcount job produced.

Usage: python3 merge.py pumasi-out/<job id>
"""

import collections
import json
import pathlib
import sys

total = collections.Counter()
for f in sorted(pathlib.Path(sys.argv[1]).glob("task-*/counts.json")):
    total.update(json.loads(f.read_text()))

print(f"{sum(total.values())} words, {len(total)} distinct")
for word, n in total.most_common(10):
    print(f"{n:>6}  {word}")
