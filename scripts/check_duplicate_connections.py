#!/usr/bin/env python3
"""Check for duplicate and dangling connections in the remem store.

Checks:
  1. Duplicate (source, target, relationship_type) directed edges — same edge twice.
  2. Dangling references — connections whose target no longer exists in the store.
  3. Per-memory adjacency duplicates — /related returns the same neighbor twice.

Iterates over all memories via the /memories endpoint and calls /related on each,
avoiding the O(N²) paginated /connections endpoint entirely.

Usage:
    python scripts/check_duplicate_connections.py [--url URL]

Defaults:
    --url   http://localhost:4545/api/v1
"""

import argparse
import sys
import urllib.request
import json
from collections import Counter

PAGE = 100  # /memories page size (server-side max)


def get(base_url: str, path: str) -> dict:
    url = f"{base_url}{path}"
    with urllib.request.urlopen(url) as r:
        return json.load(r)


def fetch_all_memories(base_url: str) -> list[dict]:
    """Page through /memories and return all non-archived memories."""
    memories = []
    offset = 0
    total = None
    while True:
        data = get(base_url, f"/memories?limit={PAGE}&offset={offset}")
        page = data.get("memories", [])
        if total is None:
            total = data.get("total", 0)
            print(f"  Total memories in store: {total:,}")
        memories.extend(m for m in page if not m.get("archived", False))
        offset += len(page)
        if offset % 5000 == 0:
            print(f"  ... fetched {offset:,}/{total:,}", flush=True)
        if not page or offset >= total:
            break
    return memories


def main() -> int:
    parser = argparse.ArgumentParser(description="Check remem for duplicate/dangling connections")
    parser.add_argument("--url", default="http://localhost:4545/api/v1")
    args = parser.parse_args()
    base = args.url.rstrip("/")

    print("Fetching all memories...")
    memories = fetch_all_memories(base)
    memory_ids = {m["id"] for m in memories}
    print(f"  Live memories: {len(memory_ids):,}")

    # Walk every memory's adjacency list
    print(f"\nScanning /related for all {len(memory_ids):,} memories...")
    directed_edges: Counter = Counter()   # (src, tgt, rel) → count
    dangling: list[tuple] = []
    per_memory_dupes: list[tuple] = []
    memories_with_conn = 0

    for i, mid in enumerate(memory_ids, 1):
        if i % 2000 == 0:
            print(f"  ... {i:,}/{len(memory_ids):,}", flush=True)

        related = get(base, f"/memories/{mid}/related?depth=1").get("related", [])
        if not related:
            continue
        memories_with_conn += 1

        # Check adjacency-list duplicates for this memory
        neighbor_pairs = [
            (r["memory"]["id"], r["connection"]["relationship_type"])
            for r in related
        ]
        counts = Counter(neighbor_pairs)
        for (tgt, rel), n in counts.items():
            if n > 1:
                per_memory_dupes.append((mid, tgt, rel, n))

        # Record directed edges and dangling refs
        for r in related:
            tgt = r["memory"]["id"]
            rel = r["connection"]["relationship_type"]
            directed_edges[(mid, tgt, rel)] += 1
            if tgt not in memory_ids:
                dangling.append((mid, tgt, rel))

    # Deduplicate dangling (same edge may appear once)
    dangling = list(dict.fromkeys(dangling))

    # Global duplicate directed edges
    global_dupes = {k: v for k, v in directed_edges.items() if v > 1}

    # ── Results ──────────────────────────────────────────────────────────────
    ok = True
    print()
    print("=" * 60)

    print(f"Memories with connections : {memories_with_conn:,}")
    print(f"Unique directed edges     : {len(directed_edges):,}")
    print()

    print("1. Duplicate directed edges (same source→target→rel seen >1 time)")
    print(f"   Found: {len(global_dupes)}")
    if global_dupes:
        ok = False
        for (src, tgt, rel), n in list(global_dupes.items())[:10]:
            print(f"   src={src}  tgt={tgt}  rel={rel}  x{n}")
        if len(global_dupes) > 10:
            print(f"   ... and {len(global_dupes) - 10} more")
    else:
        print("   OK — no duplicates")

    print()
    print("2. Dangling references (target not in live memory set)")
    print(f"   Found: {len(dangling)}")
    if dangling:
        ok = False
        for src, tgt, rel in dangling[:10]:
            print(f"   src={src}  tgt={tgt}  rel={rel}")
        if len(dangling) > 10:
            print(f"   ... and {len(dangling) - 10} more")
    else:
        print("   OK — all targets exist")

    print()
    print("3. Per-memory adjacency duplicates (same neighbor twice in /related)")
    print(f"   Found: {len(per_memory_dupes)}")
    if per_memory_dupes:
        ok = False
        for src, tgt, rel, n in per_memory_dupes[:10]:
            print(f"   src={src}  tgt={tgt}  rel={rel}  x{n}")
        if len(per_memory_dupes) > 10:
            print(f"   ... and {len(per_memory_dupes) - 10} more")
    else:
        print("   OK — no adjacency duplicates")

    print()
    print("=" * 60)
    if ok:
        print("RESULT: CLEAN")
        return 0
    else:
        print("RESULT: FAIL — issues detected (see above)")
        return 1


if __name__ == "__main__":
    sys.exit(main())
