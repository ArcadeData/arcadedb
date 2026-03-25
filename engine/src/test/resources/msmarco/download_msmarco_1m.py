#!/usr/bin/env python3
"""
Downloads the first parquet shard of MSMARCO v2.1 embeddings from Hugging Face
and converts to .f32 binary format for the ArcadeDB vector benchmark.

Only downloads ~4GB (1 shard with ~1.9M vectors) instead of the full 245GB dataset.
Produces 1M normalized 1024-dim float32 vectors in .f32 shard format.

Usage:
    python3 download_msmarco_1m.py [output_dir]

Output:
    output_dir/msmarco-passages-1000000.shard0000.f32  (100K vectors each)
    output_dir/msmarco-passages-1000000.meta.json
    output_dir/msmarco-passages-1000000.gt.jsonl        (1000 queries, top-50 exact neighbors)
"""
import json
import os
import struct
import sys
import time
from pathlib import Path

import numpy as np

try:
    from huggingface_hub import hf_hub_download
except ImportError:
    print("Install: pip install huggingface_hub numpy pyarrow")
    sys.exit(1)

DATASET = "Cohere/msmarco-v2.1-embed-english-v3"
PARQUET_FILE = "passages_parquet/msmarco_v2.1_doc_segmented_00.parquet"
DIM = 1024
TARGET_COUNT = 1_000_000
SHARD_SIZE = 100_000
GT_QUERIES = 1000
GT_TOPK = 50


def main():
    out_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("msmarco-1m")
    out_dir.mkdir(parents=True, exist_ok=True)

    # Check if already done
    meta_file = out_dir / f"msmarco-passages-{TARGET_COUNT}.meta.json"
    if meta_file.exists():
        print(f"Already exists: {meta_file}")
        return

    # Download single parquet shard (~4GB)
    print(f"[1/4] Downloading {PARQUET_FILE} from HuggingFace (~4GB)...")
    t0 = time.time()
    local_path = hf_hub_download(
        repo_id=DATASET,
        filename=PARQUET_FILE,
        repo_type="dataset",
    )
    print(f"  Downloaded in {time.time() - t0:.1f}s: {local_path}")

    # Read embeddings from parquet
    import pyarrow.parquet as pq

    print(f"[2/4] Reading embeddings from parquet (first {TARGET_COUNT:,} vectors)...")
    t0 = time.time()

    table = pq.read_table(local_path, columns=["emb"])
    col = table.column("emb")

    vectors = []
    for i in range(min(len(col), TARGET_COUNT)):
        arr = col[i].as_py()
        vectors.append(np.array(arr, dtype=np.float32))
        if len(vectors) >= TARGET_COUNT:
            break

    vectors = np.stack(vectors)
    print(f"  Read {vectors.shape[0]:,} vectors ({vectors.shape[1]}-dim) in {time.time() - t0:.1f}s")

    # L2-normalize
    norms = np.linalg.norm(vectors, axis=1, keepdims=True)
    vectors = vectors / (norms + 1e-12)

    # Write .f32 shards
    print(f"[3/4] Writing {vectors.shape[0]:,} vectors to .f32 shards (shard_size={SHARD_SIZE:,})...")
    t0 = time.time()
    count = vectors.shape[0]
    shards = []

    for shard_idx in range(0, count, SHARD_SIZE):
        end = min(shard_idx + SHARD_SIZE, count)
        shard_count = end - shard_idx
        shard_name = f"msmarco-passages-{TARGET_COUNT}.shard{shard_idx // SHARD_SIZE:04d}.f32"
        shard_path = out_dir / shard_name

        with open(shard_path, "wb") as f:
            vectors[shard_idx:end].tofile(f)

        shards.append({"file": shard_name, "count": shard_count})
        print(f"  Wrote {shard_name} ({shard_count:,} vectors, {os.path.getsize(shard_path) / 1e6:.1f}MB)")

    # Write metadata
    meta = {
        "dim": DIM,
        "dtype": "float32",
        "count": count,
        "shard_size": SHARD_SIZE,
        "shards": shards,
    }
    with open(meta_file, "w") as f:
        json.dump(meta, f, indent=2)
    print(f"  Wrote metadata: {meta_file}")
    print(f"  Shards written in {time.time() - t0:.1f}s")

    # Generate ground truth
    print(f"[4/4] Computing ground truth ({GT_QUERIES} queries, top-{GT_TOPK})...")
    t0 = time.time()

    rng = np.random.RandomState(42)
    query_indices = rng.choice(count, size=GT_QUERIES, replace=False)
    query_vectors = vectors[query_indices]

    gt_file = out_dir / f"msmarco-passages-{TARGET_COUNT}.gt.jsonl"
    with open(gt_file, "w") as f:
        for qi, qidx in enumerate(query_indices):
            # Cosine similarity = dot product for normalized vectors
            sims = vectors @ query_vectors[qi]
            # Exclude the query itself
            sims[qidx] = -1
            top_indices = np.argpartition(-sims, GT_TOPK)[:GT_TOPK]
            top_indices = top_indices[np.argsort(-sims[top_indices])]

            query_floats = query_vectors[qi].tolist()
            neighbors = top_indices.tolist()

            obj = {"query": query_floats, "neighbors": neighbors}
            f.write(json.dumps(obj) + "\n")

            if (qi + 1) % 100 == 0:
                print(f"  {qi + 1}/{GT_QUERIES} queries...")

    print(f"  Ground truth written in {time.time() - t0:.1f}s: {gt_file}")
    print(f"\n[DONE] MSMARCO-1M ready at: {out_dir}")
    print(f"  Vectors: {count:,} x {DIM}-dim")
    print(f"  Shards: {len(shards)}")
    total_bytes = sum(os.path.getsize(out_dir / s["file"]) for s in shards)
    print(f"  Total size: {total_bytes / 1e9:.2f} GB")


if __name__ == "__main__":
    main()
