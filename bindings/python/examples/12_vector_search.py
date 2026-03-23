#!/usr/bin/env python3
"""
Example 12: Vector Search (search-only)

Backends:
- arcadedb (embedded)
- faiss (local index file)
- lancedb (local Lance table)
- bruteforce (exact scan with NumPy)
- pgvector (PostgreSQL + pgvector extension)
- qdrant (client-server via Docker)
- milvus (client-server via Docker Compose)

For client-server backends (pgvector, qdrant, milvus), peak RSS is measured as:
  client process RSS + server process tree RSS.

Parameter normalization note:
- ArcadeDB/jvector uses `overquery_factor` directly.
- HNSW backends expose `ef_search`.
- These knobs are not semantically identical, so this benchmark uses a fixed
    normalization: `ef_search = 0.5 * k * overquery_factor` for
    faiss/pgvector/qdrant/milvus and LanceDB HNSW-like search tuning.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import random
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Dict, List
from urllib.request import Request, urlopen, urlretrieve

import numpy as np

try:
    import psutil
except Exception:
    psutil = None

META_FILENAME_RE = re.compile(r"msmarco-passages-(.+?)\.meta\.json")
GT_FILENAME_RE = re.compile(r"msmarco-passages-(.+?)\.gt")
SIZE_TOKEN_RE = re.compile(
    r"^\s*([0-9]*\.?[0-9]+)\s*([kmgt]?)(?:i?b)?\s*$", re.IGNORECASE
)
HNSW_EF_NORMALIZATION = 0.5
MILVUS_MEMORY_WEIGHTS = {
    "standalone": 0.70,
    "minio": 0.15,
    "etcd": 0.15,
}
MILVUS_CPU_WEIGHTS = {
    "standalone": 0.90,
    "minio": 0.05,
    "etcd": 0.05,
}
LATEST_RESOLUTION_CACHE: Dict[str, str] = {}

MILVUS_RUNNER_DOCKERFILE = """FROM python:3.12-slim

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update \\
    && apt-get install -y --no-install-recommends \\
        curl \
        docker-cli \\
        docker-compose \\
    && rm -rf /var/lib/apt/lists/*

RUN curl -LsSf https://astral.sh/uv/install.sh | env UV_INSTALL_DIR=/usr/local/bin sh \
    && uv pip install --system numpy pymilvus psutil
"""


def get_docker_version() -> str | None:
    docker = shutil.which("docker")
    if not docker:
        return None
    try:
        out = subprocess.check_output(
            [docker, "--version"],
            stderr=subprocess.STDOUT,
            text=True,
            timeout=5,
        )
    except (subprocess.SubprocessError, OSError):
        return None
    return out.strip() or None


def fetch_json(url: str) -> dict:
    req = Request(url, headers={"User-Agent": "arcadedb-bench"})
    with urlopen(req, timeout=30) as response:
        payload = json.load(response)
    if not isinstance(payload, dict):
        raise RuntimeError(f"Expected JSON object from {url}")
    return payload


def resolve_latest_pgvector_image() -> str:
    cached = LATEST_RESOLUTION_CACHE.get("pgvector_image")
    if cached:
        return cached

    payload = fetch_json(
        "https://hub.docker.com/v2/repositories/pgvector/pgvector/tags?page_size=100"
    )
    best_major = -1
    best_tag = None
    for item in payload.get("results", []):
        tag = str(item.get("name") or "")
        match = re.match(r"^pg(\d+)-trixie$", tag)
        if not match:
            continue
        major = int(match.group(1))
        if major > best_major:
            best_major = major
            best_tag = tag

    if not best_tag:
        raise RuntimeError("Could not resolve latest pgvector Docker tag")

    resolved = f"pgvector/pgvector:{best_tag}"
    LATEST_RESOLUTION_CACHE["pgvector_image"] = resolved
    return resolved


def resolve_qdrant_image(image: str) -> str:
    if image in {"", "latest"}:
        return "qdrant/qdrant:latest"
    return image


def resolve_milvus_compose_version(release_tag: str) -> str:
    if release_tag not in {"", "latest"}:
        return release_tag

    cached = LATEST_RESOLUTION_CACHE.get("milvus_compose_version")
    if cached:
        return cached

    payload = fetch_json(
        "https://api.github.com/repos/milvus-io/milvus/releases/latest"
    )
    resolved = str(payload.get("tag_name") or "").strip()
    if not resolved:
        raise RuntimeError("Could not resolve latest Milvus release tag")
    LATEST_RESOLUTION_CACHE["milvus_compose_version"] = resolved
    return resolved


def uv_bootstrap_commands(python_cmd: str = "python") -> List[str]:
    return [
        (
            f'{python_cmd} -c "import json, pathlib, platform, shutil, tarfile, urllib.request; '
            "arch={'x86_64':'x86_64-unknown-linux-gnu','amd64':'x86_64-unknown-linux-gnu','aarch64':'aarch64-unknown-linux-gnu','arm64':'aarch64-unknown-linux-gnu'}[platform.machine().lower()]; "
            "req=urllib.request.Request('https://api.github.com/repos/astral-sh/uv/releases/latest', headers={'User-Agent':'arcadedb-bench'}); "
            "version=json.load(urllib.request.urlopen(req, timeout=30))['tag_name'].lstrip('v'); "
            "archive=pathlib.Path('/tmp/uv.tar.gz'); "
            "archive.write_bytes(urllib.request.urlopen(f'https://github.com/astral-sh/uv/releases/download/{version}/uv-{arch}.tar.gz', timeout=30).read()); "
            "target=pathlib.Path('/tmp/uv-extract'); target.mkdir(parents=True, exist_ok=True); "
            "tarfile.open(archive, 'r:gz').extractall(target); "
            "uv_bin=next(p for p in target.rglob('uv') if p.is_file()); "
            "pathlib.Path('/tmp/uv-bin').mkdir(parents=True, exist_ok=True); "
            "shutil.copy2(uv_bin, '/tmp/uv-bin/uv')\""
        ),
        'export PATH="/tmp/uv-bin:$PATH"',
    ]


def is_running_in_docker() -> bool:
    if Path("/.dockerenv").exists():
        return True
    try:
        content = Path("/proc/1/cgroup").read_text(encoding="utf-8")
    except FileNotFoundError:
        return False
    return "docker" in content or "containerd" in content


def rss_mb() -> float:
    if psutil:
        return psutil.Process().memory_info().rss / (1024 * 1024)
    try:
        import resource

        return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
    except Exception:
        return float("nan")


def read_proc_rss_mb(pid: int) -> float:
    try:
        status = Path(f"/proc/{pid}/status").read_text(encoding="utf-8")
    except OSError:
        return 0.0
    for line in status.splitlines():
        if line.startswith("VmRSS:"):
            parts = line.split()
            if len(parts) >= 2:
                try:
                    return float(parts[1]) / 1024.0
                except ValueError:
                    return 0.0
    return 0.0


def process_tree_pids(root_pid: int) -> set[int]:
    pids = {root_pid}

    if psutil:
        try:
            root = psutil.Process(root_pid)
            for child in root.children(recursive=True):
                pids.add(child.pid)
        except psutil.Error:
            pass
        return pids

    try:
        out = subprocess.check_output(["ps", "-eo", "pid,ppid"], text=True)
    except Exception:
        return pids

    children_by_parent: dict[int, list[int]] = {}
    for line in out.splitlines()[1:]:
        parts = line.strip().split()
        if len(parts) != 2:
            continue
        try:
            pid = int(parts[0])
            ppid = int(parts[1])
        except ValueError:
            continue
        children_by_parent.setdefault(ppid, []).append(pid)

    frontier = [root_pid]
    while frontier:
        parent = frontier.pop()
        for child in children_by_parent.get(parent, []):
            if child not in pids:
                pids.add(child)
                frontier.append(child)

    return pids


def combined_rss_provider(
    server_pid_provider: Callable[[], int | None],
) -> Callable[[], float]:
    def _rss() -> float:
        total = read_proc_rss_mb(os.getpid())
        server_pid = server_pid_provider()
        if server_pid is not None:
            for pid in process_tree_pids(server_pid):
                total += read_proc_rss_mb(pid)
        return total

    return _rss


def combined_rss_provider_multi(
    server_pids_provider: Callable[[], List[int]],
) -> Callable[[], float]:
    def _rss() -> float:
        total = read_proc_rss_mb(os.getpid())
        for server_pid in server_pids_provider():
            for pid in process_tree_pids(server_pid):
                total += read_proc_rss_mb(pid)
        return total

    return _rss


def timed_section(name: str, fn, rss_provider: Callable[[], float] = rss_mb):
    start_t = time.perf_counter()
    start_rss = rss_provider()
    result = fn()
    end_rss = rss_provider()
    dur = time.perf_counter() - start_t
    print(
        f"[phase] {name:<12} time={dur:8.3f}s | "
        f"rss_before={start_rss:8.1f} MB | rss_after={end_rss:8.1f} MB | "
        f"delta={end_rss - start_rss:8.1f} MB"
    )
    return result, dur, start_rss, end_rss


def start_cpu_logger(
    interval_sec: int = 2,
    rss_provider: Callable[[], float] = rss_mb,
):
    if not psutil:
        return None

    proc = psutil.Process()
    proc.cpu_percent(None)
    stop_event = threading.Event()

    def _loop():
        while not stop_event.wait(interval_sec):
            cpu = proc.cpu_percent(None)
            rss = rss_provider()
            print(f"[cpu] {cpu:5.1f}% | rss={rss:8.1f} MB")

    thread = threading.Thread(target=_loop, daemon=True)
    thread.start()
    return stop_event


def configure_reproducibility(seed: int) -> None:
    random.seed(seed)
    np.random.seed(seed)

    try:
        import torch

        torch.manual_seed(seed)
        if torch.cuda.is_available():
            torch.cuda.manual_seed_all(seed)
    except ImportError:
        pass


def parse_overqueries(raw: str) -> List[float]:
    values: List[float] = []
    for part in raw.split(","):
        token = part.strip()
        if not token:
            continue
        try:
            val = float(token)
        except ValueError as exc:
            raise SystemExit(f"Invalid overquery value: {token}") from exc
        if val <= 0:
            raise SystemExit("overquery values must be positive")
        values.append(val)
    if not values:
        raise SystemExit("No overquery values provided")
    return values


def _match_label(pattern: re.Pattern[str], name: str) -> str | None:
    m = pattern.match(name)
    if m:
        return m.group(1)
    return None


def _sort_key(path: Path, pattern: re.Pattern[str]) -> tuple[int, object]:
    label = _match_label(pattern, path.name)
    if label is None:
        return (2, path.name)
    if label.isdigit():
        return (0, int(label))
    return (1, label)


def resolve_msmarco_dataset(
    dataset_dir: Path,
) -> tuple[List[dict], Path, int, str, int]:
    if not dataset_dir.is_dir():
        raise SystemExit(f"{dataset_dir} is not a directory")

    meta_files = sorted(
        dataset_dir.glob("msmarco-passages-*.meta.json"),
        key=lambda p: _sort_key(p, META_FILENAME_RE),
    )
    if not meta_files:
        raise SystemExit("No MSMARCO .meta.json found in dataset directory")

    meta_path = meta_files[-1]
    label = _match_label(META_FILENAME_RE, meta_path.name) or ""
    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    dim = int(meta["dim"])

    gt_candidates = sorted(
        dataset_dir.glob("msmarco-passages-*.gt.jsonl"),
        key=lambda p: _sort_key(p, GT_FILENAME_RE),
    )
    if not gt_candidates:
        raise SystemExit("No MSMARCO GT file found (*.gt.jsonl)")
    gt_path = gt_candidates[-1]

    shard_pattern = (
        f"msmarco-passages-{label}.shard*.f32"
        if label
        else "msmarco-passages-*.shard*.f32"
    )
    shards = sorted(dataset_dir.glob(shard_pattern))
    if not shards:
        raise SystemExit("No MSMARCO shard files found")

    sources: List[dict] = []
    total = 0
    for shard in shards:
        rows = (shard.stat().st_size // 4) // dim
        sources.append({"path": shard, "start": total, "count": rows})
        total += rows

    return sources, gt_path, dim, label or "dataset", total


def resolve_stackoverflow_dataset(
    dataset_dir: Path, dataset_name: str
) -> tuple[List[dict], Path, int, str, int]:
    vectors_dir = dataset_dir / "vectors"
    if not vectors_dir.is_dir():
        raise SystemExit(f"StackOverflow vectors directory not found: {vectors_dir}")

    corpus = "all"
    meta_path = vectors_dir / f"{dataset_name}-{corpus}.meta.json"
    gt_path = vectors_dir / f"{dataset_name}-{corpus}.gt.jsonl"

    if not meta_path.exists():
        raise SystemExit(f"StackOverflow vector meta not found: {meta_path}")
    if not gt_path.exists():
        raise SystemExit(f"StackOverflow GT not found: {gt_path}")

    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    dim = int(meta["dim"])

    shard_items = meta.get("shards", [])
    if not shard_items:
        raise SystemExit(f"No shards listed in {meta_path}")

    sources: List[dict] = []
    total = 0
    for shard in shard_items:
        shard_path = vectors_dir / str(shard["path"])
        if not shard_path.exists():
            raise SystemExit(f"Missing shard file: {shard_path}")
        shard_count = int(shard["count"])
        shard_start = int(shard.get("start", total))
        sources.append({"path": shard_path, "start": shard_start, "count": shard_count})
        total = max(total, shard_start + shard_count)

    label = str(meta.get("corpus") or corpus)
    return sources, gt_path, dim, label, total


def resolve_dataset(
    dataset_name: str,
) -> tuple[List[dict], Path, int, str, int, Path]:
    dataset_dir = Path(__file__).parent / "data" / dataset_name
    if not dataset_dir.exists():
        raise SystemExit(f"Dataset not found: {dataset_dir}")

    if dataset_name.upper().startswith("MSMARCO-"):
        sources, gt_path, dim, label, total = resolve_msmarco_dataset(dataset_dir)
        return sources, gt_path, dim, label, total, dataset_dir

    if dataset_name.startswith("stackoverflow-"):
        sources, gt_path, dim, label, total = resolve_stackoverflow_dataset(
            dataset_dir, dataset_name
        )
        return sources, gt_path, dim, label, total, dataset_dir

    raise SystemExit(
        "Unsupported dataset. Use MSMARCO-* or stackoverflow-* dataset names."
    )


def load_queries(gt_path: Path, limit: int | None) -> List[int]:
    qids: List[int] = []
    with open(gt_path, "r", encoding="utf-8") as handle:
        for line in handle:
            if limit is not None and len(qids) >= limit:
                break
            obj = json.loads(line)
            qids.append(int(obj["query_id"]))
    return qids


def load_ground_truth(gt_path: Path) -> dict[int, List[int]]:
    ground_truth: dict[int, List[int]] = {}
    with open(gt_path, "r", encoding="utf-8") as handle:
        for line in handle:
            obj = json.loads(line)
            qid = int(obj["query_id"])
            ground_truth[qid] = [int(entry["doc_id"]) for entry in obj.get("topk", [])]
    return ground_truth


def materialize_queries(
    sources: List[dict], query_ids: List[int], dim: int
) -> np.ndarray:
    queries = np.empty((len(query_ids), dim), dtype=np.float32)
    shard_map: dict[Path, List[tuple[int, int]]] = {}

    for q_idx, qid in enumerate(query_ids):
        for source in sources:
            start = int(source["start"])
            end = start + int(source["count"])
            if start <= qid < end:
                shard_map.setdefault(source["path"], []).append((q_idx, qid - start))
                break

    for source in sources:
        assigns = shard_map.get(source["path"])
        if not assigns:
            continue
        mm = np.memmap(
            source["path"],
            mode="r",
            dtype=np.float32,
            shape=(int(source["count"]), dim),
        )
        for q_idx, local_idx in assigns:
            queries[q_idx] = mm[local_idx]
        del mm

    return queries


def materialize_corpus_vectors(sources: List[dict], dim: int) -> np.ndarray:
    total_rows = 0
    for source in sources:
        start = int(source["start"])
        count = int(source["count"])
        total_rows = max(total_rows, start + count)

    vectors = np.zeros((total_rows, dim), dtype=np.float32)
    for source in sorted(sources, key=lambda item: int(item["start"])):
        start = int(source["start"])
        count = int(source["count"])
        mm = np.memmap(
            source["path"],
            mode="r",
            dtype=np.float32,
            shape=(count, dim),
        )
        vectors[start : start + count] = mm
        del mm

    return vectors


def normalize_rows(vectors: np.ndarray) -> np.ndarray:
    norms = np.linalg.norm(vectors, axis=1, keepdims=True)
    norms = np.maximum(norms, 1e-12)
    return vectors / norms


def search_arcadedb(
    index,
    queries: np.ndarray,
    qids: List[int],
    gt_full: dict[int, List[int]],
    k: int,
    overquery_factor: float,
    quantization: str,
) -> dict:
    latencies_ms: List[float] = []
    recalls: List[float] = []

    def _extract_result_id(rec) -> int | None:
        if rec is None:
            return None

        if isinstance(rec, dict):
            rid = rec.get("id")
            if rid is not None:
                return int(rid)

            inner = rec.get("record")
            if inner is not None:
                rec = inner

        if hasattr(rec, "get"):
            rid = rec.get("id")
            if rid is not None:
                return int(rid)

        return None

    effective_overquery = max(1, int(round(overquery_factor)))

    for q_idx, qid in enumerate(qids):
        qvec = queries[q_idx]
        start = time.perf_counter()

        if isinstance(index, dict):
            db = index["db"]
            index_name = index["name"]
            qvec_literal = "[" + ", ".join(str(float(x)) for x in qvec.tolist()) + "]"
            rs = db.query(
                "sql",
                f"SELECT vectorNeighbors('{index_name}', {qvec_literal}, {int(k)}) as res",
            ).to_list()
            neighbors = rs[0].get("res") if rs else []
            results = [(rec, 0.0) for rec in neighbors]
        elif quantization.upper() == "PRODUCT":
            results = index.find_nearest_approximate(
                qvec, k=k, overquery_factor=effective_overquery
            )
        else:
            results = index.find_nearest(
                qvec,
                k=k,
                overquery_factor=effective_overquery,
            )

        latencies_ms.append((time.perf_counter() - start) * 1000)

        result_ids: List[int] = []
        for rec, _score in results:
            rid = _extract_result_id(rec)
            if rid is not None:
                result_ids.append(rid)

        gt_list = gt_full.get(qid)
        if not gt_list:
            continue

        retrieved = set(result_ids[:k])
        gt_set = set(gt_list[:k])
        recalls.append(len(retrieved & gt_set) / k)

    recall_mean = float(np.mean(recalls)) if recalls else None
    lat_mean = float(np.mean(latencies_ms)) if latencies_ms else None
    lat_p95 = float(np.percentile(latencies_ms, 95)) if latencies_ms else None

    return {
        "queries": len(qids),
        "recall_mean": recall_mean,
        "latency_ms_mean": lat_mean,
        "latency_ms_p95": lat_p95,
        "recall_count": len(recalls),
    }


def search_faiss(
    index,
    queries: np.ndarray,
    qids: List[int],
    gt_full: dict[int, List[int]],
    k: int,
    overquery_factor: float,
) -> dict:
    import faiss

    latencies_ms: List[float] = []
    recalls: List[float] = []

    ef_search = overquery_to_ef_search(k, overquery_factor)

    hnsw = None
    if hasattr(index, "hnsw"):
        hnsw = index.hnsw
    elif hasattr(index, "index") and hasattr(index.index, "hnsw"):
        hnsw = index.index.hnsw
    if hnsw is not None:
        hnsw.efSearch = int(ef_search)

    for q_idx, qid in enumerate(qids):
        qvec = np.ascontiguousarray(
            queries[q_idx : q_idx + 1].astype("float32", copy=True)
        )
        faiss.normalize_L2(qvec)

        start = time.perf_counter()
        _dist, ids = index.search(qvec, int(k))
        latencies_ms.append((time.perf_counter() - start) * 1000)

        result_ids = [int(doc_id) for doc_id in ids[0].tolist() if int(doc_id) >= 0]

        gt_list = gt_full.get(qid)
        if not gt_list:
            continue
        recalls.append(len(set(result_ids[:k]) & set(gt_list[:k])) / k)

    recall_mean = float(np.mean(recalls)) if recalls else None
    lat_mean = float(np.mean(latencies_ms)) if latencies_ms else None
    lat_p95 = float(np.percentile(latencies_ms, 95)) if latencies_ms else None

    return {
        "queries": len(qids),
        "recall_mean": recall_mean,
        "latency_ms_mean": lat_mean,
        "latency_ms_p95": lat_p95,
        "recall_count": len(recalls),
    }


def search_lancedb(
    table,
    queries: np.ndarray,
    qids: List[int],
    gt_full: dict[int, List[int]],
    k: int,
    overquery_factor: float,
    build_config: dict,
) -> dict:
    latencies_ms: List[float] = []
    recalls: List[float] = []

    lancedb_cfg = build_config.get("lancedb") if isinstance(build_config, dict) else {}
    if not isinstance(lancedb_cfg, dict):
        lancedb_cfg = {}

    index_type = str(lancedb_cfg.get("index_type") or "IVF_HNSW_SQ").upper()
    ef_search = overquery_to_ef_search(k, overquery_factor)
    nprobes = None
    if index_type.startswith("IVF_"):
        nprobes = (
            1
            if int(lancedb_cfg.get("num_partitions") or 1) <= 1
            else max(1, int(round(overquery_factor * 4)))
        )

    applied_ef_search = None
    applied_nprobes = None

    for q_idx, qid in enumerate(qids):
        start = time.perf_counter()
        search = table.search(queries[q_idx].tolist()).metric("cosine").limit(int(k))
        if hasattr(search, "ef"):
            try:
                search = search.ef(int(ef_search))
                applied_ef_search = int(ef_search)
            except Exception:
                pass
        elif hasattr(search, "ef_search"):
            try:
                search = search.ef_search(int(ef_search))
                applied_ef_search = int(ef_search)
            except Exception:
                pass

        if nprobes is not None and hasattr(search, "nprobes"):
            try:
                search = search.nprobes(int(nprobes))
                applied_nprobes = int(nprobes)
            except Exception:
                pass
        rows = search.to_list()
        latencies_ms.append((time.perf_counter() - start) * 1000)

        result_ids: List[int] = []
        for row in rows:
            rid = row.get("id") if isinstance(row, dict) else None
            if rid is not None:
                result_ids.append(int(rid))

        gt_list = gt_full.get(qid)
        if not gt_list:
            continue
        recalls.append(len(set(result_ids[:k]) & set(gt_list[:k])) / k)

    recall_mean = float(np.mean(recalls)) if recalls else None
    lat_mean = float(np.mean(latencies_ms)) if latencies_ms else None
    lat_p95 = float(np.percentile(latencies_ms, 95)) if latencies_ms else None

    return {
        "queries": len(qids),
        "recall_mean": recall_mean,
        "latency_ms_mean": lat_mean,
        "latency_ms_p95": lat_p95,
        "recall_count": len(recalls),
        "effective_ef_search": applied_ef_search,
        "effective_nprobes": applied_nprobes,
    }


def search_bruteforce(
    corpus_vectors_normalized: np.ndarray,
    queries: np.ndarray,
    qids: List[int],
    gt_full: dict[int, List[int]],
    k: int,
) -> dict:
    latencies_ms: List[float] = []
    recalls: List[float] = []

    query_norm = np.linalg.norm(queries, axis=1, keepdims=True)
    query_norm = np.maximum(query_norm, 1e-12)
    queries_normalized = queries / query_norm

    corpus_rows = int(corpus_vectors_normalized.shape[0])
    topk = min(int(k), corpus_rows)
    if topk <= 0:
        raise SystemExit("Bruteforce backend has no corpus vectors to search")

    for q_idx, qid in enumerate(qids):
        start = time.perf_counter()
        scores = corpus_vectors_normalized @ queries_normalized[q_idx]
        if topk == corpus_rows:
            ranked_idx = np.argsort(scores)[::-1][:topk]
        else:
            candidate_idx = np.argpartition(scores, -topk)[-topk:]
            ranked_idx = candidate_idx[np.argsort(scores[candidate_idx])[::-1]]
        latencies_ms.append((time.perf_counter() - start) * 1000)

        result_ids = [int(doc_id) for doc_id in ranked_idx.tolist()]

        gt_list = gt_full.get(qid)
        if not gt_list:
            continue
        recalls.append(len(set(result_ids[:k]) & set(gt_list[:k])) / k)

    recall_mean = float(np.mean(recalls)) if recalls else None
    lat_mean = float(np.mean(latencies_ms)) if latencies_ms else None
    lat_p95 = float(np.percentile(latencies_ms, 95)) if latencies_ms else None

    return {
        "queries": len(qids),
        "recall_mean": recall_mean,
        "latency_ms_mean": lat_mean,
        "latency_ms_p95": lat_p95,
        "recall_count": len(recalls),
    }


def open_lancedb_table(lancedb_dir: Path, table_name: str):
    import lancedb

    db = lancedb.connect(str(lancedb_dir))
    table = db.open_table(table_name)
    return db, table


def vector_to_pg_literal(vec: np.ndarray) -> str:
    return "[" + ",".join(f"{float(x):.7g}" for x in vec.tolist()) + "]"


def overquery_to_ef_search(k: int, overquery_factor: float) -> int:
    return max(
        int(k),
        int(round(HNSW_EF_NORMALIZATION * k * overquery_factor)),
    )


def search_pgvector(
    conn,
    queries: np.ndarray,
    qids: List[int],
    gt_full: dict[int, List[int]],
    k: int,
    overquery_factor: float,
) -> dict:
    latencies_ms: List[float] = []
    recalls: List[float] = []

    ef_search = overquery_to_ef_search(k, overquery_factor)
    with conn.cursor() as cur:
        for q_idx, qid in enumerate(qids):
            start = time.perf_counter()
            cur.execute(f"SET LOCAL hnsw.ef_search = {int(ef_search)}")
            cur.execute(
                "SELECT id FROM vectordata ORDER BY vector <=> %s::vector LIMIT %s",
                (vector_to_pg_literal(queries[q_idx]), int(k)),
            )
            rows = cur.fetchall()
            latencies_ms.append((time.perf_counter() - start) * 1000)

            result_ids = [int(row[0]) for row in rows]
            gt_list = gt_full.get(qid)
            if not gt_list:
                continue
            recalls.append(len(set(result_ids[:k]) & set(gt_list[:k])) / k)

    recall_mean = float(np.mean(recalls)) if recalls else None
    lat_mean = float(np.mean(latencies_ms)) if latencies_ms else None
    lat_p95 = float(np.percentile(latencies_ms, 95)) if latencies_ms else None

    return {
        "queries": len(qids),
        "recall_mean": recall_mean,
        "latency_ms_mean": lat_mean,
        "latency_ms_p95": lat_p95,
        "recall_count": len(recalls),
    }


def search_qdrant(
    client,
    collection_name: str,
    queries: np.ndarray,
    qids: List[int],
    gt_full: dict[int, List[int]],
    k: int,
    overquery_factor: float,
) -> dict:
    from qdrant_client import models

    latencies_ms: List[float] = []
    recalls: List[float] = []

    ef_search = overquery_to_ef_search(k, overquery_factor)
    for q_idx, qid in enumerate(qids):
        start = time.perf_counter()
        response = client.query_points(
            collection_name=collection_name,
            query=queries[q_idx].tolist(),
            limit=int(k),
            search_params=models.SearchParams(hnsw_ef=int(ef_search)),
            with_payload=False,
            with_vectors=False,
        )
        latencies_ms.append((time.perf_counter() - start) * 1000)

        points = getattr(response, "points", response)
        result_ids: List[int] = []
        for point in points:
            point_id = getattr(point, "id", None)
            if point_id is not None:
                result_ids.append(int(point_id))

        gt_list = gt_full.get(qid)
        if not gt_list:
            continue
        recalls.append(len(set(result_ids[:k]) & set(gt_list[:k])) / k)

    recall_mean = float(np.mean(recalls)) if recalls else None
    lat_mean = float(np.mean(latencies_ms)) if latencies_ms else None
    lat_p95 = float(np.percentile(latencies_ms, 95)) if latencies_ms else None

    return {
        "queries": len(qids),
        "recall_mean": recall_mean,
        "latency_ms_mean": lat_mean,
        "latency_ms_p95": lat_p95,
        "recall_count": len(recalls),
    }


def get_qdrant_version(client) -> str | None:
    try:
        info = client.info()
        version = getattr(info, "version", None)
        return str(version) if version else None
    except Exception:
        return None


def qdrant_project_name(db_path: Path) -> str:
    digest = hashlib.sha1(str(db_path).encode("utf-8")).hexdigest()[:10]
    return f"arcadb-qdrant-{digest}"


def qdrant_container_name(db_path: Path) -> str:
    return f"{qdrant_project_name(db_path)}-server"


def start_qdrant_container(
    *,
    db_path: Path,
    data_dir: Path,
    port: int,
    mem_limit: str,
    cpus: float,
    image: str,
) -> str:
    docker_bin = resolve_docker_binary()
    container_name = qdrant_container_name(db_path)
    data_dir.mkdir(parents=True, exist_ok=True)

    subprocess.run(
        [docker_bin, "rm", "-f", container_name],
        check=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    host_data_dir = str(data_dir.resolve())

    out = subprocess.check_output(
        [
            docker_bin,
            "run",
            "-d",
            "--name",
            container_name,
            "-p",
            f"{int(port)}:6333",
            "-v",
            f"{host_data_dir}:/qdrant/storage",
            "--memory",
            mem_limit,
            "--memory-swap",
            mem_limit,
            "--cpus",
            str(cpus),
            image,
        ],
        text=True,
    ).strip()
    if not out:
        raise SystemExit("Failed to start Qdrant container")
    return out


def stop_qdrant_container(db_path: Path) -> None:
    docker_bin = resolve_docker_binary()
    container_name = qdrant_container_name(db_path)
    subprocess.run(
        [docker_bin, "rm", "-f", container_name],
        check=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def qdrant_container_pid(db_path: Path) -> int | None:
    docker_bin = resolve_docker_binary()
    container_name = qdrant_container_name(db_path)
    try:
        out = subprocess.check_output(
            [docker_bin, "inspect", "-f", "{{.State.Pid}}", container_name],
            text=True,
        ).strip()
        pid = int(out)
        return pid if pid > 0 else None
    except (subprocess.SubprocessError, OSError, ValueError):
        return None


def wait_for_qdrant_ready(host: str, port: int, timeout_sec: int = 120) -> None:
    start = time.perf_counter()
    urls = [
        f"http://{host}:{port}/readyz",
        f"http://{host}:{port}/collections",
    ]
    while True:
        for url in urls:
            try:
                with urlopen(url, timeout=3) as response:
                    if 200 <= int(response.status) < 500:
                        return
            except Exception:
                continue

        if time.perf_counter() - start > timeout_sec:
            raise SystemExit(
                f"Qdrant did not become ready within {timeout_sec}s at {host}:{port}"
            )
        time.sleep(1)


def search_milvus(
    collection,
    queries: np.ndarray,
    qids: List[int],
    gt_full: dict[int, List[int]],
    k: int,
    overquery_factor: float,
) -> dict:
    def is_transient_milvus_search_error(exc: Exception) -> bool:
        msg = str(exc).lower()
        transient_tokens = (
            "channel distribution is not serviceable",
            "channel not available",
            "querynode",
            "service unavailable",
            "code=503",
        )
        return any(token in msg for token in transient_tokens)

    latencies_ms: List[float] = []
    recalls: List[float] = []

    ef_search = overquery_to_ef_search(k, overquery_factor)
    search_params = {
        "metric_type": "COSINE",
        "params": {"ef": int(ef_search)},
    }

    for q_idx, qid in enumerate(qids):
        start = time.perf_counter()
        rows = None
        max_retries = 30
        retry_delay_sec = 1.0
        for attempt in range(max_retries):
            try:
                rows = collection.search(
                    data=[queries[q_idx].tolist()],
                    anns_field="vector",
                    param=search_params,
                    limit=int(k),
                    output_fields=["id"],
                )
                break
            except Exception as exc:
                if not is_transient_milvus_search_error(exc):
                    raise
                if attempt == max_retries - 1:
                    raise
                time.sleep(retry_delay_sec)

        if rows is None:
            raise RuntimeError("Milvus search returned no result rows")
        latencies_ms.append((time.perf_counter() - start) * 1000)

        hits = rows[0] if rows else []
        result_ids = [int(getattr(hit, "id", -1)) for hit in hits]

        gt_list = gt_full.get(qid)
        if not gt_list:
            continue
        recalls.append(len(set(result_ids[:k]) & set(gt_list[:k])) / k)

    recall_mean = float(np.mean(recalls)) if recalls else None
    lat_mean = float(np.mean(latencies_ms)) if latencies_ms else None
    lat_p95 = float(np.percentile(latencies_ms, 95)) if latencies_ms else None

    return {
        "queries": len(qids),
        "recall_mean": recall_mean,
        "latency_ms_mean": lat_mean,
        "latency_ms_p95": lat_p95,
        "recall_count": len(recalls),
    }


def run_repeated_search(
    search_fn: Callable[[np.ndarray, List[int]], dict],
    *,
    queries: np.ndarray,
    qids: List[int],
    query_runs: int,
    query_order: str,
    seed: int,
) -> dict:
    if query_runs < 1:
        raise SystemExit("--query-runs must be >= 1")

    per_run_stats: List[dict] = []

    for run_idx in range(query_runs):
        if query_order == "shuffled":
            order = list(range(len(qids)))
            random.Random(seed + run_idx).shuffle(order)
            run_qids = [qids[idx] for idx in order]
            run_queries = queries[order]
        else:
            run_qids = qids
            run_queries = queries

        run_stats = search_fn(run_queries, run_qids)
        run_stats = {
            **run_stats,
            "run": run_idx + 1,
            "query_order_hash": hashlib.sha1(
                ",".join(str(v) for v in run_qids).encode("utf-8")
            ).hexdigest(),
        }
        per_run_stats.append(run_stats)

    recall_vals = [
        float(item["recall_mean"])
        for item in per_run_stats
        if item.get("recall_mean") is not None
    ]
    latency_mean_vals = [
        float(item["latency_ms_mean"])
        for item in per_run_stats
        if item.get("latency_ms_mean") is not None
    ]
    latency_p95_vals = [
        float(item["latency_ms_p95"])
        for item in per_run_stats
        if item.get("latency_ms_p95") is not None
    ]

    baseline_queries = int(per_run_stats[0].get("queries", 0)) if per_run_stats else 0
    baseline_recall_count = (
        int(per_run_stats[0].get("recall_count", 0)) if per_run_stats else 0
    )
    cold_run_latency_ms = (
        float(per_run_stats[0].get("latency_ms_mean"))
        if per_run_stats and per_run_stats[0].get("latency_ms_mean") is not None
        else None
    )
    warm_run_latencies_ms = [
        float(item.get("latency_ms_mean"))
        for item in per_run_stats[1:]
        if item.get("latency_ms_mean") is not None
    ]
    consistent = all(
        int(item.get("queries", -1)) == baseline_queries
        and int(item.get("recall_count", -1)) == baseline_recall_count
        for item in per_run_stats
    )

    return {
        "queries": baseline_queries,
        "recall_count": baseline_recall_count,
        "recall_mean": float(np.mean(recall_vals)) if recall_vals else None,
        "latency_ms_mean": (
            float(np.mean(latency_mean_vals)) if latency_mean_vals else None
        ),
        "latency_ms_p95": (
            float(np.mean(latency_p95_vals)) if latency_p95_vals else None
        ),
        "recall_mean_min": float(np.min(recall_vals)) if recall_vals else None,
        "recall_mean_max": float(np.max(recall_vals)) if recall_vals else None,
        "latency_ms_mean_min": (
            float(np.min(latency_mean_vals)) if latency_mean_vals else None
        ),
        "latency_ms_mean_max": (
            float(np.max(latency_mean_vals)) if latency_mean_vals else None
        ),
        "latency_ms_p95_min": (
            float(np.min(latency_p95_vals)) if latency_p95_vals else None
        ),
        "latency_ms_p95_max": (
            float(np.max(latency_p95_vals)) if latency_p95_vals else None
        ),
        "query_runs": query_runs,
        "query_order": query_order,
        "seed": seed,
        "consistent_across_runs": consistent,
        "query_cold_time_s": (
            cold_run_latency_ms / 1000.0 if cold_run_latency_ms is not None else None
        ),
        "query_warm_mean_s": (
            (float(np.mean(warm_run_latencies_ms)) / 1000.0)
            if warm_run_latencies_ms
            else None
        ),
        "query_result_hash_stable": consistent,
        "query_row_count_stable": consistent,
        "runs": per_run_stats,
    }


def resolve_docker_binary() -> str:
    docker_bin = shutil.which("docker") or shutil.which("docker.io")
    if docker_bin:
        return docker_bin
    for candidate in (
        Path("/usr/bin/docker"),
        Path("/usr/sbin/docker"),
        Path("/usr/local/bin/docker"),
        Path("/usr/bin/docker.io"),
        Path("/usr/sbin/docker.io"),
    ):
        if candidate.exists():
            return str(candidate)
    return "docker"


def docker_compose_base_args(compose_file: Path, project_name: str) -> List[str]:
    docker_bin = resolve_docker_binary()
    return [
        docker_bin,
        "compose",
        "-f",
        str(compose_file),
        "-p",
        project_name,
    ]


def ensure_milvus_compose_file(compose_file: Path, release_tag: str) -> None:
    if compose_file.exists():
        raw = compose_file.read_text(encoding="utf-8")
    else:
        compose_file.parent.mkdir(parents=True, exist_ok=True)
        url = (
            "https://github.com/milvus-io/milvus/releases/download/"
            f"{release_tag}/milvus-standalone-docker-compose.yml"
        )
        urlretrieve(url, str(compose_file))
        raw = compose_file.read_text(encoding="utf-8")

    sanitized = re.sub(r"(?m)^\s*version\s*:\s*.*\n", "", raw)
    sanitized = re.sub(r"(?m)^\s*container_name:\s*.*\n", "", sanitized)
    sanitized = re.sub(
        r"(?ms)(^\s*networks:\s*\n(?:.*\n)*?^\s*milvus:\s*\n)(\s*name:\s*milvus\s*\n)",
        r"\1",
        sanitized,
    )
    if sanitized != raw:
        compose_file.write_text(sanitized, encoding="utf-8")


def milvus_project_name(db_path: Path) -> str:
    digest = hashlib.sha1(str(db_path).encode("utf-8")).hexdigest()[:10]
    return f"arcadb-milvus-{digest}"


def milvus_container_ids(compose_file: Path, project_name: str) -> List[str]:
    try:
        out = subprocess.check_output(
            [*docker_compose_base_args(compose_file, project_name), "ps", "-q"],
            text=True,
        )
    except (subprocess.SubprocessError, OSError):
        return []
    return [line.strip() for line in out.splitlines() if line.strip()]


def milvus_container_name(container_id: str) -> str:
    docker_bin = resolve_docker_binary()
    try:
        out = subprocess.check_output(
            [docker_bin, "inspect", "-f", "{{.Name}}", container_id],
            text=True,
        ).strip()
        return out.lstrip("/").lower()
    except (subprocess.SubprocessError, OSError):
        return ""


def milvus_container_role(name: str) -> str | None:
    normalized = name.lower()
    for role in ("standalone", "minio", "etcd"):
        if role in normalized:
            return role
    return None


def milvus_weight_map(
    container_ids: List[str],
    weights_by_role: Dict[str, float],
) -> Dict[str, float]:
    names = {cid: milvus_container_name(cid) for cid in container_ids}
    return {
        cid: weights_by_role.get(milvus_container_role(names[cid]) or "", 0.0)
        for cid in container_ids
    }


def milvus_role_allocation(
    total_amount: int | float,
    weights_by_role: Dict[str, float],
) -> Dict[str, float]:
    total_weight = sum(weights_by_role.values())
    if total_weight <= 0:
        return {}
    return {
        role: float(total_amount) * weight / total_weight
        for role, weight in weights_by_role.items()
    }


def milvus_memory_limits_by_container(
    container_ids: List[str],
    total_mem_limit: str,
) -> Dict[str, str]:
    if not container_ids:
        return {}

    total_mib = max(parse_size_to_mib(total_mem_limit), len(container_ids))
    weight_map = milvus_weight_map(container_ids, MILVUS_MEMORY_WEIGHTS)

    total_weight = sum(weight_map.values())
    if total_weight <= 0:
        base = total_mib // len(container_ids)
        rem = total_mib % len(container_ids)
        result: Dict[str, str] = {}
        for idx, cid in enumerate(container_ids):
            mib = base + (1 if idx < rem else 0)
            result[cid] = f"{max(1, mib)}m"
        return result

    raw = {cid: (total_mib * weight_map[cid] / total_weight) for cid in container_ids}
    alloc = {cid: max(1, int(raw[cid])) for cid in container_ids}
    assigned = sum(alloc.values())

    if assigned < total_mib:
        order = sorted(
            container_ids,
            key=lambda cid: (raw[cid] - int(raw[cid])),
            reverse=True,
        )
        missing = total_mib - assigned
        for i in range(missing):
            alloc[order[i % len(order)]] += 1
    elif assigned > total_mib:
        order = sorted(
            container_ids,
            key=lambda cid: (raw[cid] - int(raw[cid])),
        )
        excess = assigned - total_mib
        idx = 0
        while excess > 0 and idx < len(order) * 4:
            cid = order[idx % len(order)]
            if alloc[cid] > 1:
                alloc[cid] -= 1
                excess -= 1
            idx += 1

    return {cid: f"{mib}m" for cid, mib in alloc.items()}


def milvus_cpu_limits_by_container(
    container_ids: List[str],
    total_cpus: float,
) -> Dict[str, str]:
    if not container_ids:
        return {}

    total = max(float(total_cpus), 0.01)
    weight_map = milvus_weight_map(container_ids, MILVUS_CPU_WEIGHTS)

    total_weight = sum(weight_map.values())
    if total_weight <= 0:
        per_container = total / len(container_ids)
        return {cid: format_cpu_quota(per_container) for cid in container_ids}

    cpu_limits: Dict[str, str] = {}
    for cid in container_ids:
        allocated = total * weight_map[cid] / total_weight
        cpu_limits[cid] = format_cpu_quota(max(allocated, 0.01))

    return cpu_limits


def apply_container_limits(
    container_ids: List[str],
    *,
    mem_limit: str,
    cpus: float,
) -> None:
    docker_bin = resolve_docker_binary()
    mem_limits = milvus_memory_limits_by_container(container_ids, mem_limit)
    cpu_limits = milvus_cpu_limits_by_container(container_ids, cpus)
    for container_id in container_ids:
        container_mem = mem_limits.get(container_id, mem_limit)
        container_cpu = cpu_limits.get(container_id, format_cpu_quota(cpus))
        subprocess.run(
            [
                docker_bin,
                "update",
                "--memory",
                container_mem,
                "--memory-swap",
                container_mem,
                "--cpus",
                container_cpu,
                container_id,
            ],
            check=True,
        )


def start_milvus_compose(
    compose_file: Path,
    project_name: str,
    *,
    mem_limit: str,
    cpus: float,
) -> None:
    cmd = [*docker_compose_base_args(compose_file, project_name), "up", "-d"]
    subprocess.run(cmd, check=True)
    container_ids = milvus_container_ids(compose_file, project_name)
    if not container_ids:
        raise SystemExit("Milvus compose started but no containers were found")
    apply_container_limits(container_ids, mem_limit=mem_limit, cpus=cpus)


def stop_milvus_compose(compose_file: Path, project_name: str) -> None:
    cmd = [*docker_compose_base_args(compose_file, project_name), "down"]
    subprocess.run(cmd, check=True)


def milvus_container_pids(compose_file: Path, project_name: str) -> List[int]:
    container_ids = milvus_container_ids(compose_file, project_name)
    docker_bin = resolve_docker_binary()
    pids: List[int] = []
    for container_id in container_ids:
        try:
            pid_text = subprocess.check_output(
                [docker_bin, "inspect", "-f", "{{.State.Pid}}", container_id],
                text=True,
            ).strip()
            pid = int(pid_text)
            if pid > 0:
                pids.append(pid)
        except (subprocess.SubprocessError, OSError, ValueError):
            continue

    return pids


def wait_for_milvus_ready(host: str, port: int, timeout_sec: int = 300) -> None:
    import logging

    from pymilvus import connections, utility

    logging.getLogger("pymilvus").setLevel(logging.CRITICAL)

    start = time.perf_counter()
    last_report = 0.0
    print(
        f"[milvus] Waiting for server readiness at {host}:{port} "
        f"(timeout={timeout_sec}s)"
    )
    while True:
        try:
            connections.connect(alias="_bootstrap", host=host, port=str(port))
            _ = utility.get_server_version(using="_bootstrap")
            connections.disconnect(alias="_bootstrap")
            elapsed = time.perf_counter() - start
            print(f"[milvus] Server ready after {elapsed:.1f}s")
            return
        except Exception:
            elapsed = time.perf_counter() - start
            if elapsed > timeout_sec:
                raise SystemExit(
                    "Milvus did not become ready within "
                    f"{timeout_sec}s at {host}:{port}"
                )
            if elapsed - last_report >= 5.0:
                print(f"[milvus] Still waiting for server readiness... {elapsed:.1f}s")
                last_report = elapsed
            time.sleep(1)


def wait_for_milvus_collection_load(
    collection,
    *,
    timeout_sec: int = 300,
    poll_sec: float = 1.0,
) -> None:
    import logging

    logging.getLogger("pymilvus").setLevel(logging.CRITICAL)
    start = time.perf_counter()
    last_report = 0.0
    collection_name = getattr(collection, "name", "<unknown>")
    print(
        f"[milvus] Waiting for collection load: {collection_name} "
        f"(timeout={timeout_sec}s)"
    )
    while True:
        try:
            collection.load()
            elapsed = time.perf_counter() - start
            print(f"[milvus] Collection loaded after {elapsed:.1f}s: {collection_name}")
            return
        except Exception as exc:
            msg = str(exc)
            transient = (
                "service resource insufficient" in msg
                or "currentStreamingNode=0" in msg
                or "expectedStreamingNode=1" in msg
            )
            if not transient:
                raise

            elapsed = time.perf_counter() - start
            if elapsed > timeout_sec:
                raise SystemExit(
                    "Milvus collection load did not become ready within "
                    f"{timeout_sec}s"
                ) from exc
            if elapsed - last_report >= 5.0:
                print(f"[milvus] Still waiting for collection load... {elapsed:.1f}s")
                last_report = elapsed
            time.sleep(poll_sec)


def get_milvus_version(host: str, port: int) -> str | None:
    try:
        from pymilvus import connections, utility

        alias = "_version"
        connections.connect(alias=alias, host=host, port=str(port))
        version = utility.get_server_version(using=alias)
        connections.disconnect(alias=alias)
        return str(version) if version else None
    except Exception:
        return None


def load_existing_build_config(db_path: Path) -> dict:
    config: dict = {}
    results_json = db_path / "results.json"
    if not results_json.exists():
        return config

    try:
        payload = json.loads(results_json.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return config

    return payload.get("config", {}) if isinstance(payload, dict) else {}


def find_binary(name: str) -> str:
    direct = shutil.which(name)
    if direct:
        return direct

    for base in (Path("/usr/lib/postgresql"), Path("/usr/local/bin"), Path("/usr/bin")):
        if base.is_dir() and base.name == "postgresql":
            for version_dir in sorted(base.glob("*"), reverse=True):
                candidate = version_dir / "bin" / name
                if candidate.exists():
                    return str(candidate)
        else:
            candidate = base / name
            if candidate.exists():
                return str(candidate)

    raise SystemExit(f"Required PostgreSQL binary not found in PATH: {name}")


def start_postgres(data_dir: Path, port: int, shared_buffers: str) -> None:
    pg_ctl = find_binary("pg_ctl")
    opts = f"-F -p {port} -c shared_buffers={shared_buffers}"
    subprocess.run([pg_ctl, "-D", str(data_dir), "-o", opts, "-w", "start"], check=True)


def stop_postgres(data_dir: Path) -> None:
    pg_ctl = find_binary("pg_ctl")
    subprocess.run([pg_ctl, "-D", str(data_dir), "-m", "fast", "stop"], check=True)


def get_postmaster_pid(data_dir: Path) -> int | None:
    pid_file = data_dir / "postmaster.pid"
    if not pid_file.exists():
        return None
    try:
        return int(pid_file.read_text(encoding="utf-8").splitlines()[0].strip())
    except (OSError, ValueError, IndexError):
        return None


def default_docker_image(backend: str) -> str:
    if backend == "pgvector":
        return resolve_latest_pgvector_image()
    return "python:3.12-slim"


def parse_size_to_mib(value: str) -> int:
    match = SIZE_TOKEN_RE.match(value)
    if not match:
        raise ValueError(f"Invalid size: {value}")

    amount = float(match.group(1))
    unit = (match.group(2) or "m").lower()
    scale = {
        "k": 1 / 1024,
        "m": 1,
        "g": 1024,
        "t": 1024 * 1024,
        "": 1,
    }[unit]
    mib = int(amount * scale)
    return max(1, mib)


def format_mib_as_docker_limit(mib: int) -> str:
    return f"{max(1, int(mib))}m"


def split_total_memory_budget(
    total_mem_limit: str, server_fraction: float
) -> tuple[str, str]:
    total_mib = parse_size_to_mib(total_mem_limit)
    server_mib = int(round(total_mib * server_fraction))
    server_mib = max(1, min(total_mib - 1 if total_mib > 1 else 1, server_mib))
    client_mib = max(1, total_mib - server_mib)
    return format_mib_as_docker_limit(client_mib), format_mib_as_docker_limit(
        server_mib
    )


def split_total_cpu_budget(
    total_cpus: float, server_fraction: float
) -> tuple[float, float]:
    total = max(0.1, float(total_cpus))
    if total <= 0.2:
        half = total / 2.0
        return half, total - half

    server = total * server_fraction
    min_share = min(0.5, total / 2.0)
    server = max(min_share, min(total - min_share, server))
    client = max(0.1, total - server)
    return client, server


def format_cpu_quota(cpus: float) -> str:
    value = max(0.01, float(cpus))
    text = f"{value:.3f}".rstrip("0").rstrip(".")
    return text if text else "0.01"


def resolve_server_fraction(args) -> float:
    fraction = float(getattr(args, "server_fraction", 0.8))
    if not 0.0 < fraction < 1.0:
        raise SystemExit("--server-fraction must be > 0 and < 1")
    return fraction


def backend_server_budget(args) -> tuple[str, float]:
    backend = args.backend
    mem_limit = args.mem_limit
    cpus = float(args.threads)

    if backend not in {"qdrant", "milvus"}:
        return mem_limit, cpus

    if not is_running_in_docker():
        return mem_limit, cpus

    server_fraction = resolve_server_fraction(args)
    _client_mem, server_mem = split_total_memory_budget(mem_limit, server_fraction)
    _client_cpu, server_cpu = split_total_cpu_budget(cpus, server_fraction)
    return server_mem, server_cpu


def backend_client_budget(args) -> tuple[str, float]:
    backend = args.backend
    mem_limit = args.mem_limit
    cpus = float(args.threads)

    if backend not in {"qdrant", "milvus"}:
        return mem_limit, cpus

    server_fraction = resolve_server_fraction(args)
    client_mem, _server_mem = split_total_memory_budget(mem_limit, server_fraction)
    client_cpu, _server_cpu = split_total_cpu_budget(cpus, server_fraction)
    return client_mem, client_cpu


def budget_allocation_report(args) -> dict:
    total_mem = args.mem_limit
    total_cpus = float(args.threads)
    backend = args.backend

    report = {
        "model": "single_global_budget",
        "backend": backend,
        "total": {
            "memory_limit": total_mem,
            "cpus": format_cpu_quota(total_cpus),
            "threads_input": args.threads,
        },
    }

    if backend in {"qdrant", "milvus"}:
        server_fraction = resolve_server_fraction(args)
        client_mem, server_mem = split_total_memory_budget(total_mem, server_fraction)
        client_cpu, server_cpu = split_total_cpu_budget(total_cpus, server_fraction)
        report["split"] = {
            "client_fraction": round(1.0 - server_fraction, 2),
            "server_fraction": round(server_fraction, 2),
            "wrapper_client_allocation": {
                "memory_limit": client_mem,
                "cpus": format_cpu_quota(client_cpu),
            },
            "server_allocation": {
                "memory_limit": server_mem,
                "cpus": format_cpu_quota(server_cpu),
            },
        }
        if backend == "milvus":
            server_mem_mib = parse_size_to_mib(server_mem)
            report["split"]["server_container_allocation"] = {
                "memory_limit": {
                    role: format_mib_as_docker_limit(int(round(mib)))
                    for role, mib in milvus_role_allocation(
                        server_mem_mib, MILVUS_MEMORY_WEIGHTS
                    ).items()
                },
                "cpus": {
                    role: format_cpu_quota(cpus)
                    for role, cpus in milvus_role_allocation(
                        server_cpu, MILVUS_CPU_WEIGHTS
                    ).items()
                },
            }
    else:
        report["split"] = {
            "client_fraction": 1.0,
            "server_fraction": 0.0,
            "single_runtime_allocation": {
                "memory_limit": total_mem,
                "cpus": format_cpu_quota(total_cpus),
            },
        }

    return report


def resolve_pg_shared_buffers(mem_limit: str, configured: str | None) -> str:
    if configured:
        return configured

    total_mib = parse_size_to_mib(mem_limit)
    shared_mib = max(16, int(total_mib * 0.25))
    return f"{shared_mib}MB"


def resolve_arcadedb_heap_size(
    mem_limit: str,
    heap_fraction: float,
) -> str:
    if heap_fraction <= 0 or heap_fraction > 1:
        raise ValueError(f"Invalid heap fraction: {heap_fraction}")

    total_mib = parse_size_to_mib(mem_limit)
    heap_mib = max(256, int(total_mib * heap_fraction))
    return f"{heap_mib}m"


def ensure_milvus_runner_image(docker_bin: str) -> str:
    image_tag = "arcadedb-milvus-bench-runner:py312-v1"
    inspect = subprocess.run(
        [docker_bin, "image", "inspect", image_tag],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    if inspect.returncode == 0:
        return image_tag

    print("Building Milvus benchmark runner image (first run only)...")
    with tempfile.TemporaryDirectory(prefix="milvus-runner-") as tmp_dir:
        tmp_path = Path(tmp_dir)
        dockerfile = tmp_path / "Dockerfile"
        dockerfile.write_text(MILVUS_RUNNER_DOCKERFILE, encoding="utf-8")
        subprocess.run(
            [
                docker_bin,
                "build",
                "-t",
                image_tag,
                str(tmp_path),
            ],
            check=True,
        )
    return image_tag


def sync_path_ownership_via_docker(docker_bin: str, path: Path) -> None:
    if not path.exists():
        return
    uid = os.getuid()
    gid = os.getgid()
    subprocess.run(
        [
            docker_bin,
            "run",
            "--rm",
            "-v",
            f"{path}:/target",
            "alpine:3.20",
            "sh",
            "-lc",
            f"chown -R {uid}:{gid} /target && chmod -R u+rwX /target",
        ],
        check=True,
    )


def best_effort_sync_outputs(args, docker_bin: str) -> None:
    if args.backend not in {"milvus", "qdrant"}:
        return
    try:
        sync_path_ownership_via_docker(docker_bin, Path(args.db_path).resolve())
    except Exception as exc:
        print(f"[warn] Unable to sync output ownership automatically: {exc}")


def run_in_docker(args) -> bool:
    if os.environ.get("GITHUB_ACTIONS", "").lower() == "true":
        return False

    if os.name == "nt":
        return False

    if is_running_in_docker():
        return False

    docker = shutil.which("docker")
    if not docker:
        return False

    repo_root = Path(__file__).resolve().parents[3]
    host_uid = os.getuid() if hasattr(os, "getuid") else None
    host_gid = os.getgid() if hasattr(os, "getgid") else None
    user_spec = (
        f"{host_uid}:{host_gid}"
        if host_uid is not None and host_gid is not None
        else None
    )
    workspace_mount_src = str(repo_root)
    workspace_mount_dst = "/workspace"
    work_dir = "/workspace/bindings/python/examples"
    extra_run_args: List[str] = []

    if args.backend in {"milvus", "qdrant"}:
        workspace_mount_dst = str(repo_root)
        work_dir = str(repo_root / "bindings/python/examples")
        extra_run_args.extend(
            [
                "-v",
                "/var/run/docker.sock:/var/run/docker.sock",
                "--pid=host",
                "--add-host",
                "host.docker.internal:host-gateway",
            ]
        )

    wrapper_mem_limit, wrapper_cpus = backend_client_budget(args)

    filtered_args: List[str] = []
    skip_next = False
    hidden_args = {"--docker-image"}
    custom_docker_image = False
    for arg in sys.argv[1:]:
        if skip_next:
            skip_next = False
            continue
        if arg in hidden_args:
            if arg == "--docker-image":
                custom_docker_image = True
            skip_next = True
            continue
        if arg.startswith("--docker-image="):
            custom_docker_image = True
            continue
        filtered_args.append(arg)

    if args.backend == "milvus":
        has_milvus_host = any(
            arg == "--milvus-host" or arg.startswith("--milvus-host=")
            for arg in filtered_args
        )
        if not has_milvus_host:
            filtered_args.extend(["--milvus-host", "host.docker.internal"])
    elif args.backend == "qdrant":
        has_qdrant_host = any(
            arg == "--qdrant-host" or arg.startswith("--qdrant-host=")
            for arg in filtered_args
        )
        if not has_qdrant_host:
            filtered_args.extend(["--qdrant-host", "host.docker.internal"])

    arcadedb_wheel_mount_path = None
    if args.backend == "arcadedb_sql":
        wheel_candidates = sorted(
            (repo_root / "bindings/python/dist").glob("*embed*.whl")
        )
        if not wheel_candidates:
            if os.environ.get("GITHUB_ACTIONS", "").lower() == "true":
                raise RuntimeError(
                    "ArcadeDB wheel not found in bindings/python/dist during GitHub Actions run"
                )
            print(
                "[info] No local ArcadeDB wheel found in bindings/python/dist; "
                "skipping Docker wrapper and running natively."
            )
            return False
        arcadedb_wheel_mount_path = (
            f"/workspace/bindings/python/dist/{wheel_candidates[0].name}"
        )

    image = args.docker_image or default_docker_image(args.backend)

    if args.backend == "arcadedb_sql":
        inner_cmd = " && ".join(
            [
                "python -m venv /tmp/bench-venv",
                ". /tmp/bench-venv/bin/activate",
                *uv_bootstrap_commands("python"),
                "uv pip install numpy psutil",
                f'uv pip install "{arcadedb_wheel_mount_path}"',
                "echo 'Starting vector search benchmark...'",
                f"python -u 12_vector_search.py {' '.join(filtered_args)}",
            ]
        )
        run_user_args = ["-u", user_spec] if user_spec is not None else []
    elif args.backend == "faiss":
        inner_cmd = " && ".join(
            [
                "python -m venv /tmp/bench-venv",
                ". /tmp/bench-venv/bin/activate",
                *uv_bootstrap_commands("python"),
                "uv pip install faiss-cpu numpy psutil",
                "echo 'Starting vector search benchmark...'",
                f"python -u 12_vector_search.py {' '.join(filtered_args)}",
            ]
        )
        run_user_args = ["-u", user_spec] if user_spec is not None else []
    elif args.backend == "lancedb":
        inner_cmd = " && ".join(
            [
                "python -m venv /tmp/bench-venv",
                ". /tmp/bench-venv/bin/activate",
                *uv_bootstrap_commands("python"),
                "uv pip install lancedb numpy psutil",
                "echo 'Starting vector search benchmark...'",
                f"python -u 12_vector_search.py {' '.join(filtered_args)}",
            ]
        )
        run_user_args = ["-u", user_spec] if user_spec is not None else []
    elif args.backend == "bruteforce":
        inner_cmd = " && ".join(
            [
                "python -m venv /tmp/bench-venv",
                ". /tmp/bench-venv/bin/activate",
                *uv_bootstrap_commands("python"),
                "uv pip install numpy psutil",
                "echo 'Starting vector search benchmark...'",
                f"python -u 12_vector_search.py {' '.join(filtered_args)}",
            ]
        )
        run_user_args = ["-u", user_spec] if user_spec is not None else []
    elif args.backend == "qdrant":
        inner_cmd = " && ".join(
            [
                "apt-get update",
                "apt-get install -y --no-install-recommends docker.io docker-cli",
                "rm -rf /var/lib/apt/lists/*",
                "python -m venv /tmp/bench-venv",
                ". /tmp/bench-venv/bin/activate",
                *uv_bootstrap_commands("python"),
                "uv pip install numpy qdrant-client psutil",
                "echo 'Starting vector search benchmark...'",
                f"python -u 12_vector_search.py {' '.join(filtered_args)}",
            ]
        )
        run_user_args = []
    elif args.backend == "milvus":
        if not custom_docker_image and image == "python:3.12-slim":
            image = ensure_milvus_runner_image(docker)
            inner_cmd = " && ".join(
                [
                    "echo 'Starting vector search benchmark...'",
                    f"python -u 12_vector_search.py {' '.join(filtered_args)}",
                ]
            )
        else:
            inner_cmd = " && ".join(
                [
                    "apt-get update",
                    "apt-get install -y --no-install-recommends python3 python3-venv docker.io docker-cli docker-compose",
                    "rm -rf /var/lib/apt/lists/*",
                    "python -m venv /tmp/bench-venv",
                    ". /tmp/bench-venv/bin/activate",
                    *uv_bootstrap_commands("python"),
                    "uv pip install numpy pymilvus psutil",
                    "echo 'Starting vector search benchmark...'",
                    f"python -u 12_vector_search.py {' '.join(filtered_args)}",
                ]
            )
        run_user_args = []
    else:
        if host_uid is None or host_gid is None:
            return False
        user_inner_cmd = " && ".join(
            [
                "python3 -m venv /tmp/bench-venv",
                ". /tmp/bench-venv/bin/activate",
                *uv_bootstrap_commands("python"),
                "uv pip install numpy psycopg[binary] psutil",
                "echo 'Starting vector search benchmark...'",
                f"python -u 12_vector_search.py {' '.join(filtered_args)}",
            ]
        )
        inner_cmd = " && ".join(
            [
                "apt-get update",
                "apt-get install -y --no-install-recommends python3 python3-venv gosu",
                "rm -rf /var/lib/apt/lists/*",
                f"getent group {os.getgid()} >/dev/null || echo 'benchgrp:x:{os.getgid()}:' >> /etc/group",
                f"getent passwd {os.getuid()} >/dev/null || echo 'benchusr:x:{os.getuid()}:{os.getgid()}::/tmp:/bin/sh' >> /etc/passwd",
                f"gosu {user_spec} sh -lc {shlex.quote(user_inner_cmd)}",
            ]
        )
        run_user_args = []

    cmd = [
        docker,
        "run",
        "--rm",
        *extra_run_args,
        *run_user_args,
        "--memory",
        wrapper_mem_limit,
        "--cpus",
        format_cpu_quota(wrapper_cpus),
        "-e",
        "XDG_CACHE_HOME=/tmp",
        "-v",
        f"{workspace_mount_src}:{workspace_mount_dst}",
        "-w",
        work_dir,
        image,
        "sh",
        "-lc",
        inner_cmd,
    ]

    print("Launching Docker container...")
    try:
        subprocess.run(cmd, check=True)
    finally:
        best_effort_sync_outputs(args, docker)
    return True


def collect_runtime_metadata(
    args,
    quantization: str,
    runtime_versions: dict[str, str | None],
    effective_heap_size: str | None,
) -> dict:
    return {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "argv": sys.argv[1:],
        "python_version": sys.version,
        "platform": platform.platform(),
        "machine": platform.machine(),
        "processor": platform.processor(),
        "logical_cpu_count": os.cpu_count(),
        "physical_cpu_count": psutil.cpu_count(logical=False) if psutil else None,
        "threads_limit": args.threads,
        "mem_limit": args.mem_limit,
        "heap_size": effective_heap_size,
        "jvm_args": args.jvm_args,
        "docker_image": args.docker_image or default_docker_image(args.backend),
        "docker_version": get_docker_version(),
        "backend": args.backend,
        "runtime_versions": runtime_versions,
        "is_running_in_docker": is_running_in_docker(),
        "quantization": quantization,
        "hnsw_ef_normalization": HNSW_EF_NORMALIZATION,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Example 12: vector search-only benchmark"
    )
    parser.add_argument(
        "--backend",
        choices=[
            "arcadedb_sql",
            "faiss",
            "lancedb",
            "bruteforce",
            "pgvector",
            "qdrant",
            "milvus",
        ],
        default="arcadedb_sql",
    )
    parser.add_argument("--dataset", required=True)
    parser.add_argument(
        "--db-path",
        required=True,
        help="Path to existing benchmark output directory from Example 11",
    )
    parser.add_argument(
        "--overquery-factors",
        default="1,2,3,4,6,8",
        help=(
            "Sweep values. For arcadedb: overquery factors. "
            "For faiss/pgvector/qdrant/milvus and LanceDB HNSW-like tuning: "
            "ef_search = 0.5 * k * factor. "
            "Default: 1,2,3,4,6,8"
        ),
    )
    parser.add_argument("--k", type=int, default=50)
    parser.add_argument("--query-limit", type=int, default=1000)
    parser.add_argument("--query-runs", type=int, default=1)
    parser.add_argument(
        "--query-order",
        choices=["fixed", "shuffled"],
        default="shuffled",
        help="Query order across runs (default: shuffled)",
    )
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--run-label",
        type=str,
        default=None,
        help="Optional label appended to output filename",
    )
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument(
        "--mem-limit",
        type=str,
        default="4g",
        help="Use at least 25%% of vector index build memory",
    )
    parser.add_argument(
        "--server-fraction",
        type=float,
        default=0.8,
        help=(
            "Server share of total mem/cpu budget for client-server backends "
            "(0-1, default: 0.8)"
        ),
    )
    parser.add_argument(
        "--jvm-heap-fraction",
        type=float,
        default=0.80,
        help="JVM heap fraction of --mem-limit (default: 0.80)",
    )
    parser.add_argument("--jvm-args", default=None)
    parser.add_argument("--docker-image", type=str, default=None)

    parser.add_argument("--pg-host", default="127.0.0.1")
    parser.add_argument("--pg-port", type=int, default=6543)
    parser.add_argument("--pg-user", default="postgres")
    parser.add_argument("--pg-password", default="")
    parser.add_argument("--pg-database", default="bench")
    parser.add_argument(
        "--pg-shared-buffers",
        default=None,
        help="Postgres shared_buffers (default: 25%% of --mem-limit)",
    )
    parser.add_argument("--milvus-host", default="127.0.0.1")
    parser.add_argument("--milvus-port", type=int, default=19530)
    parser.add_argument(
        "--milvus-compose-version",
        default="latest",
        help="Milvus release tag for standalone docker-compose file",
    )
    parser.add_argument("--milvus-collection", default="vectordata")
    parser.add_argument("--qdrant-host", default="127.0.0.1")
    parser.add_argument("--qdrant-port", type=int, default=6333)
    parser.add_argument("--qdrant-image", default="qdrant/qdrant:latest")

    args = parser.parse_args()
    if args.run_label:
        args.run_label = args.run_label.strip().replace("/", "-").replace(" ", "_")
    if args.jvm_heap_fraction <= 0 or args.jvm_heap_fraction > 1:
        parser.error("--jvm-heap-fraction must be > 0 and <= 1")
    args.qdrant_image = resolve_qdrant_image(args.qdrant_image)
    args.milvus_compose_version = resolve_milvus_compose_version(
        args.milvus_compose_version
    )
    if args.backend == "pgvector" and not args.docker_image:
        args.docker_image = resolve_latest_pgvector_image()
    configure_reproducibility(args.seed)

    if run_in_docker(args):
        return

    pg_shared_buffers = resolve_pg_shared_buffers(
        args.mem_limit,
        args.pg_shared_buffers,
    )
    arcadedb_heap_size = resolve_arcadedb_heap_size(
        args.mem_limit,
        args.jvm_heap_fraction,
    )

    run_start_perf = time.perf_counter()
    run_start_utc = datetime.now(timezone.utc).isoformat()

    overqueries = parse_overqueries(args.overquery_factors)
    db_path = Path(args.db_path).resolve()
    if not db_path.exists():
        raise SystemExit(f"DB path not found: {db_path}")

    sources, gt_path, dim, label, total_rows, dataset_dir = resolve_dataset(
        args.dataset
    )

    qids = load_queries(gt_path, limit=args.query_limit)
    qids = [qid for qid in qids if qid < total_rows][: args.query_limit]
    gt_full = load_ground_truth(gt_path)
    qids = [qid for qid in qids if qid in gt_full][: args.query_limit]
    if not qids:
        raise SystemExit("No valid query IDs with ground truth found")

    queries, _dur, _r0, _r1 = timed_section(
        "load_queries",
        lambda: materialize_queries(sources, qids, dim=dim),
    )

    build_config = load_existing_build_config(db_path)
    quantization = str(build_config.get("quantization", "NONE")).upper()

    sweeps: List[dict] = []

    def record_phase(
        name: str, result, dur: float, rss_before: float, rss_after: float
    ):
        phase = {
            "name": name,
            "time_sec": dur,
            "rss_before_mb": rss_before,
            "rss_after_mb": rss_after,
            "rss_delta_mb": rss_after - rss_before,
        }
        if isinstance(result, dict):
            phase.update(result)
        return phase

    runtime_versions: dict[str, str | None] = {}

    if args.backend == "arcadedb_sql":
        import arcadedb_embedded as arcadedb

        stop_cpu = start_cpu_logger(2)
        jvm_kwargs: Dict[str, str] = {}
        jvm_kwargs["heap_size"] = arcadedb_heap_size
        if args.jvm_args is not None:
            jvm_kwargs["jvm_args"] = args.jvm_args

        try:
            for overquery in overqueries:
                print("\n" + "-" * 80)
                print(f"Overquery factor: {overquery}")
                print("-" * 80)

                phases: List[dict] = []

                db, dur, r0, r1 = timed_section(
                    "open_db",
                    lambda: arcadedb.open_database(str(db_path), jvm_kwargs=jvm_kwargs),
                )
                phases.append(record_phase("open_db", {}, dur, r0, r1))

                stats, dur, r0, r1 = timed_section(
                    "search",
                    lambda: run_repeated_search(
                        lambda run_queries, run_qids: search_arcadedb(
                            {"db": db, "name": "VectorData[vector]"},
                            run_queries,
                            run_qids,
                            gt_full,
                            k=args.k,
                            overquery_factor=overquery,
                            quantization=quantization,
                        ),
                        queries=queries,
                        qids=qids,
                        query_runs=args.query_runs,
                        query_order=args.query_order,
                        seed=args.seed,
                    ),
                )
                phases.append(record_phase("search", stats, dur, r0, r1))

                _, dur, r0, r1 = timed_section("close_db", lambda: db.close())
                phases.append(record_phase("close_db", {}, dur, r0, r1))

                sweeps.append(
                    {
                        "overquery_factor": overquery,
                        "effective_overquery_factor": max(
                            1,
                            int(round(overquery)),
                        ),
                        "phases": phases,
                        "recall_mean": stats.get("recall_mean"),
                        "recall_count": stats.get("recall_count"),
                        "latency_ms_mean": stats.get("latency_ms_mean"),
                        "latency_ms_p95": stats.get("latency_ms_p95"),
                        "queries": stats.get("queries"),
                    }
                )
        finally:
            if stop_cpu is not None:
                stop_cpu.set()

        runtime_versions["arcadedb"] = getattr(arcadedb, "__version__", None)
        runtime_versions["faiss"] = None
        runtime_versions["lancedb"] = None
        runtime_versions["postgres"] = None
        runtime_versions["qdrant"] = None

    elif args.backend == "faiss":
        import faiss

        index_path = db_path / "faiss.index"
        if not index_path.exists():
            raise SystemExit(f"FAISS index file not found: {index_path}")

        stop_cpu = start_cpu_logger(2)
        try:
            for overquery in overqueries:
                print("\n" + "-" * 80)
                print(f"Overquery factor: {overquery}")
                print(
                    f"Effective ef_search: {overquery_to_ef_search(args.k, overquery)}"
                )
                print("-" * 80)

                phases: List[dict] = []

                index, dur, r0, r1 = timed_section(
                    "open_db",
                    lambda: faiss.read_index(str(index_path)),
                )
                phases.append(record_phase("open_db", {}, dur, r0, r1))

                stats, dur, r0, r1 = timed_section(
                    "search",
                    lambda: run_repeated_search(
                        lambda run_queries, run_qids: search_faiss(
                            index,
                            run_queries,
                            run_qids,
                            gt_full,
                            k=args.k,
                            overquery_factor=overquery,
                        ),
                        queries=queries,
                        qids=qids,
                        query_runs=args.query_runs,
                        query_order=args.query_order,
                        seed=args.seed,
                    ),
                )
                phases.append(record_phase("search", stats, dur, r0, r1))

                _, dur, r0, r1 = timed_section("close_db", lambda: None)
                phases.append(record_phase("close_db", {}, dur, r0, r1))

                sweeps.append(
                    {
                        "overquery_factor": overquery,
                        "effective_ef_search": overquery_to_ef_search(
                            args.k,
                            overquery,
                        ),
                        "phases": phases,
                        "recall_mean": stats.get("recall_mean"),
                        "recall_count": stats.get("recall_count"),
                        "latency_ms_mean": stats.get("latency_ms_mean"),
                        "latency_ms_p95": stats.get("latency_ms_p95"),
                        "queries": stats.get("queries"),
                    }
                )
        finally:
            if stop_cpu is not None:
                stop_cpu.set()

        runtime_versions["faiss"] = getattr(faiss, "__version__", None)
        runtime_versions["arcadedb"] = None
        runtime_versions["lancedb"] = None
        runtime_versions["postgres"] = None
        runtime_versions["qdrant"] = None
        runtime_versions["milvus"] = None

    elif args.backend == "lancedb":
        import lancedb

        lancedb_dir = db_path / "lancedb-data"
        if not lancedb_dir.exists():
            raise SystemExit(f"LanceDB data dir not found: {lancedb_dir}")

        table_name = "vectordata"
        stop_cpu = start_cpu_logger(2)
        try:
            for overquery in overqueries:
                print("\n" + "-" * 80)
                print(f"Overquery factor: {overquery}")
                print("-" * 80)

                phases: List[dict] = []

                (db, table), dur, r0, r1 = timed_section(
                    "open_db",
                    lambda: open_lancedb_table(lancedb_dir, table_name),
                )
                phases.append(record_phase("open_db", {}, dur, r0, r1))

                stats, dur, r0, r1 = timed_section(
                    "search",
                    lambda: run_repeated_search(
                        lambda run_queries, run_qids: search_lancedb(
                            table,
                            run_queries,
                            run_qids,
                            gt_full,
                            k=args.k,
                            overquery_factor=overquery,
                            build_config=build_config,
                        ),
                        queries=queries,
                        qids=qids,
                        query_runs=args.query_runs,
                        query_order=args.query_order,
                        seed=args.seed,
                    ),
                )
                phases.append(record_phase("search", stats, dur, r0, r1))

                close_db_fn = getattr(db, "close", None)
                _, dur, r0, r1 = timed_section(
                    "close_db",
                    lambda: close_db_fn() if callable(close_db_fn) else None,
                )
                phases.append(record_phase("close_db", {}, dur, r0, r1))

                sweeps.append(
                    {
                        "overquery_factor": overquery,
                        "effective_ef_search": stats.get("effective_ef_search"),
                        "effective_nprobes": stats.get("effective_nprobes"),
                        "phases": phases,
                        "recall_mean": stats.get("recall_mean"),
                        "recall_count": stats.get("recall_count"),
                        "latency_ms_mean": stats.get("latency_ms_mean"),
                        "latency_ms_p95": stats.get("latency_ms_p95"),
                        "queries": stats.get("queries"),
                    }
                )
        finally:
            if stop_cpu is not None:
                stop_cpu.set()

        runtime_versions["lancedb"] = getattr(lancedb, "__version__", None)
        runtime_versions["arcadedb"] = None
        runtime_versions["faiss"] = None
        runtime_versions["postgres"] = None
        runtime_versions["qdrant"] = None
        runtime_versions["milvus"] = None

    elif args.backend == "bruteforce":
        stop_cpu = start_cpu_logger(2)
        try:
            for overquery in overqueries:
                print("\n" + "-" * 80)
                print(f"Overquery factor: {overquery}")
                print("-" * 80)

                phases: List[dict] = []

                corpus_vectors_normalized, dur, r0, r1 = timed_section(
                    "open_db",
                    lambda: normalize_rows(materialize_corpus_vectors(sources, dim)),
                )
                phases.append(record_phase("open_db", {}, dur, r0, r1))

                stats, dur, r0, r1 = timed_section(
                    "search",
                    lambda: run_repeated_search(
                        lambda run_queries, run_qids: search_bruteforce(
                            corpus_vectors_normalized,
                            run_queries,
                            run_qids,
                            gt_full,
                            k=args.k,
                        ),
                        queries=queries,
                        qids=qids,
                        query_runs=args.query_runs,
                        query_order=args.query_order,
                        seed=args.seed,
                    ),
                )
                phases.append(record_phase("search", stats, dur, r0, r1))

                _, dur, r0, r1 = timed_section("close_db", lambda: None)
                phases.append(record_phase("close_db", {}, dur, r0, r1))

                sweeps.append(
                    {
                        "overquery_factor": overquery,
                        "effective_overquery_factor": 1,
                        "phases": phases,
                        "recall_mean": stats.get("recall_mean"),
                        "recall_count": stats.get("recall_count"),
                        "latency_ms_mean": stats.get("latency_ms_mean"),
                        "latency_ms_p95": stats.get("latency_ms_p95"),
                        "queries": stats.get("queries"),
                    }
                )
        finally:
            if stop_cpu is not None:
                stop_cpu.set()

        runtime_versions["arcadedb"] = None
        runtime_versions["faiss"] = None
        runtime_versions["lancedb"] = None
        runtime_versions["postgres"] = None
        runtime_versions["qdrant"] = None
        runtime_versions["milvus"] = None

    elif args.backend == "qdrant":
        from qdrant_client import QdrantClient

        qdrant_mem_limit, qdrant_cpus = backend_server_budget(args)
        qdrant_data_dir = db_path / "qdrant-data"
        if not qdrant_data_dir.exists():
            raise SystemExit(f"Qdrant data dir not found: {qdrant_data_dir}")

        collection_name = "vectordata"
        start_qdrant_container(
            db_path=db_path,
            data_dir=qdrant_data_dir,
            port=args.qdrant_port,
            mem_limit=qdrant_mem_limit,
            cpus=qdrant_cpus,
            image=args.qdrant_image,
        )
        wait_for_qdrant_ready(args.qdrant_host, args.qdrant_port)

        def qdrant_pid_provider() -> int | None:
            return qdrant_container_pid(db_path)

        rss_provider = combined_rss_provider(qdrant_pid_provider)
        stop_cpu = start_cpu_logger(2, rss_provider=rss_provider)

        try:
            for overquery in overqueries:
                print("\n" + "-" * 80)
                print(f"Overquery factor: {overquery}")
                print(
                    f"Effective ef_search: {overquery_to_ef_search(args.k, overquery)}"
                )
                print("-" * 80)

                phases: List[dict] = []

                client, dur, r0, r1 = timed_section(
                    "open_db",
                    lambda: QdrantClient(
                        host=args.qdrant_host,
                        port=args.qdrant_port,
                        timeout=120.0,
                        check_compatibility=False,
                    ),
                    rss_provider=rss_provider,
                )
                phases.append(record_phase("open_db", {}, dur, r0, r1))

                try:
                    stats, dur, r0, r1 = timed_section(
                        "search",
                        lambda: run_repeated_search(
                            lambda run_queries, run_qids: search_qdrant(
                                client,
                                collection_name=collection_name,
                                queries=run_queries,
                                qids=run_qids,
                                gt_full=gt_full,
                                k=args.k,
                                overquery_factor=overquery,
                            ),
                            queries=queries,
                            qids=qids,
                            query_runs=args.query_runs,
                            query_order=args.query_order,
                            seed=args.seed,
                        ),
                        rss_provider=rss_provider,
                    )
                    phases.append(record_phase("search", stats, dur, r0, r1))
                finally:
                    _, dur, r0, r1 = timed_section(
                        "close_db",
                        lambda: client.close(),
                        rss_provider=rss_provider,
                    )
                    phases.append(record_phase("close_db", {}, dur, r0, r1))

                sweeps.append(
                    {
                        "overquery_factor": overquery,
                        "effective_ef_search": overquery_to_ef_search(
                            args.k,
                            overquery,
                        ),
                        "phases": phases,
                        "recall_mean": stats.get("recall_mean"),
                        "recall_count": stats.get("recall_count"),
                        "latency_ms_mean": stats.get("latency_ms_mean"),
                        "latency_ms_p95": stats.get("latency_ms_p95"),
                        "queries": stats.get("queries"),
                    }
                )

            version_client = QdrantClient(
                host=args.qdrant_host,
                port=args.qdrant_port,
                timeout=120.0,
                check_compatibility=False,
            )
            try:
                runtime_versions["qdrant"] = get_qdrant_version(version_client)
            finally:
                version_client.close()
            runtime_versions["arcadedb"] = None
            runtime_versions["faiss"] = None
            runtime_versions["lancedb"] = None
            runtime_versions["postgres"] = None
        finally:
            if stop_cpu is not None:
                stop_cpu.set()
            stop_qdrant_container(db_path)

    elif args.backend == "milvus":
        from pymilvus import Collection, connections

        milvus_mem_limit, milvus_cpus = backend_server_budget(args)
        compose_file = db_path / "milvus-compose" / "docker-compose.yml"
        ensure_milvus_compose_file(compose_file, args.milvus_compose_version)
        project_name = milvus_project_name(db_path)

        start_milvus_compose(
            compose_file,
            project_name,
            mem_limit=milvus_mem_limit,
            cpus=milvus_cpus,
        )
        wait_for_milvus_ready(args.milvus_host, args.milvus_port)

        def milvus_pids_provider() -> List[int]:
            return milvus_container_pids(compose_file, project_name)

        rss_provider = combined_rss_provider_multi(milvus_pids_provider)
        stop_cpu = start_cpu_logger(2, rss_provider=rss_provider)
        alias = "milvus-bench-search"

        try:
            for overquery in overqueries:
                print("\n" + "-" * 80)
                print(f"Overquery factor: {overquery}")
                print(
                    f"Effective ef_search: {overquery_to_ef_search(args.k, overquery)}"
                )
                print("-" * 80)

                phases: List[dict] = []

                _, dur, r0, r1 = timed_section(
                    "open_db",
                    lambda: connections.connect(
                        alias=alias,
                        host=args.milvus_host,
                        port=str(args.milvus_port),
                    ),
                    rss_provider=rss_provider,
                )
                phases.append(record_phase("open_db", {}, dur, r0, r1))

                try:
                    collection = Collection(args.milvus_collection, using=alias)
                    wait_for_milvus_collection_load(collection)

                    stats, dur, r0, r1 = timed_section(
                        "search",
                        lambda: run_repeated_search(
                            lambda run_queries, run_qids: search_milvus(
                                collection,
                                run_queries,
                                run_qids,
                                gt_full,
                                k=args.k,
                                overquery_factor=overquery,
                            ),
                            queries=queries,
                            qids=qids,
                            query_runs=args.query_runs,
                            query_order=args.query_order,
                            seed=args.seed,
                        ),
                        rss_provider=rss_provider,
                    )
                    phases.append(record_phase("search", stats, dur, r0, r1))
                finally:
                    _, dur, r0, r1 = timed_section(
                        "close_db",
                        lambda: connections.disconnect(alias=alias),
                        rss_provider=rss_provider,
                    )
                    phases.append(record_phase("close_db", {}, dur, r0, r1))

                sweeps.append(
                    {
                        "overquery_factor": overquery,
                        "effective_ef_search": overquery_to_ef_search(
                            args.k,
                            overquery,
                        ),
                        "phases": phases,
                        "recall_mean": stats.get("recall_mean"),
                        "recall_count": stats.get("recall_count"),
                        "latency_ms_mean": stats.get("latency_ms_mean"),
                        "latency_ms_p95": stats.get("latency_ms_p95"),
                        "queries": stats.get("queries"),
                    }
                )

            runtime_versions["milvus"] = get_milvus_version(
                args.milvus_host,
                args.milvus_port,
            )
            runtime_versions["arcadedb"] = None
            runtime_versions["faiss"] = None
            runtime_versions["lancedb"] = None
            runtime_versions["postgres"] = None
            runtime_versions["qdrant"] = None
        finally:
            if stop_cpu is not None:
                stop_cpu.set()
            stop_milvus_compose(compose_file, project_name)

    else:
        import psycopg

        pg_data_dir = db_path / "postgres-data"
        if not pg_data_dir.exists():
            raise SystemExit(f"Postgres data dir not found: {pg_data_dir}")

        start_postgres(pg_data_dir, args.pg_port, pg_shared_buffers)

        def server_pid_provider() -> int | None:
            return get_postmaster_pid(pg_data_dir)

        rss_provider = combined_rss_provider(server_pid_provider)
        stop_cpu = start_cpu_logger(2, rss_provider=rss_provider)

        try:
            for overquery in overqueries:
                print("\n" + "-" * 80)
                print(f"Overquery factor: {overquery}")
                print(
                    f"Effective ef_search: {overquery_to_ef_search(args.k, overquery)}"
                )
                print("-" * 80)

                phases: List[dict] = []

                conn, dur, r0, r1 = timed_section(
                    "open_db",
                    lambda: psycopg.connect(
                        host=args.pg_host,
                        port=args.pg_port,
                        user=args.pg_user,
                        password=args.pg_password,
                        dbname=args.pg_database,
                        autocommit=False,
                    ),
                    rss_provider=rss_provider,
                )
                phases.append(record_phase("open_db", {}, dur, r0, r1))

                try:
                    stats, dur, r0, r1 = timed_section(
                        "search",
                        lambda: run_repeated_search(
                            lambda run_queries, run_qids: search_pgvector(
                                conn,
                                run_queries,
                                run_qids,
                                gt_full,
                                k=args.k,
                                overquery_factor=overquery,
                            ),
                            queries=queries,
                            qids=qids,
                            query_runs=args.query_runs,
                            query_order=args.query_order,
                            seed=args.seed,
                        ),
                        rss_provider=rss_provider,
                    )
                    phases.append(record_phase("search", stats, dur, r0, r1))
                finally:
                    _, dur, r0, r1 = timed_section(
                        "close_db",
                        lambda: conn.close(),
                        rss_provider=rss_provider,
                    )
                    phases.append(record_phase("close_db", {}, dur, r0, r1))

                sweeps.append(
                    {
                        "overquery_factor": overquery,
                        "effective_ef_search": overquery_to_ef_search(
                            args.k,
                            overquery,
                        ),
                        "phases": phases,
                        "recall_mean": stats.get("recall_mean"),
                        "recall_count": stats.get("recall_count"),
                        "latency_ms_mean": stats.get("latency_ms_mean"),
                        "latency_ms_p95": stats.get("latency_ms_p95"),
                        "queries": stats.get("queries"),
                    }
                )

            conn_v = psycopg.connect(
                host=args.pg_host,
                port=args.pg_port,
                user=args.pg_user,
                password=args.pg_password,
                dbname=args.pg_database,
                autocommit=True,
            )
            with conn_v.cursor() as cur:
                cur.execute("SHOW server_version")
                row = cur.fetchone()
                runtime_versions["postgres"] = str(row[0]) if row else None
            conn_v.close()
            runtime_versions["arcadedb"] = None
            runtime_versions["faiss"] = None
            runtime_versions["lancedb"] = None
            runtime_versions["qdrant"] = None
            runtime_versions["milvus"] = None
        finally:
            if stop_cpu is not None:
                stop_cpu.set()
            stop_postgres(pg_data_dir)

    if "qdrant" not in runtime_versions:
        runtime_versions["qdrant"] = None
    if "milvus" not in runtime_versions:
        runtime_versions["milvus"] = None
    if "faiss" not in runtime_versions:
        runtime_versions["faiss"] = None
    if "lancedb" not in runtime_versions:
        runtime_versions["lancedb"] = None

    rss_after_vals = [
        phase["rss_after_mb"]
        for sweep in sweeps
        for phase in sweep["phases"]
        if phase.get("rss_after_mb") is not None
    ]
    peak_rss_mb = max(rss_after_vals) if rss_after_vals else None

    results = {
        "run": {
            "start_utc": run_start_utc,
            "end_utc": datetime.now(timezone.utc).isoformat(),
            "total_time_s": time.perf_counter() - run_start_perf,
            "run_label": args.run_label,
        },
        "environment": collect_runtime_metadata(
            args,
            quantization,
            runtime_versions,
            effective_heap_size=(
                arcadedb_heap_size if args.backend == "arcadedb_sql" else None
            ),
        ),
        "budget": budget_allocation_report(args),
        "dataset": {
            "name": args.dataset,
            "label": label,
            "dir": str(dataset_dir),
            "gt_path": str(gt_path),
            "dim": dim,
            "rows": total_rows,
            "query_limit": args.query_limit,
            "query_count": len(qids),
        },
        "db": {
            "path": str(db_path),
            "backend": args.backend,
            "quantization": quantization,
            "pg_shared_buffers_effective": (
                pg_shared_buffers if args.backend == "pgvector" else None
            ),
            "arcadedb_heap_size_effective": (
                arcadedb_heap_size if args.backend == "arcadedb_sql" else None
            ),
            "build_config": build_config,
        },
        "search": {
            "k": args.k,
            "query_runs": args.query_runs,
            "query_order": args.query_order,
            "seed": args.seed,
            "run_label": args.run_label,
            "overquery_factors": overqueries,
            "hnsw_ef_search_mapping": [
                {
                    "factor": factor,
                    "effective_ef_search": overquery_to_ef_search(args.k, factor),
                }
                for factor in overqueries
            ],
            "lancedb_search_mapping": (
                [
                    {
                        "factor": factor,
                        "effective_ef_search": overquery_to_ef_search(args.k, factor),
                        "effective_nprobes": (
                            1
                            if str(
                                ((build_config.get("lancedb") or {}).get("index_type"))
                                or ""
                            )
                            .upper()
                            .startswith("IVF_")
                            else None
                        ),
                    }
                    for factor in overqueries
                ]
                if args.backend == "lancedb"
                else None
            ),
            "sweeps": sweeps,
        },
        "peak_rss_mb": peak_rss_mb,
        "peak_rss_note": "client + server for client-server backends",
        "telemetry": {
            "run_status": "success",
            "error_type": None,
            "error_message": None,
            "db_create_time_s": None,
            "db_open_time_s": (
                float(
                    np.mean(
                        [
                            phase["time_sec"]
                            for sweep in sweeps
                            for phase in sweep.get("phases", [])
                            if phase.get("name") == "open_db"
                        ]
                    )
                )
                if any(
                    phase.get("name") == "open_db"
                    for sweep in sweeps
                    for phase in sweep.get("phases", [])
                )
                else None
            ),
            "db_close_time_s": (
                float(
                    np.mean(
                        [
                            phase["time_sec"]
                            for sweep in sweeps
                            for phase in sweep.get("phases", [])
                            if phase.get("name") == "close_db"
                        ]
                    )
                )
                if any(
                    phase.get("name") == "close_db"
                    for sweep in sweeps
                    for phase in sweep.get("phases", [])
                )
                else None
            ),
            "query_cold_time_s": (
                float(
                    np.mean(
                        [
                            sweep.get("phases", [])[1].get("query_cold_time_s")
                            for sweep in sweeps
                            if len(sweep.get("phases", [])) > 1
                            and sweep.get("phases", [])[1].get("query_cold_time_s")
                            is not None
                        ]
                    )
                )
                if any(
                    len(sweep.get("phases", [])) > 1
                    and sweep.get("phases", [])[1].get("query_cold_time_s") is not None
                    for sweep in sweeps
                )
                else None
            ),
            "query_warm_mean_s": (
                float(
                    np.mean(
                        [
                            sweep.get("phases", [])[1].get("query_warm_mean_s")
                            for sweep in sweeps
                            if len(sweep.get("phases", [])) > 1
                            and sweep.get("phases", [])[1].get("query_warm_mean_s")
                            is not None
                        ]
                    )
                )
                if any(
                    len(sweep.get("phases", [])) > 1
                    and sweep.get("phases", [])[1].get("query_warm_mean_s") is not None
                    for sweep in sweeps
                )
                else None
            ),
            "query_result_hash_stable": all(
                bool(sweep.get("phases", [])[1].get("query_result_hash_stable", True))
                for sweep in sweeps
                if len(sweep.get("phases", [])) > 1
            ),
            "query_row_count_stable": all(
                bool(sweep.get("phases", [])[1].get("query_row_count_stable", True))
                for sweep in sweeps
                if len(sweep.get("phases", [])) > 1
            ),
        },
    }

    if args.run_label:
        results_path = db_path / f"search_results_{args.run_label}.json"
    else:
        results_path = db_path / "search_results.json"
    results_path.write_text(json.dumps(results, indent=2), encoding="utf-8")

    print("\nResults")
    print("-" * 80)
    for sweep in sweeps:
        oq = sweep["overquery_factor"]
        recall = sweep.get("recall_mean")
        lat = sweep.get("latency_ms_mean")
        p95 = sweep.get("latency_ms_p95")
        recall_text = f"{recall:.4f}" if recall is not None else "n/a"
        lat_text = f"{lat:.2f}" if lat is not None else "n/a"
        p95_text = f"{p95:.2f}" if p95 is not None else "n/a"
        if args.backend in {"pgvector", "qdrant", "milvus", "faiss", "lancedb"}:
            ef_text = str(sweep.get("effective_ef_search"))
            extra = ""
            if args.backend == "lancedb" and sweep.get("effective_nprobes") is not None:
                extra = f" | nprobes={sweep.get('effective_nprobes')}"
            print(
                f"factor={oq:>4} | ef_search={ef_text}{extra} | "
                f"recall@{args.k}={recall_text} | latency_mean_ms={lat_text} | "
                f"latency_p95_ms={p95_text}"
            )
        else:
            print(
                f"oq={oq:>4} | recall@{args.k}={recall_text} | "
                f"latency_mean_ms={lat_text} | latency_p95_ms={p95_text}"
            )
    if peak_rss_mb is not None:
        print(
            "Peak RSS: "
            f"{peak_rss_mb:.1f} MB "
            "(client + server for client-server backends)"
        )
    print(f"Results saved to: {results_path}")


if __name__ == "__main__":
    main()
