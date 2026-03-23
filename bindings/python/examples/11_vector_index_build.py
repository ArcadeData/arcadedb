#!/usr/bin/env python3
"""
Example 11: Vector Index Build (build-only)

Backends:
- arcadedb (embedded)
- pgvector (PostgreSQL + pgvector extension)
- qdrant (client-server via Docker)
- milvus (client-server via Docker Compose)
- faiss (in-process index)
- lancedb (local LanceDB)

Datasets:
- MSMARCO: data/MSMARCO-*/
- StackOverflow: data/stackoverflow-*/vectors/ (uses combined 'all')

For client-server backends (pgvector, qdrant, milvus), peak RSS is measured as:
  client process RSS + server process tree RSS.
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
except ImportError:
    psutil = None

META_FILENAME_RE = re.compile(r"msmarco-passages-(.+?)\.meta\.json")
SIZE_TOKEN_RE = re.compile(
    r"^\s*([0-9]*\.?[0-9]+)\s*([kmgt]?)(?:i?b)?\s*$", re.IGNORECASE
)
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
    except (ImportError, OSError, ValueError):
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
    except (subprocess.SubprocessError, OSError):
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
        f"[phase] {name:<14} time={dur:8.3f}s | "
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

    t = threading.Thread(target=_loop, daemon=True)
    t.start()
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


def heap_tag_from_args(heap_size: str | None, jvm_args: str | None) -> str:
    if heap_size:
        return heap_size
    if jvm_args:
        m = re.search(r"-Xmx(\S+)", jvm_args)
        if m:
            return m.group(1)
    return "default"


def size_tag(value: str) -> str:
    normalized = re.sub(r"[^0-9a-z]+", "", value.lower())
    return normalized or "default"


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


def resolve_msmarco_dataset(dataset_dir: Path) -> tuple[List[dict], int, str, int]:
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
    for path in shards:
        rows = (path.stat().st_size // 4) // dim
        sources.append({"path": path, "start": total, "count": rows})
        total += rows

    return sources, dim, label or "dataset", total


def resolve_stackoverflow_dataset(
    dataset_dir: Path, dataset_name: str, corpus: str
) -> tuple[List[dict], int, str, int]:
    vectors_dir = dataset_dir / "vectors"
    if not vectors_dir.is_dir():
        raise SystemExit(f"StackOverflow vectors directory not found: {vectors_dir}")

    meta_path = vectors_dir / f"{dataset_name}-{corpus}.meta.json"
    if not meta_path.exists():
        raise SystemExit(f"StackOverflow vector meta not found: {meta_path}")

    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    dim = int(meta["dim"])
    count = int(meta["count"])

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

    if count != total:
        print(
            "[warn] count in meta does not match computed shard total: "
            f"meta={count}, shards={total}"
        )

    label = str(meta.get("corpus") or corpus)
    return sources, dim, label, total


def resolve_vector_dataset(
    dataset_name: str, corpus: str
) -> tuple[List[dict], int, str, int, Path]:
    dataset_dir = Path(__file__).parent / "data" / dataset_name
    if not dataset_dir.exists():
        raise SystemExit(
            f"Dataset not found: {dataset_dir}. Run download_data.py first."
        )

    if dataset_name.upper().startswith("MSMARCO-"):
        sources, dim, label, total = resolve_msmarco_dataset(dataset_dir)
        return sources, dim, label, total, dataset_dir

    if dataset_name.startswith("stackoverflow-"):
        sources, dim, label, total = resolve_stackoverflow_dataset(
            dataset_dir, dataset_name, corpus
        )
        return sources, dim, label, total, dataset_dir

    raise SystemExit(
        "Unsupported dataset. Use MSMARCO-* or stackoverflow-* dataset names."
    )


def stream_shards(
    sources: List[dict],
    dim: int,
    batch_size: int,
    max_rows: int | None = None,
):
    total_rows = sum(int(s["count"]) for s in sources)
    remaining = max_rows if max_rows is not None else total_rows

    for source in sources:
        if remaining <= 0:
            break

        shard_rows = int(source["count"])
        take_total = min(shard_rows, remaining)
        if take_total <= 0:
            continue

        mm = np.memmap(
            source["path"],
            mode="r",
            dtype=np.float32,
            shape=(shard_rows, dim),
        )
        start = 0
        while start < take_total:
            end = min(start + batch_size, take_total)
            yield int(source["start"]) + start, mm[start:end]
            start = end
        del mm

        remaining -= take_total


def ingest_vectors_arcadedb(
    db,
    sources: List[dict],
    dim: int,
    count: int,
    batch_size: int,
    to_java_float_array,
) -> int:
    for command in (
        "CREATE VERTEX TYPE VectorData",
        "CREATE PROPERTY VectorData.id INTEGER",
        "CREATE PROPERTY VectorData.vector ARRAY_OF_FLOATS",
    ):
        try:
            db.command("sql", command)
        except Exception:
            pass

    ingested = 0
    for base_id, batch in stream_shards(
        sources,
        dim=dim,
        batch_size=batch_size,
        max_rows=count,
    ):
        with db.transaction():
            for idx, vec in enumerate(batch, start=base_id):
                jvec = to_java_float_array(vec) if to_java_float_array else vec.tolist()
                db.command(
                    "sql",
                    "INSERT INTO VectorData SET id = ?, vector = ?",
                    int(idx),
                    jvec,
                )
                ingested += 1

    return ingested


def create_index_arcadedb(
    db,
    dim: int,
    max_connections: int,
    beam_width: int,
    quantization: str,
    store_vectors_in_graph: bool,
    add_hierarchy: bool,
):
    quant = None if quantization.upper() == "NONE" else quantization.upper()
    return db.create_vector_index(
        vertex_type="VectorData",
        vector_property="vector",
        dimensions=dim,
        distance_function="cosine",
        max_connections=max_connections,
        beam_width=beam_width,
        quantization=quant,
        store_vectors_in_graph=store_vectors_in_graph,
        add_hierarchy=add_hierarchy,
        build_graph_now=True,
    )


def create_index_faiss(dim: int, max_connections: int, beam_width: int):
    import faiss

    index_hnsw = faiss.IndexHNSWFlat(
        int(dim),
        int(max_connections),
        faiss.METRIC_INNER_PRODUCT,
    )
    index_hnsw.hnsw.efConstruction = int(beam_width)
    return faiss.IndexIDMap2(index_hnsw)


def ingest_vectors_faiss(
    index,
    sources: List[dict],
    dim: int,
    count: int,
    batch_size: int,
) -> int:
    import faiss

    ingested = 0
    for base_id, batch in stream_shards(
        sources,
        dim=dim,
        batch_size=batch_size,
        max_rows=count,
    ):
        vectors = np.ascontiguousarray(batch.astype("float32", copy=True))
        faiss.normalize_L2(vectors)
        ids = np.arange(base_id, base_id + vectors.shape[0], dtype=np.int64)
        index.add_with_ids(vectors, ids)
        ingested += int(vectors.shape[0])
    return ingested


def persist_faiss_index(index, db_path: Path) -> Path:
    import faiss

    index_path = db_path / "faiss.index"
    faiss.write_index(index, str(index_path))
    return index_path


def ingest_vectors_lancedb(
    db,
    table_name: str,
    sources: List[dict],
    dim: int,
    count: int,
    batch_size: int,
):
    table = None
    ingested = 0
    for base_id, batch in stream_shards(
        sources,
        dim=dim,
        batch_size=batch_size,
        max_rows=count,
    ):
        rows = [
            {"id": int(idx), "vector": vec.tolist()}
            for idx, vec in enumerate(batch, start=base_id)
        ]
        if not rows:
            continue
        if table is None:
            table = db.create_table(table_name, rows, mode="overwrite")
        else:
            table.add(rows)
        ingested += len(rows)
    if table is None:
        table = db.create_table(
            table_name, [{"id": 0, "vector": [0.0] * dim}], mode="overwrite"
        )
        table.delete("id = 0")
    return table, ingested


def create_index_lancedb(
    table,
    max_connections: int,
    beam_width: int,
) -> dict:
    common_kwargs = {
        "metric": "cosine",
        "vector_column_name": "vector",
        "m": int(max_connections),
        "ef_construction": int(beam_width),
    }
    attempts = [
        ("HNSW", {}),
        ("IVF_HNSW_SQ", {"num_partitions": 1}),
    ]
    last_error: Exception | None = None

    for index_type, extra_kwargs in attempts:
        try:
            table.create_index(
                index_type=index_type,
                **common_kwargs,
                **extra_kwargs,
            )
            config = {
                "metric": "cosine",
                "index_type": index_type,
                "hnsw_m": int(max_connections),
                "hnsw_ef_construct": int(beam_width),
            }
            if "num_partitions" in extra_kwargs:
                config["num_partitions"] = int(extra_kwargs["num_partitions"])
                config["quantization"] = "SQ"
            else:
                config["quantization"] = "NONE"

            print(
                "Using LanceDB index mode: "
                f"{index_type}"
                + (
                    f" (num_partitions={extra_kwargs['num_partitions']})"
                    if "num_partitions" in extra_kwargs
                    else ""
                )
            )
            return config
        except (AttributeError, RuntimeError, TypeError, ValueError) as exc:
            last_error = exc

    raise RuntimeError(
        "Unable to create LanceDB index in HNSW-like mode"
    ) from last_error


def vector_to_pg_literal(vec: np.ndarray) -> str:
    return "[" + ",".join(f"{float(x):.7g}" for x in vec.tolist()) + "]"


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


def init_postgres_data_dir(data_dir: Path, pg_user: str) -> None:
    initdb = find_binary("initdb")
    subprocess.run(
        [initdb, "-D", str(data_dir), "-A", "trust", "-U", pg_user],
        check=True,
    )


def start_postgres(data_dir: Path, port: int, shared_buffers: str) -> None:
    pg_ctl = find_binary("pg_ctl")
    opts = f"-F -p {port} -c shared_buffers={shared_buffers}"
    subprocess.run(
        [pg_ctl, "-D", str(data_dir), "-o", opts, "-w", "start"],
        check=True,
    )


def stop_postgres(data_dir: Path) -> None:
    pg_ctl = find_binary("pg_ctl")
    subprocess.run(
        [pg_ctl, "-D", str(data_dir), "-m", "fast", "stop"],
        check=True,
    )


def get_postmaster_pid(data_dir: Path) -> int | None:
    pid_file = data_dir / "postmaster.pid"
    if not pid_file.exists():
        return None
    try:
        line = pid_file.read_text(encoding="utf-8").splitlines()[0].strip()
        return int(line)
    except (OSError, ValueError, IndexError):
        return None


def ensure_pg_schema(conn, dim: int) -> None:
    with conn.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS vector")
        cur.execute("DROP TABLE IF EXISTS vectordata")
        cur.execute(
            f"CREATE TABLE vectordata(id INTEGER PRIMARY KEY, vector vector({dim}))"
        )
    conn.commit()


def ingest_vectors_pgvector(
    conn,
    sources: List[dict],
    dim: int,
    count: int,
    batch_size: int,
) -> int:
    ingested = 0
    for base_id, batch in stream_shards(
        sources,
        dim=dim,
        batch_size=batch_size,
        max_rows=count,
    ):
        rows = [
            (int(idx), vector_to_pg_literal(vec))
            for idx, vec in enumerate(batch, start=base_id)
        ]
        with conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO vectordata(id, vector) VALUES (%s, %s::vector)",
                rows,
            )
        conn.commit()
        ingested += len(rows)
    return ingested


def create_index_pgvector(conn, max_connections: int, beam_width: int) -> None:
    m_val = int(max_connections)
    ef_val = int(beam_width)
    with conn.cursor() as cur:
        cur.execute(
            "CREATE INDEX vectordata_vector_hnsw "
            "ON vectordata USING hnsw (vector vector_cosine_ops) "
            f"WITH (m = {m_val}, ef_construction = {ef_val})"
        )
        cur.execute("ANALYZE vectordata")
    conn.commit()


def create_collection_qdrant(
    client, collection_name: str, dim: int, max_connections: int, beam_width: int
) -> None:
    from qdrant_client import models

    client.recreate_collection(
        collection_name=collection_name,
        vectors_config=models.VectorParams(
            size=int(dim),
            distance=models.Distance.COSINE,
        ),
        hnsw_config=models.HnswConfigDiff(
            m=int(max_connections),
            ef_construct=int(beam_width),
        ),
    )


def ingest_vectors_qdrant(
    client,
    collection_name: str,
    sources: List[dict],
    dim: int,
    count: int,
    batch_size: int,
) -> int:
    from qdrant_client import models

    upsert_chunk_size = 1000
    ingested = 0
    for base_id, batch in stream_shards(
        sources,
        dim=dim,
        batch_size=batch_size,
        max_rows=count,
    ):
        points = [
            models.PointStruct(id=int(idx), vector=vec.tolist())
            for idx, vec in enumerate(batch, start=base_id)
        ]
        for start in range(0, len(points), upsert_chunk_size):
            client.upsert(
                collection_name=collection_name,
                points=points[start : start + upsert_chunk_size],
                wait=True,
            )
        ingested += len(points)
    return ingested


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

    sanitized = re.sub(r"(?m)^\s*container_name:\s*.*\n", "", raw)
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
    while True:
        try:
            connections.connect(alias="_bootstrap", host=host, port=str(port))
            _ = utility.get_server_version(using="_bootstrap")
            connections.disconnect(alias="_bootstrap")
            return
        except Exception:
            if time.perf_counter() - start > timeout_sec:
                raise SystemExit(
                    f"Milvus did not become ready within {timeout_sec}s at {host}:{port}"
                )
            time.sleep(1)


def create_collection_milvus(collection, max_connections: int, beam_width: int) -> None:
    index_params = {
        "index_type": "HNSW",
        "metric_type": "COSINE",
        "params": {
            "M": int(max_connections),
            "efConstruction": int(beam_width),
        },
    }
    collection.create_index(field_name="vector", index_params=index_params)


def ingest_vectors_milvus(
    collection,
    sources: List[dict],
    dim: int,
    count: int,
    batch_size: int,
) -> int:
    ingested = 0
    for base_id, batch in stream_shards(
        sources,
        dim=dim,
        batch_size=batch_size,
        max_rows=count,
    ):
        ids = [int(idx) for idx in range(base_id, base_id + len(batch))]
        vectors = [vec.tolist() for vec in batch]
        collection.insert([ids, vectors])
        ingested += len(ids)
    collection.flush()
    return ingested


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


def get_postgres_version(conn) -> str | None:
    try:
        with conn.cursor() as cur:
            cur.execute("SHOW server_version")
            row = cur.fetchone()
            if row:
                return str(row[0])
    except (OSError, RuntimeError, ValueError):
        return None
    return None


def dir_size_mb(path: Path) -> float:
    if not path.exists():
        return 0.0
    total = 0
    for file in path.rglob("*"):
        if file.is_file():
            total += file.stat().st_size
    return total / (1024 * 1024)


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
        sync_path_ownership_via_docker(docker_bin, Path(args.db_root).resolve())
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
    extra_run_args: list[str] = []

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

    filtered_args: list[str] = []
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

    image = args.docker_image
    if args.backend == "pgvector" and image == "python:3.12-slim":
        image = default_docker_image(args.backend)

    if args.backend == "arcadedb_sql":
        inner_cmd = " && ".join(
            [
                "python -m venv /tmp/bench-venv",
                ". /tmp/bench-venv/bin/activate",
                *uv_bootstrap_commands("python"),
                "uv pip install numpy psutil",
                f'uv pip install "{arcadedb_wheel_mount_path}"',
                "echo 'Starting vector build benchmark...'",
                f"python -u 11_vector_index_build.py {' '.join(filtered_args)}",
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
                "echo 'Starting vector build benchmark...'",
                f"python -u 11_vector_index_build.py {' '.join(filtered_args)}",
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
                "echo 'Starting vector build benchmark...'",
                f"python -u 11_vector_index_build.py {' '.join(filtered_args)}",
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
                "echo 'Starting vector build benchmark...'",
                f"python -u 11_vector_index_build.py {' '.join(filtered_args)}",
            ]
        )
        run_user_args = []
    elif args.backend == "milvus":
        if not custom_docker_image and image == "python:3.12-slim":
            image = ensure_milvus_runner_image(docker)
            inner_cmd = " && ".join(
                [
                    "echo 'Starting vector build benchmark...'",
                    f"python -u 11_vector_index_build.py {' '.join(filtered_args)}",
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
                    "echo 'Starting vector build benchmark...'",
                    f"python -u 11_vector_index_build.py {' '.join(filtered_args)}",
                ]
            )
        run_user_args = []
    else:
        if host_uid is None or host_gid is None:
            return False
        bench_user = "benchusr"
        user_inner_cmd = " && ".join(
            [
                "python3 -m venv /tmp/bench-venv",
                ". /tmp/bench-venv/bin/activate",
                *uv_bootstrap_commands("python"),
                "uv pip install numpy psycopg[binary] psutil",
                "echo 'Starting vector build benchmark...'",
                f"python -u 11_vector_index_build.py {' '.join(filtered_args)}",
            ]
        )
        inner_cmd = " && ".join(
            [
                "apt-get update",
                (
                    "apt-get install -y --no-install-recommends "
                    "python3 python3-venv gosu"
                ),
                "rm -rf /var/lib/apt/lists/*",
                (
                    f"getent group {os.getgid()} >/dev/null || "
                    f"echo 'benchgrp:x:{os.getgid()}:' >> /etc/group"
                ),
                (
                    f"getent passwd {os.getuid()} >/dev/null || "
                    f"echo '{bench_user}:x:{os.getuid()}:{os.getgid()}::/tmp:/bin/sh' "
                    ">> /etc/passwd"
                ),
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
    *,
    heap_size: str | None,
    runtime_versions: dict[str, str | None],
) -> dict:
    return {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "argv": sys.argv[1:],
        "python_version": sys.version,
        "platform": platform.platform(),
        "machine": platform.machine(),
        "processor": platform.processor(),
        "logical_cpu_count": os.cpu_count(),
        "physical_cpu_count": (psutil.cpu_count(logical=False) if psutil else None),
        "threads_limit": args.threads,
        "mem_limit": args.mem_limit,
        "heap_size": heap_size,
        "jvm_args": args.jvm_args,
        "docker_image": args.docker_image or default_docker_image(args.backend),
        "docker_version": get_docker_version(),
        "backend": args.backend,
        "runtime_versions": runtime_versions,
        "is_running_in_docker": is_running_in_docker(),
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Example 11: vector index build-only benchmark"
    )
    parser.add_argument(
        "--backend",
        choices=["arcadedb_sql", "pgvector", "qdrant", "milvus", "faiss", "lancedb"],
        default="arcadedb_sql",
        help="Vector backend (default: arcadedb_sql)",
    )
    parser.add_argument(
        "--dataset",
        required=True,
        help="Dataset name under examples/data (e.g., MSMARCO-1M, stackoverflow-tiny)",
    )
    parser.add_argument(
        "--max-connections",
        type=int,
        default=16,
        help="HNSW max connections / m (default: 16)",
    )
    parser.add_argument(
        "--beam-width",
        type=int,
        default=100,
        help="HNSW beam width / ef_construction (default: 100)",
    )
    parser.add_argument(
        "--quantization",
        choices=["NONE", "INT8", "BINARY", "PRODUCT"],
        default="NONE",
        help="ArcadeDB quantization mode (ignored by pgvector)",
    )
    parser.add_argument(
        "--store-vectors-in-graph",
        action="store_true",
        help="ArcadeDB/JVector-specific option",
    )
    parser.add_argument(
        "--add-hierarchy",
        type=lambda v: str(v).lower() in {"1", "true", "yes", "on"},
        default=True,
        help="ArcadeDB/JVector-specific option (true/false, default: true)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=10_000,
        help="Vectors per ingest batch (default: 10000)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for reproducibility (default: 42)",
    )
    parser.add_argument(
        "--run-label",
        type=str,
        default=None,
        help="Optional label appended to output directory and result filename",
    )
    parser.add_argument(
        "--db-root",
        default="my_test_databases",
        help="Directory to place DBs/results (default: my_test_databases)",
    )
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--mem-limit", type=str, default="4g")
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
    parser.add_argument(
        "--docker-image",
        type=str,
        default="python:3.12-slim",
        help="Docker image to use (default: python:3.12-slim)",
    )

    parser.add_argument("--pg-host", default="127.0.0.1")
    parser.add_argument("--pg-port", type=int, default=6543)
    parser.add_argument("--pg-user", default="postgres")
    parser.add_argument("--pg-password", default="")
    parser.add_argument("--pg-database", default="bench")
    parser.add_argument(
        "--pg-shared-buffers",
        default=None,
        help=(
            "Postgres shared_buffers when backend=pgvector. "
            "Default: 25%% of --mem-limit"
        ),
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
    if args.backend == "pgvector" and args.docker_image == "python:3.12-slim":
        args.docker_image = resolve_latest_pgvector_image()
    configure_reproducibility(args.seed)

    run_start_perf = time.perf_counter()
    run_start_utc = datetime.now(timezone.utc).isoformat()

    if run_in_docker(args):
        return

    arcadedb_heap_size = resolve_arcadedb_heap_size(
        args.mem_limit,
        args.jvm_heap_fraction,
    )

    pg_shared_buffers = resolve_pg_shared_buffers(
        args.mem_limit,
        args.pg_shared_buffers,
    )

    stackoverflow_corpus = "all"
    dataset_info, _dur, _r0, _r1 = timed_section(
        "load_corpus",
        lambda: resolve_vector_dataset(args.dataset, stackoverflow_corpus),
    )
    sources, dim, label, total_rows, dataset_dir = dataset_info

    if total_rows <= 0:
        raise SystemExit("Resolved dataset has zero vectors")

    count = total_rows
    heap_tag = heap_tag_from_args(arcadedb_heap_size, args.jvm_args)

    param_dir = "_".join(
        [
            f"backend={args.backend}",
            f"dataset={args.dataset}",
            f"label={label}",
            f"mem={size_tag(args.mem_limit)}",
            f"heap={heap_tag}",
            f"maxconn={args.max_connections}",
            f"beam={args.beam_width}",
            f"quant={args.quantization.lower()}",
            f"store={'on' if args.store_vectors_in_graph else 'off'}",
            f"hier={'on' if args.add_hierarchy else 'off'}",
            f"batch={args.batch_size}",
            f"count={count}",
            f"run={args.run_label}" if args.run_label else "run=default",
        ]
    )

    db_root = Path(args.db_root).resolve()
    db_root.mkdir(parents=True, exist_ok=True)
    db_path = db_root / param_dir

    if db_path.exists():
        shutil.rmtree(db_path)
    db_path.mkdir(parents=True, exist_ok=True)

    print("=" * 80)
    print("Example 11: Vector Index Build (build-only)")
    print("=" * 80)
    print(f"Backend: {args.backend}")
    print(f"Dataset: {args.dataset}")
    print(f"Resolved dataset dir: {dataset_dir}")
    print(f"Resolved vectors: {total_rows:,}")
    print(f"Ingest count: {count:,}")
    print(f"Dimensions: {dim}")
    print(f"Max connections: {args.max_connections}")
    print(f"Beam width: {args.beam_width}")
    if args.backend == "pgvector":
        print(f"Postgres shared_buffers: {pg_shared_buffers}")
    print(f"DB path: {db_path}")
    print()

    phases: Dict[str, dict] = {}

    def record(name: str, result, dur: float, rss_start: float, rss_end: float):
        phases[name] = {
            "time_sec": dur,
            "rss_before_mb": rss_start,
            "rss_after_mb": rss_end,
            "rss_delta_mb": rss_end - rss_start,
        }
        if isinstance(result, dict):
            phases[name].update(result)

    runtime_versions: dict[str, str | None] = {}
    lancedb_index_config: dict[str, object] | None = None

    if args.backend == "arcadedb_sql":
        stop_cpu = start_cpu_logger(2)
        import arcadedb_embedded as arcadedb

        jvm_kwargs: Dict[str, str] = {}
        heap_size = arcadedb_heap_size
        if heap_size is not None:
            jvm_kwargs["heap_size"] = heap_size
        if args.jvm_args is not None:
            jvm_kwargs["jvm_args"] = args.jvm_args

        db, dur, r0, r1 = timed_section(
            "create_db",
            lambda: arcadedb.create_database(str(db_path), jvm_kwargs=jvm_kwargs),
        )
        record("create_db", {"db_path": str(db_path)}, dur, r0, r1)

        try:
            to_java_float_array = getattr(arcadedb, "to_java_float_array", None)
            ingest_started_at = datetime.now(timezone.utc).isoformat()
            print(f"Ingest start (arcadedb, UTC): {ingest_started_at}")
            ingested, dur, r0, r1 = timed_section(
                "ingest",
                lambda: ingest_vectors_arcadedb(
                    db,
                    sources=sources,
                    dim=dim,
                    count=count,
                    batch_size=args.batch_size,
                    to_java_float_array=to_java_float_array,
                ),
            )
            ingest_ended_at = datetime.now(timezone.utc).isoformat()
            print(
                f"Ingest end   (arcadedb, UTC): {ingest_ended_at} "
                f"(elapsed={dur:.2f}s)"
            )
            record("ingest", {"ingested": int(ingested)}, dur, r0, r1)

            _index, dur, r0, r1 = timed_section(
                "create_index",
                lambda: create_index_arcadedb(
                    db,
                    dim=dim,
                    max_connections=args.max_connections,
                    beam_width=args.beam_width,
                    quantization=args.quantization,
                    store_vectors_in_graph=args.store_vectors_in_graph,
                    add_hierarchy=args.add_hierarchy,
                ),
            )
            record("create_index", {}, dur, r0, r1)
        finally:
            try:
                _, dur, r0, r1 = timed_section("close_db", db.close)
                record("close_db", {}, dur, r0, r1)
            except RuntimeError:
                pass

            if stop_cpu is not None:
                stop_cpu.set()

        runtime_versions["arcadedb"] = getattr(arcadedb, "__version__", None)
        runtime_versions["postgres"] = None
        runtime_versions["faiss"] = None
        runtime_versions["lancedb"] = None

    elif args.backend == "faiss":
        import faiss

        stop_cpu = start_cpu_logger(2)

        index, dur, r0, r1 = timed_section(
            "create_db",
            lambda: {"index": None},
        )
        record("create_db", {"db_path": str(db_path)}, dur, r0, r1)

        try:
            index_obj, dur, r0, r1 = timed_section(
                "create_index",
                lambda: create_index_faiss(
                    dim=dim,
                    max_connections=args.max_connections,
                    beam_width=args.beam_width,
                ),
            )
            record("create_index", {}, dur, r0, r1)

            ingested, dur, r0, r1 = timed_section(
                "ingest",
                lambda: ingest_vectors_faiss(
                    index=index_obj,
                    sources=sources,
                    dim=dim,
                    count=count,
                    batch_size=args.batch_size,
                ),
            )
            record("ingest", {"ingested": int(ingested)}, dur, r0, r1)

            _, dur, r0, r1 = timed_section(
                "close_db",
                lambda: persist_faiss_index(index_obj, db_path),
            )
            record("close_db", {}, dur, r0, r1)
        finally:
            if stop_cpu is not None:
                stop_cpu.set()

        runtime_versions["faiss"] = getattr(faiss, "__version__", None)
        runtime_versions["arcadedb"] = None
        runtime_versions["postgres"] = None
        runtime_versions["qdrant"] = None
        runtime_versions["milvus"] = None
        runtime_versions["lancedb"] = None

    elif args.backend == "lancedb":
        import lancedb

        stop_cpu = start_cpu_logger(2)

        db, dur, r0, r1 = timed_section(
            "create_db",
            lambda: lancedb.connect(str(db_path / "lancedb-data")),
        )
        record("create_db", {"db_path": str(db_path)}, dur, r0, r1)

        table_name = "vectordata"
        try:
            (table, ingested), dur, r0, r1 = timed_section(
                "ingest",
                lambda: ingest_vectors_lancedb(
                    db=db,
                    table_name=table_name,
                    sources=sources,
                    dim=dim,
                    count=count,
                    batch_size=args.batch_size,
                ),
            )
            record("ingest", {"ingested": int(ingested)}, dur, r0, r1)

            lancedb_index_config, dur, r0, r1 = timed_section(
                "create_index",
                lambda: create_index_lancedb(
                    table,
                    max_connections=args.max_connections,
                    beam_width=args.beam_width,
                ),
            )
            record("create_index", lancedb_index_config, dur, r0, r1)

            close_db_fn = getattr(db, "close", None)
            _, dur, r0, r1 = timed_section(
                "close_db",
                lambda: close_db_fn() if callable(close_db_fn) else None,
            )
            record("close_db", {}, dur, r0, r1)
        finally:
            if stop_cpu is not None:
                stop_cpu.set()

        runtime_versions["lancedb"] = getattr(lancedb, "__version__", None)
        runtime_versions["arcadedb"] = None
        runtime_versions["postgres"] = None
        runtime_versions["qdrant"] = None
        runtime_versions["milvus"] = None
        runtime_versions["faiss"] = None

    elif args.backend == "qdrant":
        from qdrant_client import QdrantClient

        qdrant_mem_limit, qdrant_cpus = backend_server_budget(args)
        qdrant_data_dir = db_path / "qdrant-data"
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

        client, dur, r0, r1 = timed_section(
            "create_db",
            lambda: QdrantClient(
                host=args.qdrant_host,
                port=args.qdrant_port,
                timeout=120.0,
                check_compatibility=False,
            ),
            rss_provider=rss_provider,
        )
        record("create_db", {"db_path": str(db_path)}, dur, r0, r1)

        try:
            _, dur, r0, r1 = timed_section(
                "create_index",
                lambda: create_collection_qdrant(
                    client,
                    collection_name=collection_name,
                    dim=dim,
                    max_connections=args.max_connections,
                    beam_width=args.beam_width,
                ),
                rss_provider=rss_provider,
            )
            record("create_index", {}, dur, r0, r1)

            ingested, dur, r0, r1 = timed_section(
                "ingest",
                lambda: ingest_vectors_qdrant(
                    client,
                    collection_name=collection_name,
                    sources=sources,
                    dim=dim,
                    count=count,
                    batch_size=args.batch_size,
                ),
                rss_provider=rss_provider,
            )
            record("ingest", {"ingested": int(ingested)}, dur, r0, r1)

            runtime_versions["qdrant"] = get_qdrant_version(client)
            runtime_versions["arcadedb"] = None
            runtime_versions["postgres"] = None
            runtime_versions["faiss"] = None
            runtime_versions["lancedb"] = None
        finally:
            try:
                _, dur, r0, r1 = timed_section(
                    "close_db",
                    client.close,
                    rss_provider=rss_provider,
                )
                record("close_db", {}, dur, r0, r1)
            except RuntimeError:
                pass

            if stop_cpu is not None:
                stop_cpu.set()

            stop_qdrant_container(db_path)

    elif args.backend == "milvus":
        from pymilvus import (
            Collection,
            CollectionSchema,
            DataType,
            FieldSchema,
            connections,
            utility,
        )

        milvus_mem_limit, milvus_cpus = backend_server_budget(args)
        compose_dir = db_path / "milvus-compose"
        compose_file = compose_dir / "docker-compose.yml"
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

        alias = "milvus-bench-build"
        collection = None
        try:
            _, dur, r0, r1 = timed_section(
                "create_db",
                lambda: connections.connect(
                    alias=alias,
                    host=args.milvus_host,
                    port=str(args.milvus_port),
                ),
                rss_provider=rss_provider,
            )
            record("create_db", {"db_path": str(db_path)}, dur, r0, r1)

            if utility.has_collection(args.milvus_collection, using=alias):
                utility.drop_collection(args.milvus_collection, using=alias)

            schema = CollectionSchema(
                fields=[
                    FieldSchema(
                        name="id",
                        dtype=DataType.INT64,
                        is_primary=True,
                        auto_id=False,
                    ),
                    FieldSchema(
                        name="vector",
                        dtype=DataType.FLOAT_VECTOR,
                        dim=dim,
                    ),
                ],
                description="Vector benchmark collection",
            )
            collection = Collection(
                name=args.milvus_collection,
                schema=schema,
                using=alias,
            )

            _, dur, r0, r1 = timed_section(
                "create_index",
                lambda: create_collection_milvus(
                    collection,
                    max_connections=args.max_connections,
                    beam_width=args.beam_width,
                ),
                rss_provider=rss_provider,
            )
            record("create_index", {}, dur, r0, r1)

            ingested, dur, r0, r1 = timed_section(
                "ingest",
                lambda: ingest_vectors_milvus(
                    collection,
                    sources=sources,
                    dim=dim,
                    count=count,
                    batch_size=args.batch_size,
                ),
                rss_provider=rss_provider,
            )
            record("ingest", {"ingested": int(ingested)}, dur, r0, r1)

            runtime_versions["milvus"] = get_milvus_version(
                args.milvus_host,
                args.milvus_port,
            )
            runtime_versions["arcadedb"] = None
            runtime_versions["postgres"] = None
            runtime_versions["qdrant"] = None
            runtime_versions["faiss"] = None
            runtime_versions["lancedb"] = None
        finally:
            try:
                _, dur, r0, r1 = timed_section(
                    "close_db",
                    lambda: connections.disconnect(alias=alias),
                    rss_provider=rss_provider,
                )
                record("close_db", {}, dur, r0, r1)
            except RuntimeError:
                pass

            if stop_cpu is not None:
                stop_cpu.set()

            stop_milvus_compose(compose_file, project_name)

    else:
        import psycopg

        pg_data_dir = db_path / "postgres-data"
        init_postgres_data_dir(pg_data_dir, args.pg_user)
        start_postgres(pg_data_dir, args.pg_port, pg_shared_buffers)

        def server_pid_provider() -> int | None:
            return get_postmaster_pid(pg_data_dir)

        rss_provider = combined_rss_provider(server_pid_provider)
        stop_cpu = start_cpu_logger(2, rss_provider=rss_provider)

        conn: psycopg.Connection | None = None
        try:
            conn, dur, r0, r1 = timed_section(
                "create_db",
                lambda: psycopg.connect(
                    host=args.pg_host,
                    port=args.pg_port,
                    user=args.pg_user,
                    password=args.pg_password,
                    dbname="postgres",
                    autocommit=True,
                ),
                rss_provider=rss_provider,
            )
            record("create_db", {"db_path": str(db_path)}, dur, r0, r1)

            with conn.cursor() as cur:
                cur.execute(f"DROP DATABASE IF EXISTS {args.pg_database}")
                cur.execute(f"CREATE DATABASE {args.pg_database}")
            conn.close()

            conn = psycopg.connect(
                host=args.pg_host,
                port=args.pg_port,
                user=args.pg_user,
                password=args.pg_password,
                dbname=args.pg_database,
                autocommit=False,
            )

            ensure_pg_schema(conn, dim)

            ingested, dur, r0, r1 = timed_section(
                "ingest",
                lambda: ingest_vectors_pgvector(
                    conn,
                    sources=sources,
                    dim=dim,
                    count=count,
                    batch_size=args.batch_size,
                ),
                rss_provider=rss_provider,
            )
            record("ingest", {"ingested": int(ingested)}, dur, r0, r1)

            _, dur, r0, r1 = timed_section(
                "create_index",
                lambda: create_index_pgvector(
                    conn,
                    max_connections=args.max_connections,
                    beam_width=args.beam_width,
                ),
                rss_provider=rss_provider,
            )
            record("create_index", {}, dur, r0, r1)

            runtime_versions["postgres"] = get_postgres_version(conn)
            runtime_versions["arcadedb"] = None
            runtime_versions["qdrant"] = None
            runtime_versions["milvus"] = None
            runtime_versions["faiss"] = None
            runtime_versions["lancedb"] = None
        finally:
            if conn is not None:
                try:
                    _, dur, r0, r1 = timed_section(
                        "close_db",
                        conn.close,
                        rss_provider=rss_provider,
                    )
                    record("close_db", {}, dur, r0, r1)
                except RuntimeError:
                    pass

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
        phase.get("rss_after_mb")
        for phase in phases.values()
        if phase.get("rss_after_mb") is not None
    ]
    peak_rss_mb = max(rss_after_vals) if rss_after_vals else None

    results = {
        "run": {
            "start_utc": run_start_utc,
            "end_utc": datetime.now(timezone.utc).isoformat(),
            "total_time_s": time.perf_counter() - run_start_perf,
        },
        "environment": collect_runtime_metadata(
            args,
            heap_size=(arcadedb_heap_size if args.backend == "arcadedb_sql" else None),
            runtime_versions=runtime_versions,
        ),
        "budget": budget_allocation_report(args),
        "dataset": {
            "name": args.dataset,
            "corpus": (
                stackoverflow_corpus
                if args.dataset.startswith("stackoverflow-")
                else None
            ),
            "label": label,
            "dir": str(dataset_dir),
            "dim": dim,
            "rows": total_rows,
        },
        "config": {
            "backend": args.backend,
            "count": count,
            "max_connections": args.max_connections,
            "beam_width": args.beam_width,
            "quantization": args.quantization,
            "store_vectors_in_graph": args.store_vectors_in_graph,
            "add_hierarchy": args.add_hierarchy,
            "batch_size": args.batch_size,
            "seed": args.seed,
            "run_label": args.run_label,
            "heap": heap_tag,
            "pg": {
                "host": args.pg_host,
                "port": args.pg_port,
                "user": args.pg_user,
                "database": args.pg_database,
                "shared_buffers": args.pg_shared_buffers,
                "shared_buffers_effective": pg_shared_buffers,
                "data_dir": str(db_path / "postgres-data"),
            },
            "arcadedb": {
                "heap_size_effective": (
                    arcadedb_heap_size if args.backend == "arcadedb_sql" else None
                ),
            },
            "qdrant": {
                "data_dir": str(db_path / "qdrant-data"),
                "collection": "vectordata",
                "hnsw_m": args.max_connections,
                "hnsw_ef_construct": args.beam_width,
            },
            "milvus": {
                "host": args.milvus_host,
                "port": args.milvus_port,
                "compose_version": args.milvus_compose_version,
                "compose_file": str(db_path / "milvus-compose" / "docker-compose.yml"),
                "collection": args.milvus_collection,
                "hnsw_m": args.max_connections,
                "hnsw_ef_construct": args.beam_width,
            },
            "faiss": {
                "index_file": str(db_path / "faiss.index"),
                "metric": "cosine_via_inner_product_normalized",
                "hnsw_m": args.max_connections,
                "hnsw_ef_construct": args.beam_width,
            },
            "lancedb": {
                "data_dir": str(db_path / "lancedb-data"),
                "table": "vectordata",
                "metric": (
                    str((lancedb_index_config or {}).get("metric"))
                    if lancedb_index_config
                    else "cosine"
                ),
                "index_type": (
                    str((lancedb_index_config or {}).get("index_type"))
                    if lancedb_index_config
                    else "IVF_HNSW_SQ"
                ),
                "num_partitions": ((lancedb_index_config or {}).get("num_partitions")),
                "quantization": ((lancedb_index_config or {}).get("quantization")),
                "hnsw_m": args.max_connections,
                "hnsw_ef_construct": args.beam_width,
            },
        },
        "phases": phases,
        "db_path": str(db_path),
        "db_size_mb": dir_size_mb(db_path),
        "peak_rss_mb": peak_rss_mb,
        "telemetry": {
            "run_status": "success",
            "error_type": None,
            "error_message": None,
            "db_create_time_s": (
                phases.get("create_db", {}).get("time_sec")
                if isinstance(phases, dict)
                else None
            ),
            "db_open_time_s": (
                phases.get("open_db", {}).get("time_sec")
                if isinstance(phases, dict)
                else None
            ),
            "db_close_time_s": (
                phases.get("close_db", {}).get("time_sec")
                if isinstance(phases, dict)
                else None
            ),
            "query_cold_time_s": None,
            "query_warm_mean_s": None,
            "query_result_hash_stable": None,
            "query_row_count_stable": None,
        },
    }

    if args.run_label:
        results_json = db_path / f"results_{args.run_label}.json"
    else:
        results_json = db_path / "results.json"
    results_json.parent.mkdir(parents=True, exist_ok=True)
    results_json.write_text(json.dumps(results, indent=2), encoding="utf-8")

    print("\nResults")
    print("-" * 80)
    if "ingest" in phases:
        print(f"Ingested: {phases['ingest'].get('ingested', 0):,}")
    if "ingest" in phases:
        print(f"Ingest time: {phases['ingest']['time_sec']:.2f}s")
    if "create_index" in phases:
        print(f"Index build time: {phases['create_index']['time_sec']:.2f}s")
    print(f"DB size: {results['db_size_mb']:.1f} MB")
    if peak_rss_mb is not None:
        suffix = " (client + server for client-server backends)"
        print(f"Peak RSS: {peak_rss_mb:.1f} MB{suffix}")
    print(f"Results saved to: {results_json}")


if __name__ == "__main__":
    main()
