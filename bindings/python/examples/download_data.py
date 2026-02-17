#!/usr/bin/env python3
"""
Download datasets for ArcadeDB import examples.

This script downloads both MovieLens and Stack Exchange datasets with
consistent, pinned versions for reproducibility, and systematically
injects NULL values for comprehensive testing.

Features:
- Progress bars for all long-running operations (requires tqdm: uv pip install tqdm)
- Timing measurements for performance monitoring
- Memory-efficient streaming for large files
- Smart sampling for fast verification (100K rows)
- Stack Overflow vector conversion (requires sentence-transformers + torch)
- Stack Overflow unified vector ground-truth generation (MSMARCO-style)

Available datasets:
1. MovieLens (movie ratings, tags, genres):
    - movielens-small: ~1 MB, ~100K ratings, 9K movies, 600 users
    - movielens-large: ~265 MB, ~33M ratings, 86K movies, 280K users

2. Stack Exchange (Q&A posts, users, tags, links):
    - stackoverflow-tiny: ~34 MB, ~100K records (subset of stackoverflow-small)
    - stackoverflow-small: ~642 MB, 1.41M records (cs.stackexchange.com, 2024-06-30)
    - stackoverflow-medium: ~2.9 GB, 5.56M records (stats.stackexchange.com, 2024-06-30)
    - stackoverflow-large: ~10 GB, subset of full stackoverflow.com (2024-06-30)
    - stackoverflow-xlarge: ~50 GB, subset of full stackoverflow.com (2024-06-30)
    - stackoverflow-full: ~323 GB, full stackoverflow.com (2024-06-30)

3. TPC-H (table benchmark):
    - tpch-sf1: TPC-H scale factor 1 (dbgen, local generation)
    - tpch-sf10: TPC-H scale factor 10 (dbgen, local generation)
    - tpch-sf100: TPC-H scale factor 100 (dbgen, local generation)

4. LDBC SNB Interactive v1 (graph benchmark):
    - ldbc-snb-sf1: Scale factor 1 (CsvMergeForeign, LongDateFormatter, Docker datagen)
    - ldbc-snb-sf10: Scale factor 10 (CsvMergeForeign, LongDateFormatter, Docker datagen)
    - ldbc-snb-sf100: Scale factor 100 (CsvMergeForeign, LongDateFormatter, Docker datagen)

5. MSMARCO v2.1 embeddings (vector benchmark):
    - msmarco-1m: 1M passage vectors
    - msmarco-5m: 5M passage vectors
    - msmarco-10m: 10M passage vectors


Stack Exchange Data Note:
- Pinned to 2024-06-30 quarterly dump for reproducibility
- Downloaded from archive.org
- 7z compressed format (requires py7zr library)
- stackoverflow-tiny is derived from stackoverflow-small (first 10k rows per XML,
  full Tags.xml)
- stackoverflow-large is derived from stackoverflow-full (proportional subset)
- stackoverflow-xlarge is derived from stackoverflow-full (proportional subset)

NULL Value Injection (MovieLens only):
- Enabled by default for MovieLens CSV files
- Systematically injects empty strings "" (CSV NULL equivalent)
  * movies.csv: genres (2%), title (0.5%)
  * ratings.csv: timestamp (3%), rating (1%)
  * links.csv: imdbId (5%), tmdbId (8%)
  * tags.csv: tag (5%), timestamp (2%)
- Uses deterministic random seed (42) for reproducibility
- Can be disabled with --no-nulls flag
- Stack Exchange XML files are downloaded as-is (no modification)

License:
- MovieLens: Free for educational use (grouplens.org)
- Stack Exchange: CC BY-SA (archive.org/details/stackexchange)
- MSMARCO: See dataset card on Hugging Face
- TPC-H: Generated with dbgen (local build or Docker); see TPC license terms
- LDBC SNB: Generated locally via Docker (ldbc/datagen). See LDBC terms.

Usage:
    python download_data.py movielens-small
    python download_data.py movielens-large
    python download_data.py movielens-small --no-nulls  # Skip NULL injection
    python download_data.py stackoverflow-tiny
    python download_data.py stackoverflow-small
    python download_data.py stackoverflow-medium
    python download_data.py stackoverflow-large
    python download_data.py stackoverflow-xlarge
    python download_data.py stackoverflow-full
    python download_data.py stackoverflow-small --verify-only  # Verify existing
    python download_data.py tpch-sf1
    python download_data.py tpch-sf10
    python download_data.py tpch-sf100
    python download_data.py ldbc-snb-sf1
    python download_data.py ldbc-snb-sf10
    python download_data.py ldbc-snb-sf100
    python download_data.py msmarco-1m
    python download_data.py msmarco-5m
    python download_data.py msmarco-10m
    python download_data.py stackoverflow-tiny --no-vectors  # Skip vector generation
"""

import argparse
import html
import json
import os
import re
import shutil
import subprocess
import time
import urllib.request
import zipfile
from pathlib import Path

try:
    from tqdm import tqdm
except ImportError:
    # Fallback if tqdm not installed
    tqdm = None


def ensure_clean_dir(path: Path, label: str) -> None:
    if path.exists():
        print(f"[CLEAN] Removing existing {label} directory: {path}")
        shutil.rmtree(path)


def parse_tpch_ddl(ddl_path: Path) -> dict[str, dict[str, object]]:
    ddl_text = ddl_path.read_text(encoding="utf-8", errors="ignore")
    tables: dict[str, dict[str, object]] = {}
    for match in re.finditer(r"CREATE TABLE\s+(\w+)\s*\((.*?)\);", ddl_text, re.S):
        table = match.group(1).lower()
        body = match.group(2)
        columns = []
        types = {}
        for raw_line in body.splitlines():
            line = raw_line.strip().rstrip(",")
            if not line:
                continue
            col_match = re.match(r"^([A-Z_]+)\s+([A-Z]+(?:\([^)]*\))?)", line)
            if not col_match:
                continue
            col = col_match.group(1).lower()
            col_type = col_match.group(2).upper()
            if col_type.startswith("DECIMAL"):
                inferred = "double"
            elif col_type.startswith("INT"):
                inferred = "integer"
            elif col_type.startswith("DATE"):
                inferred = "date"
            else:
                inferred = "string"
            columns.append(col)
            types[col] = inferred
        tables[table] = {"columns": columns, "types": types}
    if not tables:
        raise RuntimeError(f"No tables parsed from {ddl_path}")
    return tables


def _iter_tpch_records(tbl_path: Path, chunk_size: int = 1024 * 1024):
    """Yield TPC-H records split by the record delimiter '|\n'.

    TPC-H .tbl files can contain embedded newlines inside fields, so we
    cannot parse line-by-line. The record delimiter is the trailing "|\n".
    """
    delimiter = "|\n"
    buffer = ""
    with tbl_path.open("r", encoding="utf-8", errors="ignore") as fin:
        while True:
            chunk = fin.read(chunk_size)
            if not chunk:
                break
            buffer += chunk
            while True:
                idx = buffer.find(delimiter)
                if idx == -1:
                    break
                record = buffer[:idx]
                buffer = buffer[idx + len(delimiter) :]
                yield record
        if buffer.strip():
            yield buffer.rstrip("|")


def convert_tpch_tbl_to_csv(tbl_path: Path, csv_path: Path, columns: list[str]) -> None:
    import csv

    csv_path.parent.mkdir(parents=True, exist_ok=True)
    with csv_path.open("w", encoding="utf-8", newline="") as fout:
        writer = csv.writer(
            fout,
            delimiter="|",
            quoting=csv.QUOTE_NONE,
            escapechar="\\",
        )
        writer.writerow(columns)
        for record in _iter_tpch_records(tbl_path):
            fields = record.split("|")
            if len(fields) != len(columns):
                raise ValueError(
                    f"Unexpected column count in {tbl_path.name}: "
                    f"{len(fields)} (expected {len(columns)})"
                )
            fields = [f.replace("\n", " ").replace("\r", " ") for f in fields]
            writer.writerow(fields)


def generate_tpch_csv_and_schema(out_dir: Path, ddl_path: Path) -> None:
    ddl_schema = parse_tpch_ddl(ddl_path)
    csv_dir = out_dir / "csv"
    csv_dir.mkdir(parents=True, exist_ok=True)

    for table, meta in ddl_schema.items():
        columns = meta["columns"]
        tbl_path = out_dir / f"{table}.tbl"
        if not tbl_path.exists():
            raise FileNotFoundError(f"Missing {tbl_path}")
        csv_path = csv_dir / f"{table}.csv"
        if not csv_path.exists():
            print(f"[convert] {tbl_path.name} -> csv/{csv_path.name}")
            convert_tpch_tbl_to_csv(tbl_path, csv_path, columns)

    schema_path = out_dir / "schema.json"
    if not schema_path.exists():
        schema_path.write_text(
            json.dumps({"tables": ddl_schema}, indent=2), encoding="utf-8"
        )
        print(f"[schema] wrote {schema_path}")


def _infer_value_type(value: str) -> str:
    if value is None:
        return "string"
    value = value.strip()
    if value == "":
        return "string"
    if value.lower() in {"true", "false"}:
        return "boolean"
    if re.fullmatch(r"-?\d+", value):
        return "integer"
    if re.fullmatch(r"-?\d+\.\d+", value):
        return "double"
    return "string"


def _merge_types(current: str | None, new_type: str) -> str:
    if current is None:
        return new_type
    if current == "string" or new_type == "string":
        return "string"
    if current == "double" or new_type == "double":
        return "double"
    if current == "integer" or new_type == "integer":
        return "integer"
    if current == "boolean" or new_type == "boolean":
        return "boolean"
    return "string"


def _looks_like_header(row: list[str]) -> bool:
    if not row:
        return False
    tokens = 0
    for val in row:
        if re.fullmatch(r"[A-Za-z][A-Za-z0-9_.]*", val or ""):
            tokens += 1
    return tokens >= max(1, int(0.6 * len(row)))


def _apply_ldbc_overrides(name: str, inferred: str) -> str:
    lowered = name.lower()
    if lowered.endswith(".id") or lowered.endswith("_id"):
        return "integer"
    if lowered in {"ispartof", "issubclassof"}:
        return "integer"
    return inferred


def generate_ldbc_snb_schema(out_dir: Path, sample_rows: int = 100_000) -> None:
    import csv

    schema = {"nodes": {}, "edges": {}}
    csv_files = sorted(out_dir.rglob("*.csv"))
    if not csv_files:
        return

    def base_name(path: Path) -> str:
        stem = path.stem
        return re.sub(r"_\d+_\d+$", "", stem)

    for csv_path in csv_files:
        if csv_path.name.endswith(".crc"):
            continue
        base = base_name(csv_path)
        if base in schema["nodes"] or base in schema["edges"]:
            continue

        with csv_path.open("r", encoding="utf-8", errors="ignore", newline="") as f:
            reader = csv.reader(f, delimiter="|")
            try:
                first_row = next(reader)
            except StopIteration:
                continue

            if not _looks_like_header(first_row):
                print(f"[schema] skip (no header): {csv_path}")
                continue

            header = first_row
            type_map: dict[str, str | None] = {name: None for name in header}
            rows_read = 0
            for row in reader:
                if not row:
                    continue
                rows_read += 1
                for idx, name in enumerate(header):
                    value = row[idx] if idx < len(row) else ""
                    inferred = _infer_value_type(value)
                    type_map[name] = _merge_types(type_map[name], inferred)
                if rows_read >= sample_rows:
                    break

        properties = {
            name: _apply_ldbc_overrides(name, (type_map[name] or "string"))
            for name in header
        }

        tokens = base.split("_")
        if len(tokens) >= 3:
            source = tokens[0]
            target = tokens[-1]
            label = "_".join(tokens[1:-1])
            schema["edges"][base] = {
                "label": label,
                "from": source,
                "to": target,
                "file": str(csv_path.relative_to(out_dir)),
                "properties": properties,
            }
        else:
            schema["nodes"][base] = {
                "type": base,
                "file": str(csv_path.relative_to(out_dir)),
                "properties": properties,
            }

    schema_path = out_dir / "schema.json"
    schema_path.write_text(json.dumps(schema, indent=2), encoding="utf-8")
    print(f"[schema] wrote {schema_path}")


def introduce_null_values_movielens(extract_dir):
    """
    Systematically introduce NULL values in MovieLens CSV files.

    Strategy:
    - Uses deterministic random seed (42) for reproducibility
    - Injects empty strings "" for nullable fields (CSV NULL equivalent)
    - Never NULLs primary/foreign keys (movieId, userId)
    - Percentages chosen to mimic realistic missing data patterns

    NULL Injection Targets:
    - movies.csv: genres (2%), title (0.5%)
    - ratings.csv: timestamp (3%), rating (1%)
    - links.csv: imdbId (5%), tmdbId (8%)
    - tags.csv: tag (5%), timestamp (2%)
    """
    import csv
    import random

    # Seed for reproducibility - same NULLs every time
    random.seed(42)

    print("\n[WORK] Systematically introducing NULL values in MovieLens CSV files...")
    overall_start = time.time()

    # Configuration: file -> {nullable_field: percentage}
    null_config = {
        "movies.csv": {
            "genres": 0.02,  # 2% NULL genres (unclassified movies)
            "title": 0.005,  # 0.5% NULL titles (edge case testing)
        },
        "ratings.csv": {
            "timestamp": 0.03,  # 3% NULL timestamps (missing metadata)
            "rating": 0.01,  # 1% NULL ratings (invalid/missing ratings)
        },
        "links.csv": {
            "imdbId": 0.05,  # 5% NULL imdbId (movies not in IMDb)
            "tmdbId": 0.08,  # 8% NULL tmdbId (movies not in TMDb)
        },
        "tags.csv": {
            "tag": 0.05,  # 5% NULL tags (empty/missing tags)
            "timestamp": 0.02,  # 2% NULL timestamps (missing metadata)
        },
    }

    # Process each CSV file based on configuration
    for filename, nullable_fields in null_config.items():
        csv_path = extract_dir / filename
        if not csv_path.exists():
            continue

        start_time = time.time()
        temp_path = csv_path.with_suffix(".csv.tmp")
        null_counts = {field: 0 for field in nullable_fields}

        # Count total rows for progress bar
        total_rows = sum(1 for _ in open(csv_path, encoding="utf-8")) - 1

        with open(csv_path, "r", encoding="utf-8") as f_in, open(
            temp_path, "w", encoding="utf-8", newline=""
        ) as f_out:
            reader = csv.DictReader(f_in)
            writer = csv.DictWriter(f_out, fieldnames=reader.fieldnames)
            writer.writeheader()

            # Progress bar
            iterator = reader
            if tqdm:
                iterator = tqdm(
                    reader, total=total_rows, desc=f"   {filename}", unit=" rows"
                )

            for row in iterator:
                # Inject NULLs based on configuration
                for field, percentage in nullable_fields.items():
                    if random.random() < percentage:
                        row[field] = ""
                        null_counts[field] += 1
                writer.writerow(row)

        # Replace original with modified file
        temp_path.replace(csv_path)
        elapsed = time.time() - start_time

        # Print results
        null_summary = ", ".join(
            f"{count} NULL {field}" for field, count in null_counts.items()
        )
        print(f"   [OK] {filename}: {null_summary} ({elapsed:.2f}s)")

    overall_elapsed = time.time() - overall_start
    print(f"\n[TIME]  Total CSV NULL injection time: {overall_elapsed:.2f}s")


def download_movielens(size="large", inject_nulls=True):
    """Download and extract MovieLens dataset.

    Args:
        size: 'large' (~265 MB, ~33M ratings) or 'small' (~1 MB, ~100K ratings)
        inject_nulls: Whether to introduce NULL values for testing (default: True)
    """

    # Create data directory
    data_dir = Path(__file__).parent / "data"
    data_dir.mkdir(exist_ok=True)

    # Dataset configurations
    datasets = {
        "small": {
            "url": "https://files.grouplens.org/datasets/movielens/ml-latest-small.zip",
            "dirname": "movielens-small",
            "description": "~100K ratings, ~9K movies",
            "size_mb": "~1 MB",
        },
        "large": {
            "url": "https://files.grouplens.org/datasets/movielens/ml-latest.zip",
            "dirname": "movielens-large",
            "description": "~33M ratings, ~86K movies",
            "size_mb": "~265 MB",
        },
    }

    if size not in datasets:
        raise ValueError(f"Unknown dataset size: {size}. Choose 'small' or 'large'")

    config = datasets[size]
    url = config["url"]
    dirname = config["dirname"]
    zip_path = data_dir / f"movielens-{size}.zip"
    extract_dir = data_dir / dirname

    # Check if already downloaded
    ensure_clean_dir(extract_dir, f"MovieLens {size}")

    print(f"[DOWNLOAD] Downloading MovieLens {size} dataset")
    print(f"   Description: {config['description']} ({config['size_mb']})")
    print(f"   URL: {url}")
    print("   This may take a few minutes...")
    print()

    download_start = time.time()

    try:
        # Download with progress
        def report_progress(block_num, block_size, total_size):
            downloaded = block_num * block_size
            percent = min(100, (downloaded / total_size) * 100) if total_size > 0 else 0
            downloaded_mb = downloaded / (1024 * 1024)
            total_mb = total_size / (1024 * 1024)
            print(
                f"\r   Progress: {percent:.1f}% "
                f"({downloaded_mb:.1f}/{total_mb:.1f} MB)",
                end="",
            )

        urllib.request.urlretrieve(url, zip_path, reporthook=report_progress)
        print()  # New line after progress
        download_elapsed = time.time() - download_start
        print(f"[OK] Downloaded to: {zip_path} " f"({download_elapsed:.2f}s)")

        # Extract
        extract_start = time.time()
        print("[EXTRACT] Extracting...")
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            # Extract to temp dir (zip contains ml-latest/ml-latest-small)
            temp_extract = data_dir / "temp_extract"
            zip_ref.extractall(temp_extract)

            # Find the extracted folder (ml-latest or ml-latest-small)
            extracted_folders = list(temp_extract.iterdir())
            if len(extracted_folders) == 1 and extracted_folders[0].is_dir():
                source_dir = extracted_folders[0]
                # Move contents to our target directory
                if extract_dir.exists():
                    shutil.rmtree(extract_dir)
                shutil.move(str(source_dir), str(extract_dir))
                # Clean up temp directory
                shutil.rmtree(temp_extract)
            else:
                raise Exception(f"Unexpected zip structure in {temp_extract}")

        extract_elapsed = time.time() - extract_start
        print(f"[OK] Extracted to: {extract_dir} ({extract_elapsed:.2f}s)")

        # Introduce NULL values for testing
        if inject_nulls:
            introduce_null_values_movielens(extract_dir)
        else:
            print("\n[SKIP]  Skipping NULL value injection (--no-nulls flag)")

        # Show file sizes
        print("\n[STATS] Dataset contents:")
        for csv_file in extract_dir.glob("*.csv"):
            size_mb = csv_file.stat().st_size / (1024 * 1024)
            print(f"   - {csv_file.name}: {size_mb:.1f} MB")

        # Clean up zip file
        zip_path.unlink()
        print("\n[CLEAN] Cleaned up zip file")

        return extract_dir

    except Exception as e:
        print(f"[ERROR] Error downloading dataset: {e}")
        print(f"   You can manually download from: {url}")
        raise


def download_stackoverflow(size="small"):
    """Download and extract Stack Exchange dataset.

    Args:
        size: 'tiny' (~34 MB), 'small' (~80 MB), 'medium' (~500 MB),
              'large' (~10 GB subset), 'xlarge' (~50 GB subset), or
              'full' (~323 GB)
    """
    # Create data directory
    data_dir = Path(__file__).parent / "data"
    data_dir.mkdir(exist_ok=True)

    if size == "tiny":
        source_dir = data_dir / "stackoverflow-small"
        if not source_dir.exists():
            download_stackoverflow(size="small")
        return create_stackoverflow_tiny(source_dir=source_dir)

    if size == "large":
        source_dir = data_dir / "stackoverflow-full"
        if not source_dir.exists():
            download_stackoverflow(size="full")
        return create_stackoverflow_large(
            source_dir=source_dir,
            target_size_gb=10,
            out_name="stackoverflow-large",
            label="large",
        )

    if size == "xlarge":
        source_dir = data_dir / "stackoverflow-full"
        if not source_dir.exists():
            download_stackoverflow(size="full")
        return create_stackoverflow_large(
            source_dir=source_dir,
            target_size_gb=50,
            out_name="stackoverflow-xlarge",
            label="xlarge",
        )

    try:
        import py7zr
    except ImportError:
        print("[ERROR] Missing dependency: py7zr")
        print("   Install with: uv pip install py7zr")
        raise

    # Dataset configurations (pinned to 2024-06-30 for reproducibility)
    datasets = {
        "small": {
            "urls": [
                "https://archive.org/download/stackexchange/" "cs.stackexchange.com.7z"
            ],
            "dirname": "stackoverflow-small",
            "site": "cs.stackexchange.com",
            "description": "~80K posts (Computer Science)",
            "size_mb": "~80 MB",
            "date": "2024-06-30",
        },
        "medium": {
            "urls": [
                "https://archive.org/download/stackexchange/"
                "stats.stackexchange.com.7z"
            ],
            "dirname": "stackoverflow-medium",
            "site": "stats.stackexchange.com",
            "description": "~300K posts (Statistics)",
            "size_mb": "~500 MB",
            "date": "2024-06-30",
        },
        "full": {
            # Full Stack Overflow dump split into multiple parts
            "urls": [
                "https://archive.org/download/stackexchange/"
                "stackoverflow.com-Posts.7z",
                "https://archive.org/download/stackexchange/"
                "stackoverflow.com-Users.7z",
                "https://archive.org/download/stackexchange/"
                "stackoverflow.com-Comments.7z",
                "https://archive.org/download/stackexchange/"
                "stackoverflow.com-Tags.7z",
                "https://archive.org/download/stackexchange/"
                "stackoverflow.com-Votes.7z",
                "https://archive.org/download/stackexchange/"
                "stackoverflow.com-Badges.7z",
                "https://archive.org/download/stackexchange/"
                "stackoverflow.com-PostLinks.7z",
                "https://archive.org/download/stackexchange/"
                "stackoverflow.com-PostHistory.7z",
            ],
            "dirname": "stackoverflow-full",
            "site": "stackoverflow.com",
            "description": "Full Stack Overflow dump (all XML files)",
            "size_mb": "~323 GB",
            "date": "2024-06-30",
        },
    }

    if size not in datasets:
        raise ValueError(
            "Unknown dataset size: "
            f"{size}. Choose 'tiny', 'small', 'medium', 'large', 'xlarge', or 'full'"
        )

    config = datasets[size]
    dirname = config["dirname"]
    extract_dir = data_dir / dirname

    # Check if already downloaded
    ensure_clean_dir(extract_dir, f"Stack Exchange {size}")

    print(f"[DOWNLOAD] Downloading Stack Exchange {size} dataset")
    print(f"   Site: {config['site']}")
    print(f"   Description: {config['description']} ({config['size_mb']})")
    print(f"   Date: {config['date']} (pinned for reproducibility)")
    print("   This may take several minutes...")
    print()

    download_start = time.time()

    try:
        # Download with progress
        def report_progress(block_num, block_size, total_size):
            downloaded = block_num * block_size
            percent = min(100, (downloaded / total_size) * 100) if total_size > 0 else 0
            downloaded_mb = downloaded / (1024 * 1024)
            total_mb = total_size / (1024 * 1024)
            print(
                f"\r   Progress: {percent:.1f}% "
                f"({downloaded_mb:.1f}/{total_mb:.1f} MB)",
                end="",
            )

        extract_dir.mkdir(exist_ok=True)

        # Download one or multiple files depending on dataset
        for url in config["urls"]:
            filename = url.split("/")[-1]
            archive_path = data_dir / filename
            print(f"\n[DOWNLOAD] Downloading {filename}")
            file_start = time.time()
            urllib.request.urlretrieve(url, archive_path, reporthook=report_progress)
            print()  # New line after progress
            file_elapsed = time.time() - file_start
            print(f"[OK] Downloaded to: {archive_path} ({file_elapsed:.2f}s)")

            # Extract 7z file
            extract_start = time.time()
            print(f"[EXTRACT] Extracting {filename}...")
            with py7zr.SevenZipFile(archive_path, mode="r") as archive:
                archive.extractall(path=extract_dir)

            extract_elapsed = time.time() - extract_start
            print(f"[OK] Extracted ({extract_elapsed:.2f}s)")

            # Clean up archive file
            archive_path.unlink()

        download_elapsed = time.time() - download_start
        print(f"\n[TIME]  Total download time: {download_elapsed:.2f}s")

        print(f"\n[OK] Extracted to: {extract_dir}")

        # Show file sizes
        print("\n[STATS] Dataset contents:")
        xml_files = list(extract_dir.glob("*.xml"))
        if xml_files:
            for xml_file in sorted(xml_files):
                size_mb = xml_file.stat().st_size / (1024 * 1024)
                print(f"   - {xml_file.name}: {size_mb:.1f} MB")
        else:
            print("   [WARNING]  No XML files found")

        return extract_dir

    except Exception as e:
        print(f"[ERROR] Error downloading dataset: {e}")
        print(f"   You can manually download from: {config['urls'][0]}")
        raise


def _write_stackoverflow_subset(
    source_path: Path,
    target_path: Path,
    max_rows: int,
) -> int:
    """Write a truncated Stack Exchange XML file with the first max_rows rows."""
    root_tag = None
    row_count = 0
    closed = False

    with source_path.open(
        "r", encoding="utf-8", errors="ignore"
    ) as fin, target_path.open("w", encoding="utf-8") as fout:
        for line in fin:
            if root_tag is None:
                fout.write(line)
                match = re.match(r"\s*<(\w+)>\s*$", line)
                if match:
                    root_tag = match.group(1)
                continue

            if re.match(r"\s*</\w+>\s*$", line):
                if not closed:
                    fout.write(line)
                    closed = True
                break

            if re.match(r"\s*<row\b", line):
                if row_count < max_rows:
                    fout.write(line)
                    row_count += 1
                if row_count >= max_rows and root_tag:
                    fout.write(f"</{root_tag}>\n")
                    closed = True
                    break
                continue

            if row_count < max_rows:
                fout.write(line)

        if not closed and root_tag:
            fout.write(f"</{root_tag}>\n")

    return row_count


def _write_stackoverflow_subset_by_bytes(
    source_path: Path,
    target_path: Path,
    max_bytes: int,
) -> int:
    """Write a truncated Stack Exchange XML file up to max_bytes."""
    root_tag = None
    row_count = 0
    closed = False
    bytes_written = 0

    with source_path.open(
        "r", encoding="utf-8", errors="ignore"
    ) as fin, target_path.open("w", encoding="utf-8") as fout:
        for line in fin:
            if root_tag is None:
                fout.write(line)
                bytes_written += len(line.encode("utf-8"))
                match = re.match(r"\s*<(\w+)>\s*$", line)
                if match:
                    root_tag = match.group(1)
                continue

            if re.match(r"\s*</\w+>\s*$", line):
                if not closed:
                    fout.write(line)
                    bytes_written += len(line.encode("utf-8"))
                    closed = True
                break

            if re.match(r"\s*<row\b", line):
                if row_count == 0 or bytes_written < max_bytes:
                    fout.write(line)
                    bytes_written += len(line.encode("utf-8"))
                    row_count += 1
                if bytes_written >= max_bytes and root_tag:
                    fout.write(f"</{root_tag}>\n")
                    bytes_written += len(f"</{root_tag}>\n".encode("utf-8"))
                    closed = True
                    break
                continue

            if bytes_written < max_bytes:
                fout.write(line)
                bytes_written += len(line.encode("utf-8"))

        if not closed and root_tag:
            fout.write(f"</{root_tag}>\n")

    return row_count


def create_stackoverflow_tiny(source_dir: Path, max_rows: int = 10_000) -> Path:
    """Create a tiny Stack Exchange subset from the small dataset."""
    data_dir = Path(__file__).parent / "data"
    out_dir = data_dir / "stackoverflow-tiny"

    ensure_clean_dir(out_dir, "Stack Exchange tiny")
    out_dir.mkdir(parents=True, exist_ok=True)

    xml_files = [
        "Posts.xml",
        "Users.xml",
        "Comments.xml",
        "Tags.xml",
        "Badges.xml",
        "PostLinks.xml",
        "PostHistory.xml",
        "Votes.xml",
    ]
    keep_full = {"Tags.xml"}

    print("[BUILD] Creating stackoverflow-tiny from stackoverflow-small")
    print(f"   Source: {source_dir}")
    print(f"   Output: {out_dir}")
    print(f"   Rows per file: {max_rows} (except Tags.xml)")
    print()

    for filename in xml_files:
        source_path = source_dir / filename
        target_path = out_dir / filename
        if not source_path.exists():
            raise FileNotFoundError(f"Missing source XML: {source_path}")

        if filename in keep_full:
            shutil.copy2(source_path, target_path)
            print(f"   [OK] {filename}: copied full file")
            continue

        count = _write_stackoverflow_subset(
            source_path=source_path,
            target_path=target_path,
            max_rows=max_rows,
        )
        print(f"   [OK] {filename}: {count:,} rows")

    print("\n[OK] Created stackoverflow-tiny dataset")
    return out_dir


def create_stackoverflow_large(
    source_dir: Path,
    target_size_gb: int = 10,
    out_name: str = "stackoverflow-large",
    label: str = "large",
) -> Path:
    """Create a large Stack Exchange subset from the full dataset."""
    data_dir = Path(__file__).parent / "data"
    out_dir = data_dir / out_name

    ensure_clean_dir(out_dir, f"Stack Exchange {label}")
    out_dir.mkdir(parents=True, exist_ok=True)

    xml_files = [
        "Posts.xml",
        "Users.xml",
        "Comments.xml",
        "Tags.xml",
        "Badges.xml",
        "PostLinks.xml",
        "PostHistory.xml",
        "Votes.xml",
    ]

    file_sizes = {}
    total_bytes = 0
    for filename in xml_files:
        source_path = source_dir / filename
        if not source_path.exists():
            raise FileNotFoundError(f"Missing source XML: {source_path}")
        size = source_path.stat().st_size
        file_sizes[filename] = size
        total_bytes += size

    target_bytes_total = target_size_gb * 1024 * 1024 * 1024
    if total_bytes <= 0:
        raise RuntimeError("Full Stack Exchange dataset is empty.")

    ratio = min(1.0, target_bytes_total / total_bytes)

    print(f"[BUILD] Creating {out_name} from stackoverflow-full")
    print(f"   Source: {source_dir}")
    print(f"   Output: {out_dir}")
    print(f"   Target size: ~{target_size_gb} GB")
    print(f"   Ratio: {ratio:.6f}")
    print()

    for filename in xml_files:
        source_path = source_dir / filename
        target_path = out_dir / filename
        target_bytes = max(1, int(file_sizes[filename] * ratio))

        if target_bytes >= file_sizes[filename]:
            shutil.copy2(source_path, target_path)
            print(f"   [OK] {filename}: copied full file")
            continue

        count = _write_stackoverflow_subset_by_bytes(
            source_path=source_path,
            target_path=target_path,
            max_bytes=target_bytes,
        )
        print(f"   [OK] {filename}: {count:,} rows")

    print(f"\n[OK] Created {out_name} dataset")
    return out_dir


def _iter_stackoverflow_rows(xml_path: Path, fields: list[str]):
    import xml.etree.ElementTree as ET

    context = ET.iterparse(xml_path, events=("start", "end"))
    _, root = next(context)
    for event, elem in context:
        if event == "end" and elem.tag == "row":
            attrs = elem.attrib
            yield {key: attrs.get(key) for key in fields}
            elem.clear()
            root.clear()


def _clean_stackoverflow_text(text: str | None) -> str:
    if not text:
        return ""
    text = html.unescape(text)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


class _VectorShardWriter:
    def __init__(self, out_dir: Path, base_name: str, shard_size: int):
        self.out_dir = out_dir
        self.base_name = base_name
        self.shard_size = shard_size
        self.shards: list[dict[str, int | str]] = []
        self._writer = None
        self._current_path: Path | None = None
        self._shard_idx = 0
        self._filled = 0
        self.count = 0
        self.dim: int | None = None

    def _open_new(self) -> None:
        if self._writer:
            self._close_current()
        self._current_path = (
            self.out_dir / f"{self.base_name}.shard{self._shard_idx:04d}.f32"
        )
        self._writer = open(self._current_path, "wb", buffering=1 << 20)
        self._shard_idx += 1
        self._filled = 0

    def _close_current(self) -> None:
        if not self._writer or not self._current_path:
            return
        self._writer.close()
        self.shards.append(
            {
                "path": self._current_path.name,
                "count": self._filled,
                "start": self.count - self._filled,
            }
        )
        self._writer = None
        self._current_path = None
        self._filled = 0

    def write(self, vectors) -> None:
        if vectors.size == 0:
            return
        if self.dim is None:
            self.dim = int(vectors.shape[1])
        idx = 0
        total = int(vectors.shape[0])
        while idx < total:
            if not self._writer or self._filled == self.shard_size:
                self._open_new()
            take = min(self.shard_size - self._filled, total - idx)
            self._writer.write(vectors[idx : idx + take].tobytes(order="C"))
            idx += take
            self._filled += take
            self.count += take
            if self._filled == self.shard_size:
                self._close_current()

    def close(self) -> None:
        if self._writer:
            self._close_current()


def embed_stackoverflow_vectors(
    extract_dir: Path,
    dataset_name: str,
    model_name: str = "all-MiniLM-L6-v2",
    batch_size: int = 256,
    shard_size: int = 100_000,
    max_rows: int | None = None,
    progress_every: int = 10_000,
    gt_queries: int = 1000,
    gt_topk: int = 50,
    gt_chunk: int = 4096,
) -> None:
    try:
        import numpy as np
        import torch
        from sentence_transformers import SentenceTransformer
    except ImportError as exc:
        raise RuntimeError(
            "Missing dependencies for Stack Overflow embeddings. "
            "Install with: uv pip install sentence-transformers torch numpy"
        ) from exc

    device = "cuda" if torch.cuda.is_available() else "cpu"
    print(f"[VECTORS] Loading model: {model_name} ({device})")
    model = SentenceTransformer(model_name, device=device)
    tokenizer = getattr(model, "tokenizer", None)
    max_seq_len = None
    if hasattr(model, "get_max_seq_length"):
        max_seq_len = model.get_max_seq_length()
    if not max_seq_len or max_seq_len <= 0:
        max_seq_len = None

    out_dir = extract_dir / "vectors"
    out_dir.mkdir(parents=True, exist_ok=True)

    def _truncate_for_model(text: str) -> tuple[str, bool]:
        if not max_seq_len:
            return text, False
        if tokenizer is None:
            approx_max_chars = max_seq_len * 4
            if len(text) <= approx_max_chars:
                return text, False
            return text[:approx_max_chars], True
        encoded = tokenizer(
            text,
            truncation=True,
            max_length=max_seq_len,
            return_overflowing_tokens=True,
        )
        truncated = bool(encoded.get("overflowing_tokens"))
        input_ids = encoded["input_ids"]
        if input_ids and isinstance(input_ids[0], list):
            input_ids = input_ids[0]
        return tokenizer.decode(input_ids, skip_special_tokens=True), truncated

    def process_corpus(
        name: str,
        rows_iter,
        text_builder,
        id_builder,
        text_fields: list[str],
    ) -> None:
        ids_path = out_dir / f"{dataset_name}-{name}.ids.jsonl"
        writer = _VectorShardWriter(out_dir, f"{dataset_name}-{name}", shard_size)
        total_written = 0
        skipped = 0
        truncated = 0
        seen_rows = 0
        batch_texts: list[str] = []
        batch_ids: list[dict[str, object]] = []

        def flush() -> None:
            nonlocal total_written, batch_texts, batch_ids
            if not batch_texts:
                return
            remaining = None if max_rows is None else max_rows - total_written
            if remaining is not None and remaining <= 0:
                batch_texts = []
                batch_ids = []
                return
            if remaining is not None and len(batch_texts) > remaining:
                batch_texts = batch_texts[:remaining]
                batch_ids = batch_ids[:remaining]
            vectors = model.encode(
                batch_texts,
                batch_size=batch_size,
                normalize_embeddings=True,
                convert_to_numpy=True,
                show_progress_bar=False,
            )
            vectors = vectors.astype(np.float32, copy=False)
            writer.write(vectors)
            for idx, meta in enumerate(batch_ids):
                meta["vector_id"] = total_written + idx
                ids_file.write(json.dumps(meta) + "\n")
            total_written += len(batch_texts)
            batch_texts = []
            batch_ids = []

        print(f"[VECTORS] Building {name} vectors...")
        with open(ids_path, "w", encoding="utf-8") as ids_file:
            for row in rows_iter:
                seen_rows += 1
                if max_rows is not None and total_written >= max_rows:
                    break
                text = _clean_stackoverflow_text(text_builder(row))
                if not text:
                    skipped += 1
                    continue
                text, was_truncated = _truncate_for_model(text)
                if was_truncated:
                    truncated += 1
                batch_texts.append(text)
                batch_ids.append(id_builder(row))
                if len(batch_texts) >= batch_size:
                    flush()
                if progress_every and seen_rows % progress_every == 0:
                    in_flight = len(batch_texts)
                    print(
                        f"[VECTORS] {name}: seen {seen_rows:,}, "
                        f"embedded {total_written + in_flight:,}, "
                        f"skipped {skipped:,}"
                    )
            flush()

        writer.close()
        meta = {
            "dataset": dataset_name,
            "corpus": name,
            "model": model_name,
            "device": device,
            "dim": writer.dim,
            "dtype": "float32",
            "count": writer.count,
            "shard_size": shard_size,
            "shards": writer.shards,
            "text_fields": text_fields,
            "ids_file": ids_path.name,
            "skipped_empty": skipped,
            "truncated": truncated,
            "max_seq_length": max_seq_len,
        }
        meta_path = out_dir / f"{dataset_name}-{name}.meta.json"
        meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")
        print(
            f"[VECTORS] {name}: {writer.count:,} vectors, "
            f"{len(writer.shards)} shards (skipped {skipped:,})"
        )

    posts_xml = extract_dir / "Posts.xml"
    comments_xml = extract_dir / "Comments.xml"
    if not posts_xml.exists() or not comments_xml.exists():
        raise FileNotFoundError("Posts.xml or Comments.xml not found")

    def question_text(row: dict[str, str | None]) -> str:
        title = row.get("Title") or ""
        body = row.get("Body") or ""
        if title and body:
            return f"{title}\n\n{body}"
        return title or body

    def answer_text(row: dict[str, str | None]) -> str:
        return row.get("Body") or ""

    process_corpus(
        "questions",
        (
            row
            for row in _iter_stackoverflow_rows(
                posts_xml, ["Id", "PostTypeId", "Title", "Body"]
            )
            if row.get("PostTypeId") == "1"
        ),
        question_text,
        lambda row: {"post_id": int(row.get("Id") or 0), "post_type": "question"},
        ["Title", "Body"],
    )

    process_corpus(
        "answers",
        (
            row
            for row in _iter_stackoverflow_rows(posts_xml, ["Id", "PostTypeId", "Body"])
            if row.get("PostTypeId") == "2"
        ),
        answer_text,
        lambda row: {"post_id": int(row.get("Id") or 0), "post_type": "answer"},
        ["Body"],
    )

    process_corpus(
        "comments",
        _iter_stackoverflow_rows(comments_xml, ["Id", "PostId", "Text"]),
        lambda row: row.get("Text") or "",
        lambda row: {
            "comment_id": int(row.get("Id") or 0),
            "post_id": int(row.get("PostId") or 0),
        },
        ["Text"],
    )

    corpus_names = ["questions", "answers", "comments"]
    corpus_metas: list[dict[str, object]] = []
    for name in corpus_names:
        meta_path = out_dir / f"{dataset_name}-{name}.meta.json"
        if not meta_path.exists():
            raise FileNotFoundError(f"Missing vector metadata: {meta_path}")
        corpus_metas.append(json.loads(meta_path.read_text(encoding="utf-8")))

    dims = {
        int(meta["dim"])
        for meta in corpus_metas
        if meta.get("dim") is not None and int(meta.get("count", 0)) > 0
    }
    if len(dims) > 1:
        raise RuntimeError(
            f"Mismatched vector dimensions across corpora: {sorted(dims)}"
        )
    combined_dim = next(iter(dims)) if dims else None

    combined_shards: list[dict[str, object]] = []
    combined_total = 0
    for meta in corpus_metas:
        source_corpus = str(meta["corpus"])
        for shard in meta.get("shards", []):
            shard_obj = dict(shard)
            shard_path = out_dir / str(shard_obj["path"])
            shard_count = int(shard_obj["count"])
            combined_shards.append(
                {
                    "path": str(shard_obj["path"]),
                    "path_obj": shard_path,
                    "count": shard_count,
                    "start": combined_total,
                    "source_corpus": source_corpus,
                }
            )
            combined_total += shard_count

    combined_ids_path = out_dir / f"{dataset_name}-all.ids.jsonl"
    global_id = 0
    with open(combined_ids_path, "w", encoding="utf-8") as fout:
        for name in corpus_names:
            source_ids = out_dir / f"{dataset_name}-{name}.ids.jsonl"
            with open(source_ids, "r", encoding="utf-8") as fin:
                for line in fin:
                    if not line.strip():
                        continue
                    obj = json.loads(line)
                    obj["source_corpus"] = name
                    obj["source_vector_id"] = obj.get("vector_id")
                    obj["vector_id"] = global_id
                    fout.write(json.dumps(obj) + "\n")
                    global_id += 1

    if global_id != combined_total:
        raise RuntimeError(
            "Combined id count does not match combined vector count: "
            f"ids={global_id}, vectors={combined_total}"
        )

    def build_gt_sharded(
        *,
        shards: list[dict[str, object]],
        total_count: int,
        dim: int,
        gt_path: Path,
        q_count: int,
        topk: int,
    ) -> None:
        import heapq
        import mmap

        print(f"[GT] building exact GT for {q_count} queries, k={topk}")

        q_count = min(q_count, total_count)
        rng = np.random.default_rng()
        q_indices = rng.choice(total_count, size=q_count, replace=False)

        queries = np.empty((q_count, dim), dtype=np.float32)
        shard_map: dict[Path, list[tuple[int, int]]] = {}

        for qi, gidx in enumerate(q_indices):
            for shard in shards:
                shard_start = int(shard["start"])
                shard_count = int(shard["count"])
                if shard_start <= gidx < shard_start + shard_count:
                    shard_map.setdefault(Path(shard["path_obj"]), []).append(
                        (qi, int(gidx - shard_start))
                    )
                    break

        def close_memmap(mm: "np.memmap | None") -> None:
            if mm is None:
                return
            mm.flush()
            m = getattr(mm, "_mmap", None)
            if m is not None:
                try:
                    m.madvise(mmap.MADV_DONTNEED)
                except Exception:
                    pass
                m.close()

        for shard in shards:
            shard_path = Path(shard["path_obj"])
            assigns = shard_map.get(shard_path)
            if not assigns:
                continue
            mm = np.memmap(
                shard_path,
                dtype=np.float32,
                mode="r",
                shape=(int(shard["count"]), dim),
            )
            for qi, local_idx in assigns:
                queries[qi] = mm[local_idx]
            close_memmap(mm)

        heaps = [[] for _ in range(q_count)]

        for shard in shards:
            shard_path = Path(shard["path_obj"])
            print(f"[GT] scanning {shard_path.name}")
            mm = np.memmap(
                shard_path,
                dtype=np.float32,
                mode="r",
                shape=(int(shard["count"]), dim),
            )
            for off in range(0, int(shard["count"]), gt_chunk):
                block = mm[off : off + gt_chunk]
                sims = block @ queries.T
                for qi in range(q_count):
                    heap = heaps[qi]
                    col = sims[:, qi]
                    for i, score in enumerate(col):
                        doc_id = int(shard["start"]) + off + i
                        if len(heap) < topk:
                            heapq.heappush(heap, (float(score), doc_id))
                        else:
                            heapq.heappushpop(heap, (float(score), doc_id))
            close_memmap(mm)

        with open(gt_path, "w", encoding="utf-8") as f:
            for qi, heap in enumerate(heaps):
                heap.sort(reverse=True)
                json.dump(
                    {
                        "query_id": int(q_indices[qi]),
                        "topk": [
                            {"doc_id": int(doc_id), "score": float(score)}
                            for score, doc_id in heap
                        ],
                    },
                    f,
                )
                f.write("\n")

        print(f"[GT] wrote {gt_path}")

    gt_path = out_dir / f"{dataset_name}-all.gt.jsonl"
    if combined_total <= 0:
        print("[GT] skipping GT generation (no vectors found)")
    elif combined_dim is None:
        print("[GT] skipping GT generation (missing vector dimensions)")
    else:
        build_gt_sharded(
            shards=combined_shards,
            total_count=combined_total,
            dim=int(combined_dim),
            gt_path=gt_path,
            q_count=gt_queries,
            topk=gt_topk,
        )

    combined_meta_path = out_dir / f"{dataset_name}-all.meta.json"
    combined_meta = {
        "dataset": dataset_name,
        "corpus": "all",
        "source_corpora": corpus_names,
        "model": model_name,
        "device": device,
        "dim": combined_dim,
        "dtype": "float32",
        "count": combined_total,
        "shard_size": shard_size,
        "shards": [
            {
                "path": str(shard["path"]),
                "count": int(shard["count"]),
                "start": int(shard["start"]),
                "source_corpus": str(shard["source_corpus"]),
            }
            for shard in combined_shards
        ],
        "ids_file": combined_ids_path.name,
        "gt_file": gt_path.name,
        "gt_queries": min(gt_queries, combined_total),
        "gt_topk": gt_topk,
        "max_seq_length": max_seq_len,
    }
    combined_meta_path.write_text(
        json.dumps(combined_meta, indent=2),
        encoding="utf-8",
    )
    print(
        f"[VECTORS] all: {combined_total:,} vectors, "
        f"{len(combined_shards)} shards, "
        f"gt={gt_path.name}"
    )


def download_tpch(scale_factor: int = 10) -> Path:
    """Generate TPC-H data using dbgen via Docker."""
    data_dir = Path(__file__).parent / "data"
    data_dir.mkdir(exist_ok=True)

    out_dir = data_dir / f"tpch-sf{scale_factor}"
    dbgen_zip = data_dir / "tpch-dbgen.zip"
    dbgen_dir = data_dir / "tpch-dbgen"

    ensure_clean_dir(out_dir, f"TPC-H SF{scale_factor}")

    marker = out_dir / "customer.tbl"
    legacy_marker = dbgen_dir / "customer.tbl"
    ddl_path = dbgen_dir / "dss.ddl"
    if marker.exists():
        print(f"[OK] TPC-H already generated at: {out_dir}")
        generate_tpch_csv_and_schema(out_dir, ddl_path)
        return out_dir
    if legacy_marker.exists():
        print(f"[OK] TPC-H already generated at: {dbgen_dir}")
        generate_tpch_csv_and_schema(out_dir, ddl_path)
        return out_dir

    if not dbgen_dir.exists():
        print("[DOWNLOAD] Downloading dbgen source (TPC-H)")
        url = "https://github.com/electrum/tpch-dbgen/archive/refs/heads/master.zip"

        def report_progress(block_num, block_size, total_size):
            downloaded = block_num * block_size
            percent = min(100, (downloaded / total_size) * 100) if total_size > 0 else 0
            downloaded_mb = downloaded / (1024 * 1024)
            total_mb = total_size / (1024 * 1024)
            print(
                f"\r   Progress: {percent:.1f}% "
                f"({downloaded_mb:.1f}/{total_mb:.1f} MB)",
                end="",
            )

        urllib.request.urlretrieve(url, dbgen_zip, reporthook=report_progress)
        print()

        extract_dir = data_dir / "tpch-dbgen-extract"
        if extract_dir.exists():
            shutil.rmtree(extract_dir)
        extract_dir.mkdir(parents=True, exist_ok=True)

        with zipfile.ZipFile(dbgen_zip, "r") as zip_ref:
            zip_ref.extractall(extract_dir)

        extracted_folders = [p for p in extract_dir.iterdir() if p.is_dir()]
        if len(extracted_folders) != 1:
            raise RuntimeError("Unexpected dbgen zip structure")

        extracted = extracted_folders[0]
        if dbgen_dir.exists():
            shutil.rmtree(dbgen_dir)
        shutil.move(str(extracted), str(dbgen_dir))
        shutil.rmtree(extract_dir)
        dbgen_zip.unlink(missing_ok=True)

    out_dir.mkdir(parents=True, exist_ok=True)
    dists_src = dbgen_dir / "dists.dss"
    if not dists_src.exists():
        raise RuntimeError("dists.dss not found in dbgen directory")

    if shutil.which("docker") is None:
        raise RuntimeError("Docker is required to generate TPC-H datasets.")
    print("[BUILD] Building dbgen via Docker")
    print(f"[GENERATE] TPC-H SF{scale_factor} via Docker (this can take a while)")
    cmd = [
        "docker",
        "run",
        "--rm",
        "--mount",
        f"type=bind,source={dbgen_dir},target=/work",
        "--mount",
        f"type=bind,source={out_dir},target=/out",
        "gcc:13",
        "sh",
        "-lc",
        "make -C /work && cd /out && /work/dbgen -s "
        f"{scale_factor} -f -b /work/dists.dss",
    ]
    subprocess.run(cmd, check=True)

    if not (out_dir / "customer.tbl").exists():
        legacy_tbls = list(dbgen_dir.glob("*.tbl"))
        if legacy_tbls:
            for tbl in legacy_tbls:
                shutil.move(str(tbl), str(out_dir / tbl.name))

    generate_tpch_csv_and_schema(out_dir, ddl_path)
    print(f"[OK] TPC-H generated at: {out_dir}")
    return out_dir


def download_ldbc_snb(scale_factor: int = 1) -> Path:
    """Generate LDBC SNB Interactive v1 dataset via Docker datagen."""
    if scale_factor not in {1, 10, 100}:
        raise ValueError("Unsupported LDBC SNB scale factor. Use 1, 10, or 100.")

    data_dir = Path(__file__).parent / "data"
    data_dir.mkdir(exist_ok=True)

    out_dir = data_dir / f"ldbc-snb-sf{scale_factor}"
    marker = out_dir / ".ldbc_snb_ok"
    ensure_clean_dir(out_dir, f"LDBC SNB SF{scale_factor}")

    if shutil.which("docker") is None:
        raise RuntimeError("Docker is required to generate LDBC SNB datasets.")

    params_dir = data_dir / "ldbc-snb-datagen"
    params_dir.mkdir(parents=True, exist_ok=True)
    params_path = params_dir / f"params-csv-merge-foreign-longdate-sf{scale_factor}.ini"

    if not params_path.exists():
        template_url = (
            "https://raw.githubusercontent.com/ldbc/ldbc_snb_datagen_hadoop/"
            "main/params-csv-merge-foreign.ini"
        )
        print("[DOWNLOAD] LDBC SNB params template")
        template = urllib.request.urlopen(template_url).read().decode("utf-8")
        lines = []
        inserted = False
        for line in template.splitlines():
            if line.startswith("ldbc.snb.datagen.generator.scaleFactor:"):
                lines.append(
                    f"ldbc.snb.datagen.generator.scaleFactor:snb.interactive.{scale_factor}"
                )
                lines.append(
                    "ldbc.snb.datagen.serializer.dateFormatter:"
                    "ldbc.snb.datagen.util.formatter.LongDateFormatter"
                )
                inserted = True
            else:
                lines.append(line)
        if not inserted:
            lines.insert(
                0,
                f"ldbc.snb.datagen.generator.scaleFactor:snb.interactive.{scale_factor}",
            )
            lines.insert(
                1,
                "ldbc.snb.datagen.serializer.dateFormatter:"
                "ldbc.snb.datagen.util.formatter.LongDateFormatter",
            )
        params_path.write_text("\n".join(lines) + "\n")

    hadoop_xmx = {1: "-Xmx2G", 10: "-Xmx8G", 100: "-Xmx24G"}[scale_factor]

    print(f"[GENERATE] LDBC SNB Interactive SF{scale_factor} via Docker")
    out_dir.mkdir(parents=True, exist_ok=True)
    cmd = [
        "docker",
        "run",
        "--rm",
        "--mount",
        f"type=bind,source={out_dir},target=/opt/ldbc_snb_datagen/out",
        "--mount",
        f"type=bind,source={params_path},target=/opt/ldbc_snb_datagen/params.ini",
        "-e",
        f"HADOOP_CLIENT_OPTS={hadoop_xmx}",
        "ldbc/datagen",
    ]
    subprocess.run(cmd, check=True)

    marker.write_text("ok")
    generate_ldbc_snb_schema(out_dir)
    print(f"[OK] LDBC SNB generated at: {out_dir}")
    return out_dir


def convert_msmarco_parquet_to_shards(
    *,
    parquet_glob: str,
    out_dir: Path,
    count: int,
    shard_size: int = 100_000,
    batch_rows: int = 8192,
    fsync_every: int = 50_000,
    progress_every: int = 100_000,
    q_count: int = 1000,
    topk: int = 50,
    chunk: int = 4096,
):
    """Convert MSMARCO parquet parts to shard files and build GT."""
    try:
        import glob
        import heapq
        import json
        import mmap

        import numpy as np
        import pyarrow.parquet as pq
    except ImportError as exc:
        raise RuntimeError(
            "Missing dependencies for MSMARCO conversion. "
            "Install with: uv pip install numpy pyarrow"
        ) from exc

    def fmt_secs(secs: float | None) -> str:
        if secs is None or secs <= 0:
            return "?"
        m, s = divmod(int(secs + 0.5), 60)
        h, m = divmod(m, 60)
        return f"{h:d}:{m:02d}:{s:02d}" if h else f"{m:02d}:{s:02d}"

    def sync_and_advise(fd: int) -> None:
        try:
            os.fsync(fd)
        except OSError:
            pass
        try:
            os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)
        except (AttributeError, OSError):
            pass

    def close_memmap(mm: "np.memmap | None") -> None:
        if mm is None:
            return
        mm.flush()
        m = getattr(mm, "_mmap", None)
        if m is not None:
            try:
                m.madvise(mmap.MADV_DONTNEED)
            except Exception:
                pass
            m.close()

    def build_gt_sharded(
        *,
        shards: list[dict],
        total_count: int,
        dim: int,
        gt_path: Path,
        q_count: int,
        topk: int,
    ) -> None:
        print(f"[GT] building exact GT for {q_count} queries, k={topk}")

        q_count = min(q_count, total_count)
        rng = np.random.default_rng()
        q_indices = rng.choice(total_count, size=q_count, replace=False)

        queries = np.empty((q_count, dim), dtype=np.float32)
        shard_map: dict[Path, list[tuple[int, int]]] = {}

        for qi, gidx in enumerate(q_indices):
            for s in shards:
                if s["start"] <= gidx < s["start"] + s["count"]:
                    shard_map.setdefault(s["path"], []).append((qi, gidx - s["start"]))
                    break

        for s in shards:
            assigns = shard_map.get(s["path"])
            if not assigns:
                continue
            mm = np.memmap(
                s["path"], dtype=np.float32, mode="r", shape=(s["count"], dim)
            )
            for qi, li in assigns:
                queries[qi] = mm[li]
            close_memmap(mm)

        heaps = [[] for _ in range(q_count)]

        for s in shards:
            print(f"[GT] scanning {s['path'].name}")
            mm = np.memmap(
                s["path"], dtype=np.float32, mode="r", shape=(s["count"], dim)
            )
            for off in range(0, s["count"], chunk):
                block = mm[off : off + chunk]
                sims = block @ queries.T
                for qi in range(q_count):
                    heap = heaps[qi]
                    col = sims[:, qi]
                    for i, score in enumerate(col):
                        doc_id = s["start"] + off + i
                        if len(heap) < topk:
                            heapq.heappush(heap, (score, doc_id))
                        else:
                            heapq.heappushpop(heap, (score, doc_id))
            close_memmap(mm)

        with open(gt_path, "w", encoding="utf-8") as f:
            for qi, heap in enumerate(heaps):
                heap.sort(reverse=True)
                json.dump(
                    {
                        "query_id": int(q_indices[qi]),
                        "topk": [
                            {"doc_id": int(d), "score": float(s)} for s, d in heap
                        ],
                    },
                    f,
                )
                f.write("\n")

        print(f"[GT] wrote {gt_path}")

    parquet_files = sorted(glob.glob(parquet_glob))
    if not parquet_files:
        raise RuntimeError("No parquet files found for MSMARCO")

    out_dir.mkdir(parents=True, exist_ok=True)
    print(f"[init] Found {len(parquet_files)} parquet files to process")

    dim = None
    written = 0
    filled = 0
    shard_idx = 0
    shard_start = 0
    shards: list[dict] = []

    path = None
    writer = None
    t0 = time.time()
    last_report = 0

    for file_idx, parquet_file in enumerate(parquet_files):
        if written >= count:
            break

        print(
            f"[process] File {file_idx + 1}/{len(parquet_files)}: "
            f"{Path(parquet_file).name}"
        )

        pf = pq.ParquetFile(parquet_file)
        for record_batch in pf.iter_batches(columns=["emb"], batch_size=batch_rows):
            col = record_batch.column(0)
            offsets = col.offsets.to_numpy()
            values = np.asarray(
                col.values.to_numpy(zero_copy_only=False), dtype=np.float32
            )

            if dim is None:
                dim = int(offsets[1] - offsets[0])
                path = out_dir / f"msmarco-passages-{count}.shard{shard_idx:04d}.f32"
                writer = open(path, "wb", buffering=1 << 20)

            spans = offsets[1:] - offsets[:-1]
            if not np.all(spans == dim):
                raise RuntimeError(
                    f"Non-uniform embedding dimension detected in {parquet_file}"
                )

            embs = np.asarray(values, dtype=np.float32).reshape(-1, dim)
            embs = embs / (np.linalg.norm(embs, axis=1, keepdims=True) + 1e-12)
            embs = np.ascontiguousarray(embs)

            off = 0
            while off < len(embs) and written < count:
                take = min(shard_size - filled, count - written, len(embs) - off)
                writer.write(embs[off : off + take].tobytes(order="C"))
                off += take
                filled += take
                written += take

                if written % fsync_every == 0:
                    sync_and_advise(writer.fileno())

                if filled == shard_size and written < count:
                    sync_and_advise(writer.fileno())
                    writer.close()
                    shards.append({"path": path, "count": filled, "start": shard_start})
                    shard_start += filled
                    shard_idx += 1
                    filled = 0
                    path = (
                        out_dir / f"msmarco-passages-{count}.shard{shard_idx:04d}.f32"
                    )
                    writer = open(path, "wb", buffering=1 << 20)

            if written - last_report >= progress_every:
                elapsed = time.time() - t0
                rate = written / elapsed
                eta = (count - written) / rate if rate else None
                print(
                    f"[convert] {written:,}/{count:,} | {rate:,.0f} v/s | "
                    f"eta {fmt_secs(eta)}"
                )
                last_report = written

            if written >= count:
                break

    if writer is not None:
        sync_and_advise(writer.fileno())
        writer.close()
        if filled > 0:
            shards.append({"path": path, "count": filled, "start": shard_start})

    meta = {
        "dim": dim,
        "dtype": "float32",
        "count": written,
        "shard_size": shard_size,
    }
    (out_dir / f"msmarco-passages-{count}.meta.json").write_text(json.dumps(meta))

    print(f"[done] wrote {written:,} vectors across {len(shards)} shards")

    build_gt_sharded(
        shards=shards,
        total_count=written,
        dim=dim,
        gt_path=out_dir / f"msmarco-passages-{count}.gt.jsonl",
        q_count=q_count,
        topk=topk,
    )


def download_msmarco(count: int, data_dir: Path) -> Path:
    """Download MSMARCO v2.1 parquet parts and convert to shard files."""
    size_label = f"{count // 1_000_000}M"
    out_dir = data_dir / f"MSMARCO-{size_label}"
    meta_path = out_dir / f"msmarco-passages-{count}.meta.json"

    ensure_clean_dir(out_dir, f"MSMARCO {size_label}")

    hf_home = Path(os.environ.get("HF_HOME", str(data_dir / "hf_cache")))
    hf_home.mkdir(parents=True, exist_ok=True)
    local_dir = hf_home / "datasets" / "Cohere___msmarco-v2.1-embed-english-v3"

    if shutil.which("hf") is None:
        raise RuntimeError(
            "Missing Hugging Face CLI. Install with: uv pip install huggingface_hub"
        )

    print("[DOWNLOAD] MSMARCO v2.1 parquet parts (Hugging Face)")
    subprocess.run(
        [
            "hf",
            "download",
            "Cohere/msmarco-v2.1-embed-english-v3",
            "--repo-type",
            "dataset",
            "--include",
            "passages_parquet/*",
            "--local-dir",
            str(local_dir),
        ],
        check=True,
    )

    parquet_glob = (local_dir / "passages_parquet" / "*.parquet").as_posix()
    out_dir.mkdir(parents=True, exist_ok=True)

    print("[CONVERT] Converting MSMARCO parquet to shard files")
    convert_msmarco_parquet_to_shards(
        parquet_glob=parquet_glob,
        out_dir=out_dir,
        count=count,
    )

    print(f"[OK] MSMARCO ready at: {out_dir}")
    return out_dir


def verify_csv_nulls(extract_dir, dataset_type="movielens", sample_size=None):
    """
    Verify NULL injection in CSV files.

    Args:
        extract_dir: Path to extracted dataset directory
        dataset_type: 'movielens' (only type supported currently)
        sample_size: If provided, only check first N rows (for speed)

    Returns:
        dict: Verification results with counts and examples
    """
    import csv

    verification_start = time.time()
    results = {}

    if dataset_type == "movielens":
        csv_files = {
            "movies.csv": ["genres", "title"],
            "ratings.csv": ["timestamp", "rating"],
            "links.csv": ["imdbId", "tmdbId"],
            "tags.csv": ["tag", "timestamp"],
        }

        for filename, nullable_fields in csv_files.items():
            csv_path = extract_dir / filename
            if not csv_path.exists():
                continue

            file_start = time.time()

            # Stream processing for large files
            null_counts = {field: 0 for field in nullable_fields}
            examples = {}
            total_rows = 0

            with open(csv_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)

                # Progress bar
                iterator = reader
                if tqdm and sample_size:
                    iterator = tqdm(
                        reader,
                        total=sample_size,
                        desc=f"   Verifying {filename}",
                        unit=" rows",
                    )

                for row in iterator:
                    total_rows += 1

                    # Check for NULLs
                    for field in nullable_fields:
                        if row[field] == "":
                            null_counts[field] += 1
                            # Capture first example
                            if field not in examples:
                                examples[field] = {k: v for k, v in row.items()}

                    # Early exit for sampling
                    if sample_size and total_rows >= sample_size:
                        break

            file_elapsed = time.time() - file_start

            file_results = {
                "total_rows": total_rows,
                "null_counts": null_counts,
                "examples": examples,
                "sampled": sample_size is not None and total_rows >= sample_size,
                "time": file_elapsed,
            }

            results[filename] = file_results

    verification_elapsed = time.time() - verification_start
    print(f"\n[TIME]  CSV verification time: {verification_elapsed:.2f}s")
    return results


def verify_xml_nulls(extract_dir, sample_size=None):
    """
    Verify XML file structure.

    Checks:
    1. Counts records
    2. Counts attributes
    3. Identifies natural NULL values (empty attributes)

    Args:
        extract_dir: Path to extracted dataset directory
        sample_size: If provided, only check first N records (for speed)

    Returns:
        dict: Verification results
    """
    import xml.etree.ElementTree as ET

    verification_start = time.time()
    results = {}

    xml_files = [
        "Posts.xml",
        "Users.xml",
        "Comments.xml",
        "Tags.xml",
        "Badges.xml",
        "PostLinks.xml",
        "PostHistory.xml",
        "Votes.xml",
    ]

    for filename in xml_files:
        xml_path = extract_dir / filename
        if not xml_path.exists():
            continue

        file_start = time.time()

        # Parse XML iteratively for large files
        context = ET.iterparse(xml_path, events=("start", "end"))
        _, root = next(context)  # Get root element

        all_attrs = set()
        null_counts = {}
        example_with_nulls = None
        total_rows = 0

        # Progress bar
        iterator = context
        if tqdm and sample_size:
            iterator = tqdm(
                context,
                total=sample_size,
                desc=f"   Verifying {filename}",
                unit=" rows",
            )

        for event, elem in iterator:
            if event == "end" and elem.tag == "row":
                total_rows += 1

                # Collect all attributes
                all_attrs.update(elem.attrib.keys())

                # Count NULLs (empty string attributes)
                for key, value in elem.attrib.items():
                    if value == "":
                        null_counts[key] = null_counts.get(key, 0) + 1

                # Get example with NULLs
                if example_with_nulls is None:
                    nulls = [k for k, v in elem.attrib.items() if v == ""]
                    if nulls:
                        example_with_nulls = {
                            "total_attrs": len(elem.attrib),
                            "null_attrs": nulls[:5],
                            "null_count": len(nulls),
                        }

                # Clear element to save memory
                elem.clear()
                root.clear()

                # Early exit for sampling
                if sample_size and total_rows >= sample_size:
                    break

        if total_rows == 0:
            continue

        file_elapsed = time.time() - file_start

        results[filename] = {
            "total_rows": total_rows,
            "total_attributes": len(all_attrs),
            "null_counts": null_counts,
            "example_with_nulls": example_with_nulls,
            "sampled": sample_size is not None and total_rows >= sample_size,
            "time": file_elapsed,
        }

    verification_elapsed = time.time() - verification_start
    print(f"\n[TIME]  XML verification time: {verification_elapsed:.2f}s")
    return results


def _count_xml_rows_fast(xml_path: Path) -> int:
    """Count Stack Exchange <row .../> entries using a fast line scan."""
    row_count = 0
    with xml_path.open("r", encoding="utf-8", errors="ignore") as fin:
        for line in fin:
            if line.lstrip().startswith("<row "):
                row_count += 1
    return row_count


def emit_stackoverflow_entity_counts(extract_dir: Path) -> dict[str, int]:
    """Print markdown-friendly Stack Overflow entity counts and return them."""
    mapping = [
        ("Users.xml", "User"),
        ("Posts.xml", "Post"),
        ("Comments.xml", "Comment"),
        ("Badges.xml", "Badge"),
        ("Votes.xml", "Vote"),
        ("PostLinks.xml", "PostLink"),
        ("Tags.xml", "Tag"),
        ("PostHistory.xml", "PostHistory"),
    ]

    print()
    print("[COUNTS] Stack Overflow entity counts")
    print("   (copy-friendly for markdown)")

    counts: dict[str, int] = {}
    total = 0
    for filename, label in mapping:
        xml_path = extract_dir / filename
        if not xml_path.exists():
            continue
        count = _count_xml_rows_fast(xml_path)
        counts[label] = count
        total += count
        print(f"- {label}: {count:,}")

    counts["Total"] = total
    print(f"- Total: {total:,}")
    print()
    return counts


def print_verification_report(csv_results, xml_results, inject_nulls):
    """Print verification report."""
    print()
    print("=" * 70)
    print("[STATS] Dataset Verification Report")
    print("=" * 70)
    print()

    # CSV verification (MovieLens)
    if csv_results:
        if inject_nulls:
            print("[OK] NULL injection was ENABLED")
            print()

        print("CSV Files:")
        for filename, data in csv_results.items():
            sampled_note = " (sampled)" if data.get("sampled") else ""
            print(f"  [FILE] {filename}{sampled_note}:")
            print(f"     Total rows: {data['total_rows']}")
            for field, count in data["null_counts"].items():
                pct = (count / data["total_rows"]) * 100
                status = "[OK]" if count > 0 else "[ERROR]"
                print(f"     {status} NULL {field}: {count} ({pct:.1f}%)")
        print()

    # XML verification (Stack Exchange)
    if xml_results:
        print("XML Files:")
        print()
        print("  [STATS] Stack Exchange data (original, unmodified)")
        print()
        for filename, data in xml_results.items():
            sampled_note = " (sampled)" if data.get("sampled") else ""
            print(f"  [FILE] {filename}{sampled_note}:")
            print(f"     Total rows: {data['total_rows']}")
            print(f"     Unique attributes: {data['total_attributes']}")
            if data["null_counts"]:
                print('     Empty attributes (attribute=""):')
                for field, count in sorted(data["null_counts"].items())[:5]:
                    pct = (count / data["total_rows"]) * 100
                    print(f"       - {field}: {count} ({pct:.1f}%)")
                if len(data["null_counts"]) > 5:
                    print(
                        f"       ... and {len(data['null_counts']) - 5} " f"more fields"
                    )
        print()

    print("=" * 70)
    print("[OK] Verification Complete")
    print("=" * 70)


def main():
    """Main function."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description="Download datasets for ArcadeDB examples",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Available datasets:

MovieLens (movie ratings):
    movielens-small  - ~1 MB, ~100K ratings, 9K movies, 600 users
    movielens-large  - ~265 MB, ~33M ratings, 86K movies, 280K users

Stack Exchange (Q&A posts, pinned to 2024-06-30):
    stackoverflow-tiny    - ~34 MB, ~100K rows (subset of stackoverflow-small)
    stackoverflow-small   - ~80 MB, ~80K posts (cs.stackexchange.com)
    stackoverflow-medium  - ~500 MB, ~300K posts (stats.stackexchange.com)
    stackoverflow-large   - ~10 GB, subset of stackoverflow-full
    stackoverflow-xlarge  - ~50 GB, subset of stackoverflow-full
    stackoverflow-full    - ~323 GB, full stackoverflow.com

TPC-H (table benchmark):
    tpch-sf1    - Scale factor 1 (generated locally via dbgen)
    tpch-sf10   - Scale factor 10 (generated locally via dbgen)
    tpch-sf100  - Scale factor 100 (generated locally via dbgen)

LDBC SNB Interactive v1 (graph benchmark):
    ldbc-snb-sf1   - Scale factor 1 (CsvMergeForeign, LongDateFormatter)
    ldbc-snb-sf10  - Scale factor 10 (CsvMergeForeign, LongDateFormatter)
    ldbc-snb-sf100 - Scale factor 100 (CsvMergeForeign, LongDateFormatter)

MSMARCO v2.1 embeddings (vector benchmark):
    msmarco-1m  - 1M passage vectors
    msmarco-5m  - 5M passage vectors
    msmarco-10m - 10M passage vectors

Examples:
    python download_data.py movielens-small
    python download_data.py movielens-large
    python download_data.py movielens-small --no-nulls  # Skip NULL injection
    python download_data.py stackoverflow-tiny
    python download_data.py stackoverflow-small
    python download_data.py stackoverflow-medium
    python download_data.py stackoverflow-large
    python download_data.py stackoverflow-xlarge
    python download_data.py stackoverflow-full
    python download_data.py stackoverflow-small --verify-only  # Verify existing
    python download_data.py msmarco-1m
    python download_data.py msmarco-5m
    python download_data.py msmarco-10m
    python download_data.py tpch-sf1
    python download_data.py tpch-sf10
    python download_data.py tpch-sf100
    python download_data.py ldbc-snb-sf1
    python download_data.py ldbc-snb-sf10
    python download_data.py ldbc-snb-sf100

Note: Stack Exchange datasets require py7zr library:
    uv pip install py7zr

NULL Handling:
    MovieLens (CSV): NULL injection enabled by default (use --no-nulls to skip)
    - Injects empty strings "" in nullable fields (2-8% of values)
    - Makes synthetic data more realistic for testing

Stack Exchange (XML): Original data (no modification)
    - Data downloaded and extracted as-is from archive.org
    - stackoverflow-tiny is built locally from stackoverflow-small

Use --verify-only to verify existing datasets without re-downloading.

Note: Verification uses smart sampling (100K rows) for fast performance.
        """,
    )
    parser.add_argument(
        "dataset",
        choices=[
            "movielens-small",
            "movielens-large",
            "stackoverflow-tiny",
            "stackoverflow-small",
            "stackoverflow-medium",
            "stackoverflow-large",
            "stackoverflow-xlarge",
            "stackoverflow-full",
            "msmarco-1m",
            "msmarco-5m",
            "msmarco-10m",
            "tpch-sf1",
            "tpch-sf10",
            "tpch-sf100",
            "ldbc-snb-sf1",
            "ldbc-snb-sf10",
            "ldbc-snb-sf100",
        ],
        help="Dataset to download",
    )
    parser.add_argument(
        "--no-nulls",
        action="store_true",
        help="Skip NULL injection for MovieLens CSV files",
    )
    parser.add_argument(
        "--verify-only",
        action="store_true",
        help="Only verify existing dataset (skip download)",
    )
    parser.add_argument(
        "--no-vectors",
        action="store_true",
        help="Skip Stack Overflow vector generation",
    )
    parser.add_argument(
        "--vector-model",
        type=str,
        default="all-MiniLM-L6-v2",
        help="Embedding model for Stack Overflow vectors (default: all-MiniLM-L6-v2)",
    )
    parser.add_argument(
        "--vector-batch-size",
        type=int,
        default=256,
        help="Embedding batch size for vector generation (default: 256)",
    )
    parser.add_argument(
        "--vector-shard-size",
        type=int,
        default=100_000,
        help="Vectors per shard file (default: 100000)",
    )
    parser.add_argument(
        "--vector-max-rows",
        type=int,
        default=None,
        help="Optional max vectors per corpus (questions/answers/comments)",
    )
    parser.add_argument(
        "--vector-gt-queries",
        type=int,
        default=1000,
        help="Number of sampled queries for Stack Overflow GT (default: 1000)",
    )
    parser.add_argument(
        "--vector-gt-topk",
        type=int,
        default=50,
        help="Top-k neighbors per sampled query for Stack Overflow GT (default: 50)",
    )
    args = parser.parse_args()

    print("=" * 70)
    print(
        "[DOWNLOAD] Dataset Download"
        if not args.verify_only
        else "[STATS] Dataset Verification"
    )
    print("=" * 70)
    print()

    # Determine dataset directory
    data_dir = Path(__file__).parent / "data"

    if args.dataset.startswith("tpch-"):
        scale = int(args.dataset.replace("tpch-sf", ""))
        if scale not in {1, 10, 100}:
            raise ValueError("Unsupported TPC-H scale factor. Use 1, 10, or 100.")
        out_dir = data_dir / f"tpch-sf{scale}"
        marker = out_dir / "customer.tbl"

        if args.verify_only:
            if marker.exists():
                print(f"[OK] TPC-H dataset exists at: {out_dir}")
            else:
                print(f"[ERROR] TPC-H dataset not found: {out_dir}")
            return

        download_tpch(scale_factor=scale)
        return

    if args.dataset.startswith("ldbc-snb-"):
        scale = int(args.dataset.replace("ldbc-snb-sf", ""))
        out_dir = data_dir / f"ldbc-snb-sf{scale}"
        marker = out_dir / ".ldbc_snb_ok"

        if args.verify_only:
            if marker.exists():
                print(f"[OK] LDBC SNB dataset exists at: {out_dir}")
            else:
                print(f"[ERROR] LDBC SNB dataset not found: {out_dir}")
            return

        download_ldbc_snb(scale_factor=scale)
        return

    if args.dataset.startswith("msmarco-"):
        counts = {
            "msmarco-1m": 1_000_000,
            "msmarco-5m": 5_000_000,
            "msmarco-10m": 10_000_000,
        }
        count = counts[args.dataset]
        size_label = f"{count // 1_000_000}M"
        out_dir = data_dir / f"MSMARCO-{size_label}"
        meta_path = out_dir / f"msmarco-passages-{count}.meta.json"

        if args.verify_only:
            if meta_path.exists():
                print(f"[OK] MSMARCO dataset exists at: {out_dir}")
            else:
                print(f"[ERROR] MSMARCO dataset not found: {out_dir}")
            return

        download_msmarco(count=count, data_dir=data_dir)
        return
    if args.dataset.startswith("movielens-"):
        size = args.dataset.replace("movielens-", "")
        dirname = f"movielens-{size}"
    else:
        size = args.dataset.replace("stackoverflow-", "")
        dirname = f"stackoverflow-{size}"

    extract_dir = data_dir / dirname

    # Verify-only mode
    if args.verify_only:
        if not extract_dir.exists():
            print(f"[ERROR] Dataset not found: {extract_dir}")
            print("   Run without --verify-only to download first.")
            return

        print(f"[DIR] Verifying existing dataset: {extract_dir}")
        print("[FAST] Using smart sampling (100K rows) for fast verification")
        print()

        sample_size = 100000

        if args.dataset.startswith("movielens-"):
            csv_results = verify_csv_nulls(
                extract_dir, dataset_type="movielens", sample_size=sample_size
            )
            print_verification_report(
                csv_results, {}, inject_nulls=True  # Assume NULLs exist
            )
        else:
            xml_results = verify_xml_nulls(extract_dir, sample_size=sample_size)
            print_verification_report(
                {}, xml_results, inject_nulls=True  # Assume NULLs exist
            )
            emit_stackoverflow_entity_counts(extract_dir)
        return

    # Download requested dataset
    if args.dataset.startswith("movielens-"):
        size = args.dataset.replace("movielens-", "")
        extract_dir = download_movielens(size=size, inject_nulls=not args.no_nulls)

        print()
        print("=" * 70)
        print("[OK] MovieLens Dataset Ready!")
        print("=" * 70)
        print()
        print("[INFO] Use this dataset in examples:")
        print(f"   data_dir = Path('{extract_dir}')")
        print("   movies_csv = data_dir / 'movies.csv'")
        print("   ratings_csv = data_dir / 'ratings.csv'")
        print()
        print("[INFO] Dataset info:")
        if size == "large":
            print("   - ~86,000 movies")
            print("   - ~33,000,000 ratings")
            print("   - ~280,000 users")
        else:
            print("   - ~9,000 movies")
            print("   - ~100,000 ratings")
            print("   - ~600 users")
        print("   - Licensed for educational use")
        print()

        # Run verification with smart sampling
        sample_size = 100000 if size == "large" else None
        csv_results = verify_csv_nulls(
            extract_dir, dataset_type="movielens", sample_size=sample_size
        )
        print_verification_report(csv_results, {}, inject_nulls=not args.no_nulls)

    elif args.dataset.startswith("stackoverflow-"):
        size = args.dataset.replace("stackoverflow-", "")
        extract_dir = download_stackoverflow(size=size)

        print()
        print("=" * 70)
        print("[OK] Stack Exchange Dataset Ready!")
        print("=" * 70)
        print()
        print("[INFO] Use this dataset in examples:")
        print(f"   data_dir = Path('{extract_dir}')")
        print("   posts_xml = data_dir / 'Posts.xml'")
        print("   users_xml = data_dir / 'Users.xml'")
        print()
        print("[INFO] Dataset info:")
        if size == "tiny":
            print("   - Site: cs.stackexchange.com")
            print("   - ~10,000 rows per XML (Tags.xml full)")
            print("   - ~100,000 total rows")
        elif size == "small":
            print("   - Site: cs.stackexchange.com")
            print("   - ~80,000 posts (questions + answers)")
            print("   - ~50,000 users")
        elif size == "medium":
            print("   - Site: stats.stackexchange.com")
            print("   - ~300,000 posts (questions + answers)")
            print("   - ~150,000 users")
        elif size == "large":
            print("   - Site: stackoverflow.com")
            print("   - ~10 GB subset of full stackoverflow.com dump")
        else:
            print("   - Site: stackoverflow.com")
            print("   - ~20,000,000 posts (questions + answers)")
            print("   - ~10,000,000 users")
        print("   - Date: 2024-06-30 (pinned for reproducibility)")
        print("   - License: CC BY-SA")
        print()

        emit_stackoverflow_entity_counts(extract_dir)

        if not args.no_vectors:
            embed_stackoverflow_vectors(
                extract_dir=extract_dir,
                dataset_name=args.dataset,
                model_name=args.vector_model,
                batch_size=args.vector_batch_size,
                shard_size=args.vector_shard_size,
                max_rows=args.vector_max_rows,
                gt_queries=args.vector_gt_queries,
                gt_topk=args.vector_gt_topk,
            )
            print()

        # Run verification with smart sampling
        sample_size = 100000 if size in ["medium", "large", "xlarge", "full"] else None
        xml_results = verify_xml_nulls(extract_dir, sample_size=sample_size)
        print_verification_report({}, xml_results, inject_nulls=not args.no_nulls)

    print()


if __name__ == "__main__":
    main()
