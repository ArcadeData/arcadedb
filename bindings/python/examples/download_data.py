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

Available datasets:
1. MovieLens (movie ratings, tags, genres):
   - movielens-small: ~1 MB, ~100K ratings, 9K movies, 600 users
   - movielens-large: ~265 MB, ~33M ratings, 86K movies, 280K users

2. Stack Exchange (Q&A posts, users, tags, links):
   - stackoverflow-small: ~642 MB, 1.41M records (cs.stackexchange.com, 2024-06-30)
   - stackoverflow-medium: ~2.9 GB, 5.56M records (stats.stackexchange.com, 2024-06-30)
   - stackoverflow-large: ~323 GB, records (full stackoverflow.com, 2024-06-30)


Stack Exchange Data Note:
- Pinned to 2024-06-30 quarterly dump for reproducibility
- Downloaded from archive.org
- 7z compressed format (requires py7zr library)

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

Usage:
    python download_data.py movielens-small
    python download_data.py movielens-large
    python download_data.py stackoverflow-small
    python download_data.py stackoverflow-medium
    python download_data.py stackoverflow-large
    python download_data.py movielens-small --no-nulls  # Skip NULL injection
    python download_data.py stackoverflow-small --verify-only  # Verify existing
"""

import argparse
import shutil
import time
import urllib.request
import zipfile
from pathlib import Path

try:
    from tqdm import tqdm
except ImportError:
    # Fallback if tqdm not installed
    tqdm = None


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
    if extract_dir.exists():
        print(f"[OK] Dataset already exists at: {extract_dir}")
        print(f"   Size: {config['description']} ({config['size_mb']})")
        print()
        for csv_file in ["movies.csv", "ratings.csv", "tags.csv", "links.csv"]:
            file_path = extract_dir / csv_file
            if file_path.exists():
                size_mb = file_path.stat().st_size / (1024 * 1024)
                print(f"   - {csv_file}: {size_mb:.1f} MB")

        # Ask if user wants to re-introduce NULL values
        print(
            "\n[INFO] To re-introduce NULL values, delete the data directory and re-run."
        )
        return extract_dir

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
        size: 'small' (~80 MB), 'medium' (~500 MB), or 'large' (~5 GB)
    """
    try:
        import py7zr
    except ImportError:
        print("[ERROR] Missing dependency: py7zr")
        print("   Install with: uv pip install py7zr")
        raise

    # Create data directory
    data_dir = Path(__file__).parent / "data"
    data_dir.mkdir(exist_ok=True)

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
        "large": {
            # Full Stack Overflow dump now split into multiple parts
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
            "dirname": "stackoverflow-large",
            "site": "stackoverflow.com",
            "description": "Full Stack Overflow dump (all XML files)",
            "size_mb": "~5–6 GB",
            "date": "2024-06-30",
        },
    }

    if size not in datasets:
        raise ValueError(
            f"Unknown dataset size: {size}. Choose 'small', 'medium', or 'large'"
        )

    config = datasets[size]
    dirname = config["dirname"]
    extract_dir = data_dir / dirname

    # Check if already downloaded
    if extract_dir.exists():
        print(f"[OK] Dataset already exists at: {extract_dir}")
        print(f"   Site: {config['site']}")
        print(f"   Size: {config['description']} ({config['size_mb']})")
        print(f"   Date: {config['date']}")
        print()

        # Show extracted files
        xml_files = list(extract_dir.glob("*.xml"))
        if xml_files:
            print("   Files:")
            for xml_file in sorted(xml_files):
                size_mb = xml_file.stat().st_size / (1024 * 1024)
                print(f"   - {xml_file.name}: {size_mb:.1f} MB")

        print("\n[INFO] To re-download, delete the data directory and re-run.")
        return extract_dir

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
  stackoverflow-small   - ~80 MB, ~80K posts (cs.stackexchange.com)
  stackoverflow-medium  - ~500 MB, ~300K posts (stats.stackexchange.com)
  stackoverflow-large   - ~5 GB, full Posts.xml (stackoverflow.com)

Examples:
  python download_data.py movielens-small
  python download_data.py movielens-large
  python download_data.py stackoverflow-small
  python download_data.py stackoverflow-medium
  python download_data.py stackoverflow-large
  python download_data.py movielens-small --no-nulls  # Skip NULL injection
  python download_data.py stackoverflow-small --verify-only  # Verify existing

Note: Stack Exchange datasets require py7zr library:
    uv pip install py7zr

NULL Handling:
  MovieLens (CSV): NULL injection enabled by default (use --no-nulls to skip)
    - Injects empty strings "" in nullable fields (2-8% of values)
    - Makes synthetic data more realistic for testing

  Stack Exchange (XML): Original data (no modification)
    - Data downloaded and extracted as-is from archive.org

Use --verify-only to verify existing datasets without re-downloading.

Note: Verification uses smart sampling (100K rows) for fast performance.
        """,
    )
    parser.add_argument(
        "dataset",
        choices=[
            "movielens-small",
            "movielens-large",
            "stackoverflow-small",
            "stackoverflow-medium",
            "stackoverflow-large",
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
        if size == "small":
            print("   - Site: cs.stackexchange.com")
            print("   - ~80,000 posts (questions + answers)")
            print("   - ~50,000 users")
        elif size == "medium":
            print("   - Site: stats.stackexchange.com")
            print("   - ~300,000 posts (questions + answers)")
            print("   - ~150,000 users")
        else:
            print("   - Site: stackoverflow.com")
            print("   - ~20,000,000 posts (questions + answers)")
            print("   - ~10,000,000 users")
        print("   - Date: 2024-06-30 (pinned for reproducibility)")
        print("   - License: CC BY-SA")
        print()

        # Run verification with smart sampling
        sample_size = 100000 if size in ["medium", "large"] else None
        xml_results = verify_xml_nulls(extract_dir, sample_size=sample_size)
        print_verification_report({}, xml_results, inject_nulls=not args.no_nulls)

    print()


if __name__ == "__main__":
    main()
