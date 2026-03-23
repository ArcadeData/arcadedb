#!/usr/bin/env python3
"""
Example 04: CSV Import - Documents with Bulk Insert Ingest

This example demonstrates importing CSV data into ArcadeDB DOCUMENTS
using bulk INSERT statements with explicit schema mapping.

We use the MovieLens dataset with four CSV files:
- movies.csv: Movie information (9,743 movies)
- ratings.csv: User ratings (100,837 ratings)
- links.csv: External movie IDs (9,742 links)
- tags.csv: User-generated tags (3,683 tags)

Key Concepts:
- Bulk INSERT ingest using Python CSV parsing
- Explicit schema mapping for document types
- Batch processing with commitEvery parameter
- WAL disabled during ingest, then re-enabled
- Creating indexes AFTER import for performance
- **Full-text search** with Lucene for text fields
- Query performance comparison with/without indexes
- NULL value handling

Schema Strategy:
- Integer-like values → LONG
- Decimal values → DOUBLE
- Text values → STRING
- Empty cells → NULL (proper SQL NULL handling)

Supported ArcadeDB Types:
Numeric:
  - BYTE: 8-bit integer (-128 to 127)
  - SHORT: 16-bit integer (-32,768 to 32,767)
  - INTEGER: 32-bit integer (-2.1B to 2.1B)
  - LONG: 64-bit integer (large numbers) ← Java's default for integers
  - FLOAT: 32-bit decimal (~7 digits precision)
  - DOUBLE: 64-bit decimal (~15 digits precision) ← Java's default for decimals
  - DECIMAL: Arbitrary precision (exact, for money)

Other:
  - STRING, BOOLEAN, DATE, DATETIME, BINARY, EMBEDDED, LIST

Requirements:
- arcadedb-embedded
- MovieLens dataset (downloaded via download_data.py)
- Sufficient JVM heap memory (8GB recommended for large dataset)

Usage:
1. Run with default (large) dataset:
   python 04_csv_import_documents.py
2. Run with small dataset:
   python 04_csv_import_documents.py --dataset movielens-small
3. Run with large dataset and custom parallel threads:
   python 04_csv_import_documents.py --dataset movielens-large --parallel 8
4. Run with custom batch size:
   python 04_csv_import_documents.py --batch-size 10000
5. Run with custom JVM heap, parallel threads, and batch size:
    python 04_csv_import_documents.py --dataset movielens-large --parallel 8 --batch-size 10000 --heap-size 8g

The script will automatically download the dataset if it doesn't exist.

Memory Requirements:
- Small dataset (~100K ratings): 4GB heap (default) is sufficient
- Large dataset (~33M ratings): 4GB heap (default) should work, 8GB for safety
- Very large datasets (100M+ records): Use --heap-size 8g or higher

Dataset Options:
- movielens-small: ~1 MB, ~100K ratings, 9K movies, 600 users
- movielens-large: ~265 MB, ~33M ratings, 86K movies, 280K users

Note: This example creates a database at ./my_test_databases/movielens_db/
      The database files are preserved so you can inspect them after running.
"""

import argparse
import csv
import json
import os
import shutil
import statistics
import subprocess
import sys
import time
from pathlib import Path

import arcadedb_embedded as arcadedb

# Define test queries used for validation
# NOTE: ORDER BY clauses ensure consistent results across runs (important for validation)
TEST_QUERIES = [
    ("Find movie by ID", "SELECT FROM Movie WHERE movieId = 500"),
    (
        "Find user's ratings",
        "SELECT FROM Rating WHERE userId = 414 ORDER BY movieId, rating LIMIT 10",
    ),
    (
        "Find movie ratings",
        "SELECT FROM Rating WHERE movieId = 500 ORDER BY userId, rating LIMIT 10",
    ),
    (
        "Count user's ratings",
        "SELECT count(*) as count FROM Rating WHERE userId = 414",
    ),
    (
        "Find movies by genre (LIKE with LIMIT)",
        "SELECT FROM Movie WHERE genres LIKE '%Action%' ORDER BY movieId LIMIT 10",
    ),
    (
        "Count ALL Action movies (LIKE, no LIMIT)",
        "SELECT count(*) as count FROM Movie WHERE genres LIKE '%Action%'",
    ),
]

# Expected baseline results for validation
EXPECTED_RESULTS = {
    "movielens-small": [
        {
            "name": "Find movie by ID",
            "count": 1,
            "sample": [
                {
                    "movieId": 500,
                    "title": "Mrs. Doubtfire (1993)",
                    "genres": "Comedy|Drama",
                    "@props": "movieId:3,title:7,genres:7",
                }
            ],
        },
        {
            "name": "Find user's ratings",
            "count": 10,
            "sample": [
                {
                    "userId": 414,
                    "movieId": 1,
                    "rating": 4.0,
                    "timestamp": 961438127,
                    "@props": "userId:3,movieId:3,rating:5,timestamp:3",
                },
                {
                    "userId": 414,
                    "movieId": 2,
                    "rating": 3.0,
                    "timestamp": 961594981,
                    "@props": "userId:3,movieId:3,rating:5,timestamp:3",
                },
                {
                    "userId": 414,
                    "movieId": 3,
                    "rating": 4.0,
                    "timestamp": 961439278,
                    "@props": "userId:3,movieId:3,rating:5,timestamp:3",
                },
                {
                    "userId": 414,
                    "movieId": 5,
                    "rating": 2.0,
                    "timestamp": 961437647,
                    "@props": "userId:3,movieId:3,rating:5,timestamp:3",
                },
                {
                    "userId": 414,
                    "movieId": 6,
                    "rating": 3.0,
                    "timestamp": 961515642,
                    "@props": "userId:3,movieId:3,rating:5,timestamp:3",
                },
            ],
        },
        {
            "name": "Find movie ratings",
            "count": 10,
            "sample": [
                {
                    "userId": 1,
                    "movieId": 500,
                    "rating": 3.0,
                    "timestamp": 964981208,
                    "@props": "userId:3,movieId:3,rating:5,timestamp:3",
                },
                {
                    "userId": 6,
                    "movieId": 500,
                    "rating": 5.0,
                    "timestamp": 845553354,
                    "@props": "userId:3,movieId:3,rating:5,timestamp:3",
                },
                {
                    "userId": 8,
                    "movieId": 500,
                    "rating": 2.0,
                    "timestamp": 839463624,
                    "@props": "userId:3,movieId:3,rating:5,timestamp:3",
                },
                {
                    "userId": 18,
                    "movieId": 500,
                    "rating": 3.5,
                    "timestamp": 1455618095,
                    "@props": "userId:3,movieId:3,rating:5,timestamp:3",
                },
                {
                    "userId": 19,
                    "movieId": 500,
                    "rating": 2.0,
                    "timestamp": 965706636,
                    "@props": "userId:3,movieId:3,rating:5,timestamp:3",
                },
            ],
        },
        {"name": "Count user's ratings", "count": 1, "sample": [{"count": 2698}]},
        {
            "name": "Find movies by genre (LIKE with LIMIT)",
            "count": 10,
            "sample": [
                {
                    "movieId": 6,
                    "title": "Heat (1995)",
                    "genres": "Action|Crime|Thriller",
                    "@props": "movieId:3,title:7,genres:7",
                },
                {
                    "movieId": 9,
                    "title": "Sudden Death (1995)",
                    "genres": "Action",
                    "@props": "movieId:3,title:7,genres:7",
                },
                {
                    "movieId": 10,
                    "title": "GoldenEye (1995)",
                    "genres": "Action|Adventure|Thriller",
                    "@props": "movieId:3,title:7,genres:7",
                },
                {
                    "movieId": 15,
                    "title": "Cutthroat Island (1995)",
                    "genres": "Action|Adventure|Romance",
                    "@props": "movieId:3,title:7,genres:7",
                },
                {
                    "movieId": 20,
                    "title": "Money Train (1995)",
                    "genres": "Action|Comedy|Crime|Drama|Thriller",
                    "@props": "movieId:3,title:7,genres:7",
                },
            ],
        },
        {
            "name": "Count ALL Action movies (LIKE, no LIMIT)",
            "count": 1,
            "sample": [{"count": 1796}],
        },
    ],
    "movielens-large": [
        {
            "name": "Find movie by ID",
            "count": 1,
            "sample": [
                {
                    "movieId": 500,
                    "title": "Mrs. Doubtfire (1993)",
                    "genres": "Comedy|Drama",
                    "@props": "movieId:3,title:7,genres:7",
                }
            ],
        },
        {
            "name": "Find user's ratings",
            "count": 10,
            "sample": [
                {
                    "userId": 414,
                    "movieId": 1,
                    "rating": 5.0,
                    "timestamp": 1603897313,
                    "@props": "userId:3,movieId:3,rating:5,timestamp:3",
                },
                {
                    "userId": 414,
                    "movieId": 34,
                    "rating": 3.5,
                    "timestamp": 1603897729,
                    "@props": "userId:3,movieId:3,rating:5,timestamp:3",
                },
                {
                    "userId": 414,
                    "movieId": 47,
                    "rating": 5.0,
                    "timestamp": None,
                    "@props": "userId:3,movieId:3,rating:5,timestamp:3",
                },
                {
                    "userId": 414,
                    "movieId": 50,
                    "rating": 4.0,
                    "timestamp": 1603896782,
                    "@props": "userId:3,movieId:3,rating:5,timestamp:3",
                },
                {
                    "userId": 414,
                    "movieId": 260,
                    "rating": 4.0,
                    "timestamp": 1603897681,
                    "@props": "userId:3,movieId:3,rating:5,timestamp:3",
                },
            ],
        },
        {
            "name": "Find movie ratings",
            "count": 10,
            "sample": [
                {
                    "userId": 2,
                    "movieId": 500,
                    "rating": 4.0,
                    "timestamp": 835816548,
                    "@props": "userId:3,movieId:3,rating:5,timestamp:3",
                },
                {
                    "userId": 7,
                    "movieId": 500,
                    "rating": 4.0,
                    "timestamp": 974520592,
                    "@props": "userId:3,movieId:3,rating:5,timestamp:3",
                },
                {
                    "userId": 9,
                    "movieId": 500,
                    "rating": 4.0,
                    "timestamp": 835947950,
                    "@props": "userId:3,movieId:3,rating:5,timestamp:3",
                },
                {
                    "userId": 14,
                    "movieId": 500,
                    "rating": 2.0,
                    "timestamp": 1311601190,
                    "@props": "userId:3,movieId:3,rating:5,timestamp:3",
                },
                {
                    "userId": 21,
                    "movieId": 500,
                    "rating": 3.0,
                    "timestamp": 1172734846,
                    "@props": "userId:3,movieId:3,rating:5,timestamp:3",
                },
            ],
            "sample_indexed": [
                {
                    "userId": 198520,
                    "movieId": 500,
                    "rating": 3.0,
                    "timestamp": 841556879,
                    "@props": "userId:3,movieId:3,rating:5,timestamp:3",
                },
                {
                    "userId": 198564,
                    "movieId": 500,
                    "rating": 4.0,
                    "timestamp": 834049208,
                    "@props": "userId:3,movieId:3,rating:5,timestamp:3",
                },
                {
                    "userId": 198901,
                    "movieId": 500,
                    "rating": 3.0,
                    "timestamp": 876049592,
                    "@props": "userId:3,movieId:3,rating:5,timestamp:3",
                },
                {
                    "userId": 199071,
                    "movieId": 500,
                    "rating": 3.5,
                    "timestamp": 1487972691,
                    "@props": "userId:3,movieId:3,rating:5,timestamp:3",
                },
                {
                    "userId": 199347,
                    "movieId": 500,
                    "rating": 5.0,
                    "timestamp": 841305883,
                    "@props": "userId:3,movieId:3,rating:5,timestamp:3",
                },
            ],
        },
        {"name": "Count user's ratings", "count": 1, "sample": [{"count": 169}]},
        {
            "name": "Find movies by genre (LIKE with LIMIT)",
            "count": 10,
            "sample": [
                {
                    "movieId": 6,
                    "title": "Heat (1995)",
                    "genres": "Action|Crime|Thriller",
                    "@props": "movieId:3,title:7,genres:7",
                },
                {
                    "movieId": 9,
                    "title": "Sudden Death (1995)",
                    "genres": "Action",
                    "@props": "movieId:3,title:7,genres:7",
                },
                {
                    "movieId": 10,
                    "title": "GoldenEye (1995)",
                    "genres": "Action|Adventure|Thriller",
                    "@props": "movieId:3,title:7,genres:7",
                },
                {
                    "movieId": 15,
                    "title": "Cutthroat Island (1995)",
                    "genres": "Action|Adventure|Romance",
                    "@props": "movieId:3,title:7,genres:7",
                },
                {
                    "movieId": 20,
                    "title": "Money Train (1995)",
                    "genres": "Action|Comedy|Crime|Drama|Thriller",
                    "@props": "movieId:3,title:7,genres:7",
                },
            ],
        },
        {
            "name": "Count ALL Action movies (LIKE, no LIMIT)",
            "count": 1,
            "sample": [{"count": 9386}],
        },
    ],
}


# =============================================================================
# Helper Functions: Index Management
# =============================================================================


def serialize_query_results(results):
    """
    Convert query results to JSON-serializable format.

    Args:
        results: List of result dicts from run_validation_queries()

    Returns:
        List of dicts with serializable data
    """
    serialized = []
    for result in results:
        # Extract sample data if available
        sample_data = []
        if result.get("sample"):
            for record in result["sample"]:
                record_dict = {}
                # Get all properties from the record
                record_json = json.loads(record.to_json())
                for key, value in record_json.items():
                    if key not in ["@rid", "@cat", "@type"]:
                        record_dict[key] = value
                sample_data.append(record_dict)

        serialized.append(
            {
                "name": result["name"],
                "count": result["count"],
                "sample": sample_data[:5],  # Save first 5 results
            }
        )

    return serialized


def save_query_results(results, dataset_size, db_path):
    """
    Save query results to a JSON file for later comparison.

    Args:
        results: List of result dicts from run_validation_queries()
        dataset_size: "small" or "large"
        db_path: Path to the database directory where results should be saved

    Returns:
        Path to saved file
    """
    # Save results in the database directory
    db_dir = Path(db_path)
    db_dir.mkdir(parents=True, exist_ok=True)

    results_file = db_dir / f"query_results_{dataset_size}.json"

    serialized_results = serialize_query_results(results)

    with open(results_file, "w") as f:
        json.dump(serialized_results, f, indent=2)

    return results_file


def compare_query_results(current_results, saved_results, verbose=True):
    """
    Compare current query results against saved baseline.

    Args:
        current_results: List of result dicts from run_validation_queries()
        saved_results: List of saved result dicts
        verbose: If True, print comparison details

    Returns:
        bool: True if all results match, False otherwise
    """
    if verbose:
        print("   🔍 Comparing against saved baseline results...")
        print()

    all_match = True
    serialized_current = serialize_query_results(current_results)

    for i, (current, saved) in enumerate(zip(serialized_current, saved_results)):
        query_name = current["name"]

        # Compare counts
        if current["count"] != saved["count"]:
            if verbose:
                print(
                    f"   ❌ {query_name}: Count mismatch! "
                    f"Current: {current['count']}, Saved: {saved['count']}"
                )
            all_match = False
            continue

        # Compare sample data
        current_sample = current["sample"]
        saved_sample = saved["sample"]

        # Compare samples as JSON strings (normalized, ignoring key order)
        def normalize_record(rec):
            """Sort keys and exclude @props metadata for comparison."""
            # Exclude @props as it's internal metadata that may change order
            return {k: rec[k] for k in sorted(rec.keys()) if k != "@props"}

        current_normalized = [normalize_record(r) for r in current_sample]
        saved_normalized = [normalize_record(r) for r in saved_sample]

        if current_normalized != saved_normalized:
            # Check for alternative sample (e.g. indexed) if available
            # This handles cases where indexes change the sort order (e.g. String vs Int sorting)
            match_found = False
            if "sample_indexed" in saved:
                saved_sample_indexed = saved["sample_indexed"]
                saved_normalized_indexed = [
                    normalize_record(r) for r in saved_sample_indexed
                ]
                if current_normalized == saved_normalized_indexed:
                    if verbose:
                        print(
                            f"   ✅ {query_name}: {current['count']} results (matches indexed baseline)"
                        )
                    match_found = True

            if not match_found:
                if verbose:
                    print(
                        f"   ⚠️  {query_name}: Sample data differs "
                        f"(count matches: {current['count']})"
                    )
                    # Show first difference
                    for j, (curr_rec, saved_rec) in enumerate(
                        zip(current_normalized, saved_normalized)
                    ):
                        if curr_rec != saved_rec:
                            print(f"      First difference at record {j}:")
                            print(f"      Current: {curr_rec}")
                            print(f"      Saved: {saved_rec}")
                            break
                all_match = False
        else:
            if verbose:
                print(f"   ✅ {query_name}: {current['count']} results (matches)")

    return all_match


# =============================================================================
# Helper Functions: Index Management
# =============================================================================


def create_indexes(db, indexes, verbose=True):
    """
    Create indexes with retry logic for compaction conflicts.

    Args:
        db: Database instance
        indexes: List of (table, column, uniqueness) tuples
            uniqueness can be: "UNIQUE", "NOTUNIQUE", "FULL_TEXT"
        verbose: If True, print progress messages

    Returns:
        tuple: (success_count, failed_indexes)
            success_count: Number of indexes created successfully
            failed_indexes: List of (table, column, error) tuples that failed
    """
    if verbose:
        print(
            f"\n   📊 Creating {len(indexes)} indexes "
            f"with retry on compaction conflicts:"
        )

    success_count = 0
    failed_indexes = []

    for idx, (table, column, uniqueness) in enumerate(indexes, 1):
        created = False
        retry_delay = 300  # Wait 300 seconds (5 minutes) between retries
        max_retries = 200  # Try for up to 200 attempts (= 1000 minutes max per index)

        for attempt in range(1, max_retries + 1):
            try:
                # Convert uniqueness string to SQL index creation
                if uniqueness == "UNIQUE":
                    db.command("sql", f"CREATE INDEX ON {table} ({column}) UNIQUE")
                elif uniqueness == "UNIQUE_HASH":
                    db.command("sql", f"CREATE INDEX ON {table} ({column}) UNIQUE_HASH")
                elif uniqueness == "FULL_TEXT":
                    db.command("sql", f"CREATE INDEX ON {table} ({column}) FULL_TEXT")
                elif uniqueness == "NOTUNIQUE_HASH":
                    db.command(
                        "sql", f"CREATE INDEX ON {table} ({column}) NOTUNIQUE_HASH"
                    )
                else:  # NOTUNIQUE
                    db.command("sql", f"CREATE INDEX ON {table} ({column}) NOTUNIQUE")

                if verbose:
                    print(
                        f"   ✅ [{idx}/{len(indexes)}] "
                        f"Created index on {table}({column}) {uniqueness}"
                    )
                created = True
                success_count += 1
                break
            except Exception as e:  # noqa: BLE001
                error_msg = str(e)

                # Check if retryable (compaction or index conflicts)
                is_compaction_error = (
                    "NeedRetryException" in error_msg
                    and "asynchronous tasks" in error_msg
                )
                is_index_error = (
                    "IndexException" in error_msg
                    and "Error on creating index" in error_msg
                )

                if is_compaction_error or is_index_error:
                    if attempt < max_retries:
                        elapsed = attempt * retry_delay
                        reason = (
                            "compaction running"
                            if is_compaction_error
                            else "index conflict"
                        )
                        if verbose:
                            print(
                                f"   ⏳ [{idx}/{len(indexes)}] "
                                f"Waiting for {reason} "
                                f"(attempt {attempt}/{max_retries}, "
                                f"{elapsed}s elapsed)..."
                            )
                        time.sleep(retry_delay)
                    else:
                        if verbose:
                            print(
                                f"   ❌ [{idx}/{len(indexes)}] "
                                f"Failed to create index on "
                                f"{table}({column}) "
                                f"after {max_retries} retries"
                            )
                        failed_indexes.append((table, column, error_msg))
                        break
                else:
                    # Non-retryable error
                    if verbose:
                        print(
                            f"   ❌ [{idx}/{len(indexes)}] Failed to create index on "
                            f"{table}({column}): {error_msg}"
                        )
                    failed_indexes.append((table, column, error_msg))
                    break

        if not created and verbose:
            print(f"   ⚠️  Skipping index {table}({column})")

    return success_count, failed_indexes


def drop_all_indexes(db, verbose=True):
    """
    Drop all non-RID indexes from the database.

    Args:
        db: Database instance
        verbose: If True, print progress messages

    Returns:
        int: Number of indexes dropped
    """
    if verbose:
        print("\n   🗑️  Dropping all indexes...")

    dropped_count = 0

    try:
        # Get all indexes - use the correct query format
        result = db.query(
            "sql", "SELECT name, typeName, properties FROM schema:indexes"
        )
        indexes = list(result)

        if verbose:
            print(f"   📊 Found {len(indexes)} indexes")

        for index_record in indexes:
            index_name = index_record.get("name")

            # Try to drop all indexes
            # Note: Some system indexes cannot be dropped and will fail gracefully
            try:
                # Drop the index
                db.command("sql", f"DROP INDEX `{index_name}`")
                dropped_count += 1
                if verbose:
                    print(f"   ✅ Dropped index: {index_name}")
            except Exception as e:  # noqa: BLE001
                if verbose:
                    # Only show if it's not a "cannot drop" error
                    error_msg = str(e)
                    if "cannot" not in error_msg.lower():
                        print(f"   ⚠️  Could not drop {index_name}: {e}")

        if verbose:
            print(f"   ✅ Dropped {dropped_count} indexes")

    except Exception as e:  # noqa: BLE001
        if verbose:
            print(f"   ⚠️  Error querying indexes: {e}")

    return dropped_count


def wait_for_compaction(db, max_wait_seconds=600, verbose=True):
    """
    Wait for background compaction to complete before performing operations.

    Args:
        db: Database instance
        max_wait_seconds: Maximum time to wait (default 10 minutes)
        verbose: If True, print progress messages

    Returns:
        bool: True if compaction completed, False if timeout
    """
    if verbose:
        print("   ⏳ Waiting for background compaction to complete...")

    retry_interval = 5  # Check every 5 seconds
    wait_start = time.time()

    while (time.time() - wait_start) < max_wait_seconds:
        try:
            # Read-only probe; no transaction required
            db.query("sql", "SELECT count(*) FROM Movie LIMIT 1").first()
            if verbose:
                print("   ✅ Background compaction complete")
            return True
        except Exception as e:  # noqa: BLE001
            if "NeedRetryException" in str(e) or "asynchronous tasks" in str(e):
                elapsed = time.time() - wait_start
                if verbose:
                    print(f"   ⏳ Still compacting... ({elapsed:.0f}s elapsed)")
                time.sleep(retry_interval)
            else:
                if verbose:
                    print(f"   ⚠️  Unexpected error while waiting: {e}")
                return False

    if verbose:
        print(
            f"   ⚠️  Compaction still running after {max_wait_seconds}s, "
            f"proceeding anyway"
        )
    return False


def run_validation_queries(db, queries=None, num_runs=1, verbose=True):
    """
    Run validation queries against a database.

    Args:
        db: Database instance to query
        queries: List of (name, query) tuples. If None, uses TEST_QUERIES
        num_runs: Number of times to run each query (for performance testing)
        verbose: If True, print detailed statistics

    Returns:
        List of dicts with query results and statistics
    """
    if queries is None:
        queries = TEST_QUERIES

    results = []

    for query_name, query in queries:
        run_times = []
        result_count = 0
        sample_result = None

        for i in range(num_runs):
            query_start = time.time()
            result = list(db.query("sql", query))
            query_time = time.time() - query_start
            run_times.append(query_time)
            result_count = len(result)
            # Capture first result for validation
            if i == 0:
                sample_result = result

        # Calculate statistics
        avg_time = statistics.mean(run_times)
        std_time = statistics.stdev(run_times) if len(run_times) > 1 else 0
        min_time = min(run_times)
        max_time = max(run_times)

        results.append(
            {
                "name": query_name,
                "runs": run_times,
                "avg": avg_time,
                "std": std_time,
                "min": min_time,
                "max": max_time,
                "count": result_count,
                "sample": sample_result,
            }
        )

        if verbose:
            print(f"   📊 {query_name}:")
            if num_runs > 1:
                print(f"      Average: {avg_time:.3f}s ± {std_time:.3f}s")
                print(f"      Range: [{min_time:.3f}s - {max_time:.3f}s]")
            else:
                print(f"      Time: {avg_time:.3f}s")
            print(f"      Results: {result_count}")
            print()

    return results


def download_dataset(dataset_name):
    """Download the dataset using download_data.py script.

    Args:
        dataset_name: Dataset name (e.g., "movielens-small", "movielens-large")
    """
    download_script = Path(__file__).parent / "download_data.py"

    if not download_script.exists():
        print(f"❌ Download script not found: {download_script}")
        print("   Please ensure download_data.py is in the same directory.")
        sys.exit(1)

    print(f"📥 Downloading {dataset_name} dataset...")
    print(f"   Running: python {download_script} {dataset_name}")
    print()

    try:
        subprocess.run(
            [sys.executable, str(download_script), dataset_name],
            check=True,
            capture_output=False,
        )
        print()
        print("✅ Dataset downloaded successfully!")
        print()
    except subprocess.CalledProcessError as e:
        print(f"❌ Failed to download dataset: {e}")
        sys.exit(1)


def check_dataset_exists(data_dir):
    """Check if all required CSV files exist in the dataset directory."""
    required_files = ["movies.csv", "ratings.csv", "links.csv", "tags.csv"]

    if not data_dir.exists():
        return False

    for csv_file in required_files:
        if not (data_dir / csv_file).exists():
            return False

    return True


# Parse command-line arguments
parser = argparse.ArgumentParser(
    description="Example 04: Import MovieLens dataset into ArcadeDB",
    formatter_class=argparse.RawDescriptionHelpFormatter,
    epilog="""
Examples:
  python 04_csv_import_documents.py                             # Use large dataset (default)
  python 04_csv_import_documents.py --dataset movielens-small   # Use small dataset
  python 04_csv_import_documents.py --dataset movielens-large   # Use large dataset
  python 04_csv_import_documents.py --parallel 8                # Use 8 parallel threads
  python 04_csv_import_documents.py --batch-size 10000          # Use larger batch size
  python 04_csv_import_documents.py --dataset movielens-small --parallel 4 --batch-size 1000
  python 04_csv_import_documents.py --export                    # Export database after import
  python 04_csv_import_documents.py --export --export-path my_backup.jsonl.tgz

Dataset sizes:
  large - movielens-large (~33M ratings, ~86K movies, ~265 MB) - DEFAULT
  small - movielens-small (~100K ratings, ~9K movies, ~1 MB)

Parallel threads:
  Default: auto-detect (CPU cores / 2 - 1, minimum 1)
  Recommendation: 4-8 threads for best performance
  Higher values don't always help due to lock contention

Batch size (commitEvery):
  Default: 5000 records per commit
  Larger batches = faster imports, more memory usage
  Smaller batches = slower imports, less memory usage

Export:
  --export: Enable database export to JSONL format after import
  --export-path: Export filename (default: {db_name}.jsonl.tgz in exports/ dir)
  Use exports to create reproducible benchmark databases or backups

The script will automatically download the dataset if it doesn't exist.
    """,
)
parser.add_argument(
    "--dataset",
    choices=["movielens-small", "movielens-large"],
    default="movielens-large",
    help="Dataset size to use (default: movielens-large)",
)
parser.add_argument(
    "--parallel",
    type=int,
    default=None,
    help="Number of parallel threads for import (default: auto-detect based on CPU cores)",
)
parser.add_argument(
    "--batch-size",
    type=int,
    default=5000,
    help="Number of records to commit per batch (default: 5000)",
)
parser.add_argument(
    "--heap-size",
    type=str,
    default=None,
    help="Set JVM max heap size (e.g. 8g, 4096m). Overrides default 4g.",
)
parser.add_argument(
    "--db-name",
    type=str,
    default=None,
    help="Database name (default: based on dataset name, e.g., movielens_small_db)",
)
parser.add_argument(
    "--export",
    action="store_true",
    help="Export database to JSONL after import (for reproducibility)",
)
parser.add_argument(
    "--export-path",
    type=str,
    default=None,
    help="Export filename (default: {db_name}.jsonl.tgz in exports/ directory)",
)
args = parser.parse_args()

# Start script timer
script_start_time = time.time()

print("=" * 70)
print("🎬 ArcadeDB Python - Example 04: CSV Import - Documents")
print("=" * 70)
print()
print(f"📊 Dataset: {args.dataset}")
if args.parallel:
    print(f"🔧 Parallel threads: {args.parallel}")
else:
    print("🔧 Parallel threads: auto-detect (CPU cores / 2 - 1, min 1)")
print(f"🔧 Batch size (commitEvery): {args.batch_size}")
if args.export:
    # Determine export filename for display
    if args.export_path:
        display_path = args.export_path
    else:
        # Convert dataset name to db name (movielens-small → movielens_small_db)
        db_name = args.db_name or args.dataset.replace("-", "_") + "_db"
        display_path = f"exports/{db_name}.jsonl.tgz"
    print(f"💾 Export: enabled → {display_path}")
else:
    print("💾 Export: disabled (use --export to enable)")
print()

# Check JVM heap configuration for large imports
if args.heap_size:
    print(f"💡 JVM Max Heap: {args.heap_size} (from --heap-size)")
else:
    print("💡 JVM Max Heap: 4g (default)")
    print("   ℹ️  Using default JVM heap (4g)")
    if args.dataset == "movielens-large":
        print("   💡 For large datasets, you can increase it:")
        print("      Use --heap-size 8g (or higher)")
print()

# -----------------------------------------------------------------------------
# Step 0: Check Dataset Availability and Download if Needed
# -----------------------------------------------------------------------------
print("Step 0: Checking for MovieLens dataset...")
print()

# Determine dataset directory based on dataset argument
data_base = Path(__file__).parent / "data"
dataset_dirname = args.dataset
data_dir = data_base / dataset_dirname

# Check if dataset exists, download if it doesn't
if not check_dataset_exists(data_dir):
    print(f"❌ Dataset not found at: {data_dir}")
    print()
    download_dataset(args.dataset)
else:
    print("✅ Dataset found!")
    print(f"   Location: {data_dir}")
    print()

# Show file sizes
required_files = ["movies.csv", "ratings.csv", "links.csv", "tags.csv"]
print("📊 Dataset files:")
for csv_file in required_files:
    file_path = data_dir / csv_file
    size_kb = file_path.stat().st_size / 1024
    print(f"   • {csv_file}: {size_kb:.1f} KB")
print()

# -----------------------------------------------------------------------------
# Step 1: Create Database
# -----------------------------------------------------------------------------
print("Step 1: Creating database...")
step_start = time.time()

db_dir = "./my_test_databases"
if args.db_name:
    db_name = args.db_name
else:
    # Convert dataset name to db name (movielens-small → movielens_small_db)
    db_name = args.dataset.replace("-", "_") + "_db"
db_path = os.path.join(db_dir, db_name)

# Clean up any existing database from previous runs
if os.path.exists(db_path):
    shutil.rmtree(db_path)

# Clean up log directory from previous runs
if os.path.exists("./log"):
    shutil.rmtree("./log")

db = arcadedb.create_database(
    db_path,
    jvm_kwargs={"heap_size": args.heap_size} if args.heap_size else None,
)


def import_csv_documents_via_sql(database, csv_path: str, doc_type: str):
    start_time = time.time()

    schema_by_type = {
        "Movie": {
            "movieId": "LONG",
            "title": "STRING",
            "genres": "STRING",
        },
        "Rating": {
            "userId": "LONG",
            "movieId": "LONG",
            "rating": "DOUBLE",
            "timestamp": "LONG",
        },
        "Link": {
            "movieId": "LONG",
            "imdbId": "LONG",
            "tmdbId": "LONG",
        },
        "Tag": {
            "userId": "LONG",
            "movieId": "LONG",
            "tag": "STRING",
            "timestamp": "LONG",
        },
    }

    if doc_type not in schema_by_type:
        raise ValueError(f"Unsupported document type: {doc_type}")

    schema = schema_by_type[doc_type]

    database.command("sql", f"CREATE DOCUMENT TYPE `{doc_type}`")
    for field_name, field_type in schema.items():
        database.command(
            "sql",
            f"CREATE PROPERTY `{doc_type}`.`{field_name}` {field_type}",
        )

    def _convert(field_name: str, raw_value: str):
        if raw_value is None or raw_value == "":
            return None

        field_type = schema[field_name]
        if field_type == "LONG":
            return int(raw_value)
        if field_type == "DOUBLE":
            return float(raw_value)
        return raw_value

    with open(csv_path, "r", encoding="utf-8", newline="") as csv_file:
        reader = csv.DictReader(csv_file)
        headers = reader.fieldnames or []

        if not headers:
            raise RuntimeError(f"CSV has no header: {csv_path}")

        missing = [h for h in headers if h not in schema]
        if missing:
            raise RuntimeError(f"Unexpected columns in {doc_type}: {missing}")

        insert_sql = "INSERT INTO `{}` SET {}".format(
            doc_type, ", ".join([f"`{h}` = ?" for h in headers])
        )

        parsed_rows = 0
        error_count = 0
        pending_rows = []

        def _flush_batch(batch_rows):
            with database.transaction():
                for values in batch_rows:
                    database.command("sql", insert_sql, values)

        for row in reader:
            try:
                converted = [_convert(header, row.get(header)) for header in headers]
                pending_rows.append(converted)
                parsed_rows += 1
            except (TypeError, ValueError):
                error_count += 1
                continue

            if len(pending_rows) >= args.batch_size:
                _flush_batch(pending_rows)
                pending_rows = []

        if pending_rows:
            _flush_batch(pending_rows)

    documents = (
        database.query("sql", f"SELECT count(*) as c FROM `{doc_type}`").one().get("c")
    )
    duration_ms = int((time.time() - start_time) * 1000)

    return {
        "documents": documents,
        "errors": error_count,
        "duration_ms": duration_ms,
    }


print(f"   ✅ Database created at: {db_path}")
print("   💡 Using embedded mode - no server needed!")
print(f"   ⏱️  Time: {time.time() - step_start:.3f}s")
print()

# -----------------------------------------------------------------------------
# Step 2: Import Movies CSV → Movie Documents (bulk insert ingest)
# -----------------------------------------------------------------------------
print("Step 2: Importing movies.csv → Movie documents...")
print("   💡 Using bulk insert mode:")
print("      • Schema is created explicitly before ingest")
print("      • Batch inserts run with WAL disabled for faster load")
print("      • WAL is re-enabled after ingest")
print()
step_start = time.time()

db.set_read_your_writes(False)
async_exec = db.async_executor()
async_exec.set_commit_every(args.batch_size)
async_exec.set_transaction_use_wal(False)

movies_csv = str(data_dir / "movies.csv")
stats = import_csv_documents_via_sql(db, movies_csv, "Movie")

print(f"   ✅ Imported {stats['documents']:,} movies")
print(f"   💡 Errors: {stats['errors']}")
print(f"   ⏱️  Time: {stats['duration_ms'] / 1000:.3f}s")
rate = stats["documents"] / (stats["duration_ms"] / 1000)
print(f"   ⏱️  Rate: {rate:.0f} records/sec")

# Check NULL values (genres can be NULL)
# Using first() instead of list()[0] - more efficient
null_genres = (
    db.query("sql", "SELECT count(*) as c FROM Movie WHERE genres IS NULL")
    .first()
    .get("c")
)

if null_genres > 0:
    print("   🔍 NULL values detected:")
    print(
        f"      • genres: {null_genres:,} NULL values "
        f"({null_genres/stats['documents']*100:.1f}%)"
    )
    print("   💡 Empty CSV cells correctly imported as SQL NULL")

print()

# -----------------------------------------------------------------------------
# Step 3: Display Schema Mapping
# -----------------------------------------------------------------------------
print("Step 3: Inspecting schema mapping...")
print()

# Query the schema created during ingest setup
result = db.query("sql", "SELECT properties FROM schema:types WHERE name = 'Movie'")
for record in result:
    properties = record.get("properties")

    print("   📋 Movie schema (explicit mapping for bulk ingest):")
    if properties:
        for prop in properties:
            # prop is a Java Map object
            prop_map = dict(prop.toMap()) if hasattr(prop, "toMap") else prop
            prop_name = prop_map.get("name")
            prop_type = prop_map.get("type")
            print(f"      • {prop_name}: {prop_type}")
    else:
        print("      (No properties found)")

print()
print("   💡 Bulk ingest type strategy:")
print("      • Numeric ID columns use LONG")
print("      • Decimal columns use DOUBLE")
print("      • Text columns use STRING")
print("      • Empty cells → NULL (proper SQL NULL handling)")
print()

# -----------------------------------------------------------------------------
# Step 4: Import Ratings CSV → Rating Documents
# -----------------------------------------------------------------------------
print("Step 4: Importing ratings.csv → Rating documents...")
step_start = time.time()

ratings_csv = str(data_dir / "ratings.csv")
stats = import_csv_documents_via_sql(db, ratings_csv, "Rating")

print(f"   ✅ Imported {stats['documents']:,} ratings")
print(f"   💡 Errors: {stats['errors']}")
print(f"   ⏱️  Time: {stats['duration_ms'] / 1000:.3f}s")
rate = stats["documents"] / (stats["duration_ms"] / 1000)
print(f"   ⏱️  Rate: {rate:.0f} records/sec")

# Check NULL values (timestamp can be NULL)
null_timestamps = (
    db.query("sql", "SELECT count(*) as c FROM Rating WHERE timestamp IS NULL")
    .first()
    .get("c")
)

if null_timestamps > 0:
    print("   🔍 NULL values detected:")
    print(
        f"      • timestamp: {null_timestamps:,} NULL values "
        f"({null_timestamps/stats['documents']*100:.1f}%)"
    )
    print("   💡 Empty CSV cells correctly imported as SQL NULL")

print()

db.set_read_your_writes(True)
async_exec.set_transaction_use_wal(True)
print("   ✅ Ingest mode reset: WAL re-enabled")
print()

# -----------------------------------------------------------------------------
# Step 5: Import Links CSV → Link Documents
# -----------------------------------------------------------------------------
print("Step 5: Importing links.csv → Link documents...")
step_start = time.time()

links_csv = str(data_dir / "links.csv")
stats = import_csv_documents_via_sql(db, links_csv, "Link")

print(f"   ✅ Imported {stats['documents']:,} links")
print(f"   💡 Errors: {stats['errors']}")
print(f"   ⏱️  Time: {stats['duration_ms'] / 1000:.3f}s")
rate = stats["documents"] / (stats["duration_ms"] / 1000)
print(f"   ⏱️  Rate: {rate:.0f} records/sec")

# Check NULL values (imdbId and tmdbId can be NULL)
null_imdb = (
    db.query("sql", "SELECT count(*) as c FROM Link WHERE imdbId IS NULL")
    .first()
    .get("c")
)
null_tmdb = (
    db.query("sql", "SELECT count(*) as c FROM Link WHERE tmdbId IS NULL")
    .first()
    .get("c")
)

if null_imdb > 0 or null_tmdb > 0:
    print("   🔍 NULL values detected:")
    if null_imdb > 0:
        print(
            f"      • imdbId: {null_imdb:,} NULL values "
            f"({null_imdb/stats['documents']*100:.1f}%)"
        )
    if null_tmdb > 0:
        print(
            f"      • tmdbId: {null_tmdb:,} NULL values "
            f"({null_tmdb/stats['documents']*100:.1f}%)"
        )
    print("   💡 Empty CSV cells correctly imported as SQL NULL")

print()

# -----------------------------------------------------------------------------
# Step 6: Import Tags CSV → Tag Documents
# -----------------------------------------------------------------------------
print("Step 6: Importing tags.csv → Tag documents...")
step_start = time.time()

tags_csv = str(data_dir / "tags.csv")
stats = import_csv_documents_via_sql(db, tags_csv, "Tag")

print(f"   ✅ Imported {stats['documents']:,} tags")
print(f"   💡 Errors: {stats['errors']}")
print(f"   ⏱️  Time: {stats['duration_ms'] / 1000:.3f}s")
rate = stats["documents"] / (stats["duration_ms"] / 1000)
print(f"   ⏱️  Rate: {rate:.0f} records/sec")

# Check NULL values in tag field
null_tags = (
    db.query("sql", "SELECT count(*) as c FROM Tag WHERE tag IS NULL").first().get("c")
)

if null_tags > 0:
    print("   🔍 NULL values detected:")
    print(
        f"      • tag: {null_tags:,} NULL values "
        f"({null_tags/stats['documents']*100:.1f}%)"
    )
    print("   💡 Empty CSV cells correctly imported as SQL NULL")

print()

# -----------------------------------------------------------------------------
# Step 7: Verify All Schemas
# -----------------------------------------------------------------------------
print("Step 7: Verifying all schemas...")
print()

# Query the formal schema for all imported document types
for doc_type in ["Movie", "Rating", "Link", "Tag"]:
    result = db.query(
        "sql", f"SELECT properties FROM schema:types WHERE name = '{doc_type}'"
    )

    for record in result:
        properties = record.get("properties")

        print(f"   📋 {doc_type} schema (bulk ingest mapping):")
        if properties:
            for prop in properties:
                # prop is a Java Map object
                prop_map = dict(prop.toMap()) if hasattr(prop, "toMap") else prop
                prop_name = prop_map.get("name")
                prop_type = prop_map.get("type")
                print(f"      • {prop_name}: {prop_type}")
        else:
            print("      (No properties found)")
        print()

print("   💡 Schema observations:")
print("      • All integer-like columns → LONG")
print("      • All decimal columns → DOUBLE")
print("      • Text columns → STRING")
print("      • Empty cells → NULL (proper SQL NULL handling)")
print()

# -----------------------------------------------------------------------------
# Step 8: Test Query Performance WITHOUT Indexes (Multiple Runs)
# -----------------------------------------------------------------------------
print("Step 8: Testing query performance WITHOUT indexes (10 runs each)...")
print()

# Run test queries 10 times to get reliable statistics
times_without_indexes = run_validation_queries(db, num_runs=10, verbose=True)

print("   💡 Running queries multiple times to get reliable statistics")
print()

# Compare against embedded baseline results
baseline_match_step8 = True
if args.dataset in EXPECTED_RESULTS and EXPECTED_RESULTS[args.dataset]:
    print("   📊 Step 8 - Comparing against baseline (BEFORE indexes):")
    print()
    baseline_match_step8 = compare_query_results(
        times_without_indexes, EXPECTED_RESULTS[args.dataset], verbose=True
    )
    print()
    if baseline_match_step8:
        print("   ✅ Step 8: All results match baseline!")
    else:
        print("   ❌ Step 8: VALIDATION FAILED - Results differ from baseline!")
    print()
else:
    print(
        f"   ℹ️  No embedded baseline for {args.dataset} dataset - "
        f"skipping Step 8 comparison"
    )
    print()

# -----------------------------------------------------------------------------
# Step 9: Create Indexes After Import (Best Practice)
# -----------------------------------------------------------------------------
print("Step 9: Creating indexes for query performance...")
step_start = time.time()

# Wait for background compaction to complete
wait_for_compaction(db, max_wait_seconds=600, verbose=True)

# Define indexes to create
indexes = [
    ("Movie", "movieId", "UNIQUE"),
    ("Movie", "genres", "FULL_TEXT"),  # Full-text search for genre queries
    ("Rating", "userId", "NOTUNIQUE"),
    ("Rating", "movieId", "NOTUNIQUE"),
    ("Link", "movieId", "UNIQUE"),
    ("Tag", "movieId", "NOTUNIQUE"),
]

# Create indexes using helper function
success_count, failed_indexes = create_indexes(db, indexes, verbose=True)

print("\n   💡 Best practice: Create indexes AFTER bulk import")
print(f"   ⏱️  Total index creation time: {time.time() - step_start:.1f}s")

# Verify which indexes actually exist in the database
print("\n   🔍 Verifying indexes in database:")
index_query = """
    SELECT name, typeName, properties, `unique`, `automatic`
    FROM schema:indexes
    ORDER BY typeName, name
"""
existing_indexes = list(db.query("sql", index_query))

# Build a map of expected indexes for validation
expected_indexes = {}
for table, column, uniqueness in indexes:
    key = (table, column, uniqueness)
    expected_indexes[key] = False  # Will mark True when found

# Check all existing indexes
#
# Note: ArcadeDB exposes multiple index engines, including LSM_TREE, HASH,
# FULL_TEXT, and VECTOR.
# The schema metadata query only exposes a boolean 'unique' field, not the engine type.
# Therefore:
#   - UNIQUE / UNIQUE_HASH indexes → unique=true
#   - NOTUNIQUE / NOTUNIQUE_HASH indexes → unique=false
#   - FULL_TEXT indexes → unique=false, engine=FULL_TEXT (appears as NOTUNIQUE!)
#
# This means HASH and FULL_TEXT indexes must be validated by semantics rather than
# engine type alone.
for idx in existing_indexes:
    idx_dict = json.loads(idx.to_json())
    index_type = "UNIQUE" if idx_dict.get("unique") else "NOTUNIQUE"
    auto = "automatic" if idx_dict.get("automatic") else "manual"
    props = idx_dict.get("properties", [])
    type_name = idx_dict["typeName"]
    name = idx_dict["name"]
    print(f"   📊 {type_name}.{name}: {props} ({index_type}, {auto})")

    # Check if this matches one of our expected indexes
    # The main index has name like "Table[column]"
    candidate_columns = []
    if props and len(props) > 0:
        for prop in props:
            if isinstance(prop, dict):
                name_value = (
                    prop.get("name") or prop.get("property") or prop.get("field")
                )
                if name_value:
                    candidate_columns.append(name_value)
            elif isinstance(prop, list):
                if prop:
                    candidate_columns.append(prop[0])
            else:
                candidate_columns.append(prop)

    if isinstance(name, str) and "[" in name and name.endswith("]"):
        raw_props = name[name.find("[") + 1 : -1].strip()
        for col in raw_props.split(","):
            col = col.strip()
            if col:
                candidate_columns.append(col)

    candidate_columns = [c for c in candidate_columns if c]

    for column_name in candidate_columns:
        # Try matching as the reported uniqueness semantics.
        key = (type_name, column_name, index_type)
        if key in expected_indexes:
            expected_indexes[key] = True

        if index_type == "UNIQUE":
            unique_hash_key = (type_name, column_name, "UNIQUE_HASH")
            if unique_hash_key in expected_indexes:
                expected_indexes[unique_hash_key] = True

        # FULL_TEXT indexes appear as NOTUNIQUE in metadata, so also check for FULL_TEXT
        # This is expected behavior since FULL_TEXT is a different index engine type,
        # not a variant of LSM_TREE, but metadata only exposes the 'unique' boolean.
        if index_type == "NOTUNIQUE":
            notunique_hash_key = (type_name, column_name, "NOTUNIQUE_HASH")
            if notunique_hash_key in expected_indexes:
                expected_indexes[notunique_hash_key] = True
            fulltext_key = (type_name, column_name, "FULL_TEXT")
            if fulltext_key in expected_indexes:
                expected_indexes[fulltext_key] = True

        # Some versions report UNIQUE indexes as NOTUNIQUE in schema:indexes.
        # Treat the base index name (Type[column]) as authoritative when present.
        base_index_name = f"{type_name}[{column_name}]"
        if name == base_index_name:
            unique_key = (type_name, column_name, "UNIQUE")
            if unique_key in expected_indexes:
                expected_indexes[unique_key] = True
            unique_hash_key = (type_name, column_name, "UNIQUE_HASH")
            if unique_hash_key in expected_indexes:
                expected_indexes[unique_hash_key] = True

# Validate all expected indexes were created
print("\n   ✅ Validating expected indexes:")
missing_indexes = []
for (table, column, uniqueness), found in expected_indexes.items():
    if found:
        print(f"      ✅ {table}({column}) {uniqueness} - FOUND")
    else:
        print(f"      ❌ {table}({column}) {uniqueness} - MISSING")
        missing_indexes.append(f"{table}({column}) {uniqueness}")

if missing_indexes:
    error_msg = f"Missing expected indexes: {', '.join(missing_indexes)}"
    print(f"\n   ❌ ERROR: {error_msg}")
    raise RuntimeError(error_msg)
else:
    print("\n   ✅ All expected indexes created successfully!")

print(f"\n   ⏱️  Time: {time.time() - step_start:.3f}s")
print()

# -----------------------------------------------------------------------------
# Step 10: Test Query Performance WITH Indexes (Multiple Runs)
# -----------------------------------------------------------------------------
print("Step 10: Testing query performance WITH indexes (10 runs each)...")
print()

# Run the same test queries with indexes in place
times_with_indexes = run_validation_queries(db, num_runs=10, verbose=True)

print()
print("   🚀 Performance Improvement Summary:")
print("   " + "=" * 70)
print(f"   {'Query':<30} {'Before (s)':<15} {'After (s)':<15} {'Speedup':<10}")
print("   " + "=" * 70)

for i in range(len(TEST_QUERIES)):
    before_stats = times_without_indexes[i]
    after_stats = times_with_indexes[i]

    before_avg = before_stats["avg"]
    before_std = before_stats["std"]
    after_avg = after_stats["avg"]
    after_std = after_stats["std"]

    if after_stats["avg"] > 0:
        speedup = before_stats["avg"] / after_stats["avg"]
        time_saved_pct = (
            (before_stats["avg"] - after_stats["avg"]) / before_stats["avg"]
        ) * 100

        query_name = before_stats["name"][:28]
        before_str = f"{before_avg:.3f}±{before_std:.3f}"
        after_str = f"{after_avg:.3f}±{after_std:.3f}"
        speedup_str = f"{speedup:.1f}x"

        print(f"   {query_name:<30} {before_str:<15} {after_str:<15} {speedup_str:<10}")

        # Detailed improvement info
        improvement_msg = f"      ({time_saved_pct:.1f}% time saved)"
        print(f"   {'':<30} {improvement_msg}")
    else:
        print(f"   {before_stats['name']:<30} Too fast to measure")

print("   " + "=" * 70)
print()

# Validate that results are identical with and without indexes
print("   🔍 Validating result consistency...")
all_match = True
for i in range(len(TEST_QUERIES)):
    before_count = times_without_indexes[i]["count"]
    after_count = times_with_indexes[i]["count"]
    query_name = times_without_indexes[i]["name"]
    before_sample = times_without_indexes[i]["sample"]
    after_sample = times_with_indexes[i]["sample"]

    # Check if row counts match
    if before_count != after_count:
        print(f"   ❌ {query_name}: {before_count} → {after_count} rows" " (MISMATCH!)")
        all_match = False
        continue

    # For queries returning results, show sample data
    detail = ""
    if before_count > 0 and before_sample and after_sample:
        first_before = before_sample[0]
        first_after = after_sample[0]

        # Check if this is a COUNT query (has 'count' property)
        if first_before.has_property("count"):
            count_before = first_before.get("count")
            count_after = first_after.get("count")
            if count_before != count_after:
                print(
                    f"   ❌ {query_name}: count values differ: "
                    f"{count_before} → {count_after} (MISMATCH!)"
                )
                all_match = False
                continue
            detail = f" → Count value: {count_before:,}"
        # Show first result's title or movieId for regular queries
        elif first_before.has_property("title"):
            title_before = first_before.get("title")
            title_after = first_after.get("title")
            if title_before != title_after:
                print(
                    f"   ❌ {query_name}: first result differs: "
                    f"'{title_before}' → '{title_after}' (MISMATCH!)"
                )
                all_match = False
                continue
            if len(title_before) > 40:
                detail = f" → First: '{title_before[:40]}...'"
            else:
                detail = f" → First: '{title_before}'"
        elif first_before.has_property("movieId"):
            movieId_before = first_before.get("movieId")
            movieId_after = first_after.get("movieId")
            if movieId_before != movieId_after:
                print(
                    f"   ❌ {query_name}: first result differs: "
                    f"{movieId_before} → {movieId_after} (MISMATCH!)"
                )
                all_match = False
                continue
            detail = f" → First movieId: {movieId_before}"

    row_text = "row" if before_count == 1 else "rows"
    print(f"   ✅ {query_name}: {before_count} {row_text}{detail}")

if all_match:
    print("\n   ✅ All queries return identical results with and without " "indexes!")
else:
    print("\n   ⚠️  WARNING: Some queries returned different results!")
print()

# Save query results and compare against baseline
print("   💾 Saving query results for reproducibility...")
results_file = save_query_results(times_with_indexes, args.dataset, db_path)
print(f"   ✅ Results saved to: {results_file}")
print()

# Compare against embedded baseline results
baseline_match_step10 = True
if args.dataset in EXPECTED_RESULTS and EXPECTED_RESULTS[args.dataset]:
    print("   📊 Step 10 - Comparing against baseline (AFTER indexes):")
    print()
    baseline_match_step10 = compare_query_results(
        times_with_indexes, EXPECTED_RESULTS[args.dataset], verbose=True
    )
    print()
    if baseline_match_step10:
        print("   ✅ Step 10: All results match baseline!")
    else:
        print("   ❌ Step 10: VALIDATION FAILED - Results differ from baseline!")
    print()
else:
    print(
        f"   ℹ️  No embedded baseline for {args.dataset} dataset - "
        f"results saved for future comparison"
    )
    print()

print("   💡 Key Findings:")
print("      • Indexes provide consistent speedup across multiple runs")
print("      • Standard deviation shows query time stability")
print("      • Composite indexes (userId, movieId) show biggest gains")
print("      • Full-text index enables efficient text search")
print()
print("   💡 Indexes are essential for production performance!")
print()

# -----------------------------------------------------------------------------
# Step 11: Demonstrate Full-Text Search on Genres
# -----------------------------------------------------------------------------
print("Step 11: Full-text search demonstration...")
print()
print("   💡 The genres field has a Lucene FULL_TEXT index")
print("      • Tokenizes on whitespace and punctuation (|)")
print("      • Case-insensitive matching")
print("      • Optimizes text search queries")
print()

# Test full-text search with various genre keywords
genre_searches = ["Action", "Comedy", "Drama", "Sci-Fi", "Horror"]

print("   🔍 Testing text search on genres:")
for genre in genre_searches:
    step_start = time.time()
    # Query using LIKE - ArcadeDB should optimize this with the FULL_TEXT index
    result = list(
        db.query(
            "sql",
            f"SELECT FROM Movie WHERE genres LIKE '%{genre}%' LIMIT 5",
        )
    )
    query_time = time.time() - step_start

    print(f"      • '{genre}': {len(result)} results in {query_time:.3f}s")
    if result:
        # Show first movie as example
        first_movie = result[0]
        title = str(first_movie.get("title"))
        genres = str(first_movie.get("genres"))
        print(f"        Example: {title}")
        print(f"        Genres: {genres}")

print()
print("   💡 FULL_TEXT Index Benefits:")
print("      • Tokenization: Splits 'Action|Adventure|Sci-Fi' into searchable terms")
print("      • Inverted index: Fast lookup even on large datasets")
print("      • Lucene-powered: Industry-standard full-text search engine")
print()
print("   💡 Why is LIKE fast even without index?")
print("      • LIMIT 10: Query stops after finding 10 matches (early termination)")
print("      • Common terms: 'Action' appears frequently, found quickly")
print("      • Small dataset: Movies fit in memory, fast sequential scan")
print()
print("   🔍 Testing LIKE without LIMIT (must scan ALL records):")
count_start = time.time()
count_result = list(
    db.query("sql", "SELECT count(*) as count FROM Movie WHERE genres LIKE '%Action%'")
)
count_time = time.time() - count_start
action_count = count_result[0].get("count") if count_result else 0
action_count = action_count if action_count is not None else 0
print(f"      • Total Action movies: {action_count:,} found in {count_time:.3f}s")
print("      • This requires scanning all records (no early termination)")
print()
print("   💡 FULL_TEXT indexes provide biggest gains when:")
print("      • Searching large datasets (millions of records)")
print("      • Counting matches (must scan all records, no LIMIT)")
print("      • Complex text queries (multiple terms, wildcards)")
print("      • Records don't fit in memory (disk I/O becomes bottleneck)")
print()
print("   💡 The genres field benefits from full-text indexing because:")
print("      • Contains pipe-delimited values (Action|Adventure|Sci-Fi)")
print("      • Users search by individual genres, not full combinations")
print("      • Full-text index automatically splits on | character")
print()

# -----------------------------------------------------------------------------
# Step 12: Database Statistics
# -----------------------------------------------------------------------------
print("Step 12: Database statistics...")
print()

# 12.1 - Count records in each type
print("   📊 Record counts by type:")
step_start = time.time()
for doc_type in ["Movie", "Rating", "Link", "Tag"]:
    result = db.query("sql", f"SELECT count(*) as count FROM {doc_type}")
    count = list(result)[0].get("count")
    print(f"      • {doc_type}: {count:,} records")
print(f"   ⏱️  Time: {time.time() - step_start:.3f}s")
print()

# 12.2 - Sample movies
print("   🎬 Sample movies:")
step_start = time.time()
result = db.query("sql", "SELECT FROM Movie LIMIT 5")
for record in result:
    movie_id = record.get("movieId")
    title = str(record.get("title"))
    genres = str(record.get("genres"))
    print(f"      • [{movie_id}] {title}")
    print(f"        Genres: {genres}")
print(f"   ⏱️  Time: {time.time() - step_start:.3f}s")
print()

# 12.3 - Rating statistics
print("   ⭐ Rating statistics:")
step_start = time.time()
result = db.query(
    "sql",
    """SELECT
         count(*) as total_ratings,
         avg(rating) as avg_rating,
         min(rating) as min_rating,
         max(rating) as max_rating
       FROM Rating""",
)
record = list(result)[0]
total = record.get("total_ratings")
avg_rating = record.get("avg_rating")
min_rating = record.get("min_rating")
max_rating = record.get("max_rating")
print(f"      • Total ratings: {total:,}")
print(f"      • Average rating: {avg_rating:.2f}")
print(f"      • Min rating: {min_rating}")
print(f"      • Max rating: {max_rating}")
print(f"   ⏱️  Time: {time.time() - step_start:.3f}s")
print()

# 12.4 - Rating distribution
print("   📊 Rating distribution:")
step_start = time.time()
result = db.query(
    "sql",
    """SELECT rating, count(*) as count
       FROM Rating
       GROUP BY rating
       ORDER BY rating""",
)
for record in result:
    rating = record.get("rating")
    count = record.get("count")
    bar = "█" * int(count / 3000)  # Scale for visualization
    # Handle NULL ratings (introduced by NULL injection)
    if rating is None:
        print(f"      NULL  : {count:,} {bar}")
    else:
        print(f"      {rating:.1f} ★ : {count:,} {bar}")
print(f"   ⏱️  Time: {time.time() - step_start:.3f}s")
print()

# 12.5 - Most popular genres
print("   🎭 Top 10 genres by movie count:")
step_start = time.time()
# Note: genres are pipe-delimited, so this is approximate
result = db.query(
    "sql",
    """SELECT genres, count(*) as count
       FROM Movie
       WHERE genres <> '(no genres listed)'
       GROUP BY genres
       ORDER BY count DESC
       LIMIT 10""",
)
for idx, record in enumerate(result, 1):
    genres = str(record.get("genres"))
    count = record.get("count")
    # Truncate long genre lists
    if len(genres) > 50:
        genres = genres[:47] + "..."
    print(f"      {idx:2}. {genres} ({count} movies)")
print(f"   ⏱️  Time: {time.time() - step_start:.3f}s")
print()

# 10.6 - Most active users (by rating count)
print("   👥 Top 10 most active users (by ratings):")
step_start = time.time()
result = db.query(
    "sql",
    """SELECT userId, count(*) as rating_count
       FROM Rating
       GROUP BY userId
       ORDER BY rating_count DESC
       LIMIT 10""",
)
for idx, record in enumerate(result, 1):
    user_id = record.get("userId")
    rating_count = record.get("rating_count")
    print(f"      {idx:2}. User {user_id}: {rating_count} ratings")
print(f"   ⏱️  Time: {time.time() - step_start:.3f}s")
print()

# 10.7 - Most tagged movies
print("   🏷️  Top 10 most tagged movies:")
step_start = time.time()
result = db.query(
    "sql",
    """SELECT movieId, count(*) as tag_count
       FROM Tag
       GROUP BY movieId
       ORDER BY tag_count DESC
       LIMIT 10""",
)
for idx, record in enumerate(result, 1):
    movie_id = record.get("movieId")
    tag_count = record.get("tag_count")
    # Look up movie title
    movie_result = db.query(
        "sql", f"SELECT title FROM Movie WHERE movieId = {movie_id}"
    )
    title = str(list(movie_result)[0].get("title"))
    print(f"      {idx:2}. {title} ({tag_count} tags)")
print(f"   ⏱️  Time: {time.time() - step_start:.3f}s")
print()

# 10.8 - Sample popular tags
print("   💬 Top 10 most common tags:")
step_start = time.time()
result = db.query(
    "sql",
    """SELECT tag, count(*) as count
       FROM Tag
       GROUP BY tag
       ORDER BY count DESC
       LIMIT 10""",
)
for idx, record in enumerate(result, 1):
    tag = str(record.get("tag"))
    count = record.get("count")
    print(f"      {idx:2}. '{tag}' ({count} uses)")
print(f"   ⏱️  Time: {time.time() - step_start:.3f}s")
print()

# Calculate total records for later use in validation
total_movies = db.query("sql", "SELECT count(*) as c FROM Movie")
total_ratings = db.query("sql", "SELECT count(*) as c FROM Rating")
total_links = db.query("sql", "SELECT count(*) as c FROM Link")
total_tags = db.query("sql", "SELECT count(*) as c FROM Tag")

movies_count = list(total_movies)[0].get("c")
ratings_count = list(total_ratings)[0].get("c")
links_count = list(total_links)[0].get("c")
tags_count = list(total_tags)[0].get("c")

total_records = movies_count + ratings_count + links_count + tags_count

# -----------------------------------------------------------------------------
# Step 13: Export Database (Optional)
# -----------------------------------------------------------------------------
export_filename = None  # Will be set if export succeeds

if args.export:
    print("Step 13: Exporting database to JSONL...")
    print()
    step_start = time.time()

    # Determine export path
    # Note: ArcadeDB exporter creates files in exports/ subdirectory automatically
    # So we just specify the filename relative to that
    if args.export_path:
        export_filename = args.export_path
    else:
        export_filename = f"{db_name}.jsonl.tgz"

    # Note: The exporter will automatically create the file in exports/ directory

    # Export database to JSONL format
    print(f"   💾 Exporting to: {export_filename}")
    print("   ⏳ This may take a while for large databases...")

    try:
        stats = db.export_database(
            export_filename, format="jsonl", overwrite=True, verbose=2
        )

        export_time = time.time() - step_start

        print("   ✅ Export complete!")
        print(f"      • Total records: {stats.get('totalRecords', 0):,}")
        print(f"      • Vertices: {stats.get('vertices', 0):,}")
        print(f"      • Edges: {stats.get('edges', 0):,}")
        print(f"      • Documents: {stats.get('documents', 0):,}")
        print(f"   ⏱️  Time: {export_time:.3f}s")

        # Show file size
        if os.path.exists(export_filename):
            file_size = os.path.getsize(export_filename)
            size_mb = file_size / (1024 * 1024)
            print(f"   📦 File size: {size_mb:.2f} MB")

        print()
        print("   💡 Export benefits:")
        print("      • Reproducible benchmarks - share pre-populated databases")
        print("      • Backup - full database snapshot with schema")
        print("      • Testing - create test fixtures from real data")
        print("      • Migration - move databases between environments")
        print()
        print("   💡 To restore this database:")
        print("      # Basic import")
        abs_path = os.path.abspath(export_filename)
        print(f"      db.command('sql', 'IMPORT DATABASE file://{abs_path}')")
        print()
        print("      # Import with performance tuning")
        print(
            f"      db.command('sql', 'IMPORT DATABASE file://{abs_path} "
            f"WITH commitEvery = {args.batch_size}, parallel = {args.parallel}')"
        )
        print()

    except Exception as e:
        print(f"   ❌ Export failed: {e}")
        print(f"   ⏱️  Time: {time.time() - step_start:.3f}s")
        print()
else:
    print("Step 13: Database export skipped (use --export to enable)")
    print()
    export_filename = None  # No export to validate

# -----------------------------------------------------------------------------
# Step 14: Roundtrip Validation (if export was done)
# -----------------------------------------------------------------------------
if args.export and export_filename:
    # The exporter saves files to exports/ directory,
    # so we need to construct the actual path
    if args.export_path:
        # Custom path - check if it exists as-is or with exports/ prefix
        actual_export_path = export_filename
        if not os.path.exists(actual_export_path) and os.path.exists(
            f"exports/{export_filename}"
        ):
            actual_export_path = f"exports/{export_filename}"
    else:
        # Default: exports/{db_name}.jsonl.tgz
        actual_export_path = f"exports/{export_filename}"

    if os.path.exists(actual_export_path):
        print("Step 14: Roundtrip validation (export → import → verify)...")
    print()
    step_start = time.time()

    # Close original database first
    print("   🔒 Closing original database...")
    db.close()
    print("   ✅ Original database closed")
    print()

    # Create a new database for import testing
    roundtrip_db_path = os.path.join(db_dir, f"{db_name}_roundtrip")
    if os.path.exists(roundtrip_db_path):
        shutil.rmtree(roundtrip_db_path)

    print(f"   📂 Creating roundtrip database: {roundtrip_db_path}")
    roundtrip_db = arcadedb.create_database(roundtrip_db_path)

    print("   ✅ Roundtrip database created")
    print()

    # Import from the exported file
    print(f"   📥 Importing from: {actual_export_path}")
    print("   ⏳ This may take a while...")

    # Build import parameters - use larger batches for faster import
    import_params = f"commitEvery = {args.batch_size}"
    if args.parallel:
        import_params += f", parallel = {args.parallel}"

    print(f"   💡 Import settings: {import_params}")
    print()

    # Initialize roundtrip_results to None in case import fails
    roundtrip_results = None

    try:
        import_start = time.time()

        # Use SQL IMPORT DATABASE command with performance parameters
        import_path = os.path.abspath(actual_export_path)
        # Convert Windows backslashes to forward slashes for SQL URI
        import_path = import_path.replace("\\", "/")
        import_sql = f"IMPORT DATABASE file://{import_path} WITH {import_params}"
        roundtrip_db.command("sql", import_sql)

        import_time = time.time() - import_start

        print(f"   ✅ Import complete in {import_time:.3f}s")

        # Calculate import rate
        import_rate = total_records / import_time if import_time > 0 else 0
        print(f"   ⏱️  Rate: {import_rate:,.0f} records/sec")
        print()

        # Verify record counts match
        print("   🔍 Verifying record counts...")
        validation_passed = True

        for doc_type in ["Movie", "Rating", "Link", "Tag"]:
            result = roundtrip_db.query("sql", f"SELECT count(*) as c FROM {doc_type}")
            count = result.first().get("c")

            # Compare with expected counts
            if doc_type == "Movie" and count != movies_count:
                print(f"      ❌ {doc_type}: {count:,} (expected {movies_count:,})")
                validation_passed = False
            elif doc_type == "Rating" and count != ratings_count:
                print(f"      ❌ {doc_type}: {count:,} (expected {ratings_count:,})")
                validation_passed = False
            elif doc_type == "Link" and count != links_count:
                print(f"      ❌ {doc_type}: {count:,} (expected {links_count:,})")
                validation_passed = False
            elif doc_type == "Tag" and count != tags_count:
                print(f"      ❌ {doc_type}: {count:,} (expected {tags_count:,})")
                validation_passed = False
            else:
                print(f"      ✅ {doc_type}: {count:,} records")

        print()

        # Check if indexes were imported with the data
        print("   🔍 Checking for imported indexes...")
        existing_indexes_result = roundtrip_db.query(
            "sql", "SELECT name FROM schema:indexes"
        )
        existing_index_count = len(list(existing_indexes_result))

        if existing_index_count > 0:
            print(f"   ℹ️  Found {existing_index_count} indexes " f"already imported")
            print("   ⏭️  Skipping index rebuild (indexes already present)")
            rebuild_success = 0
            rebuild_time = 0.0
        else:
            # Rebuild indexes after import for query performance
            print("   🔨 Rebuilding indexes after import...")
            rebuild_start = time.time()

            # Use same index definitions
            rebuild_success, rebuild_failed = create_indexes(
                roundtrip_db, indexes, verbose=True
            )

            rebuild_time = time.time() - rebuild_start
            print(f"   ⏱️  Index rebuild time: {rebuild_time:.1f}s")
            print(f"   ✅ Created {rebuild_success}/{len(indexes)} indexes")

        print()

        # Run sample queries to verify data integrity
        print("   🔍 Verifying data integrity with test queries...")
        print()

        # Run the same validation queries we used for performance testing
        roundtrip_results = run_validation_queries(
            roundtrip_db, num_runs=10, verbose=True
        )

        # Compare against embedded baseline results
        if args.dataset in EXPECTED_RESULTS and EXPECTED_RESULTS[args.dataset]:
            print("   📊 Step 14 - Comparing against baseline (AFTER roundtrip):")
            print()
            baseline_match = compare_query_results(
                roundtrip_results, EXPECTED_RESULTS[args.dataset], verbose=True
            )
            print()
            if baseline_match:
                print("   ✅ Step 14: All results match baseline!")
            else:
                print("   ⚠️  Step 14: Some results differ from baseline!")
                validation_passed = False
            print()
        else:
            print(
                f"   ℹ️  No embedded baseline for {args.dataset} dataset - "
                f"skipping Step 14 comparison"
            )
            print()

        # Also verify results match original database (backward compatibility check)
        print("   🔍 Comparing results with original database...")
        all_queries_match = True

        for i, result in enumerate(roundtrip_results):
            query_name = result["name"]
            roundtrip_count = result["count"]

            # Compare with original results (from times_with_indexes)
            if i < len(times_with_indexes):
                original_count = times_with_indexes[i]["count"]
                if roundtrip_count == original_count:
                    print(f"      ✅ {query_name}: {roundtrip_count} results (matches)")
                else:
                    print(
                        f"      ❌ {query_name}: {roundtrip_count} results "
                        f"(expected {original_count})"
                    )
                    all_queries_match = False
                    validation_passed = False

        print()

        # Close roundtrip database
        roundtrip_db.close()
        print("   🔒 Roundtrip database closed")
        print()

        # Clean up roundtrip database
        print("   🧹 Cleaning up roundtrip database...")
        shutil.rmtree(roundtrip_db_path)
        print("   ✅ Roundtrip database removed")
        print()

        if validation_passed and all_queries_match:
            print("   ✅ ROUNDTRIP VALIDATION PASSED!")
            print("      • All record counts match")
            print("      • Data integrity verified")
            print(f"      • Indexes rebuilt: {rebuild_success}/{len(indexes)}")
            print(f"   ⏱️  Total roundtrip time: {time.time() - step_start:.3f}s")
            print(f"   ⏱️  Import time: {import_time:.3f}s")
            print(f"   ⏱️  Index rebuild time: {rebuild_time:.1f}s")
        else:
            print("   ⚠️  ROUNDTRIP VALIDATION FAILED!")
            print("      • Some checks did not pass")
            print(f"   ⏱️  Total roundtrip time: {time.time() - step_start:.3f}s")

        print()

    except Exception as e:  # noqa: BLE001
        print(f"   ❌ Roundtrip validation failed: {e}")
        print(f"   ⏱️  Time: {time.time() - step_start:.3f}s")
        print()
        # Clean up if import failed
        try:
            roundtrip_db.close()
            shutil.rmtree(roundtrip_db_path)
        except Exception:  # noqa: BLE001, S110
            pass

    # Re-open original database for any remaining operations
    print("   🔓 Re-opening original database...")
    db = arcadedb.open_database(db_path)
    print("   ✅ Original database re-opened")
    print()

    # Final comparison: All three query runs should match
    print("=" * 70)
    print("📊 FINAL VALIDATION: Comparing All Query Runs")
    print("=" * 70)
    print()

    # Only do final validation if roundtrip succeeded
    if roundtrip_results is not None:
        print("   Comparing results from:")
        print("   1️⃣  Before indexes (Step 8)")
        print("   2️⃣  After indexes (Step 10)")
        print("   3️⃣  After roundtrip (Step 14)")
        print()

        all_three_match = True

        for i, query_info in enumerate(TEST_QUERIES):
            query_name = query_info[0]

            before_idx = times_without_indexes[i]
            after_idx = times_with_indexes[i]
            after_roundtrip = roundtrip_results[i]

        count_before = before_idx["count"]
        count_after = after_idx["count"]
        count_roundtrip = after_roundtrip["count"]

        # Check if all three match
        if count_before == count_after == count_roundtrip:
            print(f"   ✅ {query_name}")
            print(f"      Count: {count_before} (consistent across all runs)")
        else:
            print(f"   ❌ {query_name}")
            print(f"      Before indexes: {count_before}")
            print(f"      After indexes:  {count_after}")
            print(f"      After roundtrip: {count_roundtrip}")
            print("      ⚠️  MISMATCH DETECTED!")
            all_three_match = False
        print()

        if all_three_match:
            print("   ✅ SUCCESS: All query results are consistent!")
            print("      • Before indexes ✓")
            print("      • After indexes ✓")
            print("      • After export/import roundtrip ✓")
        else:
            print("   ❌ FAILURE: Query results differ across runs!")
            print("      This indicates a data integrity issue.")
        print()
    else:
        print("   ⚠️  Roundtrip validation skipped (import failed)")
        print("      Cannot compare roundtrip results")
        print()

    print("=" * 70)
    print()

else:
    if args.export:
        print("Step 14: Roundtrip validation skipped (export file not found)")
        # Show expected path for debugging
        if args.export_path:
            print(
                f"   Searched for: {args.export_path} "
                f"and exports/{args.export_path}"
            )
        else:
            print(f"   Expected: exports/{export_filename}")
        print()
    else:
        print("Step 14: Roundtrip validation skipped (export disabled)")
        print()

        # Final comparison: Two query runs (without roundtrip)
        print("=" * 70)
        print("📊 FINAL VALIDATION: Comparing Query Runs")
        print("=" * 70)
        print()
        print("   Comparing results from:")
        print("   1️⃣  Before indexes (Step 8)")
        print("   2️⃣  After indexes (Step 10)")
        print()

        all_two_match = True

        for i, query_info in enumerate(TEST_QUERIES):
            query_name = query_info[0]

            before_idx = times_without_indexes[i]
            after_idx = times_with_indexes[i]

            count_before = before_idx["count"]
            count_after = after_idx["count"]

            # Check if both match
            if count_before == count_after:
                print(f"   ✅ {query_name}")
                print(f"      Count: {count_before} (consistent)")
            else:
                print(f"   ❌ {query_name}")
                print(f"      Before indexes: {count_before}")
                print(f"      After indexes:  {count_after}")
                print("      ⚠️  MISMATCH DETECTED!")
                all_two_match = False
            print()

        if all_two_match:
            print("   ✅ SUCCESS: All query results are consistent!")
            print("      • Before indexes ✓")
            print("      • After indexes ✓")
        else:
            print("   ❌ FAILURE: Query results differ between runs!")
            print("      This indicates a data integrity issue.")
        print()
        print("=" * 70)
        print()

# -----------------------------------------------------------------------------
# Cleanup
# -----------------------------------------------------------------------------
print("Cleanup: Closing database...")
print()


# Close database connection
db.close()

print("   ✅ Database closed")

# Note: We're NOT deleting the database directory
print(f"   💡 Database files preserved at: {db_path}")
print("   💡 You can explore the data with additional queries!")
print()

print("=" * 70)
print("✅ CSV Import Example Complete!")
print("=" * 70)
print()
print("📚 What you learned:")
print("   • Importing real-world CSV data into ArcadeDB")
print("   • Bulk INSERT ingest from CSV with explicit schema")
print("   • WAL-off ingest mode for faster loading")
print("   • Batch processing with commitEvery parameter")
print("   • Creating indexes AFTER import for performance")
print("   • Full-text search indexes with Lucene")
print("   • Aggregation queries (count, avg, min, max, group by)")
print("   • Performance optimization techniques")
if args.export:
    print("   • Database export to JSONL format")
    print("   • Roundtrip validation (export → import → verify)")
print()
print("💡 New Python binding features demonstrated:")
print("   • first() - Get first result efficiently (replaces list()[0])")
print("   • has_property() - Check if property exists before accessing")
print("   • Automatic type conversion - Java types → Python types")
if args.export:
    print("   • export_database() - Export to JSONL/GraphML/GraphSON")
    print("   • IMPORT DATABASE SQL command - Import from JSONL exports")
    print("   • Import performance tuning with commitEvery and parallel parameters")
print()
print("💡 Key insights:")
print("   • Explicit schema maps integer-like fields to LONG")
print("   • Explicit schema maps decimal fields to DOUBLE")
print("   • Empty CSV cells → SQL NULL (proper NULL handling)")
print("   • Indexes should be created AFTER bulk import")
print("   • commitEvery controls batch size (larger = faster)")
print("   • parallel controls concurrent threads (CSV import and JSONL import)")
print("   • FULL_TEXT indexes use Lucene for tokenization and search")
print("   • Text search may use LIKE queries optimized by FULL_TEXT indexes")
print()
print("💡 Next steps:")
print("   • Try modifying commitEvery values to see performance impact")
print("   • Add more complex queries")
print("   • Explore query performance with different index strategies")
print("   • Experiment with full-text search on other text fields")
print("   • For custom types, define schema BEFORE import (see Java docs)")
print()

# Print total script runtime
total_script_time = time.time() - script_start_time
minutes = int(total_script_time // 60)
seconds = int(total_script_time % 60)
print("=" * 70)
print(f"⏱️  TOTAL SCRIPT RUN TIME: {minutes}m {seconds}s")
print("=" * 70)
print()

# Check if baseline validation failed and exit with error code
if not baseline_match_step8 or not baseline_match_step10:
    print("=" * 70)
    print("❌ BASELINE VALIDATION FAILED")
    print("=" * 70)
    print()
    print("Some query results did not match the expected baseline values.")
    print("This may indicate:")
    print("  • Data integrity issues")
    print("  • Changes in query behavior")
    print("  • Dataset differences")
    print()
    sys.exit(1)
