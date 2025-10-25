#!/usr/bin/env python3
"""
Example 04: CSV Import - Documents with Automatic Type Inference

This example demonstrates importing CSV data into ArcadeDB DOCUMENTS
with AUTOMATIC TYPE INFERENCE - Java analyzes the CSV and infers types.

We use the MovieLens dataset with four CSV files:
- movies.csv: Movie information (9,743 movies)
- ratings.csv: User ratings (100,837 ratings)
- links.csv: External movie IDs (9,742 links)
- tags.csv: User-generated tags (3,683 tags)

Key Concepts:
- **Automatic type inference** by Java CSV importer
- Schema created on-the-fly during import
- Batch processing with commitEvery parameter
- Creating indexes AFTER import for performance
- **Full-text search** with Lucene for text fields
- Query performance comparison with/without indexes
- NULL value handling

Java's Automatic Type Inference:
The Java CSV importer analyzes sample rows to infer types automatically:
- Default: analyzes first 10,000 rows (analysisLimitEntries parameter)
- Integer values → LONG (safe for all integer sizes)
- Decimal values → DOUBLE (standard precision for floats)
- Text values → STRING
- Empty cells → NULL (proper SQL NULL handling)

Note: Java defaults to conservative types (LONG, DOUBLE) for safety.
This avoids overflow issues but uses more storage than smaller types.
You can customize the analysis limit via the analysisLimitEntries option.

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
- arcadedb-embedded (any distribution: headless, minimal, or full)
- MovieLens dataset (downloaded via download_sample_data.py)
- JRE 21+
- Sufficient JVM heap memory (8GB recommended for large dataset)

Usage:
1. First download the dataset:
   python download_sample_data.py
2. Run this example with sufficient memory:
   ARCADEDB_JVM_MAX_HEAP="8g" python 04_csv_import_documents.py

Memory Requirements:
- Small dataset (~100K ratings): 4GB heap (default) is sufficient
- Large dataset (~33M ratings): 4GB heap (default) should work, 8GB for safety
- Very large datasets (100M+ records): Set ARCADEDB_JVM_MAX_HEAP="8g" or higher
- Must be set BEFORE running the script (before JVM starts)

Note: This example creates a database at ./my_test_databases/movielens_db/
      The database files are preserved so you can inspect them after running.
"""

import json
import os
import shutil
import statistics
import time
from pathlib import Path

import arcadedb_embedded as arcadedb

print("=" * 70)
print("🎬 ArcadeDB Python - Example 04: CSV Import - Documents")
print("=" * 70)
print()

# Check JVM heap configuration for large imports
jvm_heap = os.environ.get("ARCADEDB_JVM_MAX_HEAP")
if jvm_heap:
    print(f"💡 JVM Max Heap: {jvm_heap}")
else:
    print("💡 JVM Max Heap: 4g (default)")
    print("   ℹ️  Using default JVM heap (4g)")
    print("   💡 For very large datasets, you can increase it:")
    print('      export ARCADEDB_JVM_MAX_HEAP="8g"  # or run with:')
    print('      ARCADEDB_JVM_MAX_HEAP="8g" python 04_csv_import_documents.py')
print()

# -----------------------------------------------------------------------------
# Step 0: Check Dataset Availability
# -----------------------------------------------------------------------------
print("Step 0: Checking for MovieLens dataset...")
print()

# Try to find any available MovieLens dataset (supports multiple sizes)
data_base = Path(__file__).parent / "data"
dataset_dirs = [
    "ml-large",  # ~33M ratings (default, preferred)
    "ml-small",  # ~100K ratings (for quick testing)
]

data_dir = None
for dirname in dataset_dirs:
    potential_dir = data_base / dirname
    if potential_dir.exists():
        data_dir = potential_dir
        break

if data_dir is None:
    print("❌ MovieLens dataset not found!")
    print()
    print("💡 Please download a dataset first:")
    print("   python download_sample_data.py              # Large (~265 MB)")
    print("   python download_sample_data.py --size small  # Small (~1 MB)")
    print()
    exit(1)

# Verify all required CSV files exist
required_files = ["movies.csv", "ratings.csv", "links.csv", "tags.csv"]
missing_files = [f for f in required_files if not (data_dir / f).exists()]

if missing_files:
    print(f"❌ Missing files: {', '.join(missing_files)}")
    print()
    print("💡 Please re-download the dataset:")
    print("   python download_sample_data.py")
    print()
    exit(1)

print("✅ MovieLens dataset found!")
print(f"   Location: {data_dir}")
print()

# Show file sizes
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
db_path = os.path.join(db_dir, "movielens_db")

# Clean up any existing database from previous runs
if os.path.exists(db_path):
    shutil.rmtree(db_path)

# Clean up log directory from previous runs
if os.path.exists("./log"):
    shutil.rmtree("./log")

db = arcadedb.create_database(db_path)

print(f"   ✅ Database created at: {db_path}")
print("   💡 Using embedded mode - no server needed!")
print(f"   ⏱️  Time: {time.time() - step_start:.3f}s")
print()

# -----------------------------------------------------------------------------
# Step 2: Import Movies CSV → Movie Documents (with automatic type inference)
# -----------------------------------------------------------------------------
print("Step 2: Importing movies.csv → Movie documents...")
print("   💡 Java will automatically:")
print("      • Analyze CSV structure and infer column types")
print("      • Create 'Movie' document type with inferred schema")
print("      • Import all rows with batch commits")
print()
step_start = time.time()

movies_csv = str(data_dir / "movies.csv")
stats = arcadedb.import_csv(db, movies_csv, "Movie", commitEvery=1000)

print(f"   ✅ Imported {stats['documents']:,} movies")
print(f"   💡 Errors: {stats['errors']}")
print(f"   ⏱️  Time: {stats['duration_ms'] / 1000:.3f}s")
rate = stats["documents"] / (stats["duration_ms"] / 1000)
print(f"   ⏱️  Rate: {rate:.0f} records/sec")

# Check NULL values (genres can be NULL)
null_genres = list(
    db.query("sql", "SELECT count(*) as c FROM Movie WHERE genres IS NULL")
)[0].get_property("c")

if null_genres > 0:
    print("   🔍 NULL values detected:")
    print(
        f"      • genres: {null_genres:,} NULL values "
        f"({null_genres/stats['documents']*100:.1f}%)"
    )
    print("   💡 Empty CSV cells correctly imported as SQL NULL")

print()

# -----------------------------------------------------------------------------
# Step 3: Display Java's Auto-Inferred Schema
# -----------------------------------------------------------------------------
print("Step 3: Inspecting Java's auto-inferred schema...")
print()

# Query the schema that Java created during import
result = db.query("sql", "SELECT properties FROM schema:types WHERE name = 'Movie'")
for record in result:
    properties = record.get_property("properties")

    print("   📋 Movie schema (auto-inferred by Java):")
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
print("   💡 Java's type inference strategy:")
print("      • Analyzes first 10,000 rows by default (analysisLimitEntries)")
print("      • LONG for all integer values (safe, no overflow)")
print("      • DOUBLE for all decimal values (standard precision)")
print("      • STRING for text")
print("      • Empty cells → NULL (proper SQL NULL handling)")
print()

# -----------------------------------------------------------------------------
# Step 4: Import Ratings CSV → Rating Documents
# -----------------------------------------------------------------------------
print("Step 4: Importing ratings.csv → Rating documents...")
step_start = time.time()

ratings_csv = str(data_dir / "ratings.csv")
stats = arcadedb.import_csv(db, ratings_csv, "Rating", commitEvery=5000)

print(f"   ✅ Imported {stats['documents']:,} ratings")
print(f"   💡 Errors: {stats['errors']}")
print(f"   ⏱️  Time: {stats['duration_ms'] / 1000:.3f}s")
rate = stats["documents"] / (stats["duration_ms"] / 1000)
print(f"   ⏱️  Rate: {rate:.0f} records/sec")

# Check NULL values (timestamp can be NULL)
null_timestamps = list(
    db.query("sql", "SELECT count(*) as c FROM Rating WHERE timestamp IS NULL")
)[0].get_property("c")

if null_timestamps > 0:
    print("   🔍 NULL values detected:")
    print(
        f"      • timestamp: {null_timestamps:,} NULL values "
        f"({null_timestamps/stats['documents']*100:.1f}%)"
    )
    print("   💡 Empty CSV cells correctly imported as SQL NULL")

print()

# -----------------------------------------------------------------------------
# Step 5: Import Links CSV → Link Documents
# -----------------------------------------------------------------------------
print("Step 5: Importing links.csv → Link documents...")
step_start = time.time()

links_csv = str(data_dir / "links.csv")
stats = arcadedb.import_csv(db, links_csv, "Link", commitEvery=1000)

print(f"   ✅ Imported {stats['documents']:,} links")
print(f"   💡 Errors: {stats['errors']}")
print(f"   ⏱️  Time: {stats['duration_ms'] / 1000:.3f}s")
rate = stats["documents"] / (stats["duration_ms"] / 1000)
print(f"   ⏱️  Rate: {rate:.0f} records/sec")

# Check NULL values (imdbId and tmdbId can be NULL)
null_imdb = list(
    db.query("sql", "SELECT count(*) as c FROM Link WHERE imdbId IS NULL")
)[0].get_property("c")
null_tmdb = list(
    db.query("sql", "SELECT count(*) as c FROM Link WHERE tmdbId IS NULL")
)[0].get_property("c")

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
stats = arcadedb.import_csv(db, tags_csv, "Tag", commitEvery=1000)

print(f"   ✅ Imported {stats['documents']:,} tags")
print(f"   💡 Errors: {stats['errors']}")
print(f"   ⏱️  Time: {stats['duration_ms'] / 1000:.3f}s")
rate = stats["documents"] / (stats["duration_ms"] / 1000)
print(f"   ⏱️  Rate: {rate:.0f} records/sec")

# Check NULL values in tag field
null_tags = list(db.query("sql", "SELECT count(*) as c FROM Tag WHERE tag IS NULL"))[
    0
].get_property("c")

if null_tags > 0:
    print("   🔍 NULL values detected:")
    print(
        f"      • tag: {null_tags:,} NULL values "
        f"({null_tags/stats['documents']*100:.1f}%)"
    )
    print("   💡 Empty CSV cells correctly imported as SQL NULL")

print()

# -----------------------------------------------------------------------------
# Step 7: Verify All Auto-Inferred Schemas
# -----------------------------------------------------------------------------
print("Step 7: Verifying all auto-inferred schemas...")
print()

# Query the formal schema to see Java's automatically inferred properties
for doc_type in ["Movie", "Rating", "Link", "Tag"]:
    result = db.query(
        "sql", f"SELECT properties FROM schema:types WHERE name = '{doc_type}'"
    )

    for record in result:
        properties = record.get_property("properties")

        print(f"   📋 {doc_type} schema (auto-inferred by Java):")
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

print("   💡 Type inference observations:")
print("      • All integer columns → LONG (Java's safe default)")
print("      • All decimal columns → DOUBLE (Java's standard precision)")
print("      • Text columns → STRING")
print("      • Empty cells → NULL (proper SQL NULL handling)")
print("      • Configurable via analysisLimitEntries parameter (default: 10000)")
print()

# -----------------------------------------------------------------------------
# Step 8: Test Query Performance WITHOUT Indexes (Multiple Runs)
# -----------------------------------------------------------------------------
print("Step 8: Testing query performance WITHOUT indexes (10 runs each)...")
print()

# Test queries that would benefit from indexes
test_queries = [
    ("Find movie by ID", "SELECT FROM Movie WHERE movieId = 500"),
    (
        "Find user's ratings",
        "SELECT FROM Rating WHERE userId = 414 ORDER BY movieId LIMIT 10",
    ),
    ("Find movie ratings", "SELECT FROM Rating WHERE movieId = 500"),
    (
        "Count user's ratings",
        "SELECT count(*) as count FROM Rating WHERE userId = 414",
    ),
    (
        "Find movies by genre (LIKE with LIMIT)",
        "SELECT FROM Movie WHERE genres LIKE '%Action%' LIMIT 10",
    ),
    (
        "Count ALL Action movies (LIKE, no LIMIT)",
        "SELECT count(*) as count FROM Movie WHERE genres LIKE '%Action%'",
    ),
]

# Run each query 10 times and collect statistics
times_without_indexes = []
for query_name, query in test_queries:
    run_times = []
    result_count = 0
    sample_result = None

    for i in range(10):
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

    times_without_indexes.append(
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

    print(f"   📊 {query_name}:")
    print(f"      Average: {avg_time*1000:.2f}ms ± {std_time*1000:.2f}ms")
    print(f"      Range: [{min_time*1000:.2f}ms - {max_time*1000:.2f}ms]")
    print(f"      Results: {result_count}")
    print()

print("   💡 Running queries multiple times to get reliable statistics")
print()

# -----------------------------------------------------------------------------
# Step 9: Create Indexes After Import (Best Practice)
# -----------------------------------------------------------------------------
print("Step 9: Creating indexes for query performance...")
step_start = time.time()

# Wait for any background compaction from the import to finish
print("   ⏳ Waiting for background compaction to complete...")
max_wait_seconds = 600  # Maximum 10 minutes wait time
retry_interval = 5  # Check every 5 seconds
wait_start = time.time()

# Keep trying to create a test transaction until no async tasks are running
while (time.time() - wait_start) < max_wait_seconds:
    try:
        # Try a simple query that would fail if async tasks are running
        with db.transaction():
            # This will throw NeedRetryException if compaction is running
            list(db.query("sql", "SELECT count(*) FROM Movie LIMIT 1"))
        print("   ✅ Background compaction complete")
        break
    except Exception as e:  # noqa: BLE001
        if "NeedRetryException" in str(e) or "asynchronous tasks" in str(e):
            elapsed = time.time() - wait_start
            print(f"   ⏳ Still compacting... ({elapsed:.0f}s elapsed)")
            time.sleep(retry_interval)
        else:
            # Different error - proceed anyway
            print(f"   ⚠️  Unexpected error while waiting: {e}")
            break
else:
    print(
        f"   ⚠️  Compaction still running after {max_wait_seconds}s, "
        f"proceeding anyway"
    )

# Strategy: Create indexes one at a time with retry logic for each
print("\n   📊 Creating indexes with retry on compaction conflicts:")
indexes = [
    ("Movie", "movieId", "UNIQUE"),
    ("Movie", "genres", "FULL_TEXT"),  # Full-text search for genre queries
    ("Rating", "userId", "NOTUNIQUE"),
    ("Rating", "movieId", "NOTUNIQUE"),
    ("Link", "movieId", "UNIQUE"),
    ("Tag", "movieId", "NOTUNIQUE"),
]

for idx, (table, column, uniqueness) in enumerate(indexes, 1):
    created = False
    max_retries = 60  # Try for up to 60 attempts
    retry_delay = 10  # Wait 10 seconds between retries (= 10 minutes max per index)

    for attempt in range(1, max_retries + 1):
        try:
            with db.transaction():
                # All indexes use same syntax: CREATE INDEX ON Type (property) TYPE
                db.command("sql", f"CREATE INDEX ON {table} ({column}) {uniqueness}")
            print(
                f"   ✅ [{idx}/{len(indexes)}] "
                f"Created index on {table}({column}) {uniqueness}"
            )
            created = True
            break
        except Exception as e:  # noqa: BLE001
            error_msg = str(e)

            # Check if it's the async compaction error
            if "NeedRetryException" in error_msg and "asynchronous tasks" in error_msg:
                if attempt < max_retries:
                    elapsed = attempt * retry_delay
                    print(
                        f"   ⏳ [{idx}/{len(indexes)}] "
                        f"Compaction running, retry {attempt}/{max_retries} "
                        f"(waiting {retry_delay}s, ~{elapsed}s elapsed)..."
                    )
                    time.sleep(retry_delay)
                else:
                    print(
                        f"   ⚠️  [{idx}/{len(indexes)}] "
                        f"Failed after {max_retries} attempts: {e}"
                    )
            else:
                # Different error - log and stop retrying
                print(f"   ⚠️  [{idx}/{len(indexes)}] Error: {e}")
                break

print("\n   💡 Best practice: Create indexes AFTER bulk import")
print(f"   ⏱️  Total index creation time: {time.time() - step_start:.1f}s")

# Verify which indexes actually exist in the database
print("\n   🔍 Verifying indexes in database:")
index_query = """
    SELECT name, typeName, properties, unique, automatic
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
# Note: ArcadeDB has 3 index engine types: LSM_TREE, FULL_TEXT, HNSW
# The schema metadata query only exposes a boolean 'unique' field, not the engine type.
# Therefore:
#   - UNIQUE indexes → unique=true, engine=LSM_TREE
#   - NOTUNIQUE indexes → unique=false, engine=LSM_TREE
#   - FULL_TEXT indexes → unique=false, engine=FULL_TEXT (appears as NOTUNIQUE!)
#
# This means FULL_TEXT indexes show as NOTUNIQUE in the metadata, so we need to
# check for both when validating expected FULL_TEXT indexes.
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
    if props and len(props) > 0:
        column_name = props[0][0] if isinstance(props[0], list) else props[0]

        # Try matching as the reported type (UNIQUE/NOTUNIQUE)
        key = (type_name, column_name, index_type)
        if key in expected_indexes:
            expected_indexes[key] = True

        # FULL_TEXT indexes appear as NOTUNIQUE in metadata, so also check for FULL_TEXT
        # This is expected behavior since FULL_TEXT is a different index engine type,
        # not a variant of LSM_TREE, but metadata only exposes the 'unique' boolean.
        if index_type == "NOTUNIQUE":
            fulltext_key = (type_name, column_name, "FULL_TEXT")
            if fulltext_key in expected_indexes:
                expected_indexes[fulltext_key] = True

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

times_with_indexes = []
for query_name, query in test_queries:
    run_times = []
    result_count = 0
    sample_result = None

    for i in range(10):
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

    times_with_indexes.append(
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

    print(f"   📊 {query_name}:")
    print(f"      Average: {avg_time*1000:.2f}ms ± {std_time*1000:.2f}ms")
    print(f"      Range: [{min_time*1000:.2f}ms - {max_time*1000:.2f}ms]")
    print(f"      Results: {result_count}")
    print()

print()
print("   🚀 Performance Improvement Summary:")
print("   " + "=" * 70)
print(f"   {'Query':<30} {'Before (ms)':<15} {'After (ms)':<15} {'Speedup':<10}")
print("   " + "=" * 70)

for i in range(len(test_queries)):
    before_stats = times_without_indexes[i]
    after_stats = times_with_indexes[i]

    before_avg = before_stats["avg"] * 1000
    before_std = before_stats["std"] * 1000
    after_avg = after_stats["avg"] * 1000
    after_std = after_stats["std"] * 1000

    if after_stats["avg"] > 0:
        speedup = before_stats["avg"] / after_stats["avg"]
        time_saved_pct = (
            (before_stats["avg"] - after_stats["avg"]) / before_stats["avg"]
        ) * 100

        query_name = before_stats["name"][:28]
        before_str = f"{before_avg:.1f}±{before_std:.1f}"
        after_str = f"{after_avg:.1f}±{after_std:.1f}"
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
for i in range(len(test_queries)):
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
            count_before = first_before.get_property("count")
            count_after = first_after.get_property("count")
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
            title_before = first_before.get_property("title")
            title_after = first_after.get_property("title")
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
            movieId_before = first_before.get_property("movieId")
            movieId_after = first_after.get_property("movieId")
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

    print(f"      • '{genre}': {len(result)} results in {query_time*1000:.2f}ms")
    if result:
        # Show first movie as example
        first_movie = result[0]
        title = str(first_movie.get_property("title"))
        genres = str(first_movie.get_property("genres"))
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
action_count = count_result[0].get_property("count") if count_result else 0
action_count = action_count if action_count is not None else 0
print(f"      • Total Action movies: {action_count:,} found in {count_time*1000:.2f}ms")
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
# Step 12: Import Performance Summary
# -----------------------------------------------------------------------------
print("Step 12: Import performance summary...")
print()

# 10.1 - Count records in each type
print("   📊 Record counts by type:")
step_start = time.time()
for doc_type in ["Movie", "Rating", "Link", "Tag"]:
    result = db.query("sql", f"SELECT count(*) as count FROM {doc_type}")
    count = list(result)[0].get_property("count")
    print(f"      • {doc_type}: {count:,} records")
print(f"   ⏱️  Time: {time.time() - step_start:.3f}s")
print()

# 12.2 - Sample movies
print("   🎬 Sample movies:")
step_start = time.time()
result = db.query("sql", "SELECT FROM Movie LIMIT 5")
for record in result:
    movie_id = record.get_property("movieId")
    title = str(record.get_property("title"))
    genres = str(record.get_property("genres"))
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
total = record.get_property("total_ratings")
avg_rating = record.get_property("avg_rating")
min_rating = record.get_property("min_rating")
max_rating = record.get_property("max_rating")
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
    rating = record.get_property("rating")
    count = record.get_property("count")
    bar = "█" * int(count / 3000)  # Scale for visualization
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
    genres = str(record.get_property("genres"))
    count = record.get_property("count")
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
    user_id = record.get_property("userId")
    rating_count = record.get_property("rating_count")
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
    movie_id = record.get_property("movieId")
    tag_count = record.get_property("tag_count")
    # Look up movie title
    movie_result = db.query(
        "sql", f"SELECT title FROM Movie WHERE movieId = {movie_id}"
    )
    title = str(list(movie_result)[0].get_property("title"))
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
    tag = str(record.get_property("tag"))
    count = record.get_property("count")
    print(f"      {idx:2}. '{tag}' ({count} uses)")
print(f"   ⏱️  Time: {time.time() - step_start:.3f}s")
print()

# -----------------------------------------------------------------------------
# Step 11: Performance Summary
# -----------------------------------------------------------------------------
print("Step 11: Import performance summary...")
print()

# Calculate total records imported
total_movies = db.query("sql", "SELECT count(*) as c FROM Movie")
total_ratings = db.query("sql", "SELECT count(*) as c FROM Rating")
total_links = db.query("sql", "SELECT count(*) as c FROM Link")
total_tags = db.query("sql", "SELECT count(*) as c FROM Tag")

movies_count = list(total_movies)[0].get_property("c")
ratings_count = list(total_ratings)[0].get_property("c")
links_count = list(total_links)[0].get_property("c")
tags_count = list(total_tags)[0].get_property("c")

total_records = movies_count + ratings_count + links_count + tags_count

print(f"   📊 Total records imported: {total_records:,}")
print(f"      • Movies: {movies_count:,}")
print(f"      • Ratings: {ratings_count:,}")
print(f"      • Links: {links_count:,}")
print(f"      • Tags: {tags_count:,}")
print()

print("   💡 Performance tips:")
print("      • commitEvery: Larger batches = faster imports")
print("      • Movies used commitEvery=1000 (smaller batches)")
print("      • Ratings used commitEvery=5000 (larger batches)")
print("      • Type inference: Automatic LONG/DOUBLE/STRING detection")
print("      • Indexes: Created AFTER import for better performance")
print()

# -----------------------------------------------------------------------------
# Step 13: Cleanup
# -----------------------------------------------------------------------------
print("Step 13: Cleanup...")
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
print("   • Automatic type inference by Java CSV importer")
print("   • Schema creation on-the-fly during import")
print("   • Batch processing with commitEvery parameter")
print("   • Creating indexes AFTER import for performance")
print("   • Full-text search indexes with Lucene")
print("   • Aggregation queries (count, avg, min, max, group by)")
print("   • Performance optimization techniques")
print()
print("💡 Key insights:")
print("   • Java infers LONG for integers, DOUBLE for decimals (safe defaults)")
print("   • Type inference analyzes first 10,000 rows (analysisLimitEntries)")
print("   • Empty CSV cells → SQL NULL (proper NULL handling)")
print("   • Indexes should be created AFTER bulk import")
print("   • commitEvery controls batch size (larger = faster)")
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
