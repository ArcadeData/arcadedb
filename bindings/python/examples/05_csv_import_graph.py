#!/usr/bin/env python3
"""
Graph Creation Benchmark - Clean Architecture

This benchmark compares graph creation strategies with multiple options:
- Method: Java API vs SQL
- Async: Async executor (parallel) vs Synchronous (sequential)
- Indexes: With indexes vs Without indexes

Architecture:
=============
- Shared data loading functions (properly paginated)
- Shared vertex creation logic
- Shared edge creation logic
- Method-specific executors (Java API vs SQL)
- Async executor support:
  * Java API: Fully supported via BatchContext (RECOMMENDED FOR PERFORMANCE)
  * SQL: Limited support - concurrent modifications occur during bulk vertex
         creation. Works better for edge creation and updates.
- Index-aware implementations

Async Executor:
===============
The async executor enables parallel processing with configurable worker threads:

**Java API + Async (RECOMMENDED FOR BEST PERFORMANCE):**
- Uses BatchContext which wraps the async executor
- Handles concurrent page modifications internally
- Parallel vertex and edge creation
- Command: `--method java` (async enabled by default)

**SQL Mode (ALWAYS SYNCHRONOUS FOR VERTICES):**
- Direct SQL commands via transactions
- Avoids ConcurrentModificationException (multiple threads → same pages)
- Sequential processing (one operation at a time)
- Command: `--method sql`
- The `--no-async` flag has no effect on SQL (always synchronous)

Note: Async executor with SQL INSERT causes concurrent modification errors
during bulk vertex creation, so SQL mode always uses synchronous transactions.

Proper Database-Level Streaming:
=================================
ALL queries use LIMIT-based pagination to avoid loading entire result sets:
- Link data: Paginated with @rid > {last_rid} LIMIT {batch_size}
- Movies: Paginated with @rid > {last_rid} LIMIT {batch_size}
- Ratings: Paginated with @rid > {last_rid} LIMIT {batch_size}
- Tags: Paginated with @rid > {last_rid} LIMIT {batch_size}

Exception: User vertices use `SELECT COUNT(*) as count FROM (SELECT DISTINCT FROM ...)`
(difficult to paginate efficiently)

Dataset Sources:
----------------
Two options for loading source data:

**Option 1: Document Database (default, --source-db)**
Reads from Document DB (created by 04_csv_import_documents.py):
- Rating → RATED edges (User → Movie)
- Tag → TAGGED edges (User → Movie)
- Movie → Movie vertices
- Link → merged into Movie vertices (imdbId, tmdbId)

**Option 2: Imported JSONL (--import-jsonl)**
Imports pre-exported JSONL file and reads from imported database:
- Faster initial setup (no CSV import needed)
- Good for reproducible benchmarks
- Measures import time separately
- Example: --import-jsonl ./exports/ml_small_db.jsonl.tgz

Expected Results (small dataset):
==================================
✓ Vertices: 610 Users + 9,742 Movies = 10,352 total
✓ Edges: 98,734 RATED + 3,494 TAGGED = 102,228 total

Performance (small dataset):
- Java API w/ indexes + async: ~5-10K vertices/sec, ~2-3K edges/sec (FASTEST)
- SQL w/ indexes (sync): Slower than Java API (sequential processing)
- Without indexes: MUCH slower (no optimization)

Usage:
======
# Recommended (fastest):
python 05_csv_import_graph.py --size small --method java

# Compare SQL (synchronous):
python 05_csv_import_graph.py --size small --method sql

# Compare Java API without async (synchronous):
python 05_csv_import_graph.py --size small --method java --no-async

# Export graph database for reproducibility:
python 05_csv_import_graph.py --size small --method java --export

# Import from document DB export, create graph, and export result:
python 05_csv_import_graph.py --size small --import-jsonl ./exports/ml_small_db.jsonl.tgz --export

# Compare all methods:
./run_benchmark_05_csv_import_graph.sh small 5000 4 all_6

# Compare all methods with export (includes roundtrip validation):
./run_benchmark_05_csv_import_graph.sh small 5000 4 all_6 --export

Export & Roundtrip Validation:
===============================
When --export is enabled:
1. Graph database is exported to JSONL (compressed)
2. Export is imported into a new database
3. All counts are verified (users, movies, edges)
4. Sample data is validated (User #1 ratings/tags)
5. Both export and import times are measured

This validates the complete cycle: graph creation → export → import → verify
"""

import argparse
import json
import os
import shutil
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import arcadedb_embedded as arcadedb
import numpy as np


@dataclass
class BenchmarkStats:
    """Statistics for a single operation."""

    count: int = 0
    duration: float = 0.0
    query_times: list[float] = None

    def __post_init__(self):
        if self.query_times is None:
            self.query_times = []

    @property
    def rate(self) -> float:
        """Items per second."""
        return self.count / self.duration if self.duration > 0 else 0

    def add_query_time(self, elapsed: float):
        """Record a query execution time."""
        self.query_times.append(elapsed)

    def query_stats(self) -> tuple[float, float]:
        """Return (mean, std) of query times in seconds."""
        if not self.query_times:
            return 0.0, 0.0
        times = np.array(self.query_times)
        return float(np.mean(times)), float(np.std(times))


class DataLoader:
    """Handles paginated data loading from source database."""

    def __init__(self, source_db_path: Path, batch_size: int):
        self.source_db_path = source_db_path
        self.batch_size = batch_size

    def load_links_paginated(self) -> dict[int, dict[str, Any]]:
        """Load Link data with proper pagination (database-level streaming).

        Returns dict mapping movieId -> {imdbId, tmdbId}
        """
        print("Loading Link data (paginated)...")
        start_time = time.time()
        links_data = {}
        batch_count = 0

        with arcadedb.open_database(str(self.source_db_path)) as source_db:
            last_rid = "#-1:-1"
            while True:
                query = f"""
                    SELECT *, @rid as rid FROM Link
                    WHERE @rid > {last_rid}
                    ORDER BY @rid
                    LIMIT {self.batch_size}
                """
                chunk = list(source_db.query("sql", query))
                if not chunk:
                    break

                for record in chunk:
                    movie_id = record.get_property("movieId")
                    links_data[movie_id] = {
                        "imdbId": (
                            record.get_property("imdbId")
                            if record.has_property("imdbId")
                            else None
                        ),
                        "tmdbId": (
                            record.get_property("tmdbId")
                            if record.has_property("tmdbId")
                            else None
                        ),
                    }

                last_rid = chunk[-1].get_property("rid")
                batch_count += 1

        elapsed = time.time() - start_time
        print(
            f"✓ Loaded {len(links_data):,} links in {elapsed:.2f}s "
            f"({batch_count} batches)"
        )
        return links_data

    def count_data(self) -> tuple[int, int, int, int]:
        """Count totals: users, movies, ratings, tags."""
        print("Counting data...")
        with arcadedb.open_database(str(self.source_db_path)) as source_db:
            # Count distinct users using subquery
            result = list(
                source_db.query(
                    "sql",
                    """
                    SELECT COUNT(*) as count FROM (
                        SELECT DISTINCT userId FROM Rating
                    )
                    """,
                )
            )
            total_users = result[0].get_property("count")

            # Count movies
            result = list(source_db.query("sql", "SELECT COUNT(*) as count FROM Movie"))
            total_movies = result[0].get_property("count")

            # Count ratings
            result = list(
                source_db.query(
                    "sql",
                    "SELECT COUNT(*) as count FROM Rating WHERE timestamp IS NOT NULL",
                )
            )
            total_ratings = result[0].get_property("count")

            # Count tags
            result = list(
                source_db.query(
                    "sql",
                    "SELECT COUNT(*) as count FROM Tag "
                    "WHERE timestamp IS NOT NULL AND tag IS NOT NULL",
                )
            )
            total_tags = result[0].get_property("count")

        print(
            f"✓ Found {total_users:,} users, {total_movies:,} movies, "
            f"{total_ratings:,} ratings, {total_tags:,} tags"
        )
        return total_users, total_movies, total_ratings, total_tags


class VertexCreator:
    """Creates vertices using either Java API or SQL."""

    def __init__(
        self,
        db: Any,
        data_loader: DataLoader,
        batch_size: int,
        parallel_level: int = 4,
        use_java_api: bool = True,
        use_async: bool = True,
    ):
        self.db = db
        self.data_loader = data_loader
        self.batch_size = batch_size
        self.parallel_level = parallel_level
        self.use_java_api = use_java_api
        self.use_async = use_async

    def create_all_vertices(
        self, total_users: int, total_movies: int, links_data: dict
    ) -> tuple[int, int, BenchmarkStats, BenchmarkStats]:
        """Create all User and Movie vertices.

        Returns: (user_count, movie_count, user_stats, movie_stats)
        """
        user_count, user_stats = self._create_users(total_users)
        movie_count, movie_stats = self._create_movies(total_movies, links_data)
        return user_count, movie_count, user_stats, movie_stats

    def _create_users(self, total_users: int) -> tuple[int, BenchmarkStats]:
        """Create User vertices.

        Note: Uses SELECT DISTINCT (not paginated - efficient pagination difficult)
        """
        print(f"Creating {total_users:,} User vertices...")
        stats = BenchmarkStats()
        start_time = time.time()
        user_count = 0
        batch_count = 0

        if self.use_async and self.use_java_api:
            # Java API with BatchContext (async) - WORKS WELL
            with arcadedb.BatchContext(
                self.db, batch_size=self.batch_size, parallel=self.parallel_level
            ) as batch:
                with arcadedb.open_database(
                    str(self.data_loader.source_db_path)
                ) as source_db:
                    query = "SELECT DISTINCT userId FROM Rating ORDER BY userId"
                    for record in source_db.query("sql", query):
                        user_id = record.get_property("userId")
                        batch.create_vertex("User", userId=user_id)
                        user_count += 1

                        if user_count % self.batch_size == 0:
                            batch_count += 1
                            self._report_progress(
                                "User",
                                batch_count,
                                user_count,
                                total_users,
                                start_time,
                            )
        else:
            # SQL mode (always synchronous for vertices - async causes
            # ConcurrentModificationException due to multiple threads writing
            # to same pages)
            with arcadedb.open_database(
                str(self.data_loader.source_db_path)
            ) as source_db:
                query = "SELECT DISTINCT userId FROM Rating ORDER BY userId"
                batch_user_ids = []

                for record in source_db.query("sql", query):
                    user_id = record.get_property("userId")
                    batch_user_ids.append(user_id)

                    if len(batch_user_ids) >= self.batch_size:
                        with self.db.transaction():
                            for uid in batch_user_ids:
                                sql = f"INSERT INTO User SET userId = {uid}"
                                self.db.command("sql", sql)
                        user_count += len(batch_user_ids)
                        batch_count += 1
                        batch_user_ids = []
                        self._report_progress(
                            "User", batch_count, user_count, total_users, start_time
                        )

                # Handle remaining
                if batch_user_ids:
                    with self.db.transaction():
                        for uid in batch_user_ids:
                            sql = f"INSERT INTO User SET userId = {uid}"
                            self.db.command("sql", sql)
                    user_count += len(batch_user_ids)
                    batch_count += 1

        stats.count = user_count
        stats.duration = time.time() - start_time
        print(f"✓ Created {user_count:,} User vertices")
        print(f"  ⏱️  {stats.duration:.2f}s ({stats.rate:.0f} vertices/sec)")
        print()
        return user_count, stats

    def _create_movies(
        self, total_movies: int, links_data: dict
    ) -> tuple[int, BenchmarkStats]:
        """Create Movie vertices with pagination (database-level streaming)."""
        print(f"Creating {total_movies:,} Movie vertices...")
        stats = BenchmarkStats()
        start_time = time.time()
        movie_count = 0
        batch_count = 0

        if self.use_java_api:
            # Java API with BatchContext
            with arcadedb.BatchContext(
                self.db, batch_size=self.batch_size, parallel=self.parallel_level
            ) as batch:
                with arcadedb.open_database(
                    str(self.data_loader.source_db_path)
                ) as source_db:
                    last_rid = "#-1:-1"
                    while True:
                        query_start = time.time()
                        query = f"""
                            SELECT *, @rid as rid FROM Movie
                            WHERE @rid > {last_rid}
                            ORDER BY @rid
                            LIMIT {self.batch_size}
                        """
                        chunk = list(source_db.query("sql", query))
                        query_time = time.time() - query_start
                        stats.add_query_time(query_time)
                        if not chunk:
                            break

                        for record in chunk:
                            movie_id = record.get_property("movieId")
                            title = (
                                record.get_property("title")
                                if record.has_property("title")
                                else ""
                            )
                            genres = (
                                record.get_property("genres")
                                if record.has_property("genres")
                                else ""
                            )

                            # Merge Link data
                            props = {
                                "movieId": movie_id,
                                "title": title or "",
                                "genres": genres or "",
                            }
                            link_data = links_data.get(movie_id)
                            if link_data:
                                if link_data["imdbId"] is not None:
                                    props["imdbId"] = link_data["imdbId"]
                                if link_data["tmdbId"] is not None:
                                    props["tmdbId"] = link_data["tmdbId"]

                            batch.create_vertex("Movie", **props)
                            movie_count += 1

                        batch_count += 1
                        last_rid = chunk[-1].get_property("rid")
                        self._report_progress(
                            "Movie",
                            batch_count,
                            movie_count,
                            total_movies,
                            start_time,
                            query_time,
                        )
        else:
            # SQL INSERT in batched transactions
            with arcadedb.open_database(
                str(self.data_loader.source_db_path)
            ) as source_db:
                last_rid = "#-1:-1"
                while True:
                    query_start = time.time()
                    query = f"""
                        SELECT *, @rid as rid FROM Movie
                        WHERE @rid > {last_rid}
                        ORDER BY @rid
                        LIMIT {self.batch_size}
                    """
                    chunk = list(source_db.query("sql", query))
                    query_time = time.time() - query_start
                    stats.add_query_time(query_time)
                    if not chunk:
                        break

                    with self.db.transaction():
                        for record in chunk:
                            movie_id = record.get_property("movieId")
                            title = (
                                record.get_property("title")
                                if record.has_property("title")
                                else ""
                            )
                            genres = (
                                record.get_property("genres")
                                if record.has_property("genres")
                                else ""
                            )

                            # Escape SQL strings
                            title = (title or "").replace("'", "\\'")
                            genres = (genres or "").replace("'", "\\'")

                            sql = (
                                f"INSERT INTO Movie SET "
                                f"movieId = {movie_id}, "
                                f"title = '{title}', "
                                f"genres = '{genres}'"
                            )

                            # Merge Link data
                            link_data = links_data.get(movie_id)
                            if link_data:
                                if link_data["imdbId"] is not None:
                                    imdb_id = str(link_data["imdbId"])
                                    imdb_id = imdb_id.replace("'", "\\'")
                                    sql += f", imdbId = '{imdb_id}'"
                                if link_data["tmdbId"] is not None:
                                    sql += f", tmdbId = {link_data['tmdbId']}"

                            self.db.command("sql", sql)

                    movie_count += len(chunk)
                    batch_count += 1
                    last_rid = chunk[-1].get_property("rid")
                    self._report_progress(
                        "Movie",
                        batch_count,
                        movie_count,
                        total_movies,
                        start_time,
                        query_time,
                    )

        stats.count = movie_count
        stats.duration = time.time() - start_time
        print(f"✓ Created {movie_count:,} Movie vertices")
        print(f"  ⏱️  {stats.duration:.2f}s ({stats.rate:.0f} vertices/sec)")

        mean, std = stats.query_stats()
        if stats.query_times:
            print(
                f"  Query times: {mean:.2f} ± {std:.2f}s "
                f"({len(stats.query_times)} queries)"
            )
        print()
        return movie_count, stats

    def _report_progress(
        self,
        entity_type: str,
        batch_num: int,
        current: int,
        total: int,
        start_time: float,
        query_time: float = None,
    ):
        """Print progress report."""
        elapsed = time.time() - start_time
        if elapsed > 0:
            rate = current / elapsed
            remaining = total - current
            pct = (current / total) * 100 if total > 0 else 0
            progress_msg = (
                f"  Batch {batch_num}: {current:,}/{total:,} "
                f"{entity_type}s ({pct:.1f}%, {remaining:,} remaining, "
                f"{rate:.0f}/sec)"
            )
            if query_time is not None:
                progress_msg += f", query: {query_time:.2f}s"
            print(progress_msg)


class EdgeCreator:
    """Creates edges using either Java API or SQL."""

    def __init__(
        self,
        db: Any,
        data_loader: DataLoader,
        batch_size: int,
        use_java_api: bool = True,
        has_indexes: bool = True,
        use_async: bool = True,
        parallel_level: int = 4,
    ):
        self.db = db
        self.data_loader = data_loader
        self.batch_size = batch_size
        self.use_java_api = use_java_api
        self.has_indexes = has_indexes
        self.use_async = use_async
        self.parallel_level = parallel_level

    def create_all_edges(
        self, total_ratings: int, total_tags: int
    ) -> tuple[int, int, BenchmarkStats, BenchmarkStats]:
        """Create all RATED and TAGGED edges.

        Returns: (rated_count, tagged_count, rated_stats, tagged_stats)
        """
        rated_count, rated_stats = self._create_rated_edges(total_ratings)
        tagged_count, tagged_stats = self._create_tagged_edges(total_tags)
        return rated_count, tagged_count, rated_stats, tagged_stats

    def _create_rated_edges(self, total_ratings: int) -> tuple[int, BenchmarkStats]:
        """Create RATED edges with pagination (database-level streaming)."""
        print(f"Creating {total_ratings:,} RATED edges...")
        stats = BenchmarkStats()
        start_time = time.time()
        edge_count = 0
        batch_count = 0

        with arcadedb.open_database(str(self.data_loader.source_db_path)) as source_db:
            last_rid = "#-1:-1"
            while True:
                # Load batch of ratings
                query_start = time.time()
                query = f"""
                    SELECT *, @rid as rid FROM Rating
                    WHERE timestamp IS NOT NULL AND @rid > {last_rid}
                    ORDER BY @rid
                    LIMIT {self.batch_size}
                """
                chunk = list(source_db.query("sql", query))
                query_time = time.time() - query_start
                stats.add_query_time(query_time)
                if not chunk:
                    break

                with self.db.transaction():
                    # Build vertex cache for this batch
                    cache_start = time.time()
                    user_cache, movie_cache = self._build_vertex_cache(chunk)
                    cache_time = time.time() - cache_start
                    stats.add_query_time(cache_time)

                    # Create edges
                    for record in chunk:
                        user_id = record.get_property("userId")
                        movie_id = record.get_property("movieId")
                        rating = record.get_property("rating")
                        timestamp = record.get_property("timestamp")

                        if self.use_java_api:
                            user_vertex = user_cache.get(user_id)
                            movie_vertex = movie_cache.get(movie_id)
                            if user_vertex and movie_vertex:
                                edge = user_vertex.newEdge(
                                    "RATED",
                                    movie_vertex,
                                    "rating",
                                    rating,
                                    "timestamp",
                                    timestamp,
                                )
                                edge.save()
                                edge_count += 1
                        else:
                            # SQL CREATE EDGE
                            user_rid = user_cache.get(user_id)
                            movie_rid = movie_cache.get(movie_id)
                            if user_rid and movie_rid:
                                sql = (
                                    f"CREATE EDGE RATED "
                                    f"FROM {user_rid} TO {movie_rid} "
                                    f"SET rating = {rating}, timestamp = {timestamp}"
                                )
                                self.db.command("sql", sql)
                                edge_count += 1

                batch_count += 1
                last_rid = chunk[-1].get_property("rid")
                total_query_time = query_time + cache_time
                self._report_progress(
                    "RATED",
                    batch_count,
                    edge_count,
                    total_ratings,
                    start_time,
                    total_query_time,
                )

        stats.count = edge_count
        stats.duration = time.time() - start_time
        print(f"✓ Created {edge_count:,} RATED edges")
        print(f"  ⏱️  {stats.duration:.2f}s ({stats.rate:.0f} edges/sec)")

        mean, std = stats.query_stats()
        if stats.query_times:
            print(
                f"  Query times: {mean:.2f} ± {std:.2f}s "
                f"({len(stats.query_times)} queries)"
            )
        print()
        return edge_count, stats

    def _create_tagged_edges(self, total_tags: int) -> tuple[int, BenchmarkStats]:
        """Create TAGGED edges with pagination (database-level streaming)."""
        print(f"Creating {total_tags:,} TAGGED edges...")
        stats = BenchmarkStats()
        start_time = time.time()
        edge_count = 0
        batch_count = 0

        with arcadedb.open_database(str(self.data_loader.source_db_path)) as source_db:
            last_rid = "#-1:-1"
            while True:
                # Load batch of tags
                query_start = time.time()
                query = f"""
                    SELECT *, @rid as rid FROM Tag
                    WHERE timestamp IS NOT NULL
                        AND tag IS NOT NULL
                        AND @rid > {last_rid}
                    ORDER BY @rid
                    LIMIT {self.batch_size}
                """
                chunk = list(source_db.query("sql", query))
                query_time = time.time() - query_start
                stats.add_query_time(query_time)
                if not chunk:
                    break

                with self.db.transaction():
                    # Build vertex cache for this batch
                    cache_start = time.time()
                    user_cache, movie_cache = self._build_vertex_cache(chunk)
                    cache_time = time.time() - cache_start
                    stats.add_query_time(cache_time)

                    # Create edges
                    for record in chunk:
                        user_id = record.get_property("userId")
                        movie_id = record.get_property("movieId")
                        tag = record.get_property("tag") or ""
                        timestamp = record.get_property("timestamp")

                        if self.use_java_api:
                            user_vertex = user_cache.get(user_id)
                            movie_vertex = movie_cache.get(movie_id)
                            if user_vertex and movie_vertex:
                                edge = user_vertex.newEdge(
                                    "TAGGED",
                                    movie_vertex,
                                    "tag",
                                    tag,
                                    "timestamp",
                                    timestamp,
                                )
                                edge.save()
                                edge_count += 1
                        else:
                            # SQL CREATE EDGE
                            user_rid = user_cache.get(user_id)
                            movie_rid = movie_cache.get(movie_id)
                            if user_rid and movie_rid:
                                tag_escaped = tag.replace("'", "\\'")
                                sql = (
                                    f"CREATE EDGE TAGGED "
                                    f"FROM {user_rid} TO {movie_rid} "
                                    f"SET tag = '{tag_escaped}', "
                                    f"timestamp = {timestamp}"
                                )
                                self.db.command("sql", sql)
                                edge_count += 1

                batch_count += 1
                last_rid = chunk[-1].get_property("rid")
                total_query_time = query_time + cache_time
                self._report_progress(
                    "TAGGED",
                    batch_count,
                    edge_count,
                    total_tags,
                    start_time,
                    total_query_time,
                )

        stats.count = edge_count
        stats.duration = time.time() - start_time
        print(f"✓ Created {edge_count:,} TAGGED edges")
        print(f"  ⏱️  {stats.duration:.2f}s ({stats.rate:.0f} edges/sec)")

        mean, std = stats.query_stats()
        if stats.query_times:
            print(
                f"  Query times: {mean:.2f} ± {std:.2f}s "
                f"({len(stats.query_times)} queries)"
            )
        print()
        return edge_count, stats

    def _build_vertex_cache(self, chunk: list) -> tuple[dict, dict]:
        """Build vertex cache for a batch of records.

        For Java API: Returns (user_vertices, movie_vertices)
        For SQL: Returns (user_rids, movie_rids)
        """
        user_ids = list({r.get_property("userId") for r in chunk})
        movie_ids = list({r.get_property("movieId") for r in chunk})

        user_cache = {}
        movie_cache = {}

        if self.use_java_api:
            # Fetch Java vertex objects
            if user_ids:
                user_ids_str = ",".join(str(uid) for uid in user_ids)
                query = f"SELECT FROM User WHERE userId IN [{user_ids_str}]"
                for result in self.db.query("sql", query):
                    uid = result.get_property("userId")
                    vertex = result._java_result.getElement().get().asVertex()
                    user_cache[uid] = vertex

            if movie_ids:
                movie_ids_str = ",".join(str(mid) for mid in movie_ids)
                query = f"SELECT FROM Movie WHERE movieId IN [{movie_ids_str}]"
                for result in self.db.query("sql", query):
                    mid = result.get_property("movieId")
                    vertex = result._java_result.getElement().get().asVertex()
                    movie_cache[mid] = vertex
        else:
            # Fetch RIDs for SQL CREATE EDGE
            if user_ids:
                user_ids_str = ",".join(str(uid) for uid in user_ids)
                query = (
                    f"SELECT @rid as rid, userId FROM User "
                    f"WHERE userId IN [{user_ids_str}]"
                )
                for result in self.db.query("sql", query):
                    uid = result.get_property("userId")
                    rid = result.get_property("rid").toString()
                    user_cache[uid] = rid

            if movie_ids:
                movie_ids_str = ",".join(str(mid) for mid in movie_ids)
                query = (
                    f"SELECT @rid as rid, movieId FROM Movie "
                    f"WHERE movieId IN [{movie_ids_str}]"
                )
                for result in self.db.query("sql", query):
                    mid = result.get_property("movieId")
                    rid = result.get_property("rid").toString()
                    movie_cache[mid] = rid

        return user_cache, movie_cache

    def _report_progress(
        self,
        edge_type: str,
        batch_num: int,
        current: int,
        total: int,
        start_time: float,
        query_time: float = None,
    ):
        """Print progress report."""
        elapsed = time.time() - start_time
        if elapsed > 0:
            rate = current / elapsed
            remaining = total - current
            pct = (current / total) * 100 if total > 0 else 0
            progress_msg = (
                f"  Batch {batch_num}: {current:,}/{total:,} "
                f"edges ({pct:.1f}%, {remaining:,} remaining, "
                f"{rate:.0f}/sec)"
            )
            if query_time is not None:
                progress_msg += f", query: {query_time:.2f}s"
            print(progress_msg)


def import_from_jsonl(jsonl_path: Path, target_db_path: Path) -> float:
    """Import database from JSONL export.

    Returns: import time in seconds
    """
    print(f"Importing from JSONL: {jsonl_path}")
    print(f"  → Target database: {target_db_path}")

    # Delete target if it exists
    if target_db_path.exists():
        shutil.rmtree(target_db_path)

    # Create empty database
    with arcadedb.create_database(str(target_db_path)) as db:
        pass  # Just create it

    # Import using SQL command
    start_time = time.time()
    with arcadedb.open_database(str(target_db_path)) as db:
        import_path = jsonl_path.absolute()
        print(f"  📥 Importing from: {import_path}")
        db.command("sql", f"import database file://{import_path}")

    elapsed = time.time() - start_time

    # Count imported records
    with arcadedb.open_database(str(target_db_path)) as db:
        result = list(db.query("sql", "SELECT count(*) as count FROM Movie"))
        movie_count = result[0].get_property("count")
        result = list(db.query("sql", "SELECT count(*) as count FROM Rating"))
        rating_count = result[0].get_property("count")
        result = list(db.query("sql", "SELECT count(*) as count FROM Tag"))
        tag_count = result[0].get_property("count")
        result = list(db.query("sql", "SELECT count(*) as count FROM Link"))
        link_count = result[0].get_property("count")
        total_records = movie_count + rating_count + tag_count + link_count

    print(f"  ✓ Imported {total_records:,} records in {elapsed:.2f}s")
    print(f"    ({total_records / elapsed:.0f} records/sec)")
    print(
        f"    Movies: {movie_count:,}, Ratings: {rating_count:,}, "
        f"Tags: {tag_count:,}, Links: {link_count:,}"
    )
    print()

    return elapsed


# Define expected baseline results for validation
# This structure mirrors the document example (04_csv_import_documents.py)
# Format: query results with count and sample data for verification
EXPECTED_RESULTS = {
    "small": {
        "counts": {"users": 610, "movies": 9742, "rated": 98734, "tagged": 3494},
        "samples": {
            "user1_ratings": 223,
            "user1_tags": 0,
            "movie1_title": "Toy Story (1995)",
            "movie1_genres": "Adventure|Animation|Children|Comedy|Fantasy",
        },
        "queries": [
            {
                "name": "Query 1: Movies rated by User #1 (SQL - Basic Traversal)",
                "count": 223,
            },
            {
                "name": "Query 2: Movies rated 5.0 by User #1 (SQL - Edge Property Filter)",
                "count": 119,
            },
            {
                "name": "Query 3: Rating statistics for top 5 active users (SQL - Aggregations)",
                "count": 5,
                "sample": {"top_user_id": 414, "top_user_ratings": 2645},
            },
            {
                "name": "Query 4: Top 10 most rated movies (SQL - Aggregations)",
                "count": 10,
                "sample": {"top_movie": "Forrest Gump (1994)", "top_movie_count": 317},
            },
            {
                "name": "Query 5: Top 10 most tagged movies (SQL - Aggregations)",
                "count": 10,
                "sample": {"top_movie": "Pulp Fiction (1994)", "top_movie_tags": 170},
            },
            {
                "name": "Query 6: Users who rated same movies as User #1 (SQL - MATCH Pattern)",
                "count": 15173,
            },
            {
                "name": "Query 7: Users with similar taste to User #1 (SQL - MATCH + Aggregation)",
                "count": 482,
            },
            {
                "name": "Query 8: Rating distribution across all ratings (SQL - Aggregation)",
                "count": 10,
            },
            {
                "name": "Query 9: User #1's top-rated movies (Cypher - Basic Pattern)",
                "count": 193,
            },
            {
                "name": "Query 10: Users who rated same movies as User #1 (Cypher - Pattern)",
                "count": 602,
                "sample": {"top_user_id": 414, "top_shared": 187},
            },
        ],
    },
    "large": {
        # TODO: Fill in after first successful large run
        "counts": {
            "users": None,
            "movies": None,
            "rated": None,
            "tagged": None,
        },
        "samples": {
            "user1_ratings": None,
            "user1_tags": None,
            "movie1_title": "Toy Story (1995)",  # Should be same
            "movie1_genres": "Adventure|Animation|Children|Comedy|Fantasy",
        },
        "queries": [
            {
                "name": "Query 1: Movies rated by User #1 (SQL - Basic Traversal)",
                "count": None,
            },
            {
                "name": "Query 2: Movies rated 5.0 by User #1 (SQL - Edge Property Filter)",
                "count": None,
            },
            {
                "name": "Query 3: Rating statistics for top 5 active users (SQL - Aggregations)",
                "count": 5,
                "sample": {"top_user_id": None, "top_user_ratings": None},
            },
            {
                "name": "Query 4: Top 10 most rated movies (SQL - Aggregations)",
                "count": 10,
                "sample": {"top_movie": None, "top_movie_count": None},
            },
            {
                "name": "Query 5: Top 10 most tagged movies (SQL - Aggregations)",
                "count": 10,
                "sample": {"top_movie": None, "top_movie_tags": None},
            },
            {
                "name": "Query 6: Users who rated same movies as User #1 (SQL - MATCH Pattern)",
                "count": None,
            },
            {
                "name": "Query 7: Users with similar taste to User #1 (SQL - MATCH + Aggregation)",
                "count": None,
            },
            {
                "name": "Query 8: Rating distribution across all ratings (SQL - Aggregation)",
                "count": None,
            },
            {
                "name": "Query 9: User #1's top-rated movies (Cypher - Basic Pattern)",
                "count": None,
            },
            {
                "name": "Query 10: Users who rated same movies as User #1 (Cypher - Pattern)",
                "count": None,
                "sample": {"top_user_id": None, "top_shared": None},
            },
        ],
    },
}


def get_expected_values(size: str) -> dict:
    """Get expected values for a specific dataset size.

    Returns the 'counts' and 'samples' portions of EXPECTED_RESULTS.
    """
    if size not in EXPECTED_RESULTS:
        return {}

    result = {}
    result.update(EXPECTED_RESULTS[size].get("counts", {}))
    result.update(EXPECTED_RESULTS[size].get("samples", {}))
    return result


def create_schema(db: Any, create_indexes: bool = True):
    """Create graph schema with optional indexes."""
    print("Creating graph schema...")
    start_time = time.time()

    # Create vertex types
    db.command("sql", "CREATE VERTEX TYPE User IF NOT EXISTS")
    db.command("sql", "CREATE VERTEX TYPE Movie IF NOT EXISTS")

    # Create edge types
    db.command("sql", "CREATE EDGE TYPE RATED IF NOT EXISTS")
    db.command("sql", "CREATE EDGE TYPE TAGGED IF NOT EXISTS")

    # Create properties
    db.command("sql", "CREATE PROPERTY User.userId IF NOT EXISTS INTEGER")
    db.command("sql", "CREATE PROPERTY Movie.movieId IF NOT EXISTS INTEGER")
    db.command("sql", "CREATE PROPERTY Movie.title IF NOT EXISTS STRING")
    db.command("sql", "CREATE PROPERTY Movie.genres IF NOT EXISTS STRING")
    db.command("sql", "CREATE PROPERTY Movie.imdbId IF NOT EXISTS STRING")
    db.command("sql", "CREATE PROPERTY Movie.tmdbId IF NOT EXISTS INTEGER")
    db.command("sql", "CREATE PROPERTY RATED.rating IF NOT EXISTS FLOAT")
    db.command("sql", "CREATE PROPERTY RATED.timestamp IF NOT EXISTS LONG")
    db.command("sql", "CREATE PROPERTY TAGGED.tag IF NOT EXISTS STRING")
    db.command("sql", "CREATE PROPERTY TAGGED.timestamp IF NOT EXISTS LONG")

    if create_indexes:
        print("Creating indexes...")
        db.command("sql", "CREATE INDEX IF NOT EXISTS ON User (userId) UNIQUE")
        db.command("sql", "CREATE INDEX IF NOT EXISTS ON Movie (movieId) UNIQUE")
        print("✓ Indexes created")
    else:
        print("⚠️  Indexes disabled (--no-index)")

    elapsed = time.time() - start_time
    print(f"✓ Schema created in {elapsed:.2f}s")
    print()


def serialize_query_results(results):
    """
    Convert query results to JSON-serializable format matching
    EXPECTED_RESULTS structure.

    Args:
        results: Tuple of (counts_dict, samples_dict, queries_list)
                 from validate_and_query()

    Returns:
        Dict matching EXPECTED_RESULTS structure with counts, samples,
        and queries
    """
    counts, samples, queries = results

    return {
        "counts": counts,
        "samples": samples,
        "queries": queries,
    }


def save_query_results(results, dataset_size, db_path):
    """
    Save query results to a JSON file for later comparison.

    Args:
        results: List of result dicts from run_and_validate_queries()
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

    with open(results_file, "w", encoding="utf-8") as f:
        json.dump(serialized_results, f, indent=2)

    return results_file


def validate_and_query(
    db: Any,
    size: str,
    user_count: int,
    movie_count: int,
    edge_count: int,
    tagged_count: int,
    check_baseline: bool = True,
):
    """
    Combined validation: basic counts + all query validation.

    This function:
    1. Validates basic counts (users, movies, edges)
    2. Validates sample data (User #1, Movie #1)
    3. Runs all 10 graph queries
    4. Validates query results against baseline
    5. Returns ((counts, samples, queries), validation_passed)
    """
    start_time = time.time()

    print("=" * 70)
    print("Validation & Query Testing")
    print("=" * 70)
    print()

    # Count vertices
    result = list(db.query("sql", "SELECT count(*) as count FROM User"))
    user_count_check = result[0].get_property("count")
    result = list(db.query("sql", "SELECT count(*) as count FROM Movie"))
    movie_count_check = result[0].get_property("count")

    # Count edges
    result = list(db.query("sql", "SELECT count(*) as count FROM RATED"))
    rated_count_check = result[0].get_property("count")
    result = list(db.query("sql", "SELECT count(*) as count FROM TAGGED"))
    tagged_count_check = result[0].get_property("count")

    # Sample data validation: check a specific user's ratings
    sample_user_query = """
        SELECT out('RATED').size() as rating_count,
               out('TAGGED').size() as tag_count
        FROM User
        WHERE userId = 1
    """
    sample_user_results = list(db.query("sql", sample_user_query))
    if sample_user_results:
        sample_user = sample_user_results[0]
        user1_ratings = sample_user.get_property("rating_count")
        user1_tags = sample_user.get_property("tag_count")
    else:
        user1_ratings = None
        user1_tags = None

    # Sample data validation: check a specific movie exists with properties
    sample_movie_query = """
        SELECT title, genres
        FROM Movie
        WHERE movieId = 1
    """
    movie_results = list(db.query("sql", sample_movie_query))
    if movie_results:
        sample_movie = movie_results[0]
        movie1_title = sample_movie.get_property("title")
        movie1_genres = sample_movie.get_property("genres")
    else:
        movie1_title = None
        movie1_genres = None

    print("✓ Vertex counts:")
    print(f"  Users:  {user_count_check:,}")
    print(f"  Movies: {movie_count_check:,}")
    print("✓ Edge counts:")
    print(f"  RATED:  {rated_count_check:,}")
    print(f"  TAGGED: {tagged_count_check:,}")
    print("✓ Sample data (User #1):")
    print(f"  Ratings: {user1_ratings}")
    print(f"  Tags:    {user1_tags}")
    print("✓ Sample data (Movie #1):")
    print(f"  Title:  {movie1_title}")
    print(f"  Genres: {movie1_genres}")
    print()

    # Validation checks
    validation_passed = True
    if user_count_check != user_count:
        print(
            f"❌ User count mismatch! " f"Expected {user_count}, got {user_count_check}"
        )
        validation_passed = False
    if movie_count_check != movie_count:
        print(
            f"❌ Movie count mismatch! "
            f"Expected {movie_count}, got {movie_count_check}"
        )
        validation_passed = False
    if rated_count_check != edge_count:
        print(
            f"❌ RATED edge count mismatch! "
            f"Expected {edge_count}, got {rated_count_check}"
        )
        validation_passed = False
    if tagged_count_check != tagged_count:
        print(
            f"❌ TAGGED edge count mismatch! "
            f"Expected {tagged_count}, got {tagged_count_check}"
        )
        validation_passed = False

    if validation_passed:
        print("✅ Basic validation passed!")
    else:
        print("⚠️  Some validation checks failed!")

    # Compare against expected values for this dataset size
    expected = get_expected_values(size)
    if expected and any(v is not None for v in expected.values()):
        print()
        print(f"Comparing against expected baseline for {size} dataset:")

        expected_passed = True

        if expected["users"] is not None:
            if user_count_check != expected["users"]:
                print(
                    f"  ❌ Users: expected {expected['users']}, "
                    f"got {user_count_check}"
                )
                expected_passed = False
            else:
                print(f"  ✓ Users: {user_count_check}")

        if expected["movies"] is not None:
            if movie_count_check != expected["movies"]:
                print(
                    f"  ❌ Movies: expected {expected['movies']}, "
                    f"got {movie_count_check}"
                )
                expected_passed = False
            else:
                print(f"  ✓ Movies: {movie_count_check}")

        if expected["rated"] is not None:
            if rated_count_check != expected["rated"]:
                print(
                    f"  ❌ RATED edges: expected {expected['rated']}, "
                    f"got {rated_count_check}"
                )
                expected_passed = False
            else:
                print(f"  ✓ RATED edges: {rated_count_check}")

        if expected["tagged"] is not None:
            if tagged_count_check != expected["tagged"]:
                print(
                    f"  ❌ TAGGED edges: expected {expected['tagged']}, "
                    f"got {tagged_count_check}"
                )
                expected_passed = False
            else:
                print(f"  ✓ TAGGED edges: {tagged_count_check}")

        if expected["user1_ratings"] is not None:
            if user1_ratings != expected["user1_ratings"]:
                print(
                    f"  ❌ User #1 ratings: expected "
                    f"{expected['user1_ratings']}, got {user1_ratings}"
                )
                expected_passed = False
            else:
                print(f"  ✓ User #1 ratings: {user1_ratings}")

        if expected["user1_tags"] is not None:
            if user1_tags != expected["user1_tags"]:
                print(
                    f"  ❌ User #1 tags: expected "
                    f"{expected['user1_tags']}, got {user1_tags}"
                )
                expected_passed = False
            else:
                print(f"  ✓ User #1 tags: {user1_tags}")

        if expected["movie1_title"] is not None:
            if movie1_title != expected["movie1_title"]:
                print(
                    f"  ❌ Movie #1 title: expected "
                    f"'{expected['movie1_title']}', got '{movie1_title}'"
                )
                expected_passed = False
            else:
                print(f"  ✓ Movie #1 title: {movie1_title}")

        if expected["movie1_genres"] is not None:
            if movie1_genres != expected["movie1_genres"]:
                print(
                    f"  ❌ Movie #1 genres: expected "
                    f"'{expected['movie1_genres']}', got '{movie1_genres}'"
                )
                expected_passed = False
            else:
                print(f"  ✓ Movie #1 genres: {movie1_genres}")

        if expected_passed:
            print("  ✅ All expected baseline values match!")
        else:
            print("  ⚠️  Some expected values don't match!")
            validation_passed = False

    print()
    print()

    # Run queries
    print("Running 10 graph queries...")
    print("-" * 70)
    print()

    query_results, query_validation_passed = run_and_validate_queries(
        db, size, check_baseline=check_baseline
    )

    # Update overall validation status
    validation_passed = validation_passed and query_validation_passed

    # Package results in EXPECTED_RESULTS format
    counts_dict = {
        "users": user_count_check,
        "movies": movie_count_check,
        "rated": rated_count_check,
        "tagged": tagged_count_check,
    }

    samples_dict = {
        "user1_ratings": user1_ratings,
        "user1_tags": user1_tags,
        "movie1_title": movie1_title,
        "movie1_genres": movie1_genres,
    }

    results = (counts_dict, samples_dict, query_results)

    elapsed = time.time() - start_time
    print()
    print("=" * 70)
    if validation_passed:
        print(f"✅ All validation & queries passed! ({elapsed:.2f}s)")
    else:
        print(f"⚠️  Some validations failed ({elapsed:.2f}s)")
    print("=" * 70)
    print()

    return results, validation_passed


def validate_results(
    db: Any,
    size: str,
    user_count: int,
    movie_count: int,
    edge_count: int,
    tagged_count: int,
):
    """
    DEPRECATED: Use validate_and_query() instead.
    Kept for backwards compatibility.
    """
    query_results, validation_passed = validate_and_query(
        db, size, user_count, movie_count, edge_count, tagged_count, check_baseline=True
    )
    return validation_passed


def run_and_validate_queries(db: Any, size: str, check_baseline: bool = True):
    """Run all graph queries and validate against baseline.

    This unified function:
    - Runs all 10 graph queries (SQL + Cypher)
    - Collects results in a structured format
    - Validates against EXPECTED_RESULTS if check_baseline=True
    - Outputs JSON for easy copy-paste into EXPECTED_RESULTS
    - Returns (results_list, validation_passed)

    Similar to the query validation in 04_csv_import_documents.py
    """

    # Store results in the same format as EXPECTED_RESULTS
    results = []
    all_passed = True

    if size not in EXPECTED_RESULTS:
        expected_queries = []
        check_baseline = False
    else:
        expected_queries = EXPECTED_RESULTS[size].get("queries", [])

    # Query 1: User's Rated Movies (SQL)
    print("1. Movies rated by User #1 (SQL - Basic Traversal)")
    print("-" * 70)
    start = time.time()
    result = db.query("sql", "SELECT expand(out('RATED')) FROM User WHERE userId = 1")
    movies = list(result)
    elapsed = time.time() - start

    query1_result = {
        "name": "Query 1: Movies rated by User #1 (SQL - Basic Traversal)",
        "count": len(movies),
    }
    results.append(query1_result)

    print(f"  Found {len(movies)} movies in {elapsed:.3f}s")
    if movies and len(movies) > 0:
        sample_movie = movies[0]
        print(f"  Sample: '{sample_movie.get_property('title')}'")
        print(f"  Genres: {sample_movie.get_property('genres')}")

    if check_baseline and len(expected_queries) > 0:
        expected_count = expected_queries[0].get("count")
        if expected_count is not None and len(movies) != expected_count:
            print(f"  ❌ Count mismatch: expected {expected_count}, got {len(movies)}")
            all_passed = False
        elif expected_count is not None:
            print(f"  ✓ Count matches baseline: {len(movies)}")
    print()

    # Query 2: High-Rated Movies by User (SQL with Edge Filtering)
    print("2. Movies rated 5.0 by User #1 (SQL - Edge Property Filter)")
    print("-" * 70)
    start = time.time()
    result = db.query(
        "sql",
        """
        SELECT expand(outE('RATED')[rating = 5.0].inV())
        FROM User WHERE userId = 1
        """,
    )
    high_rated = list(result)
    elapsed = time.time() - start

    query2_result = {
        "name": "Query 2: Movies rated 5.0 by User #1 (SQL - Edge Property Filter)",
        "count": len(high_rated),
    }
    results.append(query2_result)

    print(f"  Found {len(high_rated)} movies with 5.0 rating in {elapsed:.3f}s")
    if high_rated:
        for i, movie in enumerate(high_rated[:3]):
            print(f"  {i+1}. {movie.get_property('title')}")

    if check_baseline and len(expected_queries) > 1:
        expected_count = expected_queries[1].get("count")
        if expected_count is not None and len(high_rated) != expected_count:
            print(
                f"  ❌ Count mismatch: expected {expected_count}, "
                f"got {len(high_rated)}"
            )
            all_passed = False
        elif expected_count is not None:
            print(f"  ✓ Count matches baseline: {len(high_rated)}")
    print()

    # Query 3: Rating Statistics per User (SQL Aggregations)
    print("3. Rating statistics for top 5 active users (SQL - Aggregations)")
    print("-" * 70)
    start = time.time()
    result = db.query(
        "sql",
        """
     SELECT u.userId as userId,
         COUNT(e) as num_ratings,
         AVG(e.rating) as avg_rating,
         MIN(e.rating) as min_rating,
         MAX(e.rating) as max_rating
     FROM (
       MATCH {type: User, as: u}.outE('RATED'){as: e} RETURN u,e
     )
     GROUP BY u.userId
     ORDER BY num_ratings DESC
     LIMIT 5
        """,
    )
    stats = list(result)
    elapsed = time.time() - start

    query3_result = {
        "name": "Query 3: Rating statistics for top 5 active users (SQL - Aggregations)",
        "count": len(stats),
        "sample": {},
    }
    if stats:
        top_user = stats[0]
        query3_result["sample"]["top_user_id"] = top_user.get_property("userId")
        query3_result["sample"]["top_user_ratings"] = top_user.get_property(
            "num_ratings"
        )
    results.append(query3_result)

    print(f"  Computed statistics for top users in {elapsed:.3f}s")
    print(f"  {'User':<8} {'#Ratings':<10} {'Avg':<8} {'Min':<6} {'Max':<6}")
    print(f"  {'-'*8} {'-'*10} {'-'*8} {'-'*6} {'-'*6}")
    for record in stats:
        user_id = record.get_property("userId")
        num = record.get_property("num_ratings")
        avg = record.get_property("avg_rating")
        min_r = record.get_property("min_rating")
        max_r = record.get_property("max_rating")
        print(f"  {user_id:<8} {num:<10} {avg:<8.2f} {min_r:<6.1f} {max_r:<6.1f}")

    if check_baseline and len(expected_queries) > 2:
        expected_sample = expected_queries[2].get("sample", {})
        exp_top_id = expected_sample.get("top_user_id")
        exp_top_ratings = expected_sample.get("top_user_ratings")

        if exp_top_id is not None and stats:
            actual_top_id = stats[0].get_property("userId")
            if actual_top_id != exp_top_id:
                print(
                    f"  ❌ Top user mismatch: expected {exp_top_id}, "
                    f"got {actual_top_id}"
                )
                all_passed = False
            else:
                print(f"  ✓ Top user matches baseline: {actual_top_id}")

        if exp_top_ratings is not None and stats:
            actual_ratings = stats[0].get_property("num_ratings")
            if actual_ratings != exp_top_ratings:
                print(
                    f"  ❌ Top user ratings mismatch: expected {exp_top_ratings}, "
                    f"got {actual_ratings}"
                )
                all_passed = False
            else:
                print(f"  ✓ Top user ratings match baseline: {actual_ratings}")
    print()

    # Query 4: Most Rated Movies (SQL Aggregations)
    print("4. Top 10 most rated movies (SQL - Aggregations)")
    print("-" * 70)
    start = time.time()
    result = db.query(
        "sql",
        """
        SELECT m.movieId as movieId,
               m.title as title,
               COUNT(e) as num_ratings,
               AVG(e.rating) as avg_rating
        FROM (
          MATCH {type: Movie, as: m}.inE('RATED'){as: e} RETURN m, e
        )
        GROUP BY m.movieId, m.title
        ORDER BY num_ratings DESC
        LIMIT 10
        """,
    )
    top_movies = list(result)
    elapsed = time.time() - start

    query4_result = {
        "name": "Query 4: Top 10 most rated movies (SQL - Aggregations)",
        "count": len(top_movies),
        "sample": {},
    }
    if top_movies:
        top_movie = top_movies[0]
        query4_result["sample"]["top_movie"] = top_movie.get_property("title")
        query4_result["sample"]["top_movie_count"] = top_movie.get_property(
            "num_ratings"
        )
    results.append(query4_result)

    print(f"  Found top 10 movies in {elapsed:.3f}s")
    print(f"  {'#':<4} {'Title':<50} {'Ratings':<10} {'Avg':<6}")
    print(f"  {'-'*4} {'-'*50} {'-'*10} {'-'*6}")
    for i, record in enumerate(top_movies, 1):
        title = record.get_property("title")
        num = record.get_property("num_ratings")
        avg = record.get_property("avg_rating")
        print(f"  {i:<4} {title:<50.50} {num:<10} {avg:<6.2f}")

    if check_baseline and len(expected_queries) > 3:
        expected_sample = expected_queries[3].get("sample", {})
        exp_top_movie = expected_sample.get("top_movie")
        exp_top_count = expected_sample.get("top_movie_count")

        if exp_top_movie is not None and top_movies:
            actual_movie = top_movies[0].get_property("title")
            if actual_movie != exp_top_movie:
                print(
                    f"  ❌ Top movie mismatch: expected '{exp_top_movie}', "
                    f"got '{actual_movie}'"
                )
                all_passed = False
            else:
                print(f"  ✓ Top movie matches baseline: {actual_movie}")

        if exp_top_count is not None and top_movies:
            actual_count = top_movies[0].get_property("num_ratings")
            if actual_count != exp_top_count:
                print(
                    f"  ❌ Top movie count mismatch: expected {exp_top_count}, "
                    f"got {actual_count}"
                )
                all_passed = False
            else:
                print(f"  ✓ Top movie count matches baseline: {actual_count}")
    print()

    # Query 5: Most Tagged Movies (SQL Aggregations)
    print("5. Top 10 most tagged movies (SQL - Aggregations)")
    print("-" * 70)
    start = time.time()
    result = db.query(
        "sql",
        """
        SELECT m.movieId as movieId,
               m.title as title,
               COUNT(e) as num_tags
        FROM (
          MATCH {type: Movie, as: m}.inE('TAGGED'){as: e} RETURN m, e
        )
        GROUP BY m.movieId, m.title
        ORDER BY num_tags DESC
        LIMIT 10
        """,
    )
    tagged_movies = list(result)
    elapsed = time.time() - start

    query5_result = {
        "name": "Query 5: Top 10 most tagged movies (SQL - Aggregations)",
        "count": len(tagged_movies),
        "sample": {},
    }
    if tagged_movies:
        top_tagged = tagged_movies[0]
        query5_result["sample"]["top_movie"] = top_tagged.get_property("title")
        query5_result["sample"]["top_movie_tags"] = top_tagged.get_property("num_tags")
    results.append(query5_result)

    print(f"  Found top 10 tagged movies in {elapsed:.3f}s")
    print(f"  {'#':<4} {'Title':<50} {'Tags':<6}")
    print(f"  {'-'*4} {'-'*50} {'-'*6}")
    for i, record in enumerate(tagged_movies, 1):
        title = record.get_property("title")
        num = record.get_property("num_tags")
        print(f"  {i:<4} {title:<50.50} {num:<6}")

    if check_baseline and len(expected_queries) > 4:
        expected_sample = expected_queries[4].get("sample", {})
        exp_top_movie = expected_sample.get("top_movie")
        exp_top_tags = expected_sample.get("top_movie_tags")

        if exp_top_movie is not None and tagged_movies:
            actual_movie = tagged_movies[0].get_property("title")
            if actual_movie != exp_top_movie:
                print(
                    f"  ❌ Most tagged mismatch: expected '{exp_top_movie}', "
                    f"got '{actual_movie}'"
                )
                all_passed = False
            else:
                print(f"  ✓ Most tagged matches baseline: {actual_movie}")

        if exp_top_tags is not None and tagged_movies:
            actual_tags = tagged_movies[0].get_property("num_tags")
            if actual_tags != exp_top_tags:
                print(
                    f"  ❌ Tag count mismatch: expected {exp_top_tags}, "
                    f"got {actual_tags}"
                )
                all_passed = False
            else:
                print(f"  ✓ Tag count matches baseline: {actual_tags}")
    print()

    # Query 6: Collaborative Filtering - Same Movies (SQL MATCH)
    print("6. Users who rated same movies as User #1 (SQL - MATCH Pattern)")
    print("-" * 70)
    start = time.time()
    result = db.query(
        "sql",
        """
        SELECT friend.userId as other_user,
               movie.title as common_movie,
               a.rating as my_rating,
               b.rating as their_rating
        FROM (
          MATCH {type: User, where: (userId = 1), as: me}
                .outE('RATED'){as: a}
                .inV(){as: movie}
                .inE('RATED'){as: b}
                .outV(){as: friend, where: (userId != 1)}
          RETURN me, friend, movie, a, b
        )
        """,
    )
    collaborative = list(result)
    elapsed = time.time() - start

    query6_result = {
        "name": "Query 6: Users who rated same movies as User #1 (SQL - MATCH Pattern)",
        "count": len(collaborative),
    }
    results.append(query6_result)

    print(f"  Found {len(collaborative)} collaborative patterns in {elapsed:.3f}s")
    print(f"  {'User':<8} {'Movie':<40} {'My':<6} {'Their':<6}")
    print(f"  {'-'*8} {'-'*40} {'-'*6} {'-'*6}")
    for i, record in enumerate(collaborative):
        if i >= 10:  # Only display first 10
            break
        other = record.get_property("other_user")
        movie = record.get_property("common_movie")
        my_r = record.get_property("my_rating")
        their_r = record.get_property("their_rating")
        print(f"  {other:<8} {movie:<40.40} {my_r:<6.1f} {their_r:<6.1f}")

    if check_baseline and len(expected_queries) > 5:
        expected_count = expected_queries[5].get("count")
        if expected_count is not None and len(collaborative) != expected_count:
            print(
                f"  ❌ Count mismatch: expected {expected_count}, "
                f"got {len(collaborative)}"
            )
            all_passed = False
        elif expected_count is not None:
            print(f"  ✓ Count matches baseline: {len(collaborative)}")
    print()

    # Query 7: Users with Similar Tastes (SQL MATCH with Aggregation)
    print("7. Users with similar taste to User #1 (SQL - MATCH + Aggregation)")
    print("-" * 70)
    start = time.time()
    result = db.query(
        "sql",
        """
        SELECT friend.userId as similar_user,
               count(*) as shared_high_ratings
        FROM (
          MATCH {type: User, where: (userId = 1)}
                .outE('RATED'){where: (rating >= 4.5), as: myRating}
                .inV(){as: movie}
                .inE('RATED'){where: (rating >= 4.5), as: theirRating}
                .outV(){where: (userId != 1), as: friend}
          RETURN friend
        )
        GROUP BY friend.userId
        ORDER BY shared_high_ratings DESC
        """,
    )
    similar_users = list(result)
    elapsed = time.time() - start

    query7_result = {
        "name": "Query 7: Users with similar taste to User #1 (SQL - MATCH + Aggregation)",
        "count": len(similar_users),
    }
    results.append(query7_result)

    print(f"  Found {len(similar_users)} similar users in {elapsed:.3f}s")
    print(f"  {'User':<8} {'Shared High Ratings':<20}")
    print(f"  {'-'*8} {'-'*20}")
    for i, record in enumerate(similar_users):
        if i >= 10:  # Only display first 10
            break
        user = record.get_property("similar_user")
        shared = record.get_property("shared_high_ratings")
        print(f"  {user:<8} {shared:<20}")

    if check_baseline and len(expected_queries) > 6:
        expected_count = expected_queries[6].get("count")
        if expected_count is not None and len(similar_users) != expected_count:
            print(
                f"  ❌ Count mismatch: expected {expected_count}, "
                f"got {len(similar_users)}"
            )
            all_passed = False
        elif expected_count is not None:
            print(f"  ✓ Count matches baseline: {len(similar_users)}")
    print()

    # Query 8: Rating Distribution (SQL Aggregation)
    print("8. Rating distribution across all ratings (SQL - Aggregation)")
    print("-" * 70)
    start = time.time()
    result = db.query(
        "sql",
        """
        SELECT rating, count(*) as frequency
        FROM RATED
        GROUP BY rating
        ORDER BY rating
        """,
    )
    distribution = list(result)
    elapsed = time.time() - start

    query8_result = {
        "name": "Query 8: Rating distribution across all ratings (SQL - Aggregation)",
        "count": len(distribution),
    }
    results.append(query8_result)

    print(f"  Computed distribution in {elapsed:.3f}s")
    print(f"  {'Rating':<10} {'Frequency':<12} {'Bar':<40}")
    print(f"  {'-'*10} {'-'*12} {'-'*40}")
    if distribution:
        max_freq = max(r.get_property("frequency") for r in distribution)
    else:
        max_freq = 1
    for record in distribution:
        rating = record.get_property("rating")
        freq = record.get_property("frequency")
        bar_len = int((freq / max_freq) * 40)
        bar = "█" * bar_len
        print(f"  {rating:<10.1f} {freq:<12,} {bar}")

    if check_baseline and len(expected_queries) > 7:
        expected_count = expected_queries[7].get("count")
        if expected_count is not None and len(distribution) != expected_count:
            print(
                f"  ❌ Count mismatch: expected {expected_count}, "
                f"got {len(distribution)}"
            )
            all_passed = False
        elif expected_count is not None:
            print(f"  ✓ Count matches baseline: {len(distribution)}")
    print()

    # Query 9: Basic Cypher Pattern
    print("9. User #1's top-rated movies (Cypher - Basic Pattern)")
    print("-" * 70)
    start = time.time()
    result = db.query(
        "cypher",
        """
        MATCH (u:User {userId: 1})-[r:RATED]->(m:Movie)
        WHERE r.rating >= 4.0
        RETURN m.title as title, r.rating as rating
        ORDER BY r.rating DESC
        """,
    )
    cypher_results = list(result)
    elapsed = time.time() - start

    query9_result = {
        "name": "Query 9: User #1's top-rated movies (Cypher - Basic Pattern)",
        "count": len(cypher_results),
    }
    results.append(query9_result)

    print(f"  Found {len(cypher_results)} movies in {elapsed:.3f}s")
    print(f"  {'#':<4} {'Title':<50} {'Rating':<8}")
    print(f"  {'-'*4} {'-'*50} {'-'*8}")
    for i, record in enumerate(cypher_results):
        if i >= 10:  # Only display first 10
            break
        title = record.get_property("title")
        rating = record.get_property("rating")
        print(f"  {i+1:<4} {title:<50.50} {rating:<8.1f}")

    if check_baseline and len(expected_queries) > 8:
        expected_count = expected_queries[8].get("count")
        if expected_count is not None and len(cypher_results) != expected_count:
            print(
                f"  ❌ Count mismatch: expected {expected_count}, "
                f"got {len(cypher_results)}"
            )
            all_passed = False
        elif expected_count is not None:
            print(f"  ✓ Count matches baseline: {len(cypher_results)}")
    print()

    # Query 10: Collaborative Filtering (Cypher)
    print("10. Users who rated same movies as User #1 (Cypher - Pattern)")
    print("-" * 70)
    start = time.time()
    result = db.query(
        "cypher",
        """
        MATCH (u1:User {userId: 1})-[:RATED]->(m:Movie)<-[:RATED]-(u2:User)
        WHERE u2.userId <> 1
        RETURN u2.userId as other_user, count(m) as shared_movies
        ORDER BY shared_movies DESC
        """,
    )
    collab_cypher = list(result)
    elapsed = time.time() - start

    query10_result = {
        "name": "Query 10: Users who rated same movies as User #1 (Cypher - Pattern)",
        "count": len(collab_cypher),
        "sample": {},
    }
    if collab_cypher:
        top_user = collab_cypher[0]
        query10_result["sample"]["top_user_id"] = top_user.get_property("other_user")
        query10_result["sample"]["top_shared"] = top_user.get_property("shared_movies")
    results.append(query10_result)

    print(f"  Found {len(collab_cypher)} users in {elapsed:.3f}s")
    print(f"  {'User':<8} {'Shared Movies':<15}")
    print(f"  {'-'*8} {'-'*15}")
    for i, record in enumerate(collab_cypher):
        if i >= 10:  # Only display first 10
            break
        user = record.get_property("other_user")
        shared = record.get_property("shared_movies")
        print(f"  {user:<8} {shared:<15}")

    if check_baseline and len(expected_queries) > 9:
        expected_sample = expected_queries[9].get("sample", {})
        exp_top_id = expected_sample.get("top_user_id")
        exp_top_shared = expected_sample.get("top_shared")

        if exp_top_id is not None and collab_cypher:
            actual_top_id = collab_cypher[0].get_property("other_user")
            if actual_top_id != exp_top_id:
                print(
                    f"  ❌ Top user mismatch: expected {exp_top_id}, "
                    f"got {actual_top_id}"
                )
                all_passed = False
            else:
                print(f"  ✓ Top user matches baseline: {actual_top_id}")

        if exp_top_shared is not None and collab_cypher:
            actual_shared = collab_cypher[0].get_property("shared_movies")
            if actual_shared != exp_top_shared:
                print(
                    f"  ❌ Shared count mismatch: expected {exp_top_shared}, "
                    f"got {actual_shared}"
                )
                all_passed = False
            else:
                print(f"  ✓ Shared count matches baseline: {actual_shared}")
    print()

    # Print summary
    print("=" * 70)
    if check_baseline:
        if all_passed:
            print("✅ All results match baseline!")
        else:
            print("❌ Some results differ from baseline!")
    else:
        print("ℹ️  Baseline checking disabled (no expected values)")
    print("=" * 70)
    print()

    # Output JSON for easy copy-paste into EXPECTED_RESULTS
    print("📋 Query Results (JSON format for EXPECTED_RESULTS):")
    print("-" * 70)
    print(json.dumps(results, indent=8))
    print("-" * 70)
    print()

    return results, all_passed
    print()

    # Query 2: High-Rated Movies by User (SQL with Edge Filtering)
    print("Query 2: Movies rated 5.0 by User #1 (SQL - Edge Property Filter)")
    print("-" * 70)
    start = time.time()
    result = db.query(
        "sql",
        """
        SELECT expand(outE('RATED')[rating = 5.0].inV())
        FROM User WHERE userId = 1
        """,
    )
    high_rated = list(result)
    elapsed = time.time() - start

    query2_result = {
        "name": "Query 2: Movies rated 5.0 by User #1 (SQL - Edge Property Filter)",
        "count": len(high_rated),
    }
    results.append(query2_result)

    print(f"  Found {len(high_rated)} movies with 5.0 rating in {elapsed:.3f}s")
    if high_rated:
        for i, movie in enumerate(high_rated[:3]):
            print(f"  {i+1}. {movie.get_property('title')}")

    if check_baseline and len(expected_queries) > 1:
        expected_count = expected_queries[1].get("count")
        if expected_count is not None and len(high_rated) != expected_count:
            print(
                f"  ❌ Count mismatch: expected {expected_count}, "
                f"got {len(high_rated)}"
            )
            all_passed = False
        elif expected_count is not None:
            print(f"  ✓ Count matches baseline: {len(high_rated)}")
    print()

    # Query 3: Rating Statistics per User (SQL Aggregations)
    print("Query 3: Rating statistics for top 5 active users (SQL - Aggregations)")
    print("-" * 70)
    start = time.time()
    result = db.query(
        "sql",
        """
     SELECT u.userId as userId,
         COUNT(e) as num_ratings,
         AVG(e.rating) as avg_rating,
         MIN(e.rating) as min_rating,
         MAX(e.rating) as max_rating
     FROM (
       MATCH {type: User, as: u}.outE('RATED'){as: e} RETURN u,e
     )
     GROUP BY u.userId
     ORDER BY num_ratings DESC
     LIMIT 5
        """,
    )
    stats = list(result)
    elapsed = time.time() - start

    query3_result = {
        "name": "Query 3: Rating statistics for top 5 active users (SQL - Aggregations)",
        "count": len(stats),
        "sample": {},
    }
    if stats:
        top_user = stats[0]
        query3_result["sample"]["top_user_id"] = top_user.get_property("userId")
        query3_result["sample"]["top_user_ratings"] = top_user.get_property(
            "num_ratings"
        )
    results.append(query3_result)

    print(f"  Computed statistics for top users in {elapsed:.3f}s")
    print(f"  {'User':<8} {'#Ratings':<10} {'Avg':<8} {'Min':<6} {'Max':<6}")
    print(f"  {'-'*8} {'-'*10} {'-'*8} {'-'*6} {'-'*6}")
    for record in stats:
        user_id = record.get_property("userId")
        num = record.get_property("num_ratings")
        avg = record.get_property("avg_rating")
        min_r = record.get_property("min_rating")
        max_r = record.get_property("max_rating")
        print(f"  {user_id:<8} {num:<10} {avg:<8.2f} {min_r:<6.1f} {max_r:<6.1f}")

    if check_baseline and len(expected_queries) > 2:
        expected_sample = expected_queries[2].get("sample", {})
        exp_top_id = expected_sample.get("top_user_id")
        exp_top_ratings = expected_sample.get("top_user_ratings")

        if exp_top_id is not None and stats:
            actual_top_id = stats[0].get_property("userId")
            if actual_top_id != exp_top_id:
                print(
                    f"  ❌ Top user mismatch: expected {exp_top_id}, got {actual_top_id}"
                )
                all_passed = False
            else:
                print(f"  ✓ Top user matches baseline: {actual_top_id}")

        if exp_top_ratings is not None and stats:
            actual_ratings = stats[0].get_property("num_ratings")
            if actual_ratings != exp_top_ratings:
                print(
                    f"  ❌ Top user ratings mismatch: expected {exp_top_ratings}, got {actual_ratings}"
                )
                all_passed = False
            else:
                print(f"  ✓ Top user ratings match baseline: {actual_ratings}")
    print()

    # Query 4: Most Rated Movies (SQL Aggregations)
    print("Query 4: Top 10 most rated movies (SQL - Aggregations)")
    print("-" * 70)
    start = time.time()
    result = db.query(
        "sql",
        """
        SELECT m.movieId as movieId,
               m.title as title,
               COUNT(e) as num_ratings,
               AVG(e.rating) as avg_rating
        FROM (
          MATCH {type: Movie, as: m}.inE('RATED'){as: e} RETURN m, e
        )
        GROUP BY m.movieId, m.title
        ORDER BY num_ratings DESC
        LIMIT 10
        """,
    )
    top_movies = list(result)
    elapsed = time.time() - start

    query4_result = {
        "name": "Query 4: Top 10 most rated movies (SQL - Aggregations)",
        "count": len(top_movies),
        "sample": {},
    }
    if top_movies:
        top_movie = top_movies[0]
        query4_result["sample"]["top_movie"] = top_movie.get_property("title")
        query4_result["sample"]["top_movie_count"] = top_movie.get_property(
            "num_ratings"
        )
    results.append(query4_result)

    print(f"  Found top 10 movies in {elapsed:.3f}s")
    print(f"  {'#':<4} {'Title':<50} {'Ratings':<10} {'Avg':<6}")
    print(f"  {'-'*4} {'-'*50} {'-'*10} {'-'*6}")
    for i, record in enumerate(top_movies, 1):
        title = record.get_property("title")
        num = record.get_property("num_ratings")
        avg = record.get_property("avg_rating")
        print(f"  {i:<4} {title:<50.50} {num:<10} {avg:<6.2f}")

    if check_baseline and len(expected_queries) > 3:
        expected_sample = expected_queries[3].get("sample", {})
        exp_top_movie = expected_sample.get("top_movie")
        exp_top_count = expected_sample.get("top_movie_count")

        if exp_top_movie is not None and top_movies:
            actual_movie = top_movies[0].get_property("title")
            if actual_movie != exp_top_movie:
                print(
                    f"  ❌ Top movie mismatch: expected '{exp_top_movie}', got '{actual_movie}'"
                )
                all_passed = False
            else:
                print(f"  ✓ Top movie matches baseline: {actual_movie}")

        if exp_top_count is not None and top_movies:
            actual_count = top_movies[0].get_property("num_ratings")
            if actual_count != exp_top_count:
                print(
                    f"  ❌ Top movie count mismatch: expected {exp_top_count}, got {actual_count}"
                )
                all_passed = False
            else:
                print(f"  ✓ Top movie count matches baseline: {actual_count}")
    print()

    # Query 5: Most Tagged Movies (SQL Aggregations)
    print("Query 5: Top 10 most tagged movies (SQL - Aggregations)")
    print("-" * 70)
    start = time.time()
    result = db.query(
        "sql",
        """
        SELECT m.movieId as movieId,
               m.title as title,
               COUNT(e) as num_tags
        FROM (
          MATCH {type: Movie, as: m}.inE('TAGGED'){as: e} RETURN m, e
        )
        GROUP BY m.movieId, m.title
        ORDER BY num_tags DESC
        LIMIT 10
        """,
    )
    tagged_movies = list(result)
    elapsed = time.time() - start

    query5_result = {
        "name": "Query 5: Top 10 most tagged movies (SQL - Aggregations)",
        "count": len(tagged_movies),
        "sample": {},
    }
    if tagged_movies:
        top_tagged = tagged_movies[0]
        query5_result["sample"]["top_movie"] = top_tagged.get_property("title")
        query5_result["sample"]["top_movie_tags"] = top_tagged.get_property("num_tags")
    results.append(query5_result)

    print(f"  Found top 10 tagged movies in {elapsed:.3f}s")
    print(f"  {'#':<4} {'Title':<50} {'Tags':<6}")
    print(f"  {'-'*4} {'-'*50} {'-'*6}")
    for i, record in enumerate(tagged_movies, 1):
        title = record.get_property("title")
        num = record.get_property("num_tags")
        print(f"  {i:<4} {title:<50.50} {num:<6}")

    if check_baseline and len(expected_queries) > 4:
        expected_sample = expected_queries[4].get("sample", {})
        exp_top_movie = expected_sample.get("top_movie")
        exp_top_tags = expected_sample.get("top_movie_tags")

        if exp_top_movie is not None and tagged_movies:
            actual_movie = tagged_movies[0].get_property("title")
            if actual_movie != exp_top_movie:
                print(
                    f"  ❌ Most tagged mismatch: expected '{exp_top_movie}', got '{actual_movie}'"
                )
                all_passed = False
            else:
                print(f"  ✓ Most tagged matches baseline: {actual_movie}")

        if exp_top_tags is not None and tagged_movies:
            actual_tags = tagged_movies[0].get_property("num_tags")
            if actual_tags != exp_top_tags:
                print(
                    f"  ❌ Tag count mismatch: expected {exp_top_tags}, got {actual_tags}"
                )
                all_passed = False
            else:
                print(f"  ✓ Tag count matches baseline: {actual_tags}")
    print()

    # Query 6: Collaborative Filtering - Same Movies (SQL MATCH)
    print("Query 6: Users who rated same movies as User #1 (SQL - MATCH Pattern)")
    print("-" * 70)
    start = time.time()
    result = db.query(
        "sql",
        """
        SELECT friend.userId as other_user,
               movie.title as common_movie,
               a.rating as my_rating,
               b.rating as their_rating
        FROM (
          MATCH {type: User, where: (userId = 1), as: me}
                .outE('RATED'){as: a}
                .inV(){as: movie}
                .inE('RATED'){as: b}
                .outV(){as: friend, where: (userId != 1)}
          RETURN me, friend, movie, a, b
        )
        """,
    )
    collaborative = list(result)
    elapsed = time.time() - start

    query6_result = {
        "name": "Query 6: Users who rated same movies as User #1 (SQL - MATCH Pattern)",
        "count": len(collaborative),
    }
    results.append(query6_result)

    print(f"  Found {len(collaborative)} collaborative patterns in {elapsed:.3f}s")
    print(f"  {'User':<8} {'Movie':<40} {'My':<6} {'Their':<6}")
    print(f"  {'-'*8} {'-'*40} {'-'*6} {'-'*6}")
    for i, record in enumerate(collaborative):
        if i >= 10:  # Only display first 10
            break
        other = record.get_property("other_user")
        movie = record.get_property("common_movie")
        my_r = record.get_property("my_rating")
        their_r = record.get_property("their_rating")
        print(f"  {other:<8} {movie:<40.40} {my_r:<6.1f} {their_r:<6.1f}")

    if check_baseline and len(expected_queries) > 5:
        expected_count = expected_queries[5].get("count")
        if expected_count is not None and len(collaborative) != expected_count:
            print(
                f"  ❌ Count mismatch: expected {expected_count}, got {len(collaborative)}"
            )
            all_passed = False
        elif expected_count is not None:
            print(f"  ✓ Count matches baseline: {len(collaborative)}")
    print()

    # Query 7: Users with Similar Tastes (SQL MATCH with Aggregation)
    print("Query 7: Users with similar taste to User #1 (SQL - MATCH + Aggregation)")
    print("-" * 70)
    start = time.time()
    result = db.query(
        "sql",
        """
        SELECT friend.userId as similar_user,
               count(*) as shared_high_ratings
        FROM (
          MATCH {type: User, where: (userId = 1)}
                .outE('RATED'){where: (rating >= 4.5), as: myRating}
                .inV(){as: movie}
                .inE('RATED'){where: (rating >= 4.5), as: theirRating}
                .outV(){where: (userId != 1), as: friend}
          RETURN friend
        )
        GROUP BY friend.userId
        ORDER BY shared_high_ratings DESC
        """,
    )
    similar_users = list(result)
    elapsed = time.time() - start

    query7_result = {
        "name": "Query 7: Users with similar taste to User #1 (SQL - MATCH + Aggregation)",
        "count": len(similar_users),
    }
    results.append(query7_result)

    print(f"  Found {len(similar_users)} similar users in {elapsed:.3f}s")
    print(f"  {'User':<8} {'Shared High Ratings':<20}")
    print(f"  {'-'*8} {'-'*20}")
    for i, record in enumerate(similar_users):
        if i >= 10:  # Only display first 10
            break
        user = record.get_property("similar_user")
        shared = record.get_property("shared_high_ratings")
        print(f"  {user:<8} {shared:<20}")

    if check_baseline and len(expected_queries) > 6:
        expected_count = expected_queries[6].get("count")
        if expected_count is not None and len(similar_users) != expected_count:
            print(
                f"  ❌ Count mismatch: expected {expected_count}, got {len(similar_users)}"
            )
            all_passed = False
        elif expected_count is not None:
            print(f"  ✓ Count matches baseline: {len(similar_users)}")
    print()

    # Query 8: Rating Distribution (SQL Aggregation)
    print("Query 8: Rating distribution across all ratings (SQL - Aggregation)")
    print("-" * 70)
    start = time.time()
    result = db.query(
        "sql",
        """
        SELECT rating, count(*) as frequency
        FROM RATED
        GROUP BY rating
        ORDER BY rating
        """,
    )
    distribution = list(result)
    elapsed = time.time() - start

    query8_result = {
        "name": "Query 8: Rating distribution across all ratings (SQL - Aggregation)",
        "count": len(distribution),
    }
    results.append(query8_result)

    print(f"  Computed distribution in {elapsed:.3f}s")
    print(f"  {'Rating':<10} {'Frequency':<12} {'Bar':<40}")
    print(f"  {'-'*10} {'-'*12} {'-'*40}")
    if distribution:
        max_freq = max(r.get_property("frequency") for r in distribution)
    else:
        max_freq = 1
    for record in distribution:
        rating = record.get_property("rating")
        freq = record.get_property("frequency")
        bar_len = int((freq / max_freq) * 40)
        bar = "█" * bar_len
        print(f"  {rating:<10.1f} {freq:<12,} {bar}")

    if check_baseline and len(expected_queries) > 7:
        expected_count = expected_queries[7].get("count")
        if expected_count is not None and len(distribution) != expected_count:
            print(
                f"  ❌ Count mismatch: expected {expected_count}, got {len(distribution)}"
            )
            all_passed = False
        elif expected_count is not None:
            print(f"  ✓ Count matches baseline: {len(distribution)}")
    print()

    # Query 9: Basic Cypher Pattern
    print("Query 9: User #1's top-rated movies (Cypher - Basic Pattern)")
    print("-" * 70)
    start = time.time()
    result = db.query(
        "cypher",
        """
        MATCH (u:User {userId: 1})-[r:RATED]->(m:Movie)
        WHERE r.rating >= 4.0
        RETURN m.title as title, r.rating as rating
        ORDER BY r.rating DESC
        """,
    )
    cypher_results = list(result)
    elapsed = time.time() - start

    query9_result = {
        "name": "Query 9: User #1's top-rated movies (Cypher - Basic Pattern)",
        "count": len(cypher_results),
    }
    results.append(query9_result)

    print(f"  Found {len(cypher_results)} movies in {elapsed:.3f}s")
    print(f"  {'#':<4} {'Title':<50} {'Rating':<8}")
    print(f"  {'-'*4} {'-'*50} {'-'*8}")
    for i, record in enumerate(cypher_results):
        if i >= 10:  # Only display first 10
            break
        title = record.get_property("title")
        rating = record.get_property("rating")
        print(f"  {i+1:<4} {title:<50.50} {rating:<8.1f}")

    if check_baseline and len(expected_queries) > 8:
        expected_count = expected_queries[8].get("count")
        if expected_count is not None and len(cypher_results) != expected_count:
            print(
                f"  ❌ Count mismatch: expected {expected_count}, got {len(cypher_results)}"
            )
            all_passed = False
        elif expected_count is not None:
            print(f"  ✓ Count matches baseline: {len(cypher_results)}")
    print()

    # Query 10: Collaborative Filtering (Cypher)
    print("Query 10: Users who rated same movies as User #1 (Cypher - Pattern)")
    print("-" * 70)
    start = time.time()
    result = db.query(
        "cypher",
        """
        MATCH (u1:User {userId: 1})-[:RATED]->(m:Movie)<-[:RATED]-(u2:User)
        WHERE u2.userId <> 1
        RETURN u2.userId as other_user, count(m) as shared_movies
        ORDER BY shared_movies DESC
        """,
    )
    collab_cypher = list(result)
    elapsed = time.time() - start

    query10_result = {
        "name": "Query 10: Users who rated same movies as User #1 (Cypher - Pattern)",
        "count": len(collab_cypher),
        "sample": {},
    }
    if collab_cypher:
        top_user = collab_cypher[0]
        query10_result["sample"]["top_user_id"] = top_user.get_property("other_user")
        query10_result["sample"]["top_shared"] = top_user.get_property("shared_movies")
    results.append(query10_result)

    print(f"  Found {len(collab_cypher)} users in {elapsed:.3f}s")
    print(f"  {'User':<8} {'Shared Movies':<15}")
    print(f"  {'-'*8} {'-'*15}")
    for i, record in enumerate(collab_cypher):
        if i >= 10:  # Only display first 10
            break
        user = record.get_property("other_user")
        shared = record.get_property("shared_movies")
        print(f"  {user:<8} {shared:<15}")

    if check_baseline and len(expected_queries) > 9:
        expected_sample = expected_queries[9].get("sample", {})
        exp_top_id = expected_sample.get("top_user_id")
        exp_top_shared = expected_sample.get("top_shared")

        if exp_top_id is not None and collab_cypher:
            actual_top_id = collab_cypher[0].get_property("other_user")
            if actual_top_id != exp_top_id:
                print(
                    f"  ❌ Top user mismatch: expected {exp_top_id}, got {actual_top_id}"
                )
                all_passed = False
            else:
                print(f"  ✓ Top user matches baseline: {actual_top_id}")

        if exp_top_shared is not None and collab_cypher:
            actual_shared = collab_cypher[0].get_property("shared_movies")
            if actual_shared != exp_top_shared:
                print(
                    f"  ❌ Shared count mismatch: expected {exp_top_shared}, got {actual_shared}"
                )
                all_passed = False
            else:
                print(f"  ✓ Shared count matches baseline: {actual_shared}")
    print()

    # Print summary
    print("=" * 70)
    if check_baseline:
        if all_passed:
            print("✅ All results match baseline!")
        else:
            print("❌ Some results differ from baseline!")
    else:
        print("ℹ️  Baseline checking disabled (no expected values)")
    print("=" * 70)
    print()

    # Output JSON for easy copy-paste into EXPECTED_RESULTS
    print("📋 Query Results (JSON format for EXPECTED_RESULTS):")
    print("-" * 70)
    print(json.dumps(results, indent=8))
    print("-" * 70)
    print()

    return results, all_passed


def run_advanced_queries(db: Any, size: str):
    """
    DEPRECATED: Use validate_and_query() or run_and_validate_queries() instead.

    This function is kept for backwards compatibility but just wraps
    run_and_validate_queries().
    """
    _, validation_passed = run_and_validate_queries(db, size, check_baseline=True)
    return validation_passed


def main():
    parser = argparse.ArgumentParser(description="Graph Creation Benchmark")
    parser.add_argument(
        "--size",
        choices=["small", "large"],
        default="small",
        help="Dataset size (default: small)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=5000,
        help="Batch size for operations (default: 5000)",
    )
    parser.add_argument(
        "--parallel",
        type=int,
        default=4,
        help="Parallel level for async executor (1-16, default: 4)",
    )
    parser.add_argument(
        "--method",
        choices=["java", "sql"],
        default="java",
        help="Creation method: 'java' (Java API - RECOMMENDED) or 'sql' (SQL commands)",
    )
    parser.add_argument(
        "--no-async",
        action="store_true",
        help="Disable async executor (use synchronous transactions - slower)",
    )
    parser.add_argument(
        "--no-index",
        action="store_true",
        help="Skip creating indexes (will be slower)",
    )
    parser.add_argument(
        "--db-name",
        type=str,
        default=None,
        help="Database name (default: ml_graph_{size}_db)",
    )
    parser.add_argument(
        "--source-db",
        type=str,
        default=None,
        help="Custom source database path (default: ./my_test_databases/ml_{size}_db)",
    )
    parser.add_argument(
        "--import-jsonl",
        type=str,
        default=None,
        help="Import from JSONL export instead of using source-db "
        "(e.g., ./exports/ml_small_db.jsonl.tgz)",
    )
    parser.add_argument(
        "--skip-queries",
        action="store_true",
        help="Skip advanced graph queries (faster for benchmarking)",
    )
    parser.add_argument(
        "--export",
        action="store_true",
        help="Export graph database to JSONL after creation (for reproducibility)",
    )
    parser.add_argument(
        "--export-path",
        type=str,
        default=None,
        help="Export filename (default: {db_name}.jsonl.tgz in exports/ directory)",
    )
    args = parser.parse_args()

    # Track total script execution time
    script_start_time = time.time()

    # Set default database name if not provided
    if args.db_name is None:
        db_name = f"ml_graph_{args.size}_db"
    else:
        db_name = args.db_name

    print("=" * 70)
    print("🚀 Graph Creation Benchmark")
    print("=" * 70)
    print(f"Dataset: {args.size}")
    print(f"Batch size: {args.batch_size:,}")
    print(f"Parallel level: {args.parallel}")
    print(f"Method: {args.method} API")
    print(f"Async: {'disabled' if args.no_async else 'enabled'}")
    print(f"Indexes: {'disabled' if args.no_index else 'enabled'}")
    print(f"Database: {db_name}")

    # Display export configuration
    if args.export:
        if args.export_path:
            display_path = args.export_path
        else:
            display_path = f"exports/{db_name}.jsonl.tgz"
        print(f"💾 Export: enabled → {display_path}")
    else:
        print("💾 Export: disabled (use --export to enable)")

    print()

    # Handle source data loading
    import_time = 0.0

    if args.import_jsonl:
        # Import from JSONL export
        jsonl_path = Path(args.import_jsonl)
        if not jsonl_path.exists():
            print(f"❌ JSONL export not found: {jsonl_path}")
            print(
                f"   Export a database with: python 04_csv_import_documents.py "
                f"--size {args.size} --export"
            )
            sys.exit(1)

        print(f"✓ Found JSONL export: {jsonl_path}")
        print(f"  Size: {jsonl_path.stat().st_size / (1024 * 1024):.2f} MB")
        print()

        # Create temporary database from import
        doc_db_path = Path(f"./my_test_databases/ml_{args.size}_db_imported")
        print("Step 0: Importing Source Database from JSONL")
        print("=" * 70)
        import_time = import_from_jsonl(jsonl_path, doc_db_path)

    else:
        # Use existing database
        if args.source_db:
            doc_db_path = Path(args.source_db)
        else:
            doc_db_path = Path(f"./my_test_databases/ml_{args.size}_db")

        if not doc_db_path.exists():
            print(f"❌ Source database not found: {doc_db_path}")
            print(f"   Run: python 04_csv_import_documents.py --size {args.size}")
            print("   OR use --import-jsonl to import from JSONL export")
            sys.exit(1)

        print(f"✓ Found source database: {doc_db_path}")
        print()

    # Create graph database
    graph_db_path = Path(f"./my_test_databases/{db_name}")
    if graph_db_path.exists():
        shutil.rmtree(graph_db_path)

    # Initialize components
    data_loader = DataLoader(doc_db_path, args.batch_size)
    use_java_api = args.method == "java"
    use_async = not args.no_async
    create_indexes = not args.no_index

    # Step 1: Create schema
    print("Step 1: Creating Schema")
    print("=" * 70)
    with arcadedb.create_database(str(graph_db_path)) as db:
        create_schema(db, create_indexes)

    # Step 2: Load data and count
    print("Step 2: Loading Data")
    print("=" * 70)
    links_data = data_loader.load_links_paginated()
    total_users, total_movies, total_ratings, total_tags = data_loader.count_data()
    print()

    # Step 3: Create vertices
    print("Step 3: Creating Vertices")
    print("=" * 70)
    with arcadedb.open_database(str(graph_db_path)) as db:
        vertex_creator = VertexCreator(
            db,
            data_loader,
            args.batch_size,
            args.parallel,
            use_java_api,
            use_async,
        )
        user_count, movie_count, user_stats, movie_stats = (
            vertex_creator.create_all_vertices(total_users, total_movies, links_data)
        )

    # Step 4: Create edges
    print("Step 4: Creating Edges")
    print("=" * 70)
    with arcadedb.open_database(str(graph_db_path)) as db:
        edge_creator = EdgeCreator(
            db,
            data_loader,
            args.batch_size,
            use_java_api,
            create_indexes,
            use_async,
            args.parallel,
        )
        rated_count, tagged_count, rated_stats, tagged_stats = (
            edge_creator.create_all_edges(total_ratings, total_tags)
        )

    # Step 5: Validation & Query Testing
    print("Step 5: Validation & Query Testing")
    print("=" * 70)

    query_results_before = []
    validation_passed_before = False

    if not args.skip_queries:
        with arcadedb.open_database(str(graph_db_path)) as db:
            query_results_before, validation_passed_before = validate_and_query(
                db,
                args.size,
                user_count,
                movie_count,
                rated_count,
                tagged_count,
                check_baseline=True,
            )

        # Save query results for easy copy-paste
        if query_results_before:
            try:
                results_file = save_query_results(
                    query_results_before, args.size, graph_db_path
                )
                print(f"💾 Query results saved to: {results_file}")
                print()
            except Exception as e:
                print(f"⚠️  Failed to save query results: {e}")
                print()

        if validation_passed_before:
            print("✅ Step 5: All validation & queries passed!")
        else:
            print("❌ Step 5: Some validations or queries failed!")
        print()
    else:
        # Still run basic validation even if queries are skipped
        with arcadedb.open_database(str(graph_db_path)) as db:
            validate_results(
                db, args.size, user_count, movie_count, rated_count, tagged_count
            )
        print("⏭️  Query testing skipped (--skip-queries flag)")
        print()

    # Step 6: Export Database (Optional)
    export_filename = None
    export_time = 0.0

    if args.export:
        print("Step 6: Exporting Graph Database to JSONL")
        print("=" * 70)
        step_start = time.time()

        # Determine export path
        if args.export_path:
            export_filename = args.export_path
        else:
            export_filename = f"{db_name}.jsonl.tgz"

        print(f"   💾 Exporting to: {export_filename}")
        print("   ⏳ This may take a while for large graphs...")
        print()

        try:
            with arcadedb.open_database(str(graph_db_path)) as db:
                stats = db.export_database(
                    export_filename, format="jsonl", overwrite=True, verbose=2
                )

            export_time = time.time() - step_start

            print("   ✅ Export complete!")
            print(f"      • Total records: {stats.get('totalRecords', 0):,}")
            print(f"      • Vertices: {stats.get('vertices', 0):,}")
            print(f"      • Edges: {stats.get('edges', 0):,}")
            print(f"   ⏱️  Time: {export_time:.3f}s")

            # Show file size
            if os.path.exists(export_filename):
                file_size = os.path.getsize(export_filename)
                size_mb = file_size / (1024 * 1024)
                print(f"   📦 File size: {size_mb:.2f} MB")

            print()
            print("   💡 Export benefits:")
            print("      • Reproducible benchmarks - share pre-populated graphs")
            print("      • Backup - full graph snapshot with schema")
            print("      • Testing - create test fixtures from real data")
            print("      • Migration - move databases between environments")
            print()

        except Exception as e:
            print(f"   ❌ Export failed: {e}")
            print(f"   ⏱️  Time: {time.time() - step_start:.3f}s")
            export_filename = None

        print()
    else:
        print("Step 6: Database export skipped (use --export to enable)")
        print()

    # Step 7: Roundtrip Validation (if export was done)
    roundtrip_import_time = 0.0

    if args.export and export_filename:
        # Determine actual export path
        if args.export_path:
            actual_export_path = export_filename
            if not os.path.exists(actual_export_path) and os.path.exists(
                f"exports/{export_filename}"
            ):
                actual_export_path = f"exports/{export_filename}"
        else:
            actual_export_path = f"exports/{export_filename}"

        if os.path.exists(actual_export_path):
            print("Step 7: Roundtrip Validation (export → import → verify)")
            print("=" * 70)
            step_start = time.time()

            # Create a new database for import testing
            roundtrip_db_path = Path(f"./my_test_databases/{db_name}_roundtrip")
            if roundtrip_db_path.exists():
                shutil.rmtree(roundtrip_db_path)

            print(f"   📂 Creating roundtrip database: {roundtrip_db_path}")

            try:
                with arcadedb.create_database(str(roundtrip_db_path)) as roundtrip_db:
                    print("   ✅ Roundtrip database created")
                    print()

                    # Import from the exported file
                    print(f"   📥 Importing from: {actual_export_path}")
                    print("   ⏳ This may take a while...")
                    print()

                    import_start = time.time()

                    # Use SQL IMPORT DATABASE command
                    import_path = os.path.abspath(actual_export_path)
                    import_sql = f"IMPORT DATABASE file://{import_path}"
                    roundtrip_db.command("sql", import_sql)

                    roundtrip_import_time = time.time() - import_start

                    print(f"   ✅ Import complete in {roundtrip_import_time:.3f}s")

                    # Calculate import rate
                    total_records = (
                        user_count + movie_count + rated_count + tagged_count
                    )
                    import_rate = (
                        total_records / roundtrip_import_time
                        if roundtrip_import_time > 0
                        else 0
                    )
                    print(f"   ⏱️  Rate: {import_rate:,.0f} records/sec")
                    print()

                    # Verify record counts match
                    print("   🔍 Verifying record counts...")
                    validation_passed = True

                    # Count vertices
                    result = list(
                        roundtrip_db.query("sql", "SELECT count(*) as count FROM User")
                    )
                    rt_user_count = result[0].get_property("count")

                    result = list(
                        roundtrip_db.query("sql", "SELECT count(*) as count FROM Movie")
                    )
                    rt_movie_count = result[0].get_property("count")

                    # Count edges
                    result = list(
                        roundtrip_db.query("sql", "SELECT count(*) as count FROM RATED")
                    )
                    rt_rated_count = result[0].get_property("count")

                    result = list(
                        roundtrip_db.query(
                            "sql", "SELECT count(*) as count FROM TAGGED"
                        )
                    )
                    rt_tagged_count = result[0].get_property("count")

                    # Compare counts
                    if rt_user_count == user_count:
                        print(f"      ✅ Users: {rt_user_count:,} (matches)")
                    else:
                        print(
                            f"      ❌ Users: {rt_user_count:,} (expected {user_count:,})"
                        )
                        validation_passed = False

                    if rt_movie_count == movie_count:
                        print(f"      ✅ Movies: {rt_movie_count:,} (matches)")
                    else:
                        print(
                            f"      ❌ Movies: {rt_movie_count:,} (expected {movie_count:,})"
                        )
                        validation_passed = False

                    if rt_rated_count == rated_count:
                        print(f"      ✅ RATED edges: {rt_rated_count:,} (matches)")
                    else:
                        print(
                            f"      ❌ RATED edges: {rt_rated_count:,} (expected {rated_count:,})"
                        )
                        validation_passed = False

                    if rt_tagged_count == tagged_count:
                        print(f"      ✅ TAGGED edges: {rt_tagged_count:,} (matches)")
                    else:
                        print(
                            f"      ❌ TAGGED edges: {rt_tagged_count:,} (expected {tagged_count:,})"
                        )
                        validation_passed = False

                    print()

                    # Verify sample data
                    print("   🔍 Verifying sample data (User #1)...")
                    sample_user_query = """
                        SELECT out('RATED').size() as rating_count,
                               out('TAGGED').size() as tag_count
                        FROM User
                        WHERE userId = 1
                    """
                    sample_results = list(roundtrip_db.query("sql", sample_user_query))
                    if sample_results:
                        rt_user1_ratings = sample_results[0].get_property(
                            "rating_count"
                        )
                        rt_user1_tags = sample_results[0].get_property("tag_count")

                        # Get expected values
                        expected = get_expected_values(args.size)
                        exp_ratings = expected.get("user1_ratings", 0)
                        exp_tags = expected.get("user1_tags", 0)

                        if rt_user1_ratings == exp_ratings:
                            print(
                                f"      ✅ User #1 ratings: {rt_user1_ratings} (matches)"
                            )
                        else:
                            print(
                                f"      ❌ User #1 ratings: {rt_user1_ratings} (expected {exp_ratings})"
                            )
                            validation_passed = False

                        if rt_user1_tags == exp_tags:
                            print(f"      ✅ User #1 tags: {rt_user1_tags} (matches)")
                        else:
                            print(
                                f"      ❌ User #1 tags: {rt_user1_tags} (expected {exp_tags})"
                            )
                            validation_passed = False

                    print()

                    # Run queries on roundtrip database and validate against baseline
                    if not args.skip_queries:
                        print("   🔍 Running queries on roundtrip database...")
                        print()

                        _, validation_passed_after = run_and_validate_queries(
                            roundtrip_db, args.size, check_baseline=True
                        )

                        validation_passed = (
                            validation_passed and validation_passed_after
                        )
                        print()

                    # Report roundtrip times
                    total_roundtrip_time = export_time + roundtrip_import_time
                    print("   📊 Roundtrip Performance:")
                    print(f"      • Export time:  {export_time:.3f}s")
                    print(f"      • Import time:  {roundtrip_import_time:.3f}s")
                    print(f"      • Total:        {total_roundtrip_time:.3f}s")
                    print()

                    # Print final validation status
                    if validation_passed:
                        print("   ✅ Step 7: All validation & queries passed!")
                    else:
                        print("   ❌ Step 7: Some validations or queries failed!")
                    print()

                # Clean up roundtrip database
                print("   🧹 Cleaning up roundtrip database...")
                shutil.rmtree(roundtrip_db_path)
                print("   ✅ Roundtrip database removed")
                print()

            except Exception as e:
                print(f"   ❌ Roundtrip validation failed: {e}")
                print()
                # Try to clean up if exists
                if roundtrip_db_path.exists():
                    shutil.rmtree(roundtrip_db_path)
        else:
            print(
                f"Step 7: Roundtrip validation skipped "
                f"(export file not found: {actual_export_path})"
            )
            print()
    else:
        print("Step 7: Roundtrip validation skipped (no export)")
        print()

    # Summary
    print("=" * 70)
    print("Summary")
    print("=" * 70)

    if import_time > 0:
        print(f"JSONL Import: {import_time:.2f}s")
        print()

    total_vertex_time = user_stats.duration + movie_stats.duration
    total_edge_time = rated_stats.duration + tagged_stats.duration
    total_creation_time = total_vertex_time + total_edge_time
    total_script_time = time.time() - script_start_time

    print(f"Vertices: {user_count + movie_count:,} in {total_vertex_time:.2f}s")
    print(f"  ({(user_count + movie_count) / total_vertex_time:.0f} vertices/sec)")
    print()
    print(f"Edges: {rated_count + tagged_count:,} in {total_edge_time:.2f}s")
    print(f"  ({(rated_count + tagged_count) / total_edge_time:.0f} edges/sec)")
    print()
    print(f"Creation time: {total_creation_time:.2f}s (vertices + edges only)")
    print()

    if export_time > 0:
        print(f"Export time: {export_time:.2f}s")
        if roundtrip_import_time > 0:
            print(f"Roundtrip import time: {roundtrip_import_time:.2f}s")
            total_roundtrip = export_time + roundtrip_import_time
            print(f"Total roundtrip time: {total_roundtrip:.2f}s")
        print()

    if import_time > 0:
        print(
            f"Total script time: {total_script_time:.2f}s "
            f"(includes JSONL import: {import_time:.2f}s, "
            f"schema, loading, validation)"
        )
    else:
        print(
            f"Total script time: {total_script_time:.2f}s "
            f"(includes schema, loading, validation)"
        )
    print("=" * 70)


if __name__ == "__main__":
    main()
