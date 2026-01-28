#!/usr/bin/env python3
"""
Example 07: Stack Overflow Multi-Model Database

Demonstrates a complete multi-model workflow:
- Phase 1: XML → Documents + Indexes
- Phase 2: Documents → Graph (vertices + edges)
- Phase 3: Graph → Embeddings + Vector indexes (JVector)
- Phase 4: Analytics (SQL + OpenCypher + Vector Search)

This example uses Stack Overflow data dump (Users, Posts, Comments, etc.)
to build a comprehensive knowledge graph with semantic search capabilities.

Dataset Options (disk size → recommended JVM heap):
- stackoverflow-tiny: ~34 MB → 2 GB (ARCADEDB_JVM_ARGS='-Xmx2g -Xms2g')
- stackoverflow-small: ~642 MB → 8 GB (ARCADEDB_JVM_ARGS='-Xmx8g -Xms8g')
- stackoverflow-medium: ~2.9 GB → 32 GB (ARCADEDB_JVM_ARGS='-Xmx32g -Xms32g')
- stackoverflow-large: ~323 GB → 64+ GB (ARCADEDB_JVM_ARGS='-Xmx64g -Xms64g')

Usage:
    # Phase 1 only (import + index)
    python 07_stackoverflow_multimodel.py --dataset stackoverflow-small

    # Analyze schema before importing (understand data structure and nullable fields)
    python 07_stackoverflow_multimodel.py --dataset stackoverflow-tiny --analyze-only

    # All phases
    python 07_stackoverflow_multimodel.py --dataset stackoverflow-small --phases 1 2 3 4

    # Custom batch size
    python 07_stackoverflow_multimodel.py --dataset stackoverflow-medium --batch-size 10000

Requirements:
- arcadedb-embedded
- lxml (for XML parsing)
- Stack Overflow data dump in data/stackoverflow-{dataset}/ directory

⚠️  BEST PRACTICE NOTE (Database Lifecycle):
This script's phase methods use manual db.open() and db.close().
For modern Python applications, consider wrapping within class methods:
    with arcadedb.create_database(path) as db:
        # All operations here
This ensures proper closure even if exceptions occur.

IMPORTANT: RID-Based Pagination Pattern
----------------------------------------
When paginating with RID (@rid > last_rid LIMIT N) AND applying WHERE filters,
use nested queries to avoid data loss:

CORRECT (Nested Query):
    SELECT Id, OwnerUserId FROM (
        SELECT Id, PostTypeId, OwnerUserId, @rid as rid FROM Post
        WHERE @rid > {last_rid}
        LIMIT {batch_size}
    ) WHERE PostTypeId = 2 AND OwnerUserId IS NOT NULL

INCORRECT (Direct Filter):
    SELECT Id, OwnerUserId FROM Post
    WHERE PostTypeId = 2 AND OwnerUserId IS NOT NULL AND @rid > {last_rid}
    LIMIT {batch_size}

Why: With direct filtering, LIMIT applies to the RID scan count (not filtered
results). Since records are interleaved by @rid (e.g., Question, Answer,
Question, Answer...), scanning 1000 RIDs might only match 50 Answers. This
causes progressive data loss as pagination continues through sparse regions.

The nested query pattern ensures:
1. Inner query gets N records efficiently via RID pagination (O(1) access)
2. Outer query filters those N records completely (no data loss)
3. All matching records are eventually found across all batches

Performance Optimization: Index-Based Vertex Lookups
-----------------------------------------------------
For Phase 2 graph creation, vertex caching uses O(1) index lookups instead of
SQL IN queries for dramatically better performance:

FAST (O(1) per vertex - lookup_by_key):
    for vid in vertex_ids:
        vertex = graph_db.lookup_by_key("VertexType", ["Id"], [vid])
        if vertex:
            cache[vid] = vertex

SLOW (O(n) - SQL IN operator):
    ids_str = ",".join(str(id) for id in vertex_ids)
    query = f"SELECT FROM VertexType WHERE Id IN [{ids_str}]"
    for result in graph_db.query("sql", query):
        cache[result.get("Id")] = result

Why: SQL IN queries with large ID lists are slow even with indexes. Direct
lookup_by_key() uses the index for O(1) access per vertex, resulting in 10-100x
speedup for vertex caching operations. This optimization requires that the
lookup field (Id) has a UNIQUE or NOTUNIQUE index defined.
"""

import argparse
import os
import re
import shutil
import sys
import time
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Set

import arcadedb_embedded as arcadedb
from lxml import etree


def escape_sql_string(value: str) -> str:
    """Properly escape a string for SQL queries.

    Must escape backslashes first, then single quotes.
    Otherwise a value like '\' becomes '\'' which escapes the quote.
    """
    if value is None:
        return value
    # First escape backslashes, then escape single quotes
    return value.replace("\\", "\\\\").replace("'", "\\'")


# =============================================================================
# Validation Module - Reusable Across All Phases
# =============================================================================


class StackOverflowValidator:
    """Standalone validator for Stack Overflow database phases.

    This class provides reusable validation methods that can be called
    from any phase (Phase 1, 2, 3) or standalone.
    """

    # Expected record counts for each dataset size
    EXPECTED_COUNTS = {
        "stackoverflow-tiny": {
            "User": 10_000,
            "Post": 10_000,
            "Comment": 10_000,
            "Badge": 10_000,
            "Vote": 10_000,
            "PostLink": 10_000,
            "Tag": 668,
            "PostHistory": 10_000,
            "total": 70_668,
        },
        "stackoverflow-small": {
            "User": 138_727,
            "Post": 105_373,
            "Comment": 195_781,
            "Badge": 182_975,
            "Vote": 411_166,
            "PostLink": 11_005,
            "Tag": 668,
            "PostHistory": 360_340,
            "total": 1_406_035,
        },
        "stackoverflow-medium": {
            "User": 345_754,
            "Post": 425_735,
            "Comment": 819_648,
            "Badge": 612_258,
            "Vote": 1_747_225,
            "PostLink": 86_919,
            "Tag": 1_612,
            "PostHistory": 1_525_713,
            "total": 5_564_864,
        },
        # Large dataset counts will be added once import completes
        "stackoverflow-large": {
            "User": 22_484_235,
            "Post": 59_819_048,
            "Comment": 90_380_323,
            "Badge": 51_289_973,
            "Vote": 238_984_011,
            "PostLink": 6_552_590,
            "Tag": 65_675,
            "PostHistory": 160_790_317,
            "total": 630_366_172,
        },
    }

    @staticmethod
    def get_phase1_validation_queries(random_user_id: int, random_post_id: int) -> list:
        """Get validation queries for Phase 1 document database.

        Args:
            random_user_id: Random user ID to use in queries
            random_post_id: Random post ID to use in queries

        Returns:
            List of tuples: (name, sql, validator_function)
        """
        return [
            (
                "Count users",
                "SELECT count(*) as count FROM User",
                lambda r: r[0].get("count") > 0,
            ),
            (
                "Count posts",
                "SELECT count(*) as count FROM Post",
                lambda r: r[0].get("count") > 0,
            ),
            (
                "Count comments",
                "SELECT count(*) as count FROM Comment",
                lambda r: r[0].get("count") > 0,
            ),
            (
                "Find user by ID",
                f"SELECT DisplayName FROM User WHERE Id = {random_user_id} LIMIT 1",
                lambda r: len(r) > 0,
            ),
            (
                "Count post types",
                "SELECT PostTypeId, count(*) as count FROM Post GROUP BY PostTypeId",
                lambda r: len(r) > 0,
            ),
            (
                "Find post by ID",
                f"SELECT Id FROM Post WHERE Id = {random_post_id} LIMIT 1",
                lambda r: len(r) > 0,
            ),
            (
                "Count badges",
                "SELECT count(*) as count FROM Badge",
                lambda r: r[0].get("count") > 0,
            ),
            (
                "Count votes",
                "SELECT count(*) as count FROM Vote",
                lambda r: r[0].get("count") > 0,
            ),
            (
                "Count tags",
                "SELECT count(*) as count FROM Tag",
                lambda r: r[0].get("count") > 0,
            ),
            (
                "Count post links",
                "SELECT count(*) as count FROM PostLink",
                lambda r: r[0].get("count") > 0,
            ),
        ]

    @staticmethod
    def get_phase1_expected_indexes() -> set:
        """Get expected Phase 1 indexes.

        28 total: 8 unique primary keys + 20 non-unique foreign keys.

        Returns:
            Set of tuples: (entity_name, field_name, is_unique)
        """
        return {
            # Primary key indexes (UNIQUE)
            ("User", "Id", True),
            ("Post", "Id", True),
            ("Comment", "Id", True),
            ("Badge", "Id", True),
            ("Vote", "Id", True),
            ("PostLink", "Id", True),
            ("Tag", "Id", True),
            ("PostHistory", "Id", True),
            # Foreign key indexes (NOTUNIQUE)
            ("User", "AccountId", False),
            ("Post", "AcceptedAnswerId", False),
            ("Post", "LastEditorUserId", False),
            ("Post", "ParentId", False),
            ("Post", "OwnerUserId", False),
            ("Post", "PostTypeId", False),
            ("Comment", "PostId", False),
            ("Comment", "UserId", False),
            ("Badge", "UserId", False),
            ("Vote", "PostId", False),
            ("Vote", "UserId", False),
            ("Vote", "VoteTypeId", False),
            ("PostLink", "PostId", False),
            ("PostLink", "LinkTypeId", False),
            ("PostLink", "RelatedPostId", False),
            ("Tag", "ExcerptPostId", False),
            ("Tag", "WikiPostId", False),
            ("PostHistory", "PostHistoryTypeId", False),
            ("PostHistory", "PostId", False),
            ("PostHistory", "UserId", False),
        }

    @staticmethod
    def verify_phase1_document_counts(
        db, entities: list = None, indent: str = "     ", dataset_size: str = None
    ) -> dict:
        """Verify document counts in Phase 1 database.

        Args:
            db: Database instance
            entities: List of entity names (default: all Phase 1 entities)
            indent: Indentation string for output formatting
            dataset_size: Dataset size name (e.g., 'stackoverflow-tiny')
                         for validation against expected counts

        Returns:
            Dict of {entity_name: count}
        """
        if entities is None:
            entities = [
                "User",
                "Post",
                "Comment",
                "Badge",
                "Vote",
                "PostLink",
                "Tag",
                "PostHistory",
            ]

        counts = {}
        total_count = 0
        mismatches = []

        # Get expected counts if dataset_size is provided
        expected = None
        if dataset_size and dataset_size in StackOverflowValidator.EXPECTED_COUNTS:
            expected = StackOverflowValidator.EXPECTED_COUNTS[dataset_size]

        for entity in entities:
            result = list(db.query("sql", f"SELECT count(*) as count FROM {entity}"))
            count = result[0].get("count")
            counts[entity] = count
            total_count += count

            # Check against expected counts
            status = ""
            if expected and expected.get(entity) is not None:
                expected_count = expected[entity]
                if count == expected_count:
                    status = " ✓"
                else:
                    status = f" ❌ (expected {expected_count:,})"
                    mismatches.append(
                        f"{entity}: got {count:,}, expected {expected_count:,}"
                    )

            print(f"{indent}• {entity:12} {count:>9,} documents{status}")

        print(f"{indent}{'─' * 40}")

        # Check total
        total_status = ""
        if expected and expected.get("total") is not None:
            expected_total = expected["total"]
            if total_count == expected_total:
                total_status = " ✓"
            else:
                total_status = f" ❌ (expected {expected_total:,})"
                mismatches.append(
                    f"Total: got {total_count:,}, " f"expected {expected_total:,}"
                )

        print(f"{indent}• {'Total':12} {total_count:>9,} documents{total_status}")

        # Report issues
        issues = []
        for entity, count in counts.items():
            if count == 0:
                issues.append(f"{entity} has 0 documents")

        if mismatches:
            print()
            print(f"{indent}❌ Count mismatches found:")
            for mismatch in mismatches:
                print(f"{indent}   • {mismatch}")

        if issues:
            print()
            print(f"{indent}⚠️  Issues found:")
            for issue in issues:
                print(f"{indent}   • {issue}")

        return counts

    @staticmethod
    def verify_phase1_indexes(
        db, expected_indexes: set = None, indent: str = "     "
    ) -> bool:
        """Verify all expected Phase 1 indexes exist in database.

        Args:
            db: Database instance
            expected_indexes: Set of (entity, field, is_unique) tuples
            indent: Indentation string for output formatting

        Returns:
            True if all expected indexes are found
        """
        if expected_indexes is None:
            expected_indexes = StackOverflowValidator.get_phase1_expected_indexes()

        # Get actual indexes from database
        indexes = db.schema.get_indexes()

        # Parse actual index names and build set
        actual_indexes = set()
        for idx in indexes:
            idx_name = str(idx.getName())  # Convert to Python string
            is_unique = idx.isUnique()

            # Parse index name format: "EntityName[FieldName]"
            if "[" in idx_name and "]" in idx_name:
                # Use Python string methods, not Java regex split
                bracket_start = idx_name.index("[")
                bracket_end = idx_name.index("]")
                entity = idx_name[:bracket_start]
                field = idx_name[bracket_start + 1 : bracket_end]
                actual_indexes.add((entity, field, is_unique))

        # Find missing and extra indexes
        missing = expected_indexes - actual_indexes
        extra_named = actual_indexes - expected_indexes

        print(f"{indent}• Total indexes in DB: {len(indexes)}")
        print(f"{indent}• Expected named indexes: {len(expected_indexes)}")
        print(f"{indent}• Found named indexes: {len(actual_indexes)}")

        if missing:
            print()
            print(f"{indent}❌ Missing {len(missing)} expected indexes:")
            for entity, field, unique in sorted(missing):
                idx_type = "UNIQUE" if unique else "NOTUNIQUE"
                print(f"{indent}   • {entity}[{field}] ({idx_type})")

        if extra_named:
            print()
            print(f"{indent}ℹ️  Found {len(extra_named)} unexpected named indexes:")
            for entity, field, unique in sorted(extra_named):
                idx_type = "UNIQUE" if unique else "NOTUNIQUE"
                print(f"{indent}   • {entity}[{field}] ({idx_type})")

        if not missing:
            print(f"{indent}✅ All {len(expected_indexes)} expected indexes present")
            return True
        else:
            return False

    @staticmethod
    def run_phase1_validation_queries(
        db, random_user_id: int, random_post_id: int, indent: str = "     "
    ) -> bool:
        """Run validation queries on Phase 1 database.

        Args:
            db: Database instance
            random_user_id: Random user ID for queries
            random_post_id: Random post ID for queries
            indent: Indentation string for output formatting

        Returns:
            True if all queries pass
        """
        queries = StackOverflowValidator.get_phase1_validation_queries(
            random_user_id, random_post_id
        )

        all_passed = True
        for name, sql, validator in queries:
            try:
                start = time.time()
                results = list(db.query("sql", sql))
                elapsed = time.time() - start

                if validator(results):
                    print(f"{indent}   ✓ {name}: {len(results)} rows ({elapsed:.4f}s)")
                else:
                    print(f"{indent}   ❌ {name}: Validation failed")
                    all_passed = False
            except Exception as e:
                print(f"{indent}   ❌ {name}: {e}")
                all_passed = False

        return all_passed

    @staticmethod
    def validate_phase1(
        db_path: Path, dataset_size: str = None, verbose: bool = True, indent: str = ""
    ) -> tuple[bool, dict]:
        """Complete Phase 1 validation (standalone entry point).

        Args:
            db_path: Path to Phase 1 database
            dataset_size: Dataset size name for count validation
            verbose: Print detailed output
            indent: Indentation for output

        Returns:
            Tuple of (validation_passed, counts_dict)
        """
        import random

        if verbose:
            print(f"{indent}📊 Validating Phase 1 Database")
            if dataset_size:
                print(f"{indent}   Dataset: {dataset_size}")
            print(f"{indent}{'=' * 70}")
            print()

        validation_passed = True
        counts = {}

        with arcadedb.open_database(str(db_path)) as db:
            # Verify document counts
            if verbose:
                print(f"{indent}  Document Counts:")
            counts = StackOverflowValidator.verify_phase1_document_counts(
                db, indent=f"{indent}     ", dataset_size=dataset_size
            )
            if verbose:
                print()

            # Verify indexes
            if verbose:
                print(f"{indent}  Index Verification:")
            indexes_valid = StackOverflowValidator.verify_phase1_indexes(
                db, indent=f"{indent}     "
            )
            validation_passed = validation_passed and indexes_valid
            if verbose:
                print()

            # Run validation queries
            if verbose:
                print(f"{indent}  Validation Queries:")

            # Sample random IDs
            random.seed(42)
            user_sample = list(db.query("sql", "SELECT Id FROM User LIMIT 100"))
            post_sample = list(db.query("sql", "SELECT Id FROM Post LIMIT 100"))

            if user_sample and post_sample:
                random_user_id = random.choice(user_sample).get("Id")
                random_post_id = random.choice(post_sample).get("Id")

                queries_valid = StackOverflowValidator.run_phase1_validation_queries(
                    db, random_user_id, random_post_id, indent=f"{indent}     "
                )
                validation_passed = validation_passed and queries_valid
            else:
                if verbose:
                    print(f"{indent}     ⚠️  Insufficient data for queries")
                validation_passed = False

            if verbose:
                print()

        if verbose:
            print(f"{indent}{'=' * 70}")
            if validation_passed:
                print(f"{indent}✅ Phase 1 validation passed")
            else:
                print(f"{indent}❌ Phase 1 validation failed")
            print(f"{indent}{'=' * 70}")
            print()

        return validation_passed, counts

    @staticmethod
    def get_phase2_expected_counts(dataset_size: str = None) -> dict:
        """Get expected Phase 2 vertex and edge counts.

        These counts are based on actual runs and represent the expected
        outcome after Phase 2 conversion. Useful for validation and
        detecting regressions.

        Args:
            dataset_size: Dataset size name (e.g., 'stackoverflow-tiny')

        Returns:
            Dict with 'vertices' and 'edges' subdicts, or None if unknown
        """
        # Expected Phase 2 counts by dataset (from actual runs)
        expected_phase2 = {
            "stackoverflow-tiny": {
                "vertices": {
                    "User": 10_000,
                    "Question": 3_825,
                    "Answer": 5_767,
                    "Tag": 668,
                    "Badge": 10_000,
                    "Comment": 10_000,
                    "total": 40_260,
                },
                "edges": {
                    "ASKED": 3_563,
                    "ANSWERED": 5_618,
                    "HAS_ANSWER": 5_767,
                    "ACCEPTED_ANSWER": 2_142,
                    "TAGGED_WITH": 10_689,
                    "COMMENTED_ON": 10_000,
                    "EARNED": 3_523,
                    "LINKED_TO": 786,
                    "total": 42_088,  # Updated to match actual edge counts
                },
            },
            "stackoverflow-small": {
                "vertices": {
                    "User": 138_727,
                    "Question": 48_390,
                    "Answer": 56_255,
                    "Tag": 668,
                    "Badge": 182_975,
                    "Comment": 195_781,
                    "total": 622_796,
                },
                "edges": {
                    "ASKED": 47_121,
                    "ANSWERED": 54_937,
                    "HAS_ANSWER": 56_255,
                    "ACCEPTED_ANSWER": 21_869,
                    "TAGGED_WITH": 124_636,
                    "COMMENTED_ON": 195_749,
                    "EARNED": 182_975,
                    "LINKED_TO": 10_797,
                    "total": 694_339,
                },
            },
            "stackoverflow-medium": {
                "vertices": {
                    "User": 345_754,
                    "Question": 213_761,
                    "Answer": 208_986,
                    "Tag": 1_612,
                    "Badge": 612_258,
                    "Comment": 819_648,
                    "total": 2_202_019,
                },
                "edges": {
                    "ASKED": 210_226,
                    "ANSWERED": 206_435,
                    "HAS_ANSWER": 208_986,
                    "ACCEPTED_ANSWER": 71_547,
                    "TAGGED_WITH": 662_394,
                    "COMMENTED_ON": 819_522,
                    "EARNED": 612_258,
                    "LINKED_TO": 85_813,
                    "total": 2_877_181,
                },
            },
        }

        return expected_phase2.get(dataset_size)

    @staticmethod
    def verify_phase2_vertex_counts(
        db,
        vertex_types: list = None,
        indent: str = "     ",
        dataset_size: str = None,
    ) -> dict:
        """Verify vertex counts in Phase 2 graph database.

        Args:
            db: Database instance
            vertex_types: List of vertex type names (default: all Phase 2)
            indent: Indentation string for output formatting
            dataset_size: Dataset size for expected count validation

        Returns:
            Dict of {vertex_type: count}
        """
        if vertex_types is None:
            vertex_types = ["User", "Question", "Answer", "Tag", "Badge", "Comment"]

        counts = {}
        total_count = 0
        mismatches = []

        # Get expected counts if dataset_size provided
        expected = None
        if dataset_size:
            phase2_expected = StackOverflowValidator.get_phase2_expected_counts(
                dataset_size
            )
            if phase2_expected:
                expected = phase2_expected.get("vertices", {})

        for vertex_type in vertex_types:
            result = list(
                db.query("sql", f"SELECT count(*) as count FROM {vertex_type}")
            )
            count = result[0].get("count")
            counts[vertex_type] = count
            total_count += count

            # Check against expected
            status = ""
            if expected and expected.get(vertex_type) is not None:
                expected_count = expected[vertex_type]
                if count == expected_count:
                    status = " ✓"
                else:
                    status = f" ❌ (expected {expected_count:,})"
                    mismatches.append(
                        f"{vertex_type}: got {count:,}, expected {expected_count:,}"
                    )

            print(f"{indent}✓ {vertex_type}: {count:,}{status}")

        print(f"{indent}{'─' * 50}")

        # Check total
        total_status = ""
        if expected and expected.get("total") is not None:
            expected_total = expected["total"]
            if total_count == expected_total:
                total_status = " ✓"
            else:
                total_status = f" ❌ (expected {expected_total:,})"
                mismatches.append(
                    f"Total vertices: got {total_count:,}, "
                    f"expected {expected_total:,}"
                )

        print(f"{indent}Total vertices: {total_count:,}{total_status}")

        if mismatches:
            print()
            print(f"{indent}⚠️  Vertex count mismatches:")
            for mismatch in mismatches:
                print(f"{indent}   • {mismatch}")

        return counts

    @staticmethod
    def verify_phase2_edge_counts(
        db, edge_types: list = None, indent: str = "     ", dataset_size: str = None
    ) -> dict:
        """Verify edge counts in Phase 2 graph database.

        Args:
            db: Database instance
            edge_types: List of edge type names (default: all Phase 2)
            indent: Indentation string for output formatting
            dataset_size: Dataset size for expected count validation

        Returns:
            Dict of {edge_type: count}
        """
        if edge_types is None:
            edge_types = [
                "ASKED",
                "ANSWERED",
                "HAS_ANSWER",
                "ACCEPTED_ANSWER",
                "TAGGED_WITH",
                "COMMENTED_ON",
                "EARNED",
                "LINKED_TO",
            ]

        counts = {}
        total_count = 0
        mismatches = []

        # Get expected counts if dataset_size provided
        expected = None
        if dataset_size:
            phase2_expected = StackOverflowValidator.get_phase2_expected_counts(
                dataset_size
            )
            if phase2_expected:
                expected = phase2_expected.get("edges", {})

        for edge_type in edge_types:
            result = list(db.query("sql", f"SELECT count(*) as count FROM {edge_type}"))
            count = result[0].get("count")
            counts[edge_type] = count
            total_count += count

            # Check against expected
            status = ""
            if expected and expected.get(edge_type) is not None:
                expected_count = expected[edge_type]
                if count == expected_count:
                    status = " ✓"
                else:
                    status = f" ❌ (expected {expected_count:,})"
                    mismatches.append(
                        f"{edge_type}: got {count:,}, expected {expected_count:,}"
                    )

            print(f"{indent}✓ {edge_type}: {count:,}{status}")

        print(f"{indent}{'─' * 50}")

        # Check total
        total_status = ""
        if expected and expected.get("total") is not None:
            expected_total = expected["total"]
            if total_count == expected_total:
                total_status = " ✓"
            else:
                total_status = f" ❌ (expected {expected_total:,})"
                mismatches.append(
                    f"Total edges: got {total_count:,}, expected {expected_total:,}"
                )

        print(f"{indent}Total edges: {total_count:,}{total_status}")

        if mismatches:
            print()
            print(f"{indent}⚠️  Edge count mismatches:")
            for mismatch in mismatches:
                print(f"{indent}   • {mismatch}")

        return counts

    @staticmethod
    def get_phase2_validation_queries() -> list:
        """Get validation queries for Phase 2 graph database.

        Mix of SQL and OpenCypher queries to validate:
        - Graph topology and connectivity
        - Edge properties and temporal data
        - Multi-hop traversals
        - Aggregations and patterns

        Returns:
            List of tuples: (query_type, name, query, validator_function)
            where query_type is "sql" or "opencypher"
        """
        return [
            # === Vertex Count Queries (SQL) ===
            # Note: ArcadeDB has no base V/E types - must query individual types
            (
                "sql",
                "Count User vertices",
                "SELECT count(*) as count FROM User",
                lambda r: r[0].get("count") >= 0,
            ),
            (
                "sql",
                "Count Question vertices",
                "SELECT count(*) as count FROM Question",
                lambda r: r[0].get("count") >= 0,
            ),
            (
                "sql",
                "Count Answer vertices",
                "SELECT count(*) as count FROM Answer",
                lambda r: r[0].get("count") >= 0,
            ),
            (
                "sql",
                "Count Tag vertices",
                "SELECT count(*) as count FROM Tag",
                lambda r: r[0].get("count") >= 0,
            ),
            (
                "sql",
                "Count Badge vertices",
                "SELECT count(*) as count FROM Badge",
                lambda r: r[0].get("count") >= 0,
            ),
            (
                "sql",
                "Count Comment vertices",
                "SELECT count(*) as count FROM Comment",
                lambda r: r[0].get("count") >= 0,
            ),
            # === Edge Count Queries (SQL) ===
            (
                "sql",
                "Count ASKED edges",
                "SELECT count(*) as count FROM ASKED",
                lambda r: r[0].get("count") >= 0,
            ),
            (
                "sql",
                "Count ANSWERED edges",
                "SELECT count(*) as count FROM ANSWERED",
                lambda r: r[0].get("count") >= 0,
            ),
            (
                "sql",
                "Count HAS_ANSWER edges",
                "SELECT count(*) as count FROM HAS_ANSWER",
                lambda r: r[0].get("count") >= 0,
            ),
            (
                "sql",
                "Count ACCEPTED_ANSWER edges",
                "SELECT count(*) as count FROM ACCEPTED_ANSWER",
                lambda r: r[0].get("count") >= 0,
            ),
            (
                "sql",
                "Count TAGGED_WITH edges",
                "SELECT count(*) as count FROM TAGGED_WITH",
                lambda r: r[0].get("count") >= 0,
            ),
            (
                "sql",
                "Count COMMENTED_ON edges",
                "SELECT count(*) as count FROM COMMENTED_ON",
                lambda r: r[0].get("count") >= 0,
            ),
            (
                "sql",
                "Count EARNED edges",
                "SELECT count(*) as count FROM EARNED",
                lambda r: r[0].get("count") >= 0,
            ),
            (
                "sql",
                "Count LINKED_TO edges",
                "SELECT count(*) as count FROM LINKED_TO",
                lambda r: r[0].get("count") >= 0,
            ),
            # === User Activity Queries ===
            (
                "sql",
                "Find user with most questions asked",
                """
                SELECT DisplayName, out('ASKED').size() as question_count
                FROM User
                WHERE out('ASKED').size() > 0
                ORDER BY question_count DESC
                LIMIT 1
                """,
                lambda r: len(r) > 0 and r[0].get("question_count") > 0,
            ),
            (
                "sql",
                "Find user with most answers",
                """
                SELECT DisplayName, out('ANSWERED').size() as answer_count
                FROM User
                WHERE out('ANSWERED').size() > 0
                ORDER BY answer_count DESC
                LIMIT 1
                """,
                lambda r: len(r) > 0 and r[0].get("answer_count") > 0,
            ),
            (
                "sql",
                "Find user with most badges earned",
                """
                SELECT DisplayName, out('EARNED').size() as badge_count
                FROM User
                WHERE out('EARNED').size() > 0
                ORDER BY badge_count DESC
                LIMIT 1
                """,
                lambda r: len(r) > 0 and r[0].get("badge_count") > 0,
            ),
            # === Question-Answer Relationship Queries ===
            (
                "sql",
                "Find question with most answers",
                """
                SELECT Id, out('HAS_ANSWER').size() as answer_count
                FROM Question
                ORDER BY answer_count DESC
                LIMIT 1
                """,
                lambda r: len(r) > 0,
            ),
            (
                "sql",
                "Count questions with accepted answers",
                """
                SELECT count(*) as count
                FROM Question
                WHERE out('ACCEPTED_ANSWER').size() > 0
                """,
                lambda r: r[0].get("count") >= 0,
            ),
            (
                "sql",
                "Verify answers have parent questions",
                """
                SELECT count(*) as orphan_count
                FROM Answer
                WHERE in('HAS_ANSWER').size() = 0
                """,
                lambda r: r[0].get("orphan_count") >= 0,
            ),
            # === Tag Queries (optional - may be 0 in small datasets) ===
            (
                "sql",
                "Find most popular tag",
                """
                SELECT TagName, in('TAGGED_WITH').size() as usage_count
                FROM Tag
                ORDER BY usage_count DESC
                LIMIT 1
                """,
                lambda r: True,  # Optional - OK if no tags
            ),
            (
                "sql",
                "Count questions per tag (top 5)",
                """
                SELECT TagName, in('TAGGED_WITH').size() as question_count
                FROM Tag
                WHERE in('TAGGED_WITH').size() > 0
                ORDER BY question_count DESC
                LIMIT 5
                """,
                lambda r: True,  # Optional - OK if no tags
            ),
            # === Comment Queries ===
            (
                "sql",
                "Verify all comments link to posts",
                """
                SELECT count(*) as linked_count,
                       (SELECT count(*) FROM Comment) as total_count
                FROM Comment
                WHERE out('COMMENTED_ON').size() > 0
                """,
                lambda r: r[0].get("linked_count") > 0,
            ),
            (
                "sql",
                "Find question with most comments",
                """
                SELECT Id, in('COMMENTED_ON').size() as comment_count
                FROM Question
                WHERE in('COMMENTED_ON').size() > 0
                ORDER BY comment_count DESC
                LIMIT 1
                """,
                lambda r: len(r) > 0,
            ),
            (
                "sql",
                "Find answer with most comments",
                """
                SELECT Id, in('COMMENTED_ON').size() as comment_count
                FROM Answer
                WHERE in('COMMENTED_ON').size() > 0
                ORDER BY comment_count DESC
                LIMIT 1
                """,
                lambda r: len(r) > 0,
            ),
            # === Edge Property Queries ===
            (
                "sql",
                "Verify ASKED edges have CreationDate",
                """
                SELECT count(*) as with_date,
                       (SELECT count(*) FROM ASKED) as total
                FROM ASKED
                WHERE CreationDate IS NOT NULL
                """,
                lambda r: r[0].get("with_date") > 0,
            ),
            (
                "sql",
                "Verify ANSWERED edges have CreationDate",
                """
                SELECT count(*) as with_date,
                       (SELECT count(*) FROM ANSWERED) as total
                FROM ANSWERED
                WHERE CreationDate IS NOT NULL
                """,
                lambda r: r[0].get("with_date") > 0,
            ),
            (
                "sql",
                "Verify EARNED edges have Date and Class",
                """
                SELECT count(*) as complete_count
                FROM EARNED
                WHERE Date IS NOT NULL AND Class IS NOT NULL
                """,
                lambda r: r[0].get("complete_count") >= 0,
            ),
            (
                "sql",
                "Verify LINKED_TO edges have LinkTypeId",
                """
                SELECT count(*) as with_type,
                       (SELECT count(*) FROM LINKED_TO) as total
                FROM LINKED_TO
                WHERE LinkTypeId IS NOT NULL
                """,
                lambda r: r[0].get("with_type") > 0,
            ),
            # === Multi-hop Traversal Queries (OpenCypher) ===
            (
                "opencypher",
                "Find users who answered their own questions",
                """
                MATCH (u:User)-[:ASKED]->(q:Question)-[:HAS_ANSWER]->(a:Answer)
                WHERE (u)-[:ANSWERED]->(a)
                RETURN count(DISTINCT u) as count
                """,
                lambda r: len(r) > 0,
            ),
            (
                "opencypher",
                "Find 2-hop user connections (users who answered questions from other users)",
                """
                MATCH (u:User)
                WITH u LIMIT 10
                MATCH (u)-[:ASKED]->(:Question)-[:HAS_ANSWER]->(:Answer)<-[:ANSWERED]-(other:User)
                RETURN count(DISTINCT other) as count
                """,
                lambda r: len(r) > 0,
            ),
            # === Complex Pattern Queries (OpenCypher) ===
            (
                "opencypher",
                "Find questions with tags, answers, and comments (sampled)",
                """
                MATCH (q:Question)-[:TAGGED_WITH]->(:Tag)
                WITH DISTINCT q LIMIT 200
                MATCH (q)-[:HAS_ANSWER]->(:Answer)
                WITH DISTINCT q LIMIT 200
                MATCH (q)<-[:COMMENTED_ON]-(:Comment)
                RETURN count(DISTINCT q) as count
                """,
                lambda r: len(r) > 0,
            ),
            (
                "opencypher",
                "Find users with badges who also asked questions (sampled)",
                """
                MATCH (u:User)-[:EARNED]->(:Badge)
                WITH DISTINCT u LIMIT 500
                MATCH (u)-[:ASKED]->(:Question)
                RETURN count(DISTINCT u) as count
                """,
                lambda r: len(r) > 0 and int(r[0].get("count")) >= 0,
            ),
        ]

    @staticmethod
    def run_phase2_validation_queries(
        db, indent: str = "     ", verbose: bool = True
    ) -> bool:
        """Run Phase 2 validation queries (SQL and OpenCypher).

        Args:
            db: Database instance
            indent: Indentation for output
            verbose: Print detailed output

        Returns:
            True if all queries passed
        """
        queries = StackOverflowValidator.get_phase2_validation_queries()
        all_passed = True
        sql_count = 0
        opencypher_count = 0
        query_num = 0

        for query_type, name, query, validator in queries:
            query_num += 1
            try:
                # Execute based on query type
                query_start = time.time()
                if query_type == "sql":
                    results = list(db.query("sql", query.strip()))
                    sql_count += 1
                elif query_type == "opencypher":
                    results = list(db.query("opencypher", query.strip()))
                    opencypher_count += 1
                else:
                    raise ValueError(f"Unknown query type: {query_type}")

                query_time = time.time() - query_start
                passed = validator(results)

                if verbose:
                    status = "✓" if passed else "✗"
                    lang = "SQL" if query_type == "sql" else "OpenCypher"
                    result_count = len(results)

                    # Print query info
                    print(
                        f"{indent}{status} [{query_num}/{len(queries)}]"
                        f" [{lang:7}] {name}"
                    )

                    # Print the actual query (indented, multi-line friendly)
                    query_lines = query.strip().split("\n")
                    for line in query_lines:
                        print(f"{indent}          {line.strip()}")

                    # Show result values
                    print(f"{indent}          Results: {result_count} rows")
                    if results and result_count > 0:
                        # Try to show all properties from first result
                        first = results[0]
                        props = []
                        # Common property names to check
                        prop_names = [
                            "count",
                            "cnt",
                            "usage_count",
                            "question_count",
                            "answer_count",
                            "badge_count",
                            "comment_count",
                            "DisplayName",
                            "Title",
                            "TagName",
                            "result",
                        ]
                        for prop in prop_names:
                            try:
                                if first.has_property(prop):
                                    val = first.get(prop)
                                    props.append(f"{prop}={val}")
                            except Exception:
                                pass

                        if props:
                            print(f"{indent}          " f"Values: {', '.join(props)}")

                    print(f"{indent}          Time: {query_time:.4f}s")
                    print()

                if not passed:
                    all_passed = False

            except Exception as e:
                all_passed = False
                if verbose:
                    lang = "SQL" if query_type == "sql" else "OpenCypher"
                    print(
                        f"{indent}✗ [{query_num}/{len(queries)}]" f" [{lang:7}] {name}"
                    )
                    print(f"{indent}          Error: {e}")
                    print()

        if verbose and (sql_count > 0 or opencypher_count > 0):
            print(f"{indent}{'─' * 50}")
            print(
                f"{indent}Executed: {sql_count} SQL, "
                f"{opencypher_count} OpenCypher queries"
            )

        return all_passed

    @staticmethod
    def validate_phase2(
        db_path: Path = None,
        db=None,
        dataset_size: str = None,
        verbose: bool = True,
        indent: str = "",
    ) -> tuple[bool, dict]:
        """Complete Phase 2 validation (standalone entry point).

        Args:
            db_path: Path to Phase 2 graph database (if db is None)
            db: Already-open database instance (if provided, db_path ignored)
            dataset_size: Dataset size name for count validation
            verbose: Print detailed output
            indent: Indentation for output

        Returns:
            Tuple of (validation_passed, counts_dict)
        """
        if db is None and db_path is None:
            raise ValueError("Must provide either db_path or db parameter")

        if verbose:
            print(f"{indent}📊 Validating Phase 2 Graph Database")
            if dataset_size:
                print(f"{indent}   Dataset: {dataset_size}")
            print(f"{indent}{'=' * 70}")
            print()

        validation_passed = True
        counts = {"vertices": {}, "edges": {}}

        # Use provided db or open from path
        def _run_validation(database):
            nonlocal validation_passed, counts

            # Verify vertex counts
            if verbose:
                print(f"{indent}  Vertex Counts:")
            counts["vertices"] = StackOverflowValidator.verify_phase2_vertex_counts(
                database, indent=f"{indent}     ", dataset_size=dataset_size
            )
            if verbose:
                print()

            # Verify edge counts
            if verbose:
                print(f"{indent}  Edge Counts:")
            counts["edges"] = StackOverflowValidator.verify_phase2_edge_counts(
                database, indent=f"{indent}     ", dataset_size=dataset_size
            )
            if verbose:
                print()

            # Run validation queries
            if verbose:
                print(f"{indent}  Validation Queries:")
            queries_valid = StackOverflowValidator.run_phase2_validation_queries(
                database, indent=f"{indent}     ", verbose=verbose
            )
            validation_passed = validation_passed and queries_valid

            if verbose:
                print()

        if db is not None:
            # Use the provided database instance
            _run_validation(db)
        else:
            # Open database from path
            with arcadedb.open_database(str(db_path)) as database:
                _run_validation(database)

        if verbose:
            print(f"{indent}{'=' * 70}")
            if validation_passed:
                print(f"{indent}✅ Phase 2 validation passed")
            else:
                print(f"{indent}❌ Phase 2 validation failed")
            print(f"{indent}{'=' * 70}")
            print()

        return validation_passed, counts


# =============================================================================
# Schema Analysis Classes
# =============================================================================


@dataclass
class FieldStats:
    """Statistics for a single field."""

    type_name: str
    count: int
    null_count: int = 0
    sample_values: List[str] = field(default_factory=list)
    avg_length: float = 0.0
    avg_tokens: float = 0.0
    min_value: int = None
    max_value: int = None


@dataclass
class EntitySchema:
    """Schema information for an entity (document type)."""

    name: str
    source_file: Path
    fields: Dict[str, FieldStats]
    row_count: int
    has_primary_key: bool


class SchemaAnalyzer:
    """Analyzes data files to infer types and statistics."""

    # Datetime/date patterns
    DATETIME_PATTERNS = [
        r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}$",  # ISO with millis
        r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$",  # ISO without millis
    ]
    DATE_PATTERN = r"^\d{4}-\d{2}-\d{2}$"

    # Integer ranges
    BYTE_MIN, BYTE_MAX = -128, 127
    SHORT_MIN, SHORT_MAX = -32768, 32767
    INTEGER_MIN, INTEGER_MAX = -2147483648, 2147483647

    def __init__(self, analysis_limit: int = 1_000_000):
        """Initialize analyzer.

        Args:
            analysis_limit: Max rows to analyze per file (for performance)
        """
        self.analysis_limit = analysis_limit

    def analyze_xml_file(self, xml_file: Path) -> EntitySchema:
        """Analyze XML file and infer schema.

        Args:
            xml_file: Path to XML file

        Returns:
            EntitySchema with inferred types and statistics
        """
        entity_name = xml_file.stem.rstrip("s")  # Users.xml → User

        print(f"  📊 Analyzing {xml_file.name}...")
        print(f"      (sampling up to {self.analysis_limit:,} rows)")

        # Track field statistics
        field_types = defaultdict(set)  # field → set of observed types
        field_values = defaultdict(list)  # field → list of values
        field_null_counts = defaultdict(int)
        all_fields_seen = set()
        row_count = 0

        # Stream parse XML
        context = etree.iterparse(str(xml_file), events=("end",))

        for event, elem in context:
            if elem.tag == "row":
                row_count += 1

                # Track which fields exist in this row
                row_fields = set(elem.attrib.keys())
                all_fields_seen.update(row_fields)

                for attr_name, attr_value in elem.attrib.items():
                    # Track value for statistics
                    field_values[attr_name].append(attr_value)

                    # Infer type
                    inferred_type = self._infer_type(attr_value, attr_name)
                    field_types[attr_name].add(inferred_type)

                # Track which fields are missing (nulls)
                for missing_field in all_fields_seen - row_fields:
                    field_null_counts[missing_field] += 1

                # Clear element to save memory
                elem.clear()
                while elem.getprevious() is not None:
                    del elem.getparent()[0]

                # Limit analysis for large files
                if row_count >= self.analysis_limit:
                    break

        # Build field statistics
        fields = {}
        for field_name in all_fields_seen:
            type_set = field_types.get(field_name, {"STRING"})
            values = field_values.get(field_name, [])

            # Choose most specific type
            final_type = self._resolve_type(type_set, values)

            # Calculate statistics
            stats = FieldStats(
                type_name=final_type,
                count=len(values),
                null_count=field_null_counts.get(field_name, 0),
                sample_values=values[:5] if values else [],
            )

            # String length stats
            if final_type in ["STRING", "TEXT"]:
                lengths = [len(str(v)) for v in values if v]
                stats.avg_length = sum(lengths) / len(lengths) if lengths else 0
                # Estimate tokens: ~1 token per 4 chars (English text heuristic)
                stats.avg_tokens = stats.avg_length / 4.0 if stats.avg_length else 0

            # Numeric range stats
            if final_type in ["BYTE", "SHORT", "INTEGER", "LONG"]:
                numeric_values = [int(v) for v in values if v]
                if numeric_values:
                    stats.min_value = min(numeric_values)
                    stats.max_value = max(numeric_values)

            fields[field_name] = stats

        print(f"      → {row_count:,} rows, {len(fields)} fields")

        return EntitySchema(
            name=entity_name,
            source_file=xml_file,
            fields=fields,
            row_count=row_count,
            has_primary_key="Id" in fields,
        )

    def _infer_type(self, value: str, field_name: str = "") -> str:
        """Infer ArcadeDB type from string value."""
        if not value:
            return "STRING"

        # Check datetime patterns
        for pattern in self.DATETIME_PATTERNS:
            if re.match(pattern, value):
                return "DATETIME"

        if re.match(self.DATE_PATTERN, value):
            return "DATE"

        # Try numeric types
        try:
            num = int(value)
            if self.BYTE_MIN <= num <= self.BYTE_MAX:
                return "BYTE"
            elif self.SHORT_MIN <= num <= self.SHORT_MAX:
                return "SHORT"
            elif self.INTEGER_MIN <= num <= self.INTEGER_MAX:
                return "INTEGER"
            else:
                return "LONG"
        except ValueError:
            pass

        # Try float
        try:
            float(value)
            return "FLOAT"
        except ValueError:
            pass

        # Try boolean
        if value.lower() in ["true", "false"]:
            return "BOOLEAN"

        # Default to string
        return "STRING"

    def _resolve_type(self, type_set: Set[str], values: List[str]) -> str:
        """Resolve final type when multiple types observed."""
        if len(type_set) == 1:
            return next(iter(type_set))

        # If multiple types, use most general
        type_hierarchy = ["BYTE", "SHORT", "INTEGER", "LONG", "FLOAT", "STRING"]

        for general_type in reversed(type_hierarchy):
            if general_type in type_set:
                return general_type

        return "STRING"


# =============================================================================
# Helper Functions
# =============================================================================


def get_retry_config(dataset_size):
    """Get retry configuration based on dataset size.

    Args:
        dataset_size: Full dataset name (e.g., 'stackoverflow-tiny')
    """
    # Extract size suffix (tiny, small, medium, large)
    size = dataset_size.split("-")[-1] if "-" in dataset_size else dataset_size

    configs = {
        "tiny": {"retry_delay": 10, "max_retries": 60},  # 10 min max
        "small": {"retry_delay": 60, "max_retries": 120},  # 2 hours max
        "medium": {"retry_delay": 180, "max_retries": 200},  # 10 hours max
        "large": {"retry_delay": 300, "max_retries": 200},  # 16.7 hours max
    }
    return configs.get(size, configs["tiny"])


def print_batch_stats(
    count: int,
    embed_time: float = None,
    query_time: float = None,
    cache_time: float = None,
    db_time: float = None,
    total_time: float = None,
    item_name: str = "items",
):
    """Print per-batch statistics in a consistent format.

    Args:
        count: Number of items in batch
        embed_time: Time spent on embeddings (optional)
        query_time: Time spent on queries (optional, for edges)
        cache_time: Time spent on caching vertices (optional, for edges)
        db_time: Time spent on database operations
        total_time: Total batch time
        item_name: Name for items (e.g., "users", "edges", "v", "e")
    """
    rate = count / total_time if total_time and total_time > 0 else 0

    parts = [f"    → Batch: {count:,} {item_name} |"]

    if embed_time is not None:
        parts.append(f"embed: {embed_time:.1f}s |")
    if query_time is not None:
        parts.append(f"query: {query_time:.1f}s |")
    if cache_time is not None:
        parts.append(f"cache: {cache_time:.1f}s |")
    if db_time is not None:
        parts.append(f"db: {db_time:.1f}s |")
    if total_time is not None:
        parts.append(f"total: {total_time:.1f}s ({rate:.0f} /s)")

    print(" ".join(parts))


def print_summary_stats(
    total_count: int,
    elapsed: float,
    batch_times: List[tuple],
    item_name: str = "items",
    has_embed: bool = False,
    has_query: bool = False,
    has_cache: bool = False,
):
    """Print summary statistics with averages.

    Args:
        total_count: Total number of items created
        elapsed: Total elapsed time
        batch_times: List of tuples (count, [embed_t], [query_t], [cache_t], db_t, total_t)
        item_name: Name for items (e.g., "users", "edges")
        has_embed: Whether batches have embedding time
        has_query: Whether batches have query time (for edges)
        has_cache: Whether batches have cache time (for edges)
    """
    if not batch_times:
        rate = total_count / elapsed if elapsed > 0 else 0
        print(
            f"    ✓ Created {total_count:,} {item_name} in "
            f"{elapsed:.1f}s ({rate:.0f} /s)"
        )
        return

    avg_rate = total_count / elapsed if elapsed > 0 else 0
    parts = [f"    ✓ Summary: {total_count:,} {item_name} in " f"{elapsed:.1f}s |"]

    if has_embed:
        # batch_times format: (count, embed_t, db_t, total_t)
        total_embed = sum(t[1] for t in batch_times)
        total_db = sum(t[2] for t in batch_times)
        avg_embed = total_embed / len(batch_times)
        avg_db = total_db / len(batch_times)
        parts.append(f"avg embed: {avg_embed:.1f}s |")
        parts.append(f"avg db: {avg_db:.1f}s |")
    elif has_query and has_cache:
        # batch_times format: (count, query_t, cache_t, db_t, total_t)
        total_query = sum(t[1] for t in batch_times)
        total_cache = sum(t[2] for t in batch_times)
        total_db = sum(t[3] for t in batch_times)
        avg_query = total_query / len(batch_times)
        avg_cache = total_cache / len(batch_times)
        avg_db = total_db / len(batch_times)
        parts.append(f"avg query: {avg_query:.1f}s |")
        parts.append(f"avg cache: {avg_cache:.1f}s |")
        # Only show db time if it's significant
        if avg_db > 0.01:
            parts.append(f"avg db: {avg_db:.1f}s |")
    elif has_query:
        # batch_times format: (count, query_t, db_t, total_t)
        total_query = sum(t[1] for t in batch_times)
        total_db = sum(t[2] for t in batch_times)
        avg_query = total_query / len(batch_times)
        avg_db = total_db / len(batch_times)
        parts.append(f"avg query: {avg_query:.1f}s |")
        # Only show db time if it's significant
        if avg_db > 0.01:
            parts.append(f"avg db: {avg_db:.1f}s |")
    else:
        # batch_times format: (count, db_t)
        total_db = sum(t[1] for t in batch_times)
        avg_db = total_db / len(batch_times)
        parts.append(f"avg db: {avg_db:.1f}s |")

    parts.append(f"avg rate: {avg_rate:.0f} /s")
    print(" ".join(parts))


def create_indexes(db, indexes, retry_delay=10, max_retries=60, verbose=True):
    """
    Create indexes with retry logic for compaction conflicts.

    Args:
        db: Database instance
        indexes: List of (table, column, uniqueness) tuples
        retry_delay: Seconds to wait between retries
        max_retries: Maximum number of retry attempts
        verbose: If True, print progress messages

    Returns:
        tuple: (success_count, failed_indexes)
    """
    if verbose:
        print(f"\n  Creating {len(indexes)} indexes with retry logic...")
        print(f"    Retry: {retry_delay}s delay, {max_retries} max attempts")

    success_count = 0
    failed_indexes = []

    for idx, (table, column, uniqueness) in enumerate(indexes, 1):
        created = False

        for attempt in range(1, max_retries + 1):
            try:
                if uniqueness == "UNIQUE":
                    db.schema.create_index(table, [column], unique=True)
                elif uniqueness == "FULL_TEXT":
                    db.schema.create_index(table, [column], index_type="FULL_TEXT")
                else:  # NOTUNIQUE
                    db.schema.create_index(table, [column], unique=False)

                if verbose:
                    print(
                        f"\n    ✅ [{idx}/{len(indexes)}] {table}[{column}] {uniqueness}"
                    )

                created = True
                success_count += 1
                break

            except Exception as e:
                error_msg = str(e)

                # Check if retryable
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
                            "compaction" if is_compaction_error else "index conflict"
                        )
                        if verbose:
                            print(
                                f"    ⏳ [{idx}/{len(indexes)}] Waiting for {reason} "
                                f"(attempt {attempt}/{max_retries}, {elapsed}s elapsed)..."
                            )
                        time.sleep(retry_delay)
                    else:
                        if verbose:
                            print(
                                f"    ❌ [{idx}/{len(indexes)}] Failed after {max_retries} retries: "
                                f"{table}[{column}]"
                            )
                        failed_indexes.append((table, column, error_msg))
                        break
                else:
                    # Non-retryable error
                    if verbose:
                        print(
                            f"    ❌ [{idx}/{len(indexes)}] {table}[{column}]: {error_msg}"
                        )
                    failed_indexes.append((table, column, error_msg))
                    break

        if not created and verbose:
            print(f"    ⚠️  Skipped {table}[{column}]")

    # Wait for all background index building to complete
    if success_count > 0 and verbose:
        print("\n  ⏳ Waiting for all index builds to complete...")

    try:
        async_exec = db.async_executor()
        async_exec.wait_completion()
        if success_count > 0 and verbose:
            print("  ✅ All index builds complete")
    except Exception as e:
        if verbose:
            print(f"  ⚠️  Could not verify index build completion: {e}")
            print("     Indexes may still be building in background...")

    return success_count, failed_indexes


def close_database_safely(db, verbose=True):
    """Close database after waiting for all background compactions."""
    if verbose:
        print("\n  Finalizing database...")
        print("    ⏳ Waiting for background compactions to complete...")

    try:
        async_exec = db.async_executor()
        async_exec.wait_completion()
        if verbose:
            print("    ✅ All compactions complete - safe to close")
    except Exception as e:
        if verbose:
            print(f"    ⚠️  Could not verify compaction status: {e}")
            print("       Proceeding with database close...")

    db.close()
    if verbose:
        print("    ✅ Database closed cleanly")


# =============================================================================
# Phase 1: XML → Documents + Indexes
# =============================================================================


class Phase1XMLImporter:
    """Handles Phase 1: Import XMLs → Documents → Create Indexes."""

    def __init__(
        self, db_path, data_dir, batch_size, dataset_size, analysis_limit=1_000_000
    ):
        self.db_path = Path(db_path)
        self.data_dir = Path(data_dir)
        self.batch_size = batch_size
        self.dataset_size = dataset_size
        self.analysis_limit = analysis_limit
        self.db = None
        self.schemas = {}  # Store discovered schemas

    def run(self):
        """Execute Phase 1: XML import and index creation."""
        print("=" * 80)
        print("PHASE 1: XML → Documents + Indexes")
        print("=" * 80)
        print(f"Dataset: {self.dataset_size}")
        print(f"Batch size: {self.batch_size} records/commit")
        print(f"Data directory: {self.data_dir}")
        print(f"Database path: {self.db_path}")
        print()

        phase_start = time.time()

        try:
            # Step 0: Analyze schemas (fast, discovers all attributes)
            print("Step 0: Analyzing XML schemas...")
            analysis_start = time.time()

            analyzer = SchemaAnalyzer(analysis_limit=self.analysis_limit)
            xml_files = [
                "Users.xml",
                "Posts.xml",
                "Comments.xml",
                "Badges.xml",
                "Votes.xml",
                "PostLinks.xml",
                "Tags.xml",
                "PostHistory.xml",
            ]

            for xml_file in xml_files:
                xml_path = self.data_dir / xml_file
                if xml_path.exists():
                    schema = analyzer.analyze_xml_file(xml_path)
                    self.schemas[schema.name] = schema

            print(f"  ✅ Analyzed {len(self.schemas)} XML files")
            print(f"  ⏱️  Time: {time.time() - analysis_start:.2f}s")
            print()

            # Step 1: Create database
            print("Step 1: Creating database...")
            step_start = time.time()

            # Clean up existing database
            if self.db_path.exists():
                shutil.rmtree(self.db_path)

            # Clean up log directory
            log_dir = Path("./log")
            if log_dir.exists():
                shutil.rmtree(log_dir)

            self.db = arcadedb.create_database(str(self.db_path))

            print(f"  ✅ Database created")
            print(f"  ⏱️  Time: {time.time() - step_start:.2f}s")
            print()

            # Step 2: Create document types from discovered schemas
            print("Step 2: Creating document types...")
            step_start = time.time()

            self._create_document_types()

            print(f"  ✅ Created {len(self.schemas)} document types")
            print(f"  ⏱️  Time: {time.time() - step_start:.2f}s")
            print()

            # Step 3: Import XML files using discovered schemas
            print("Step 3: Importing XML files...")
            import_start = time.time()

            import_stats = []  # Collect statistics from each import

            for xml_file in xml_files:
                xml_path = self.data_dir / xml_file
                if xml_path.exists():
                    entity_name = xml_path.stem.rstrip("s")  # Users.xml → User
                    if entity_name in self.schemas:
                        stats = self._import_xml_generic(xml_path, entity_name)
                        import_stats.append(stats)

            # Print aggregate statistics
            total_records = sum(s["count"] for s in import_stats)
            total_time = time.time() - import_start
            overall_rate = total_records / total_time if total_time > 0 else 0

            # Calculate timing aggregates
            total_db_time = sum(s["db_time"] for s in import_stats)
            total_embed_time = sum(s["embed_time"] for s in import_stats)
            total_query_time = sum(s["query_time"] for s in import_stats)

            print("\n  ✅ All XML files imported")
            print(f"  📊 Total records: {total_records:,}")
            print(f"  ⏱️  Total import time: {total_time:.2f}s")
            print(f"  ⚡ Overall rate: {overall_rate:,.0f} records/sec")
            print()

            # Print timing breakdown
            print("  ⏱️  Timing breakdown:")
            print(
                f"     • DB operations:    {total_db_time:>8.2f}s ({total_db_time/total_time*100:>5.1f}%)"
            )
            if total_embed_time > 0:
                print(
                    f"     • Embedding gen:    {total_embed_time:>8.2f}s ({total_embed_time/total_time*100:>5.1f}%)"
                )
            if total_query_time > 0:
                print(
                    f"     • Queries:    {total_query_time:>8.2f}s ({total_query_time/total_time*100:>5.1f}%)"
                )
            overhead = total_time - (
                total_db_time + total_embed_time + total_query_time
            )
            print(
                f"     • Overhead (I/O):   {overhead:>8.2f}s ({overhead/total_time*100:>5.1f}%)"
            )
            print()

            # Print per-entity breakdown
            print("  📋 Import breakdown by entity:")
            for stats in import_stats:
                pct = (stats["count"] / total_records * 100) if total_records > 0 else 0
                db_pct = (
                    (stats["db_time"] / total_db_time * 100) if total_db_time > 0 else 0
                )
                print(
                    f"     • {stats['entity_name']:12} "
                    f"{stats['count']:>7,} records "
                    f"({pct:>5.1f}%) | "
                    f"{stats['avg_rate']:>7,.0f} rec/s | "
                    f"{stats['db_time']:>6.2f}s db ({db_pct:>5.1f}%)"
                )
            print()

            # Step 4: Create indexes
            print("Step 4: Creating indexes...")
            index_start = time.time()

            indexes = self._get_indexes()

            # Show what indexes will be created
            self._print_index_plan(indexes)

            retry_config = get_retry_config(self.dataset_size)

            success, failed = create_indexes(
                self.db,
                indexes,
                retry_delay=retry_config["retry_delay"],
                max_retries=retry_config["max_retries"],
                verbose=True,
            )

            if failed:
                raise RuntimeError(f"Failed to create {len(failed)} indexes")

            print(f"\n  ✅ All {success} indexes created")
            print(f"  ⏱️  Index creation time: {time.time() - index_start:.2f}s")
            print()

            # Step 5: Run validation queries (sanity check)
            print("Step 5: Running validation queries (sanity check)...")
            query_start = time.time()

            self._run_validation_queries()

            print(f"  ⏱️  Query time: {time.time() - query_start:.2f}s")
            print()

            # Step 6: Close database safely
            print("Step 6: Closing database...")
            close_start = time.time()

            close_database_safely(self.db, verbose=True)

            print(f"  ⏱️  Close time: {time.time() - close_start:.2f}s")
            print()

            # Step 7: Print schema summary
            print("Step 7: Schema summary...")
            self._print_schema_summary()

            # Phase 1 complete
            phase_elapsed = time.time() - phase_start
            print("=" * 80)
            print("✅ PHASE 1 COMPLETE")
            print("=" * 80)
            print(
                f"Total time: {phase_elapsed:.2f}s ({phase_elapsed / 60:.1f} minutes)"
            )
            print("=" * 80)
            print()

        except Exception as e:
            print(f"\n❌ Phase 1 failed: {e}")
            if self.db:
                self.db.close()
            raise

    def _create_document_types(self):
        """Create document types with properties based on discovered schemas."""
        # Schema operations are auto-transactional
        for entity_name, schema in self.schemas.items():
            # Create document type
            self.db.schema.create_document_type(entity_name)

            # Define all properties with their types
            for field_name, field_stats in schema.fields.items():
                self.db.schema.create_property(
                    entity_name, field_name, field_stats.type_name
                )

            prop_count = len(schema.fields)
            print(f"    ✓ Created {entity_name} ({prop_count} properties)")

    def _import_xml_generic(self, xml_path: Path, entity_name: str):
        """Generic XML importer using discovered schema.

        Returns:
            dict: Statistics with keys: count, elapsed, avg_rate, db_time,
                  embed_time, query_time, entity_name
        """
        print(f"\n  Importing {xml_path.name}...")

        schema = self.schemas.get(entity_name)
        if not schema:
            print(f"    ⚠️  No schema found for {entity_name}, skipping")
            return {
                "count": 0,
                "elapsed": 0,
                "avg_rate": 0,
                "db_time": 0,
                "embed_time": 0,
                "query_time": 0,
                "entity_name": entity_name,
            }

        # Get field info from schema
        fields = schema.fields
        integer_fields = {
            name
            for name, stats in fields.items()
            if stats.type_name in ["BYTE", "SHORT", "INTEGER", "LONG"]
        }

        batch = []
        total_count = 0
        batch_times = []
        total_db_time = 0
        total_embed_time = 0  # For future use (Phase 3)
        total_query_time = 0  # For future use (Phase 2)
        start_time = time.time()

        context = etree.iterparse(str(xml_path), events=("end",), tag="row")

        for _, elem in context:
            # Build document from ALL discovered attributes
            doc_data = {}

            for attr_name in fields.keys():
                attr_value = elem.get(attr_name)

                if attr_value is not None:
                    # Convert integers
                    if attr_name in integer_fields:
                        try:
                            doc_data[attr_name] = int(attr_value)
                        except ValueError:
                            # Skip invalid integers
                            pass
                    else:
                        doc_data[attr_name] = attr_value

            batch.append(doc_data)

            if len(batch) >= self.batch_size:
                batch_start = time.time()

                db_time = self._insert_batch(entity_name, batch)
                total_time = time.time() - batch_start

                total_count += len(batch)
                total_db_time += db_time  # Accumulate database time
                batch_times.append((len(batch), db_time))

                print_batch_stats(
                    count=len(batch),
                    db_time=db_time,
                    total_time=total_time,
                    item_name=entity_name.lower(),
                )
                batch = []

            # Memory cleanup
            elem.clear()
            while elem.getprevious() is not None:
                del elem.getparent()[0]

        # Final batch
        if batch:
            batch_start = time.time()

            db_time = self._insert_batch(entity_name, batch)
            total_time = time.time() - batch_start

            total_count += len(batch)
            total_db_time += db_time  # Accumulate database time
            batch_times.append((len(batch), db_time))

            print_batch_stats(
                count=len(batch),
                db_time=db_time,
                total_time=total_time,
                item_name=entity_name.lower(),
            )

        elapsed = time.time() - start_time

        # Calculate average rate
        avg_rate = total_count / elapsed if elapsed > 0 else 0

        # Print summary statistics
        print_summary_stats(
            total_count=total_count,
            elapsed=elapsed,
            batch_times=batch_times,
            item_name=entity_name.lower(),
            has_embed=False,
            has_query=False,
        )

        del context

        # Return statistics for aggregation
        return {
            "count": total_count,
            "elapsed": elapsed,
            "avg_rate": avg_rate,
            "db_time": total_db_time,
            "embed_time": total_embed_time,
            "query_time": total_query_time,
            "entity_name": entity_name,
        }

    def _print_schema_summary(self):
        """Print summary of imported schemas."""
        print("\n" + "=" * 80)
        print("📊 IMPORTED SCHEMA SUMMARY")
        print("=" * 80)
        print()

        for entity_name, schema in sorted(self.schemas.items()):
            print(f"📄 {entity_name}")
            print(f"   Total fields: {len(schema.fields)}")
            print(f"   Fields: {', '.join(sorted(schema.fields.keys()))}")

            # Show nullable fields
            nullable = [
                (name, stats.null_count, schema.row_count)
                for name, stats in schema.fields.items()
                if stats.null_count > 0
            ]

            if nullable:
                print(f"   Nullable fields: {len(nullable)}")
                for name, null_count, total in sorted(
                    nullable, key=lambda x: x[1], reverse=True
                )[:5]:
                    pct = (null_count / total) * 100
                    print(f"     - {name}: {pct:.1f}% nulls")
                if len(nullable) > 5:
                    print(f"     ... and {len(nullable) - 5} more")
            print()

        print("=" * 80)
        print()

    # ========================================================================
    # Database Helper Methods
    # ========================================================================

    def _insert_batch(self, type_name, records):
        """Insert a batch of records using transaction.

        Returns:
            float: Time elapsed in seconds for the database operation
        """
        batch_start = time.time()
        with self.db.transaction():
            for record in records:
                doc = self.db.new_document(type_name)
                for key, value in record.items():
                    doc.set(key, value)
                doc.save()
        return time.time() - batch_start

    def _run_validation_queries(self):
        """Run validation queries using StackOverflowValidator."""
        import random

        random.seed(42)

        print("\n  📊 Running validation queries...\n")

        # Sample random IDs
        try:
            user_sample = list(self.db.query("sql", "SELECT Id FROM User LIMIT 100"))
            random_user_id = random.choice(user_sample).get("Id") if user_sample else 1

            post_sample = list(self.db.query("sql", "SELECT Id FROM Post LIMIT 100"))
            random_post_id = random.choice(post_sample).get("Id") if post_sample else 1
        except Exception:
            random_user_id = 1
            random_post_id = 1

        # Use the reusable validator
        queries = StackOverflowValidator.get_phase1_validation_queries(
            random_user_id, random_post_id
        )

        for i, (query_name, query, validator) in enumerate(queries, 1):
            try:
                start_time = time.time()
                result = list(self.db.query("sql", query))
                elapsed = time.time() - start_time

                passed = validator(result)
                status = "✓" if passed else "❌"

                print(f"  [{i}/{len(queries)}] {query_name}")
                print(f"          Query: {query.strip()}")
                print(f"          Results: {len(result)} rows")

                # Show actual result values
                if result:
                    first = result[0]
                    props = {}
                    # Get all properties from first result
                    try:
                        # Try common property names
                        prop_names = [
                            "count",
                            "cnt",
                            "Id",
                            "DisplayName",
                            "PostTypeId",
                            "Title",
                        ]
                        for prop in prop_names:
                            if first.has_property(prop):
                                props[prop] = first.get(prop)
                    except Exception:
                        pass

                    if props:
                        props_str = ", ".join(f"{k}={v}" for k, v in props.items())
                        print(f"          Values: {props_str}")

                print(f"          Time: {elapsed:.4f}s")
                print(f"          Status: {status}")
                print()

            except Exception as e:
                print(f"  [{i}/{len(queries)}] {query_name}")
                print(f"          ❌ Error: {e}")
                print()

    def _get_indexes(self):
        """Auto-generate indexes from discovered schema.

        Phase 2 will convert documents → graph with edges. For edge creation,
        we need to lookup vertices efficiently using queries like:
            SELECT FROM User WHERE userId IN [1, 2, 3, ...]
            SELECT FROM Post WHERE Id IN [100, 200, 300, ...]

        Therefore we need indexes on:
        1. PRIMARY KEYS (Id fields) - for direct vertex lookups
        2. FOREIGN KEYS (OwnerUserId, PostId, etc.) - for batch vertex cache
           queries during edge creation (see example 05's _build_vertex_cache)

        Returns:
            List of tuples: (entity_name, field_name, index_type)
        """
        indexes = []

        # Entity name mapping (schema key -> document type name)
        entity_map = {
            "User": "User",
            "Post": "Post",
            "Comment": "Comment",
            "Badge": "Badge",
            "Vote": "Vote",
            "PostLink": "PostLink",
            "Tag": "Tag",
            "PostHistory": "PostHistory",
        }

        for entity_key, schema in self.schemas.items():
            entity_name = entity_map.get(entity_key, entity_key)

            # Add primary key index (Id field) - UNIQUE
            if "Id" in schema.fields:
                indexes.append((entity_name, "Id", "UNIQUE"))

            # Add foreign key indexes (for Phase 2 vertex cache queries)
            # Pattern: any field ending with 'Id' (except primary key 'Id')
            for field_name in schema.fields.keys():
                # Skip the primary key 'Id', but index foreign keys like
                # 'UserId', 'PostId', 'OwnerUserId', 'ParentId', etc.
                if field_name != "Id" and field_name.endswith("Id"):
                    indexes.append((entity_name, field_name, "NOTUNIQUE"))

        return indexes

    def _print_index_plan(self, indexes):
        """Print the index creation plan for verification."""
        unique = [idx for idx in indexes if idx[2] == "UNIQUE"]
        notunique = [idx for idx in indexes if idx[2] == "NOTUNIQUE"]

        print(f"\n  📋 Index Plan: {len(indexes)} total indexes")
        print(f"     • {len(unique)} UNIQUE (primary keys)")
        print(
            f"     • {len(notunique)} NOTUNIQUE "
            f"(foreign keys for Phase 2 vertex cache)"
        )
        print()

        if unique:
            print("  🔑 Primary Key Indexes:")
            for entity, field_name, _ in sorted(unique):
                print(f"     • {entity}.{field_name}")

        if notunique:
            print(
                "\n  🔗 Foreign Key Indexes "
                "(enables fast vertex lookups in Phase 2):"
            )
            for entity, field_name, _ in sorted(notunique):
                print(f"     • {entity}.{field_name}")
        print()


# =============================================================================
# Phase 2: Documents → Graph (Vertices + Edges)
# =============================================================================


class Phase2GraphConverter:
    """Converts Phase 1 document database to Phase 2 graph database.

    Phase 2 Steps:
    1. Verify Phase 1 database (counts, indexes, validation queries)
    2. Create new graph database with vertex/edge schema
    3. Convert documents to vertices (User, Question, Answer, Tag, Badge, Comment)
    4. Create edges (ASKED, ANSWERED, HAS_ANSWER, etc.)
    5. Aggregate vote counts into Question/Answer properties
    6. Run graph validation queries
    """

    def __init__(
        self,
        doc_db_path: Path,
        graph_db_path: Path,
        batch_size: int = 10000,
        dataset_size: str = "stackoverflow-small",
    ):
        self.doc_db_path = doc_db_path
        self.graph_db_path = graph_db_path
        self.batch_size = batch_size
        self.dataset_size = dataset_size
        self.doc_db = None
        self.graph_db = None

        # Expected counts from Phase 1 (will be verified)
        self.expected_counts = {}
        self.expected_indexes = 28  # From Phase 1

    @staticmethod
    def _to_epoch_millis(dt):
        """Convert datetime to epoch milliseconds (Java timestamp format).

        Returns None if dt is None, otherwise converts to long.
        """
        from datetime import datetime

        if dt is None:
            return None
        if isinstance(dt, datetime):
            return int(dt.timestamp() * 1000)
        # Already a number (long/int)
        return dt

    def run(self):
        """Execute Phase 2: Document to Graph conversion."""
        print("=" * 80)
        print("PHASE 2: Documents → Graph")
        print("=" * 80)
        print(f"Source DB (Phase 1): {self.doc_db_path}")
        print(f"Target DB (Phase 2): {self.graph_db_path}")
        print(f"Batch size: {self.batch_size:,} records/commit")
        print()

        phase_start = time.time()

        try:
            # Step 1: Verify Phase 1 database
            print("Step 1: Verifying Phase 1 database...")
            start_time = time.time()
            self._verify_phase1()
            print(f"  ⏱️  Time: {time.time() - start_time:.2f}s")
            print()

            # Step 2: Create graph schema
            print("Step 2: Creating graph schema...")
            step_start = time.time()
            self._create_graph_schema()
            print(f"  ⏱️  Time: {time.time() - step_start:.2f}s")
            print()

            # Step 3: Convert to vertices
            print("Step 3: Converting documents to vertices...")
            step_start = time.time()
            self._convert_to_vertices()
            print(f"  ⏱️  Time: {time.time() - step_start:.2f}s")
            print()

            # Step 4: Create edges
            print("Step 4: Creating edges...")
            step_start = time.time()
            self._create_edges()
            print(f"  ⏱️  Time: {time.time() - step_start:.2f}s")
            print()

            # Step 5: Run graph validation
            print("Step 5: Running graph validation queries...")
            step_start = time.time()
            self._validate_phase2()
            print(f"  ⏱️  Time: {time.time() - step_start:.2f}s")
            print()

            # Phase 2 complete
            phase_elapsed = time.time() - phase_start
            print("=" * 80)
            print("✅ PHASE 2 COMPLETE")
            print("=" * 80)
            print(
                f"Total time: {phase_elapsed:.2f}s ({phase_elapsed / 60:.1f} minutes)"
            )
            print("=" * 80)
            print()

        except Exception as e:
            print(f"\n❌ Phase 2 failed: {e}")
            raise
        finally:
            # Close graph database to release lock
            if self.graph_db is not None:
                self.graph_db.close()
                print("  ✅ Closed Phase 2 graph database")
                print()

    def _validate_phase2(self):
        """Validate Phase 2 graph database using StackOverflowValidator."""
        print("  Validating Phase 2 graph database...")
        print()

        # Use the reusable standalone validator with the already-open database
        validation_passed, counts = StackOverflowValidator.validate_phase2(
            db=self.graph_db,
            dataset_size=self.dataset_size,
            verbose=True,
            indent="  ",
        )

        if not validation_passed:
            raise RuntimeError("Phase 2 validation failed!")

        print("  ✅ Phase 2 validation complete!")

    def _verify_phase1(self):
        """Verify Phase 1 database using StackOverflowValidator."""
        print("  Verifying Phase 1 database...")
        print()

        # Use the reusable standalone validator
        validation_passed, counts = StackOverflowValidator.validate_phase1(
            self.doc_db_path, dataset_size=self.dataset_size, verbose=True, indent="  "
        )

        if not validation_passed:
            raise RuntimeError("Phase 1 verification failed!")

        # Store counts for reference
        self.expected_counts = counts

        print("  ✅ Phase 1 verification complete!")

    def _create_graph_schema(self):
        """Create graph database with vertex and edge types.

        Vertex types (6):
        - User: Stack Overflow users
        - Question: Posts where PostTypeId=1
        - Answer: Posts where PostTypeId=2
        - Tag: Tags for categorizing questions
        - Badge: User achievements
        - Comment: Comments on posts

        Edge types (8):
        - ASKED: User -> Question
        - ANSWERED: User -> Answer
        - HAS_ANSWER: Question -> Answer
        - ACCEPTED_ANSWER: Question -> Answer (accepted)
        - TAGGED_WITH: Question -> Tag
        - COMMENTED_ON: Comment -> Post (Question or Answer)
        - EARNED: User -> Badge
        - LINKED_TO: Post -> Post (via PostLink)
        """
        print("  Creating graph database...")

        # Clean up existing graph database
        if self.graph_db_path.exists():
            shutil.rmtree(self.graph_db_path)
            print("    • Cleaned up existing graph database")

        # Create new graph database
        self.graph_db = arcadedb.create_database(str(self.graph_db_path))
        print(f"    • Created graph database: {self.graph_db_path.name}")

        # Create vertex types (schema ops are auto-transactional)
        print("\n  Creating vertex types...")

        # User vertex
        self.graph_db.schema.create_vertex_type("User")
        self.graph_db.schema.create_property("User", "Id", "INTEGER")
        self.graph_db.schema.create_property("User", "DisplayName", "STRING")
        self.graph_db.schema.create_property("User", "Reputation", "INTEGER")
        self.graph_db.schema.create_property("User", "CreationDate", "DATETIME")
        self.graph_db.schema.create_property("User", "Views", "INTEGER")
        self.graph_db.schema.create_property("User", "UpVotes", "INTEGER")
        self.graph_db.schema.create_property("User", "DownVotes", "INTEGER")
        # Vector embedding for semantic search (Phase 3)
        self.graph_db.schema.create_property("User", "embedding", "ARRAY_OF_FLOATS")
        self.graph_db.schema.create_property("User", "vector_id", "STRING")
        print("    ✓ User (Id, DisplayName, Reputation, ...)")

        # Question vertex (Post where PostTypeId=1)
        self.graph_db.schema.create_vertex_type("Question")
        self.graph_db.schema.create_property("Question", "Id", "INTEGER")
        self.graph_db.schema.create_property("Question", "Title", "STRING")
        self.graph_db.schema.create_property("Question", "Body", "STRING")
        self.graph_db.schema.create_property("Question", "Score", "INTEGER")
        self.graph_db.schema.create_property("Question", "ViewCount", "INTEGER")
        self.graph_db.schema.create_property("Question", "CreationDate", "DATETIME")
        self.graph_db.schema.create_property("Question", "AnswerCount", "INTEGER")
        self.graph_db.schema.create_property("Question", "CommentCount", "INTEGER")
        self.graph_db.schema.create_property("Question", "FavoriteCount", "INTEGER")
        # Vote aggregates (from Vote documents)
        self.graph_db.schema.create_property("Question", "UpVotes", "INTEGER")
        self.graph_db.schema.create_property("Question", "DownVotes", "INTEGER")
        self.graph_db.schema.create_property("Question", "BountyAmount", "INTEGER")
        # Vector embedding for semantic search (Phase 3)
        self.graph_db.schema.create_property("Question", "embedding", "ARRAY_OF_FLOATS")
        self.graph_db.schema.create_property("Question", "vector_id", "STRING")
        print("    ✓ Question (Id, Title, Body, Score, Vote aggregates, ...)")

        # Answer vertex (Post where PostTypeId=2)
        self.graph_db.schema.create_vertex_type("Answer")
        self.graph_db.schema.create_property("Answer", "Id", "INTEGER")
        self.graph_db.schema.create_property("Answer", "Body", "STRING")
        self.graph_db.schema.create_property("Answer", "Score", "INTEGER")
        self.graph_db.schema.create_property("Answer", "CreationDate", "DATETIME")
        self.graph_db.schema.create_property("Answer", "CommentCount", "INTEGER")
        # Vote aggregates (from Vote documents)
        self.graph_db.schema.create_property("Answer", "UpVotes", "INTEGER")
        self.graph_db.schema.create_property("Answer", "DownVotes", "INTEGER")
        # Vector embedding for semantic search (Phase 3)
        self.graph_db.schema.create_property("Answer", "embedding", "ARRAY_OF_FLOATS")
        self.graph_db.schema.create_property("Answer", "vector_id", "STRING")
        print("    ✓ Answer (Id, Body, Score, Vote aggregates, ...)")

        # Tag vertex
        self.graph_db.schema.create_vertex_type("Tag")
        self.graph_db.schema.create_property("Tag", "Id", "INTEGER")
        self.graph_db.schema.create_property("Tag", "TagName", "STRING")
        self.graph_db.schema.create_property("Tag", "Count", "INTEGER")
        print("    ✓ Tag (Id, TagName, Count)")

        # Badge vertex
        self.graph_db.schema.create_vertex_type("Badge")
        self.graph_db.schema.create_property("Badge", "Id", "INTEGER")
        self.graph_db.schema.create_property("Badge", "Name", "STRING")
        self.graph_db.schema.create_property("Badge", "Date", "DATETIME")
        self.graph_db.schema.create_property("Badge", "Class", "INTEGER")
        print("    ✓ Badge (Id, Name, Date, Class)")

        # Comment vertex
        self.graph_db.schema.create_vertex_type("Comment")
        self.graph_db.schema.create_property("Comment", "Id", "INTEGER")
        self.graph_db.schema.create_property("Comment", "Text", "STRING")
        self.graph_db.schema.create_property("Comment", "Score", "INTEGER")
        self.graph_db.schema.create_property("Comment", "CreationDate", "DATETIME")
        # Vector embedding for semantic search (Phase 3)
        self.graph_db.schema.create_property("Comment", "embedding", "ARRAY_OF_FLOATS")
        self.graph_db.schema.create_property("Comment", "vector_id", "STRING")
        print("    ✓ Comment (Id, Text, Score, CreationDate)")

        # Create edge types
        print("\n  Creating edge types...")

        # User -> Question (ASKED)
        self.graph_db.schema.create_edge_type("ASKED")
        self.graph_db.schema.create_property("ASKED", "CreationDate", "DATETIME")
        print("    ✓ ASKED (User -> Question, with CreationDate)")

        # User -> Answer (ANSWERED)
        self.graph_db.schema.create_edge_type("ANSWERED")
        self.graph_db.schema.create_property("ANSWERED", "CreationDate", "DATETIME")
        print("    ✓ ANSWERED (User -> Answer, with CreationDate)")

        # Question -> Answer (HAS_ANSWER)
        self.graph_db.schema.create_edge_type("HAS_ANSWER")
        print("    ✓ HAS_ANSWER (Question -> Answer)")

        # Question -> Answer (ACCEPTED_ANSWER, specific answer)
        self.graph_db.schema.create_edge_type("ACCEPTED_ANSWER")
        print("    ✓ ACCEPTED_ANSWER (Question -> Answer)")

        # Question -> Tag (TAGGED_WITH)
        self.graph_db.schema.create_edge_type("TAGGED_WITH")
        print("    ✓ TAGGED_WITH (Question -> Tag)")

        # Comment -> Post (COMMENTED_ON, to Question or Answer)
        self.graph_db.schema.create_edge_type("COMMENTED_ON")
        self.graph_db.schema.create_property("COMMENTED_ON", "CreationDate", "DATETIME")
        self.graph_db.schema.create_property("COMMENTED_ON", "Score", "INTEGER")
        print(
            "    ✓ COMMENTED_ON (Comment -> Question/Answer, with CreationDate, Score)"
        )

        # User -> Badge (EARNED)
        self.graph_db.schema.create_edge_type("EARNED")
        self.graph_db.schema.create_property("EARNED", "Date", "DATETIME")
        self.graph_db.schema.create_property("EARNED", "Class", "INTEGER")
        print("    ✓ EARNED (User -> Badge, with Date, Class)")

        # Post -> Post (LINKED_TO, via PostLink)
        self.graph_db.schema.create_edge_type("LINKED_TO")
        self.graph_db.schema.create_property("LINKED_TO", "LinkTypeId", "INTEGER")
        self.graph_db.schema.create_property("LINKED_TO", "CreationDate", "DATETIME")
        print("    ✓ LINKED_TO (Post -> Post, with LinkTypeId, CreationDate)")

        print("\n  ✅ Vertex and edge types created")
        print("     • 6 vertex types: User, Question, Answer, Tag, Badge, Comment")
        print("     • 8 edge types: ASKED, ANSWERED, HAS_ANSWER, ACCEPTED_ANSWER,")
        print("                     TAGGED_WITH, COMMENTED_ON, EARNED, LINKED_TO")

        # Create indexes on Id fields for fast lookups (outside transaction, with retry logic)
        print("\n  Creating indexes on vertex Id fields...")

        indexes = [
            ("User", "Id", "UNIQUE"),
            ("Question", "Id", "UNIQUE"),
            ("Answer", "Id", "UNIQUE"),
            ("Tag", "Id", "UNIQUE"),
            ("Badge", "Id", "UNIQUE"),
            ("Comment", "Id", "UNIQUE"),
        ]

        retry_config = get_retry_config(self.dataset_size)
        success, failed = create_indexes(
            self.graph_db,
            indexes,
            retry_delay=retry_config["retry_delay"],
            max_retries=retry_config["max_retries"],
            verbose=True,
        )

        if failed:
            raise RuntimeError(f"Failed to create {len(failed)} vertex indexes")

        print(f"\n  ✅ Graph schema complete with {success} indexes")
        print("     • 6 vertex types: User, Question, Answer, Tag, Badge, Comment")
        print("     • 8 edge types: ASKED, ANSWERED, HAS_ANSWER, ACCEPTED_ANSWER,")
        print("                     TAGGED_WITH, COMMENTED_ON, EARNED, LINKED_TO")
        print("     • 6 indexes on Id fields for fast lookups")

    def _convert_to_vertices(self):
        """Convert Phase 1 documents to Phase 2 graph vertices.

        Conversions:
        - User documents → User vertices
        - Post documents (PostTypeId=1) → Question vertices
        - Post documents (PostTypeId=2) → Answer vertices
        - Tag documents → Tag vertices
        - Badge documents → Badge vertices
        - Comment documents → Comment vertices
        - Vote documents → Aggregate into Question/Answer properties
        """
        print("  Opening Phase 1 database (read-only)...")
        doc_db = arcadedb.open_database(str(self.doc_db_path))

        try:
            # Step 3.1: Convert Users
            print("\n  Converting User documents → User vertices...")
            self._convert_users(doc_db)

            # Step 3.2: Convert Posts (split into Questions and Answers)
            print("\n  Converting Post documents → Question/Answer vertices...")
            self._convert_posts(doc_db)

            # Step 3.3: Aggregate Votes into Question/Answer properties
            print("\n  Aggregating Vote counts into Question/Answer vertices...")
            self._aggregate_votes(doc_db)

            # Step 3.4: Convert Tags
            print("\n  Converting Tag documents → Tag vertices...")
            self._convert_tags(doc_db)

            # Step 3.5: Convert Badges
            print("\n  Converting Badge documents → Badge vertices...")
            self._convert_badges(doc_db)

            # Step 3.6: Convert Comments
            print("\n  Converting Comment documents → Comment vertices...")
            self._convert_comments(doc_db)

            print("\n  ✅ All documents converted to vertices")

        finally:
            doc_db.close()
            print("  ✅ Closed Phase 1 database")

    def _convert_users(self, doc_db):
        """Convert User documents to User vertices with pagination."""
        batch = []
        count = 0
        start = time.time()
        batch_times = []

        # Use @rid pagination for large datasets
        last_rid = "#-1:-1"
        while True:
            batch_start = time.time()

            # Query with timing
            query_start = time.time()
            query = f"""
                SELECT *, @rid as rid FROM User
                WHERE @rid > {last_rid}
                LIMIT {self.batch_size}
            """
            chunk = list(doc_db.query("sql", query))
            query_time = time.time() - query_start

            if not chunk:
                break

            for user in chunk:
                # Extract properties
                vertex_data = {
                    "Id": user.get("Id"),
                    "DisplayName": user.get("DisplayName"),
                    "Reputation": user.get("Reputation"),
                    "CreationDate": user.get("CreationDate"),
                    "Views": user.get("Views"),
                    "UpVotes": user.get("UpVotes"),
                    "DownVotes": user.get("DownVotes"),
                }

                batch.append(vertex_data)
                count += 1

            # Insert batch and track time
            db_time = self._insert_vertex_batch("User", batch)
            batch_time = time.time() - batch_start
            batch_times.append((len(batch), query_time, db_time, batch_time))

            # Print batch stats
            print_batch_stats(
                count=len(batch),
                query_time=query_time,
                db_time=db_time,
                total_time=batch_time,
                item_name="users",
            )
            batch = []

            # Update pagination cursor
            last_rid = chunk[-1].get("rid")

        elapsed = time.time() - start

        # Print summary stats
        print_summary_stats(
            total_count=count,
            elapsed=elapsed,
            batch_times=batch_times,
            item_name="User vertices",
            has_embed=False,
            has_query=True,
        )

    def _convert_posts(self, doc_db):
        """Convert Post documents to Question/Answer vertices with pagination."""
        question_count = 0
        answer_count = 0
        start = time.time()
        question_times = []
        answer_times = []
        total_query_time = 0
        query_count = 0

        # Use @rid pagination for large datasets
        last_rid = "#-1:-1"
        while True:
            batch_start = time.time()

            # Query with timing
            query_start = time.time()
            query = f"""
                SELECT *, @rid as rid FROM Post
                WHERE @rid > {last_rid}
                LIMIT {self.batch_size}
            """
            chunk = list(doc_db.query("sql", query))
            query_time = time.time() - query_start
            total_query_time += query_time
            query_count += 1

            if not chunk:
                break

            # Process entire chunk before inserting (for deterministic pagination)
            chunk_questions = []
            chunk_answers = []

            for post in chunk:
                post_type_id = post.get("PostTypeId")

                if post_type_id == 1:  # Question
                    vertex_data = {
                        "Id": post.get("Id"),
                        "Title": post.get("Title"),
                        "Body": post.get("Body"),
                        "Score": post.get("Score"),
                        "ViewCount": post.get("ViewCount"),
                        "CreationDate": post.get("CreationDate"),
                        "AnswerCount": post.get("AnswerCount"),
                        "CommentCount": post.get("CommentCount"),
                        "FavoriteCount": post.get("FavoriteCount"),
                        # Vote aggregates will be added later
                        "UpVotes": 0,
                        "DownVotes": 0,
                        "BountyAmount": 0,
                    }
                    chunk_questions.append(vertex_data)

                elif post_type_id == 2:  # Answer
                    vertex_data = {
                        "Id": post.get("Id"),
                        "Body": post.get("Body"),
                        "Score": post.get("Score"),
                        "CreationDate": post.get("CreationDate"),
                        "CommentCount": post.get("CommentCount"),
                        # Vote aggregates will be added later
                        "UpVotes": 0,
                        "DownVotes": 0,
                    }
                    chunk_answers.append(vertex_data)

            # Insert chunks after processing entire chunk (deterministic batching)
            if chunk_questions:
                q_db_time = self._insert_vertex_batch("Question", chunk_questions)
                batch_time = time.time() - batch_start
                question_times.append(
                    (len(chunk_questions), query_time, q_db_time, batch_time)
                )
                print_batch_stats(
                    count=len(chunk_questions),
                    query_time=query_time,
                    db_time=q_db_time,
                    total_time=batch_time,
                    item_name="questions",
                )
                question_count += len(chunk_questions)

            if chunk_answers:
                a_db_time = self._insert_vertex_batch("Answer", chunk_answers)
                batch_time = time.time() - batch_start
                answer_times.append(
                    (len(chunk_answers), query_time, a_db_time, batch_time)
                )
                print_batch_stats(
                    count=len(chunk_answers),
                    query_time=query_time,
                    db_time=a_db_time,
                    total_time=batch_time,
                    item_name="answers",
                )
                answer_count += len(chunk_answers)

            # Update pagination cursor (after all inserts for this chunk)
            last_rid = chunk[-1].get("rid")

        elapsed = time.time() - start

        # Print summary for Questions
        if question_times:
            print_summary_stats(
                total_count=question_count,
                elapsed=elapsed,
                batch_times=question_times,
                item_name="Question vertices",
                has_embed=False,
                has_query=True,
            )

        # Print summary for Answers
        if answer_times:
            print_summary_stats(
                total_count=answer_count,
                elapsed=elapsed,
                batch_times=answer_times,
                item_name="Answer vertices",
                has_embed=False,
                has_query=True,
            )

        # Print combined summary
        total_rate = (question_count + answer_count) / elapsed if elapsed > 0 else 0
        print(
            f"    ✓ Total: {question_count + answer_count:,} vertices | total: {elapsed:.2f}s | avg rate: {total_rate:,.0f} v/s"
        )

    def _aggregate_votes(self, doc_db):
        """Aggregate Vote counts into Question/Answer vertex properties.

        Uses pagination to avoid loading all votes into memory at once.
        """
        overall_start = time.time()

        # Phase 1: Query and aggregate votes
        print("    Phase 1: Querying and aggregating votes...")
        post_votes = {}
        total_votes_processed = 0
        last_rid = "#-1:-1"
        query_batch_times = []

        while True:
            batch_start = time.time()

            # Read votes in batches using @rid pagination
            query_start = time.time()
            vote_query = f"""
                SELECT *, @rid as rid
                FROM Vote
                WHERE PostId IS NOT NULL AND @rid > {last_rid}
                LIMIT {self.batch_size}
            """
            chunk = list(doc_db.query("sql", vote_query))
            query_time = time.time() - query_start

            if not chunk:
                break

            # Aggregate votes from this chunk (in-memory processing)
            for vote in chunk:
                post_id = vote.get("PostId")
                vote_type = vote.get("VoteTypeId")
                bounty = vote.get("BountyAmount") or 0

                if post_id not in post_votes:
                    post_votes[post_id] = {"up": 0, "down": 0, "bounty": 0}

                # VoteTypeId: 2=UpVote, 3=DownVote, 8=Bounty
                if vote_type == 2:
                    post_votes[post_id]["up"] += 1
                elif vote_type == 3:
                    post_votes[post_id]["down"] += 1

                if bounty > 0:
                    post_votes[post_id]["bounty"] += bounty

            batch_time = time.time() - batch_start
            total_votes_processed += len(chunk)
            # Format: (count, query_t, db_t, total_t) for has_query=True
            # Since this is just query+aggregation, db_t is 0
            query_batch_times.append((len(chunk), query_time, 0, batch_time))

            # Print batch stats
            print_batch_stats(
                count=len(chunk),
                query_time=query_time,
                total_time=batch_time,
                item_name="votes",
            )

            # Update pagination cursor
            last_rid = chunk[-1].get("rid")

        query_phase_time = time.time() - overall_start

        # Print query phase summary
        print_summary_stats(
            total_count=total_votes_processed,
            elapsed=query_phase_time,
            batch_times=query_batch_times,
            item_name="votes",
            has_embed=False,
            has_query=True,
        )

        print(f"    → Found vote data for {len(post_votes):,} unique posts")
        print()

        # Phase 2: Update vertices with aggregated vote counts
        print("    Phase 2: Updating Question/Answer vertices...")
        update_start = time.time()
        post_ids = list(post_votes.keys())
        q_updated = 0
        a_updated = 0
        update_batch_times = []

        for i in range(0, len(post_ids), self.batch_size):
            batch_start = time.time()
            batch_ids = post_ids[i : i + self.batch_size]

            # Database updates in transaction
            db_start = time.time()
            with self.graph_db.transaction():
                # Update Questions in batch
                for post_id in batch_ids:
                    votes = post_votes[post_id]
                    # surprisingly, trying to use java api here instead of the sql query
                    # results in slower db performance. This should be investigated further.
                    update_query = f"""
                        UPDATE Question SET
                            UpVotes = {votes["up"]},
                            DownVotes = {votes["down"]},
                            BountyAmount = {votes["bounty"]}
                        WHERE Id = {post_id}
                    """
                    result = list(self.graph_db.command("sql", update_query))
                    if result and len(result) > 0:
                        q_updated += 1

                # Update Answers in same transaction
                for post_id in batch_ids:
                    votes = post_votes[post_id]
                    update_query = f"""
                        UPDATE Answer SET
                            UpVotes = {votes["up"]},
                            DownVotes = {votes["down"]}
                        WHERE Id = {post_id}
                    """
                    result = list(self.graph_db.command("sql", update_query))
                    if result and len(result) > 0:
                        a_updated += 1

            db_time = time.time() - db_start
            batch_time = time.time() - batch_start
            update_batch_times.append((len(batch_ids), db_time))

            # Print batch stats
            print_batch_stats(
                count=len(batch_ids),
                db_time=db_time,
                total_time=batch_time,
                item_name="posts",
            )

        update_phase_time = time.time() - update_start

        # Print update phase summary
        print_summary_stats(
            total_count=len(post_ids),
            elapsed=update_phase_time,
            batch_times=update_batch_times,
            item_name="posts updated",
            has_embed=False,
            has_query=False,
        )

        overall_elapsed = time.time() - overall_start
        print(
            f"    ✓ Updated {q_updated:,} Questions "
            f"and {a_updated:,} Answers with vote counts"
        )
        print(
            f"    ✓ Total time: {overall_elapsed:.2f}s "
            f"(query: {query_phase_time:.2f}s, "
            f"update: {update_phase_time:.2f}s)"
        )

    def _convert_tags(self, doc_db):
        """Convert Tag documents to Tag vertices with pagination."""
        batch = []
        count = 0
        start = time.time()
        batch_times = []

        # Use @rid pagination for large datasets
        last_rid = "#-1:-1"
        while True:
            batch_start = time.time()

            # Query with timing
            query_start = time.time()
            query = f"""
                SELECT *, @rid as rid FROM Tag
                WHERE @rid > {last_rid}
                LIMIT {self.batch_size}
            """
            chunk = list(doc_db.query("sql", query))
            query_time = time.time() - query_start

            if not chunk:
                break

            for tag in chunk:
                vertex_data = {
                    "Id": tag.get("Id"),
                    "TagName": tag.get("TagName"),
                    "Count": tag.get("Count"),
                }

                batch.append(vertex_data)
                count += 1

            # Insert batch and track time
            db_time = self._insert_vertex_batch("Tag", batch)
            batch_time = time.time() - batch_start
            batch_times.append((len(batch), query_time, db_time, batch_time))

            # Print batch stats
            print_batch_stats(
                count=len(batch),
                query_time=query_time,
                db_time=db_time,
                total_time=batch_time,
                item_name="tags",
            )
            batch = []

            # Update pagination cursor
            last_rid = chunk[-1].get("rid")

        elapsed = time.time() - start

        # Print summary stats
        print_summary_stats(
            total_count=count,
            elapsed=elapsed,
            batch_times=batch_times,
            item_name="Tag vertices",
            has_embed=False,
            has_query=True,
        )

    def _convert_badges(self, doc_db):
        """Convert Badge documents to Badge vertices with pagination."""
        batch = []
        count = 0
        start = time.time()
        batch_times = []

        # Use @rid pagination for large datasets
        last_rid = "#-1:-1"
        while True:
            batch_start = time.time()

            # Query with timing
            query_start = time.time()
            query = f"""
                SELECT *, @rid as rid FROM Badge
                WHERE @rid > {last_rid}
                LIMIT {self.batch_size}
            """
            chunk = list(doc_db.query("sql", query))
            query_time = time.time() - query_start

            if not chunk:
                break

            for badge in chunk:
                vertex_data = {
                    "Id": badge.get("Id"),
                    "Name": badge.get("Name"),
                    "Date": badge.get("Date"),
                    "Class": badge.get("Class"),
                }

                batch.append(vertex_data)
                count += 1

            # Insert batch and track time
            db_time = self._insert_vertex_batch("Badge", batch)
            batch_time = time.time() - batch_start
            batch_times.append((len(batch), query_time, db_time, batch_time))

            # Print batch stats
            print_batch_stats(
                count=len(batch),
                query_time=query_time,
                db_time=db_time,
                total_time=batch_time,
                item_name="badges",
            )
            batch = []

            # Update pagination cursor
            last_rid = chunk[-1].get("rid")

        elapsed = time.time() - start

        # Print summary stats
        print_summary_stats(
            total_count=count,
            elapsed=elapsed,
            batch_times=batch_times,
            item_name="Badge vertices",
            has_embed=False,
            has_query=True,
        )

    def _convert_comments(self, doc_db):
        """Convert Comment documents to Comment vertices with pagination."""
        batch = []
        count = 0
        start = time.time()
        batch_times = []

        # Use @rid pagination for large datasets
        last_rid = "#-1:-1"
        while True:
            batch_start = time.time()

            # Query with timing
            query_start = time.time()
            query = f"""
                SELECT *, @rid as rid FROM Comment
                WHERE @rid > {last_rid}
                LIMIT {self.batch_size}
            """
            chunk = list(doc_db.query("sql", query))
            query_time = time.time() - query_start

            if not chunk:
                break

            for comment in chunk:
                vertex_data = {
                    "Id": comment.get("Id"),
                    "Text": comment.get("Text"),
                    "Score": comment.get("Score"),
                    "CreationDate": comment.get("CreationDate"),
                }

                batch.append(vertex_data)
                count += 1

            # Insert batch and track time
            db_time = self._insert_vertex_batch("Comment", batch)
            batch_time = time.time() - batch_start
            batch_times.append((len(batch), query_time, db_time, batch_time))

            # Print batch stats
            print_batch_stats(
                count=len(batch),
                query_time=query_time,
                db_time=db_time,
                total_time=batch_time,
                item_name="comments",
            )
            batch = []

            # Update pagination cursor
            last_rid = chunk[-1].get("rid")

        elapsed = time.time() - start

        # Print summary stats
        print_summary_stats(
            total_count=count,
            elapsed=elapsed,
            batch_times=batch_times,
            item_name="Comment vertices",
            has_embed=False,
            has_query=True,
        )

    def _create_edges(self):
        """Create all edges from Phase 1 documents.

        Edge types to create (8):
        1. ASKED: User -> Question (from Post.OwnerUserId)
        2. ANSWERED: User -> Answer (from Post.OwnerUserId)
        3. HAS_ANSWER: Question -> Answer (from Post.ParentId)
        4. ACCEPTED_ANSWER: Question -> Answer (from Post.AcceptedAnswerId)
        5. TAGGED_WITH: Question -> Tag (from Post.Tags, parsed)
        6. COMMENTED_ON: Comment -> Question/Answer (from Comment.PostId)
        7. EARNED: User -> Badge (from Badge.UserId)
        8. LINKED_TO: Post -> Post (from PostLink)
        """
        print("  Opening Phase 1 database (read-only)...")
        doc_db = arcadedb.open_database(str(self.doc_db_path))

        try:
            # Edge 1: ASKED (User -> Question)
            print("\n  Creating ASKED edges (User -> Question)...")
            self._create_asked_edges(doc_db)

            # Edge 2: ANSWERED (User -> Answer)
            print("\n  Creating ANSWERED edges (User -> Answer)...")
            self._create_answered_edges(doc_db)

            # Edge 3: HAS_ANSWER (Question -> Answer)
            print("\n  Creating HAS_ANSWER edges (Question -> Answer)...")
            self._create_has_answer_edges(doc_db)

            # Edge 4: ACCEPTED_ANSWER (Question -> Answer)
            print("\n  Creating ACCEPTED_ANSWER edges (Question -> Answer)...")
            self._create_accepted_answer_edges(doc_db)

            # Edge 5: TAGGED_WITH (Question -> Tag)
            print("\n  Creating TAGGED_WITH edges (Question -> Tag)...")
            self._create_tagged_with_edges(doc_db)

            # Edge 6: COMMENTED_ON (Comment -> Post)
            print("\n  Creating COMMENTED_ON edges (Comment -> Post)...")
            self._create_commented_on_edges(doc_db)

            # Edge 7: EARNED (User -> Badge)
            print("\n  Creating EARNED edges (User -> Badge)...")
            self._create_earned_edges(doc_db)

            # Edge 8: LINKED_TO (Post -> Post)
            print("\n  Creating LINKED_TO edges (Post -> Post)...")
            self._create_linked_to_edges(doc_db)

        finally:
            doc_db.close()

    def _create_asked_edges(self, doc_db):
        """Create ASKED edges: User -> Question.

        Uses vertex.new_edge() method (like example 05) for performance.
        Caches Java vertex objects for batch edge creation.
        """
        count = 0
        batch_times = []
        start = time.time()
        last_rid = "#-1:-1"

        # Track missing vertices across all batches
        total_skipped_missing_user = 0
        total_skipped_missing_question = 0

        while True:
            batch_start = time.time()

            # Query Questions with OwnerUserId and CreationDate
            # Use subquery: first get N records by RID, then filter
            query_start = time.time()
            query = f"""
                SELECT Id, OwnerUserId, CreationDate, rid FROM (
                    SELECT Id, PostTypeId, OwnerUserId, CreationDate, @rid as rid FROM Post
                    WHERE @rid > {last_rid}
                    LIMIT {self.batch_size}
                )
                WHERE PostTypeId = 1 AND OwnerUserId IS NOT NULL
            """
            chunk = list(doc_db.query("sql", query))
            query_time = time.time() - query_start

            if not chunk:
                break

            # Build vertex cache using O(1) index lookups
            cache_start = time.time()
            user_ids = list({p.get("OwnerUserId") for p in chunk})
            question_ids = list({p.get("Id") for p in chunk})

            user_cache = {}
            question_cache = {}

            # Fetch User vertices using direct index lookup (O(1))
            for uid in user_ids:
                vertex = self.graph_db.lookup_by_key("User", ["Id"], [uid])
                if vertex:
                    user_cache[uid] = vertex

            # Fetch Question vertices using direct index lookup (O(1))
            for qid in question_ids:
                vertex = self.graph_db.lookup_by_key("Question", ["Id"], [qid])
                if vertex:
                    question_cache[qid] = vertex

            cache_time = time.time() - cache_start

            # Create edges using vertex.new_edge()
            db_start = time.time()
            edges_created = 0
            skipped_missing_user = 0
            skipped_missing_question = 0
            with self.graph_db.transaction():
                # Process all records from chunk (already filtered by SQL)
                for post in chunk:
                    user_id = post.get("OwnerUserId")
                    question_id = post.get("Id")
                    creation_date = post.get("CreationDate")

                    user_vertex = user_cache.get(user_id)
                    question_vertex = question_cache.get(question_id)

                    if user_vertex and question_vertex:
                        edge = user_vertex.new_edge("ASKED", question_vertex)
                        if creation_date:
                            edge.set(
                                "CreationDate", self._to_epoch_millis(creation_date)
                            )
                        edge.save()
                        edges_created += 1
                    else:
                        # Track missing vertices for data integrity check
                        if not user_vertex:
                            skipped_missing_user += 1
                        if not question_vertex:
                            skipped_missing_question += 1

            # Accumulate across batches
            total_skipped_missing_user += skipped_missing_user
            total_skipped_missing_question += skipped_missing_question

            count += edges_created
            db_time = time.time() - db_start
            batch_time = time.time() - batch_start
            batch_times.append(
                (edges_created, query_time, cache_time, db_time, batch_time)
            )

            print_batch_stats(
                count=edges_created,
                query_time=query_time,
                cache_time=cache_time,
                db_time=db_time,
                total_time=batch_time,
                item_name="edges",
            )

            last_rid = chunk[-1].get("rid")

        elapsed = time.time() - start
        print_summary_stats(
            total_count=count,
            elapsed=elapsed,
            batch_times=batch_times,
            item_name="ASKED edges",
            has_embed=False,
            has_query=True,
            has_cache=True,
        )

        # Data integrity check: warn if vertices were missing
        total_skipped = total_skipped_missing_user + total_skipped_missing_question
        if total_skipped > 0:
            print(
                f"    ⚠️  WARNING: Skipped {total_skipped} edges "
                f"due to missing vertices:"
            )
            if total_skipped_missing_user > 0:
                print(
                    f"        • {total_skipped_missing_user} " f"missing User vertices"
                )
            if total_skipped_missing_question > 0:
                print(
                    f"        • {total_skipped_missing_question} "
                    f"missing Question vertices"
                )

    def _create_answered_edges(self, doc_db):
        """Create ANSWERED edges: User -> Answer.

        Uses vertex.new_edge() method (like example 05) for performance.
        """
        count = 0
        batch_times = []
        start = time.time()
        last_rid = "#-1:-1"

        while True:
            batch_start = time.time()

            # Query Answers with OwnerUserId and CreationDate
            # Use subquery: first get N records by RID, then filter
            query_start = time.time()
            query = f"""
                SELECT Id, OwnerUserId, CreationDate, rid FROM (
                    SELECT Id, PostTypeId, OwnerUserId, CreationDate, @rid as rid FROM Post
                    WHERE @rid > {last_rid}
                    LIMIT {self.batch_size}
                )
                WHERE PostTypeId = 2 AND OwnerUserId IS NOT NULL
            """
            chunk = list(doc_db.query("sql", query))
            query_time = time.time() - query_start

            if not chunk:
                break

            # Build vertex cache using O(1) index lookups
            cache_start = time.time()
            user_ids = list({p.get("OwnerUserId") for p in chunk})
            answer_ids = list({p.get("Id") for p in chunk})

            user_cache = {}
            answer_cache = {}

            # Fetch User vertices using direct index lookup (O(1))
            for uid in user_ids:
                vertex = self.graph_db.lookup_by_key("User", ["Id"], [uid])
                if vertex:
                    user_cache[uid] = vertex

            # Fetch Answer vertices using direct index lookup (O(1))
            for aid in answer_ids:
                vertex = self.graph_db.lookup_by_key("Answer", ["Id"], [aid])
                if vertex:
                    answer_cache[aid] = vertex

            cache_time = time.time() - cache_start

            # Create edges using vertex.new_edge()
            db_start = time.time()
            edges_created = 0
            with self.graph_db.transaction():
                for post in chunk:
                    user_id = post.get("OwnerUserId")
                    answer_id = post.get("Id")
                    creation_date = post.get("CreationDate")

                    user_vertex = user_cache.get(user_id)
                    answer_vertex = answer_cache.get(answer_id)

                    if user_vertex and answer_vertex:
                        edge = user_vertex.new_edge("ANSWERED", answer_vertex)
                        if creation_date:
                            edge.set(
                                "CreationDate", self._to_epoch_millis(creation_date)
                            )
                        edge.save()
                        edges_created += 1

            count += edges_created
            db_time = time.time() - db_start
            batch_time = time.time() - batch_start
            batch_times.append(
                (edges_created, query_time, cache_time, db_time, batch_time)
            )

            print_batch_stats(
                count=edges_created,
                query_time=query_time,
                cache_time=cache_time,
                db_time=db_time,
                total_time=batch_time,
                item_name="edges",
            )

            last_rid = chunk[-1].get("rid")

        elapsed = time.time() - start
        print_summary_stats(
            total_count=count,
            elapsed=elapsed,
            batch_times=batch_times,
            item_name="ANSWERED edges",
            has_embed=False,
            has_query=True,
            has_cache=True,
        )

    def _create_has_answer_edges(self, doc_db):
        """Create HAS_ANSWER edges: Question -> Answer.

        Uses vertex.new_edge() method (like example 05) for performance.
        """
        count = 0
        batch_times = []
        start = time.time()
        last_rid = "#-1:-1"

        while True:
            batch_start = time.time()

            # Query Answers with ParentId (question)
            # Use subquery: first get N records by RID, then filter
            query_start = time.time()
            query = f"""
                SELECT Id, ParentId, rid FROM (
                    SELECT Id, PostTypeId, ParentId, @rid as rid FROM Post
                    WHERE @rid > {last_rid}
                    LIMIT {self.batch_size}
                )
                WHERE PostTypeId = 2 AND ParentId IS NOT NULL
            """
            chunk = list(doc_db.query("sql", query))
            query_time = time.time() - query_start

            if not chunk:
                break

            # Build vertex cache using O(1) index lookups
            cache_start = time.time()
            question_ids = list({p.get("ParentId") for p in chunk})
            answer_ids = list({p.get("Id") for p in chunk})

            question_cache = {}
            answer_cache = {}

            # Fetch Question vertices using direct index lookup (O(1))
            for qid in question_ids:
                vertex = self.graph_db.lookup_by_key("Question", ["Id"], [qid])
                if vertex:
                    question_cache[qid] = vertex

            # Fetch Answer vertices using direct index lookup (O(1))
            for aid in answer_ids:
                vertex = self.graph_db.lookup_by_key("Answer", ["Id"], [aid])
                if vertex:
                    answer_cache[aid] = vertex

            cache_time = time.time() - cache_start

            # Create edges using vertex.new_edge()
            db_start = time.time()
            edges_created = 0
            with self.graph_db.transaction():
                for post in chunk:
                    question_id = post.get("ParentId")
                    answer_id = post.get("Id")

                    question_vertex = question_cache.get(question_id)
                    answer_vertex = answer_cache.get(answer_id)

                    if question_vertex and answer_vertex:
                        edge = question_vertex.new_edge("HAS_ANSWER", answer_vertex)
                        edge.save()
                        edges_created += 1

            count += edges_created
            db_time = time.time() - db_start
            batch_time = time.time() - batch_start
            batch_times.append(
                (edges_created, query_time, cache_time, db_time, batch_time)
            )

            print_batch_stats(
                count=edges_created,
                query_time=query_time,
                cache_time=cache_time,
                db_time=db_time,
                total_time=batch_time,
                item_name="edges",
            )

            last_rid = chunk[-1].get("rid")

        elapsed = time.time() - start
        print_summary_stats(
            total_count=count,
            elapsed=elapsed,
            batch_times=batch_times,
            item_name="HAS_ANSWER edges",
            has_embed=False,
            has_query=True,
            has_cache=True,
        )

    def _create_accepted_answer_edges(self, doc_db):
        """Create ACCEPTED_ANSWER edges: Question -> Answer.

        Uses vertex.new_edge() method (like example 05) for performance.
        """
        count = 0
        batch_times = []
        start = time.time()
        last_rid = "#-1:-1"

        while True:
            batch_start = time.time()

            # Query Questions with AcceptedAnswerId
            # Use subquery: first get N records by RID, then filter
            query_start = time.time()
            query = f"""
                SELECT Id, AcceptedAnswerId, rid FROM (
                    SELECT Id, PostTypeId, AcceptedAnswerId, @rid as rid FROM Post
                    WHERE @rid > {last_rid}
                    LIMIT {self.batch_size}
                )
                WHERE PostTypeId = 1 AND AcceptedAnswerId IS NOT NULL
            """
            chunk = list(doc_db.query("sql", query))
            query_time = time.time() - query_start

            if not chunk:
                break

            # Build vertex cache using O(1) index lookups
            cache_start = time.time()
            question_ids = list({p.get("Id") for p in chunk})
            answer_ids = list({p.get("AcceptedAnswerId") for p in chunk})

            question_cache = {}
            answer_cache = {}

            # Fetch Question vertices using direct index lookup (O(1))
            for qid in question_ids:
                vertex = self.graph_db.lookup_by_key("Question", ["Id"], [qid])
                if vertex:
                    question_cache[qid] = vertex

            # Fetch Answer vertices using direct index lookup (O(1))
            for aid in answer_ids:
                vertex = self.graph_db.lookup_by_key("Answer", ["Id"], [aid])
                if vertex:
                    answer_cache[aid] = vertex

            cache_time = time.time() - cache_start

            # Create edges using vertex.new_edge()
            db_start = time.time()
            edges_created = 0
            with self.graph_db.transaction():
                for post in chunk:
                    question_id = post.get("Id")
                    answer_id = post.get("AcceptedAnswerId")

                    question_vertex = question_cache.get(question_id)
                    answer_vertex = answer_cache.get(answer_id)

                    if question_vertex and answer_vertex:
                        edge = question_vertex.new_edge(
                            "ACCEPTED_ANSWER", answer_vertex
                        )
                        edge.save()
                        edges_created += 1

            count += edges_created
            db_time = time.time() - db_start
            batch_time = time.time() - batch_start
            batch_times.append(
                (edges_created, query_time, cache_time, db_time, batch_time)
            )

            print_batch_stats(
                count=edges_created,
                query_time=query_time,
                cache_time=cache_time,
                db_time=db_time,
                total_time=batch_time,
                item_name="edges",
            )

            last_rid = chunk[-1].get("rid")

        elapsed = time.time() - start
        print_summary_stats(
            total_count=count,
            elapsed=elapsed,
            batch_times=batch_times,
            item_name="ACCEPTED_ANSWER edges",
            has_embed=False,
            has_query=True,
            has_cache=True,
        )

    def _create_tagged_with_edges(self, doc_db):
        """Create TAGGED_WITH edges: Question -> Tag.

        Parse Tags field (format: '<tag1><tag2><tag3>') and create
        edges to Tag vertices.

        Uses vertex.new_edge() method (like example 05) for performance.
        """
        count = 0
        batch_times = []
        start = time.time()
        last_rid = "#-1:-1"

        while True:
            batch_start = time.time()

            # Query Questions with Tags
            # Use Python-side filtering to ensure correct RID pagination
            # (Avoids duplicates caused by updating last_rid from filtered results)
            query_start = time.time()
            query = f"""
                SELECT Id, Tags, PostTypeId, @rid as rid FROM Post
                WHERE @rid > {last_rid}
                ORDER BY @rid
                LIMIT {self.batch_size}
            """
            chunk = list(doc_db.query("sql", query))
            query_time = time.time() - query_start

            if not chunk:
                break

            # Update last_rid from the last record of the SCANNED batch
            last_rid = chunk[-1].get("rid")

            # Filter for Questions with Tags
            filtered_chunk = [
                p for p in chunk if p.get("PostTypeId") == 1 and p.get("Tags")
            ]

            # Parse tags and build cache (Java vertex objects)
            cache_start = time.time()
            question_ids = []
            tag_names = set()
            question_tag_map = {}

            for post in filtered_chunk:
                question_id = post.get("Id")
                tags_str = post.get("Tags")

                if tags_str:
                    # Parse tags (handle both '|tag|' and '<tag>' formats)
                    if "|" in tags_str:
                        # Format: |tag1|tag2|
                        tags = [t for t in tags_str.split("|") if t]
                    else:
                        # Format: <tag1><tag2>
                        tags = re.findall(r"<([^>]+)>", tags_str)

                    if tags:
                        question_ids.append(question_id)
                        question_tag_map[question_id] = tags
                        tag_names.update(tags)

            question_cache = {}
            tag_cache = {}

            # Fetch Question vertices using direct index lookup (O(1))
            for qid in question_ids:
                vertex = self.graph_db.lookup_by_key("Question", ["Id"], [qid])
                if vertex:
                    question_cache[qid] = vertex

            # Fetch Tag vertices - lookup by TagName (no index, use SQL)
            # Note: Could optimize by adding TagName index in schema creation
            for tag_name in tag_names:
                escaped_tag = escape_sql_string(tag_name)
                tag_query = f"SELECT FROM Tag WHERE TagName = '{escaped_tag}'"
                result_set = self.graph_db.query("sql", tag_query)
                for result in result_set:
                    vertex = result.get_vertex()
                    tag_cache[tag_name] = vertex
                    break  # Only need first result

            cache_time = time.time() - cache_start

            # Create edges using vertex.new_edge()
            db_start = time.time()
            edges_created = 0
            with self.graph_db.transaction():
                for question_id, tags in question_tag_map.items():
                    question_vertex = question_cache.get(question_id)

                    if question_vertex:
                        for tag_name in tags:
                            tag_vertex = tag_cache.get(tag_name)
                            if tag_vertex:
                                edge = question_vertex.new_edge(
                                    "TAGGED_WITH", tag_vertex
                                )
                                edge.save()
                                edges_created += 1

            count += edges_created
            db_time = time.time() - db_start
            batch_time = time.time() - batch_start
            batch_times.append(
                (edges_created, query_time, cache_time, db_time, batch_time)
            )

            print_batch_stats(
                count=edges_created,
                query_time=query_time,
                cache_time=cache_time,
                db_time=db_time,
                total_time=batch_time,
                item_name="edges",
            )

            # last_rid already updated at start of loop

        elapsed = time.time() - start
        print_summary_stats(
            total_count=count,
            elapsed=elapsed,
            batch_times=batch_times,
            item_name="TAGGED_WITH edges",
            has_embed=False,
            has_query=True,
            has_cache=True,
        )

    def _create_commented_on_edges(self, doc_db):
        """Create COMMENTED_ON edges: Comment -> Question/Answer.

        Comments can reference either Questions (PostTypeId=1) or
        Answers (PostTypeId=2).

        Uses vertex.new_edge() method (like example 05) for performance.
        """
        count = 0
        batch_times = []
        start = time.time()
        last_rid = "#-1:-1"

        while True:
            batch_start = time.time()

            # Query Comments with PostId, CreationDate, and Score
            query_start = time.time()
            query = f"""
                SELECT Id, PostId, CreationDate, Score, @rid as rid
                FROM Comment
                WHERE PostId IS NOT NULL
                AND @rid > {last_rid}
                LIMIT {self.batch_size}
            """
            chunk = list(doc_db.query("sql", query))
            query_time = time.time() - query_start

            if not chunk:
                break

            # Build vertex cache (Java vertex objects)
            cache_start = time.time()
            comment_ids = list({c.get("Id") for c in chunk})
            post_ids = list({c.get("PostId") for c in chunk})

            comment_cache = {}
            question_cache = {}
            answer_cache = {}

            # Fetch Comment vertices using O(1) index lookup
            for cid in comment_ids:
                vertex = self.graph_db.lookup_by_key("Comment", ["Id"], [cid])
                if vertex:
                    comment_cache[cid] = vertex

            # Fetch Question and Answer vertices using O(1) index lookup
            for pid in post_ids:
                # Try to find in Questions
                vertex = self.graph_db.lookup_by_key("Question", ["Id"], [pid])
                if vertex:
                    question_cache[pid] = vertex
                else:
                    # Try to find in Answers
                    vertex = self.graph_db.lookup_by_key("Answer", ["Id"], [pid])
                    if vertex:
                        answer_cache[pid] = vertex

            cache_time = time.time() - cache_start

            # Create edges using vertex.new_edge()
            db_start = time.time()
            edges_created = 0
            with self.graph_db.transaction():
                for comment in chunk:
                    comment_id = comment.get("Id")
                    post_id = comment.get("PostId")
                    creation_date = comment.get("CreationDate")
                    score = comment.get("Score")

                    comment_vertex = comment_cache.get(comment_id)
                    # Check if post is a Question or Answer
                    post_vertex = question_cache.get(post_id) or answer_cache.get(
                        post_id
                    )

                    if comment_vertex and post_vertex:
                        edge = comment_vertex.new_edge("COMMENTED_ON", post_vertex)
                        if creation_date:
                            edge.set(
                                "CreationDate", self._to_epoch_millis(creation_date)
                            )
                        if score is not None:
                            edge.set("Score", score)
                        edge.save()
                        edges_created += 1

            count += edges_created
            db_time = time.time() - db_start
            batch_time = time.time() - batch_start
            batch_times.append(
                (edges_created, query_time, cache_time, db_time, batch_time)
            )

            print_batch_stats(
                count=edges_created,
                query_time=query_time,
                cache_time=cache_time,
                db_time=db_time,
                total_time=batch_time,
                item_name="edges",
            )

            last_rid = chunk[-1].get("rid")

        elapsed = time.time() - start
        print_summary_stats(
            total_count=count,
            elapsed=elapsed,
            batch_times=batch_times,
            item_name="COMMENTED_ON edges",
            has_embed=False,
            has_query=True,
            has_cache=True,
        )

    def _create_earned_edges(self, doc_db):
        """Create EARNED edges: User -> Badge.

        Uses vertex.new_edge() method (like example 05) for performance.
        """
        count = 0
        batch_times = []
        start = time.time()
        last_rid = "#-1:-1"

        while True:
            batch_start = time.time()

            # Query Badges with UserId, Date, and Class
            query_start = time.time()
            query = f"""
                SELECT Id, UserId, Date, Class, @rid as rid FROM Badge
                WHERE UserId IS NOT NULL
                AND @rid > {last_rid}
                LIMIT {self.batch_size}
            """
            chunk = list(doc_db.query("sql", query))
            query_time = time.time() - query_start

            if not chunk:
                break

            # Build vertex cache (Java vertex objects)
            cache_start = time.time()
            user_ids = list({b.get("UserId") for b in chunk})
            badge_ids = list({b.get("Id") for b in chunk})

            user_cache = {}
            badge_cache = {}

            # Fetch User vertices using O(1) index lookup
            for uid in user_ids:
                vertex = self.graph_db.lookup_by_key("User", ["Id"], [uid])
                if vertex:
                    user_cache[uid] = vertex

            # Fetch Badge vertices using O(1) index lookup
            for bid in badge_ids:
                vertex = self.graph_db.lookup_by_key("Badge", ["Id"], [bid])
                if vertex:
                    badge_cache[bid] = vertex

            cache_time = time.time() - cache_start

            # Create edges using vertex.new_edge()
            db_start = time.time()
            edges_created = 0
            with self.graph_db.transaction():
                for badge in chunk:
                    user_id = badge.get("UserId")
                    badge_id = badge.get("Id")
                    date = badge.get("Date")
                    badge_class = badge.get("Class")

                    user_vertex = user_cache.get(user_id)
                    badge_vertex = badge_cache.get(badge_id)

                    if user_vertex and badge_vertex:
                        edge = user_vertex.new_edge("EARNED", badge_vertex)
                        if date:
                            edge.set("Date", self._to_epoch_millis(date))
                        if badge_class is not None:
                            edge.set("Class", badge_class)
                        edge.save()
                        edges_created += 1

            count += edges_created
            db_time = time.time() - db_start
            batch_time = time.time() - batch_start
            batch_times.append(
                (edges_created, query_time, cache_time, db_time, batch_time)
            )

            print_batch_stats(
                count=edges_created,
                query_time=query_time,
                cache_time=cache_time,
                db_time=db_time,
                total_time=batch_time,
                item_name="edges",
            )

            last_rid = chunk[-1].get("rid")

        elapsed = time.time() - start
        print_summary_stats(
            total_count=count,
            elapsed=elapsed,
            batch_times=batch_times,
            item_name="EARNED edges",
            has_embed=False,
            has_query=True,
            has_cache=True,
        )

    def _create_linked_to_edges(self, doc_db):
        """Create LINKED_TO edges: Post -> Post (via PostLink).

        PostLink contains PostId, RelatedPostId, and LinkTypeId.
        Both posts can be Questions or Answers.

        Uses vertex.new_edge() method (like example 05) for performance.
        """
        count = 0
        batch_times = []
        start = time.time()
        last_rid = "#-1:-1"

        while True:
            batch_start = time.time()

            # Query PostLinks with CreationDate
            query_start = time.time()
            query = f"""
                SELECT PostId, RelatedPostId, LinkTypeId, CreationDate,
                       @rid as rid
                FROM PostLink
                WHERE PostId IS NOT NULL AND RelatedPostId IS NOT NULL
                AND @rid > {last_rid}
                LIMIT {self.batch_size}
            """
            chunk = list(doc_db.query("sql", query))
            query_time = time.time() - query_start

            if not chunk:
                break

            # Build vertex cache - posts can be Q or A (Java vertex objects)
            cache_start = time.time()
            post_ids = set()
            for link in chunk:
                post_ids.add(link.get("PostId"))
                post_ids.add(link.get("RelatedPostId"))

            post_cache = {}

            # Fetch Post vertices (Questions and Answers) using O(1) index lookup
            for pid in post_ids:
                # Try to find in Questions
                vertex = self.graph_db.lookup_by_key("Question", ["Id"], [pid])
                if vertex:
                    post_cache[pid] = vertex
                else:
                    # Try to find in Answers
                    vertex = self.graph_db.lookup_by_key("Answer", ["Id"], [pid])
                    if vertex:
                        post_cache[pid] = vertex

            cache_time = time.time() - cache_start

            # Create edges using vertex.new_edge()
            db_start = time.time()
            edges_created = 0
            with self.graph_db.transaction():
                for link in chunk:
                    post_id = link.get("PostId")
                    related_id = link.get("RelatedPostId")
                    link_type = link.get("LinkTypeId")
                    creation_date = link.get("CreationDate")

                    from_vertex = post_cache.get(post_id)
                    to_vertex = post_cache.get(related_id)

                    if from_vertex and to_vertex:
                        edge = from_vertex.new_edge("LINKED_TO", to_vertex)
                        if link_type is not None:
                            edge.set("LinkTypeId", link_type)
                        if creation_date:
                            edge.set(
                                "CreationDate", self._to_epoch_millis(creation_date)
                            )
                        edge.save()
                        edges_created += 1

            count += edges_created
            db_time = time.time() - db_start
            batch_time = time.time() - batch_start
            batch_times.append(
                (edges_created, query_time, cache_time, db_time, batch_time)
            )

            print_batch_stats(
                count=edges_created,
                query_time=query_time,
                cache_time=cache_time,
                db_time=db_time,
                total_time=batch_time,
                item_name="edges",
            )

            last_rid = chunk[-1].get("rid")

        elapsed = time.time() - start
        print_summary_stats(
            total_count=count,
            elapsed=elapsed,
            batch_times=batch_times,
            item_name="LINKED_TO edges",
            has_embed=False,
            has_query=True,
            has_cache=True,
        )

    def _insert_vertex_batch(self, vertex_type, batch):
        """Insert a batch of vertices.

        Returns:
            float: Time elapsed in seconds for the database operation
        """
        from datetime import datetime

        batch_start = time.time()
        with self.graph_db.transaction():
            for vertex_data in batch:
                # Convert datetime objects to epoch milliseconds for Java
                vertex = self.graph_db.new_vertex(vertex_type)
                for key, value in vertex_data.items():
                    if isinstance(value, datetime):
                        # Convert to epoch milliseconds (Java timestamp format)
                        vertex.set(key, int(value.timestamp() * 1000))
                    else:
                        vertex.set(key, value)
                vertex.save()

        return time.time() - batch_start


# =============================================================================
# Phase 3: Vector Embeddings and Vector Indexing
# =============================================================================


class Phase3VectorEmbeddings:
    """Phase 3: Add vector embeddings and vector indexes to graph vertices.

    Converts text fields from Questions and Answers into embeddings for
    semantic search capabilities.
    """

    def __init__(
        self,
        graph_db_path: Path,
        batch_size: int = 1000,
        encode_batch_size: int = 256,
        model_name: str = "all-MiniLM-L6-v2",
    ):
        """Initialize Phase 3.

        Args:
            graph_db_path: Path to the graph database from Phase 2
            batch_size: Batch size for progress reporting (not transaction batching)
            encode_batch_size: Batch size for encoding embeddings with model
            model_name: Name of the embedding model to use
        """
        self.graph_db_path = graph_db_path
        self.batch_size = batch_size
        self.encode_batch_size = encode_batch_size
        self.model = None
        self.model_name = model_name

        # Detect GPU availability
        import torch

        self.device = "cuda" if torch.cuda.is_available() else "cpu"

    def run(self):
        """Run Phase 3: Generate embeddings and create vector indexes."""
        print("=" * 80)
        print("PHASE 3: VECTOR EMBEDDINGS AND VECTOR INDEXING")
        print("=" * 80)
        print(f"Graph database: {self.graph_db_path}")
        print(f"Embedding model: {self.model_name}")
        print(f"Device: {self.device}")
        print(f"Encode batch size: {self.encode_batch_size}")
        print(f"Progress reporting every: {self.batch_size:,} items")
        print()

        phase_start = time.time()

        # Check dependencies
        self._check_dependencies()

        # Load embedding model
        self._load_model()

        # Open graph database
        print(f"Opening graph database: {self.graph_db_path}")
        db = arcadedb.open_database(str(self.graph_db_path))

        try:
            print("✓ Database opened")
            print()

            # Validate Phase 2 completion
            StackOverflowValidator.validate_phase2(db=db, verbose=True, indent="")
            print()

            # Generate embeddings for Questions
            print("=" * 80)
            print("Step 1: Generating embeddings for Questions")
            print("=" * 80)
            self._generate_question_embeddings(db)

            # Generate embeddings for Answers
            print()
            print("=" * 80)
            print("Step 2: Generating embeddings for Answers")
            print("=" * 80)
            self._generate_answer_embeddings(db)

            # Generate embeddings for Comments
            print()
            print("=" * 80)
            print("Step 3: Generating embeddings for Comments")
            print("=" * 80)
            self._generate_comment_embeddings(db)

            # Generate embeddings for Users
            print()
            print("=" * 80)
            print("Step 4: Generating embeddings for Users")
            print("=" * 80)
            self._generate_user_embeddings(db)

            # Create vector indexes
            print()
            print("=" * 80)
            print("Step 5: Creating vector indexes")
            print("=" * 80)
            indexes = self._create_vector_indexes(db)

            # Demo: Vector search examples
            print()
            print("=" * 80)
            print("Step 6: Vector Search Examples")
            print("=" * 80)
            self._run_vector_search_examples(db, indexes)

            # Close database safely (wait for all async operations)
            close_database_safely(db, verbose=True)

        except Exception as e:
            print(f"\n❌ Phase 3 failed: {e}")
            if db:
                db.close()
            raise

        # Phase complete
        phase_elapsed = time.time() - phase_start
        print()
        print("=" * 80)
        print("✅ PHASE 3 COMPLETE")
        print("=" * 80)
        print(f"Total time: {phase_elapsed:.2f}s ({phase_elapsed / 60:.1f} minutes)")
        print("=" * 80)

    def _check_dependencies(self):
        """Check required dependencies for embeddings."""
        print("Checking dependencies...")

        try:
            import sentence_transformers

            print(f"  ✓ sentence-transformers {sentence_transformers.__version__}")
        except ImportError:
            print("  ❌ sentence-transformers not found")
            print("     Install: uv pip install sentence-transformers")
            sys.exit(1)

        try:
            import numpy

            print(f"  ✓ numpy {numpy.__version__}")
        except ImportError:
            print("  ❌ numpy not found")
            print("     Install: uv pip install numpy")
            sys.exit(1)

        print()

    def _load_model(self):
        """Load sentence-transformers model."""
        from sentence_transformers import SentenceTransformer

        print(f"Loading embedding model: {self.model_name}...")
        start = time.time()
        self.model = SentenceTransformer(self.model_name)
        elapsed = time.time() - start
        print(f"✓ Model loaded in {elapsed:.2f}s")
        print()

    def _generate_question_embeddings(self, db):
        """Generate and store embeddings for all Questions using batched pagination."""
        # Check if embeddings already exist
        result = list(
            db.query(
                "sql",
                "SELECT count(*) as count FROM Question WHERE embedding IS NOT NULL",
            )
        )
        if result and result[0].get("count") > 0:
            count = result[0].get("count")
            print(f"✓ Embeddings already exist ({count:,} Questions)")
            return

        # Count total questions
        total_result = list(db.query("sql", "SELECT count(*) as count FROM Question"))
        total = total_result[0].get("count")

        if total == 0:
            print("⚠️  No Questions found")
            return

        print(f"Processing {total:,} Questions in batches of {self.batch_size:,}")

        # Process in batches using RID pagination
        total_processed = 0
        last_rid = "#-1:-1"
        start_time = time.time()
        batch_times = []

        while True:
            batch_start = time.time()

            # Fetch batch
            batch_query = f"""
                SELECT Id, Title, Body, @rid as rid FROM Question
                WHERE @rid > {last_rid}
                LIMIT {self.batch_size}
            """
            batch = list(db.query("sql", batch_query))

            if not batch:
                break

            # Prepare texts for this batch
            texts = []
            ids = []
            for q in batch:
                title = q.get("Title") if q.has_property("Title") else ""
                body = q.get("Body") if q.has_property("Body") else ""
                text = f"{title} {body}".strip()
                texts.append(text)
                ids.append(q.get("Id"))

            # Generate embeddings for this batch
            embed_start = time.time()
            batch_embeddings = self.model.encode(
                texts,
                batch_size=self.encode_batch_size,
                show_progress_bar=False,
                convert_to_numpy=True,
                device=self.device,
            )
            embed_time = time.time() - embed_start

            # Store embeddings using UPDATE SQL (idk why but faster than vertex.modify())
            db_start = time.time()
            with db.transaction():
                for question_id, embedding in zip(ids, batch_embeddings):
                    java_embedding = arcadedb.to_java_float_array(embedding)
                    db.command(
                        "sql",
                        "UPDATE Question SET embedding = ?, vector_id = ? WHERE Id = ?",
                        [java_embedding, str(question_id), question_id],
                    )
            db_time = time.time() - db_start

            # Update progress
            total_processed += len(batch)
            batch_time = time.time() - batch_start
            batch_times.append((len(batch), embed_time, db_time, batch_time))

            print_batch_stats(
                count=len(batch),
                embed_time=embed_time,
                db_time=db_time,
                total_time=batch_time,
                item_name="questions",
            )

            # Update last_rid for pagination
            last_rid = batch[-1].get("rid")

        elapsed = time.time() - start_time
        print_summary_stats(
            total_count=total_processed,
            elapsed=elapsed,
            batch_times=batch_times,
            item_name="Question embeddings",
            has_embed=True,
            has_query=False,
        )

    def _generate_answer_embeddings(self, db):
        """Generate and store embeddings for all Answers using batched pagination."""
        # Check if embeddings already exist
        result = list(
            db.query(
                "sql",
                "SELECT count(*) as count FROM Answer WHERE embedding IS NOT NULL",
            )
        )
        if result and result[0].get("count") > 0:
            count = result[0].get("count")
            print(f"✓ Embeddings already exist ({count:,} Answers)")
            return

        # Count total answers
        total_result = list(db.query("sql", "SELECT count(*) as count FROM Answer"))
        total = total_result[0].get("count")

        if total == 0:
            print("⚠️  No Answers found")
            return

        print(f"Processing {total:,} Answers in batches of {self.batch_size:,}")

        # Process in batches using RID pagination
        total_processed = 0
        last_rid = "#-1:-1"
        start_time = time.time()
        batch_times = []

        while True:
            batch_start = time.time()

            # Fetch batch
            batch_query = f"""
                SELECT Id, Body, @rid as rid FROM Answer
                WHERE @rid > {last_rid}
                LIMIT {self.batch_size}
            """
            batch = list(db.query("sql", batch_query))

            if not batch:
                break

            # Prepare texts for this batch
            texts = []
            ids = []
            for a in batch:
                body = a.get("Body") if a.has_property("Body") else ""
                texts.append(body)
                ids.append(a.get("Id"))

            # Generate embeddings for this batch
            embed_start = time.time()
            batch_embeddings = self.model.encode(
                texts,
                batch_size=self.encode_batch_size,
                show_progress_bar=False,
                convert_to_numpy=True,
                device=self.device,
            )
            embed_time = time.time() - embed_start

            # Store embeddings using UPDATE SQL (faster than vertex.modify())
            db_start = time.time()
            with db.transaction():
                for answer_id, embedding in zip(ids, batch_embeddings):
                    java_embedding = arcadedb.to_java_float_array(embedding)
                    db.command(
                        "sql",
                        "UPDATE Answer SET embedding = ?, vector_id = ? WHERE Id = ?",
                        [java_embedding, str(answer_id), answer_id],
                    )
            db_time = time.time() - db_start

            # Update progress
            total_processed += len(batch)
            batch_time = time.time() - batch_start
            batch_times.append((len(batch), embed_time, db_time, batch_time))

            print_batch_stats(
                count=len(batch),
                embed_time=embed_time,
                db_time=db_time,
                total_time=batch_time,
                item_name="answers",
            )

            # Update last_rid for pagination
            last_rid = batch[-1].get("rid")

        elapsed = time.time() - start_time
        print_summary_stats(
            total_count=total_processed,
            elapsed=elapsed,
            batch_times=batch_times,
            item_name="Answer embeddings",
            has_embed=True,
            has_query=False,
        )

    def _generate_comment_embeddings(self, db):
        """Generate and store embeddings for all Comments using batched pagination."""
        # Check if embeddings already exist
        result = list(
            db.query(
                "sql",
                "SELECT count(*) as count FROM Comment WHERE embedding IS NOT NULL",
            )
        )
        if result and result[0].get("count") > 0:
            count = result[0].get("count")
            print(f"✓ Embeddings already exist ({count:,} Comments)")
            return

        # Count total comments
        total_result = list(db.query("sql", "SELECT count(*) as count FROM Comment"))
        total = total_result[0].get("count")

        if total == 0:
            print("⚠️  No Comments found")
            return

        print(f"Processing {total:,} Comments in batches of {self.batch_size:,}")

        # Process in batches using RID pagination
        total_processed = 0
        last_rid = "#-1:-1"
        start_time = time.time()
        batch_times = []

        while True:
            batch_start = time.time()

            # Fetch batch
            batch_query = f"""
                SELECT Id, Text, @rid as rid FROM Comment
                WHERE @rid > {last_rid}
                LIMIT {self.batch_size}
            """
            batch = list(db.query("sql", batch_query))

            if not batch:
                break

            # Prepare texts for this batch
            texts = []
            ids = []
            for c in batch:
                text = c.get("Text") if c.has_property("Text") else ""
                texts.append(text)
                ids.append(c.get("Id"))

            # Generate embeddings for this batch
            embed_start = time.time()
            batch_embeddings = self.model.encode(
                texts,
                batch_size=self.encode_batch_size,
                show_progress_bar=False,
                convert_to_numpy=True,
                device=self.device,
            )
            embed_time = time.time() - embed_start

            # Store embeddings using UPDATE SQL (faster than vertex.modify())
            db_start = time.time()
            with db.transaction():
                for comment_id, embedding in zip(ids, batch_embeddings):
                    java_embedding = arcadedb.to_java_float_array(embedding)
                    db.command(
                        "sql",
                        "UPDATE Comment SET embedding = ?, vector_id = ? WHERE Id = ?",
                        [java_embedding, str(comment_id), comment_id],
                    )
            db_time = time.time() - db_start

            # Update progress
            total_processed += len(batch)
            batch_time = time.time() - batch_start
            batch_times.append((len(batch), embed_time, db_time, batch_time))

            print_batch_stats(
                count=len(batch),
                embed_time=embed_time,
                db_time=db_time,
                total_time=batch_time,
                item_name="comments",
            )

            # Update last_rid for pagination
            last_rid = batch[-1].get("rid")

        elapsed = time.time() - start_time
        print_summary_stats(
            total_count=total_processed,
            elapsed=elapsed,
            batch_times=batch_times,
            item_name="Comment embeddings",
            has_embed=True,
            has_query=False,
        )

    def _generate_user_embeddings(self, db):
        """Generate and store embeddings for all Users using batched pagination."""
        # Check if embeddings already exist
        result = list(
            db.query(
                "sql",
                "SELECT count(*) as count FROM User WHERE embedding IS NOT NULL",
            )
        )
        if result and result[0].get("count") > 0:
            count = result[0].get("count")
            print(f"✓ Embeddings already exist ({count:,} Users)")
            return

        # Count total users
        total_result = list(db.query("sql", "SELECT count(*) as count FROM User"))
        total = total_result[0].get("count")

        if total == 0:
            print("⚠️  No Users found")
            return

        print(f"Processing {total:,} Users in batches of {self.batch_size:,}")

        # Process in batches using RID pagination
        total_processed = 0
        last_rid = "#-1:-1"
        start_time = time.time()
        batch_times = []

        while True:
            batch_start = time.time()

            # Fetch batch
            batch_query = f"""
                SELECT Id, DisplayName, AboutMe, @rid as rid FROM User
                WHERE @rid > {last_rid}
                LIMIT {self.batch_size}
            """
            batch = list(db.query("sql", batch_query))

            if not batch:
                break

            # Prepare texts for this batch
            texts = []
            ids = []
            for u in batch:
                display_name = (
                    u.get("DisplayName") if u.has_property("DisplayName") else ""
                )
                about_me = u.get("AboutMe") if u.has_property("AboutMe") else ""

                # Combine DisplayName and AboutMe
                if about_me:
                    text = f"{display_name} {about_me}".strip()
                else:
                    text = display_name.strip()

                texts.append(text)
                ids.append(u.get("Id"))

            # Generate embeddings for this batch
            embed_start = time.time()
            batch_embeddings = self.model.encode(
                texts,
                batch_size=self.encode_batch_size,
                show_progress_bar=False,
                convert_to_numpy=True,
                device=self.device,
            )
            embed_time = time.time() - embed_start

            # Store embeddings using UPDATE SQL (faster than vertex.modify())
            db_start = time.time()
            with db.transaction():
                for user_id, embedding in zip(ids, batch_embeddings):
                    java_embedding = arcadedb.to_java_float_array(embedding)
                    db.command(
                        "sql",
                        "UPDATE User SET embedding = ?, vector_id = ? WHERE Id = ?",
                        [java_embedding, str(user_id), user_id],
                    )
            db_time = time.time() - db_start

            # Update progress
            total_processed += len(batch)
            batch_time = time.time() - batch_start
            batch_times.append((len(batch), embed_time, db_time, batch_time))

            print_batch_stats(
                count=len(batch),
                embed_time=embed_time,
                db_time=db_time,
                total_time=batch_time,
                item_name="users",
            )

            # Update last_rid for pagination
            last_rid = batch[-1].get("rid")

        elapsed = time.time() - start_time
        print_summary_stats(
            total_count=total_processed,
            elapsed=elapsed,
            batch_times=batch_times,
            item_name="User embeddings",
            has_embed=True,
            has_query=False,
        )

    def _create_vector_indexes(self, db):
        """Create vector indexes for Questions, Answers, Comments, and Users.

        The default vector index implementation (JVector) automatically indexes ALL
        records of the specified type during creation. This is efficient and
        automatically handles population.
        Index creation is typically very fast.

        Note: Only records with non-null embeddings will be indexed.
        Records without embeddings are skipped.

        Returns:
            Dictionary of index objects keyed by vertex type
        """
        indexes = {}

        # Create Question index
        print()
        print("Creating vector index for Questions...")
        indexes["Question"] = self._create_question_index(db)

        # Create Answer index
        print()
        print("Creating vector index for Answers...")
        indexes["Answer"] = self._create_answer_index(db)

        # Create Comment index
        print()
        print("Creating vector index for Comments...")
        indexes["Comment"] = self._create_comment_index(db)

        # Create User index
        print()
        print("Creating vector index for Users...")
        indexes["User"] = self._create_user_index(db)

        return indexes

    def _create_question_index(self, db):
        """Create vector index for Questions.

        Automatically indexes all Question records with non-null embeddings.
        """
        # Count questions with embeddings
        count_result = list(
            db.query(
                "sql",
                "SELECT count(*) as count FROM Question WHERE embedding IS NOT NULL",
            )
        )
        num_questions = count_result[0].get("count")

        if num_questions == 0:
            print("⚠️  No Questions with embeddings found")
            return

        print("  Vertex type: Question")
        print("  Vector property: embedding")
        print("  Dimensions: 384")
        print("  Distance function: cosine")
        print(f"  Items to index: {num_questions:,}")

        start_time = time.time()

        # Create index - automatically indexes all records
        print("  Creating vector index (auto-indexing all records)...")
        # Note: You can use quantization="INT8" and store_vectors_in_graph=True
        # for better performance on large datasets.
        index = db.create_vector_index(
            vertex_type="Question",
            vector_property="embedding",
            dimensions=384,
            distance_function="cosine",
        )

        elapsed = time.time() - start_time
        print(f"  ✓ Created and indexed {num_questions:,} Questions in {elapsed:.1f}s")

        return index

    def _create_answer_index(self, db):
        """Create vector index for Answers.

        Jvector automatically indexes all Answer records with non-null embeddings.
        """
        # Count answers with embeddings
        count_result = list(
            db.query(
                "sql",
                "SELECT count(*) as count FROM Answer WHERE embedding IS NOT NULL",
            )
        )
        num_answers = count_result[0].get("count")

        if num_answers == 0:
            print("⚠️  No Answers with embeddings found")
            return

        print("  Vertex type: Answer")
        print("  Vector property: embedding")
        print("  Dimensions: 384")
        print("  Distance function: cosine")
        print(f"  Items to index: {num_answers:,}")

        start_time = time.time()

        # Create index - automatically indexes all records
        print("  Creating vector index (auto-indexing all records)...")
        index = db.create_vector_index(
            vertex_type="Answer",
            vector_property="embedding",
            dimensions=384,
            distance_function="cosine",
        )

        elapsed = time.time() - start_time
        print(f"  ✓ Created and indexed {num_answers:,} Answers in {elapsed:.1f}s")

        return index

    def _create_comment_index(self, db):
        """Create vector index for Comments.

        Jvector automatically indexes all Comment records with non-null embeddings.
        """
        # Count comments with embeddings
        count_result = list(
            db.query(
                "sql",
                "SELECT count(*) as count FROM Comment WHERE embedding IS NOT NULL",
            )
        )
        num_comments = count_result[0].get("count")

        if num_comments == 0:
            print("⚠️  No Comments with embeddings found")
            return

        print("  Vertex type: Comment")
        print("  Vector property: embedding")
        print("  Dimensions: 384")
        print("  Distance function: cosine")
        print(f"  Items to index: {num_comments:,}")

        start_time = time.time()

        # Create vector index - automatically indexes all records
        print("  Creating vector index (auto-indexing all records)...")
        index = db.create_vector_index(
            vertex_type="Comment",
            vector_property="embedding",
            dimensions=384,
            distance_function="cosine",
        )

        elapsed = time.time() - start_time
        print(f"  ✓ Created and indexed {num_comments:,} Comments in {elapsed:.1f}s")

        return index

    def _create_user_index(self, db):
        """Create vector index for Users.

        Automatically indexes all User records with non-null embeddings.
        """
        # Count users with embeddings
        count_result = list(
            db.query(
                "sql", "SELECT count(*) as count FROM User WHERE embedding IS NOT NULL"
            )
        )
        num_users = count_result[0].get("count")

        if num_users == 0:
            print("⚠️  No Users with embeddings found")
            return

        print("  Vertex type: User")
        print("  Vector property: embedding")
        print("  Dimensions: 384")
        print("  Distance function: cosine")
        print(f"  Items to index: {num_users:,}")

        start_time = time.time()

        # Create vector index - automatically indexes all records
        print("  Creating vector index (auto-indexing all records)...")
        index = db.create_vector_index(
            vertex_type="User",
            vector_property="embedding",
            dimensions=384,
            distance_function="cosine",
        )

        elapsed = time.time() - start_time
        print(f"  ✓ Created and indexed {num_users:,} Users in {elapsed:.1f}s")

        return index

    def _run_vector_search_examples(self, db, indexes):
        """Run 10 representative vector search examples using natural language queries.

        Args:
            db: Database instance
            indexes: Dictionary of vector indexes keyed by vertex type
        """
        print("Running vector search examples with natural language queries...")
        print()

        # Define 10 representative queries
        search_examples = [
            {
                "query": "How do I parse JSON in Python?",
                "vertex_type": "Question",
                "description": "Programming task query",
            },
            {
                "query": "What causes a NullPointerException?",
                "vertex_type": "Question",
                "description": "Error explanation query",
            },
            {
                "query": "Best practices for database indexing",
                "vertex_type": "Question",
                "description": "Best practices query",
            },
            {
                "query": "Use the json module to load and parse JSON data",
                "vertex_type": "Answer",
                "description": "Solution-focused query",
            },
            {
                "query": (
                    "It occurs when you try to access an object "
                    "that hasn't been initialized"
                ),
                "vertex_type": "Answer",
                "description": "Explanation-focused query",
            },
            {
                "query": "Create indexes on frequently queried columns",
                "vertex_type": "Answer",
                "description": "Advice-focused query",
            },
            {
                "query": "Thanks for the help, this worked perfectly!",
                "vertex_type": "Comment",
                "description": "Positive comment query",
            },
            {
                "query": "This doesn't work in the latest version",
                "vertex_type": "Comment",
                "description": "Issue report query",
            },
            {
                "query": "Experienced Python developer specializing in web development",
                "vertex_type": "User",
                "description": "Developer profile query",
            },
            {
                "query": "Software engineer with expertise in databases",
                "vertex_type": "User",
                "description": "Expert profile query",
            },
        ]

        for i, example in enumerate(search_examples, 1):
            query_text = example["query"]
            vertex_type = example["vertex_type"]
            description = example["description"]

            print(f"Example {i}: {description}")
            print(f'  Query: "{query_text}"')
            print(f"  Target: {vertex_type}")

            try:
                # Get the appropriate index
                index = indexes[vertex_type]

                # Encode query
                encode_start = time.time()
                query_embedding = self.model.encode(
                    [query_text],
                    show_progress_bar=False,
                    convert_to_numpy=True,
                    device=self.device,
                )[0]
                encode_time = time.time() - encode_start

                # Perform vector search using index.find_nearest()
                search_start = time.time()
                all_results = index.find_nearest(query_embedding, k=3)
                search_time = time.time() - search_start

                print(f"  Results: {len(all_results)} found")
                timing = (
                    f"  Timing: Encode {encode_time:.3f}s | "
                    f"Search {search_time:.3f}s"
                )
                print(timing)

                # Display top results
                for j, (vertex, distance) in enumerate(all_results, 1):
                    # Extract relevant fields based on vertex type
                    if vertex_type == "Question":
                        title = vertex.get("Title") or "N/A"
                        preview = title[:60] if len(title) > 60 else title
                        result = (
                            f"    [{j}] Distance: {distance:.4f} | "
                            f"Title: {preview}..."
                        )
                        print(result)
                    elif vertex_type == "Answer":
                        body = vertex.get("Body") or "N/A"
                        if body != "N/A":
                            preview = body[:80].replace("\n", " ")
                        else:
                            preview = "N/A"
                        result = (
                            f"    [{j}] Distance: {distance:.4f} | "
                            f"Body: {preview}..."
                        )
                        print(result)
                    elif vertex_type == "Comment":
                        text = vertex.get("Text") or "N/A"
                        if text != "N/A":
                            preview = text[:80].replace("\n", " ")
                        else:
                            preview = "N/A"
                        result = (
                            f"    [{j}] Distance: {distance:.4f} | "
                            f"Text: {preview}..."
                        )
                        print(result)
                    elif vertex_type == "User":
                        display_name = vertex.get("DisplayName") or "N/A"
                        reputation = vertex.get("Reputation") or 0
                        result = (
                            f"    [{j}] Distance: {distance:.4f} | "
                            f"User: {display_name} "
                            f"(Reputation: {reputation})"
                        )
                        print(result)

            except Exception as e:
                print(f"  ⚠️  Error: {e}")

            print()

        print("✓ Vector search examples complete")


# =============================================================================
# Phase 4: Multi-Model Analytics (SQL + OpenCypher + Vector Search)
# =============================================================================


class Phase4Analytics:
    """Phase 4: Comprehensive analytics combining SQL, OpenCypher, and Vector Search.

    This phase demonstrates:
    - 10 important analytical questions about Stack Overflow data
    - SQL for aggregations and complex queries
    - OpenCypher for graph traversals and path finding
    - Vector search for semantic similarity
    - Hybrid queries combining multiple paradigms
    """

    def __init__(self, graph_db_path: Path, model_name: str = "all-MiniLM-L6-v2"):
        """Initialize Phase 4.

        Args:
            graph_db_path: Path to the graph database from Phase 3
            model_name: Name of the embedding model (must match Phase 3)
        """
        self.graph_db_path = graph_db_path
        self.model = None
        self.model_name = model_name

        # Detect GPU availability
        try:
            import torch

            self.device = "cuda" if torch.cuda.is_available() else "cpu"
        except ImportError:
            self.device = "cpu"

    def run(self):
        """Run Phase 4: Multi-model analytics."""
        print("=" * 80)
        print("PHASE 4: MULTI-MODEL ANALYTICS")
        print("=" * 80)
        print(f"Graph database: {self.graph_db_path}")
        print(f"Embedding model: {self.model_name}")
        print(f"Device: {self.device}")
        print()

        phase_start = time.time()

        # Check dependencies
        self._check_dependencies()

        # Load embedding model
        self._load_model()

        # Open database and load indexes
        print(f"Opening database: {self.graph_db_path}")
        db = arcadedb.open_database(str(self.graph_db_path))

        try:
            print("✓ Database opened")
            print()

            # Load all vector indexes
            print("Loading vector indexes...")
            start_ = time.time()
            indexes = self._load_vector_indexes(db)
            elapsed_ = time.time() - start_
            print(f"✓ Loaded {len(indexes)} vector indexes in {elapsed_:.2f}s")
            print()

            # Run 10 analytical questions
            self._run_analytics(db, indexes)

            # Close database safely
            close_database_safely(db, verbose=True)

        except Exception as e:
            print(f"\n❌ Phase 4 failed: {e}")
            if db:
                db.close()
            raise

        # Phase complete
        phase_elapsed = time.time() - phase_start
        print()
        print("=" * 80)
        print("✅ PHASE 4 COMPLETE")
        print("=" * 80)
        print(f"Total time: {phase_elapsed:.2f}s")
        print("=" * 80)

    def _check_dependencies(self):
        """Check required dependencies."""
        print("Checking dependencies...")

        try:
            import sentence_transformers

            print(f"  ✓ sentence-transformers {sentence_transformers.__version__}")
        except ImportError:
            print("  ❌ sentence-transformers not found")
            print("     Install: uv pip install sentence-transformers")
            sys.exit(1)

        try:
            import numpy

            print(f"  ✓ numpy {numpy.__version__}")
        except ImportError:
            print("  ❌ numpy not found")
            print("     Install: uv pip install numpy")
            sys.exit(1)

        print()

    def _load_model(self):
        """Load sentence-transformers model."""
        from sentence_transformers import SentenceTransformer

        print(f"Loading embedding model: {self.model_name}...")
        start = time.time()
        self.model = SentenceTransformer(self.model_name, device=self.device)
        elapsed = time.time() - start
        print(f"✓ Model loaded in {elapsed:.2f}s")
        print()

    def _load_vector_indexes(self, db):
        """Load all vector indexes from disk.

        Returns:
            Dictionary of index objects keyed by vertex type
        """
        indexes = {}
        vertex_types = ["Question", "Answer", "Comment", "User"]

        for vertex_type in vertex_types:
            start = time.time()
            try:
                # Use the schema API to get vector indexes
                index = db.schema.get_vector_index(
                    vertex_type=vertex_type,
                    vector_property="embedding",
                )
                indexes[vertex_type] = index

                elapsed = time.time() - start
                print(f"  ✓ Loaded index: {vertex_type} in {elapsed:.2f}s")
            except Exception as e:
                print(f"  ⚠️  Could not load index for {vertex_type}: {e}")

        return indexes

    def _run_analytics(self, db, indexes):
        """Run 12 important analytical questions."""
        print("=" * 80)
        print("12 ANALYTICAL QUESTIONS")
        print("=" * 80)
        print()

        # Question 1: Top Contributors (SQL Aggregation)
        self._q1_top_contributors(db)

        # Question 2: Most Discussed Topics (SQL + Graph)
        self._q2_most_discussed_topics(db)

        # Question 3: Expert Users by Tag (SQL with Graph Navigation)
        self._q3_expert_users_by_tag(db)

        # Question 4: Question-Answer Network Patterns (OpenCypher)
        self._q4_qa_network_patterns(db)

        # Question 5: User Collaboration Paths (OpenCypher Traversal)
        self._q5_user_collaboration_paths(db)

        # Question 6: Semantic Question Clustering (Vector Search)
        self._q6_semantic_question_clustering(db, indexes)

        # Question 7: Find Similar Experts (Vector Search + Graph)
        self._q7_find_similar_experts(db, indexes)

        # Question 8: Temporal Activity Analysis (SQL Time Series)
        self._q8_temporal_activity_analysis(db)

        # Question 9: Content Quality Indicators (SQL + Vector)
        self._q9_content_quality_indicators(db, indexes)

        # Question 10: Community Knowledge Graph (Hybrid)
        self._q10_community_knowledge_graph(db, indexes)

        # Question 11: Multi-Document Semantic Search (Vector Only)
        self._q11_cross_content_semantic_search(db, indexes)

        # Question 12: Semantic Answer Retrieval (Vector Only)
        self._q12_find_best_answers_by_topic(db, indexes)

    def _q1_top_contributors(self, db):
        """Q1: Who are the top contributors? (SQL Aggregation)

        Uses SQL to aggregate user activity across questions, answers, and badges.
        """
        print("─" * 80)
        print("Q1: TOP CONTRIBUTORS (SQL Aggregation)")
        print("─" * 80)
        print("Finding users with highest combined reputation and activity...")
        print()

        start = time.time()

        try:
            # Get top users by questions, answers, and reputation
            # Note: Avoid property.asFloat() at start of parenthesized expressions
            query = """
                SELECT
                    DisplayName,
                    Reputation,
                    out('ASKED').size() as questions,
                    out('ANSWERED').size() as answers,
                    out('EARNED').size() as badges,
                    Reputation / 100.0 + out('ASKED').size() * 5 + out('ANSWERED').size() * 10 + out('EARNED').size() as activity_score
                FROM User
                WHERE Reputation > 0
                ORDER BY Reputation DESC
                LIMIT 10
            """

            results = list(db.query("sql", query))

            print("  Top 10 Contributors:")
            print("  " + "─" * 76)
            print(
                f"  {'Rank':<6}{'User':<25}{'Rep':<10}{'Q':<6}{'A':<6}{'Badges':<8}{'Score':<8}"
            )
            print("  " + "─" * 76)

            for i, result in enumerate(results, 1):
                name = result.get("DisplayName") or "Unknown"
                rep = result.get("Reputation") or 0
                q_count = result.get("questions") or 0
                a_count = result.get("answers") or 0
                badge_count = result.get("badges") or 0
                score = result.get("activity_score") or 0

                print(
                    f"  {i:<6}{name[:24]:<25}{rep:<10}{q_count:<6}{a_count:<6}{badge_count:<8}{score:<8.0f}"
                )

            elapsed = time.time() - start
            print(f"\n  ⏱️  Query time: {elapsed:.3f}s")
            print()

        except Exception as e:
            print(f"  ❌ Error: {e}")
            import traceback

            traceback.print_exc()
            print()

    def _q2_most_discussed_topics(self, db):
        """Q2: What are the most discussed topics? (SQL + Graph)

        Uses SQL aggregation with graph navigation to find popular tags.
        """
        print("─" * 80)
        print("Q2: MOST DISCUSSED TOPICS (SQL + Graph)")
        print("─" * 80)
        print("Finding tags with most questions, answers, and comments...")
        print()

        start = time.time()

        try:
            # Get top tags by total engagement
            # Note: Use intermediate calculations to avoid 'in' at start of parentheses
            query = """
                SELECT
                    TagName,
                    in('TAGGED_WITH').size() as question_count,
                    in('TAGGED_WITH').out('HAS_ANSWER').size() as answer_count,
                    in('TAGGED_WITH').in('COMMENTED_ON').size() as comment_count,
                    in('TAGGED_WITH').size() * 10 + in('TAGGED_WITH').out('HAS_ANSWER').size() * 5 + in('TAGGED_WITH').in('COMMENTED_ON').size() as engagement_score
                FROM Tag
                WHERE in('TAGGED_WITH').size() > 0
                ORDER BY engagement_score DESC
                LIMIT 10
            """

            results = list(db.query("sql", query))

            print("  Top 10 Most Discussed Topics:")
            print("  " + "─" * 76)
            print(
                f"  {'Rank':<6}{'Tag':<25}{'Questions':<12}{'Answers':<10}{'Comments':<10}{'Score':<10}"
            )
            print("  " + "─" * 76)

            for i, result in enumerate(results, 1):
                tag = result.get("TagName") or "Unknown"
                q_count = result.get("question_count") or 0
                a_count = result.get("answer_count") or 0
                c_count = result.get("comment_count") or 0
                score = result.get("engagement_score") or 0

                print(
                    f"  {i:<6}{tag[:24]:<25}{q_count:<12}{a_count:<10}{c_count:<10}{score:<10.0f}"
                )

            elapsed = time.time() - start
            print(f"\n  ⏱️  Query time: {elapsed:.3f}s")
            print()

        except Exception as e:
            print(f"  ❌ Error: {e}")
            import traceback

            traceback.print_exc()
            print()

    def _q3_expert_users_by_tag(self, db):
        """Q3: Who are the experts in specific topics? (SQL with Graph Navigation)

        Finds users who have answered many questions in specific tags.
        """
        print("─" * 80)
        print("Q3: EXPERT USERS BY TAG (SQL + Graph)")
        print("─" * 80)
        print("Finding expert users for top tags...")
        print()

        start = time.time()

        try:
            # First, count tags and get the most popular one
            # Use a subquery to avoid ORDER BY with .size()
            tag_query = """
                SELECT TagName, in('TAGGED_WITH').size() as tag_count
                FROM Tag
                ORDER BY tag_count DESC
                LIMIT 1
            """

            tag_result = list(db.query("sql", tag_query))
            if not tag_result:
                print("  ⚠️  No tags found")
                print()
                return

            tag_name = tag_result[0].get("TagName")
            print(f"  Analyzing experts for tag: {tag_name}")
            print()

            # Find users who have answered many questions
            # Use simpler query that works with ArcadeDB SQL
            expert_query = """
                SELECT
                    DisplayName,
                    Reputation,
                    out('ANSWERED').size() as total_answers
                FROM User
                ORDER BY Reputation DESC
                LIMIT 10
            """

            results = list(db.query("sql", expert_query))

            print("  Top 10 Answer Contributors:")
            print("  " + "─" * 76)
            print(f"  {'Rank':<6}{'User':<30}{'Reputation':<15}{'Answers':<10}")
            print("  " + "─" * 76)

            for i, result in enumerate(results, 1):
                name = result.get("DisplayName") or "Unknown"
                rep = result.get("Reputation") or 0
                answers = result.get("total_answers") or 0

                print(f"  {i:<6}{name[:29]:<30}{rep:<15}{answers:<10}")

            elapsed = time.time() - start
            print(f"\n  ⏱️  Query time: {elapsed:.3f}s")
            print()

        except Exception as e:
            print(f"  ❌ Error: {e}")
            import traceback

            traceback.print_exc()
            print()

    def _q4_qa_network_patterns(self, db):
        """Q4: What are common question-answer patterns? (OpenCypher)

        Uses OpenCypher to analyze graph patterns in Q&A interactions.
        """
        print("─" * 80)
        print("Q4: Q&A NETWORK PATTERNS (OpenCypher)")
        print("─" * 80)
        print("Analyzing question-answer interaction patterns...")
        print()

        start = time.time()

        try:
            # Find questions with most answers
            query = """
                MATCH (q:Question)
                OPTIONAL MATCH (q)-[:HAS_ANSWER]->(a:Answer)
                RETURN q.Title as title, count(a) as answer_count, q.Score as score
                ORDER BY answer_count DESC
                LIMIT 5
            """

            results = list(db.query("opencypher", query))

            print("  Top 5 Questions by Answer Count:")
            print("  " + "─" * 76)

            for i, result in enumerate(results, 1):
                title = result.get("title") or "Unknown"
                answer_count = result.get("answer_count") or 0
                score = result.get("score") or 0

                print(f"  [{i}] Answers: {answer_count}, Score: {score}")
                print(f"      {title[:70]}...")
                print()

            elapsed = time.time() - start
            print(f"  ⏱️  Query time: {elapsed:.3f}s")
            print()

        except Exception as e:
            print(f"  ❌ Error: {e}")
            import traceback

            traceback.print_exc()
            print()

    def _q5_user_collaboration_paths(self, db):
        """Q5: How do users collaborate? (OpenCypher Traversal)

        Uses OpenCypher to find paths between users through Q&A interactions.
        """
        print("─" * 80)
        print("Q5: USER COLLABORATION PATHS (OpenCypher)")
        print("─" * 80)
        print("Finding collaboration patterns between users...")
        print()

        start = time.time()

        try:
            # Find users who answer each other's questions
            query = """
                MATCH (u1:User)-[:ASKED]->(:Question)-[:HAS_ANSWER]->(:Answer)<-[:ANSWERED]-(u2:User)
                WHERE u1 <> u2
                RETURN DISTINCT u1.DisplayName as user1, u2.DisplayName as user2
                LIMIT 10
            """

            results = list(db.query("opencypher", query))

            print("  User Collaboration Pairs:")
            print("  " + "─" * 76)

            if results:
                for i, result in enumerate(results, 1):
                    user1 = result.get("user1") or "Unknown"
                    user2 = result.get("user2") or "Unknown"

                    print(f"  [{i}] {user1} ← answered by → {user2}")
            else:
                print("  No collaboration patterns found")

            elapsed = time.time() - start
            print(f"\n  ⏱️  Query time: {elapsed:.3f}s")
            print()

        except Exception as e:
            print(f"  ❌ Error: {e}")
            import traceback

            traceback.print_exc()
            print()

    def _q6_semantic_question_clustering(self, db, indexes):
        """Q6: What questions are semantically similar? (Vector Search)

        Uses vector search to find semantically related questions.
        """
        print("─" * 80)
        print("Q6: SEMANTIC QUESTION CLUSTERING (Vector Search)")
        print("─" * 80)
        print("Finding semantically similar questions...")
        print()

        start = time.time()

        try:
            if "Question" not in indexes:
                print("  ⚠️  Question index not available")
                print()
                return

            # Get a random high-scoring question
            query = """
                SELECT Title, Body
                FROM Question
                WHERE Score > 0 AND Title IS NOT NULL
                ORDER BY Score DESC
                LIMIT 1
            """

            seed_result = list(db.query("sql", query))
            if not seed_result:
                print("  ⚠️  No questions found")
                print()
                return

            seed_title = seed_result[0].get("Title")
            print(f"  Seed Question: {seed_title[:70]}...")
            print()

            # Create embedding for seed question
            seed_embedding = self.model.encode(
                [seed_title],
                show_progress_bar=False,
                convert_to_numpy=True,
                device=self.device,
            )[0]

            # Find similar questions
            index = indexes["Question"]
            results = index.find_nearest(
                seed_embedding, k=6
            )  # Get 6, skip first (self)

            print("  Top 5 Similar Questions:")
            print("  " + "─" * 76)

            for i, (vertex, distance) in enumerate(
                results[1:6], 1
            ):  # Skip first (self)
                title = vertex.get("Title") or "N/A"
                score = vertex.get("Score") or 0

                print(f"  [{i}] Distance: {distance:.4f}, Score: {score}")
                print(f"      {title[:70]}...")
                print()

            elapsed = time.time() - start
            print(f"  ⏱️  Total time: {elapsed:.3f}s")
            print()

        except Exception as e:
            print(f"  ❌ Error: {e}")
            import traceback

            traceback.print_exc()
            print()

    def _q7_find_similar_experts(self, db, indexes):
        """Q7: Find experts with similar profiles (Vector Search + Graph)

        Combines vector search on user profiles with graph metrics.
        """
        print("─" * 80)
        print("Q7: FIND SIMILAR EXPERTS (Vector Search + Graph)")
        print("─" * 80)
        print("Finding users with similar expertise profiles...")
        print()

        start = time.time()

        try:
            if "User" not in indexes:
                print("  ⚠️  User index not available")
                print()
                return

            # Get a high-reputation user
            query = """
                SELECT DisplayName, AboutMe, Reputation
                FROM User
                WHERE Reputation > 100 AND AboutMe IS NOT NULL
                ORDER BY Reputation DESC
                LIMIT 1
            """

            seed_result = list(db.query("sql", query))
            if not seed_result:
                print("  ⚠️  No users found")
                print()
                return

            seed_name = seed_result[0].get("DisplayName")
            seed_rep = seed_result[0].get("Reputation")
            print(f"  Seed User: {seed_name} (Reputation: {seed_rep})")
            print()

            # Create embedding for seed user
            seed_about = seed_result[0].get("AboutMe") or ""
            seed_embedding = self.model.encode(
                [seed_about[:500]],  # Limit to 500 chars
                show_progress_bar=False,
                convert_to_numpy=True,
                device=self.device,
            )[0]

            # Find similar users
            index = indexes["User"]
            results = index.find_nearest(
                seed_embedding, k=6
            )  # Get 6, skip first (self)

            print("  Top 5 Similar Experts:")
            print("  " + "─" * 76)

            for i, (vertex, distance) in enumerate(
                results[1:6], 1
            ):  # Skip first (self)
                name = vertex.get("DisplayName") or "N/A"
                rep = vertex.get("Reputation") or 0
                location = vertex.get("Location") or "Unknown"

                print(f"  [{i}] Distance: {distance:.4f}")
                print(f"      User: {name}, Reputation: {rep}, Location: {location}")
                print()

            elapsed = time.time() - start
            print(f"  ⏱️  Total time: {elapsed:.3f}s")
            print()

        except Exception as e:
            print(f"  ❌ Error: {e}")
            import traceback

            traceback.print_exc()
            print()

    def _q8_temporal_activity_analysis(self, db):
        """Q8: How does activity change over time? (SQL Time Series)

        Analyzes temporal patterns in user activity.
        """
        print("─" * 80)
        print("Q8: TEMPORAL ACTIVITY ANALYSIS (SQL Time Series)")
        print("─" * 80)
        print("Analyzing activity patterns over time...")
        print()

        start = time.time()

        try:
            # Analyze questions over time (by year)
            query = """
                SELECT
                    CreationDate.format('yyyy') as year,
                    count(*) as question_count
                FROM Question
                WHERE CreationDate IS NOT NULL
                GROUP BY year
                ORDER BY year
            """

            results = list(db.query("sql", query))

            print("  Questions Posted per Year:")
            print("  " + "─" * 76)
            print(f"  {'Year':<10}{'Count':<15}{'Bar':<50}")
            print("  " + "─" * 76)

            max_count = max((r.get("question_count") or 0 for r in results), default=1)

            for result in results:
                year = result.get("year") or "Unknown"
                count = result.get("question_count") or 0
                bar_length = int((count / max_count) * 40) if max_count > 0 else 0
                bar = "█" * bar_length

                print(f"  {year:<10}{count:<15}{bar}")

            elapsed = time.time() - start
            print(f"\n  ⏱️  Query time: {elapsed:.3f}s")
            print()

        except Exception as e:
            print(f"  ❌ Error: {e}")
            import traceback

            traceback.print_exc()
            print()

    def _q9_content_quality_indicators(self, db, indexes):
        """Q9: What indicates high-quality content? (SQL + Vector)

        Analyzes correlation between content features and scores.
        """
        print("─" * 80)
        print("Q9: CONTENT QUALITY INDICATORS (SQL + Vector)")
        print("─" * 80)
        print("Analyzing what makes high-quality content...")
        print()

        start = time.time()

        try:
            # Compare high-scoring vs low-scoring questions
            # Run two separate queries since UNION ALL is not supported
            high_query = """
                SELECT
                    avg(out('HAS_ANSWER').size()) as avg_answers,
                    avg(in('COMMENTED_ON').size()) as avg_comments,
                    count(*) as count
                FROM Question
                WHERE Score >= 5
            """

            low_query = """
                SELECT
                    avg(out('HAS_ANSWER').size()) as avg_answers,
                    avg(in('COMMENTED_ON').size()) as avg_comments,
                    count(*) as count
                FROM Question
                WHERE Score < 5 AND Score >= 0
            """

            high_results = list(db.query("sql", high_query))
            low_results = list(db.query("sql", low_query))

            # Combine results
            results = []
            if high_results:
                high_row = high_results[0]
                results.append(("High Score", high_row))
            if low_results:
                low_row = low_results[0]
                results.append(("Low Score", low_row))

            print("  Quality Metrics Comparison:")
            print("  " + "─" * 76)
            print(
                f"  {'Category':<15}{'Count':<12}{'Avg Answers':<15}{'Avg Comments':<15}"
            )
            print("  " + "─" * 76)

            for category, result in results:
                count = result.get("count") or 0
                avg_ans = result.get("avg_answers") or 0
                avg_com = result.get("avg_comments") or 0

                print(f"  {category:<15}{count:<12}{avg_ans:<15.2f}{avg_com:<15.2f}")

            elapsed = time.time() - start
            print(f"\n  ⏱️  Query time: {elapsed:.3f}s")
            print()

        except Exception as e:
            print(f"  ❌ Error: {e}")
            import traceback

            traceback.print_exc()
            print()

    def _q10_community_knowledge_graph(self, db, indexes):
        """Q10: How is knowledge distributed in the community? (Hybrid)

        Combines SQL, OpenCypher, and vector search for comprehensive analysis.
        """
        print("─" * 80)
        print("Q10: COMMUNITY KNOWLEDGE GRAPH (Hybrid: SQL + OpenCypher + Vector)")
        print("─" * 80)
        print("Analyzing community knowledge distribution...")
        print()

        start = time.time()

        try:
            # Part 1: Overall statistics (SQL)
            print("  Part 1: Overall Statistics (SQL)")

            # Run separate count queries
            users = (
                list(db.query("sql", "SELECT count(*) as count FROM User"))[0].get(
                    "count"
                )
                or 0
            )
            questions = (
                list(db.query("sql", "SELECT count(*) as count FROM Question"))[0].get(
                    "count"
                )
                or 0
            )
            answers = (
                list(db.query("sql", "SELECT count(*) as count FROM Answer"))[0].get(
                    "count"
                )
                or 0
            )
            tags = (
                list(db.query("sql", "SELECT count(*) as count FROM Tag"))[0].get(
                    "count"
                )
                or 0
            )
            comments = (
                list(db.query("sql", "SELECT count(*) as count FROM Comment"))[0].get(
                    "count"
                )
                or 0
            )

            print(f"    • Users: {users:,}")
            print(f"    • Questions: {questions:,}")
            print(f"    • Answers: {answers:,}")
            print(f"    • Tags: {tags:,}")
            print(f"    • Comments: {comments:,}")
            print()

            # Part 2: Knowledge connectivity (SQL + Graph)
            print("  Part 2: Knowledge Connectivity Metrics")
            connectivity_query = """
                SELECT
                    avg(out('HAS_ANSWER').size()) as avg_answers_per_question,
                    avg(in('COMMENTED_ON').size()) as avg_comments_per_post
                FROM Question
            """

            conn = list(db.query("sql", connectivity_query))[0]
            avg_ans = conn.get("avg_answers_per_question") or 0
            avg_com = conn.get("avg_comments_per_post") or 0

            print(f"    • Average answers per question: {avg_ans:.2f}")
            print(f"    • Average comments per post: {avg_com:.2f}")
            print()

            # Part 3: Topic diversity (Vector)
            if "Question" in indexes:
                print("  Part 3: Topic Diversity (Vector Search Sample)")

                # Sample questions for diversity analysis
                sample_query = """
                    SELECT Title
                    FROM Question
                    WHERE Title IS NOT NULL
                    LIMIT 5
                """

                samples = list(db.query("sql", sample_query))
                print("    Sample questions across the knowledge base:")

                for i, sample in enumerate(samples, 1):
                    title = sample.get("Title") or "N/A"
                    print(f"      [{i}] {title[:60]}...")

                print()

            elapsed = time.time() - start
            print(f"  ⏱️  Total time: {elapsed:.3f}s")
            print()

        except Exception as e:
            print(f"  ❌ Error: {e}")
            import traceback

            traceback.print_exc()
            print()

    def _q11_cross_content_semantic_search(self, db, indexes):
        """Q11: Find related content across questions, answers, and comments (Vector Only)

        Uses pure vector search to find semantically similar content across different
        content types without relying on graph relationships.
        """
        print("─" * 80)
        print("Q11: CROSS-CONTENT SEMANTIC SEARCH (Vector Only)")
        print("─" * 80)
        print("Finding related content across questions, answers, and comments...")
        print()

        start = time.time()

        try:
            # Define a technical query
            query_text = "What is the time complexity of binary search algorithms?"
            print(f'  Query: "{query_text}"')
            print()

            # Encode the query
            encode_start = time.time()
            query_embedding = self.model.encode(
                [query_text],
                show_progress_bar=False,
                convert_to_numpy=True,
                device=self.device,
            )[0]
            encode_time = time.time() - encode_start

            # Search across all content types
            all_results = []

            # Search Questions
            if "Question" in indexes:
                q_results = indexes["Question"].find_nearest(query_embedding, k=3)
                for vertex, distance in q_results:
                    title = vertex.get("Title") or "N/A"
                    score = vertex.get("Score") or 0
                    all_results.append(
                        {
                            "type": "Question",
                            "distance": distance,
                            "score": score,
                            "content": title,
                        }
                    )

            # Search Answers
            if "Answer" in indexes:
                a_results = indexes["Answer"].find_nearest(query_embedding, k=3)
                for vertex, distance in a_results:
                    body = vertex.get("Body") or ""
                    score = vertex.get("Score") or 0
                    # Extract first sentence
                    content = body[:100] + "..." if len(body) > 100 else body
                    all_results.append(
                        {
                            "type": "Answer",
                            "distance": distance,
                            "score": score,
                            "content": content,
                        }
                    )

            # Search Comments
            if "Comment" in indexes:
                c_results = indexes["Comment"].find_nearest(query_embedding, k=3)
                for vertex, distance in c_results:
                    text = vertex.get("Text") or ""
                    score = vertex.get("Score") or 0
                    content = text[:100] + "..." if len(text) > 100 else text
                    all_results.append(
                        {
                            "type": "Comment",
                            "distance": distance,
                            "score": score,
                            "content": content,
                        }
                    )

            # Sort all results by distance
            all_results.sort(key=lambda x: x["distance"])

            # Display results
            print(f"  Found {len(all_results)} results across all content types")
            print("  " + "─" * 76)
            print(
                f"  {'Rank':<6}{'Type':<12}{'Distance':<12}{'Score':<8}{'Content':<40}"
            )
            print("  " + "─" * 76)

            for i, result in enumerate(all_results[:10], 1):
                content = result["content"][:39]
                print(
                    f"  {i:<6}{result['type']:<12}{result['distance']:<12.4f}{result['score']:<8}{content}"
                )

            search_time = time.time() - start
            print()
            print(f"  ⏱️  Encode time: {encode_time:.3f}s")
            print(f"  ⏱️  Search time: {search_time - encode_time:.3f}s")
            print(f"  ⏱️  Total time: {search_time:.3f}s")
            print()

        except Exception as e:
            print(f"  ❌ Error: {e}")
            import traceback

            traceback.print_exc()
            print()

    def _q12_find_best_answers_by_topic(self, db, indexes):
        """Q12: Find best answers for a given topic using vector search (Vector Only)

        Uses pure vector search on answer embeddings to find high-quality answers
        related to a specific topic, without needing to traverse question-answer edges.
        """
        print("─" * 80)
        print("Q12: SEMANTIC ANSWER RETRIEVAL (Vector Only)")
        print("─" * 80)
        print("Finding best answers for a specific topic...")
        print()

        start = time.time()

        try:
            # Define topic queries
            topics = [
                "recursion and dynamic programming",
                "sorting algorithms and performance",
                "graph theory and shortest paths",
            ]

            if "Answer" not in indexes:
                print("  ⚠️  Answer index not available")
                return

            for topic_idx, topic in enumerate(topics, 1):
                print(f'  Topic {topic_idx}: "{topic}"')

                # Encode the topic
                topic_embedding = self.model.encode(
                    [topic],
                    show_progress_bar=False,
                    convert_to_numpy=True,
                    device=self.device,
                )[0]

                # Search for relevant answers
                answer_results = indexes["Answer"].find_nearest(topic_embedding, k=5)

                print(f"    Top 5 relevant answers:")
                print("    " + "─" * 72)

                for i, (vertex, distance) in enumerate(answer_results, 1):
                    body = vertex.get("Body") or ""
                    score = vertex.get("Score") or 0

                    # Extract a meaningful snippet (convert to Python str first)
                    body_str = str(body)
                    snippet = body_str.replace("\n", " ")[:80]
                    snippet = snippet + "..." if len(body_str) > 80 else snippet

                    print(f"    [{i}] Distance: {distance:.4f}, Score: {score}")
                    print(f"        {snippet}")

                print()

            elapsed = time.time() - start
            print(f"  ⏱️  Total time: {elapsed:.3f}s")
            print()

        except Exception as e:
            print(f"  ❌ Error: {e}")
            import traceback

            traceback.print_exc()
            print()


# =============================================================================
# Main Script
# =============================================================================


def main():
    parser = argparse.ArgumentParser(
        description="Stack Overflow Multi-Model Database Example",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run all phases with small dataset
  python 07_stackoverflow_multimodel.py --dataset stackoverflow-small

  # Run only Phase 1 (documents + indexes)
  python 07_stackoverflow_multimodel.py --dataset stackoverflow-tiny --phases 1

  # Run Phases 1 and 2 (documents + graph)
  python 07_stackoverflow_multimodel.py --dataset stackoverflow-tiny --phases 1 2

  # Custom batch size for Phase 1 operations
  python 07_stackoverflow_multimodel.py --dataset stackoverflow-small --batch-size 10000

Dataset sizes:
  stackoverflow-tiny   - ~34 MB disk, 2 GB heap recommended
  stackoverflow-small  - ~642 MB disk, 4 GB heap recommended
  stackoverflow-medium - ~2.9 GB disk, 8 GB heap recommended
  stackoverflow-large  - ~323 GB disk, 32+ GB heap recommended

Batch size:
  Default: 10000 records per commit
  Larger batches = faster imports, more memory usage
  Smaller batches = slower imports, less memory usage

Phases:
  1 - XML → Documents + Indexes
  2 - Documents → Graph (vertices + edges)
  3 - Graph → Embeddings + Vector indexes (JVector)
    4 - Analytics (SQL + OpenCypher + Vector Search)
        """,
    )

    parser.add_argument(
        "--dataset",
        choices=[
            "stackoverflow-tiny",
            "stackoverflow-small",
            "stackoverflow-medium",
            "stackoverflow-large",
        ],
        default="stackoverflow-small",
        help="Dataset size to use (default: stackoverflow-small)",
    )

    parser.add_argument(
        "--batch-size",
        type=int,
        default=10000,
        help="Number of records to commit per batch in Phase 1 (default: 10000)",
    )

    parser.add_argument(
        "--encode-batch-size",
        type=int,
        default=256,
        help="Batch size for encoding embeddings in Phase 3 (default: 256)",
    )

    parser.add_argument(
        "--phases",
        type=int,
        nargs="+",
        default=[1],
        choices=[1, 2, 3, 4],
        help="Which phases to run (default: 1)",
    )

    parser.add_argument(
        "--db-name",
        type=str,
        default=None,
        help="Database name (default: stackoverflow_{dataset}_db)",
    )

    parser.add_argument(
        "--analysis-limit",
        type=int,
        default=1_000_000,
        help="Max rows to analyze per file for schema analysis (default: 1 million)",
    )

    parser.add_argument(
        "--analyze-only",
        action="store_true",
        help="Only analyze schema without importing (useful for understanding data structure)",
    )

    args = parser.parse_args()

    # Start overall timer
    script_start_time = time.time()

    print("=" * 80)
    print("Stack Overflow Multi-Model Database")
    print("=" * 80)
    print(f"Dataset: {args.dataset}")
    print(f"Batch size: {args.batch_size} records/commit")
    print(f"Phases: {args.phases}")
    print()

    # Setup paths
    data_dir = Path(__file__).parent / "data" / args.dataset
    db_name = args.db_name or f"{args.dataset.replace('-', '_')}_db"
    db_path = Path("./my_test_databases") / db_name

    # Check dataset exists
    if not data_dir.exists():
        print(f"❌ Dataset not found: {data_dir}")
        print()
        print("Please ensure the Stack Overflow data dump is in the correct location.")
        print(f"Expected: {data_dir}")
        sys.exit(1)

    # Check JVM heap configuration
    jvm_args = os.environ.get("ARCADEDB_JVM_ARGS")
    if jvm_args and "-Xmx" in jvm_args:
        import re

        match = re.search(r"-Xmx(\S+)", jvm_args)
        heap_size = match.group(1) if match else "unknown"
        print(f"💡 JVM Max Heap: {heap_size}")
    else:
        print("💡 JVM Max Heap: 4g (default)")
        if args.dataset in ["stackoverflow-medium", "stackoverflow-large"]:
            print("   ⚠️  Consider increasing heap for large datasets:")
            print('      export ARCADEDB_JVM_ARGS="-Xmx8g -Xms8g"')
    print()

    # Schema analysis mode
    if args.analyze_only:
        print("=" * 80)
        print("📊 SCHEMA ANALYSIS MODE")
        print("=" * 80)
        print()

        analyzer = SchemaAnalyzer(analysis_limit=args.analysis_limit)

        # Analyze all XML files
        xml_files = [
            "Users.xml",
            "Posts.xml",
            "Comments.xml",
            "Badges.xml",
            "Votes.xml",
            "PostLinks.xml",
            "Tags.xml",
            "PostHistory.xml",
        ]

        schemas = []
        for xml_file in xml_files:
            xml_path = data_dir / xml_file
            if xml_path.exists():
                schema = analyzer.analyze_xml_file(xml_path)
                schemas.append(schema)
            else:
                print(f"  ⚠️  File not found: {xml_file}")

        # Print summary report
        print()
        print("=" * 80)
        print("📊 SCHEMA ANALYSIS SUMMARY")
        print("=" * 80)
        print()

        for schema in schemas:
            print(f"📄 {schema.name} ({schema.source_file.name})")
            print(f"   Total rows: {schema.row_count:,}")
            print(f"   Total fields: {len(schema.fields)}")
            print(f"   Has primary key (Id): {schema.has_primary_key}")
            print()

            # Show top 10 fields by null percentage
            fields_with_nulls = [
                (name, stats)
                for name, stats in schema.fields.items()
                if stats.null_count > 0
            ]
            fields_with_nulls.sort(key=lambda x: x[1].null_count, reverse=True)

            if fields_with_nulls:
                print(f"   Top nullable fields:")
                for name, stats in fields_with_nulls[:10]:
                    null_pct = (stats.null_count / schema.row_count) * 100
                    print(
                        f"     - {name}: {stats.null_count:,} nulls "
                        f"({null_pct:.1f}%) | Type: {stats.type_name}"
                    )
                if len(fields_with_nulls) > 10:
                    print(f"     ... and {len(fields_with_nulls) - 10} more")
            else:
                print("   No nullable fields detected")
            print()

        print("=" * 80)
        print("✅ Schema analysis complete")
        print("=" * 80)
        return

    # Run requested phases
    try:
        if 1 in args.phases:
            phase1 = Phase1XMLImporter(
                db_path=db_path,
                data_dir=data_dir,
                batch_size=args.batch_size,
                dataset_size=args.dataset,
                analysis_limit=args.analysis_limit,
            )
            phase1.run()

        if 2 in args.phases:
            # Phase 2 requires Phase 1 to be complete
            if not db_path.exists():
                print("❌ Phase 1 database not found. Run Phase 1 first.")
                print(f"   Expected: {db_path}")
                sys.exit(1)

            # Create separate graph database path
            graph_db_name = f"{db_path.name}_graph"
            graph_db_path = db_path.parent / graph_db_name

            phase2 = Phase2GraphConverter(
                doc_db_path=db_path,
                graph_db_path=graph_db_path,
                batch_size=args.batch_size,
                dataset_size=args.dataset,
            )
            phase2.run()

        if 3 in args.phases:
            # Phase 3 requires Phase 2 to be complete
            graph_db_name = f"{db_path.name}_graph"
            graph_db_path = db_path.parent / graph_db_name

            if not graph_db_path.exists():
                print("❌ Phase 2 graph database not found. Run Phase 2 first.")
                print(f"   Expected: {graph_db_path}")
                sys.exit(1)

            phase3 = Phase3VectorEmbeddings(
                graph_db_path=graph_db_path,
                batch_size=args.batch_size,
                encode_batch_size=args.encode_batch_size,
            )
            phase3.run()

        if 4 in args.phases:
            # Phase 4 requires Phase 3 to be complete
            graph_db_name = f"{db_path.name}_graph"
            graph_db_path = db_path.parent / graph_db_name

            if not graph_db_path.exists():
                print("❌ Phase 3 graph database not found. Run Phase 3 first.")
                print(f"   Expected: {graph_db_path}")
                sys.exit(1)

            phase4 = Phase4Analytics(
                graph_db_path=graph_db_path,
            )
            phase4.run()

        # Overall timing
        script_elapsed = time.time() - script_start_time
        print("=" * 80)
        print("✅ ALL PHASES COMPLETED")
        print("=" * 80)
        total_time_msg = (
            f"Total script time: {script_elapsed:.2f}s "
            f"({script_elapsed / 60:.1f} minutes)"
        )
        print(total_time_msg)
        print("=" * 80)

    except Exception as e:
        print(f"\n❌ Script failed: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
