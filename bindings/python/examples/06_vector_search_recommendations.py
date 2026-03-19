#!/usr/bin/env python3
"""
Example 06: Vector Search Movie Recommendations

Demonstrates vector embeddings and HNSW (JVector) indexing for semantic movie search.
Compares traditional graph queries with vector similarity search.

PERFORMANCE OPTIMIZATION
-------------------------
Graph-Based Collaborative Filtering Performance:
• Full mode: Comprehensive but slow (24-39s per query on large dataset)
  - Analyzes all users who rated the query movie highly
  - Processes 100K+ intermediate results from graph traversal fanout
  - Best for offline batch recommendations

• Fast mode: Sampled with 150-300x speedup (0.1-0.2s per query)
  - Limits intermediate results to 25K (approximately 50 users' worth)
  - Uses nested SELECT with LIMIT before GROUP BY aggregation
  - Still produces high-quality recommendations
  - Best for real-time recommendations

For the large dataset (20M ratings), use:
    --heap-size 8g

KNOWN ISSUES: ArcadeDB Bugs and Limitations
--------------------------------------------

1. **NOTUNIQUE Index Breaks String Equality (ArcadeDB Bug)**:
   Creating a NOTUNIQUE index on a string property AFTER loading a large
   dataset (86K+ records) causes the = operator to return 0 results for
   exact string matches, while LIKE continues to work correctly.

   Example:
   - Before index: WHERE title = 'Toy Story (1995)' → 1 result ✓
   - After index:  WHERE title = 'Toy Story (1995)' → 0 results ✗
   - After index:  WHERE title LIKE 'Toy Story (1995)' → 1 result ✓

   WORKAROUND: This script does NOT create a NOTUNIQUE index on Movie.title.
   We use the = operator for exact title matching (works without index) and
   rely on the Movie[movieId] UNIQUE index for fast MATCH traversals.

2. **FULL_TEXT Index Wrong Semantics**:
   FULL_TEXT index changes the = operator to perform tokenized word search
   instead of exact matching, returning semantically incorrect results.

   Example with FULL_TEXT index:
   - WHERE title = 'Toy Story (1995)' → 1,686 results (any movie with
     "Toy", "Story", or "1995" in title) ✗

   CONCLUSION: FULL_TEXT is NOT suitable for exact title matching.

3. **JSONL Export/Import Broken for Vectors**: Float arrays are NOT properly
   preserved during JSONL export/import:
   - Embeddings exported as Java toString() strings: "[F@113ee1ce"
   - Original vector data (384 floats) is completely lost

   IMPACT: Cannot backup/restore vector databases using JSONL format.
   After import, embeddings must be regenerated and indexes rebuilt.

Features:
- Real embeddings using sentence-transformers (two models for comparison)
- HNSW (JVector) indexing for fast similarity search
- Compare graph-based vs vector-based recommendations (4 methods)
- Performance timing and optimization strategies

Dataset: MovieLens small (9,742 movies from Example 05)

Usage:
    # Import from JSONL export (recommended)
    python 06_vector_search_recommendations.py \\
        --import-jsonl ./exports/movielens_graph_small_db.jsonl.tgz

    # Use existing database
    python 06_vector_search_recommendations.py \\
        --source-db my_test_databases/movielens_graph_small_db
"""

import argparse
import os
import shutil
import sys
import time
from pathlib import Path

import arcadedb_embedded as arcadedb
import numpy as np

# Try to import sentence_transformers
try:
    from sentence_transformers import SentenceTransformer

    HAS_TRANSFORMERS = True
except ImportError:
    HAS_TRANSFORMERS = False


def check_dependencies():
    """Check if required dependencies are installed."""
    if not HAS_TRANSFORMERS:
        print("ERROR: sentence-transformers not installed.")
        print("Please install it to run this example:")
        print("  uv pip install sentence-transformers")
        sys.exit(1)


def import_from_jsonl(jsonl_path, db_path, jvm_kwargs=None):
    """Import database from JSONL export."""
    start_time = time.time()

    # Create new database
    with arcadedb.create_database(str(db_path), jvm_kwargs=jvm_kwargs) as db:
        # Import using SQL IMPORT DATABASE command
        abs_path = Path(jsonl_path).resolve()
        # Convert Windows backslashes to forward slashes for SQL URI
        import_path = str(abs_path).replace("\\", "/")
        print(f"Importing from {import_path}...")
        db.command("sql", f"IMPORT DATABASE file://{import_path}")

    return time.time() - start_time


def load_embedding_model(model_name):
    """Load sentence-transformer model."""
    print(f"Loading model: {model_name}...")
    start_time = time.time()
    model = SentenceTransformer(model_name)
    print(f"✓ Model loaded in {time.time() - start_time:.2f}s")
    return model


def generate_embeddings(
    db, model, model_name, property_suffix="", limit=None, force_embed=False
):
    """Generate embeddings for movies and store them.

    Args:
        db: Database instance
        model: Loaded SentenceTransformer model
        model_name: Name of the model (for logging)
        property_suffix: Suffix for property names (e.g., "_v1", "_v2")
        limit: Max movies to process (for testing)

    Returns:
        Number of movies embedded
    """
    embedding_prop = f"embedding{property_suffix}"
    vector_id_prop = f"vector_id{property_suffix}"

    # Check if embeddings already exist
    try:
        count = db.count_type("Movie")
        # Check if property exists and has values
        query = (
            f"SELECT count(*) as count FROM Movie WHERE {embedding_prop} IS NOT NULL"
        )
        result = list(db.query("sql", query))
        existing_embeddings = result[0].get("count")

        if existing_embeddings > 0 and not force_embed:
            print(f"Found {existing_embeddings} existing embeddings for {model_name}")
            return existing_embeddings
    except Exception:
        pass

    print(f"Generating embeddings using {model_name}...")

    # Create properties if they don't exist
    db.schema.get_or_create_property("Movie", embedding_prop, "ARRAY_OF_FLOATS")
    db.schema.get_or_create_property("Movie", vector_id_prop, "STRING")

    # Fetch movies
    query = "SELECT FROM Movie"
    if limit:
        query += f" LIMIT {limit}"

    movies = list(db.query("sql", query))
    total = len(movies)
    print(f"Processing {total} movies...")

    # Prepare text for embedding
    # Format: "Title (Year): Genres"
    texts = []
    for movie in movies:
        title = movie.get("title")
        genres = movie.get("genres")
        text = f"{title}: {genres}"
        texts.append(text)

    # Generate embeddings in batches
    start_encode = time.time()
    embeddings = model.encode(
        texts,
        batch_size=32,
        show_progress_bar=True,
        convert_to_numpy=True,
        normalize_embeddings=True,
    )

    elapsed_encode = time.time() - start_encode
    rate = total / elapsed_encode
    print(
        f"✓ Encoded {total:,} movies in {elapsed_encode:.1f}s "
        f"({rate:.0f} movies/sec)"
    )

    # Store embeddings using Java API
    print(f"Storing embeddings ({embedding_prop}, {vector_id_prop})...")
    start_store = time.time()

    with db.transaction():
        # Use the same movies list we generated embeddings for
        for movie_result, embedding in zip(movies, embeddings):
            # Convert Result to Vertex
            movie = movie_result.get_vertex()
            if not movie:
                continue

            # Get mutable version of vertex
            mutable_vertex = movie.modify()

            # Set embedding property with custom name
            java_embedding = arcadedb.to_java_float_array(embedding)
            mutable_vertex.set(embedding_prop, java_embedding)
            # Create vector_id property
            movie_id = str(movie_result.get("movieId"))
            mutable_vertex.set(vector_id_prop, movie_id)
            mutable_vertex.save()

    elapsed_store = time.time() - start_store
    print(f"✓ Stored {total:,} embeddings in {elapsed_store:.1f}s")

    return total


def create_vector_index(db, property_suffix=""):
    """Create vector index on Movie embeddings and populate it.

    Args:
        db: Database instance
        property_suffix: Suffix for property names (e.g., "_v1", "_v2")

    Returns:
        VectorIndex object
    """
    embedding_prop = f"embedding{property_suffix}"

    # Count movies with embeddings to determine max_items
    query = f"SELECT FROM Movie WHERE {embedding_prop} IS NOT NULL"
    result_list = list(db.query("sql", query))
    num_movies = len(result_list)

    print(f"\nCreating HNSW (JVector) index for {embedding_prop}...")
    print("  metric=cosine, max_connections=32, beam_width=256")

    start_time = time.time()

    # Using new defaults: max_connections=32, beam_width=256
    # New options available:
    # - quantization="INT8" or "BINARY"
    # - store_vectors_in_graph=True
    index = db.create_vector_index(
        vertex_type="Movie",
        vector_property=embedding_prop,
        dimensions=384,
        distance_function="cosine",
    )

    elapsed = time.time() - start_time
    print(f"✓ Created and indexed {num_movies:,} movies in {elapsed:.1f}s")

    return index


def graph_based_recommendations(db, movie_title, limit=5, mode="full"):
    """Graph-based collaborative filtering.

    Uses graph traversal to find movies rated highly by users who rated
    the query movie highly.

    Args:
        db: Database instance
        movie_title: Title of query movie
        limit: Number of results to return
        mode: "full" for comprehensive (slow), "fast" for sampled (fast)

    Returns:
        Query time in seconds
    """
    # Find the movie by title, get movieId for fast MATCH (uses Movie[movieId] index)
    movies = list(
        db.query(
            "sql",
            "SELECT movieId FROM Movie WHERE title = :title",
            {"title": movie_title},
        )
    )

    if not movies:
        print(f"Movie not found: {movie_title}")
        return 0.0

    # Get movieId for indexed lookups
    query_movie_id = movies[0].get("movieId")

    if mode == "fast":
        # Fast mode: Limit intermediate results for 100-300x speedup
        # Sample ~50 users worth of data (50 * 500 ratings = 25,000 results)
        query = """
        SELECT FROM (
            SELECT other_movie.title as title,
                   AVG(rating.rating) as avg_rating,
                   COUNT(*) as rating_count
            FROM (
                SELECT * FROM (
                    MATCH {type: Movie, where: (movieId = :movieId), as: query_movie}
                          .inE('RATED'){where: (rating >= 4.0), as: user_rating}
                          .outV(){as: user}
                          .outE('RATED'){where: (rating >= 4.0), as: rating}
                          .inV(){where: (movieId <> :movieId), as: other_movie}
                    RETURN other_movie, rating
                )
                LIMIT 25000
            )
            GROUP BY other_movie.title
        )
        WHERE rating_count >= 5
        ORDER BY avg_rating DESC, rating_count DESC
        LIMIT :limit
        """
    else:
        # Full mode: Analyze ALL users (slow but comprehensive)
        query = """
        SELECT FROM (
            SELECT other_movie.title as title,
                   AVG(rating.rating) as avg_rating,
                   COUNT(*) as rating_count
            FROM (
                MATCH {type: Movie, where: (movieId = :movieId), as: query_movie}
                      .inE('RATED'){where: (rating >= 4.0), as: user_rating}
                      .outV(){as: user}
                      .outE('RATED'){where: (rating >= 4.0), as: rating}
                      .inV(){where: (movieId <> :movieId), as: other_movie}
                RETURN other_movie, rating
            )
            GROUP BY other_movie.title
        )
        WHERE rating_count >= 5
        ORDER BY avg_rating DESC, rating_count DESC
        LIMIT :limit
        """

    start_time = time.time()
    results = db.query("sql", query, {"movieId": query_movie_id, "limit": limit})
    elapsed = time.time() - start_time

    print(f"   Results ({mode} mode):")
    for i, row in enumerate(results, 1):
        print(
            f"   {i}. {row.get('title')} "
            f"(Rating: {row.get('avg_rating'):.1f}, "
            f"Votes: {row.get('rating_count')})"
        )

    return elapsed


def vector_based_recommendations(
    db, index, model, movie_title, property_suffix="", limit=5
):
    """Vector-based semantic similarity search.

    Args:
        db: Database instance
        index: VectorIndex object
        model: SentenceTransformer model
        movie_title: Title of query movie
        property_suffix: Suffix for property names
        limit: Number of results to return

    Returns:
        Query time in seconds
    """
    # Find the movie to get its genres
    movies = list(
        db.query(
            "sql",
            "SELECT title, genres FROM Movie WHERE title = :title",
            {"title": movie_title},
        )
    )

    if not movies:
        print(f"Movie not found: {movie_title}")
        return 0.0

    movie = movies[0]
    title = movie.get("title")
    genres = movie.get("genres")
    text = f"{title}: {genres}"

    # Generate embedding for query
    query_embedding = model.encode(
        text, convert_to_numpy=True, normalize_embeddings=True
    )

    start_time = time.time()
    # Search index
    results = index.find_nearest(query_embedding, k=limit + 1)  # +1 to exclude self
    elapsed = time.time() - start_time

    print("   Results:")
    count = 0
    for vertex, score in results:
        res_title = vertex.get("title")
        if res_title == movie_title:
            continue  # Skip self

        count += 1
        if count > limit:
            break

        print(f"   {count}. {res_title} (Score: {score:.4f})")

    return elapsed


def main():
    parser = argparse.ArgumentParser(description="Vector Search Movie Recommendations")

    parser.add_argument(
        "--db-path",
        default="./my_test_databases/movielens_vector_db",
        help="Path to working database",
    )

    parser.add_argument(
        "--import-jsonl",
        help=(
            "Import graph database from JSONL file "
            "(e.g., ./exports/movielens_graph_small_db.jsonl.tgz)"
        ),
    )

    parser.add_argument(
        "--source-db",
        help=(
            "Copy from existing graph database "
            "(e.g., my_test_databases/movielens_graph_small_db)"
        ),
    )

    parser.add_argument(
        "--force-embed",
        action="store_true",
        required=False,
        help="Force re-generation of embeddings",
    )

    parser.add_argument(
        "--limit",
        type=int,
        required=False,
        help="Limit number of movies to process (for debugging)",
    )
    parser.add_argument(
        "--heap-size",
        type=str,
        default=None,
        help="Set JVM max heap size (e.g. 8g, 4096m). Overrides default 4g.",
    )

    args = parser.parse_args()

    jvm_kwargs = {"heap_size": args.heap_size} if args.heap_size else {}

    # Track overall timing
    script_start_time = time.time()

    print("=" * 80)
    print("Example 06: Vector Search Movie Recommendations".center(80))
    print("=" * 80)

    # Check dependencies
    print("\nChecking dependencies...")
    check_dependencies()

    # Setup: Always start fresh - either from JSONL or by copying from source
    work_db = Path(args.db_path)

    print("\nSetting up working database...")

    if args.import_jsonl:
        # Option 1: Import from JSONL
        jsonl_path = Path(args.import_jsonl)
        if not jsonl_path.exists():
            print(f"ERROR: JSONL file not found: {jsonl_path}")
            sys.exit(1)

        # Remove old working database
        if work_db.exists():
            print(f"  Removing old: {work_db}")
            shutil.rmtree(work_db)

        # Import from JSONL
        import_time = import_from_jsonl(jsonl_path, work_db, jvm_kwargs=jvm_kwargs)
        print(f"  ✓ Working database ready: {work_db}")
        print(f"  ⏱️  Import time: {import_time:.2f}s")
    elif args.source_db:
        # Option 2: Copy from source database
        source_db = Path(args.source_db)

        if not source_db.exists():
            print(f"ERROR: Source database not found: {source_db}")
            print("Run Example 05 first to create the graph database")
            print("Or use --import-jsonl to import from JSONL export")
            sys.exit(1)

        # Remove old working copy and create fresh one
        if work_db.exists():
            print(f"  Removing old: {work_db}")
            shutil.rmtree(work_db)

        print(f"  Copying fresh database from: {source_db}")
        shutil.copytree(source_db, work_db)
        print(f"  ✓ Working database ready: {work_db}")
    else:
        # Option 3: Use existing database (if it exists)
        if not work_db.exists():
            print(f"ERROR: Database not found at {work_db}")
            print("Please provide --import-jsonl or --source-db to create it.")
            sys.exit(1)
        print(f"  Using existing database: {work_db}")

    # Load database
    print(f"\nOpening database: {args.db_path}")

    with arcadedb.open_database(args.db_path, jvm_kwargs=jvm_kwargs) as db:
        # Build vector indexes for 2 models
        print("\n" + "=" * 80)
        print("BUILDING VECTOR INDEXES")
        print("=" * 80)

        # Model 1: all-MiniLM-L6-v2
        model_1_name = "all-MiniLM-L6-v2"
        print(f"\nModel 1: {model_1_name}")
        model_1 = load_embedding_model(model_1_name)
        num_embedded = generate_embeddings(
            db,
            model_1,
            model_1_name,
            "_v1",
            limit=args.limit,
            force_embed=args.force_embed,
        )
        print(f"✓ Embedded {num_embedded:,} movies")
        index_v1 = create_vector_index(db, property_suffix="_v1")

        # Model 2: paraphrase-MiniLM-L6-v2
        model_2_name = "paraphrase-MiniLM-L6-v2"
        print(f"\nModel 2: {model_2_name}")
        model_2 = load_embedding_model(model_2_name)
        num_embedded = generate_embeddings(
            db,
            model_2,
            model_2_name,
            "_v2",
            limit=args.limit,
            force_embed=args.force_embed,
        )
        print(f"✓ Embedded {num_embedded:,} movies")
        index_v2 = create_vector_index(db, property_suffix="_v2")

        # Run searches for 5 diverse movies
        test_movies = [
            "Toy Story (1995)",  # Animation, Family
            "Matrix, The (1999)",  # Sci-Fi, Action
            "Pulp Fiction (1994)",  # Crime, Drama
            "Forrest Gump (1994)",  # Drama, Romance
            "Jurassic Park (1993)",  # Adventure, Sci-Fi
        ]

        print("\n" + "=" * 80)
        print("COMPARING SEARCH METHODS")
        print("=" * 80)

        for movie_title in test_movies:
            print(f"\n{'='*80}")
            print(f"Query: {movie_title}")
            print("=" * 80)

            # Method 1: Graph-based Full (comprehensive but slow)
            print("\n1. Graph-Based Full (collaborative filtering - comprehensive):")
            graph_full_time = graph_based_recommendations(
                db, movie_title, limit=5, mode="full"
            )
            print(f"   ⏱️  {graph_full_time:.3f}s")

            # Method 2: Graph-based Fast (sampled, 150-300x faster)
            print("\n2. Graph-Based Fast (collaborative filtering - sampled):")
            graph_fast_time = graph_based_recommendations(
                db, movie_title, limit=5, mode="fast"
            )
            print(f"   ⏱️  {graph_fast_time:.3f}s")

            # Method 3: Vector with model 1
            print(f"\n3. Vector ({model_1_name}):")
            vector1_time = vector_based_recommendations(
                db, index_v1, model_1, movie_title, "_v1", limit=5
            )
            print(f"   ⏱️  {vector1_time:.3f}s")

            # Method 4: Vector with model 2
            print(f"\n4. Vector ({model_2_name}):")
            vector2_time = vector_based_recommendations(
                db, index_v2, model_2, movie_title, "_v2", limit=5
            )
            print(f"   ⏱️  {vector2_time:.3f}s")

    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print("• Graph-based Full: Comprehensive collaborative filtering")
    print("  - Analyzes all users who rated the query movie")
    print("  - Most thorough but slow (24-39s per query)")
    print("  - Best for offline batch recommendations")
    print()
    print("• Graph-based Fast: Sampled collaborative filtering")
    print("  - Limits to ~50 users worth of data (25K intermediate results)")
    print("  - 150-300x faster (0.1-0.2s per query)")
    print("  - Still produces high-quality recommendations")
    print("  - Best for real-time recommendations")
    print()
    print("• Vector-based: Semantic similarity (JVector index)")
    print("  - Finds movies with similar plot/genre descriptions")
    print("  - Fast (~0.2s) with no cold start problem")
    print("  - Works for new movies without ratings")
    print("  - Different models capture different similarities")
    print("=" * 80)

    # Overall timing summary
    script_elapsed = time.time() - script_start_time
    print("\n" + "=" * 80)
    print("OVERALL TIMING")
    print("=" * 80)
    minutes = script_elapsed / 60
    print(f"Total script execution time: {script_elapsed:.2f}s " f"({minutes:.1f} min)")
    print("=" * 80)


if __name__ == "__main__":
    main()
