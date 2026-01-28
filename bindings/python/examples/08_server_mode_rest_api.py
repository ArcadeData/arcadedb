"""
ArcadeDB Python Example 08: Server Mode, Studio & Concurrent HTTP Clients
=========================================================================

This example demonstrates ArcadeDB's dual nature:
1.  **Embedded Server**: Starts a full ArcadeDB server from Python.
    *(Note: While this library is primarily for embedded use, it can also act as a server host)*
2.  **Interactive Studio**: Provides a web-based UI (http://localhost:2480) for exploration.
3.  **HTTP API**: Allows external clients (like Python's `requests`) to interact with the DB.
4.  **Concurrency**: Shows how the server handles parallel requests (SQL & OpenCypher) from multiple clients.

Prerequisites:
    Run example 07 first to generate the 'stackoverflow_small_db_graph' dataset.
"""

import concurrent.futures
import os
import time

import arcadedb_embedded as arcadedb
import requests
from requests.auth import HTTPBasicAuth

# Configuration
ROOT_PATH = "./my_test_databases"
ROOT_PASSWORD = "playwithdata"
HTTP_PORT = 2480
HTTP_URL = f"http://localhost:{HTTP_PORT}"

# Define a diverse set of queries to simulate a real-world workload
# (Language, Description, Query)
WORKLOAD_QUERIES = [
    # === Vertex Count Queries (SQL) ===
    ("sql", "Count User vertices", "SELECT count(*) as count FROM User"),
    ("sql", "Count Question vertices", "SELECT count(*) as count FROM Question"),
    ("sql", "Count Answer vertices", "SELECT count(*) as count FROM Answer"),
    ("sql", "Count Tag vertices", "SELECT count(*) as count FROM Tag"),
    ("sql", "Count Badge vertices", "SELECT count(*) as count FROM Badge"),
    ("sql", "Count Comment vertices", "SELECT count(*) as count FROM Comment"),
    # === Edge Count Queries (SQL) ===
    ("sql", "Count ASKED edges", "SELECT count(*) as count FROM ASKED"),
    ("sql", "Count ANSWERED edges", "SELECT count(*) as count FROM ANSWERED"),
    ("sql", "Count HAS_ANSWER edges", "SELECT count(*) as count FROM HAS_ANSWER"),
    (
        "sql",
        "Count ACCEPTED_ANSWER edges",
        "SELECT count(*) as count FROM ACCEPTED_ANSWER",
    ),
    ("sql", "Count TAGGED_WITH edges", "SELECT count(*) as count FROM TAGGED_WITH"),
    ("sql", "Count COMMENTED_ON edges", "SELECT count(*) as count FROM COMMENTED_ON"),
    ("sql", "Count EARNED edges", "SELECT count(*) as count FROM EARNED"),
    ("sql", "Count LINKED_TO edges", "SELECT count(*) as count FROM LINKED_TO"),
    # === User Activity Queries ===
    (
        "sql",
        "Find user with most questions asked",
        "SELECT DisplayName, out('ASKED').size() as question_count FROM User WHERE out('ASKED').size() > 0 ORDER BY question_count DESC LIMIT 1",
    ),
    (
        "sql",
        "Find user with most answers",
        "SELECT DisplayName, out('ANSWERED').size() as answer_count FROM User WHERE out('ANSWERED').size() > 0 ORDER BY answer_count DESC LIMIT 1",
    ),
    (
        "sql",
        "Find user with most badges earned",
        "SELECT DisplayName, out('EARNED').size() as badge_count FROM User WHERE out('EARNED').size() > 0 ORDER BY badge_count DESC LIMIT 1",
    ),
    # === Question-Answer Relationship Queries ===
    (
        "sql",
        "Find question with most answers",
        "SELECT Id, out('HAS_ANSWER').size() as answer_count FROM Question ORDER BY answer_count DESC LIMIT 1",
    ),
    (
        "sql",
        "Count questions with accepted answers",
        "SELECT count(*) as count FROM Question WHERE out('ACCEPTED_ANSWER').size() > 0",
    ),
    (
        "sql",
        "Verify answers have parent questions",
        "SELECT count(*) as orphan_count FROM Answer WHERE in('HAS_ANSWER').size() = 0",
    ),
    # === Tag Queries ===
    (
        "sql",
        "Find most popular tag",
        "SELECT TagName, in('TAGGED_WITH').size() as usage_count FROM Tag ORDER BY usage_count DESC LIMIT 1",
    ),
    (
        "sql",
        "Count questions per tag (top 5)",
        "SELECT TagName, in('TAGGED_WITH').size() as question_count FROM Tag WHERE in('TAGGED_WITH').size() > 0 ORDER BY question_count DESC LIMIT 5",
    ),
    # === Comment Queries ===
    (
        "sql",
        "Verify all comments link to posts",
        "SELECT count(*) as linked_count, (SELECT count(*) FROM Comment) as total_count FROM Comment WHERE out('COMMENTED_ON').size() > 0",
    ),
    (
        "sql",
        "Find question with most comments",
        "SELECT Id, in('COMMENTED_ON').size() as comment_count FROM Question WHERE in('COMMENTED_ON').size() > 0 ORDER BY comment_count DESC LIMIT 1",
    ),
    (
        "sql",
        "Find answer with most comments",
        "SELECT Id, in('COMMENTED_ON').size() as comment_count FROM Answer WHERE in('COMMENTED_ON').size() > 0 ORDER BY comment_count DESC LIMIT 1",
    ),
    # === Edge Property Queries ===
    (
        "sql",
        "Verify ASKED edges have CreationDate",
        "SELECT count(*) as with_date, (SELECT count(*) FROM ASKED) as total FROM ASKED WHERE CreationDate IS NOT NULL",
    ),
    (
        "sql",
        "Verify ANSWERED edges have CreationDate",
        "SELECT count(*) as with_date, (SELECT count(*) FROM ANSWERED) as total FROM ANSWERED WHERE CreationDate IS NOT NULL",
    ),
    (
        "sql",
        "Verify EARNED edges have Date and Class",
        "SELECT count(*) as complete_count FROM EARNED WHERE Date IS NOT NULL AND Class IS NOT NULL",
    ),
    (
        "sql",
        "Verify LINKED_TO edges have LinkTypeId",
        "SELECT count(*) as with_type, (SELECT count(*) FROM LINKED_TO) as total FROM LINKED_TO WHERE LinkTypeId IS NOT NULL",
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
    ),
    (
        "opencypher",
        "Find 2-hop user connections",
        """
        MATCH (u:User)
        WITH u LIMIT 10
        MATCH (u)-[:ASKED]->(:Question)-[:HAS_ANSWER]->(:Answer)<-[:ANSWERED]-(other:User)
        RETURN count(DISTINCT other) as count
        """,
    ),
    # === Complex Pattern Queries (OpenCypher) ===
    (
        "opencypher",
        "Find questions with tags, answers, and comments",
        """
        MATCH (q:Question)-[:TAGGED_WITH]->(:Tag)
        WITH DISTINCT q LIMIT 200
        MATCH (q)-[:HAS_ANSWER]->(:Answer)
        WITH DISTINCT q LIMIT 200
        MATCH (q)<-[:COMMENTED_ON]-(:Comment)
        RETURN count(DISTINCT q) as count
        """,
    ),
    (
        "opencypher",
        "Find users with badges who also asked questions",
        """
        MATCH (u:User)-[:EARNED]->(:Badge)
        WITH DISTINCT u LIMIT 500
        MATCH (u)-[:ASKED]->(:Question)
        RETURN count(DISTINCT u) as count
        """,
    ),
]


def print_header(title):
    print(f"\n{'=' * 80}")
    print(f" {title}")
    print(f"{'=' * 80}")


def check_database_exists(path, name):
    db_path = os.path.join(path, name)
    return os.path.exists(db_path)


def run_client_query(client_id, db_name, query_def):
    """Executes a query via HTTP API simulating a remote client."""
    language, name, query = query_def
    url = f"{HTTP_URL}/api/v1/command/{db_name}"

    payload = {"language": language, "command": query}

    start_time = time.time()
    try:
        response = requests.post(
            url,
            json=payload,
            auth=HTTPBasicAuth("root", ROOT_PASSWORD),
            timeout=30,  # Some graph queries might take a moment
        )
        duration = time.time() - start_time

        if response.status_code == 200:
            result = response.json()
            records = result.get("result", [])

            # Extract a meaningful result summary
            summary = f"{len(records)} rows"
            if records:
                first = records[0]
                if isinstance(first, dict):
                    if "count" in first:
                        summary = f"Count={first['count']}"
                    elif "cnt" in first:
                        summary = f"Count={first['cnt']}"
                elif isinstance(first, int):
                    summary = f"Result={first}"

            return (
                f"Client {client_id} [{name}]: Success ({summary}) in {duration:.3f}s"
            )
        else:
            return f"Client {client_id} [{name}]: Failed ({response.status_code}) - {response.text}"

    except Exception as e:
        return f"Client {client_id} [{name}]: Error - {str(e)}"


def demonstrate_concurrency(db_name, num_clients=6):
    print_header("Demonstrating Concurrent HTTP Clients (Mixed Workload)")
    print(
        f"Simulating {num_clients} concurrent clients executing {len(WORKLOAD_QUERIES)} SQL and OpenCypher queries..."
    )

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_clients) as executor:
        futures = []
        for i, query_def in enumerate(WORKLOAD_QUERIES):
            # Distribute queries among the simulated clients
            client_id = (i % num_clients) + 1
            futures.append(
                executor.submit(run_client_query, client_id, db_name, query_def)
            )

        for future in concurrent.futures.as_completed(futures):
            print(future.result())


def main():
    db_name = "stackoverflow_small_db_graph"
    abs_root_path = os.path.abspath(ROOT_PATH)

    print_header(f"Example 08: Server Mode (Database: {db_name})")

    # 1. Validation
    if not os.path.exists(abs_root_path):
        os.makedirs(abs_root_path)

    if not check_database_exists(abs_root_path, db_name):
        print(f"❌ Database '{db_name}' not found in {abs_root_path}")
        print("   Please run Example 07 first to generate the dataset.")
        return

    # 2. Start Server
    print("\nStarting ArcadeDB Server...")
    server = arcadedb.create_server(
        root_path=abs_root_path,
        root_password=ROOT_PASSWORD,
        config={
            "http_port": HTTP_PORT,
            "host": "0.0.0.0",
            "server_databaseDirectory": abs_root_path,
        },
    )

    try:
        server.start()
        print(f"✅ Server running on port {HTTP_PORT}")

        # Wait a moment for HTTP listener to be fully active
        time.sleep(2)

        # 3. Run Concurrency Demo
        demonstrate_concurrency(db_name)

        # 4. Interactive Mode
        print_header("Interactive Studio Session")
        print(f"The server is running. Open your browser to:\n")
        print(f"  👉 {server.get_studio_url()}  (or http://localhost:{HTTP_PORT})")
        print(f"\nCredentials:")
        print(f"  Database: {db_name}")
        print(f"  User:     root")
        print(f"  Password: {ROOT_PASSWORD}")

        print("\nFor more information, visit: https://docs.arcadedb.com/")

        print("\n[Press Ctrl+C to stop the server]")

        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        print("\n\nStopping server...")
    except Exception as e:
        print(f"\nError: {e}")
    finally:
        if server.is_started():
            server.stop()
            print("Server stopped.")


if __name__ == "__main__":
    main()
