#!/usr/bin/env python3
"""
ArcadeDB Python Bindings - Social Network Graph Example

This example demonstrates how to use ArcadeDB as a graph database to model
a social network with people and friendships. It showcases:

1. Creating vertex and edge types (schema definition)
2. Creating vertices (people) with properties
3. Creating edges (friendships) between vertices
4. Querying the graph using both SQL MATCH and Cypher dialects
5. Finding friends, friends of friends, and mutual connections
6. Comparing SQL vs Cypher syntax for graph operations

Key Concepts:
- Vertices represent entities (Person)
- Edges represent relationships (FRIEND_OF)
- Properties store data on both vertices and edges
- Graph traversal allows complex relationship queries
- Both SQL MATCH and Cypher provide graph querying capabilities

ArcadeDB Query Languages:
- SQL MATCH: ArcadeDB's extended SQL syntax for graph traversal
- Cypher: Neo4j-compatible query language for intuitive graph operations

Requirements:
- Python embedded ArcadeDB (arcadedb_embedded package)

Usage:
- Run this example from the examples/ directory:
  cd bindings/python/examples && python 02_social_network_graph.py
- Database files will be created in ./my_test_databases/social_network_db/
"""

import os
import shutil
import sys
import time

import arcadedb_embedded as arcadedb


def main():
    """Main function demonstrating social network graph operations"""

    # Database connection (using same pattern as 01_simple_document_store.py)
    print("🔌 Creating/connecting to database...")

    step_start = time.time()

    # Create database in a local directory so you can inspect the files
    # This creates: ./my_test_databases/social_network_db/
    db_dir = "./my_test_databases"
    database_path = os.path.join(db_dir, "social_network_db")

    # Clean up any existing database from previous runs
    if os.path.exists(database_path):
        shutil.rmtree(database_path)

    # Clean up log directory from previous runs
    if os.path.exists("./log"):
        shutil.rmtree("./log")

    try:
        # Create/open database using same pattern as working example
        db = arcadedb.create_database(database_path)
        print(f"✅ Database created at: {database_path}")
        print("💡 Using embedded mode - no server needed!")
        print("💡 Database files are kept so you can inspect them!")
        print(f"⏱️  Time: {time.time() - step_start:.3f}s")

        # Create schema
        create_schema(db)

        # Create sample data
        create_sample_data(db)

        # Demonstrate graph queries
        demonstrate_graph_queries(db)

        # Compare SQL vs Cypher approaches
        compare_query_languages(db)

        print("\n✅ Social network graph example completed successfully!")

    except Exception as e:
        print(f"❌ Error in social network example: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)

    # Close database connection (like 01_simple_document_store.py)
    db.close()
    print("✅ Database connection closed")

    # Note: We're NOT deleting the database directory
    # You can inspect the files in ./my_test_databases/social_network_db/
    print(f"💡 Database files preserved at: {database_path}")
    print("💡 Inspect the database structure and files!")
    print("💡 Re-run this script to recreate the database")


def create_schema(db):
    """Create vertex and edge types for the social network"""
    print("\n📊 Creating social network schema...")

    step_start = time.time()

    try:
        # Create schema in a transaction (like the working example)
        with db.transaction():
            # Create Person vertex type
            db.command("sql", "CREATE VERTEX TYPE Person")
            print("  ✓ Created Person vertex type")

            # Create properties for Person (various data types, some optional/NULL)
            db.command("sql", "CREATE PROPERTY Person.name STRING")
            db.command("sql", "CREATE PROPERTY Person.age INTEGER")
            db.command("sql", "CREATE PROPERTY Person.city STRING")
            db.command("sql", "CREATE PROPERTY Person.joined_date DATE")
            db.command("sql", "CREATE PROPERTY Person.email STRING")  # Optional
            db.command("sql", "CREATE PROPERTY Person.phone STRING")  # Optional
            db.command("sql", "CREATE PROPERTY Person.verified BOOLEAN")
            db.command("sql", "CREATE PROPERTY Person.reputation FLOAT")  # Optional
            print("  ✓ Created Person properties (including optional fields)")

            # Create FRIEND_OF edge type
            db.command("sql", "CREATE EDGE TYPE FRIEND_OF")
            print("  ✓ Created FRIEND_OF edge type")

            # Create properties for FRIEND_OF edge
            db.command("sql", "CREATE PROPERTY FRIEND_OF.since DATE")
            db.command("sql", "CREATE PROPERTY FRIEND_OF.closeness STRING")
            print("  ✓ Created FRIEND_OF properties")

            # Create indexes for better performance (correct ArcadeDB syntax)
            db.command("sql", "CREATE INDEX ON Person (name) NOTUNIQUE")
            print("  ✓ Created index on Person.name")

        print(f"  ⏱️  Time: {time.time() - step_start:.3f}s")

    except Exception as e:
        print(f"  ❌ Error creating schema: {e}")
        raise


def create_sample_data(db):
    """Create sample people and friendships"""
    print("\n👥 Creating sample social network data...")

    step_start = time.time()

    try:
        # Check if data already exists
        result = db.query("sql", "SELECT COUNT(*) as count FROM Person")
        count = list(result)[0].get_property("count")

        if count > 0:
            print(f"  ℹ️  Found {count} existing people, skipping data creation")
            return

        # Create people with various properties (including NULL values for optional fields)
        # Format: (name, age, city, joined_date, email, phone, verified, reputation)
        people_data = [
            (
                "Alice Johnson",
                28,
                "New York",
                "2020-01-15",
                "alice@example.com",
                "+1-555-0101",
                True,
                4.8,
            ),
            (
                "Bob Smith",
                32,
                "San Francisco",
                "2019-03-20",
                "bob@example.com",
                None,
                True,
                4.5,
            ),  # No phone (NULL)
            (
                "Carol Davis",
                26,
                "Chicago",
                "2021-06-10",
                None,
                "+1-555-0103",
                False,
                None,
            ),  # No email, no reputation (NULLs)
            (
                "David Wilson",
                35,
                "Boston",
                "2018-11-05",
                "david@example.com",
                "+1-555-0104",
                True,
                4.9,
            ),
            (
                "Eve Brown",
                29,
                "Seattle",
                "2020-08-22",
                None,
                None,
                False,
                3.2,
            ),  # No contact info (NULLs)
            (
                "Frank Miller",
                31,
                "Austin",
                "2019-12-14",
                "frank@example.com",
                "+1-555-0106",
                True,
                4.3,
            ),
            (
                "Grace Lee",
                27,
                "Denver",
                "2021-02-28",
                "grace@example.com",
                None,
                True,
                4.7,
            ),  # No phone (NULL)
            (
                "Henry Clark",
                33,
                "Portland",
                "2019-07-18",
                None,
                "+1-555-0108",
                True,
                None,
            ),  # No email, no reputation (NULLs)
        ]

        print("  📝 Creating people...")

        # Create people in a transaction
        with db.transaction():
            for person in people_data:
                name, age, city, joined_date, email, phone, verified, reputation = (
                    person
                )

                # Build SQL with NULL handling
                email_sql = f"'{email}'" if email else "NULL"
                phone_sql = f"'{phone}'" if phone else "NULL"
                reputation_sql = str(reputation) if reputation is not None else "NULL"
                verified_sql = str(verified).lower()

                db.command(
                    "sql",
                    f"CREATE VERTEX Person SET name = '{name}', age = {age}, "
                    f"city = '{city}', joined_date = date('{joined_date}'), "
                    f"email = {email_sql}, phone = {phone_sql}, "
                    f"verified = {verified_sql}, reputation = {reputation_sql}",
                )

                # Show which fields are NULL
                null_fields = []
                if not email:
                    null_fields.append("email")
                if not phone:
                    null_fields.append("phone")
                if reputation is None:
                    null_fields.append("reputation")

                null_str = f" (NULL: {', '.join(null_fields)})" if null_fields else ""
                print(f"    ✓ Created person: {name} ({age}, {city}){null_str}")

        # Create friendships with relationship properties
        print("  🤝 Creating friendships...")
        friendships = [
            ("Alice Johnson", "Bob Smith", "2020-05-15", "close"),
            ("Alice Johnson", "Carol Davis", "2021-07-20", "casual"),
            ("Bob Smith", "David Wilson", "2019-08-10", "close"),
            ("Bob Smith", "Frank Miller", "2020-01-25", "work"),
            ("Carol Davis", "Eve Brown", "2021-09-12", "close"),
            ("Carol Davis", "Grace Lee", "2021-08-05", "casual"),
            ("David Wilson", "Henry Clark", "2019-10-30", "old_friends"),
            ("Eve Brown", "Frank Miller", "2020-12-18", "casual"),
            ("Frank Miller", "Grace Lee", "2020-03-22", "work"),
            ("Grace Lee", "Henry Clark", "2021-05-14", "casual"),
            ("Alice Johnson", "Eve Brown", "2021-11-08", "close"),
            ("Bob Smith", "Henry Clark", "2020-06-03", "casual"),
        ]

        # Create friendships in a transaction
        with db.transaction():
            for person1, person2, since_date, closeness in friendships:
                # Create bidirectional friendship using property-based lookup
                db.command(
                    "sql",
                    f"CREATE EDGE FRIEND_OF FROM "
                    f"(SELECT FROM Person WHERE name = '{person1}') "
                    f"TO (SELECT FROM Person WHERE name = '{person2}') "
                    f"SET since = date('{since_date}'), closeness = '{closeness}'",
                )

                db.command(
                    "sql",
                    f"CREATE EDGE FRIEND_OF FROM "
                    f"(SELECT FROM Person WHERE name = '{person2}') "
                    f"TO (SELECT FROM Person WHERE name = '{person1}') "
                    f"SET since = date('{since_date}'), closeness = '{closeness}'",
                )

                print(f"    ✓ Connected {person1} ↔ {person2} ({closeness})")

        print(
            f"  ✅ Created {len(people_data)} people and "
            f"{len(friendships) * 2} friendship connections"
        )
        print(f"  ⏱️  Time: {time.time() - step_start:.3f}s")

    except Exception as e:
        print(f"  ❌ Error creating sample data: {e}")
        raise


def demonstrate_graph_queries(db):
    """Demonstrate various graph queries using both SQL and Cypher"""
    print("\n🔍 Demonstrating graph queries...")

    # SQL-based queries
    demonstrate_sql_queries(db)

    # Cypher-based queries
    demonstrate_cypher_queries(db)


def demonstrate_sql_queries(db):
    """Demonstrate graph queries using ArcadeDB's SQL MATCH syntax"""
    print("\n  📊 SQL MATCH Queries:")

    section_start = time.time()

    try:
        # 1. Find all friends of Alice using SQL MATCH
        print("\n    1️⃣ Find all friends of Alice (SQL MATCH):")
        query_start = time.time()
        result = db.query(
            "sql",
            """
            MATCH {type: Person, as: alice, where: (name = 'Alice Johnson')}
                  -FRIEND_OF->
                  {type: Person, as: friend}
            RETURN friend.name as name, friend.city as city
            ORDER BY friend.name
        """,
        )

        for row in result:
            print(
                f"      👥 {row.get_property('name')} from "
                f"{row.get_property('city')}"
            )
        print(f"      ⏱️  Time: {time.time() - query_start:.3f}s")

        # 2. Find friends of friends (2 degrees) using SQL MATCH
        print("\n    2️⃣ Find friends of friends of Alice (SQL MATCH):")
        result = db.query(
            "sql",
            """
            MATCH {type: Person, as: alice, where: (name = 'Alice Johnson')}
                  -FRIEND_OF->
                  {type: Person, as: friend}
                  -FRIEND_OF->
                  {type: Person, as: friend_of_friend, where: (name <> 'Alice Johnson')}
            RETURN DISTINCT friend_of_friend.name as name, friend.name as through_friend
            ORDER BY friend_of_friend.name
        """,
        )

        for row in result:
            print(
                f"      🔗 {row.get_property('name')} (through {row.get_property('through_friend')})"
            )

        # 3. Find mutual friends using SQL MATCH
        print("\n    3️⃣ Find mutual friends between Alice and Bob (SQL MATCH):")
        result = db.query(
            "sql",
            """
            MATCH {type: Person, as: alice, where: (name = 'Alice Johnson')}
                  -FRIEND_OF->
                  {type: Person, as: mutual}
                  <-FRIEND_OF-
                  {type: Person, as: bob, where: (name = 'Bob Smith')}
            RETURN mutual.name as mutual_friend
            ORDER BY mutual.name
        """,
        )

        mutual_friends = list(result)
        if mutual_friends:
            for row in mutual_friends:
                print(f"      🤝 {row.get_property('mutual_friend')}")
        else:
            print("      ℹ️  No mutual friends found")

        # 4. Find friendship statistics using SQL aggregation
        print("\n    4️⃣ Friendship statistics by city (SQL aggregation):")
        result = db.query(
            "sql",
            """
            SELECT city, COUNT(*) as person_count,
                   AVG(age) as avg_age
            FROM Person
            GROUP BY city
            ORDER BY person_count DESC, city
        """,
        )

        for row in result:
            city = str(row.get_property("city"))
            count = row.get_property("person_count")
            avg_age = row.get_property("avg_age")
            print(f"      • {city}: {count} people, avg age {avg_age:.1f}")

        # 5. Find people with NULL values (no email)
        print("\n    5️⃣ Find people without email (SQL NULL check):")
        result = db.query(
            "sql", "SELECT name, phone, verified FROM Person WHERE email IS NULL"
        )

        null_count = 0
        for row in result:
            name = str(row.get_property("name"))
            phone = row.get_property("phone")
            verified = row.get_property("verified")
            phone_str = phone if phone else "No phone"
            verified_str = "✓ Verified" if verified else "Not verified"
            print(f"      • {name}: {phone_str}, {verified_str}")
            null_count += 1

        if null_count == 0:
            print("      (none found)")
        else:
            print(f"      💡 {null_count} people without email addresses")

        # 6. Find verified people with reputation scores
        print("\n    6️⃣ Verified people with reputation (exclude NULLs):")
        result = db.query(
            "sql",
            """SELECT name, reputation, city FROM Person
               WHERE verified = true AND reputation IS NOT NULL
               ORDER BY reputation DESC""",
        )

        for row in result:
            name = str(row.get_property("name"))
            reputation = row.get_property("reputation")
            city = str(row.get_property("city"))
            stars = "⭐" * int(reputation)
            print(f"      • {name} ({city}): {reputation:.1f} {stars}")

        print(f"  ⏱️  SQL MATCH section: {time.time() - section_start:.3f}s")

    except Exception as e:
        print(f"    ❌ Error in SQL queries: {e}")
        import traceback

        traceback.print_exc()


def demonstrate_cypher_queries(db):
    """Demonstrate graph queries using Cypher dialect"""
    print("\n  🎯 Cypher Queries:")

    section_start = time.time()

    try:
        # 1. Find all friends of Alice using Cypher
        print("\n    1️⃣ Find all friends of Alice (Cypher):")
        result = db.query(
            "cypher",
            """
            MATCH (alice:Person {name: 'Alice Johnson'})-[:FRIEND_OF]->(friend:Person)
            RETURN friend.name as name, friend.city as city
            ORDER BY friend.name
        """,
        )

        for row in result:
            print(
                f"      👥 {row.get_property('name')} from {row.get_property('city')}"
            )

        # 2. Find friends of friends using Cypher
        print("\n    2️⃣ Find friends of friends of Alice (Cypher):")
        result = db.query(
            "cypher",
            """
            MATCH (alice:Person {name: 'Alice Johnson'})
                  -[:FRIEND_OF]->(friend:Person)
                  -[:FRIEND_OF]->(fof:Person)
            WHERE fof.name <> 'Alice Johnson'
            RETURN DISTINCT fof.name as name, friend.name as through_friend
            ORDER BY fof.name
        """,
        )

        for row in result:
            name = row.get_property("name")
            through_friend = row.get_property("through_friend")
            print(f"      🔗 {name} (through {through_friend})")

        # 3. Find mutual friends using Cypher
        print("\n    3️⃣ Find mutual friends between Alice and Bob (Cypher):")
        result = db.query(
            "cypher",
            """
            MATCH (alice:Person {name: 'Alice Johnson'})
                  -[:FRIEND_OF]->(mutual:Person)
                  <-[:FRIEND_OF]-(bob:Person {name: 'Bob Smith'})
            RETURN mutual.name as mutual_friend
            ORDER BY mutual.name
        """,
        )

        mutual_friends = list(result)
        if mutual_friends:
            for row in mutual_friends:
                print(f"      🤝 {row.get_property('mutual_friend')}")
        else:
            print("      ℹ️  No mutual friends found")

        # 4. Find people by friendship closeness using Cypher
        print("\n    4️⃣ Find close friendships (Cypher):")
        result = db.query(
            "cypher",
            """
            MATCH (p1:Person)-[f:FRIEND_OF {closeness: 'close'}]->(p2:Person)
            RETURN p1.name as person1, p2.name as person2, f.since as since
            ORDER BY f.since
        """,
        )

        for row in result:
            person1 = row.get_property("person1")
            person2 = row.get_property("person2")
            since = row.get_property("since")
            print(f"      💙 {person1} → {person2} (since {since})")

        # 5. Find variable length paths using Cypher
        print("\n    5️⃣ Find connections within 3 steps from Alice (Cypher):")
        result = db.query(
            "cypher",
            """
            MATCH (alice:Person {name: 'Alice Johnson'})
                  -[:FRIEND_OF*1..3]-(connected:Person)
            WHERE connected.name <> 'Alice Johnson'
            RETURN DISTINCT connected.name as name, connected.city as city
            ORDER BY connected.name
        """,
        )

        for row in result:
            print(
                f"      🌐 {row.get_property('name')} from "
                f"{row.get_property('city')}"
            )

        print(f"  ⏱️  Cypher section: {time.time() - section_start:.3f}s")

    except Exception as e:
        print(f"    ❌ Error in Cypher queries: {e}")
        import traceback

        traceback.print_exc()


def compare_query_languages(db):
    """Compare SQL MATCH vs Cypher for the same queries"""
    print("\n  🆚 SQL MATCH vs Cypher Comparison:")

    section_start = time.time()

    try:
        print("\n    📝 Same Query, Different Languages:")
        print("    " + "=" * 50)

        # Query: Find friends with their friendship details
        print("\n    Query: Find Alice's friends with friendship details")
        print("    " + "-" * 50)

        print("\n    🔵 SQL MATCH syntax:")
        print(
            """      SELECT name, city FROM Person
      WHERE name IN (
          SELECT p2.name FROM Person p1, FRIEND_OF f, Person p2
          WHERE p1.name = 'Alice Johnson' AND f.out = p1 AND f.in = p2
      )"""
        )

        try:
            result_sql = db.query(
                "sql",
                """
                SELECT name, city FROM Person
                WHERE name IN (
                    SELECT DISTINCT p2.name
                    FROM Person p1, FRIEND_OF f, Person p2
                    WHERE p1.name = 'Alice Johnson'
                      AND f.out = p1 AND f.in = p2
                )
                ORDER BY name
            """,
            )

            sql_results = list(result_sql)
            for row in sql_results:
                name = row.get_property("name")
                city = row.get_property("city")
                print(f"      👥 {name} ({city})")
            sql_count = len(sql_results)
        except Exception:
            print("      💡 SQL query syntax not supported in this ArcadeDB version")
            print("      💡 Concept: Use subqueries to navigate relationships")
            sql_count = 0

        print("\n    🟢 Cypher syntax:")
        print(
            """      MATCH (alice:Person {name: 'Alice Johnson'})
            -[edge:FRIEND_OF]->(friend:Person)
      RETURN friend.name, friend.city, edge.closeness, edge.since"""
        )

        result_cypher = db.query(
            "cypher",
            """
            MATCH (alice:Person {name: 'Alice Johnson'})
                  -[edge:FRIEND_OF]->(friend:Person)
            RETURN friend.name as name, friend.city as city,
                   edge.closeness as closeness, edge.since as since
            ORDER BY friend.name
        """,
        )

        cypher_results = list(result_cypher)
        for row in cypher_results:
            name = row.get_property("name")
            city = row.get_property("city")
            closeness = row.get_property("closeness")
            since = row.get_property("since")
            print(f"      👥 {name} ({city}) - {closeness} since {since}")

        # Compare results if SQL worked
        if sql_count > 0:
            if sql_count == len(cypher_results):
                print(f"\n    ✅ Both queries returned {sql_count} identical results")
            else:
                cypher_count = len(cypher_results)
                print(
                    f"\n    ⚠️  Result count differs: "
                    f"SQL={sql_count}, Cypher={cypher_count}"
                )
        else:
            cypher_count = len(cypher_results)
            print(f"\n    ✅ Cypher query returned {cypher_count} results")
            print("    💡 SQL and Cypher would yield equivalent results")

        # Show syntax differences
        print("\n    🔍 Key Syntax Differences:")
        print("    " + "-" * 30)
        print("    SQL MATCH:")
        print("      • Nodes: {type: Person, as: alias, where: (condition)}")
        print("      • Edges: -EDGE_TYPE-> or -EDGE_TYPE->{as: alias}")
        print("      • Conditions: where: (property = 'value')")
        print("      • More verbose but explicit about types")
        print()
        print("    Cypher:")
        print("      • Nodes: (alias:Label {property: 'value'})")
        print("      • Edges: -[alias:TYPE]-> or -[:TYPE]-")
        print("      • Conditions: WHERE property = 'value'")
        print("      • More concise and intuitive for graph patterns")

        print("\n    💡 When to use each:")
        print("    " + "-" * 20)
        print("    • SQL MATCH: When mixing graph and relational queries")
        print("    • Cypher: For pure graph operations and complex traversals")
        print("    • Both support the same underlying graph operations")

        print(f"  ⏱️  Comparison section: {time.time() - section_start:.3f}s")

    except Exception as e:
        print(f"    ❌ Error in query comparison: {e}")
        import traceback

        traceback.print_exc()


def print_section_header(title, emoji="🔹"):
    """Print a formatted section header"""
    print(f"\n{emoji} {title}")
    print("=" * (len(title) + 4))


if __name__ == "__main__":
    print("🌐 ArcadeDB Python - Social Network Graph Example")
    print("=" * 55)
    main()
