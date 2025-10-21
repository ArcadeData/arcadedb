"""
ArcadeDB Python bindings demonstration script.

This script demonstrates two different ways to access ArcadeDB:

1. **Java API (embedded)** - Direct in-process access via JPype
   - Fastest performance (no network overhead)
   - Full feature access
   - Single-process only

2. **HTTP REST API (remote)** - Standard HTTP requests
   - Good for remote access
   - Language agnostic
   - Moderate performance

Performance Comparison:
📊 Performance Summary:
   API Mode: JAVA
   Initialization + DB: 0.571s
   Schema creation: 0.060s
   Products insertion: 0.013s

📊 Performance Summary:
   API Mode: HTTP
   Initialization + DB: 0.822s
   Schema creation: 0.096s
   Products insertion: 0.025s

Usage:
    python basic.py --mode java    # Embedded Java API (fastest)
    python basic.py --mode http    # HTTP REST API (remote)
"""

import argparse
import os
import shutil
import time

import arcadedb_embedded as arcadedb
import requests
from requests.auth import HTTPBasicAuth

# Parse command line arguments
parser = argparse.ArgumentParser(
    description="ArcadeDB Multi-Model Database Demo - Java API vs HTTP API"
)
parser.add_argument(
    "--mode",
    choices=["java", "http"],
    default="java",
    help="API mode: 'java' for embedded Java API, 'http' for HTTP REST API",
)
args = parser.parse_args()

API_MODE = args.mode
ROOT_PASSWORD = "test12345"

print("\n" + "=" * 80)
print("DEMO: ArcadeDB Multi-Model Database - Documents, Vertices, and Edges")
print(f"API Mode: {API_MODE.upper()} API")
print("=" * 80)


# ============================================================================
# HTTP API Wrapper Classes (for comparison with embedded Java API)
# ============================================================================
class HTTPDatabase:
    """Lightweight wrapper for HTTP API database operations."""

    def __init__(self, base_url, db_name, username="root", password="test12345"):
        self.base_url = base_url
        self.db_name = db_name
        self.auth = HTTPBasicAuth(username, password)

    def command(self, language, command):
        """Execute a command via HTTP API."""
        url = f"{self.base_url}/api/v1/command/{self.db_name}"
        response = requests.post(
            url,
            auth=self.auth,
            json={"language": language, "command": command},
            timeout=30,
        )
        if response.status_code != 200:
            raise RuntimeError(f"HTTP command failed: {response.text}")
        return response.json() if response.text else None

    def query(self, language, command):
        """Execute a query via HTTP API."""
        url = f"{self.base_url}/api/v1/query/{self.db_name}"
        response = requests.post(
            url,
            auth=self.auth,
            json={"language": language, "command": command},
            timeout=30,
        )
        if response.status_code != 200:
            raise RuntimeError(f"HTTP query failed: {response.text}")
        return HTTPResultSet(response.json().get("result", []))

    def transaction(self):
        """Context manager for transactions (simplified for HTTP)."""
        return HTTPTransactionContext(self)

    def close(self):
        """Close method (no-op for HTTP - server manages connection)."""
        pass


class HTTPTransactionContext:
    """Simplified transaction context for HTTP API."""

    def __init__(self, database):
        self.database = database
        self.commands = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # For HTTP, we execute all commands in transaction mode
        # Note: This is simplified - real implementation would use sessions
        return False


class HTTPResultSet:
    """Wrapper for HTTP API result sets."""

    def __init__(self, results):
        self.results = results if results else []

    def __iter__(self):
        return iter([HTTPResult(r) for r in self.results])


class HTTPResult:
    """Wrapper for HTTP API result records."""

    def __init__(self, record_data):
        self.record = record_data

    def get_property(self, prop_name):
        """Get property from result record."""
        return self.record.get(prop_name)


class HTTPServer:
    """Wrapper for accessing server via HTTP instead of embedded Java."""

    def __init__(self, java_server):
        self.java_server = java_server
        self.base_url = None
        self.port = None

    def start(self):
        """Start the underlying Java server."""
        self.java_server.start()
        self.port = self.java_server.get_http_port()
        self.base_url = f"http://localhost:{self.port}"

    def stop(self):
        """Stop the server."""
        self.java_server.stop()

    def get_http_port(self):
        """Get HTTP port."""
        return self.port

    def get_studio_url(self):
        """Get Studio URL."""
        return self.java_server.get_studio_url()

    def create_database(self, db_name):
        """Create database using Java API, return HTTP wrapper."""
        # Use Java API to create database
        self.java_server.create_database(db_name)
        # Return HTTP wrapper for operations
        return HTTPDatabase(self.base_url, db_name, "root", ROOT_PASSWORD)


# ============================================================================
# Timing decorator
# ============================================================================
def timed_section(section_name):
    """Decorator to time code sections."""

    def decorator(func):
        def wrapper(*args, **kwargs):
            start = time.time()
            result = func(*args, **kwargs)
            elapsed = time.time() - start
            print(f"   ⏱️  {section_name}: {elapsed:.3f}s")
            return result

        return wrapper

    return decorator


# ============================================================================
# Setup
# ============================================================================

# ============================================================================
# Setup
# ============================================================================
root_path = "./my_test_databases"

# Cleanup before starting
if os.path.exists(root_path):
    shutil.rmtree(root_path, ignore_errors=True)
    print(f"🧹 Cleaned up existing directory: {root_path}")

# ============================================================================
# Step 1: Initialize and Create Database
# ============================================================================
print("\n1️⃣  Initializing ArcadeDB and creating database...")
init_start_time = time.time()

if API_MODE == "java":
    # Embedded mode - JVM initialization + DB creation in one step
    print("   ℹ️  Using embedded mode (JVM initialization + database creation)")
    from arcadedb_embedded import create_database

    db_path = f"{root_path}/databases/mydb"
    db = create_database(db_path)
    server = None
    init_time = time.time() - init_start_time

    print(f"   ✅ Database 'mydb' created")
    print(f"   ⏱️  JVM init + DB creation: {init_time:.3f}s")
else:
    # HTTP mode - Server startup (includes JVM init) + DB creation
    print(
        "   ℹ️  Starting HTTP server (JVM initialization + server + database creation)"
    )
    java_server = arcadedb.create_server(
        root_path=root_path,
        root_password=ROOT_PASSWORD,
    )

    # Wrap server for HTTP API access
    server = HTTPServer(java_server)
    server.start()

    # Now create database
    db = server.create_database("mydb")
    init_time = time.time() - init_start_time

    print(f"   ✅ Server started on port {server.get_http_port()}")
    print(f"   📊 Studio URL: {server.get_studio_url()}")
    print(f"   ✅ Database 'mydb' created")
    print(f"   ⏱️  Server init + DB creation: {init_time:.3f}s")
print("\n3️⃣  Creating comprehensive schema...")
start_time = time.time()
print("   📄 Document types...")
db.command("sql", "CREATE DOCUMENT TYPE Product")
db.command("sql", "CREATE DOCUMENT TYPE Order")
db.command("sql", "CREATE DOCUMENT TYPE Review")

print("   🔵 Vertex types...")
db.command("sql", "CREATE VERTEX TYPE Person")
db.command("sql", "CREATE VERTEX TYPE Company")
db.command("sql", "CREATE VERTEX TYPE Category")

print("   ➡️  Edge types...")
db.command("sql", "CREATE EDGE TYPE WorksFor")
db.command("sql", "CREATE EDGE TYPE Purchased")
db.command("sql", "CREATE EDGE TYPE BelongsTo")
db.command("sql", "CREATE EDGE TYPE Reviewed")
db.command("sql", "CREATE EDGE TYPE KnowsPerson")
schema_time = time.time() - start_time

print("   ✅ Schema created with 6 document/vertex types and 5 edge types")
print(f"   ⏱️  Schema creation: {schema_time:.3f}s")

# ============================================================================
# DOCUMENTS: Product catalog
# ============================================================================
print("\n3️⃣  Inserting products (documents)...")
start_time = time.time()
with db.transaction():
    db.command(
        "sql",
        "INSERT INTO Product SET name = 'Laptop Pro 15', price = 1299, "
        "stock = 15, category = 'Electronics', brand = 'TechCo'",
    )
    db.command(
        "sql",
        "INSERT INTO Product SET name = 'Wireless Mouse', price = 29, "
        "stock = 150, category = 'Accessories', brand = 'ClickTech'",
    )
    db.command(
        "sql",
        "INSERT INTO Product SET name = 'Mechanical Keyboard', price = 89, "
        "stock = 75, category = 'Accessories', brand = 'KeyMaster'",
    )
    db.command(
        "sql",
        "INSERT INTO Product SET name = 'USB-C Hub', price = 45, "
        "stock = 200, category = 'Accessories', brand = 'ConnectPlus'",
    )
    db.command(
        "sql",
        "INSERT INTO Product SET name = '27in 4K Monitor', price = 399, "
        "stock = 30, category = 'Electronics', brand = 'ViewTech'",
    )
    db.command(
        "sql",
        "INSERT INTO Product SET name = 'Laptop Stand', price = 35, "
        "stock = 100, category = 'Furniture', brand = 'DeskEase'",
    )
    db.command(
        "sql",
        "INSERT INTO Product SET name = 'Webcam HD', price = 79, "
        "stock = 60, category = 'Electronics', brand = 'ViewTech'",
    )
    db.command(
        "sql",
        "INSERT INTO Product SET name = 'Desk Mat', price = 19, "
        "stock = 250, category = 'Accessories', brand = 'DeskEase'",
    )
products_insert_time = time.time() - start_time
print("   ✅ Inserted 8 products")
print(f"   ⏱️  Products insertion: {products_insert_time:.3f}s")

# ============================================================================
# DOCUMENTS: Orders
# ============================================================================
print("\n4️⃣  Creating orders (documents)...")
with db.transaction():
    db.command(
        "sql",
        "INSERT INTO Order SET order_id = 'ORD-001', customer = 'Alice Johnson', "
        "total = 1417, items = 2, date = '2025-10-15', status = 'delivered'",
    )
    db.command(
        "sql",
        "INSERT INTO Order SET order_id = 'ORD-002', customer = 'Bob Smith', "
        "total = 108, items = 2, date = '2025-10-18', status = 'shipped'",
    )
    db.command(
        "sql",
        "INSERT INTO Order SET order_id = 'ORD-003', customer = 'Carol White', "
        "total = 433, items = 3, date = '2025-10-20', status = 'processing'",
    )
print("   ✅ Inserted 3 orders")

# ============================================================================
# DOCUMENTS: Reviews
# ============================================================================
print("\n5️⃣  Adding product reviews (documents)...")
with db.transaction():
    db.command(
        "sql",
        "INSERT INTO Review SET product = 'Laptop Pro 15', rating = 5, "
        "reviewer = 'Alice Johnson', comment = 'Amazing performance and build quality!'",
    )
    db.command(
        "sql",
        "INSERT INTO Review SET product = 'Wireless Mouse', rating = 4, "
        "reviewer = 'Bob Smith', "
        "comment = 'Good mouse, but battery life could be better'",
    )
    db.command(
        "sql",
        "INSERT INTO Review SET product = 'Mechanical Keyboard', rating = 5, "
        "reviewer = 'Carol White', comment = 'Best keyboard I have ever used!'",
    )
    db.command(
        "sql",
        "INSERT INTO Review SET product = '27in 4K Monitor', rating = 5, "
        "reviewer = 'Alice Johnson', "
        "comment = 'Crystal clear display, perfect for design work'",
    )
    db.command(
        "sql",
        "INSERT INTO Review SET product = 'USB-C Hub', rating = 3, "
        "reviewer = 'David Lee', comment = 'Works fine but gets warm during use'",
    )
print("   ✅ Inserted 5 reviews")

# ============================================================================
# GRAPH: People and Companies (vertices)
# ============================================================================
print("\n6️⃣  Creating people and companies (vertices)...")
with db.transaction():
    # People
    db.command(
        "sql",
        "CREATE VERTEX Person SET name = 'Alice Johnson', age = 32, "
        "city = 'San Francisco', role = 'Software Engineer'",
    )
    db.command(
        "sql",
        "CREATE VERTEX Person SET name = 'Bob Smith', age = 28, "
        "city = 'New York', role = 'Product Manager'",
    )
    db.command(
        "sql",
        "CREATE VERTEX Person SET name = 'Carol White', age = 35, "
        "city = 'Seattle', role = 'Data Scientist'",
    )
    db.command(
        "sql",
        "CREATE VERTEX Person SET name = 'David Lee', age = 29, "
        "city = 'Austin', role = 'Designer'",
    )
    db.command(
        "sql",
        "CREATE VERTEX Person SET name = 'Eve Martinez', age = 31, "
        "city = 'Boston', role = 'DevOps Engineer'",
    )
    db.command(
        "sql",
        "CREATE VERTEX Person SET name = 'Frank Chen', age = 27, "
        "city = 'San Francisco', role = 'Backend Developer'",
    )

    # Companies
    db.command(
        "sql",
        "CREATE VERTEX Company SET name = 'TechCorp', industry = 'Technology', "
        "employees = 500, founded = 2010",
    )
    db.command(
        "sql",
        "CREATE VERTEX Company SET name = 'DataVision', industry = 'Analytics', "
        "employees = 150, founded = 2015",
    )
    db.command(
        "sql",
        "CREATE VERTEX Company SET name = 'CloudFirst', industry = 'Cloud Computing', "
        "employees = 300, founded = 2012",
    )

    # Categories
    db.command(
        "sql",
        "CREATE VERTEX Category SET name = 'Electronics', "
        "description = 'Electronic devices and gadgets'",
    )
    db.command(
        "sql",
        "CREATE VERTEX Category SET name = 'Accessories', "
        "description = 'Computer and desk accessories'",
    )
    db.command(
        "sql",
        "CREATE VERTEX Category SET name = 'Furniture', "
        "description = 'Office furniture and equipment'",
    )

print("   ✅ Created 6 people, 3 companies, and 3 categories")

# ============================================================================
# GRAPH: Relationships (edges)
# ============================================================================
print("\n7️⃣  Creating relationships (edges)...")
with db.transaction():
    # Work relationships
    db.command(
        "sql",
        "CREATE EDGE WorksFor FROM (SELECT FROM Person WHERE name = 'Alice Johnson') "
        "TO (SELECT FROM Company WHERE name = 'TechCorp') "
        "SET since = 2020, position = 'Senior Engineer'",
    )
    db.command(
        "sql",
        "CREATE EDGE WorksFor FROM (SELECT FROM Person WHERE name = 'Bob Smith') "
        "TO (SELECT FROM Company WHERE name = 'TechCorp') "
        "SET since = 2021, position = 'Product Manager'",
    )
    db.command(
        "sql",
        "CREATE EDGE WorksFor FROM (SELECT FROM Person WHERE name = 'Carol White') "
        "TO (SELECT FROM Company WHERE name = 'DataVision') "
        "SET since = 2019, position = 'Lead Data Scientist'",
    )
    db.command(
        "sql",
        "CREATE EDGE WorksFor FROM (SELECT FROM Person WHERE name = 'David Lee') "
        "TO (SELECT FROM Company WHERE name = 'CloudFirst') "
        "SET since = 2022, position = 'UX Designer'",
    )
    db.command(
        "sql",
        "CREATE EDGE WorksFor FROM (SELECT FROM Person WHERE name = 'Eve Martinez') "
        "TO (SELECT FROM Company WHERE name = 'CloudFirst') "
        "SET since = 2020, position = 'DevOps Lead'",
    )
    db.command(
        "sql",
        "CREATE EDGE WorksFor FROM (SELECT FROM Person WHERE name = 'Frank Chen') "
        "TO (SELECT FROM Company WHERE name = 'TechCorp') "
        "SET since = 2023, position = 'Backend Developer'",
    )

    # Social connections
    db.command(
        "sql",
        "CREATE EDGE KnowsPerson FROM "
        "(SELECT FROM Person WHERE name = 'Alice Johnson') "
        "TO (SELECT FROM Person WHERE name = 'Bob Smith') "
        "SET since = 2020, relationship = 'colleague'",
    )
    db.command(
        "sql",
        "CREATE EDGE KnowsPerson FROM "
        "(SELECT FROM Person WHERE name = 'Alice Johnson') "
        "TO (SELECT FROM Person WHERE name = 'Frank Chen') "
        "SET since = 2023, relationship = 'mentor'",
    )
    db.command(
        "sql",
        "CREATE EDGE KnowsPerson FROM "
        "(SELECT FROM Person WHERE name = 'Carol White') "
        "TO (SELECT FROM Person WHERE name = 'David Lee') "
        "SET since = 2021, relationship = 'friend'",
    )
    db.command(
        "sql",
        "CREATE EDGE KnowsPerson FROM "
        "(SELECT FROM Person WHERE name = 'Eve Martinez') "
        "TO (SELECT FROM Person WHERE name = 'David Lee') "
        "SET since = 2022, relationship = 'colleague'",
    )

print("   ✅ Created 6 WorksFor edges and 4 KnowsPerson edges")

# ============================================================================
# QUERIES: Documents
# ============================================================================
print("\n8️⃣  Querying documents...")
print("   📦 All products ordered by price:")
result = db.query("sql", "SELECT FROM Product ORDER BY price DESC")
for record in result:
    name = str(record.get_property("name"))
    price = record.get_property("price")
    stock = record.get_property("stock")
    brand = str(record.get_property("brand"))
    print(f"      • {name:25} ${price:4} (stock: {stock:3}) - {brand}")

print("\n   💰 Products by category:")
result = db.query(
    "sql", "SELECT category, count(*) as count FROM Product GROUP BY category"
)
for record in result:
    category = str(record.get_property("category"))
    count = record.get_property("count")
    print(f"      • {category:15} {count} products")

print("\n   ⭐ High-rated products (rating >= 5):")
result = db.query(
    "sql", "SELECT product, rating, reviewer FROM Review WHERE rating >= 5"
)
for record in result:
    product = str(record.get_property("product"))
    rating = record.get_property("rating")
    reviewer = str(record.get_property("reviewer"))
    print(f"      • {product:25} ⭐ {rating} - by {reviewer}")

# ============================================================================
# QUERIES: Graph traversals
# ============================================================================
print("\n9️⃣  Querying graph relationships...")
print("   � People working at TechCorp:")
result = db.query(
    "sql",
    "SELECT name, role FROM Person WHERE out('WorksFor').name CONTAINS 'TechCorp'",
)
for record in result:
    name = str(record.get_property("name"))
    role = str(record.get_property("role"))
    print(f"      • {name:20} {role}")

print("\n   🏢 All employment relationships:")
result = db.query("sql", "SELECT name, employees FROM Company ORDER BY employees DESC")
for record in result:
    company = str(record.get_property("name"))
    employees = record.get_property("employees")
    # Count people working for this company via WorksFor edges
    emp_count_result = db.query(
        "sql",
        "SELECT count(*) as cnt FROM Person "
        f"WHERE out('WorksFor').name CONTAINS '{company}'",
    )
    emp_count = list(emp_count_result)[0].get_property("cnt")
    print(f"      • {company:15} {employees} total employees, {emp_count} in graph")

print("\n   🤝 Alice's network (people she knows):")
result = db.query(
    "sql", "SELECT expand(out('KnowsPerson')) FROM Person WHERE name = 'Alice Johnson'"
)
for record in result:
    name = str(record.get_property("name"))
    role = str(record.get_property("role"))
    city = str(record.get_property("city"))
    print(f"      • {name:20} {role:20} ({city})")

print("\n   🌐 Two-hop connections from Alice (friends of friends):")
result = db.query(
    "sql",
    "SELECT name, role FROM ("
    "  SELECT expand(out('KnowsPerson').out('KnowsPerson')) "
    "  FROM Person WHERE name = 'Alice Johnson'"
    ") WHERE name != 'Alice Johnson'",
)
for record in result:
    name = str(record.get_property("name"))
    role = str(record.get_property("role"))
    print(f"      • {name:20} {role}")

# ============================================================================
# ADVANCED QUERIES
# ============================================================================
print("\n🔟 Advanced analytics...")
print("   📊 Company size distribution:")
result = db.query("sql", "SELECT name, employees FROM Company ORDER BY employees DESC")
for record in result:
    company = str(record.get_property("name"))
    emp = record.get_property("employees")
    print(f"      • {company:15} {'█' * (emp // 50)} {emp}")

print("\n   💵 Revenue potential by category:")
result = db.query(
    "sql",
    "SELECT category, sum(price * stock) as value FROM Product "
    "GROUP BY category ORDER BY value DESC",
)
for record in result:
    category = str(record.get_property("category"))
    value = record.get_property("value")
    print(f"      • {category:15} ${value:,}")

print("\n   ⭐ Average rating by product:")
result = db.query(
    "sql",
    "SELECT product, avg(rating) as avg_rating, count(*) as num_reviews FROM Review "
    "GROUP BY product ORDER BY avg_rating DESC",
)
for record in result:
    product = str(record.get_property("product"))
    avg_rating = record.get_property("avg_rating")
    num_reviews = record.get_property("num_reviews")
    print(f"      • {product:25} ⭐ {avg_rating:.1f} ({num_reviews} reviews)")

# ============================================================================
# STATISTICS
# ============================================================================
print("\n1️⃣1️⃣  Database statistics...")
stats = {
    "Products": db.query("sql", "SELECT count(*) as c FROM Product"),
    "Orders": db.query("sql", "SELECT count(*) as c FROM Order"),
    "Reviews": db.query("sql", "SELECT count(*) as c FROM Review"),
    "People": db.query("sql", "SELECT count(*) as c FROM Person"),
    "Companies": db.query("sql", "SELECT count(*) as c FROM Company"),
    "Categories": db.query("sql", "SELECT count(*) as c FROM Category"),
    "WorksFor": db.query("sql", "SELECT count(*) as c FROM WorksFor"),
    "KnowsPerson": db.query("sql", "SELECT count(*) as c FROM KnowsPerson"),
}

print("   📈 Record counts:")
for label, result in stats.items():
    count = list(result)[0].get_property("c")
    print(f"      • {label:15} {count:3} records")

print("\n✅ Demo Complete!")
if API_MODE == "http":
    print(f"   🌐 View in Studio: {server.get_studio_url()}")
    print("   💡 Try running graph queries in the Studio interface!")
else:
    print("   💡 Embedded mode - no Studio UI available")
    print("   💡 To use Studio, run with --mode http")

# Print timing summary
print("\n📊 Performance Summary:")
print(f"   API Mode: {API_MODE.upper()}")
print(f"   Initialization + DB: {init_time:.3f}s")
print(f"   Schema creation: {schema_time:.3f}s")
print(f"   Products insertion: {products_insert_time:.3f}s")

# Cleanup
print("\n🧹 Cleaning up...")

# Close database
db.close()
print("   ✅ Database closed")

# Stop server if in HTTP mode
if API_MODE == "http":
    server.stop()
    print("   ✅ Server stopped")

if os.path.exists(root_path):
    shutil.rmtree(root_path, ignore_errors=True)
    print(f"   🧹 Cleaned up {root_path}")

print("\n👋 Done!\n")
