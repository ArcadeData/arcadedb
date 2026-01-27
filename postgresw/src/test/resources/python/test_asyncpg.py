#!/usr/bin/env python3
"""
ArcadeDB asyncpg Test Suite

Tests PostgreSQL wire protocol compatibility with Python asyncpg driver.
Related issues:
- https://github.com/ArcadeData/arcadedb/issues/1630 (bind message parsing)
- https://github.com/ArcadeData/arcadedb/issues/668 (asyncpg compatibility)

Usage:
    1. Start ArcadeDB server with PostgreSQL plugin enabled:
       bin/server.sh -Darcadedb.server.plugins="Postgres:com.arcadedb.postgres.PostgresProtocolPlugin"

    2. Create a test database:
       curl -X POST -u root:playwithdata http://localhost:2480/api/v1/server \
           -d '{"command":"create database asyncpg_testdb"}' -H "Content-Type: application/json"

    3. Install asyncpg:
       pip install asyncpg

    4. Run tests:
       python test_asyncpg.py [--host HOST] [--port PORT] [--database DB] [--user USER] [--password PWD]

Environment variables (alternative to command line args):
    ARCADEDB_HOST (default: localhost)
    ARCADEDB_PORT (default: 5432)
    ARCADEDB_DATABASE (default: asyncpg_testdb)
    ARCADEDB_USER (default: root)
    ARCADEDB_PASSWORD (default: playwithdata)
"""

import asyncio
import argparse
import os
import sys
from typing import Optional

# Check for asyncpg
try:
    import asyncpg
except ImportError:
    print("ERROR: asyncpg not installed. Run: pip install asyncpg")
    sys.exit(1)


class TestConfig:
    """Test configuration"""
    def __init__(self, host: str, port: int, database: str, user: str, password: str):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password


class TestResult:
    """Test result tracking"""
    def __init__(self):
        self.passed = 0
        self.failed = 0
        self.errors = []

    def add_pass(self, name: str):
        self.passed += 1
        print(f"  PASSED: {name}")

    def add_fail(self, name: str, error: str):
        self.failed += 1
        self.errors.append((name, error))
        print(f"  FAILED: {name}")
        print(f"    Error: {error}")

    def summary(self) -> bool:
        total = self.passed + self.failed
        print(f"\nResults: {self.passed}/{total} tests passed")
        if self.errors:
            print("\nFailed tests:")
            for name, error in self.errors:
                print(f"  - {name}: {error}")
        return self.failed == 0


async def get_connection(config: TestConfig) -> asyncpg.Connection:
    """Create a connection to ArcadeDB"""
    return await asyncpg.connect(
        host=config.host,
        port=config.port,
        database=config.database,
        user=config.user,
        password=config.password,
        ssl=None,
        timeout=30
    )


async def test_connection(config: TestConfig, result: TestResult):
    """Test 1: Basic connection"""
    test_name = "Basic connection"
    try:
        conn = await get_connection(config)
        await conn.close()
        result.add_pass(test_name)
    except Exception as e:
        result.add_fail(test_name, str(e))


async def test_simple_query(config: TestConfig, result: TestResult):
    """Test 2: Simple query without parameters"""
    test_name = "Simple query"
    try:
        conn = await get_connection(config)
        try:
            # Query schema types
            rows = await conn.fetch("SELECT FROM schema:types")
            result.add_pass(test_name)
        finally:
            await conn.close()
    except Exception as e:
        result.add_fail(test_name, str(e))


async def test_create_type_and_insert(config: TestConfig, result: TestResult):
    """Test 3: Create document type and insert data"""
    test_name = "Create type and insert"
    try:
        conn = await get_connection(config)
        try:
            # Create type (ignore error if already exists)
            try:
                await conn.execute("CREATE DOCUMENT TYPE AsyncpgTest")
            except Exception:
                pass  # Type may already exist

            # Insert data
            await conn.execute("INSERT INTO AsyncpgTest SET id = 'test1', name = 'Alice', value = 100")
            await conn.execute("INSERT INTO AsyncpgTest SET id = 'test2', name = 'Bob', value = 200")
            result.add_pass(test_name)
        finally:
            await conn.close()
    except Exception as e:
        result.add_fail(test_name, str(e))


async def test_parameterized_select(config: TestConfig, result: TestResult):
    """Test 4: Parameterized SELECT query (Issue #1630)"""
    test_name = "Parameterized SELECT"
    try:
        conn = await get_connection(config)
        try:
            # This is the key test for issue #1630
            rows = await conn.fetch("SELECT FROM AsyncpgTest WHERE id = $1", "test1")
            if len(rows) == 1:
                result.add_pass(test_name)
            else:
                result.add_fail(test_name, f"Expected 1 row, got {len(rows)}")
        finally:
            await conn.close()
    except Exception as e:
        result.add_fail(test_name, str(e))


async def test_multiple_parameters(config: TestConfig, result: TestResult):
    """Test 5: Query with multiple parameters"""
    test_name = "Multiple parameters"
    try:
        conn = await get_connection(config)
        try:
            # Note: Using strings for all params since ArcadeDB declares VARCHAR for all param types
            rows = await conn.fetch(
                "SELECT FROM AsyncpgTest WHERE name = $1 AND value = $2",
                "Alice", "100"
            )
            if len(rows) == 1:
                result.add_pass(test_name)
            else:
                result.add_fail(test_name, f"Expected 1 row, got {len(rows)}")
        finally:
            await conn.close()
    except Exception as e:
        result.add_fail(test_name, str(e))


async def test_parameterized_insert(config: TestConfig, result: TestResult):
    """Test 6: INSERT with parameters"""
    test_name = "Parameterized INSERT"
    try:
        conn = await get_connection(config)
        try:
            # Note: Using strings for all params since ArcadeDB declares VARCHAR for all param types
            await conn.execute(
                "INSERT INTO AsyncpgTest SET id = $1, name = $2, value = $3",
                "test3", "Charlie", "300"
            )
            # Verify
            rows = await conn.fetch("SELECT FROM AsyncpgTest WHERE id = $1", "test3")
            if len(rows) == 1:
                result.add_pass(test_name)
            else:
                result.add_fail(test_name, f"Expected 1 row after insert, got {len(rows)}")
        finally:
            await conn.close()
    except Exception as e:
        result.add_fail(test_name, str(e))


async def test_transaction(config: TestConfig, result: TestResult):
    """Test 7: Transaction support"""
    test_name = "Transaction"
    try:
        conn = await get_connection(config)
        try:
            async with conn.transaction():
                await conn.execute("INSERT INTO AsyncpgTest SET id = 'tx_test', name = 'TxTest', value = 999")
            # Verify commit
            rows = await conn.fetch("SELECT FROM AsyncpgTest WHERE id = $1", "tx_test")
            if len(rows) == 1:
                result.add_pass(test_name)
            else:
                result.add_fail(test_name, f"Transaction not committed, got {len(rows)} rows")
        finally:
            await conn.close()
    except Exception as e:
        result.add_fail(test_name, str(e))


async def cleanup(config: TestConfig):
    """Clean up test data"""
    try:
        conn = await get_connection(config)
        try:
            await conn.execute("DELETE FROM AsyncpgTest")
        except:
            pass
        finally:
            await conn.close()
    except:
        pass


async def run_tests(config: TestConfig) -> bool:
    """Run all tests"""
    print("=" * 60)
    print("ArcadeDB asyncpg Test Suite")
    print(f"Server: {config.host}:{config.port}")
    print(f"Database: {config.database}")
    print(f"asyncpg version: {asyncpg.__version__}")
    print("=" * 60)

    result = TestResult()

    print("\n[Connection Tests]")
    await test_connection(config, result)

    print("\n[Query Tests]")
    await test_simple_query(config, result)
    await test_create_type_and_insert(config, result)

    print("\n[Parameterized Query Tests - Issue #1630]")
    await test_parameterized_select(config, result)
    await test_multiple_parameters(config, result)
    await test_parameterized_insert(config, result)

    print("\n[Transaction Tests]")
    await test_transaction(config, result)

    print("\n" + "=" * 60)
    success = result.summary()
    print("=" * 60)

    # Cleanup
    await cleanup(config)

    return success


def main():
    parser = argparse.ArgumentParser(description="ArcadeDB asyncpg Test Suite")
    parser.add_argument("--host", default=os.environ.get("ARCADEDB_HOST", "localhost"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("ARCADEDB_PORT", "5432")))
    parser.add_argument("--database", default=os.environ.get("ARCADEDB_DATABASE", "asyncpg_testdb"))
    parser.add_argument("--user", default=os.environ.get("ARCADEDB_USER", "root"))
    parser.add_argument("--password", default=os.environ.get("ARCADEDB_PASSWORD", "playwithdata"))
    args = parser.parse_args()

    config = TestConfig(
        host=args.host,
        port=args.port,
        database=args.database,
        user=args.user,
        password=args.password
    )

    success = asyncio.run(run_tests(config))
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
