#!/usr/bin/env python3
"""Example 19: Hash Index Exact-Match Lookup Workflow.

This example mirrors several HASH index behaviors covered by the Java engine tests, but
packages them as a longer SQL-first Python workflow.

Workflow covered:
- create unique, non-unique, and composite HASH indexes with SQL
- insert deterministic product and warehouse inventory records
- inspect HASH index metadata through the schema API
- run exact-match lookups against single-property and composite HASH indexes
- verify missing-key behavior for both unique and composite exact matches
- update indexed values and confirm HASH index maintenance
- trigger a duplicate-key failure and verify transaction rollback semantics
- close and reopen the database to confirm index persistence

Requirements:
- A packaged ArcadeDB runtime that supports HASH indexes

Notes:
- HASH indexes are for exact-match predicates, not ordered or range access.
- The example stays SQL-first because that is the current examples posture.
"""

from __future__ import annotations

import argparse
import shutil
from pathlib import Path

import arcadedb_embedded as arcadedb

PRODUCTS = [
    {
        "sku": "SKU-100",
        "name": "Mechanical Keyboard",
        "category": "input",
        "brand": "KeyForge",
        "price": 129,
        "active": True,
    },
    {
        "sku": "SKU-101",
        "name": "Ergonomic Mouse",
        "category": "input",
        "brand": "KeyForge",
        "price": 79,
        "active": True,
    },
    {
        "sku": "SKU-102",
        "name": "4K Monitor",
        "category": "display",
        "brand": "ViewPeak",
        "price": 399,
        "active": True,
    },
    {
        "sku": "SKU-103",
        "name": "USB-C Dock",
        "category": "accessory",
        "brand": "Docksmith",
        "price": 189,
        "active": True,
    },
    {
        "sku": "SKU-104",
        "name": "Laptop Stand",
        "category": "accessory",
        "brand": "Docksmith",
        "price": 59,
        "active": True,
    },
    {
        "sku": "SKU-105",
        "name": "Noise-Canceling Headset",
        "category": "audio",
        "brand": "SoundNest",
        "price": 229,
        "active": True,
    },
    {
        "sku": "SKU-106",
        "name": "Portable SSD",
        "category": "storage",
        "brand": "DataSwift",
        "price": 149,
        "active": False,
    },
    {
        "sku": "SKU-107",
        "name": "Webcam Pro",
        "category": "video",
        "brand": "ViewPeak",
        "price": 119,
        "active": True,
    },
]


STOCK_POSITIONS = [
    {
        "warehouse": "north",
        "sku": "SKU-100",
        "on_hand": 25,
        "reserved": 4,
        "reorder_band": "healthy",
    },
    {
        "warehouse": "north",
        "sku": "SKU-101",
        "on_hand": 18,
        "reserved": 3,
        "reorder_band": "healthy",
    },
    {
        "warehouse": "west",
        "sku": "SKU-102",
        "on_hand": 7,
        "reserved": 2,
        "reorder_band": "watch",
    },
    {
        "warehouse": "west",
        "sku": "SKU-103",
        "on_hand": 11,
        "reserved": 1,
        "reorder_band": "healthy",
    },
    {
        "warehouse": "south",
        "sku": "SKU-104",
        "on_hand": 6,
        "reserved": 1,
        "reorder_band": "watch",
    },
    {
        "warehouse": "south",
        "sku": "SKU-105",
        "on_hand": 4,
        "reserved": 2,
        "reorder_band": "urgent",
    },
    {
        "warehouse": "east",
        "sku": "SKU-106",
        "on_hand": 3,
        "reserved": 0,
        "reorder_band": "urgent",
    },
    {
        "warehouse": "east",
        "sku": "SKU-107",
        "on_hand": 13,
        "reserved": 2,
        "reorder_band": "healthy",
    },
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Example 19: hash index exact-match lookup workflow"
    )
    parser.add_argument(
        "--db-path",
        default="./my_test_databases/hash_index_exact_match_db",
        help="Database path (default: ./my_test_databases/hash_index_exact_match_db)",
    )
    return parser.parse_args()


def reset_db(db_path: Path) -> None:
    if db_path.exists():
        shutil.rmtree(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)


def create_schema(db) -> bool:
    db.command("sql", "CREATE DOCUMENT TYPE Product")
    db.command("sql", "CREATE PROPERTY Product.sku STRING")
    db.command("sql", "CREATE PROPERTY Product.name STRING")
    db.command("sql", "CREATE PROPERTY Product.category STRING")
    db.command("sql", "CREATE PROPERTY Product.brand STRING")
    db.command("sql", "CREATE PROPERTY Product.price INTEGER")
    db.command("sql", "CREATE PROPERTY Product.active BOOLEAN")

    db.command("sql", "CREATE DOCUMENT TYPE StockPosition")
    db.command("sql", "CREATE PROPERTY StockPosition.warehouse STRING")
    db.command("sql", "CREATE PROPERTY StockPosition.sku STRING")
    db.command("sql", "CREATE PROPERTY StockPosition.on_hand INTEGER")
    db.command("sql", "CREATE PROPERTY StockPosition.reserved INTEGER")
    db.command("sql", "CREATE PROPERTY StockPosition.reorder_band STRING")

    try:
        db.command("sql", "CREATE INDEX ON Product (sku) UNIQUE_HASH")
        db.command("sql", "CREATE INDEX ON Product (category) NOTUNIQUE_HASH")
        db.command(
            "sql",
            "CREATE INDEX ON Product (brand, category) NOTUNIQUE_HASH",
        )
        db.command(
            "sql",
            "CREATE INDEX ON StockPosition (warehouse, sku) UNIQUE_HASH",
        )
        db.command(
            "sql",
            "CREATE INDEX ON StockPosition (reorder_band) NOTUNIQUE_HASH",
        )
    except arcadedb.ArcadeDBError as exc:
        message = str(exc)
        if (
            "Index type 'UNIQUE_HASH' is not supported" in message
            or "Invalid index type 'HASH'" in message
        ):
            print("HASH index support is not available in this packaged runtime.")
            print("This example depends on UNIQUE_HASH and NOTUNIQUE_HASH support.")
            print(f"Original error: {exc}")
            return False
        raise

    return True


def insert_seed_data(db) -> None:
    with db.transaction():
        for product in PRODUCTS:
            db.command(
                "sql",
                "INSERT INTO Product SET sku = ?, name = ?, category = ?, "
                "brand = ?, price = ?, active = ?",
                product["sku"],
                product["name"],
                product["category"],
                product["brand"],
                product["price"],
                product["active"],
            )

        for position in STOCK_POSITIONS:
            db.command(
                "sql",
                "INSERT INTO StockPosition SET warehouse = ?, sku = ?, "
                "on_hand = ?, reserved = ?, reorder_band = ?",
                position["warehouse"],
                position["sku"],
                position["on_hand"],
                position["reserved"],
                position["reorder_band"],
            )


def require(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def rows_to_dicts(result_set, fields: list[str]) -> list[dict]:
    rows = []
    for row in result_set:
        rows.append({field: row.get(field) for field in fields})
    return rows


def print_rows(title: str, rows: list[dict]) -> None:
    print(title)
    if not rows:
        print("  (no rows)")
        print()
        return

    for row in rows:
        print(f"  - {row}")
    print(f"  Rows returned: {len(rows)}")
    print()


def print_hash_indexes(db) -> None:
    print("HASH indexes discovered through schema metadata:")
    indexes = [
        idx
        for idx in db.schema.get_indexes()
        if str(idx.getType()) == "HASH"
        and (
            str(idx.getName()).startswith("Product[")
            or str(idx.getName()).startswith("StockPosition[")
        )
    ]
    for idx in sorted(indexes, key=lambda item: str(item.getName())):
        index_name = str(idx.getName())
        print(
            f"  - {index_name} | unique={idx.isUnique()} | "
            f"type={idx.getType()} | exists={db.schema.exists_index(index_name)}"
        )
        require(
            db.schema.get_index_by_name(index_name) is not None,
            f"Expected schema lookup for {index_name} to succeed",
        )
    print()


def run_exact_match_phase(db) -> None:
    print("Phase 1: Single-property and composite exact-match lookups")
    print()

    sku_rows = rows_to_dicts(
        db.query("sql", "SELECT FROM Product WHERE sku = ?", "SKU-102"),
        ["sku", "name", "category", "brand", "price", "active"],
    )
    require(len(sku_rows) == 1, "Expected exactly one row for SKU-102")
    print_rows("Unique HASH lookup on Product.sku:", sku_rows)

    category_rows = rows_to_dicts(
        db.query(
            "sql",
            "SELECT FROM Product WHERE category = ? ORDER BY sku",
            "accessory",
        ),
        ["sku", "name", "category", "brand", "price"],
    )
    require(
        len(category_rows) == 2,
        "Expected two accessory rows from Product.category HASH lookup",
    )
    print_rows("Non-unique HASH lookup on Product.category:", category_rows)

    composite_rows = rows_to_dicts(
        db.query(
            "sql",
            "SELECT FROM Product WHERE brand = ? AND category = ? ORDER BY sku",
            "Docksmith",
            "accessory",
        ),
        ["sku", "name", "brand", "category", "price"],
    )
    require(
        len(composite_rows) == 2,
        "Expected two rows for composite Product[brand,category] lookup",
    )
    print_rows("Composite HASH lookup on Product[brand,category]:", composite_rows)

    stock_rows = rows_to_dicts(
        db.query(
            "sql",
            "SELECT FROM StockPosition WHERE warehouse = ? AND sku = ?",
            "south",
            "SKU-105",
        ),
        ["warehouse", "sku", "on_hand", "reserved", "reorder_band"],
    )
    require(
        len(stock_rows) == 1,
        "Expected one row for StockPosition[warehouse,sku] lookup",
    )
    print_rows(
        "Composite unique HASH lookup on StockPosition[warehouse,sku]:",
        stock_rows,
    )

    urgent_rows = rows_to_dicts(
        db.query(
            "sql",
            "SELECT FROM StockPosition WHERE reorder_band = ? ORDER BY sku",
            "urgent",
        ),
        ["warehouse", "sku", "on_hand", "reserved", "reorder_band"],
    )
    require(
        len(urgent_rows) == 2,
        "Expected two rows for StockPosition.reorder_band lookup",
    )
    print_rows("Non-unique HASH lookup on StockPosition.reorder_band:", urgent_rows)

    missing_product = rows_to_dicts(
        db.query("sql", "SELECT FROM Product WHERE sku = ?", "SKU-999"),
        ["sku"],
    )
    missing_stock = rows_to_dicts(
        db.query(
            "sql",
            "SELECT FROM StockPosition WHERE warehouse = ? AND sku = ?",
            "north",
            "SKU-999",
        ),
        ["warehouse", "sku"],
    )
    require(not missing_product, "Expected missing Product lookup to return zero rows")
    require(
        not missing_stock,
        "Expected missing StockPosition lookup to return zero rows",
    )
    print("Missing-key lookups:")
    print(f"  Product.sku = 'SKU-999' -> {len(missing_product)} rows")
    print(f"  StockPosition[north, SKU-999] -> {len(missing_stock)} rows")
    print()


def run_update_phase(db) -> None:
    print("Phase 2: Update indexed values and verify HASH index maintenance")
    print()

    with db.transaction():
        db.command(
            "sql",
            "UPDATE Product SET category = ?, brand = ? WHERE sku = ?",
            "video-pro",
            "ViewPeakStudio",
            "SKU-107",
        )
        db.command(
            "sql",
            "UPDATE StockPosition SET warehouse = ?, reserved = ? "
            "WHERE warehouse = ? AND sku = ?",
            "central",
            0,
            "south",
            "SKU-105",
        )

    old_product_rows = rows_to_dicts(
        db.query(
            "sql",
            "SELECT FROM Product WHERE brand = ? AND category = ?",
            "ViewPeak",
            "video",
        ),
        ["sku", "brand", "category"],
    )
    new_product_rows = rows_to_dicts(
        db.query(
            "sql",
            "SELECT FROM Product WHERE brand = ? AND category = ?",
            "ViewPeakStudio",
            "video-pro",
        ),
        ["sku", "brand", "category", "name"],
    )
    old_stock_rows = rows_to_dicts(
        db.query(
            "sql",
            "SELECT FROM StockPosition WHERE warehouse = ? AND sku = ?",
            "south",
            "SKU-105",
        ),
        ["warehouse", "sku", "reserved", "reorder_band"],
    )
    new_stock_rows = rows_to_dicts(
        db.query(
            "sql",
            "SELECT FROM StockPosition WHERE warehouse = ? AND sku = ?",
            "central",
            "SKU-105",
        ),
        ["warehouse", "sku", "reserved", "reorder_band"],
    )

    require(
        not old_product_rows,
        "Expected old Product composite HASH key to return zero rows",
    )
    require(
        len(new_product_rows) == 1,
        "Expected updated Product composite HASH key to return one row",
    )
    require(
        not old_stock_rows,
        "Expected old StockPosition composite HASH key to return zero rows",
    )
    require(
        len(new_stock_rows) == 1,
        "Expected updated StockPosition composite HASH key to return one row",
    )

    print_rows("Old Product composite HASH key after update:", old_product_rows)
    print_rows("New Product composite HASH key after update:", new_product_rows)
    print_rows("Old StockPosition composite HASH key after update:", old_stock_rows)
    print_rows("New StockPosition composite HASH key after update:", new_stock_rows)


def run_failed_transaction_phase(db) -> None:
    print("Phase 3: Duplicate-key rejection and rollback semantics")
    print()

    before_product_count = (
        db.query("sql", "SELECT count(*) AS count FROM Product").first().get("count")
    )
    before_stock_count = (
        db.query("sql", "SELECT count(*) AS count FROM StockPosition")
        .first()
        .get("count")
    )

    try:
        with db.transaction():
            db.command(
                "sql",
                "INSERT INTO Product SET sku = ?, name = ?, category = ?, "
                "brand = ?, price = ?, active = ?",
                "SKU-102",
                "Duplicate 4K Monitor",
                "display",
                "ViewPeak",
                499,
                True,
            )
            db.command(
                "sql",
                "INSERT INTO StockPosition SET warehouse = ?, sku = ?, "
                "on_hand = ?, reserved = ?, reorder_band = ?",
                "north",
                "SKU-199",
                9,
                0,
                "watch",
            )
    except arcadedb.ArcadeDBError as exc:
        print(f"Duplicate insert rejected as expected: {exc}")
    else:
        raise RuntimeError(
            "Expected duplicate HASH key rejection, but transaction succeeded"
        )

    after_product_count = (
        db.query("sql", "SELECT count(*) AS count FROM Product").first().get("count")
    )
    after_stock_count = (
        db.query("sql", "SELECT count(*) AS count FROM StockPosition")
        .first()
        .get("count")
    )

    require(
        before_product_count == after_product_count,
        "Product count changed after failed duplicate-key transaction",
    )
    require(
        before_stock_count == after_stock_count,
        "StockPosition count changed after failed duplicate-key transaction",
    )

    rollback_probe = rows_to_dicts(
        db.query(
            "sql",
            "SELECT FROM StockPosition WHERE warehouse = ? AND sku = ?",
            "north",
            "SKU-199",
        ),
        ["warehouse", "sku"],
    )
    require(
        not rollback_probe,
        "Rollback probe row should not exist after failed transaction",
    )

    print("Counts remained unchanged after failed transaction.")
    print(f"  Product count: {after_product_count}")
    print(f"  StockPosition count: {after_stock_count}")
    print(f"  Rolled-back StockPosition probe rows: {len(rollback_probe)}")
    print()


def run_reopen_phase(db_path: Path) -> None:
    print("Phase 4: Reopen verification")
    print()

    with arcadedb.open_database(str(db_path)) as reopened_db:
        expected_indexes = [
            "Product[sku]",
            "Product[category]",
            "Product[brand,category]",
            "StockPosition[warehouse,sku]",
            "StockPosition[reorder_band]",
        ]

        print("Index existence after reopen:")
        for index_name in expected_indexes:
            exists = reopened_db.schema.exists_index(index_name)
            print(f"  - {index_name}: {exists}")
            require(exists, f"Expected {index_name} to exist after reopen")
        print()

        reopened_product = rows_to_dicts(
            reopened_db.query(
                "sql",
                "SELECT FROM Product WHERE brand = ? AND category = ?",
                "ViewPeakStudio",
                "video-pro",
            ),
            ["sku", "brand", "category", "name"],
        )
        reopened_stock = rows_to_dicts(
            reopened_db.query(
                "sql",
                "SELECT FROM StockPosition WHERE warehouse = ? AND sku = ?",
                "central",
                "SKU-105",
            ),
            ["warehouse", "sku", "reserved", "reorder_band"],
        )

        require(
            len(reopened_product) == 1,
            "Expected reopened Product composite lookup to return one row",
        )
        require(
            len(reopened_stock) == 1,
            "Expected reopened StockPosition composite lookup to return one row",
        )

        print_rows("Reopened composite Product HASH lookup:", reopened_product)
        print_rows("Reopened composite StockPosition HASH lookup:", reopened_stock)

        reopened_product_count = (
            reopened_db.query("sql", "SELECT count(*) AS count FROM Product")
            .first()
            .get("count")
        )
        reopened_stock_count = (
            reopened_db.query("sql", "SELECT count(*) AS count FROM StockPosition")
            .first()
            .get("count")
        )
        print("Counts after reopen:")
        print(f"  Product count: {reopened_product_count}")
        print(f"  StockPosition count: {reopened_stock_count}")
        print()


def main() -> int:
    args = parse_args()
    db_path = Path(args.db_path)

    print("=" * 72)
    print("ArcadeDB Python - Example 19: Hash Index Exact-Match Lookup Workflow")
    print("=" * 72)
    print()

    reset_db(db_path)

    with arcadedb.create_database(str(db_path)) as db:
        print(f"Database created at: {db_path}")
        print("Approach: SQL-first HASH index workflow from Python bindings")
        print(
            "Focus: multiple HASH index shapes, exact-match lookups, updates, "
            "rollback behavior, and reopen verification"
        )
        print()

        if not create_schema(db):
            return 0

        insert_seed_data(db)

        print(
            f"Inserted {len(PRODUCTS)} Product records and "
            f"{len(STOCK_POSITIONS)} StockPosition records"
        )
        print("Reminder: HASH indexes are for exact-match lookups, not range scans.")
        print()

        print_hash_indexes(db)
        run_exact_match_phase(db)
        run_update_phase(db)
        run_failed_transaction_phase(db)

    run_reopen_phase(db_path)

    print("Example 19 completed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
