#!/usr/bin/env python3
"""Example 18: Geo Predicates With WKT Points And Polygons.

This example keeps the Python surface SQL-first and uses the existing geospatial
SQL functions instead of inventing a Python geometry API.

Workflow covered:
- create point and polygon document types using WKT stored in STRING fields
- create a GEOSPATIAL index on point coordinates
- run direct geo.within / geo.intersects sanity checks on WKT literals
- query indexed point records with polygon filters via geo.within and geo.intersects
- query stored polygon records with polygon overlap predicates
- close and reopen the database to verify persisted geospatial index metadata
- drop the index and rerun the same filter to demonstrate full-scan fallback

Requirements:
- A packaged ArcadeDB runtime that supports geo.* SQL functions and GEOSPATIAL indexes

Notes:
- The example is intentionally SQL-first because that is the current examples posture.
- Geospatial properties are stored as WKT strings and interpreted in SQL via geo.*.
"""

from __future__ import annotations

import argparse
import shutil
from pathlib import Path

import arcadedb_embedded as arcadedb

DEPOTS = [
    {
        "depot_id": "dep-rome",
        "name": "Rome Hub",
        "city": "Rome",
        "kind": "hub",
        "coords": "POINT (12.5 41.9)",
        "daily_capacity": 220,
    },
    {
        "depot_id": "dep-milan",
        "name": "Milan Hub",
        "city": "Milan",
        "kind": "hub",
        "coords": "POINT (9.2 45.5)",
        "daily_capacity": 260,
    },
    {
        "depot_id": "dep-naples",
        "name": "Naples Relay",
        "city": "Naples",
        "kind": "relay",
        "coords": "POINT (14.3 40.8)",
        "daily_capacity": 140,
    },
    {
        "depot_id": "dep-florence",
        "name": "Florence Relay",
        "city": "Florence",
        "kind": "relay",
        "coords": "POINT (11.3 43.8)",
        "daily_capacity": 120,
    },
    {
        "depot_id": "dep-turin",
        "name": "Turin Crossdock",
        "city": "Turin",
        "kind": "crossdock",
        "coords": "POINT (7.7 45.1)",
        "daily_capacity": 90,
    },
    {
        "depot_id": "dep-bari",
        "name": "Bari Hub",
        "city": "Bari",
        "kind": "hub",
        "coords": "POINT (16.9 41.1)",
        "daily_capacity": 160,
    },
]


SERVICE_ZONES = [
    {
        "zone_id": "zone-central",
        "purpose": "core-coverage",
        "area": "POLYGON ((10 40, 14.5 40, 14.5 44.5, 10 44.5, 10 40))",
    },
    {
        "zone_id": "zone-northwest",
        "purpose": "overflow",
        "area": "POLYGON ((7 44, 10.5 44, 10.5 46, 7 46, 7 44))",
    },
    {
        "zone_id": "zone-south",
        "purpose": "express",
        "area": "POLYGON ((12 37.5, 18 37.5, 18 42.2, 12 42.2, 12 37.5))",
    },
]


ROME_POINT = "POINT (12.5 41.9)"
MEDITERRANEAN_BOX = "POLYGON ((10 40, 14.5 40, 14.5 44.5, 10 44.5, 10 40))"
TYRRHENIAN_BOX = "POLYGON ((11 40.5, 15.2 40.5, 15.2 43.5, 11 43.5, 11 40.5))"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Example 18: geo predicates with WKT points and polygons"
    )
    parser.add_argument(
        "--db-path",
        default="./my_test_databases/geo_predicates_wkt_db",
        help="Database path (default: ./my_test_databases/geo_predicates_wkt_db)",
    )
    return parser.parse_args()


def reset_db(db_path: Path) -> None:
    if db_path.exists():
        shutil.rmtree(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)


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


def ensure_geo_support(db) -> bool:
    try:
        within_probe = (
            db.query(
                "sql",
                "SELECT geo.within(?, ?) AS inside",
                ROME_POINT,
                MEDITERRANEAN_BOX,
            )
            .first()
            .get("inside")
        )
    except arcadedb.ArcadeDBError as exc:
        message = str(exc)
        if "Unknown method name: within" in message:
            print(
                "Geospatial SQL functions are not available in this packaged runtime."
            )
            print(
                "This example depends on geo.within, geo.intersects, and "
                "geo.geomFromText."
            )
            print(f"Original error: {exc}")
            return False
        raise

    require(
        within_probe is True,
        "Expected Rome point to be within the Mediterranean box",
    )
    return True


def create_schema(db) -> bool:
    db.command("sql", "CREATE DOCUMENT TYPE Depot")
    db.command("sql", "CREATE PROPERTY Depot.depot_id STRING")
    db.command("sql", "CREATE PROPERTY Depot.name STRING")
    db.command("sql", "CREATE PROPERTY Depot.city STRING")
    db.command("sql", "CREATE PROPERTY Depot.kind STRING")
    db.command("sql", "CREATE PROPERTY Depot.coords STRING")
    db.command("sql", "CREATE PROPERTY Depot.daily_capacity INTEGER")

    db.command("sql", "CREATE DOCUMENT TYPE ServiceZone")
    db.command("sql", "CREATE PROPERTY ServiceZone.zone_id STRING")
    db.command("sql", "CREATE PROPERTY ServiceZone.purpose STRING")
    db.command("sql", "CREATE PROPERTY ServiceZone.area STRING")

    try:
        db.command("sql", "CREATE INDEX ON Depot (coords) GEOSPATIAL")
    except arcadedb.ArcadeDBError as exc:
        message = str(exc)
        if (
            "Index type 'GEOSPATIAL' is not supported" in message
            or "Invalid index type 'GEOSPATIAL'" in message
        ):
            print("GEOSPATIAL index support is not available in this packaged runtime.")
            print("This example depends on CREATE INDEX ... GEOSPATIAL.")
            print(f"Original error: {exc}")
            return False
        raise

    return True


def insert_seed_data(db) -> None:
    with db.transaction():
        for depot in DEPOTS:
            db.command(
                "sql",
                "INSERT INTO Depot SET depot_id = ?, name = ?, city = ?, kind = ?, "
                "coords = ?, daily_capacity = ?",
                depot["depot_id"],
                depot["name"],
                depot["city"],
                depot["kind"],
                depot["coords"],
                depot["daily_capacity"],
            )

        for zone in SERVICE_ZONES:
            db.command(
                "sql",
                "INSERT INTO ServiceZone SET zone_id = ?, purpose = ?, area = ?",
                zone["zone_id"],
                zone["purpose"],
                zone["area"],
            )


def print_geo_indexes(db) -> None:
    print("GEOSPATIAL indexes discovered through schema metadata:")
    indexes = [
        idx
        for idx in db.schema.get_indexes()
        if str(idx.getType()) == "GEOSPATIAL"
        and str(idx.getName()).startswith("Depot[")
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


def run_predicate_sanity_phase(db) -> None:
    print("Phase 1: Direct WKT predicate sanity checks")
    print()

    within_row = db.query(
        "sql",
        "SELECT geo.within(?, ?) AS inside",
        ROME_POINT,
        MEDITERRANEAN_BOX,
    ).first()
    disjoint_row = db.query(
        "sql",
        "SELECT geo.within('POINT (9.2 45.5)', ?) AS inside",
        MEDITERRANEAN_BOX,
    ).first()
    polygon_intersects_row = db.query(
        "sql",
        "SELECT geo.intersects(?, ?) AS overlaps",
        MEDITERRANEAN_BOX,
        TYRRHENIAN_BOX,
    ).first()
    null_row = db.query(
        "sql",
        "SELECT geo.within(null, ?) AS inside",
        MEDITERRANEAN_BOX,
    ).first()

    require(
        within_row.get("inside") is True,
        "Expected within() sanity check to be true",
    )
    require(
        disjoint_row.get("inside") is False,
        "Expected Milan to be outside the Mediterranean box",
    )
    require(
        polygon_intersects_row.get("overlaps") is True,
        "Expected the two query polygons to intersect",
    )
    require(
        null_row.get("inside") is None,
        "Expected geo.within(null, polygon) to return null",
    )

    print(f"  Rome within central polygon: {within_row.get('inside')}")
    print(f"  Milan within central polygon: {disjoint_row.get('inside')}")
    print(
        "  Central polygon intersects extension polygon: "
        f"{polygon_intersects_row.get('overlaps')}"
    )
    print(f"  Null input handling: {null_row.get('inside')}")
    print()


def run_indexed_point_queries(db) -> None:
    print("Phase 2: Indexed point-in-polygon queries")
    print()

    within_rows = rows_to_dicts(
        db.query(
            "sql",
            "SELECT name, city, kind, daily_capacity FROM Depot "
            "WHERE geo.within(coords, geo.geomFromText(?)) = true ORDER BY city",
            MEDITERRANEAN_BOX,
        ),
        ["name", "city", "kind", "daily_capacity"],
    )
    intersects_rows = rows_to_dicts(
        db.query(
            "sql",
            "SELECT name, city, kind FROM Depot "
            "WHERE geo.intersects(coords, geo.geomFromText(?)) = true ORDER BY city",
            MEDITERRANEAN_BOX,
        ),
        ["name", "city", "kind"],
    )
    grouped_rows = rows_to_dicts(
        db.query(
            "sql",
            "SELECT kind, count(*) AS depot_count, sum(daily_capacity) AS capacity "
            "FROM Depot WHERE geo.within(coords, geo.geomFromText(?)) = true "
            "GROUP BY kind ORDER BY kind",
            MEDITERRANEAN_BOX,
        ),
        ["kind", "depot_count", "capacity"],
    )

    expected_cities = ["Florence", "Naples", "Rome"]
    require(
        [row["city"] for row in within_rows] == expected_cities,
        "Expected indexed geo.within query to return Florence, Naples, and Rome",
    )
    require(
        [row["city"] for row in intersects_rows] == expected_cities,
        "Expected indexed geo.intersects query to match the same point set",
    )
    require(
        len(grouped_rows) == 2,
        "Expected grouped geo.within results for two depot kinds",
    )

    print_rows("Indexed geo.within(coords, polygon) results:", within_rows)
    print_rows("Indexed geo.intersects(coords, polygon) results:", intersects_rows)
    print_rows("Capacity rollup inside the polygon:", grouped_rows)


def run_polygon_overlap_phase(db) -> None:
    print("Phase 3: Stored polygon overlap queries")
    print()

    overlapping_zones = rows_to_dicts(
        db.query(
            "sql",
            "SELECT zone_id, purpose FROM ServiceZone "
            "WHERE geo.intersects(area, geo.geomFromText(?)) = true ORDER BY zone_id",
            TYRRHENIAN_BOX,
        ),
        ["zone_id", "purpose"],
    )

    require(
        [row["zone_id"] for row in overlapping_zones] == ["zone-central", "zone-south"],
        "Expected extension polygon to intersect the central and south service zones",
    )

    print_rows(
        "Stored service-zone polygons intersecting the query polygon:",
        overlapping_zones,
    )


def run_reopen_phase(db_path: Path) -> None:
    print("Phase 4: Reopen verification")
    print()

    with arcadedb.open_database(str(db_path)) as reopened_db:
        exists = reopened_db.schema.exists_index("Depot[coords]")
        print(f"  Depot[coords] exists after reopen: {exists}")
        require(exists, "Expected Depot[coords] geospatial index to exist after reopen")
        print()

        reopened_rows = rows_to_dicts(
            reopened_db.query(
                "sql",
                "SELECT city, kind FROM Depot "
                "WHERE geo.within(coords, geo.geomFromText(?)) = true ORDER BY city",
                MEDITERRANEAN_BOX,
            ),
            ["city", "kind"],
        )
        require(
            [row["city"] for row in reopened_rows] == ["Florence", "Naples", "Rome"],
            "Expected reopened geospatial lookup to return Florence, Naples, and Rome",
        )
        print_rows("Reopened indexed geo.within query:", reopened_rows)


def run_fallback_phase(db_path: Path) -> None:
    print("Phase 5: Drop index and verify full-scan fallback")
    print()

    with arcadedb.open_database(str(db_path)) as db:
        db.command("sql", "DROP INDEX `Depot[coords]`")

        exists = db.schema.exists_index("Depot[coords]")
        print(f"  Depot[coords] exists after drop: {exists}")
        require(not exists, "Expected Depot[coords] index to be dropped")
        print()

        fallback_rows = rows_to_dicts(
            db.query(
                "sql",
                "SELECT city, kind FROM Depot "
                "WHERE geo.within(coords, geo.geomFromText(?)) = true ORDER BY city",
                MEDITERRANEAN_BOX,
            ),
            ["city", "kind"],
        )
        require(
            [row["city"] for row in fallback_rows] == ["Florence", "Naples", "Rome"],
            "Expected fallback geo.within query to return the same cities "
            "after dropping the index",
        )
        print_rows("Fallback geo.within query after dropping the index:", fallback_rows)


def main() -> int:
    args = parse_args()
    db_path = Path(args.db_path)

    print("=" * 72)
    print("ArcadeDB Python - Example 18: Geo Predicates With WKT Points And Polygons")
    print("=" * 72)
    print()

    reset_db(db_path)

    with arcadedb.create_database(str(db_path)) as db:
        print(f"Database created at: {db_path}")
        print("Approach: SQL-first geospatial workflow from Python bindings")
        print(
            "Focus: WKT points and polygons, indexed geo predicates, "
            "stored polygon overlaps, reopen verification, and full-scan fallback"
        )
        print()

        if not ensure_geo_support(db):
            return 0

        if not create_schema(db):
            return 0

        insert_seed_data(db)

        print(
            f"Inserted {len(DEPOTS)} Depot records and "
            f"{len(SERVICE_ZONES)} ServiceZone records"
        )
        print(
            "Reminder: GEOSPATIAL indexes accelerate spatial predicates "
            "but do not change SQL semantics."
        )
        print()

        print_geo_indexes(db)
        run_predicate_sanity_phase(db)
        run_indexed_point_queries(db)
        run_polygon_overlap_phase(db)

    run_reopen_phase(db_path)
    run_fallback_phase(db_path)

    print("Example 18 completed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
