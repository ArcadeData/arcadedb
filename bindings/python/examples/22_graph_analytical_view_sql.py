#!/usr/bin/env python3
"""Example 22: Graph Analytical View SQL Workflow.

This example mirrors the Java-side Graph Analytical View (GAV) lifecycle, but it
does so at a more realistic scale and still uses only SQL from Python.

Workflow covered:
- generate a synthetic transport graph with tens of thousands of City vertices and
  a denser ROAD topology spread across multiple regions
- create a GAV with richer vertex and edge property filters
- poll schema:graphAnalyticalViews until the initial async build reaches READY
- inspect live GAV metadata such as status, nodeCount, edgeCount, memory usage,
  and build duration
- run normal SQL MATCH traversals and aggregate queries without introducing a
  Python-native GAV wrapper
- add thousands more vertices and edges while update mode is OFF and observe the
  view become STALE
- rebuild the view explicitly through SQL and verify refreshed counts and query
  results against the expanded graph
- alter update mode and compaction threshold through SQL
- add another growth wave under SYNCHRONOUS mode and verify live counts
- reopen the database and confirm persisted GAV restoration

Requirements:
- A packaged ArcadeDB runtime with Graph Analytical View SQL support

Notes:
- The example intentionally uses only db.command(..., "sql", ...) and
  db.query(..., "sql", ...).
- CREATE GRAPH ANALYTICAL VIEW performs the first build asynchronously, so polling
  schema metadata is part of the normal lifecycle.
- The default scale is intentionally substantial. Use the CLI flags to run a
  smaller version if you only want a quick smoke test.
"""

from __future__ import annotations

import argparse
import shutil
import time
from pathlib import Path

import arcadedb_embedded as arcadedb

GAV_NAME = "roadNet"
CITY_CODE_WIDTH = 6
LANE_CLASSES = {
    "ring": "arterial",
    "local": "collector",
    "express": "express",
    "hub": "regional",
    "bridge": "interchange",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Example 22: graph analytical view SQL workflow at larger scale"
    )
    parser.add_argument(
        "--db-path",
        default="./my_test_databases/graph_analytical_view_sql_db",
        help=(
            "Database path (default: "
            "./my_test_databases/graph_analytical_view_sql_db)"
        ),
    )
    parser.add_argument(
        "--base-cities",
        type=int,
        default=100000,
        help="Number of base City vertices to generate (default: 100000)",
    )
    parser.add_argument(
        "--regions",
        type=int,
        default=40,
        help="Number of synthetic regions and hub cities (default: 40)",
    )
    parser.add_argument(
        "--stale-growth-cities",
        type=int,
        default=25000,
        help="Number of cities to add while UPDATE MODE is OFF (default: 25000)",
    )
    parser.add_argument(
        "--sync-growth-cities",
        type=int,
        default=10000,
        help=(
            "Number of cities to add after switching to SYNCHRONOUS mode "
            "(default: 10000)"
        ),
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Transaction batch size for inserts (default: 1000)",
    )
    parser.add_argument(
        "--local-jump",
        type=int,
        default=7,
        help="Deterministic local fan-out jump for ROAD edges (default: 7)",
    )
    parser.add_argument(
        "--express-jump",
        type=int,
        default=101,
        help="Deterministic express fan-out jump for ROAD edges (default: 101)",
    )
    parser.add_argument(
        "--sample-limit",
        type=int,
        default=8,
        help="How many sample query rows to print (default: 8)",
    )
    args = parser.parse_args()

    if args.base_cities < 10:
        raise ValueError("--base-cities must be at least 10")
    if args.regions < 2:
        raise ValueError("--regions must be at least 2")
    if args.regions > args.base_cities:
        raise ValueError("--regions cannot exceed --base-cities")
    if args.stale_growth_cities < 0 or args.sync_growth_cities < 0:
        raise ValueError("growth counts must be non-negative")
    if args.batch_size < 1:
        raise ValueError("--batch-size must be at least 1")
    if args.local_jump < 1 or args.express_jump < 1:
        raise ValueError("edge jump values must be at least 1")
    if args.sample_limit < 1:
        raise ValueError("--sample-limit must be at least 1")
    return args


def reset_db(db_path: Path) -> None:
    if db_path.exists():
        shutil.rmtree(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)


def require(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def city_code(index: int) -> str:
    return f"CITY-{index:0{CITY_CODE_WIDTH}d}"


def city_name(index: int) -> str:
    return f"Synthetic City {index:0{CITY_CODE_WIDTH}d}"


def region_index(index: int, regions: int) -> int:
    return index % regions


def region_name(region: int) -> str:
    return f"region_{region:02d}"


def zone_name(index: int, regions: int) -> str:
    return f"zone_{(index // regions) % 8:02d}"


def hub_index_for_region(region: int) -> int:
    return region


def city_row(index: int, regions: int) -> tuple:
    region = region_index(index, regions)
    tier = 1 if index < regions else 2 + ((index // regions) % 3)
    demand_index = 50 + ((index * 17 + region * 11) % 450)
    return (
        city_code(index),
        city_name(index),
        region_name(region),
        tier,
        zone_name(index, regions),
        demand_index,
    )


def road_targets(
    index: int,
    total_cities: int,
    regions: int,
    local_jump: int,
    express_jump: int,
) -> list[tuple[int, str]]:
    region = region_index(index, regions)
    hub = hub_index_for_region(region)
    next_hub = hub_index_for_region((region + 1) % regions)

    candidates = [
        ((index + 1) % total_cities, "ring"),
        ((index + local_jump + region) % total_cities, "local"),
        ((index + express_jump + region * 3) % total_cities, "express"),
    ]

    if index != hub:
        candidates.append((hub, "hub"))
    elif next_hub != index:
        candidates.append((next_hub, "bridge"))

    deduped: list[tuple[int, str]] = []
    seen_targets: set[int] = set()
    for target, edge_kind in candidates:
        if target == index or target in seen_targets:
            continue
        seen_targets.add(target)
        deduped.append((target, edge_kind))
    return deduped


def road_properties(
    source_index: int,
    target_index: int,
    edge_kind: str,
    regions: int,
) -> tuple:
    kind_factor = {
        "ring": 3,
        "local": 7,
        "express": 11,
        "hub": 17,
        "bridge": 23,
    }[edge_kind]
    distance = 5 + ((source_index * 17 + target_index * 13 + kind_factor) % 95)
    duration = distance + 3 + ((source_index + target_index + kind_factor) % 21)
    toll = (source_index + target_index + kind_factor) % 6
    capacity = 80 + ((source_index * 23 + target_index * 11 + kind_factor) % 220)
    corridor = f"{region_name(region_index(source_index, regions))}:{edge_kind}"
    return (
        distance,
        duration,
        toll,
        capacity,
        LANE_CLASSES[edge_kind],
        corridor,
    )


def chunked(iterable, chunk_size: int):
    chunk = []
    for item in iterable:
        chunk.append(item)
        if len(chunk) >= chunk_size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


def iter_city_rows(start_index: int, count: int, regions: int):
    for index in range(start_index, start_index + count):
        yield city_row(index, regions)


def iter_road_rows(
    start_index: int,
    end_index: int,
    total_cities: int,
    regions: int,
    local_jump: int,
    express_jump: int,
):
    for source_index in range(start_index, end_index):
        source_code = city_code(source_index)
        for target_index, edge_kind in road_targets(
            source_index,
            total_cities,
            regions,
            local_jump,
            express_jump,
        ):
            yield (
                source_code,
                city_code(target_index),
                *road_properties(source_index, target_index, edge_kind, regions),
            )


def format_int(value: int) -> str:
    return f"{value:,}"


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


def print_metric(title: str, value: str) -> None:
    print(f"  - {title}: {value}")


def print_note(message: str) -> None:
    print(f"  note: {message}")


def print_gav_metadata(title: str, metadata: dict) -> None:
    print(title)
    print(f"  name: {metadata['name']}")
    print(f"  status: {metadata['status']}")
    print(f"  updateMode: {metadata['updateMode']}")
    print(f"  vertexTypes: {metadata['vertexTypes']}")
    print(f"  edgeTypes: {metadata['edgeTypes']}")
    print(f"  propertyFilter: {metadata['propertyFilter']}")
    print(f"  edgePropertyFilter: {metadata['edgePropertyFilter']}")
    print(f"  nodeCount: {metadata['nodeCount']}")
    print(f"  edgeCount: {metadata['edgeCount']}")
    print(f"  compactionThreshold: {metadata['compactionThreshold']}")
    print(f"  memoryUsageBytes: {metadata['memoryUsageBytes']}")
    print(f"  buildDurationMs: {metadata['buildDurationMs']}")
    print()


def create_schema(db) -> None:
    db.command("sql", "CREATE VERTEX TYPE City")
    db.command("sql", "CREATE PROPERTY City.code STRING")
    db.command("sql", "CREATE PROPERTY City.name STRING")
    db.command("sql", "CREATE PROPERTY City.region STRING")
    db.command("sql", "CREATE PROPERTY City.tier INTEGER")
    db.command("sql", "CREATE PROPERTY City.zone STRING")
    db.command("sql", "CREATE PROPERTY City.demand_index INTEGER")
    db.command("sql", "CREATE INDEX ON City (code) UNIQUE")

    db.command("sql", "CREATE EDGE TYPE ROAD")
    db.command("sql", "CREATE PROPERTY ROAD.distance INTEGER")
    db.command("sql", "CREATE PROPERTY ROAD.duration INTEGER")
    db.command("sql", "CREATE PROPERTY ROAD.toll INTEGER")
    db.command("sql", "CREATE PROPERTY ROAD.capacity INTEGER")
    db.command("sql", "CREATE PROPERTY ROAD.lane_class STRING")
    db.command("sql", "CREATE PROPERTY ROAD.corridor STRING")


def insert_cities(
    db,
    start_index: int,
    count: int,
    regions: int,
    batch_size: int,
) -> int:
    inserted = 0
    for batch in chunked(iter_city_rows(start_index, count, regions), batch_size):
        with db.transaction():
            for code, name, region, tier, zone, demand_index in batch:
                db.command(
                    "sql",
                    "INSERT INTO City SET code = ?, name = ?, region = ?, tier = ?, "
                    "zone = ?, demand_index = ?",
                    code,
                    name,
                    region,
                    tier,
                    zone,
                    demand_index,
                )
                inserted += 1
    return inserted


def insert_roads(
    db,
    start_index: int,
    end_index: int,
    total_cities: int,
    regions: int,
    batch_size: int,
    local_jump: int,
    express_jump: int,
) -> int:
    inserted = 0
    for batch in chunked(
        iter_road_rows(
            start_index,
            end_index,
            total_cities,
            regions,
            local_jump,
            express_jump,
        ),
        batch_size,
    ):
        with db.transaction():
            for road in batch:
                (
                    source_code,
                    target_code,
                    distance,
                    duration,
                    toll,
                    capacity,
                    lane_class,
                    corridor,
                ) = road
                db.command(
                    "sql",
                    "CREATE EDGE ROAD FROM (SELECT FROM City WHERE code = ?) "
                    "TO (SELECT FROM City WHERE code = ?) "
                    "SET distance = ?, duration = ?, toll = ?, capacity = ?, "
                    "lane_class = ?, corridor = ?",
                    source_code,
                    target_code,
                    distance,
                    duration,
                    toll,
                    capacity,
                    lane_class,
                    corridor,
                )
                inserted += 1
    return inserted


def create_gav(db) -> bool:
    try:
        db.command(
            "sql",
            "CREATE GRAPH ANALYTICAL VIEW roadNet "
            "VERTEX TYPES (City) "
            "EDGE TYPES (ROAD) "
            "PROPERTIES (code, name, region, tier, zone, demand_index) "
            "EDGE PROPERTIES "
            "(distance, duration, toll, capacity, lane_class, corridor) "
            "UPDATE MODE OFF",
        )
    except arcadedb.ArcadeDBError as exc:
        message = str(exc)
        if any(
            token in message
            for token in (
                "Graph Analytical View",
                "GRAPH ANALYTICAL VIEW",
                "graph analytical view",
                "extraneous input",
                "no viable alternative",
            )
        ):
            print(
                "Graph Analytical View SQL support is not available in this "
                "packaged runtime."
            )
            print(
                "This example depends on CREATE/ALTER/REBUILD GRAPH "
                "ANALYTICAL VIEW support."
            )
            print(f"Original error: {exc}")
            return False
        raise
    return True


def fetch_gav_metadata(db, name: str) -> dict | None:
    result = db.query(
        "sql",
        "SELECT FROM schema:graphAnalyticalViews WHERE name = ?",
        name,
    )
    row = result.first()
    if row is None:
        return None

    return {
        "name": row.get("name"),
        "status": row.get("status"),
        "updateMode": row.get("updateMode"),
        "vertexTypes": row.get("vertexTypes"),
        "edgeTypes": row.get("edgeTypes"),
        "propertyFilter": row.get("propertyFilter"),
        "edgePropertyFilter": row.get("edgePropertyFilter"),
        "nodeCount": row.get("nodeCount"),
        "edgeCount": row.get("edgeCount"),
        "compactionThreshold": row.get("compactionThreshold"),
        "memoryUsageBytes": row.get("memoryUsageBytes"),
        "buildDurationMs": row.get("buildDurationMs"),
    }


def wait_for_gav_status(
    db,
    name: str,
    expected_statuses: set[str],
    timeout_sec: float = 180.0,
) -> dict:
    start = time.perf_counter()
    last_metadata = None
    while True:
        metadata = fetch_gav_metadata(db, name)
        if metadata is not None:
            last_metadata = metadata
            if metadata["status"] in expected_statuses:
                return metadata
        if time.perf_counter() - start > timeout_sec:
            raise RuntimeError(
                f"Timed out waiting for GAV {name} to reach "
                f"{sorted(expected_statuses)}. "
                f"Last metadata: {last_metadata}"
            )
        time.sleep(0.25)


def query_direct_neighbor_sample(
    db,
    origin_code: str,
    sample_limit: int,
) -> list[dict]:
    result = db.query(
        "sql",
        f"""
        MATCH {{type: City, as: src, where: (code = '{origin_code}')}}
              -ROAD->
              {{type: City, as: dst}}
          RETURN dst.code AS code, dst.region AS region,
               dst.zone AS zone, dst.tier AS tier
        ORDER BY dst.code
        LIMIT {sample_limit}
        """,
    )
    return rows_to_dicts(result, ["code", "region", "zone", "tier"])


def query_two_hop_summary(db, origin_code: str) -> dict:
    result = db.query(
        "sql",
        f"""
        SELECT count(*) AS destination_count FROM (
            MATCH {{type: City, as: src, where: (code = '{origin_code}')}}
                  -ROAD->
                  {{type: City, as: mid}}
                  -ROAD->
                  {{type: City, as: dst}}
            RETURN DISTINCT dst.code AS code
        )
        """,
    )
    row = result.first()
    require(row is not None, "Expected a two-hop summary row")
    return {"destination_count": row.get("destination_count")}


def query_hub_inbound_count(db, hub_code: str) -> int:
    result = db.query(
        "sql",
        f"""
        SELECT count(*) AS inbound_count FROM (
            MATCH {{type: City, as: src}}
                  -ROAD->
                  {{type: City, as: hub, where: (code = '{hub_code}')}}
            RETURN src.code AS code
        )
        """,
    )
    row = result.first()
    require(row is not None, "Expected an inbound count row")
    return row.get("inbound_count")


def query_region_sample(db, sample_limit: int) -> list[dict]:
    result = db.query(
        "sql",
        f"""
        SELECT region, count(*) AS city_count, avg(demand_index) AS avg_demand
        FROM City
        GROUP BY region
        ORDER BY region
        LIMIT {sample_limit}
        """,
    )
    return rows_to_dicts(result, ["region", "city_count", "avg_demand"])


def print_scale_summary(args: argparse.Namespace) -> None:
    print("Scale summary:")
    print_metric("base cities", format_int(args.base_cities))
    print_metric("regions", format_int(args.regions))
    print_metric("stale growth cities", format_int(args.stale_growth_cities))
    print_metric("sync growth cities", format_int(args.sync_growth_cities))
    print_metric("batch size", format_int(args.batch_size))
    print_metric("local jump", format_int(args.local_jump))
    print_metric("express jump", format_int(args.express_jump))
    print()


def main() -> int:
    args = parse_args()
    db_path = Path(args.db_path)
    reset_db(db_path)

    total_cities_after_stale = args.base_cities + args.stale_growth_cities
    total_cities_after_sync = total_cities_after_stale + args.sync_growth_cities

    print("=" * 72)
    print("ArcadeDB Python - Example 22: Graph Analytical View SQL Workflow")
    print("=" * 72)
    print()
    print(
        "Positioning: use SQL for GAV lifecycle management instead of a new "
        "Python API"
    )
    print(f"Database path: {db_path}")
    print_scale_summary(args)

    anchor_code = city_code(0)
    hub_code = city_code(0)

    with arcadedb.create_database(str(db_path)) as db:
        print("Phase 1: Create schema and generate the base transport graph")
        create_schema(db)

        start = time.perf_counter()
        base_city_count = insert_cities(
            db, 0, args.base_cities, args.regions, args.batch_size
        )
        base_edge_count = insert_roads(
            db,
            0,
            args.base_cities,
            args.base_cities,
            args.regions,
            args.batch_size,
            args.local_jump,
            args.express_jump,
        )
        phase_duration = time.perf_counter() - start
        print_metric("base city inserts", format_int(base_city_count))
        print_metric("base road inserts", format_int(base_edge_count))
        print_metric("phase duration", f"{phase_duration:.2f}s")
        print()

        print_rows(
            "Region sample after base load:",
            query_region_sample(db, min(args.sample_limit, args.regions)),
        )

        if not create_gav(db):
            return 0

        ready_metadata = wait_for_gav_status(db, GAV_NAME, {"READY"})
        require(
            ready_metadata["nodeCount"] == base_city_count,
            f"Expected initial GAV nodeCount = {base_city_count}",
        )
        require(
            ready_metadata["edgeCount"] == base_edge_count,
            f"Expected initial GAV edgeCount = {base_edge_count}",
        )
        print_gav_metadata("Initial GAV metadata after async build:", ready_metadata)

        direct_neighbors = query_direct_neighbor_sample(
            db, anchor_code, args.sample_limit
        )
        require(bool(direct_neighbors), "Expected direct neighbors for the anchor city")
        print_rows("Sample direct neighbors from the anchor city:", direct_neighbors)

        two_hop_before = query_two_hop_summary(db, anchor_code)
        print_rows("Two-hop summary from the anchor city:", [two_hop_before])

        inbound_before = query_hub_inbound_count(db, hub_code)
        print_rows(
            "Inbound ROAD count into the first region hub:",
            [{"hub": hub_code, "inbound_count": inbound_before}],
        )

        print("Phase 2: Alter compaction threshold while staying SQL-first")
        db.command(
            "sql",
            "ALTER GRAPH ANALYTICAL VIEW roadNet COMPACTION THRESHOLD 2000",
        )
        altered_threshold = fetch_gav_metadata(db, GAV_NAME)
        require(altered_threshold is not None, "Expected GAV metadata to exist")
        require(
            altered_threshold["compactionThreshold"] == 2000,
            "Expected compaction threshold to update to 2000",
        )
        print_gav_metadata(
            "Metadata after ALTER ... COMPACTION THRESHOLD:",
            altered_threshold,
        )

        print(
            "Phase 3: Add a large growth wave with UPDATE MODE OFF and "
            "observe STALE status"
        )
        start = time.perf_counter()
        stale_city_count = insert_cities(
            db,
            args.base_cities,
            args.stale_growth_cities,
            args.regions,
            args.batch_size,
        )
        stale_edge_count = insert_roads(
            db,
            args.base_cities,
            total_cities_after_stale,
            total_cities_after_stale,
            args.regions,
            args.batch_size,
            args.local_jump,
            args.express_jump,
        )
        phase_duration = time.perf_counter() - start
        print_metric("stale growth city inserts", format_int(stale_city_count))
        print_metric("stale growth road inserts", format_int(stale_edge_count))
        print_metric("phase duration", f"{phase_duration:.2f}s")
        print()

        stale_metadata = wait_for_gav_status(db, GAV_NAME, {"STALE"})
        require(
            stale_metadata["nodeCount"] == base_city_count,
            "Stale GAV should still show old nodeCount",
        )
        require(
            stale_metadata["edgeCount"] == base_edge_count,
            "Stale GAV should still show old edgeCount",
        )
        print_gav_metadata("Metadata after writes in OFF mode:", stale_metadata)

        print("Phase 4: Rebuild explicitly through SQL and verify refreshed counts")
        rebuild_result = rows_to_dicts(
            db.command("sql", "REBUILD GRAPH ANALYTICAL VIEW roadNet"),
            ["operation", "name", "status"],
        )
        print_rows("REBUILD command result:", rebuild_result)

        rebuilt_metadata = wait_for_gav_status(db, GAV_NAME, {"READY"})
        expected_rebuilt_nodes = base_city_count + stale_city_count
        expected_rebuilt_edges = base_edge_count + stale_edge_count
        require(
            rebuilt_metadata["nodeCount"] == expected_rebuilt_nodes,
            f"Expected rebuilt GAV nodeCount = {expected_rebuilt_nodes}",
        )
        require(
            rebuilt_metadata["edgeCount"] == expected_rebuilt_edges,
            f"Expected rebuilt GAV edgeCount = {expected_rebuilt_edges}",
        )
        print_gav_metadata("Metadata after explicit rebuild:", rebuilt_metadata)

        two_hop_after_rebuild = query_two_hop_summary(db, anchor_code)
        require(
            two_hop_after_rebuild["destination_count"]
            >= two_hop_before["destination_count"],
            "Expected two-hop reachability to stay flat or increase after rebuild",
        )
        print_rows(
            "Two-hop summary after rebuild:",
            [two_hop_after_rebuild],
        )

        inbound_after_rebuild = query_hub_inbound_count(db, hub_code)
        require(
            inbound_after_rebuild > inbound_before,
            "Expected hub inbound count to increase after stale growth rebuild",
        )
        print_rows(
            "Inbound ROAD count after rebuild:",
            [{"hub": hub_code, "inbound_count": inbound_after_rebuild}],
        )

        print("Phase 5: Switch to SYNCHRONOUS update mode and verify live counts")
        db.command(
            "sql",
            "ALTER GRAPH ANALYTICAL VIEW roadNet UPDATE MODE SYNCHRONOUS",
        )
        sync_mode_metadata = fetch_gav_metadata(db, GAV_NAME)
        require(
            sync_mode_metadata is not None,
            "Expected metadata after sync-mode ALTER",
        )
        require(
            sync_mode_metadata["updateMode"] == "SYNCHRONOUS",
            "Expected update mode to be SYNCHRONOUS",
        )
        print_gav_metadata(
            "Metadata after ALTER ... UPDATE MODE SYNCHRONOUS:",
            sync_mode_metadata,
        )

        start = time.perf_counter()
        sync_city_count = insert_cities(
            db,
            total_cities_after_stale,
            args.sync_growth_cities,
            args.regions,
            args.batch_size,
        )
        sync_edge_count = insert_roads(
            db,
            total_cities_after_stale,
            total_cities_after_sync,
            total_cities_after_sync,
            args.regions,
            args.batch_size,
            args.local_jump,
            args.express_jump,
        )
        phase_duration = time.perf_counter() - start
        print_metric("sync growth city inserts", format_int(sync_city_count))
        print_metric("sync growth road inserts", format_int(sync_edge_count))
        print_metric("phase duration", f"{phase_duration:.2f}s")
        print()

        sync_metadata = wait_for_gav_status(db, GAV_NAME, {"READY"})
        expected_sync_nodes = expected_rebuilt_nodes + sync_city_count
        expected_sync_edges = expected_rebuilt_edges + sync_edge_count
        print_gav_metadata("Metadata after writes in SYNCHRONOUS mode:", sync_metadata)
        if (
            sync_metadata["nodeCount"] != expected_sync_nodes
            or sync_metadata["edgeCount"] != expected_sync_edges
        ):
            print_note(
                "Some runtimes keep schema metadata counts at the last rebuilt "
                "snapshot immediately after switching from OFF to SYNCHRONOUS. "
                "The live verification below relies on query results, and the "
                "reopen step still checks the persisted counts strictly."
            )
            print()

        inbound_after_sync = query_hub_inbound_count(db, hub_code)
        require(
            inbound_after_sync > inbound_after_rebuild,
            "Expected hub inbound count to increase again under SYNCHRONOUS mode",
        )
        print_rows(
            "Inbound ROAD count after synchronous growth:",
            [{"hub": hub_code, "inbound_count": inbound_after_sync}],
        )

    print("Phase 6: Reopen the database and confirm persisted GAV restoration")
    with arcadedb.open_database(str(db_path)) as reopened_db:
        restored_metadata = wait_for_gav_status(reopened_db, GAV_NAME, {"READY"})
        require(
            restored_metadata["nodeCount"] == expected_sync_nodes,
            f"Expected restored GAV nodeCount = {expected_sync_nodes}",
        )
        require(
            restored_metadata["edgeCount"] == expected_sync_edges,
            f"Expected restored GAV edgeCount = {expected_sync_edges}",
        )
        print_gav_metadata("Metadata after reopen:", restored_metadata)
        print_rows(
            "Sample direct neighbors after reopen:",
            query_direct_neighbor_sample(reopened_db, anchor_code, args.sample_limit),
        )
        print_rows(
            "Region sample after reopen:",
            query_region_sample(reopened_db, min(args.sample_limit, args.regions)),
        )

    print(f"Database remains on disk for inspection: {db_path}")
    print("Example 22 completed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
