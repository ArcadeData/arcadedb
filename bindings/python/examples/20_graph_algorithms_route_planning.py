#!/usr/bin/env python3
"""Example 20: Graph Algorithms Route Planning.

This example stays SQL-first, but the workflow is intentionally more involved than the
baseline pathfinding demo. It models a small multi-modal logistics network and uses
ArcadeDB graph algorithms to answer different routing questions from Python bindings.

Workflow covered:
- create a directed transport graph with Road, Rail, and Ferry edges
- compare minimum-hop shortestPath() against weighted dijkstra() and astar()
- optimize for different edge cost properties: distance, duration, and risk
- cross-check astar() against dijkstra() for multiple weighted objectives
- demonstrate direction-sensitive routing with OUT vs BOTH traversal
- use sqlscript variables with both vertex values and @rid inputs
- inspect the returned RID path as segment-level route summaries
- reopen the database and rerun weighted and constrained queries

Requirements:
- A packaged ArcadeDB runtime that supports shortestPath(), dijkstra(), and
  astar() SQL functions

Notes:
- shortestPath() is minimum-hop and ignores edge weights.
- dijkstra() and astar() can optimize different edge properties.
- astar() supports an options map for heuristics, direction, and other
    weighted-routing controls.
"""

from __future__ import annotations

import argparse
import shutil
from pathlib import Path

import arcadedb_embedded as arcadedb

CITIES = [
    {"code": "ALPHA", "name": "Alpha Hub", "lat": 0.0, "lon": 0.0},
    {"code": "BRAVO", "name": "Bravo Junction", "lat": 1.0, "lon": 0.0},
    {"code": "CHARLIE", "name": "Charlie Market", "lat": 2.0, "lon": 0.0},
    {"code": "DELTA", "name": "Delta Port", "lat": 3.0, "lon": 0.0},
    {"code": "ECHO", "name": "Echo Rail Yard", "lat": 1.8, "lon": 0.6},
    {"code": "FOXTROT", "name": "Foxtrot Ferry Dock", "lat": 1.3, "lon": 1.2},
    {"code": "GHOST", "name": "Ghost Outpost", "lat": 5.0, "lon": 5.0},
    {"code": "HOTEL", "name": "Hotel Service Depot", "lat": 3.4, "lon": 0.5},
]


ROUTES = [
    {
        "edge_type": "Road",
        "from": "ALPHA",
        "to": "DELTA",
        "distance": 20,
        "duration": 18,
        "risk": 1,
        "lane": "express-direct",
    },
    {
        "edge_type": "Road",
        "from": "ALPHA",
        "to": "BRAVO",
        "distance": 2,
        "duration": 5,
        "risk": 2,
        "lane": "corridor",
    },
    {
        "edge_type": "Road",
        "from": "BRAVO",
        "to": "CHARLIE",
        "distance": 2,
        "duration": 5,
        "risk": 2,
        "lane": "corridor",
    },
    {
        "edge_type": "Road",
        "from": "CHARLIE",
        "to": "DELTA",
        "distance": 2,
        "duration": 5,
        "risk": 2,
        "lane": "corridor",
    },
    {
        "edge_type": "Road",
        "from": "ALPHA",
        "to": "FOXTROT",
        "distance": 3,
        "duration": 7,
        "risk": 4,
        "lane": "scenic-link",
    },
    {
        "edge_type": "Road",
        "from": "DELTA",
        "to": "HOTEL",
        "distance": 2,
        "duration": 4,
        "risk": 1,
        "lane": "service-spur",
    },
    {
        "edge_type": "Rail",
        "from": "ALPHA",
        "to": "ECHO",
        "distance": 4,
        "duration": 2,
        "risk": 3,
        "lane": "rail-express",
    },
    {
        "edge_type": "Rail",
        "from": "ECHO",
        "to": "DELTA",
        "distance": 4,
        "duration": 2,
        "risk": 3,
        "lane": "rail-express",
    },
    {
        "edge_type": "Rail",
        "from": "BRAVO",
        "to": "ECHO",
        "distance": 3,
        "duration": 3,
        "risk": 3,
        "lane": "rail-connector",
    },
    {
        "edge_type": "Ferry",
        "from": "FOXTROT",
        "to": "DELTA",
        "distance": 5,
        "duration": 9,
        "risk": 5,
        "lane": "harbor-ferry",
    },
    {
        "edge_type": "Ferry",
        "from": "BRAVO",
        "to": "FOXTROT",
        "distance": 6,
        "duration": 6,
        "risk": 5,
        "lane": "barge-transfer",
    },
]

ROUTE_BY_PAIR = {(route["from"], route["to"]): route.copy() for route in ROUTES}
WEIGHT_FIELDS = ("distance", "duration", "risk")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Example 20: graph algorithms route planning workflow"
    )
    parser.add_argument(
        "--db-path",
        default="./my_test_databases/graph_algorithms_route_planning_db",
        help=(
            "Database path (default: "
            "./my_test_databases/graph_algorithms_route_planning_db)"
        ),
    )
    return parser.parse_args()


def reset_db(db_path: Path) -> None:
    if db_path.exists():
        shutil.rmtree(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)


def require(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


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


def print_route_summary(title: str, summary: dict) -> None:
    print(title)
    print(f"  Codes: {' -> '.join(summary['codes']) if summary['codes'] else '[]'}")
    print(f"  Modes: {summary['modes']}")
    print(f"  Hops: {summary['hops']}")
    print(
        "  Totals: "
        f"distance={summary['distance']}, "
        f"duration={summary['duration']}, "
        f"risk={summary['risk']}"
    )
    print("  Segments:")
    if not summary["segments"]:
        print("    (no segments)")
    else:
        for segment in summary["segments"]:
            print(
                "    - "
                f"{segment['from']} -> {segment['to']} "
                f"[{segment['edge_type']} / {segment['lane']}] "
                f"distance={segment['distance']} "
                f"duration={segment['duration']} "
                f"risk={segment['risk']} "
                f"reverse={segment['reverse_traversal']}"
            )
    print()


def ensure_graph_algo_support(db) -> bool:
    try:
        script = """
        LET $src = (SELECT FROM City WHERE code = 'ALPHA' LIMIT 1);
        LET $dst = (SELECT FROM City WHERE code = 'DELTA' LIMIT 1);
        SELECT shortestPath($src.@rid, $dst.@rid) AS path FROM City LIMIT 1;
        """
        path = get_path_from_sqlscript(db, script)
        require(
            path is not None,
            "Expected shortestPath() probe to return a path column",
        )
        return True
    except arcadedb.ArcadeDBError as exc:
        message = str(exc)
        if any(
            token in message
            for token in (
                "Unknown method name: shortestPath",
                "Unknown method name: dijkstra",
                "Unknown method name: astar",
            )
        ):
            print(
                "Graph algorithm SQL functions are not available in this "
                "packaged runtime."
            )
            print("This example depends on shortestPath(), dijkstra(), and astar().")
            print(f"Original error: {exc}")
            return False
        raise


def create_schema(db) -> None:
    db.command("sql", "CREATE VERTEX TYPE City")
    db.command("sql", "CREATE PROPERTY City.code STRING")
    db.command("sql", "CREATE PROPERTY City.name STRING")
    db.command("sql", "CREATE PROPERTY City.lat DOUBLE")
    db.command("sql", "CREATE PROPERTY City.lon DOUBLE")

    for edge_type in ("Road", "Rail", "Ferry"):
        db.command("sql", f"CREATE EDGE TYPE {edge_type}")
        db.command("sql", f"CREATE PROPERTY {edge_type}.distance LONG")
        db.command("sql", f"CREATE PROPERTY {edge_type}.duration LONG")
        db.command("sql", f"CREATE PROPERTY {edge_type}.risk LONG")
        db.command("sql", f"CREATE PROPERTY {edge_type}.lane STRING")

    db.command("sql", "CREATE INDEX ON City (code) UNIQUE_HASH")


def insert_seed_data(db) -> None:
    with db.transaction():
        for city in CITIES:
            db.command(
                "sql",
                "INSERT INTO City SET code = ?, name = ?, lat = ?, lon = ?",
                city["code"],
                city["name"],
                city["lat"],
                city["lon"],
            )

        for route in ROUTES:
            db.command(
                "sql",
                f"CREATE EDGE {route['edge_type']} "
                "FROM (SELECT FROM City WHERE code = ? LIMIT 1) "
                "TO (SELECT FROM City WHERE code = ? LIMIT 1) "
                "SET distance = ?, duration = ?, risk = ?, lane = ?",
                route["from"],
                route["to"],
                route["distance"],
                route["duration"],
                route["risk"],
                route["lane"],
            )


def get_path_from_sqlscript(db, script: str) -> list | None:
    result = db.command("sqlscript", script)
    path = None
    for row in result:
        if "path" in row.property_names:
            path = row.get("path")
    return path


def rid_list_to_codes(db, path: list | None) -> list[str]:
    if not path:
        return []

    codes = []
    for rid in path:
        row = db.query("sql", "SELECT code FROM City WHERE @rid = ?", rid).first()
        require(row is not None, f"Expected City lookup by RID {rid} to succeed")
        codes.append(row.get("code"))
    return codes


def get_segments_for_codes(codes: list[str]) -> list[dict]:
    segments = []
    for start_code, end_code in zip(codes, codes[1:]):
        reverse_traversal = False
        if (start_code, end_code) in ROUTE_BY_PAIR:
            route = ROUTE_BY_PAIR[(start_code, end_code)]
        elif (end_code, start_code) in ROUTE_BY_PAIR:
            route = ROUTE_BY_PAIR[(end_code, start_code)]
            reverse_traversal = True
        else:
            raise RuntimeError(
                f"Expected seeded route {start_code} -> {end_code} to exist"
            )

        segments.append(
            {
                "from": start_code,
                "to": end_code,
                "edge_type": route["edge_type"],
                "lane": route["lane"],
                "distance": int(route["distance"]),
                "duration": int(route["duration"]),
                "risk": int(route["risk"]),
                "reverse_traversal": reverse_traversal,
            }
        )
    return segments


def summarize_path(db, label: str, path: list | None) -> dict:
    codes = rid_list_to_codes(db, path)
    segments = get_segments_for_codes(codes)
    summary = {
        "algorithm": label,
        "codes": codes,
        "hops": max(0, len(codes) - 1),
        "segments": segments,
        "modes": [segment["edge_type"] for segment in segments],
    }
    for field in WEIGHT_FIELDS:
        summary[field] = sum(segment[field] for segment in segments)
    return summary


def run_algorithm_phase(db) -> None:
    print("Phase 1: Minimum-hop vs weighted routing")
    print()

    shortest_path = get_path_from_sqlscript(
        db,
        """
        LET $src = (SELECT FROM City WHERE code = 'ALPHA' LIMIT 1);
        LET $dst = (SELECT FROM City WHERE code = 'DELTA' LIMIT 1);
        SELECT shortestPath($src.@rid, $dst.@rid) AS path FROM City LIMIT 1;
        """,
    )
    dijkstra_path = get_path_from_sqlscript(
        db,
        """
        LET $src = (SELECT FROM City WHERE code = 'ALPHA' LIMIT 1);
        LET $dst = (SELECT FROM City WHERE code = 'DELTA' LIMIT 1);
        SELECT dijkstra($src, $dst, 'distance') AS path FROM City LIMIT 1;
        """,
    )
    astar_path = get_path_from_sqlscript(
        db,
        """
        LET $src = (SELECT FROM City WHERE code = 'ALPHA' LIMIT 1);
        LET $dst = (SELECT FROM City WHERE code = 'DELTA' LIMIT 1);
        SELECT astar(
          $src,
          $dst,
          'distance',
          {
            direction: 'out',
            'parallel': true,
            edgeTypeNames: ['Road'],
            vertexAxisNames: ['lat', 'lon'],
            heuristicFormula: 'EUCLIDEANNOSQR'
          }
        ) AS path FROM City LIMIT 1;
        """,
    )

    shortest_summary = summarize_path(db, "shortestPath", shortest_path)
    dijkstra_summary = summarize_path(db, "dijkstra-distance", dijkstra_path)
    astar_summary = summarize_path(db, "astar-distance-road-only", astar_path)

    require(
        shortest_summary["codes"] == ["ALPHA", "DELTA"],
        "Expected shortestPath() to choose the direct one-hop route",
    )
    require(
        dijkstra_summary["codes"] == ["ALPHA", "BRAVO", "CHARLIE", "DELTA"],
        "Expected dijkstra(distance) to choose the cheaper corridor",
    )
    require(
        astar_summary["codes"] == ["ALPHA", "BRAVO", "CHARLIE", "DELTA"],
        "Expected road-only astar(distance) to match the corridor route",
    )
    require(
        shortest_summary["distance"] == 20,
        "Expected direct shortestPath() route to cost 20 distance",
    )
    require(
        dijkstra_summary["distance"] == 6,
        "Expected dijkstra(distance) route to cost 6 distance",
    )
    require(
        astar_summary["distance"] == 6,
        "Expected road-only astar(distance) route to cost 6 distance",
    )

    print_route_summary("shortestPath() summary:", shortest_summary)
    print_route_summary("dijkstra(distance) summary:", dijkstra_summary)
    print_route_summary("astar(distance, road-only) summary:", astar_summary)
    print("Interpretation:")
    print("  shortestPath() minimizes hop count, so it takes the direct express road.")
    print(
        "  dijkstra() and astar() optimize the requested weight field, so "
        "they accept more hops to reduce total distance."
    )
    print()


def run_objective_phase(db) -> None:
    print("Phase 2: Different business objectives on the same graph")
    print()

    distance_path = get_path_from_sqlscript(
        db,
        """
        LET $src = (SELECT FROM City WHERE code = 'ALPHA' LIMIT 1);
        LET $dst = (SELECT FROM City WHERE code = 'DELTA' LIMIT 1);
        SELECT dijkstra($src, $dst, 'distance') AS path FROM City LIMIT 1;
        """,
    )
    duration_path = get_path_from_sqlscript(
        db,
        """
        LET $src = (SELECT FROM City WHERE code = 'ALPHA' LIMIT 1);
        LET $dst = (SELECT FROM City WHERE code = 'DELTA' LIMIT 1);
        SELECT dijkstra($src, $dst, 'duration') AS path FROM City LIMIT 1;
        """,
    )
    risk_path = get_path_from_sqlscript(
        db,
        """
        LET $src = (SELECT FROM City WHERE code = 'ALPHA' LIMIT 1);
        LET $dst = (SELECT FROM City WHERE code = 'DELTA' LIMIT 1);
        SELECT dijkstra($src, $dst, 'risk') AS path FROM City LIMIT 1;
        """,
    )

    distance_summary = summarize_path(db, "distance-optimal", distance_path)
    duration_summary = summarize_path(db, "duration-optimal", duration_path)
    risk_summary = summarize_path(db, "risk-optimal", risk_path)

    require(
        distance_summary["codes"] == ["ALPHA", "BRAVO", "CHARLIE", "DELTA"],
        "Expected distance-optimal route to follow the road corridor",
    )
    require(
        duration_summary["codes"] == ["ALPHA", "ECHO", "DELTA"],
        "Expected duration-optimal route to use the rail express",
    )
    require(
        risk_summary["codes"] == ["ALPHA", "DELTA"],
        "Expected risk-optimal route to use the direct low-risk road",
    )

    print_route_summary("dijkstra(distance) summary:", distance_summary)
    print_route_summary("dijkstra(duration) summary:", duration_summary)
    print_route_summary("dijkstra(risk) summary:", risk_summary)
    print_rows(
        "Objective totals:",
        [
            {
                "objective": summary["algorithm"],
                "codes": summary["codes"],
                "distance": summary["distance"],
                "duration": summary["duration"],
                "risk": summary["risk"],
            }
            for summary in (distance_summary, duration_summary, risk_summary)
        ],
    )


def run_astar_crosscheck_phase(db) -> None:
    print("Phase 3: A* vs Dijkstra cross-checks")
    print()

    dijkstra_duration_path = get_path_from_sqlscript(
        db,
        """
        LET $src = (SELECT FROM City WHERE code = 'ALPHA' LIMIT 1);
        LET $dst = (SELECT FROM City WHERE code = 'DELTA' LIMIT 1);
        SELECT dijkstra($src, $dst, 'duration') AS path FROM City LIMIT 1;
        """,
    )
    astar_duration_path = get_path_from_sqlscript(
        db,
        """
        LET $src = (SELECT FROM City WHERE code = 'ALPHA' LIMIT 1);
        LET $dst = (SELECT FROM City WHERE code = 'DELTA' LIMIT 1);
        SELECT astar(
          $src,
          $dst,
          'duration',
          {
            direction: 'out',
            'parallel': true,
            vertexAxisNames: ['lat', 'lon'],
            heuristicFormula: 'EUCLIDEANNOSQR',
            tieBreaker: true
          }
        ) AS path FROM City LIMIT 1;
        """,
    )
    dijkstra_risk_path = get_path_from_sqlscript(
        db,
        """
        LET $src = (SELECT FROM City WHERE code = 'ALPHA' LIMIT 1);
        LET $dst = (SELECT FROM City WHERE code = 'DELTA' LIMIT 1);
        SELECT dijkstra($src, $dst, 'risk') AS path FROM City LIMIT 1;
        """,
    )
    astar_risk_path = get_path_from_sqlscript(
        db,
        """
        LET $src = (SELECT FROM City WHERE code = 'ALPHA' LIMIT 1);
        LET $dst = (SELECT FROM City WHERE code = 'DELTA' LIMIT 1);
        SELECT astar(
          $src,
          $dst,
          'risk',
          {
            direction: 'out',
            'parallel': true,
            vertexAxisNames: ['lat', 'lon'],
            heuristicFormula: 'EUCLIDEANNOSQR',
            tieBreaker: true
          }
        ) AS path FROM City LIMIT 1;
        """,
    )

    dijkstra_duration_summary = summarize_path(
        db,
        "dijkstra-duration-crosscheck",
        dijkstra_duration_path,
    )
    astar_duration_summary = summarize_path(
        db,
        "astar-duration-crosscheck",
        astar_duration_path,
    )
    dijkstra_risk_summary = summarize_path(
        db,
        "dijkstra-risk-crosscheck",
        dijkstra_risk_path,
    )
    astar_risk_summary = summarize_path(
        db,
        "astar-risk-crosscheck",
        astar_risk_path,
    )

    require(
        dijkstra_duration_summary["codes"] == ["ALPHA", "ECHO", "DELTA"],
        "Expected dijkstra(duration) to use the rail express",
    )
    require(
        astar_duration_summary["codes"] == dijkstra_duration_summary["codes"],
        "Expected astar(duration) to match dijkstra(duration)",
    )
    require(
        dijkstra_risk_summary["codes"] == ["ALPHA", "DELTA"],
        "Expected dijkstra(risk) to use the direct low-risk road",
    )
    require(
        astar_risk_summary["codes"] == dijkstra_risk_summary["codes"],
        "Expected astar(risk) to match dijkstra(risk)",
    )

    print_route_summary(
        "dijkstra(duration) summary:",
        dijkstra_duration_summary,
    )
    print_route_summary(
        "astar(duration) summary:",
        astar_duration_summary,
    )
    print_route_summary(
        "dijkstra(risk) summary:",
        dijkstra_risk_summary,
    )
    print_route_summary(
        "astar(risk) summary:",
        astar_risk_summary,
    )
    print("Interpretation:")
    print(
        "  Once the graph has multiple cost properties, you can use A* as a "
        "faster weighted search while still checking that it agrees with "
        "Dijkstra on the chosen objective."
    )
    print()


def run_direction_phase(db) -> None:
    print("Phase 4: Direction-sensitive routing")
    print()

    out_only_path = get_path_from_sqlscript(
        db,
        """
        LET $src = (SELECT FROM City WHERE code = 'DELTA' LIMIT 1);
        LET $dst = (SELECT FROM City WHERE code = 'ALPHA' LIMIT 1);
        SELECT shortestPath($src, $dst, 'OUT', 'Road') AS path FROM City LIMIT 1;
        """,
    )
    both_path = get_path_from_sqlscript(
        db,
        """
        LET $src = (SELECT FROM City WHERE code = 'DELTA' LIMIT 1);
        LET $dst = (SELECT FROM City WHERE code = 'ALPHA' LIMIT 1);
        SELECT shortestPath($src, $dst, 'BOTH', 'Road') AS path FROM City LIMIT 1;
        """,
    )
    out_only_codes = rid_list_to_codes(db, out_only_path)
    both_summary = summarize_path(db, "shortestPath-both", both_path)

    require(
        out_only_codes in ([], ["DELTA"]),
        "Expected DELTA -> ALPHA with OUT Road traversal to be unreachable",
    )
    require(
        bool(both_summary["codes"])
        and both_summary["codes"][0] == "DELTA"
        and both_summary["codes"][-1] == "ALPHA"
        and both_summary["hops"] >= 1,
        "Expected BOTH-direction traversal to produce a reachable DELTA -> ALPHA route",
    )

    print(f"OUT-only shortestPath(DELTA -> ALPHA) codes: {out_only_codes}")
    print_route_summary("BOTH-direction summary:", both_summary)
    print("Interpretation:")
    print(
        "  Direction alone can turn the same graph from unreachable to "
        "reachable when reverse traversal is allowed."
    )
    print()


def run_sqlscript_rid_phase(db) -> None:
    print("Phase 5: RID-variable SQLScript usage")
    print()

    dijkstra_path = get_path_from_sqlscript(
        db,
        """
        LET $src = (SELECT FROM City WHERE code = 'ALPHA' LIMIT 1);
        LET $dst = (SELECT FROM City WHERE code = 'DELTA' LIMIT 1);
        SELECT dijkstra($src.@rid, $dst.@rid, 'duration', 'OUT') AS path
        FROM City LIMIT 1;
        """,
    )
    astar_path = get_path_from_sqlscript(
        db,
        """
        LET $src = (SELECT FROM City WHERE code = 'ALPHA' LIMIT 1);
        LET $dst = (SELECT FROM City WHERE code = 'DELTA' LIMIT 1);
        SELECT astar(
          $src.@rid,
          $dst.@rid,
          'duration',
          {
            direction: 'out',
            edgeTypeNames: ['Road', 'Rail', 'Ferry'],
            vertexAxisNames: ['lat', 'lon'],
            heuristicFormula: 'EUCLIDEANNOSQR'
          }
        ) AS path FROM City LIMIT 1;
        """,
    )

    dijkstra_summary = summarize_path(db, "dijkstra-rid-duration", dijkstra_path)
    astar_summary = summarize_path(db, "astar-rid-duration", astar_path)

    require(
        dijkstra_summary["codes"] == ["ALPHA", "ECHO", "DELTA"],
        "Expected RID-variable dijkstra(duration) to use the rail express",
    )
    require(
        astar_summary["codes"] == ["ALPHA", "ECHO", "DELTA"],
        "Expected RID-variable astar(duration) to match the rail express",
    )

    print_route_summary("RID-variable dijkstra(duration) summary:", dijkstra_summary)
    print_route_summary("RID-variable astar(duration) summary:", astar_summary)


def run_disconnected_phase(db) -> None:
    print("Phase 6: Disconnected destination behavior")
    print()

    shortest_path = get_path_from_sqlscript(
        db,
        """
        LET $src = (SELECT FROM City WHERE code = 'ALPHA' LIMIT 1);
        LET $dst = (SELECT FROM City WHERE code = 'GHOST' LIMIT 1);
        SELECT shortestPath($src, $dst, 'BOTH') AS path FROM City LIMIT 1;
        """,
    )

    codes = rid_list_to_codes(db, shortest_path)
    require(
        not codes or len(codes) <= 1,
        "Expected disconnected shortestPath() query to return no usable path",
    )
    print(f"shortestPath(ALPHA -> GHOST) returned codes: {codes}")
    print()


def run_reopen_phase(db_path: Path) -> None:
    print("Phase 7: Reopen verification")
    print()

    with arcadedb.open_database(str(db_path)) as reopened_db:
        city_count = (
            reopened_db.query("sql", "SELECT count(*) AS count FROM City")
            .first()
            .get("count")
        )
        route_count = sum(
            reopened_db.query("sql", f"SELECT count(*) AS count FROM {edge_type}")
            .first()
            .get("count")
            for edge_type in ("Road", "Rail", "Ferry")
        )
        require(
            city_count == len(CITIES),
            "Expected City count to persist across reopen",
        )
        require(
            route_count == len(ROUTES),
            "Expected transport edge count to persist across reopen",
        )

        duration_path = get_path_from_sqlscript(
            reopened_db,
            """
            LET $src = (SELECT FROM City WHERE code = 'ALPHA' LIMIT 1);
            LET $dst = (SELECT FROM City WHERE code = 'DELTA' LIMIT 1);
            SELECT dijkstra($src, $dst, 'duration') AS path FROM City LIMIT 1;
            """,
        )
        constrained_path = get_path_from_sqlscript(
            reopened_db,
            """
            LET $src = (SELECT FROM City WHERE code = 'DELTA' LIMIT 1);
            LET $dst = (SELECT FROM City WHERE code = 'ALPHA' LIMIT 1);
            SELECT shortestPath($src, $dst, 'BOTH', 'Road') AS path
            FROM City LIMIT 1;
            """,
        )
        duration_summary = summarize_path(
            reopened_db,
            "dijkstra-reopen-duration",
            duration_path,
        )
        constrained_summary = summarize_path(
            reopened_db,
            "shortestPath-reopen-both",
            constrained_path,
        )

        require(
            duration_summary["codes"] == ["ALPHA", "ECHO", "DELTA"],
            "Expected reopened duration query to return the rail express",
        )
        require(
            bool(constrained_summary["codes"])
            and constrained_summary["codes"][0] == "DELTA"
            and constrained_summary["codes"][-1] == "ALPHA",
            "Expected reopened constrained query to return the BOTH-direction route",
        )

        print(f"City count after reopen: {city_count}")
        print(f"Transport edge count after reopen: {route_count}")
        print_route_summary(
            "Reopened dijkstra(duration) summary:",
            duration_summary,
        )
        print_route_summary(
            "Reopened shortestPath(BOTH, Road) summary:",
            constrained_summary,
        )


def main() -> int:
    args = parse_args()
    db_path = Path(args.db_path)

    print("=" * 72)
    print("ArcadeDB Python - Example 20: Graph Algorithms Route Planning")
    print("=" * 72)
    print()

    reset_db(db_path)

    with arcadedb.create_database(str(db_path)) as db:
        print(f"Database created at: {db_path}")
        print("Approach: SQL-first multi-modal routing workflow from Python bindings")
        print(
            "Focus: hop count vs weighted routing, optimization objectives, "
            "A* cross-checks, direction, RID variables, and reopen checks"
        )
        print()

        create_schema(db)
        insert_seed_data(db)

        print(f"Inserted {len(CITIES)} City vertices")
        print(
            "Inserted transport edges: "
            f"Road={sum(route['edge_type'] == 'Road' for route in ROUTES)}, "
            f"Rail={sum(route['edge_type'] == 'Rail' for route in ROUTES)}, "
            f"Ferry={sum(route['edge_type'] == 'Ferry' for route in ROUTES)}"
        )
        print(
            "Reminder: shortestPath() ignores edge weights, while dijkstra() "
            "and astar() optimize the requested property."
        )
        print()

        if not ensure_graph_algo_support(db):
            return 0

        run_algorithm_phase(db)
        run_objective_phase(db)
        run_astar_crosscheck_phase(db)
        run_direction_phase(db)
        run_sqlscript_rid_phase(db)
        run_disconnected_phase(db)

    run_reopen_phase(db_path)

    print("Example 20 completed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
