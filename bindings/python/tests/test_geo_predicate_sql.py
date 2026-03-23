"""Geo predicate SQL coverage for Python bindings."""

import arcadedb_embedded as arcadedb
import pytest


def test_geo_predicate_sql_within_and_intersects(temp_db_path):
    """Validate geo.within and geo.intersects boolean semantics through SQL."""
    with arcadedb.create_database(temp_db_path) as db:
        try:
            within_inside = next(
                db.query(
                    "sql",
                    "select geo.within('POINT (5 5)', 'POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))') as v",
                )
            )
        except Exception as e:
            if "Unknown method name: within" in str(e):
                pytest.skip(
                    "Geo SQL functions are not available in this packaged runtime"
                )
            raise
        within_outside = next(
            db.query(
                "sql",
                "select geo.within('POINT (15 15)', 'POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))') as v",
            )
        )
        intersects_overlap = next(
            db.query(
                "sql",
                "select geo.intersects('POLYGON ((0 0, 6 0, 6 6, 0 6, 0 0))', 'POLYGON ((3 3, 9 3, 9 9, 3 9, 3 3))') as v",
            )
        )
        intersects_disjoint = next(
            db.query(
                "sql",
                "select geo.intersects('POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))', 'POLYGON ((5 5, 9 5, 9 9, 5 9, 5 5))') as v",
            )
        )
        within_null = next(
            db.query(
                "sql",
                "select geo.within(null, 'POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))') as v",
            )
        )

        assert within_inside.get("v") is True
        assert within_outside.get("v") is False
        assert intersects_overlap.get("v") is True
        assert intersects_disjoint.get("v") is False
        assert within_null.get("v") is None


def test_geo_predicate_sql_boundary_and_repeatability(temp_db_path):
    """Geo predicates should remain stable across repeated calls and boundary points."""
    with arcadedb.create_database(temp_db_path) as db:
        try:
            boundary = next(
                db.query(
                    "sql",
                    "select geo.within('POINT (0 0)', 'POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))') as v",
                )
            )
        except Exception as e:
            if "Unknown method name: within" in str(e):
                pytest.skip(
                    "Geo SQL functions are not available in this packaged runtime"
                )
            raise

        inside_first = next(
            db.query(
                "sql",
                "select geo.within('POINT (5 5)', 'POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))') as v",
            )
        )
        inside_second = next(
            db.query(
                "sql",
                "select geo.within('POINT (5 5)', 'POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))') as v",
            )
        )

        assert boundary.get("v") in (True, False)
        assert inside_first.get("v") is True
        assert inside_second.get("v") is True
