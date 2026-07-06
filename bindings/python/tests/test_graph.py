def test_graph_batch_new_edges_bulk(tmp_path):
    """new_edges buffers many property-less edges in one crossing."""
    import arcadedb_embedded as arcadedb

    with arcadedb.create_database(str(tmp_path / "bulk_edges_db")) as db:
        db.command("sql", "CREATE VERTEX TYPE BP")
        db.command("sql", "CREATE EDGE TYPE BE")
        with db.graph_batch(use_wal=False) as batch:
            rids = batch.create_vertices("BP", [{"id": i} for i in range(100)])
            sources = [rids[i] for i in range(0, 99)]
            dests = [rids[i + 1] for i in range(0, 99)]
            batch.new_edges(sources, "BE", dests)

        count = db.query("sql", "SELECT count(*) as c FROM BE").first().get("c")
        assert count == 99


def test_graph_batch_create_vertices_json_bulk_correctness(tmp_path):
    """The JSON bulk vertex path stores values identically to the matrix path
    (and datetimes fall back to the matrix path with types preserved)."""
    import datetime

    import arcadedb_embedded as arcadedb

    with arcadedb.create_database(str(tmp_path / "bulk_v_db")) as db:
        db.command("sql", "CREATE VERTEX TYPE BV")
        with db.graph_batch(use_wal=False) as batch:
            rids = batch.create_vertices(
                "BV",
                [
                    {"id": 1, "name": "a", "score": 1.5, "ok": True, "nul": None},
                    {"id": 2, "name": "b", "score": 2.5, "ok": False, "nul": None},
                ],
            )
            assert len(rids) == 2 and all(r.startswith("#") for r in rids)

            # non-JSON-safe values (datetime) take the matrix fallback
            rids2 = batch.create_vertices(
                "BV", [{"id": 3, "ts": datetime.datetime(2026, 1, 2, 3, 4, 5)}]
            )
            assert len(rids2) == 1

        row = db.query("sql", "SELECT FROM BV WHERE id = 1").first()
        assert row.get("name") == "a"
        assert row.get("score") == 1.5
        assert row.get("ok") is True

        row3 = db.query("sql", "SELECT FROM BV WHERE id = 3").first()
        assert row3.get("ts") is not None


def test_graph_batch_new_edges_bulk_with_properties(tmp_path):
    """new_edges with per-edge property dicts stores values correctly."""
    import arcadedb_embedded as arcadedb

    with arcadedb.create_database(str(tmp_path / "bulk_ep_db")) as db:
        db.command("sql", "CREATE VERTEX TYPE PV")
        db.command("sql", "CREATE EDGE TYPE PE")
        with db.graph_batch(use_wal=False) as batch:
            rids = batch.create_vertices("PV", [{"id": i} for i in range(4)])
            batch.new_edges(
                [rids[0], rids[1]],
                "PE",
                [rids[2], rids[3]],
                properties=[{"w": 1.5, "label": "x"}, {"w": 2.5, "label": "y"}],
            )

        rows = db.query("sql", "SELECT w, label FROM PE ORDER BY w").to_list()
        assert [(r["w"], r["label"]) for r in rows] == [(1.5, "x"), (2.5, "y")]
