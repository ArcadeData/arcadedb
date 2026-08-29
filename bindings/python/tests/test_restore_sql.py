"""RESTORE statement coverage for the Python bindings.

This family had no test at all. The gap was left open deliberately while
upstream #6069 was outstanding: RESTORE put the record back but did not fold
the bucket's record-count delta, so `SELECT count(*)` returned one fewer than
a full scan and the disagreement survived close and reopen. Pinning the broken
behaviour would have baked a bug into the suite, and pinning the correct
behaviour would have failed until the fix landed.

Three upstream defects in this one statement family, all now fixed, all
pinned here as regression guards:

  #6069  the bucket record-count delta was not folded (86cb4673be)
  #6096  same-transaction DELETE + RESTORE wrote the record into the page
         header and lost it, while the count still reported it (59e590aaa9)
  #6120  index entries were never re-added, so an indexed query missed the
         record and a UNIQUE index stopped rejecting duplicates (d1c7494fc3)

#6096 was ours, found while verifying the #6069 fix. #6120 was spun off from
#6096 by the maintainer, who found it while fixing ours and kept it out of
scope on purpose. Every one of the three was closed with no comment, so the
strict xfail on #6096 is what actually told us it had landed.

The count-vs-scan comparison is the load-bearing assertion, not the row
contents. A wrong count came back with an ordinary success and no warning, so
nothing but asking the same question two ways could tell it apart from a
correct one.

On the `# nosec B608` markers: these interpolate a type name or a RID into
SQL, which bandit flags. `DELETE FROM Note WHERE @rid = :rid` does bind
correctly and was measured working, but the deletes here keep the direct-RID
form on purpose, because that is what the upstream repro used and a different
delete path could plausibly exercise different bucket bookkeeping, which is
the very thing under test. `RESTORE ... RID :rid` is not an option at all: the
RID is statement syntax there and the parser rejects a bound parameter.
"""

import arcadedb_embedded as arcadedb
import pytest


def _count_two_ways(db, type_name):
    """(count(*), rows a full scan returns). These must agree."""
    counted = db.query(
        "sql",
        f"SELECT count(*) AS c FROM {type_name}",  # nosec B608 - test-owned type name
    ).to_list()[0]["c"]
    scanned = len(
        db.query(
            "sql",
            f"SELECT FROM {type_name}",  # nosec B608 - test-owned type name
        ).to_list()
    )
    return counted, scanned


def test_restore_document_restores_the_record_count(temp_db_path):
    """count(*) must agree with a full scan after RESTORE, and after reopen.

    Reopening is half the test: the original bug persisted across close and
    reopen, which is what proved it was the stored count rather than a stale
    in-memory statistic.
    """
    with arcadedb.create_database(temp_db_path) as db:
        db.command("sql", "CREATE DOCUMENT TYPE Note")
        with db.transaction():
            for i in range(3):
                db.command("sql", "INSERT INTO Note SET i = :i", {"i": i})

        rid = db.query("sql", "SELECT @rid AS r FROM Note WHERE i = 1").to_list()[0][
            "r"
        ]
        assert _count_two_ways(db, "Note") == (3, 3)

        with db.transaction():
            db.command(
                "sql",
                f"DELETE FROM {rid}",  # nosec B608 - RID from our own query
            )
        assert _count_two_ways(db, "Note") == (2, 2)

        with db.transaction():
            db.command(
                "sql",
                f"RESTORE DOCUMENT Note RID {rid} SET i = 1",  # nosec B608 - RID is syntax
            )
        assert _count_two_ways(db, "Note") == (
            3,
            3,
        ), "count(*) disagrees with a full scan after RESTORE (upstream #6069)"

    with arcadedb.open_database(temp_db_path) as db:
        assert _count_two_ways(db, "Note") == (3, 3), (
            "the count disagreement survived reopen, so it is the persisted "
            "count rather than a stale in-memory statistic (upstream #6069)"
        )


def test_restore_document_returns_the_record_intact(temp_db_path):
    """The restored record keeps its original RID and the SET properties.

    Delete and restore in SEPARATE transactions, which is the shape the
    original #6096 repro used. The same-transaction shape is covered below and
    was broken until #6096 was fixed.
    """
    with arcadedb.create_database(temp_db_path) as db:
        db.command("sql", "CREATE DOCUMENT TYPE Note")
        with db.transaction():
            db.command("sql", "INSERT INTO Note SET i = 7, tag = 'keep'")

        rid = db.query("sql", "SELECT @rid AS r FROM Note").to_list()[0]["r"]
        with db.transaction():
            db.command(
                "sql",
                f"DELETE FROM {rid}",  # nosec B608 - RID from our own query
            )
        with db.transaction():
            db.command(
                "sql",
                f"RESTORE DOCUMENT Note RID {rid} SET i = 7, tag = 'keep'",  # nosec B608
            )

        rows = db.query("sql", "SELECT @rid AS r, i, tag FROM Note").to_list()
        assert len(rows) == 1
        assert str(rows[0]["r"]) == str(rid), "RESTORE should reuse the original RID"
        assert rows[0]["i"] == 7
        assert rows[0]["tag"] == "keep"


def test_restore_in_same_transaction_as_delete(temp_db_path):
    """DELETE then RESTORE inside ONE transaction: upstream #6096, now fixed.

    This shipped as xfail(strict=True) while #6096 was open, and the strict
    marker is what reported the fix: the suite went red on XPASS. There was no
    comment on the issue and no release note, so nothing else would have said
    so. Now a plain assertion, and the regression guard.

    The defect was never the counter. `findContentInsertionOffset()` took a
    plain max() over the page's slot table without treating a 0 entry as a
    hole, so on an all-holes page the max was 0, which was then read as a
    record header and produced an insertion offset of 1, inside the 8194-byte
    page header. The restore wrote the record over its own slot-table entry.
    count(*) was right about a record that was not there, because the DELETE's
    -1 and the RESTORE's +1 cancelled out.
    """
    with arcadedb.create_database(temp_db_path) as db:
        db.command("sql", "CREATE DOCUMENT TYPE Note")
        with db.transaction():
            db.command("sql", "INSERT INTO Note SET i = 7")

        rid = db.query("sql", "SELECT @rid AS r FROM Note").to_list()[0]["r"]
        with db.transaction():
            db.command(
                "sql",
                f"DELETE FROM {rid}",  # nosec B608 - RID from our own query
            )
            db.command(
                "sql",
                f"RESTORE DOCUMENT Note RID {rid} SET i = 7",  # nosec B608 - RID is syntax
            )

        counted, scanned = _count_two_ways(db, "Note")
        assert (counted, scanned) == (
            1,
            1,
        ), f"count(*)={counted} but a full scan returned {scanned} rows (#6096)"


def test_restore_vertex_restores_the_record_count(temp_db_path):
    """The fix covers VERTEX as well as DOCUMENT, so pin both.

    Named separately rather than parametrized because a vertex carries edge
    bookkeeping a document does not, so a future regression could plausibly
    hit one and not the other.
    """
    with arcadedb.create_database(temp_db_path) as db:
        db.command("sql", "CREATE VERTEX TYPE Person")
        with db.transaction():
            for name in ("a", "b", "c"):
                db.command("sql", "INSERT INTO Person SET name = :n", {"n": name})

        rid = db.query(
            "sql", "SELECT @rid AS r FROM Person WHERE name = 'b'"
        ).to_list()[0]["r"]

        with db.transaction():
            db.command(
                "sql",
                f"DELETE FROM {rid}",  # nosec B608 - RID from our own query
            )
        assert _count_two_ways(db, "Person") == (2, 2)

        with db.transaction():
            db.command(
                "sql",
                f"RESTORE VERTEX Person RID {rid} SET name = 'b'",  # nosec B608 - RID is syntax
            )
        assert _count_two_ways(db, "Person") == (3, 3), (
            "count(*) disagrees with a full scan after RESTORE VERTEX "
            "(upstream #6069)"
        )


def test_restore_readds_index_entries(temp_db_path):
    """RESTORE must put the record back in its INDEXES too: upstream #6120.

    Spun off from our #6096 by the maintainer, who found it while fixing that
    one and kept it out of scope deliberately. The three RESTORE statements
    called LocalBucket.restoreRecordAtPosition() directly and bypassed
    LocalDatabase.createRecord(), which is where a normal insert indexes the
    document.

    The duplicate-insert assertion is the sharp one. Querying by an indexed
    property could in principle be answered by a scan and pass even with the
    index entry missing, but a UNIQUE index's own duplicate check cannot: if
    the entry is absent, the second insert is accepted and the uniqueness
    constraint has silently stopped holding.
    """
    with arcadedb.create_database(temp_db_path) as db:
        db.command("sql", "CREATE DOCUMENT TYPE Product")
        db.command("sql", "CREATE PROPERTY Product.sku STRING")
        db.command("sql", "CREATE INDEX ON Product (sku) UNIQUE")
        with db.transaction():
            db.command("sql", "INSERT INTO Product SET sku = 'SKU-1', n = 1")

        rid = db.query("sql", "SELECT @rid AS r FROM Product").to_list()[0]["r"]
        with db.transaction():
            db.command(
                "sql",
                f"DELETE FROM {rid}",  # nosec B608 - RID from our own query
            )
        with db.transaction():
            db.command(
                "sql",
                f"RESTORE DOCUMENT Product RID {rid} SET sku = 'SKU-1', n = 1",  # nosec B608
            )

        via_index = db.query("sql", "SELECT FROM Product WHERE sku = 'SKU-1'").to_list()
        assert (
            len(via_index) == 1
        ), "a query on the indexed property missed the restored record (#6120)"
        assert _count_two_ways(db, "Product") == (1, 1)

        with pytest.raises(Exception):
            with db.transaction():
                db.command("sql", "INSERT INTO Product SET sku = 'SKU-1', n = 2")
