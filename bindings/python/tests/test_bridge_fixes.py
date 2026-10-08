"""Fixes in the Java bridge (src/java/com/arcadedb/python): #112 and #116.

They only hold with the bridge jar built from this tree, which the wheel build
does; a wheel older than the fix fails them.
"""

import json
import subprocess  # nosec B404 - test-controlled child process
import sys

import arcadedb_embedded as arcadedb
import pytest


def test_insert_many_nested_values_read_as_python_in_the_same_transaction(
    temp_db_path,
):
    """insert_many stored the parsed JSONArray itself, so inside the transaction
    that ran it a list property read back as a Java JSONArray and only became a
    list once the record was serialized (#112)."""
    with arcadedb.create_database(temp_db_path) as db:
        db.command("sql", "CREATE DOCUMENT TYPE Item")
        with db.transaction():
            db.insert_many(
                "Item",
                [
                    {"tags": ["a", "b"], "nested": {"k": [1, 2], "z": None}, "n": 1},
                    {"tags": [], "nested": {}, "n": 2},
                ],
                commit_every=0,
            )
            inside = db.query("sql", "SELECT FROM Item ORDER BY n").to_list()
        after = db.query("sql", "SELECT FROM Item ORDER BY n").to_list()

    expected = [
        {"tags": ["a", "b"], "nested": {"k": [1, 2], "z": None}, "n": 1},
        {"tags": [], "nested": {}, "n": 2},
    ]
    assert inside == expected
    assert after == expected
    assert type(inside[0]["tags"]) is list


_DATE_SCRIPT = """
import json
import sys
from datetime import date

import arcadedb_embedded as arcadedb
from arcadedb_embedded.jvm import start_jvm

start_jvm(heap_size="1g", jvm_args=["-Duser.timezone=" + sys.argv[2]])
db = arcadedb.create_database(sys.argv[1])
db.command("sql", "CREATE DOCUMENT TYPE Event")
with db.transaction():
    db.command(
        "sql",
        "INSERT INTO Event SET day = :day, days = :days, meta = :meta",
        {
            "day": date(2024, 1, 2),
            "days": [date(2024, 1, 2), date(2024, 1, 3)],
            "meta": {"on": date(2024, 1, 2), "n": 1},
        },
    )
batch = db.query("sql", "SELECT FROM Event").to_json_list()[0]
single = json.loads(db.query("sql", "SELECT FROM Event").first().to_json())
print(json.dumps({"batch": batch, "single": single}), flush=True)
db.close()
"""

MIDNIGHT_UTC_2024_01_02 = 1704153600000
DAY_MS = 86_400_000


@pytest.mark.parametrize("zone", ["UTC", "Asia/Seoul", "America/Los_Angeles"])
def test_to_json_list_writes_a_date_as_midnight_utc_in_any_jvm_zone(tmp_path, zone):
    """to_json_list wrote a DATE as midnight in the JVM's zone, so it disagreed
    with Result.to_json() and decoded to the previous day east of UTC (#116).
    Each zone needs its own JVM, hence the child process."""
    proc = subprocess.run(  # nosec B603 - fixed argv, no shell, test-owned
        [sys.executable, "-c", _DATE_SCRIPT, str(tmp_path / "db"), zone],
        capture_output=True,
        text=True,
        timeout=180,
    )
    assert proc.returncode == 0, proc.stderr[-2000:]
    out = json.loads(proc.stdout.strip().splitlines()[-1])
    batch, single = out["batch"], out["single"]

    assert batch["day"] == MIDNIGHT_UTC_2024_01_02
    assert batch["day"] == single["day"]
    # a DATE inside a list or a map takes the same path
    assert batch["days"] == [MIDNIGHT_UTC_2024_01_02, MIDNIGHT_UTC_2024_01_02 + DAY_MS]
    assert batch["meta"] == {"on": MIDNIGHT_UTC_2024_01_02, "n": 1}
