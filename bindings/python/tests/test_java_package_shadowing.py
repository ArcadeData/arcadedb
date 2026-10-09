"""A folder named ``java/`` or ``com/`` must not change what a query returns.

Until 2026-09-23 the type converter resolved Java classes through JPype's
``java`` import hook (``from java.lang import String``). That hook resolves the
top-level name through ``sys.path`` like any Python import, so a directory
called ``java/`` on the path became a namespace package that shadowed it. The
lookup raised ImportError, every typed conversion was skipped, and a Java
String fell through to the generic sequence fallback, which iterated it:

    >>> db.query("sql", "SELECT name FROM Person").to_list()
    [{'name': ['A', 'd', 'a']}]

No error, in the SHIPPED wheel. The trigger is ordinary: running a script from
the root of any mixed Java/Python project, whose Java sources live in
``java/``, or a Maven tree whose packages start at ``com/``. It also hit every
test run from a checkout of this repository, whose bridge sources live at
``bindings/python/src/java`` -- which is how it was found, as eleven failing
vector tests that had twice been blamed on something else.

These run in a SUBPROCESS on purpose. In-process, JPype's ``java`` module is
already in ``sys.modules`` before the test could plant a folder, so the test
would pass with or without the fix.
"""

import os
import subprocess  # nosec B404 - runs sys.executable on a script this file owns
import sys
import textwrap

import pytest

_SCRIPT = textwrap.dedent("""
    import os, shutil, tempfile, json, datetime, decimal
    import arcadedb_embedded as A
    root = tempfile.mkdtemp(prefix="shadow")
    db = A.create_database(os.path.join(root, "db"))
    try:
        db.command("sql", "CREATE DOCUMENT TYPE Person")
        db.command("sql", "CREATE PROPERTY Person.name STRING")
        db.command("sql", "CREATE PROPERTY Person.born DATETIME")
        db.command("sql", "CREATE PROPERTY Person.balance DECIMAL")
        with db.transaction():
            db.command(
                "sql",
                "INSERT INTO Person SET name = 'Ada', born = date('1815-12-10', "
                "'yyyy-MM-dd'), balance = 19.99, tags = ['x', 'yz']",
            )
        row = db.query("sql", "SELECT name, born, balance, tags FROM Person").to_list()[0]
        got = db.query("sql", "SELECT name FROM Person").first().get("name")
        print(json.dumps({
            "name": row["name"],
            "name_type": type(row["name"]).__name__,
            "get": got,
            "born_type": type(row["born"]).__name__,
            "balance_type": type(row["balance"]).__name__,
            "tags": row["tags"],
            "loaded_from": os.path.dirname(A.__file__),
        }))
    finally:
        db.close()
        shutil.rmtree(root, ignore_errors=True)
    """)


@pytest.mark.parametrize("folder", ["java", "com"])
def test_a_java_or_com_folder_on_the_path_does_not_break_conversion(tmp_path, folder):
    shadow = tmp_path / folder / "example"
    shadow.mkdir(parents=True)
    (shadow / "App.java").write_text("class App {}\n")

    # THE CHILD MUST RUN THE CODE UNDER TEST. The first version inherited the
    # parent's environment, whose PYTHONPATH was relative ("bindings/python/src")
    # and so resolved to nothing from the child's temp working directory: the
    # child quietly imported the installed, unfixed wheel, and the test measured
    # the wrong code in both directions. Pin it to the parent's package by
    # absolute path, and assert below that it got there.
    import arcadedb_embedded

    pkg_dir = os.path.dirname(arcadedb_embedded.__file__)
    env = os.environ.copy()
    env["PYTHONPATH"] = os.pathsep.join(
        [os.path.dirname(pkg_dir)]
        + [p for p in env.get("PYTHONPATH", "").split(os.pathsep) if p]
    )

    # `python -c` puts the working directory at sys.path[0], exactly as running
    # `python app.py` from a project root does.
    proc = subprocess.run(  # nosec B603 - fixed argv: sys.executable and _SCRIPT
        [sys.executable, "-c", _SCRIPT],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        timeout=300,
    )
    assert proc.returncode == 0, proc.stderr[-2000:]
    import json

    out = json.loads(proc.stdout.strip().splitlines()[-1])

    assert os.path.realpath(out["loaded_from"]) == os.path.realpath(
        pkg_dir
    ), "the child imported a different arcadedb_embedded than the one under test"

    assert out["name"] == "Ada", f"strings came back as {out['name']!r}"
    assert out["name_type"] == "str"
    assert out["get"] == "Ada"
    # the typed branches, not just the string one, must still be reached
    assert out["born_type"] == "datetime"
    assert out["balance_type"] == "Decimal"
    assert out["tags"] == ["x", "yz"]
