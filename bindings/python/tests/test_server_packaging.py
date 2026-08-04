"""The server stack is actually IN the wheel, and the API is reachable.

This file exists because of a specific failure. On 2026-07-05 commit cfcde0c2
excluded the server JARs and deleted ``server.py`` to save ~7 MB, that shipped
in stable 26.7.2 on 2026-07-09, and nobody noticed for three weeks until a
downstream user said so on the commit itself. Every other server test in this
suite is guarded by ``has_server_support()``, so when the JARs vanished those
tests did not fail. **They skipped, silently, and the suite stayed green.**

A guard that skips when the thing it guards is missing cannot detect the thing
going missing. So these tests deliberately do NOT skip: if the server stack is
absent, they fail.

If a future build genuinely wants a slim wheel, that is a decision to make on
purpose, by deleting this file with a commit message that says why, not by
letting a packaging change quietly turn a suite green.
"""

import os
import zipfile

import pytest

# The JAR families that make server mode work. Sizes as measured on the
# 26.8.1-SNAPSHOT image (uncompressed, MB) are in docs/guide/server.md; the
# whole set is ~7.65 MB uncompressed and ~7.3 MB on the wheel.
REQUIRED_JAR_PREFIXES = [
    "arcadedb-server",  # the server itself, 0.66 MB
    "arcadedb-studio",  # web UI assets, 2.60 MB, contains zero .class files
    "undertow-core",  # HTTP server, 2.21 MB
    "xnio-api",  # undertow's IO layer
    "xnio-nio",
    "wildfly-common",
    "jboss-logging",
    "jboss-threads",
    "micrometer-core",  # required at server startup, not optional
]


def _jar_names():
    from arcadedb_embedded.jvm import get_jar_path

    jar_dir = get_jar_path()
    assert os.path.isdir(jar_dir), f"jar directory missing: {jar_dir}"
    return sorted(os.listdir(jar_dir))


def test_server_jars_are_bundled():
    """Every JAR server mode needs is present. Fails, never skips."""
    names = _jar_names()
    missing = [
        p for p in REQUIRED_JAR_PREFIXES if not any(n.startswith(p) for n in names)
    ]
    assert not missing, (
        f"server JARs missing from the wheel: {missing}\n"
        f"This is how server mode was lost in 26.7.2. If the removal is "
        f"deliberate, delete this test explicitly rather than leaving the "
        f"server tests to skip themselves into a green suite.\n"
        f"jars present: {names}"
    )


def test_server_api_is_importable_and_exported():
    """create_server / ArcadeDBServer are importable AND in __all__.

    Both halves matter: the classes existing but not being exported is the
    same breakage from a user's point of view, since the documented import is
    ``from arcadedb_embedded import create_server``.
    """
    import arcadedb_embedded as adb

    assert hasattr(adb, "ArcadeDBServer"), "ArcadeDBServer not importable"
    assert hasattr(adb, "create_server"), "create_server not importable"
    assert "ArcadeDBServer" in adb.__all__, "ArcadeDBServer missing from __all__"
    assert "create_server" in adb.__all__, "create_server missing from __all__"


def test_has_server_support_agrees_with_reality():
    """The skip-guard other server tests rely on must not lie.

    If ``has_server_support()`` returned False while the JARs are present,
    every server test would skip and the suite would look fine while covering
    nothing. That is precisely the failure mode this file exists to close, so
    the guard itself is checked against the JARs it claims to detect.
    """
    from tests.conftest import has_server_support

    names = _jar_names()
    jars_present = any(n.startswith("arcadedb-studio") for n in names)
    assert has_server_support() == jars_present, (
        f"has_server_support() returned {has_server_support()} but studio JAR "
        f"present={jars_present}. The guard and the wheel disagree, so server "
        f"tests are skipping or running for the wrong reason."
    )


def test_studio_jar_carries_no_classes():
    """Studio is static assets only, which is why bundling it is cheap.

    docs/guide/server.md tells users Studio costs disk and nothing else,
    on the grounds that a JAR with no ``.class`` entries cannot execute. If a
    future Studio release starts shipping code, that claim stops being true
    and the documentation needs revisiting, so assert the premise rather than
    trusting it.
    """
    from arcadedb_embedded.jvm import get_jar_path

    jar_dir = get_jar_path()
    studio = [n for n in os.listdir(jar_dir) if n.startswith("arcadedb-studio")]
    assert studio, "studio JAR missing (see test_server_jars_are_bundled)"

    with zipfile.ZipFile(os.path.join(jar_dir, studio[0])) as z:
        classes = [n for n in z.namelist() if n.endswith(".class")]
    assert not classes, (
        f"arcadedb-studio now ships {len(classes)} .class files. "
        f"docs/guide/server.md claims Studio costs disk only because it "
        f"contains no executable code; update that claim."
    )


@pytest.mark.server
def test_server_starts_and_serves_http(temp_server_root):
    """End-to-end: the bundled stack actually starts and answers.

    The packaging tests above prove the files are present. This proves they
    work together, which is the claim a user actually cares about.
    """
    requests = pytest.importorskip("requests")
    from arcadedb_embedded import create_server
    from tests.conftest import TEST_PASSWORD

    with create_server(
        root_path=temp_server_root, root_password=TEST_PASSWORD
    ) as server:
        assert server.is_started()
        port = server.get_http_port()
        r = requests.get(
            f"http://localhost:{port}/api/v1/server",
            auth=("root", TEST_PASSWORD),
            timeout=30,
        )
        assert r.status_code == 200, f"server returned {r.status_code}"
        assert "version" in r.json()
