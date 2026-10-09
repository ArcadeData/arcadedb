"""The dev-mode runtime cache must follow the wheel it was extracted from.

Running from a source checkout, the jars and the JRE are extracted from the
most recent wheel in ``dist/`` into ``.runtime-cache/``. Until 2026-09-23 that
cache was extracted once and trusted forever: a cache made on 2026-06-04 from a
26.6.1 wheel was still being served three months later beside a 26.10.1 wheel
built that morning. Every source-tree run in between executed against June's
engine without the python bridge jar -- strings came back as lists of
characters and twelve vector tests failed -- and nothing said why.

These tests pin the three properties that failure lacked. None starts a JVM.
"""

import os
import zipfile

import pytest
from arcadedb_embedded import jvm

RES = "fixture_res"  # a name the package itself never ships, so the fallback runs


def _wheel(dist, name, files, mtime):
    """Write a wheel containing arcadedb_embedded/<RES>/<file> entries."""
    path = dist / name
    with zipfile.ZipFile(path, "w") as z:
        for fname, body in files.items():
            z.writestr(f"arcadedb_embedded/{RES}/{fname}", body)
    os.utime(path, (mtime, mtime))
    return path


@pytest.fixture
def project(tmp_path, monkeypatch):
    (tmp_path / "dist").mkdir()
    monkeypatch.setattr(jvm, "_project_dir", lambda: tmp_path)
    return tmp_path


def test_newest_wheel_is_chosen_by_build_time_not_by_name(project):
    """A reverse string sort ranks 26.9.1 above 26.10.1 because "9" > "1"."""
    dist = project / "dist"
    _wheel(dist, "arcadedb_embedded-26.9.1-py3-none-any.whl", {"v.txt": "old"}, 1_000)
    _wheel(dist, "arcadedb_embedded-26.10.1-py3-none-any.whl", {"v.txt": "new"}, 2_000)

    out = jvm._extract_runtime_resource(RES)

    assert (out / "v.txt").read_text() == "new"


def test_a_rebuilt_wheel_replaces_the_cache_and_leaves_nothing_behind(project):
    """The June 4 failure: a newer wheel appears and the old extraction stays."""
    dist = project / "dist"
    name = "arcadedb_embedded-26.10.1.dev0-py3-none-any.whl"
    _wheel(dist, name, {"engine-26.6.1.jar": "june", "only-in-old.jar": "x"}, 1_000)
    first = jvm._extract_runtime_resource(RES)
    assert (first / "engine-26.6.1.jar").exists()

    # same filename, rebuilt: different contents and a later mtime
    _wheel(dist, name, {"engine-26.10.1.jar": "september"}, 2_000)
    second = jvm._extract_runtime_resource(RES)

    assert (second / "engine-26.10.1.jar").read_text() == "september"
    # no overlay: a classpath carrying two engines is worse than an error
    assert not (second / "engine-26.6.1.jar").exists()
    assert not (second / "only-in-old.jar").exists()


def test_an_unchanged_wheel_is_not_re_extracted(project):
    """The cache is still a cache: the same wheel does not pay extraction twice."""
    dist = project / "dist"
    _wheel(dist, "arcadedb_embedded-26.10.1-py3-none-any.whl", {"a.jar": "a"}, 1_000)
    out = jvm._extract_runtime_resource(RES)
    (out / "marker").write_text("still here")

    again = jvm._extract_runtime_resource(RES)

    assert (again / "marker").read_text() == "still here"


def test_a_cache_from_before_stamping_is_treated_as_stale(project):
    """Every cache that exists today predates the stamp, and must not be trusted."""
    dist = project / "dist"
    legacy = project / ".runtime-cache" / "arcadedb_embedded" / RES
    legacy.mkdir(parents=True)
    (legacy / "engine-26.6.1.jar").write_text("june")
    _wheel(
        dist,
        "arcadedb_embedded-26.10.1-py3-none-any.whl",
        {"engine-26.10.1.jar": "sep"},
        2_000,
    )

    out = jvm._extract_runtime_resource(RES)

    assert (out / "engine-26.10.1.jar").exists()
    assert not (out / "engine-26.6.1.jar").exists()
