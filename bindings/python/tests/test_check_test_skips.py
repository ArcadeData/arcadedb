"""The skip gate: a test that skipped for a reason nobody accepted fails the CI job."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "check_test_skips.py"


def _load():
    spec = importlib.util.spec_from_file_location("check_test_skips", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def _junit(path: Path, skipped: list[tuple[str, str, str]]) -> str:
    """A JUnit file with one skipped case per (name, type, message)."""
    cases = "".join(
        f'<testcase classname="tests.test_x" name="{name}">'
        f'<skipped type="{kind}" message="{message}">x.py:1: {message}</skipped></testcase>'
        for name, kind, message in skipped
    )
    path.write_text(
        f'<?xml version="1.0"?><testsuites><testsuite tests="{len(skipped) + 1}" skipped="{len(skipped)}">'
        f'<testcase classname="tests.test_x" name="test_ok"/>{cases}</testsuite></testsuites>'
    )
    return str(path)


def test_a_run_with_no_skips_passes(tmp_path, capsys):
    module = _load()
    assert (
        module.main([_junit(tmp_path / "r.xml", []), "--platform", "linux/amd64"]) == 0
    )
    assert "OK" in capsys.readouterr().out


# The ids are explicit on purpose: CI greps the JUnit file for the importorskip reason text, and a
# parameter id that contained it would trip that gate on a run in which nothing is missing.
@pytest.mark.parametrize(
    "message",
    [
        pytest.param("OpenCypher not available", id="missing-feature"),
        pytest.param("Requires GraphML/GraphSON support", id="gremlin"),
        pytest.param("Requires server support", id="server"),
        pytest.param("the test sends SIGINT to a child process", id="sigint"),
        pytest.param(
            "bindings/python/docs is not present on this branch", id="docs-branch"
        ),
        pytest.param(
            "could not import 'pyarrow': No module named 'pyarrow'",
            id="missing-package",
        ),
        pytest.param("NumPy not installed", id="numpy"),
    ],
)
def test_every_skip_fails_by_default(tmp_path, message, capsys):
    module = _load()
    assert module.ALLOWED_SKIPS == ()
    xml = _junit(tmp_path / "r.xml", [("test_y", "pytest.skip", message)])
    for platform in ("linux/amd64", "darwin/arm64", "windows/amd64"):
        assert module.main([xml, "--platform", platform]) == 1, platform
    out = capsys.readouterr().out
    assert "ERROR" in out and message in out and "test_y" in out


def test_an_accepted_skip_passes_only_on_the_platforms_it_names(
    tmp_path, monkeypatch, capsys
):
    module = _load()
    monkeypatch.setattr(
        module,
        "ALLOWED_SKIPS",
        ((r"^no SIGINT here$", ("windows",), "Windows cannot do it"),),
    )
    xml = _junit(tmp_path / "r.xml", [("test_s", "pytest.skip", "no SIGINT here")])
    assert module.main([xml, "--platform", "windows/amd64"]) == 0
    assert module.main([xml, "--platform", "linux/arm64"]) == 1
    assert "test_s" in capsys.readouterr().out


def test_an_xfail_is_not_a_skip(tmp_path):
    module = _load()
    xml = _junit(
        tmp_path / "r.xml",
        [("test_known", "pytest.xfail", "ArcadeData/arcadedb#9277: open upstream")],
    )
    assert module.main([xml, "--platform", "linux/amd64"]) == 0


def test_the_script_prints_ascii_only(tmp_path, capsys):
    module = _load()
    xml = _junit(
        tmp_path / "r.xml", [("test_y", "pytest.skip", "OpenCypher not available")]
    )
    module.main([xml, "--platform", "linux/amd64"])
    module.main([_junit(tmp_path / "ok.xml", []), "--platform", "linux/amd64"])
    out = capsys.readouterr().out
    out.encode("ascii")
    assert SCRIPT_PATH.read_text(encoding="utf-8").isascii()
