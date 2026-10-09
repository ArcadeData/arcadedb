"""The wheel size gate: a wheel at or over the limit fails the build, odd sizes warn.

The tests use small limits instead of 100 MB files, so they run in milliseconds and need no built wheel.
"""

from __future__ import annotations

import importlib.util
import os
import zipfile
from pathlib import Path

import pytest

SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "verify_wheel_size.py"


def _load():
    spec = importlib.util.spec_from_file_location("verify_wheel_size", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def _wheel(path: Path, members: dict[str, int]) -> Path:
    """A zip file with incompressible members of the given sizes, so its size is close to their sum."""
    with zipfile.ZipFile(path, "w", zipfile.ZIP_STORED) as zf:
        for name, size in members.items():
            zf.writestr(name, os.urandom(size))
    return path


def test_a_wheel_under_the_limit_passes(tmp_path, capsys):
    module = _load()
    wheel = _wheel(tmp_path / "a-1-py3-none-any.whl", {"x/engine.jar": 400_000})
    assert (
        module.main(
            [str(wheel), "--max-mb", "1", "--warn-mb", "0.9", "--min-mb", "0.1"]
        )
        == 0
    )
    assert "OK:" in capsys.readouterr().out


def test_a_wheel_at_or_over_the_limit_fails_and_names_the_largest_members(
    tmp_path, capsys
):
    module = _load()
    wheel = _wheel(
        tmp_path / "a-1-py3-none-any.whl",
        {
            "jars/arcadedb-gremlin.jar": 700_000,
            "jars/engine.jar": 400_000,
            "small.txt": 10,
        },
    )
    assert module.main([str(wheel), "--max-mb", "1"]) == 1
    err = capsys.readouterr().err
    assert "over the 1 MB limit" in err
    assert err.index("arcadedb-gremlin.jar") < err.index(
        "engine.jar"
    )  # the largest first


def test_the_limit_is_a_decimal_megabyte_and_the_size_is_compared_with_at_or_over(
    tmp_path,
):
    module = _load()
    wheel = _wheel(tmp_path / "a-1-py3-none-any.whl", {"j": 400_000})
    size = wheel.stat().st_size
    assert module.check(wheel, max_mb=(size + 1) / 1_000_000, warn_mb=10, min_mb=0) == 0
    assert module.check(wheel, max_mb=size / 1_000_000, warn_mb=10, min_mb=0) == 1


def test_a_wheel_close_to_the_limit_warns_and_a_tiny_one_warns_differently(
    tmp_path, capsys
):
    module = _load()
    near = _wheel(tmp_path / "near-1-py3-none-any.whl", {"j": 950_000})
    assert (
        module.main([str(near), "--max-mb", "1", "--warn-mb", "0.9", "--min-mb", "0.1"])
        == 0
    )
    assert "within" in capsys.readouterr().out
    tiny = _wheel(tmp_path / "tiny-1-py3-none-any.whl", {"j": 1_000})
    assert (
        module.main([str(tiny), "--max-mb", "1", "--warn-mb", "0.9", "--min-mb", "0.5"])
        == 0
    )
    assert "engine or the JRE" in capsys.readouterr().out


def test_every_wheel_is_checked_and_the_worst_verdict_wins(tmp_path):
    module = _load()
    ok = _wheel(tmp_path / "ok-1-py3-none-any.whl", {"j": 100_000})
    big = _wheel(tmp_path / "big-1-py3-none-any.whl", {"j": 900_000})
    assert module.main([str(ok), str(big), "--max-mb", "0.5", "--min-mb", "0"]) == 1
    assert module.main([str(ok), "--max-mb", "0.5", "--min-mb", "0"]) == 0


def test_the_environment_variable_overrides_the_limit_and_says_so(
    tmp_path, capsys, monkeypatch
):
    module = _load()
    wheel = _wheel(tmp_path / "a-1-py3-none-any.whl", {"j": 900_000})
    assert module.main([str(wheel), "--max-mb", "0.5"]) == 1
    monkeypatch.setenv("ARCADEDB_WHEEL_MAX_MB", "5")
    capsys.readouterr()
    assert module.main([str(wheel), "--max-mb", "0.5", "--min-mb", "0"]) == 0
    assert "ARCADEDB_WHEEL_MAX_MB" in capsys.readouterr().out


def test_a_missing_file_or_a_non_wheel_is_a_usage_error(tmp_path):
    module = _load()
    assert module.main([str(tmp_path / "nope.whl")]) == 2
    not_zip = tmp_path / "x.whl"
    not_zip.write_text("not a zip")
    assert module.main([str(not_zip)]) == 2


def test_the_default_limit_is_the_policy_of_100_decimal_megabytes_and_main_uses_it(
    tmp_path, monkeypatch
):
    module = _load()
    assert (module.MB, module.DEFAULT_MAX_MB) == (1_000_000, 100.0)
    assert module.DEFAULT_WARN_MB < module.DEFAULT_MAX_MB
    wheel = _wheel(tmp_path / "a-1-py3-none-any.whl", {"j": 900_000})
    monkeypatch.setattr(
        module, "DEFAULT_MAX_MB", 0.5
    )  # main() reads the default when it parses its arguments
    assert module.main([str(wheel)]) == 1
    monkeypatch.setattr(module, "DEFAULT_MAX_MB", 5.0)
    monkeypatch.setattr(module, "DEFAULT_MIN_MB", 0.0)
    assert module.main([str(wheel)]) == 0


def test_the_script_prints_only_ascii_so_a_windows_console_cannot_crash_it():
    """The Windows CI job failed once with a UnicodeEncodeError on an emoji in the output (cp1252 console)."""
    source = SCRIPT_PATH.read_text(encoding="utf-8")
    body = source.split('"""', 2)[2]
    assert not [c for c in body if ord(c) > 127]
