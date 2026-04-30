"""Regression tests for issue #4037: wheel manylinux platform tag.

The Linux wheel must be tagged with the same glibc version that the bundled JRE
binaries actually require. Tagging higher than the JRE needs (e.g. manylinux_2_35
when the JRE only references GLIBC_2.34 symbols) blocks installation on systems
running glibc 2.34 (RHEL 9, Rocky 9, AlmaLinux 9, Amazon Linux 2023, ...) for no
reason. Tagging lower lets the wheel install where the JRE cannot run.

These tests exercise the verifier script directly with synthetic ELF files so we
do not depend on Docker or a built wheel.

@author Luca Garulli (l.garulli@arcadedata.com)
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "verify_wheel_platform_tag.py"


def _load_verifier():
    spec = importlib.util.spec_from_file_location("verify_wheel_platform_tag", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def _make_elf_with_glibc_symbols(path: Path, versions: list[str]) -> None:
    """Create a tiny pseudo-ELF file containing GLIBC_X.Y strings.

    The verifier only scans for the literal pattern ``GLIBC_<major>.<minor>``;
    it does not parse ELF dynamic sections. A minimal ELF magic prefix plus the
    embedded version strings is enough to mimic a real binary for the test.
    """
    payload = b"\x7fELF" + b"\x00" * 12
    payload += b"\x00".join(f"GLIBC_{v}".encode("ascii") for v in versions)
    payload += b"\x00"
    path.write_bytes(payload)


def test_max_glibc_in_dir_picks_highest(tmp_path: Path) -> None:
    verifier = _load_verifier()
    _make_elf_with_glibc_symbols(tmp_path / "java", ["2.17", "2.34"])
    _make_elf_with_glibc_symbols(tmp_path / "libjvm.so", ["2.17", "2.28", "2.34"])

    assert verifier.max_glibc_in_dir(tmp_path) == (2, 34)


def test_parse_wheel_tag_extracts_version_and_arch(tmp_path: Path) -> None:
    verifier = _load_verifier()
    wheel = tmp_path / "arcadedb_embedded-26.4.2-py3-none-manylinux_2_34_x86_64.whl"
    wheel.write_bytes(b"")

    glibc, arch = verifier.parse_wheel_tag(wheel)
    assert glibc == (2, 34)
    assert arch == "x86_64"


def test_main_succeeds_when_tag_matches(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    verifier = _load_verifier()
    jre_dir = tmp_path / "jre"
    jre_dir.mkdir()
    _make_elf_with_glibc_symbols(jre_dir / "libjvm.so", ["2.17", "2.34"])

    wheel = tmp_path / "arcadedb_embedded-26.4.2-py3-none-manylinux_2_34_x86_64.whl"
    wheel.write_bytes(b"")

    rc = _run_main(verifier, [str(wheel), str(jre_dir)])
    assert rc == 0
    assert "OK" in capsys.readouterr().out


def test_main_fails_when_tag_higher_than_jre(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """Reproduces issue #4037: wheel tagged manylinux_2_35 but JRE only needs GLIBC_2.34."""
    verifier = _load_verifier()
    jre_dir = tmp_path / "jre"
    jre_dir.mkdir()
    _make_elf_with_glibc_symbols(jre_dir / "libjvm.so", ["2.17", "2.34"])

    wheel = tmp_path / "arcadedb_embedded-26.4.2-py3-none-manylinux_2_35_x86_64.whl"
    wheel.write_bytes(b"")

    rc = _run_main(verifier, [str(wheel), str(jre_dir)])
    captured = capsys.readouterr()
    assert rc != 0
    assert "does not match" in captured.err
    assert "manylinux_2_35" in captured.err or "manylinux_2_34" in captured.err


def test_main_fails_when_tag_lower_than_jre(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """A too-low tag would let the wheel install on systems where the JRE cannot run."""
    verifier = _load_verifier()
    jre_dir = tmp_path / "jre"
    jre_dir.mkdir()
    _make_elf_with_glibc_symbols(jre_dir / "libjvm.so", ["2.17", "2.36"])

    wheel = tmp_path / "arcadedb_embedded-26.4.2-py3-none-manylinux_2_28_x86_64.whl"
    wheel.write_bytes(b"")

    rc = _run_main(verifier, [str(wheel), str(jre_dir)])
    assert rc != 0
    assert "does not match" in capsys.readouterr().err


def _run_main(verifier, argv: list[str]) -> int:
    import sys

    saved = sys.argv
    sys.argv = ["verify_wheel_platform_tag.py", *argv]
    try:
        return verifier.main()
    finally:
        sys.argv = saved


def test_module_exposes_public_api() -> None:
    """Sanity check: the verifier module imports without errors and exposes its API."""
    verifier = _load_verifier()
    assert hasattr(verifier, "max_glibc_in_dir")
    assert hasattr(verifier, "parse_wheel_tag")
    assert hasattr(verifier, "main")
