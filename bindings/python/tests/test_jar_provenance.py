"""The wheel can say which engine it carries, not just which version it is.

`__version__` is the package version. It is baked from the pom and says
nothing about the JARs that ended up in the wheel, so a build from a locally
patched Java tree reports the released version while shipping a different
engine. That happened: a local build shipped 59 JARs instead of 64 with a
bt-server-patched engine, and only a packaging test that counted JAR names
noticed. Benchmarks against such a wheel record the released version string
beside numbers the release cannot reproduce.

`jar_fingerprint()` hashes what is actually on disk, so two installs agree iff
their engines agree. These tests check the hash covers what it claims rather
than merely returning a string, because a fingerprint nobody can trust is worse
than none: it invites exactly the assumption it was added to prevent.
"""

import hashlib
import os

import arcadedb_embedded as adb


def test_fingerprint_is_deterministic():
    """Same install, same answer. Otherwise it cannot compare two installs."""
    a = adb.jar_fingerprint()
    b = adb.jar_fingerprint()
    assert a["sha256"] == b["sha256"]
    assert a["count"] == b["count"]
    assert len(a["sha256"]) == 64, "expected a hex sha256"


def test_fingerprint_matches_what_is_on_disk():
    """count and bytes are read from the filesystem, not asserted."""
    fp = adb.jar_fingerprint()
    names = [n for n in os.listdir(fp["jar_dir"]) if n.endswith(".jar")]
    assert fp["count"] == len(
        names
    ), f"fingerprint claims {fp['count']} JARs, directory has {len(names)}"
    total = sum(os.path.getsize(os.path.join(fp["jar_dir"], n)) for n in names)
    assert fp["bytes"] == total
    assert fp["count"] > 0, "a wheel with no JARs is not a working install"


def test_hash_actually_covers_every_jar_name_and_content():
    """Recompute the combined hash from the per-JAR digests.

    This is the test that makes the fingerprint worth trusting. Without it,
    sha256 could be a hash of the count, or of one JAR, or of nothing, and
    every other assertion here would still pass.
    """
    fp = adb.jar_fingerprint(per_jar=True)
    combined = hashlib.sha256()
    for entry in fp["jars"]:
        combined.update(entry["name"].encode())
        combined.update(bytes.fromhex(entry["sha256"]))
    assert combined.hexdigest() == fp["sha256"], (
        "combined hash is not the hash of the per-JAR (name, digest) pairs, so "
        "it does not mean what the docstring says it means"
    )


def test_per_jar_digests_are_the_real_file_digests():
    """Spot-check against the bytes on disk, so the per-JAR list is evidence."""
    fp = adb.jar_fingerprint(per_jar=True)
    # The largest JAR: most likely to catch a truncated or streamed-wrong read.
    biggest = max(fp["jars"], key=lambda e: e["bytes"])
    path = os.path.join(fp["jar_dir"], biggest["name"])
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    assert h.hexdigest() == biggest["sha256"], f"digest wrong for {biggest['name']}"
    assert os.path.getsize(path) == biggest["bytes"]


def test_renaming_a_jar_would_change_the_fingerprint():
    """Name is hashed, not only content.

    Two JARs swapping filenames leaves the multiset of contents identical. If
    only contents were hashed, that swap would be invisible, and it is exactly
    what a mis-staged JAR_LIB_DIR can produce.
    """
    fp = adb.jar_fingerprint(per_jar=True)
    jars = fp["jars"]
    if len(jars) < 2:
        # Not a skip in disguise: with one JAR the property is vacuous, and
        # test_fingerprint_matches_what_is_on_disk already asserts count > 0.
        return
    swapped = hashlib.sha256()
    order = [jars[1], jars[0]] + jars[2:]
    for name_entry, digest_entry in zip(jars, order):
        swapped.update(name_entry["name"].encode())
        swapped.update(bytes.fromhex(digest_entry["sha256"]))
    assert (
        swapped.hexdigest() != fp["sha256"]
    ), "swapping two JARs' contents does not change the fingerprint"


def test_engine_hash_excludes_our_own_jar():
    """engine_sha256 answers "same ArcadeDB?", sha256 answers "same build?".

    Measured 2026-08-04: the PyPI 26.8.1 wheel and a local build of the same
    version have identical size, identical JAR count, identical total JAR
    bytes, and different sha256. 63 of 64 JARs are byte-identical; the only
    difference is arcadedb-python-bridge.jar, at the same 10907 bytes, because
    it is compiled during the build and carries timestamps.

    So the two hashes must actually differ in coverage, or engine_sha256 is
    decoration and someone will compare the wrong one.
    """
    fp = adb.jar_fingerprint(per_jar=True)
    ours = [j for j in fp["jars"] if not j["engine"]]
    assert ours, "no JAR is marked as ours; _OUR_JARS is stale against the wheel"
    assert fp["engine_count"] == fp["count"] - len(ours)
    assert (
        fp["engine_sha256"] != fp["sha256"]
    ), "engine hash equals the full hash, so it is not excluding anything"

    combined = hashlib.sha256()
    for entry in fp["jars"]:
        if entry["engine"]:
            combined.update(entry["name"].encode())
            combined.update(bytes.fromhex(entry["sha256"]))
    assert (
        combined.hexdigest() == fp["engine_sha256"]
    ), "engine_sha256 is not the hash of exactly the engine JARs"


def test_our_jar_list_still_matches_the_wheel():
    """_OUR_JARS names a JAR that exists.

    If the bridge JAR is ever renamed, every engine comparison silently starts
    including a non-reproducible JAR again and stops meaning what it says.
    """
    from arcadedb_embedded.jvm import _OUR_JARS

    names = {j["name"] for j in adb.jar_fingerprint(per_jar=True)["jars"]}
    missing = _OUR_JARS - names
    assert not missing, (
        f"_OUR_JARS lists {sorted(missing)}, absent from the wheel. Renamed? "
        f"engine_sha256 is now hashing a JAR we build."
    )


def test_exported_from_the_package_root():
    """Harnesses record this next to engine_version, so it must be public."""
    assert "jar_fingerprint" in adb.__all__
    assert callable(adb.jar_fingerprint)
