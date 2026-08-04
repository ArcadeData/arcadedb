"""Tests for the citation helper (offline: Zenodo access is mocked)."""

import pytest
from arcadedb_embedded import citation
from arcadedb_embedded.exceptions import ArcadeDBError


@pytest.fixture(autouse=True)
def _no_network(monkeypatch):
    """Fail loudly if a test reaches for the real Zenodo API."""

    def boom(url):
        raise AssertionError(f"unexpected network call: {url}")

    monkeypatch.setattr(citation, "_zenodo_get", boom)


def test_cached_version_no_network(monkeypatch):
    monkeypatch.setitem(
        citation._VERSION_DOI_MAP, "26.1.1.post3", "10.5281/zenodo.18399749"
    )
    assert citation.cite("26.1.1.post3") == "https://doi.org/10.5281/zenodo.18399749"


def test_dev_version_rejected_without_network():
    with pytest.raises(ArcadeDBError, match="not a release"):
        citation.cite("26.8.1.dev0")


def test_live_lookup_mocked(monkeypatch):
    pages = {
        1: {
            "hits": {
                "hits": [
                    {
                        "metadata": {"version": "26.6.1"},
                        "doi": "10.5281/zenodo.20708871",
                    }
                ]
            }
        },
        2: {"hits": {"hits": []}},
    }

    def fake_get(url):
        if url.endswith(f"/records/{citation._ZENODO_CONCEPT_RECID}"):
            return {"id": "99999"}
        page = int(url.rsplit("page=", 1)[1])
        return pages[page]

    monkeypatch.setattr(citation, "_zenodo_get", fake_get)
    monkeypatch.delitem(citation._VERSION_DOI_MAP, "26.6.1", raising=False)
    assert citation.cite("26.6.1") == "https://doi.org/10.5281/zenodo.20708871"
    # resolved DOI is cached for the process
    assert citation._VERSION_DOI_MAP["26.6.1"] == "10.5281/zenodo.20708871"


def test_unknown_version_not_on_zenodo(monkeypatch):
    def fake_get(url):
        if url.endswith(f"/records/{citation._ZENODO_CONCEPT_RECID}"):
            return {"id": "99999"}
        return {"hits": {"hits": []}}

    monkeypatch.setattr(citation, "_zenodo_get", fake_get)
    with pytest.raises(ArcadeDBError, match="not found"):
        citation.cite("99.9.9")
