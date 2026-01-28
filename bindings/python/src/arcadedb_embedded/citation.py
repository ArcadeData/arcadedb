"""Citation helpers for ArcadeDB Embedded Python."""

from __future__ import annotations

from typing import Optional

from ._version import __version__
from .exceptions import ArcadeDBError

# Map released package versions to Zenodo version-specific DOIs.
# Update this mapping on each release.
_VERSION_DOI_MAP = {
    "26.1.1.post3": "10.5281/zenodo.18399749",
}


def cite(version: Optional[str] = None) -> str:
    """Return the DOI URL for a given ArcadeDB Embedded Python version.

    Parameters
    ----------
    version : str or None
        The version to cite. If None, the current installed version is used.

    Returns
    -------
    doi_url : str
        The DOI URL for the given version.

    Raises
    ------
    ArcadeDBError
        If the requested version is not found in the citation index.

    Examples
    --------
    >>> import arcadedb_embedded as arcadedb
    >>> arcadedb.cite("26.1.1.post3")
    "https://doi.org/10.5281/zenodo.18399749"
    """

    if version is None:
        version = __version__

    if version not in _VERSION_DOI_MAP:
        if "dev" in version:
            raise ArcadeDBError(
                f"Version {version} is not yet released and therefore does not yet have a citable DOI."
            )
        raise ArcadeDBError(f"Version {version} not found in the citation index")

    return f"https://doi.org/{_VERSION_DOI_MAP[version]}"
