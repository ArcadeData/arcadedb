"""
Setup configuration for arcadedb-embedded.

This file forces the wheel to be platform-specific (not py3-none-any)
because we bundle platform-specific JRE binaries.
"""

from setuptools import setup
from setuptools.dist import Distribution


class BinaryDistribution(Distribution):
    """Mark package as having platform-specific content."""

    def has_ext_modules(self):
        # Return True to indicate this is a platform-specific package
        # even though we don't have C extensions
        return True


# All other configuration is in pyproject.toml
setup(distclass=BinaryDistribution)
