"""Range-addressable reverse identity locator artifacts."""

from .benchmark import (
    IdentityLocatorBenchmarkConfig,
    IdentityLocatorBenchmarkResult,
    benchmark_identity_locator,
)
from .builder import (
    IdentityLocatorBuildConfig,
    IdentityLocatorBuildResult,
    build_identity_locator,
)
from .format import IdentityLocatorHeader, NamespaceDescriptor
from .reader import IdentityLocatorReader, StarRef, validate_identity_locator

__all__ = [
    "IdentityLocatorBuildConfig",
    "IdentityLocatorBuildResult",
    "IdentityLocatorBenchmarkConfig",
    "IdentityLocatorBenchmarkResult",
    "IdentityLocatorHeader",
    "IdentityLocatorReader",
    "NamespaceDescriptor",
    "StarRef",
    "build_identity_locator",
    "benchmark_identity_locator",
    "validate_identity_locator",
]
