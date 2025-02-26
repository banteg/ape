"""
Core caching system for ApeWorX.

This module provides a unified caching system with multiple backends
and specialized cache types for different parts of the framework.
"""

# Import these directly to avoid circular imports
from ape.cache.backends import (
    CacheBackend,
    DiskCache,
    MemoryCache,
    NullCache,
    get_cache_backend,
)

__all__ = [
    "CacheBackend",
    "DiskCache",
    "MemoryCache",
    "NullCache",
    "get_cache_backend",
]
