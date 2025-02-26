"""
Cache manager for the ApeWorX caching system.

This module provides a central manager for coordinating between different cache backends
and specialized cache types.
"""

import time
from functools import cached_property
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Type,
    Union,
    Protocol,
    runtime_checkable,
)

from ape.cache.backends import CacheBackend, MemoryCache, get_cache_backend
from ape.logging import logger
from ape.utils.basemodel import ManagerAccessMixin


class CacheManager(ManagerAccessMixin):
    """
    Central manager for the caching system.

    The cache manager coordinates between different cache backends and provides
    a unified API for all caching operations. It also handles cache invalidation
    strategies and collects metrics on cache usage.
    """

    def __init__(self):
        """Initialize the cache manager."""
        self._backends: Dict[str, CacheBackend] = {}
        self._metrics: Dict[str, Dict[str, Any]] = {
            "hits": {},
            "misses": {},
            "sets": {},
            "deletes": {},
        }
        self._start_time = time.time()

    @cached_property
    def default_backend(self) -> CacheBackend:
        """
        Get the default cache backend.

        Returns:
            CacheBackend: The default cache backend.
        """
        # Use memory cache as default for fast access
        return self.get_backend("memory")

    def get_backend(self, backend_type: str, **kwargs) -> CacheBackend:
        """
        Get a cache backend instance.

        Args:
            backend_type: Type of cache backend ('memory', 'disk', or 'null').
            **kwargs: Additional arguments to pass to the backend constructor.

        Returns:
            CacheBackend: A cache backend instance.
        """
        cache_key = f"{backend_type}:{hash(frozenset(kwargs.items()))}"

        if cache_key not in self._backends:
            self._backends[cache_key] = get_cache_backend(backend_type, **kwargs)

        return self._backends[cache_key]

    def get(
        self, key: str, backend: Optional[Union[str, CacheBackend]] = None
    ) -> Optional[Any]:
        """
        Retrieve a value from the cache.

        Args:
            key: The cache key to retrieve.
            backend: The backend to use. Can be a backend instance or a backend type string.
                Defaults to the default backend.

        Returns:
            The cached value or None if not found.
        """
        cache = self._get_backend_instance(backend)
        namespace = self._get_backend_name(cache)

        value = cache.get(key)

        # Update metrics
        if value is None:
            self._increment_metric("misses", namespace)
        else:
            self._increment_metric("hits", namespace)

        return value

    def set(
        self,
        key: str,
        value: Any,
        ttl: Optional[int] = None,
        backend: Optional[Union[str, CacheBackend]] = None,
    ) -> None:
        """
        Store a value in the cache.

        Args:
            key: The cache key to store.
            value: The value to cache.
            ttl: Time-to-live in seconds. None means no expiration.
            backend: The backend to use. Can be a backend instance or a backend type string.
                Defaults to the default backend.
        """
        cache = self._get_backend_instance(backend)
        namespace = self._get_backend_name(cache)

        cache.set(key, value, ttl)
        self._increment_metric("sets", namespace)

    def delete(
        self, key: str, backend: Optional[Union[str, CacheBackend]] = None
    ) -> None:
        """
        Remove a value from the cache.

        Args:
            key: The cache key to remove.
            backend: The backend to use. Can be a backend instance or a backend type string.
                Defaults to the default backend.
        """
        cache = self._get_backend_instance(backend)
        namespace = self._get_backend_name(cache)

        cache.delete(key)
        self._increment_metric("deletes", namespace)

    def clear(self, backend: Optional[Union[str, CacheBackend]] = None) -> None:
        """
        Clear all values from the cache.

        Args:
            backend: The backend to use. Can be a backend instance or a backend type string.
                Defaults to the default backend.
        """
        cache = self._get_backend_instance(backend)
        cache.clear()

    def exists(
        self, key: str, backend: Optional[Union[str, CacheBackend]] = None
    ) -> bool:
        """
        Check if a key exists in the cache.

        Args:
            key: The cache key to check.
            backend: The backend to use. Can be a backend instance or a backend type string.
                Defaults to the default backend.

        Returns:
            True if the key exists, False otherwise.
        """
        cache = self._get_backend_instance(backend)
        return cache.exists(key)

    def get_or_set(
        self,
        key: str,
        default_factory: Callable[[], Any],
        ttl: Optional[int] = None,
        backend: Optional[Union[str, CacheBackend]] = None,
    ) -> Any:
        """
        Get a value from the cache or set it if not present.

        Args:
            key: The cache key.
            default_factory: Function to call to generate the value if not in cache.
            ttl: Time-to-live in seconds. None means no expiration.
            backend: The backend to use. Can be a backend instance or a backend type string.
                Defaults to the default backend.

        Returns:
            The cached or newly generated value.
        """
        cache = self._get_backend_instance(backend)
        namespace = self._get_backend_name(cache)

        value = cache.get(key)
        if value is None:
            self._increment_metric("misses", namespace)
            value = default_factory()
            cache.set(key, value, ttl)
            self._increment_metric("sets", namespace)
        else:
            self._increment_metric("hits", namespace)

        return value

    def get_metrics(self) -> Dict[str, Any]:
        """
        Get cache usage metrics.

        Returns:
            Dict[str, Any]: A dictionary of cache metrics.
        """
        # Create a copy of metrics
        metrics: Dict[str, Any] = {}
        for key, value in self._metrics.items():
            metrics[key] = value.copy()

        # Calculate hit rates
        hit_rates: Dict[str, float] = {}
        for backend in self._backends:
            hits = self._metrics["hits"].get(backend, 0)
            misses = self._metrics["misses"].get(backend, 0)
            total = hits + misses
            hit_rates[backend] = hits / total if total > 0 else 0

        metrics["hit_rates"] = hit_rates
        metrics["uptime"] = time.time() - self._start_time

        return metrics

    def _get_backend_instance(
        self, backend: Optional[Union[str, CacheBackend]]
    ) -> CacheBackend:
        """
        Get a cache backend instance from a backend specification.

        Args:
            backend: The backend to use. Can be a backend instance or a backend type string.
                Defaults to the default backend.

        Returns:
            CacheBackend: A cache backend instance.
        """
        if backend is None:
            return self.default_backend
        elif isinstance(backend, str):
            return self.get_backend(backend)
        elif isinstance(backend, CacheBackend):
            return backend
        else:
            raise TypeError(f"Invalid backend type: {type(backend)}")

    def _get_backend_name(self, backend: CacheBackend) -> str:
        """
        Get the name of a cache backend.

        Args:
            backend: The backend to get the name for.

        Returns:
            str: The name of the backend.
        """
        return backend.name

    def _increment_metric(self, metric_type: str, namespace: str) -> None:
        """
        Increment a metric counter.

        Args:
            metric_type: The type of metric to increment.
            namespace: The namespace of the metric.
        """
        if namespace not in self._metrics[metric_type]:
            self._metrics[metric_type][namespace] = 0

        self._metrics[metric_type][namespace] += 1
