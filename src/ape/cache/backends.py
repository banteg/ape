"""
Cache backend implementations for the ApeWorX caching system.

This module provides different cache backend implementations that can be used
with the caching system, including memory, disk, and null backends.
"""

import json
import os
import pickle
import sqlite3
import time
from abc import ABC, abstractmethod
from collections import OrderedDict
from contextlib import contextmanager
from functools import wraps
from pathlib import Path
from typing import (
    Any,
    Callable,
    Dict,
    Iterator,
    Optional,
    Protocol,
    TypeVar,
    Union,
    cast,
    runtime_checkable,
)

from ape.exceptions import ApeException
from ape.logging import logger
from ape.utils.basemodel import ManagerAccessMixin

T = TypeVar("T")


@runtime_checkable
class CacheBackend(Protocol):
    """Protocol defining the interface for all cache backends."""

    @property
    def name(self) -> str:
        """The name of the cache backend."""
        ...

    def get(self, key: str) -> Optional[Any]:
        """
        Retrieve a value from the cache.

        Args:
            key: The cache key to retrieve.

        Returns:
            The cached value or None if not found.
        """
        ...

    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        """
        Store a value in the cache with optional TTL.

        Args:
            key: The cache key to store.
            value: The value to cache.
            ttl: Time-to-live in seconds. None means no expiration.
        """
        ...

    def delete(self, key: str) -> None:
        """
        Remove a value from the cache.

        Args:
            key: The cache key to remove.
        """
        ...

    def clear(self) -> None:
        """Clear all values from the cache."""
        ...

    def exists(self, key: str) -> bool:
        """
        Check if a key exists in the cache.

        Args:
            key: The cache key to check.

        Returns:
            True if the key exists, False otherwise.
        """
        ...

    def get_or_set(
        self, key: str, default_factory: Callable[[], T], ttl: Optional[int] = None
    ) -> T:
        """
        Get a value from the cache or set it if not present.

        Args:
            key: The cache key.
            default_factory: Function to call to generate the value if not in cache.
            ttl: Time-to-live in seconds. None means no expiration.

        Returns:
            The cached or newly generated value.
        """
        ...

    def get_many(self, keys: list[str]) -> Dict[str, Any]:
        """
        Retrieve multiple values from the cache.

        Args:
            keys: List of cache keys to retrieve.

        Returns:
            Dictionary mapping keys to their cached values.
        """
        ...

    def set_many(self, mapping: Dict[str, Any], ttl: Optional[int] = None) -> None:
        """
        Store multiple values in the cache.

        Args:
            mapping: Dictionary mapping keys to values.
            ttl: Time-to-live in seconds. None means no expiration.
        """
        ...

    def delete_many(self, keys: list[str]) -> None:
        """
        Remove multiple values from the cache.

        Args:
            keys: List of cache keys to remove.
        """
        ...

    def incr(self, key: str, delta: int = 1) -> int:
        """
        Increment a numeric value in the cache.

        Args:
            key: The cache key.
            delta: Amount to increment by.

        Returns:
            The new value.

        Raises:
            ValueError: If the value is not an integer.
        """
        ...

    def decr(self, key: str, delta: int = 1) -> int:
        """
        Decrement a numeric value in the cache.

        Args:
            key: The cache key.
            delta: Amount to decrement by.

        Returns:
            The new value.

        Raises:
            ValueError: If the value is not an integer.
        """
        ...


class BaseCacheBackend(ABC):
    """Base class for cache backend implementations."""

    @property
    @abstractmethod
    def name(self) -> str:
        """The name of the cache backend."""
        ...

    @abstractmethod
    def get(self, key: str) -> Optional[Any]:
        """
        Retrieve a value from the cache.

        Args:
            key: The cache key to retrieve.

        Returns:
            The cached value or None if not found.
        """
        ...

    @abstractmethod
    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        """
        Store a value in the cache with optional TTL.

        Args:
            key: The cache key to store.
            value: The value to cache.
            ttl: Time-to-live in seconds. None means no expiration.
        """
        ...

    @abstractmethod
    def delete(self, key: str) -> None:
        """
        Remove a value from the cache.

        Args:
            key: The cache key to remove.
        """
        ...

    @abstractmethod
    def clear(self) -> None:
        """Clear all values from the cache."""
        ...

    @abstractmethod
    def exists(self, key: str) -> bool:
        """
        Check if a key exists in the cache.

        Args:
            key: The cache key to check.

        Returns:
            True if the key exists, False otherwise.
        """
        ...

    def get_or_set(
        self, key: str, default_factory: Callable[[], T], ttl: Optional[int] = None
    ) -> T:
        """
        Get a value from the cache or set it if not present.

        Args:
            key: The cache key.
            default_factory: Function to call to generate the value if not in cache.
            ttl: Time-to-live in seconds. None means no expiration.

        Returns:
            The cached or newly generated value.
        """
        value = self.get(key)
        if value is None:
            value = default_factory()
            self.set(key, value, ttl)
        return cast(T, value)

    def get_many(self, keys: list[str]) -> Dict[str, Any]:
        """
        Retrieve multiple values from the cache.

        Args:
            keys: List of cache keys to retrieve.

        Returns:
            Dictionary mapping keys to their cached values.
        """
        return {key: value for key in keys if (value := self.get(key)) is not None}

    def set_many(self, mapping: Dict[str, Any], ttl: Optional[int] = None) -> None:
        """
        Store multiple values in the cache.

        Args:
            mapping: Dictionary mapping keys to values.
            ttl: Time-to-live in seconds. None means no expiration.
        """
        for key, value in mapping.items():
            self.set(key, value, ttl)

    def delete_many(self, keys: list[str]) -> None:
        """
        Remove multiple values from the cache.

        Args:
            keys: List of cache keys to remove.
        """
        for key in keys:
            self.delete(key)

    def incr(self, key: str, delta: int = 1) -> int:
        """
        Increment a numeric value in the cache.

        Args:
            key: The cache key.
            delta: Amount to increment by.

        Returns:
            The new value.

        Raises:
            ValueError: If the value is not an integer.
        """
        value = self.get(key)
        if value is None:
            value = 0
        elif not isinstance(value, int):
            raise ValueError(f"Cannot increment non-integer value: {value}")

        new_value = value + delta
        self.set(key, new_value)
        return new_value

    def decr(self, key: str, delta: int = 1) -> int:
        """
        Decrement a numeric value in the cache.

        Args:
            key: The cache key.
            delta: Amount to decrement by.

        Returns:
            The new value.

        Raises:
            ValueError: If the value is not an integer.
        """
        return self.incr(key, -delta)


def is_json_serializable(value: Any) -> bool:
    """
    Check if a value can be serialized to JSON.

    Args:
        value: The value to check.

    Returns:
        bool: True if the value can be serialized to JSON, False otherwise.
    """
    try:
        json.dumps(value)
        return True
    except (TypeError, OverflowError, ValueError):
        return False


class CacheError(ApeException):
    """Exception raised for cache-related errors."""

    pass


class MemoryCache(BaseCacheBackend):
    """In-memory LRU cache implementation."""

    def __init__(self, max_size: int = 1000):
        """
        Initialize the memory cache.

        Args:
            max_size: Maximum number of items to store in the cache.
        """
        self._cache: OrderedDict[str, tuple[Any, Optional[float]]] = OrderedDict()
        self.max_size = max_size

    @property
    def name(self) -> str:
        return "memory"

    def get(self, key: str) -> Optional[Any]:
        if key not in self._cache:
            return None

        value, expires_at = self._cache[key]

        # Check if expired
        if expires_at is not None and time.time() > expires_at:
            self.delete(key)
            return None

        # Move to end (most recently used)
        self._cache.move_to_end(key)
        return value

    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        # Calculate expiration time if TTL is provided
        expires_at = time.time() + ttl if ttl is not None else None

        # Add to cache
        self._cache[key] = (value, expires_at)

        # Move to end (most recently used)
        self._cache.move_to_end(key)

        # Evict oldest items if cache is too large
        while len(self._cache) > self.max_size:
            self._cache.popitem(last=False)

    def delete(self, key: str) -> None:
        self._cache.pop(key, None)

    def clear(self) -> None:
        self._cache.clear()

    def exists(self, key: str) -> bool:
        if key not in self._cache:
            return False

        _, expires_at = self._cache[key]

        # Check if expired
        if expires_at is not None and time.time() > expires_at:
            self.delete(key)
            return False

        return True


class DiskCache(BaseCacheBackend, ManagerAccessMixin):
    """Disk-based cache implementation using SQLite."""

    def __init__(self, namespace: str, max_size: int = 1024 * 1024 * 1024):
        """
        Initialize the disk cache.

        Args:
            namespace: Namespace for the cache (used in file path).
            max_size: Maximum size in bytes (default: 1GB).
        """
        self.namespace = namespace
        self.max_size = max_size
        self._conn = None
        self._initialize_db()

    @property
    def name(self) -> str:
        return "disk"

    @property
    def _db_path(self) -> Path:
        """Get the path to the SQLite database file."""
        cache_dir = self.config_manager.DATA_FOLDER / "cache" / self.namespace
        cache_dir.mkdir(parents=True, exist_ok=True)
        return cache_dir / "cache.db"

    def _initialize_db(self) -> None:
        """Initialize the SQLite database."""
        with self._get_connection() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS cache (
                    key TEXT PRIMARY KEY,
                    value BLOB NOT NULL,
                    expires_at REAL,
                    created_at REAL NOT NULL,
                    size INTEGER NOT NULL
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_expires_at ON cache(expires_at)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_created_at ON cache(created_at)"
            )

    @contextmanager
    def _get_connection(self) -> Iterator[sqlite3.Connection]:
        """Get a SQLite connection."""
        conn = sqlite3.connect(str(self._db_path))
        try:
            yield conn
        finally:
            conn.close()

    def _serialize(self, value: Any) -> tuple[bytes, int]:
        """
        Serialize a value for storage.

        Returns:
            Tuple of (serialized_data, size_in_bytes)
        """
        if is_json_serializable(value):
            data = json.dumps(value).encode("utf-8")
        else:
            try:
                data = pickle.dumps(value)
            except (pickle.PickleError, TypeError) as e:
                raise CacheError(f"Could not serialize value: {e}") from e

        return data, len(data)

    def _deserialize(self, data: bytes) -> Any:
        """Deserialize a value from storage."""
        try:
            # Try JSON first
            return json.loads(data.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            # Fall back to pickle
            try:
                return pickle.loads(data)
            except pickle.PickleError as e:
                raise CacheError(f"Could not deserialize value: {e}") from e

    def get(self, key: str) -> Optional[Any]:
        with self._get_connection() as conn:
            cursor = conn.execute(
                "SELECT value, expires_at FROM cache WHERE key = ?", (key,)
            )
            row = cursor.fetchone()

            if row is None:
                return None

            value_data, expires_at = row

            # Check if expired
            if expires_at is not None and time.time() > expires_at:
                self.delete(key)
                return None

            return self._deserialize(value_data)

    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        # Serialize the value
        try:
            data, size = self._serialize(value)
        except CacheError as e:
            logger.warning(f"Failed to cache value for key '{key}': {e}")
            return

        # Calculate expiration time if TTL is provided
        expires_at = time.time() + ttl if ttl is not None else None

        with self._get_connection() as conn:
            # Insert or replace the value
            conn.execute(
                """
                INSERT OR REPLACE INTO cache (key, value, expires_at, created_at, size)
                VALUES (?, ?, ?, ?, ?)
                """,
                (key, data, expires_at, time.time(), size),
            )

            # Enforce size limit
            self._enforce_size_limit(conn)

    def delete(self, key: str) -> None:
        with self._get_connection() as conn:
            conn.execute("DELETE FROM cache WHERE key = ?", (key,))

    def clear(self) -> None:
        with self._get_connection() as conn:
            conn.execute("DELETE FROM cache")

    def exists(self, key: str) -> bool:
        with self._get_connection() as conn:
            cursor = conn.execute("SELECT expires_at FROM cache WHERE key = ?", (key,))
            row = cursor.fetchone()

            if row is None:
                return False

            expires_at = row[0]

            # Check if expired
            if expires_at is not None and time.time() > expires_at:
                self.delete(key)
                return False

            return True

    def _enforce_size_limit(self, conn: sqlite3.Connection) -> None:
        """Enforce the maximum cache size by removing old entries."""
        # Get current cache size
        cursor = conn.execute("SELECT SUM(size) FROM cache")
        row = cursor.fetchone()
        current_size = row[0] or 0

        if current_size <= self.max_size:
            return

        # Remove expired entries first
        conn.execute(
            "DELETE FROM cache WHERE expires_at IS NOT NULL AND expires_at < ?",
            (time.time(),),
        )

        # Check size again
        cursor = conn.execute("SELECT SUM(size) FROM cache")
        row = cursor.fetchone()
        current_size = row[0] or 0

        if current_size <= self.max_size:
            return

        # Remove oldest entries until under size limit
        cursor = conn.execute("SELECT key, size FROM cache ORDER BY created_at ASC")

        to_delete = []
        deleted_size = 0

        for key, size in cursor:
            to_delete.append(key)
            deleted_size += size
            if current_size - deleted_size <= self.max_size:
                break

        if to_delete:
            placeholders = ",".join("?" for _ in to_delete)
            conn.execute(f"DELETE FROM cache WHERE key IN ({placeholders})", to_delete)


class NullCache(BaseCacheBackend):
    """No-op cache implementation."""

    @property
    def name(self) -> str:
        return "null"

    def get(self, key: str) -> Optional[Any]:
        return None

    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        pass

    def delete(self, key: str) -> None:
        pass

    def clear(self) -> None:
        pass

    def exists(self, key: str) -> bool:
        return False


def get_cache_backend(backend_type: str, **kwargs) -> CacheBackend:
    """
    Get a cache backend instance.

    Args:
        backend_type: Type of cache backend ('memory', 'disk', or 'null').
        **kwargs: Additional arguments to pass to the backend constructor.

    Returns:
        A cache backend instance.

    Raises:
        ValueError: If the backend type is not recognized.
    """
    if backend_type == "memory":
        return MemoryCache(**kwargs)
    elif backend_type == "disk":
        return DiskCache(**kwargs)
    elif backend_type == "null":
        return NullCache()
    else:
        raise ValueError(f"Unknown cache backend type: {backend_type}")
