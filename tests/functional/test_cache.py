import time
from pathlib import Path
from typing import Dict, List, Optional

import pytest

from ape.cache.backends import CacheBackend, DiskCache, MemoryCache, NullCache
from ape.cache.manager import CacheManager
from ape.cache.specialized import ContractCache, NetworkCache, TransactionCache


class TestCacheBackends:
    def test_memory_cache(self):
        # Create a memory cache
        cache = MemoryCache(max_size=10)

        # Test basic operations
        cache.set("key1", "value1")
        assert cache.get("key1") == "value1"
        assert cache.exists("key1")
        assert not cache.exists("key2")

        # Test TTL
        cache.set("key2", "value2", ttl=1)
        assert cache.get("key2") == "value2"
        time.sleep(1.1)  # Wait for TTL to expire
        assert cache.get("key2") is None
        assert not cache.exists("key2")

        # Test delete
        cache.set("key3", "value3")
        assert cache.get("key3") == "value3"
        cache.delete("key3")
        assert cache.get("key3") is None
        assert not cache.exists("key3")

        # Test clear
        cache.set("key4", "value4")
        cache.clear()
        assert cache.get("key4") is None
        assert not cache.exists("key4")

        # Test LRU eviction
        for i in range(15):  # More than max_size
            cache.set(f"key{i}", f"value{i}")
        # The first 5 keys should be evicted
        for i in range(5):
            assert cache.get(f"key{i}") is None
        # The last 10 keys should still be there
        for i in range(5, 15):
            assert cache.get(f"key{i}") == f"value{i}"

    def test_null_cache(self):
        # Create a null cache
        cache = NullCache()

        # Test basic operations
        cache.set("key1", "value1")
        assert cache.get("key1") is None
        assert not cache.exists("key1")

        # Test delete and clear (should do nothing)
        cache.delete("key1")
        cache.clear()

    def test_get_or_set(self):
        # Create a memory cache
        cache = MemoryCache()

        # Test get_or_set with missing key
        value = cache.get_or_set("key1", lambda: "computed_value")
        assert value == "computed_value"
        assert cache.get("key1") == "computed_value"

        # Test get_or_set with existing key
        calls = 0

        def expensive_computation():
            nonlocal calls
            calls += 1
            return "expensive_value"

        value = cache.get_or_set("key1", expensive_computation)
        assert value == "computed_value"  # Should use cached value
        assert calls == 0  # Should not call the function

    def test_get_many_set_many(self):
        # Create a memory cache
        cache = MemoryCache()

        # Test set_many
        cache.set_many({"key1": "value1", "key2": "value2", "key3": "value3"})
        assert cache.get("key1") == "value1"
        assert cache.get("key2") == "value2"
        assert cache.get("key3") == "value3"

        # Test get_many
        values = cache.get_many(["key1", "key2", "key4"])
        assert values == {"key1": "value1", "key2": "value2"}

        # Test delete_many
        cache.delete_many(["key1", "key3"])
        assert cache.get("key1") is None
        assert cache.get("key2") == "value2"
        assert cache.get("key3") is None

    def test_incr_decr(self):
        # Create a memory cache
        cache = MemoryCache()

        # Test incr with missing key
        assert cache.incr("counter") == 1
        assert cache.get("counter") == 1

        # Test incr with existing key
        assert cache.incr("counter") == 2
        assert cache.get("counter") == 2

        # Test incr with delta
        assert cache.incr("counter", 5) == 7
        assert cache.get("counter") == 7

        # Test decr
        assert cache.decr("counter") == 6
        assert cache.get("counter") == 6

        # Test decr with delta
        assert cache.decr("counter", 3) == 3
        assert cache.get("counter") == 3

        # Test incr with non-integer value
        cache.set("string", "value")
        with pytest.raises(ValueError):
            cache.incr("string")


class TestCacheManager:
    def test_get_backend(self):
        # Create a cache manager
        manager = CacheManager()

        # Test getting different backends
        memory_cache = manager.get_backend("memory")
        assert isinstance(memory_cache, MemoryCache)

        disk_cache = manager.get_backend("disk", namespace="test")
        assert isinstance(disk_cache, DiskCache)

        null_cache = manager.get_backend("null")
        assert isinstance(null_cache, NullCache)

        # Test getting the same backend twice
        memory_cache2 = manager.get_backend("memory")
        assert memory_cache is memory_cache2  # Should be the same instance

        # Test getting a backend with different parameters
        memory_cache3 = manager.get_backend("memory", max_size=100)
        assert memory_cache is not memory_cache3  # Should be a different instance

        # Test getting an unknown backend
        with pytest.raises(ValueError):
            manager.get_backend("unknown")

    def test_default_backend(self):
        # Create a cache manager
        manager = CacheManager()

        # Test default backend
        assert isinstance(manager.default_backend, MemoryCache)

    def test_cache_operations(self):
        # Create a cache manager
        manager = CacheManager()

        # Test basic operations
        manager.set("key1", "value1")
        assert manager.get("key1") == "value1"
        assert manager.exists("key1")
        assert not manager.exists("key2")

        # Test delete
        manager.delete("key1")
        assert manager.get("key1") is None
        assert not manager.exists("key1")

        # Test clear
        manager.set("key2", "value2")
        manager.clear()
        assert manager.get("key2") is None
        assert not manager.exists("key2")

        # Test get_or_set
        value = manager.get_or_set("key3", lambda: "computed_value")
        assert value == "computed_value"
        assert manager.get("key3") == "computed_value"

    def test_metrics(self):
        # Create a cache manager
        manager = CacheManager()

        # Test metrics with no operations
        metrics = manager.get_metrics()
        assert "hits" in metrics
        assert "misses" in metrics
        assert "sets" in metrics
        assert "deletes" in metrics
        assert "hit_rates" in metrics
        assert "uptime" in metrics

        # Test metrics with operations
        manager.set("key1", "value1")
        manager.get("key1")  # Hit
        manager.get("key2")  # Miss
        manager.delete("key1")

        metrics = manager.get_metrics()
        assert metrics["hits"].get("memory", 0) == 1
        assert metrics["misses"].get("memory", 0) == 1
        assert metrics["sets"].get("memory", 0) == 1
        assert metrics["deletes"].get("memory", 0) == 1
        assert metrics["hit_rates"].get("memory", 0) == 0.5  # 1 hit, 1 miss


class TestSpecializedCaches:
    def test_contract_cache(self):
        # Create a cache manager
        manager = CacheManager()

        # Create a contract cache
        contract_cache = ContractCache(manager)

        # Test namespace
        assert contract_cache.namespace == "contract"

        # Test key generation
        key = contract_cache._get_key("test")
        assert key == "contract:test"

        # Test basic operations
        contract_cache.set("key1", "value1")
        assert contract_cache.get("key1") == "value1"
        assert contract_cache.exists("key1")
        assert not contract_cache.exists("key2")

        # Test delete
        contract_cache.delete("key1")
        assert contract_cache.get("key1") is None
        assert not contract_cache.exists("key1")

    def test_network_cache(self):
        # Create a cache manager
        manager = CacheManager()

        # Create a network cache
        network_cache = NetworkCache(manager)

        # Test namespace
        assert network_cache.namespace == "network"

        # Test key generation
        key = network_cache._get_key("test")
        assert key == "network:test"

        # Test basic operations
        network_cache.set("key1", "value1")
        assert network_cache.get("key1") == "value1"
        assert network_cache.exists("key1")
        assert not network_cache.exists("key2")

        # Test delete
        network_cache.delete("key1")
        assert network_cache.get("key1") is None
        assert not network_cache.exists("key1")

    def test_transaction_cache(self):
        # Create a cache manager
        manager = CacheManager()

        # Create a transaction cache
        transaction_cache = TransactionCache(manager)

        # Test namespace
        assert transaction_cache.namespace == "transaction"

        # Test key generation
        key = transaction_cache._get_key("test")
        assert key == "transaction:test"

        # Test basic operations
        transaction_cache.set("key1", "value1")
        assert transaction_cache.get("key1") == "value1"
        assert transaction_cache.exists("key1")
        assert not transaction_cache.exists("key2")

        # Test delete
        transaction_cache.delete("key1")
        assert transaction_cache.get("key1") is None
        assert not transaction_cache.exists("key1")
