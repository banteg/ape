# ApeWorX Caching System

This module provides a unified caching system for ApeWorX with multiple backends and specialized cache types for different parts of the framework.

## Architecture

The caching system consists of the following components:

### Core Components

1. **Cache Backends**: Implementations of the `CacheBackend` protocol that provide different storage mechanisms:
   - `MemoryCache`: Fast in-memory cache using LRU eviction
   - `DiskCache`: Persistent cache using SQLite
   - `NullCache`: No-op implementation for testing or when caching is disabled

2. **Cache Manager**: Central manager that coordinates between different cache backends and provides a unified API for all caching operations.

3. **Specialized Cache Types**: Cache implementations tailored for specific parts of the framework:
   - `ContractCache`: For contract ABIs, bytecode, and metadata
   - `NetworkCache`: For network-specific data
   - `QueryCache`: For blockchain data queries
   - `TransactionCache`: For transaction history and receipts

## Usage

### Basic Usage

```python
from ape.cache import CacheManager, MemoryCache

# Create a cache manager
cache_manager = CacheManager()

# Get a memory cache backend
memory_cache = cache_manager.get_backend("memory")

# Store a value in the cache
memory_cache.set("key", "value")

# Retrieve a value from the cache
value = memory_cache.get("key")

# Check if a key exists in the cache
exists = memory_cache.exists("key")

# Delete a value from the cache
memory_cache.delete("key")

# Clear all values from the cache
memory_cache.clear()
```

### Using Specialized Caches

```python
from ape.cache.specialized import ContractCache, TransactionCache

# Create specialized caches
contract_cache = ContractCache(cache_manager)
transaction_cache = TransactionCache(cache_manager)

# Store a contract type
contract_cache.set_contract_type(address, contract_type)

# Retrieve a contract type
contract_type = contract_cache.get_contract_type(address)

# Store a transaction
transaction_cache.set_transaction(txn_hash, transaction)

# Retrieve a transaction
transaction = transaction_cache.get_transaction(txn_hash)
```

### Cache Backends

The caching system supports multiple backends:

1. **Memory Cache**: Fast in-memory cache using LRU eviction
   ```python
   memory_cache = cache_manager.get_backend("memory", max_size=1000)
   ```

2. **Disk Cache**: Persistent cache using SQLite
   ```python
   disk_cache = cache_manager.get_backend("disk", namespace="contracts", max_size=1024**3)
   ```

3. **Null Cache**: No-op implementation for testing or when caching is disabled
   ```python
   null_cache = cache_manager.get_backend("null")
   ```

### Advanced Features

1. **Time-to-Live (TTL)**: Set expiration time for cache entries
   ```python
   cache.set("key", "value", ttl=3600)  # Expires after 1 hour
   ```

2. **Get or Set**: Retrieve a value or compute and store it if not present
   ```python
   value = cache.get_or_set("key", lambda: expensive_computation(), ttl=3600)
   ```

3. **Metrics**: Get cache usage metrics
   ```python
   metrics = cache_manager.get_metrics()
   hit_rate = metrics["hit_rates"]["memory"]
   ```

## Design Principles

1. **Unified API**: All caching operations use the same interface
2. **Flexible Backends**: Easy to switch between memory, disk, or remote caching
3. **Intelligent Invalidation**: Smart strategies to keep cache fresh
4. **Configurable**: Extensive options for tuning cache behavior
5. **Metrics**: Visibility into cache performance
6. **Type Safety**: Better typing for cached objects
