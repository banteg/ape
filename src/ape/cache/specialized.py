"""
Specialized cache types for the ApeWorX caching system.

This module provides specialized cache types for different parts of the framework,
such as contract caching, network caching, and transaction caching.
"""

from functools import cached_property
from typing import Any, Callable, Dict, List, Optional, Type, Union, cast

from ethpm_types import ContractType

from ape.api.networks import ProxyInfoAPI
from ape.api.query import ContractCreation
from ape.api.transactions import ReceiptAPI, TransactionAPI
from ape.cache.backends import CacheBackend
from ape.cache.manager import CacheManager
from ape.logging import logger
from ape.types.address import AddressType
from ape.utils.basemodel import ManagerAccessMixin


class SpecializedCache(ManagerAccessMixin):
    """Base class for specialized cache types."""

    def __init__(self, cache_manager: CacheManager, namespace: str):
        """
        Initialize a specialized cache.

        Args:
            cache_manager: The cache manager to use.
            namespace: The namespace for this cache type.
        """
        self.cache_manager = cache_manager
        self.namespace = namespace

    def _get_key(self, *parts: str) -> str:
        """
        Generate a cache key from parts.

        Args:
            *parts: Parts to include in the key.

        Returns:
            str: The cache key.
        """
        return f"{self.namespace}:{':'.join(str(p) for p in parts)}"

    def get(
        self, key: str, backend: Optional[Union[str, CacheBackend]] = None
    ) -> Optional[Any]:
        """
        Retrieve a value from the cache.

        Args:
            key: The cache key to retrieve.
            backend: The backend to use.

        Returns:
            The cached value or None if not found.
        """
        return self.cache_manager.get(self._get_key(key), backend=backend)

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
            backend: The backend to use.
        """
        self.cache_manager.set(self._get_key(key), value, ttl=ttl, backend=backend)

    def delete(
        self, key: str, backend: Optional[Union[str, CacheBackend]] = None
    ) -> None:
        """
        Remove a value from the cache.

        Args:
            key: The cache key to remove.
            backend: The backend to use.
        """
        self.cache_manager.delete(self._get_key(key), backend=backend)

    def exists(
        self, key: str, backend: Optional[Union[str, CacheBackend]] = None
    ) -> bool:
        """
        Check if a key exists in the cache.

        Args:
            key: The cache key to check.
            backend: The backend to use.

        Returns:
            True if the key exists, False otherwise.
        """
        return self.cache_manager.exists(self._get_key(key), backend=backend)

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
            backend: The backend to use.

        Returns:
            The cached or newly generated value.
        """
        return self.cache_manager.get_or_set(
            self._get_key(key), default_factory, ttl=ttl, backend=backend
        )


class ContractCache(SpecializedCache):
    """Specialized cache for contract-related data."""

    def __init__(self, cache_manager: CacheManager):
        """
        Initialize the contract cache.

        Args:
            cache_manager: The cache manager to use.
        """
        super().__init__(cache_manager, "contract")
        self._memory_cache: Dict[str, Any] = {}

    def get_contract_type(
        self, address: AddressType, network_id: Optional[int] = None
    ) -> Optional[ContractType]:
        """
        Get a contract type from the cache.

        Args:
            address: The contract address.
            network_id: The network ID. Defaults to the current network ID.

        Returns:
            The contract type or None if not found.
        """
        if network_id is None:
            network_id = self.provider.chain_id

        key = f"type:{address}:{network_id}"
        return cast(Optional[ContractType], self.get(key))

    def set_contract_type(
        self,
        address: AddressType,
        contract_type: ContractType,
        network_id: Optional[int] = None,
        ttl: Optional[int] = None,
    ) -> None:
        """
        Store a contract type in the cache.

        Args:
            address: The contract address.
            contract_type: The contract type to cache.
            network_id: The network ID. Defaults to the current network ID.
            ttl: Time-to-live in seconds. None means no expiration.
        """
        if network_id is None:
            network_id = self.provider.chain_id

        key = f"type:{address}:{network_id}"
        self.set(key, contract_type, ttl=ttl)

    def get_proxy_info(
        self, address: AddressType, network_id: Optional[int] = None
    ) -> Optional[ProxyInfoAPI]:
        """
        Get proxy information from the cache.

        Args:
            address: The proxy contract address.
            network_id: The network ID. Defaults to the current network ID.

        Returns:
            The proxy information or None if not found.
        """
        if network_id is None:
            network_id = self.provider.chain_id

        key = f"proxy:{address}:{network_id}"
        return cast(Optional[ProxyInfoAPI], self.get(key))

    def set_proxy_info(
        self,
        address: AddressType,
        proxy_info: ProxyInfoAPI,
        network_id: Optional[int] = None,
        ttl: Optional[int] = None,
    ) -> None:
        """
        Store proxy information in the cache.

        Args:
            address: The proxy contract address.
            proxy_info: The proxy information to cache.
            network_id: The network ID. Defaults to the current network ID.
            ttl: Time-to-live in seconds. None means no expiration.
        """
        if network_id is None:
            network_id = self.provider.chain_id

        key = f"proxy:{address}:{network_id}"
        self.set(key, proxy_info, ttl=ttl)

    def get_creation_metadata(
        self, address: AddressType, network_id: Optional[int] = None
    ) -> Optional[ContractCreation]:
        """
        Get contract creation metadata from the cache.

        Args:
            address: The contract address.
            network_id: The network ID. Defaults to the current network ID.

        Returns:
            The contract creation metadata or None if not found.
        """
        if network_id is None:
            network_id = self.provider.chain_id

        key = f"creation:{address}:{network_id}"
        return cast(Optional[ContractCreation], self.get(key))

    def set_creation_metadata(
        self,
        address: AddressType,
        creation_metadata: ContractCreation,
        network_id: Optional[int] = None,
        ttl: Optional[int] = None,
    ) -> None:
        """
        Store contract creation metadata in the cache.

        Args:
            address: The contract address.
            creation_metadata: The contract creation metadata to cache.
            network_id: The network ID. Defaults to the current network ID.
            ttl: Time-to-live in seconds. None means no expiration.
        """
        if network_id is None:
            network_id = self.provider.chain_id

        key = f"creation:{address}:{network_id}"
        self.set(key, creation_metadata, ttl=ttl)


class NetworkCache(SpecializedCache):
    """Specialized cache for network-related data."""

    def __init__(self, cache_manager: CacheManager):
        """
        Initialize the network cache.

        Args:
            cache_manager: The cache manager to use.
        """
        super().__init__(cache_manager, "network")

    def get_chain_id(self, network_name: str) -> Optional[int]:
        """
        Get a chain ID from the cache.

        Args:
            network_name: The network name.

        Returns:
            The chain ID or None if not found.
        """
        key = f"chain_id:{network_name}"
        return cast(Optional[int], self.get(key))

    def set_chain_id(
        self, network_name: str, chain_id: int, ttl: Optional[int] = None
    ) -> None:
        """
        Store a chain ID in the cache.

        Args:
            network_name: The network name.
            chain_id: The chain ID to cache.
            ttl: Time-to-live in seconds. None means no expiration.
        """
        key = f"chain_id:{network_name}"
        self.set(key, chain_id, ttl=ttl)

    def get_gas_price(self, network_id: int) -> Optional[int]:
        """
        Get a gas price from the cache.

        Args:
            network_id: The network ID.

        Returns:
            The gas price or None if not found.
        """
        key = f"gas_price:{network_id}"
        return cast(Optional[int], self.get(key))

    def set_gas_price(
        self, network_id: int, gas_price: int, ttl: Optional[int] = None
    ) -> None:
        """
        Store a gas price in the cache.

        Args:
            network_id: The network ID.
            gas_price: The gas price to cache.
            ttl: Time-to-live in seconds. None means no expiration.
        """
        key = f"gas_price:{network_id}"
        self.set(key, gas_price, ttl=ttl)


class QueryCache(SpecializedCache):
    """Specialized cache for query-related data."""

    def __init__(self, cache_manager: CacheManager):
        """
        Initialize the query cache.

        Args:
            cache_manager: The cache manager to use.
        """
        super().__init__(cache_manager, "query")


class TransactionCache(SpecializedCache):
    """Specialized cache for transaction-related data."""

    def __init__(self, cache_manager: CacheManager):
        """
        Initialize the transaction cache.

        Args:
            cache_manager: The cache manager to use.
        """
        super().__init__(cache_manager, "transaction")

    def get_transaction(self, txn_hash: str) -> Optional[TransactionAPI]:
        """
        Get a transaction from the cache.

        Args:
            txn_hash: The transaction hash.

        Returns:
            The transaction or None if not found.
        """
        key = f"tx:{txn_hash}"
        return cast(Optional[TransactionAPI], self.get(key))

    def set_transaction(
        self, txn_hash: str, transaction: TransactionAPI, ttl: Optional[int] = None
    ) -> None:
        """
        Store a transaction in the cache.

        Args:
            txn_hash: The transaction hash.
            transaction: The transaction to cache.
            ttl: Time-to-live in seconds. None means no expiration.
        """
        key = f"tx:{txn_hash}"
        self.set(key, transaction, ttl=ttl)

    def get_receipt(self, txn_hash: str) -> Optional[ReceiptAPI]:
        """
        Get a receipt from the cache.

        Args:
            txn_hash: The transaction hash.

        Returns:
            The receipt or None if not found.
        """
        key = f"receipt:{txn_hash}"
        return cast(Optional[ReceiptAPI], self.get(key))

    def set_receipt(
        self, txn_hash: str, receipt: ReceiptAPI, ttl: Optional[int] = None
    ) -> None:
        """
        Store a receipt in the cache.

        Args:
            txn_hash: The transaction hash.
            receipt: The receipt to cache.
            ttl: Time-to-live in seconds. None means no expiration.
        """
        key = f"receipt:{txn_hash}"
        self.set(key, receipt, ttl=ttl)
