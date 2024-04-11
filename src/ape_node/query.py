from collections.abc import Iterator
from functools import singledispatchmethod
from typing import Optional, TypedDict

from ape.api import ReceiptAPI
from ape.api.query import ContractCreationQuery, QueryAPI, QueryType
from ape.exceptions import QueryEngineError
from ape.types.address import AddressType
from ape_ethereum.provider import EthereumNodeProvider


class ContractCreation(TypedDict):
    receipt: ReceiptAPI
    deployer: AddressType
    factory: AddressType | None
    block: int


class OTSQueryEngine(QueryAPI):
    @singledispatchmethod
    def estimate_query(self, query: QueryType) -> Optional[int]:  # type: ignore[override]
        return None

    @singledispatchmethod
    def perform_query(self, query: QueryType) -> Iterator:  # type: ignore[override]
        raise QueryEngineError(
            f"{self.__class__.__name__} cannot handle {query.__class__.__name__} queries."
        )

    @estimate_query.register
    def estimate_contract_creation_query(self, query: ContractCreationQuery) -> Optional[int]:
        if provider := self.network_manager.active_provider:
            if not isinstance(provider, EthereumNodeProvider):
                return None
            elif uri := provider.http_uri:
                return 225 if uri.startswith("http://") else 600

        return None

    @perform_query.register
    def get_contract_creation_receipt(
        self, query: ContractCreationQuery
    ) -> Iterator[ContractCreation]:
        if self.network_manager.active_provider and isinstance(self.provider, EthereumNodeProvider):
            ots = self.provider.make_request("ots_getContractCreator", [query.contract])
            if ots is None:
                return None
            creator = self.conversion_manager.convert(ots["creator"], AddressType)
            receipt = self.provider.get_receipt(ots["hash"])
            yield {
                "receipt": receipt,
                "deployer": receipt.sender,
                "factory": creator if creator != receipt.sender else None,
                "block": receipt.block_number,
            }
