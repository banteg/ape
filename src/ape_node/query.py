from functools import singledispatchmethod
from typing import Iterator, Optional

from pydantic import BaseModel

from ape.api import ReceiptAPI
from ape.api.query import ContractCreationQuery, QueryAPI, QueryType
from ape.exceptions import QueryEngineError
from ape.types.address import AddressType
from ape_ethereum.provider import EthereumNodeProvider


class ContractCreation(BaseModel):
    receipt: ReceiptAPI
    deployer: AddressType
    factory: Optional[AddressType]
    deploy_block: int


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
        if self.provider._ots_api_level is not None:
            return 300
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
            yield ContractCreation(
                receipt=receipt,
                deployer=receipt.sender,
                factory=creator if creator != receipt.sender else None,
                deploy_block=receipt.block_number,
            )
