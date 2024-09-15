# encoding: utf-8

from datetime import datetime, timezone
import logging
from typing import List

from dbsession import session_maker
from models.AddressBalance import AddressBalance
from models.Block import Block
from models.Transaction import Transaction
from models.TxAddrMapping import TxAddrMapping

_logger = logging.getLogger(__name__)


class VirtualChainProcessor(object):
    """
    VirtualChainProcessor polls the command getVirtualSelectedParentChainFromBlockRequest and updates transactions
    with is_accepted False or True.

    To make sure all blocks are already in database, the VirtualChain processor has a prepare function, which is
    basically a temporary storage. This buffer should be processed AFTER the blocks and transactions are added.
    """

    def __init__(self, client, start_point):
        self.virtual_chain_response = None
        self.start_point = start_point
        self.client = client
        self.should_update_balances = False

    async def __get_balances_for_addresses(self, addresses: List[str]):
        resp = await self.client.request(
            "getBalancesByAddressesRequest",
            params={"addresses": addresses},
            timeout=60,
        )

        return [
            AddressBalance(
                address=i["address"],
                balance=int(i.get("balance", 0)),
                updated_at=datetime.now(timezone.utc),
            )
            for i in resp["getBalancesByAddressesResponse"].get("entries", [])
        ]

    async def __update_transactions_in_db(self):
        """
        goes through one parentChainResponse and updates the is_accepted field in the database.
        """
        accepted_ids = []
        rejected_blocks = []
        last_known_chain_block = None
        addresses_to_find_balance = set()

        if self.virtual_chain_response is None:
            return

        parent_chain_response = self.virtual_chain_response
        parent_chain_blocks = [
            x["acceptingBlockHash"]
            for x in parent_chain_response["acceptedTransactionIds"]
        ]

        # find intersection of database blocks and virtual parent chain
        with session_maker() as s:
            parent_chain_blocks_in_db = (
                s.query(Block)
                .filter(Block.hash.in_(parent_chain_blocks))
                .with_entities(Block.hash)
                .all()
            )
            parent_chain_blocks_in_db = [x[0] for x in parent_chain_blocks_in_db]

        # go through all acceptedTransactionIds and stop if a block hash is not in database
        for tx_accept_dict in parent_chain_response["acceptedTransactionIds"]:
            accepting_block_hash = tx_accept_dict["acceptingBlockHash"]

            if accepting_block_hash not in parent_chain_blocks_in_db:
                break  # Stop once we reached a none existing block

            last_known_chain_block = accepting_block_hash
            accepted_ids.append(
                (
                    tx_accept_dict["acceptingBlockHash"],
                    tx_accept_dict.get("acceptedTransactionIds", []),
                )
            )

            if len(accepted_ids) >= 5000:
                break

        # add rejected blocks if needed
        rejected_blocks.extend(parent_chain_response.get("removedChainBlockHashes", []))

        with session_maker() as s:
            status_updated_tx_ids = []  # pending tx ids to update address balances

            # set is_accepted to False, when blocks were removed from virtual parent chain
            if rejected_blocks:
                _logger.debug(f"Found rejected blocks: {rejected_blocks}")

                rejected_tx_ids = [
                    x[0]
                    for x in s.query(Transaction.transaction_id)
                    .filter(Transaction.accepting_block_hash.in_(rejected_blocks))
                    .all()
                ]
                count = (
                    s.query(Transaction)
                    .filter(Transaction.accepting_block_hash.in_(rejected_blocks))
                    .update({"is_accepted": False, "accepting_block_hash": None})
                )

                s.commit()
                _logger.info(f"DONE: Set is_accepted=False for {count} TXs")

                status_updated_tx_ids.extend(rejected_tx_ids)

            # set is_accepted to True and add accepting_block_hash
            _logger.debug(
                f"START: Set is_accepted=True for {len(accepted_ids)} transactions."
            )
            for accepting_block_hash, accepted_tx_ids in accepted_ids:
                s.query(Transaction).filter(
                    Transaction.transaction_id.in_(accepted_tx_ids)
                ).update(
                    {"is_accepted": True, "accepting_block_hash": accepting_block_hash}
                )

                status_updated_tx_ids.extend(accepted_tx_ids)

            s.commit()
            _logger.debug(
                f"DONE: Set is_accepted=True for {len(accepted_ids)} transactions."
            )

            if self.should_update_balances:
                status_updated_tx_ids = list(set(status_updated_tx_ids))
                addrs = (
                    s.query(TxAddrMapping.address)
                    .filter(TxAddrMapping.transaction_id.in_(status_updated_tx_ids))
                    .all()
                )
                addresses_to_find_balance.update([i[0] for i in addrs])

            # Update balance addresses
            if None in addresses_to_find_balance:
                addresses_to_find_balance.remove(None)

            await self.update_address_balances(list(addresses_to_find_balance))

        # Mark last known/processed as start point for the next query
        if last_known_chain_block:
            self.start_point = last_known_chain_block

        # Clear the current response
        self.virtual_chain_response = None

    async def update_address_balances(self, addresses: List[str]):
        """
        Updates the balances for the given addresses
        """
        BATCH_SIZE = 300
        _logger.info(f"START: Update {len(addresses)} address balances")

        address_balance_rows = []
        for i in range(0, len(addresses), BATCH_SIZE):
            rows = await self.__get_balances_for_addresses(
                addresses[i : i + BATCH_SIZE]
            )
            address_balance_rows.extend(rows)

        with session_maker() as s:
            try:
                s.bulk_save_objects(address_balance_rows)
                s.commit()
            except Exception as e:
                _logger.info(
                    f"Encountered errors when upserting address balance, error: {e}"
                )
                s.rollback()

        _logger.info(f"FINISH: Update {len(addresses)} address balances")

    async def update_accepted_info(self):
        """
        Updates the is_accepted flag to all blocks
        """
        resp = await self.client.request(
            "getVirtualSelectedParentChainFromBlockRequest",
            {"startHash": self.start_point, "includeAcceptedTransactionIds": True},
            timeout=240,
        )

        # if there is a response, add to queue and set new startpoint
        if resp["getVirtualSelectedParentChainFromBlockResponse"]:
            self.virtual_chain_response = resp[
                "getVirtualSelectedParentChainFromBlockResponse"
            ]
        else:
            self.virtual_chain_response = None

        _logger.debug("Updating TXs in DB")
        await self.__update_transactions_in_db()
        _logger.debug("Updating TXs in DB done.")
