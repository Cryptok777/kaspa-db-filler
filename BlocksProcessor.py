# encoding: utf-8

import asyncio
import logging
from datetime import datetime
import os
from sqlalchemy import insert

from sqlalchemy.exc import IntegrityError

from dbsession import session_maker
from models.Block import Block
from models.Transaction import Transaction, TransactionOutput, TransactionInput
from models.TxAddrMapping import TxAddrMapping
from utils.Event import Event

_logger = logging.getLogger(__name__)

CLUSTER_SIZE_INITIAL = int(os.getenv("CLUSTER_SIZE_INITIAL", 150))
CLUSTER_SIZE_SYNCED = 20
CLUSTER_WAIT_SECONDS = 1

import insert_ignore


class BlocksProcessor(object):
    """
    BlocksProcessor polls kaspad for blocks and adds the meta information and it's transactions into database.
    """

    def __init__(self, client):
        self.client = client
        self.blocks_to_add = []
        self.on_commited = Event()

        self.txs = {}
        self.txs_output = []
        self.txs_input = []
        self.tx_addr_mapping = []

        # cache for checking already added tx mapping
        self.tx_addr_cache = []

        # Did the loop already see the DAG tip
        self.synced = False

    async def loop(self, start_point):
        # go through each block added to DAG
        async for block_hash, block in self.blockiter(start_point):
            if block_hash is not None:
                # prepare add block and tx to database
                await self.__add_block_to_queue(block_hash, block)
                await self.__add_tx_to_queue(block_hash, block)

            # if cluster size is reached, insert to database
            bcc = len(self.blocks_to_add) >= (
                CLUSTER_SIZE_INITIAL if not self.synced else CLUSTER_SIZE_SYNCED
            )

            # or we are waiting and cache is not empty
            if bcc or (block_hash is None and len(self.blocks_to_add) >= 1):
                await self.commit_blocks()
                await self.commit_txs()
                await self.add_and_commit_tx_addr_mapping()
                await self.on_commited()

    async def blockiter(self, start_point):
        """
        generator for iterating the blocks added to the blockDAG
        """
        low_hash = start_point
        while True:
            resp = await self.client.request(
                "getBlocksRequest",
                params={
                    "lowHash": low_hash,
                    "includeTransactions": True,
                    "includeBlocks": True,
                },
                timeout=60,
            )

            # if it's not synced, get the tiphash, which has to be found for getting synced
            if not self.synced:
                daginfo = await self.client.request("getBlockDagInfoRequest", {})

            # go through each block and yield
            for i, _ in enumerate(resp["getBlocksResponse"].get("blockHashes", [])):
                if not self.synced:
                    if daginfo["getBlockDagInfoResponse"]["tipHashes"][0] == _:
                        _logger.info("Found tip hash. Generator is synced now.")
                        self.synced = True

                # ignore the first block, which is not start point. It is already processed by previous request
                if _ == low_hash and _ != start_point:
                    continue

                # yield blockhash and it's data
                yield _, resp["getBlocksResponse"]["blocks"][i]

            # new low hash is the last hash of previous response
            if len(resp["getBlocksResponse"].get("blockHashes", [])) > 1:
                low_hash = resp["getBlocksResponse"]["blockHashes"][-1]
            else:
                _logger.debug("")
                await asyncio.sleep(2)

            # if synced, poll blocks after 1s
            if self.synced:
                _logger.debug(
                    f"Iterator waiting {CLUSTER_WAIT_SECONDS}s for next request."
                )
                yield None, None
                await asyncio.sleep(CLUSTER_WAIT_SECONDS)

    def __get_address_from_tx_outputs(self, transaction_id, index):
        with session_maker() as session:
            return (
                session.query(TransactionOutput.script_public_key_address)
                .where(TransactionOutput.transaction_id == transaction_id)
                .where(TransactionOutput.index == index)
                .scalar()
            )

    async def __add_tx_to_queue(self, block_hash, block):
        """
        Adds block's transactions to queue. This is only prepartion without commit!
        """
        # Go through blocks
        for transaction in block["transactions"]:
            tx_id = transaction["verboseData"]["transactionId"]

            staging_txs_output = []
            staging_txs_inputs = []
            staging_tx_addr_mapping = []

            # Check, that the transaction isn't prepared yet. Otherwise ignore
            # Often transactions are added in more than one block
            if not self.is_tx_id_in_queue(tx_id):
                # Add transactions output
                for index, out in enumerate(transaction.get("outputs", [])):
                    staging_txs_output.append(
                        TransactionOutput(
                            transaction_id=tx_id,
                            index=index,
                            amount=out["amount"],
                            script_public_key=out["scriptPublicKey"]["scriptPublicKey"],
                            script_public_key_address=out["verboseData"][
                                "scriptPublicKeyAddress"
                            ],
                            script_public_key_type=out["verboseData"][
                                "scriptPublicKeyType"
                            ],
                        )
                    )

                    staging_tx_addr_mapping.append(
                        TxAddrMapping(
                            transaction_id=tx_id,
                            address=out["verboseData"]["scriptPublicKeyAddress"],
                            block_time=int(transaction["verboseData"]["blockTime"]),
                            is_accepted=False,
                        )
                    )

                # Add transactions input
                for index, tx_in in enumerate(transaction.get("inputs", [])):
                    staging_txs_inputs.append(
                        TransactionInput(
                            transaction_id=tx_id,
                            index=index,
                            previous_outpoint_hash=tx_in["previousOutpoint"][
                                "transactionId"
                            ],
                            previous_outpoint_index=int(
                                tx_in["previousOutpoint"].get("index", 0)
                            ),
                            signature_script=tx_in["signatureScript"],
                            sig_op_count=tx_in.get("sigOpCount", 0),
                        )
                    )

                    inp_address = self.__get_address_from_tx_outputs(
                        tx_in["previousOutpoint"]["transactionId"],
                        tx_in["previousOutpoint"].get("index", 0),
                    )

                    # if tx is in the output cache and not in DB yet
                    if inp_address is None:
                        for output in self.txs_output:
                            if output.transaction_id == tx_in["previousOutpoint"][
                                "transactionId"
                            ] and output.index == tx_in["previousOutpoint"].get(
                                "index", 0
                            ):
                                inp_address = output.script_public_key_address
                                break
                        else:
                            _logger.warning(
                                f"Unable to find address for {tx_in['previousOutpoint']['transactionId']}"
                                f" ({tx_in['previousOutpoint'].get('index', 0)})"
                            )

                    staging_tx_addr_mapping.append(
                        TxAddrMapping(
                            transaction_id=tx_id,
                            address=inp_address,
                            block_time=int(transaction["verboseData"]["blockTime"]),
                            is_accepted=False,
                        )
                    )

                # Add transaction
                self.txs[tx_id] = Transaction(
                    subnetwork_id=transaction["subnetworkId"],
                    transaction_id=tx_id,
                    hash=transaction["verboseData"]["hash"],
                    mass=transaction["verboseData"].get("mass"),
                    block_hash=[transaction["verboseData"]["blockHash"]],
                    block_time=int(transaction["verboseData"]["blockTime"]),
                )

                # apply staging inputs/outputs/tx_addr_mapping
                self.txs_output.extend(staging_txs_output)
                self.txs_input.extend(staging_txs_inputs)
                self.tx_addr_mapping.extend(staging_tx_addr_mapping)

            else:
                # If the block is already in the Queue, merge the block_hashes.
                self.txs[tx_id].block_hash = list(
                    set(self.txs[tx_id].block_hash + [block_hash])
                )

    async def add_and_commit_tx_addr_mapping(self):
        cnt = 0

        with session_maker() as session:
            to_be_added = []
            for tx_addr_mapping in self.tx_addr_mapping:
                if (
                    tx_addr_tuple := (
                        tx_addr_mapping.transaction_id,
                        tx_addr_mapping.address,
                    )
                ) not in self.tx_addr_cache:
                    to_be_added.append(tx_addr_mapping)
                    cnt += 1
                    self.tx_addr_cache.append(tx_addr_tuple)

            session.bulk_save_objects(to_be_added)
            try:
                session.commit()
            except IntegrityError:
                session.rollback()
                _logger.error("Error adding tx-address mapping")
                raise

            _logger.info(f"Added {cnt} tx-address mapping items successfully")

        self.tx_addr_mapping = []
        self.tx_addr_cache = self.tx_addr_cache[-100:]  # get the next 100 items

    async def commit_txs(self):
        """
        Add all queued transactions and it's in- and outputs to database
        """
        # First go through all transactions and check, if there are already added ones.
        # If yes, update block_hash and remove from queue
        tx_ids_to_add = list(self.txs.keys())
        with session_maker() as session:
            tx_items = (
                session.query(Transaction)
                .filter(Transaction.transaction_id.in_(tx_ids_to_add))
                .all()
            )
            for tx_item in tx_items:
                tx_item.block_hash = list(
                    (
                        set(tx_item.block_hash)
                        | set(self.txs[tx_item.transaction_id].block_hash)
                    )
                )
                self.txs.pop(tx_item.transaction_id)

            session.commit()

        with session_maker() as session:
            session.bulk_save_objects(self.txs.values())

            pending_objects = []

            for tx_output in self.txs_output:
                if tx_output.transaction_id in self.txs:
                    pending_objects.append(tx_output)

            for tx_input in self.txs_input:
                if tx_input.transaction_id in self.txs:
                    pending_objects.append(tx_input)

            session.bulk_save_objects(pending_objects)

            try:
                session.commit()
                _logger.debug(f"Added {len(self.txs)} TXs to database")

                # reset queues
                self.txs = {}
                self.txs_input = []
                self.txs_output = []

            except IntegrityError:
                session.rollback()
                _logger.error(f"Error adding TXs to database")
                raise

    async def __add_block_to_queue(self, block_hash, block):
        """
        Adds a block to the queue, which is used for adding a cluster
        """

        block_entity = Block(
            hash=block_hash,
            accepted_id_merkle_root=block["header"]["acceptedIdMerkleRoot"],
            difficulty=block["verboseData"]["difficulty"],
            is_chain_block=block["verboseData"].get("isChainBlock", False),
            merge_set_blues_hashes=block["verboseData"].get("mergeSetBluesHashes", []),
            merge_set_reds_hashes=block["verboseData"].get("mergeSetRedsHashes", []),
            selected_parent_hash=block["verboseData"]["selectedParentHash"],
            bits=block["header"]["bits"],
            blue_score=int(block["header"]["blueScore"]),
            blue_work=block["header"]["blueWork"],
            daa_score=int(block["header"]["daaScore"]),
            hash_merkle_root=block["header"]["hashMerkleRoot"],
            nonce=block["header"]["nonce"],
            parents=block["header"]["parents"][0]["parentHashes"],
            pruning_point=block["header"]["pruningPoint"],
            timestamp=datetime.utcfromtimestamp(
                int(block["header"]["timestamp"]) / 1000
            ).isoformat(),
            utxo_commitment=block["header"]["utxoCommitment"],
            version=block["header"]["version"],
        )

        # remove same block hash
        self.blocks_to_add = [b for b in self.blocks_to_add if b.hash != block_hash]
        self.blocks_to_add.append(block_entity)

    async def commit_blocks(self):
        """
        Insert queued blocks to database
        """
        # delete already set old blocks
        with session_maker() as session:
            session.query(Block).filter(
                Block.hash.in_([b.hash for b in self.blocks_to_add])
            ).delete()
            session.commit()

        # insert blocks
        with session_maker() as session:
            session.bulk_save_objects(self.blocks_to_add)
            try:
                session.commit()
                _logger.debug(
                    f"Added {len(self.blocks_to_add)} blocks to database. "
                    f"Timestamp: {self.blocks_to_add[-1].timestamp}"
                )

                # reset queue
                self.blocks_to_add = []
            except IntegrityError:
                session.rollback()
                _logger.error("Error adding group of blocks")
                raise

    def is_tx_id_in_queue(self, tx_id):
        """
        Checks if given TX ID is already in the queue
        """
        return tx_id in self.txs
