# encoding: utf-8

import asyncio
import logging
from datetime import datetime
import os
from sqlalchemy import insert, select
from sqlalchemy import update

from sqlalchemy.exc import IntegrityError

from dbsession import session_maker, async_session_maker
from models.Block import Block
from models.Transaction import Transaction, TransactionOutput, TransactionInput
from models.TxAddrMapping import TxAddrMapping
from utils.Event import Event
from TxAddressMappingProcessor import TxAddressMappingProcessor

from typing import List, Type

_logger = logging.getLogger(__name__)

CLUSTER_SIZE_INITIAL = int(os.getenv("CLUSTER_SIZE_INITIAL", 150))
CLUSTER_SIZE_SYNCED = 10
CLUSTER_WAIT_SECONDS = 1

import insert_ignore

BATCH_SIZE = 5000
MAX_CONCURRENT_BATCHES = 10


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

        # Toggles
        self.add_tx_addr_mapping = True
        self.add_tx_addr_mapping_async = not self.add_tx_addr_mapping
        self.update_block_hash = True

        self.tx_addr_mapping_processor = TxAddressMappingProcessor()

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

                if self.add_tx_addr_mapping_async:
                    await self.tx_addr_mapping_processor.add_batch()
                    self.tx_addr_mapping_processor.start_processing()

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
                await asyncio.sleep(2)

            # if synced, poll blocks after 1s
            if self.synced:
                _logger.debug(
                    f"Iterator waiting {CLUSTER_WAIT_SECONDS}s for next request."
                )
                yield None, None
                await asyncio.sleep(CLUSTER_WAIT_SECONDS)

    async def __get_address_from_tx_outputs(self, transaction_id, index):
        async with async_session_maker() as session:
            result = await session.execute(
                select(TransactionOutput.script_public_key_address)
                .where(TransactionOutput.transaction_id == transaction_id)
                .where(TransactionOutput.index == index)
            )
            return result.scalar()

    async def process_input(self, tx_id, index, tx_in, block_time):
        staging_input = TransactionInput(
            transaction_id=tx_id,
            index=int(index),
            previous_outpoint_hash=tx_in["previousOutpoint"]["transactionId"],
            previous_outpoint_index=int(tx_in["previousOutpoint"].get("index", 0)),
            signature_script=tx_in["signatureScript"],
            sig_op_count=tx_in.get("sigOpCount", 0),
        )

        if not self.add_tx_addr_mapping:
            return staging_input, None

        inp_address = await self.__get_address_from_tx_outputs(
            tx_in["previousOutpoint"]["transactionId"],
            tx_in["previousOutpoint"].get("index", 0),
        )

        # if tx is in the output cache and not in DB yet
        if inp_address is None:
            for output in self.txs_output:
                if output.transaction_id == tx_in["previousOutpoint"][
                    "transactionId"
                ] and output.index == tx_in["previousOutpoint"].get("index", 0):
                    inp_address = output.script_public_key_address
                    break

        staging_mapping = TxAddrMapping(
            transaction_id=tx_id,
            address=inp_address,
            block_time=int(block_time),
            is_accepted=False,
        )

        return staging_input, staging_mapping

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
                            index=int(index),
                            amount=int(out["amount"]),
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
                input_tasks = [
                    self.process_input(
                        tx_id, index, tx_in, transaction["verboseData"]["blockTime"]
                    )
                    for index, tx_in in enumerate(transaction.get("inputs", []))
                ]
                input_results = await asyncio.gather(*input_tasks)

                for input_result, mapping_result in input_results:
                    staging_txs_inputs.append(input_result)
                    if mapping_result:
                        staging_tx_addr_mapping.append(mapping_result)

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
        if not self.add_tx_addr_mapping:
            _logger.info("Skipping tx-addr mapping")
            return

        try:
            to_be_added = []
            for tx_addr_mapping in self.tx_addr_mapping:
                if (
                    tx_addr_tuple := (
                        tx_addr_mapping.transaction_id,
                        tx_addr_mapping.address,
                    )
                ) not in self.tx_addr_cache:
                    to_be_added.append(tx_addr_mapping)
                    self.tx_addr_cache.append(tx_addr_tuple)

            await self.insert_batches(TxAddrMapping, to_be_added)

            _logger.info(
                f"Added {len(to_be_added)} tx-address mapping items successfully"
            )

            self.tx_addr_mapping = []
            self.tx_addr_cache = self.tx_addr_cache[-100:]  # keep the last 100 items

        except IntegrityError:
            _logger.error("Error adding tx-address mapping")
            raise

    async def update_transaction_block_hash(self, session, tx_item):
        if self.update_block_hash:
            new_block_hash = list(
                set(tx_item.block_hash)
                | set(self.txs[tx_item.transaction_id].block_hash)
            )
            stmt = (
                update(Transaction)
                .where(Transaction.transaction_id == tx_item.transaction_id)
                .values(block_hash=new_block_hash)
            )
            await session.execute(stmt)

        # self.txs.pop(tx_item.transaction_id)

    async def commit_txs(self):
        """
        Add all queued transactions and their inputs and outputs to database concurrently in multiple batches
        """
        tx_ids_to_add = list(self.txs.keys())

        _logger.info(f"Finding existing transactions in DB and update block_hash")
        async with async_session_maker() as session:
            # Check for existing transactions and update block_hash
            existing_tx_query = select(Transaction).filter(
                Transaction.transaction_id.in_(tx_ids_to_add)
            )
            existing_tx_result = await session.execute(existing_tx_query)
            existing_tx_items = existing_tx_result.scalars().all()

            tasks = [
                self.update_transaction_block_hash(session, tx_item)
                for tx_item in existing_tx_items
            ]
            await asyncio.gather(*tasks)
            await session.commit()

        _logger.info(
            f"Finished finding existing transactions in DB and update block_hash, {len(self.txs)} transactions left"
        )
        if len(self.txs) == 0:
            _logger.info("No transactions to add")
            return

        _logger.debug(
            f"Start: Adding {len(self.txs)} TXs, {len(self.txs_output)} outputs, {len(self.txs_input)} inputs"
        )

        try:
            await asyncio.gather(
                self.insert_batches(Transaction, list(self.txs.values())),
                self.insert_batches(
                    TransactionOutput,
                    [out for out in self.txs_output if out.transaction_id in self.txs],
                ),
                self.insert_batches(
                    TransactionInput,
                    [inp for inp in self.txs_input if inp.transaction_id in self.txs],
                ),
            )

            _logger.debug(
                f"Added {len(self.txs)} TXs, {len(self.txs_output)} outputs, {len(self.txs_input)} inputs"
            )

            # Reset queues
            self.txs = {}
            self.txs_input = []
            self.txs_output = []

        except IntegrityError:
            _logger.error("Error adding TXs to database")
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

    async def insert_batches(self, model: Type, items: List):
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_BATCHES)

        async def insert_single_batch(batch):
            async with semaphore:
                async with async_session_maker() as batch_session:
                    batch_dicts = [item.__dict__ for item in batch]
                    for item_dict in batch_dicts:
                        item_dict.pop("_sa_instance_state", None)
                    await batch_session.execute(insert(model), batch_dicts)
                    await batch_session.commit()

        tasks = []
        for i in range(0, len(items), BATCH_SIZE):
            batch = items[i : i + BATCH_SIZE]
            tasks.append(asyncio.create_task(insert_single_batch(batch)))

        await asyncio.gather(*tasks)
