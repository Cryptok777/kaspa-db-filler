import asyncio
import logging
from typing import List, Dict, Type
from sqlalchemy import insert, select
from sqlalchemy.exc import IntegrityError
from collections import defaultdict

from dbsession import async_session_maker
from models.Transaction import TransactionOutput
from models.TxAddrMapping import TxAddrMapping
from sqlalchemy import text

import insert_ignore

_logger = logging.getLogger(__name__)

BATCH_SIZE = 5000
MAX_CONCURRENT_BATCHES = 10


class TxAddressMappingProcessor:
    def __init__(self):
        self.queue = asyncio.Queue()
        self.processing_task = None

    async def process_queue(self):
        while True:
            batch = await self.queue.get()
            if batch is None:
                _logger.info("Received stop signal, stopping queue processing")
                break

            await self.run_backfill_query()
            self.queue.task_done()
        _logger.info("Queue processing stopped")

    async def run_backfill_query(self):
        _logger.info("Running backfill query")
        async with async_session_maker() as s:
            try:
                await s.execute(
                    text(
                        """
                            DO $$
                            DECLARE
                                max_block_time bigint;
                            BEGIN
                                SELECT (SELECT MAX(block_time) from tx_id_address_mapping) INTO max_block_time;

                                -- Backfill tx_mapping table
                                with tx_mapping AS (
                                    SELECT DISTINCT
                                        t.transaction_id,
                                        ti_input.script_public_key_address AS address,
                                        t.block_time,
                                        t.is_accepted
                                    FROM
                                        public.transactions AS t
                                        LEFT JOIN (
                                        SELECT
                                            ti.transaction_id,
                                            ti.index,
                                            ti.previous_outpoint_hash,
                                            ti.previous_outpoint_index,
                                            to_prev.script_public_key_address
                                        FROM
                                            public.transactions_inputs AS ti
                                            LEFT JOIN public.transactions_outputs AS to_prev ON ti.previous_outpoint_hash = to_prev.transaction_id
                                            AND ti.previous_outpoint_index :: integer = to_prev.index
                                        ) AS ti_input ON t.transaction_id = ti_input.transaction_id
                                    WHERE
                                        TRUE
                                        AND ti_input.script_public_key_address is not null
                                    UNION ALL
                                    SELECT DISTINCT
                                        t.transaction_id,
                                        t_out.script_public_key_address AS address,
                                        t.block_time,
                                        t.is_accepted
                                    FROM
                                        public.transactions AS t
                                        LEFT JOIN public.transactions_outputs AS t_out ON t.transaction_id = t_out.transaction_id
                                    WHERE
                                        TRUE
                                        AND t_out.script_public_key_address is not null
                                )

                                INSERT INTO tx_id_address_mapping ( transaction_id, address, block_time, is_accepted )
                                SELECT * FROM tx_mapping
                                    WHERE TRUE
                                    AND block_time >= max_block_time
                                ON CONFLICT (transaction_id, address) DO NOTHING;

                            END $$;
                        """
                    )
                )

                await s.commit()
                _logger.info("Finished running backfill query")
            except Exception as e:
                _logger.error(f"Error when running backfill query: {e}")
                await s.rollback()

    async def _process_batch(self, batch: Dict[str, List]):
        try:
            tx_addr_mappings = []

            # Prepare all inputs for batch processing
            all_inputs = []
            for tx_data in batch["transactions"].values():
                all_inputs.extend(tx_data["inputs"])

            # Fetch addresses for all inputs in one go
            _logger.info("Start fetching addresses for all inputs")
            input_addresses = await self._get_addresses_for_inputs(all_inputs)
            _logger.info("Finished fetching addresses for all inputs")

            for tx_id, tx_data in batch["transactions"].items():
                tx_addr_mappings.extend(
                    await self._process_transaction(tx_id, tx_data, input_addresses)
                )

            _logger.debug(f"Start inserting {len(tx_addr_mappings)} TxAddrMappings")
            await self._insert_tx_addr_mappings(tx_addr_mappings)
            _logger.info(f"Finished Inserting {len(tx_addr_mappings)} TxAddrMappings")
        except Exception as e:
            _logger.error(f"Error processing TxAddrMapping batch: {e}", exc_info=True)

    async def _process_transaction(
        self, tx_id: str, tx_data: Dict, input_addresses: Dict
    ) -> List[TxAddrMapping]:
        tx_addr_mappings = []

        # Process outputs
        for output in tx_data["outputs"]:
            tx_addr_mappings.append(
                TxAddrMapping(
                    transaction_id=tx_id,
                    address=output.script_public_key_address,
                    block_time=tx_data["block_time"],
                    is_accepted=False,
                )
            )

        # Process inputs
        for input_data in tx_data["inputs"]:
            key = (
                input_data.previous_outpoint_hash,
                input_data.previous_outpoint_index,
            )
            address = input_addresses.get(key)

            # If address is not found in the database, check the current batch of outputs
            if address is None:
                for output in tx_data["outputs"]:
                    if (
                        output.transaction_id == input_data.previous_outpoint_hash
                        and output.index == input_data.previous_outpoint_index
                    ):
                        address = output.script_public_key_address
                        break

            if address:
                tx_addr_mappings.append(
                    TxAddrMapping(
                        transaction_id=tx_id,
                        address=address,
                        block_time=tx_data["block_time"],
                        is_accepted=False,
                    )
                )

        return tx_addr_mappings

    async def _get_addresses_for_inputs(self, inputs: List) -> Dict:
        # Group inputs by transaction_id to minimize database queries
        inputs_by_tx = defaultdict(list)
        for input_data in inputs:
            inputs_by_tx[input_data.previous_outpoint_hash].append(
                input_data.previous_outpoint_index
            )

        addresses = {}
        async with async_session_maker() as session:
            for tx_id, indices in inputs_by_tx.items():
                result = await session.execute(
                    select(
                        TransactionOutput.index,
                        TransactionOutput.script_public_key_address,
                    )
                    .where(TransactionOutput.transaction_id == tx_id)
                    .where(TransactionOutput.index.in_(indices))
                )
                for row in result:
                    addresses[(tx_id, row.index)] = row.script_public_key_address

        return addresses

    async def _insert_tx_addr_mappings(self, tx_addr_mappings: List[TxAddrMapping]):
        await self._insert_batches(TxAddrMapping, tx_addr_mappings)

    async def _insert_batches(self, model: Type, items: List):
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_BATCHES)

        async def insert_single_batch(batch):
            async with semaphore:
                async with async_session_maker() as batch_session:
                    try:
                        batch_dicts = [item.__dict__ for item in batch]
                        for item_dict in batch_dicts:
                            item_dict.pop("_sa_instance_state", None)
                        await batch_session.execute(insert(model), batch_dicts)
                        await batch_session.commit()
                    except IntegrityError:
                        _logger.error(f"IntegrityError when inserting batch")
                        await batch_session.rollback()
                        raise
                    except Exception:
                        _logger.error(f"Error when inserting batch")
                        await batch_session.rollback()
                        raise

        tasks = []
        for i in range(0, len(items), BATCH_SIZE):
            batch = items[i : i + BATCH_SIZE]
            tasks.append(asyncio.create_task(insert_single_batch(batch)))

        await asyncio.gather(*tasks)

    async def add_batch(self):
        # outputs_dict = {}
        # inputs_dict = {}

        # for out in txs_output:
        #     if out.transaction_id not in outputs_dict:
        #         outputs_dict[out.transaction_id] = []
        #     outputs_dict[out.transaction_id].append(out)

        # for inp in txs_input:
        #     if inp.transaction_id not in inputs_dict:
        #         inputs_dict[inp.transaction_id] = []
        #     inputs_dict[inp.transaction_id].append(inp)

        # # Create the batch using the dictionaries
        # batch = {
        #     "transactions": {
        #         tx_id: {
        #             "outputs": outputs_dict.get(tx_id, []),
        #             "inputs": inputs_dict.get(tx_id, []),
        #             "block_time": tx.block_time,
        #         }
        #         for tx_id, tx in txs.items()
        #     }
        # }
        if self.queue.empty():
            await self.queue.put(True)

    def start_processing(self):
        if self.processing_task is None or self.processing_task.done():
            self.processing_task = asyncio.create_task(self.process_queue())

    async def stop(self):
        await self.queue.put(None)
        if self.processing_task:
            await self.processing_task
