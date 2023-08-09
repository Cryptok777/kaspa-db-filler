from typing import List

from models.Transaction import Transaction
from models.Block import Block
from models.Transaction import TransactionInput
from models.Transaction import TransactionOutput
from models.TxAddrMapping import TxAddrMapping

from sqlalchemy.dialects.postgresql import insert as pg_insert
from dbsession import session_maker


class DBStore:
    def __init__(self):
        pass

    def add_blocks(self, blocks: List[Block]):
        with session_maker() as session:
            statement = (
                pg_insert(Block)
                .values([i.__dict__ for i in blocks])
                .on_conflict_do_nothing()
            )
            session.execute(statement)
            session.commit()

    def add_transactions(self, transactions: List[Transaction]):
        pass

    def add_transaction_inputs(self, transaction_inputs: List[TransactionInput]):
        pass

    def add_transaction_outputs(self, transaction_outputs: List[TransactionOutput]):
        pass

    def add_tx_mapping(self, tx_mapping: List[TxAddrMapping]):
        pass
