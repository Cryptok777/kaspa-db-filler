from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql import Insert
from models.AddressBalance import AddressBalance
from models.Transaction import TransactionInput, TransactionOutput, Transaction
from models.TxAddrMapping import TxAddrMapping

"""
When imported, automatically make all insert not fail on duplicate keys
"""


def append_statement_to_ignore(statement: str):
    on_conflict_stmt = " ON CONFLICT DO NOTHING "

    returning_position = statement.find("RETURNING")
    if returning_position >= 0:
        return (
            statement[:returning_position]
            + on_conflict_stmt
            + statement[returning_position:]
        )
    else:
        return statement + on_conflict_stmt


def append_statement_to_update_address_balance(statement: str):
    on_conflict_stmt = " ON CONFLICT (address) DO UPDATE SET (balance, updated_at) = (EXCLUDED.balance, EXCLUDED.updated_at) "

    returning_position = statement.find("RETURNING")
    if returning_position >= 0:
        return (
            statement[:returning_position]
            + on_conflict_stmt
            + statement[returning_position:]
        )
    else:
        return statement + on_conflict_stmt


@compiles(Insert, "postgresql")
def postgresql_on_conflict(insert, compiler, **kw):
    statement = compiler.visit_insert(insert, **kw)

    if insert.table.name in [
        Transaction.__tablename__,
        TransactionInput.__tablename__,
        TransactionOutput.__tablename__,
        TxAddrMapping.__tablename__,
    ]:
        return append_statement_to_ignore(statement)

    if insert.table.name in [
        AddressBalance.__tablename__,
    ]:
        return append_statement_to_update_address_balance(statement)

    return statement
