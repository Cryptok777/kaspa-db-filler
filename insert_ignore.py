from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql import Insert
from models import Transaction
from models.Transaction import TransactionInput, TransactionOutput, Transaction
from models.TxAddrMapping import TxAddrMapping

"""
When imported, automatically make all insert not fail on duplicate keys
"""


@compiles(Insert, "postgresql")
def postgresql_on_conflict_do_nothing(insert, compiler, **kw):
    statement = compiler.visit_insert(insert, **kw)

    if insert.table.name not in [
        TxAddrMapping.__tablename__,
        TransactionInput.__tablename__,
        TransactionOutput.__tablename__,
    ]:
        return statement

    # IF we have a "RETURNING" clause, we must insert before it
    returning_position = statement.find("RETURNING")
    if returning_position >= 0:
        return (
            statement[:returning_position]
            + "ON CONFLICT DO NOTHING "
            + statement[returning_position:]
        )
    else:
        return statement + " ON CONFLICT DO NOTHING"


# @compiles(Insert, "postgresql")
# def postgresql_on_conflict_do_update(insert, compiler, **kw):
#     statement = compiler.visit_insert(insert, **kw)

#     if insert.table.name not in [
#         Transaction.__tablename__,
#     ]:
#         return statement

#     on_conflict_statement = f"ON CONFLICT (transaction_id) DO UPDATE SET (is_accepted, accepting_block_hash) = (EXCLUDED.is_accepted, EXCLUDED.accepting_block_hash)"

#     return statement + on_conflict_statement
