-- 1. Edit start_hash in main.py with stater block hash, run the script
-- 2. Edit the block time from sql, and then run this sql


INSERT INTO tx_id_address_mapping ( transaction_id, address, block_time, is_accepted )
(select
  transactions.transaction_id,
  transactions_outputs.script_public_key_address as address,
  transactions.block_time,
  transactions.is_accepted
from
  transactions_outputs
  LEFT JOIN transactions ON transactions_outputs.transaction_id = transactions.transaction_id
WHERE
  transactions.block_time >= 1678158002000)
ON CONFLICT (transaction_id, address) DO NOTHING



UPDATE
  tx_id_address_mapping
SET
  is_accepted = subquery.is_accepted
FROM
  (
    select
      transactions.transaction_id,
      transactions_outputs.script_public_key_address as address,
      transactions.block_time,
      transactions.is_accepted
    from
      transactions_outputs
      LEFT JOIN transactions ON transactions_outputs.transaction_id = transactions.transaction_id
    WHERE
      transactions.block_time >= 1678158002000
  ) AS subquery
WHERE
  tx_id_address_mapping.transaction_id = subquery.transaction_id
  AND tx_id_address_mapping.address = subquery.address;
