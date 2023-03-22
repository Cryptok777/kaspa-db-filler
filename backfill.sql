-- 1. Edit start_hash in main.py with stater block hash, run the script
-- 2. Edit the block time from sql, and then run this sql


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
    AND t.block_time >= 1678860545000 - 12 * 60 * 60 * 1e3
    AND t.block_time <= 1678860545000 + 12 * 60 * 60 * 1e3
--     AND t.transaction_id = '2668510ce29f181c4db5e99ab5c6aa38ba9de3137a2a260d29acf61f46e72ecb'
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
    AND t.block_time >= 1678860545000 - 12 * 60 * 60 * 1e3
    AND t.block_time <= 1678860545000 + 12 * 60 * 60 * 1e3
--     AND t.transaction_id = '2668510ce29f181c4db5e99ab5c6aa38ba9de3137a2a260d29acf61f46e72ecb'
)

INSERT INTO tx_id_address_mapping ( transaction_id, address, block_time, is_accepted )
SELECT * FROM tx_mapping
ON CONFLICT (transaction_id, address) DO NOTHING



-- Update is_accepted state
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
      transactions.block_time >= 1678861764165
  ) AS subquery
WHERE
  tx_id_address_mapping.transaction_id = subquery.transaction_id
  AND tx_id_address_mapping.address = subquery.address;
