{{ config(materialized='incremental', unique_key=['symbol', 'trade_id']) }}

with valid_records as (
    select
        *,
        row_number() over (
            partition by symbol, trade_id
            order by ingestion_time desc, bronze_written_at desc
        ) as dedupe_rank
    from {{ ref('bronze_crypto_trades') }}
    where trade_id is not null
      and symbol is not null
      and price > 0
      and quantity > 0
      and trade_time > 0
)

select
    trade_id,
    symbol,
    price,
    quantity,
    trade_time,
    trade_time_ts,
    is_buyer_maker,
    ingestion_time,
    is_injected,
    fault_type,
    batch_run_id,
    bronze_written_at,
    current_timestamp() as silver_written_at
from valid_records
where dedupe_rank = 1

{% if is_incremental() %}
  and trade_time_ts >= (
      select coalesce(max(trade_time_ts), timestamp('1970-01-01 00:00:00'))
      from {{ this }}
  )
{% endif %}
