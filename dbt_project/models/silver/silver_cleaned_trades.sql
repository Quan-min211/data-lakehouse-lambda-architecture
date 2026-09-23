{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by=['trade_date']
) }}

with valid_records as (
    select
        *,
        -- Deduplicate: giữ bản ghi mới nhất theo (symbol, trade_id)
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
    to_date(trade_time_ts) as trade_date,
    current_timestamp() as silver_written_at
from valid_records
where dedupe_rank = 1

{% if is_incremental() %}
  -- Chỉ xử lý các partition ngày có dữ liệu mới, không quét lại toàn bộ
  and to_date(trade_time_ts) >= (
      select coalesce(max(trade_date), date('1970-01-01'))
      from {{ this }}
  )
{% endif %}
