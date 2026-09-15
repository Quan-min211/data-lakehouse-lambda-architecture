{{ config(materialized='view') }}

select
    cast(trade_id as bigint) as trade_id,
    upper(cast(symbol as string)) as symbol,
    cast(price as double) as price,
    cast(quantity as double) as quantity,
    cast(trade_time as bigint) as trade_time,
    cast(trade_time_ts as timestamp) as trade_time_ts,
    cast(is_buyer_maker as boolean) as is_buyer_maker,
    cast(ingestion_time as bigint) as ingestion_time,
    cast(is_injected as boolean) as is_injected,
    cast(fault_type as string) as fault_type,
    cast(batch_run_id as string) as batch_run_id,
    cast(bronze_written_at as timestamp) as bronze_written_at
from iceberg_catalog.bronze.crypto_trades
