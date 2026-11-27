{{ config(materialized='view') }}

{% if target.type == 'spark' %}
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
{% else %}
select
    cast(trade_id as signed) as trade_id,
    upper(cast(symbol as char(32))) as symbol,
    cast(price as double) as price,
    cast(quantity as double) as quantity,
    cast(trade_time as signed) as trade_time,
    cast(trade_time_ts as datetime(3)) as trade_time_ts,
    cast(is_buyer_maker as unsigned) as is_buyer_maker,
    cast(ingestion_time as signed) as ingestion_time,
    cast(is_injected as unsigned) as is_injected,
    cast(fault_type as char(64)) as fault_type,
    cast(batch_run_id as char(64)) as batch_run_id,
    cast(bronze_written_at as datetime(3)) as bronze_written_at
from {{ target.schema }}.raw_crypto_trades
{% endif %}
