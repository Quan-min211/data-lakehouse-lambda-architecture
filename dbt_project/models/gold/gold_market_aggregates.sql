{{ config(materialized='table') }}

with bucketed as (
    select
        symbol,
        window(trade_time_ts, '1 minute') as trade_window,
        price,
        quantity,
        trade_time
    from {{ ref('silver_cleaned_trades') }}
),

aggregated as (
    select
        symbol,
        trade_window.start as window_start,
        trade_window.end as window_end,
        min(named_struct('trade_time', trade_time, 'price', price)).price as open_price,
        max(price) as high_price,
        min(price) as low_price,
        max(named_struct('trade_time', trade_time, 'price', price)).price as close_price,
        sum(quantity) as volume,
        count(*) as trade_count,
        sum(price * quantity) / sum(quantity) as vwap
    from bucketed
    group by symbol, trade_window
)

select
    symbol,
    window_start,
    window_end,
    round(open_price, 4) as open_price,
    round(high_price, 4) as high_price,
    round(low_price, 4) as low_price,
    round(close_price, 4) as close_price,
    round(volume, 6) as volume,
    cast(trade_count as bigint) as trade_count,
    round(vwap, 4) as vwap,
    case
        when abs(close_price - open_price) / nullif(open_price, 0) >= 0.02 then 1
        when (high_price - low_price) / nullif(low_price, 0) >= 0.03 then 1
        else 0
    end as is_spike,
    current_timestamp() as gold_written_at
from aggregated
