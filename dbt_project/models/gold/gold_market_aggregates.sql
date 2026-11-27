{% if target.type == 'spark' %}
{{ config(materialized='table', file_format='parquet') }}

with bucketed as (
    select
        symbol,
        window(trade_time_ts, '1 minute') as trade_window,
        trade_id,
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
        -- Trùng mili-giây thì phá thế hòa theo trade_id (giống sort (trade_time, trade_id) của Python)
        min(named_struct('trade_time', trade_time, 'trade_id', trade_id, 'price', price)).price as open_price,
        max(price) as high_price,
        min(price) as low_price,
        max(named_struct('trade_time', trade_time, 'trade_id', trade_id, 'price', price)).price as close_price,
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

{% else %}
{{ config(materialized='table') }}

with ordered_trades as (
    select
        symbol,
        str_to_date(date_format(trade_time_ts, '%Y-%m-%d %H:%i:00'), '%Y-%m-%d %H:%i:%s') as window_start,
        date_add(str_to_date(date_format(trade_time_ts, '%Y-%m-%d %H:%i:00'), '%Y-%m-%d %H:%i:%s'), interval 1 minute) as window_end,
        trade_id,
        price,
        quantity,
        trade_time,
        first_value(price) over (
            partition by symbol, str_to_date(date_format(trade_time_ts, '%Y-%m-%d %H:%i:00'), '%Y-%m-%d %H:%i:%s')
            order by trade_time asc, trade_id asc
        ) as open_price,
        first_value(price) over (
            partition by symbol, str_to_date(date_format(trade_time_ts, '%Y-%m-%d %H:%i:00'), '%Y-%m-%d %H:%i:%s')
            order by trade_time desc, trade_id desc
        ) as close_price
    from {{ ref('silver_cleaned_trades') }}
),

aggregated as (
    select
        symbol,
        window_start,
        window_end,
        max(open_price) as open_price,
        max(price) as high_price,
        min(price) as low_price,
        max(close_price) as close_price,
        sum(quantity) as volume,
        count(*) as trade_count,
        sum(price * quantity) / nullif(sum(quantity), 0) as vwap
    from ordered_trades
    group by symbol, window_start, window_end
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
    cast(trade_count as signed) as trade_count,
    round(vwap, 4) as vwap,
    case
        when abs(close_price - open_price) / nullif(open_price, 0) >= 0.02 then 1
        when (high_price - low_price) / nullif(low_price, 0) >= 0.03 then 1
        else 0
    end as is_spike,
    current_timestamp(3) as gold_written_at
from aggregated
{% endif %}
