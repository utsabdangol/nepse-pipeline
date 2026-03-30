SELECT
    id,
    symbol,
    open,
    high,
    low,
    close,
    ltp,
    volume,
    turnover,
    scraped_date
FROM {{ source('public', 'nepse_prices') }}
