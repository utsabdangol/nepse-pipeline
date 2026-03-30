SELECT
    symbol,
    scraped_date,
    ltp,
    price_change,
    change_pct,
    -- calculates the 7-day average price for each symbol using a window function
    -- partion is used to group stocks
    -- order by is used to ensure the average is calculated in the correct order of dates
    -- row preceding and current row is used to specify the range of rows to include in the average calculation (7 days)
    AVG(ltp) OVER (
        PARTITION BY symbol
        ORDER BY scraped_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS avg_7_day,
    AVG(ltp) OVER (
        PARTITION BY symbol
        ORDER BY scraped_date
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS avg_30_day,
    -- calculates the 30-day maximum price for each symbol using a window function
    MAX(ltp) OVER (
        PARTITION BY symbol
        ORDER BY scraped_date
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS max_30_day,
    MIN(ltp) OVER (
        PARTITION BY symbol
        ORDER BY scraped_date
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS min_30_day
FROM {{ ref('silver_nepse_prices') }}
