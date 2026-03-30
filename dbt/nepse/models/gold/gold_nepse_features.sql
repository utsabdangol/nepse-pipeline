SELECT
    symbol,
    scraped_date,
    ltp,
    price_change,
    change_pct,
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
