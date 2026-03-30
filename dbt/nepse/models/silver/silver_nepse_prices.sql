--selects different columns from the bronze table and calculates price change and percentage change
SELECT
    symbol,
    open,
    high,
    low,
    close,
    ltp,
    -- changes the string volume to a numeric value by removing commas and casting it to BIGINT
    -- using bigint because volume can be a large number and int might overflow
    CAST(REPLACE(volume, ',', '') AS BIGINT) AS volume,
    scraped_date,
    --round is used because sql want typing for numric valuse
    ROUND((ltp - close)::numeric, 2) AS price_change,
    -- nullif is used to avoid division by zero error when close is zero,
    -- it returns null instead of performing the division
    ROUND(((ltp - close) / NULLIF(close, 0) * 100)::numeric, 2) AS change_pct
-- bronze pulls the data from postgres, sliver pulls the data from bronze and perfroms transformations on it
FROM {{ ref('bronze_nepse_prices') }}
-- reason for using ref
-- 1. resolves the correct schema automatically
-- 2. builds a dependency graph (silver depends on bronze)
-- 3. guarantees bronze runs BEFORE silver
-- 4. if bronze fails, silver is automatically skipped
WHERE symbol IS NOT NULL
