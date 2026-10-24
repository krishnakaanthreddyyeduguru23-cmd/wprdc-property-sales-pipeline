-- Mart: answers "How has the median market sale price changed by municipality,
-- and what share of recorded transfers are actual market sales?"
CREATE SCHEMA IF NOT EXISTS mart;

CREATE OR REPLACE TABLE mart.muni_year_market_sales AS
WITH by_year AS (
    SELECT
        municipality_code,
        any_value(municipality)                                   AS municipality,
        sale_year,
        count(*)                                                  AS recorded_transfers,
        count(*) FILTER (WHERE is_market_sale)                    AS market_sales,
        count(*) FILTER (WHERE is_multi_parcel)                   AS multi_parcel_rows,
        median(price) FILTER (WHERE is_market_sale)               AS median_price,
        quantile_cont(price, 0.25) FILTER (WHERE is_market_sale)  AS p25_price,
        quantile_cont(price, 0.75) FILTER (WHERE is_market_sale)  AS p75_price
    FROM fact.fct_sales
    WHERE sale_year BETWEEN 2013 AND year(current_date)
      AND municipality_code IS NOT NULL
    GROUP BY municipality_code, sale_year
)
SELECT
    *,
    round(market_sales / recorded_transfers, 4) AS market_share,
    round(median_price / lag(median_price) OVER (PARTITION BY municipality_code ORDER BY sale_year) - 1, 4)
        AS median_price_yoy_change,
    market_sales >= 20 AS is_reliable   -- small samples make medians noisy
FROM by_year;
