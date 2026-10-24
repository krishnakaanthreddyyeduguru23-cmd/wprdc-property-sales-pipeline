-- Question: Which municipalities saw the largest growth in median market sale price
-- from 2019 to 2025, and what share of their recorded transfers were market sales?
WITH y AS (
    SELECT municipality_code, municipality, sale_year, median_price, market_sales, market_share
    FROM mart.muni_year_market_sales
    WHERE is_reliable AND sale_year IN (2019, 2025)
)
SELECT
    a.municipality,
    a.median_price                              AS median_2019,
    b.median_price                              AS median_2025,
    round(b.median_price / a.median_price - 1, 3) AS growth,
    b.market_sales                              AS market_sales_2025,
    b.market_share                              AS market_share_2025
FROM y a JOIN y b USING (municipality_code)
WHERE a.sale_year = 2019 AND b.sale_year = 2025
ORDER BY growth DESC
LIMIT 15;
