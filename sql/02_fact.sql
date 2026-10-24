-- Fact: one row per parcel sale, with the business flags analysts need.
CREATE SCHEMA IF NOT EXISTS fact;

CREATE OR REPLACE TABLE fact.fct_sales AS
WITH deeds AS (
    -- One deed can transfer several parcels. Pricing across those parcels is not
    -- consistent (sometimes repeated, sometimes $1 on all but one), so multi-parcel
    -- deeds are excluded from per-property price statistics.
    SELECT deed_book, deed_page, sale_date, count(DISTINCT parcel_id) AS parcels_on_deed
    FROM staging.stg_sales
    WHERE deed_book IS NOT NULL AND deed_page IS NOT NULL
    GROUP BY ALL
)
SELECT
    s.*,
    year(s.sale_date)                         AS sale_year,
    coalesce(d.parcels_on_deed, 1)            AS parcels_on_deed,
    coalesce(d.parcels_on_deed, 1) > 1        AS is_multi_parcel,
    -- Market (arm's-length) sale: the county's VALID SALE code, a real price, one parcel.
    (
        s.sale_desc = 'VALID SALE'
        AND s.price >= 1000
        AND coalesce(d.parcels_on_deed, 1) = 1
    )                                         AS is_market_sale
FROM staging.stg_sales s
LEFT JOIN deeds d USING (deed_book, deed_page, sale_date);