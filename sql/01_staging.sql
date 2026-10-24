-- Staging: type, trim and deduplicate the current snapshot. No business rules here.
CREATE SCHEMA IF NOT EXISTS staging;

CREATE OR REPLACE TABLE staging.stg_sales AS
WITH src AS (
    SELECT * FROM raw.sales WHERE snapshot_id = '{{snapshot_id}}'
),
typed AS (
    SELECT
        upper(trim(PARID))                                    AS parcel_id,
        nullif(trim(FULL_ADDRESS), '')                        AS full_address,
        nullif(trim(PROPERTYCITY), '')                        AS city,
        left(regexp_replace(trim(PROPERTYZIP), '\.0+$', ''), 5) AS zip,
        nullif(trim(SCHOOLDESC), '')                          AS school_district,
        regexp_replace(trim(MUNICODE), '\.0+$', '')           AS municipality_code,
        nullif(trim(MUNIDESC), '')                            AS municipality,
        -- The data dictionary says dd/mm/yyyy, but the datastore exports ISO dates.
        -- Accept known formats explicitly; anything else stays NULL and fails a check.
        try_strptime(trim(SALEDATE),   ['%Y-%m-%d', '%Y-%m-%dT%H:%M:%S', '%m/%d/%Y'])::DATE AS sale_date,
        try_strptime(trim(RECORDDATE), ['%Y-%m-%d', '%Y-%m-%dT%H:%M:%S', '%m/%d/%Y'])::DATE AS record_date,
        nullif(trim(SALEDATE), '')                            AS sale_date_raw,
        try_cast(trim(PRICE) AS DOUBLE)                       AS price,
        nullif(trim(DEEDBOOK), '')                            AS deed_book,
        nullif(trim(DEEDPAGE), '')                            AS deed_page,
        upper(nullif(trim(SALECODE), ''))                     AS sale_code,
        upper(nullif(trim(SALEDESC), ''))                     AS sale_desc,
        upper(nullif(trim(INSTRTYP), ''))                     AS instrument_type,
        upper(nullif(trim(INSTRTYPDESC), ''))                 AS instrument_desc
    FROM src
)
SELECT
    -- Natural key: one deed (book/page) transferring one parcel on one date.
    -- _id from the CKAN datastore is NOT used: it is reassigned on every reload.
    md5(concat_ws('|', parcel_id, coalesce(deed_book, ''), coalesce(deed_page, ''),
                  coalesce(sale_date::VARCHAR, ''))) AS sale_id,
    *
FROM typed
-- Exact re-listings of the same sale collapse to one row; keep the latest recording.
QUALIFY row_number() OVER (PARTITION BY sale_id ORDER BY record_date DESC NULLS LAST, price DESC NULLS LAST) = 1;
