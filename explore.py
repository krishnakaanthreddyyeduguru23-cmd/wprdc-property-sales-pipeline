"""Explore the built warehouse to gather findings for the README."""
import duckdb

con = duckdb.connect("data/warehouse.duckdb", read_only=True)


def show(title, sql):
    print(f"\n== {title} ==")
    print(con.execute(sql).df().to_string(index=False))


show("Rows: raw vs. after dedup", """
    SELECT (SELECT count(*) FROM raw.sales) AS raw_rows,
           (SELECT count(*) FROM fact.fct_sales) AS sales""")

show("Market sale share overall", """
    SELECT round(avg(is_market_sale::INT), 3) AS market_share
    FROM fact.fct_sales""")

show("Sale descriptions with price stats", """
    SELECT sale_desc, count(*) AS n,
           median(price) AS median_price,
           count(*) FILTER (WHERE price <= 1) AS dollar_or_less
    FROM fact.fct_sales GROUP BY 1 ORDER BY n DESC LIMIT 25""")

show("Price distribution", """
    SELECT count(*) FILTER (WHERE price = 0) AS zero,
           count(*) FILTER (WHERE price = 1) AS one_dollar,
           count(*) FILTER (WHERE price >= 1000) AS over_1000
    FROM fact.fct_sales""")

show("Multi-parcel rows", """
    SELECT count(*) AS rows,
           count(DISTINCT (deed_book, deed_page, sale_date)) AS deeds
    FROM fact.fct_sales WHERE is_multi_parcel""")

show("Multi-parcel deeds: total vs. inconsistent prices", """
    SELECT count(*) AS multi_parcel_deeds,
           count(*) FILTER (WHERE dp > 1) AS with_different_prices
    FROM (SELECT deed_book, deed_page, sale_date, count(DISTINCT price) AS dp
          FROM fact.fct_sales WHERE is_multi_parcel GROUP BY ALL)""")

show("Multi-parcel deeds with different prices (sample)", """
    SELECT deed_book, deed_page, sale_date,
           count(*) AS parcels,
           count(DISTINCT price) AS distinct_prices,
           max(price) AS max_price, min(price) AS min_price
    FROM fact.fct_sales WHERE is_multi_parcel
    GROUP BY ALL HAVING count(DISTINCT price) > 1
    ORDER BY parcels DESC LIMIT 10""")

show("Recorded long before sale (sample)", """
    SELECT parcel_id, sale_date, record_date, price, sale_desc
    FROM fact.fct_sales
    WHERE record_date < sale_date - INTERVAL 30 DAY
    ORDER BY sale_date - record_date DESC LIMIT 10""")