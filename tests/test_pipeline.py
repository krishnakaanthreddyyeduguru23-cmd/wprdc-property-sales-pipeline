"""Offline tests: build small CSVs shaped like the WPRDC export and run the real pipeline."""
import csv
from pathlib import Path

import duckdb
import pytest

import pipeline

COLUMNS = ["_id", "PARID", "FULL_ADDRESS", "PROPERTYHOUSENUM", "PROPERTYFRACTION",
           "PROPERTYADDRESSDIR", "PROPERTYADDRESSSTREET", "PROPERTYADDRESSSUF",
           "PROPERTYADDRESSUNITDESC", "PROPERTYUNITNO", "PROPERTYCITY", "PROPERTYSTATE",
           "PROPERTYZIP", "SCHOOLCODE", "SCHOOLDESC", "MUNICODE", "MUNIDESC", "RECORDDATE",
           "SALEDATE", "PRICE", "DEEDBOOK", "DEEDPAGE", "SALECODE", "SALEDESC", "INSTRTYP",
           "INSTRTYPDESC"]


def sale(parid, price, saledate, book, page, desc="VALID SALE", code="0",
         muni="101", muni_name="Alpha Borough", recorddate=None):
    return {"_id": "1", "PARID": parid, "FULL_ADDRESS": "1 MAIN ST", "PROPERTYCITY": "PITTSBURGH",
            "PROPERTYSTATE": "PA", "PROPERTYZIP": "15222.0", "SCHOOLDESC": "Alpha SD",
            "MUNICODE": muni, "MUNIDESC": muni_name, "RECORDDATE": recorddate or saledate,
            "SALEDATE": saledate, "PRICE": price, "DEEDBOOK": book, "DEEDPAGE": page,
            "SALECODE": code, "SALEDESC": desc, "INSTRTYP": "DE", "INSTRTYPDESC": "DEED"}


def base_rows():
    rows = [
        sale("0001A00001000000", "200000", "2019-03-01", "B1", "P1"),
        sale("0001A00002000000", "220000", "2019-06-01", "B1", "P2"),
        sale("0001A00003000000", "240000", "2019-09-01", "B1", "P3"),
        sale("0001A00004000000", "300000", "2025-03-01", "B2", "P1"),
        sale("0001A00005000000", "320000", "2025-06-01", "B2", "P2"),
        # Non-market: $1 family transfer
        sale("0001A00006000000", "1", "2025-07-01", "B2", "P3", desc="LOVE AND AFFECTION SALE", code="AA"),
        # Multi-parcel deed: two parcels, full price repeated on each row
        sale("0001A00007000000", "900000", "2025-08-01", "B2", "P4"),
        sale("0001A00008000000", "900000", "2025-08-01", "B2", "P4"),
    ]
    # Exact duplicate listing of an existing sale
    rows.append(dict(rows[0]))
    return rows


def write_csv(path: Path, rows):
    with path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=COLUMNS)
        w.writeheader()
        for r in rows:
            w.writerow({c: r.get(c, "") for c in COLUMNS})
    return path


def run(tmp_path, rows, name="src.csv"):
    src = write_csv(tmp_path / name, rows)
    return pipeline.main(["--source-file", str(src), "--data-dir", str(tmp_path / "data")])


def query(tmp_path, sql):
    con = duckdb.connect(str(tmp_path / "data" / "warehouse.duckdb"), read_only=True)
    try:
        return con.execute(sql).fetchall()
    finally:
        con.close()


def test_happy_path_builds_mart(tmp_path):
    assert run(tmp_path, base_rows()) == 0
    rows = query(tmp_path, "SELECT sale_year, recorded_transfers, market_sales, median_price "
                           "FROM mart.muni_year_market_sales ORDER BY sale_year")
    # 2019: duplicate collapsed -> 3 sales, median 220k
    # 2025: 2 market sales; $1 transfer and both multi-parcel rows excluded
    assert rows == [(2019, 3, 3, 220000.0), (2025, 5, 2, 310000.0)]


def test_rerun_is_idempotent(tmp_path):
    assert run(tmp_path, base_rows()) == 0
    assert run(tmp_path, base_rows()) == 0
    assert query(tmp_path, "SELECT count(*) FROM raw.ingestion_log") == [(1,)]
    assert query(tmp_path, "SELECT count(*) FROM raw.sales") == [(9,)]
    assert query(tmp_path, "SELECT count(*) FROM fact.fct_sales") == [(8,)]


def test_updated_snapshot_applies_corrections(tmp_path):
    assert run(tmp_path, base_rows()) == 0
    updated = base_rows()
    updated[5]["SALEDESC"] = "VALID SALE"      # county re-validates a sale
    updated[5]["PRICE"] = "150000"
    updated.append(sale("0001A00009000000", "330000", "2025-09-01", "B2", "P5"))
    assert run(tmp_path, updated, "src2.csv") == 0
    assert query(tmp_path, "SELECT count(*) FROM raw.ingestion_log") == [(2,)]
    assert query(tmp_path, "SELECT market_sales FROM mart.muni_year_market_sales "
                           "WHERE sale_year = 2025") == [(4,)]


def test_negative_price_fails(tmp_path):
    rows = base_rows()
    rows[3]["PRICE"] = "-5"
    assert run(tmp_path, rows) == 1


def test_unexpected_date_format_fails(tmp_path):
    rows = base_rows()
    rows[2]["SALEDATE"] = "01.09.2019"
    assert run(tmp_path, rows) == 1


def test_truncated_snapshot_fails(tmp_path):
    assert run(tmp_path, base_rows()) == 0
    assert run(tmp_path, base_rows()[:3], "small.csv") == 1


def test_schema_change_fails(tmp_path):
    rows = base_rows()
    src = tmp_path / "bad.csv"
    with src.open("w") as f:
        f.write("PARID,PRICE\n0001A00001000000,100\n")
    with pytest.raises(RuntimeError, match="missing columns"):
        pipeline.main(["--source-file", str(src), "--data-dir", str(tmp_path / "data")])
