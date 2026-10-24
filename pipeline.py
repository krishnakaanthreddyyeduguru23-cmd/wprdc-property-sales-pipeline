"""
WPRDC Allegheny County Property Sales pipeline.

Layers:
  raw      -> source CSV landed byte-for-byte + loaded as all-VARCHAR, keyed by snapshot
  staging  -> typed, trimmed, deduplicated sales
  fact     -> one row per parcel sale, with market-sale and multi-parcel flags
  mart     -> municipality x year market sale prices (the analytical output)

Run:  python pipeline.py               # fetch latest from the CKAN API
      python pipeline.py --source-file x.csv   # offline / tests
"""
import argparse
import datetime as dt
import hashlib
import json
import logging
import shutil
import sys
from pathlib import Path

import duckdb
import requests

CKAN_API = "https://data.wprdc.org/api/3/action"
RESOURCE_ID = "5bbe6c55-bce6-4edb-9d04-68edeb6bf7b1"  # Property Sales Transactions
DUMP_URL = f"https://data.wprdc.org/datastore/dump/{RESOURCE_ID}"

EXPECTED_COLUMNS = {
    "PARID", "FULL_ADDRESS", "PROPERTYCITY", "PROPERTYZIP", "SCHOOLDESC",
    "MUNICODE", "MUNIDESC", "RECORDDATE", "SALEDATE", "PRICE",
    "DEEDBOOK", "DEEDPAGE", "SALECODE", "SALEDESC", "INSTRTYP", "INSTRTYPDESC",
}

SQL_DIR = Path(__file__).parent / "sql"
log = logging.getLogger("pipeline")


# ---------------------------------------------------------------- ingest
def sha256_of(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def fetch_source_metadata() -> dict:
    """Ask CKAN when the resource was last modified (used for lineage)."""
    r = requests.get(f"{CKAN_API}/resource_show", params={"id": RESOURCE_ID}, timeout=60)
    r.raise_for_status()
    body = r.json()
    if not body.get("success"):
        raise RuntimeError(f"CKAN resource_show failed: {body}")
    res = body["result"]
    return {"last_modified": res.get("last_modified") or res.get("metadata_modified")}


def download(url: str, dest: Path) -> None:
    """Stream to a temp file, then rename, so a failed download never leaves a partial file."""
    tmp = dest.with_suffix(".part")
    with requests.get(url, stream=True, timeout=300) as r:
        r.raise_for_status()
        with tmp.open("wb") as f:
            for chunk in r.iter_content(1 << 20):
                f.write(chunk)
    tmp.replace(dest)


def land_raw(data_dir: Path, source_file: str | None) -> tuple[Path, dict]:
    """Land the source unchanged under data/raw/, named by content hash."""
    raw_dir = data_dir / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    staging_file = raw_dir / "incoming.csv"

    if source_file:
        shutil.copyfile(source_file, staging_file)
        meta = {"last_modified": None, "source": str(source_file)}
    else:
        meta = fetch_source_metadata()
        meta["source"] = DUMP_URL
        download(DUMP_URL, staging_file)

    # Guard against the API returning an error page instead of data.
    with staging_file.open("rb") as f:
        head = f.read(2048).lstrip(b"\xef\xbb\xbf").lstrip()
    if head[:1] == b"<" or b"," not in head.split(b"\n", 1)[0]:
        staging_file.unlink()
        raise RuntimeError(f"Download is not a CSV. First bytes: {head[:200]!r}")

    digest = sha256_of(staging_file)
    final = raw_dir / f"sales_{digest[:16]}.csv"
    if final.exists():
        staging_file.unlink()           # identical content already landed
    else:
        staging_file.replace(final)
    meta["sha256"] = digest
    return final, meta


# ---------------------------------------------------------------- load
def load_raw(con, raw_file: Path, meta: dict) -> str:
    snapshot_id = meta["sha256"][:16]
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")
    con.execute("""
        CREATE TABLE IF NOT EXISTS raw.ingestion_log (
            snapshot_id VARCHAR PRIMARY KEY, sha256 VARCHAR, source VARCHAR,
            source_last_modified VARCHAR, loaded_at TIMESTAMP, row_count BIGINT,
            file_path VARCHAR)""")

    if con.execute("SELECT 1 FROM raw.ingestion_log WHERE snapshot_id = ?", [snapshot_id]).fetchone():
        log.info("Snapshot %s already loaded; skipping raw load (idempotent).", snapshot_id)
        return snapshot_id

    # All columns as text: raw stays exactly what the source said.
    # The WPRDC export mixes line endings: the header ends in CRLF but data rows end in LF,
    # which DuckDB's strict parser rejects. strict_mode = false accepts it; the column
    # count and required-column checks still guard against a genuinely malformed file.
    # Encoding falls back to latin-1 in case free-text fields are not valid UTF-8.
    last_error = None
    for encoding in ("utf-8", "latin-1"):
        try:
            con.execute(f"""
                CREATE OR REPLACE TEMP TABLE incoming AS
                SELECT * FROM read_csv('{raw_file.as_posix()}',
                    all_varchar = true, header = true, delim = ',', quote = '"',
                    escape = '"', encoding = '{encoding}', max_line_size = 10000000,
                    strict_mode = false)""")
            log.info("Read raw file as %s.", encoding)
            break
        except duckdb.Error as e:
            last_error = e
            log.warning("Could not read raw file as %s: %s", encoding, str(e).splitlines()[0])
    else:
        raise RuntimeError(f"Could not parse {raw_file}") from last_error
    cols = {c[0].upper() for c in con.execute("DESCRIBE incoming").fetchall()}
    missing = EXPECTED_COLUMNS - cols
    if missing:
        raise RuntimeError(f"Source schema changed; missing columns: {sorted(missing)}")

    if not con.execute("SELECT 1 FROM information_schema.tables "
                       "WHERE table_schema='raw' AND table_name='sales'").fetchone():
        con.execute("CREATE TABLE raw.sales AS SELECT *, ''::VARCHAR AS snapshot_id "
                    "FROM incoming WHERE false")

    con.execute("BEGIN")
    con.execute("DELETE FROM raw.sales WHERE snapshot_id = ?", [snapshot_id])
    con.execute("INSERT INTO raw.sales BY NAME SELECT *, ? AS snapshot_id FROM incoming", [snapshot_id])
    n = con.execute("SELECT count(*) FROM incoming").fetchone()[0]
    con.execute("INSERT INTO raw.ingestion_log VALUES (?, ?, ?, ?, now(), ?, ?)",
                [snapshot_id, meta["sha256"], meta.get("source"), meta.get("last_modified"),
                 n, raw_file.as_posix()])
    con.execute("COMMIT")
    log.info("Loaded snapshot %s with %s rows.", snapshot_id, n)
    return snapshot_id


# ---------------------------------------------------------------- transform
def run_sql_file(con, name: str, snapshot_id: str) -> None:
    sql = (SQL_DIR / name).read_text().replace("{{snapshot_id}}", snapshot_id)
    con.execute(sql)


def transform(con, snapshot_id: str) -> None:
    for f in ("01_staging.sql", "02_fact.sql", "03_mart.sql"):
        run_sql_file(con, f, snapshot_id)
        log.info("Built %s", f)


# ---------------------------------------------------------------- quality
def run_checks(con, snapshot_id: str, data_dir: Path) -> bool:
    checks = json.loads((SQL_DIR / "checks.json").read_text())
    results, ok = [], True
    for c in checks:
        sql = c["sql"].replace("{{snapshot_id}}", snapshot_id)
        failing = con.execute(sql).fetchone()[0] or 0
        passed = failing <= c.get("max_failing", 0)
        if not passed and c["severity"] == "error":
            ok = False
        results.append({**{k: c[k] for k in ("name", "severity", "why")},
                        "failing_rows": int(failing), "passed": passed})
        level = logging.INFO if passed else (logging.ERROR if c["severity"] == "error" else logging.WARNING)
        log.log(level, "[%s] %s: %s failing", "PASS" if passed else c["severity"].upper(), c["name"], failing)
    (data_dir / "quality_report.json").write_text(json.dumps(
        {"snapshot_id": snapshot_id, "run_at": dt.datetime.now().isoformat(), "checks": results}, indent=2))
    return ok


# ---------------------------------------------------------------- main
def main(argv=None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--source-file", help="Load a local CSV instead of calling the API")
    p.add_argument("--data-dir", default="data")
    args = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    data_dir = Path(args.data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)
    raw_file, meta = land_raw(data_dir, args.source_file)

    con = duckdb.connect(str(data_dir / "warehouse.duckdb"))
    try:
        snapshot_id = load_raw(con, raw_file, meta)
        transform(con, snapshot_id)
        ok = run_checks(con, snapshot_id, data_dir)
    finally:
        con.close()

    if not ok:
        log.error("Data quality checks failed. See %s/quality_report.json", data_dir)
        return 1
    log.info("Done. Output table: mart.muni_year_market_sales")
    return 0


if __name__ == "__main__":
    sys.exit(main())