# Allegheny County Property Sales Pipeline
 
This pulls Allegheny County property sale transactions from the WPRDC CKAN API, lands the raw file exactly as downloaded, models it into a clean fact table, runs quality checks, and builds one analytical table to answer a question about the local housing market.
 
## The question
 
**Which municipalities saw the biggest rise in median home sale price between 2019 and 2025, counting only real market sales?**
 
That last part ended up mattering more than I expected. Most "sales" in this dataset aren't sales in the market sense at all (more on that below).
 
The output table is `mart.muni_year_market_sales`. It has one row per municipality per year: how many transfers were recorded, how many were market sales, the median and quartile prices, year-over-year change, and a flag for whether the sample is big enough to trust.
 
## Answer
 
Looking only at municipalities with at least 20 market sales in both 2019 and 2025:
 
| Municipality | Median 2019 | Median 2025 | Growth | Market sales in 2025 |
| --- | --- | --- | --- | --- |
| 20th Ward, Pittsburgh | $59,750 | $192,500 | +222% | 68 |
| 27th Ward, Pittsburgh | $70,000 | $178,000 | +154% | 66 |
| Dormont | $120,000 | $285,000 | +138% | 86 |
| Jefferson Hills | $155,250 | $327,000 | +111% | 87 |
| Penn Hills | $89,500 | $184,250 | +106% | 325 |
 
I'd read the top of this list carefully. In the 20th Ward, only 16% of 2025 transfers were market sales, and a median can jump because the *mix* of homes changed (for example, more renovated flips selling) rather than because every house got more valuable. Penn Hills is the result I trust most, because it has 325 market sales behind it.
 
## Running it
 
```bash
python -m venv .venv
.venv\Scripts\activate          # on Mac/Linux: source .venv/bin/activate
pip install -r requirements.txt
 
python pipeline.py              # download, build every layer, run checks
python answer.py                # print the answer above
python explore.py               # the queries behind the findings below
pytest -q                       # offline tests on small synthetic files
```
 
`pipeline.py` exits with a non-zero code if any error-level check fails, and writes every check result to `data/quality_report.json`.
 
## How it's built
 
| Layer | Where | What's in it |
| --- | --- | --- |
| Raw | `data/raw/sales_<hash>.csv`, `raw.sales` | The file exactly as downloaded, loaded as text columns and tagged with a snapshot ID |
| Staging | `staging.stg_sales` | Typed, trimmed, deduplicated rows |
| Fact | `fact.fct_sales` | One row per parcel sale, plus `is_market_sale` and `is_multi_parcel` flags |
| Mart | `mart.muni_year_market_sales` | Municipality by year market statistics |
 
It's Python, DuckDB, and plain SQL files. Nothing needs a cloud account.
 
## Re-running and updates
 
Running it twice doesn't duplicate anything. The raw file is named by a hash of its contents, so downloading the same data twice gives the same file. Each load is tagged with a snapshot ID, and if that snapshot is already loaded, the raw load is skipped. You can see this in the log: "Snapshot already loaded; skipping raw load." Everything downstream is rebuilt with `CREATE OR REPLACE`, so the output is identical every time.
 
For updates: WPRDC refreshes this file monthly, and the county goes back and corrects older records. Sale validation codes are explicitly described as subject to change. That rules out a simple incremental load by date, because it would never pick up a correction to a sale from three years ago. So each new monthly file becomes a new snapshot, the modeled layers are rebuilt from it, and older snapshots stay in `raw.sales` so you can see what the county changed between months.
 
## Quality checks
 
The checks are in `sql/checks.json`. Each has a severity and a one-line reason. Errors stop the pipeline; warnings get logged and reported.
 
Three of them exist because of things I found in this specific dataset:
 
- **`multi_parcel_deeds_excluded`:** One deed can transfer many parcels at once. If each parcel row were treated as its own sale, prices would be badly distorted. This fails if any deed contributes more than one market sale.
- **`multi_parcel_price_is_repeated`** (warning): I originally assumed the deed price was repeated on every parcel. This check tests that assumption, and it turned out to be wrong for 561 deeds (details below).
- **`dates_have_plausible_year`:** Added after I found a record date with the year 212. There's exactly one such row today. The check allows up to 5 and fails beyond that, so a systemic problem would stop the pipeline.

The others cover the basics: an empty or truncated download (a file with more than 10% fewer rows than the previous one fails), missing columns, dates that don't parse, missing parcel IDs, negative prices, future sale dates, duplicate sale keys, $0 and $1 sales leaking into price statistics, and internal consistency in the mart.
 
The tests deliberately feed in bad data (a negative price, a date in an unexpected format, a truncated file, a missing column) and confirm the pipeline stops.
 
## The three decisions that mattered most
 
**1. Rebuild from the full snapshot instead of loading incrementally.**
Because the county corrects historical records, an incremental load keyed on dates would quietly keep stale data. The full file is 506,315 rows and the whole pipeline runs in about 2  seconds on my laptop, so getting correctness costs almost nothing.
*What I didn't do:* an incremental merge on `RECORDDATE`.
 
**2. Use a natural key, not the CKAN `_id`.**
The `_id` column gets reassigned whenever the datastore reloads, so it can't identify the same sale across months. I key sales on parcel ID, deed book, deed page, and sale date. That turned out to be unique across all 506,315 rows.
*What I didn't do:* `_id`, or a hash of the whole row (which would change every time the county corrected a field, turning one sale into two).
 
**3. Define "market sale" narrowly, and make it testable.**
A market sale here means the county coded it "VALID SALE", the price is at least $1,000, and it's the only parcel on its deed. Some other categories, like "TIME ON MARKET" and "OTHER", have prices that look market-like, but the county flags them as unrepresentative, so I left them out. That's a conservative choice, and it's easy to change in one line of `sql/02_fact.sql`.
*What I didn't do:* include every transfer (dominated by $1 transfers), or use a price cutoff alone (which would keep multi-parcel and non-arm's-length sales).
 
## What surprised me
 
- **Only about 1 in 5 transfers is a market sale (19.8%).** The biggest single category is "LOVE AND AFFECTION SALE", with 104,538 rows, more than "VALID SALE" (100,171). Nearly all of them were for $1 or less. Any analysis that treated every row as a sale would mostly be measuring family transfers.
- **The file mixes line endings.** The header line ends in `\r\n` but every data row ends in `\n`, and DuckDB's strict CSV parser refused to read it. I set the CSV format explicitly and relaxed strict mode, relying on the column and data checks to catch a file that's genuinely broken.
- **Multi-parcel deeds don't price consistently.** There are 69,236 rows across 24,980 multi-parcel deeds. My check flagged 561 deeds where parcels on the same deed had different prices, and they fall into three patterns:
  - token amounts, like $10 on one parcel and $1 on the rest;
  - small differences that look like typos, like $7,160,367 vs. $7,160,067;
  - exact factor-of-10 differences, like $1,300,000 vs. $130,000, which look like a dropped zero.
  
  All multi-parcel deeds are excluded from the market statistics, so the answer isn't affected, but I wouldn't have known about this without the check.
- **A date in the year 212.** One record date is "212-08-01", almost certainly 2012 with a digit missing. It's a valid date as far as the parser is concerned, so my date-format check didn't catch it. A few other rows have record and sale dates exactly 10 years apart, which also look like typos. The `recorded_before_sale` warning flags 126 rows like this.
- **Grouping by deed finds more multi-parcel sales than the county's own label.** About 1,900 more rows than are coded "MULTI-PARCEL SALE".
## At 100x the data, or with daily refreshes
 
Full rebuilds stop making sense at that point. I would:
- hash each row and compare it to the previous snapshot, then merge only the rows that were added, changed, or removed into a history table, keeping a record of each change to a sale's validation code;
- land raw data as partitioned Parquet in object storage instead of one big CSV;
- page through the CKAN `datastore_search` API instead of downloading a single dump, with retries and backoff;
- schedule it with Airflow or Dagster, and alert on failed checks and late data;
- move the models and checks into dbt for lineage, documentation, and tests as code.

## What I didn't do
 
- **Join to parcel characteristics** (square footage, bedrooms). That would allow price per square foot and help separate real appreciation from changes in which homes sold, but it goes beyond the question.
- **Adjust for inflation.** Nominal prices answer the question as asked.
- **Fix the bad records.** I flag them rather than guessing what the right value should be. Correcting source data is the county's call, not mine.
- **Orchestration or cloud deployment.** The brief asked for something local and reproducible.


 
