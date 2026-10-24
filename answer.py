"""Print the answer to the analysis question from the built mart."""
from pathlib import Path
import duckdb

con = duckdb.connect("data/warehouse.duckdb", read_only=True)
print(con.execute(Path("sql/answer.sql").read_text()).df().to_string(index=False))
