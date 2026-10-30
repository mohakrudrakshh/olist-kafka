
from pathlib import Path

import pyarrow.csv as csv
import pyarrow.parquet as pq

PROJECT_ROOT = Path(__file__).resolve().parents[1]
CSV_FILE = PROJECT_ROOT.parent / "raw" / "olist_orders_dataset.csv"
PARQUET_FILE = PROJECT_ROOT / "data" / "parquet" / "olist_orders.parquet"

PARQUET_FILE.parent.mkdir(parents=True, exist_ok=True)

print("Reading CSV:", CSV_FILE)

table = csv.read_csv(CSV_FILE)

print("Rows:", table.num_rows)
print("Columns:", table.num_columns)

pq.write_table(
    table,
    PARQUET_FILE,
    compression="snappy"
)

print("Parquet created:", PARQUET_FILE)

verified = pq.read_table(PARQUET_FILE)

print("Verified Parquet rows:", verified.num_rows)
print("Verified Parquet columns:", verified.num_columns)
print("CSV bytes:", CSV_FILE.stat().st_size)
print("Parquet bytes:", PARQUET_FILE.stat().st_size)
