"""
Prints exactly what's in SQL and in the progress file so we can stop
guessing at formats. Read-only — changes nothing.

Run:  python diagnose.py
"""
import json, os
import pandas as pd
from sqlalchemy import text
import atto_scraper as a

PROGRESS_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "backfill_progress.json")

with a.engine.begin() as conn:
    df = pd.read_sql(text(
        "SELECT trip_id, destination_city, travel_date "
        "FROM atto_trip_schedules WHERE mode='FLIGHT'"), conn)

print("=" * 60)
print(f"FLIGHT rows in SQL: {len(df)}")
print(f"NULL travel_date:   {int(df['travel_date'].isna().sum())}")
print("=" * 60)

print("\n--- 5 sample trip_ids (note how the date is formatted) ---")
for t in df['trip_id'].head(5):
    print(f"  {t!r}")
    print(f"      rsplit('-',1)[-1] = {t.rsplit('-',1)[-1]!r}")

print("\n--- distinct travel_date values ---")
print(sorted(df['travel_date'].dropna().unique().tolist()))

print("\n--- distinct destination_city values ---")
print(sorted(df['destination_city'].dropna().unique().tolist()))

try:
    with open(PROGRESS_FILE, encoding="utf-8") as f:
        prog = json.load(f)
    done = prog.get("done", [])
    print(f"\n--- progress file: {len(done)} entries, repaired={prog.get('repaired')} ---")
    print("first 5:", done[:5])
except Exception as e:
    print(f"\n--- progress file unreadable: {e} ---")

print("\n--- what backfill will look for ---")
print("first 5 keys it expects:",
      [f"{d}|{dt}" for dt in a.get_upcoming_dates(6)[3:] for d in ["Samarkand"]])