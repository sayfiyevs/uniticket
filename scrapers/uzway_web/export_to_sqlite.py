"""
Exports atto_trip_schedules from SQL Server into a single SQLite file.

Why: your SQL Server runs on your laptop with Windows Authentication. A
deployed app can't reach it. SQLite is one file with no server — commit it
alongside your code and the app works anywhere.

Trade-off: this is a SNAPSHOT. The deployed site shows whatever the data
looked like when you exported. Re-run this and redeploy to refresh. For a
hackathon demo that's fine — schedules 25 days out barely move.

Usage:
    python export_to_sqlite.py
    python export_to_sqlite.py --out uzway.db
"""

import os
import sqlite3
import sys
from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine, text

SOURCE = (
    "mssql+pyodbc://@DESKTOP-5K5RC72/UzWayDB?"
    "driver=ODBC+Driver+17+for+SQL+Server&"
    "Trusted_Connection=yes&"
    "TrustServerCertificate=yes"
)

DEFAULT_OUT = "uzway.db"


def main():
    out = DEFAULT_OUT
    if "--out" in sys.argv:
        try:
            out = sys.argv[sys.argv.index("--out") + 1]
        except IndexError:
            print("--out needs a filename")
            sys.exit(1)

    print("Reading from SQL Server...")
    engine = create_engine(SOURCE)
    with engine.begin() as conn:
        df = pd.read_sql(text("SELECT * FROM atto_trip_schedules"), conn)

    print(f"{len(df)} row(s) read.")
    if df.empty:
        print("Nothing to export — run the scraper first.")
        sys.exit(1)

    # Report what's going out, so a half-empty export is obvious now rather
    # than during the demo.
    print("\nBy mode:")
    for mode, count in df['mode'].value_counts().items():
        print(f"  {mode}: {count}")

    if 'travel_date' in df.columns:
        dates = df['travel_date'].dropna()
        if not dates.empty:
            print(f"\nDates: {dates.min()} .. {dates.max()}")

    pairs = df.groupby(['origin_city', 'destination_city']).size()
    print(f"City pairs with service: {len(pairs)}")

    # datetime columns don't survive SQLite cleanly; store as text.
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].astype(str)

    if os.path.exists(out):
        os.remove(out)

    print(f"\nWriting {out}...")
    with sqlite3.connect(out) as conn:
        df.to_sql('atto_trip_schedules', conn, index=False)
        # Indexes the route search actually uses — without these, every
        # query scans the whole table and connection search gets slow.
        conn.execute("CREATE INDEX idx_origin ON atto_trip_schedules(origin_city)")
        conn.execute("CREATE INDEX idx_dest ON atto_trip_schedules(destination_city)")
        conn.execute("CREATE INDEX idx_pair ON atto_trip_schedules(origin_city, destination_city)")
        conn.execute("CREATE INDEX idx_date ON atto_trip_schedules(travel_date)")

    size_mb = os.path.getsize(out) / 1024 / 1024
    print(f"Done. {out} is {size_mb:.1f} MB, exported {datetime.now():%Y-%m-%d %H:%M}.")

    if size_mb > 90:
        print("\nWARNING: over 90 MB. GitHub rejects files above 100 MB.")
        print("Trim old dates before exporting if this grows further.")

    print("\nNext: point the app at this file with")
    print(f"    set UZWAY_DB=sqlite:///{out}")


if __name__ == "__main__":
    main()
