"""
Repairs backfill_progress.json — version 2.

Two problems to undo, both caused by running backfill_flights.py against
an older atto_scraper.py:

  1. That version's flight records had no travel_date field, so every
     flight row it wrote has travel_date = NULL. The date is still
     recoverable: trip_id ends in the departure date as DD.MM.YYYY.

  2. When the network dropped, failed routes were recorded as complete
     (the old fetch_flight_schedules returned a bare list, which the
     wrapper read as "succeeded, no flights").

So: recover travel_date from trip_id, write it back to the table, then
rebuild the progress file from routes that demonstrably have rows.

Run:  python repair_progress.py
"""

import json
import os
from datetime import datetime

import pandas as pd
from sqlalchemy import text

import atto_scraper as a

PROGRESS_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "backfill_progress.json"
)


def date_from_trip_id(trip_id):
    """ATTO-FLIGHT-HY1234-TAS-SKD-25.08.2026 -> '2026-08-25'."""
    try:
        tail = trip_id.rsplit("-", 1)[-1]
        return datetime.strptime(tail, "%d.%m.%Y").strftime("%Y-%m-%d")
    except Exception:
        return None


def main():
    try:
        with open(PROGRESS_FILE, "r", encoding="utf-8") as f:
            claimed = set(json.load(f).get("done", []))
    except Exception:
        claimed = set()

    print(f"Progress file claims {len(claimed)} completed route(s).")

    with a.engine.begin() as conn:
        df = pd.read_sql(
            text("""
                SELECT trip_id, destination_city, travel_date
                FROM atto_trip_schedules
                WHERE mode = 'FLIGHT'
            """),
            conn
        )

    print(f"Found {len(df)} flight row(s) in SQL.")
    if df.empty:
        print("No flight data at all — nothing to repair.")
        return

    missing = int(df['travel_date'].isna().sum())
    print(f"{missing} row(s) have a NULL travel_date — recovering from trip_id.")

    # Recover the date for rows written by the older version.
    df['recovered'] = df.apply(
        lambda r: r['travel_date'] if pd.notna(r['travel_date'])
        else date_from_trip_id(r['trip_id']),
        axis=1
    )

    unrecoverable = int(df['recovered'].isna().sum())
    if unrecoverable:
        print(f"WARNING: {unrecoverable} row(s) had no parseable date in trip_id "
              f"and will be ignored.")

    # Write the recovered dates back so the table is actually usable for
    # training queries, not just for this repair.
    to_fix = df[df['travel_date'].isna() & df['recovered'].notna()]
    if not to_fix.empty:
        print(f"Writing travel_date back for {len(to_fix)} row(s)...")
        with a.engine.begin() as conn:
            conn.execute(
                text("UPDATE atto_trip_schedules SET travel_date = :d "
                     "WHERE trip_id = :t"),
                [{"d": r.recovered, "t": r.trip_id} for r in to_fix.itertuples()]
            )
        print("Done.")

    verified = {
        f"{r.destination_city}|{r.recovered}"
        for r in df.itertuples() if r.recovered
    }
    print(f"\nDatabase proves {len(verified)} route/date pair(s) completed.")

    bogus = claimed - verified
    if bogus:
        print(f"{len(bogus)} route(s) marked done with no rows to show for it "
              f"(the network outage) — re-queued:")
        for key in sorted(bogus)[:15]:
            dest, date = key.split("|")
            print(f"  {a.ORIGIN_CITY} -> {dest} ({date})")
        if len(bogus) > 15:
            print(f"  ... and {len(bogus) - 15} more")

    recovered_count = len(verified - claimed)
    if recovered_count:
        print(f"{recovered_count} route(s) had rows but weren't in the progress "
              f"file — now marked done, so they won't be re-fetched.")

    with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
        json.dump({"done": sorted(verified),
                   "updated_at": datetime.now().isoformat(timespec="seconds"),
                   "repaired": True},
                  f, indent=2)

    print(f"\nRewrote {PROGRESS_FILE} with {len(verified)} verified route(s).")
    print("Now run:  python backfill_flights.py")


if __name__ == "__main__":
    main()