"""
One-off flight backfill.

You already have trains, buses (25 days) and flights for days 0-2.
This fills in flights for the REST of the horizon (day 3 onward) and
then stops. It is not scheduled and does not run the ground pipeline.

Run it with:      python backfill_flights.py

Safe to interrupt: progress is written to backfill_progress.json after
every route, so re-running picks up where it stopped instead of redoing
the hours already spent. Delete that file to force a clean start.

Everything is imported from atto_scraper.py, so this stays in sync with
whatever headers/parsing/DB logic lives there — keep both files in the
same folder.
"""

import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import atto_scraper as a

# ==========================================
# SETTINGS
# ==========================================

START_DAY = 0

# None = go to the end of the discovered horizon (25 as of last scan).
END_DAY = None

# Parallel searches. 3 is a reasonable middle; drop to 2 if you see the
# timeout rate climb, raise to 4-5 if the API tolerates it.
CONCURRENCY = 3

# How many times to re-attempt routes that gave up waiting for results.
RETRY_PASSES = 2

PROGRESS_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "backfill_progress.json"
)


# ==========================================
# PROGRESS TRACKING
# ==========================================
def load_progress():
    try:
        with open(PROGRESS_FILE, "r", encoding="utf-8") as f:
            return set(json.load(f).get("done", []))
    except Exception:
        return set()


def save_progress(done):
    try:
        with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
            json.dump({"done": sorted(done),
                       "updated_at": datetime.now().isoformat(timespec="seconds")},
                      f, indent=2)
    except Exception as e:
        print(f"  [progress] Could not write {PROGRESS_FILE}: {e}")


def route_key(dest, date):
    return f"{dest}|{date}"


# ==========================================
# FETCH
# ==========================================
def fetch_one(session, airport_codes, dest, date):
    """Wraps atto_scraper.fetch_flight_schedules.

    The current version returns (schedules, timed_out). Older copies of
    the file return a bare list, which is dangerous here: a network error
    would look like 'succeeded, no flights' and the route would be marked
    done and never retried. So we refuse to run against the old version
    rather than silently corrupting the progress file.
    """
    result = a.fetch_flight_schedules(
        session,
        airport_codes[a.ORIGIN_CITY], airport_codes[dest],
        a.ORIGIN_CITY, dest, date
    )
    if not isinstance(result, tuple):
        raise RuntimeError(
            "atto_scraper.fetch_flight_schedules returned a list, not a "
            "(schedules, timed_out) tuple. You are running an outdated "
            "atto_scraper.py — update it before backfilling, or failed "
            "routes will be recorded as complete."
        )
    return result


def run_batch(session, airport_codes, routes, done):
    """Runs a list of (dest, date) routes. Returns (schedules, timed_out)."""
    schedules = []
    timed_out = []

    with ThreadPoolExecutor(max_workers=CONCURRENCY) as ex:
        futures = {
            ex.submit(fetch_one, session, airport_codes, dest, date): (dest, date)
            for dest, date in routes
        }

        for future in as_completed(futures):
            dest, date = futures[future]
            try:
                result, did_time_out = future.result()
            except RuntimeError:
                raise  # version mismatch — abort rather than corrupt progress
            except Exception as e:
                print(f"  {a.ORIGIN_CITY} -> {dest} ({date}) failed: {e}")
                timed_out.append((dest, date))
                continue

            if did_time_out:
                # Don't mark as done — we want to come back to it.
                timed_out.append((dest, date))
            else:
                schedules.extend(result)
                done.add(route_key(dest, date))

    return schedules, timed_out


# ==========================================
# MAIN
# ==========================================
def main():
    horizon = a.load_horizon()
    end_day = horizon if END_DAY is None else min(END_DAY, horizon)

    if START_DAY >= end_day:
        print(f"Nothing to do: START_DAY={START_DAY} but horizon is only {horizon} days.")
        return

    all_dates = a.get_upcoming_dates(end_day)
    dates = all_dates[START_DAY:]

    done = load_progress()
    if done:
        print(f"Resuming — {len(done)} route(s) already completed in an earlier run.")

    session = a.build_session()

    destinations = [c for c in a.STATIONS if c != a.ORIGIN_CITY]
    print("Looking up airport codes...")
    airport_codes = a.get_airport_codes(session, [a.ORIGIN_CITY] + destinations)
    print(f"Airports found for: {list(airport_codes.keys())}")

    if a.ORIGIN_CITY not in airport_codes:
        print(f"No airport code for {a.ORIGIN_CITY} — cannot build routes. "
              f"Likely rate-limited; wait a few minutes and rerun.")
        sys.exit(1)

    air_destinations = [d for d in destinations if d in airport_codes]

    pending = [
        (dest, date)
        for date in dates for dest in air_destinations
        if route_key(dest, date) not in done
    ]

    if not pending:
        print("All routes in range already done. Delete backfill_progress.json to redo.")
        return

    print(f"\nBackfilling days +{START_DAY} to +{end_day - 1} "
          f"({dates[0]} .. {dates[-1]})")
    print(f"{len(pending)} route/date combos remaining, {CONCURRENCY} at a time.")
    print(f"Rough estimate: {len(pending) * 75 / CONCURRENCY / 60:.0f} minutes.\n")

    started = datetime.now()
    grand_total = 0
    still_failing = []

    # Day by day, syncing as we go — an interrupted run keeps its work.
    for date in dates:
        routes = [(dest, date) for dest in air_destinations
                  if route_key(dest, date) not in done]
        if not routes:
            continue

        print(f"\n--- {date} ({len(routes)} routes) ---")
        schedules, timed_out = run_batch(session, airport_codes, routes, done)
        still_failing.extend(timed_out)
        grand_total += len(schedules)

        save_progress(done)
        print(f"{date}: {len(schedules)} flights, {len(timed_out)} timed out. Syncing...")
        a.sync_to_sql(schedules)

    # Retry the ones that gave up. The aggregator is inconsistent rather
    # than broken, so second and third attempts often succeed.
    for pass_num in range(RETRY_PASSES):
        if not still_failing:
            break
        print(f"\n=== Retry pass {pass_num + 1}: {len(still_failing)} route(s) ===")
        time.sleep(15)  # small breather before hammering it again
        retry_routes, still_failing = still_failing, []
        schedules, still_failing = run_batch(session, airport_codes, retry_routes, done)
        grand_total += len(schedules)
        save_progress(done)
        print(f"Recovered {len(schedules)} flights, {len(still_failing)} still failing.")
        a.sync_to_sql(schedules)

    elapsed = (datetime.now() - started).total_seconds() / 60
    print(f"\n{'=' * 60}")
    print(f"Backfill done in {elapsed:.0f} min. {grand_total} flight records added.")
    if still_failing:
        print(f"{len(still_failing)} route(s) never resolved:")
        for dest, date in sorted(still_failing):
            print(f"  {a.ORIGIN_CITY} -> {dest} ({date})")
        print("Rerun this script later to retry just those — progress is saved.")
    else:
        print("Every route resolved.")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()