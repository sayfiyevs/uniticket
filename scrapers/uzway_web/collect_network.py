"""
Collects the full ground-transport network: every city pair in both
directions, not just Tashkent -> X.

This is what makes connecting journeys possible. Without legs like
Bukhara -> Karshi in the database, a route search can never find
"Nukus -> Bukhara -> Karshi" — it can only ever suggest direct trips.

Cost: 16 cities = 240 ordered pairs. At 25 days x 2 modes that's ~12,000
requests, about 20 minutes at concurrency 4. Ground requests are single
and fast, unlike flights.

Usage:
    python collect_network.py                  full network, full horizon
    python collect_network.py --days 7         first 7 days only (faster)
    python collect_network.py --cities Tashkent,Samarkand,Bukhara,Karshi,Navoi
    python collect_network.py --hub Tashkent,Samarkand,Bukhara

--hub is the cheap option: only collects pairs where at least one end is
a hub city. Most real connections route through a handful of hubs anyway,
and it cuts the request count by more than half.
"""

import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import atto_scraper as a

MAX_CONCURRENT = 4

# Sync in batches rather than at the very end, so an interrupted run
# still leaves usable data behind.
SYNC_EVERY = 400


def parse_args():
    argv = sys.argv[1:]
    opts = {"days": None, "cities": None, "hubs": None}

    def value_after(flag):
        if flag in argv:
            try:
                return argv[argv.index(flag) + 1]
            except IndexError:
                print(f"{flag} needs a value")
                sys.exit(1)
        return None

    d = value_after("--days")
    if d:
        opts["days"] = int(d)

    c = value_after("--cities")
    if c:
        opts["cities"] = [x.strip() for x in c.split(",") if x.strip()]

    h = value_after("--hub")
    if h:
        opts["hubs"] = [x.strip() for x in h.split(",") if x.strip()]

    return opts


def build_pairs(cities, hubs=None):
    """Every ordered pair, optionally restricted to hub-touching ones."""
    pairs = [(o, d) for o in cities for d in cities if o != d]
    if hubs:
        pairs = [(o, d) for o, d in pairs if o in hubs or d in hubs]
    return pairs


def collect(session, pairs, dates):
    routes = [(o, d, date) for date in dates for o, d in pairs]
    total = len(routes) * 2

    print(f"\nCollecting {total} requests "
          f"({len(pairs)} pair(s) x {len(dates)} day(s) x 2 modes), "
          f"{MAX_CONCURRENT} at a time.")
    print(f"Rough estimate: {total * 0.12 / 60:.0f}-{total * 0.25 / 60:.0f} minutes.\n")

    collected, failed, done = [], 0, 0
    batch = []
    started = datetime.now()

    with ThreadPoolExecutor(max_workers=MAX_CONCURRENT) as ex:
        futures = {}
        for origin, dest, date in routes:
            futures[ex.submit(a.fetch_train_schedules, session, origin, dest, date)] = \
                ("train", origin, dest, date)
            futures[ex.submit(a.fetch_bus_schedules, session, origin, dest, date)] = \
                ("bus", origin, dest, date)

        for future in as_completed(futures):
            mode, origin, dest, date = futures[future]
            done += 1
            try:
                result = future.result()
                batch.extend(result)
                collected.extend(result)
            except Exception as e:
                failed += 1
                if failed <= 10:      # don't drown the log in identical errors
                    print(f"  {mode} {origin} -> {dest} ({date}): {e}")
                elif failed == 11:
                    print("  (further errors suppressed)")

            if len(batch) >= SYNC_EVERY:
                print(f"\n  ... {done}/{total} requests, {len(collected)} records. Syncing batch...")
                a.sync_to_sql(batch)
                batch = []

            if done % 500 == 0:
                elapsed = (datetime.now() - started).total_seconds() / 60
                rate = done / elapsed if elapsed else 0
                remaining = (total - done) / rate if rate else 0
                print(f"  [{done}/{total}] {len(collected)} records, "
                      f"{elapsed:.1f} min elapsed, ~{remaining:.0f} min left")

    if batch:
        print("\nSyncing final batch...")
        a.sync_to_sql(batch)

    return collected, failed, total


def summarise(collected, pairs):
    """Which pairs actually have service — the useful output of this run."""
    by_pair = {}
    for s in collected:
        key = (s['origin_city'], s['destination_city'])
        by_pair.setdefault(key, {})
        by_pair[key][s['mode']] = by_pair[key].get(s['mode'], 0) + 1

    connected = sorted(by_pair)
    empty = [p for p in pairs if p not in by_pair]

    print(f"\n{len(connected)} pair(s) have service, {len(empty)} have none.\n")
    print("Routes with service:")
    for o, d in connected:
        detail = ", ".join(f"{v} {k.lower()}" for k, v in sorted(by_pair[(o, d)].items()))
        print(f"  {o} -> {d}: {detail}")

    if empty:
        print(f"\nNo service found ({len(empty)}):")
        preview = ", ".join(f"{o}->{d}" for o, d in empty[:25])
        print(f"  {preview}" + (f" ... and {len(empty) - 25} more" if len(empty) > 25 else ""))
        print("\n  These are mostly genuine — few operators run city-to-city")
        print("  services that bypass the regional hubs.")


def main():
    opts = parse_args()

    cities = opts["cities"] or list(a.STATIONS)
    unknown = [c for c in cities if c not in a.STATIONS]
    if unknown:
        print(f"Unknown cities: {', '.join(unknown)}")
        print(f"Known: {', '.join(sorted(a.STATIONS))}")
        sys.exit(1)

    hubs = opts["hubs"]
    if hubs:
        bad = [h for h in hubs if h not in cities]
        if bad:
            print(f"Hub not in city list: {', '.join(bad)}")
            sys.exit(1)

    pairs = build_pairs(cities, hubs)
    days = opts["days"] or a.load_horizon()
    dates = a.get_upcoming_dates(days)

    print("=" * 62)
    print("Network collection — all pairs, both directions")
    print("=" * 62)
    print(f"Cities:  {len(cities)}")
    print(f"Pairs:   {len(pairs)}" + (f" (hub-restricted: {', '.join(hubs)})" if hubs else ""))
    print(f"Dates:   {dates[0]} .. {dates[-1]} ({len(dates)} day(s))")

    started = datetime.now()
    session = a.build_session()
    collected, failed, total = collect(session, pairs, dates)
    elapsed = (datetime.now() - started).total_seconds() / 60

    print("\n" + "=" * 62)
    print(f"Done in {elapsed:.1f} min. {len(collected)} record(s), "
          f"{failed}/{total} request(s) failed.")
    print("=" * 62)

    summarise(collected, pairs)


if __name__ == "__main__":
    main()
