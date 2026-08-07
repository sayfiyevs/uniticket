"""
Thin CLI wrapper around individual atto_scraper.py pipeline functions.

atto_scraper.py's own __main__ block starts an infinite APScheduler daemon
(cron jobs for horizon discovery, ground transport, and flights). That's
correct for a always-on server, but not something a web UI button can
sensibly trigger. This wrapper lets a single pipeline function be run once,
on demand, via a plain subprocess call, with all of its normal print()
output going to stdout as usual.

Nothing in atto_scraper.py's logic is changed by this file - it only calls
functions that already exist there.

Usage:
    python run_pipeline.py discover_horizon
    python run_pipeline.py ground_pipeline
    python run_pipeline.py flight_pipeline_near
    python run_pipeline.py flight_pipeline_full
"""

import sys

import atto_scraper as a

ALLOWED = {
    "discover_horizon": a.discover_horizon,
    "ground_pipeline": a.ground_pipeline,
    "flight_pipeline_near": a.flight_pipeline_near,
    "flight_pipeline_full": a.flight_pipeline_full,
}


def main():
    if len(sys.argv) != 2 or sys.argv[1] not in ALLOWED:
        print(f"Usage: python run_pipeline.py [{' | '.join(ALLOWED)}]")
        sys.exit(1)

    name = sys.argv[1]
    print(f"=== Running atto_scraper.{name}() ===\n")
    ALLOWED[name]()
    print(f"\n=== {name} finished ===")


if __name__ == "__main__":
    main()
