# UzWay Ops — web interface

A small Flask site that wraps your existing scripts so everything they print
to a terminal shows up on a web page instead, with forms for the inputs each
one needs.

## Setup

Requires **ODBC Driver 17 for SQL Server** installed on this machine (same
one this already ran on, since `atto_scraper.py` and `route_engine.py` both
connect to `DESKTOP-5K5RC72\UzWayDB` via Windows integrated auth).

```bash
pip install -r requirements.txt
python app.py
```

Then open **http://127.0.0.1:5000**.

## What changed vs. your original files

Two small, additive changes — nothing about scraping, parsing, ranking, or
the DB upsert logic was touched:

1. **`atto_scraper.py`, one line** — the hardcoded
   `"access_token": "REPLACE_WITH_TOKEN_FROM_DEVTOOLS"` now reads
   `os.environ.get("ATTO_ACCESS_TOKEN", "REPLACE_WITH_TOKEN_FROM_DEVTOOLS")`.
   If that env var isn't set, behavior is identical to before. The web
   Settings page sets it, so you can paste a fresh token from DevTools
   without editing the file each time.
2. **`run_pipeline.py`, new file** — a 20-line wrapper so a single button
   can call `atto_scraper.discover_horizon()`, `ground_pipeline()`,
   `flight_pipeline_near()`, or `flight_pipeline_full()` once, on demand.
   Your file's own `__main__` block (the `BlockingScheduler` daemon) is
   untouched and still works exactly as before if you run
   `python atto_scraper.py` directly — the web UI just doesn't invoke it,
   since a forever-running cron daemon isn't something a page button should
   start.

Everything else (`backfill_flights.py`, `collect_network.py`, `diagnose.py`,
`repair_progress.py`, `route_engine.py`) is byte-for-byte what you uploaded,
just renamed `route_engine__2_.py` → `route_engine.py` so the imports in the
other scripts resolve.

## How the pages map to your scripts

| Page | What it runs |
|---|---|
| **Search** | Imports `route_engine.py` directly and calls its own `parse_query` / `find_itineraries` / `rank_itineraries` / `format_itineraries` — same functions the CLI uses. Results are shown as route cards, and the exact console-style text is available in a collapsible panel underneath. |
| **Pipelines** | Runs `run_pipeline.py <name>`, i.e. one call to `discover_horizon()`, `ground_pipeline()`, `flight_pipeline_near()`, or `flight_pipeline_full()` from `atto_scraper.py`. |
| **Backfill Flights** | Runs `backfill_flights.py` as-is (same `START_DAY`/`END_DAY`/`CONCURRENCY` constants at the top of that file control it — edit those if you want different settings, same as before). |
| **Collect Network** | Runs `collect_network.py`, passing through `--days` / `--cities` / `--hub` from the form exactly as the script's own `parse_args()` expects. |
| **Diagnose** | Runs `diagnose.py` (read-only). |
| **Repair Progress** | Runs `repair_progress.py`. |
| **Dashboard** | Reads `atto_horizon.json` and `backfill_progress.json` directly — no DB or API calls, just the same files the scripts already maintain. |

## How output gets to the browser

The maintenance scripts (backfill, network, diagnose, repair, pipelines) are
launched as real subprocesses — `python -u <script>` — exactly as if you'd
typed that in a terminal. Their stdout is streamed line-by-line into an
in-memory buffer per job; the page polls for new lines about once a second
and appends them to a console-style panel. Long jobs (backfill can run for
hours) keep working in the background even if you navigate away — reopening
the page won't reattach to it, but the process itself isn't killed unless
you hit Stop.

Search is different: since it's meant to feel interactive, `route_engine.py`
is imported directly rather than run as a subprocess, and results are loaded
from SQL and cached for 20 seconds so repeated searches don't re-hit the
database every time.

## Known limitations / things to check on your end

- **I couldn't test this against your real database or the live ATTO API.**
  This sandbox can't reach `tickets.atto.uz` or a SQL Server instance, so
  I verified everything I could without them: every page renders, the job
  runner correctly streams output and reports failures (tested against a
  throwaway script and against `diagnose.py` failing with an expected
  "driver not found" error here), and the search pipeline was verified
  end-to-end against a small synthetic dataset shaped like real scraped
  rows. The actual scrape/search results depend on your DB and a live
  token, which I have no way to reach from here.
- This is built for **local use on one machine** — no auth, no multi-user
  job isolation. Fine for a hackathon demo; not meant to be exposed on the
  open internet.
- Job history isn't persisted — restarting `app.py` forgets any jobs that
  were running.
