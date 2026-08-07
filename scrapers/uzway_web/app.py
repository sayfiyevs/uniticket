"""
UzWay Ops - web front end for the existing UzWay scraper/route-engine scripts.

This file does not change how atto_scraper.py or route_engine.py work - it
imports route_engine.py directly (for fast, interactive search) and runs the
other scripts (backfill_flights.py, collect_network.py, diagnose.py,
repair_progress.py, run_pipeline.py) exactly as they'd be run from a
terminal, as subprocesses, streaming their normal print() output back to the
browser instead of a console.

Two modes:

  LOCAL (default)   Everything works: search plus all the job/ops pages.
                    Run with:  python app.py
                    Open:      http://127.0.0.1:5000

  PUBLIC            Set UZWAY_PUBLIC=1. Only the search pages respond;
                    every job, settings and diagnostics route returns 404.
                    Use this for any deployment reachable from the internet
                    - the job routes launch subprocesses on the server and
                    /settings hands out the ATTO access token, neither of
                    which belongs on a public URL.
"""

import json
import os
import subprocess
import sys
import threading
import time
import uuid
from datetime import datetime

import pandas as pd
from flask import Flask, jsonify, render_template, request

import route_engine as re_engine

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
HORIZON_FILE = os.path.join(BASE_DIR, "atto_horizon.json")
PROGRESS_FILE = os.path.join(BASE_DIR, "backfill_progress.json")
SETTINGS_FILE = os.path.join(BASE_DIR, "web_settings.json")

app = Flask(__name__)

# ==========================================================
# PUBLIC MODE
# ==========================================================
# When deployed publicly this app must expose ONLY search. The job routes
# start subprocesses (collect_network.py, backfill_flights.py, ...), which
# would let any visitor run scrapers on the server, and /settings returns
# the ATTO access token. Both are fine on your own laptop and unacceptable
# on a public URL.
PUBLIC_MODE = os.environ.get("UZWAY_PUBLIC") == "1"

PUBLIC_PATHS = {"/", "/search", "/api/search", "/api/state"}


@app.before_request
def _restrict_public_surface():
    if not PUBLIC_MODE:
        return None
    path = request.path.rstrip("/") or "/"
    if path in PUBLIC_PATHS or path.startswith("/static"):
        return None
    return jsonify({"error": "Not available."}), 404


# ==========================================================
# SETTINGS (just the ATTO access token, persisted locally)
# ==========================================================
def load_settings():
    try:
        with open(SETTINGS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"access_token": ""}


def save_settings(data):
    with open(SETTINGS_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


def subprocess_env():
    """Base env for launched scripts, with the saved access token injected.
    atto_scraper.py reads ATTO_ACCESS_TOKEN via os.environ.get(...) with a
    fallback to its original hardcoded placeholder, so this is optional."""
    env = os.environ.copy()
    token = load_settings().get("access_token", "").strip()
    if token:
        env["ATTO_ACCESS_TOKEN"] = token
    return env


# ==========================================================
# JOB MANAGER - runs the existing scripts as subprocesses and
# captures their stdout/stderr live, line by line.
# ==========================================================
class Job:
    def __init__(self, job_id, label, cmd):
        self.id = job_id
        self.label = label
        self.cmd = cmd
        self.lines = []
        self.status = "running"  # running | done | failed | stopped
        self.returncode = None
        self.started_at = datetime.now().isoformat(timespec="seconds")
        self.finished_at = None
        self.proc = None
        self.lock = threading.Lock()

    def append(self, line):
        with self.lock:
            self.lines.append(line)

    def snapshot(self, since=0):
        with self.lock:
            return list(self.lines[since:]), len(self.lines)

    def to_meta(self):
        return {
            "id": self.id,
            "label": self.label,
            "status": self.status,
            "returncode": self.returncode,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
        }


JOBS = {}
JOBS_LOCK = threading.Lock()


def start_job(label, args):
    """args: list of args passed to `python -u <args...>` inside BASE_DIR."""
    job_id = uuid.uuid4().hex[:10]
    cmd = [sys.executable, "-u"] + args
    job = Job(job_id, label, cmd)
    with JOBS_LOCK:
        JOBS[job_id] = job

    def run():
        try:
            job.proc = subprocess.Popen(
                cmd,
                cwd=BASE_DIR,
                env=subprocess_env(),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
            )
            for line in job.proc.stdout:
                job.append(line.rstrip("\n"))
            job.proc.wait()
            job.returncode = job.proc.returncode
            job.status = "done" if job.returncode == 0 else "failed"
        except Exception as e:
            job.append(f"[web] Failed to launch: {e}")
            job.status = "failed"
        finally:
            job.finished_at = datetime.now().isoformat(timespec="seconds")

    threading.Thread(target=run, daemon=True).start()
    return job_id


# ==========================================================
# DASHBOARD STATE - read straight from the same JSON files the
# scripts themselves read/write. No DB or API calls needed.
# ==========================================================
def read_json(path, default):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def dashboard_state():
    horizon = read_json(HORIZON_FILE, {})
    progress = read_json(PROGRESS_FILE, {"done": []})

    age_hours = None
    if horizon.get("discovered_at"):
        try:
            discovered = datetime.fromisoformat(horizon["discovered_at"])
            age_hours = round((datetime.now() - discovered).total_seconds() / 3600, 1)
        except Exception:
            pass

    by_city = {}
    for key in progress.get("done", []):
        city = key.split("|", 1)[0]
        by_city[city] = by_city.get(city, 0) + 1
    by_city = dict(sorted(by_city.items(), key=lambda kv: -kv[1]))

    return {
        "days_ahead": horizon.get("days_ahead"),
        "horizon_discovered_at": horizon.get("discovered_at"),
        "horizon_age_hours": age_hours,
        "backfill_done": len(progress.get("done", [])),
        "backfill_updated_at": progress.get("updated_at"),
        "backfill_repaired": progress.get("repaired", False),
        "backfill_by_city": by_city,
        "cities": sorted(getattr(re_engine, "CITY_ALIASES", {}).values()),
        "public_mode": PUBLIC_MODE,
    }


# ==========================================================
# SEARCH - imports route_engine directly and calls its existing
# public functions (parse_query / find_itineraries / rank_itineraries /
# format_itineraries) rather than reformatting anything by hand.
# ==========================================================
_TRIPS_CACHE = {"df": None, "loaded_at": 0, "dropped": 0}

# The deployed copy reads a static SQLite snapshot that cannot change while
# the process runs, so re-reading it is wasted work on every search.
# Locally the scraper is actively writing, so a short TTL keeps results fresh.
TRIPS_CACHE_TTL = 10 ** 9 if PUBLIC_MODE else 20


def get_trips(force=False):
    now = time.time()
    if force or _TRIPS_CACHE["df"] is None or (now - _TRIPS_CACHE["loaded_at"]) > TRIPS_CACHE_TTL:
        df = re_engine.load_trips()
        _TRIPS_CACHE["df"] = df
        _TRIPS_CACHE["dropped"] = df.attrs.get("dropped", 0)
        _TRIPS_CACHE["loaded_at"] = now
    return _TRIPS_CACHE["df"], _TRIPS_CACHE["dropped"]


def jsafe(v):
    """JSON-safe conversion for whatever pandas/numpy scalar comes out of a
    leg dict (NaN, numpy int64/float64, Timestamps, etc.)."""
    if v is None:
        return None
    try:
        if pd.isna(v):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(v, (pd.Timestamp,)):
        return str(v)
    if hasattr(v, "item"):  # numpy scalar
        return v.item()
    return v


def serialize_leg(leg, party_size):
    price = leg.get("price")
    leg_total = (price * party_size) if pd.notna(price) else None
    return {
        "mode": leg.get("mode"),
        "mode_label": re_engine.MODE_LABELS.get(leg.get("mode"), leg.get("mode")),
        "origin_city": leg.get("origin_city"),
        "destination_city": leg.get("destination_city"),
        "departure_time": jsafe(leg.get("departure_time")),
        "arrival_time": jsafe(leg.get("arrival_time")),
        "travel_date": leg.get("travel_date"),
        "transport_name": leg.get("transport_name"),
        "duration_label": re_engine.fmt_duration(leg.get("duration")),
        "price_per_person": jsafe(price),
        "price_total": jsafe(leg_total),
        "price_label": re_engine.fmt_price(leg_total),
        "free_seats": jsafe(leg.get("free_seats")),
        "baggage": re_engine.baggage_note(leg),
        "estimated": bool(leg.get("estimated")),
    }


def serialize_itinerary(it, party_size, idx):
    legs = []
    for n, leg in enumerate(it):
        entry = serialize_leg(leg, party_size)
        if n < len(it) - 1:
            wait = it[n + 1]["t_start"] - leg["t_end"]
            entry["wait_after_label"] = re_engine.fmt_duration(wait)
        legs.append(entry)

    per_person = re_engine.itinerary_price(it)
    total = re_engine.itinerary_price(it, party_size)
    return {
        "rank": idx,
        "changes": len(it) - 1,
        "legs": legs,
        "duration_label": re_engine.fmt_duration(re_engine.itinerary_duration(it)),
        "price_per_person_label": re_engine.fmt_price(per_person),
        "price_total_label": re_engine.fmt_price(total),
        "has_estimated": any(leg.get("estimated") for leg in it),
    }


@app.route("/")
def dashboard():
    # Publicly there is no ops dashboard to show, so land on search instead.
    if PUBLIC_MODE:
        return render_template("search.html", state=dashboard_state())
    return render_template("dashboard.html", state=dashboard_state())


@app.route("/search")
def search_page():
    return render_template("search.html", state=dashboard_state())


@app.route("/api/search", methods=["POST"])
def api_search():
    payload = request.get_json(force=True) or {}
    question = (payload.get("question") or "").strip()
    max_legs = int(payload.get("max_legs") or 2)

    if not question:
        return jsonify({"error": "Type where you want to go."}), 400

    try:
        # A public visitor must not be able to force a database reload on
        # every request; the snapshot can't change anyway.
        force = bool(payload.get("refresh")) and not PUBLIC_MODE
        df, dropped = get_trips(force=force)
    except Exception as e:
        return jsonify({
            "error": "Couldn't reach the schedules database.",
            "detail": str(e),
        }), 502

    q = re_engine.parse_query(question)
    if not q["destination"]:
        return jsonify({
            "error": 'Couldn\'t identify a destination. Try something like '
                     '"Tashkent to Bukhara, cheapest".'
        }), 200
    if not q["origin"]:
        q["origin"] = "Tashkent"
    if q["origin"] == q["destination"]:
        return jsonify({"error": f"Origin and destination are both {q['origin']}."}), 200

    itineraries, notes = re_engine.find_itineraries(
        df, q["origin"], q["destination"],
        date=q["date"], baggage_kg=q["baggage_kg"],
        modes=q["modes"], party_size=q["party_size"],
        max_legs=max_legs, limit=24,
    )
    ranked = re_engine.rank_itineraries(itineraries, q["preference"])[:8]
    console_text = re_engine.format_itineraries(ranked, q, notes)

    return jsonify({
        "query": q,
        "notes": notes,
        "itineraries": [serialize_itinerary(it, q["party_size"], i + 1)
                         for i, it in enumerate(ranked)],
        "console_text": console_text,
        "legs_loaded": int(len(df)),
        "legs_dropped": int(dropped),
    })


# ==========================================================
# JOB PAGES (each wraps one existing script)
#
# All of these are blocked by _restrict_public_surface when
# UZWAY_PUBLIC=1 is set.
# ==========================================================
@app.route("/backfill")
def backfill_page():
    return render_template("job_page.html",
                            title="Backfill Flights",
                            subtitle="Runs backfill_flights.py - fills in flight data for day 3 "
                                     "onward across the discovered horizon. Resumable; progress "
                                     "is saved to backfill_progress.json.",
                            job_kind="backfill", show_network_form=False, state=dashboard_state())


@app.route("/network")
def network_page():
    return render_template("job_page.html",
                            title="Collect Network",
                            subtitle="Runs collect_network.py - collects ground transport for "
                                     "every city pair (not just from Tashkent), so connecting "
                                     "journeys can be found.",
                            job_kind="network", show_network_form=True, state=dashboard_state())


@app.route("/pipelines")
def pipelines_page():
    return render_template("pipelines.html", state=dashboard_state())


@app.route("/diagnose")
def diagnose_page():
    return render_template("job_page.html",
                            title="Diagnose",
                            subtitle="Runs diagnose.py - a read-only check of what's actually in "
                                     "SQL vs. what the progress file claims. Changes nothing.",
                            job_kind="diagnose", show_network_form=False, state=dashboard_state())


@app.route("/repair")
def repair_page():
    return render_template("job_page.html",
                            title="Repair Progress",
                            subtitle="Runs repair_progress.py - recovers missing travel_date "
                                     "values from trip_id, and rebuilds backfill_progress.json "
                                     "from what SQL can actually prove is done.",
                            job_kind="repair", show_network_form=False, state=dashboard_state())


@app.route("/settings")
def settings_page():
    return render_template("settings.html", settings=load_settings(), state=dashboard_state())


@app.route("/api/settings", methods=["POST"])
def api_save_settings():
    payload = request.get_json(force=True) or {}
    save_settings({"access_token": (payload.get("access_token") or "").strip()})
    return jsonify({"ok": True})


# ==========================================================
# JOB API - generic start/poll/stop, used by every job page
# ==========================================================
@app.route("/api/jobs/start", methods=["POST"])
def api_start_job():
    payload = request.get_json(force=True) or {}
    kind = payload.get("kind")

    if kind == "backfill":
        job_id = start_job("Backfill flights", ["backfill_flights.py"])
    elif kind == "diagnose":
        job_id = start_job("Diagnose", ["diagnose.py"])
    elif kind == "repair":
        job_id = start_job("Repair progress", ["repair_progress.py"])
    elif kind == "network":
        args = ["collect_network.py"]
        days = payload.get("days")
        cities = payload.get("cities")
        hubs = payload.get("hubs")
        if days:
            args += ["--days", str(int(days))]
        if cities:
            args += ["--cities", cities]
        if hubs:
            args += ["--hub", hubs]
        job_id = start_job("Collect network", args)
    elif kind == "pipeline":
        name = payload.get("pipeline")
        allowed = {"discover_horizon", "ground_pipeline",
                   "flight_pipeline_near", "flight_pipeline_full"}
        if name not in allowed:
            return jsonify({"error": "Unknown pipeline."}), 400
        job_id = start_job(f"Pipeline: {name}", ["run_pipeline.py", name])
    else:
        return jsonify({"error": "Unknown job kind."}), 400

    return jsonify({"job_id": job_id})


@app.route("/api/jobs/<job_id>")
def api_job_status(job_id):
    job = JOBS.get(job_id)
    if not job:
        return jsonify({"error": "Unknown job id."}), 404
    since = int(request.args.get("since", 0))
    lines, total = job.snapshot(since)
    meta = job.to_meta()
    meta["lines"] = lines
    meta["total_lines"] = total
    return jsonify(meta)


@app.route("/api/jobs/<job_id>/stop", methods=["POST"])
def api_stop_job(job_id):
    job = JOBS.get(job_id)
    if not job:
        return jsonify({"error": "Unknown job id."}), 404
    if job.proc and job.status == "running":
        job.proc.terminate()
        job.status = "stopped"
        job.append("[web] Stopped by user.")
        job.finished_at = datetime.now().isoformat(timespec="seconds")
    return jsonify({"ok": True})


@app.route("/api/jobs")
def api_list_jobs():
    with JOBS_LOCK:
        jobs = sorted(JOBS.values(), key=lambda j: j.started_at, reverse=True)
    return jsonify([j.to_meta() for j in jobs[:20]])


@app.route("/api/state")
def api_state():
    return jsonify(dashboard_state())


if __name__ == "__main__":
    # debug=True enables the Werkzeug console, which executes arbitrary code
    # on an unhandled exception. Fine locally, catastrophic in public.
    #
    # UZWAY_HOST=0.0.0.0 makes the app reachable from other devices on your
    # network (phone on the same Wi-Fi or hotspot) without a tunnel.
    app.run(
        host=os.environ.get("UZWAY_HOST", "127.0.0.1"),
        port=int(os.environ.get("PORT", 5000)),
        debug=not PUBLIC_MODE,
        threaded=True,
    )
