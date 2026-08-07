import json
import os
import requests
import pandas as pd
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from sqlalchemy import create_engine, text
from apscheduler.schedulers.blocking import BlockingScheduler
from datetime import datetime, timedelta

# ==========================================
# 1. CONFIGURATION & DATABASE SETUP
# ==========================================
DB_CONNECTION_STRING = (
    "mssql+pyodbc://@DESKTOP-5K5RC72/UzWayDB?"
    "driver=ODBC+Driver+17+for+SQL+Server&"
    "Trusted_Connection=yes&"
    "TrustServerCertificate=yes"
)

engine = create_engine(DB_CONNECTION_STRING)

# Exact API Endpoints
RAILWAY_ENDPOINT = "https://tickets.atto.uz/v1.0/customer/eticket/railways/search/trains"
BUS_ENDPOINT = "https://tickets.atto.uz/v1.0/customer/avtoticket/trips/list"
AIRPORTS_ENDPOINT = "https://tickets.atto.uz/v1.0/customer/avia/airports"
FLIGHT_SEARCH_ENDPOINT = "https://tickets.atto.uz/v1.0/customer/avia/flights/search-new"
FLIGHT_OFFERS_ENDPOINT = "https://tickets.atto.uz/v1.0/customer/avia/flights/offers"

# Station ID mapping (used for TRAINS only — bus API takes city names directly)
STATIONS = {
    "Tashkent": "4201",
    "Samarkand": "4177",
    "Bukhara": "4366",
    "Khiva": "4358",
    "Urgench": "4160",
    "Nukus": "4194",
    "Navoi": "4185",
    "Andijan": "4367",
    "Karshi": "4188",
    "Djizakh": "4196",
    "Termez": "4174",
    "Gulistan": "4202",
    "Kokand": "4360",
    "Margilan": "4170",
    "Pap": "4168",
    "Namangan": "4179"
}

# The bus API spells some city names differently than the train API
# (confirmed: "Navoi" -> "Navai" in bus responses). Add overrides here as
# you discover more mismatches — if a bus route comes back suspiciously
# empty for a city that should have buses, check this first.
BUS_NAME_OVERRIDES = {
    "Navoi": "Navai",
}

def bus_name_for(city):
    return BUS_NAME_OVERRIDES.get(city, city)

# Origin city for all routes.
ORIGIN_CITY = "Tashkent"

# ---- Booking-horizon discovery settings -------------------------------
# DAYS_AHEAD is no longer a fixed constant. Instead, discover_horizon()
# walks forward day by day until it hits EMPTY_STREAK_LIMIT consecutive
# days with zero ground records, and saves the result to HORIZON_FILE.
# The frequent pipelines just read that saved number.

# Used before the first discovery run, and as a floor we never go below.
DEFAULT_DAYS_AHEAD = 2

# Hard ceiling on discovery, so a bug or an always-populated API can't
# send us walking forward forever.
MAX_HORIZON_DAYS = 45

# How many consecutive empty days end the scan.
EMPTY_STREAK_LIMIT = 3

# Flights are 60-90s+ PER ROUTE, so the full horizon takes hours. Rather
# than truncating coverage, we split it: a near-term sweep several times a
# day (prices move most close to departure) and a full-horizon sweep once
# overnight, so the ML training set eventually covers every bookable day.
FLIGHT_NEAR_DAYS = 4

# Parallel flight searches. ATTO's aggregator is flaky under load — at 2
# workers roughly a quarter of routes time out. Raising this cuts wall
# time but tends to raise the timeout rate, so it's a trade, not free
# speed. 3 is a reasonable middle; drop back to 2 if failures spike.
FLIGHT_CONCURRENCY = 3

# Routes that time out get retried at the end of the run. The aggregator
# is inconsistent rather than broken, so a second attempt usually works.
FLIGHT_RETRY_PASSES = 1

# Where the discovered horizon is persisted, so a restart doesn't
# silently drop us back to DEFAULT_DAYS_AHEAD until the next daily scan.
HORIZON_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "atto_horizon.json")

# If more than this share of a day's requests error out, we treat that day
# as inconclusive rather than "empty" — otherwise a network blip could
# permanently shrink the horizon.
MAX_ERROR_RATE = 0.5

# How many routes to hit at once. Keep this modest (3-5) to stay polite
# to their backend rather than firing all 15 requests simultaneously.
MAX_CONCURRENT_REQUESTS = 4

# ==========================================
# 2. HELPER FUNCTIONS
# ==========================================
def format_date_for_atto(date_str):
    """Converts 'YYYY-MM-DD' string into ATTO's required 'DD.MM.YYYY' format."""
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    return dt.strftime("%d.%m.%Y")

def get_upcoming_dates(n_days):
    """Returns a list of 'YYYY-MM-DD' strings starting today, for n_days total."""
    today = datetime.now()
    return [(today + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(n_days)]

def date_offset(days):
    """'YYYY-MM-DD' string for today + days."""
    return (datetime.now() + timedelta(days=days)).strftime("%Y-%m-%d")

# ---- Horizon persistence ----------------------------------------------
def load_horizon():
    """Reads the last discovered horizon from disk. Falls back to the
    default if the file is missing or unreadable."""
    try:
        with open(HORIZON_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        days = int(data.get("days_ahead", DEFAULT_DAYS_AHEAD))
        return max(DEFAULT_DAYS_AHEAD, min(days, MAX_HORIZON_DAYS))
    except Exception:
        return DEFAULT_DAYS_AHEAD

def save_horizon(days_ahead):
    """Persists the discovered horizon so restarts keep the wider range."""
    payload = {
        "days_ahead": int(days_ahead),
        "discovered_at": datetime.now().isoformat(timespec="seconds"),
    }
    try:
        with open(HORIZON_FILE, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)
        print(f"[horizon] Saved days_ahead={days_ahead} to {HORIZON_FILE}")
    except Exception as e:
        print(f"[horizon] Could not write {HORIZON_FILE}: {e}")

def horizon_age_hours():
    """Hours since the last successful discovery, or None if never run."""
    try:
        with open(HORIZON_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        discovered = datetime.fromisoformat(data["discovered_at"])
        return (datetime.now() - discovered).total_seconds() / 3600
    except Exception:
        return None

def get_airport_codes(session, cities, _retry=True):
    """
    Looks up the IATA airport code for each city by calling the airports
    search endpoint once per city and filtering to the 'Uzbekistan' country
    group. Not every city has an airport — those are simply left out of the
    returned dict, and flight routes involving them are skipped later.

    Failures are logged rather than swallowed: a silent empty result here
    is indistinguishable from "this city has no airport", which cost us a
    whole flight run once already.
    """
    codes = {}
    for city in cities:
        try:
            resp = session.get(AIRPORTS_ENDPOINT, params={"search": city}, timeout=15)
            if resp.status_code != 200:
                print(f"  [airports] {city}: HTTP {resp.status_code} — {resp.text[:200]}")
                continue
            data = resp.json()
            groups = data.get('data', {}).get('airports', []) if isinstance(data, dict) else []
            if not groups:
                print(f"  [airports] {city}: 200 OK but no airport groups returned.")
                continue

            uz_airports = []
            for group in groups:
                if group.get('countryName') == 'Uzbekistan':
                    uz_airports = group.get('airports') or []
                    break

            if not uz_airports:
                continue  # this city likely has no airport

            # Prefer an entry whose region/airport name actually mentions the
            # city (search is fuzzy and can return other Uzbek airports too).
            match = next(
                (ap for ap in uz_airports
                 if city.lower() in (ap.get('regionName') or '').lower()
                 or city.lower() in (ap.get('airportName') or '').lower()),
                uz_airports[0]
            )
            codes[city] = match.get('airportCode')

        except Exception as e:
            print(f"  [airports] lookup failed for {city}: {e}")

    # Zero airports across every city means the endpoint refused us, not
    # that Uzbekistan has no airports. Usually a soft rate-limit right
    # after the ground scan hammers the API. Back off and try once more.
    if not codes and _retry:
        print("[airports] Nothing found for ANY city — likely rate-limited. "
              "Waiting 30s and retrying with a fresh session...")
        time.sleep(30)
        return get_airport_codes(build_session(), cities, _retry=False)

    return codes

# ==========================================
# 3. CORE SCRAPER FUNCTIONS
# ==========================================
def fetch_train_schedules(session, from_name, to_name, travel_date_str):
    from_id = STATIONS.get(from_name)
    to_id = STATIONS.get(to_name)

    if not from_id or not to_id:
        print(f"Missing station ID mapping for {from_name} or {to_name}")
        return []

    formatted_date = format_date_for_atto(travel_date_str)
    current_ms = int(time.time() * 1000)

    print(f"[{datetime.now().strftime('%H:%M:%S')}] Fetching train from {from_name} -> {to_name} ({formatted_date})...")

    params = {
        "stationFrom": from_id,
        "stationTo": to_id,
        "fromName": from_name,
        "toName": to_name,
        "depDate": formatted_date,
        "_updTime": current_ms,
        "transportType": "train"
    }

    try:
        try:
            response = session.get(RAILWAY_ENDPOINT, params=params, timeout=20)
        except requests.exceptions.Timeout:
            print(" -> Timed out, retrying once...")
            response = session.get(RAILWAY_ENDPOINT, params=params, timeout=20)

        if response.status_code != 200:
            print(f"-> Server Error {response.status_code}: {response.text}")
            return []

        data = response.json()

        # Real shape confirmed via DevTools: {"data": {"items": [...], "success": true}, "success": true}
        items = data.get('data', {}).get('items', []) if isinstance(data, dict) else (data if isinstance(data, list) else [])

        schedules = []
        for item in items:
            trip_number = item.get('number') or 'N/A'
            departure = item.get('departure', {}) or {}
            arrival = item.get('arrival', {}) or {}

            # Grab cheapest/first seat class info if present (optional, extra detail)
            cars = item.get('cars') or []
            first_car = cars[0] if cars else {}

            schedules.append({
                "trip_id": f"ATTO-TRAIN-{trip_number}-{from_id}-{to_id}-{formatted_date}",
                "mode": "TRAIN",
                "origin_city": from_name,
                "destination_city": to_name,
                "travel_date": travel_date_str,
                "departure_time": departure.get('localTime'),
                "arrival_time": arrival.get('localTime'),
                "transport_name": item.get('brand') or item.get('type') or "ATTO Train",
                "free_seats": first_car.get('freeSeats'),
                "price": first_car.get('amount'),
                # Flight-only fields — kept as None so every mode produces
                # the same column set and the MERGE statement stays valid.
                "duration_minutes": None,
                "stops": None,
                "cabin": None,
                "service_class": first_car.get('type') if first_car else None,
                "baggage": None,
                "hand_baggage": None,
                "last_scraped_at": datetime.now()
            })

        print(f" -> Found {len(schedules)} trains.")
        return schedules

    except Exception as e:
        print(f"Error fetching train schedules: {e}")
        raise


def fetch_bus_schedules(session, from_name, to_name, travel_date_str):
    # Bus API takes plain English city names + YYYY-MM-DD date, no station IDs.
    from_name_bus = bus_name_for(from_name)
    to_name_bus = bus_name_for(to_name)

    print(f"[{datetime.now().strftime('%H:%M:%S')}] Fetching bus from {from_name} -> {to_name} ({travel_date_str})...")

    params = {
        "fromNameEn": from_name_bus,
        "toNameEn": to_name_bus,
        "date": travel_date_str
    }

    try:
        try:
            response = session.get(BUS_ENDPOINT, params=params, timeout=20)
        except requests.exceptions.Timeout:
            print(" -> Timed out, retrying once...")
            response = session.get(BUS_ENDPOINT, params=params, timeout=20)

        if response.status_code != 200:
            print(f"-> Server Error {response.status_code}: {response.text}")
            return []

        data = response.json()

        # Real shape confirmed via DevTools:
        # {"data": {"trips": [...], "success": true}, "success": true}
        trips = data.get('data', {}).get('trips', []) if isinstance(data, dict) else (data if isinstance(data, list) else [])

        schedules = []
        for trip in trips:
            trip_id = trip.get('trip_id') or 'N/A'
            etd = trip.get('etd')  # full "YYYY-MM-DD HH:MM:SS"
            eta = trip.get('eta')

            schedules.append({
                "trip_id": f"ATTO-BUS-{trip_id}",
                "mode": "BUS",
                "origin_city": from_name,
                "destination_city": to_name,
                "travel_date": travel_date_str,
                "departure_time": etd.split(' ')[1] if etd else None,
                "arrival_time": eta.split(' ')[1] if eta else None,
                "transport_name": trip.get('transporter_name') or trip.get('bus_model_name') or "ATTO Bus",
                "free_seats": (trip.get('seats_count') or 0) - (trip.get('sold_seats_count') or 0) - (trip.get('reserved_seats_count') or 0),
                "price": trip.get('price'),
                # Flight-only fields — see note in fetch_train_schedules.
                "duration_minutes": None,
                "stops": None,
                "cabin": None,
                "service_class": None,
                "baggage": None,
                "hand_baggage": None,
                "last_scraped_at": datetime.now()
            })

        print(f" -> Found {len(schedules)} buses.")
        return schedules

    except Exception as e:
        print(f"Error fetching bus schedules: {e}")
        raise

def fetch_flight_schedules(session, from_code, to_code, from_name, to_name, travel_date_str):
    """Returns (schedules, timed_out).

    timed_out matters: an empty list because the aggregator never finished
    is completely different from an empty list because the route has no
    flights, and only the first is worth retrying."""
    # Flights are async: kick off a search (get a requestId), then poll the
    # offers endpoint until the server reports status "finished".
    formatted_date = format_date_for_atto(travel_date_str)

    print(f"[{datetime.now().strftime('%H:%M:%S')}] Fetching flight from {from_name} -> {to_name} ({formatted_date})...")

    try:
        search_resp = session.get(
            FLIGHT_SEARCH_ENDPOINT,
            params={"date": formatted_date, "fromStation": from_code, "toStation": to_code, "ADT": 1},
            timeout=20
        )
        if search_resp.status_code != 200:
            print(f"-> Server Error {search_resp.status_code}: {search_resp.text}")
            return [], True

        request_id = search_resp.json().get('data', {}).get('requestId')
        if not request_id:
            print(" -> No requestId returned, skipping.")
            return [], True

        # Poll for results. The real frontend polls repeatedly (seen as
        # 304s) until status flips to "finished" — mirror that with a
        # capped number of attempts so a stuck request can't hang forever.
        # CONFIRMED via manual testing: this can genuinely take ~60-70
        # seconds to resolve (real-time airline aggregation on their end,
        # not throttling) — budget generously rather than giving up early.
        # Poll for results. IMPORTANT: the server returns usable offers
        # while status is still "pending" — it streams them in as vendors
        # respond. Earlier versions only read offers once status flipped to
        # "finished", which threw away complete result sets on every route
        # that ran out of polling budget. Now we keep the best snapshot we
        # have seen and use it even if "finished" never arrives.
        offers = []
        last_status = None
        timed_out = False
        for attempt in range(45):
            time.sleep(2)
            try:
                poll_resp = session.get(
                    FLIGHT_OFFERS_ENDPOINT,
                    params={"requestId": request_id, "offset": 0, "limit": 50},
                    timeout=20
                )
            except requests.exceptions.Timeout:
                continue

            if poll_resp.status_code != 200:
                continue

            poll_data = poll_resp.json().get('data', {})
            last_status = poll_data.get('status')
            current = poll_data.get('offers') or []

            if last_status == 'finished':
                offers = current or offers
                break

            # Still pending: hang on to the fullest snapshot so far.
            if len(current) > len(offers):
                offers = current
        else:
            if offers:
                print(f" -> Never reached 'finished' (last status: {last_status}), "
                      f"but keeping {len(offers)} offer(s) collected while pending.")
            else:
                print(f" -> Gave up waiting, no offers at all (last status: {last_status}).")
                timed_out = True

        # Build one row PER FARE, not per flight. The same physical flight
        # comes back several times with different baggage allowances and
        # very different prices (288k vs 587k on the same Centrum Air 029),
        # so a trip_id keyed only on flight number collapses them and the
        # dedupe keeps an arbitrary one.
        #
        # The offer-level "flight_number" is a vendor fare UUID which looks
        # unique but may be regenerated per search — using it directly would
        # insert fresh rows every run instead of updating. So we build a
        # deterministic key from the fare's actual characteristics, with a
        # price-rank tiebreaker for fares that differ only in ways this
        # response doesn't expose (refundability, vendor).
        raw_entries = []
        for offer in offers:
            flights_info = offer.get('flights_info') or []
            first_leg = flights_info[0] if flights_info else {}

            airline_code = (first_leg.get('operating_airline_code') or '').strip()
            raw_num = str(first_leg.get('flight_number') or '').strip()
            # Same flight appears as both "C6029" and "029" across fares.
            if airline_code and raw_num.upper().startswith(airline_code.upper()):
                raw_num = raw_num[len(airline_code):]
            flight_num = raw_num.lstrip('0') or '0'

            bags = offer.get('baggages_info') or {}

            def _bag(d):
                d = d or {}
                return f"{d.get('value', 0)}{d.get('unit', '')}"

            checked = _bag(bags.get('bage'))
            carry_on = _bag(bags.get('hand_baggage'))
            bag_sig = f"{checked}+{carry_on}"

            cabin = offer.get('cabin') or ''
            dep_date = offer.get('departure_date', formatted_date)

            raw_entries.append({
                "group": (airline_code, flight_num, dep_date, cabin, bag_sig),
                "offer": offer,
                "first_leg": first_leg,
                "airline_code": airline_code,
                "flight_num": flight_num,
                "dep_date": dep_date,
                "cabin": cabin,
                "bag_sig": bag_sig,
                "checked": checked,
                "carry_on": carry_on,
                "price": offer.get('price') or 0,
            })

        # Rank within each group by price so identical-looking fares get a
        # stable, repeatable suffix instead of overwriting each other.
        raw_entries.sort(key=lambda e: (e["group"], e["price"]))
        rank_counter = {}

        schedules = []
        for entry in raw_entries:
            g = entry["group"]
            rank_counter[g] = rank_counter.get(g, 0) + 1
            rank = rank_counter[g]

            offer = entry["offer"]
            first_leg = entry["first_leg"]

            trip_id = (f"ATTO-FLIGHT-{entry['airline_code']}{entry['flight_num']}"
                       f"-{from_code}-{to_code}-{entry['dep_date']}"
                       f"-{entry['cabin']}-{entry['bag_sig']}-{rank}")

            schedules.append({
                "trip_id": trip_id,
                "mode": "FLIGHT",
                "origin_city": from_name,
                "destination_city": to_name,
                "travel_date": travel_date_str,
                "departure_time": offer.get('departure_time'),
                "arrival_time": offer.get('arrival_time'),
                "transport_name": first_leg.get('operating_airline_name') or "ATTO Flight",
                "free_seats": None,  # genuinely absent from this endpoint
                "price": offer.get('price'),
                "duration_minutes": first_leg.get('duration_minutes'),
                "stops": offer.get('stops'),
                "cabin": entry["cabin"] or None,
                "service_class": first_leg.get('service_class'),
                "baggage": entry["checked"],
                "hand_baggage": entry["carry_on"],
                "last_scraped_at": datetime.now()
            })

        print(f" -> Found {len(schedules)} flight fares.")
        return schedules, timed_out

    except Exception as e:
        print(f"Error fetching flight schedules: {e}")
        return [], True

# ==========================================
# 4. DATABASE UPSERT LOGIC
# ==========================================
def sync_to_sql(schedules_data):
    if not schedules_data:
        print("No new data to sync.")
        return

    df_schedules = pd.DataFrame(schedules_data)

    # Same bus/train trip can legitimately show up in multiple destination
    # searches (e.g. a bus running Tashkent -> Navoi -> Zarafshan appears
    # once per stop we search for). trip_id doesn't encode which search
    # found it, so dedupe here to avoid a PK violation on insert.
    before = len(df_schedules)
    df_schedules = df_schedules.drop_duplicates(subset='trip_id', keep='last')
    if len(df_schedules) < before:
        print(f"Dropped {before - len(df_schedules)} duplicate trip_id rows "
              f"(same trip appearing across multiple destination searches).")

    with engine.begin() as conn:
        df_schedules.to_sql('stg_atto_schedules', con=conn, if_exists='replace', index=False)

        # --- DDL first, as its OWN statement -----------------------------
        # SQL Server resolves column names when it compiles a batch, so an
        # ALTER TABLE ADD and a query using the new column cannot live in
        # the same batch — the query fails to compile before the ALTER ever
        # runs. Hence three separate conn.execute() calls below.
        conn.execute(text("""
            IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='atto_trip_schedules' and xtype='U')
            BEGIN
                CREATE TABLE atto_trip_schedules (
                    trip_id VARCHAR(150) PRIMARY KEY,
                    mode VARCHAR(20),
                    origin_city VARCHAR(100),
                    destination_city VARCHAR(100),
                    travel_date VARCHAR(20),
                    departure_time VARCHAR(100),
                    arrival_time VARCHAR(100),
                    transport_name VARCHAR(150),
                    free_seats VARCHAR(20),
                    price FLOAT,
                    duration_minutes INT,
                    stops INT,
                    cabin VARCHAR(20),
                    service_class VARCHAR(60),
                    baggage VARCHAR(30),
                    hand_baggage VARCHAR(30),
                    last_scraped_at DATETIME
                )
            END
        """))

        # Migration for tables created before these columns existed. Each
        # runs as its own batch (see note above) and is a no-op once the
        # column is present. Add future columns to this list.
        new_columns = {
            "travel_date": "VARCHAR(20)",
            "duration_minutes": "INT",
            "stops": "INT",
            "cabin": "VARCHAR(20)",
            "service_class": "VARCHAR(60)",
            "baggage": "VARCHAR(30)",
            "hand_baggage": "VARCHAR(30)",
        }
        for col, coltype in new_columns.items():
            conn.execute(text(f"""
                IF COL_LENGTH('atto_trip_schedules', '{col}') IS NULL
                    ALTER TABLE atto_trip_schedules ADD {col} {coltype}
            """))

        merge_query = text("""
            MERGE atto_trip_schedules AS target
            USING stg_atto_schedules AS source
            ON (target.trip_id = source.trip_id)
            WHEN MATCHED THEN
                UPDATE SET
                    target.travel_date = source.travel_date,
                    target.departure_time = source.departure_time,
                    target.arrival_time = source.arrival_time,
                    target.free_seats = source.free_seats,
                    target.price = source.price,
                    target.duration_minutes = source.duration_minutes,
                    target.stops = source.stops,
                    target.cabin = source.cabin,
                    target.service_class = source.service_class,
                    target.baggage = source.baggage,
                    target.hand_baggage = source.hand_baggage,
                    target.last_scraped_at = source.last_scraped_at
            WHEN NOT MATCHED THEN
                INSERT (trip_id, mode, origin_city, destination_city, travel_date,
                        departure_time, arrival_time, transport_name, free_seats, price,
                        duration_minutes, stops, cabin, service_class, baggage,
                        hand_baggage, last_scraped_at)
                VALUES (source.trip_id, source.mode, source.origin_city,
                        source.destination_city, source.travel_date,
                        source.departure_time, source.arrival_time, source.transport_name,
                        source.free_seats, source.price, source.duration_minutes,
                        source.stops, source.cabin, source.service_class, source.baggage,
                        source.hand_baggage, source.last_scraped_at);
        """)
        conn.execute(merge_query)
        print(f"Successfully synced {len(df_schedules)} records to SQL Server.")

# ==========================================
# 5. AUTOMATION & SCHEDULING PIPELINE
# ==========================================
def build_session():
    session = requests.Session()

    # Headers copied exactly from a real browser request (DevTools -> Network -> Fetch/XHR)
    # against the "trains?stationTo=..." request. Do not simplify/guess these ---
    # the API validates several of them strictly (device_os, api_version, etc).
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/151.0.0.0 Safari/537.36 Edg/151.0.0.0",
        "Accept": "application/json",
        "Accept-Language": "en-US,en;q=0.9",
        "Content-Type": "application/json",
        # NOTE: real requests are sent with Origin/Referer on ticket.atto.uz
        # (no "s") even though the request itself goes to tickets.atto.uz
        # (with "s"). This is a legitimate cross-subdomain CORS setup on
        # their end, not a typo.
        "Referer": "https://ticket.atto.uz/",
        "Origin": "https://ticket.atto.uz",
        "api_version": "1",
        "device": "WINDOWS",
        "device_os": "WINDOWS",
        "language": "en",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-site",
        # IMPORTANT: the real request also includes an "access_token" header
        # (a UUID-looking value). Check in DevTools whether this value stays
        # the same across page reloads / new browser sessions:
        #   - If it's stable, it's likely tied to a cookie set on your first
        #     visit to the site, in which case session.get() warm-up below
        #     may already cause `requests` to send it automatically once you
        #     find the right cookie -> header mapping.
        #   - If it changes on every reload, it's a rotating session token
        #     and you'll need to find where the frontend obtains it (likely
        #     a POST /auth or similar call) and fetch a fresh one here before
        #     each scrape run.
        # Hardcoding the exact value below WILL work short-term but is not
        # a durable fix if it expires/rotates.
        "access_token": os.environ.get("ATTO_ACCESS_TOKEN", "REPLACE_WITH_TOKEN_FROM_DEVTOOLS"),
    })

    # Warm up session cookies
    try:
        session.get("https://tickets.atto.uz/", timeout=10)
    except Exception:
        pass

    return session


def fetch_ground_batch(session, routes):
    """Runs train+bus lookups for a list of (origin, dest, date) tuples.

    Returns (schedules, failed_calls, total_calls). The failure counts let
    discover_horizon() tell 'this day genuinely has no trips' apart from
    'the API was throwing errors at us', which look identical otherwise.
    """
    schedules = []
    failed = 0
    total = 0

    with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_REQUESTS) as executor:
        futures = {}
        for origin, dest, date in routes:
            futures[executor.submit(fetch_train_schedules, session, origin, dest, date)] = ("train", origin, dest, date)
            futures[executor.submit(fetch_bus_schedules, session, origin, dest, date)] = ("bus", origin, dest, date)

        total = len(futures)

        for future in as_completed(futures):
            mode, origin, dest, date = futures[future]
            try:
                schedules.extend(future.result())
            except Exception as e:
                failed += 1
                print(f"{mode} route {origin} -> {dest} ({date}) failed entirely: {e}")

    return schedules, failed, total


def discover_horizon():
    """Walks forward day by day to find how far ahead ATTO actually has
    inventory, and saves that number for the frequent pipelines to use.

    Stops after EMPTY_STREAK_LIMIT consecutive days with zero records, or
    at MAX_HORIZON_DAYS, whichever comes first. Everything it fetches gets
    synced to SQL too — no point throwing away data we already paid for.

    Expensive by design (potentially 45 days x 15 cities x 2 modes), which
    is exactly why it runs once a day rather than every 10 minutes.
    """
    print(f"\n{'='*60}\n[horizon] Starting booking-horizon discovery...\n{'='*60}")
    started = datetime.now()

    session = build_session()
    destinations = [city for city in STATIONS if city != ORIGIN_CITY]

    empty_streak = 0
    last_day_with_data = -1
    all_schedules = []
    aborted = False

    for offset in range(MAX_HORIZON_DAYS):
        date = date_offset(offset)
        routes = [(ORIGIN_CITY, dest, date) for dest in destinations]

        print(f"\n[horizon] Day +{offset} ({date}) — probing {len(routes)} destinations...")
        day_schedules, failed, total = fetch_ground_batch(session, routes)

        # A day where most calls errored tells us nothing about inventory.
        # Treat it as inconclusive and stop, keeping the previous horizon,
        # rather than letting a network blip permanently shrink our range.
        if total and (failed / total) > MAX_ERROR_RATE:
            print(f"[horizon] Day +{offset}: {failed}/{total} requests failed — "
                  f"too unreliable to judge. Aborting scan, keeping existing horizon.")
            all_schedules.extend(day_schedules)
            aborted = True
            break

        found = len(day_schedules)
        all_schedules.extend(day_schedules)

        if found > 0:
            last_day_with_data = offset
            empty_streak = 0
            print(f"[horizon] Day +{offset} ({date}): {found} records.")
        else:
            empty_streak += 1
            print(f"[horizon] Day +{offset} ({date}): empty "
                  f"({empty_streak}/{EMPTY_STREAK_LIMIT} in a row).")
            if empty_streak >= EMPTY_STREAK_LIMIT:
                print(f"[horizon] {EMPTY_STREAK_LIMIT} empty days in a row — stopping scan.")
                break
    else:
        print(f"[horizon] Reached MAX_HORIZON_DAYS ({MAX_HORIZON_DAYS}) with data "
              f"still coming back. Capping there — raise the constant if you want more.")

    # Sync everything the scan collected (this doubles as a full refresh).
    print(f"\n[horizon] Scan collected {len(all_schedules)} records — syncing...")
    sync_to_sql(all_schedules)

    elapsed = (datetime.now() - started).total_seconds() / 60

    if aborted:
        current = load_horizon()
        print(f"[horizon] Scan aborted after {elapsed:.1f} min. "
              f"Horizon unchanged at {current} day(s).")
        return current

    # last_day_with_data is a 0-based offset; +1 converts to a day count.
    discovered = max(DEFAULT_DAYS_AHEAD, last_day_with_data + 1)

    if last_day_with_data < 0:
        print(f"[horizon] No data found on ANY day — that's suspicious "
              f"(bad token? API change?). Falling back to {DEFAULT_DAYS_AHEAD} day(s) "
              f"and leaving the saved horizon alone.")
        return load_horizon()

    save_horizon(discovered)
    print(f"[horizon] Done in {elapsed:.1f} min. Inventory runs {discovered} day(s) "
          f"ahead (last day with data: +{last_day_with_data}).")
    return discovered


def ground_pipeline():
    """Trains + buses. Fast (single request per route), safe to run every
    few minutes. Uses whatever horizon discover_horizon() last found."""
    session = build_session()

    days_ahead = load_horizon()

    # Build routes automatically: origin -> every other city in STATIONS,
    # repeated for each day in the discovered horizon.
    # Add/remove cities by editing the STATIONS dict, not this list.
    destinations = [city for city in STATIONS if city != ORIGIN_CITY]
    dates = get_upcoming_dates(days_ahead)
    routes = [(ORIGIN_CITY, dest, date) for date in dates for dest in destinations]

    print(f"[ground] Fetching {len(routes)} route/date combos from {ORIGIN_CITY} "
          f"across {len(dates)} day(s) ({dates[0]} .. {dates[-1]}) "
          f"({MAX_CONCURRENT_REQUESTS} at a time)...")

    all_schedules, failed, total = fetch_ground_batch(session, routes)
    if failed:
        print(f"[ground] {failed}/{total} requests failed this run.")

    sync_to_sql(all_schedules)


def _run_flight_routes(session, routes, airport_codes):
    """Runs a batch of (origin, dest, date) flight routes concurrently.
    Returns (schedules, timed_out_routes)."""
    schedules = []
    timed_out = []

    with ThreadPoolExecutor(max_workers=FLIGHT_CONCURRENCY) as ex:
        futures = {
            ex.submit(
                fetch_flight_schedules, session,
                airport_codes[origin], airport_codes[dest],
                origin, dest, date
            ): (origin, dest, date)
            for origin, dest, date in routes
        }

        for future in as_completed(futures):
            origin, dest, date = futures[future]
            try:
                result, did_time_out = future.result()
                schedules.extend(result)
                if did_time_out:
                    timed_out.append((origin, dest, date))
            except Exception as e:
                print(f"flight route {origin} -> {dest} ({date}) failed entirely: {e}")
                timed_out.append((origin, dest, date))

    return schedules, timed_out


def flight_pipeline(days_ahead=None, label="flight"):
    """Flights. Slow (60-90s+ per route via search-new + polling).

    days_ahead=None means the full discovered horizon. Data is synced day
    by day rather than all at the end, so a crash three hours into a full
    sweep doesn't throw away everything collected so far.
    """
    session = build_session()

    horizon = load_horizon()
    days_ahead = horizon if days_ahead is None else min(days_ahead, horizon)

    destinations = [city for city in STATIONS if city != ORIGIN_CITY]
    dates = get_upcoming_dates(days_ahead)

    print(f"\n[{label}] Looking up airport codes...")
    airport_codes = get_airport_codes(session, [ORIGIN_CITY] + destinations)
    print(f"[{label}] Airports found for: {list(airport_codes.keys())}")

    if ORIGIN_CITY not in airport_codes:
        print(f"[{label}] No airport code for {ORIGIN_CITY} — cannot build routes. Aborting.")
        return

    air_destinations = [d for d in destinations if d in airport_codes]
    total_routes = len(air_destinations) * len(dates)

    print(f"[{label}] {total_routes} route/date combos across {len(dates)} day(s) "
          f"({dates[0]} .. {dates[-1]}), {FLIGHT_CONCURRENCY} at a time.")
    print(f"[{label}] Rough estimate: {total_routes * 75 / FLIGHT_CONCURRENCY / 60:.0f} minutes.")

    started = datetime.now()
    all_timed_out = []
    grand_total = 0

    # One day at a time, syncing as we go.
    for date in dates:
        routes = [(ORIGIN_CITY, dest, date) for dest in air_destinations]
        day_schedules, timed_out = _run_flight_routes(session, routes, airport_codes)
        all_timed_out.extend(timed_out)
        grand_total += len(day_schedules)

        print(f"[{label}] {date}: {len(day_schedules)} flights, "
              f"{len(timed_out)}/{len(routes)} timed out. Syncing...")
        sync_to_sql(day_schedules)

    # Retry pass — the aggregator is inconsistent rather than broken, so
    # routes that gave up once often succeed on a second attempt.
    for pass_num in range(FLIGHT_RETRY_PASSES):
        if not all_timed_out:
            break
        print(f"\n[{label}] Retry pass {pass_num + 1}: "
              f"{len(all_timed_out)} route(s) that timed out earlier...")
        retry_routes, all_timed_out = all_timed_out, []
        retry_schedules, all_timed_out = _run_flight_routes(session, retry_routes, airport_codes)
        grand_total += len(retry_schedules)
        print(f"[{label}] Retry recovered {len(retry_schedules)} flights, "
              f"{len(all_timed_out)} still failing.")
        sync_to_sql(retry_schedules)

    elapsed = (datetime.now() - started).total_seconds() / 60
    print(f"\n[{label}] Done in {elapsed:.0f} min. {grand_total} flight records total. "
          f"{len(all_timed_out)} route(s) unresolved — they'll be retried next run.")


def flight_pipeline_near():
    """Near-term flights, checked several times a day: prices move most
    close to departure."""
    flight_pipeline(days_ahead=FLIGHT_NEAR_DAYS, label="flight-near")


def flight_pipeline_full():
    """Every bookable day, once overnight. This is what gives the ML model
    training data for queries like 'flights two weeks out'."""
    flight_pipeline(days_ahead=None, label="flight-full")


if __name__ == "__main__":
    print("Starting ATTO API Scraper Pipeline...")

    age = horizon_age_hours()
    if age is None:
        print("[startup] No saved horizon found — running discovery first.")
        discover_horizon()
    elif age > 24:
        print(f"[startup] Saved horizon is {age:.1f}h old — refreshing.")
        discover_horizon()
    else:
        print(f"[startup] Using saved horizon ({load_horizon()} days, "
              f"{age:.1f}h old). Running ground pipeline.")
        ground_pipeline()

    # Breathing room before flights: the ground scan can fire ~3000 requests
    # in under two minutes, and hitting the airports endpoint immediately
    # after that came back completely empty (soft rate-limit).
    print("[startup] Pausing 30s before flights to let the API settle...")
    time.sleep(30)

    flight_pipeline_near()

    scheduler = BlockingScheduler()
    # Horizon discovery: expensive full scan, once a day in the quiet hours.
    # It syncs everything it fetches, so it doubles as a nightly full refresh.
    scheduler.add_job(discover_horizon, 'cron', hour=3)
    # Ground transport (trains/buses): fast, safe to check often.
    scheduler.add_job(ground_pipeline, 'interval', minutes=10)
    # Near-term flights: prices move most close to departure.
    scheduler.add_job(flight_pipeline_near, 'cron', hour='6,12,18')
    # Full-horizon flight sweep: hours long, so it runs overnight when
    # nothing competes with it. This is the job that builds out coverage
    # for far-future dates.
    scheduler.add_job(flight_pipeline_full, 'cron', hour=1,
                      max_instances=1, misfire_grace_time=3600)

    try:
        print("Scheduler running. Press Ctrl+C to exit.")
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        print("Scraper stopped.")
