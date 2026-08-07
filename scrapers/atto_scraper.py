import requests
import pandas as pd
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from sqlalchemy import create_engine, text
from apscheduler.schedulers.blocking import BlockingScheduler
from datetime import datetime

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

# Hub cities that get scraped AS AN ORIGIN to every other city. This is
# what lets the route engine find genuine multi-city journeys (e.g.
# Termez -> Samarkand -> Navoi) instead of every connection being forced
# through a single city just because that's the only data collected.
#
# Chosen to reflect Uzbekistan's actual rail/bus backbone: most
# regional travel really does route through Tashkent, Samarkand, or
# Bukhara in practice, so this isn't an arbitrary pick — it mirrors how
# people actually travel. Add more hubs only if you have a specific
# reason; each one roughly adds another full destination sweep to the
# request volume.
HUB_CITIES = ["Tashkent", "Samarkand", "Bukhara"]

# How many days ahead ground_pipeline checks each run. This is NOT fixed —
# it's set automatically by discover_horizon() (see below), which finds how
# far out data actually exists. This value is just the fallback used before
# discover_horizon() has run for the first time.
current_days_ahead = 2

# discover_horizon() keeps expanding the date range day by day until this
# many CONSECUTIVE days in a row come back with zero train/bus records —
# that's taken as "we've reached the edge of their booking window."
EMPTY_DAY_STREAK_LIMIT = 3

# Hard safety cap on discover_horizon() regardless of streak logic, so a
# fluke (e.g. one route always returning something) can't run away and
# scan months of dates in a single job.
MAX_HORIZON_DAYS = 60

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
    from datetime import timedelta
    today = datetime.now()
    return [(today + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(n_days)]

def get_airport_codes(session, cities):
    """
    Looks up the IATA airport code for each city by calling the airports
    search endpoint once per city and filtering to the 'Uzbekistan' country
    group. Not every city has an airport — those are simply left out of the
    returned dict, and flight routes involving them are skipped later.
    """
    codes = {}
    for city in cities:
        try:
            resp = session.get(AIRPORTS_ENDPOINT, params={"search": city}, timeout=15)
            if resp.status_code != 200:
                continue
            data = resp.json()
            groups = data.get('data', {}).get('airports', []) if isinstance(data, dict) else []

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
            print(f"Airport lookup failed for {city}: {e}")

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
                "departure_time": departure.get('localTime'),
                "arrival_time": arrival.get('localTime'),
                "transport_name": item.get('brand') or item.get('type') or "ATTO Train",
                "free_seats": first_car.get('freeSeats'),
                "price": first_car.get('amount'),
                "last_scraped_at": datetime.now()
            })

        print(f" -> Found {len(schedules)} trains.")
        return schedules

    except Exception as e:
        print(f"Error fetching train schedules: {e}")
        return []


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
                "departure_time": etd.split(' ')[1] if etd else None,
                "arrival_time": eta.split(' ')[1] if eta else None,
                "transport_name": trip.get('transporter_name') or trip.get('bus_model_name') or "ATTO Bus",
                "free_seats": (trip.get('seats_count') or 0) - (trip.get('sold_seats_count') or 0) - (trip.get('reserved_seats_count') or 0),
                "price": trip.get('price'),
                "last_scraped_at": datetime.now()
            })

        print(f" -> Found {len(schedules)} buses.")
        return schedules

    except Exception as e:
        print(f"Error fetching bus schedules: {e}")
        return []

def fetch_flight_schedules(session, from_code, to_code, from_name, to_name, travel_date_str):
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
            return []

        request_id = search_resp.json().get('data', {}).get('requestId')
        if not request_id:
            print(" -> No requestId returned, skipping.")
            return []

        # Poll for results. The real frontend polls repeatedly (seen as
        # 304s) until status flips to "finished" — mirror that with a
        # capped number of attempts so a stuck request can't hang forever.
        # CONFIRMED via manual testing: this can genuinely take ~60-70
        # seconds to resolve (real-time airline aggregation on their end,
        # not throttling) — budget generously rather than giving up early.
        offers = []
        last_status = None
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
            if last_status == 'finished':
                offers = poll_data.get('offers', [])
                break
        else:
            print(f" -> Gave up waiting for flight results (last status: {last_status}).")

        schedules = []
        for offer in offers:
            flights_info = offer.get('flights_info') or []
            first_leg = flights_info[0] if flights_info else {}

            airline_code = first_leg.get('operating_airline_code', '')
            flight_num = first_leg.get('flight_number', '')
            dep_date = offer.get('departure_date', formatted_date)

            schedules.append({
                "trip_id": f"ATTO-FLIGHT-{airline_code}{flight_num}-{from_code}-{to_code}-{dep_date}",
                "mode": "FLIGHT",
                "origin_city": from_name,
                "destination_city": to_name,
                "departure_time": offer.get('departure_time'),
                "arrival_time": offer.get('arrival_time'),
                "transport_name": first_leg.get('operating_airline_name') or "ATTO Flight",
                "free_seats": None,  # not exposed by this endpoint
                "price": offer.get('price'),
                "last_scraped_at": datetime.now()
            })

        print(f" -> Found {len(schedules)} flights.")
        return schedules

    except Exception as e:
        print(f"Error fetching flight schedules: {e}")
        return []

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

        merge_query = text("""
            IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='atto_trip_schedules' and xtype='U')
            BEGIN
                CREATE TABLE atto_trip_schedules (
                    trip_id VARCHAR(150) PRIMARY KEY,
                    mode VARCHAR(20),
                    origin_city VARCHAR(100),
                    destination_city VARCHAR(100),
                    departure_time VARCHAR(100),
                    arrival_time VARCHAR(100),
                    transport_name VARCHAR(150),
                    free_seats VARCHAR(20),
                    price FLOAT,
                    last_scraped_at DATETIME
                )
            END;

            MERGE atto_trip_schedules AS target
            USING stg_atto_schedules AS source
            ON (target.trip_id = source.trip_id)
            WHEN MATCHED THEN
                UPDATE SET
                    target.departure_time = source.departure_time,
                    target.arrival_time = source.arrival_time,
                    target.free_seats = source.free_seats,
                    target.price = source.price,
                    target.last_scraped_at = source.last_scraped_at
            WHEN NOT MATCHED THEN
                INSERT (trip_id, mode, origin_city, destination_city, departure_time, arrival_time, transport_name, free_seats, price, last_scraped_at)
                VALUES (source.trip_id, source.mode, source.origin_city, source.destination_city, source.departure_time, source.arrival_time, source.transport_name, source.free_seats, source.price, source.last_scraped_at);
        """)
        conn.execute(merge_query)
        print(f"Successfully synced {len(df_schedules)} records to SQL Server.")

# ==========================================
# 5. AUTOMATION & SCHEDULING PIPELINE
# ==========================================
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
        "access_token": "REPLACE_WITH_TOKEN_FROM_DEVTOOLS",
    })

    # Warm up session cookies
    try:
        session.get("https://tickets.atto.uz/", timeout=10)
    except Exception:
        pass

    return session


def discover_horizon():
    """
    Expands the search range day by day (today, +1, +2, ...) checking
    trains+buses for every HUB_CITIES -> destination pair, until
    EMPTY_DAY_STREAK_LIMIT consecutive days come back with zero total
    records — that's treated as the edge of ATTO's booking window.
    Updates current_days_ahead so ground_pipeline's frequent runs know how
    far to check without having to rediscover this every single time.

    This is deliberately run rarely (see scheduler setup) rather than every
    10 minutes — if bookings are genuinely open 30-45+ days out, redoing
    this full expanding scan every 10 minutes would multiply request volume
    enormously for no benefit, since the horizon doesn't change that fast.

    Also syncs whatever data it finds along the way, since it's already
    fetching it.
    """
    global current_days_ahead

    session = build_session()

    # Every hub -> every OTHER city (including other hubs, so e.g.
    # Samarkand -> Bukhara is covered directly rather than only via a
    # 3rd-leg connection).
    hub_routes = [
        (hub, dest)
        for hub in HUB_CITIES
        for dest in STATIONS
        if dest != hub
    ]

    print(f"[horizon] Discovering how many days ahead has data "
          f"({len(hub_routes)} hub->destination pairs/day, "
          f"stopping after {EMPTY_DAY_STREAK_LIMIT} empty days in a row, "
          f"capped at {MAX_HORIZON_DAYS} days)...")

    all_schedules = []
    consecutive_empty = 0
    last_nonempty_offset = -1
    day_offset = 0

    while consecutive_empty < EMPTY_DAY_STREAK_LIMIT and day_offset < MAX_HORIZON_DAYS:
        date_str = get_upcoming_dates(day_offset + 1)[-1]  # the single date at this offset

        day_schedules = []
        with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_REQUESTS) as executor:
            futures = {}
            for hub, dest in hub_routes:
                futures[executor.submit(fetch_train_schedules, session, hub, dest, date_str)] = ("train", hub, dest)
                futures[executor.submit(fetch_bus_schedules, session, hub, dest, date_str)] = ("bus", hub, dest)

            for future in as_completed(futures):
                mode, hub, dest = futures[future]
                try:
                    day_schedules.extend(future.result())
                except Exception as e:
                    print(f"[horizon] {mode} {hub} -> {dest} on {date_str} failed: {e}")

        print(f"[horizon] Day +{day_offset} ({date_str}): {len(day_schedules)} records")
        all_schedules.extend(day_schedules)

        if day_schedules:
            consecutive_empty = 0
            last_nonempty_offset = day_offset
        else:
            consecutive_empty += 1

        day_offset += 1

    # Save whatever we found during discovery too, since it's real data.
    sync_to_sql(all_schedules)

    new_days_ahead = max(last_nonempty_offset + 1, 1)  # always check at least 1 day
    print(f"[horizon] Data found through day +{last_nonempty_offset}. "
          f"ground_pipeline will now check {new_days_ahead} day(s) ahead "
          f"until the next horizon discovery run.")
    current_days_ahead = new_days_ahead


def ground_pipeline():
    """Trains + buses, from EVERY hub city to every other city. Fast
    (single request per route), safe to run every few minutes. Uses
    whatever range discover_horizon() last determined — see
    current_days_ahead."""
    session = build_session()

    # Every hub -> every OTHER city, repeated for each of the next
    # current_days_ahead days. This is what lets the route engine build
    # real multi-city journeys (e.g. Termez -> Samarkand -> Navoi) instead
    # of being forced through a single hub just because that's the only
    # data that exists. Add/remove hubs via HUB_CITIES, cities via STATIONS.
    dates = get_upcoming_dates(current_days_ahead)
    routes = [
        (hub, dest, date)
        for date in dates
        for hub in HUB_CITIES
        for dest in STATIONS
        if dest != hub
    ]

    print(f"[ground] Fetching {len(routes)} route/date combos across "
          f"{len(HUB_CITIES)} hubs ({', '.join(HUB_CITIES)}) and "
          f"{len(dates)} day(s): {dates} "
          f"({MAX_CONCURRENT_REQUESTS} at a time)...")

    all_schedules = []

    with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_REQUESTS) as executor:
        futures = {}

        for origin, dest, date in routes:
            futures[executor.submit(fetch_train_schedules, session, origin, dest, date)] = ("train", origin, dest)
            futures[executor.submit(fetch_bus_schedules, session, origin, dest, date)] = ("bus", origin, dest)

        for future in as_completed(futures):
            mode, origin, dest = futures[future]
            try:
                all_schedules.extend(future.result())
            except Exception as e:
                print(f"{mode} route {origin} -> {dest} failed entirely: {e}")

    sync_to_sql(all_schedules)


def flight_pipeline():
    """Flights only. Slow (each route can take 60-90+ seconds to resolve
    via search-new + polling), so this runs on its own, much less frequent
    schedule rather than sharing the 10-minute ground-transport interval."""
    session = build_session()

    destinations = list(STATIONS.keys())
    dates = get_upcoming_dates(current_days_ahead)

    print("[flight] Looking up airport codes...")
    airport_codes = get_airport_codes(session, destinations)
    print(f"[flight] Airports found for: {list(airport_codes.keys())}")

    flight_routes = [
        (hub, dest, date)
        for date in dates
        for hub in HUB_CITIES
        for dest in STATIONS
        if dest != hub and hub in airport_codes and dest in airport_codes
    ]

    print(f"[flight] Fetching {len(flight_routes)} flight route/date combos "
          f"across {len(HUB_CITIES)} hubs (2 at a time, each can take 60-90+ seconds — "
          f"this will take a while with multiple hubs, that's expected)...")

    all_schedules = []

    with ThreadPoolExecutor(max_workers=2) as flight_executor:
        flight_futures = {
            flight_executor.submit(
                fetch_flight_schedules, session,
                airport_codes[origin], airport_codes[dest],
                origin, dest, date
            ): ("flight", origin, dest)
            for origin, dest, date in flight_routes
        }

        for future in as_completed(flight_futures):
            mode, origin, dest = flight_futures[future]
            try:
                all_schedules.extend(future.result())
            except Exception as e:
                print(f"{mode} route {origin} -> {dest} failed entirely: {e}")

    sync_to_sql(all_schedules)


if __name__ == "__main__":
    print("Starting ATTO API Scraper Pipeline...")

    # Run both once immediately
    ground_pipeline()
    flight_pipeline()

    scheduler = BlockingScheduler()
    # Ground transport (trains/buses): fast, safe to check often.
    scheduler.add_job(ground_pipeline, 'interval', minutes=10)
    # Flights: slow (60-90s+ per route), checked a few times a day instead.
    # Adjust the hour list below to whatever check times make sense for you.
    scheduler.add_job(flight_pipeline, 'cron', hour='6,12,18')

    try:
        print("Scheduler running. Press Ctrl+C to exit.")
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        print("Scraper stopped.")