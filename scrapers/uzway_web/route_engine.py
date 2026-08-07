"""
UzWay route engine — finds and ranks travel options from the scraped
atto_trip_schedules table.

This is deliberately NOT a machine-learning model. "Find the cheapest or
fastest way from A to B" is a search problem with an exact answer, so a
trained model could only approximate it worse, and would need labelled
training data that doesn't exist. Everything here is deterministic and
fully explainable — useful when a judge asks how it works.

Usage:
    python route_engine.py "Tashkent to Kashkadarya, 2kg baggage, cheapest"
    python route_engine.py "Tashkent to Samarkand, I have a child"
    python route_engine.py "Tashkent to Bukhara with 2 person more"
    python route_engine.py --list-cities
"""

import re
import sys
import unicodedata
import difflib
from datetime import datetime, timedelta

import pandas as pd
from sqlalchemy import create_engine, text

DB_CONNECTION_STRING = (
    "mssql+pyodbc://@DESKTOP-5K5RC72/UzWayDB?"
    "driver=ODBC+Driver+17+for+SQL+Server&"
    "Trusted_Connection=yes&"
    "TrustServerCertificate=yes"
)

# ==========================================================
# CITY / REGION ALIASES
# ==========================================================
CITY_ALIASES = {
    "tashkent": "Tashkent", "toshkent": "Tashkent", "ташкент": "Tashkent",
    "tashkend": "Tashkent", "tosh": "Tashkent",
    "samarkand": "Samarkand", "samarqand": "Samarkand", "самарканд": "Samarkand",
    "bukhara": "Bukhara", "buxoro": "Bukhara", "бухара": "Bukhara",
    "buhara": "Bukhara",
    "urgench": "Urgench", "urganch": "Urgench", "ургенч": "Urgench",
    "khorezm": "Urgench", "xorazm": "Urgench", "хорезм": "Urgench",
    "khiva": "Khiva", "xiva": "Khiva", "хива": "Khiva",
    "nukus": "Nukus", "нукус": "Nukus",
    "karakalpakstan": "Nukus", "qoraqalpogiston": "Nukus",
    "каракалпакстан": "Nukus",
    "navoi": "Navoi", "navoiy": "Navoi", "навои": "Navoi", "navai": "Navoi",
    "andijan": "Andijan", "andijon": "Andijan", "андижан": "Andijan",
    "karshi": "Karshi", "qarshi": "Karshi", "карши": "Karshi",
    "kashkadarya": "Karshi", "qashqadaryo": "Karshi", "кашкадарья": "Karshi",
    "djizakh": "Djizakh", "jizzakh": "Djizakh", "jizzax": "Djizakh",
    "джизак": "Djizakh", "dizak": "Djizakh",
    "termez": "Termez", "termiz": "Termez", "термез": "Termez",
    "surkhandarya": "Termez", "surxondaryo": "Termez", "сурхандарья": "Termez",
    "gulistan": "Gulistan", "guliston": "Gulistan", "гулистан": "Gulistan",
    "sirdaryo": "Gulistan", "syrdarya": "Gulistan", "сырдарья": "Gulistan",
    "kokand": "Kokand", "qoqon": "Kokand", "qo'qon": "Kokand", "коканд": "Kokand",
    "margilan": "Margilan", "margilon": "Margilan", "marg'ilon": "Margilan",
    "маргилан": "Margilan",
    "pap": "Pap", "пап": "Pap",
    "namangan": "Namangan", "наманган": "Namangan",
}

MODE_LABELS = {"TRAIN": "Train", "BUS": "Bus", "FLIGHT": "Flight"}


def normalise(s):
    """Lowercase, strip accents and punctuation for alias matching."""
    if not s:
        return ""
    s = unicodedata.normalize("NFKD", str(s))
    s = "".join(c for c in s if not unicodedata.combining(c))
    return re.sub(r"[^a-z0-9а-я]", "", s.lower())


_ALIAS_LOOKUP = {normalise(k): v for k, v in CITY_ALIASES.items()}

_SUFFIXES = ["gacha", "ganacha", "dagi", "dan", "den", "tan", "ten",
             "gan", "ga", "qa", "ka", "da", "ta", "ni", "ning", "у", "е", "а"]


def resolve_city(name):
    """'Kashkadarya' -> 'Karshi', 'Toshkentdan' -> 'Tashkent', 'Tahkent' (typo) -> 'Tashkent'."""
    key = normalise(name)
    if not key:
        return None
    if key in _ALIAS_LOOKUP:
        return _ALIAS_LOOKUP[key]

    for suf in _SUFFIXES:
        if key.endswith(suf) and len(key) - len(suf) >= 3:
            stem = key[:-len(suf)]
            if stem in _ALIAS_LOOKUP:
                return _ALIAS_LOOKUP[stem]

    if len(key) >= 4:
        hits = {v for k, v in _ALIAS_LOOKUP.items() if k.startswith(key)}
        if len(hits) == 1:
            return hits.pop()

    # Last resort: tolerate small typos ("Tahkent" -> "Tashkent", missing
    # one letter). Only trusted when it's a close, unambiguous match —
    # this is meant to catch a dropped/swapped letter, not guess wildly.
    if len(key) >= 5:
        close = difflib.get_close_matches(key, _ALIAS_LOOKUP.keys(), n=1, cutoff=0.8)
        if close:
            return _ALIAS_LOOKUP[close[0]]

    return None


# ==========================================================
# DATA LOADING
# ==========================================================
def load_trips(conn_string=DB_CONNECTION_STRING):
    """Pulls every scraped trip out of SQL Server."""
    engine = create_engine(conn_string)
    with engine.begin() as conn:
        df = pd.read_sql(text("""
            SELECT trip_id, mode, origin_city, destination_city, travel_date,
                   departure_time, arrival_time, transport_name, free_seats,
                   price, duration_minutes, stops, cabin, service_class,
                   baggage, hand_baggage, last_scraped_at
            FROM atto_trip_schedules
        """), conn)
    return prepare(df)


def prepare(df):
    """Cleans raw rows into something rankable."""
    df = df.copy()

    df['price'] = pd.to_numeric(df['price'], errors='coerce')
    df['dep_min'] = df['departure_time'].apply(time_to_minutes)
    df['arr_min'] = df['arrival_time'].apply(time_to_minutes)
    df['duration'] = df.apply(compute_duration, axis=1)

    before = len(df)
    df = df[df['price'].notna() & (df['price'] > 0)]
    df = df[df['duration'].notna()]
    df.attrs['dropped'] = before - len(df)

    return df.reset_index(drop=True)


def time_to_minutes(t):
    """'16:00', '06:40:00.', '8:5' -> minutes since midnight."""
    if not t or (isinstance(t, float) and pd.isna(t)):
        return None
    m = re.match(r"^\s*(\d{1,2})[:.](\d{1,2})", str(t))
    if not m:
        return None
    hh, mm = int(m.group(1)), int(m.group(2))
    if hh > 23 or mm > 59:
        return None
    return hh * 60 + mm


def compute_duration(row):
    """Minutes in transit.

    Flights carry duration_minutes from the API. Trains and buses only
    store clock times with no date, so an overnight train departing 23:00
    and arriving 06:00 would compute as -1020 minutes. Wrapping past
    midnight fixes that — at the cost of being unable to distinguish a
    7-hour overnight trip from a 31-hour one. Good enough for ranking;
    worth knowing before anyone trusts it for scheduling.
    """
    d = row.get('duration_minutes')
    if pd.notna(d) and d and float(d) > 0:
        return float(d)

    dep, arr = row.get('dep_min'), row.get('arr_min')
    if dep is None or arr is None:
        return None
    delta = arr - dep
    if delta < 0:
        delta += 24 * 60
    return float(delta) if delta > 0 else None


# ==========================================================
# BAGGAGE
# ==========================================================
def baggage_ok(row, need_kg):
    """Can this option carry need_kg?

    Only flights restrict baggage. Trains and buses in Uzbekistan don't
    enforce a weight limit for normal luggage, so they always pass.
    """
    if need_kg is None or row['mode'] != 'FLIGHT':
        return True

    checked = parse_baggage(row.get('baggage'))
    hand = parse_baggage(row.get('hand_baggage'))

    for kind, val in (('checked', checked), ('hand', hand)):
        if val is None:
            continue
        amount, unit = val
        if unit == 'KG' and amount >= need_kg:
            return True
        if unit == 'PC' and amount >= 1:
            if kind == 'checked' or need_kg <= 8:
                return True
    return False


def parse_baggage(s):
    """'8KG' -> (8.0, 'KG'); '1PC' -> (1.0, 'PC'); '0KG' -> None."""
    if not s or pd.isna(s):
        return None
    m = re.match(r"^\s*([\d.]+)\s*([A-Za-z]+)", str(s))
    if not m:
        return None
    amount = float(m.group(1))
    if amount <= 0:
        return None
    return amount, m.group(2).upper()


def baggage_note(row):
    """Human-readable allowance for display."""
    if row['mode'] != 'FLIGHT':
        return "no limit"
    checked = parse_baggage(row.get('baggage'))
    hand = parse_baggage(row.get('hand_baggage'))
    parts = []
    if checked:
        parts.append(f"{checked[0]:g}{checked[1]} checked")
    if hand:
        parts.append(f"{hand[0]:g}{hand[1]} cabin")
    return ", ".join(parts) if parts else "cabin only"


# ==========================================================
# PARTY SIZE / SEATS
# ==========================================================
def seats_ok(row, party_size):
    """
    Can this single leg fit the whole party?

    Trains/buses expose free_seats from the scraper, so we can check
    directly. Flights do NOT — the offers endpoint we scrape doesn't
    return seat counts — so flight legs always pass here regardless of
    party size; the caller surfaces a note that flight availability
    wasn't actually verified.

    Unknown/unparseable free_seats values (None, blank, non-numeric) are
    treated as "don't know" rather than "no seats" — better to show an
    option that might not fit than to silently hide options because of a
    scraping gap.
    """
    if party_size <= 1:
        return True
    if row.get('mode') == 'FLIGHT':
        return True

    seats = row.get('free_seats')
    if seats is None or (isinstance(seats, float) and pd.isna(seats)):
        return True

    try:
        seats = int(float(str(seats).strip()))
    except (ValueError, TypeError):
        return True

    return seats >= party_size


def parse_party(low):
    """
    Pulls party size out of free text. Handles:
      - "I have a child"                       -> self + 1 child = 2
      - "with 2 person more" / "2 more people" -> self + 2 = 3
      - "2 adults 1 child"                     -> 2 adults + 1 child = 3
      - "family of 4" / "party of 3"           -> explicit total
      - "3 people" / "3 passengers" / "3 pax"  -> explicit total

    Returns (party_size, children_count). party_size is always >= 1.
    Precedence: an explicit total (family of N / N more / N people) wins
    over an adults+children split, which wins over a bare "child" mention.
    This is a heuristic for a demo tool, not a booking-grade parser — when
    in doubt it under-assumes (defaults to 1) rather than guessing high.
    """
    adults = None
    children = 0

    m = re.search(r'(\d+)\s*adults?\D{0,12}?(\d+)\s*(?:child|children|kid|kids)', low)
    if m:
        adults = int(m.group(1))
        children = int(m.group(2))

    if adults is None:
        m = re.search(r'(\d+)\s*adults?', low)
        if m:
            adults = int(m.group(1))

    if not children:
        m = re.search(r'(\d+)\s*(?:child|children|kid|kids)', low)
        if m:
            children = int(m.group(1))
        elif re.search(r'\b(?:a|one)?\s*child\b|\bkid\b', low):
            children = 1

    total = None
    m = re.search(r'(?:family|party|group)\s+of\s+(\d+)', low)
    if m:
        total = int(m.group(1))

    if total is None:
        # "with 2 person more" / "2 more people" / "2 extra passengers" —
        # additional to the person asking, in either word order. Checked
        # BEFORE the generic "N people" pattern below, since "2 person
        # more" would otherwise be misread as a flat total of 2 (the word
        # "person" alone matches that pattern too).
        for pattern in (
            r'(\d+)\s*(?:more|extra|additional)\s*(?:persons?|people)?',
            r'(\d+)\s*(?:persons?|people)\s*(?:more|extra|additional)',
        ):
            m = re.search(pattern, low)
            if m:
                total = 1 + int(m.group(1))
                break

    if total is None:
        m = re.search(r'(\d+)\s*(?:people|persons?|passengers?|pax|travell?ers?)\b', low)
        if m and adults is None:
            total = int(m.group(1))

    if total is not None:
        party_size = total
    elif adults is not None:
        party_size = adults + children
    elif children:
        party_size = 1 + children
    else:
        party_size = 1

    return max(party_size, 1), children


# ==========================================================
# QUERY PARSING
# ==========================================================
PREF_PATTERNS = {
    'cheapest': [r'cheap', r'arzon', r'дешев', r'budget', r'low.?cost', r'eng arzon'],
    'fastest': [r'fast', r'quick', r'tez', r'быстр', r'shortest', r'soon'],
}


def parse_query(txt):
    """Turns free text into a structured search.

    Regex rather than an LLM: it runs offline, costs nothing, and is
    trivial to debug on stage. Swap in an LLM call later if you want to
    handle messier phrasing.
    """
    low = txt.lower()
    q = {'origin': None, 'destination': None, 'baggage_kg': None,
         'preference': 'balanced', 'date': None, 'modes': None,
         'party_size': 1, 'children': 0}

    pair = re.search(r'from\s+([\w\'\- ]+?)\s+to\s+([\w\'\- ]+?)(?:[,.]|\s+with|\s+and|\s+i\b|$)', low)
    if not pair:
        pair = re.search(r'([\w\']+)\s*(?:->|→|-)\s*([\w\']+)', low)
    if not pair:
        pair = re.search(r'([\w\'\- ]+?)\s+to\s+([\w\'\- ]+?)(?:[,.]|\s+with|\s+and|$)', low)

    if pair:
        q['origin'] = resolve_city(pair.group(1))
        q['destination'] = resolve_city(pair.group(2))

    if not q['origin'] or not q['destination']:
        found = []
        for token in re.findall(r"[\w']+", low):
            city = resolve_city(token)
            if city and (not found or found[-1] != city):
                found.append(city)

        # Never let a leftover single match fill in a role with the SAME
        # city already assigned to the other role — that silently turns
        # "Navai to Tahkent" (an unrecognized second city) into "Navoi to
        # Navoi" instead of surfacing that the destination wasn't understood.
        if q['origin'] and not q['destination']:
            candidates = [c for c in found if c != q['origin']]
            if candidates:
                q['destination'] = candidates[0]
        elif q['destination'] and not q['origin']:
            candidates = [c for c in found if c != q['destination']]
            if candidates:
                q['origin'] = candidates[0]
        elif not q['origin'] and not q['destination']:
            if len(found) >= 2:
                q['origin'], q['destination'] = found[0], found[1]
            elif len(found) == 1:
                q['destination'] = found[0]

    bag = re.search(r'(\d+(?:[.,]\d+)?)\s*(?:kg|kilo|kilogram|кг)', low)
    if bag:
        q['baggage_kg'] = float(bag.group(1).replace(',', '.'))

    # Party size / children — see parse_party() for the full rule set.
    q['party_size'], q['children'] = parse_party(low)

    for pref, patterns in PREF_PATTERNS.items():
        if any(re.search(p, low) for p in patterns):
            q['preference'] = pref
            break

    if re.search(r'\btomorrow\b|\bertaga\b|завтра', low):
        q['date'] = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
    elif re.search(r'\btoday\b|\bbugun\b|сегодня', low):
        q['date'] = datetime.now().strftime('%Y-%m-%d')
    else:
        iso = re.search(r'(\d{4}-\d{2}-\d{2})', txt)
        dmy = re.search(r'(\d{1,2})[./](\d{1,2})[./](\d{4})', txt)
        if iso:
            q['date'] = iso.group(1)
        elif dmy:
            d, m, y = dmy.groups()
            q['date'] = f"{y}-{int(m):02d}-{int(d):02d}"

    modes = []
    if re.search(r'\bflight|fly|plane|samolyot|самолет', low):
        modes.append('FLIGHT')
    if re.search(r'\btrain|poyezd|поезд', low):
        modes.append('TRAIN')
    if re.search(r'\bbus|avtobus|автобус', low):
        modes.append('BUS')
    if modes:
        q['modes'] = modes

    return q


# ==========================================================
# ITINERARY SEARCH (direct + connections)
# ==========================================================
MIN_LAYOVER = 45
MAX_LAYOVER = 14 * 60
MAX_BRANCH = 40


def leg_start(row):
    """Absolute minute of departure."""
    d = row.get('date_ord')
    if d is None or pd.isna(d) or row.get('dep_min') is None:
        return None
    return int(d) * 1440 + int(row['dep_min'])


def add_time_index(df):
    """Adds date_ord / t_start / t_end used by the connection search."""
    df = df.copy()
    df['date_ord'] = pd.to_datetime(df['travel_date'], errors='coerce').map(
        lambda d: d.toordinal() if pd.notna(d) else None)
    df['t_start'] = df.apply(leg_start, axis=1)
    df['t_end'] = df.apply(
        lambda r: None if r['t_start'] is None or pd.isna(r['duration'])
        else r['t_start'] + int(r['duration']), axis=1)
    return df


def mirror_legs(df):
    """Reversed copies of every leg, flagged as estimates."""
    m = df.copy()
    m['origin_city'], m['destination_city'] = df['destination_city'], df['origin_city']
    m['estimated'] = True
    m['trip_id'] = m['trip_id'].astype(str) + "-REV"
    return m


def build_index(df, baggage_kg=None, modes=None, party_size=1):
    """origin city -> legs departing from it, sorted by departure time."""
    legs = df[df['t_start'].notna() & df['t_end'].notna()]
    if modes:
        filtered = legs[legs['mode'].isin(modes)]
        if not filtered.empty:
            legs = filtered
    if baggage_kg is not None:
        mask = legs.apply(lambda r: baggage_ok(r, baggage_kg), axis=1)
        if mask.any():
            legs = legs[mask]
    if party_size > 1:
        mask = legs.apply(lambda r: seats_ok(r, party_size), axis=1)
        if mask.any():
            legs = legs[mask]

    index = {}
    for row in legs.to_dict('records'):
        index.setdefault(row['origin_city'], []).append(row)
    for city in index:
        index[city].sort(key=lambda r: r['t_start'])
    return index


def find_itineraries(df, origin, destination, date=None, baggage_kg=None,
                     modes=None, party_size=1, max_legs=2, limit=10,
                     allow_mirrored=True):
    """Returns (itineraries, notes). Each itinerary is a list of leg dicts."""
    notes = []
    df = add_time_index(df)
    if 'estimated' not in df.columns:
        df['estimated'] = False

    def run(source_df):
        index = build_index(source_df, baggage_kg, modes, party_size)
        found = []

        def walk(city, path, visited):
            if len(path) >= max_legs:
                return
            candidates = index.get(city, [])

            if path:
                earliest = path[-1]['t_end'] + MIN_LAYOVER
                latest = path[-1]['t_end'] + MAX_LAYOVER
                candidates = [c for c in candidates
                              if earliest <= c['t_start'] <= latest]
            elif date:
                candidates = [c for c in candidates if c['travel_date'] == date]

            candidates = sorted(candidates, key=lambda c: c['t_start'])[:MAX_BRANCH]

            for leg in candidates:
                nxt = leg['destination_city']
                if nxt in visited:
                    continue
                new_path = path + [leg]
                if nxt == destination:
                    found.append(new_path)
                    if len(found) > 400:
                        return
                else:
                    walk(nxt, new_path, visited | {nxt})

        walk(origin, [], {origin})
        return found

    itineraries = run(df)

    if not itineraries and allow_mirrored:
        combined = pd.concat([df, mirror_legs(df)], ignore_index=True)
        itineraries = run(combined)
        if itineraries:
            notes.append(
                "No scraped legs for this direction, so some legs below are "
                "REVERSED schedules shown as estimates. Operators and prices are "
                "usually similar; departure times are not. Run collect_network.py "
                "to replace these with real data."
            )

    if not itineraries:
        if party_size > 1:
            notes.append(f"No journey found for a party of {party_size} — try a smaller "
                         f"group, or check without a date to see if seats free up.")
        if date:
            notes.append(f"No journey found from {origin} to {destination} on {date}. "
                         f"Try without a date, or allow 2 changes (--legs 3).")
        else:
            notes.append(f"No journey found from {origin} to {destination}. "
                         f"The database may not cover these cities yet — "
                         f"run collect_network.py.")
        return [], notes

    itineraries = dedupe_itineraries(itineraries)

    direct = sum(1 for it in itineraries if len(it) == 1)
    if direct == 0:
        notes.append(f"No direct service — every option below requires a change.")

    if party_size > 1:
        notes.append(f"Prices below are for all {party_size} traveler(s) "
                     f"(per-seat price × {party_size}).")
        if any(leg['mode'] == 'FLIGHT' for it in itineraries for leg in it):
            notes.append("Seat availability isn't exposed for flight offers by this "
                         "scraper, so flight legs aren't filtered by party size — "
                         "confirm seats are actually available before booking.")
        ground_legs = [leg for it in itineraries for leg in it if leg['mode'] in ('TRAIN', 'BUS')]
        if ground_legs and any(
            leg.get('free_seats') in (None, '') or
            (isinstance(leg.get('free_seats'), float) and pd.isna(leg.get('free_seats')))
            for leg in ground_legs
        ):
            notes.append("Some train/bus legs are missing seat-count data from the "
                         "scrape and were shown without a seat-availability check.")

    return itineraries[:limit], notes


def dedupe_itineraries(itineraries):
    """One entry per (route, mode) shape — keeps the cheapest of each so the
    output isn't twelve near-identical buses."""
    best = {}
    for it in itineraries:
        key = tuple((leg['origin_city'], leg['destination_city'], leg['mode'])
                    for leg in it)
        cost = itinerary_price(it)
        if key not in best or cost < itinerary_price(best[key]):
            best[key] = it
    return list(best.values())


# ==========================================================
# SCORING
# ==========================================================
def itinerary_price(it, party_size=1):
    """Total price for the whole party. Note: this scales every leg's
    scraped (per-seat) price by party_size — there's no child-fare data in
    the scraped API, so a child costs the same as an adult seat here. This
    is surfaced as a note to the user rather than silently assumed."""
    return sum(float(leg['price']) for leg in it if pd.notna(leg.get('price'))) * party_size


def itinerary_duration(it):
    """Door to door, including time spent waiting between legs."""
    return it[-1]['t_end'] - it[0]['t_start']


def itinerary_transit(it):
    return sum(float(leg['duration']) for leg in it)


def rank_itineraries(itineraries, preference='balanced'):
    # party_size deliberately isn't threaded through here — scaling every
    # itinerary's price by the same constant factor doesn't change which
    # one is cheapest/fastest, so ranking uses the per-seat price and only
    # the final display multiplies it out.
    if not itineraries:
        return []

    if preference == 'cheapest':
        return sorted(itineraries, key=lambda it: (itinerary_price(it),
                                                   itinerary_duration(it)))
    if preference == 'fastest':
        return sorted(itineraries, key=lambda it: (itinerary_duration(it),
                                                   itinerary_price(it)))

    prices = [itinerary_price(it) for it in itineraries]
    durations = [itinerary_duration(it) for it in itineraries]
    plo, phi = min(prices), max(prices)
    dlo, dhi = min(durations), max(durations)

    def score(it):
        p = 0 if phi == plo else (itinerary_price(it) - plo) / (phi - plo)
        d = 0 if dhi == dlo else (itinerary_duration(it) - dlo) / (dhi - dlo)
        return p + d + 0.15 * (len(it) - 1)

    return sorted(itineraries, key=lambda it: (score(it), itinerary_price(it)))


# ==========================================================
# OUTPUT
# ==========================================================
def fmt_duration(mins):
    if mins is None or pd.isna(mins):
        return "?"
    h, m = divmod(int(mins), 60)
    return f"{h}h {m:02d}m" if h else f"{m}m"


def fmt_price(p):
    return f"{int(p):,} so'm".replace(",", " ") if pd.notna(p) else "?"


def fmt_clock(row, which):
    t = row.get(f'{which}_time')
    return str(t)[:5] if t else "?"


def format_itineraries(itineraries, query, notes):
    out = []
    origin = query.get('origin') or '?'
    dest = query.get('destination') or '?'
    party_size = query.get('party_size', 1)

    out.append("")
    out.append("=" * 68)
    out.append(f"  {origin}  →  {dest}")
    bits = []
    if query.get('date'):
        bits.append(query['date'])
    if party_size > 1:
        child_note = f", incl. {query['children']} child" if query.get('children') else ""
        bits.append(f"{party_size} travelers{child_note}")
    if query.get('baggage_kg'):
        bits.append(f"{query['baggage_kg']:g}kg baggage")
    bits.append({'cheapest': 'cheapest first',
                 'fastest': 'fastest first',
                 'balanced': 'best value'}[query.get('preference', 'balanced')])
    out.append(f"  {' · '.join(bits)}")
    out.append("=" * 68)

    for n in notes:
        out.append(f"  ! {n}")
    if notes:
        out.append("")

    if not itineraries:
        out.append("  Nothing found.")
        return "\n".join(out)

    for i, it in enumerate(itineraries, 1):
        changes = len(it) - 1
        label = "Direct" if changes == 0 else f"{changes} change{'s' if changes > 1 else ''}"
        est = " [SOME LEGS ESTIMATED]" if any(leg.get('estimated') for leg in it) else ""

        per_person = itinerary_price(it)
        total = itinerary_price(it, party_size)
        price_str = fmt_price(total)
        if party_size > 1:
            price_str += f" total ({fmt_price(per_person)}/person)"

        out.append(f"  {i}. {label} · {price_str} · "
                   f"{fmt_duration(itinerary_duration(it))} total{est}")

        for n, leg in enumerate(it):
            arrow = "└─" if n == len(it) - 1 else "├─"
            tag = " (est.)" if leg.get('estimated') else ""
            leg_price = leg['price'] * party_size if pd.notna(leg.get('price')) else leg['price']
            out.append(f"     {arrow} {MODE_LABELS.get(leg['mode'], leg['mode'])}: "
                       f"{leg['origin_city']} {fmt_clock(leg, 'departure')} → "
                       f"{leg['destination_city']} {fmt_clock(leg, 'arrival')}"
                       f"   {fmt_price(leg_price)}{tag}")
            seat_note = ""
            if party_size > 1 and leg['mode'] in ('TRAIN', 'BUS'):
                seats = leg.get('free_seats')
                if seats not in (None, '') and not (isinstance(seats, float) and pd.isna(seats)):
                    seat_note = f" · {seats} seats free"
            out.append(f"        {leg['transport_name']} · "
                       f"{fmt_duration(leg['duration'])} · {leg['travel_date']}{seat_note}")

            if n < len(it) - 1:
                wait = it[n + 1]['t_start'] - leg['t_end']
                out.append(f"        wait {fmt_duration(wait)} in {leg['destination_city']}")
        out.append("")

    return "\n".join(out)


# ==========================================================
# CLI
# ==========================================================
def answer(df, question, limit=8, max_legs=2):
    q = parse_query(question)

    if not q['destination']:
        return ("Couldn't identify a destination. Try:\n"
                "  \"Nukus to Kashkadarya, cheapest\"")
    if not q['origin']:
        q['origin'] = 'Tashkent'

    if q['origin'] == q['destination']:
        return f"Origin and destination are both {q['origin']}."

    itineraries, notes = find_itineraries(
        df, q['origin'], q['destination'],
        date=q['date'], baggage_kg=q['baggage_kg'],
        modes=q['modes'], party_size=q['party_size'],
        max_legs=max_legs, limit=limit * 3
    )
    ranked = rank_itineraries(itineraries, q['preference'])[:limit]
    return format_itineraries(ranked, q, notes)


def main():
    if '--list-cities' in sys.argv:
        print("Known destinations:", ", ".join(sorted(set(CITY_ALIASES.values()))))
        return

    max_legs = 2
    if '--legs' in sys.argv:
        try:
            max_legs = int(sys.argv[sys.argv.index('--legs') + 1])
        except (IndexError, ValueError):
            print("--legs needs a number (2 = one change, 3 = two changes)")
            return

    skip = set()
    for i, arg in enumerate(sys.argv):
        if arg == '--legs':
            skip.update({i, i + 1})
    question = " ".join(a for i, a in enumerate(sys.argv[1:], 1)
                        if i not in skip and not a.startswith('--'))
    if not question:
        question = input("Where do you want to go? ")

    print("Loading schedules...")
    df = load_trips()
    dropped = df.attrs.get('dropped', 0)
    print(f"{len(df)} usable legs loaded"
          + (f" ({dropped} skipped: missing price or time)" if dropped else ""))

    print(answer(df, question, max_legs=max_legs))


if __name__ == "__main__":
    main()