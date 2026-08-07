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
    python route_engine.py --list-cities
"""

import re
import sys
import unicodedata
from datetime import datetime, timedelta

import pandas as pd
from sqlalchemy import create_engine, text

# Local development talks to SQL Server. A deployed copy has no access to
# your laptop, so it reads a SQLite snapshot instead (see
# export_to_sqlite.py). Set UZWAY_DB to switch:
#     set UZWAY_DB=sqlite:///uzway.db
import os

DB_CONNECTION_STRING = os.environ.get("UZWAY_DB") or (
    "mssql+pyodbc://@DESKTOP-5K5RC72/UzWayDB?"
    "driver=ODBC+Driver+17+for+SQL+Server&"
    "Trusted_Connection=yes&"
    "TrustServerCertificate=yes"
)

# ==========================================================
# CITY / REGION ALIASES
# ==========================================================
# Users say "Kashkadarya" (a region); the data has "Karshi" (its capital).
# Also covers Uzbek and Russian spellings. Keys are normalised to lowercase
# ASCII-ish before lookup, so add plain lowercase entries here.
CITY_ALIASES = {
    # Tashkent
    "tashkent": "Tashkent", "toshkent": "Tashkent", "ташкент": "Tashkent",
    "tashkend": "Tashkent", "tosh": "Tashkent",
    # Samarkand
    "samarkand": "Samarkand", "samarqand": "Samarkand", "самарканд": "Samarkand",
    # Bukhara
    "bukhara": "Bukhara", "buxoro": "Bukhara", "бухара": "Bukhara",
    "buhara": "Bukhara",
    # Khorezm / Urgench / Khiva
    "urgench": "Urgench", "urganch": "Urgench", "ургенч": "Urgench",
    "khorezm": "Urgench", "xorazm": "Urgench", "хорезм": "Urgench",
    "khiva": "Khiva", "xiva": "Khiva", "хива": "Khiva",
    # Karakalpakstan / Nukus
    "nukus": "Nukus", "нукус": "Nukus",
    "karakalpakstan": "Nukus", "qoraqalpogiston": "Nukus",
    "каракалпакстан": "Nukus",
    # Navoi
    "navoi": "Navoi", "navoiy": "Navoi", "навои": "Navoi", "navai": "Navoi",
    # Andijan
    "andijan": "Andijan", "andijon": "Andijan", "андижан": "Andijan",
    # Kashkadarya / Karshi
    "karshi": "Karshi", "qarshi": "Karshi", "карши": "Karshi",
    "kashkadarya": "Karshi", "qashqadaryo": "Karshi", "кашкадарья": "Karshi",
    # Jizzakh
    "djizakh": "Djizakh", "jizzakh": "Djizakh", "jizzax": "Djizakh",
    "джизак": "Djizakh", "dizak": "Djizakh",
    # Surkhandarya / Termez
    "termez": "Termez", "termiz": "Termez", "термез": "Termez",
    "surkhandarya": "Termez", "surxondaryo": "Termez", "сурхандарья": "Termez",
    # Sirdaryo / Gulistan
    "gulistan": "Gulistan", "guliston": "Gulistan", "гулистан": "Gulistan",
    "sirdaryo": "Gulistan", "syrdarya": "Gulistan", "сырдарья": "Gulistan",
    # Fergana valley
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


# Pre-normalise the alias table once.
_ALIAS_LOOKUP = {normalise(k): v for k, v in CITY_ALIASES.items()}


# Uzbek attaches case endings directly to the noun ("Toshkentdan Qarshiga"
# = "from Tashkent to Karshi"), so an exact match fails on real phrasing.
# Longest first, so "gacha" is tried before "ga".
_SUFFIXES = ["gacha", "ganacha", "dagi", "dan", "den", "tan", "ten",
             "gan", "ga", "qa", "ka", "da", "ta", "ni", "ning", "у", "е", "а"]


def resolve_city(name):
    """'Kashkadarya' -> 'Karshi', 'Toshkentdan' -> 'Tashkent'."""
    key = normalise(name)
    if not key:
        return None
    if key in _ALIAS_LOOKUP:
        return _ALIAS_LOOKUP[key]

    # Strip a grammatical ending and retry.
    for suf in _SUFFIXES:
        if key.endswith(suf) and len(key) - len(suf) >= 3:
            stem = key[:-len(suf)]
            if stem in _ALIAS_LOOKUP:
                return _ALIAS_LOOKUP[stem]

    # Last resort: unique prefix match ("samarq" -> Samarkand). Only
    # accepted when exactly one city matches, to avoid guessing wrong.
    if len(key) >= 4:
        hits = {v for k, v in _ALIAS_LOOKUP.items() if k.startswith(key)}
        if len(hits) == 1:
            return hits.pop()
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

    # Rows we cannot rank are worse than useless in a demo — they produce
    # blank cells and "None so'm" in the output. Drop them, but report how
    # many so the loss is visible rather than silent.
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
        delta += 24 * 60          # crossed midnight
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
        # A "piece" allowance is typically 20-23kg checked, ~8kg hand.
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
         'preference': 'balanced', 'date': None, 'modes': None}

    # "from X to Y" / "X to Y" / "X dan Y ga"
    pair = re.search(r'from\s+([\w\'\- ]+?)\s+to\s+([\w\'\- ]+?)(?:[,.]|\s+with|\s+and|\s+i\b|$)', low)
    if not pair:
        pair = re.search(r'([\w\']+)\s*(?:->|→|-)\s*([\w\']+)', low)
    if not pair:
        pair = re.search(r'([\w\'\- ]+?)\s+to\s+([\w\'\- ]+?)(?:[,.]|\s+with|\s+and|$)', low)

    if pair:
        q['origin'] = resolve_city(pair.group(1))
        q['destination'] = resolve_city(pair.group(2))

    # Fall back to scanning for any known city names in order.
    if not q['origin'] or not q['destination']:
        found = []
        for token in re.findall(r"[\w']+", low):
            city = resolve_city(token)
            if city and (not found or found[-1] != city):
                found.append(city)
        if len(found) >= 2:
            q['origin'] = q['origin'] or found[0]
            q['destination'] = q['destination'] or found[1]
        elif len(found) == 1 and not q['destination']:
            q['destination'] = found[0]

    # Baggage: "2kg", "2 kg", "2 kilo", "2кг"
    bag = re.search(r'(\d+(?:[.,]\d+)?)\s*(?:kg|kilo|kilogram|кг)', low)
    if bag:
        q['baggage_kg'] = float(bag.group(1).replace(',', '.'))

    # Preference
    for pref, patterns in PREF_PATTERNS.items():
        if any(re.search(p, low) for p in patterns):
            q['preference'] = pref
            break

    # Date
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

    # Explicit mode requests
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
# A journey is a list of legs. One leg = direct. Two legs = one change.
#
# Timing uses absolute minutes (day number * 1440 + clock time) so that a
# leg arriving 06:20 the morning after a 23:10 departure connects
# correctly to something leaving at 09:00 that same morning.

MIN_LAYOVER = 45        # minutes; below this a change isn't realistic
MAX_LAYOVER = 14 * 60   # beyond this it's an overnight stay, not a connection
MAX_BRANCH = 40         # candidate legs explored per intermediate city


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
    """Reversed copies of every leg, flagged as estimates.

    The scraper may not cover a direction. Prices and operators are
    usually symmetric, so a mirrored leg is a reasonable stand-in for a
    demo — but departure times are NOT the same service, which is why
    every mirrored leg is labelled and only used when nothing real exists.
    """
    m = df.copy()
    m['origin_city'], m['destination_city'] = df['destination_city'], df['origin_city']
    m['estimated'] = True
    m['trip_id'] = m['trip_id'].astype(str) + "-REV"
    return m


def build_index(df, baggage_kg=None, modes=None):
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

    index = {}
    for row in legs.to_dict('records'):
        index.setdefault(row['origin_city'], []).append(row)
    for city in index:
        index[city].sort(key=lambda r: r['t_start'])
    return index


def find_itineraries(df, origin, destination, date=None, baggage_kg=None,
                     modes=None, max_legs=2, limit=10, allow_mirrored=True):
    """Returns (itineraries, notes). Each itinerary is a list of leg dicts."""
    notes = []
    df = add_time_index(df)
    if 'estimated' not in df.columns:
        df['estimated'] = False

    def run(source_df):
        index = build_index(source_df, baggage_kg, modes)
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

            # Explore cheapest and earliest options first, then cap — without
            # this, a busy hub with 40 buses a day explodes the search.
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

    # Nothing real? Fall back to mirrored legs so the demo still answers,
    # clearly marked.
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
def itinerary_price(it):
    return sum(float(leg['price']) for leg in it if pd.notna(leg.get('price')))


def itinerary_duration(it):
    """Door to door, including time spent waiting between legs."""
    return it[-1]['t_end'] - it[0]['t_start']


def itinerary_transit(it):
    return sum(float(leg['duration']) for leg in it)


def rank_itineraries(itineraries, preference='balanced'):
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
        # Small penalty per change: a connection is real hassle even when
        # the numbers look good.
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

    out.append("")
    out.append("=" * 68)
    out.append(f"  {origin}  →  {dest}")
    bits = []
    if query.get('date'):
        bits.append(query['date'])
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

        out.append(f"  {i}. {label} · {fmt_price(itinerary_price(it))} · "
                   f"{fmt_duration(itinerary_duration(it))} total{est}")

        for n, leg in enumerate(it):
            arrow = "└─" if n == len(it) - 1 else "├─"
            tag = " (est.)" if leg.get('estimated') else ""
            out.append(f"     {arrow} {MODE_LABELS.get(leg['mode'], leg['mode'])}: "
                       f"{leg['origin_city']} {fmt_clock(leg, 'departure')} → "
                       f"{leg['destination_city']} {fmt_clock(leg, 'arrival')}"
                       f"   {fmt_price(leg['price'])}{tag}")
            out.append(f"        {leg['transport_name']} · "
                       f"{fmt_duration(leg['duration'])} · {leg['travel_date']}")

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
        modes=q['modes'], max_legs=max_legs, limit=limit * 3
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
