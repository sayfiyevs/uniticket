# UniTicket — multi-modal route search for Uzbekistan

**Note: The site is not currently available!!**

Ask for a journey in plain language and get ranked travel options across
trains, buses and flights — including journeys that require changing
transport along the way.

```
"I want to travel from Tashkent to Kashkadarya and I have 2kg baggage,
 I want the cheapest way"
```

```
1. Direct · 145 000 so'm · 7h 10m
   └─ Train: Tashkent 23:10 → Karshi 06:20
      Sharq · 7h 10m · 2026-08-09

2. 1 change · 180 000 so'm · 15h 30m
   ├─ Bus: Nukus 06:00 → Bukhara 15:00   120 000 so'm
      wait 3h 00m in Bukhara
   └─ Train: Bukhara 18:00 → Karshi 21:30   60 000 so'm
```

---

## What it does

**Understands natural language.** "Kashkadarya" resolves to Karshi (the
region's capital, which is what the ticketing system actually calls it).
Uzbek case endings work — `toshkentdan qarshiga` parses correctly — as do
Russian and English city names. It extracts baggage weight, party size,
dates, preferred transport, and whether you want cheapest or fastest.

**Finds connections, not just direct routes.** Many Uzbek city pairs have
no direct service. The engine searches for journeys with one or two
changes, checking that each connection is actually catchable: at least 45
minutes to change, and not so long that it becomes an overnight stay.
Times are handled in absolute minutes, so an overnight train arriving
06:20 correctly connects to a 09:00 departure the next morning.

**Ranks honestly.** *Cheapest* sorts on total price. *Fastest* sorts on
door-to-door time including waiting. *Balanced* normalises both to 0–1 and
adds a small penalty per change, because a connection is real hassle even
when the numbers look good.

**Filters by baggage.** Only flights restrict luggage — a 2kg bag fits an
8KG cabin allowance but not a fare with no allowance at all. Trains and
buses always pass. The interface reports how many fares were excluded
rather than silently dropping them.

---

## Why this isn't machine learning

"Find the cheapest way from A to B" over a table of schedules is a graph
search problem with an exact answer. A trained model would approximate
that answer worse, require labelled training data that doesn't exist, and
occasionally invent routes that aren't real.

Everything here is deterministic. Every result traces back to specific
rows in the database, which matters when someone asks how it works.

Machine learning would earn its place on the layer above — predicting
whether a price will drop before departure, scoring which routes reliably
have inventory, learning that a user who asks for "fastest" still won't
accept triple the price. Those need historical data this project is only
now starting to accumulate.

---

## How the data gets here

There is no public API for Uzbek transport schedules. The data comes from
ATTO's own ticketing endpoints, reverse-engineered from browser traffic.

```
atto_scraper.py       trains, buses, flights → SQL Server
collect_network.py    every city pair, both directions (ground transport)
backfill_flights.py   resumable flight collection across the full horizon
export_to_sqlite.py   snapshot the database into one portable file
route_engine.py       parsing, connection search, ranking
app.py                Flask web interface
```

Three details that took real work:

**The booking horizon is discovered, not hardcoded.** `discover_horizon()`
walks forward day by day until three consecutive days return nothing, then
saves that number. Currently about 25 days. It runs once nightly rather
than on every cycle, because rediscovering a 25-day horizon every ten
minutes would multiply request volume for no benefit.

**Flight results arrive while the search is still "pending".** ATTO
aggregates across vendors and streams offers in as they respond. An
earlier version only read results once the status flipped to `finished`,
which discarded complete result sets on any route that ran out of polling
budget — roughly a quarter of them. The poller now keeps the fullest
snapshot it has seen.

**One row per fare, not per flight.** The same physical flight comes back
several times with different baggage allowances and very different prices
— 288,943 vs 587,457 so'm on one Centrum Air departure. Keying on flight
number alone collapsed them and kept an arbitrary one.

---

## Running it locally

Requires Python 3.11, SQL Server with ODBC Driver 17, and a valid ATTO
access token.

```bash
pip install -r requirements.txt
python app.py
```

Open `http://127.0.0.1:5000`.

The local build includes an operations console — trigger collection jobs,
watch their output stream live, inspect the horizon and backfill state.
That surface is disabled in the deployed copy.

### Deployed mode

```bash
UZWAY_PUBLIC=1              # search only; every job route returns 404
UZWAY_DB=sqlite:///uzway.db # read the snapshot instead of SQL Server
```

The public deployment reads a SQLite snapshot rather than a live database,
so it shows data as of the last export. Refresh by re-running
`export_to_sqlite.py` and redeploying.

---

## Known limitations (as it is just a prototype)

Worth stating plainly rather than discovering during a demo.

**Durations for ground transport are approximate.** The API returns clock
times without dates, so a departure at 23:10 arriving 06:20 is assumed to
take 7h10m. A trip genuinely lasting 31 hours would look identical. Fine
for ranking, not for planning a connection you'd miss.

**The engine has no map.** It knows which legs exist, not where cities
are, so it can propose a journey that doubles back geographically if the
numbers happen to work. Options beaten on both price and time by another
option are pruned, which removes most of these.

**Flights are collected outbound from Tashkent only.** Uzbek domestic
aviation is hub-and-spoke, so this covers most real cases, but journeys
needing an inbound flight leg fall back to mirrored schedules, labelled
`[ESTIMATED]` in the interface.

**Seat availability is unknown for flights.** ATTO's offers endpoint
doesn't expose it. Trains and buses report real counts. A flight appearing
in results means at least one seat exists, not how many.

**Snapshot, not live data.** See deployed mode above.

---

## Legal note

This scrapes an undocumented third-party API. It was built for a hackathon
and is not affiliated with ATTO or any operator. Anyone extending it for
production use should check ATTO's terms of service first — many sites
prohibit automated collection even of publicly visible data.
