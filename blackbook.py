#!/usr/bin/env python3
"""
Blackbook: cross-channel price & inventory discovery for luxury services.

Subcommands
  capture  - drive public booking flows in Playwright, record every JSON XHR/fetch response
  infer    - infer schemas of captured endpoints, flag price/vehicle/inventory fields
  extract  - pull price observations out of captured responses into SQLite
  monitor  - capture + extract on a loop, score opportunities, fire alerts
  report   - print current best opportunities and per-channel history

Scope rules baked in:
  * Only public, unauthenticated pages you could load in a normal browser.
  * No cookie/token replay against endpoints you weren't served, no auth bypass.
  * Polite: one flow at a time, jittered delays, hard request cap per run.

Setup
  pip install playwright --break-system-packages   (or in a venv)
  playwright install chromium
  python blackbook.py init                 # writes providers.json + blackbook.db
  python blackbook.py capture --provider avis_one_direct --pickup "Thane West, Thane" \
        --dropoff "Chhatrapati Shivaji Maharaj International Airport Terminal 2" \
        --date 2026-12-05 --time 06:30
  python blackbook.py infer
  python blackbook.py extract
  python blackbook.py report

Selectors in providers.json are starting points: open each site once with
--headed, confirm the selectors, and edit the JSON. Everything else is generic.
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import random
import re
import sqlite3
import statistics
import sys
import time
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import urlparse

HERE = Path(__file__).resolve().parent
DB_PATH = Path(os.environ.get("BLACKBOOK_DB", HERE / "blackbook.db"))
PROVIDERS_PATH = Path(os.environ.get("BLACKBOOK_PROVIDERS", HERE / "providers.json"))
CAPTURE_DIR = Path(os.environ.get("BLACKBOOK_CAPTURES", HERE / "captures"))
ALERT_WEBHOOK = os.environ.get("BLACKBOOK_WEBHOOK", "")  # Slack/Discord/ntfy URL
MAX_RESPONSES_PER_RUN = 400
MAX_BODY_BYTES = 2_000_000

# --------------------------------------------------------------------------- #
# Provider definitions
# --------------------------------------------------------------------------- #

DEFAULT_PROVIDERS: dict[str, dict[str, Any]] = {
    # Same supplier (Avis India) exposed through three channels -> distribution arbitrage check.
    "avis_one_direct": {
        "supplier": "Avis India Mobility Solutions",
        "channel": "direct",
        "start_url": "https://www.avis.co.in/one",
        "vehicle_guarantee": "named_model",  # E-Class is sold as a named model, not "or similar"
        "gst_rate": 0.18,                    # verify at checkout; page footnotes have said 12% and 18%
        "tolls_included": True,
        "steps": [
            {"action": "goto", "url": "{start_url}"},
            {"action": "wait", "ms": 2500},
            {"action": "click_text", "text": "Chauffeur Drive", "optional": True},
            {"action": "fill", "selector": "input[placeholder*='Pickup' i]", "value": "{pickup}"},
            {"action": "wait", "ms": 1500},
            {"action": "press", "selector": "input[placeholder*='Pickup' i]", "key": "ArrowDown"},
            {"action": "press", "selector": "input[placeholder*='Pickup' i]", "key": "Enter"},
            {"action": "fill", "selector": "input[placeholder*='Drop' i]", "value": "{dropoff}"},
            {"action": "wait", "ms": 1500},
            {"action": "press", "selector": "input[placeholder*='Drop' i]", "key": "ArrowDown"},
            {"action": "press", "selector": "input[placeholder*='Drop' i]", "key": "Enter"},
            {"action": "click_text", "text": "Book Your Ride", "optional": True},
            {"action": "wait", "ms": 8000},
        ],
    },
    "avis_partner_airindia": {
        "supplier": "Avis India Mobility Solutions",
        "channel": "airline_partner",
        "start_url": "https://www.avis.co.in/partner/AirIndia",
        "vehicle_guarantee": "named_model",
        "gst_rate": 0.18,
        "tolls_included": True,
        "steps": "inherit:avis_one_direct",
    },
    "avis_partner_sc": {
        "supplier": "Avis India Mobility Solutions",
        "channel": "card_partner",
        "start_url": "https://www.avis.co.in/partner/sc",
        "vehicle_guarantee": "named_model",
        "gst_rate": 0.18,
        "tolls_included": True,
        "promo_note": "SCAVISONE: up to 10% off Avis One, BIN-locked to Standard Chartered Beyond",
        "steps": "inherit:avis_one_direct",
    },
    "transfeero": {
        "supplier": "Transfeero (local subcontractor)",
        "channel": "aggregator",
        "start_url": "https://www.transfeero.com/en/",
        "vehicle_guarantee": "or_similar",
        "gst_rate": 0.0,  # quoted all-inclusive
        "tolls_included": True,
        "steps": [
            {"action": "goto", "url": "{start_url}"},
            {"action": "wait", "ms": 2500},
            {"action": "fill", "selector": "input[name*='from' i], input[placeholder*='From' i]", "value": "{pickup}"},
            {"action": "wait", "ms": 1500},
            {"action": "press", "selector": "input[name*='from' i], input[placeholder*='From' i]", "key": "Enter"},
            {"action": "fill", "selector": "input[name*='to' i], input[placeholder*='To' i]", "value": "{dropoff}"},
            {"action": "wait", "ms": 1500},
            {"action": "press", "selector": "input[name*='to' i], input[placeholder*='To' i]", "key": "Enter"},
            {"action": "click_text", "text": "Search", "optional": True},
            {"action": "wait", "ms": 8000},
        ],
    },
    "blacklane": {
        "supplier": "Blacklane (local partner)",
        "channel": "aggregator_premium",
        "start_url": "https://www.blacklane.com/en/",
        "vehicle_guarantee": "or_similar",
        "gst_rate": 0.0,
        "tolls_included": True,
        "steps": [
            {"action": "goto", "url": "{start_url}"},
            {"action": "wait", "ms": 3000},
            {"action": "fill", "selector": "input[placeholder*='Address' i] >> nth=0", "value": "{pickup}"},
            {"action": "wait", "ms": 1500},
            {"action": "press", "selector": "input[placeholder*='Address' i] >> nth=0", "key": "Enter"},
            {"action": "fill", "selector": "input[placeholder*='Address' i] >> nth=1", "value": "{dropoff}"},
            {"action": "wait", "ms": 1500},
            {"action": "press", "selector": "input[placeholder*='Address' i] >> nth=1", "key": "Enter"},
            {"action": "click_text", "text": "Search", "optional": True},
            {"action": "wait", "ms": 8000},
        ],
    },
    "cityflo_luxe": {
        "supplier": "Cityflo LUXE",
        "channel": "direct",
        "start_url": "https://luxe.cityflo.com/airport-transfers",
        "vehicle_guarantee": "non_target",  # Innova/HyCross/BYD: benchmark only
        "gst_rate": 0.0,
        "tolls_included": True,
        "steps": [
            {"action": "goto", "url": "{start_url}"},
            {"action": "wait", "ms": 6000},
        ],
    },
}

TARGET_VEHICLE_PATTERNS = [
    (r"\b(e[\s-]?class|e\s?2\d0|e\s?350|eqe)\b", "mercedes_e"),
    (r"\b(c[\s-]?class|c\s?2\d0|c\s?300)\b", "mercedes_c"),
    (r"\b(s[\s-]?class|maybach)\b", "mercedes_s"),
    (r"\b(5[\s-]?series|5\d0[ide]|i5)\b", "bmw_5"),
    (r"\b(3[\s-]?series|3\d0[ide]|i4)\b", "bmw_3"),
    (r"\b(7[\s-]?series|7\d0[ide]|i7)\b", "bmw_7"),
    (r"\b(v[\s-]?class)\b", "mercedes_v"),
    (r"\b(business\s?class|business\s?sedan|standard\s?sedan|luxury\s?sedan)\b", "class_business"),
    (r"\b(first\s?class)\b", "class_first"),
]
ACCEPTABLE_NORMALIZED = {"mercedes_e", "mercedes_c", "mercedes_s", "bmw_5", "bmw_3", "bmw_7"}
PRICE_KEYS = re.compile(r"(price|fare|amount|total|cost|rate|tariff|charge)", re.I)
VEHICLE_KEYS = re.compile(r"(vehicle|car|model|category|class|name|title|type|product)", re.I)
CURRENCY_KEYS = re.compile(r"(currency|curr|iso)", re.I)
INVENTORY_KEYS = re.compile(r"(available|availability|sold.?out|inventory|stock|slots?|eta)", re.I)
TAX_KEYS = re.compile(r"(tax|gst|vat|toll|fee|surcharge)", re.I)
NON_PRICE_KEYS = re.compile(r"(id$|_id|ids?$|extra|per_?km|per_?hr|slash|strike|original|previous|security|deposit)", re.I)

FX_TO_INR = {"INR": 1.0, "USD": float(os.environ.get("BLACKBOOK_USDINR", "88.5")),
             "EUR": float(os.environ.get("BLACKBOOK_EURINR", "102.0")),
             "GBP": float(os.environ.get("BLACKBOOK_GBPINR", "117.0"))}


def load_providers() -> dict[str, dict[str, Any]]:
    data = json.loads(PROVIDERS_PATH.read_text()) if PROVIDERS_PATH.exists() else DEFAULT_PROVIDERS
    for name, p in data.items():
        if isinstance(p.get("steps"), str) and p["steps"].startswith("inherit:"):
            parent = p["steps"].split(":", 1)[1]
            p["steps"] = data[parent]["steps"]
    return data


# --------------------------------------------------------------------------- #
# Storage
# --------------------------------------------------------------------------- #

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS runs (
  run_id TEXT PRIMARY KEY, provider TEXT, started_at TEXT, params TEXT, n_responses INTEGER
);
CREATE TABLE IF NOT EXISTS responses (
  id INTEGER PRIMARY KEY AUTOINCREMENT, run_id TEXT, provider TEXT, captured_at TEXT,
  method TEXT, url TEXT, host TEXT, path TEXT, status INTEGER, req_body TEXT,
  body_sha TEXT, body_path TEXT
);
CREATE TABLE IF NOT EXISTS endpoints (
  host TEXT, path_template TEXT, provider TEXT, method TEXT, first_seen TEXT, last_seen TEXT,
  hits INTEGER, schema_json TEXT, price_paths TEXT, vehicle_paths TEXT, inventory_paths TEXT,
  PRIMARY KEY (host, path_template, method)
);
CREATE TABLE IF NOT EXISTS observations (
  id INTEGER PRIMARY KEY AUTOINCREMENT, observed_at TEXT, run_id TEXT, provider TEXT,
  supplier TEXT, channel TEXT, route_key TEXT, pickup_date TEXT, pickup_time TEXT,
  lead_hours REAL, vehicle_raw TEXT, vehicle_norm TEXT, price REAL, currency TEXT,
  price_inr_all_in REAL, taxes_included INTEGER, source_url TEXT, json_path TEXT,
  UNIQUE (run_id, provider, vehicle_raw, price, json_path)
);
CREATE TABLE IF NOT EXISTS alerts (
  id INTEGER PRIMARY KEY AUTOINCREMENT, created_at TEXT, observation_id INTEGER,
  score REAL, message TEXT
);
"""


def db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA_SQL)
    return conn


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


# --------------------------------------------------------------------------- #
# Capture (Playwright network inspection)
# --------------------------------------------------------------------------- #

@dataclass
class RouteParams:
    pickup: str
    dropoff: str
    date: str
    time: str
    pax: int = 1

    @property
    def key(self) -> str:
        return f"{self.pickup.lower().strip()}->{self.dropoff.lower().strip()}"

    def as_dict(self) -> dict[str, Any]:
        return {"pickup": self.pickup, "dropoff": self.dropoff, "date": self.date,
                "time": self.time, "pax": self.pax}


def render(template: str, ctx: dict[str, Any]) -> str:
    out = template
    for k, v in ctx.items():
        out = out.replace("{" + k + "}", str(v))
    return out


async def run_steps(page, steps: list[dict[str, Any]], ctx: dict[str, Any]) -> None:
    for step in steps:
        action = step["action"]
        optional = step.get("optional", False)
        try:
            if action == "goto":
                await page.goto(render(step["url"], ctx), wait_until="domcontentloaded", timeout=45000)
            elif action == "wait":
                await page.wait_for_timeout(step["ms"] + random.randint(0, 600))
            elif action == "fill":
                loc = page.locator(step["selector"]).first
                await loc.click(timeout=10000)
                await loc.fill("")
                await loc.type(render(step["value"], ctx), delay=random.randint(40, 90))
            elif action == "press":
                await page.locator(step["selector"]).first.press(step["key"], timeout=10000)
            elif action == "click":
                await page.locator(step["selector"]).first.click(timeout=10000)
            elif action == "click_text":
                await page.get_by_text(step["text"], exact=False).first.click(timeout=10000)
            elif action == "select":
                await page.locator(step["selector"]).first.select_option(render(step["value"], ctx))
            elif action == "scroll":
                await page.mouse.wheel(0, step.get("dy", 1500))
            else:
                raise ValueError(f"unknown action {action}")
        except Exception as exc:  # noqa: BLE001
            if optional:
                print(f"  [skip optional] {action}: {exc.__class__.__name__}")
                continue
            print(f"  [step failed] {action} {step}: {exc}")
            raise


async def capture(provider_name: str, params: RouteParams, headed: bool = False) -> str:
    from playwright.async_api import async_playwright

    providers = load_providers()
    prov = providers[provider_name]
    run_id = f"{provider_name}-{int(time.time())}-{random.randint(1000, 9999)}"
    run_dir = CAPTURE_DIR / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    conn = db()
    conn.execute("INSERT INTO runs VALUES (?,?,?,?,?)",
                 (run_id, provider_name, now_iso(), json.dumps(params.as_dict()), 0))
    conn.commit()
    counter = {"n": 0}
    pending: list[asyncio.Task] = []

    async def on_response(resp):
        if counter["n"] >= MAX_RESPONSES_PER_RUN:
            return
        req = resp.request
        if req.resource_type not in ("xhr", "fetch", "document", "other"):
            return
        ctype = (resp.headers or {}).get("content-type", "")
        if "json" not in ctype and "graphql" not in req.url.lower():
            return
        try:
            body = await resp.body()
        except Exception:  # noqa: BLE001
            return
        if not body or len(body) > MAX_BODY_BYTES:
            return
        counter["n"] += 1
        sha = hashlib.sha256(body).hexdigest()[:16]
        body_path = run_dir / f"{counter['n']:04d}-{sha}.json"
        body_path.write_bytes(body)
        u = urlparse(resp.url)
        conn.execute(
            "INSERT INTO responses (run_id, provider, captured_at, method, url, host, path, status,"
            " req_body, body_sha, body_path) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (run_id, provider_name, now_iso(), req.method, resp.url, u.netloc, u.path, resp.status,
             (req.post_data or "")[:20000], sha, str(body_path)))
        conn.commit()

    ctx = {**params.as_dict(), "start_url": prov["start_url"]}
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=not headed)
        context = await browser.new_context(
            locale="en-IN", timezone_id="Asia/Kolkata",
            viewport={"width": 1366, "height": 900},
        )
        page = await context.new_page()
        page.on("response", lambda r: pending.append(asyncio.ensure_future(on_response(r))))
        print(f"[capture] {provider_name} -> {prov['start_url']}")
        try:
            await run_steps(page, prov["steps"], ctx)
        finally:
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)
            (run_dir / "page.html").write_text(await page.content())
            await page.screenshot(path=str(run_dir / "final.png"), full_page=True)
            await browser.close()
    conn.execute("UPDATE runs SET n_responses=? WHERE run_id=?", (counter["n"], run_id))
    conn.commit()
    print(f"[capture] {run_id}: {counter['n']} JSON responses -> {run_dir}")
    return run_id


# --------------------------------------------------------------------------- #
# Schema inference
# --------------------------------------------------------------------------- #

ID_SEGMENT = re.compile(r"^([0-9]+|[0-9a-f]{8,}|[0-9a-f-]{32,36}|[A-Za-z0-9_-]{20,})$")


def path_template(path: str) -> str:
    return "/".join(":id" if ID_SEGMENT.match(seg) else seg for seg in path.split("/"))


def infer_schema(obj: Any, prefix: str = "$", out: dict[str, set] | None = None, depth: int = 0):
    out = out if out is not None else {}
    if depth > 12:
        return out
    if isinstance(obj, dict):
        out.setdefault(prefix, set()).add("object")
        for k, v in obj.items():
            infer_schema(v, f"{prefix}.{k}", out, depth + 1)
    elif isinstance(obj, list):
        out.setdefault(prefix, set()).add("array")
        for item in obj[:25]:
            infer_schema(item, f"{prefix}[]", out, depth + 1)
    else:
        out.setdefault(prefix, set()).add(type(obj).__name__)
    return out


def classify_paths(schema: dict[str, set]) -> tuple[list[str], list[str], list[str]]:
    price, vehicle, inventory = [], [], []
    for p, types in schema.items():
        leaf = p.rsplit(".", 1)[-1].replace("[]", "")
        if PRICE_KEYS.search(leaf) and types & {"int", "float", "str"}:
            price.append(p)
        if VEHICLE_KEYS.search(leaf) and "str" in types:
            vehicle.append(p)
        if INVENTORY_KEYS.search(leaf):
            inventory.append(p)
    return price, vehicle, inventory


def cmd_infer() -> None:
    conn = db()
    rows = conn.execute("SELECT * FROM responses").fetchall()
    for r in rows:
        try:
            data = json.loads(Path(r["body_path"]).read_text())
        except Exception:  # noqa: BLE001
            continue
        tmpl = path_template(r["path"])
        schema = infer_schema(data)
        existing = conn.execute(
            "SELECT schema_json, hits, first_seen FROM endpoints WHERE host=? AND path_template=? AND method=?",
            (r["host"], tmpl, r["method"])).fetchone()
        if existing:
            merged = {k: set(v) for k, v in json.loads(existing["schema_json"]).items()}
            for k, v in schema.items():
                merged.setdefault(k, set()).update(v)
            schema, hits, first = merged, existing["hits"] + 1, existing["first_seen"]
        else:
            hits, first = 1, r["captured_at"]
        pp, vp, ip = classify_paths(schema)
        conn.execute(
            "INSERT OR REPLACE INTO endpoints VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (r["host"], tmpl, r["provider"], r["method"], first, r["captured_at"], hits,
             json.dumps({k: sorted(v) for k, v in schema.items()}),
             json.dumps(pp), json.dumps(vp), json.dumps(ip)))
    conn.commit()
    print(f"{'provider':<24} {'method':<6} {'hits':>4}  endpoint  [price fields]")
    for e in conn.execute("SELECT * FROM endpoints ORDER BY provider, hits DESC"):
        pp = json.loads(e["price_paths"])
        if not pp:
            continue
        print(f"{e['provider']:<24} {e['method']:<6} {e['hits']:>4}  {e['host']}{e['path_template']}")
        for p in pp[:8]:
            print(f"{'':>38}{p}")


# --------------------------------------------------------------------------- #
# Extraction
# --------------------------------------------------------------------------- #

def to_number(v: Any) -> float | None:
    if isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, str):
        m = re.search(r"(\d[\d,]*\.?\d*)", v.replace("₹", ""))
        if m:
            try:
                return float(m.group(1).replace(",", ""))
            except ValueError:
                return None
    return None


def normalize_vehicle(text: str) -> str:
    t = text.lower()
    for pat, norm in TARGET_VEHICLE_PATTERNS:
        if re.search(pat, t):
            return norm
    return "other"


def walk_offers(obj: Any, path: str = "$") -> Iterable[dict[str, Any]]:
    """Yield dicts that look like an offer: a price-ish numeric + a vehicle-ish string nearby."""
    if isinstance(obj, dict):
        prices = {k: to_number(v) for k, v in obj.items()
                  if PRICE_KEYS.search(k) and not TAX_KEYS.search(k) and not NON_PRICE_KEYS.search(k)
                  and to_number(v)}
        labels = [str(v) for k, v in obj.items() if VEHICLE_KEYS.search(k) and isinstance(v, str)]
        # vehicle info is often one level down, e.g. {"vehicle": {"name": "..."}}
        for k, v in obj.items():
            if VEHICLE_KEYS.search(k) and isinstance(v, dict):
                labels += [str(x) for kk, x in v.items() if isinstance(x, str) and VEHICLE_KEYS.search(kk)]
        currency = next((str(v) for k, v in obj.items() if CURRENCY_KEYS.search(k) and isinstance(v, str)), None)
        tax_flag = any(TAX_KEYS.search(k) for k in obj.keys())
        if prices and labels:
            # prefer "total"/"final" over "base"
            ranked = sorted(prices.items(), key=lambda kv: (
                0 if re.search(r"(total|final|grand|payable|base_?price$|baseprice$|^price$|^fare$)", kv[0], re.I) else
                2 if re.search(r"(base|net|strike|original|was)", kv[0], re.I) else 1))
            key, val = ranked[0]
            yield {"price": val, "price_key": key, "label": " | ".join(labels)[:300],
                   "currency": currency, "has_tax_fields": tax_flag, "json_path": f"{path}.{key}"}
        for k, v in obj.items():
            yield from walk_offers(v, f"{path}.{k}")
    elif isinstance(obj, list):
        for i, item in enumerate(obj[:200]):
            yield from walk_offers(item, f"{path}[{i}]")


def lead_hours(date: str, tm: str, observed: datetime) -> float | None:
    try:
        dt = datetime.fromisoformat(f"{date}T{tm}:00+05:30")
        return round((dt - observed).total_seconds() / 3600, 1)
    except ValueError:
        return None


def all_in_inr(price: float, currency: str | None, prov: dict[str, Any], has_tax_fields: bool) -> tuple[float, bool]:
    cur = (currency or ("INR" if price > 400 else "USD")).upper()[:3]
    inr = price * FX_TO_INR.get(cur, 1.0)
    gst = float(prov.get("gst_rate", 0.0))
    taxes_included = gst == 0.0 or has_tax_fields
    if not taxes_included:
        inr *= 1 + gst
    return round(inr, 2), taxes_included


def cmd_extract(run_ids: list[str] | None = None) -> int:
    conn = db()
    providers = load_providers()
    q = "SELECT r.*, runs.params FROM responses r JOIN runs USING(run_id)"
    rows = conn.execute(q + (" WHERE run_id IN (%s)" % ",".join("?" * len(run_ids)) if run_ids else ""),
                        run_ids or []).fetchall()
    n = 0
    for r in rows:
        prov = providers.get(r["provider"], {})
        params = json.loads(r["params"])
        try:
            data = json.loads(Path(r["body_path"]).read_text())
        except Exception:  # noqa: BLE001
            continue
        observed = datetime.fromisoformat(r["captured_at"])
        for offer in walk_offers(data):
            if offer["price"] < 50 or offer["price"] > 500_000:
                continue
            inr, tax_in = all_in_inr(offer["price"], offer["currency"], prov, offer["has_tax_fields"])
            try:
                cur = conn.execute(
                    "INSERT OR IGNORE INTO observations (observed_at, run_id, provider, supplier, channel,"
                    " route_key, pickup_date, pickup_time, lead_hours, vehicle_raw, vehicle_norm, price,"
                    " currency, price_inr_all_in, taxes_included, source_url, json_path)"
                    " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (r["captured_at"], r["run_id"], r["provider"], prov.get("supplier"), prov.get("channel"),
                     f"{params['pickup'].lower()}->{params['dropoff'].lower()}", params["date"], params["time"],
                     lead_hours(params["date"], params["time"], observed), offer["label"],
                     normalize_vehicle(offer["label"]), offer["price"], offer["currency"], inr,
                     int(tax_in), r["url"], offer["json_path"]))
                n += cur.rowcount
            except sqlite3.Error as exc:
                print(f"  [db] {exc}")
    conn.commit()
    print(f"[extract] processed {len(rows)} responses, {n} new observations")
    return n


# --------------------------------------------------------------------------- #
# Scoring, cross-channel matching, alerts
# --------------------------------------------------------------------------- #

GUARANTEE_WEIGHT = {"named_model": 1.0, "or_similar": 0.6, "non_target": 0.0}


@dataclass
class Opportunity:
    obs: sqlite3.Row
    score: float
    reasons: list[str] = field(default_factory=list)


def score_observation(conn: sqlite3.Connection, o: sqlite3.Row, target_inr: float,
                      providers: dict[str, dict[str, Any]]) -> Opportunity:
    prov = providers.get(o["provider"], {})
    reasons: list[str] = []
    s = 0.0
    if o["vehicle_norm"] in ACCEPTABLE_NORMALIZED:
        s += 30
        reasons.append(f"target model ({o['vehicle_norm']})")
    elif o["vehicle_norm"] == "class_business":
        s += 15
        reasons.append("business-class bucket, model not named")
    else:
        return Opportunity(o, 0.0, ["non-target vehicle"])
    s *= GUARANTEE_WEIGHT.get(prov.get("vehicle_guarantee", "or_similar"), 0.5)
    if prov.get("vehicle_guarantee") == "or_similar":
        reasons.append("'or similar' fleet: confirm make in writing")
    price = o["price_inr_all_in"]
    if price <= target_inr:
        s += 40 + min(20, (target_inr - price) / target_inr * 100)
        reasons.append(f"under target by INR {target_inr - price:,.0f}")
    else:
        s += max(0, 20 - (price - target_inr) / target_inr * 100)
        reasons.append(f"over target by INR {price - target_inr:,.0f}")
    hist = [r[0] for r in conn.execute(
        "SELECT price_inr_all_in FROM observations WHERE provider=? AND vehicle_norm=? AND id<>?",
        (o["provider"], o["vehicle_norm"], o["id"]))]
    if len(hist) >= 3:
        med = statistics.median(hist)
        if price < med * 0.9:
            s += 10
            reasons.append(f"{(1 - price / med) * 100:.0f}% below this channel's median")
    same_supplier = conn.execute(
        "SELECT channel, MIN(price_inr_all_in) p FROM observations WHERE supplier=? AND vehicle_norm=?"
        " AND provider<>? GROUP BY channel", (o["supplier"], o["vehicle_norm"], o["provider"])).fetchall()
    for row in same_supplier:
        if row["p"] and price < row["p"] * 0.95:
            s += 10
            reasons.append(f"cheaper than same supplier via {row['channel']} (INR {row['p']:,.0f})")
    if not o["taxes_included"]:
        reasons.append(f"GST added at {prov.get('gst_rate', 0) * 100:.0f}% (estimated)")
    return Opportunity(o, round(s, 1), reasons)


def send_alert(msg: str) -> None:
    print(f"[ALERT] {msg}")
    if not ALERT_WEBHOOK:
        return
    payload = json.dumps({"text": msg, "content": msg}).encode()
    req = urllib.request.Request(ALERT_WEBHOOK, data=payload, headers={"Content-Type": "application/json"})
    try:
        urllib.request.urlopen(req, timeout=10).read()
    except Exception as exc:  # noqa: BLE001
        print(f"  [alert webhook failed] {exc}")


def cmd_report(target_inr: float, alert_threshold: float | None = None, top: int = 15) -> None:
    conn = db()
    providers = load_providers()
    obs = conn.execute(
        "SELECT * FROM observations WHERE observed_at >= datetime('now','-14 days') ORDER BY observed_at DESC"
    ).fetchall()
    opps = sorted((score_observation(conn, o, target_inr, providers) for o in obs),
                  key=lambda x: -x.score)
    seen = set()
    print(f"\nTop opportunities (target INR {target_inr:,.0f} all-in)\n")
    for op in opps:
        o = op.obs
        k = (o["provider"], o["vehicle_norm"], round(o["price_inr_all_in"]))
        if k in seen or op.score == 0:
            continue
        seen.add(k)
        print(f"{op.score:5.1f}  {o['provider']:<22} {o['vehicle_norm']:<14} INR {o['price_inr_all_in']:>9,.0f}"
              f"  lead {o['lead_hours']}h  {o['observed_at']}")
        print(f"       {o['vehicle_raw'][:110]}")
        print(f"       {'; '.join(op.reasons)}")
        if alert_threshold is not None and op.score >= alert_threshold:
            exists = conn.execute("SELECT 1 FROM alerts WHERE observation_id=?", (o["id"],)).fetchone()
            if not exists:
                msg = (f"Blackbook: {o['provider']} {o['vehicle_norm']} INR {o['price_inr_all_in']:,.0f} "
                       f"for {o['route_key']} on {o['pickup_date']} {o['pickup_time']} (score {op.score})")
                send_alert(msg)
                conn.execute("INSERT INTO alerts (created_at, observation_id, score, message) VALUES (?,?,?,?)",
                             (now_iso(), o["id"], op.score, msg))
                conn.commit()
        if len(seen) >= top:
            break
    print("\nPer-channel history (min / median / n)")
    for r in conn.execute(
            "SELECT provider, vehicle_norm, MIN(price_inr_all_in) mn, COUNT(*) n FROM observations"
            " GROUP BY provider, vehicle_norm HAVING vehicle_norm<>'other' ORDER BY mn"):
        vals = [x[0] for x in conn.execute(
            "SELECT price_inr_all_in FROM observations WHERE provider=? AND vehicle_norm=?",
            (r["provider"], r["vehicle_norm"]))]
        print(f"  {r['provider']:<22} {r['vehicle_norm']:<14} {r['mn']:>9,.0f} / "
              f"{statistics.median(vals):>9,.0f} / {r['n']}")


# --------------------------------------------------------------------------- #
# Monitor loop (sweeps pickup times + lead times to expose dynamic pricing)
# --------------------------------------------------------------------------- #

async def monitor(providers_to_run: list[str], base: RouteParams, times: list[str], dates: list[str],
                  target_inr: float, threshold: float, interval_min: int, once: bool) -> None:
    while True:
        for prov in providers_to_run:
            for d in dates:
                for t in times:
                    p = RouteParams(base.pickup, base.dropoff, d, t, base.pax)
                    try:
                        run_id = await capture(prov, p)
                        cmd_extract([run_id])
                    except Exception as exc:  # noqa: BLE001
                        print(f"[monitor] {prov} {d} {t} failed: {exc}")
                    await asyncio.sleep(random.uniform(20, 45))
        cmd_report(target_inr, alert_threshold=threshold)
        if once:
            return
        await asyncio.sleep(interval_min * 60)


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #

def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)

    sub.add_parser("init")

    def route_args(p):
        p.add_argument("--pickup", required=True)
        p.add_argument("--dropoff", required=True)
        p.add_argument("--pax", type=int, default=1)

    c = sub.add_parser("capture")
    c.add_argument("--provider", required=True)
    route_args(c)
    c.add_argument("--date", required=True)
    c.add_argument("--time", required=True)
    c.add_argument("--headed", action="store_true")

    sub.add_parser("infer")
    sub.add_parser("extract")

    r = sub.add_parser("report")
    r.add_argument("--target-inr", type=float, default=4400)
    r.add_argument("--alert-threshold", type=float, default=None)

    m = sub.add_parser("monitor")
    m.add_argument("--providers", default="avis_one_direct,avis_partner_airindia,avis_partner_sc,transfeero,blacklane")
    route_args(m)
    m.add_argument("--dates", required=True, help="comma-separated YYYY-MM-DD")
    m.add_argument("--times", default="04:30,06:30,10:00,14:00,22:30")
    m.add_argument("--target-inr", type=float, default=4400)
    m.add_argument("--alert-threshold", type=float, default=70)
    m.add_argument("--interval-min", type=int, default=180)
    m.add_argument("--once", action="store_true")

    a = ap.parse_args(argv)
    if a.cmd == "init":
        if not PROVIDERS_PATH.exists():
            PROVIDERS_PATH.write_text(json.dumps(DEFAULT_PROVIDERS, indent=2))
        db().close()
        CAPTURE_DIR.mkdir(exist_ok=True)
        print(f"wrote {PROVIDERS_PATH} and {DB_PATH}")
    elif a.cmd == "capture":
        rid = asyncio.run(capture(a.provider, RouteParams(a.pickup, a.dropoff, a.date, a.time, a.pax), a.headed))
        cmd_extract([rid])
    elif a.cmd == "infer":
        cmd_infer()
    elif a.cmd == "extract":
        cmd_extract()
    elif a.cmd == "report":
        cmd_report(a.target_inr, a.alert_threshold)
    elif a.cmd == "monitor":
        asyncio.run(monitor(a.providers.split(","), RouteParams(a.pickup, a.dropoff, "", "", a.pax),
                            a.times.split(","), a.dates.split(","), a.target_inr,
                            a.alert_threshold, a.interval_min, a.once))


if __name__ == "__main__":
    main(sys.argv[1:])
