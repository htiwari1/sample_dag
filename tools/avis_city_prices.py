#!/usr/bin/env python3
"""Replays the exact public call avis.co.in's own booking page makes when a city is selected:
POST https://apisavis.avis.co.in/api/booking/getmodelprices  {"cityName": "<city>"}
No auth, no cookies, same body the page sends. Writes data/avis_city_prices.json."""
import json, sys, time, urllib.request
from datetime import datetime, timezone

URL = "https://apisavis.avis.co.in/api/booking/getmodelprices"
CITIES = sys.argv[1:] or ["Mumbai", "Thane", "Navi Mumbai", "Delhi - NCR", "Pune", "Bangalore"]
HEADERS = {"Content-Type": "application/json", "Origin": "https://www.avis.co.in",
           "Referer": "https://www.avis.co.in/one", "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) Chrome/130.0 Safari/537.36"}
out = {"fetched_at": datetime.now(timezone.utc).isoformat(timespec="seconds"), "cities": {}}
for city in CITIES:
    req = urllib.request.Request(URL, data=json.dumps({"cityName": city}).encode(), headers=HEADERS, method="POST")
    try:
        body = json.loads(urllib.request.urlopen(req, timeout=20).read())
    except Exception as exc:
        out["cities"][city] = {"error": str(exc)}
        continue
    rows = body.get("result") or []
    out["cities"][city] = [{k: r.get(k) for k in ("modelName", "avisOne", "baseprice", "slashprice",
                            "extra_km", "extra_hr", "taxprice", "discount", "spclprice", "dynamicprice", "short_desc")}
                           for r in rows]
    print(city, json.dumps(out["cities"][city]))
    time.sleep(2)
json.dump(out, open("data/avis_city_prices.json", "w"), indent=2)
