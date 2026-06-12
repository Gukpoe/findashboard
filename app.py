"""FinDashboard web - live U.S. + Singapore stocks dashboard.

Sections:
  - Steady premium generators: low-beta U.S. defensives with liquid options
    (thin option chains excluded via CBOE volume).
  - Unusual volume surge (U.S.): 15 min / 1 hour / 1 day / 1 week / 1 month.
  - Unusual volume surge (Singapore STI): 1 day / 1 week / 1 month.

Data: finviz.com screeners (scraped), CBOE delayed option quotes,
Yahoo Finance chart + spark APIs. A background thread rebuilds the page
every REFRESH_SECONDS; the page polls /quotes every 15 s for live prices.

Run locally:   python app.py            (http://localhost:8588)
Production:    waitress-serve --host=0.0.0.0 --port=$PORT app:app
"""

import json
import os
import re
import threading
import time
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from html import escape, unescape
from pathlib import Path
from xml.etree import ElementTree
from zoneinfo import ZoneInfo

import requests
from flask import Flask, Response

# ---------------- config ----------------

PORT = int(os.environ.get("PORT", "8588"))
REFRESH_SECONDS = int(os.environ.get("REFRESH_SECONDS", "60"))
MIN_OPT_VOLUME = int(os.environ.get("MIN_OPT_VOLUME", "5000"))
DATA_DIR = Path(os.environ.get("FINDASH_DATA", Path.home() / ".findash-data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0 Safari/537.36")

STABLE_INFO = {
    "VZ": "Verizon Communications", "T": "AT&T", "MRK": "Merck & Co",
    "JNJ": "Johnson & Johnson", "ED": "Consolidated Edison", "KMB": "Kimberly-Clark",
    "CL": "Colgate-Palmolive", "KO": "Coca-Cola", "SO": "Southern Company",
    "PEP": "PepsiCo", "PG": "Procter & Gamble", "DUK": "Duke Energy",
    "GILD": "Gilead Sciences", "MO": "Altria Group", "GIS": "General Mills",
    "ARCC": "Ares Capital",
}
STABLE_TICKERS = ",".join(STABLE_INFO)
FORCE_YAHOO = os.environ.get("FORCE_YAHOO", "") == "1"
STABLE_URL = f"https://finviz.com/screener.ashx?v=171&t={STABLE_TICKERS}&o=ticker"
SURGE_URLS = [
    "https://finviz.com/screener.ashx?v=141&f=cap_smallover,sh_relvol_o1.5,sh_avgvol_o300&o=-relativevolume&r=1",
    "https://finviz.com/screener.ashx?v=141&f=cap_smallover,sh_relvol_o1.5,sh_avgvol_o300&o=-relativevolume&r=21",
]

# STI constituents (Yahoo .SI symbols); fetch failures degrade gracefully
STI_LIST = [
    ("D05.SI", "DBS Group"), ("O39.SI", "OCBC Bank"), ("U11.SI", "UOB"),
    ("Z74.SI", "Singtel"), ("S63.SI", "ST Engineering"), ("C6L.SI", "Singapore Airlines"),
    ("S68.SI", "Singapore Exchange"), ("BN4.SI", "Keppel"), ("U96.SI", "Sembcorp Industries"),
    ("5E2.SI", "Seatrium"), ("9CI.SI", "CapitaLand Investment"),
    ("C38U.SI", "CapitaLand Integrated Comm Trust"), ("A17U.SI", "CapitaLand Ascendas REIT"),
    ("G13.SI", "Genting Singapore"), ("F34.SI", "Wilmar International"),
    ("Y92.SI", "Thai Beverage"), ("C52.SI", "ComfortDelGro"), ("S58.SI", "SATS"),
    ("C09.SI", "City Developments"), ("U14.SI", "UOL Group"), ("H78.SI", "Hongkong Land"),
    ("D01.SI", "DFI Retail Group"), ("C07.SI", "Jardine Cycle & Carriage"),
    ("J36.SI", "Jardine Matheson"), ("M44U.SI", "Mapletree Logistics Trust"),
    ("ME8U.SI", "Mapletree Industrial Trust"), ("N2IU.SI", "Mapletree PanAsia Comm Trust"),
    ("BUOU.SI", "Frasers Logistics & Comm Trust"), ("J69U.SI", "Frasers Centrepoint Trust"),
    ("V03.SI", "Venture Corporation"), ("BS6.SI", "Yangzijiang Shipbuilding"),
    ("AJBU.SI", "Keppel DC REIT"),
]

NEWS_FEEDS = [
    ("CNBC", "https://www.cnbc.com/id/100003114/device/rss/rss.html"),
    ("MarketWatch", "https://feeds.content.dowjones.io/public/rss/mw_topstories"),
    ("Yahoo Finance", "https://finance.yahoo.com/news/rssindex"),
]

# keyword -> tag shown on the headline; used to rank macro-relevant news first
MACRO_KEYWORDS = {
    "fed": "Fed", "fomc": "Fed", "powell": "Fed", "interest rate": "Rates",
    "rate cut": "Rates", "rate hike": "Rates", "treasury": "Rates", "yield": "Rates",
    "inflation": "Inflation", "cpi": "Inflation", "ppi": "Inflation", "pce": "Inflation",
    "payroll": "Jobs", "jobs report": "Jobs", "unemployment": "Jobs",
    "trump": "Trump", "white house": "Washington", "congress": "Washington",
    "shutdown": "Washington", "debt ceiling": "Washington", "election": "Politics",
    "tariff": "Trade", "trade war": "Trade", "china": "China",
    "iran": "Middle East", "israel": "Middle East", "middle east": "Middle East",
    "hormuz": "Middle East", "gaza": "Middle East",
    "oil": "Oil", "opec": "Oil", "crude": "Oil",
    "war": "Geopolitics", "sanction": "Geopolitics", "nato": "Geopolitics",
    "ukraine": "Geopolitics", "russia": "Geopolitics", "geopolit": "Geopolitics",
    "gdp": "Economy", "recession": "Economy", "stimulus": "Economy",
    "dollar": "FX", "ecb": "Central banks", "boj": "Central banks",
    "spacex": "SpaceX", "musk": "Musk", "gold": "Gold", "bitcoin": "Crypto",
}

NEWS_CACHE = {"t": 0.0, "items": []}
CAL_CACHE = {"t": 0.0, "events": []}

STATE = {"html": None, "tickers": [], "built_at": None}
QUOTE_CACHE = {"t": 0.0, "json": "{}"}
BAD_QUOTE_SYMS = set()
_lock = threading.Lock()


def log(msg):
    print(f"[{datetime.now():%H:%M:%S}] {msg}", flush=True)


def fetch(url, timeout=30, retries=3):
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(url, headers={"User-Agent": UA}, timeout=timeout)
            r.raise_for_status()
            return r.text
        except Exception as e:
            log(f"fetch attempt {attempt} failed: {e}")
            if attempt < retries:
                time.sleep(5 * attempt)
    return None


def load_json(path):
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_json(path, obj):
    try:
        Path(path).write_text(json.dumps(obj, separators=(",", ":")), encoding="utf-8")
    except Exception as e:
        log(f"could not save {path}: {e}")


# ---------------- finviz parsing ----------------

def convert_num(s):
    if s is None:
        return None
    s = re.sub(r"<[^>]+>", "", s).strip().replace("%", "").replace(",", "")
    if s in ("-", ""):
        return None
    mult = 1.0
    if s.endswith("B"):
        mult, s = 1e9, s[:-1]
    elif s.endswith("M"):
        mult, s = 1e6, s[:-1]
    elif s.endswith("K"):
        mult, s = 1e3, s[:-1]
    try:
        return float(s) * mult
    except ValueError:
        return None


def parse_screener_rows(html):
    rows = []
    if not html:
        return rows
    for chunk in html.split('<tr class="styled-row')[1:]:
        end = chunk.find("</tr>")
        if end > 0:
            chunk = chunk[:end]
        m = re.search(r'data-boxover-ticker="([^"]+)"', chunk)
        if not m:
            continue
        ticker = m.group(1)
        cm = re.search(r'data-boxover-company="([^"]*)"', chunk)
        company = unescape(cm.group(1)) if cm else ""
        cells = [
            unescape(re.sub(r"<[^>]+>", "", c).strip())
            for c in re.findall(r'<a href="[^"]*\bt=[A-Za-z0-9.\-]+[^"]*"[^>]*>(.*?)</a>', chunk, re.S)
        ]
        rows.append({"ticker": ticker, "company": company, "cells": cells})
    return rows


def get_stable_data():
    rows = parse_screener_rows(fetch(STABLE_URL))
    out = []
    for r in rows:
        c = r["cells"]
        if len(c) < 12:
            continue
        # v=171 technical view: [2]=Beta [3]=ATR [9]=RSI [10]=Price [11]=Change [-1]=Volume
        price = convert_num(c[10])
        if not price or price <= 0:
            continue
        atr = convert_num(c[3])
        out.append({
            "ticker": r["ticker"], "company": r["company"],
            "beta": convert_num(c[2]),
            "atr_pct": round(100.0 * atr / price, 1) if atr else None,
            "rsi": convert_num(c[9]), "price": price,
            "change": convert_num(c[11]), "volume": convert_num(c[-1]),
        })
    out.sort(key=lambda s: s["beta"] if s["beta"] is not None else 99)
    return out


def get_surge_data():
    rows = []
    for url in SURGE_URLS:
        rows += parse_screener_rows(fetch(url))
        time.sleep(2)
    out, seen = [], set()
    for r in rows:
        c = r["cells"]
        if len(c) < 8 or r["ticker"] in seen:
            continue
        seen.add(r["ticker"])
        # v=141 performance view: [2]=PerfWeek [3]=PerfMonth; from end:
        # [-1]=Volume [-2]=Change [-3]=Price [-4]=RelVolume [-5]=AvgVolume
        out.append({
            "ticker": r["ticker"], "company": r["company"],
            "perf_week": convert_num(c[2]), "perf_month": convert_num(c[3]),
            "avg_vol": convert_num(c[-5]), "rel_vol": convert_num(c[-4]),
            "price": convert_num(c[-3]), "change": convert_num(c[-2]),
            "volume": convert_num(c[-1]),
        })
    return out


# ---------------- Yahoo fallbacks (Finviz blocks many datacenter IPs) ----------------

def _rsi14(closes):
    if len(closes) < 15:
        return None
    deltas = [closes[i] - closes[i - 1] for i in range(len(closes) - 14, len(closes))]
    avg_gain = sum(d for d in deltas if d > 0) / 14
    avg_loss = sum(-d for d in deltas if d < 0) / 14
    if avg_loss == 0:
        return 100
    return round(100 - 100 / (1 + avg_gain / avg_loss))


def _beta(rets, mkt_rets):
    n = min(len(rets), len(mkt_rets))
    if n < 30:
        return None
    r, m = rets[-n:], mkt_rets[-n:]
    mr, mm = sum(r) / n, sum(m) / n
    var = sum((x - mm) ** 2 for x in m) / n
    if var == 0:
        return None
    cov = sum((r[i] - mr) * (m[i] - mm) for i in range(n)) / n
    return round(cov / var, 2)


def get_stable_data_yahoo():
    cache = load_json(DATA_DIR / "yh_stable.json")
    if cache.get("t") and time.time() - cache["t"] < 600:
        return cache["rows"]
    try:
        mres = yahoo_chart("^GSPC")
        mcloses = [c for c in mres["indicators"]["quote"][0]["close"] if c]
        mrets = [mcloses[i] / mcloses[i - 1] - 1 for i in range(1, len(mcloses))]
    except Exception as e:
        log(f"  S&P chart failed: {e}")
        mrets = []
    rows = []
    for t, name in STABLE_INFO.items():
        try:
            res = yahoo_chart(t)
            q = res["indicators"]["quote"][0]
            bars = [(c, h, l, v) for c, h, l, v in
                    zip(q["close"], q["high"], q["low"], q["volume"]) if c and h and l]
            if len(bars) < 30:
                continue
            closes = [b[0] for b in bars]
            price = float(res["meta"]["regularMarketPrice"])
            atr = sum(h - l for _, h, l, _ in bars[-14:]) / 14
            rets = [closes[i] / closes[i - 1] - 1 for i in range(1, len(closes))]
            rows.append({
                "ticker": t, "company": name,
                "beta": _beta(rets, mrets),
                "atr_pct": round(100 * atr / price, 1),
                "rsi": _rsi14(closes), "price": price,
                "change": round((price / closes[-2] - 1) * 100, 2),
                "volume": bars[-1][3],
            })
            time.sleep(0.25)
        except Exception as e:
            log(f"  stable fallback failed for {t}: {e}")
    rows.sort(key=lambda s: s["beta"] if s["beta"] is not None else 99)
    save_json(DATA_DIR / "yh_stable.json", {"t": time.time(), "rows": rows})
    return rows


def get_surge_data_yahoo():
    out, seen = [], set()
    for scr in ("most_actives", "day_gainers", "day_losers"):
        try:
            r = requests.get(
                "https://query1.finance.yahoo.com/v1/finance/screener/predefined/saved"
                f"?scrIds={scr}&count=100&region=US",
                headers={"User-Agent": UA}, timeout=20)
            r.raise_for_status()
            for q in r.json()["finance"]["result"][0]["quotes"]:
                sym = q.get("symbol", "")
                if not sym or sym in seen or q.get("quoteType") != "EQUITY":
                    continue
                avg = q.get("averageDailyVolume3Month") or 0
                vol = q.get("regularMarketVolume") or 0
                cap = q.get("marketCap")
                if avg < 300_000 or not vol or (cap is not None and cap < 300e6):
                    continue
                rel = round(vol / avg, 1)
                if rel < 1.2:
                    continue
                seen.add(sym)
                out.append({
                    "ticker": sym,
                    "company": q.get("shortName") or q.get("longName") or "",
                    "perf_week": None, "perf_month": None,
                    "avg_vol": avg, "rel_vol": rel,
                    "price": q.get("regularMarketPrice"),
                    "change": round(q.get("regularMarketChangePercent") or 0, 2),
                    "volume": vol,
                })
        except Exception as e:
            log(f"  yahoo screener {scr} failed: {e}")
    out.sort(key=lambda s: -s["rel_vol"])
    return out[:40]


# ---------------- option volume (CBOE) ----------------

def get_option_volumes(tickers):
    path = DATA_DIR / "optvol.json"
    cache = load_json(path)
    out, dirty = {}, False
    for t in tickers:
        entry = cache.get(t)
        if entry and time.time() - entry["t"] < 1800:
            out[t] = entry["vol"]
            continue
        try:
            r = requests.get(f"https://cdn.cboe.com/api/global/delayed_quotes/options/{t}.json",
                             headers={"User-Agent": UA}, timeout=30)
            r.raise_for_status()
            body = r.text
            # isolate the options array: equity share volume sits outside it
            i = body.find('"options"')
            if i > 0:
                body = body[i:]
            j = body.find("]")
            if j > 0:
                body = body[:j]
            total = sum(int(v) for v in re.findall(r'"volume":\s*(\d+)', body))
            out[t] = total
            cache[t] = {"t": time.time(), "vol": total}
            dirty = True
            time.sleep(0.4)
        except Exception as e:
            log(f"  option volume fetch failed for {t}: {e}")
            if entry:
                out[t] = entry["vol"]
    if dirty:
        save_json(path, cache)
    return out


# ---------------- intraday volume snapshots (15m / 1h) ----------------

def get_window_surge(history, current, window_min, surge_data):
    now = datetime.now(timezone.utc)
    best, best_score = None, None
    for snap in history:
        age = (now - datetime.fromisoformat(snap["t"])).total_seconds() / 60
        if age < window_min * 0.5 or age > window_min * 1.9:
            continue
        score = abs(age - window_min)
        if best_score is None or score < best_score:
            best, best_score = snap, score
    if best is None:
        return None
    actual_min = (now - datetime.fromisoformat(best["t"])).total_seconds() / 60
    by_ticker = {s["ticker"]: s for s in surge_data}
    results = []
    for ticker, vol in current.items():
        old = best["v"].get(ticker)
        info = by_ticker.get(ticker)
        if old is None or info is None or not info.get("avg_vol"):
            continue
        delta = vol - old
        if delta <= 0:  # negative = volume counter reset (new session)
            continue
        expected = info["avg_vol"] / 390.0 * actual_min
        if expected <= 0:
            continue
        results.append({"ticker": ticker, "company": info["company"],
                        "ratio": round(delta / expected, 1), "delta": delta,
                        "price": info["price"], "change": info["change"]})
    results.sort(key=lambda r: -r["ratio"])
    return {"window_min": round(actual_min), "rows": results[:20]}


# ---------------- weekly / monthly volume trends (Yahoo) ----------------

def yahoo_chart(symbol):
    r = requests.get(
        f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?range=6mo&interval=1d",
        headers={"User-Agent": UA}, timeout=30)
    r.raise_for_status()
    return r.json()["chart"]["result"][0]


def get_vol_trends(surge_data, us_open):
    path = DATA_DIR / "voltrend.json"
    cache = load_json(path)
    out, dirty = {}, False
    stale = [s for s in surge_data
             if not (s["ticker"] in cache and time.time() - cache[s["ticker"]]["t"] < 6 * 3600)]
    if stale:
        log(f"Fetching daily volume history for {len(stale)} tickers (Yahoo)...")
    for s in surge_data:
        t = s["ticker"]
        entry = cache.get(t)
        if entry and time.time() - entry["t"] < 6 * 3600:
            out[t] = entry
            continue
        try:
            res = yahoo_chart(t)
            raw = res["indicators"]["quote"][0]["volume"]
            closes = [c for c in res["indicators"]["quote"][0]["close"] if c]
            if us_open and len(raw) > 1:
                raw = raw[:-1]  # drop today's partial bar
            vols = [v for v in raw if v]
            n = len(vols)
            if n >= 45:
                wk = sum(vols[-5:]) / 5
                wk_pre = vols[:-5][-60:]
                mo = sum(vols[-21:]) / 21
                mo_pre = vols[:-21][-63:]
                if wk_pre and mo_pre:
                    entry = {"t": time.time(),
                             "week": round(wk / (sum(wk_pre) / len(wk_pre)), 1),
                             "month": round(mo / (sum(mo_pre) / len(mo_pre)), 1),
                             "pw": round((closes[-1] / closes[-6] - 1) * 100, 2) if len(closes) >= 6 else None,
                             "pm": round((closes[-1] / closes[-22] - 1) * 100, 2) if len(closes) >= 22 else None}
                    out[t] = entry
                    cache[t] = entry
                    dirty = True
            time.sleep(0.3)
        except Exception as e:
            log(f"  volume history fetch failed for {t}: {e}")
            if entry:
                out[t] = entry
    if dirty:
        save_json(path, cache)
    # fill week/month price change when the surge source didn't provide it (Yahoo screener path)
    for s in surge_data:
        e = out.get(s["ticker"])
        if e:
            if s.get("perf_week") is None:
                s["perf_week"] = e.get("pw")
            if s.get("perf_month") is None:
                s["perf_month"] = e.get("pm")
    return out


# ---------------- Singapore STI ----------------

def get_sti_data(sg_open):
    path = DATA_DIR / "stivol.json"
    cache = load_json(path)
    out, dirty = [], False
    stale = [s for s, _ in STI_LIST
             if not (s in cache and time.time() - cache[s]["t"] < 600)]
    if stale:
        log(f"Fetching SGX volume history for {len(stale)} STI tickers (Yahoo)...")
    for sym, name in STI_LIST:
        entry = cache.get(sym)
        if not (entry and time.time() - entry["t"] < 600):
            entry = None
            try:
                res = yahoo_chart(sym)
                q = res["indicators"]["quote"][0]
                pairs = [(v, c) for v, c in zip(q["volume"], q["close"]) if v and c]
                n = len(pairs)
                if n >= 45:
                    vols = [p[0] for p in pairs]
                    closes = [p[1] for p in pairs]
                    base = vols[max(0, n - 64):n - 1]
                    base_avg = sum(base) / len(base) if base else 0
                    rel1d = round(vols[-1] / base_avg, 1) if base_avg else 0
                    cv = vols[:-1] if (sg_open and len(vols) > 1) else vols
                    wk = sum(cv[-5:]) / 5
                    wk_pre = cv[:-5][-60:]
                    mo = sum(cv[-21:]) / 21
                    mo_pre = cv[:-21][-63:]
                    week = round(wk / (sum(wk_pre) / len(wk_pre)), 1) if wk_pre else 0
                    month = round(mo / (sum(mo_pre) / len(mo_pre)), 1) if mo_pre else 0
                    price = float(res["meta"]["regularMarketPrice"])
                    entry = {
                        "t": time.time(), "price": price,
                        "chg": round((price / closes[-2] - 1) * 100, 2) if n >= 2 else None,
                        "rel1d": rel1d, "week": week, "month": month,
                        "avg": round(base_avg), "vol": vols[-1],
                        "perfW": round((price / closes[-6] - 1) * 100, 2) if n >= 6 else None,
                        "perfM": round((price / closes[-22] - 1) * 100, 2) if n >= 22 else None,
                    }
                    cache[sym] = entry
                    dirty = True
                time.sleep(0.3)
            except Exception as e:
                log(f"  SGX fetch failed for {sym}: {e}")
        if entry:
            out.append({"sym": sym, "code": sym.replace(".SI", ""), "company": name, **entry})
    if dirty:
        save_json(path, cache)
    return out


# ---------------- breaking news ----------------

def get_news():
    if time.time() - NEWS_CACHE["t"] < 300:
        return NEWS_CACHE["items"]
    items, seen = [], set()
    for source, url in NEWS_FEEDS:
        try:
            r = requests.get(url, headers={"User-Agent": UA}, timeout=20)
            r.raise_for_status()
            root = ElementTree.fromstring(r.content)
            for item in root.iter("item"):
                title = (item.findtext("title") or "").strip()
                link = (item.findtext("link") or "").strip()
                if not title or not link:
                    continue
                key = re.sub(r"\W+", "", title.lower())[:60]
                if key in seen:
                    continue
                seen.add(key)
                try:
                    dt = parsedate_to_datetime(item.findtext("pubDate"))
                except Exception:
                    # unknown publish time: rank below headlines with real timestamps
                    dt = datetime.now(timezone.utc) - timedelta(hours=6)
                low = title.lower()
                tags = list(dict.fromkeys(
                    tag for kw, tag in MACRO_KEYWORDS.items()
                    if re.search(r"\b" + re.escape(kw) + r"\b", low)))[:3]
                items.append({"title": title, "link": link, "dt": dt,
                              "source": source, "tags": tags})
        except Exception as e:
            log(f"  news feed {source} failed: {e}")
    # macro-tagged headlines first, then the rest; newest first within each group
    items.sort(key=lambda x: (0 if x["tags"] else 1,
                              -x["dt"].timestamp() if x["dt"] else 0))
    NEWS_CACHE["t"] = time.time()
    NEWS_CACHE["items"] = items[:14]
    return NEWS_CACHE["items"]


# ---------------- economic calendar ----------------

# Fed's published 2026 FOMC schedule (decision day of each two-day meeting)
FOMC_2026 = ["2026-01-28", "2026-03-18", "2026-04-29", "2026-06-17",
             "2026-07-29", "2026-09-16", "2026-10-28", "2026-12-09"]


def synthetic_events():
    """Major U.S. fixtures projected over the next 14 days.

    The ForexFactory mirror only publishes the current calendar week, so late in
    the week it says nothing about next week. FOMC dates are published years
    ahead and the jobs report is the first Friday of the month - project those.
    """
    ny = ZoneInfo("America/New_York")
    now = datetime.now(timezone.utc)
    horizon = now + timedelta(days=14)
    out = []
    for d in FOMC_2026:
        dt = datetime.fromisoformat(d + "T14:00:00").replace(tzinfo=ny)
        if now <= dt <= horizon:
            out.append({"title": "FOMC rate decision & press conference", "country": "USD",
                        "impact": "High", "dt": dt, "forecast": "", "previous": ""})
    for base in (now, now + timedelta(days=31)):
        first = base.astimezone(ny).replace(day=1, hour=8, minute=30, second=0, microsecond=0)
        nfp = first + timedelta(days=(4 - first.weekday()) % 7)
        if now <= nfp <= horizon:
            out.append({"title": "Non-farm payrolls (US jobs report)", "country": "USD",
                        "impact": "High", "dt": nfp, "forecast": "", "previous": ""})
    return out


def get_calendar():
    # the FF mirror rate-limits hard (429): one fetch per 6h on success, 15 min backoff on failure
    ttl = 900 if CAL_CACHE.get("err") else 6 * 3600
    if CAL_CACHE["t"] and time.time() - CAL_CACHE["t"] < ttl:
        return CAL_CACHE["events"]
    events = []
    try:
        r = requests.get("https://nfs.faireconomy.media/ff_calendar_thisweek.json",
                         headers={"User-Agent": UA}, timeout=20)
        r.raise_for_status()
        events = r.json()
        CAL_CACHE["err"] = None
    except Exception as e:
        CAL_CACHE["err"] = str(e)
        log(f"  calendar fetch failed: {e}")
    now = datetime.now(timezone.utc) - timedelta(hours=2)
    out = []
    for e in events:
        impact = e.get("impact", "")
        country = e.get("country", "")
        # High impact from any economy moves U.S. markets; Medium only for USD/global events
        if not (impact == "High" or (impact == "Medium" and country in ("USD", "All"))):
            continue
        try:
            dt = datetime.fromisoformat(e["date"])
        except Exception:
            continue
        if dt < now:
            continue
        out.append({"title": e.get("title", ""), "country": country, "impact": impact,
                    "dt": dt, "forecast": e.get("forecast", ""), "previous": e.get("previous", "")})
    # merge projected majors the weekly feed can't see yet, skipping ones it already lists
    for syn in synthetic_events():
        markers = ("fomc", "federal funds") if "FOMC" in syn["title"] else ("non-farm", "nonfarm")
        covered = any(any(m in e["title"].lower() for m in markers)
                      and abs((e["dt"] - syn["dt"]).total_seconds()) < 36 * 3600
                      for e in out)
        if not covered:
            out.append(syn)
    out.sort(key=lambda x: x["dt"])
    CAL_CACHE["t"] = time.time()
    CAL_CACHE["events"] = out[:30]
    return CAL_CACHE["events"]


# ---------------- market clocks ----------------

def us_market_status():
    et = datetime.now(ZoneInfo("America/New_York"))
    is_open = et.weekday() < 5 and (9, 30) <= (et.hour, et.minute) and et.hour < 16
    return {"time": et, "open": is_open, "label": "OPEN" if is_open else "CLOSED"}


def sg_market_status():
    sg = datetime.now(ZoneInfo("Asia/Singapore"))
    after_open = (sg.hour, sg.minute) >= (9, 0)
    before_close = (sg.hour, sg.minute) < (17, 16)
    is_open = sg.weekday() < 5 and after_open and before_close
    return {"time": sg, "open": is_open, "label": "OPEN" if is_open else "CLOSED"}


# ---------------- HTML rendering ----------------

def fmt_vol(v):
    if v is None:
        return "-"
    if v >= 1e9:
        return f"{v / 1e9:,.1f}B"
    if v >= 1e6:
        return f"{v / 1e6:,.1f}M"
    if v >= 1e3:
        return f"{v / 1e3:,.0f}K"
    return f"{v:,.0f}"


def fmt_num(v, dec):
    return "-" if v is None else f"{v:,.{dec}f}"


def fmt_pct(v, sym=""):
    attr = f' data-chg="{sym}"' if sym else ""
    if v is None:
        return f'<td class="num"{attr}>-</td>'
    cls = "up" if v > 0 else ("dn" if v < 0 else "flat")
    sign = "+" if v > 0 else ""
    return f'<td class="num {cls}"{attr}>{sign}{v:,.2f}%</td>'


def fmt_price_cell(v, sym):
    return f'<td class="num" data-px="{sym}">${fmt_num(v, 2)}</td>'


def ticker_link(t, tv="", qurl=""):
    tv = tv or t
    qurl = qurl or f"https://finviz.com/quote.ashx?t={t}"
    return (f'<a class="tick" href="{qurl}" onclick="openChart(\'{t}\',\'{tv}\',\'{qurl}\');'
            f'return false;" title="Click for chart">{t}</a>')


def sti_ticker_link(s):
    return ticker_link(s["code"], "SGX:" + s["code"],
                       "https://sg.finance.yahoo.com/quote/" + s["sym"])


def build_window_table(result, nominal_min):
    if result is None:
        return (f'<div class="empty">Collecting snapshots - this view needs the server to have '
                f'been running for ~{nominal_min} minutes.</div>')
    if not result["rows"]:
        return '<div class="empty">No volume traded in this window (market likely closed).</div>'
    w = result["window_min"]
    parts = [f'<p class="note">Surge = shares traded in the last ~{w} min vs that stock\'s '
             f'normal pace for a {w}-min stretch (3-month average).</p>',
             '<table><thead><tr><th style="width:6%">#</th><th style="width:11%">Ticker</th>'
             '<th style="width:31%">Company</th><th class="num" style="width:14%">Surge</th>'
             '<th class="num" style="width:14%">Shares in window</th>'
             '<th class="num" style="width:12%">Price</th>'
             '<th class="num" style="width:12%">Day</th></tr></thead><tbody>']
    for i, r in enumerate(result["rows"], 1):
        parts.append(
            f'<tr><td class="mut">{i}</td><td>{ticker_link(r["ticker"])}</td>'
            f'<td class="mut">{r["company"]}</td><td class="num"><b>{fmt_num(r["ratio"], 1)}x</b></td>'
            f'<td class="num">{fmt_vol(r["delta"])}</td>'
            f'{fmt_price_cell(r["price"], r["ticker"])}{fmt_pct(r["change"], r["ticker"])}</tr>')
    parts.append("</tbody></table>")
    return "".join(parts)


def build_trend_table(surge, trends, key, period_label, chg_key):
    rows = [{"s": s, "ratio": trends[s["ticker"]][key]}
            for s in surge if s["ticker"] in trends and trends[s["ticker"]].get(key) is not None]
    if not rows:
        return '<div class="empty">No historical volume data fetched yet.</div>'
    rows.sort(key=lambda r: -r["ratio"])
    parts = [f'<p class="note">Surge = average daily volume over the last {period_label} vs that '
             f'stock\'s prior 3-month pace (Yahoo daily bars). Universe: stocks currently trading '
             f'at &ge;1.5x daily relative volume.</p>',
             f'<table><thead><tr><th style="width:6%">#</th><th style="width:11%">Ticker</th>'
             f'<th style="width:31%">Company</th><th class="num" style="width:13%">Surge</th>'
             f'<th class="num" style="width:13%">3-mo avg/day</th>'
             f'<th class="num" style="width:12%">Price</th>'
             f'<th class="num" style="width:14%">{period_label} chg</th></tr></thead><tbody>']
    for i, r in enumerate(rows[:20], 1):
        s = r["s"]
        parts.append(
            f'<tr><td class="mut">{i}</td><td>{ticker_link(s["ticker"])}</td>'
            f'<td class="mut">{s["company"]}</td><td class="num"><b>{fmt_num(r["ratio"], 1)}x</b></td>'
            f'<td class="num">{fmt_vol(s["avg_vol"])}</td>'
            f'{fmt_price_cell(s["price"], s["ticker"])}{fmt_pct(s[chg_key])}</tr>')
    parts.append("</tbody></table>")
    return "".join(parts)


def build_sti_table(sti, key, chg_key, chg_label, note):
    if not sti:
        return '<div class="empty">No SGX data fetched yet.</div>'
    top = sorted(sti, key=lambda s: -s[key])[:20]
    parts = [f'<p class="note">{note}</p>',
             f'<table><thead><tr><th style="width:6%">#</th><th style="width:11%">Ticker</th>'
             f'<th style="width:31%">Company</th><th class="num" style="width:13%">Surge</th>'
             f'<th class="num" style="width:13%">3-mo avg/day</th>'
             f'<th class="num" style="width:12%">Price</th>'
             f'<th class="num" style="width:14%">{chg_label}</th></tr></thead><tbody>']
    for i, s in enumerate(top, 1):
        chg = fmt_pct(s["chg"], s["sym"]) if chg_key == "chg" else fmt_pct(s[chg_key])
        parts.append(
            f'<tr><td class="mut">{i}</td><td>{sti_ticker_link(s)}</td>'
            f'<td class="mut">{s["company"]}</td><td class="num"><b>{fmt_num(s[key], 1)}x</b></td>'
            f'<td class="num">{fmt_vol(s["avg"])}</td>'
            f'{fmt_price_cell(s["price"], s["sym"])}{chg}</tr>')
    parts.append("</tbody></table>")
    return "".join(parts)


def fmt_ago(dt):
    mins = max(0, int((datetime.now(timezone.utc) - dt).total_seconds() // 60))
    if mins < 60:
        return f"{mins}m ago"
    if mins < 48 * 60:
        return f"{mins // 60}h ago"
    return f"{mins // 1440}d ago"


def build_news_html(items):
    if not items:
        return '<div class="empty">No headlines fetched - check server logs.</div>'
    parts = []
    for it in items:
        tags = "".join(f'<span class="tag">{t}</span>' for t in it["tags"])
        parts.append(
            f'<div class="news-item"><span class="mut" style="min-width:62px">{fmt_ago(it["dt"])}</span>'
            f'<span class="src">{it["source"]}</span>'
            f'<a href="{escape(it["link"], quote=True)}" target="_blank" rel="noopener">'
            f'{escape(it["title"])}</a>{tags}</div>')
    return "".join(parts)


def build_calendar_html(events):
    if not events:
        return '<div class="empty">No calendar events fetched - check server logs.</div>'
    ny, sg = ZoneInfo("America/New_York"), ZoneInfo("Asia/Singapore")
    parts = ['<table><thead><tr><th style="width:14%">Date</th><th style="width:10%">ET</th>'
             '<th style="width:10%">SGT</th><th style="width:34%">Event</th>'
             '<th style="width:8%">Cur</th><th style="width:10%">Impact</th>'
             '<th class="num" style="width:7%">Forecast</th>'
             '<th class="num" style="width:7%">Previous</th></tr></thead><tbody>']
    last_day = None
    for e in events:
        et = e["dt"].astimezone(ny)
        sgt = e["dt"].astimezone(sg)
        day = et.strftime("%a %d %b")
        day_cell = day if day != last_day else ""
        last_day = day
        imp_cls = "imp-high" if e["impact"] == "High" else "imp-med"
        parts.append(
            f'<tr><td>{day_cell}</td><td class="mut">{et:%H:%M}</td>'
            f'<td class="mut">{sgt:%H:%M}</td><td>{escape(e["title"])}</td>'
            f'<td class="mut">{e["country"]}</td>'
            f'<td><span class="badge {imp_cls}">{e["impact"]}</span></td>'
            f'<td class="num">{e["forecast"] or "-"}</td>'
            f'<td class="num">{e["previous"] or "-"}</td></tr>')
    parts.append("</tbody></table>")
    return "".join(parts)


TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="theme-color" id="metaTheme" content="#fafaf8">
<meta name="apple-mobile-web-app-capable" content="yes">
<title>Stocks - premium &amp; volume dashboard</title>
<style>
:root { --bg:#fafaf8; --card:#ffffff; --txt:#1a1a18; --mut:#71706b; --bd:#e4e3de; --up:#0f6e56; --dn:#a32d2d; --acc:#ef9f27; --teal:#1d9e75; --link:#185fa5; }
:root.dark { --bg:#17171a; --card:#222226; --txt:#ececea; --mut:#a3a29c; --bd:#38383d; --up:#5dcaa5; --dn:#f09595; --link:#85b7eb; }
* { box-sizing:border-box; }
html { -webkit-text-size-adjust:100%; }
body { margin:0; padding:24px; background:var(--bg); color:var(--txt); font:14px/1.5 -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,"Helvetica Neue",sans-serif; transition:background .25s ease,color .25s ease; }
.wrap { max-width:860px; margin:0 auto; }
.topbar { display:flex; align-items:flex-start; justify-content:space-between; gap:12px; }
#themeBtn { flex:none; border:1px solid var(--bd); background:var(--card); color:var(--txt); border-radius:99px; width:40px; height:40px; font-size:18px; line-height:1; cursor:pointer; transition:transform .15s; }
#themeBtn:active { transform:scale(.92); }
h1 { font-size:20px; font-weight:600; margin:0 0 2px; }
h2 { font-size:16px; font-weight:600; margin:28px 0 4px; }
.sub { color:var(--mut); font-size:12px; margin:0 0 14px; }
.badge { display:inline-block; padding:2px 10px; border-radius:99px; font-size:12px; font-weight:600; }
.badge.open { background:#e1f5ee; color:#085041; }
.badge.closed { background:#fceaea; color:#791f1f; }
.twrap { overflow-x:auto; -webkit-overflow-scrolling:touch; border-radius:10px; }
table { width:100%; border-collapse:collapse; background:var(--card); border:1px solid var(--bd); border-radius:10px; overflow:hidden; font-size:13px; }
th { text-align:left; font-size:11px; text-transform:uppercase; letter-spacing:.04em; color:var(--mut); padding:8px 10px; border-bottom:1px solid var(--bd); white-space:nowrap; }
th.num, td.num { text-align:right; }
td { padding:7px 10px; border-bottom:1px solid var(--bd); }
tr:last-child td { border-bottom:none; }
tbody tr:hover td { background:rgba(125,125,125,.06); }
.tick { color:var(--link); font-weight:600; text-decoration:none; }
.up { color:var(--up); } .dn { color:var(--dn); } .flat { color:var(--mut); }
.mut { color:var(--mut); }
.barwrap { display:flex; align-items:center; gap:8px; }
.barwrap span { min-width:38px; text-align:right; font-weight:600; }
.track { flex:1; height:6px; background:var(--bd); border-radius:3px; }
.bar { height:6px; background:var(--acc); border-radius:3px; }
.tabs { display:flex; gap:8px; margin:10px 0; flex-wrap:wrap; }
.tabs button { padding:6px 14px; border:1px solid var(--bd); background:var(--card); color:var(--txt); border-radius:8px; font-size:13px; cursor:pointer; }
.tabs button.on { background:var(--teal); border-color:var(--teal); color:#fff; font-weight:600; }
.empty { background:var(--card); border:1px dashed var(--bd); border-radius:10px; padding:18px; color:var(--mut); font-size:13px; }
.note { font-size:12px; color:var(--mut); margin:0 0 8px; }
.foot { margin-top:24px; font-size:11px; color:var(--mut); }
.news-item { display:flex; align-items:baseline; gap:10px; padding:7px 10px; background:var(--card); border:1px solid var(--bd); border-top:none; font-size:13px; flex-wrap:wrap; }
.news-item:first-child { border-top:1px solid var(--bd); border-radius:10px 10px 0 0; }
.news-item:last-child { border-radius:0 0 10px 10px; }
.news-item a { color:var(--txt); text-decoration:none; flex:1; min-width:200px; }
.news-item a:hover { color:#185fa5; }
.src { font-size:11px; color:var(--mut); border:1px solid var(--bd); border-radius:99px; padding:1px 8px; white-space:nowrap; }
.tag { font-size:11px; font-weight:600; background:#faeeda; color:#854f0b; border-radius:99px; padding:1px 8px; white-space:nowrap; }
.badge.imp-high { background:#fceaea; color:#791f1f; }
.badge.imp-med { background:#faeeda; color:#854f0b; }
#chartModal { display:none; position:fixed; inset:0; background:rgba(0,0,0,.55); z-index:50; align-items:center; justify-content:center; }
.cbox { background:var(--card); border:1px solid var(--bd); border-radius:12px; width:min(980px,94vw); height:min(640px,90vh); display:flex; flex-direction:column; overflow:hidden; }
.chead { display:flex; align-items:center; gap:14px; padding:10px 14px; border-bottom:1px solid var(--bd); }
.chead b { font-size:16px; }
.chead a { font-size:12px; color:#185fa5; }
.chead button { margin-left:auto; border:1px solid var(--bd); background:var(--card); color:var(--txt); border-radius:6px; padding:2px 10px; font-size:14px; cursor:pointer; }
.cbox iframe { flex:1; width:100%; border:0; }
@media (max-width: 680px) {
  body { padding:12px; }
  h1 { font-size:17px; }
  .tabs button { padding:9px 13px; }
  .twrap table { min-width:620px; }
  .news-item { padding:9px 10px; }
  .cbox { width:100vw; height:92vh; border-radius:12px 12px 0 0; align-self:flex-end; }
}
</style>
<script>
(function() {
  var t = null;
  try { t = localStorage.getItem('findash_theme'); } catch (e) {}
  if (!t) { t = (window.matchMedia && matchMedia('(prefers-color-scheme: dark)').matches) ? 'dark' : 'light'; }
  if (t === 'dark') { document.documentElement.classList.add('dark'); }
})();
</script>
</head>
<body>
<div class="wrap">
  <div class="topbar">
    <h1>Stocks &mdash; premium &amp; volume dashboard</h1>
    <button id="themeBtn" onclick="toggleTheme()" aria-label="Toggle dark or light mode">&#9790;</button>
  </div>
  <p class="sub">Updated @@NOW@@ UTC &middot; tables rebuild every @@RELOAD@@s (paused while a chart is open)<span id="liveq" style="display:none"> &middot; <span style="color:var(--teal);font-weight:600">&#9679; live</span> quotes as of <span id="liveqt"></span></span></p>
  <div class="tabs" style="margin:12px 0 16px">
    <button id="pgus" onclick="showPage('us')">U.S. markets <span class="badge @@STATUSCLASS@@" style="margin-left:6px">@@STATUS@@</span></button>
    <button id="pgsg" onclick="showPage('sg')">Singapore STI <span class="badge @@SGSTATUSCLASS@@" style="margin-left:6px">@@SGSTATUS@@</span></button>
  </div>

  <div id="pageUS">
  <p class="sub">@@ET@@ ET &middot; NYSE/Nasdaq <span class="badge @@STATUSCLASS@@">@@STATUS@@</span></p>

  <h2>Steady premium generators</h2>
  <p class="sub">Low-beta defensives with liquid options, sorted calmest first. ATR% = average daily range as a share of price &mdash; lower means steadier. Opt vol = option contracts traded last session (CBOE delayed); names below @@MINOPTVOL@@ contracts are excluded. Click a ticker to open its chart.</p>
  <table>
    <thead><tr><th style="width:8%">Ticker</th><th style="width:24%">Company</th><th class="num" style="width:10%">Price</th><th class="num" style="width:9%">Day</th><th class="num" style="width:8%">Beta</th><th class="num" style="width:8%">ATR %</th><th class="num" style="width:7%">RSI</th><th class="num" style="width:13%">Opt vol</th><th class="num" style="width:13%">Volume</th></tr></thead>
    <tbody>@@STABLE_ROWS@@</tbody>
  </table>
  @@EXCLUDED@@

  <h2>Unusual volume surge &mdash; top 20</h2>
  <div class="tabs">
    <button id="tb15" onclick="show(15)">15 min</button>
    <button id="tb60" onclick="show(60)">1 hour</button>
    <button id="tb1d" onclick="show(1440)">1 day</button>
    <button id="tb1w" onclick="show(10080)">1 week</button>
    <button id="tb1m" onclick="show(43200)">1 month</button>
  </div>
  <div id="pane15" style="display:none">@@TAB15@@</div>
  <div id="pane60" style="display:none">@@TAB60@@</div>
  <div id="pane1w" style="display:none">@@TAB1W@@</div>
  <div id="pane1m" style="display:none">@@TAB1M@@</div>
  <div id="pane1d" style="display:none">
    <p class="note">Relative volume = today's cumulative volume vs the 3-month daily average (small-cap and larger, avg volume &gt; 300K).</p>
    <table>
      <thead><tr><th style="width:6%">#</th><th style="width:10%">Ticker</th><th style="width:26%">Company</th><th style="width:22%">Rel volume</th><th class="num" style="width:10%">Price</th><th class="num" style="width:10%">Day</th><th class="num" style="width:16%">Vol / avg</th></tr></thead>
      <tbody>@@SURGE_ROWS@@</tbody>
    </table>
  </div>
  </div>

  <div id="pageSG" style="display:none">
  <p class="sub">@@SGT@@ SGT &middot; SGX <span class="badge @@SGSTATUSCLASS@@">@@SGSTATUS@@</span></p>
  <h2>Unusual volume surge &mdash; STI constituents</h2>
  <p class="sub">Universe: 32 Straits Times Index constituents, ranked by volume surge (Yahoo daily bars, cached 10 min). Prices update live. 15-min and 1-hour views are not available for SGX &mdash; no free intraday volume feed. Click a ticker for its chart.</p>
  <div class="tabs">
    <button id="sgtb1d" onclick="showSg('1d')">1 day</button>
    <button id="sgtb1w" onclick="showSg('1w')">1 week</button>
    <button id="sgtb1m" onclick="showSg('1m')">1 month</button>
  </div>
  <div id="sgpane1d" style="display:none">@@SG1D@@</div>
  <div id="sgpane1w" style="display:none">@@SG1W@@</div>
  <div id="sgpane1m" style="display:none">@@SG1M@@</div>
  </div>

  <h2>Latest breaking news</h2>
  <p class="sub">Macro-moving headlines first (tagged by theme), then the latest general market news. Sources: CNBC, MarketWatch, Yahoo Finance; refreshed every 5 min.</p>
  @@NEWS@@

  <h2>Market calendar &mdash; next two weeks</h2>
  <p class="sub">High-impact releases for all major economies plus medium-impact U.S. events. Detailed forecasts cover the current calendar week (ForexFactory, rolls over each weekend); FOMC decisions and jobs reports are projected for the days beyond. Times shown in both New York and Singapore.</p>
  @@CAL@@

  <p class="foot">Data scraped from finviz.com screeners; option volume from CBOE delayed quotes; SGX and historical data from Yahoo Finance; charts by TradingView. 15-min and 1-hour surges are computed from successive volume snapshots while the server runs. Not investment advice &mdash; verify implied volatility and earnings dates before selling premium.</p>
</div>
<div id="chartModal" onclick="if(event.target===this)closeChart()">
  <div class="cbox">
    <div class="chead"><b id="csym"></b><a id="cfinviz" href="#" target="_blank" rel="noopener">Quote page</a><a id="ctv" href="#" target="_blank" rel="noopener">Full TradingView</a><button onclick="closeChart()" aria-label="Close">&#10005;</button></div>
    <iframe id="cframe" title="Price chart"></iframe>
  </div>
</div>
<script>
function show(n) {
  var ids = { 15:'15', 60:'60', 1440:'1d', 10080:'1w', 43200:'1m' };
  for (var k in ids) {
    document.getElementById('pane' + ids[k]).style.display = (k == n) ? 'block' : 'none';
    document.getElementById('tb' + ids[k]).className = (k == n) ? 'on' : '';
  }
  try { localStorage.setItem('findash_tab', n); } catch (e) {}
}
var saved = 1440;
try { saved = parseInt(localStorage.getItem('findash_tab')) || 1440; } catch (e) {}
show(saved);

function showSg(n) {
  var ids = ['1d', '1w', '1m'];
  for (var i = 0; i < ids.length; i++) {
    document.getElementById('sgpane' + ids[i]).style.display = (ids[i] === n) ? 'block' : 'none';
    document.getElementById('sgtb' + ids[i]).className = (ids[i] === n) ? 'on' : '';
  }
  try { localStorage.setItem('findash_sgtab', n); } catch (e) {}
}
var sgSaved = '1d';
try { sgSaved = localStorage.getItem('findash_sgtab') || '1d'; } catch (e) {}
showSg(sgSaved);

function showPage(p) {
  document.getElementById('pageUS').style.display = (p === 'us') ? 'block' : 'none';
  document.getElementById('pageSG').style.display = (p === 'sg') ? 'block' : 'none';
  document.getElementById('pgus').className = (p === 'us') ? 'on' : '';
  document.getElementById('pgsg').className = (p === 'sg') ? 'on' : '';
  try { localStorage.setItem('findash_page', p); } catch (e) {}
}
var pgSaved = 'us';
try { pgSaved = localStorage.getItem('findash_page') || 'us'; } catch (e) {}
showPage(pgSaved);

function curTheme() { return document.documentElement.classList.contains('dark') ? 'dark' : 'light'; }
function paintTheme() {
  var d = curTheme() === 'dark';
  document.getElementById('themeBtn').innerHTML = d ? '&#9728;' : '&#9790;';
  var m = document.getElementById('metaTheme');
  if (m) { m.content = d ? '#17171a' : '#fafaf8'; }
}
function toggleTheme() {
  var t = curTheme() === 'dark' ? 'light' : 'dark';
  document.documentElement.classList.toggle('dark', t === 'dark');
  try { localStorage.setItem('findash_theme', t); } catch (e) {}
  paintTheme();
}
paintTheme();

var chartOpen = false;
function openChart(s, tv, qurl) {
  chartOpen = true;
  if (!tv) { tv = s; }
  if (!qurl) { qurl = 'https://finviz.com/quote.ashx?t=' + s; }
  document.getElementById('csym').textContent = s;
  document.getElementById('cfinviz').href = qurl;
  document.getElementById('ctv').href = 'https://www.tradingview.com/chart/?symbol=' + encodeURIComponent(tv);
  var dark = curTheme() === 'dark';
  document.getElementById('cframe').src = 'https://s.tradingview.com/widgetembed/?symbol=' + encodeURIComponent(tv) +
    '&interval=D&theme=' + (dark ? 'dark' : 'light') +
    '&style=1&timezone=exchange&withdateranges=1&hidesidetoolbar=0&symboledit=0&saveimage=0&hideideas=1';
  document.getElementById('chartModal').style.display = 'flex';
}
function closeChart() {
  chartOpen = false;
  document.getElementById('chartModal').style.display = 'none';
  document.getElementById('cframe').src = 'about:blank';
}
document.addEventListener('keydown', function(e) { if (e.key === 'Escape') closeChart(); });

setInterval(function() { if (!chartOpen) { location.reload(); } }, @@RELOAD@@ * 1000);

function applyQuotes(q) {
  var any = false;
  for (var s in q) {
    any = true;
    var els = document.querySelectorAll('[data-px="' + s + '"]');
    for (var i = 0; i < els.length; i++) { els[i].textContent = '$' + q[s].p.toFixed(2); }
    var cls = 'num ' + (q[s].c > 0 ? 'up' : (q[s].c < 0 ? 'dn' : 'flat'));
    var txt = (q[s].c > 0 ? '+' : '') + q[s].c.toFixed(2) + '%';
    els = document.querySelectorAll('[data-chg="' + s + '"]');
    for (var i = 0; i < els.length; i++) { els[i].textContent = txt; els[i].className = cls; }
  }
  if (any) {
    document.getElementById('liveq').style.display = 'inline';
    document.getElementById('liveqt').textContent = new Date().toLocaleTimeString();
  }
}
function pollQuotes() {
  fetch('/quotes').then(function(r) { return r.json(); }).then(applyQuotes).catch(function() {});
}
setInterval(pollQuotes, 15000);
pollQuotes();
</script>
</body>
</html>"""


def build_html(stable, surge, win15, win60, trends, us_status, excluded_note, sti, sg_status,
               news, calendar):
    reload_s = max(REFRESH_SECONDS, 30) + 5

    stable_rows = "".join(
        f'<tr><td>{ticker_link(s["ticker"])}</td><td class="mut">{s["company"]}</td>'
        f'{fmt_price_cell(s["price"], s["ticker"])}{fmt_pct(s["change"], s["ticker"])}'
        f'<td class="num">{fmt_num(s["beta"], 2)}</td><td class="num">{fmt_num(s["atr_pct"], 1)}</td>'
        f'<td class="num">{fmt_num(s["rsi"], 0)}</td><td class="num"><b>{fmt_vol(s.get("opt_vol"))}</b></td>'
        f'<td class="num">{fmt_vol(s["volume"])}</td></tr>'
        for s in stable)

    surge_rows = []
    for i, s in enumerate(sorted(surge, key=lambda x: -(x["rel_vol"] or 0))[:20], 1):
        pct = min(100, round((s["rel_vol"] or 0) / 8.0 * 100))
        surge_rows.append(
            f'<tr><td class="mut">{i}</td><td>{ticker_link(s["ticker"])}</td>'
            f'<td class="mut">{s["company"]}</td><td><div class="barwrap">'
            f'<div class="track"><div class="bar" style="width:{pct}%"></div></div>'
            f'<span>{fmt_num(s["rel_vol"], 1)}x</span></div></td>'
            f'{fmt_price_cell(s["price"], s["ticker"])}{fmt_pct(s["change"], s["ticker"])}'
            f'<td class="num">{fmt_vol(s["volume"])} / {fmt_vol(s["avg_vol"])}</td></tr>')

    excluded_html = f'<p class="note" style="margin-top:6px">{excluded_note}</p>' if excluded_note else ""

    html = TEMPLATE
    for token, value in {
        "@@RELOAD@@": str(reload_s),
        "@@NOW@@": datetime.now(timezone.utc).strftime("%a %d %b %Y %H:%M:%S"),
        "@@ET@@": us_status["time"].strftime("%H:%M"),
        "@@STATUS@@": us_status["label"],
        "@@STATUSCLASS@@": "open" if us_status["open"] else "closed",
        "@@MINOPTVOL@@": f"{MIN_OPT_VOLUME:,}",
        "@@EXCLUDED@@": excluded_html,
        "@@STABLE_ROWS@@": stable_rows,
        "@@SURGE_ROWS@@": "".join(surge_rows),
        "@@TAB15@@": build_window_table(win15, 15),
        "@@TAB60@@": build_window_table(win60, 60),
        "@@TAB1W@@": build_trend_table(surge, trends, "week", "week", "perf_week"),
        "@@TAB1M@@": build_trend_table(surge, trends, "month", "month", "perf_month"),
        "@@SGT@@": sg_status["time"].strftime("%H:%M"),
        "@@SGSTATUS@@": sg_status["label"],
        "@@SGSTATUSCLASS@@": "open" if sg_status["open"] else "closed",
        "@@SG1D@@": build_sti_table(sti, "rel1d", "chg", "Day",
                                    "Surge = latest session volume vs the prior 3-month daily average. "
                                    "While SGX is open this is volume so far today, so early-session readings run low."),
        "@@SG1W@@": build_sti_table(sti, "week", "perfW", "Week chg",
                                    "Surge = average daily volume over the last 5 sessions vs the prior 3-month pace."),
        "@@SG1M@@": build_sti_table(sti, "month", "perfM", "Month chg",
                                    "Surge = average daily volume over the last 21 sessions vs the prior 3-month pace."),
        "@@NEWS@@": build_news_html(news),
        "@@CAL@@": build_calendar_html(calendar),
    }.items():
        html = html.replace(token, value)
    # every table scrolls horizontally on narrow screens instead of squashing
    html = html.replace("<table>", '<div class="twrap"><table>').replace("</table>", "</table></div>")
    return html


# ---------------- live quotes ----------------

def live_quotes_json():
    with _lock:
        if time.time() - QUOTE_CACHE["t"] < 10:
            return QUOTE_CACHE["json"]
        syms = [t for t in STATE["tickers"] if t not in BAD_QUOTE_SYMS]
    out = {}

    def parse_spark(payload):
        for res in payload.get("spark", {}).get("result", []):
            meta = res["response"][0]["meta"]
            prev = meta.get("previousClose") or meta.get("chartPreviousClose")
            price = meta.get("regularMarketPrice")
            if price and prev:
                out[res["symbol"]] = {"p": round(float(price), 2),
                                      "c": round((float(price) / float(prev) - 1) * 100, 2)}

    # one unknown symbol fails the whole spark request, so chunk + per-symbol fallback
    for i in range(0, len(syms), 20):
        chunk = syms[i:i + 20]
        url = ("https://query1.finance.yahoo.com/v7/finance/spark?symbols="
               + ",".join(chunk) + "&range=1d&interval=15m")
        try:
            r = requests.get(url, headers={"User-Agent": UA}, timeout=20)
            r.raise_for_status()
            parse_spark(r.json())
        except Exception:
            for s in chunk:
                try:
                    r = requests.get("https://query1.finance.yahoo.com/v7/finance/spark?symbols="
                                     + s + "&range=1d&interval=15m",
                                     headers={"User-Agent": UA}, timeout=10)
                    r.raise_for_status()
                    parse_spark(r.json())
                except Exception:
                    BAD_QUOTE_SYMS.add(s)
                    log(f"  no live quote for {s} - skipping this session.")
    payload = json.dumps(out, separators=(",", ":"))
    with _lock:
        QUOTE_CACHE["t"] = time.time()
        QUOTE_CACHE["json"] = payload
    return payload


# ---------------- refresh loop ----------------

def refresh_cycle():
    us = us_market_status()
    log(f"Fetching screeners (US market {us['label']}, {us['time']:%H:%M} ET)...")
    stable, surge = [], []
    if not FORCE_YAHOO:
        stable = get_stable_data()
        time.sleep(2)
        surge = get_surge_data()
        log(f"Finviz: {len(stable)} stable names, {len(surge)} volume-surge names.")
    fallback = False
    if not stable:
        stable = get_stable_data_yahoo()
        fallback = True
    if not surge:
        surge = get_surge_data_yahoo()
        fallback = True
    if fallback:
        log(f"Yahoo fallback: {len(stable)} stable names, {len(surge)} volume-surge names.")

    excluded_note = ""
    if stable:
        opt_vols = get_option_volumes([s["ticker"] for s in stable])
        kept, excluded = [], []
        for s in stable:
            s["opt_vol"] = opt_vols.get(s["ticker"])
            # unknown option volume (fetch failed) -> keep visible rather than silently drop
            if s["opt_vol"] is not None and s["opt_vol"] < MIN_OPT_VOLUME:
                excluded.append(s)
            else:
                kept.append(s)
        if excluded:
            excluded_note = ("Excluded for thin option volume: "
                             + ", ".join(f'{s["ticker"]} ({fmt_vol(s["opt_vol"])})' for s in excluded))
        log(f"Option volume filter: kept {len(kept)}, excluded {len(excluded)}.")
        stable = kept
    if fallback:
        src = "U.S. screener data via Yahoo Finance (Finviz unreachable from this host)."
        excluded_note = f"{src} {excluded_note}" if excluded_note else src

    hist_path = DATA_DIR / "history.json"
    history = load_json(hist_path)
    if not isinstance(history, list):
        history = []
    current = {s["ticker"]: s["volume"] for s in surge + stable if s.get("volume")}
    win15 = get_window_surge(history, current, 15, surge)
    win60 = get_window_surge(history, current, 60, surge)
    cutoff = datetime.now(timezone.utc) - timedelta(hours=3)
    history = [h for h in history if datetime.fromisoformat(h["t"]) > cutoff]
    history.append({"t": datetime.now(timezone.utc).isoformat(), "v": current})
    save_json(hist_path, history)

    trends = get_vol_trends(surge, us["open"])
    log(f"Volume trends available for {len(trends)} of {len(surge)} tickers.")

    sg = sg_market_status()
    sti = get_sti_data(sg["open"])
    log(f"SGX data for {len(sti)} of {len(STI_LIST)} STI constituents (market {sg['label']}).")

    news = get_news()
    calendar = get_calendar()
    log(f"News: {len(news)} headlines; calendar: {len(calendar)} events.")

    html = build_html(stable, surge, win15, win60, trends, us, excluded_note, sti, sg,
                      news, calendar)
    tickers = list(dict.fromkeys(
        [s["ticker"] for s in stable] + [s["ticker"] for s in surge] + [s["sym"] for s in sti]))
    with _lock:
        STATE["html"] = html
        STATE["tickers"] = tickers
        STATE["built_at"] = datetime.now(timezone.utc)
    log(f"Dashboard rebuilt ({len(html) // 1024} KB, {len(tickers)} live tickers).")


def refresh_loop():
    while True:
        try:
            refresh_cycle()
        except Exception as e:
            log(f"Cycle failed: {e}")
        time.sleep(REFRESH_SECONDS)


# ---------------- Flask app ----------------

app = Flask(__name__)


@app.route("/")
def index():
    with _lock:
        html = STATE["html"]
    if html is None:
        return Response(
            "<meta http-equiv='refresh' content='5'>"
            "<body style='font-family:sans-serif;padding:40px'>"
            "<h3>Warming up&hellip;</h3><p>First data cycle is running; "
            "this page retries every 5 seconds.</p></body>",
            mimetype="text/html")
    return Response(html, mimetype="text/html",
                    headers={"Cache-Control": "no-store"})


@app.route("/quotes")
def quotes():
    return Response(live_quotes_json(), mimetype="application/json",
                    headers={"Cache-Control": "no-store"})


@app.route("/healthz")
def healthz():
    with _lock:
        built = STATE["built_at"]
        n_tickers = len(STATE["tickers"])
    return {"ok": True, "built_at": built.isoformat() if built else None,
            "tickers": n_tickers,
            "news": len(NEWS_CACHE["items"]),
            "calendar_events": len(CAL_CACHE["events"]),
            "calendar_error": CAL_CACHE.get("err")}


def _start_background():
    t = threading.Thread(target=refresh_loop, daemon=True, name="refresh")
    t.start()


_start_background()

if __name__ == "__main__":
    log(f"FinDashboard web starting on http://localhost:{PORT}/")
    from waitress import serve
    serve(app, host="0.0.0.0", port=PORT)
