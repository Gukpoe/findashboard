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
from html import unescape
from pathlib import Path
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

STABLE_TICKERS = "VZ,T,MRK,JNJ,ED,KMB,CL,KO,SO,PEP,PG,DUK,GILD,MO,GIS,ARCC"
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
                             "month": round(mo / (sum(mo_pre) / len(mo_pre)), 1)}
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


TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Stocks - premium &amp; volume dashboard</title>
<style>
:root { --bg:#fafaf8; --card:#ffffff; --txt:#1a1a18; --mut:#71706b; --bd:#e4e3de; --up:#0f6e56; --dn:#a32d2d; --acc:#ef9f27; --teal:#1d9e75; }
@media (prefers-color-scheme: dark) {
  :root { --bg:#1c1c1a; --card:#262624; --txt:#ececea; --mut:#a3a29c; --bd:#3a3936; --up:#5dcaa5; --dn:#f09595; }
}
* { box-sizing:border-box; }
body { margin:0; padding:24px; background:var(--bg); color:var(--txt); font:14px/1.5 "Segoe UI",system-ui,sans-serif; }
.wrap { max-width:860px; margin:0 auto; }
h1 { font-size:20px; font-weight:600; margin:0 0 2px; }
h2 { font-size:16px; font-weight:600; margin:28px 0 4px; }
.sub { color:var(--mut); font-size:12px; margin:0 0 14px; }
.badge { display:inline-block; padding:2px 10px; border-radius:99px; font-size:12px; font-weight:600; }
.badge.open { background:#e1f5ee; color:#085041; }
.badge.closed { background:#fceaea; color:#791f1f; }
table { width:100%; border-collapse:collapse; background:var(--card); border:1px solid var(--bd); border-radius:10px; overflow:hidden; font-size:13px; }
th { text-align:left; font-size:11px; text-transform:uppercase; letter-spacing:.04em; color:var(--mut); padding:8px 10px; border-bottom:1px solid var(--bd); }
th.num, td.num { text-align:right; }
td { padding:7px 10px; border-bottom:1px solid var(--bd); }
tr:last-child td { border-bottom:none; }
.tick { color:#185fa5; font-weight:600; text-decoration:none; }
@media (prefers-color-scheme: dark) { .tick { color:#85b7eb; } }
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
#chartModal { display:none; position:fixed; inset:0; background:rgba(0,0,0,.55); z-index:50; align-items:center; justify-content:center; }
.cbox { background:var(--card); border:1px solid var(--bd); border-radius:12px; width:min(980px,94vw); height:min(640px,90vh); display:flex; flex-direction:column; overflow:hidden; }
.chead { display:flex; align-items:center; gap:14px; padding:10px 14px; border-bottom:1px solid var(--bd); }
.chead b { font-size:16px; }
.chead a { font-size:12px; color:#185fa5; }
.chead button { margin-left:auto; border:1px solid var(--bd); background:var(--card); color:var(--txt); border-radius:6px; padding:2px 10px; font-size:14px; cursor:pointer; }
.cbox iframe { flex:1; width:100%; border:0; }
</style>
</head>
<body>
<div class="wrap">
  <h1>Stocks &mdash; premium &amp; volume dashboard</h1>
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

var chartOpen = false;
function openChart(s, tv, qurl) {
  chartOpen = true;
  if (!tv) { tv = s; }
  if (!qurl) { qurl = 'https://finviz.com/quote.ashx?t=' + s; }
  document.getElementById('csym').textContent = s;
  document.getElementById('cfinviz').href = qurl;
  document.getElementById('ctv').href = 'https://www.tradingview.com/chart/?symbol=' + encodeURIComponent(tv);
  var dark = window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches;
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


def build_html(stable, surge, win15, win60, trends, us_status, excluded_note, sti, sg_status):
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
    }.items():
        html = html.replace(token, value)
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
    stable = get_stable_data()
    time.sleep(2)
    surge = get_surge_data()
    log(f"Parsed {len(stable)} stable names, {len(surge)} volume-surge names.")
    if not stable and not surge:
        log("WARNING: no rows parsed - finviz may be blocking this host. Keeping previous page.")
        return

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

    html = build_html(stable, surge, win15, win60, trends, us, excluded_note, sti, sg)
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
    return {"ok": True, "built_at": built.isoformat() if built else None}


def _start_background():
    t = threading.Thread(target=refresh_loop, daemon=True, name="refresh")
    t.start()


_start_background()

if __name__ == "__main__":
    log(f"FinDashboard web starting on http://localhost:{PORT}/")
    from waitress import serve
    serve(app, host="0.0.0.0", port=PORT)
