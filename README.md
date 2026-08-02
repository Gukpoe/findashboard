# FinDashboard web

Live U.S. + Singapore stocks dashboard (Python / Flask).

- **Steady premium generators** — low-beta U.S. defensives with liquid options
  (thin option chains excluded via CBOE option volume).
- **Unusual volume surge (U.S.)** — 5 min / 15 min / 1 hour / 3 hours / 1 day / 1 week / 1 month
  (short windows use real Yahoo intraday 5-minute bars).
- **Unusual volume surge (Singapore STI)** — 1 day / 1 week / 1 month.
- Live prices every 15 s (Yahoo), click-to-chart (TradingView embed), dark mode.

Data sources: Finviz screeners (scraped), CBOE delayed option quotes, Yahoo
Finance chart/spark APIs. Not investment advice.

## Run locally

```
pip install -r requirements.txt
python app.py            # http://localhost:8588
```

Settings via environment variables: `PORT` (default 8588), `REFRESH_SECONDS`
(default 60), `MIN_OPT_VOLUME` (default 5000), `FINDASH_DATA` (state directory,
default `~/.findash-data`).

## Run locally with a public URL (hotspot / home network)

Double-click `Run Web Dashboard.cmd` — it starts the server and a Cloudflare
tunnel, then prints a `https://….trycloudflare.com` URL usable from any device.
(If your machine blocks `.cmd` too, run these two lines yourself instead — no
launcher file needed:)

```
"%LOCALAPPDATA%\findash-venv\Scripts\python.exe" app.py
"%LOCALAPPDATA%\FinDashboard\cloudflared.exe" tunnel --url http://localhost:8599
```

Tunnels are blocked on the corporate network — use a hotspot or home Wi-Fi.

## Host on Render (free, permanent URL)

1. Push this folder to a GitHub repository.
2. Sign up at https://render.com (free), choose **New > Blueprint**, and pick
   the repository — `render.yaml` configures everything automatically.
3. Your dashboard is live at `https://findashboard-<something>.onrender.com`.

Free-tier notes:
- The instance sleeps after ~15 min without visitors and takes ~60 s to wake.
  Sleeping also clears the 15-min/1-hour volume snapshot history, so those two
  views need ~15/60 min of uptime after a wake before they populate.
- Finviz sometimes blocks datacenter IPs. If the U.S. tables come up empty on
  Render (check Logs for "no rows parsed"), the Yahoo-based views (1 week,
  1 month, all of Singapore) still work. Running on a residential machine
  avoids this entirely.

Any other Python host works the same way: `Procfile` covers Heroku-style
platforms (Railway, Fly.io); the start command is
`waitress-serve --host=0.0.0.0 --port=$PORT app:app`.

## Instant public URL from your own machine (no account)

```
python app.py
cloudflared tunnel --url http://localhost:8588
```

Cloudflare prints a random `https://....trycloudflare.com` URL that anyone can
open. It lasts while both processes run.
