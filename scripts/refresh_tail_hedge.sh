#!/bin/bash
# refresh_tail_hedge.sh
#
# Daily refresh for the Tail Hedge backtester. Pulls today's COR1M / VIX-term /
# SPX-puts / SPX-1min / VIX-1min data, rebuilds cor1m_contango_bundle.json,
# commits and pushes.
#
# Scheduled by ~/Library/LaunchAgents/com.tailhedge.refresh.plist (5 PM ET
# weekdays). Safe to run manually anytime — idempotent on existing data.
#
# Exit codes:
#   0  success (or skipped — weekend / holiday / ThetaData down / no new data)
#   1  partial — committed but push failed
#   2  hard error — couldn't fetch
set -e

REPO=/Users/ravshanrakhmanov/Desktop/spx-backtester/spx-backtester
LOG=/Users/ravshanrakhmanov/Desktop/spx-backtester/spx-backtester/scripts/refresh_tail_hedge.log
THETA=http://localhost:25503/v3
TODAY=$(date +%Y-%m-%d)
TODAY_NODASH=$(date +%Y%m%d)

ts() { date '+%Y-%m-%d %H:%M:%S'; }
log() { echo "[$(ts)] $*" | tee -a "$LOG"; }

cd "$REPO"

log "=== Tail Hedge refresh for $TODAY ==="

# ── 0. Skip weekends ────────────────────────────────────────────────────────
DOW=$(date +%u)  # 1=Mon ... 7=Sun
if [ "$DOW" -ge 6 ]; then
  log "Weekend (DOW=$DOW), skipping."
  exit 0
fi

# ── 0a. Skip US market holidays ─────────────────────────────────────────────
# Added 2026-06-09 audit fix (P0 #6). Previously only weekends were filtered,
# but the header comment claimed "weekend / holiday" handling. On a US holiday
# weekday (Juneteenth, Thanksgiving, Christmas, etc.) the script would run
# and ThetaData would either error or return off-session prints — which the
# CLAUDE.md rule #3 explicitly says are NOT valid trading-day data.
# Keep this list synced with US_MARKET_HOLIDAYS in signal-engine.js.
US_HOLIDAYS_2026="2026-01-01 2026-01-19 2026-02-16 2026-04-03 2026-05-25 2026-06-19 2026-07-03 2026-09-07 2026-11-26 2026-12-25"
US_HOLIDAYS_2027="2027-01-01 2027-01-18 2027-02-15 2027-03-26 2027-05-31 2027-06-18 2027-07-05 2027-09-06 2027-11-25 2027-12-24"
ALL_HOLIDAYS="$US_HOLIDAYS_2026 $US_HOLIDAYS_2027"
for HOL in $ALL_HOLIDAYS; do
  if [ "$TODAY" = "$HOL" ]; then
    log "US market holiday ($TODAY), skipping."
    exit 0
  fi
done

# ── 1. Worker reachable? (Schwab-only since 2026-07-29 — ThetaData index sub
#       lapsed 7/23; all data now flows from the cloud worker's own captures) ──
if ! curl -s -m 6 "https://schwab-proxy.ravamt4.workers.dev/gexgate-today" | grep -qE '[0-9]'; then
  log "Worker unreachable — skipping (will retry next run)."
  exit 0
fi
GEXM_TOKEN=$(cat "$HOME/.gexm_trigger_token" 2>/dev/null)

# ── 2. Pull latest from git first (avoid conflicts with auto: commits) ──────
log "git pull --rebase"
if ! git pull --rebase --autostash >> "$LOG" 2>&1; then
  log "git pull failed — bailing out (no data fetched yet, safe to retry)"
  exit 2
fi

# ── 3. Refresh COR1M (worker KV series last 7d + history daily for older) ──
log "Refreshing COR1M from worker..."
python3 - <<PYEOF3 >> "$LOG" 2>&1
import json, datetime, urllib.request
TOKEN = open('$HOME/.gexm_trigger_token').read().strip()
W = 'https://schwab-proxy.ravamt4.workers.dev'
today = datetime.date.today()
frm = (today - datetime.timedelta(days=8)).isoformat()
req = urllib.request.Request(f'{W}/cor1m-series?from={frm}&to={today.isoformat()}', headers={'X-Sync-Secret': TOKEN, 'User-Agent': 'tail-refresh/1.0'})
kv = json.load(urllib.request.urlopen(req, timeout=60))['days']
hist = json.load(open('history_data.json'))
daily = {h['date']: h['cor1m'] for h in hist if isinstance(h, dict) and h.get('cor1m') is not None}
path = f'data/cor1m/raw_{today.year}_{today.year+1}.csv'
lines = [l for l in open(path).read().splitlines() if l]
hdr = lines[0]
FROZEN = '2026-06-10'   # last ThetaData-sourced row; everything after is rebuilt each run
rows = [l for l in lines[1:] if l[:10] <= FROZEN]
def bar(ts, o, h, l, c): return f'{ts},{o},{h},{l},{c},0,0,0.00'
d = datetime.date(2026, 6, 11)
while d <= today:
    iso = d.isoformat()
    if d.weekday() < 5:
        if iso in kv and len(kv[iso]) >= 3:
            by_h = {}
            for t, v in kv[iso]: by_h.setdefault(t[:2], []).append(float(v))
            for hh in sorted(by_h):
                vs = by_h[hh]
                rows.append(bar(f'{iso}T{hh}:30:00.000', vs[0], max(vs), min(vs), vs[-1]))
        elif iso in daily:
            v = daily[iso]
            rows.append(bar(f'{iso}T09:30:00.000', v, v, v, v))
    d += datetime.timedelta(days=1)
open(path, 'w').write(hdr + '\n' + '\n'.join(rows) + '\n')
print(f'  COR1M: rebuilt post-{FROZEN} tail, last row {rows[-1][:16]}')
PYEOF3

# ── 4. VIX term structure from worker daily feed (data/vix_term_daily.json,
#       Schwab closes appended at the 16:25 cloud tick; open cols carry close
#       values for post-2026-06-10 rows — the bundle uses closes) ───────────
log "Extending VIX term daily.csv from worker feed..."
python3 - <<PYEOF4 >> "$LOG" 2>&1
import json, csv
feed = json.load(open('data/vix_term_daily.json'))
path = 'data/vix_term/daily.csv'
rows = list(csv.reader(open(path)))
hdr, have = rows[0], {r[0] for r in rows[1:] if r}
added = 0
for d in sorted(feed):
    if d in have: continue
    v = feed[d]
    if not all(k in v for k in ('vix9d','vix','vix3m','vix6m','vvix')): continue
    r3 = v['vix']/v['vix3m'] if v['vix3m'] else ''
    r9 = v['vix9d']/v['vix'] if v['vix'] else ''
    rows.append([d, v['vix9d'], v['vix9d'], v['vix'], v['vix'], v['vix3m'], v['vix3m'],
                 v['vix6m'], v['vix6m'], v['vvix'], v['vvix'],
                 round(r3,4), round(r3,4), round(r9,4), round(r9,4)])
    added += 1
if added:
    rows = [rows[0]] + sorted(rows[1:], key=lambda r: r[0])
    with open(path, 'w', newline='') as f: csv.writer(f).writerows(rows)
print(f'  VIX term: +{added} day(s), last = {rows[-1][0]}')
PYEOF4
log "  VIX term done: last day = $(tail -1 data/vix_term/daily.csv | cut -d, -f1)"

# ── 5. SPX & VIX 1-min bars via Schwab (complete days only) ────────────────
log "Fetching SPX + VIX 1-min bars from Schwab..."
if [ "$(TZ=America/New_York date +%H%M)" -ge 1615 ] || [ "$(TZ=America/New_York date +%u)" -ge 6 ]; then
  python3 backfill_schwab_spx.py >> "$LOG" 2>&1
else
  log "  before 16:15 ET — skipping 1-min backfill (avoid partial-day files)"
fi

# ── 6. SPX 9:35 put snapshot for today ─────────────────────────────────────
log "Fetching today's 9:35 SPX put snapshot..."
if curl -s -m 3 "$THETA/option/history/quote?symbol=SPXW&expiration=$TODAY&date=$TODAY&interval=5m&format=csv" | head -c 30 | grep -q symbol; then
  python3 fetch_thetadata_diagonal.py --time 09:35 --date "$TODAY" >> "$LOG" 2>&1
else
  log "  ThetaData options unavailable (sub ends 2026-08-02) — worker's diag_chains capture covers this"
fi

# ── 7. Rebuild bundle ──────────────────────────────────────────────────────
log "Rebuilding cor1m_contango_bundle.json..."
python3 build_cor1m_contango_bundle.py >> "$LOG" 2>&1

# ── 7a. Backfill today's cor1m into history_data.json ──────────────────────
# (2026-06-09: added so the historical table's COR1M column auto-populates for
# today after EOD, alongside m8bfPL / vixClose / etc. that the worker writes.)
# Uses push-history.sh to atomically sync KV + GitHub — never plain `git push`
# on history_data.json (see CLAUDE.md rule #1).
log "Backfilling today's cor1m into history_data.json..."
TODAY_COR1M=$(python3 -c "
import json
b = json.load(open('cor1m_contango_bundle.json'))
for d in b['daily']:
    if d.get('date') == '$TODAY' and d.get('cor1m') is not None:
        print(round(float(d['cor1m']), 2)); break
" 2>/dev/null)

if [ -z "$TODAY_COR1M" ]; then
  log "  cor1m not yet computed for $TODAY (skipping history backfill)"
else
  # Check whether history_data.json already has today's cor1m (idempotent)
  HAS_COR1M=$(python3 -c "
import json
h = json.load(open('history_data.json'))
for r in h:
    if r.get('date') == '$TODAY':
        print('yes' if r.get('cor1m') is not None else 'no'); break
" 2>/dev/null)

  if [ "$HAS_COR1M" = "yes" ]; then
    log "  cor1m already in history_data.json for $TODAY ($TODAY_COR1M)"
  else
    # Surgical edit: add ONLY cor1m to today's row. Preserves all worker EOD writes.
    python3 -c "
import json
with open('history_data.json') as f: h = json.load(f)
for r in h:
    if r.get('date') == '$TODAY':
        r['cor1m'] = $TODAY_COR1M; break
with open('history_data.json','w') as f: json.dump(h, f, separators=(',',':'))
print('  cor1m=$TODAY_COR1M written for $TODAY')
" 2>&1 | tee -a "$LOG"

    # Sync KV + GitHub via canonical script (never a bare git push on this file)
    if ./scripts/push-history.sh "auto: $TODAY cor1m=$TODAY_COR1M (Tail Hedge refresh)" >> "$LOG" 2>&1; then
      log "  ✓ Synced to KV + GitHub"
    else
      log "  ✗ push-history.sh failed — history_data.json mutation rolled back manually if needed"
    fi
  fi
fi

# ── 8. Detect what changed, commit + push if anything did ──────────────────
TO_ADD=(
  cor1m_contango_bundle.json
  data/cor1m/raw_*.csv
  data/vix_term/daily.csv
)
# Also include today's bars + snapshot (the snapshot path may not match the glob below; add explicitly)
git add "${TO_ADD[@]}" 2>/dev/null || true
git add "data/spx/SPX_${TODAY_NODASH}.csv" "data/vix/VIX_${TODAY_NODASH}.csv" 2>/dev/null || true
git add "data/polygon/SPX_${TODAY_NODASH}_0935.json" 2>/dev/null || true

if git diff --cached --quiet; then
  log "No changes to commit (data was already current)."
  exit 0
fi

CHANGED=$(git diff --cached --name-only | wc -l | tr -d ' ')
log "Committing $CHANGED files..."
git commit -m "auto: tail hedge refresh $TODAY" >> "$LOG" 2>&1

if git push >> "$LOG" 2>&1; then
  log "✓ Pushed. Done."
  exit 0
else
  log "✗ Push failed (commit succeeded locally)."
  exit 1
fi
