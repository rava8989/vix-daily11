/**
 * Schwab API Proxy — Cloudflare Worker
 *
 * Routes:
 *   POST /token   → Token exchange & refresh (injects Basic auth from env secret)
 *   GET  /market/* → Pass-through proxy for market data (forwards Bearer token)
 *   POST /sync    → Browser pushes tokens/creds/discord config to KV
 *   GET  /status  → Returns last cron run result from KV
 *   OPTIONS *      → CORS preflight
 *
 * Env vars (set in Cloudflare dashboard):
 *   SCHWAB_APP_SECRET  — encrypted secret from Schwab developer portal
 *   ALLOWED_ORIGIN     — e.g. https://user.github.io
 *   SYNC_SECRET        — shared secret for /sync endpoint
 *
 * KV binding: SIGNAL_KV
 */

// ════════════════════════════════════════════════════════════════════
// RATE LIMITING
// ════════════════════════════════════════════════════════════════════

const rateLimitMap = new Map(); // Map<ip, {count: number, reset: number}>
let _tokenRefreshPromise = null; // mutex: prevents concurrent Schwab token refreshes

// ════════════════════════════════════════════════════════════════════
// SCHWAB REFRESH HEALTH — circuit-breaker state written to KV
// ────────────────────────────────────────────────────────────────────
// Surfaces "KV says tokens valid, Schwab says no" to the browser so the
// dashboard can show a red "re-auth now" banner instead of silently
// serving stale data for 24h.
// Shape:
//   { ok: true,  lastSuccess: <ms> }
//   { ok: false, lastSuccess: <ms>, lastError: <ms>, msg, consecutiveErrors }
// ════════════════════════════════════════════════════════════════════
async function recordRefreshHealth(env, ok, msg = null) {
  try {
    const prevRaw = await env.SIGNAL_KV.get('schwab_refresh_state');
    const prev = prevRaw ? JSON.parse(prevRaw) : {};
    const now = Date.now();
    const state = ok
      ? { ok: true, lastSuccess: now, consecutiveErrors: 0 }
      : {
          ok: false,
          lastSuccess: prev.lastSuccess || null,
          lastError: now,
          msg: String(msg || '').slice(0, 300),
          consecutiveErrors: (prev.consecutiveErrors || 0) + 1,
        };
    await env.SIGNAL_KV.put('schwab_refresh_state', JSON.stringify(state));
  } catch (e) {
    // Never let health-tracking break the main flow
    console.warn('[proxy] recordRefreshHealth failed:', e.message || e);
  }
}

// ── GitHub-mirror health (2026-06-09) ──
// Same pattern as recordRefreshHealth, KV key 'history_mirror_state'.
// Added after the expired-PAT incident: mirror failures were fully silent
// (console.warn only), so KV drifted ahead of GitHub for DAYS before the
// user noticed empty dashboard rows. /health now surfaces this state and
// the cron watchdog alerts Discord after repeated failures.
async function recordMirrorHealth(env, ok, msg = null) {
  try {
    const prevRaw = await env.SIGNAL_KV.get('history_mirror_state');
    const prev = prevRaw ? JSON.parse(prevRaw) : {};
    const now = Date.now();
    const state = ok
      ? { ok: true, lastSuccess: now, consecutiveErrors: 0 }
      : {
          ok: false,
          lastSuccess: prev.lastSuccess || null,
          lastError: now,
          msg: String(msg || '').slice(0, 300),
          consecutiveErrors: (prev.consecutiveErrors || 0) + 1,
        };
    await env.SIGNAL_KV.put('history_mirror_state', JSON.stringify(state));
  } catch (e) {
    console.warn('[proxy] recordMirrorHealth failed:', e.message || e);
  }
}

// ── COR1M + VVIX cloud capture (2026-06-09 — machine-independence) ─────
// Schwab quotes $COR1M / $VVIX live (validated vs ThetaData: exact match;
// no minute pricehistory exists for $COR1M, so the worker builds its own
// intraday series from quote samples). Replaces the Mac-bound ThetaData
// LaunchAgent as the LIVE data source; the local pipeline remains for the
// backtester bundle (research) only.
//
// KV keys (7-day TTL):
//   cor1m_open_<date>   {cor1m, vvix, at}         — first sample ≥ 9:30 ET
//   cor1m_series_<date> [["HH:MM", value], ...]   — ~5-min samples, cap 120
//   tail_trigger_state  {state:'TRIGGERED', since, value, detectedAt}
const COR1M_TRIGGER_THRESHOLD = 7.75;   // Balanced (recommended) preset (cor1m_contango.html)

async function captureCor1mVvix(env, etNow, token) {
  const h = etNow.getHours(), m = etNow.getMinutes();
  const todayISO = isoDateET(etNow);
  const openKey = `cor1m_open_${todayISO}`;
  const haveOpenRaw = await env.SIGNAL_KV.get(openKey);
  let haveOpen = null;
  try { haveOpen = haveOpenRaw ? JSON.parse(haveOpenRaw) : null; } catch (_) {}
  // Open window runs 9:30–10:00: $COR1M is a calculated index whose FIRST
  // print can arrive 9:35–9:40 ET (2026-06-10: 9:37). The old 9:36 cutoff
  // plus no freshness check captured yesterday's close as today's open.
  const sinceOpenMin = (h - 9) * 60 + (m - 30);
  const inOpenWindow = sinceOpenMin >= 0 && sinceOpenMin <= 30;
  const needVvixBackfill = haveOpen != null && (haveOpen.vvix == null || haveOpen.vixeq == null);
  const isSampleTick = m % 5 === 0;
  if (!(inOpenWindow && (!haveOpen || needVvixBackfill)) && !isSampleTick) return;   // throttle

  const q = await fetchSchwabJSON(
    'https://api.schwabapi.com/marketdata/v1/quotes?symbols=%24COR1M,%24VVIX,%24VIXEQ&fields=quote',
    token, env);
  // Freshness gate (2026-06-10): until an index publishes its first print of
  // the day, Schwab's quote serves the PRIOR session's last value (with a
  // stale tradeTime). Only accept a print from the last 10 minutes — during
  // RTH these indices republish every ~15s, so fresh data always qualifies.
  const isFreshQ = (qq) => {
    const tt = qq?.tradeTime ?? qq?.quoteTime;
    return tt != null && tt > 0 && (Date.now() - tt) < 10 * 60 * 1000;
  };
  const corQ = q?.['$COR1M']?.quote, vvixQ = q?.['$VVIX']?.quote, vixeqQ = q?.['$VIXEQ']?.quote;
  const corRaw = isFreshQ(corQ) ? corQ?.lastPrice : null;
  const vvixRaw = isFreshQ(vvixQ) ? vvixQ?.lastPrice : null;
  const vvix = (vvixRaw != null && vvixRaw > 0) ? parseFloat(vvixRaw.toFixed(2)) : null;
  const vixeqRaw = isFreshQ(vixeqQ) ? vixeqQ?.lastPrice : null;
  const vixeq = (vixeqRaw != null && vixeqRaw > 0) ? parseFloat(vixeqRaw.toFixed(2)) : null;

  // Aux backfill: open captured on an earlier tick before VVIX/VIXEQ printed.
  if (needVvixBackfill && (vvix != null || vixeq != null)) {
    if (haveOpen.vvix == null && vvix != null) haveOpen.vvix = vvix;
    if (haveOpen.vixeq == null && vixeq != null) haveOpen.vixeq = vixeq;
    await env.SIGNAL_KV.put(openKey, JSON.stringify(haveOpen), { expirationTtl: 7 * 86400 });
  }
  if (corRaw == null || !(corRaw > 0)) return;   // no fresh COR1M print yet — retry next tick
  const cor = parseFloat(corRaw.toFixed(2));
  const hhmm = `${String(h).padStart(2, '0')}:${String(m).padStart(2, '0')}`;

  // Read today's series (also yields prev sample for cross detection)
  const serKey = `cor1m_series_${todayISO}`;
  let series = [];
  try { const sRaw = await env.SIGNAL_KV.get(serKey); series = sRaw ? JSON.parse(sRaw) : []; } catch (_) {}
  let prev = series.length ? series[series.length - 1][1] : null;

  // First sample of the day: capture open + use the PRIOR session's last
  // sample as `prev` so overnight crosses are caught (any-cross semantics).
  if (!haveOpen) {
    await env.SIGNAL_KV.put(openKey, JSON.stringify({ cor1m: cor, vvix, vixeq, at: hhmm }),
      { expirationTtl: 7 * 86400 });
    if (prev == null) {
      for (let back = 1; back <= 4 && prev == null; back++) {
        const d = new Date(etNow); d.setDate(d.getDate() - back);
        try {
          const pRaw = await env.SIGNAL_KV.get(`cor1m_series_${isoDateET(d)}`);
          if (pRaw) { const ps = JSON.parse(pRaw); if (ps.length) prev = ps[ps.length - 1][1]; }
        } catch (_) {}
      }
    }
  }

  series.push([hhmm, cor]);
  if (series.length > 120) series = series.slice(-120);
  await env.SIGNAL_KV.put(serKey, JSON.stringify(series), { expirationTtl: 7 * 86400 });

  // LEVEL arming (owner 2026-07-20, "re-arm while under threshold"): the Tail
  // Hedge stays armed whenever COR1M ≤ threshold, not just on the cross-down.
  // After a profitable exit (state RESOLVED), it re-arms the NEXT day if COR1M
  // is still ≤ threshold — because the complacency regime (and its tail risk)
  // persists. Backtest --no-rearm: +$79,550 vs +$79,340, SAME worst day/DD;
  // near-flat P/L but closes the "insured on entry, naked while still complacent"
  // gap. Level subsumes the old cross-down. A same-day guard (resolvedOn<today)
  // prevents re-arming the instant a trade resolves.
  if (cor <= COR1M_TRIGGER_THRESHOLD) {
    const stRaw = await env.SIGNAL_KV.get('tail_trigger_state');
    const st = stRaw ? JSON.parse(stRaw) : null;
    const canArm = !st || st.state === 'WAITING'
      || (st.state === 'RESOLVED' && st.resolvedOn && st.resolvedOn < todayISO);
    if (canArm) {
      const reArm = !!(st && st.state === 'RESOLVED');   // vs a fresh cross-in
      await env.SIGNAL_KV.put('tail_trigger_state', JSON.stringify({
        state: 'TRIGGERED', since: todayISO, value: cor,
        detectedAt: new Date().toISOString(), source: 'cloud-quote',
      }));
      _tailHedgeCache = { value: null, fetchedAt: 0 };  // drop stale 'No trade' line (mirrors settle path)
      try {
        const dcRaw = await env.SIGNAL_KV.get('discord_config');
        if (dcRaw) {
          const dc = JSON.parse(dcRaw);
          if (dc.channelId) {
            await sendDiscordDM(env, dc.channelId,
              `📉 **COR1M ${reArm ? 're-armed' : 'below'} ${COR1M_TRIGGER_THRESHOLD}** (now ${cor}, ${hhmm} ET).\nTail Hedge trigger ACTIVE — buy the 9:45 ET put daily; after each profit it re-arms the next day while COR1M stays ≤ ${COR1M_TRIGGER_THRESHOLD} (skip days with VVIX ≥ 110${vvix != null ? `; VVIX now ${vvix}` : ''}).`,
              dc.proxyUrl);
          }
        }
      } catch (_) { /* notify best-effort */ }
    }
  }
}

// Today's cloud-captured COR1M open (freshness-gated by captureCor1mVvix).
// Returns the number or null when not yet captured — callers pass it to
// calculateSignal({ cor1m }) so the Diagonal COR1M_LOW filter can evaluate.
async function getCor1mOpenToday(env, todayISO) {
  try {
    const raw = await env.SIGNAL_KV.get(`cor1m_open_${todayISO}`);
    if (!raw) return null;
    const v = JSON.parse(raw)?.cor1m;
    return (v != null && isFinite(v)) ? v : null;
  } catch (_) { return null; }
}

// ════════════════════════════════════════════════════════════════════
// RESEARCH DATA CAPTURE (2026-06-10) — intraday fly marks + 9:45 put snap
// Cloud-side data collection so future research (TP/stop rules for the
// flies, Tail Hedge backtest extension without ThetaData) has raw material.
// KV keys (90-day TTL), persisted to GitHub monthly files at EOD:
//   fly_marks_<date>      {strat: [["HH:MM", mid], ...]}
//   tail_put_snap_<date>  {at, spot, puts: [{e,k,d,b,a}, ...]}
// ════════════════════════════════════════════════════════════════════

async function archiveFlyMarks(env, etNow) {
  const h = etNow.getHours(), m = etNow.getMinutes();
  if (m % 5 !== 0) return;                              // 5-min cadence
  if (h < 9 || (h === 9 && m < 35) || h >= 16) return;  // RTH after entry window
  const todayISO = isoDateET(etNow);
  const hhmm = `${String(h).padStart(2, '0')}:${String(m).padStart(2, '0')}`;
  const marks = {};
  const sources = [
    ['strad', 'straddle_open_trade'],
    ['bobf',  'bobf_open_trade'],
    ['gxbf',  'gxbf_open_trade'],
    ['diag',  'diagonal_open_trade'],
  ];
  for (const [strat, key] of sources) {
    try {
      const raw = await env.SIGNAL_KV.get(key);
      if (!raw) continue;
      const t = JSON.parse(raw);
      if (!t || t.currentValue == null || !isFinite(t.currentValue)) continue;
      // 0DTE strategies: only today\'s trade. Diagonal: any still-open trade
      // (it holds overnight; exit is next session).
      const isToday = t.openDate === todayISO;
      const stillOpen = !t.closeDate && t.status !== 'closed' && t.status !== 'expired';
      if (strat === 'diag' ? (stillOpen || isToday) : isToday) marks[strat] = t.currentValue;
    } catch (_) { /* per-strategy best-effort */ }
  }
  try {  // M8BF live mark (separate key shape — keyed by 0DTE expiry = today)
    const raw = await env.SIGNAL_KV.get(`m8bf_live_${todayISO}`);
    if (raw) {
      const r = JSON.parse(raw);
      const v = [r.currentValue, r.netDebitNow, r.debit].find(x => x != null && isFinite(x));
      if (v != null) marks.m8bf = v;
    }
  } catch (_) {}
  if (!Object.keys(marks).length) return;
  const k = `fly_marks_${todayISO}`;
  let day = {};
  try { const raw = await env.SIGNAL_KV.get(k); day = raw ? JSON.parse(raw) : {}; } catch (_) {}
  for (const [strat, v] of Object.entries(marks)) {
    (day[strat] = day[strat] || []).push([hhmm, parseFloat(Number(v).toFixed(2))]);
    if (day[strat].length > 100) day[strat] = day[strat].slice(-100);
  }
  await env.SIGNAL_KV.put(k, JSON.stringify(day), { expirationTtl: 90 * 86400 });
}

// Black-Scholes put delta — used to recover the tail's delta from a greeks-less
// Tasty fallback chain (which zeroes delta) so the 9:45 Δ-0.10 picker still works
// when Schwab's chain is unavailable (audit P1-f 2026-07-06).
function _normCdf(x) {
  // Abramowitz-Stegun 7.1.26
  const t = 1 / (1 + 0.2316419 * Math.abs(x));
  const d = 0.3989423 * Math.exp(-x * x / 2);
  const p = d * t * (0.3193815 + t * (-0.3565638 + t * (1.781478 + t * (-1.821256 + t * 1.330274))));
  return x >= 0 ? 1 - p : p;
}
function bsPutDelta(S, K, sigma, T) {
  if (!(S > 0) || !(K > 0) || !(sigma > 0) || !(T > 0)) return null;
  const R = 0.043, Q = 0.013;
  const d1 = (Math.log(S / K) + (R - Q + 0.5 * sigma * sigma) * T) / (sigma * Math.sqrt(T));
  return -Math.exp(-Q * T) * _normCdf(-d1);
}

async function captureTailPutSnap(env, etNow, masterChain) {
  const h = etNow.getHours(), m = etNow.getMinutes();
  if (!(h === 9 && m >= 45 && m <= 59)) return;   // AT the 9:45 tail entry time (quote == entry)
  if (!masterChain || !masterChain.putExpDateMap) return;
  const todayISO = isoDateET(etNow);
  const k = `tail_put_snap_${todayISO}`;
  if (await env.SIGNAL_KV.get(k)) return;          // once per day
  const exps = Object.keys(masterChain.putExpDateMap).sort().slice(0, 2);  // 0-1 DTE
  const puts = [];
  const etHrs = etNow.getHours() + etNow.getMinutes() / 60;
  for (const exp of exps) {
    const strikes = masterChain.putExpDateMap[exp] || {};
    const dte = parseInt((exp.split(':')[1] || '0'), 10);
    // T = hours-to-16:00-ET across the DTE days, floored so it never collapses.
    const T = Math.max((dte * 24 + Math.max(16 - etHrs, 0.25)) / (365 * 24), 0.25 / (365 * 24));
    for (const [strike, arr] of Object.entries(strikes)) {
      const c = Array.isArray(arr) ? arr[0] : arr;
      if (!c) continue;
      let d = (c.delta != null) ? Number(c.delta) : null;
      // Tasty fallback chains carry no greeks (delta coerced to 0) — recover a BS put
      // delta from the chain's IV so the Δ-0.10 picker still works (audit P1-f 2026-07-06).
      if ((d == null || d === 0) && c.volatility) {
        d = bsPutDelta(masterChain.spot, parseFloat(strike), (c.volatility || 0) / 100, T);
      }
      if (d == null || !(d <= -0.05 && d >= -0.35)) continue;
      puts.push({ e: exp.split(':')[0], k: parseFloat(strike), d: parseFloat(d.toFixed(3)),
                  b: c.bid ?? null, a: c.ask ?? null });
    }
  }
  if (!puts.length) {
    // audit P1-f: a greeks/IV-less fallback chain empties the Δ picker exactly
    // when Schwab is down — the one day the redundancy matters. Never silent:
    // one owner DM per day so a triggered tail can be placed manually.
    if (!TAIL_RETIRED && await claimSendSlot(env, `tail_snap_alert_${todayISO}`)) {
      try {
        const dcRaw = await env.SIGNAL_KV.get('discord_config');
        const dc = dcRaw ? JSON.parse(dcRaw) : null;
        if (dc?.channelId) {
          // P34 sweep (2026-08-03): mark 'sent' ONLY on confirmed delivery —
          // this DM is the ONLY notice that a triggered tail needs manual
          // placement; losing it silently costs the day's hedge.
          const rT = await sendDiscordDM(env, dc.channelId,
            `⚠️ **Tail snapshot EMPTY at 9:45** (chain source: ${masterChain._source || '?'} — likely a greeks-less fallback). ` +
            `If COR1M triggered today, the tail put must be placed MANUALLY.`, dc.proxyUrl);
          if (rT && rT.ok) await env.SIGNAL_KV.put(`tail_snap_alert_${todayISO}`, 'sent', { expirationTtl: 86400 });
          else console.warn('[tail-snap-alert] undelivered — claim expires for retry');
        }
      } catch (e2) { console.warn('[tail-snap-alert]', e2.message); }
    }
    return;
  }
  puts.sort((x, y) => x.e.localeCompare(y.e) || x.k - y.k);
  const hhmm = `${String(h).padStart(2, '0')}:${String(m).padStart(2, '0')}`;
  await env.SIGNAL_KV.put(k, JSON.stringify({ at: hhmm, spot: masterChain.spot ?? null, puts: puts.slice(0, 60) }),
    { expirationTtl: 90 * 86400 });
}

// Diagonal chain snapshot (2026-06-10, user: "capture diagonal like tail
// hedge so we can drop ThetaData"). At ~12:30 ET (the live entry time) store
// the SPX PUT chain slice the Diagonal pipeline needs: expiries 0-2 DTE
// (short leg + next-day exit) and 15-40 DTE (long leg band), strikes within
// spot±150 (covers +10 ITM short, -20 long, and next-day drift for exits —
// generous enough to re-test other offsets/widths later).
async function captureDiagChainSnap(env, etNow, masterChain) {
  const h = etNow.getHours(), m = etNow.getMinutes();
  if (!(h === 12 && m >= 25 && m <= 40)) return;
  if (!masterChain || !masterChain.putExpDateMap || !masterChain.spot) return;
  const todayISO = isoDateET(etNow);
  const k = `diag_chain_snap_${todayISO}`;
  if (await env.SIGNAL_KV.get(k)) return;
  const spot = masterChain.spot;
  const out = {};
  for (const [expKey, strikes] of Object.entries(masterChain.putExpDateMap)) {
    const dte = parseInt(expKey.split(':')[1] || '0', 10);
    if (!((dte >= 0 && dte <= 2) || (dte >= 15 && dte <= 40))) continue;
    const exp = expKey.split(':')[0];
    const rows = [];
    for (const [strike, arr] of Object.entries(strikes)) {
      const kk = parseFloat(strike);
      if (Math.abs(kk - spot) > 150) continue;
      const c = Array.isArray(arr) ? arr[0] : arr;
      if (!c) continue;
      rows.push([kk, c.bid ?? null, c.ask ?? null]);
    }
    if (rows.length) { rows.sort((a, b) => a[0] - b[0]); out[`${exp}:${dte}`] = rows; }
  }
  if (!Object.keys(out).length) return;
  const hhmm = `${String(h).padStart(2, '0')}:${String(m).padStart(2, '0')}`;
  await env.SIGNAL_KV.put(k, JSON.stringify({ at: hhmm, spot, puts: out }), { expirationTtl: 90 * 86400 });
}

// GXBF chain snapshot — at ~9:35 ET store the 0DTE SPX CALL chain within
// ±5% of spot incl. per-strike bid/ask/IV/volume/OI: everything build_day
// (fetch_thetadata_gxbf.py) derives centers + mids grids from. With this,
// gxbf_bt_data can be extended Schwab-only.
async function captureGxbfChainSnap(env, etNow, masterChain) {
  const h = etNow.getHours(), m = etNow.getMinutes();
  if (!(h === 9 && m >= 35 && m <= 50)) return;
  if (!masterChain || !masterChain.callExpDateMap || !masterChain.spot) return;
  const todayISO = isoDateET(etNow);
  const k = `gxbf_chain_snap_${todayISO}`;
  if (await env.SIGNAL_KV.get(k)) return;
  const spot = masterChain.spot;
  const expKey = Object.keys(masterChain.callExpDateMap).find(e => (e.split(':')[1] || '') === '0');
  if (!expKey) return;
  const rows = [];
  for (const [strike, arr] of Object.entries(masterChain.callExpDateMap[expKey])) {
    const kk = parseFloat(strike);
    if (kk < spot * 0.95 || kk > spot * 1.05) continue;
    const c = Array.isArray(arr) ? arr[0] : arr;
    if (!c) continue;
    rows.push([kk, c.bid ?? null, c.ask ?? null, c.volatility ?? null, c.totalVolume ?? 0, c.openInterest ?? 0]);
  }
  if (!rows.length) return;
  rows.sort((a, b) => a[0] - b[0]);
  const hhmm = `${String(h).padStart(2, '0')}:${String(m).padStart(2, '0')}`;
  await env.SIGNAL_KV.put(k, JSON.stringify({ at: hhmm, spot, exp: expKey.split(':')[0], calls: rows }),
    { expirationTtl: 90 * 86400 });
}

// VIX-surface snapshot (2026-06-11, optionsgelt-inspired VIX decomposition).
// At ~15:45 ET store the ~30DTE SPX smile at a sparse moneyness grid
// (85%–110% of spot, OTM side: puts below / calls above, both at ATM).
// masterChain is only ±$200 wide, so this makes its own chain call pinned to
// ONE expiry ≈ today+30d (weekend-rolled; walks ±1/2 days on holiday misses —
// SPXW expires daily, so the first probe almost always hits) with
// strikeCount=500 for the deep-put wing. Schwab supplies per-contract
// `volatility`, so day-over-day smile moves decompose into sticky-strike /
// parallel / skew components with NO IV solving. Rows: [k, right, bid, ask, iv, delta].
const SURFACE_MONEYNESS = [0.85, 0.88, 0.90, 0.92, 0.94, 0.96, 0.98, 1.00, 1.02, 1.04, 1.06, 1.10];
async function captureVixSurfaceSnap(env, etNow, token) {
  const h = etNow.getHours(), m = etNow.getMinutes();
  if (!(h === 15 && m >= 40 && m <= 55)) return;
  if (!token) return;
  const todayISO = isoDateET(etNow);
  const kvKey = `vix_surface_snap_${todayISO}`;
  if (await env.SIGNAL_KV.get(kvKey)) return;       // once per day
  // target expiry: today+30 calendar days, rolled off weekends
  const base = new Date(etNow); base.setDate(base.getDate() + 30);
  if (base.getDay() === 6) base.setDate(base.getDate() + 2);      // Sat → Mon
  else if (base.getDay() === 0) base.setDate(base.getDate() + 1); // Sun → Mon
  let data = null, expKey = null;
  for (const off of [0, 1, -1, 2, -2]) {
    const d = new Date(base); d.setDate(d.getDate() + off);
    if (d.getDay() === 0 || d.getDay() === 6) continue;
    const iso = isoDateET(d);
    try {
      const cand = await fetchSchwabJSON(
        `https://api.schwabapi.com/marketdata/v1/chains?symbol=%24SPX&strikeCount=500&fromDate=${iso}&toDate=${iso}&includeUnderlyingQuote=true&strategy=SINGLE&contractType=ALL`,
        token, env);
      const keys = Object.keys(cand?.putExpDateMap || {});
      if (keys.length) { data = cand; expKey = keys[0]; break; }
    } catch (_) { /* try next candidate day */ }
  }
  if (!data || !expKey) return;
  const spot = data.underlyingPrice || data.underlying?.last;
  if (!spot) return;
  const pickRows = (map, right) => {
    const strikes = Object.keys(map?.[expKey] || {}).map(parseFloat).sort((a, b) => a - b);
    const rows = [];
    for (const mny of SURFACE_MONEYNESS) {
      if (right === 'P' && mny > 1.001) continue;   // puts: ≤ ATM
      if (right === 'C' && mny < 0.999) continue;   // calls: ≥ ATM
      const target = spot * mny;
      let best = null, bd = Infinity;
      for (const s of strikes) { const d = Math.abs(s - target); if (d < bd) { bd = d; best = s; } }
      if (best == null || bd > spot * 0.02) continue;   // no strike within 2% of target → skip point
      const arr = map[expKey][String(best)] ?? map[expKey][best.toFixed(1)];
      const c = Array.isArray(arr) ? arr[0] : arr;
      if (!c) continue;
      const iv = (c.volatility != null && c.volatility > 0 && c.volatility < 500) ? c.volatility : null;
      rows.push([best, right, c.bid ?? null, c.ask ?? null, iv, c.delta ?? null]);
    }
    return rows;
  };
  const rows = [...pickRows(data.putExpDateMap, 'P'), ...pickRows(data.callExpDateMap, 'C')];
  if (rows.length < 6) return;                       // too sparse to be useful — retry next tick
  const hhmm = `${String(h).padStart(2, '0')}:${String(m).padStart(2, '0')}`;
  await env.SIGNAL_KV.put(kvKey, JSON.stringify(
    { at: hhmm, spot, exp: expKey.split(':')[0], dte: parseInt(expKey.split(':')[1], 10), rows }),
    { expirationTtl: 90 * 86400 });
}

// ════════════════════════════════════════════════════════════════════
// EVENING ALL-EXPIRY BOOK — bank-scale (2026-07-31, Gap card)
// ────────────────────────────────────────────────────────────────────
// Reproduces the 1,009-night research bank's formula EXACTLY so the gap
// ladder's quintile cuts (−13.9 / +10.5 / +24.7 / +34.8 B) keep applying
// after ThetaData: OI-only, fixed IV 15%, r 4.3% q 1.3%, T=(DTE+0.5)/365,
// strikes within ±12% of spot (the bank's stored window), expirations
// 1..45 DTE. Schwab's chain OI is the same morning print ThetaData served,
// so the live number continues the banked series 1:1. Do NOT confuse with
// calculateGEX's real-IV all-expiry total — different scale by design.
const EVBOOK_R = 0.043, EVBOOK_Q = 0.013, EVBOOK_IV = 0.15;
function _evbookGamma(S, K, T) {
  if (T <= 0) return 0;
  const sq = EVBOOK_IV * Math.sqrt(T);
  const d1 = (Math.log(S / K) + (EVBOOK_R - EVBOOK_Q + 0.5 * EVBOOK_IV * EVBOOK_IV) * T) / sq;
  const npdf = Math.exp(-0.5 * d1 * d1) / Math.sqrt(2 * Math.PI);
  return npdf * Math.exp(-EVBOOK_Q * T) / (S * sq);
}
async function computeEveningBook(env, token, etNow) {
  // Schwab 502s on one 46-day × 350-strike chain call (observed 2026-07-31)
  // — fetch in 7-day windows instead, one retry each; NO partial books (a
  // missing chunk would shift the scale, so any chunk failing twice throws).
  let tot = 0, oiSum = 0, nExp = 0, kLo = Infinity, kHi = -Infinity, S = null;
  // Close from the QUOTE's last index print, NOT chain underlyingPrice — the
  // chain's post-close underlying drifts (observed 7489.7 vs real 7503.9 close
  // 2026-07-31), which would poison the gap measurement at a 0.4% threshold.
  // $SPX lastPrice after 16:00 = the final index print and stays static.
  let vixClose = null;
  try {
    const q = await fetchSchwabJSON('https://api.schwabapi.com/marketdata/v1/quotes?symbols=%24SPX,%24VIX&fields=quote', token, env);
    const lp = q?.['$SPX']?.quote?.lastPrice;
    if (lp && lp > 1000) S = lp;
    const vx = q?.['$VIX']?.quote?.lastPrice;
    if (vx && vx > 5 && vx < 150) vixClose = Math.round(vx * 100) / 100;
  } catch (e) { console.warn('[evening-book] quote close failed, chain fallback:', e.message); }
  const seenExp = new Set();
  for (let off = 1; off <= 46; off += 7) {
    const from = new Date(etNow); from.setDate(from.getDate() + off);
    const to = new Date(etNow); to.setDate(to.getDate() + Math.min(off + 6, 46));
    const url = `https://api.schwabapi.com/marketdata/v1/chains?symbol=%24SPX&strikeCount=350&fromDate=${isoDateET(from)}&toDate=${isoDateET(to)}&includeUnderlyingQuote=true&strategy=SINGLE&contractType=ALL`;
    let chain = null;
    for (let attempt = 0; attempt < 2 && !chain; attempt++) {
      try { chain = await fetchSchwabJSON(url, token, env); }
      catch (e) {
        if (attempt === 1) throw new Error(`chunk +${off}d failed twice: ${e.message}`);
        await new Promise(r => setTimeout(r, 900));
      }
    }
    if (!S) S = chain.underlyingPrice || chain.underlying?.last || chain.underlying?.mark;
    if (!S) throw new Error('no SPX spot in chain');
    for (const [map, sign] of [[chain.callExpDateMap || {}, 1], [chain.putExpDateMap || {}, -1]]) {
      for (const expKey in map) {
        const dte = parseInt(expKey.split(':')[1], 10);
        if (!(dte >= 1 && dte <= 45)) continue;
        const expTag = expKey.split(':')[0] + ':' + sign;
        if (seenExp.has(expTag)) continue;          // window overlap guard
        seenExp.add(expTag);
        if (sign === 1) nExp++;
        const T = (dte + 0.5) / 365;
        for (const ks in map[expKey]) {
          const K = parseFloat(ks);
          if (!K || Math.abs(K - S) > S * 0.12) continue;
          const c = Array.isArray(map[expKey][ks]) ? map[expKey][ks][0] : map[expKey][ks];
          const oi = c?.openInterest || 0;
          if (!oi) continue;
          oiSum += oi;
          if (K < kLo) kLo = K; if (K > kHi) kHi = K;
          tot += sign * oi * _evbookGamma(S, K, T) * S * S * 100 * 0.01;
        }
      }
    }
  }
  if (oiSum === 0) throw new Error('dead chain (zero OI everywhere)');
  const h = etNow.getHours(), m = etNow.getMinutes();
  return {
    date: isoDateET(etNow), book: Math.round(tot / 1e9 * 100) / 100,
    spx: Math.round(S * 100) / 100, vix: vixClose,
    at: `${String(h).padStart(2, '0')}:${String(m).padStart(2, '0')}`,
    nExp, span: [Math.round((kLo / S - 1) * 1000) / 10, Math.round((kHi / S - 1) * 1000) / 10],
  };
}

// Compute + persist tonight's bank-scale evening book if missing (claim-gated,
// idempotent; returns the freshest snapshot or null). Shared by GET
// /evening-book (lazy) and the after-close cron backstop — AUDIT FIX
// 2026-07-31: a night with zero page hits used to leave a permanent hole in
// the gap-ladder series and starve the next morning's Gap card + Wall gauge.
async function ensureEveningBook(env, etNowEb) {
  const isoEb = isoDateET(etNowEb);
  let latest = null;
  try { latest = JSON.parse((await env.SIGNAL_KV.get('evening_book_latest')) || 'null'); } catch (_) {}
  const isTradingDayEb = etNowEb.getDay() >= 1 && etNowEb.getDay() <= 5 && !isHol(etNowEb);
  // 16:25+: the official close print has settled by then (lastPrice static).
  const afterCloseEb = etNowEb.getHours() > 16 || (etNowEb.getHours() === 16 && etNowEb.getMinutes() >= 25);
  if (!(isTradingDayEb && afterCloseEb && (!latest || latest.date < isoEb))) return latest;
  if (!(await claimSendSlot(env, `evening_book_${isoEb}`))) return latest;
  try {
    const tokEb = await getAccessToken(env);
    const snap = await computeEveningBook(env, tokEb, etNowEb);
    await env.SIGNAL_KV.put('evening_book_latest', JSON.stringify(snap));
    await env.SIGNAL_KV.put(`evening_book_${isoEb}`, 'sent', { expirationTtl: 3 * 86400 });
    try {
      const log = JSON.parse((await env.SIGNAL_KV.get('evening_book_log')) || '[]');
      if (!log.some(r => r.d === snap.date)) {
        log.push({ d: snap.date, book: snap.book, spx: snap.spx, vix: snap.vix });
        await env.SIGNAL_KV.put('evening_book_log', JSON.stringify(log));
      }
    } catch (e) { console.warn('[evening-book] log append failed:', e.message); }
    return snap;
  } catch (e) {
    console.warn('[evening-book] compute failed:', e.message);
    // release the claim so the next tick/request retries immediately
    try { const c = await env.SIGNAL_KV.get(`evening_book_${isoEb}`); if (c && c.startsWith('claim:')) await env.SIGNAL_KV.delete(`evening_book_${isoEb}`); } catch (_) {}
    return latest;
  }
}

// ════════════════════════════════════════════════════════════════════
// GEXMAGNET nightly single-stock chain collector (2026-07-02)
// ────────────────────────────────────────────────────────────────────
// Phase 0 of ~/projects/gexmagnet found NO single-stock chain snapshots
// anywhere (all chain artifacts are SPX-only), which blocks the GEXMAGNET
// Phase 1 backtest. This collects them forward: ONE post-close snapshot
// per universe ticker per day. One is enough — Schwab OI updates only
// overnight, so consecutive post-close snapshots already give clean
// day-over-day OI deltas (the Phase 2 flow-join input).
//
// Runs in the 16:41–17:15 ET window on existing cron ticks, chunked
// GEXM_CHUNK tickers/tick (lessons P17 — never blow one tick's subrequest
// budget). Progress accumulates in KV gexm_chains_part_<date>; once every
// ticker has data or failed twice, the day commits to GitHub
// data/gexm_chains/<date>.json — one file per day, NOT a monthly upsert:
// ~0.5–1 MB/day is too big for the contents-API read path to fold.
// GET /gexm-status reports; GET /gexm-trigger (auth) forces a chunk now.
// Loader on the Mac: ~/projects/gexmagnet/data/load_chains.py.

const GEXM_UNIVERSE = [
  // flow-ready in the Phase 0 audit (CheddarFlow history Jan–Jun 2026)
  'HIMS', 'ASTS', 'AG', 'MARA', 'QBTS', 'RGTI', 'JD', 'CLSK', 'VG', 'ACHR', 'ONON',
  // liquid options large-caps
  'AAPL', 'MSFT', 'NVDA', 'TSLA', 'AMZN', 'GOOGL', 'META', 'AMD', 'AVGO', 'NFLX',
  'MU', 'ORCL', 'PLTR', 'COIN', 'MSTR', 'CRWD', 'PANW', 'UBER', 'BABA', 'SMCI',
  'HOOD', 'SOFI', 'RIOT', 'INTC', 'QCOM', 'LULU', 'SNOW', 'SHOP', 'DELL', 'ARM',
];
const GEXM_CHUNK = 14;          // tickers per tick — subrequest-budget guard
const GEXM_MAX_DTE = 45;        // 1–7d swing horizon; longer expiries are dead weight
const GEXM_STRIKE_BAND = 0.30;  // keep strikes within ±30% of spot

// Schwab chain JSON → compact per-ticker snapshot:
// { spot, exp: { "YYYY-MM-DD": { c: [[strike,bid,ask,last,vol,oi,iv,delta,gamma],…], p: […] } } }
// Contracts with zero OI AND zero volume are dropped (loader treats missing
// as OI=0, so day-over-day deltas still see new OI appearing).
function _gexmCompact(chain) {
  const spot = chain.underlyingPrice || chain.underlying?.last || chain.underlying?.mark;
  if (!(spot > 0)) throw new Error('no spot');
  const out = { spot: +spot.toFixed(2), exp: {} };
  const lo = spot * (1 - GEXM_STRIKE_BAND), hi = spot * (1 + GEXM_STRIKE_BAND);
  for (const [mapName, side] of [['callExpDateMap', 'c'], ['putExpDateMap', 'p']]) {
    for (const [expKey, strikes] of Object.entries(chain[mapName] || {})) {
      const iso = expKey.split(':')[0];
      for (const arr of Object.values(strikes)) {
        const c = Array.isArray(arr) ? arr[0] : arr;
        if (!c) continue;
        const k = c.strikePrice;
        if (!(k >= lo && k <= hi)) continue;
        const oi = c.openInterest || 0, vol = c.totalVolume || 0;
        if (!oi && !vol) continue;
        const iv = (c.volatility > 0 && c.volatility < 500) ? +c.volatility.toFixed(2) : null;
        if (!out.exp[iso]) out.exp[iso] = { c: [], p: [] };
        out.exp[iso][side].push([
          k, +(c.bid ?? 0).toFixed(2), +(c.ask ?? 0).toFixed(2), +(c.last ?? 0).toFixed(2),
          vol, oi, iv,
          c.delta != null ? +(+c.delta).toFixed(3) : null,
          c.gamma != null ? +(+c.gamma).toFixed(5) : null,
        ]);
      }
    }
  }
  if (!Object.keys(out.exp).length) throw new Error('empty chain');
  return out;
}

async function gexmCollectChains(env, etNow, opts = {}) {
  const todayISO = isoDateET(etNow);
  const h = etNow.getHours(), m = etNow.getMinutes();
  const inWindow = (h === 16 && m >= 41) || (h === 17 && m <= 15);
  if (!opts.force && !inWindow) return null;
  const doneKey = `gexm_chains_done_${todayISO}`;
  if (await env.SIGNAL_KV.get(doneKey)) return { gexm: 'done' };

  let token;
  try { token = opts.token || await getAccessToken(env); }
  catch (e) { return { gexm: 'no-token', error: e.message }; }

  const partKey = `gexm_chains_part_${todayISO}`;
  const part = JSON.parse((await env.SIGNAL_KV.get(partKey)) || '{"tickers":{},"errs":{}}');
  const pending = GEXM_UNIVERSE.filter(t => !part.tickers[t] && (part.errs[t] || 0) < 2);
  const batch = pending.slice(0, opts.chunk || GEXM_CHUNK);

  const toD = new Date(etNow); toD.setDate(toD.getDate() + GEXM_MAX_DTE);
  const hhmm = `${String(h).padStart(2, '0')}:${String(m).padStart(2, '0')}`;
  for (const sym of batch) {
    try {
      const chain = await fetchSchwabJSON(
        `https://api.schwabapi.com/marketdata/v1/chains?symbol=${sym}&strikeCount=50&fromDate=${todayISO}&toDate=${isoDateET(toD)}&includeUnderlyingQuote=true&strategy=SINGLE&contractType=ALL`,
        token, env);
      part.tickers[sym] = { at: hhmm, ..._gexmCompact(chain) };
    } catch (e) {
      part.errs[sym] = (part.errs[sym] || 0) + 1;
      console.warn(`[gexm] ${sym} fetch failed (attempt ${part.errs[sym]}):`, e.message);
    }
  }

  const remaining = GEXM_UNIVERSE.filter(t => !part.tickers[t] && (part.errs[t] || 0) < 2);
  if (remaining.length) {
    await env.SIGNAL_KV.put(partKey, JSON.stringify(part), { expirationTtl: 2 * 86400 });
    return { gexm: 'partial', got: Object.keys(part.tickers).length, remaining: remaining.length };
  }

  // Every ticker resolved (data or 2 failed attempts) → persist the day.
  const failed = GEXM_UNIVERSE.filter(t => !part.tickers[t]);
  const n = Object.keys(part.tickers).length;
  await githubUpsertResearchFile(env, `data/gexm_chains/${todayISO}.json`,
    () => ({ date: todayISO, universe: GEXM_UNIVERSE.length, failed, tickers: part.tickers }),
    `auto: gexm chains ${todayISO} (${n}/${GEXM_UNIVERSE.length})`);
  await env.SIGNAL_KV.put(doneKey, 'done', { expirationTtl: 5 * 86400 });
  await env.SIGNAL_KV.put('gexm_chains_last',
    JSON.stringify({ date: todayISO, n, failed, ts: Date.now() }));
  await env.SIGNAL_KV.delete(partKey);
  console.log(`[gexm] committed ${todayISO}: ${n}/${GEXM_UNIVERSE.length}${failed.length ? ' failed=' + failed.join(',') : ''}`);
  return { gexm: 'committed', n, failed };
}


// ════════════════════════════════════════════════════════════════════
// EARNINGS PLAY v2.4 SCANNER (2026-07-06) — fully Schwab-automatic
// ────────────────────────────────────────────────────────────────────
// Gates (v2.4, backtest-locked): 1) opened-premium PW ratio in [2.33, 5]
// over 14d lookback (last 2 trading days ×2 weight); 2) 2-week run-up < 0
// AND ≥5% below rolling ATH; 3) ticker base rate ≥ 0.5 (needs ≥4 prior
// reactions); 4) prior-day VIX < 1.2 × its own 100-day mean.
// Sizing rule (owner): 100% of account per signal night, equal split.
// Mode: KV earnings_mode = 'paper' (default) | 'live' — message tag only,
// this scanner NEVER places orders.
//
// Delivery: owner DM (discord_config.channelId) + KV earnings_webhook_url.
// NEVER the Σ3 subscriber fan-out.
//
// Data: seed file data/earnings_scanner_seed.json (base rates from the 8yr
// backtest; flow windows through 2026-07-02; 6-week forward calendar as
// Nasdaq fallback). Worker self-collects flow nightly for watchlist names
// (earnflow_<sym> KV, rolling). Live intraday Gate-1 is volume-premium
// (no next-day OI confirmation available intraday) — divergence from the
// backtest's OI-confirmed proxy is exactly what paper week measures.
// KV keys: earn_board_<date>, earn_done_<date>_<step>, earnflow_<sym>,
//          earn_log (rolling paper log), earnscan_done_<date> (watchdog).

const EARN_DTE_MAX = 14, EARN_PW_LO = 7/3, EARN_PW_HI = 5.0;
const EARN_RUNUP_MAX = 0.0, EARN_ATH_MAX = -0.05, EARN_BASE_MIN = 0.5;
const EARN_VIXR_MAX = 1.2, EARN_LOOKBACK_D = 14, EARN_W2_BDAYS = 2;
const EARN_M_LO = 2.5, EARN_M_HI = 6.0, EARN_WEEKLY_MIN_DAYS = 5;  // hybrid: monthly band + weekly-presence threshold

async function earnSeed(env) {
  const cached = await env.SIGNAL_KV.get('earn_seed_cache');
  if (cached) return JSON.parse(cached);
  const r = await fetch('https://raw.githubusercontent.com/rava8989/brave/main/data/earnings_scanner_seed.json',
    { headers: { 'User-Agent': 'schwab-proxy-worker/1.0' } });
  if (!r.ok) throw new Error(`seed fetch ${r.status}`);
  const s = await r.json();
  await env.SIGNAL_KV.put('earn_seed_cache', JSON.stringify(s), { expirationTtl: 6 * 3600 });
  return s;
}

async function earnCalendar(env, fromISO, toISO) {
  // Try Nasdaq live; fall back to the seed's baked 6-week calendar.
  const ck = `earn_cal_${fromISO}_${toISO}`;
  const hit = await env.SIGNAL_KV.get(ck);
  if (hit) return JSON.parse(hit);
  const out = [];
  let src = 'nasdaq';
  try {
    for (let d = new Date(fromISO); d <= new Date(toISO); d.setDate(d.getDate() + 1)) {
      const iso = d.toISOString().slice(0, 10);
      const dow = new Date(iso + 'T12:00:00Z').getUTCDay();
      if (dow === 0 || dow === 6) continue;
      const r = await fetch(`https://api.nasdaq.com/api/calendar/earnings?date=${iso}`,
        { headers: { 'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)', 'Accept': 'application/json' } });
      if (!r.ok) throw new Error(`nasdaq ${r.status}`);
      const j = await r.json();
      for (const row of ((j.data || {}).rows || [])) {
        out.push({ ticker: (row.symbol || '').trim().toUpperCase(), report_date: iso,
                   when: row.time === 'time-after-hours' ? 'AMC'
                       : row.time === 'time-pre-market' ? 'BMO' : 'UNKNOWN' });
      }
    }
  } catch (e) {
    console.warn('[earn] nasdaq calendar failed, using seed fallback:', e.message);
    src = 'seed';
    const seed = await earnSeed(env);
    for (const ev of (seed.forward_calendar || [])) {
      const rd = String(ev.report_date).slice(0, 10);
      if (rd >= fromISO && rd <= toISO) out.push({ ticker: ev.ticker, report_date: rd, when: ev.when });
    }
  }
  const res = { src, events: out };
  await env.SIGNAL_KV.put(ck, JSON.stringify(res), { expirationTtl: 20 * 3600 });
  return res;
}

function earnPipeStatus(base_rate, pw) {
  if (base_rate == null) return 'new (no track record)';
  if (base_rate < 0.5) return 'unlikely — fades its beats';
  if (pw == null) return 'watching — flow building';
  if (pw > 6) return 'crowded — likely PASS';
  if (pw >= 2.5) return '\uD83D\uDFE2 live candidate — bullish flow + pays on beats';
  return 'watching — flow light';
}

async function earnPipeline(env) {
  // Forward pipeline from the LIVE Nasdaq calendar (rolling 15 days) — always
  // current, auto-adds newly-announced earnings, never runs dry. In-universe
  // names only; flow-so-far read from the KV windows the worker already keeps.
  const today = isoDateET(toET());
  const to = isoDateET(new Date(Date.now() + 15 * 86400_000));
  const cal = await earnCalendar(env, today, to);
  const seed = await earnSeed(env);
  const base = seed.base_rates || {};
  const uni = new Set(Object.keys(base));
  const evs = cal.events
    // Keep future dates; keep TODAY only if the report is still ahead (AMC /
    // unknown = tonight). A BMO name dated today already reported this
    // morning — its play is over (user 2026-07-10: DAL lingered all day).
    .filter(e => uni.has(e.ticker) &&
      (e.report_date > today || (e.report_date === today && e.when !== 'BMO')))
    .sort((a, b) => a.report_date < b.report_date ? -1 : a.report_date > b.report_date ? 1 : 0);
  const seen = new Set(); const rows = [];
  for (const e of evs) {
    if (seen.has(e.ticker)) continue; seen.add(e.ticker);
    const br = base[e.ticker] || {};
    let pw = null, ndays = 0;
    try {
      const fw = await earnFlowWindow(env, e.ticker, today, null);
      if (fw.nDays >= EARN_WEEKLY_MIN_DAYS) {
        pw = (fw.pw != null && isFinite(fw.pw)) ? +fw.pw.toFixed(2) : null; ndays = fw.nDays;
      } else {
        const fm = await earnMonthWindow(env, e.ticker, today, null);
        pw = (fm.pw != null && isFinite(fm.pw)) ? +fm.pw.toFixed(2) : null; ndays = fm.nDays;
      }
    } catch (_) {}
    const brate = br.base_rate ?? null;
    rows.push({ date: e.report_date, ticker: e.ticker, when: e.when,
      base_rate: brate, base_n: br.n || 0, pw_so_far: pw, flow_days: ndays,
      status: earnPipeStatus(brate, pw) });
  }
  return { built: today, calSrc: cal.src, rows };
}

async function earnRefreshPipeline(env) {
  try {
    const data = await earnPipeline(env);
    // 90h TTL (was 30h): the cache must survive Fri-evening → Mon-9:31 with no
    // weekday refresh job in between — the 30h TTL died every Saturday night,
    // and the Monday cold rebuild (~15 Nasdaq fetches) blew the page's 8s
    // timeout → stale static fallback ("as of 2026-07-08", owner 2026-08-03).
    await env.SIGNAL_KV.put('earn_pipeline_cache', JSON.stringify(data), { expirationTtl: 90 * 3600 });
    return data;
  } catch (e) { console.warn('[earn] pipeline refresh:', e.message); return null; }
}

// ── Weekly earnings-calendar audit (owner 2026-08-03) ──
// A wrong report date/time is the one error the scanner can't survive — it
// would trade the wrong night. Every weekend: force a FRESH 15-day calendar
// pull, diff it against the previous pipeline build (moves / drops / BMO-AMC
// flips), independently cross-check the names that matter (next 7 days,
// base_rate ≥ 0.5 — the potential LONG candidates) against Nasdaq's separate
// per-ticker earnings-date endpoint, rebuild the cache (weekend visitors get
// a Saturday build), and refresh the GitHub fallback mirror. DM only when
// something changed or mismatched — silent when clean.
async function earnCalendarAudit(env) {
  const etA = toET(new Date());
  const todayA = isoDateET(etA);
  const toA = isoDateET(new Date(Date.now() + 15 * 86400_000));
  await env.SIGNAL_KV.delete(`earn_cal_${todayA}_${toA}`);   // bypass the 20h calendar cache
  const oldRaw = await env.SIGNAL_KV.get('earn_pipeline_cache');
  const oldP = oldRaw ? JSON.parse(oldRaw) : null;
  const fresh = await earnRefreshPipeline(env);
  if (!fresh || !Array.isArray(fresh.rows)) return { audit: 'no-fresh' };
  const issues = [];
  if (oldP && Array.isArray(oldP.rows)) {
    const nm = new Map(fresh.rows.map(r => [r.ticker, r]));
    for (const o of oldP.rows) {
      if (o.date < todayA || o.date > toA) continue;
      const n = nm.get(o.ticker);
      if (!n) issues.push(`${o.ticker}: dropped off the calendar (was ${o.date} ${o.when})`);
      else if (n.date !== o.date) issues.push(`${o.ticker}: MOVED ${o.date} → ${n.date}`);
      else if (n.when !== o.when && n.when !== 'UNKNOWN' && o.when !== 'UNKNOWN') issues.push(`${o.ticker}: time ${o.when} → ${n.when}`);
    }
  }
  const wkA = new Date(etA); wkA.setDate(wkA.getDate() + 7);
  const soonISO = isoDateET(wkA);
  const cands = fresh.rows.filter(r => r.date <= soonISO && (r.base_rate ?? 0) >= 0.5).slice(0, 15);
  const MONA = { Jan: '01', Feb: '02', Mar: '03', Apr: '04', May: '05', Jun: '06', Jul: '07', Aug: '08', Sep: '09', Oct: '10', Nov: '11', Dec: '12' };
  let checked = 0;
  for (const c of cands) {
    try {
      const r = await fetch(`https://api.nasdaq.com/api/analyst/${encodeURIComponent(c.ticker)}/earnings-date`,
        { headers: { 'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)', 'Accept': 'application/json' } });
      if (!r.ok) continue;
      const j = await r.json();
      const txt = String((j && j.data && (j.data.announcement || j.data.reportText)) || '');
      const m = txt.match(/([A-Z][a-z]{2})[a-z]*\s+(\d{1,2}),\s+(\d{4})/);
      if (!m || !MONA[m[1]]) continue;                      // unparseable → skip, never false-alarm
      checked++;
      const tDate = `${m[3]}-${MONA[m[1]]}-${String(+m[2]).padStart(2, '0')}`;
      if (tDate !== c.date) issues.push(`${c.ticker}: calendar ${c.date} vs ticker page ${tDate} ⚠ VERIFY`);
    } catch (_) { /* per-ticker flake — skip, never alarm on the checker's own failure */ }
  }
  try {
    await githubUpsertResearchFile(env, 'data/earnings_pipeline.json',
      () => fresh, `auto: weekend calendar audit ${todayA} (${fresh.rows.length} rows)`);
  } catch (e) { console.warn('[earn-cal-audit] mirror:', e.message); }
  if (issues.length) {
    try {
      const dcRaw = await env.SIGNAL_KV.get('discord_config');
      const dc = dcRaw ? JSON.parse(dcRaw) : null;
      if (dc && dc.channelId) await sendDiscordDM(env, dc.channelId,
        `📅 **Earnings calendar audit** — ${issues.length} change(s) in the next 2 weeks:\n` +
        issues.slice(0, 20).map(x => `• ${x}`).join('\n'), dc.proxyUrl);
    } catch (_) {}
  }
  return { audit: 'done', issues: issues.length, crossChecked: checked, rows: fresh.rows.length };
}

async function earnVixOK(env, token) {
  // prior-day VIX close vs mean of last 100 closes (incl. prior day)
  const end = Date.now(), start = end - 220 * 86400_000;
  const d = await fetchSchwabJSON(
    `https://api.schwabapi.com/marketdata/v1/pricehistory?symbol=%24VIX&periodType=year&frequencyType=daily&frequency=1&startDate=${start}&endDate=${end}`,
    token, env);
  const closes = (d.candles || []).map(c => c.close).filter(x => x > 0);
  if (closes.length < 101) throw new Error('VIX history too short');
  const prior = closes[closes.length - 1];
  const last100 = closes.slice(-100);
  const mean = last100.reduce((a, b) => a + b, 0) / last100.length;
  return { ok: prior < EARN_VIXR_MAX * mean, prior: +prior.toFixed(2), ratio: +(prior / mean).toFixed(3) };
}

async function earnPriceStats(env, token, sym) {
  const end = Date.now(), start = end - 5.2 * 365 * 86400_000;
  const d = await fetchSchwabJSON(
    `https://api.schwabapi.com/marketdata/v1/pricehistory?symbol=${encodeURIComponent(sym)}&periodType=year&frequencyType=daily&frequency=1&startDate=${start}&endDate=${end}`,
    token, env);
  const cs = (d.candles || []).filter(c => c.close > 0);
  if (cs.length < 15) return null;
  const closes = cs.map(c => c.close);
  const last = closes[closes.length - 1];
  const runup = last / closes[closes.length - 11] - 1;
  const ath = Math.max(...closes);
  return { last: +last.toFixed(2), runup_2w: +runup.toFixed(4), ath_dist: +(last / ath - 1).toFixed(4) };
}

function earnFlowFromDays(days, todayISO) {
  // days: {iso: {c, p, di}} — 14 calendar-day window ending today, last 2
  // bdays double-weighted. Returns {pw, deep, callSum, putSum, nDays}.
  const cutoff = new Date(new Date(todayISO + 'T12:00:00Z').getTime() - EARN_LOOKBACK_D * 86400_000)
    .toISOString().slice(0, 10);
  const isos = Object.keys(days).filter(d => d > cutoff && d <= todayISO).sort();
  if (!isos.length) return { pw: null, deep: 0, callSum: 0, putSum: 0, nDays: 0 };
  const recent = new Set(isos.slice(-EARN_W2_BDAYS));
  let c = 0, p = 0, deep = 0;
  for (const iso of isos) {
    const w = recent.has(iso) ? 2 : 1;
    c += (days[iso].c || 0) * w;
    p += (days[iso].p || 0) * w;
    deep += (days[iso].di || 0) * w;
  }
  return { pw: p > 0 ? c / p : (c > 0 ? Infinity : null), deep, callSum: c, putSum: p, nDays: isos.length };
}

async function earnFlowWindow(env, sym, todayISO, intradayDays) {
  const seed = await earnSeed(env);
  const days = Object.assign({}, (seed.flow_seed || {})[sym] || {});
  const kv = await env.SIGNAL_KV.get(`earnflow_${sym}`);
  if (kv) Object.assign(days, JSON.parse(kv));
  if (intradayDays) Object.assign(days, intradayDays);
  return earnFlowFromDays(days, todayISO);
}

async function earnMonthWindow(env, sym, todayISO, intradayDays) {
  // Same as earnFlowWindow but for the earnings-monthly flow store.
  const days = {};
  const seed = await earnSeed(env);
  Object.assign(days, ((seed.flow_seed_month || {})[sym]) || {});
  const kv = await env.SIGNAL_KV.get(`earnflow_m_${sym}`);
  if (kv) Object.assign(days, JSON.parse(kv));
  if (intradayDays) Object.assign(days, intradayDays);
  return earnFlowFromDays(days, todayISO);
}

async function earnChainDayPremium(env, token, sym, todayISO, reportISO) {
  // Today's volume-premium by side, computed for BOTH the ≤14-DTE band
  // (weekly names) AND the earnings-capturing monthly (first expiry on/after
  // the report date, within 45d — for monthly-only names). Fetches a wide
  // enough date range to cover both.
  const toD = new Date(new Date(todayISO).getTime() + 55 * 86400_000).toISOString().slice(0, 10);
  const chain = await fetchSchwabJSON(
    `https://api.schwabapi.com/marketdata/v1/chains?symbol=${encodeURIComponent(sym)}&strikeCount=40&fromDate=${todayISO}&toDate=${toD}&includeUnderlyingQuote=true&strategy=SINGLE&contractType=ALL`,
    token, env);
  const spot = chain.underlyingPrice || (chain.underlying || {}).last;
  // pick the earnings-capturing monthly expiry key
  let mExpKey = null;
  if (reportISO) {
    const rd = new Date(reportISO), cap = new Date(new Date(reportISO).getTime() + 45 * 86400_000);
    const keys = Object.keys(chain.callExpDateMap || {}).map(k => k.split(':')[0]).sort();
    mExpKey = keys.find(k => new Date(k) >= rd && new Date(k) <= cap) || null;
  }
  let c = 0, p = 0, di = 0, mc = 0, mp = 0, mdi = 0;
  for (const [mapName, side] of [['callExpDateMap', 'c'], ['putExpDateMap', 'p']]) {
    for (const [expKey, strikes] of Object.entries(chain[mapName] || {})) {
      const iso = expKey.split(':')[0];
      const dte = Math.round((new Date(iso) - new Date(todayISO)) / 86400_000);
      const isMonth = mExpKey && iso === mExpKey;
      for (const arr of Object.values(strikes)) {
        const ct = Array.isArray(arr) ? arr[0] : arr;
        if (!ct || !(ct.totalVolume > 0)) continue;
        const mid = (ct.bid > 0 && ct.ask > 0) ? (ct.bid + ct.ask) / 2 : (ct.last > 0 ? ct.last : 0);
        const prem = ct.totalVolume * mid * 100;
        const deep = spot && ct.strikePrice < 0.92 * spot;
        if (dte >= 0 && dte <= EARN_DTE_MAX) {
          if (side === 'c') { c += prem; if (deep) di += prem; } else p += prem;
        }
        if (isMonth) {
          if (side === 'c') { mc += prem; if (deep) mdi += prem; } else mp += prem;
        }
      }
    }
  }
  return { c: Math.round(c), p: Math.round(p), di: Math.round(di),
           mc: Math.round(mc), mp: Math.round(mp), mdi: Math.round(mdi), spot };
}

async function earnParkingSleeve(env, token) {
  // Four-regime parking: SPY when above its 200-day; else CASH when prior-day
  // VIX > 25; else GLD. Returns {sleeve, spy, sma, vix}.
  const end=Date.now(), start=end-320*86400_000;
  const px=await fetchSchwabJSON(`https://api.schwabapi.com/marketdata/v1/pricehistory?symbol=SPY&periodType=year&frequencyType=daily&frequency=1&startDate=${start}&endDate=${end}`,token,env);
  const c=(px.candles||[]).map(x=>x.close).filter(x=>x>0);
  const spot=c[c.length-1], sma=c.slice(-200).reduce((a,b)=>a+b,0)/Math.min(200,c.length);
  let vixPrior=null;
  try{ const v=await fetchSchwabJSON(`https://api.schwabapi.com/marketdata/v1/pricehistory?symbol=%24VIX&periodType=year&frequencyType=daily&frequency=1&startDate=${end-20*86400_000}&endDate=${end}`,token,env);
       const vc=(v.candles||[]).map(x=>x.close).filter(x=>x>0); vixPrior=vc[vc.length-2]??vc[vc.length-1]; }catch(_){}
  let sleeve='SPY';
  if(spot<=sma) sleeve=(vixPrior!=null&&vixPrior>25)?'CASH':'GLD';
  return {sleeve, spot:+spot.toFixed(2), sma:+sma.toFixed(2), vix:vixPrior?+vixPrior.toFixed(1):null};
}

// Live last prices for a few equity symbols — ONE Schwab quotes call.
async function earnQuotesLast(env, token, syms) {
  const d = await fetchSchwabJSON(`https://api.schwabapi.com/marketdata/v1/quotes?symbols=${syms.join(',')}&fields=quote`, token, env);
  const out = {};
  for (const s of syms) {
    const q = d?.[s]?.quote;
    const px = q?.lastPrice ?? q?.closePrice ?? null;
    if (px > 0) out[s] = +px;
  }
  return out;
}

// "Since we entered" tracker for the parking sleeve (owner 2026-07-14): stamp
// entry date + entry prices (SPY/SSO/QLD/GLD) every time money MOVES into the
// sleeve — a regime flip (SPY↔GLD↔CASH, morning job) OR re-entry after an
// overnight earnings basket (exit job, force=true). Owner: "% since the last
// move" — the true holding period of the CURRENT position, so a basket night
// DOES restart the clock. earnCurrentStatus self-heals on label mismatch.
async function earnSleeveRunStamp(env, park, token, force = false) {
  if (!park || !park.sleeve) return null;
  const raw = await env.SIGNAL_KV.get('earn_sleeve_run');
  const run = raw ? JSON.parse(raw) : null;
  if (!force && run && run.sleeve === park.sleeve) return run;
  let entry = {};
  try { entry = await earnQuotesLast(env, token, ['SPY', 'SSO', 'QLD', 'GLD']); } catch (_) {}
  const fresh = { sleeve: park.sleeve, since: isoDateET(toET()), entry };
  await env.SIGNAL_KV.put('earn_sleeve_run', JSON.stringify(fresh));
  return fresh;
}

// Current position of the whole earnings package, for the "where we stand now"
// status card. earn_open_<date> exists ONLY between the 3:45 entry and the
// 9:32 next-morning exit (earnExitJob deletes it), so its presence = we're
// holding the basket. Otherwise we're parked in the sleeve — read from the most
// recent board's `park`, plus %-since-entry from earn_sleeve_run + live quotes.
async function earnCurrentStatus(env) {
  const etNow = toET();
  for (let back = 0; back <= 4; back++) {
    const d = new Date(etNow); d.setDate(d.getDate() - back);
    const raw = await env.SIGNAL_KV.get(`earn_open_${isoDateET(d)}`);
    if (raw) {
      try {
        const o = JSON.parse(raw);
        if (o.longs && o.longs.length) {
          return { status: 'EARNINGS', names: o.longs, weight: Math.round(100 / o.longs.length),
                   since: isoDateET(d), asOf: etNow.toISOString() };
        }
      } catch (_) {}
    }
  }
  let park = null, boardDate = null;
  for (let back = 0; back <= 7; back++) {
    const d = new Date(etNow); d.setDate(d.getDate() - back);
    const dISO = isoDateET(d);
    const raw = await env.SIGNAL_KV.get(`earn_board_${dISO}`);
    if (raw) { try { const b = JSON.parse(raw); if (b.park) { park = b.park; boardDate = dISO; break; } } catch (_) {} }
  }
  const sleeve = park ? park.sleeve : 'SPY';
  // %-change since sleeve entry (owner 2026-07-14). SPY sleeve also shows the
  // optional 2× alternatives (SSO/QLD) from the SAME entry date; CASH has no %.
  let since = null, chg = null;
  try {
    const runRaw = await env.SIGNAL_KV.get('earn_sleeve_run');
    let run = runRaw ? JSON.parse(runRaw) : null;
    const token = await getAccessToken(env);
    if (!run || run.sleeve !== sleeve) run = await earnSleeveRunStamp(env, park || { sleeve }, token);
    if (run) {
      since = run.since;
      const want = sleeve === 'SPY' ? ['SPY', 'SSO', 'QLD'] : sleeve === 'GLD' ? ['GLD'] : [];
      if (want.length && run.entry) {
        const last = await earnQuotesLast(env, token, want);
        chg = {};
        for (const s of want) {
          if (last[s] > 0 && run.entry[s] > 0) chg[s] = +(((last[s] / run.entry[s]) - 1) * 100).toFixed(1);
        }
        if (!Object.keys(chg).length) chg = null;
      }
    }
  } catch (e) { console.warn('[earn-status-chg]', e.message); }
  return { status: sleeve, since, chg, spot: park?.spot ?? null, sma: park?.sma ?? null,
           vix: park?.vix ?? null, boardDate, asOf: etNow.toISOString() };
}

async function earnSend(env, msg) {
  const results = { dm: false, webhook: false };
  msg = msg + '\n\n_Not financial advice. For informational/educational purposes only. You are solely responsible for your own trades._';
  try {
    const dcRaw = await env.SIGNAL_KV.get('discord_config');
    if (dcRaw) {
      const dc = JSON.parse(dcRaw);
      if (dc.channelId) results.dm = (await sendDiscordDM(env, dc.channelId, msg, dc.proxyUrl)).ok;
    }
  } catch (e) { console.warn('[earn] DM failed:', e.message); }
  try {
    const wh = await env.SIGNAL_KV.get('earnings_webhook_url');
    if (wh) {
      const r = await fetch(wh, { method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ content: msg.slice(0, 1900) }) });
      results.webhook = r.status === 204 || r.ok;
    }
  } catch (e) { console.warn('[earn] webhook failed:', e.message); }
  return results;
}

async function earnBuildBoard(env, token, boardISO, opts = {}) {
  // Board = boardISO's AMC reporters + next-trading-day BMO reporters.
  // UNKNOWN report time = treated as AMC (enter boardISO close), flagged.
  const uniRaw = await earnSeed(env);
  const uniset = new Set(Object.keys(uniRaw.base_rates || {}));
  const nextD = nextTrade(new Date(boardISO + 'T12:00:00'));
  const nextISO = isoDateET(nextD);
  const cal = await earnCalendar(env, boardISO, nextISO);
  const cands = [];
  const seen = new Set();
  // Manual additions (owner override for calendar gaps): KV earn_extra_<date>
  // = JSON array of tickers, treated as AMC reporters on boardISO.
  try {
    const extraRaw = await env.SIGNAL_KV.get(`earn_extra_${boardISO}`);
    if (extraRaw) {
      for (const t of JSON.parse(extraRaw)) {
        const tk = String(t).toUpperCase();
        if (!seen.has(tk)) { seen.add(tk);
          cands.push({ ticker: tk, when: 'AMC', report_date: boardISO,
                       inUniverse: uniset.has(tk), manual: true }); }
      }
    }
  } catch (e) { console.warn('[earn] extra parse:', e.message); }
  for (const ev of cal.events) {
    if (seen.has(ev.ticker)) continue;
    // Universe-only board (owner 2026-07-29): outside-universe names are never
    // scored, never listed, never mentioned — they can't qualify, so they are
    // pure noise. This also retires the old 40-row cap that let a heavy AMC
    // night push tomorrow's BMO universe names off the board entirely
    // (2026-07-29: BUD/BMY/RACE cut while REIT filler held slots).
    if (!uniset.has(ev.ticker)) continue;
    const isTonight = ev.report_date === boardISO && ev.when !== 'BMO';
    const isTomorrowAM = ev.report_date === nextISO && ev.when === 'BMO';
    if (!isTonight && !isTomorrowAM) continue;
    seen.add(ev.ticker);
    cands.push({ ticker: ev.ticker, when: ev.when === 'UNKNOWN' ? 'AMC?' : ev.when,
                 report_date: ev.report_date, inUniverse: true });
  }
  const vix = await earnVixOK(env, token);
  let park=null; try{ park=await earnParkingSleeve(env, token); }catch(e){ console.warn('[earn] parking:',e.message); }
  const seed = uniRaw;
  const board = [];
  // All cands are in-universe now; 100 is a pure safety ceiling (subrequest
  // budget ~1000) that no real earnings night approaches — never a silent cut.
  for (const cd of cands.slice(0, 100)) {
    const row = { ticker: cd.ticker, when: cd.when, g1: null, g2: null, g3: null, g4: vix.ok,
                  pw_ratio: null, deep_itm_usd: 0, runup_2w: null, ath_dist: null,
                  base_rate: null, base_n: 0, verdict: 'PASS', notes: [] };
    if (!cd.inUniverse && !cd.manual) { row.notes.push('outside universe (no base-rate history)'); board.push(row); continue; }
    const br = (seed.base_rates || {})[cd.ticker];
    if (br) { row.base_rate = br.base_rate; row.base_n = br.n; row.g3 = br.base_rate >= EARN_BASE_MIN; }
    else { row.g3 = false; row.notes.push('history too thin for Gate 3 (<4 prior reports)'); }
    if (cd.manual) row.notes.push('manually added');
    try {
      const ps = await earnPriceStats(env, token, cd.ticker);
      if (ps) { row.runup_2w = ps.runup_2w; row.ath_dist = ps.ath_dist;
                row.g2 = ps.runup_2w < EARN_RUNUP_MAX && ps.ath_dist <= EARN_ATH_MAX; }
    } catch (e) { row.notes.push('price fetch failed'); }
    try {
      let intraday = null, intradayM = null;
      if (opts.withIntraday) {
        const today = await earnChainDayPremium(env, token, cd.ticker, boardISO, cd.report_date);
        intraday = { [boardISO]: { c: today.c, p: today.p, di: today.di } };
        intradayM = { [boardISO]: { c: today.mc, p: today.mp, di: today.mdi } };
      }
      const fw = await earnFlowWindow(env, cd.ticker, boardISO, intraday);
      const hasWeekly = fw.nDays >= EARN_WEEKLY_MIN_DAYS;
      if (hasWeekly) {
        // weekly name → 14-DTE flow, band 2.33–5
        if (fw.pw != null && isFinite(fw.pw)) row.pw_ratio = +fw.pw.toFixed(2);
        row.deep_itm_usd = Math.round(fw.deep);
        row.g1 = fw.pw != null && fw.pw >= EARN_PW_LO && fw.pw <= EARN_PW_HI;
        if (fw.pw != null && fw.pw > EARN_PW_HI) row.verdict = 'CROWDED';
        row.flowSrc = '14d';
      } else {
        // monthly-only name → earnings-monthly flow, band 2.5–6
        const fwm = await earnMonthWindow(env, cd.ticker, boardISO, intradayM);
        if (fwm.pw != null && isFinite(fwm.pw)) row.pw_ratio = +fwm.pw.toFixed(2);
        row.deep_itm_usd = Math.round(fwm.deep);
        row.g1 = fwm.pw != null && fwm.pw >= EARN_M_LO && fwm.pw <= EARN_M_HI;
        if (fwm.pw != null && fwm.pw > EARN_M_HI) row.verdict = 'CROWDED';
        row.flowSrc = 'monthly';
        if (fwm.nDays < 3) row.notes.push(`thin monthly window (${fwm.nDays}d)`);
      }
    } catch (e) { row.notes.push('flow fetch failed'); }
    if (row.g1 && row.g2 && row.g3 && row.g4) row.verdict = 'LONG';
    board.push(row);
  }
  const longs = board.filter(b => b.verdict === 'LONG');
  return { date: boardISO, next: nextISO, calSrc: cal.src, vix, park, board, longs };
}

function earnBoardMsg(b, mode, stage) {
  const head = stage === 'final' ? `🌙 **[EARNINGS] FINAL BOARD — ${b.date}**`
             : `🌙 **[EARNINGS] morning — ${b.date}**`;
  // Leverage-alternative note (informational, user 2026-07-12): SPY parking can
  // optionally be run 2× via a leveraged ETF SHARE (not options — those bleed on
  // in-and-out). Only shown on SPY-sleeve days; SPY stays the default.
  const LEV_ALT = '\n-# Optional leverage: SSO ≈2× S&P (more return, deeper drawdown) · QLD ≈2× Nasdaq (most, wildest) — see the site. SPY is the default.';
  const parkLine = b.park ? `\n**Parking sleeve: ${b.park.sleeve}** `+(b.park.sleeve==='SPY'?'(market above its 200-day — hold SPY)'+LEV_ALT:b.park.sleeve==='GLD'?'(market below 200-day, calm — hold GLD)':'(market below 200-day + VIX>25 — sit in cash)') : '';
  // Final-stage sleeve line (the FINAL is the only Discord message since
  // 2026-07-13, so quiet-day early returns must carry the sleeve too).
  const finalPark = b.park ? `\n_Parking sleeve today: ${b.park.sleeve}${b.park.sleeve==='CASH'?' (market below 200-day + VIX>25)':b.park.sleeve==='GLD'?' (market below 200-day, calm)':' (market above 200-day)'}._` + (b.park.sleeve==='SPY' ? LEV_ALT : '') : '';
  if (!b.board.length) return `${head}${stage==='morning'?parkLine:''}\nNo trades tonight.${stage==='final'?finalPark:''}`;
  // Outside-universe names stay in the board JSON (KV earn_board_<date>) for
  // audits but are NOT listed in the message — they can never be scored, so
  // they never change the decision (user 2026-07-10: pure noise).
  const scored = b.board.filter(r => !r.notes.some(n => n.startsWith('outside universe')));
  if (!scored.length) return `${head}${stage==='morning'?parkLine:''}\nNo trades tonight.${stage==='final'?finalPark:''}`;
  const lines = scored.map(r => {
    const g = x => x === null ? '·' : x ? '✓' : '✗';
    return `${r.verdict === 'LONG' ? '🟢' : r.verdict === 'CROWDED' ? '🔴' : '⚪'} ` +
      `**${r.ticker}** (${r.when}) PW ${r.pw_ratio ?? '—'} | run ${r.runup_2w != null ? (r.runup_2w * 100).toFixed(1) + '%' : '—'} | ` +
      `ATH ${r.ath_dist != null ? (r.ath_dist * 100).toFixed(0) + '%' : '—'} | base ${r.base_rate ?? '—'} | ` +
      `G:${g(r.g1)}${g(r.g2)}${g(r.g3)}${g(r.g4)} → **${r.verdict}**` +
      (r.notes.length ? ` _(${r.notes.join('; ')})_` : '');
  });
  let tail = `\nVIX check: ${b.vix.ratio}× its 100d mean (${b.vix.ok ? 'calm ✓' : '⛔ SPIKING — all nights skipped'})` +
             ` · calendar: ${b.calSrc}`;
  if (stage === 'final' && b.longs.length) {
    const w = (100 / b.longs.length).toFixed(1);
    tail += `\n**ACTION: buy at 3:45–3:55 close — ` +
            b.longs.map(l => `${l.ticker} ${w}%`).join(' · ') + `** · sell ALL at tomorrow's open`;
  }
  if (stage === 'final' && !b.longs.length) tail += `\nNo earnings tonight. Stay parked in your sleeve (below).`;
  // Morning send is dead (2026-07-13) — the FINAL is the only message, so the
  // SPY-sleeve leverage note lives here now (it used to ride the morning parkLine).
  if (stage === 'final') tail += finalPark;
  return [head + (stage==='morning'?parkLine:''), ...lines, tail].join('\n');
}

async function earnMorningJob(env, etNow, token) {
  const iso = isoDateET(etNow);
  const key = `earn_done_${iso}_morning`;
  if (await env.SIGNAL_KV.get(key)) return;
  const h = etNow.getHours(), m = etNow.getMinutes();
  if (!(h === 9 && m >= 10 && m <= 25)) return;
  await env.SIGNAL_KV.put(key, 'running', { expirationTtl: 86400 });
  try {
    const b = await earnBuildBoard(env, token, iso, { withIntraday: false });
    await env.SIGNAL_KV.put(`earn_board_${iso}`, JSON.stringify(b), { expirationTtl: 3 * 86400 });
    // NO morning Discord send (user 2026-07-13: a 9am read that can flip by
    // 3:30 is noise — "why I need a morning board telling me something that
    // could be wrong"). The scan still runs: KV board feeds the website
    // (Tonight's board + status card) and primes the 15:30 FINAL, which is
    // now the ONLY earnings message of the day.
    await env.SIGNAL_KV.put(`earnscan_done_${iso}`, 'ok', { expirationTtl: 5 * 86400 });
    try { await earnSleeveRunStamp(env, b.park, token); } catch (e) { console.warn('[earn-sleeve]', e.message); }
    const pipe = await earnRefreshPipeline(env);   // refresh 2-week pipeline from live calendar
    // P33 (owner 2026-08-03): the page's static fallback self-refreshes daily —
    // a fallback with no refresh path rotted 26 days and showed "nothing
    // reporting" mid-earnings-season. Mirror the fresh pipeline to GitHub so
    // the fallback can never age more than one trading day.
    if (pipe && Array.isArray(pipe.rows) && pipe.rows.length) {
      try {
        await githubUpsertResearchFile(env, 'data/earnings_pipeline.json',
          () => pipe, `auto: earnings pipeline fallback ${iso} (${pipe.rows.length} rows)`);
      } catch (e) { console.warn('[earn] pipeline fallback mirror:', e.message); }
    }
  } catch (e) {
    await env.SIGNAL_KV.delete(key);          // retry next tick in window
    console.warn('[earn] morning job failed:', e.message);
  }
}

// ── Resolve yesterday's LONGs into the nightly result log (2026-07-29) ──
// The seed log froze at its build date (2026-06-30) and nothing appended live
// outcomes — track-record data loss every LONG night. Runs 9:35–9:55 ET:
// entry = prior session close, exit = today's open (Schwab daily candles),
// appended newest-first to data/earnings_play_today.json. Idempotent.
async function earnResolveJob(env, etNow, token) {
  const h = etNow.getHours(), m = etNow.getMinutes();
  if (!(h === 9 && m >= 35 && m <= 55)) return;
  const iso = isoDateET(etNow);
  const key = `earn_resolve_${iso}`;
  if (await env.SIGNAL_KV.get(key)) return;
  // previous trading day (skip weekends/holidays, up to a week back)
  let prev = null;
  for (let back = 1; back <= 7 && !prev; back++) {
    const d = toET(new Date(Date.now() - back * 86400000));
    if (d.getDay() === 0 || d.getDay() === 6 || isHol(d)) continue;
    prev = isoDateET(d);
  }
  if (!prev) return;
  const raw = await env.SIGNAL_KV.get(`earn_board_${prev}`);
  if (!raw) { await env.SIGNAL_KV.put(key, 'no-board', { expirationTtl: 86400 }); return; }
  const b = JSON.parse(raw);
  let longs = (b.board || []).filter(r => r.verdict === 'LONG');
  // Override: earn_actual_longs_<date> pins what the 3:31 FINAL actually
  // signaled — protects the log when a board is later rebuilt/restored.
  try {
    const ovRaw = await env.SIGNAL_KV.get(`earn_actual_longs_${prev}`);
    // The override is authoritative, not a filter: a seeded ticker must be
    // resolved even when the stored board was rebuilt with a different verdict
    // (FTAI 2026-07-29 was lost to the old intersect-only behavior).
    if (ovRaw) {
      const ov = JSON.parse(ovRaw);
      longs = ov.map(t => longs.find(r => r.ticker === t) ||
                          (b.board || []).find(r => r.ticker === t) ||
                          { ticker: t, pw_ratio: null, deep_itm_usd: 0, runup_2w: null, base_rate: null });
    }
  } catch (_) {}
  if (!longs.length) { await env.SIGNAL_KV.put(key, 'no-longs', { expirationTtl: 86400 }); return; }
  await env.SIGNAL_KV.put(key, 'running', { expirationTtl: 86400 });
  try {
    const rows = [];
    for (const r of longs) {
      try {
        const end = Date.now();
        const start = end - 6 * 86400000;
        const j = await fetchSchwabJSON(
          `https://api.schwabapi.com/marketdata/v1/pricehistory?symbol=${encodeURIComponent(r.ticker)}` +
          `&periodType=month&period=1&frequencyType=daily&frequency=1&startDate=${start}&endDate=${end}`, token, env);
        const cs = (j?.candles || []).map(c => ({ d: isoDateET(toET(new Date(c.datetime))), o: c.open, c: c.close }));
        const eC = cs.find(c => c.d === prev)?.c;
        const xO = cs.find(c => c.d === iso)?.o;
        if (eC > 0 && xO > 0) {
          rows.push({ date: prev, ticker: r.ticker, pw_ratio: r.pw_ratio ?? null,
                      deep_itm_usd: r.deep_itm_usd ?? 0, runup_2w: r.runup_2w ?? null,
                      base_rate: r.base_rate ?? null, verdict: 'LONG',
                      move_24h: +((xO / eC - 1).toFixed(4)), pl_r: null, live: true });
        }
      } catch (e) { console.warn('[earn-resolve]', r.ticker, e.message); }
    }
    if (rows.length) {
      await githubUpsertResearchFile(env, 'data/earnings_play_today.json',
        cur => {
          cur.log = cur.log || [];
          for (const row of rows) {
            if (!cur.log.some(x => x.date === row.date && x.ticker === row.ticker)) cur.log.unshift(row);
          }
          return cur;
        }, `auto: earnings results ${prev}`);
      await logEvent(env, 'info', 'earnings', `resolved ${rows.length} LONG(s) from ${prev}`, {});
    }
    await env.SIGNAL_KV.put(key, 'done', { expirationTtl: 86400 });
  } catch (e) {
    await env.SIGNAL_KV.delete(key);
    console.warn('[earn-resolve] failed:', e.message);
  }
}

// 9:40 "morning after" card (owner 2026-07-31): re-send yesterday's FINAL
// board to the earnings channel with an added close→open % per ticker —
// observation only, the strategy itself is unchanged. Claim-gated (P22).
async function earnAfterJob(env, etNow, token) {
  const h = etNow.getHours(), m = etNow.getMinutes();
  if (!(h === 9 && m >= 40 && m <= 55)) return;
  const iso = isoDateET(etNow);
  const key = `earn_after_${iso}`;
  const done = await env.SIGNAL_KV.get(key);
  if (done && !done.startsWith('claim:')) return;
  let prev = null;
  for (let back = 1; back <= 7 && !prev; back++) {
    const d = toET(new Date(Date.now() - back * 86400000));
    if (d.getDay() === 0 || d.getDay() === 6 || isHol(d)) continue;
    prev = isoDateET(d);
  }
  if (!prev) return;
  const raw = await env.SIGNAL_KV.get(`earn_board_${prev}`);
  if (!raw) { await env.SIGNAL_KV.put(key, 'no-board', { expirationTtl: 86400 }); return; }
  const b = JSON.parse(raw);
  const scored = (b.board || []).filter(r => !(r.notes || []).some(n => String(n).startsWith('outside universe')));
  if (!scored.length) { await env.SIGNAL_KV.put(key, 'no-rows', { expirationTtl: 86400 }); return; }
  if (!(await claimSendSlot(env, key))) return;
  try {
    for (const r of scored) {
      try {
        const end = Date.now(), start = end - 6 * 86400000;
        const j = await fetchSchwabJSON(
          `https://api.schwabapi.com/marketdata/v1/pricehistory?symbol=${encodeURIComponent(r.ticker)}` +
          `&periodType=month&period=1&frequencyType=daily&frequency=1&startDate=${start}&endDate=${end}`, token, env);
        const cs = (j?.candles || []).map(c => ({ d: isoDateET(toET(new Date(c.datetime))), o: c.open, c: c.close }));
        const eC = cs.find(c => c.d === prev)?.c;
        const xO = cs.find(c => c.d === iso)?.o;
        if (eC > 0 && xO > 0) r.afterPct = xO / eC - 1;
      } catch (e) { console.warn('[earn-after]', r.ticker, e.message); }
    }
    b.after = true; b.final = false; b.date = prev;
    let png = null;
    try { png = await renderEarningsCardPng(b); } catch (e) { console.warn('[earn-after] render:', e.message); }
    const textFallback = `🌙 **[EARNINGS] morning after — ${prev}**\n` +
      scored.map(r => `${r.ticker} ${r.afterPct != null ? (r.afterPct >= 0 ? '+' : '') + (r.afterPct * 100).toFixed(1) + '%' : '—'}`).join(' · ') +
      `\n_close → next open · observation only_`;
    // Earnings-play channel ONLY (owner 2026-07-31) — no Sigma 3 mirror.
    // P22 (audit 2026-07-31): 'sent' ONLY on confirmed delivery; a failed
    // delivery releases the claim so the next tick in 9:40–9:55 retries.
    const wh = await env.SIGNAL_KV.get('earnings_webhook_url');
    if (!wh) {
      await env.SIGNAL_KV.put(key, 'no-webhook', { expirationTtl: 86400 });
      await logEvent(env, 'warn', 'earn-after', 'morning-after card skipped — earnings_webhook_url unset');
      return;
    }
    let delivered = false;
    if (png) delivered = await postWebhookImage(wh, png, EARN_CARD_FOOTER, 'earnings-after.png');
    if (!delivered) {
      try {
        const r = await fetch(wh, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ content: textFallback.slice(0, 1900) }) });
        delivered = r.ok;
      } catch (e) { console.warn('[earn-after] text fallback failed:', e.message); }
    }
    if (delivered) {
      await env.SIGNAL_KV.put(key, 'sent', { expirationTtl: 86400 });
      await logEvent(env, 'info', 'earn-after', `morning-after card sent for ${prev}`, { rows: scored.length });
    } else {
      await env.SIGNAL_KV.delete(key);          // release claim → retry next tick
      console.warn('[earn-after] delivery failed — claim released for retry');
    }
  } catch (e) {
    console.warn('[earn-after] failed:', e.message);
  }
}

async function earnRescoreJob(env, etNow, token) {
  const iso = isoDateET(etNow);
  const m = etNow.getMinutes(), h = etNow.getHours();
  if (!((m >= 0 && m <= 2) || (m >= 30 && m <= 32))) return;     // :00/:30 ticks
  if (h < 10 || h >= 15 && !(h === 15 && m <= 2)) return;        // 10:00–15:02
  const raw = await env.SIGNAL_KV.get(`earn_board_${iso}`);
  if (!raw) return;
  const prev = JSON.parse(raw);
  if (!prev.board.length) return;
  const lock = `earn_rescore_${iso}_${h}_${m < 30 ? 0 : 30}`;
  if (await env.SIGNAL_KV.get(lock)) return;
  await env.SIGNAL_KV.put(lock, '1', { expirationTtl: 3600 });
  try {
    const b = await earnBuildBoard(env, token, iso, { withIntraday: true });
    await env.SIGNAL_KV.put(`earn_board_${iso}`, JSON.stringify(b), { expirationTtl: 3 * 86400 });
  } catch (e) { console.warn('[earn] rescore failed:', e.message); }
}

async function earnFinalJob(env, etNow, token) {
  const iso = isoDateET(etNow);
  const key = `earn_done_${iso}_final`;
  if (await env.SIGNAL_KV.get(key)) return;
  const h = etNow.getHours(), m = etNow.getMinutes();
  // Window widened to :58 (owner 2026-08-03): the FINAL must reach him NO
  // MATTER THE RESULT — failed delivery releases the marker so every
  // remaining tick retries until Discord confirms.
  if (!(h === 15 && m >= 30 && m <= 58)) return;
  await env.SIGNAL_KV.put(key, 'running', { expirationTtl: 86400 });
  try {
    const b = await earnBuildBoard(env, token, iso, { withIntraday: true });
    b.final = true;
    await env.SIGNAL_KV.put(`earn_board_${iso}`, JSON.stringify(b), { expirationTtl: 7 * 86400 });
    const mode = (await env.SIGNAL_KV.get('earnings_mode')) || 'paper';
    if (b.longs.length) {
      await env.SIGNAL_KV.put(`earn_open_${iso}`, JSON.stringify(
        { date: iso, longs: b.longs.map(l => l.ticker), mode }), { expirationTtl: 7 * 86400 });
    }
    // P22 (owner 2026-08-03: the FINAL never confirmed delivery — marker sat
    // on 'running' while both sinks could fail): mark 'sent' ONLY when at
    // least one sink confirmed; otherwise release for retry next tick.
    const sent = await earnSendCard(env, b, 'final');
    const delivered = !!(sent && (sent.dm || sent.webhook));
    if (delivered) {
      await env.SIGNAL_KV.put(key, 'sent', { expirationTtl: 86400 });
    } else {
      await env.SIGNAL_KV.delete(key);
      try { await logEvent(env, 'error', 'earn-final', 'FINAL undelivered on all sinks — retrying', { sinks: sent }); } catch (_) {}
    }
  } catch (e) {
    await env.SIGNAL_KV.delete(key);
    console.warn('[earn] final job failed:', e.message);
  }
}

async function earnExitJob(env, etNow, token) {
  // 9:32: sell reminder + outcome recording for the previous session's LONGs.
  const iso = isoDateET(etNow);
  const h = etNow.getHours(), m = etNow.getMinutes();
  if (!(h === 9 && m >= 32 && m <= 45)) return;
  const prevISO = isoDateET(prevTrade(etNow));
  const raw = await env.SIGNAL_KV.get(`earn_open_${prevISO}`);
  if (!raw) return;
  const key = `earn_done_${iso}_exit`;
  if (await env.SIGNAL_KV.get(key)) return;
  await env.SIGNAL_KV.put(key, 'running', { expirationTtl: 86400 });
  try {
    const open_ = JSON.parse(raw);
    const outs = [];
    for (const sym of open_.longs) {
      try {
        const end = Date.now(), start = end - 7 * 86400_000;
        const d = await fetchSchwabJSON(
          `https://api.schwabapi.com/marketdata/v1/pricehistory?symbol=${encodeURIComponent(sym)}&periodType=month&frequencyType=daily&frequency=1&startDate=${start}&endDate=${end}`,
          token, env);
        const cs = d.candles || [];
        const prevBar = cs[cs.length - 2], todayBar = cs[cs.length - 1];
        const ret = todayBar.open / prevBar.close - 1;
        outs.push(`${sym} ${ret >= 0 ? '+' : ''}${(ret * 100).toFixed(1)}%`);
        const logRaw = await env.SIGNAL_KV.get('earn_log');
        const log = logRaw ? JSON.parse(logRaw) : [];
        log.unshift({ date: prevISO, ticker: sym, mode: open_.mode,
                      move_24h: +ret.toFixed(4), recorded: iso });
        await env.SIGNAL_KV.put('earn_log', JSON.stringify(log.slice(0, 500)));
      } catch (e) { outs.push(`${sym} (quote failed)`); }
    }
    const mode = open_.mode || 'paper';
    await earnSend(env, `🌙 **[EARNINGS] SELL AT OPEN — now.** ` +
      `Overnight results: ${outs.join(' · ')}. Green or red — out.`);
    await env.SIGNAL_KV.delete(`earn_open_${prevISO}`);
    // Basket sold → money re-enters the sleeve NOW: force-restart the
    // "since we entered" clock at this morning's prices (owner 2026-07-14).
    try {
      const bRaw = await env.SIGNAL_KV.get(`earn_board_${iso}`);
      const park = bRaw ? JSON.parse(bRaw).park : null;
      await earnSleeveRunStamp(env, park || await earnParkingSleeve(env, token), token, true);
    } catch (e) { console.warn('[earn-sleeve-reset]', e.message); }
  } catch (e) {
    await env.SIGNAL_KV.delete(key);
    console.warn('[earn] exit job failed:', e.message);
  }
}

async function earnNightlyCollect(env, etNow, token) {
  // 16:41–17:15: append today's ≤14-DTE volume-premium day entry for every
  // name reporting within 15 days. Chunked ≤10 names/tick.
  const iso = isoDateET(etNow);
  const doneKey = `earn_collect_done_${iso}`;
  if (await env.SIGNAL_KV.get(doneKey)) return;
  const h = etNow.getHours(), m = etNow.getMinutes();
  if (!((h === 16 && m >= 41) || (h === 17 && m <= 15))) return;
  const horizon = new Date(etNow.getTime() + 15 * 86400_000).toISOString().slice(0, 10);
  const cal = await earnCalendar(env, iso, horizon);
  const uniset = new Set(Object.keys(((await earnSeed(env)).base_rates) || {}));
  const names = [...new Set(cal.events.filter(e => uniset.has(e.ticker)).map(e => e.ticker))];
  const progKey = `earn_collect_part_${iso}`;
  const done = new Set(JSON.parse((await env.SIGNAL_KV.get(progKey)) || '[]'));
  const batch = names.filter(n => !done.has(n)).slice(0, 10);
  const repOf = {};
  for (const ev of cal.events) if (!repOf[ev.ticker]) repOf[ev.ticker] = ev.report_date;
  for (const sym of batch) {
    try {
      const day = await earnChainDayPremium(env, token, sym, iso, repOf[sym]);
      for (const [pfx, rec] of [['earnflow_', { c: day.c, p: day.p, di: day.di }],
                                ['earnflow_m_', { c: day.mc, p: day.mp, di: day.mdi }]]) {
        const kvKey = `${pfx}${sym}`;
        const cur = JSON.parse((await env.SIGNAL_KV.get(kvKey)) || '{}');
        cur[iso] = rec;
        const isos = Object.keys(cur).sort().slice(-25);
        const trimmed = {}; for (const k of isos) trimmed[k] = cur[k];
        await env.SIGNAL_KV.put(kvKey, JSON.stringify(trimmed), { expirationTtl: 40 * 86400 });
      }
      done.add(sym);
    } catch (e) { console.warn(`[earn] collect ${sym}:`, e.message); done.add(sym); }
  }
  await env.SIGNAL_KV.put(progKey, JSON.stringify([...done]), { expirationTtl: 86400 });
  if (done.size >= names.length) {
    await env.SIGNAL_KV.put(doneKey, 'done', { expirationTtl: 3 * 86400 });
    await earnRefreshPipeline(env);           // pipeline reflects the day's fresh flow
    console.log(`[earn] nightly collect complete: ${done.size} names`);
  }
}

// CycleLab 5-min slot helpers — shared by the EOD append and the live feed.
function _cycSlots() {
  const slots = [];
  for (let h = 9, m = 30; h < 16; m += 5) { if (m >= 60) { m = 0; h++; if (h >= 16) break; }
    slots.push(`${String(h).padStart(2, '0')}:${String(m).padStart(2, '0')}`); }
  return slots;
}

// Schwab 1-min candles → per-5-min-slot net moves array (nulls where empty).
function _cycMovesFromCandles(candles, iso, slots, slotIdx) {
  const oc = {};   // slot idx -> [firstOpen, lastClose]
  for (const c of candles) {
    if (!(c.open > 0 && c.close > 0)) continue;
    const t = toET(new Date(c.datetime));
    if (isoDateET(t) !== iso) continue;
    const h = t.getHours(), m = t.getMinutes();
    if (h < 9 || (h === 9 && m < 30) || h >= 16) continue;
    const idx = slotIdx[`${String(h).padStart(2, '0')}:${String(m - (m % 5)).padStart(2, '0')}`];
    if (idx == null) continue;
    if (oc[idx]) oc[idx][1] = c.close; else oc[idx] = [c.open, c.close];
  }
  const moves = new Array(slots.length).fill(null);
  for (const [idx, [o, c]] of Object.entries(oc)) moves[idx] = parseFloat((c - o).toFixed(2));
  return { moves, filled: Object.keys(oc).length };
}

// CycleLab LIVE feed (2026-06-11, user: "continue the orange SPX line live").
// Once per 5 min during RTH the cron snapshots today's session-so-far into
// KV `cyc_today` ({d, w, m, at} — cyclicality_data day format + timestamp).
// Page views read it via GET /cyclicality-today (pure KV) — ZERO extra
// Schwab calls per viewer; total cost ≈ 78 pricehistory calls/day.
async function captureCycTodaySlots(env, etNow, token) {
  const h = etNow.getHours(), m = etNow.getMinutes();
  const inWindow = (h > 9 || (h === 9 && m >= 36)) && (h < 16 || (h === 16 && m <= 6));
  if (!inWindow || m % 5 !== 1 || !token) return;   // :36/:41/… — slot just completed
  if (etNow.getDay() === 0 || etNow.getDay() === 6 || isHol(etNow)) return;
  const todayISO = isoDateET(etNow);
  const slots = _cycSlots();
  const slotIdx = Object.fromEntries(slots.map((s, i) => [s, i]));
  const start = Date.parse(`${todayISO}T08:00:00Z`), end = Date.parse(`${todayISO}T23:00:00Z`);
  let hist;
  try {
    hist = await fetchSchwabJSON(
      `https://api.schwabapi.com/marketdata/v1/pricehistory?symbol=%24SPX&periodType=day&frequencyType=minute&frequency=1&startDate=${start}&endDate=${end}&needExtendedHoursData=false`,
      token, env);
  } catch (e) { console.warn('[cyc-live]', e.message); return; }
  const { moves, filled } = _cycMovesFromCandles(hist.candles || [], todayISO, slots, slotIdx);
  if (!filled) return;
  const wd = new Date(`${todayISO}T12:00:00Z`).getUTCDay() - 1;   // 0=Mon
  await env.SIGNAL_KV.put('cyc_today', JSON.stringify(
    { d: todayISO, w: wd, m: moves, at: `${String(h).padStart(2, '0')}:${String(m).padStart(2, '0')}` }),
    { expirationTtl: 86400 });
  // NDX live slots (2026-06-12) — same cadence, own KV key
  try {
    const histN = await fetchSchwabJSON(
      `https://api.schwabapi.com/marketdata/v1/pricehistory?symbol=%24NDX&periodType=day&frequencyType=minute&frequency=1&startDate=${start}&endDate=${end}&needExtendedHoursData=false`,
      token, env);
    const rN = _cycMovesFromCandles(histN.candles || [], todayISO, slots, slotIdx);
    if (rN.filled) await env.SIGNAL_KV.put('cyc_today_ndx', JSON.stringify(
      { d: todayISO, w: wd, m: rN.moves, at: `${String(h).padStart(2, '0')}:${String(m).padStart(2, '0')}` }),
      { expirationTtl: 86400 });
  } catch (e) { console.warn('[cyc-live-ndx]', e.message); }
}

// CycleLab daily feed (2026-06-10): append today\'s (and any recent missing)
// SPX session to cyclicality_data.json from Schwab 1-min pricehistory —
// 5-min slot net moves, the format build_cyclicality_data.py produces.
// ThetaData-free; the page self-updates daily. Runs at EOD + manual route.
async function appendCyclicalityDays(env, opts = {}) {
  const token = await getAccessToken(env);
  const symbol = opts.symbol || '%24SPX';                 // '%24SPX' | '%24NDX'
  const file = opts.file || 'cyclicality_data.json';      // NDX → cyclicality_ndx.json
  const backDays = Math.min(45, opts.backDays || 12);
  const slots = _cycSlots();
  const slotIdx = Object.fromEntries(slots.map((s, i) => [s, i]));

  // current file (raw — cheaper than contents API for a 450KB read)
  const curResp = await fetch(`https://raw.githubusercontent.com/rava8989/brave/main/${file}`,
    { headers: { 'User-Agent': 'schwab-proxy-worker/1.0' } });
  const cur = curResp.ok ? await curResp.json() : { slots, days: [] };
  const have = new Set(cur.days.map(x => x.d));

  // candidate days: last N calendar days, weekdays, non-holiday, missing
  const etNow = toET(new Date());
  const todo = [];
  for (let back = 0; back <= backDays; back++) {
    const d = new Date(etNow); d.setDate(d.getDate() - back);
    if (d.getDay() === 0 || d.getDay() === 6 || isHol(d)) continue;
    const iso = isoDateET(d);
    if (have.has(iso)) continue;
    // skip today before the close — only completed sessions
    if (iso === isoDateET(etNow) && (etNow.getHours() < 16)) continue;
    todo.push(iso);
  }
  if (!todo.length) return { ok: true, appended: [] };

  const appended = [];
  for (const iso of todo.sort()) {
    const start = Date.parse(`${iso}T08:00:00Z`), end = Date.parse(`${iso}T23:00:00Z`);
    let hist;
    try {
      hist = await fetchSchwabJSON(
        `https://api.schwabapi.com/marketdata/v1/pricehistory?symbol=${symbol}&periodType=day&frequencyType=minute&frequency=1&startDate=${start}&endDate=${end}&needExtendedHoursData=false`,
        token, env);
    } catch (e) { console.warn('[cyclelab] pricehistory failed', iso, e.message); continue; }
    const { moves, filled } = _cycMovesFromCandles(hist.candles || [], iso, slots, slotIdx);
    if (filled < 30) { console.warn('[cyclelab] too few slots', iso); continue; }
    const wd = new Date(`${iso}T12:00:00Z`).getUTCDay() - 1;   // 0=Mon
    appended.push({ d: iso, w: wd, m: moves });
  }
  if (!appended.length) return { ok: true, appended: [] };

  await githubUpsertResearchFile(env, file, curObj => {
    if (!curObj.days) { curObj.slots = slots; curObj.days = []; }
    const haveNow = new Set(curObj.days.map(x => x.d));
    for (const rec of appended) if (!haveNow.has(rec.d)) curObj.days.push(rec);
    curObj.days.sort((a, b) => a.d.localeCompare(b.d));
    curObj.built = isoDateET(toET(new Date()));
    return curObj;
  }, `auto: cyclicality ${appended.map(a => a.d).join(', ')}`);
  try { await logEvent(env, 'info', 'research', `cyclicality appended ${appended.length} day(s)`, { days: appended.map(a => a.d) }); } catch (_) {}
  return { ok: true, appended: appended.map(a => a.d) };
}

// ════════════════════════════════════════════════════════════════════
// ADVISORY SCORECARD (2026-06-11) — score our own morning claims.
// Idea adopted from a public vol dashboard that tracks its own predictions.
// Each evening: score GEX regime calls (PIN = day range < 100 pts;
// BREAKOUT = range >= 70 pts — thresholds from the 2026-06-10 validation:
// PIN median 57, BREAKOUT median 84, 95% of PIN days < 100) and the
// Day-type strategy claims (favored → P/L > 0; below-normal → P/L < its
// normal; flat → |P/L| < half its normal). Ledger: data/advisory_scorecard.json
// ════════════════════════════════════════════════════════════════════
const SCORE_DAYTYPE_CLAIMS = {
  'NEUTRAL/BEAR': [['diagPL', 'flat', 356], ['bobfPL', 'favored', 510]],
  'NEUTRAL/CHOP': [['diagPL', 'favored', 378]],
  'NEUTRAL/BULL': [['stradPL', 'below-normal', 1091]],
};

// Vol-flow claims (2026-06-11): scored cells = the ones that replicated both
// halves in the 3-leg study. Straddle/VOL_BID is suggestive-only — NOT scored.
// 'above-normal' → P/L > normal; 'below-normal' → P/L < normal.
const SCORE_VOLFLOW_CLAIMS = {
  VOL_BID: [['m8bfPL', 'below-normal', 434]],
  VOL_SUPPLY: [['diagPL', 'below-normal', 378]],
  MIXED: [['m8bfPL', 'above-normal', 434], ['diagPL', 'above-normal', 378]],
};

async function scoreAdvisories(env) {
  const gh = { headers: { 'User-Agent': 'schwab-proxy-worker/1.0' } };
  const [cycR, bunR, hist] = await Promise.all([
    fetch('https://raw.githubusercontent.com/rava8989/brave/main/cyclicality_data.json', gh),
    fetch('https://raw.githubusercontent.com/rava8989/brave/main/cor1m_contango_bundle.json', gh),
    env.SIGNAL_KV.get('history_data').then(r => r ? JSON.parse(r) : []),
  ]);
  if (!cycR.ok || !bunR.ok) throw new Error('source fetch failed');
  const cyc = await cycR.json();
  const daily = (await bunR.json()).daily || [];
  const regimeByDate = Object.fromEntries(daily.map(x => [x.date, x.regime]));
  const bundleDates = daily.map(x => x.date);
  const histBy = Object.fromEntries(hist.map(r => [r.date, r]));
  const cycBy = Object.fromEntries(cyc.days.map(x => [x.d, x]));

  // gex_daily monthly files
  const gexd = {};
  for (const ym of [...new Set(cyc.days.filter(x => x.d >= '2026-03-01').map(x => x.d.slice(0, 7)))]) {
    try {
      const r = await fetch(`https://raw.githubusercontent.com/rava8989/brave/main/data/gex_daily/${ym}.json`, gh);
      if (r.ok) Object.assign(gexd, await r.json());
    } catch (_) {}
  }

  // vix decomposition (vol-flow labels) — keyed by the day the label is FOR;
  // the advisory on day d used the latest label STRICTLY BEFORE d (≤5d gap).
  let decomp = {};
  try {
    const r = await fetch('https://raw.githubusercontent.com/rava8989/brave/main/data/vix_decomposition.json', gh);
    if (r.ok) decomp = await r.json();
  } catch (_) {}
  const decompDates = Object.keys(decomp).sort();
  const priorLabelFor = d => {
    let lo = 0, hi = decompDates.length - 1, best = null;
    while (lo <= hi) { const m = (lo + hi) >> 1;
      if (decompDates[m] < d) { best = decompDates[m]; lo = m + 1; } else hi = m - 1; }
    if (!best) return null;
    const gap = (new Date(d) - new Date(best)) / 86400000;
    return gap <= 5 ? decomp[best].label : null;
  };

  const dayRange = d => {
    const m = cycBy[d]?.m; if (!m) return null;
    let c = 0, hi = 0, lo = 0;
    for (const v of m) { c += (v || 0); if (c > hi) hi = c; if (c < lo) lo = c; }
    return hi - lo;
  };
  const mkDate = s => new Date(+s.slice(0, 4), +s.slice(5, 7) - 1, +s.slice(8, 10), 12);

  const scored = {};
  for (const d of Object.keys(gexd).sort()) {
    if (!cycBy[d]) continue;
    const entry = {};
    // 1. GEX regime call
    const am = gexd[d]?.am;
    const rng = dayRange(d);
    if (am?.regime && rng != null) {
      const hit = am.regime === 'BREAKOUT' ? rng >= 70 : rng < 100;
      entry.gex = { regime: am.regime, range: Math.round(rng), hit };
    }
    // 2. Day-type strategy claims (line used PRIOR day's regime + that day's shape)
    const bi = bundleDates.indexOf(d);
    const prevRegime = bi > 0 ? regimeByDate[bundleDates[bi - 1]] : null;
    const group = regimeGroup(prevRegime);
    const cycInfo = classifyCyclePrediction(cyc.days, mkDate(d));
    if (group && cycInfo) {
      const shape = { BULLISH: 'BULL', BEARISH: 'BEAR', CHOPPY: 'CHOP', MIXED: 'MIX' }[cycInfo.cls];
      const key = `${group}/${shape}`;
      entry.daytype = { key, cells: [] };
      for (const [fld, claim, normal] of (SCORE_DAYTYPE_CLAIMS[key] || [])) {
        const pnl = histBy[d]?.[fld];
        if (pnl == null || pnl === 0) continue;   // strategy didn't trade → unscorable
        const hit = claim === 'favored' ? pnl > 0
                  : claim === 'below-normal' ? pnl < normal
                  : Math.abs(pnl) < normal / 2;   // 'flat'
        entry.daytype.cells.push({ strat: fld.replace('PL', ''), claim, pnl, hit });
      }
    }
    // 3. Vol-flow claims (prior day's decomposition label)
    const vfLabel = priorLabelFor(d);
    if (vfLabel) {
      entry.volflow = { label: vfLabel, cells: [] };
      for (const [fld, claim, normal] of (SCORE_VOLFLOW_CLAIMS[vfLabel] || [])) {
        const pnl = histBy[d]?.[fld];
        if (pnl == null || pnl === 0) continue;   // strategy didn't trade → unscorable
        const hit = claim === 'above-normal' ? pnl > normal : pnl < normal;
        entry.volflow.cells.push({ strat: fld.replace('PL', ''), claim, pnl, hit });
      }
      if (!entry.volflow.cells.length) delete entry.volflow;
    }
    if (entry.gex || (entry.daytype && entry.daytype.cells.length) || entry.volflow) scored[d] = entry;
  }

  await githubUpsertResearchFile(env, 'data/advisory_scorecard.json',
    cur => { Object.assign(cur, scored); return cur; }, 'auto: advisory scorecard');
  return { days: Object.keys(scored).length };
}

// Month-to-date scorecard line for the evening preview.
async function scorecardLine(env, etNow) {
  try {
    const r = await fetch('https://raw.githubusercontent.com/rava8989/brave/main/data/advisory_scorecard.json',
      { headers: { 'User-Agent': 'schwab-proxy-worker/1.0' } });
    if (!r.ok) return null;
    const led = await r.json();
    const ym = isoDateET(etNow).slice(0, 7);
    let gh = 0, gt = 0, dh = 0, dt = 0, vh = 0, vt = 0;
    for (const [d, e] of Object.entries(led)) {
      if (!d.startsWith(ym)) continue;
      if (e.gex) { gt++; if (e.gex.hit) gh++; }
      for (const c of (e.daytype?.cells || [])) { dt++; if (c.hit) dh++; }
      for (const c of (e.volflow?.cells || [])) { vt++; if (c.hit) vh++; }
    }
    if (!gt && !dt && !vt) return null;
    const parts = [];
    if (gt) parts.push(`GEX ${gh}/${gt}`);
    if (dt) parts.push(`Day-type ${dh}/${dt}`);
    if (vt) parts.push(`Vol-flow ${vh}/${vt}`);
    return `Scorecard: ${parts.join(' · ')} (MTD)`;
  } catch (_) { return null; }
}

// Generic GitHub research-file upsert — same auth pattern as
// mirrorHistoryToGitHub but path-parameterized and merge-based.
// mutate(currentObj) → newObj. 404 (no file yet) starts from {}.
async function githubUpsertResearchFile(env, path, mutate, message) {
  if (!env.GITHUB_TOKEN) return { skipped: 'no GITHUB_TOKEN' };
  const apiUrl = `https://api.github.com/repos/rava8989/brave/contents/${path}`;
  const ghHeaders = {
    'Authorization': `Bearer ${env.GITHUB_TOKEN}`,
    'Accept': 'application/vnd.github+json',
    'User-Agent': 'schwab-proxy-worker/1.0',
    'X-GitHub-Api-Version': '2022-11-28',
  };
  // base64 must be UTF-8-safe both ways: files first written by Python carry
  // \uXXXX escapes that parse to real unicode, and naive btoa() throws on it
  // (this silently killed every worker write to earnings_play_today.json).
  const b64encodeUtf8 = (s) => {
    const u8 = new TextEncoder().encode(s);
    let bin = '';
    for (let i = 0; i < u8.length; i += 0x8000) bin += String.fromCharCode.apply(null, u8.subarray(i, i + 0x8000));
    return btoa(bin);
  };
  const getResp = await fetch(apiUrl, { headers: ghHeaders });
  let sha = null, cur = {};
  if (getResp.ok) {
    const meta = await getResp.json();
    sha = meta.sha;
    try {
      const bytes = Uint8Array.from(atob((meta.content || '').replace(/\n/g, '')), ch => ch.charCodeAt(0));
      cur = JSON.parse(new TextDecoder().decode(bytes));
    } catch (_) { cur = {}; }
  } else if (getResp.status !== 404) {
    throw new Error(`GH GET ${path} ${getResp.status}`);
  }
  const next = mutate(cur) || cur;
  const body = { message: message || `auto: update ${path}`, content: b64encodeUtf8(JSON.stringify(next, null, 0)) };
  if (sha) body.sha = sha;
  const putResp = await fetch(apiUrl, {
    method: 'PUT', headers: { ...ghHeaders, 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  if (!putResp.ok) throw new Error(`GH PUT ${path} ${putResp.status}: ${(await putResp.text()).slice(0, 150)}`);
  return { ok: true };
}

// EOD: fold today\'s KV research captures into monthly GitHub files so they
// survive the 90-day KV TTL. Best-effort — never blocks the EOD settle.
async function persistResearchArtifacts(env, etNow) {
  const todayISO = isoDateET(etNow);
  const ym = todayISO.slice(0, 7);
  const jobs = [
    [`fly_marks_${todayISO}`,      `data/fly_marks/${ym}.json`,   'fly marks'],
    [`tail_put_snap_${todayISO}`,  `data/tail_puts/${ym}.json`,   'tail put snap'],
    [`diag_chain_snap_${todayISO}`,`data/diag_chains/${ym}.json`, 'diag chain snap'],
    [`gxbf_chain_snap_${todayISO}`,`data/gxbf_chains/${ym}.json`, 'gxbf chain snap'],
    [`vix_surface_snap_${todayISO}`,`data/vix_surface/${ym}.json`,'vix surface snap'],
  ];
  const results = [];
  for (const [kvKey, ghPath, label] of jobs) {
    try {
      const raw = await env.SIGNAL_KV.get(kvKey);
      if (!raw) { results.push({ label, skipped: 'no KV data' }); continue; }
      const payload = JSON.parse(raw);
      const r = await githubUpsertResearchFile(env, ghPath,
        cur => { cur[todayISO] = payload; return cur; },
        `auto: ${label} ${todayISO}`);
      results.push({ label, ...r });
      try { await logEvent(env, 'info', 'research', `${label} persisted to ${ghPath}`, {}); } catch (_) {}
    } catch (e) {
      results.push({ label, error: e.message });
      console.warn(`[research-persist] ${label} failed:`, e.message);
      try { await logEvent(env, 'error', 'research', `${label} persist FAILED`, { msg: e.message }); } catch (_) {}
    }
  }
  // GEX daily summary: morning snapshot (gex_daily KV) + closing gex_current
  try {
    const amRaw = await env.SIGNAL_KV.get(`gex_daily_${todayISO}`);
    const pmRaw = await env.SIGNAL_KV.get('gex_current');
    const trim = g => g ? { t: g.timestamp, spot: g.spot, regime: g.regime, totalGex: g.totalGex,
                            flip: g.flipStrike ?? null, maxPos: g.maxPosStrike ?? null, maxNeg: g.maxNegStrike ?? null } : null;
    const am = amRaw ? JSON.parse(amRaw).am : null;
    const pm = pmRaw ? trim(JSON.parse(pmRaw)) : null;
    if (am || pm) {
      await githubUpsertResearchFile(env, `data/gex_daily/${ym}.json`,
        cur => { cur[todayISO] = { am, pm }; return cur; }, `auto: gex daily ${todayISO}`);
      results.push({ label: 'gex daily', ok: true });
    } else results.push({ label: 'gex daily', skipped: 'no snapshots' });
  } catch (e) {
    results.push({ label: 'gex daily', error: e.message });
    console.warn('[research-persist] gex daily failed:', e.message);
  }
  // VIXEQ daily self-feed (2026-06-11): open from the 9:30-10:00 capture
  // (cor1m_open KV), close quoted fresh now. Extends data/vixeq_daily.json
  // (ThetaData backfill 2024-10→2026-06) Schwab-only, same {date:{open,close}}.
  try {
    const openRec = JSON.parse(await env.SIGNAL_KV.get(`cor1m_open_${todayISO}`) || 'null');
    const open = openRec?.vixeq ?? null;
    let close = null;
    try {
      const tk = await getAccessToken(env);
      const q = await fetchSchwabJSON('https://api.schwabapi.com/marketdata/v1/quotes?symbols=%24VIXEQ&fields=quote', tk, env);
      const qq = q?.['$VIXEQ']?.quote;
      const tt = qq?.tradeTime ?? qq?.quoteTime;
      if (tt && isoDateET(toET(new Date(tt))) === todayISO && qq.lastPrice > 0) close = parseFloat(qq.lastPrice.toFixed(2));
    } catch (_) { /* close best-effort */ }
    if (open != null || close != null) {
      await githubUpsertResearchFile(env, 'data/vixeq_daily.json',
        cur => { cur[todayISO] = { open: open ?? cur[todayISO]?.open ?? null, close: close ?? cur[todayISO]?.close ?? null }; return cur; },
        `auto: vixeq ${todayISO}`);
      results.push({ label: 'vixeq daily', ok: true });
    } else results.push({ label: 'vixeq daily', skipped: 'no data' });
  } catch (e) {
    results.push({ label: 'vixeq daily', error: e.message });
    console.warn('[research-persist] vixeq failed:', e.message);
  }
  // VIX term-structure daily self-feed (2026-07-29): closes for the whole
  // complex, Schwab quotes at the 16:25 tick. Replaces the dead ThetaData
  // index feed (sub lapsed 7/23) — the Tail Hedge bundle reads this file.
  try {
    const syms = ['$VIX9D', '$VIX', '$VIX3M', '$VIX6M', '$VVIX', '$COR1M'];
    const tk = await getAccessToken(env);
    const q = await fetchSchwabJSON(
      `https://api.schwabapi.com/marketdata/v1/quotes?symbols=${encodeURIComponent(syms.join(','))}&fields=quote`, tk, env);
    const row = {};
    let got = 0;
    for (const s of syms) {
      const lp = q?.[s]?.quote?.lastPrice;
      if (lp != null && lp > 0) { row[s.slice(1).toLowerCase()] = parseFloat(lp.toFixed(2)); got++; }
    }
    if (got >= 4) {
      await githubUpsertResearchFile(env, 'data/vix_term_daily.json',
        cur => { cur[todayISO] = row; return cur; },
        `auto: vix term ${todayISO}`);
      results.push({ label: 'vix term daily', ok: true });
    } else results.push({ label: 'vix term daily', skipped: `only ${got} quotes` });
  } catch (e) {
    results.push({ label: 'vix term daily', error: e.message });
    console.warn('[research-persist] vix term failed:', e.message);
  }
  return results;
}

// ── COT weekly self-feed (2026-06-12) ───────────────────────────────────
// CFTC publishes Friday ~15:30 ET (data as of Tuesday). Appends any new
// weeks for the 9 currency contracts to data/cot_currencies.json on GitHub.
// Initial backfill: fetch_cot_data.py (2000→). Manual: GET /cot-refresh-now.
const COT_CODES = { EUR: '099741', JPY: '097741', GBP: '096742', CAD: '090741',
                    CHF: '092741', AUD: '232741', NZD: '112741', MXN: '095741', DXY: '098662' };

async function cotWeeklyRefresh(env) {
  const cur = await (await fetch('https://raw.githubusercontent.com/rava8989/brave/main/data/cot_currencies.json',
    { headers: { 'User-Agent': 'schwab-proxy-worker/1.0' } })).json();
  const added = {};
  for (const [key, code] of Object.entries(COT_CODES)) {
    const rows = cur.series[key] || [];
    const last = rows.length ? rows[rows.length - 1][0] : '2000-01-01';
    const q = new URLSearchParams({
      '$select': 'report_date_as_yyyy_mm_dd,open_interest_all,'
        + 'noncomm_positions_long_all,noncomm_positions_short_all,'
        + 'comm_positions_long_all,comm_positions_short_all,'
        + 'nonrept_positions_long_all,nonrept_positions_short_all',
      '$where': `cftc_contract_market_code='${code}' AND report_date_as_yyyy_mm_dd>'${last}'`,
      '$order': 'report_date_as_yyyy_mm_dd ASC', '$limit': '100',
    });
    try {
      const r = await fetch(`https://publicreporting.cftc.gov/resource/6dca-aqww.json?${q}`,
        { headers: { 'User-Agent': 'schwab-proxy-worker/1.0' } });
      if (!r.ok) continue;
      const fresh = await r.json();
      const newRows = fresh.map(x => [
        x.report_date_as_yyyy_mm_dd.slice(0, 10), +x.open_interest_all,
        +x.noncomm_positions_long_all, +x.noncomm_positions_short_all,
        +x.comm_positions_long_all, +x.comm_positions_short_all,
        +x.nonrept_positions_long_all, +x.nonrept_positions_short_all,
      ]).filter(r2 => r2.slice(1).every(Number.isFinite));
      if (newRows.length) added[key] = newRows;
    } catch (e) { console.warn('[cot]', key, e.message); }
  }
  if (!Object.keys(added).length) return { ok: true, added: 0 };
  await githubUpsertResearchFile(env, 'data/cot_currencies.json', curF => {
    for (const [key, rows] of Object.entries(added)) {
      const have = new Set((curF.series[key] || []).map(r => r[0]));
      curF.series[key] = (curF.series[key] || []).concat(rows.filter(r => !have.has(r[0])));
    }
    return curF;
  }, `auto: COT ${Object.values(added)[0][Object.values(added)[0].length - 1][0]}`);
  try { await logEvent(env, 'info', 'research', `COT refreshed: +${Object.values(added).reduce((a, b) => a + b.length, 0)} rows`, {}); } catch (_) {}
  return { ok: true, added: Object.fromEntries(Object.entries(added).map(([k, v]) => [k, v.length])) };
}

// ── Auto-Tilt advisory (2026-06-10) — ADVISORY ONLY, no sizing automation ──
// Ports multi-strategy-tester.html _tiltWindowMAR/_tiltWeightsForDay with the
// tester defaults (win 60, floor 0.25x, cap 2.0x, minTrades 5, marCap 99).
// Reads KV history_data (rows strictly BEFORE today — no look-ahead) and
// renders one line for the Discord messages: "Tilt 60d: M8BF 1.2x ...".
const TILT_P = { win: 60, floorM: 0.25, capM: 2, minTrades: 5, marCap: 99 };
const TILT_FIELDS = [['M8BF', 'm8bfPL'], ['Strad', 'stradPL'], ['BOBF', 'bobfPL'], ['GXBF', 'gxbfPL'], ['Diag', 'diagPL']];

function tiltWindowMAR(rows, i, fld, p) {
  const lo = Math.max(0, i - p.win);
  let n = 0, sum = 0, cum = 0, peak = 0, mdd = 0;
  for (let j = lo; j < i; j++) {
    const v = rows[j][fld];
    if (v == null || v === 0) continue;
    n++; sum += v; cum += v;
    if (cum > peak) peak = cum;
    const dd = peak - cum;
    if (dd > mdd) mdd = dd;
  }
  if (n < p.minTrades) return { neutral: true, mar: 0 };
  return { neutral: false, mar: mdd > 0 ? sum / mdd : (sum > 0 ? p.marCap : 0) };
}

async function computeTiltLine(env, todayISO) {
  const raw = await env.SIGNAL_KV.get('history_data');
  if (!raw) return null;
  let rows;
  try { rows = JSON.parse(raw); } catch (_) { return null; }
  if (!Array.isArray(rows) || rows.length < 20) return null;
  rows = rows.filter(r => r.date && r.date < todayISO).sort((a, b) => a.date.localeCompare(b.date));
  const i = rows.length, p = TILT_P, nAct = TILT_FIELDS.length, eq = 1 / nAct;
  const raw_ = {}, isNeutral = {};
  let rawSum = 0, nNeutral = 0;
  for (const [, fld] of TILT_FIELDS) {
    const mres = tiltWindowMAR(rows, i, fld, p);
    if (mres.neutral) { isNeutral[fld] = true; nNeutral++; }
    else { raw_[fld] = Math.min(Math.max(mres.mar, 0), p.marCap); rawSum += raw_[fld]; }
  }
  const nNon = nAct - nNeutral;
  const nonNeutralMass = 1 - nNeutral * eq;
  const parts = [];
  for (const [name, fld] of TILT_FIELDS) {
    let wi;
    if (isNeutral[fld])  wi = eq;
    else if (rawSum > 0) wi = nonNeutralMass * (raw_[fld] / rawSum);
    else                 wi = nonNeutralMass / nNon;
    wi = Math.min(Math.max(wi, p.floorM * eq), p.capM * eq);
    parts.push(`${name} ${(wi * nAct).toFixed(1)}x`);
  }
  return `Tilt 60d   │ ${parts.join(' · ')}`;
}

// Morning GEX line (2026-06-10, git-mined validation): the PIN/BREAKOUT
// regime is real — BREAKOUT mornings realized 103-pt avg ranges vs 60 on
// PIN, and the pin-style book bled on BREAKOUT mornings. One line, colored
// by regime, in both morning messages. ADVISORY only.
async function computeGexLine(env) {
  const raw = await env.SIGNAL_KV.get('gex_current');
  if (!raw) return null;
  const g = JSON.parse(raw);
  if (!g || !g.regime || g.totalGex == null) return null;
  const bn = g.totalGex / 1e9;
  const flipTxt = g.flipStrike != null ? `flip ${g.flipStrike}` : 'no flip in range (one-sided)';
  return `GEX        │ ${g.regime} ${bn >= 0 ? '+' : '-'}$${Math.abs(bn).toFixed(1)}B · ${flipTxt}`;
}

// CycleLab shape advisory for the morning message (2026-06-10).
// INFORMATIONAL ONLY. Fetches cyclicality_data.json (the worker itself keeps
// it current via the EOD append) and classifies today\'s 4-week prediction.
async function computeCycleLine(env, etNow) {
  const todayISO = isoDateET(etNow);
  const ck = `cycle_line_${todayISO}`;
  const cached = await env.SIGNAL_KV.get(ck);
  if (cached) return cached === 'none' ? null : cached;
  let line = null;
  try {
    const [r, rb] = await Promise.all([
      fetch('https://raw.githubusercontent.com/rava8989/brave/main/cyclicality_data.json',
        { headers: { 'User-Agent': 'schwab-proxy-worker/1.0' } }),
      fetch('https://raw.githubusercontent.com/rava8989/brave/main/cor1m_contango_bundle.json',
        { headers: { 'User-Agent': 'schwab-proxy-worker/1.0' } }),
    ]);
    if (r.ok) {
      const cyc = await r.json();
      const info = classifyCyclePrediction(cyc.days, etNow);
      // Regime: last bundle day (= yesterday) — today's COR1M may not be
      // captured yet at send time; regimes persist day-to-day. Honest label.
      let group = null;
      if (rb.ok) {
        try {
          const daily = (await rb.json()).daily || [];
          if (daily.length) group = regimeGroup(daily[daily.length - 1].regime);
        } catch (_) {}
      }
      line = dayTypeAdvisoryLine(group, info);
    }
  } catch (e) { console.warn('[cycle-line]', e.message); }
  await env.SIGNAL_KV.put(ck, line || 'none', { expirationTtl: 86400 });
  return line;
}

// ── Vol-flow advisory (2026-06-11) — INFORMATIONAL ONLY ────────────────
// JS port of compute_vix_decomposition.py: yesterday's ~30DTE smile vs the
// day before's splits ΔATM-IV into sticky-strike slide / parallel (real
// repricing) / twist → label. Validated cells live in signal-engine
// VOLFLOW_STATS (M8BF↓ after VOL_BID, Diag↓ after VOL_SUPPLY, both ↑ on
// MIXED — all replicate both halves). Smile source: vix_surface_snap KV
// (15:45 capture), GitHub data/vix_surface/<ym>.json fallback (incl. the
// ThetaData-backfill seed).

function _smileFn(rec) {
  const byK = {};
  for (const r of rec.rows || []) {
    const k = r[0], iv = r[4];                 // [k, right, bid, ask, iv, (delta)]
    if (iv != null && iv > 0) (byK[k] = byK[k] || []).push(iv);
  }
  const pts = Object.entries(byK)
    .map(([k, v]) => [parseFloat(k), v.reduce((a, b) => a + b, 0) / v.length])
    .sort((a, b) => a[0] - b[0]);
  if (pts.length < 4) return null;
  const ks = pts.map(p => p[0]), ivs = pts.map(p => p[1]);
  const fn = x => {
    if (x <= ks[0]) return ivs[0];
    if (x >= ks[ks.length - 1]) return ivs[ivs.length - 1];
    for (let i = 1; i < ks.length; i++) {
      if (x <= ks[i]) {
        const w = (x - ks[i - 1]) / (ks[i] - ks[i - 1]);
        return ivs[i - 1] * (1 - w) + ivs[i] * w;
      }
    }
    return ivs[ivs.length - 1];
  };
  return { fn, lo: ks[0], hi: ks[ks.length - 1] };
}

// Same math/keys as compute_vix_decomposition.py — the page chart reads both.
function computeDecompPair(prevRec, curRec) {
  const f0 = _smileFn(prevRec), f1 = _smileFn(curRec);
  if (!f0 || !f1 || !prevRec.spot || !curRec.spot) return null;
  const s0 = prevRec.spot, s1 = curRec.spot;
  const atm0 = f0.fn(s0), atm1 = f1.fn(s1);
  const dATM = atm1 - atm0;
  const slide = f0.fn(Math.min(Math.max(s1, f0.lo), f0.hi)) - f0.fn(s0);
  const klo = Math.max(f0.lo, f1.lo, s0 * 0.88), khi = Math.min(f0.hi, f1.hi, s0 * 1.06);
  if (khi <= klo) return null;
  let parallel = 0;
  for (let j = 0; j < 9; j++) parallel += f1.fn(klo + (khi - klo) * j / 8) - f0.fn(klo + (khi - klo) * j / 8);
  parallel /= 9;
  const residual = dATM - slide - parallel;
  const putSkew0 = f0.fn(s0 * 0.92) - atm0, putSkew1 = f1.fn(s1 * 0.92) - atm1;
  const callSkew0 = f0.fn(s0 * 1.04) - atm0, callSkew1 = f1.fn(s1 * 1.04) - atm1;
  let label;
  if (Math.abs(slide) >= Math.abs(dATM) * 2 / 3 && Math.abs(parallel) < Math.max(Math.abs(dATM) / 3, 0.15)) label = 'MECHANICAL';
  else if (parallel >= 0.5) label = 'VOL_BID';
  else if (parallel <= -0.5) label = 'VOL_SUPPLY';
  else label = 'MIXED';
  const r2 = x => Math.round(x * 100) / 100;
  return { atm: r2(atm1), spot: s1, dATM: r2(dATM), slide: r2(slide), parallel: r2(parallel),
           residual: r2(residual), put_skew: r2(putSkew1), d_put_skew: r2(putSkew1 - putSkew0),
           call_skew: r2(callSkew1), d_call_skew: r2(callSkew1 - callSkew0), label };
}

// EOD: compute today's decomposition record (today's 15:45 smile vs the
// prior session's) → KV vix_decomp_<date> + upsert data/vix_decomposition.json
// so the page chart self-feeds. Idempotent; best-effort.
async function computeVixDecompDaily(env, etNow) {
  const todayISO = isoDateET(etNow);
  if (etNow.getDay() === 0 || etNow.getDay() === 6 || isHol(etNow)) return { skipped: 'non-trading' };
  if (await env.SIGNAL_KV.get(`vix_decomp_${todayISO}`)) return { skipped: 'done' };
  const curRaw = await env.SIGNAL_KV.get(`vix_surface_snap_${todayISO}`);
  if (!curRaw) return { skipped: 'no surface snap today' };
  const cur = JSON.parse(curRaw);
  // prior session smile: KV walk-back ≤5d, then GitHub monthly file(s)
  let prev = null;
  for (let back = 1; back <= 5 && !prev; back++) {
    const d = new Date(etNow); d.setDate(d.getDate() - back);
    const raw = await env.SIGNAL_KV.get(`vix_surface_snap_${isoDateET(d)}`);
    if (raw) { try { prev = JSON.parse(raw); } catch (_) {} }
  }
  if (!prev) {
    const lo = new Date(etNow); lo.setDate(lo.getDate() - 5);
    const months = [...new Set([isoDateET(lo).slice(0, 7), todayISO.slice(0, 7)])];
    const found = {};
    for (const ym of months) {
      try {
        const r = await fetch(`https://raw.githubusercontent.com/rava8989/brave/main/data/vix_surface/${ym}.json`,
          { headers: { 'User-Agent': 'schwab-proxy-worker/1.0' } });
        if (r.ok) Object.assign(found, await r.json());
      } catch (_) {}
    }
    const loISO = isoDateET(lo);
    const cand = Object.keys(found).filter(d => d < todayISO && d >= loISO).sort();
    if (cand.length) prev = found[cand[cand.length - 1]];
  }
  if (!prev) return { skipped: 'no prior smile within 5d' };
  const rec = computeDecompPair(prev, cur);
  if (!rec) return { skipped: 'smiles too sparse' };
  // GitHub upsert FIRST, then the KV done-guard — a failed PUT must NOT be recorded
  // as "done": doing so blocked BOTH the aux-tick retry and the nightly watchdog heal,
  // permanently dropping that day from the dataset (audit P2 2026-07-06).
  await githubUpsertResearchFile(env, 'data/vix_decomposition.json',
    curF => { curF[todayISO] = rec; return curF; }, `auto: vix decomp ${todayISO} ${rec.label}`);
  await env.SIGNAL_KV.put(`vix_decomp_${todayISO}`, JSON.stringify(rec), { expirationTtl: 90 * 86400 });
  return { ok: true, label: rec.label };
}

// Morning-message line: YESTERDAY's label (KV walk-back ≤5d, GitHub
// data/vix_decomposition.json fallback). Cached per day like computeCycleLine.
async function computeVolFlowLine(env, etNow) {
  const todayISO = isoDateET(etNow);
  const ck = `volflow_line_${todayISO}`;
  const cached = await env.SIGNAL_KV.get(ck);
  if (cached) return cached === 'none' ? null : cached;
  let label = null;
  for (let back = 1; back <= 5 && !label; back++) {
    const d = new Date(etNow); d.setDate(d.getDate() - back);
    const raw = await env.SIGNAL_KV.get(`vix_decomp_${isoDateET(d)}`);
    if (raw) { try { label = JSON.parse(raw).label; } catch (_) {} }
  }
  if (!label) {
    try {
      const r = await fetch('https://raw.githubusercontent.com/rava8989/brave/main/data/vix_decomposition.json',
        { headers: { 'User-Agent': 'schwab-proxy-worker/1.0' } });
      if (r.ok) {
        const dd = await r.json();
        const lo = new Date(etNow); lo.setDate(lo.getDate() - 5);
        const loISO = isoDateET(lo);
        const cand = Object.keys(dd).filter(d => d < todayISO && d >= loISO).sort();
        if (cand.length) label = dd[cand[cand.length - 1]].label;
      }
    } catch (e) { console.warn('[volflow-line]', e.message); }
  }
  const line = volFlowAdvisoryLine(label);
  await env.SIGNAL_KV.put(ck, line || 'none', { expirationTtl: 86400 });
  return line;
}

// ── M8BF service-WR line (2026-06-12) — INFORMATIONAL ONLY ─────────────
// Yesterday's whole-day service win rate (history m8bfWR) + cold-streak
// context. Trailing AVERAGES tested flat — only yesterday's value + the
// trailing-5 extreme flag are shown. Cached per day.
async function computeM8bfWrLine(env, etNow) {
  const todayISO = isoDateET(etNow);
  const ck = `m8bfwr_line_${todayISO}`;
  const cached = await env.SIGNAL_KV.get(ck);
  if (cached) return cached === 'none' ? null : cached;
  let line = null;
  try {
    const raw = await env.SIGNAL_KV.get('history_data');
    if (raw) {
      const rows = JSON.parse(raw).filter(r => r.date && r.date < todayISO && r.m8bfWR != null)
        .sort((a, b) => a.date.localeCompare(b.date));
      if (rows.length >= 6) {
        const yWR = rows[rows.length - 1].m8bfWR;
        const t5 = rows.slice(-5).reduce((s, r) => s + r.m8bfWR, 0) / 5;
        // percentile of current trailing-5 vs all history
        const all5 = [];
        for (let i = 5; i <= rows.length; i++)
          all5.push(rows.slice(i - 5, i).reduce((s, r) => s + r.m8bfWR, 0) / 5);
        const pct = 100 * all5.filter(v => v <= t5).length / all5.length;
        line = m8bfWrAdvisoryLine(yWR, t5, pct);
      }
    }
  } catch (e) { console.warn('[m8bfwr-line]', e.message); }
  await env.SIGNAL_KV.put(ck, line || 'none', { expirationTtl: 86400 });
  return line;
}

// ── Nightly data-completeness watchdog (2026-06-12, user-approved) ─────
// Verifies TODAY landed in every self-feeding dataset; auto-heals via the
// idempotent jobs; Discord only when something needed fixing. Own tick
// (18:35-18:50) so it can never be starved by other chains (lessons P17).
async function dataCompletenessCheck(env, etNow) {
  const todayISO = isoDateET(etNow);
  // Before ~16:30 ET today's data legitimately doesn't exist yet — checks
  // would all false-alarm. (The scheduled run is 18:35.)
  if (etNow.getHours() < 16 || (etNow.getHours() === 16 && etNow.getMinutes() < 30))
    return { date: todayISO, skipped: 'before EOD — nothing to verify yet' };
  const ym = todayISO.slice(0, 7);
  const gh = { headers: { 'User-Agent': 'schwab-proxy-worker/1.0' } };
  const J = async (u) => { const r = await fetch(u, gh); return r.ok ? r.json() : null; };
  const healed = [], failed = [], ok = [];

  const checks = [
    ['cyclicality SPX', async () => {
      const j = await J('https://raw.githubusercontent.com/rava8989/brave/main/cyclicality_data.json');
      return !!(j && j.days.some(d => d.d === todayISO));
    }, () => appendCyclicalityDays(env)],
    ['cyclicality NDX', async () => {
      const j = await J('https://raw.githubusercontent.com/rava8989/brave/main/cyclicality_ndx.json');
      return !!(j && j.days.some(d => d.d === todayISO));
    }, () => appendCyclicalityDays(env, { symbol: '%24NDX', file: 'cyclicality_ndx.json' })],
    ['vix decomposition', async () => {
      const j = await J('https://raw.githubusercontent.com/rava8989/brave/main/data/vix_decomposition.json');
      return !!(j && j[todayISO]);
    }, () => computeVixDecompDaily(env, etNow)],
    ['research persists', async () => {
      // representative pair: fly marks (captured every RTH day) + vixeq
      const fm = await J(`https://raw.githubusercontent.com/rava8989/brave/main/data/fly_marks/${ym}.json`);
      const ve = await J('https://raw.githubusercontent.com/rava8989/brave/main/data/vixeq_daily.json');
      const kvFm = await env.SIGNAL_KV.get(`fly_marks_${todayISO}`);
      const fmOk = !kvFm || !!(fm && fm[todayISO]);   // no capture → nothing to persist
      return fmOk && !!(ve && ve[todayISO]);
    }, () => persistResearchArtifacts(env, etNow)],
    ['EOD history fields', async () => {
      const raw = await env.SIGNAL_KV.get('history_data');
      if (!raw) return false;
      const row = JSON.parse(raw).find(r => r.date === todayISO);
      return !!(row && row.vixClose != null);
    }, null],   // settle has its own retry path — report only
  ];
  if (etNow.getDay() === 5) checks.push(['COT weekly', async () => {
    const j = await J('https://raw.githubusercontent.com/rava8989/brave/main/data/cot_currencies.json');
    if (!j) return false;
    const last = j.series.EUR[j.series.EUR.length - 1][0];
    return (new Date(todayISO) - new Date(last)) / 86400000 <= 5;
  }, () => cotWeeklyRefresh(env)]);

  for (const [name, check, heal] of checks) {
    try {
      if (await check()) { ok.push(name); continue; }
      if (!heal) { failed.push(name + ' (no auto-heal)'); continue; }
      await heal();
      if (await check()) healed.push(name);
      else failed.push(name);
    } catch (e) { failed.push(`${name} (${e.message.slice(0, 60)})`); }
  }
  const result = { date: todayISO, ok: ok.length, healed, failed };
  if (healed.length || failed.length) {
    try {
      const dcRaw = await env.SIGNAL_KV.get('discord_config');
      if (dcRaw) {
        const dc = JSON.parse(dcRaw);
        if (dc.channelId) await sendDiscordDM(env, dc.channelId,
          `🩺 **Data watchdog** (${todayISO})` +
          (healed.length ? `\n✅ auto-healed: ${healed.join(', ')}` : '') +
          (failed.length ? `\n❌ NEEDS ATTENTION: ${failed.join(', ')}` : ''),
          dc.proxyUrl);
      }
    } catch (_) {}
  }
  try { await logEvent(env, failed.length ? 'error' : 'info', 'watchdog', JSON.stringify(result).slice(0, 200), {}); } catch (_) {}
  return result;
}

// ── Weekly digest (2026-06-12, user-approved) — Sundays 18:00 ET ───────
async function weeklyDigest(env) {
  const etNow = toET(new Date());
  const todayISO = isoDateET(etNow);
  const weekAgo = new Date(etNow); weekAgo.setDate(weekAgo.getDate() - 7);
  const fromISO = isoDateET(weekAgo);
  const lines = [`📒 **Weekly digest — week ending ${todayISO}**`];
  // Durable backup of the accumulating signed-flow dataset. The live `signed_flow_daily`
  // key has no TTL (so it persists), but it's the one structural dataset not otherwise
  // backed up — snapshot it weekly to a dated, 120-day-TTL key so it's recoverable if the
  // live key is ever wiped. Best-effort; never blocks the digest. (2026-06-28)
  try {
    const sfd = await env.SIGNAL_KV.get('signed_flow_daily');
    if (sfd) await env.SIGNAL_KV.put(`signed_flow_daily_bak_${todayISO}`, sfd, { expirationTtl: 120 * 86400 });
  } catch (_) {}
  try {
    const rows = JSON.parse(await env.SIGNAL_KV.get('history_data') || '[]')
      .filter(r => r.date && r.date > fromISO && r.date <= todayISO);
    const F = [['M8BF', 'm8bfPL'], ['Strad', 'stradPL'], ['BOBF', 'bobfPL'], ['GXBF', 'gxbfPL'], ['Diag', 'diagPL'], ['Tail', 'tailPL']];
    let tot = 0;
    const parts = [];
    for (const [nm, f] of F) {
      const v = rows.map(r => r[f]).filter(x => x != null && x !== 0);
      if (!v.length) continue;
      const s = v.reduce((a, b) => a + b, 0); tot += s;
      parts.push(`${nm} ${s >= 0 ? '+' : ''}$${Math.round(s).toLocaleString()} (${v.length})`);
    }
    lines.push(`P/L: ${parts.join(' · ') || 'no trades'} → **${tot >= 0 ? '+' : ''}$${Math.round(tot).toLocaleString()}**`);
  } catch (_) {}
  try {
    const led = await (await fetch('https://raw.githubusercontent.com/rava8989/brave/main/data/advisory_scorecard.json',
      { headers: { 'User-Agent': 'schwab-proxy-worker/1.0' } })).json();
    let g = [0, 0], d = [0, 0], v = [0, 0];
    for (const [dt, e] of Object.entries(led)) {
      if (dt <= fromISO || dt > todayISO) continue;
      if (e.gex) { g[1]++; if (e.gex.hit) g[0]++; }
      for (const c of (e.daytype?.cells || [])) { d[1]++; if (c.hit) d[0]++; }
      for (const c of (e.volflow?.cells || [])) { v[1]++; if (c.hit) v[0]++; }
    }
    lines.push(`Scorecard wk: GEX ${g[0]}/${g[1]} · Day-type ${d[0]}/${d[1]} · Vol-flow ${v[0]}/${v[1]}`);
  } catch (_) {}
  try {
    const nxt = [];
    const horizon = new Date(etNow); horizon.setDate(horizon.getDate() + 7);
    for (const [label, arr] of [['FED', fedSch], ['CPI', cpiSch], ['OPEX', opexSch]]) {
      for (const s of arr) {
        const d2 = parseLong(s);
        if (d2 && d2 > etNow && d2 <= horizon) nxt.push(`${label} ${isoDateET(d2).slice(5)}`);
      }
    }
    if (nxt.length) lines.push(`Next week: ${nxt.join(' · ')}`);
  } catch (_) {}
  const dcRaw = await env.SIGNAL_KV.get('discord_config');
  if (dcRaw) {
    const dc = JSON.parse(dcRaw);
    if (dc.channelId) await sendDiscordDM(env, dc.channelId, lines.join('\n'), dc.proxyUrl);
  }
  return { ok: true, lines: lines.length };
}

// ── Daily total-risk cap (2026-06-09) ──────────────────────────────────
// On multi-strategy days, Straddle + BOBF + GXBF + Diagonal can all be live
// at once and nothing checked COMBINED max-loss against the account. Each
// automated open now calls enforceRiskCap() with its own max theoretical
// loss; if existing open exposure + the new trade would exceed the cap,
// the open is refused (fail-closed: a blocked good trade costs opportunity,
// an unblocked stack can cost the account).
//
// Config: KV key 'risk_config' = { "enabled": true, "maxOpenRiskUsd": 8000 }
//   — change the cap WITHOUT redeploying:
//   npx wrangler kv key put --namespace-id=<NS> risk_config '{"enabled":true,"maxOpenRiskUsd":10000}' --remote
// Default: enabled, $8,000 (≈25% of a $31k account).
// M8BF is signal-only (user trades manually) — not gated here.
// mode: 'warn' = trade still fires, Discord warning only (user choice
// 2026-06-10); 'block' = refuse the trade. Both KV-tunable, no redeploy.
const RISK_CAP_DEFAULTS = { enabled: true, maxOpenRiskUsd: 8000, mode: 'warn' };

// Max theoretical loss per open position, in dollars:
//   Straddle / BOBF / GXBF (debit verticals/flies): debit × 100 × contracts
//   Diagonal: (debit + width) × 100 × contracts — spread can go inverted in
//   a crash through both strikes (see index.html sizing notes).
async function computeOpenRiskExposureUsd(env, todayISO) {
  const [stradRaw, bobfRaw, gxbfRaw, diagRaw] = await Promise.all([
    env.SIGNAL_KV.get('straddle_open_trade'),
    env.SIGNAL_KV.get('bobf_open_trade'),
    env.SIGNAL_KV.get('gxbf_open_trade'),
    env.SIGNAL_KV.get('diagonal_open_trade'),
  ]);
  const parts = {};
  const safe = raw => { try { return raw ? JSON.parse(raw) : null; } catch { return null; } };

  const strad = safe(stradRaw);
  if (strad && strad.openDate === todayISO && strad.status !== 'closed') {
    const debit = strad.fillDebit ?? strad.entryDebit;
    if (debit > 0) parts.straddle = Math.round(debit * 100 * (strad.contracts || 1));
  }
  const bobf = safe(bobfRaw);
  if (bobf && bobf.openDate === todayISO && bobf.status !== 'closed') {
    const debit = bobf.fillDebit ?? bobf.entryDebit;
    if (debit > 0) parts.bobf = Math.round(debit * 100 * (bobf.contracts || 1));
  }
  const gxbf = safe(gxbfRaw);
  if (gxbf && gxbf.openDate === todayISO && gxbf.status !== 'closed') {
    if (gxbf.netDebit > 0) parts.gxbf = Math.round(gxbf.netDebit * 100 * (gxbf.contracts || 1));
  }
  const diag = safe(diagRaw);
  if (diag && !diag.closeDate && diag.status !== 'closed' && diag.entryDebit != null) {
    const width = (diag.shortStrike && diag.longStrike) ? (diag.shortStrike - diag.longStrike) : 0;
    parts.diagonal = Math.round((diag.entryDebit + width) * 100 * (diag.contracts || 1));
  }
  const totalUsd = Object.values(parts).reduce((s, v) => s + v, 0);
  return { totalUsd, parts };
}

// Returns { ok: true } or { ok: false, reason } — and on a block, fires a
// once-per-strategy-per-day Discord note so refused trades are never silent.
async function enforceRiskCap(env, etNow, strategy, newTradeMaxLossUsd) {
  try {
    const cfgRaw = await env.SIGNAL_KV.get('risk_config');
    const cfg = { ...RISK_CAP_DEFAULTS, ...(cfgRaw ? JSON.parse(cfgRaw) : {}) };
    if (!cfg.enabled) return { ok: true };
    const todayISO = isoDateET(etNow);
    const { totalUsd, parts } = await computeOpenRiskExposureUsd(env, todayISO);
    const projected = totalUsd + Math.max(0, Math.round(newTradeMaxLossUsd || 0));
    if (projected <= cfg.maxOpenRiskUsd) return { ok: true };

    const warnOnly = cfg.mode !== 'block';   // default 'warn' — trade proceeds
    const reason = `open risk $${totalUsd.toLocaleString()} (${JSON.stringify(parts)}) + new ${strategy} $${Math.round(newTradeMaxLossUsd).toLocaleString()} = $${projected.toLocaleString()} > cap $${cfg.maxOpenRiskUsd.toLocaleString()}`;
    await logEvent(env, 'warn', 'risk-cap', `${strategy} ${warnOnly ? 'WARNING (trade proceeds)' : 'open BLOCKED'}`, { strategy, totalUsd, parts, newTradeMaxLossUsd, cap: cfg.maxOpenRiskUsd, mode: cfg.mode });
    // Discord note, once per strategy per day
    const alertKey = `risk_cap_alert_${strategy}_${todayISO}`;
    if (await claimSendSlot(env, alertKey)) {
      try {
        const dcRaw = await env.SIGNAL_KV.get('discord_config');
        if (dcRaw) {
          const dc = JSON.parse(dcRaw);
          if (dc.channelId) {
            // P34 sweep (2026-08-03): confirmed delivery before the marker —
            // a blocked/warned strategy the owner never hears about is a
            // silent trading change.
            const rR = await sendDiscordDM(env, dc.channelId,
              warnOnly
                ? `⚠️ **Risk warning — ${strategy.toUpperCase()} still traded** — ${reason}.\nCombined open max-loss is past your comfort line. Consider trimming. (Switch to hard blocking: KV \`risk_config\` → \`{"mode":"block"}\`.)`
                : `🛑 **Risk cap blocked ${strategy.toUpperCase()}** — ${reason}.\nRaise the cap or set \`{"mode":"warn"}\` via KV \`risk_config\` if intentional.`,
              dc.proxyUrl);
            if (rR && rR.ok) await env.SIGNAL_KV.put(alertKey, 'sent', { expirationTtl: 86400 });
            else console.warn('[risk-cap-alert] undelivered — claim expires for retry');
          }
        }
      } catch (_) { /* notify is best-effort */ }
    }
    if (warnOnly) return { ok: true, warned: true, reason };
    return { ok: false, reason };
  } catch (e) {
    // Fail-OPEN on infrastructure errors: a broken risk check must not
    // silently halt all trading. The error is logged for follow-up.
    console.warn('[risk-cap] check failed (allowing trade):', e.message || e);
    try { await logEvent(env, 'error', 'risk-cap', 'check failed — trade allowed', { msg: e.message }); } catch (_) {}
    return { ok: true, degraded: true };
  }
}

// ── Discord DM send (consolidates the standalone discord-proxy worker) ──
// Priority order, first available wins:
//   1. env.DISCORD_TOKEN (direct in-worker send — preferred, one less worker)
//   2. env.DISCORD_PROXY service binding (legacy; kept until standalone retired)
//   3. proxyUrl arg (legacy public URL)
// Returns {ok, status?, data?, error?}. Never throws — caller decides on retry.
async function sendDiscordDM(env, userId, message, proxyUrl = null) {
  if (!userId || !message) return { ok: false, error: 'missing userId/message' };

  // Detect whether message is an embed object (Option E format) or a string.
  const isEmbed = typeof message === 'object' && message !== null && !Array.isArray(message);

  // Path 1: native — DM via DISCORD_TOKEN directly (supports embeds)
  if (env.DISCORD_TOKEN) {
    try {
      const dmResp = await fetch('https://discord.com/api/v10/users/@me/channels', {
        method: 'POST',
        headers: { 'Authorization': `Bot ${env.DISCORD_TOKEN}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({ recipient_id: userId }),
      });
      if (!dmResp.ok) {
        const txt = await dmResp.text();
        return { ok: false, status: dmResp.status, error: `DM channel ${dmResp.status}: ${txt.slice(0, 200)}` };
      }
      const dm = await dmResp.json();
      const payload = isEmbed
        ? { embeds: [message] }
        : { content: String(message).slice(0, 2000) };
      const msgResp = await fetch(`https://discord.com/api/v10/channels/${dm.id}/messages`, {
        method: 'POST',
        headers: { 'Authorization': `Bot ${env.DISCORD_TOKEN}`, 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      });
      let data;
      try { data = await msgResp.json(); } catch { data = { raw: 'non-json' }; }
      if (!msgResp.ok) return { ok: false, status: msgResp.status, data, error: `send ${msgResp.status}` };
      return { ok: true, status: 200, data, source: 'native' };
    } catch (e) {
      return { ok: false, error: 'native: ' + e.message };
    }
  }

  // Path 2/3 (legacy proxies): downgrade embed → plain text since they don't pass embeds through.
  const textMessage = isEmbed ? embedToText(message) : String(message);

  // Path 2: service binding (legacy discord-proxy worker)
  if (env.DISCORD_PROXY) {
    try {
      const hdrs = { 'Content-Type': 'application/json' };
      if (env.PROXY_SECRET) hdrs['Authorization'] = `Bearer ${env.PROXY_SECRET}`;
      const r = await env.DISCORD_PROXY.fetch(new Request('https://dummy/', {
        method: 'POST', headers: hdrs,
        body: JSON.stringify({ userId, message: textMessage.slice(0, 2000) }),
      }));
      let data; try { data = await r.json(); } catch { data = { raw: 'non-json' }; }
      return { ok: r.ok, status: r.status, data, source: 'service-binding',
               ...(r.ok ? {} : { error: `service ${r.status}` }) };
    } catch (e) {
      return { ok: false, error: 'service: ' + e.message };
    }
  }

  // Path 3: HTTP proxyUrl
  if (proxyUrl && proxyUrl.startsWith('https://')) {
    try {
      const hdrs = { 'Content-Type': 'application/json' };
      if (env.PROXY_SECRET) hdrs['Authorization'] = `Bearer ${env.PROXY_SECRET}`;
      const r = await fetch(proxyUrl, {
        method: 'POST', headers: hdrs,
        body: JSON.stringify({ userId, message: textMessage.slice(0, 2000) }),
      });
      let data; try { data = await r.json(); } catch { data = { raw: 'non-json' }; }
      return { ok: r.ok, status: r.status, data, source: 'http-url',
               ...(r.ok ? {} : { error: `http ${r.status}` }) };
    } catch (e) {
      return { ok: false, error: 'http: ' + e.message };
    }
  }

  return { ok: false, error: 'no Discord transport available (no DISCORD_TOKEN/DISCORD_PROXY/proxyUrl)' };
}

// ── Signal subscribers (2026-06-15) — extra Discord user IDs that get a DM
// copy of each morning signal. KV `signal_subscribers` = [{id,label,paused}].
// Discord rule: the bot can only DM a user who shares a server with it AND
// allows server-member DMs; otherwise the API 403s (surfaced to the UI).
async function getSubscribers(env) {
  try { const raw = await env.SIGNAL_KV.get('signal_subscribers'); return raw ? JSON.parse(raw) : []; }
  catch { return []; }
}
// Post a signal to the broadcast channel via its Discord webhook (KV `signals_webhook_url`).
// Best-effort — never breaks the fanout. The channel is "another recipient" alongside the DMs,
// with no DM cap / anti-spam risk. URL lives in KV (NOT in code — this repo is public).
async function postSignalsChannel(env, message) {
  try {
    const url = await env.SIGNAL_KV.get('signals_webhook_url');
    if (!url) return { ok: false, skipped: true };
    const r = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ content: String(message).slice(0, 2000), allowed_mentions: { parse: [] } }),
    });
    return { ok: r.ok, status: r.status };
  } catch (e) { return { ok: false, error: e.message }; }
}

// Compliance disclaimer appended to EVERY subscriber-facing trade message (user
// request 2026-07-07). `-#` renders as small subtext in Discord (channel + DM).
const FANOUT_DISCLAIMER = '\n-# ⚠️ Not financial advice — for educational purposes only. Trade at your own risk.';

// ── M8BF Anchor Filter (2026-07-23, renamed 07-26): verdict for the NEXT session from the durable
// 10:30 0DTE GEX series. rank = percentile of TODAY's value among up to 120
// entries strictly before today (causal); SKIP when rank < 0.20 with >= 20 obs.
async function computeGexGateVerdict(env) {
  const serRaw = await env.SIGNAL_KV.get('gex1030_series_v1');
  if (!serRaw) return null;
  const ser = JSON.parse(serRaw);
  const todayISO = isoDateET(toET(new Date()));
  const gex = ser[todayISO];
  if (gex == null) return { verdict: null, reason: 'no 10:30 GEX captured today' };
  const prior = Object.keys(ser).filter(d => d < todayISO).sort().slice(-120).map(d => ser[d]);
  if (prior.length < 20) return { verdict: null, reason: `warmup ${prior.length}/20` };
  const rank = prior.filter(x => x <= gex).length / prior.length;
  return { verdict: rank < ANCHOR_THRESHOLD ? 'SKIP' : 'GO', gex, rank, n: prior.length, date: todayISO };
}

// Gate verdict FOR a given session date: ref = last series entry strictly
// before dateISO, ranked among its own <=120 predecessors. Deterministic, and
// guarded to the go-live date so recovery/backfill paths can never retro-gate
// history recorded under the old rules.
const GEXGATE_LIVE_FROM = '2026-07-24';
// Anchor Filter threshold (2026-07-26): skip when the previous session's 10:30
// reading ranks below this percentile of its trailing 120. Swept 6-35% and
// picked on TRAIN (2024-25) by t-stat and return/drawdown, then graded blind on
// 2026: 17% carries the same $/day as the old 20% with 15% (train) / 24% (test)
// LESS drawdown. 12-20% is a plateau — 17% is the best of an indistinguishable
// band, chosen for the smoother equity curve, not a higher return.
const ANCHOR_THRESHOLD = 0.17;
// Full gate evaluation for a session date: {skip, rank}. rank is the prev-day
// reading's percentile (0..1) of its trailing 120 — null when the guard/staleness
// rules fail open. skip = rank < 0.20 (never true when rank is null).
async function gexGateEval(env, dateISO) {
  try {
    if (!dateISO || dateISO < GEXGATE_LIVE_FROM) return { skip: false, rank: null };
    const serRaw = await env.SIGNAL_KV.get('gex1030_series_v1');
    if (!serRaw) return { skip: false, rank: null };
    const ser = JSON.parse(serRaw);
    const prior = Object.keys(ser).filter(d => d < dateISO).sort();
    if (prior.length < 21) return { skip: false, rank: null };
    // Staleness bound (audit [0]): the rule is PREV-SESSION GEX. If the last
    // series entry is older than the previous trading day (capture failed),
    // FAIL OPEN to GO — matches the 17:05 warning DM's promise.
    const refDate = prior[prior.length - 1];
    let pd = new Date(dateISO + 'T12:00:00Z');
    do { pd = new Date(pd.getTime() - 86400000); }
    while (pd.getUTCDay() === 0 || pd.getUTCDay() === 6 || isHol(toET(new Date(pd.getTime() + 64800000))));
    const prevTradeISO = pd.toISOString().slice(0, 10);
    if (refDate !== prevTradeISO) { console.warn('[gexgate] stale ref', refDate, 'for', dateISO, '— fail open'); return { skip: false, rank: null }; }
    const ref = ser[refDate];
    const base = prior.slice(0, -1).slice(-120).map(d => ser[d]);
    const rank = base.filter(x => x <= ref).length / base.length;
    return { skip: rank < ANCHOR_THRESHOLD, rank };
  } catch (e) { console.warn('[gexgate-eval]', e.message); return { skip: false, rank: null }; }
}
async function gexGateSkipFor(env, dateISO) { return (await gexGateEval(env, dateISO)).skip; }
// Fat-gamma tier (info-only indication, owner 2026-07-24): prev-day rank in
// [60, 90) — M8BF's historically strongest regime on the gated stream (72% WR
// vs 66% on other traded days; the 90+ extreme is deliberately excluded — it is
// the weakest positive bucket). Returns the integer percentile or null.
// DISPLAY ONLY: never changes trading behavior, sizing, or the bot.
function gexFatTierP(rank) {
  return (rank != null && rank >= 0.60 && rank < 0.90) ? Math.round(rank * 100) : null;
}
// Post-process a calculateSignal() result: on gate-skip days M8BF reads NO and,
// when M8BF held the primary rec, the whole trade path is neutralized so the
// window sender / bot / card all see a no-trade day. Other strategies untouched.
function applyGexGateToSignal(signal, skip, rank) {
  if (!signal) return signal;
  // Fat-gamma tier flag (info only) — set on GO days when the prev-day rank
  // lands in [60, 90). Consumed by the morning card, the signal embed, and the
  // /link-notify M8BF relay footer. Never touches theme/rec/trading.
  if (!skip) { const p = gexFatTierP(rank); if (p != null) signal.gexFatP = p; return signal; }
  signal.gexGateSkip = true;                 // explicit flag: EOD/no-trade accounting
  const GATE_TXT = 'No M8BF (Anchor Filter — unanchored)';
  if (!/^No\s/i.test(String(signal.m8bfText || '').trim())) signal.m8bfText = GATE_TXT;
  if (signal.theme === 'm8bf') {
    signal.theme = 'block';
    signal.rec = GATE_TXT;
    signal.badge = 'NO TRADE';
    signal.entryT = '';
    signal.blockT = 'anchor';
    signal.blockD = 'prev-session 10:30 anchor below p17 of trailing 120d';
    signal.crossed = true;
    signal.strikeInfo = null;
  }
  signal.m8bfStrikeInfo = null;
  return signal;
}

async function fanoutSubscribers(env, message) {
  // Single chokepoint: append the disclaimer here so NO trade path (tail, skipper
  // trades, any future caller) can reach the broadcast channel OR a subscriber DM
  // without it. Idempotent — won't double-add if a caller already included it.
  const base = String(message);
  const msg = base.includes('Not financial advice') ? base : base + FANOUT_DISCLAIMER;
  // Broadcast channel first (one post, no DM caps), then the per-user DM list.
  try { await postSignalsChannel(env, msg); } catch (_) {}
  const subs = (await getSubscribers(env)).filter(s => s && s.id && !s.paused);
  const out = [];
  for (const s of subs) {
    try {
      const r = await sendDiscordDM(env, s.id, msg);
      out.push({ id: s.id, ok: !!r.ok, status: r.status, error: r.error });
    } catch (e) { out.push({ id: s.id, ok: false, error: e.message }); }
  }
  if (out.length) { try { await logEvent(env, 'info', 'fanout', `signal → ${out.filter(x=>x.ok).length}/${out.length} subscribers`, {}); } catch {} }
  return out;
}

// ── Centralized event logger ──
// Appends to daily_log_<isoDateET> KV. Cap 200 newest entries per day, 7-day
// TTL. FIRE-AND-FORGET: failures are swallowed so logging never breaks the
// calling code path. Read back via GET /logs?date=YYYY-MM-DD (auth-required).
// Use sparingly — log only signal-grade events (signal sent/failed, trade
// open/close, phantom cleanup, auto-recovery, alerts), not every tick.
async function logEvent(env, level, tag, msg, data = null) {
  try {
    const now = new Date();
    const etNow = (typeof toET === 'function') ? toET(now) : now;
    const dateISO = (typeof isoDateET === 'function')
      ? isoDateET(etNow)
      : `${etNow.getFullYear()}-${String(etNow.getMonth()+1).padStart(2,'0')}-${String(etNow.getDate()).padStart(2,'0')}`;
    const key = `daily_log_${dateISO}`;
    const entry = {
      ts: now.toISOString(),
      etTime: `${String(etNow.getHours()).padStart(2,'0')}:${String(etNow.getMinutes()).padStart(2,'0')}:${String(etNow.getSeconds()).padStart(2,'0')}`,
      level, tag, msg,
      ...(data ? { data } : {}),
    };
    let arr = [];
    try {
      const raw = await env.SIGNAL_KV.get(key);
      arr = raw ? JSON.parse(raw) : [];
      if (!Array.isArray(arr)) arr = [];
    } catch { arr = []; }
    arr.unshift(entry);
    if (arr.length > 200) arr = arr.slice(0, 200);
    await env.SIGNAL_KV.put(key, JSON.stringify(arr), { expirationTtl: 7 * 86400 });
  } catch (e) {
    // Never let logging break the main flow
    console.warn('[logEvent] swallowed:', e.message);
  }
}

function checkRateLimit(request) {
  const ip = request.headers.get('CF-Connecting-IP') || request.headers.get('X-Forwarded-For') || 'unknown';
  const now = Date.now();

  // Clean up expired entries
  for (const [key, val] of rateLimitMap) {
    if (val.reset < now) rateLimitMap.delete(key);
  }

  const entry = rateLimitMap.get(ip);
  if (!entry || entry.reset < now) {
    rateLimitMap.set(ip, { count: 1, reset: now + 60_000 });
    return false; // not rate limited
  }

  entry.count += 1;
  if (entry.count > 60) return true; // rate limited
  return false;
}

// ════════════════════════════════════════════════════════════════════
// SIGNAL ENGINE — import from shared module (single source of truth).
// NEVER inline signal logic here. ALL signal rules live in signal-engine.js.
// Edit rules THERE so browser + worker + history page stay in sync.
// ════════════════════════════════════════════════════════════════════

import {
  cpiSch, fedSch, opexSch, holidays, vixSch, earningsSchedule, T,
  toET, dateLong, todayLong,
  isWkend, isHol, isTrade, nextTrade, prevTrade,
  isTodayAfter, isTodayBefore, parseLong, schedInMonth,
  isVixAfterOpexDay, isPostOpexMon, isLastTradeMo, isEomN, isFirstTradeMo, isFirstTradeMon,
  m8Sched, m8Msg, ordinal, wdName, tradeWdLabel,
  isEarningsDay, isNonAmznTslaEarningsDay, isDayAfterAnyEarnings,
  calculateSignal, computeDiagonalSignal, computeVixPct20d,
  classifyCyclePrediction, cycleAdvisoryLine, regimeGroup, dayTypeAdvisoryLine,
  volFlowAdvisoryLine, m8bfWrAdvisoryLine, computeSkewReading,
} from './signal-engine.js';
import { Resvg, initWasm } from '@resvg/resvg-wasm';
import resvgWasm from '@resvg/resvg-wasm/index_bg.wasm';


// ════════════════════════════════════════════════════════════════════
// TAIL HEDGE — fetch today's signal from the default (Balanced) preset in the bundle.
// Bundle is auto-refreshed daily by scripts/refresh_tail_hedge.sh.
// Returns a short status string for the Discord message.
//
// 3 possible states:
//   TRADE today @ 9:45 — buy 0DTE SPXW put delta -0.10, hold to 4 PM
//   SKIP today (VVIX X.XX ≥ 110, puts too expensive). Stay TRIGGERED.
//   No Tail Hedge today (COR1M X.XX, need cross below 7.75)
// ── TAIL HEDGE RETIRED 2026-08-03 (owner order) ────────────────────────────
// Removed from the live book AND the Σ3 service. Reason (full record in
// tasks/TAIL_HEDGE_ARCHIVE.md): the edge is real but concentrated in 3 days of
// 86; the trigger fires ~8x/month at ~-$342 per losing day = ~-$2,700/month of
// carry, which at the owner's account size (~$26k) is ~10%/month — the account
// would be exhausted before a payoff arrived. NOT a defect in the strategy: an
// account-size decision. Bring it back at ~$90k (carry <= 3%/month), and FIRST,
// before Straddle/Diagonal — it is the only convex sleeve.
// Implementation: a flag, not a deletion. Every code path below is preserved so
// resurrection is `TAIL_RETIRED = false` plus re-enabling the site sections.
// Historical tailPL in history_data.json is PRESERVED — retirement is forward-only.
// Before restarting, re-examine the COR1M trigger frequency (54 fires in 7
// months is not tail-event frequency).
const TAIL_RETIRED = true;

let _tailHedgeCache = { value: null, fetchedAt: 0 };
async function getTailHedgeStatusLine(env = null) {
  if (TAIL_RETIRED) return null;              // no status line anywhere
  // 5-minute in-worker cache — values change slowly so this is plenty fresh.
  const now = Date.now();
  if (_tailHedgeCache.value && (now - _tailHedgeCache.fetchedAt) < 5*60*1000) {
    return _tailHedgeCache.value;
  }
  try {
    // ET date (2026-06-09 fix: was UTC toISOString — wrong date in evenings).
    const etNow = toET(new Date());
    const todayISO = isoDateET(etNow);

    // CLOUD-FIRST today's values (worker's own Schwab capture); bundle is
    // the fallback + the authority for trigger RESOLUTION (profit exits are
    // only known to the backtest, rebuilt whenever the user's Mac is on).
    let cor1m = null, vvix = null;
    if (env) {
      try {
        const kvOpen = await env.SIGNAL_KV.get(`cor1m_open_${todayISO}`);
        if (kvOpen) { const o = JSON.parse(kvOpen); cor1m = o.cor1m ?? null; vvix = o.vvix ?? null; }
      } catch (_) {}
    }

    const r = await fetch('https://raw.githubusercontent.com/rava8989/brave/main/cor1m_contango_bundle.json',
      { cf: { cacheTtl: 300, cacheEverything: true } });
    let bundleTriggered = false, bundleLastDay = null, bundleLastTriggerDate = null;
    if (r.ok) {
      const b = await r.json();
      const dailyToday = (b.daily || []).find(d => d.date === todayISO);
      if (cor1m == null) cor1m = dailyToday?.cor1m ?? null;
      if (vvix == null)  vvix  = dailyToday?.vvix ?? null;
      const daily = b.daily || [];
      // Last day the bundle actually has DATA — NOT the last padded row. The
      // builder emits empty rows out to date.today(); using the final (often
      // data-less) row would push bundleLastDay past a live cloud cross and
      // silently disarm the tail. 2026-07-09 bug: a mid-campaign bundle rebuild
      // padded daily to 07-08, so the 06-22 cloud trigger (st.since) looked
      // OLDER than the bundle → cloudTriggered flipped false → "No trade today"
      // while the campaign was in fact still armed. Anchor to real data instead.
      const withData = daily.filter(d => d && d.cor1m != null);
      bundleLastDay = withData.length ? withData[withData.length - 1].date
                    : (daily.length ? daily[daily.length - 1].date : null);
      const defId = b.default_preset || 'balanced';
      const triggers = b.preset_results?.[defId]?.triggers || [];
      const last = triggers[triggers.length - 1];
      bundleTriggered = !!(last && last.exit_reason !== 'profitable');
      bundleLastTriggerDate = last?.trigger_date ?? null;   // START of the bundle's latest episode
    }

    // Cloud-detected cross SINCE the bundle's last day also counts as
    // triggered (covers PC-off stretches where the bundle goes stale).
    // A cloud RESOLVED (first profitable day, set by settleTailEOD) newer than
    // the bundle STOPS the campaign even if a stale bundle still shows active.
    let cloudTriggered = false, cloudResolved = false;
    if (env) {
      try {
        const stRaw = await env.SIGNAL_KV.get('tail_trigger_state');
        if (stRaw) {
          const st = JSON.parse(stRaw);
          // Compare the cloud episode against the bundle's last ANALYSED episode,
          // not its last DATA day (2026-07-27 bug): extending the bundle's daily
          // rows without re-running preset_results pushed bundleLastDay past the
          // live cloud trigger (data to 07-23 vs trigger 07-21), so a running
          // campaign silently disarmed and the tail skipped a day. Data recency
          // says nothing about whether the bundle knows about this episode; the
          // trigger list does. When the bundle DOES know, bundleTriggered covers it.
          cloudTriggered = st.state === 'TRIGGERED'
            && (!bundleLastTriggerDate || st.since > bundleLastTriggerDate);
          // The worker's RESOLVED is authoritative for the CURRENT trigger episode.
          // Compare resolvedOn to the bundle's last TRIGGER-START (NOT bundleLastDay,
          // the data clock): if the worker resolved on/after the bundle's latest
          // episode started, it's the SAME episode → stay stopped, even when a freshly
          // rebuilt bundle (bundleLastDay advanced past resolvedOn) still shows it
          // active and disagrees on the marginal profit. Only a bundle trigger that
          // STARTED after resolvedOn is a genuinely new episode — and a real cloud
          // cross re-arms anyway via state→TRIGGERED (which carries no resolvedOn).
          cloudResolved = st.state === 'RESOLVED' && st.resolvedOn
            && (!bundleLastTriggerDate || st.resolvedOn >= bundleLastTriggerDate);
        }
      } catch (_) {}
    }

    const isTriggered = cloudResolved ? false : (bundleTriggered || cloudTriggered);
    let line;
    if (isTriggered) {
      if (vvix != null && vvix >= 110) {
        line = `Tail Hedge │ SKIP today (VVIX ${vvix.toFixed(2)} ≥ 110)`;
      } else {
        line = `Tail Hedge │ ▶ TRADE @ 9:45 — buy 0DTE SPXW put Δ-0.10  (VVIX ${vvix?.toFixed(2) ?? '—'})`;
      }
    } else {
      const c = cor1m != null ? cor1m.toFixed(2) : '—';
      line = `Tail Hedge │ No trade today (COR1M ${c}, need < 7.75)`;
    }
    _tailHedgeCache = { value: line, fetchedAt: now };
    return line;
  } catch (e) {
    return `Tail Hedge │ status unavailable (${e.message})`;
  }
}

// SPX options-skew advisory line for the morning message (informational).
// Fetches the daily VIX-smile decomposition (put_skew/call_skew) from GitHub,
// builds the net-skew series, and returns computeSkewReading().line — or null.
// Context only, never gates. See signal-engine.js computeSkewReading().
async function computeSkewLine() {
  try {
    const r = await fetch('https://raw.githubusercontent.com/rava8989/brave/main/data/vix_decomposition.json',
      { cf: { cacheTtl: 600, cacheEverything: true }, headers: { 'User-Agent': 'schwab-proxy-worker/1.0' } });
    if (!r.ok) return null;
    const all = await r.json();
    const series = Object.keys(all).sort().map(dt => {
      const o = all[dt];
      return (o && o.spot && o.put_skew != null && o.call_skew != null)
        ? { date: dt, net: o.put_skew - o.call_skew, spot: o.spot } : null;
    }).filter(Boolean);
    const reading = computeSkewReading(series);
    return reading ? reading.line : null;
  } catch (_) { return null; }
}

// ════════════════════════════════════════════════════════════════════
// DISCORD MESSAGE BUILDER (ported from index.html discordBuildMessage)
// ════════════════════════════════════════════════════════════════════

// Shared footer — used by both the text message and the image-card `content`
// so the live.html link is clickable text instead of being baked into the PNG.
// <URL> suppresses Discord's embed preview; *…* italicizes the disclaimer.
const DISCORD_FOOTER = '📈 Trades are posted live here: <https://rava8989.github.io/brave/live.html>\n*Not financial advice. For informational purposes only.*';

function buildDiscordMessage(signal, vixValues, tailLine) {
  const GRN = '\x1b[32m', RED = '\x1b[31m', DIM = '\x1b[2m', RST = '\x1b[0m';

  const isActive  = text => text && !text.startsWith('No ');
  const isBlocked = text => text && text.startsWith('No ');
  const sigColor  = text => isActive(text) ? GRN : isBlocked(text) ? RED : DIM;

  const m8bfDisplay = signal.m8bfText.replace(/^M8BF\s*[—-]\s*/, '').replace(/^M8BF$/, '—');
  // Use m8bfStrikeInfo (independent of main signal) so strikes render on M8BF
  // line even when main signal was overridden by OPEX+1 GXBF / blocked by gap up.
  // Show only when M8BF's OWN status is active (m8bfText starts with "M8BF").
  const si = signal.m8bfStrikeInfo;
  const m8bfActiveOwn = signal.m8bfText && signal.m8bfText.startsWith('M8BF');
  const strikes = (m8bfActiveOwn && si && si.blocked) ? `Banned center strikes — skip these:  ${si.blocked.join('  ')}  Combo bans (wing-width mod 100 → banned center end):  ${Object.entries(si.comboBans || {}).map(([k,v])=>`${k}→${v}`).join('  ')}` : '';
  const m8bfReason = signal.blockT === 'hard' && signal.rec.includes('M8BF') ? signal.blockD : '';

  let inner = `${DIM}📅 ${signal.dateStr} — ${signal.dayLabel}${RST}\n`;
  inner += `${DIM}${'─'.repeat(34)}${RST}\n`;

  const mc = sigColor(signal.m8bfText);
  inner += `${mc}M8BF     │ ${m8bfDisplay}${RST}\n`;
  if (strikes) inner += `${mc}         │ ${strikes}${RST}\n`;
  if (m8bfReason) inner += `${mc}         │ ${m8bfReason}${RST}\n`;
  inner += `${sigColor(signal.stradText)}Straddle │ ${signal.stradText}${RST}\n`;
  inner += `${sigColor(signal.gxbfText)}GXBF     │ ${signal.gxbfText}${RST}\n`;
  inner += `${sigColor(signal.bobfRec)}BOBF     │ ${signal.bobfRec}${RST}\n`;
  // Diagonal (companion — 10 ITM / 20 wide, 6-filter stack: OPEX-1+EOM+EOM-1+NM+VIX_MID 50-80+COR1M_LOW, 12:30–15:00 ET window; 2026-06-09 safer-tail retune)
  if (signal.diagText) {
    inner += `${sigColor(signal.diagText)}Diagonal │ ${signal.diagText}${RST}\n`;
  }

  // Tail Hedge (companion — recommended preset thr 7.75 / delta -0.10 / VVIX<110; 2026-06-16 sweep: 2× return, half drawdown of -0.20)
  if (tailLine) {
    const tc = tailLine.includes('TRADE') ? GRN : tailLine.includes('SKIP') ? RED : DIM;
    inner += `${tc}${tailLine}${RST}\n`;
  }

  // Auto-Tilt advisory (60d MAR weights — informational, user sizes manually)
  if (signal._tiltLine) inner += `${DIM}${signal._tiltLine}${RST}\n`;

  // GEX regime (validated 2026-06-10: BREAKOUT mornings ≈ 1.7× wider days)
  if (signal._gexLine) {
    const gc = signal._gexLine.includes('PIN') ? GRN : signal._gexLine.includes('BREAKOUT') ? RED : DIM;
    inner += `${gc}${signal._gexLine}${RST}\n`;
  }

  // CycleLab week-pattern (informational — which strategies historically
  // deviate from their own normal on days like today)
  if (signal._cycleLine) {
    const cc = signal._cycleLine.includes('/BULL') ? GRN : signal._cycleLine.includes('/BEAR') ? RED : DIM;
    inner += `${cc}${signal._cycleLine}${RST}\n`;
  }
  if (signal._volFlowLine) {
    const vc = signal._volFlowLine.includes('VOL_BID') ? RED
             : signal._volFlowLine.includes('MIXED') ? GRN : DIM;
    inner += `${vc}${signal._volFlowLine}${RST}\n`;
  }
  if (signal._skewLine) {
    const kc = signal._skewLine.includes('Distribution') ? RED
             : (signal._skewLine.includes('Healthy') || signal._skewLine.includes('Capitulation')) ? GRN : DIM;
    inner += `${kc}${signal._skewLine}${RST}\n`;
  }
  if (signal._m8bfWrLine) {
    const wc = signal._m8bfWrLine.includes('✓both halves') ? GRN
             : signal._m8bfWrLine.includes('COLDEST') ? RED : DIM;
    inner += `${wc}${signal._m8bfWrLine}${RST}\n`;
  }

  // VIX values
  inner += `${DIM}${'─'.repeat(34)}${RST}\n`;
  inner += `${DIM}VIX Prev Close  │ ${vixValues.yClose ?? '—'}${RST}\n`;
  inner += `${DIM}VIX Prev Open   │ ${vixValues.yOpen ?? '—'}${RST}\n`;
  inner += `${DIM}VIX Today Open  │ ${vixValues.todayOpen ?? '—'}${RST}\n`;
  inner += `${DIM}Overnight Drop  │ ${signal.oNight.toFixed(2)}${RST}\n`;
  inner += `${DIM}Open-to-Open    │ ${isNaN(signal.o2o) ? '—' : signal.o2o.toFixed(2)}${RST}\n`;

  // SPX gap
  if (signal.spxGapPct !== null && signal.spxGapPct !== undefined) {
    const dir = signal.spxGapPct > 0 ? '▲' : '▼';
    inner += `${DIM}${'─'.repeat(34)}${RST}\n`;
    inner += `${DIM}SPX Gap         │ ${dir}${Math.abs(signal.spxGapPct).toFixed(2)}%${RST}\n`;
  }

  return `\`\`\`ansi\n${inner}\`\`\`\n${DISCORD_FOOTER}`;
}

// ════════════════════════════════════════════════════════════════════
// EMBED BUILDER — Option E (Discord rich embed)
// Returns a single embed object suitable for { embeds: [obj] } payload.
// ════════════════════════════════════════════════════════════════════
function buildSigma3Embed(signal, vixValues, vixSource) {
  const { todayOpen, yClose, yOpen } = vixValues;
  const oNight = (yClose != null && todayOpen != null) ? (yClose - todayOpen) : null;
  const o2o    = (yOpen != null && todayOpen != null)  ? (yOpen - todayOpen) : null;

  const stripPrefix = (s, p) => s ? s.replace(new RegExp(`^${p}\\s*[—-]?\\s*`), '').replace(/^No\s+\w+\s*\(?/, '').replace(/\)\s*$/, '') : '';

  const fires = [], blocked = [];

  // M8BF
  const m8bfActive = signal.m8bfText && signal.m8bfText.startsWith('M8BF');
  if (m8bfActive) {
    const sc = signal.m8bfStrikeInfo;
    // Fat-gamma tier note (info only): tells the regime story, no sizing advice.
    const fat = signal.gexFatP != null
      ? ` · 🟢 fat-gamma tier p${signal.gexFatP} — historically 72% M8BF win rate in this tier vs 66% otherwise` : '';
    fires.push(`• **M8BF** — window ${sc?.window || signal.entryT || ''}${fat}`);
  } else if (signal.m8bfText) {
    blocked.push(`• **M8BF** — ${stripPrefix(signal.m8bfText, 'No M8BF')}`);
  }

  // Straddle
  const stradActive = signal.theme === 'strad';
  if (stradActive) {
    fires.push(`• **Straddle** — ${signal.entryT || '9:32 AM'}${signal.rec?.includes('NM') ? ' (NM)' : signal.rec?.includes('EOM') ? ' (EOM)' : ''}`);
  } else if (signal.stradText) {
    blocked.push(`• **Straddle** — ${stripPrefix(signal.stradText, 'No Straddle')}`);
  }

  // GXBF
  const gxbfActive = signal.theme === 'gxbf';
  if (gxbfActive) {
    const srcTag = signal.centerSource === 'oi' ? ' · OI center' : ' · Vol center';
    fires.push(`• **GXBF** — ${signal.entryT || '9:36 AM'}${srcTag}`);
  } else if (signal.gxbfText) {
    blocked.push(`• **GXBF** — ${stripPrefix(signal.gxbfText, 'No GXBF')}`);
  }

  // BOBF
  if (signal.bobfBadge !== 'BLOCKED') {
    fires.push(`• **BOBF** — ${signal.bobfRec?.replace(/^BOBF\s*[—-]?\s*/, '') || 'fires'}`);
  } else if (signal.bobfRec) {
    blocked.push(`• **BOBF** — ${stripPrefix(signal.bobfRec, 'No BOBF')}`);
  }

  // Diagonal
  if (signal.diagGo) {
    const pctStr = signal.vixPct20d != null ? ` (VIX 20d ${signal.vixPct20d}%)` : '';
    fires.push(`• **Diagonal** — 12:30–15:00 ET${pctStr}`);
  } else if (signal.diagText) {
    blocked.push(`• **Diagonal** — ${stripPrefix(signal.diagText, 'No Diagonal')}`);
  }

  // Tail Hedge (Balanced) — passed in via tailLine (already a formatted string).
  // Detect which bucket to put it in based on the keyword.
  if (signal._tailLine) {
    const tl = signal._tailLine;
    if (tl.includes('TRADE')) {
      fires.push(`• **Tail Hedge** — buy 0DTE SPXW put Δ-0.10 @ 9:45 ET`);
    } else if (tl.includes('SKIP')) {
      blocked.push(`• **Tail Hedge** — ${tl.split('│')[1]?.trim() || 'skip today'}`);
    } else {
      blocked.push(`• **Tail Hedge** — no trigger active`);
    }
  }

  // Color: green if ANY fires, red if all blocked
  const color = fires.length > 0 ? 0x22c55e : 0xef4444;

  // SPX gap
  const gapStr = (signal.spxGapPct != null)
    ? `${signal.spxGapPct > 0 ? '▲' : '▼'}${Math.abs(signal.spxGapPct).toFixed(2)}%`
    : '—';

  // VIX data block (monospace via code fence)
  const fmt = (v) => v != null ? v.toFixed(2) : '—';
  const marketData = '```\n' +
    `VIX  ${fmt(yClose)} → ${fmt(todayOpen)}   o/n ${fmt(oNight)}   o2o ${fmt(o2o)}\n` +
    `SPX  gap ${gapStr}\n` +
    '```';

  const fields = [];
  if (fires.length)   fields.push({ name: '✅ Today\'s plan', value: fires.join('\n'),   inline: false });
  if (blocked.length) fields.push({ name: '❌ Skipping',      value: blocked.join('\n'), inline: false });
  fields.push({ name: '📊 Market data', value: marketData, inline: false });

  // M8BF avoid centers (only when M8BF actually fires)
  if (m8bfActive && signal.m8bfStrikeInfo?.blocked?.length) {
    const sc = signal.m8bfStrikeInfo;
    const combos = Object.entries(sc.comboBans || {}).map(([k,v]) => `${k}→${v}`).join(', ');
    let val = `Skip centers: \`${sc.blocked.join(' · ')}\``;
    if (combos) val += `\nCombo bans: \`${combos}\``;
    fields.push({ name: '🚫 M8BF strike map', value: val, inline: false });
  }

  return {
    title: `Sigma 3 — ${signal.dateStr}`,
    description: `_${signal.dayLabel}_`,
    color,
    fields,
    footer: { text: `${vixSource === 'schwab' ? '📡 Schwab' : '📡 Tastytrade'} · Not financial advice` },
    timestamp: new Date().toISOString(),
  };
}

// Render embed → plain text (fallback for proxy paths that don't support embeds)
function embedToText(embed) {
  let out = `**${embed.title}**\n${embed.description || ''}\n\n`;
  for (const f of (embed.fields || [])) {
    out += `**${f.name}**\n${f.value}\n\n`;
  }
  if (embed.footer?.text) out += `_${embed.footer.text}_`;
  return out.trim();
}

// ════════════════════════════════════════════════════════════════════
// SCHWAB TOKEN HELPERS
// ════════════════════════════════════════════════════════════════════

async function getAccessToken(env, forceRefresh = false) {
  const tokensRaw = await env.SIGNAL_KV.get('schwab_tokens');
  if (!tokensRaw) throw new Error('No Schwab tokens in KV — sync from browser first');
  const tokens = JSON.parse(tokensRaw);

  // Check refresh token not expired
  if (Date.now() > tokens.refreshExpiry) {
    throw new Error('Schwab refresh token expired — re-authenticate in browser');
  }

  // Refresh access token if within 2 minutes of expiry or forced (401 retry)
  if (forceRefresh || Date.now() > tokens.expiry - 120000) {
    // Mutex: if a refresh is already in-flight, all concurrent callers share the same promise
    // so only ONE actual Schwab refresh call is made (Schwab refresh tokens are single-use)
    if (!_tokenRefreshPromise) {
      let timedOut = false;
      _tokenRefreshPromise = Promise.race([
        (async () => {
          try {
            const credsRaw = await env.SIGNAL_KV.get('schwab_creds');
            if (!credsRaw) throw new Error('No Schwab creds in KV');
            const creds = JSON.parse(credsRaw);

            const body = new URLSearchParams({
              grant_type: 'refresh_token',
              refresh_token: tokens.refresh,
            });

            const resp = await fetch('https://api.schwabapi.com/v1/oauth/token', {
              method: 'POST',
              headers: {
                'Content-Type': 'application/x-www-form-urlencoded',
                'Authorization': 'Basic ' + btoa(`${creds.appKey}:${env.SCHWAB_APP_SECRET}`),
              },
              body,
            });

            // Retry-on-stale: Schwab 400s when the refresh_token has already
            // been rotated by another Worker isolate (or an external client
            // that still talks to Schwab directly). In that case, re-read KV
            // — whichever client won will have written the new tokens — and
            // return that access token. This is the cross-isolate counterpart
            // to the in-isolate _tokenRefreshPromise mutex.
            if (resp.status === 400) {
              const freshRaw = await env.SIGNAL_KV.get('schwab_tokens');
              if (freshRaw) {
                const fresh = JSON.parse(freshRaw);
                if (fresh.refresh !== tokens.refresh && Date.now() < fresh.expiry - 60000) {
                  console.warn('[proxy] Token refresh lost race; using winner from KV');
                  await recordRefreshHealth(env, true);
                  return fresh.access;
                }
              }
            }

            if (!resp.ok) {
              // Capture Schwab's actual error response so we can debug 400s.
              // Tokens are sensitive — log only first/last 4 chars + length to
              // confirm round-trip integrity without leaking the secret.
              let errBody = '';
              try { errBody = (await resp.text()).slice(0, 500); } catch {}
              const r = tokens.refresh || '';
              const fingerprint = r ? `${r.slice(0,4)}…${r.slice(-4)}(len=${r.length})` : '(none)';
              console.error('[proxy] Token refresh HTTP', resp.status, '— body:', errBody, '— refresh fp:', fingerprint, '— appKey:', creds.appKey?.slice(0, 8) + '…');
              throw new Error('Token refresh HTTP ' + resp.status + ': ' + errBody.slice(0, 120));
            }
            const data = await resp.json();
            if (!data.access_token) throw new Error('Token refresh failed: ' + (data.error_description || JSON.stringify(data)));

            // ALWAYS write to KV on a successful Schwab response, even if our
            // 30s timeout already fired. Rationale: Schwab's refresh_token is
            // single-use — if Schwab rotated, we hold the ONLY valid copy.
            // The old code's `if (!timedOut)` gate dropped the new tokens on
            // the floor when the timeout fired, guaranteeing a 24h stuck
            // state until manual re-auth (observed 2026-04-23). No other
            // isolate can possibly have fresher tokens because this specific
            // refresh_token only validates with Schwab once.
            const newTokens = {
              access: data.access_token,
              refresh: data.refresh_token || tokens.refresh,
              expiry: Date.now() + (data.expires_in * 1000),
              // Schwab ECHOES the same refresh_token on every access-token
              // refresh — presence ≠ rotation. Resetting the 7-day clock on
              // every echo pinned the countdown at 7.0d forever, so the
              // ≤3.5d evening warning + ≤1.5d dashboard alert NEVER fired
              // and the 2026-07-12 expiry hit with zero notice. Only a
              // genuinely NEW token (real re-grant) restarts the clock.
              refreshExpiry: (data.refresh_token && data.refresh_token !== tokens.refresh)
                ? Date.now() + (7 * 24 * 60 * 60 * 1000)
                : tokens.refreshExpiry,
            };
            await env.SIGNAL_KV.put('schwab_tokens', JSON.stringify(newTokens));
            if (timedOut) {
              console.warn('[proxy] Token refresh returned after timeout — wrote fresh tokens anyway');
            }
            await recordRefreshHealth(env, true);
            return newTokens.access;
          } catch (err) {
            // Record the failure so the browser UI can surface a red banner
            // instead of silently serving stale data. Re-throw to preserve
            // caller error handling.
            await recordRefreshHealth(env, false, err?.message || String(err));
            throw err;
          } finally {
            _tokenRefreshPromise = null;
          }
        })(),
        new Promise((_, reject) => setTimeout(async () => {
          timedOut = true;
          // Record timeout as health failure so browser UI catches it even if
          // the inner refresh never completes. If the inner fn eventually
          // resolves successfully, it will overwrite this with an ok=true
          // record — that's the desired convergence.
          await recordRefreshHealth(env, false, 'Token refresh timeout (30s)');
          // CRITICAL: null _tokenRefreshPromise here too. If the inner refresh
          // fetch was killed by the Workers runtime after our Response returned
          // (which happens for cron handlers when the cron tick ends), its
          // finally block never runs, leaving _tokenRefreshPromise pinned to a
          // rejected value forever. Subsequent calls in the same isolate would
          // skip the `if (!_tokenRefreshPromise)` branch and re-throw the same
          // timeout error indefinitely (observed: 77 consecutive errors over
          // 3+ hours, only fixed by a new deploy that killed the isolate).
          // Concurrent refreshes are safe: the retry-on-stale path at line ~195
          // catches the loser and returns the winner's access from KV.
          _tokenRefreshPromise = null;
          reject(new Error('Token refresh timeout (30s)'));
        }, 30000))
      ]);
    }
    return await _tokenRefreshPromise;
  }

  return tokens.access;
}

// Per-invocation Schwab call counter (2026-07-26). Schwab throttles per app,
// not per key, so the number that matters is calls/minute across everything the
// tick does. Recorded per tick into `schwab_usage` and readable at
// GET /schwab-usage — measured, not guessed.
let _schwabCalls = 0, _schwab429 = 0;
async function fetchSchwabJSON(url, token, env) {
  _schwabCalls++;
  let resp = await fetch(url, { headers: { Authorization: `Bearer ${token}` } });
  // Retry once with refreshed token on 401
  if (resp.status === 401 && env) {
    console.warn('[proxy] Schwab 401 — retrying with fresh token');
    const freshToken = await getAccessToken(env, true);
    resp = await fetch(url, { headers: { Authorization: `Bearer ${freshToken}` } });
  }
  // 429 = throttled. Previously this threw and the caller silently lost the
  // tick's data; now we honour Retry-After (capped) and retry once.
  if (resp.status === 429) {
    _schwab429++;
    const ra = parseInt(resp.headers.get('Retry-After') || '2', 10);
    const waitMs = Math.min(Math.max(isNaN(ra) ? 2 : ra, 1), 5) * 1000;
    console.warn(`[proxy] Schwab 429 — backing off ${waitMs}ms then retrying once`);
    await new Promise(r => setTimeout(r, waitMs));
    _schwabCalls++;
    resp = await fetch(url, { headers: { Authorization: `Bearer ${token}` } });
  }
  if (!resp.ok) throw new Error(`Schwab API ${resp.status}: ${url.split('?')[0]}`);
  return resp.json();
}

// Append this tick's Schwab call count to a rolling log (last 90 ticks).
// One KV read+write per tick — noise next to what a tick already does, and it
// turns "are we near the limit?" into a number anyone can look up.
async function recordSchwabUsage(env, etNow) {
  try {
    if (!_schwabCalls) return;
    const raw = await env.SIGNAL_KV.get('schwab_usage');
    const log = raw ? JSON.parse(raw) : { ticks: [], peak: 0, peakAt: null, total429: 0 };
    const hhmm = `${String(etNow.getHours()).padStart(2, '0')}:${String(etNow.getMinutes()).padStart(2, '0')}`;
    log.ticks.push({ t: hhmm, n: _schwabCalls, d: isoDateET(etNow) });
    if (log.ticks.length > 90) log.ticks = log.ticks.slice(-90);
    if (_schwabCalls > (log.peak || 0)) { log.peak = _schwabCalls; log.peakAt = `${isoDateET(etNow)} ${hhmm}`; }
    log.total429 = (log.total429 || 0) + _schwab429;
    await env.SIGNAL_KV.put('schwab_usage', JSON.stringify(log), { expirationTtl: 14 * 86400 });
  } catch (_) { /* usage telemetry must never break a tick */ }
}

// ════════════════════════════════════════════════════════════════════
// SCHEDULED HANDLER (Cron Trigger)
// ════════════════════════════════════════════════════════════════════
// TASTYTRADE BACKUP CLIENT (OAuth2 — refresh token never expires)
// ────────────────────────────────────────────────────────────────────
// Free with funded Tastytrade account. Used as a redundant data source
// when Schwab fails (e.g., token expired at 9:30 AM).
//
// OAuth2 flow:
//   1. User registers an OAuth app at my.tastytrade.com (one-time).
//   2. User visits /tasty-oauth-start (browser) → redirected to Tastytrade
//      authorize page → approves → Tastytrade redirects back to
//      /tasty-oauth-callback with ?code=...
//   3. Callback exchanges code for refresh_token, stores it in KV.
//   4. Going forward, getTastyAccessToken() uses refresh_token to mint
//      short-lived (15 min) access tokens. Refresh token never expires.
// ════════════════════════════════════════════════════════════════════

const TASTY_BASE = 'https://api.tastyworks.com';
const TASTY_AUTH_BASE = 'https://my.tastytrade.com';
const TASTY_REDIRECT_URI = 'https://schwab-proxy.ravamt4.workers.dev/tasty-oauth-callback';

// Common headers required by current Tastytrade API
function tastyHeaders(extra = {}) {
  return {
    'Accept': 'application/json',
    'Accept-Version': '20251101',
    'User-Agent': 'schwab-proxy-worker/1.0',
    ...extra,
  };
}

// Build the authorize URL the user visits to grant access
function tastyAuthorizeUrl(env) {
  const params = new URLSearchParams({
    response_type: 'code',
    client_id: env.TASTYTRADE_CLIENT_ID,
    redirect_uri: TASTY_REDIRECT_URI,
    scope: 'read',
  });
  return `${TASTY_AUTH_BASE}/auth.html?${params}`;
}

// Exchange the OAuth `code` from the callback for access_token + refresh_token
async function tastyExchangeCode(env, code) {
  const body = new URLSearchParams({
    grant_type: 'authorization_code',
    code,
    client_id: env.TASTYTRADE_CLIENT_ID,
    client_secret: env.TASTYTRADE_CLIENT_SECRET,
    redirect_uri: TASTY_REDIRECT_URI,
  });
  const resp = await fetch(`${TASTY_BASE}/oauth/token`, {
    method: 'POST',
    headers: tastyHeaders({ 'Content-Type': 'application/x-www-form-urlencoded' }),
    body,
  });
  if (!resp.ok) {
    const txt = await resp.text();
    throw new Error(`Tastytrade code-exchange HTTP ${resp.status}: ${txt.slice(0, 300)}`);
  }
  return resp.json();  // { access_token, refresh_token, expires_in, ... }
}

// Get a short-lived access token. Uses cached one if still fresh, else
// refreshes via the long-lived refresh_token (stored in KV).
async function getTastyAccessToken(env) {
  // Try cache (access tokens live ~15 min)
  const cached = await env.SIGNAL_KV.get('tasty_access_token');
  if (cached) {
    const obj = JSON.parse(cached);
    if (obj.expires_at > Date.now() + 60_000) return obj.access_token;  // 60s buffer
  }
  // Refresh
  const refresh = await env.SIGNAL_KV.get('tasty_refresh_token');
  if (!refresh) throw new Error('Tastytrade refresh_token missing — visit /tasty-oauth-start to authorize');
  if (!env.TASTYTRADE_CLIENT_SECRET) throw new Error('TASTYTRADE_CLIENT_SECRET not configured');

  const body = new URLSearchParams({
    grant_type: 'refresh_token',
    client_secret: env.TASTYTRADE_CLIENT_SECRET,
    refresh_token: refresh,
  });
  const resp = await fetch(`${TASTY_BASE}/oauth/token`, {
    method: 'POST',
    headers: tastyHeaders({ 'Content-Type': 'application/x-www-form-urlencoded' }),
    body,
  });
  if (!resp.ok) {
    const txt = await resp.text();
    throw new Error(`Tastytrade token refresh HTTP ${resp.status}: ${txt.slice(0, 300)}`);
  }
  const data = await resp.json();
  const accessToken = data.access_token;
  const expiresIn = data.expires_in || 900;  // default 15 min
  await env.SIGNAL_KV.put('tasty_access_token', JSON.stringify({
    access_token: accessToken,
    expires_at: Date.now() + (expiresIn * 1000),
  }), { expirationTtl: expiresIn });
  // Refresh token may rotate on refresh — if a new one came back, save it
  if (data.refresh_token && data.refresh_token !== refresh) {
    await env.SIGNAL_KV.put('tasty_refresh_token', data.refresh_token);
  }
  return accessToken;
}

// Get latest VIX quote via Tastytrade. Returns { price, asOf, source }.
// Endpoint per tastyware/tastytrade SDK: /market-data/{instrumentType}/{symbol}
// VIX is InstrumentType.INDEX → "Index", path /market-data/Index/VIX
// Generic Tasty index quote — same /market-data/Index endpoint as VIX but
// symbol-parameterized. Returns { price, open, asOf } or throws. Used as the
// BACKUP source when Schwab quotes are stale/down (2026-06-10 user request:
// "tasty jumps in and picks up the slack").
async function tastyGetIndexQuote(env, symbol) {
  const token = await getTastyAccessToken(env);
  const resp = await fetch(`${TASTY_BASE}/market-data/Index/${encodeURIComponent(symbol)}`,
    { headers: tastyHeaders({ 'Authorization': `Bearer ${token}` }) });
  if (!resp.ok) throw new Error(`Tasty ${symbol} HTTP ${resp.status}: ${(await resp.text()).slice(0, 150)}`);
  const d = (await resp.json())?.data || {};
  const price = d.last ?? d['last-price'] ?? d.mark ?? d.mid ?? null;
  const open = d.open ?? d['open-price'] ?? null;
  const asOf = d['updated-at'] || d['quote-time'] || d.timestamp || null;
  if (price == null) throw new Error(`Tasty ${symbol}: no price field`);
  return { price: parseFloat(price), open: open != null ? parseFloat(open) : null,
           prevClose: d['prev-close'] != null ? parseFloat(d['prev-close']) : (d['close-price'] != null ? parseFloat(d['close-price']) : null),
           asOf, raw: d };
}

async function tastyGetVix(env) {
  const token = await getTastyAccessToken(env);
  const url = `${TASTY_BASE}/market-data/Index/VIX`;
  const resp = await fetch(url, { headers: tastyHeaders({ 'Authorization': `Bearer ${token}` }) });
  if (!resp.ok) {
    const txt = await resp.text();
    throw new Error(`Tastytrade VIX HTTP ${resp.status}: ${txt.slice(0, 200)}`);
  }
  const body = await resp.json();
  const d = body?.data || body;  // payload may be wrapped
  // Tasty exposes today's open as a stable field (mirrors how /Index/SPX does
  // it). Use this for the morning signal — matches Schwab pricehistory 9:30
  // candle.open. The `last`/`mid` fields are CURRENT tick (drifts after open).
  const openRaw = d?.open ?? d?.['open-price'] ?? null;
  const openNum = openRaw != null ? parseFloat(openRaw) : null;
  // VALUE-STALENESS GUARD (2026-06-16): Tasty's `open` carries the prior CLOSE
  // as a pre-open placeholder while `updated-at` keeps ticking fresh on every
  // mark — so a timestamp gate alone (lesson 2026-05-21) lets the stale value
  // through. A real session open ~never equals the prior close to the penny;
  // equality is the signature of "open not published yet". Null it so no caller
  // mistakes it for today's open — they fall back to Schwab (the trusted source)
  // or keep polling. Bug: 2026-06-16 posted VIX open 16.2 == prevClose 16.2.
  const prevCloseRaw = d?.['prev-close'] ?? d?.['close-price'] ?? null;
  const prevCloseNum = prevCloseRaw != null ? parseFloat(prevCloseRaw) : null;
  const openStale = openNum != null && prevCloseNum != null &&
                    Math.abs(openNum - prevCloseNum) < 0.005;
  const open = openStale ? null : openNum;
  // `last`/`mid` kept for callers that want current price (e.g. vixClose path).
  // Fall back to the RAW open (pre-guard) as last resort so `.price` is always
  // a number for backward-compat callers.
  const priceRaw = d?.last ?? d?.['last-price'] ?? d?.mid ?? d?.mark ?? d?.bid ?? openNum;
  const asOf  = d?.['updated-at'] || d?.['quote-time'] || d?.timestamp;
  if (priceRaw == null) {
    throw new Error(`Tastytrade VIX: no usable price field in payload: ${JSON.stringify(d).slice(0, 250)}`);
  }
  return {
    price: parseFloat(priceRaw),
    open,                 // validated today's open — null if stale pre-open snapshot
    openStale,
    prevClose: prevCloseNum,
    asOf, source: 'tastytrade', endpoint: '/market-data/Index/VIX', raw: d,
  };
}

// Get SPX index quote via Tastytrade (same Index market-data endpoint as VIX).
// Returns { price, open, last, asOf, source, raw }. `price` = today's open if
// present (what the morning signal needs), else last/mark. Used as the
// PRIMARY for the signal's SPX-open, with Schwab as fallback — Tasty's
// refresh_token never expires, so a dead Schwab token can't kill the signal.
async function tastyGetSpx(env) {
  const token = await getTastyAccessToken(env);
  const url = `${TASTY_BASE}/market-data/Index/SPX`;
  const resp = await fetch(url, { headers: tastyHeaders({ 'Authorization': `Bearer ${token}` }) });
  if (!resp.ok) {
    const txt = await resp.text();
    throw new Error(`Tastytrade SPX HTTP ${resp.status}: ${txt.slice(0, 200)}`);
  }
  const body = await resp.json();
  const d = body?.data || body;
  const openRaw = d?.open ?? d?.['open-price'] ?? null;
  const openNum = openRaw != null ? parseFloat(openRaw) : null;
  // VALUE-STALENESS GUARD (audit P1-e 2026-07-06, same class as tastyGetVix 06-16):
  // Tasty serves the prior CLOSE as a pre-open `open` placeholder while `updated-at`
  // ticks fresh, so a timestamp gate alone lets a stale open through. A real session
  // open ~never equals the prior close to the penny — null it so callers fall back to
  // Schwab / keep polling rather than anchor the straddle center + SPX-gap gate on
  // yesterday's close on a gap day.
  const prevCloseRaw = d?.['prev-close'] ?? d?.['close-price'] ?? null;
  const prevCloseNum = prevCloseRaw != null ? parseFloat(prevCloseRaw) : null;
  const openStale = openNum != null && prevCloseNum != null && Math.abs(openNum - prevCloseNum) < 0.005;
  const open = openStale ? null : openNum;
  const last = d?.last ?? d?.['last-price'] ?? d?.mid ?? d?.mark ?? d?.bid ?? null;
  const price = (open != null ? open : last) ?? openNum;
  const asOf = d?.['updated-at'] || d?.['quote-time'] || d?.timestamp;
  if (price == null) {
    throw new Error(`Tastytrade SPX: no usable price field: ${JSON.stringify(d).slice(0, 250)}`);
  }
  return {
    price: parseFloat(price),
    open, openStale,
    prevClose: prevCloseNum,
    last: last != null ? parseFloat(last) : null,
    asOf, source: 'tastytrade', endpoint: '/market-data/Index/SPX', raw: d,
  };
}

// ─── Tastytrade SPX option-chain fetcher ─────────────────────────────────
// Returns a Schwab-shape { spot, callExpDateMap, putExpDateMap } so it
// can drop into any existing consumer as a fallback when Schwab is dead
// (token expired) or fails. opts:
//   root         — 'SPXW' (default; PM-settled 0DTE/weekly), 'SPX' (AM)
//   expirations  — array of 'YYYY-MM-DD' to limit fetch volume (recommended)
//   strikeCount  — N strikes nearest spot, per expiration (default 80)
//   contractType — 'CALL'|'PUT'|'BOTH' (default 'BOTH')
// Tasty doesn't expose open interest via REST — `openInterest:0` for now.
// Live trading paths use bid/ask/mark only, so OI=0 is acceptable.
async function tastyFetchSpxChain(env, opts = {}) {
  const root = opts.root || 'SPXW';
  const wantExp = opts.expirations ? new Set(opts.expirations) : null;
  const strikeCount = Math.max(1, opts.strikeCount || 80);
  const ct = (opts.contractType || 'BOTH').toUpperCase();
  const wantCall = ct === 'BOTH' || ct === 'CALL';
  const wantPut  = ct === 'BOTH' || ct === 'PUT';

  const token = await getTastyAccessToken(env);
  const hdr = tastyHeaders({ Authorization: `Bearer ${token}` });

  // 1) Nested chain (structure) + index spot in parallel
  const [nestedResp, idxResp] = await Promise.all([
    fetch(`${TASTY_BASE}/option-chains/${encodeURIComponent(root)}/nested`, { headers: hdr }),
    fetch(`${TASTY_BASE}/market-data/Index/SPX`, { headers: hdr }),
  ]);
  if (!nestedResp.ok) {
    const txt = await nestedResp.text();
    throw new Error(`Tasty nested chain HTTP ${nestedResp.status}: ${txt.slice(0, 200)}`);
  }
  const nestedJson = await nestedResp.json();
  const item0 = nestedJson?.data?.items?.[0];
  if (!item0) throw new Error('Tasty nested chain: empty items[]');

  let spot = null;
  if (idxResp.ok) {
    try {
      const idxJson = await idxResp.json();
      const d = idxJson?.data || {};
      const cand = d.last ?? d['last-price'] ?? d.mark ?? d.mid ?? d['close-price'];
      if (cand != null) spot = parseFloat(cand);
    } catch (_) {}
  }

  // 2) Filter expirations (optional) + nearest-N strikes (around spot if known)
  const expirations = (item0.expirations || []).filter(e => !wantExp || wantExp.has(e['expiration-date']));
  const slots = [];          // [{expKey, strikes: [{strike, callSym, putSym}]}]
  const allSyms = [];
  for (const e of expirations) {
    const expDate = e['expiration-date'];
    const dte = e['days-to-expiration'];
    const expKey = `${expDate}:${dte}`;
    let strikes = e.strikes || [];
    if (Number.isFinite(spot) && strikes.length > strikeCount) {
      strikes = strikes.slice().sort((a, b) =>
        Math.abs(parseFloat(a['strike-price']) - spot) -
        Math.abs(parseFloat(b['strike-price']) - spot)
      ).slice(0, strikeCount);
    }
    const items = [];
    for (const s of strikes) {
      const strike = parseFloat(s['strike-price']);
      const it = { strike, callSym: wantCall ? s.call : null, putSym: wantPut ? s.put : null };
      items.push(it);
      if (it.callSym) allSyms.push(it.callSym);
      if (it.putSym)  allSyms.push(it.putSym);
    }
    slots.push({ expKey, items });
  }

  // 3) Batch-fetch quotes via /market-data?symbols=sym1,sym2,... (comma-batch).
  // Chunks of 100 symbols, capped concurrency to avoid Tasty rate limits.
  const quoteMap = {};
  const CHUNK = 100;
  const MAX_PAR = 5;
  const batches = [];
  for (let i = 0; i < allSyms.length; i += CHUNK) batches.push(allSyms.slice(i, i + CHUNK));
  for (let i = 0; i < batches.length; i += MAX_PAR) {
    const wave = batches.slice(i, i + MAX_PAR);
    const results = await Promise.all(wave.map(async b => {
      const url = `${TASTY_BASE}/market-data?symbols=${encodeURIComponent(b.join(','))}`;
      const r = await fetch(url, { headers: hdr });
      if (!r.ok) {
        const t = await r.text();
        throw new Error(`Tasty batch-quote HTTP ${r.status}: ${t.slice(0, 160)}`);
      }
      return r.json();
    }));
    for (const r of results) for (const it of (r?.data?.items || [])) if (it.symbol) quoteMap[it.symbol] = it;
  }

  // 4) Assemble Schwab-compatible callExpDateMap / putExpDateMap
  const callExpDateMap = {};
  const putExpDateMap  = {};
  const num = v => (v == null || v === '') ? 0 : parseFloat(v);
  const mkContract = (q, strike, putCall) => ({
    putCall,
    symbol: q.symbol,
    bid: num(q.bid),
    ask: num(q.ask),
    last: num(q.last),
    mark: num(q.mark ?? q.mid),
    bidSize: num(q['bid-size']),
    askSize: num(q['ask-size']),
    strikePrice: strike,
    volatility: num(q.volatility) * 100,   // Schwab uses percent; Tasty decimal
    delta: num(q.delta),
    gamma: num(q.gamma),
    theta: num(q.theta),
    vega: num(q.vega),
    rho: num(q.rho),
    openInterest: 0,   // not exposed via Tasty REST
    totalVolume: 0,    // not exposed via Tasty REST
    _tastySource: true,
  });
  let mapped = 0, missing = 0;
  for (const slot of slots) {
    for (const it of slot.items) {
      const sk = it.strike.toFixed(1);   // Schwab key format "7400.0"
      if (it.callSym) {
        const q = quoteMap[it.callSym];
        if (q) {
          if (!callExpDateMap[slot.expKey]) callExpDateMap[slot.expKey] = {};
          (callExpDateMap[slot.expKey][sk] = callExpDateMap[slot.expKey][sk] || []).push(mkContract(q, it.strike, 'CALL'));
          mapped++;
        } else missing++;
      }
      if (it.putSym) {
        const q = quoteMap[it.putSym];
        if (q) {
          if (!putExpDateMap[slot.expKey]) putExpDateMap[slot.expKey] = {};
          (putExpDateMap[slot.expKey][sk] = putExpDateMap[slot.expKey][sk] || []).push(mkContract(q, it.strike, 'PUT'));
          mapped++;
        } else missing++;
      }
    }
  }

  return {
    spot,
    underlyingPrice: spot,     // Schwab also exposes this top-level alias
    callExpDateMap,
    putExpDateMap,
    fetchedAt: Date.now(),
    _source: 'tastytrade',
    _stats: { expirations: slots.length, symsRequested: allSyms.length, mapped, missing },
  };
}

// ════════════════════════════════════════════════════════════════════

async function handleEOD(env, etNow) {
  const todayISO = `${etNow.getFullYear()}-${String(etNow.getMonth()+1).padStart(2,'0')}-${String(etNow.getDate()).padStart(2,'0')}`;
  let token = null;
  try { token = await getAccessToken(env); } catch(e) { console.warn('[proxy]', e.message || e); }

  const end = Date.now();
  const start = end - 3 * 24 * 60 * 60 * 1000;
  const todayStr = etNow.toDateString();

  // Fetch VIX close — Tastytrade PRIMARY (gives true 4:15 PM official close),
  // Schwab fallback (1-min data stops at 4:00 PM so it's only ever 4:00 value).
  // Tastytrade's prev-close field reflects the previous trading day's official
  // close. Call NEXT morning to capture today's close — or use today's `last`
  // if calling at EOD before next-day refresh.
  let vixClose = null;
  try {
    const tastyVix = await tastyGetVix(env);
    const prevClose = parseFloat(tastyVix?.raw?.['prev-close']);
    const prevDate  = tastyVix?.raw?.['prev-close-date'];
    // If Tasty's prev-close-date == today's ISO, that IS today's official close.
    // (Tasty rolls prev-close at next-day open, so this branch fires when worker
    // runs the day-after-EOD reconciliation cron.)
    if (prevDate === todayISO && Number.isFinite(prevClose)) {
      vixClose = parseFloat(prevClose.toFixed(2));
    } else if (Number.isFinite(parseFloat(tastyVix?.raw?.last))) {
      // Same-day EOD path: use Tasty's current `last` near 4:15. May be 4:14
      // tick instead of the true 4:15 settle, but closer than Schwab 4:00.
      vixClose = parseFloat(parseFloat(tastyVix.raw.last).toFixed(2));
    }
  } catch (e) { console.warn('[proxy] tasty vixClose err:', e.message || e); }
  // Schwab fallback if Tasty failed (1-min data cuts off at 4:00 PM so this is
  // a "better than null" value, not the official 4:15).
  if (vixClose === null) {
    try {
      const vixHist = await fetchSchwabJSON(`https://api.schwabapi.com/marketdata/v1/pricehistory?symbol=%24VIX&periodType=day&period=3&frequencyType=minute&frequency=1&startDate=${start}&endDate=${end}&needExtendedHoursData=true`, token);
      if (vixHist.candles) {
        const todayCandles = vixHist.candles.filter(c => toET(new Date(c.datetime)).toDateString() === todayStr);
        todayCandles.sort((a, b) => a.datetime - b.datetime);
        const closeCandle = todayCandles.slice().reverse().find(c => {
          const d = toET(new Date(c.datetime));
          return d.getHours() * 60 + d.getMinutes() <= 16 * 60 + 15;
        });
        if (closeCandle) vixClose = parseFloat(closeCandle.close.toFixed(2));
      }
      if (vixClose === null) {
        const q = await fetchSchwabJSON(`https://api.schwabapi.com/marketdata/v1/quotes?symbols=%24VIX&fields=quote`, token);
        const cp = q?.['$VIX']?.quote?.closePrice;
        if (cp) vixClose = parseFloat(cp.toFixed(2));
      }
    } catch (e) { console.warn('[proxy] schwab vixClose err:', e.message || e); }
  }

  // Fetch SPX close
  let spxClose = null;
  try {
    const spxHist = await fetchSchwabJSON(`https://api.schwabapi.com/marketdata/v1/pricehistory?symbol=%24SPX&periodType=day&period=3&frequencyType=minute&frequency=1&startDate=${start}&endDate=${end}&needExtendedHoursData=true`, token);
    if (spxHist.candles) {
      const todayCandles = spxHist.candles.filter(c => toET(new Date(c.datetime)).toDateString() === todayStr);
      todayCandles.sort((a, b) => a.datetime - b.datetime);
      const closeCandle = todayCandles.slice().reverse().find(c => {
        const d = toET(new Date(c.datetime));
        return d.getHours() * 60 + d.getMinutes() <= 16 * 60 + 15;
      });
      if (closeCandle) spxClose = parseFloat(closeCandle.close.toFixed(2));
    }
  } catch (e) { console.warn('[proxy]', e.message || e); }
  // Quote-endpoint fallback in its OWN try (2026-07-10): it used to share the
  // candle fetch's try block, so a thrown candle call skipped it entirely —
  // that cascade (candles threw → quote skipped → Stooq too early at 16:17)
  // left 2026-07-09 with spxClose null and every strategy settle silently dropped.
  if (spxClose === null) {
    try {
      const q = await fetchSchwabJSON(`https://api.schwabapi.com/marketdata/v1/quotes?symbols=%24SPX&fields=quote`, token);
      const cp = q?.['$SPX']?.quote?.closePrice;
      if (cp) spxClose = parseFloat(cp.toFixed(2));
    } catch (e) { console.warn('[proxy] spx quote fallback:', e.message || e); }
  }

  // Stooq fallback for SPX close when Schwab tokens are expired
  if (spxClose === null) {
    try {
      const todayISO2 = `${etNow.getFullYear()}-${String(etNow.getMonth()+1).padStart(2,'0')}-${String(etNow.getDate()).padStart(2,'0')}`;
      spxClose = await getSpxCloseForDate(todayISO2, env);
    } catch (e) { console.warn('[proxy]', e.message || e); }
  }

  // Still nothing → every settle below will be skipped. Say so ONCE instead of
  // silently dropping the day (rule: every terminal skip path must notify).
  // The next-EOD orphan sweep + backfills self-heal once a close is available.
  if (spxClose === null) {
    try {
      const todayISO3 = `${etNow.getFullYear()}-${String(etNow.getMonth()+1).padStart(2,'0')}-${String(etNow.getDate()).padStart(2,'0')}`;
      const akE = `eod_noclose_alert_${todayISO3}`;
      if (!(await env.SIGNAL_KV.get(akE))) {
        await env.SIGNAL_KV.put(akE, '1', { expirationTtl: 86400 });
        const dcRaw = await env.SIGNAL_KV.get('discord_config');
        const dc = dcRaw ? JSON.parse(dcRaw) : null;
        if (dc && dc.channelId) await sendDiscordDM(env, dc.channelId,
          `⚠️ **EOD** — SPX close unavailable (Schwab candles + quote + Stooq all failed). Today's strategy settles are deferred; the next EOD run auto-heals via the orphan sweep + backfills.`, dc.proxyUrl);
      }
    } catch (_) {}
  }

  // Compute m8bfPL directly from today's signals + spxClose (no live_updater dependency)
  let m8bfPL = null;
  let m8bfWR = null;

  // Re-scrape all Discord signals for today (full pagination) to ensure completeness
  // The live KV polling may have missed signals if cron was down
  let fullSigs = [];
  if (env.DISCORD_USER_TOKEN) {
    try {
      fullSigs = await fetchAllDiscordSignalsForDate(env.DISCORD_USER_TOKEN, '1048242197029458040', todayISO);
      if (fullSigs.length > 0) {
        const signals = fullSigs.map(s => ({
          time: s.time, center: s.center, lower: s.lower, upper: s.upper,
          t1: s.t1, premium: s.premium, cp: s.cp ?? 0, banned: isBanned(s.center, s.lower, s.t1),
        }));
        await env.SIGNAL_KV.put('signals_today', JSON.stringify({ date: todayISO, signals }));
        console.log(`[eod] Re-scraped ${signals.length} signals for ${todayISO}`);
      }
    } catch (e) { console.warn('[eod] signal re-scrape:', e.message); }
  }

  // Compute m8bfWR = win rate across ALL signals posted today (from KV)
  if (spxClose != null) {
    try {
      const kvRaw = await env.SIGNAL_KV.get('signals_today');
      if (kvRaw) {
        const kv = JSON.parse(kvRaw);
        if (kv.date === todayISO && Array.isArray(kv.signals) && kv.signals.length > 0) {
          m8bfWR = computeWinRateFromSignals(kv.signals, spxClose);
        }
      }
    } catch (e) { console.warn('[proxy]', e.message || e); }
  }

  // Determine if today is a SKIP day per the live signal logic.
  // m8bfBlockedByLive == true means: live system would NOT trade M8BF today (calendar
  // blocks, gap, o2o, 0% rule, etc.). In that case the backtester must also skip,
  // so we set m8bfPL = 0 and add the date to M8BF_SKIP after appending trades.
  let m8bfBlockedByLive = false;

  // FIX (2026-06-09 audit P0 #3): both STEP 1 and STEP 2 used to ignore the
  // 90%-WR override in signal-engine.js. On a day where prevWR≥90 falls on
  // EOM/EOM-1/OPEX-1/NM-non-Mon, the live bot DID fire M8BF — but EOD silently
  // wrote m8bfPL=0 and added the date to M8BF_SKIP, corrupting the ledger.
  //
  // STEP 1 now DEFERS to STEP 2 when the 90% override is possible. STEP 2
  // calls calculateSignal with prevWR and trusts sig.theme === "m8bf"
  // (which incorporates the 90% override AND the GXBF exclusion).
  let calendarWouldBlock = false;
  let ninetyOverridePossible = false;
  let calendarBlockReason = '';
  {
    const eomDay = isEomN(0, etNow);
    const eom1 = isEomN(1, etNow);
    const opex1 = opexSch.some(ds => isTodayBefore(ds, etNow));
    const vixExpAfterOpex = isVixAfterOpexDay(etNow);
    const nonAmznTslaEarn = isNonAmznTslaEarningsDay(etNow);
    const cpiDay = cpiSch.includes(todayLong(etNow));
    const nmDay = isFirstTradeMo(etNow);
    const nmMon = isFirstTradeMon(etNow);
    const nmNonMon = nmDay && !nmMon;
    calendarWouldBlock = eomDay || eom1 || opex1 || vixExpAfterOpex || nonAmznTslaEarn || cpiDay || nmNonMon;
    calendarBlockReason = `eom=${eomDay}, eom-1=${eom1}, opex-1=${opex1}, vixAfterOpex=${vixExpAfterOpex}, earn=${nonAmznTslaEarn}, cpi=${cpiDay}, nm-non-mon=${nmNonMon}`;

    if (calendarWouldBlock) {
      // Look up prior m8bfWR. If ≥ 90 and not CPI day, the 90% override may
      // force M8BF (GXBF firing would suppress this — STEP 2 has VIX to check).
      try {
        const hist_q = await getHistory(env);
        const prevWREntry = (Array.isArray(hist_q) ? hist_q : [])
          .filter(e => e.date < todayISO && e.m8bfWR != null)
          .sort((a, b) => b.date.localeCompare(a.date))[0];
        const prevWR_q = prevWREntry ? parseFloat(prevWREntry.m8bfWR) : null;
        if (prevWR_q != null && prevWR_q >= 90 && !cpiDay) {
          ninetyOverridePossible = true;
        }
      } catch (e) { console.warn('[eod] prevWR lookup for 90% override:', e.message); }

      if (!ninetyOverridePossible) {
        m8bfBlockedByLive = true;
        console.log(`[eod] M8BF blocked by calendar (${calendarBlockReason})`);
      } else {
        console.log(`[eod] Calendar would block M8BF (${calendarBlockReason}) — 90% override possible, deferring to STEP 2`);
      }
    }
  }

  // STEP 2 — gap/o2o/signal-based bans (need vixOpen/spxOpen, only run if morning wrote them).
  // Also resolves the 90% override deferred from STEP 1.
  if (!m8bfBlockedByLive) {
    try {
      const hist0 = await getHistory(env);
      if (Array.isArray(hist0) && hist0.length) {
        const todayE = hist0.find(e => e.date === todayISO);
        const prior = hist0
          .filter(e => e.date < todayISO && e.vixClose != null && e.vixOpen != null)
          .sort((a, b) => b.date.localeCompare(a.date))[0];
        const prevWREntry = hist0
          .filter(e => e.date < todayISO && e.m8bfWR != null)
          .sort((a, b) => b.date.localeCompare(a.date))[0];
        if (todayE && prior && todayE.vixOpen != null) {
          const spxGapPct = (todayE.spxOpen != null && prior.spxClose != null)
            ? ((todayE.spxOpen - prior.spxClose) / prior.spxClose) * 100
            : null;
          const prevWR_s2 = prevWREntry ? parseFloat(prevWREntry.m8bfWR) : null;
          const sig = calculateSignal({
            vixToday: todayE.vixOpen,
            vixYOpen: prior.vixOpen,
            vixYClose: prior.vixClose,
            spxGapPct,
            etDate: etNow,
            prevWR: prevWR_s2,
          });
          { const _g = await gexGateEval(env, isoDateET(etNow)); applyGexGateToSignal(sig, _g.skip, _g.rank); }
          // FIX (2026-06-09 audit P0 #3): m8bfBanned alone misses the 90%-WR
          // override. The override applies when prevWR >= 90, not CPI, and
          // GXBF didn't take precedence (sig.theme !== 'gxbf' — equivalent to
          // signal-engine.js's !rec.includes("GXBF") check). When override
          // applies, M8BF fires even on a calendar-banned day.
          const gxbfTookPrecedence = (sig.theme === 'gxbf');
          const ninetyOverrideFires = (prevWR_s2 != null && prevWR_s2 >= 90 && !sig.cpiDay && !gxbfTookPrecedence);
          // Anchor Filter is absolute — a regime filter, never overridden by the 90% rule.
          const m8bfWouldNotFire = sig.gexGateSkip || ((sig.m8bfBanned || sig.cpiDay) && !ninetyOverrideFires);
          if (m8bfWouldNotFire) {
            m8bfBlockedByLive = true;
            console.log(`[eod] M8BF blocked: m8bfBanned=${sig.m8bfBanned}, cpiDay=${sig.cpiDay}, 90%override=${ninetyOverrideFires}, gxbfTook=${gxbfTookPrecedence}`);
          } else if (ninetyOverrideFires && calendarWouldBlock) {
            console.log(`[eod] 90% override CONFIRMED — calendar block (${calendarBlockReason}) overridden (prevWR=${prevWR_s2})`);
          }
        } else if (ninetyOverridePossible && calendarWouldBlock) {
          // No VIX data yet. 90% override was POSSIBLE but unverifiable.
          // Be conservative: leave m8bfBlockedByLive=false (the qualifying-
          // signal-in-window check below only fires if a real signal was
          // scraped, so we won't fabricate an M8BF P/L on a quiet day).
          console.log(`[eod] No VIX data, 90% override possible — leaving m8bfBlockedByLive=false`);
        }
      }
    } catch (e) { console.warn('[eod] live signal check:', e.message); }
  }

  // Anchor Filter — unconditional (audit [9]): the nested STEP-2 check is skipped
  // when vixOpen is missing; this one cannot be.
  if (!m8bfBlockedByLive && await gexGateSkipFor(env, todayISO)) {
    m8bfBlockedByLive = true;
    console.log('[eod] M8BF blocked by Anchor Filter (unconditional)');
  }
  // Compute m8bfPL from first qualifying signal in window (same logic as backfillMissingPL)
  if (m8bfBlockedByLive) {
    m8bfPL = 0;
  } else if (spxClose != null && fullSigs.length > 0) {
    try {
      const dow = etNow.getDay();
      const win = getM8BFWindow(dow, todayISO);
      if (win) {
        const [winLo, winHi] = win;
        // Honor the manual-cancellation skip list (same KV used by
        // selectM8bfQualifying). Without this, EOD would compute P&L for a
        // signal the user explicitly cancelled — observed 2026-05-22 where
        // EOD recorded m8bfPL from the cancelled 13:02 trade (-$363) instead
        // of the 13:06 trade actually held (-$1,311).
        let skipTimes = new Set();
        try {
          const skipRaw = await env.SIGNAL_KV.get(`m8bf_skip_signals_${todayISO}`);
          if (skipRaw) skipTimes = new Set(JSON.parse(skipRaw) || []);
        } catch (_) { /* no-op */ }

        let qualifying = null;
        for (const sig of fullSigs) {
          if (!sig.time) continue;
          if (skipTimes.has(sig.time)) continue;   // ← manual cancellation
          const [h, m] = sig.time.split(':').map(Number);
          const mins = h * 60 + m;
          if (mins >= winLo && mins < winHi && !isBanned(sig.center, sig.lower, sig.t1)) {
            qualifying = sig;
            break;
          }
        }
        if (qualifying) {
          const lo = qualifying.lower, hi = qualifying.upper;
          const wing = (hi - lo) / 2;
          const intrinsic = Math.max(0, Math.min(spxClose - lo, hi - spxClose));
          const clipped = Math.min(intrinsic, wing);
          m8bfPL = Math.round((clipped - qualifying.premium) * 100);
          console.log(`[eod] m8bfPL computed: $${m8bfPL} (center=${qualifying.center}, premium=${qualifying.premium}, spxClose=${spxClose})`);
        } else {
          // No qualifying signal in window — also a skip day for the backtester
          m8bfPL = 0;
          m8bfBlockedByLive = true;
          console.log('[eod] No qualifying signal in window — marking as skip day');
        }
      }
    } catch (e) { console.warn('[eod] m8bfPL compute:', e.message); }
  }

  // Backfill vixOpen/spxOpen if morning signal missed them
  let vixOpen = null, spxOpen = null;
  try {
    const hist = await getHistory(env);
    if (Array.isArray(hist) && hist.length) {
      const todayEntry = hist.find(e => e.date === todayISO);
      if (todayEntry && todayEntry.vixOpen == null && token) {
        // Fetch VIX open from candles
        try {
          const vixHist2 = await fetchSchwabJSON(`https://api.schwabapi.com/marketdata/v1/pricehistory?symbol=%24VIX&periodType=day&period=3&frequencyType=minute&frequency=1&startDate=${start}&endDate=${end}&needExtendedHoursData=true`, token);
          if (vixHist2.candles) {
            const openCandles = vixHist2.candles.filter(c => {
              const d = toET(new Date(c.datetime));
              return d.toDateString() === todayStr && d.getHours() === 9 && d.getMinutes() >= 30 && d.getMinutes() <= 35;
            }).sort((a, b) => a.datetime - b.datetime);
            if (openCandles.length) vixOpen = parseFloat(openCandles[0].open.toFixed(2));
          }
        } catch (e) { console.warn('[eod] vixOpen backfill:', e.message); }
      }
      if (todayEntry && todayEntry.spxOpen == null && token) {
        try {
          const spxQ = await fetchSchwabJSON(`https://api.schwabapi.com/marketdata/v1/quotes?symbols=%24SPX&fields=quote`, token);
          const op = spxQ?.['$SPX']?.quote?.openPrice;
          if (op) spxOpen = parseFloat(op.toFixed(2));
        } catch (e) { console.warn('[eod] spxOpen backfill:', e.message); }
      }
    }
  } catch (e) { console.warn('[eod] open backfill check:', e.message); }

  const fields = {};
  if (vixClose != null) fields.vixClose = vixClose;
  if (spxClose != null) fields.spxClose = spxClose;
  if (m8bfPL != null) fields.m8bfPL = m8bfPL;
  if (m8bfWR != null) fields.m8bfWR = m8bfWR;
  if (vixOpen != null) fields.vixOpen = vixOpen;
  if (spxOpen != null) fields.spxOpen = spxOpen;

  // cor1m from the worker's own cloud capture (2026-06-09) — fills the
  // history column with zero dependence on the user's Mac. Upsert merge
  // only fills when null, so a LaunchAgent-written value is never clobbered.
  try {
    const kvCor = await env.SIGNAL_KV.get(`cor1m_open_${todayISO}`);
    if (kvCor) {
      const o = JSON.parse(kvCor);
      if (o.cor1m != null) fields.cor1m = o.cor1m;
    }
  } catch (_) { /* non-critical */ }

  // wroteFields tells callers whether history_data.json was actually updated.
  // Callers use this to decide if they should set `eod_done_<date>` — we only
  // want to set it after a real write, otherwise a failed EOD (expired Schwab
  // token + Stooq hiccup) locks out every later retry for the day.
  let wroteFields = false;
  if (Object.keys(fields).length > 0) {
    await upsertHistoryGitHub(env, todayISO, fields);
    wroteFields = true;
  }

  // Settle the live-tracked straddle (if any) at SPX close → writes stradPL.
  // upsertHistoryGitHub conditional-overwrite means it won't clobber a manually-
  // set stradPL — only fills if currently null.
  if (spxClose != null) {
    try {
      const stradResult = await settleStraddleEOD(env, etNow, spxClose);
      console.log('[strad] EOD settle:', JSON.stringify(stradResult));
    } catch (e) { console.warn('[strad] EOD settle failed:', e.message); }
    try {
      const bobfResult = await settleBobfEOD(env, etNow, spxClose);
      console.log('[bobf] EOD settle:', JSON.stringify(bobfResult));
    } catch (e) { console.warn('[bobf] EOD settle failed:', e.message); }
    try {
      const gxbfResult = await settleGxbfEOD(env, etNow, spxClose);
      console.log('[gxbf] EOD settle:', JSON.stringify(gxbfResult));
    } catch (e) { console.warn('[gxbf] EOD settle failed:', e.message); }
    try {
      const tailResult = await settleTailEOD(env, etNow, spxClose);
      console.log('[tail] EOD settle:', JSON.stringify(tailResult));
    } catch (e) { console.warn('[tail] EOD settle failed:', e.message); }
  }

  // Append today's signals to TRADES database in backtester.html (reuse cached fullSigs)
  let appendResult = { appended: 0 };
  if (spxClose != null && fullSigs.length > 0) {
    try {
      appendResult = await appendTradesToBacktester(env, todayISO, etNow, fullSigs, spxClose, m8bfBlockedByLive);
    } catch (e) {
      appendResult = { appended: 0, error: e.message };
    }
  }

  return { status: 'eod', date: todayISO, vixClose, spxClose, m8bfPL, wroteFields, trades: appendResult };
}

// ════════════════════════════════════════════════════════════════════
// DISCORD SIGNAL POLLING
// ════════════════════════════════════════════════════════════════════

function parseDiscordSignal(content) {
  // Format: BUY +1 Butterfly SPX 100 ... 6455/6405/6355 CALL @14.25 LMT
  // Strikes are typically posted high→low for CALLs and low→high for PUTs.
  // FIX (2026-06-09 audit P0 #8): NORMALIZE strike ordering so that
  // lower < center < upper regardless of CALL/PUT post format. Without this,
  // PUT butterflies posted low→high (e.g. 6355/6405/6455 PUT) produced
  // upper=6355 < lower=6455, and the P/L formula
  //   max(0, min(spxClose - lower, upper - spxClose))
  // returned 0 intrinsic for ANY spxClose between strikes → silent
  // m8bfPL = -premium*100 regardless of actual outcome.
  const strikeMatch = content.match(/BUY \+1 Butterfly SPX[^/]*(\d{4,5})\/(\d{4,5})\/(\d{4,5})\s*(CALL|PUT)\s*@([\d.]+)/i);
  if (!strikeMatch) return null;
  const s1 = parseInt(strikeMatch[1]);
  const s2 = parseInt(strikeMatch[2]);
  const s3 = parseInt(strikeMatch[3]);
  const cpStr = (strikeMatch[4] || 'CALL').toUpperCase();
  const cp = cpStr === 'PUT' ? 1 : 0; // 0=CALL, 1=PUT
  const premium = parseFloat(strikeMatch[5]);
  if (isNaN(s1) || isNaN(s2) || isNaN(s3) || isNaN(premium)) return null;
  // Sort to canonical order: lower < center < upper. Wing structure is
  // symmetric around center for a standard butterfly, but defensive sort
  // handles edge cases (asymmetric flies, mis-typed posts) too.
  const [lower, center, upper] = [s1, s2, s3].sort((a, b) => a - b);
  // T1 from "Target 1: XXXX"
  const t1Match = content.match(/Target\s*1[:\s]+(\d{4,5})/i);
  const t1 = t1Match ? parseInt(t1Match[1]) : center + 5;
  return { center, upper, lower, t1, premium, cp };
}

// M8BF banned-strike check.
//
// FULL ban: center % 100 ∈ {10, 25, 35, 40, 65, 80}.
// COMBO ban: M8BF_COMBO_BANS[t1 % 100] === center % 100  → BANNED.
//   COMBO_BANS = { 0:95, 20:15, 55:50, 65:60, 85:90 }
//   t1 is the Discord "Target 1" field (from `Target 1: XXXX` in the post),
//   distinct from `lower` (= center − wing). The rule is keyed off t1's last
//   two digits, NOT lower's and NOT (center − lower).
//
// Example (today's 09:36 signal): center=7495 (cmod=95), t1=7500 (t1mod=0).
//   COMBO_BANS[0] === 95 → BANNED. With the previous (center−lower) keying
//   this got `spread%100 = 50`, missed the ban, and let the trade through.
//
// Reference unit-test cases:
//   isBanned(7495, t1=7500) === true   (t1%100=0, COMBO_BANS[0]=95, center%100=95)
//   isBanned(7395, t1=7400) === true   (same combo)
//   isBanned(7395, t1=7355) === false  (t1%100=55, COMBO_BANS[55]=50, center%100=95 ≠ 50)
function isBanned(center, lower, t1) {
  const FULL_BANS = new Set([10, 25, 35, 40, 65, 80]);
  const COMBO_BANS = { 0: 95, 20: 15, 55: 50, 65: 60, 85: 90 };
  if (FULL_BANS.has(center % 100)) return true;
  if (t1 != null) {
    const t1Mod = ((t1 % 100) + 100) % 100;
    if (COMBO_BANS[t1Mod] !== undefined && center % 100 === COMBO_BANS[t1Mod]) return true;
  }
  return false;
}

async function pollDiscordSignals(env) {
  const token = env.DISCORD_USER_TOKEN;
  const channelId = '1048242197029458040';
  if (!token) return { polled: false, reason: 'no token' };

  const etNow = toET(new Date());
  const todayISO = `${etNow.getFullYear()}-${String(etNow.getMonth()+1).padStart(2,'0')}-${String(etNow.getDate()).padStart(2,'0')}`;

  // Load existing signals from KV
  const existingRaw = await env.SIGNAL_KV.get('signals_today');
  const existing = existingRaw ? JSON.parse(existingRaw) : { date: '', signals: [] };

  let signals = [];
  let afterId = null;

  if (existing.date === todayISO) {
    signals = existing.signals || [];
    afterId = await env.SIGNAL_KV.get('discord_last_msg_id');
  }

  // First poll of the day — start from midnight UTC today
  if (!afterId) {
    const midnight = new Date(); midnight.setUTCHours(0, 0, 0, 0);
    const discordEpoch = 1420070400000n;
    afterId = ((BigInt(midnight.getTime()) - discordEpoch) << 22n).toString();
  }

  const apiUrl = `https://discord.com/api/v9/channels/${channelId}/messages?limit=100&after=${afterId}`;
  let messages;
  try {
    const resp = await fetch(apiUrl, {
      headers: {
        'Authorization': token,
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
      },
    });
    if (!resp.ok) return { polled: false, status: resp.status };
    messages = await resp.json();
    if (!Array.isArray(messages)) return { polled: false, reason: 'bad response' };
  } catch (e) {
    return { polled: false, error: e.message };
  }

  if (!messages.length) return { polled: true, newSignals: 0, total: signals.length };

  // Sort oldest → newest
  messages.sort((a, b) => a.id.localeCompare(b.id));

  // Save latest message ID
  await env.SIGNAL_KV.put('discord_last_msg_id', messages[messages.length - 1].id);

  const seenMsgIds = new Set(signals.map(s => s.msgId).filter(Boolean));
  let newCount = 0;

  for (const msg of messages) {
    if (seenMsgIds.has(msg.id)) continue;

    const msgET = toET(new Date(msg.timestamp));
    const msgISO = `${msgET.getFullYear()}-${String(msgET.getMonth()+1).padStart(2,'0')}-${String(msgET.getDate()).padStart(2,'0')}`;
    if (msgISO !== todayISO) continue;

    const sig = parseDiscordSignal(msg.content || '');
    if (!sig) continue;

    signals.push({
      time: `${String(msgET.getHours()).padStart(2,'0')}:${String(msgET.getMinutes()).padStart(2,'0')}`,
      center: sig.center,
      lower: sig.lower,
      upper: sig.upper,
      t1: sig.t1,
      premium: sig.premium,
      cp: sig.cp ?? 0,
      banned: isBanned(sig.center, sig.lower, sig.t1),
      msgId: msg.id,
    });
    seenMsgIds.add(msg.id);
    newCount++;
  }

  await env.SIGNAL_KV.put('signals_today', JSON.stringify({ date: todayISO, signals }));
  return { polled: true, newSignals: newCount, total: signals.length };
}

// ════════════════════════════════════════════════════════════════════
// DIAGONAL TRADE HANDLER (live)
// Fires at 12:30 ET each weekday: closes prior open trade, then opens
// a new one if signal-engine.js says so. State lives in KV at:
//   diagonal_open_trade  — the currently-active trade (one at a time)
//   diagonal_closed_log  — last 30 closed trades (for live page tape)
// Live page polls /diagonal-today which reads KV.
// Strikes: short = round5(spot+30), long = K_short - 40 (canonical 30/40).
// Expiries: short = next trading day, long = ~25 trading days out.
// ════════════════════════════════════════════════════════════════════

const DIAG_SHORT_OFFSET = 10;     // pts ITM (10 ITM — safer-tail config, 2026-06-09)
const DIAG_LONG_OFFSET  = 20;     // pts BELOW short (so 10 OTM relative to spot; width=20)
const DIAG_LONG_DTE     = 25;     // CALENDAR days target (matches Python long_dte=25)
const DIAG_LONG_DTE_TOL = 5;      // ±5 calendar days → 20-30 DTE acceptable range

function snap5(x) { return Math.round(x / 5) * 5; }

function isoDateET(d) {
  return `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}-${String(d.getDate()).padStart(2,'0')}`;
}

function nextTradeDayET(etDate) {
  const d = new Date(etDate);
  d.setHours(12, 0, 0, 0);
  do { d.setDate(d.getDate() + 1); } while (d.getDay() === 0 || d.getDay() === 6 || isHol(d));
  return d;
}

function addTradeDaysET(etDate, n) {
  const d = new Date(etDate);
  d.setHours(12, 0, 0, 0);
  let added = 0;
  while (added < n) {
    d.setDate(d.getDate() + 1);
    if (d.getDay() === 0 || d.getDay() === 6) continue;
    if (isHol(d)) continue;
    added++;
  }
  return d;
}

// Look up a put quote at the requested strike. Schwab chain map keys are
// "YYYY-MM-DD:DTE"; strikes are dotted strings ("7290.0"). We tolerate
// ±5 strike fuzz so a missing exact strike falls back to the closest neighbor.
function pickPutFromChain(putExpDateMap, expISO, strike) {
  const targetKey = Object.keys(putExpDateMap).find(k => k.startsWith(expISO + ':'));
  if (!targetKey) return null;
  const strikes = putExpDateMap[targetKey];
  // Try exact, then ±5, ±10
  for (const offset of [0, -5, 5, -10, 10]) {
    const k = String(strike + offset).includes('.') ? String(strike + offset) : String(strike + offset) + '.0';
    if (strikes[k] && strikes[k][0]) {
      const q = strikes[k][0];
      return {
        strike: parseFloat(q.strikePrice ?? (strike + offset)),
        bid: q.bid,
        ask: q.ask,
        mid: (q.bid != null && q.ask != null) ? (q.bid + q.ask) / 2 : null,
        symbol: q.symbol,
        expirationDate: q.expirationDate,
        daysToExpiration: q.daysToExpiration,
      };
    }
  }
  return null;
}

// Fetch SPX put chains for a date-range covering both legs in one call.
// fromDate / toDate are YYYY-MM-DD. Returns {spot, putExpDateMap}.
// Tries Schwab first; falls back to Tasty if Schwab token is dead or call fails.
async function fetchSpxPutChain(token, fromDate, toDate, env) {
  if (token) {
    try {
      const params = new URLSearchParams({
        symbol: '$SPX',
        contractType: 'PUT',
        fromDate, toDate,
        strikeCount: '60',
        includeUnderlyingQuote: 'true',
        strategy: 'SINGLE',
      });
      const data = await fetchSchwabJSON(`https://api.schwabapi.com/marketdata/v1/chains?${params}`, token, env);
      const spot = data.underlyingPrice || data.underlying?.last || data.underlying?.mark;
      return { spot, putExpDateMap: data.putExpDateMap || {}, _source: 'schwab' };
    } catch (e) {
      console.warn(`[fetchSpxPutChain] Schwab failed (${fromDate}..${toDate}) → Tasty fallback:`, e.message);
    }
  } else {
    console.warn(`[fetchSpxPutChain] no Schwab token (${fromDate}..${toDate}) → direct Tasty`);
  }
  // Tasty fallback: pull PUT chain across the date range.
  const exps = [];
  try {
    const start = new Date(fromDate + 'T12:00:00Z');
    const end = new Date(toDate + 'T12:00:00Z');
    for (let d = new Date(start); d <= end; d.setUTCDate(d.getUTCDate() + 1)) {
      exps.push(d.toISOString().slice(0, 10));
    }
  } catch (_) { exps.push(fromDate, toDate); }
  return tastyFetchSpxChain(env, { root: 'SPXW', strikeCount: 60, contractType: 'PUT', expirations: exps });
}

// Open a diagonal at 12:30 ET. Returns the trade record (or throws).
async function openDiagonalTrade(env, token, etNow, vixPct20d, preChain = null) {
  const todayISO = isoDateET(etNow);
  const shortExp = isoDateET(nextTradeDayET(etNow));
  // Calendar days, NOT trading days — matches Python long_dte=25.
  // Trading-day version pushed expiry ~10 calendar days too far (36 DTE
  // instead of 25-26 — observed 2026-05-07 picked Jun 12 instead of Jun 1).
  const _longTarget = new Date(etNow);
  _longTarget.setDate(_longTarget.getDate() + DIAG_LONG_DTE);
  const longExpTarget = isoDateET(_longTarget);

  // Fetch full chain spanning both expiries — reuse master if it covers them.
  const chain = await chainOrFetch(preChain, token, env, [shortExp, longExpTarget], 'PUT');
  const spot = chain.spot;
  const putExpDateMap = chain.putExpDateMap;
  if (!spot) throw new Error('Diagonal open: no spot price in chain response');

  const reqShort = snap5(spot + DIAG_SHORT_OFFSET);
  const shortLeg = pickPutFromChain(putExpDateMap, shortExp, reqShort);
  if (!shortLeg) throw new Error(`Diagonal open: no SPX ${reqShort}P @ ${shortExp} in chain`);
  const kShort = shortLeg.strike;            // actual filled short strike (post-fuzz)
  const kLong  = kShort - DIAG_LONG_OFFSET;  // re-anchor long on actual short

  // Long leg — find the actual expiry closest to target within tolerance
  let longLeg = null, longExpUsed = null;
  const candidateExps = Object.keys(putExpDateMap)
    .map(k => k.split(':')[0])
    .filter(d => d > shortExp);  // strictly later than short
  candidateExps.sort((a, b) => Math.abs(daysBetween(longExpTarget, a)) - Math.abs(daysBetween(longExpTarget, b)));
  for (const expISO of candidateExps) {
    const dteDiff = Math.abs(daysBetween(longExpTarget, expISO));
    if (dteDiff > DIAG_LONG_DTE_TOL) break;  // sorted, so done
    const candidate = pickPutFromChain(putExpDateMap, expISO, kLong);
    if (candidate && candidate.mid != null) { longLeg = candidate; longExpUsed = expISO; break; }
  }
  if (!longLeg) throw new Error(`Diagonal open: no long leg ~${kLong}P near ${longExpTarget}`);

  if (shortLeg.mid == null || longLeg.mid == null) throw new Error('Diagonal open: missing bid/ask on a leg');

  // Three pricings for the same diagonal:
  //   debit       = longMid  - shortMid  → theoretical fair value (mid-to-mid)
  //   askFill     = longAsk  - shortBid  → worst-case fill cost (BUY long at ask, SELL short at bid)
  //   bidExit     = longBid  - shortAsk  → worst-case close credit (SELL long at bid, BUY short at ask)
  // Real-world fills on SPX put diagonals typically land between debit and
  // askFill. The historical entryDebit field (mid-mid) is preserved as-is so
  // P&L math and prior records don't shift; askFill/bidExit are additive.
  const debit   = longLeg.mid - shortLeg.mid;
  const askFill = longLeg.ask - shortLeg.bid;
  const bidExit = longLeg.bid - shortLeg.ask;

  // Daily total-risk cap (2026-06-09): diagonal max loss = debit + width
  // (crash through both strikes inverts the spread — see index.html sizing).
  const diagWidth = kShort - longLeg.strike;
  const diagGate = await enforceRiskCap(env, etNow, 'diagonal', (debit + diagWidth) * 100);
  if (!diagGate.ok) throw new Error(`risk-cap blocked diagonal open: ${diagGate.reason}`);

  // FIX (2026-06-09 audit P0 #7): record ACTUAL filled long strike, not the
  // pre-fuzz target. pickPutFromChain() tolerates ±5-10 strike fuzz; if the
  // chain didn't have the exact target 7250P but had 7245P, longLeg.strike is
  // 7245 while kLong is still 7250. Storing kLong made the closer at line
  // ~1620 look up the wrong leg if the target strike came back into the chain
  // by close time. shortStrike already uses kShort = shortLeg.strike (line 1490)
  // — apply the same pattern to longStrike.
  const trade = {
    openDate: todayISO,
    openTimeET: `${String(etNow.getHours()).padStart(2,'0')}:${String(etNow.getMinutes()).padStart(2,'0')}`,
    spotEntry: parseFloat(spot.toFixed(2)),
    vixPct20d,
    shortStrike: kShort,
    longStrike: longLeg.strike,
    shortExp,
    longExp: longExpUsed,
    // shortDte was hard-coded to 1 — wrong on long weekends (Fri Memorial Day
    // open → Tue expiry is 4 calendar days, not 1). Use calendar-day diff so
    // shortDte mirrors longDte's semantics.
    shortDte: Math.abs(daysBetween(todayISO, shortExp)),
    longDte:  Math.abs(daysBetween(todayISO, longExpUsed)),
    shortSymbol: shortLeg.symbol,
    longSymbol: longLeg.symbol,
    entryShortMid: parseFloat(shortLeg.mid.toFixed(2)),
    entryLongMid: parseFloat(longLeg.mid.toFixed(2)),
    entryShortBid: shortLeg.bid,
    entryShortAsk: shortLeg.ask,
    entryLongBid: longLeg.bid,
    entryLongAsk: longLeg.ask,
    entryDebit: parseFloat(debit.toFixed(2)),
    entryAskFill: parseFloat(askFill.toFixed(2)),   // realistic worst-case fill
    entryBidExit: parseFloat(bidExit.toFixed(2)),   // realistic worst-case close
    contracts: 1,
    // Live fields — refreshed by every market-hours cron tick
    currentSpot: parseFloat(spot.toFixed(2)),
    currentShortMid: parseFloat(shortLeg.mid.toFixed(2)),
    currentLongMid: parseFloat(longLeg.mid.toFixed(2)),
    currentValue: parseFloat(debit.toFixed(2)),
    currentAskFill: parseFloat(askFill.toFixed(2)),
    currentBidExit: parseFloat(bidExit.toFixed(2)),
    currentPnl: 0,
    lastQuoteAt: new Date().toISOString(),
    status: 'open',
  };
  return trade;
}

// Refresh just the live-quote fields on the open trade. Cheap call.
async function refreshDiagonalLiveQuotes(env, token, preChain = null) {
  const raw = await env.SIGNAL_KV.get('diagonal_open_trade');
  if (!raw) return null;
  const trade = JSON.parse(raw);
  if (trade.status !== 'open') return null;

  // DEFENSIVE: phantom-trade cleanup. If today's close+open lifecycle already
  // ran (diag_done_<today> set) AND the open trade is from a prior day, the
  // delete in handleDiagonalTrade silently failed (CF KV occasionally drops
  // a write). Clear it now and bail — no quotes to refresh on a closed trade.
  const todayISO = isoDateET(toET());
  if (trade.openDate && trade.openDate < todayISO) {
    const diagDone = await env.SIGNAL_KV.get(`diag_done_${todayISO}`);
    if (diagDone && !diagDone.startsWith('claim:')) {   // claim: = lifecycle IN FLIGHT, not done (P24)
      console.warn(`[diag] phantom open trade detected (openDate=${trade.openDate}, diag_done set) — clearing`);
      await logEvent(env, 'warn', 'diag-phantom', 'phantom open trade cleared at refresh path',
                     { openDate: trade.openDate });
      await env.SIGNAL_KV.delete('diagonal_open_trade');
      return null;
    }
  }

  // Re-fetch the chain spanning both leg expiries — but reuse master chain
  // if it covers what we need (it almost always will).
  try {
    const chain = await chainOrFetch(preChain, token, env, [trade.shortExp, trade.longExp], 'PUT');
    const spot = chain.spot;
    const putExpDateMap = chain.putExpDateMap;
    const sNow = pickPutFromChain(putExpDateMap, trade.shortExp, trade.shortStrike);
    const lNow = pickPutFromChain(putExpDateMap, trade.longExp, trade.longStrike);
    if (!sNow || !lNow || sNow.mid == null || lNow.mid == null) return trade;
    trade.currentSpot = spot ? parseFloat(spot.toFixed(2)) : trade.currentSpot;
    trade.currentShortMid = parseFloat(sNow.mid.toFixed(2));
    trade.currentLongMid = parseFloat(lNow.mid.toFixed(2));
    trade.currentValue = parseFloat((lNow.mid - sNow.mid).toFixed(2));
    // Realistic fill estimates — what you'd actually pay to open / get to close
    // RIGHT NOW. Pulled from the same chain refresh as the mid values.
    if (sNow.ask != null && sNow.bid != null && lNow.ask != null && lNow.bid != null) {
      trade.currentAskFill = parseFloat((lNow.ask - sNow.bid).toFixed(2));
      trade.currentBidExit = parseFloat((lNow.bid - sNow.ask).toFixed(2));
    }
    trade.currentPnl = Math.round((trade.currentValue - trade.entryDebit) * 100 * trade.contracts);
    trade.lastQuoteAt = new Date().toISOString();
    await env.SIGNAL_KV.put('diagonal_open_trade', JSON.stringify(trade));
  } catch (e) {
    console.warn('[diag] refresh quotes failed:', e.message);
  }
  return trade;
}

// Close a trade at the current chain. Mutates input trade with close fields,
// returns the realized PnL.
async function closeDiagonalTrade(env, token, openTrade, etNow, preChain = null) {
  const closeISO = isoDateET(etNow);
  // For an expired short (closeISO >= shortExp), price intrinsic = max(K - SPX, 0)
  const chain = await chainOrFetch(preChain, token, env, [openTrade.shortExp, openTrade.longExp], 'PUT');
  const spot = chain.spot;
  const putExpDateMap = chain.putExpDateMap;

  let closeShortMid, closeLongMid;
  const sNow = pickPutFromChain(putExpDateMap, openTrade.shortExp, openTrade.shortStrike);
  const lNow = pickPutFromChain(putExpDateMap, openTrade.longExp, openTrade.longStrike);

  if (sNow && sNow.mid != null) {
    closeShortMid = sNow.mid;
  } else if (closeISO >= openTrade.shortExp && spot != null) {
    closeShortMid = Math.max(openTrade.shortStrike - spot, 0);  // expired intrinsic
  } else {
    throw new Error('Diagonal close: missing short leg quote');
  }
  if (lNow && lNow.mid != null) {
    closeLongMid = lNow.mid;
  } else {
    throw new Error('Diagonal close: missing long leg quote');
  }

  const closeValue = closeLongMid - closeShortMid;
  const pnl = (closeValue - openTrade.entryDebit) * 100 * openTrade.contracts;

  const closed = {
    ...openTrade,
    status: 'closed',
    closeDate: closeISO,
    closeTimeET: `${String(etNow.getHours()).padStart(2,'0')}:${String(etNow.getMinutes()).padStart(2,'0')}`,
    spotExit: spot ? parseFloat(spot.toFixed(2)) : null,
    closeShortMid: parseFloat(closeShortMid.toFixed(2)),
    closeLongMid: parseFloat(closeLongMid.toFixed(2)),
    closeValue: parseFloat(closeValue.toFixed(2)),
    pnl: Math.round(pnl),
  };
  return closed;
}

// Helper: integer days between two YYYY-MM-DD strings (calendar days).
function daysBetween(a, b) {
  const da = new Date(a + 'T12:00:00Z');
  const db = new Date(b + 'T12:00:00Z');
  return Math.round((db - da) / 86400000);
}

// Orchestrate close-then-open at 12:30 ET. Idempotent via diag_done_<date>.
async function handleDiagonalTrade(env, etNow, preChain = null) {
  const todayISO = isoDateET(etNow);
  const out = { date: todayISO, closed: null, opened: null, skipped: null };

  let token;
  try { token = await getAccessToken(env); }
  catch (e) { return { ...out, error: 'token: ' + e.message }; }

  // 1. Close prior open trade (if any & opened on a different day)
  const openRaw = await env.SIGNAL_KV.get('diagonal_open_trade');
  let openTrade = openRaw ? JSON.parse(openRaw) : null;

  if (openTrade && openTrade.openDate < todayISO && openTrade.status === 'open') {
    try {
      const closed = await closeDiagonalTrade(env, token, openTrade, etNow, preChain);
      // Commit diagPL for the OPEN date (matches history convention)
      await upsertHistoryGitHub(env, openTrade.openDate, { diagPL: closed.pnl });
      // Append to closed log
      const logRaw = await env.SIGNAL_KV.get('diagonal_closed_log');
      const log = logRaw ? JSON.parse(logRaw) : [];
      log.unshift(closed);
      await env.SIGNAL_KV.put('diagonal_closed_log', JSON.stringify(log.slice(0, 30)));
      // Clear the slot
      await env.SIGNAL_KV.delete('diagonal_open_trade');
      out.closed = { openDate: closed.openDate, closeDate: closed.closeDate, pnl: closed.pnl };
    } catch (e) {
      out.closeError = e.message;
      console.warn('[diag] close failed:', e.message);
      // Don't open a new trade if close failed — manual recovery needed
      return out;
    }
  }

  // 2. Compute today's vixPct20d for signal check (mirror handleScheduled logic)
  let vixPct20d = null;
  let vixToday = null;
  try {
    const histData = await getHistory(env);
    if (Array.isArray(histData) && histData.length) {
      const todayRow = histData.find(r => r.date === todayISO);
      vixToday = todayRow?.vixOpen != null ? parseFloat(todayRow.vixOpen) : null;
      // Same fallback as /diagonal-today: prior vixClose if today's vixOpen missing.
      if (vixToday == null) {
        const prior = histData
          .filter(r => r.date < todayISO && r.vixClose != null && r.vixClose > 0)
          .sort((a, b) => a.date.localeCompare(b.date));
        if (prior.length) vixToday = parseFloat(prior[prior.length - 1].vixClose);
      }
      const vix20 = histData
        .filter(r => r.date < todayISO && r.vixClose != null && r.vixClose > 0)
        .sort((a, b) => a.date.localeCompare(b.date))
        .slice(-20)
        .map(r => parseFloat(r.vixClose));
      // CANONICAL — see signal-engine.js computeVixPct20d (single source of
      // truth used by both this worker and the backtester).
      vixPct20d = computeVixPct20d(vixToday, vix20).pct;
    }
  } catch (e) { /* signal will be 'pending' if no vixPct20d */ }

  // 3a. Today's COR1M for the Diagonal gate (COR1M < 10 → no trade).
  //     CLOUD-FIRST (2026-06-09): the worker's own Schwab capture
  //     (cor1m_open_<date>, written ~9:30 ET) — machine-independent.
  //     Bundle fallback only if the capture is missing.
  let cor1mToday = null;
  try {
    const kvOpen = await env.SIGNAL_KV.get(`cor1m_open_${todayISO}`);
    if (kvOpen) {
      const o = JSON.parse(kvOpen);
      if (o.cor1m != null) cor1mToday = parseFloat(o.cor1m);
    }
  } catch (_) {}
  if (cor1mToday == null) {
    try {
      const r = await fetch('https://raw.githubusercontent.com/rava8989/brave/main/cor1m_contango_bundle.json',
        { headers: { 'User-Agent': 'schwab-proxy-worker/1.0' } });
      if (r.ok) {
        const bundle = await r.json();
        const todayRow = (bundle?.daily || []).find(d => d?.date === todayISO);
        if (todayRow?.cor1m != null) cor1mToday = parseFloat(todayRow.cor1m);
      }
    } catch (_) { /* leave null — signal-engine will defer with "pending COR1M data" */ }
  }

  // 3b. Compute diagonal signal (now COR1M-gated).
  const sig = computeDiagonalSignal(etNow, vixPct20d, cor1mToday);
  if (!sig.diagGo) {
    out.skipped = sig.diagSkipCode || 'no-data';
    out.signalText = sig.diagText;
    return out;
  }

  // 4. Open new trade
  try {
    const newTrade = await openDiagonalTrade(env, token, etNow, vixPct20d, preChain);
    await env.SIGNAL_KV.put('diagonal_open_trade', JSON.stringify(newTrade));
    out.opened = {
      openDate: newTrade.openDate,
      shortStrike: newTrade.shortStrike,
      longStrike: newTrade.longStrike,
      shortExp: newTrade.shortExp,
      longExp: newTrade.longExp,
      entryDebit: newTrade.entryDebit,
    };
  } catch (e) {
    out.openError = e.message;
    console.warn('[diag] open failed:', e.message);
  }

  return out;
}

// ════════════════════════════════════════════════════════════════════
// STRADDLE TRADE HANDLER (live)
// Lifecycle:
//   9:30-9:32 ET → if calculateSignal says theme==='strad', open trade
//      Fetch ATM 0DTE call+put, compute mid debit
//      If mid ≤ max_debit → fill at mid
//      Else → place working limit at max_debit, status='working'
//   Every 2-min market tick before 13:30 ET → refresh working orders.
//      If current mid ≤ max_debit → fill, status='filled'
//   13:30 ET → expire any still-working orders, status='expired'
//   16:15 ET (EOD) → if filled, compute pnl using SPX close, write stradPL
// Max debit: NM + plain Straddle = $32, EOM Straddle = $35.
// (Plain regular-day cap lowered from $35 → $32 on 2026-05-22 per user.)
// KV keys:
//   straddle_open_trade  — current trade or null
//   straddle_done_<date> — idempotency for EOD record
// ════════════════════════════════════════════════════════════════════

const STRADDLE_MAX_DEBIT_NM    = 32.00;  // NM Straddle: $3,200 max risk
const STRADDLE_MAX_DEBIT_EOM   = 35.00;  // EOM Straddle: $3,500 max risk
const STRADDLE_MAX_DEBIT_OTHER = 32.00;  // Plain regular-day Straddle: $3,200 max risk
const STRADDLE_WORK_CUTOFF_HR  = 13;     // 13:30 ET cutoff
const STRADDLE_WORK_CUTOFF_MIN = 30;

// Pick a call OR put quote at the requested strike from chain map.
function pickContractFromChain(expDateMap, expISO, strike) {
  const targetKey = Object.keys(expDateMap).find(k => k.startsWith(expISO + ':'));
  if (!targetKey) return null;
  const strikes = expDateMap[targetKey];
  for (const offset of [0, -5, 5, -10, 10]) {
    const k = String(strike + offset).includes('.') ? String(strike + offset) : String(strike + offset) + '.0';
    if (strikes[k] && strikes[k][0]) {
      const q = strikes[k][0];
      return {
        strike: parseFloat(q.strikePrice ?? (strike + offset)),
        bid: q.bid, ask: q.ask,
        mid: (q.bid != null && q.ask != null) ? (q.bid + q.ask) / 2 : null,
        symbol: q.symbol,
      };
    }
  }
  return null;
}

// Fetch full SPX option chain (call+put) for a single expiry. Used for
// straddle entry + monitoring. Mirrors GEX fetch pattern.
// Tries Schwab first; falls back to Tasty if Schwab token is dead or call fails.
async function fetchSpxFullChain(token, expDate, env) {
  if (token) {
    try {
      const baseParams = `symbol=%24SPX&strikeCount=20&fromDate=${expDate}&toDate=${expDate}&includeUnderlyingQuote=true&strategy=SINGLE`;
      const [callData, putData] = await Promise.all([
        fetchSchwabJSON(`https://api.schwabapi.com/marketdata/v1/chains?${baseParams}&contractType=CALL`, token, env),
        fetchSchwabJSON(`https://api.schwabapi.com/marketdata/v1/chains?${baseParams}&contractType=PUT`, token, env),
      ]);
      const spot = callData.underlyingPrice || callData.underlying?.last
                || putData.underlyingPrice  || putData.underlying?.last;
      return {
        spot,
        callExpDateMap: callData.callExpDateMap || {},
        putExpDateMap:  putData.putExpDateMap  || {},
        _source: 'schwab',
      };
    } catch (e) {
      console.warn(`[fetchSpxFullChain] Schwab failed (${expDate}) → Tasty fallback:`, e.message);
    }
  } else {
    console.warn(`[fetchSpxFullChain] no Schwab token (${expDate}) → direct Tasty`);
  }
  return tastyFetchSpxChain(env, { root: 'SPXW', strikeCount: 20, expirations: [expDate] });
}

// Master SPX chain — fetched ONCE per cron tick and passed to every handler
// (GEX, straddle, BOBF, diagonal). strikeCount=80 — UNCHANGED shared trading
// chain. Every strategy picks specific near-money strikes well within this band,
// so this is the live trade-execution feed and must not move for a GEX display
// tweak. (GEX's wider ±8% window/curve operate on whatever strikes this chain
// provides; a dedicated wide GEX fetch can be added later if needed.)
// No date range = Schwab returns all available expiries, 0DTE through ~30+ DTE.
async function fetchMasterSpxChain(token, env) {
  if (token) {
    try {
      const baseParams = 'symbol=%24SPX&strikeCount=80&includeUnderlyingQuote=true&strategy=SINGLE';
      const [callData, putData] = await Promise.all([
        fetchSchwabJSON(`https://api.schwabapi.com/marketdata/v1/chains?${baseParams}&contractType=CALL`, token, env),
        fetchSchwabJSON(`https://api.schwabapi.com/marketdata/v1/chains?${baseParams}&contractType=PUT`, token, env),
      ]);
      const spot = callData.underlyingPrice || callData.underlying?.last
                || putData.underlyingPrice  || putData.underlying?.last;
      return {
        spot,
        callExpDateMap: callData.callExpDateMap || {},
        putExpDateMap:  putData.putExpDateMap  || {},
        fetchedAt: Date.now(),
        _source: 'schwab',
      };
    } catch (e) {
      console.warn('[fetchMasterSpxChain] Schwab failed → Tasty fallback:', e.message);
    }
  } else {
    console.warn('[fetchMasterSpxChain] no Schwab token → direct Tasty');
  }
  return tastyFetchSpxChain(env, { root: 'SPXW', strikeCount: 80 });
}

// Returns a chain compatible with what each handler needs. If `preChain` has
// the requested expiries, reuse it (zero Schwab calls). Else falls through
// to a targeted fetch. Used by the diagonal/straddle/bobf live refreshers.
async function chainOrFetch(preChain, token, env, expectedExpiries, contractFilter = 'BOTH') {
  if (preChain) {
    const haveAll = expectedExpiries.every(exp => {
      const map = contractFilter === 'PUT' ? preChain.putExpDateMap : preChain.callExpDateMap;
      return Object.keys(map).some(k => k.startsWith(exp + ':'));
    });
    if (haveAll) return preChain;
  }
  // Fallback: fetch covering all needed expiries
  const min = expectedExpiries.reduce((a, b) => a < b ? a : b);
  const max = expectedExpiries.reduce((a, b) => a > b ? a : b);
  if (contractFilter === 'PUT') {
    return fetchSpxPutChain(token, min, max, env);
  }
  // For CALL/BOTH, do a master fetch (covers everything)
  return fetchMasterSpxChain(token, env);
}

function straddleMaxDebit(badge) {
  // badge = 'NM STRADDLE' | 'EOM STRADDLE' | 'STRADDLE'
  if (badge === 'NM STRADDLE')  return STRADDLE_MAX_DEBIT_NM;
  if (badge === 'EOM STRADDLE') return STRADDLE_MAX_DEBIT_EOM;
  return STRADDLE_MAX_DEBIT_OTHER;   // plain regular day
}

// Open or work a straddle. Called from the morning signal block once per day.
// `signal` is the calculateSignal result; we expect signal.theme === 'strad'.
async function openStraddleTrade(env, token, etNow, signal, preChain = null) {
  const todayISO = isoDateET(etNow);
  const expISO = todayISO;  // 0DTE — same-day expiry
  const chain = preChain || await fetchSpxFullChain(token, expISO, env);
  const { spot, callExpDateMap, putExpDateMap } = chain;
  if (!spot) throw new Error('Straddle open: no spot price in chain response');

  // Straddle CENTER = SPX OPEN (rounded to nearest 5), NOT spot-at-entry.
  // The morning signal block writes spxOpen to morning_signal_data_<today>
  // KV right before this runs (same handleScheduled invocation). Fall back
  // to history_data.json if KV is missing (e.g. /straddle-recovery path).
  // Final fallback to spot-at-entry preserves the old behavior if both
  // sources fail, but logs loudly so the bug is visible.
  let spxOpen = null, anchorSource = null;
  try {
    const msdRaw = await env.SIGNAL_KV.get(`morning_signal_data_${todayISO}`);
    if (msdRaw) {
      const msd = JSON.parse(msdRaw);
      if (msd.spxOpen != null && !isNaN(parseFloat(msd.spxOpen))) {
        spxOpen = parseFloat(msd.spxOpen);
        anchorSource = 'morning_signal_data KV';
      }
    }
  } catch (_) { /* fall through */ }
  if (spxOpen == null) {
    try {
      const hist = await getHistory(env);
      if (Array.isArray(hist) && hist.length) {
        const todayRow = hist.find(r => r.date === todayISO);
        if (todayRow?.spxOpen != null) {
          spxOpen = parseFloat(todayRow.spxOpen);
          anchorSource = 'history_data.json';
        }
      }
    } catch (_) { /* fall through */ }
  }
  if (spxOpen == null) {
    spxOpen = spot;
    anchorSource = 'spot-at-entry (FALLBACK — spxOpen missing)';
    console.warn(`[strad] spxOpen unavailable, using spot ${spot} as center anchor`);
  }
  const requestedK = snap5(spxOpen);
  console.log(`[strad] center anchor: spxOpen=${spxOpen} → strike=${requestedK} (source: ${anchorSource}, spot-now=${spot})`);

  // Find closest strike to spot that BOTH the call and put maps have, with
  // valid bid/ask on both. Walk outward from the requested strike in $5 steps.
  // Avoids the "fuzz lands call on 7355, put on 7350" mismatch that previously
  // threw an error (observed today 2026-05-08 — straddle never auto-opened).
  let strike = null, callLeg = null, putLeg = null;
  for (const offset of [0, -5, 5, -10, 10, -15, 15, -20, 20]) {
    const k = requestedK + offset;
    const c = pickContractFromChain(callExpDateMap, expISO, k);
    const p = pickContractFromChain(putExpDateMap,  expISO, k);
    if (c && p && c.strike === k && p.strike === k && c.mid != null && p.mid != null) {
      strike = k; callLeg = c; putLeg = p;
      break;
    }
  }
  if (!callLeg || !putLeg) {
    throw new Error(`Straddle open: no common strike with both legs near ${requestedK} for ${expISO}`);
  }

  // Long straddle: BUY call at ASK + BUY put at ASK (worst-case real fill).
  // mid-mid (debit) is the theoretical fair value — what the page shows.
  // Real cost to open is callAsk + putAsk; close credit is callBid + putBid.
  const debit   = callLeg.mid + putLeg.mid;
  const askFill = callLeg.ask + putLeg.ask;
  const bidExit = callLeg.bid + putLeg.bid;
  const maxDebit = straddleMaxDebit(signal.badge || 'STRADDLE');

  // Daily total-risk cap (2026-06-09): debit × 100 = this trade's max loss.
  const riskGate = await enforceRiskCap(env, etNow, 'straddle', debit * 100);
  if (!riskGate.ok) throw new Error(`risk-cap blocked straddle open: ${riskGate.reason}`);

  const trade = {
    openDate: todayISO,
    openTimeET: `${String(etNow.getHours()).padStart(2,'0')}:${String(etNow.getMinutes()).padStart(2,'0')}`,
    badge: signal.badge || 'STRADDLE',
    spotEntry: parseFloat(spot.toFixed(2)),
    strike,
    expDate: expISO,
    callSymbol: callLeg.symbol,
    putSymbol:  putLeg.symbol,
    entryCallMid: parseFloat(callLeg.mid.toFixed(2)),
    entryPutMid:  parseFloat(putLeg.mid.toFixed(2)),
    entryCallBid: callLeg.bid, entryCallAsk: callLeg.ask,
    entryPutBid:  putLeg.bid,  entryPutAsk:  putLeg.ask,
    entryDebit: parseFloat(debit.toFixed(2)),
    entryAskFill: parseFloat(askFill.toFixed(2)),
    entryBidExit: parseFloat(bidExit.toFixed(2)),
    maxDebit,
    contracts: 1,
    // Live fields
    currentSpot: parseFloat(spot.toFixed(2)),
    currentCallMid: parseFloat(callLeg.mid.toFixed(2)),
    currentPutMid:  parseFloat(putLeg.mid.toFixed(2)),
    currentValue:   parseFloat(debit.toFixed(2)),
    currentAskFill: parseFloat(askFill.toFixed(2)),
    currentBidExit: parseFloat(bidExit.toFixed(2)),
    currentPnl: 0,
    lastQuoteAt: new Date().toISOString(),
    // Status
    status: debit <= maxDebit ? 'filled' : 'working',
    fillDebit: debit <= maxDebit ? parseFloat(debit.toFixed(2)) : null,
    fillTimeET: debit <= maxDebit ? `${String(etNow.getHours()).padStart(2,'0')}:${String(etNow.getMinutes()).padStart(2,'0')}` : null,
    workingExpiry: `${todayISO} 13:30 ET`,
  };
  return trade;
}

// Refresh live mids on the open straddle. Also handles working→filled and
// working→expired transitions.
async function refreshStraddleLiveQuotes(env, token, etNow, preChain = null) {
  const raw = await env.SIGNAL_KV.get('straddle_open_trade');
  if (!raw) return null;
  const trade = JSON.parse(raw);
  if (trade.status === 'expired' || trade.status === 'closed') return trade;

  // Past cutoff and still working → expire
  const pastCutoff = etNow.getHours() > STRADDLE_WORK_CUTOFF_HR ||
                     (etNow.getHours() === STRADDLE_WORK_CUTOFF_HR && etNow.getMinutes() >= STRADDLE_WORK_CUTOFF_MIN);
  if (trade.status === 'working' && pastCutoff) {
    trade.status = 'expired';
    trade.expiredAt = new Date().toISOString();
    await env.SIGNAL_KV.put('straddle_open_trade', JSON.stringify(trade));
    return trade;
  }

  try {
    const chain = preChain || await fetchSpxFullChain(token, trade.expDate, env);
    const { spot, callExpDateMap, putExpDateMap } = chain;
    const c = pickContractFromChain(callExpDateMap, trade.expDate, trade.strike);
    const p = pickContractFromChain(putExpDateMap,  trade.expDate, trade.strike);
    if (!c || !p || c.mid == null || p.mid == null) return trade;
    const newDebit = c.mid + p.mid;
    trade.currentSpot   = spot ? parseFloat(spot.toFixed(2)) : trade.currentSpot;
    trade.currentCallMid = parseFloat(c.mid.toFixed(2));
    trade.currentPutMid  = parseFloat(p.mid.toFixed(2));
    trade.currentValue   = parseFloat(newDebit.toFixed(2));
    // Realistic live fill estimates — what you'd actually pay to open / get to
    // close RIGHT NOW. callAsk + putAsk for entry, callBid + putBid for exit.
    if (c.ask != null && c.bid != null && p.ask != null && p.bid != null) {
      trade.currentAskFill = parseFloat((c.ask + p.ask).toFixed(2));
      trade.currentBidExit = parseFloat((c.bid + p.bid).toFixed(2));
    }

    // Working → filled if price drops to the limit
    if (trade.status === 'working' && newDebit <= trade.maxDebit) {
      trade.status = 'filled';
      trade.fillDebit = parseFloat(newDebit.toFixed(2));
      trade.fillTimeET = `${String(etNow.getHours()).padStart(2,'0')}:${String(etNow.getMinutes()).padStart(2,'0')}`;
      // Re-snapshot leg fills at fill time
      trade.entryCallMid = parseFloat(c.mid.toFixed(2));
      trade.entryPutMid  = parseFloat(p.mid.toFixed(2));
      trade.entryDebit   = parseFloat(newDebit.toFixed(2));
      // Re-snapshot ask-fill / bid-exit at fill time too (matches diagonal)
      if (c.ask != null && c.bid != null && p.ask != null && p.bid != null) {
        trade.entryAskFill = parseFloat((c.ask + p.ask).toFixed(2));
        trade.entryBidExit = parseFloat((c.bid + p.bid).toFixed(2));
      }
    }

    // Live P&L only meaningful when filled
    if (trade.status === 'filled') {
      trade.currentPnl = Math.round((trade.currentValue - trade.entryDebit) * 100 * trade.contracts);
    }
    trade.lastQuoteAt = new Date().toISOString();
    await env.SIGNAL_KV.put('straddle_open_trade', JSON.stringify(trade));
  } catch (e) {
    console.warn('[strad] refresh failed:', e.message);
  }
  return trade;
}

// EOD: settle the straddle at SPX close. Records stradPL.
// `spxClose` provided by handleEOD (from Schwab quote).
async function settleStraddleEOD(env, etNow, spxClose, dateISO = null) {
  // dateISO: settle a PAST day's orphaned trade (P18 sweep / manual heal).
  const raw = await env.SIGNAL_KV.get('straddle_open_trade');
  if (!raw) return { status: 'no-trade' };
  const trade = JSON.parse(raw);
  const targetISO = dateISO || isoDateET(etNow);
  if (trade.openDate !== targetISO) return { status: 'wrong-date', openDate: trade.openDate };
  if (trade.status === 'closed' || trade.status === 'expired') {
    // Working order that never filled → no trade, no P&L
    if (trade.status === 'expired') {
      // Still mark in history so the day shows "no trade" cleanly?
      // Per existing convention stradPL=null for skip, 0 means "traded, broke even"
      // Leave null — the EOD cron handles m8bf etc. similarly.
    }
    return { status: trade.status };
  }
  if (trade.status !== 'filled') return { status: trade.status };

  // Intrinsic value at expiry: |spxClose - strike|
  const closeIntrinsic = Math.abs(spxClose - trade.strike);
  const pnl = Math.round((closeIntrinsic - trade.entryDebit) * 100 * trade.contracts);

  // Write P/L to history FIRST — only mark 'closed' AFTER it lands, so a throw in
  // upsertHistoryGitHub can't leave the trade closed with a blank PL column (no
  // backfill, re-settle blocked). The status guard above makes re-settle idempotent
  // (same spxClose → same pnl) (audit P2 2026-07-06).
  const stradFields = { stradPL: pnl };
  if (dateISO) stradFields.spxClose = parseFloat(spxClose.toFixed(2));
  await upsertHistoryGitHub(env, trade.openDate, stradFields);

  trade.status = 'closed';
  trade.closeDate = targetISO;
  trade.spxClose = parseFloat(spxClose.toFixed(2));
  trade.closeValue = parseFloat(closeIntrinsic.toFixed(2));
  trade.pnl = pnl;
  await env.SIGNAL_KV.put('straddle_open_trade', JSON.stringify(trade));

  // Append to closed log
  const logRaw = await env.SIGNAL_KV.get('straddle_closed_log');
  const log = logRaw ? JSON.parse(logRaw) : [];
  log.unshift(trade);
  await env.SIGNAL_KV.put('straddle_closed_log', JSON.stringify(log.slice(0, 30)));

  return { status: 'settled', pnl, strike: trade.strike, debit: trade.entryDebit, closeValue: closeIntrinsic };
}

// ════════════════════════════════════════════════════════════════════
// BOBF TRADE HANDLER (live)
// 3 types of broken-wing call butterfly, all 0DTE on SPX:
//   FRIDAY (Fri only)     — body offset +15, wings ±30 from body
//   VIX_UP  (Mon–Thu)     — body offset +25, wings ±30
//   VIX_DOWN (Mon–Thu)    — body offset +25, wings ±30
// All three: SHORT 2 body calls, LONG 1 lower-wing call, LONG 1 upper-wing.
//
// Window: 10:29 ET – 12:15 ET. First qualifying minute opens the trade.
// Mutual exclusion via bobf_done_<date> KV key — max 1 BOBF per day.
//
// Entry filters (all must pass at signal time):
//   - Calendar blackout (mirrors signal-engine bobfBlocks): CPI / NM-Mon /
//     VIX-exp / OPEX / OPEX-1 / EOM-1 / EOM-2 / earnings / VIX > 23
//   - SPX > 5-day SMA (last 5 daily closes)
//   - SPX move from open ≥ type-specific threshold
//   - RSI(14) on daily close ∈ type-specific band
//   - For VIX_UP / VIX_DOWN: overnight VIX moved in the right direction
//     by ≥ 0.01 points
//
// Friday max premium $12 → leave working limit if mid > 12, cancel at 12:15.
// VIX_UP / VIX_DOWN have no premium cap — fill at first qualifying minute.
// Held to expiration (4:00 PM cash close). PnL settled from intrinsic at
// SPX close: lower_intrinsic − 2*body_intrinsic + upper_intrinsic − debit.
// ════════════════════════════════════════════════════════════════════

const BOBF_BODY_OFFSET_FRIDAY = 15;
const BOBF_BODY_OFFSET_VIX    = 25;
const BOBF_WING_OFFSET        = 30;
const BOBF_FRIDAY_MAX_PREMIUM = 12.00;
const BOBF_VIX_O_N_THRESHOLD  = 0.01;
const BOBF_FRIDAY_MOVE_MIN    = 0.001;   // 0.1%
const BOBF_VIX_UP_MOVE_MIN    = 0.002;   // 0.2%
const BOBF_VIX_DOWN_MOVE_MIN  = 0.002;   // 0.2%
const BOBF_VIX_DOWN_MOVE_MAX  = 0.007;   // 0.7%
const BOBF_FRIDAY_RSI_MIN     = 40;
const BOBF_FRIDAY_RSI_MAX     = 65;
const BOBF_VIX_DOWN_RSI_MAX   = 70;
const BOBF_VIX_MAX            = 23;

function bobfInWindow(etNow) {
  const h = etNow.getHours(), m = etNow.getMinutes();
  if (h === 10 && m >= 29) return true;
  if (h === 11) return true;
  if (h === 12 && m < 15) return true;
  return false;
}

function bobfPastWindow(etNow) {
  const h = etNow.getHours(), m = etNow.getMinutes();
  return (h === 12 && m >= 15) || h > 12;
}

// Standard Wilder RSI(14) on daily closes. Returns null if insufficient data.
function computeRSI14(closes) {
  if (closes.length < 15) return null;
  let gain = 0, loss = 0;
  for (let i = 1; i <= 14; i++) {
    const diff = closes[i] - closes[i-1];
    if (diff > 0) gain += diff; else loss -= diff;
  }
  let avgGain = gain / 14, avgLoss = loss / 14;
  for (let i = 15; i < closes.length; i++) {
    const diff = closes[i] - closes[i-1];
    avgGain = (avgGain * 13 + Math.max(diff, 0)) / 14;
    avgLoss = (avgLoss * 13 + Math.max(-diff, 0)) / 14;
  }
  if (avgLoss === 0) return 100;
  const rs = avgGain / avgLoss;
  return 100 - 100 / (1 + rs);
}

function computeSMA5(closes) {
  if (closes.length < 5) return null;
  const last5 = closes.slice(-5);
  return last5.reduce((a,b) => a + b, 0) / 5;
}

// Pick the BOBF type for today, or null if none qualifies.
function determineBobfType(etNow, vixToday, vixYClose) {
  const dow = etNow.getDay();
  if (dow === 5) return { type: 'friday', label: 'Friday RSI BOBF', bodyOffset: BOBF_BODY_OFFSET_FRIDAY };
  if (dow >= 1 && dow <= 4) {
    if (vixToday == null || vixYClose == null) return { type: null, reason: 'VIX data missing' };
    const diff = vixToday - vixYClose;
    if (diff >=  BOBF_VIX_O_N_THRESHOLD) return { type: 'vix_up',   label: 'BOBF VIX up',   bodyOffset: BOBF_BODY_OFFSET_VIX };
    if (diff <= -BOBF_VIX_O_N_THRESHOLD) return { type: 'vix_down', label: 'BOBF VIX down', bodyOffset: BOBF_BODY_OFFSET_VIX };
    return { type: null, reason: 'flat overnight VIX (Δ<0.01)' };
  }
  return { type: null, reason: 'weekend' };
}

// Live entry-filter evaluation. Returns { ready: bool, reason: string }.
function bobfEntryReady(typeInfo, spotNow, spxOpen, sma5, rsi14) {
  if (sma5 == null || rsi14 == null) return { ready: false, reason: 'history insufficient (need 15+ days)' };
  if (spotNow <= sma5) return { ready: false, reason: `SPX ${spotNow.toFixed(2)} ≤ 5d SMA ${sma5.toFixed(2)}` };
  const moveUp = (spotNow - spxOpen) / spxOpen;
  if (typeInfo.type === 'friday') {
    if (moveUp < BOBF_FRIDAY_MOVE_MIN)  return { ready: false, reason: `move-up ${(moveUp*100).toFixed(2)}% < 0.10%` };
    if (rsi14 < BOBF_FRIDAY_RSI_MIN || rsi14 > BOBF_FRIDAY_RSI_MAX) return { ready: false, reason: `RSI ${rsi14.toFixed(1)} outside ${BOBF_FRIDAY_RSI_MIN}-${BOBF_FRIDAY_RSI_MAX} band` };
  } else if (typeInfo.type === 'vix_up') {
    if (moveUp < BOBF_VIX_UP_MOVE_MIN)  return { ready: false, reason: `move-up ${(moveUp*100).toFixed(2)}% < 0.20%` };
  } else if (typeInfo.type === 'vix_down') {
    if (moveUp < BOBF_VIX_DOWN_MOVE_MIN) return { ready: false, reason: `move-up ${(moveUp*100).toFixed(2)}% < 0.20%` };
    if (moveUp > BOBF_VIX_DOWN_MOVE_MAX) return { ready: false, reason: `move-up ${(moveUp*100).toFixed(2)}% > 0.70%` };
    if (rsi14 > BOBF_VIX_DOWN_RSI_MAX)   return { ready: false, reason: `RSI ${rsi14.toFixed(1)} > 70` };
  }
  return { ready: true, moveUp };
}

// Main entry flow — called from every cron tick during the window.
// Idempotent via bobf_done_<date>: exits early once trade fires or window expires.
// Static-filter pre-flight: runs ONCE per day at the morning signal block.
// Catches RSI / type / calendar / VIX>23 disqualifications that won't change
// intraday so we skip 60+ futile entry attempts in the 10:29-12:15 window.
// Sets bobf_done_<date> with the rejection reason; handleBobfEntry then
// short-circuits on every market tick.
async function prefilterBobf(env, etNow, vixToday, vixYClose) {
  const todayISO = isoDateET(etNow);
  const doneKey = `bobf_done_${todayISO}`;
  const existing = await env.SIGNAL_KV.get(doneKey);
  if (existing) return { skipped: 'already-done', why: existing };

  // 1. Type qualification (Fri vs Mon-Thu vix_up/down vs flat-overnight skip)
  const typeInfo = determineBobfType(etNow, vixToday, vixYClose);
  if (!typeInfo.type) {
    await env.SIGNAL_KV.put(doneKey, `no-type:${typeInfo.reason}`, { expirationTtl: 86400 });
    return { skipped: 'no-type', reason: typeInfo.reason };
  }

  // 2. Calendar blackouts + VIX>23 (mirrors signal-engine bobfBlocks)
  const cpiDay   = cpiSch.includes(todayLong(etNow));
  const nmDay    = isFirstTradeMo(etNow);
  const nmMon    = isFirstTradeMon(etNow);
  const vixExpDay = vixSch.includes(todayLong(etNow));
  const opexDay  = opexSch.includes(todayLong(etNow));
  const opex1    = opexSch.some(ds => isTodayBefore(ds, etNow));
  const eom1     = isEomN(1, etNow);
  const eom2     = isEomN(2, etNow);
  const earnDay  = isEarningsDay(etNow);
  const blackouts = [];
  if (cpiDay) blackouts.push('CPI');
  if (nmMon) blackouts.push('NM Mon');
  if (vixExpDay) blackouts.push('VIX exp');
  if (opexDay) blackouts.push('OPEX');
  if (opex1) blackouts.push('OPEX-1');
  if (eom2) blackouts.push('EOM-2');
  if (eom1) blackouts.push('EOM-1');
  if (earnDay) blackouts.push('earnings');
  if (vixToday != null && vixToday > BOBF_VIX_MAX) blackouts.push(`VIX ${vixToday}>${BOBF_VIX_MAX}`);
  if (blackouts.length) {
    await env.SIGNAL_KV.put(doneKey, `blackout:${blackouts.join(',')}`, { expirationTtl: 86400 });
    return { skipped: 'blackout', reasons: blackouts };
  }

  // 3. RSI(14) + SMA5 + spxOpen — all daily-close-based, fixed for the entire
  //    trading day. Cache them in KV so handleBobfEntry doesn't re-fetch
  //    history_data.json from GitHub on every tick (~106 saved fetches/day).
  let rsi14 = null, sma5 = null, spxOpen = null;
  try {
    const histData = await getHistory(env);
    if (Array.isArray(histData) && histData.length) {
      const todayRow = histData.find(r => r.date === todayISO);
      spxOpen = todayRow?.spxOpen != null ? parseFloat(todayRow.spxOpen) : null;
      const sortedPrior = histData
        .filter(r => r.date < todayISO && r.spxClose != null)
        .sort((a, b) => a.date.localeCompare(b.date));
      const closes30 = sortedPrior.slice(-30).map(r => parseFloat(r.spxClose));
      rsi14 = computeRSI14(closes30);
      sma5 = computeSMA5(closes30);
    }
  } catch (_) { /* leave handleBobfEntry to recheck */ }

  if (rsi14 != null) {
    if (typeInfo.type === 'friday' && (rsi14 < BOBF_FRIDAY_RSI_MIN || rsi14 > BOBF_FRIDAY_RSI_MAX)) {
      await env.SIGNAL_KV.put(doneKey, `rsi:${rsi14.toFixed(1)} outside ${BOBF_FRIDAY_RSI_MIN}-${BOBF_FRIDAY_RSI_MAX} band`, { expirationTtl: 86400 });
      return { skipped: 'rsi-out', rsi14, type: 'friday' };
    }
    if (typeInfo.type === 'vix_down' && rsi14 > BOBF_VIX_DOWN_RSI_MAX) {
      await env.SIGNAL_KV.put(doneKey, `rsi:${rsi14.toFixed(1)} > ${BOBF_VIX_DOWN_RSI_MAX}`, { expirationTtl: 86400 });
      return { skipped: 'rsi-high', rsi14, type: 'vix_down' };
    }
  }

  // SAFETY + AUTO-RECOVERY: if today's spxOpen is missing from history
  // (morning signal block failed — Schwab outage etc.), the entry handler
  // can't compute move-up% and will silently never fire. Don't give up
  // immediately: try to recover from Schwab NOW (the 9:30 candle exists
  // by 10:29 when prefilter runs). Only mark data-stale if recovery fails.
  if (spxOpen == null) {
    console.warn(`[bobf] spxOpen missing for ${todayISO}, attempting auto-recovery from Schwab 9:30 candle...`);
    try {
      const recovered = await recoverOpenPricesFromSchwab(env, etNow);
      if (recovered.spxOpen != null) {
        spxOpen = recovered.spxOpen;
        const fields = { spxOpen };
        if (recovered.vixOpen != null) fields.vixOpen = recovered.vixOpen;
        try {
          await upsertHistoryGitHub(env, todayISO, fields);
          console.log(`[bobf] auto-recovery wrote spxOpen=${spxOpen}${recovered.vixOpen != null ? ` vixOpen=${recovered.vixOpen}` : ''} for ${todayISO}`);
          await logEvent(env, 'warn', 'bobf-recover', 'auto-recovered missing spxOpen from Schwab 9:30 candle', fields);
        } catch (writeErr) {
          console.warn('[bobf] auto-recovery history write failed:', writeErr.message);
        }
      }
    } catch (e) {
      console.warn('[bobf] auto-recovery failed:', e.message);
    }
  }
  if (spxOpen == null) {
    await env.SIGNAL_KV.put(doneKey, 'data-stale: spxOpen missing — morning signal block failed and auto-recovery also failed', { expirationTtl: 86400 });
    console.warn(`[bobf] data-stale for ${todayISO}: spxOpen null even after recovery, prefilter cannot evaluate entry conditions`);
    return { skipped: 'data-stale', reason: 'spxOpen missing in history_data.json (recovery failed)' };
  }

  // Cache the day's static inputs for handleBobfEntry to reuse every tick.
  await env.SIGNAL_KV.put(`bobf_static_${todayISO}`, JSON.stringify({
    rsi14, sma5, spxOpen, vixToday, vixYClose,
    type: typeInfo.type, label: typeInfo.label, bodyOffset: typeInfo.bodyOffset,
  }), { expirationTtl: 86400 });

  // All static filters pass — entry checks during 10:29-12:15 will evaluate
  // the dynamic conditions (move-up %, SMA5).
  return { passed: true, type: typeInfo.type, rsi14 };
}

async function handleBobfEntry(env, etNow, preChain = null) {
  const todayISO = isoDateET(etNow);
  const out = { date: todayISO, status: null };

  const doneKey = `bobf_done_${todayISO}`;
  const done = await env.SIGNAL_KV.get(doneKey);
  // A `claim:` value is OUR OWN caller's in-flight claim token (P22 gate), not a
  // completed run — treat only terminal markers as done. (2026-07-20: the claim
  // gate made this self-check abort every tick → GXBF never fired on OPEX+1.)
  if (done && !done.startsWith('claim:')) return { ...out, status: 'already-done', why: done };

  // Guard: if today's open BOBF trade already exists, don't re-evaluate —
  // refreshBobfLiveQuotes handles working→filled transitions. Without this
  // guard, every tick re-runs the full entry filter and could overwrite the
  // working-order's entry strikes/debit if spot moved between ticks (which
  // would corrupt the trade record the user is actually holding).
  try {
    const existingRaw = await env.SIGNAL_KV.get('bobf_open_trade');
    if (existingRaw) {
      const existing = JSON.parse(existingRaw);
      if (existing.openDate === todayISO && existing.status !== 'closed' && existing.status !== 'expired') {
        return { ...out, status: 'already-open', existingStatus: existing.status };
      }
    }
  } catch (_) { /* fall through — KV blip will recover next tick */ }

  // Past window → mark done permanently (no fire, no trade)
  if (bobfPastWindow(etNow)) {
    await env.SIGNAL_KV.put(doneKey, 'window-passed', { expirationTtl: 86400 });
    return { ...out, status: 'window-passed' };
  }
  if (!bobfInWindow(etNow)) return { ...out, status: 'pre-window' };

  // Try BOBF static cache first (written by prefilterBobf at 9:30). If hit,
  // we skip the GitHub history fetch entirely on every market tick.
  let vixToday, spxOpen, vixYClose, sma5, rsi14, typeInfo;
  const staticRaw = await env.SIGNAL_KV.get(`bobf_static_${todayISO}`);
  if (staticRaw) {
    const s = JSON.parse(staticRaw);
    vixToday = s.vixToday; spxOpen = s.spxOpen; vixYClose = s.vixYClose;
    sma5 = s.sma5; rsi14 = s.rsi14;
    typeInfo = { type: s.type, label: s.label, bodyOffset: s.bodyOffset };
  } else {
    // Cache miss (e.g. cold start, prefilter never ran) — fall back to KV history
    let histData;
    try {
      histData = await getHistory(env);
      if (!Array.isArray(histData) || !histData.length) {
        return { ...out, status: 'error', error: 'history empty (KV not seeded)' };
      }
    } catch (e) { return { ...out, status: 'error', error: 'history fetch: ' + e.message }; }

    const todayRow = histData.find(r => r.date === todayISO);
    vixToday = todayRow?.vixOpen != null ? parseFloat(todayRow.vixOpen) : null;
    spxOpen  = todayRow?.spxOpen != null ? parseFloat(todayRow.spxOpen) : null;

    const sortedPrior = histData
      .filter(r => r.date < todayISO && r.spxClose != null)
      .sort((a, b) => a.date.localeCompare(b.date));
    if (sortedPrior.length === 0) return { ...out, status: 'error', error: 'no prior history' };

    const yClose = sortedPrior[sortedPrior.length - 1];
    vixYClose = yClose.vixClose != null ? parseFloat(yClose.vixClose) : null;
    const closes30 = sortedPrior.slice(-30).map(r => parseFloat(r.spxClose));
    sma5  = computeSMA5(closes30);
    rsi14 = computeRSI14(closes30);

    typeInfo = determineBobfType(etNow, vixToday, vixYClose);
  }
  if (!typeInfo.type) {
    await env.SIGNAL_KV.put(doneKey, 'no-type:' + (typeInfo.reason || 'unknown'), { expirationTtl: 86400 });
    return { ...out, status: 'no-type', reason: typeInfo.reason };
  }

  // Calendar blackouts (mirrors signal-engine bobfBlocks)
  const cpiDay   = cpiSch.includes(todayLong(etNow));
  const nmDay    = isFirstTradeMo(etNow);
  const nmMon    = isFirstTradeMon(etNow);
  const vixExpDay = vixSch.includes(todayLong(etNow));
  const opexDay  = opexSch.includes(todayLong(etNow));
  const opex1    = opexSch.some(ds => isTodayBefore(ds, etNow));
  const eom1     = isEomN(1, etNow);
  const eom2     = isEomN(2, etNow);
  const earnDay  = isEarningsDay(etNow);
  const blackouts = [];
  if (cpiDay) blackouts.push('CPI');
  if (nmMon) blackouts.push('NM Mon');
  if (vixExpDay) blackouts.push('VIX exp');
  if (opexDay) blackouts.push('OPEX');
  if (opex1) blackouts.push('OPEX-1');
  if (eom2) blackouts.push('EOM-2');
  if (eom1) blackouts.push('EOM-1');
  if (earnDay) blackouts.push('earnings');
  if (vixToday != null && vixToday > BOBF_VIX_MAX) blackouts.push(`VIX ${vixToday}>${BOBF_VIX_MAX}`);
  if (blackouts.length) {
    await env.SIGNAL_KV.put(doneKey, 'blackout:' + blackouts.join(','), { expirationTtl: 86400 });
    return { ...out, status: 'blackout', reason: blackouts.join(', '), type: typeInfo.type };
  }

  // Use master chain if available (saves 2 Schwab calls), else fetch our own.
  let token, spot, callExpDateMap;
  try {
    token = preChain ? null : await getAccessToken(env);   // skip token when the chain is in hand — a dead Schwab token must not error a preChain entry (audit P1-f)
    const chain = preChain || await fetchSpxFullChain(token, todayISO, env);
    spot = chain.spot; callExpDateMap = chain.callExpDateMap;
  } catch (e) { return { ...out, status: 'error', error: 'chain fetch: ' + e.message }; }
  if (!spot) return { ...out, status: 'error', error: 'no SPX spot' };

  if (spxOpen == null) return { ...out, status: 'error', error: 'no spxOpen — wait for morning EOD write' };

  // Entry-filter check
  const ready = bobfEntryReady(typeInfo, spot, spxOpen, sma5, rsi14);
  if (!ready.ready) return { ...out, status: 'waiting', reason: ready.reason, type: typeInfo.type, sma5, rsi14, spotNow: spot, spxOpen };

  // Compute strikes — pick body FIRST, then re-anchor wings on the actual body
  // strike picked (in case fuzz fallback in pickContractFromChain lands on a
  // neighbor). Otherwise wings could mismatch the body and butterfly geometry
  // breaks, which mis-prices settlement intrinsic.
  const reqBody  = snap5(spot + typeInfo.bodyOffset);
  const bodyLeg  = pickContractFromChain(callExpDateMap, todayISO, reqBody);
  if (!bodyLeg) return { ...out, status: 'error', error: `missing body leg in chain @ ${reqBody}` };
  const kBody  = bodyLeg.strike;            // actual filled body strike
  const kLower = kBody - BOBF_WING_OFFSET;
  const kUpper = kBody + BOBF_WING_OFFSET;

  const lowerLeg = pickContractFromChain(callExpDateMap, todayISO, kLower);
  const upperLeg = pickContractFromChain(callExpDateMap, todayISO, kUpper);
  if (!lowerLeg || !upperLeg) return { ...out, status: 'error', error: `missing wing leg in chain (${kLower}/${kUpper}, anchored on body ${kBody})` };
  if (lowerLeg.mid == null || bodyLeg.mid == null || upperLeg.mid == null) return { ...out, status: 'error', error: 'missing bid/ask on a leg' };
  // Verify wings landed on EXACT requested strikes (no fuzz on wings — that
  // would break the symmetric debit math).
  if (lowerLeg.strike !== kLower || upperLeg.strike !== kUpper) {
    return { ...out, status: 'error', error: `wing fuzz mismatch — lower req ${kLower}/got ${lowerLeg.strike}, upper req ${kUpper}/got ${upperLeg.strike}` };
  }

  const debit = lowerLeg.mid - 2 * bodyLeg.mid + upperLeg.mid;

  // Daily total-risk cap (2026-06-09): butterfly max loss = debit × 100.
  // Status-return (not throw): next 2-min tick re-checks; if exposure clears
  // (e.g. straddle settled), BOBF can still fire inside its window.
  {
    const bobfGate = await enforceRiskCap(env, etNow, 'bobf', debit * 100);
    if (!bobfGate.ok) {
      return { ...out, status: 'risk-cap-blocked', detail: bobfGate.reason };
    }
  }

  // ── Item 9: live VIX validator (defense-in-depth) ──
  // vixToday above is the cached morning 9:30 print. By trade-fire time
  // (10:29-12:15 ET) VIX may have spiked above the BOBF gate. Re-pull live
  // VIX and re-check the gate. If violated, abort THIS tick without marking
  // done — next 2-min tick re-checks; VIX may revert.
  try {
    const liveVixQ = await fetchSchwabJSON(
      'https://api.schwabapi.com/marketdata/v1/quotes?symbols=%24VIX&fields=quote',
      token, env);
    const liveVix = liveVixQ?.['$VIX']?.quote?.lastPrice;
    if (liveVix != null && liveVix > BOBF_VIX_MAX) {
      console.warn(`[bobf-validate] live VIX ${liveVix.toFixed(2)} > ${BOBF_VIX_MAX} — aborting fire`);
      await logEvent(env, 'warn', 'bobf-validate',
        `live VIX spiked above ${BOBF_VIX_MAX} at fire time, aborting`,
        { liveVix: parseFloat(liveVix.toFixed(2)), vixOpenCached: vixToday, gate: BOBF_VIX_MAX });
      return { ...out, status: 'vix-spiked',
               liveVix: parseFloat(liveVix.toFixed(2)),
               gate: BOBF_VIX_MAX, type: typeInfo.type };
    }
  } catch (vErr) {
    // Don't block trade on validator failure — falls through to existing logic
    console.warn('[bobf-validate] live VIX fetch failed:', vErr.message);
  }

  // Friday max-premium logic (working order pattern). VIX_UP / VIX_DOWN: no cap.
  let status, fillDebit = null, fillTimeET = null, maxDebit = null;
  if (typeInfo.type === 'friday') {
    maxDebit = BOBF_FRIDAY_MAX_PREMIUM;
    if (debit <= BOBF_FRIDAY_MAX_PREMIUM) {
      status = 'filled';
      fillDebit = debit;
      fillTimeET = `${String(etNow.getHours()).padStart(2,'0')}:${String(etNow.getMinutes()).padStart(2,'0')}`;
    } else {
      status = 'working';
    }
  } else {
    status = 'filled';
    fillDebit = debit;
    fillTimeET = `${String(etNow.getHours()).padStart(2,'0')}:${String(etNow.getMinutes()).padStart(2,'0')}`;
  }

  const trade = {
    openDate: todayISO,
    openTimeET: `${String(etNow.getHours()).padStart(2,'0')}:${String(etNow.getMinutes()).padStart(2,'0')}`,
    type: typeInfo.type,
    label: typeInfo.label,
    spotEntry: parseFloat(spot.toFixed(2)),
    spxOpen: parseFloat(spxOpen.toFixed(2)),
    moveUpPct: parseFloat((ready.moveUp * 100).toFixed(3)),
    sma5: parseFloat(sma5.toFixed(2)),
    rsi14: parseFloat(rsi14.toFixed(2)),
    vixToday, vixYClose,
    bodyStrike: kBody, lowerStrike: kLower, upperStrike: kUpper,
    expDate: todayISO,
    bodySymbol: bodyLeg.symbol, lowerSymbol: lowerLeg.symbol, upperSymbol: upperLeg.symbol,
    entryLowerMid: parseFloat(lowerLeg.mid.toFixed(2)),
    entryBodyMid:  parseFloat(bodyLeg.mid.toFixed(2)),
    entryUpperMid: parseFloat(upperLeg.mid.toFixed(2)),
    entryDebit:    parseFloat(debit.toFixed(2)),
    maxDebit,
    contracts: 1,
    // Live fields
    currentSpot: parseFloat(spot.toFixed(2)),
    currentLowerMid: parseFloat(lowerLeg.mid.toFixed(2)),
    currentBodyMid:  parseFloat(bodyLeg.mid.toFixed(2)),
    currentUpperMid: parseFloat(upperLeg.mid.toFixed(2)),
    currentValue:    parseFloat(debit.toFixed(2)),
    currentPnl: 0,
    lastQuoteAt: new Date().toISOString(),
    status, fillDebit, fillTimeET,
    workingExpiry: `${todayISO} 12:15 ET`,
  };

  await env.SIGNAL_KV.put('bobf_open_trade', JSON.stringify(trade));
  if (status === 'filled') {
    await env.SIGNAL_KV.put(doneKey, 'filled', { expirationTtl: 86400 });
  }
  // working orders: leave doneKey unset so subsequent ticks can flip working→filled

  console.log(`[bobf] opened ${status} type=${typeInfo.type} body=${kBody} debit=${debit.toFixed(2)} maxDebit=${maxDebit ?? '-'}`);
  await logEvent(env, 'info', 'bobf-open', `opened ${status} type=${typeInfo.type}`, {
    body: kBody, debit: parseFloat(debit.toFixed(2)), maxDebit, type: typeInfo.type,
  });
  return { ...out, status: 'opened', type: typeInfo.type, trade: { strikes: [kLower, kBody, kUpper], debit, status } };
}

// Refresh live mids on the open BOBF trade. Handles working→filled transition
// + working→expired at 12:15 ET.
async function refreshBobfLiveQuotes(env, token, etNow, preChain = null) {
  const raw = await env.SIGNAL_KV.get('bobf_open_trade');
  if (!raw) return null;
  const trade = JSON.parse(raw);
  if (trade.status === 'closed' || trade.status === 'expired') return trade;

  // Working order past 12:15 ET → expire
  if (trade.status === 'working' && bobfPastWindow(etNow)) {
    trade.status = 'expired';
    trade.expiredAt = new Date().toISOString();
    await env.SIGNAL_KV.put('bobf_open_trade', JSON.stringify(trade));
    await env.SIGNAL_KV.put(`bobf_done_${isoDateET(etNow)}`, 'expired', { expirationTtl: 86400 });
    return trade;
  }

  try {
    const chain = preChain || await fetchSpxFullChain(token, trade.expDate, env);
    const { spot, callExpDateMap } = chain;
    const lower = pickContractFromChain(callExpDateMap, trade.expDate, trade.lowerStrike);
    const body  = pickContractFromChain(callExpDateMap, trade.expDate, trade.bodyStrike);
    const upper = pickContractFromChain(callExpDateMap, trade.expDate, trade.upperStrike);
    if (!lower || !body || !upper || lower.mid == null || body.mid == null || upper.mid == null) return trade;

    const newDebit = lower.mid - 2 * body.mid + upper.mid;
    trade.currentSpot     = spot ? parseFloat(spot.toFixed(2)) : trade.currentSpot;
    trade.currentLowerMid = parseFloat(lower.mid.toFixed(2));
    trade.currentBodyMid  = parseFloat(body.mid.toFixed(2));
    trade.currentUpperMid = parseFloat(upper.mid.toFixed(2));
    trade.currentValue    = parseFloat(newDebit.toFixed(2));

    // Working → filled if mid drops to ≤ max debit
    let justFilled = false;
    if (trade.status === 'working' && newDebit <= trade.maxDebit) {
      trade.status = 'filled';
      trade.fillDebit = parseFloat(newDebit.toFixed(2));
      trade.fillTimeET = `${String(etNow.getHours()).padStart(2,'0')}:${String(etNow.getMinutes()).padStart(2,'0')}`;
      // Re-snapshot leg fills at the actual fill time
      trade.entryLowerMid = parseFloat(lower.mid.toFixed(2));
      trade.entryBodyMid  = parseFloat(body.mid.toFixed(2));
      trade.entryUpperMid = parseFloat(upper.mid.toFixed(2));
      trade.entryDebit    = parseFloat(newDebit.toFixed(2));
      justFilled = true;
    }

    if (trade.status === 'filled') {
      trade.currentPnl = Math.round((trade.currentValue - trade.entryDebit) * 100 * trade.contracts);
    }
    trade.lastQuoteAt = new Date().toISOString();
    // Write trade record FIRST (source of truth), then the done-key marker.
    // If worker is evicted between the two writes, next tick still finds the
    // filled trade in `bobf_open_trade` and treats it correctly. Reverse order
    // would mark "done" but lose the fill snapshot.
    await env.SIGNAL_KV.put('bobf_open_trade', JSON.stringify(trade));
    if (justFilled) {
      await env.SIGNAL_KV.put(`bobf_done_${isoDateET(etNow)}`, 'filled', { expirationTtl: 86400 });
    }
  } catch (e) { console.warn('[bobf] refresh failed:', e.message); }
  return trade;
}

// Settle BOBF at SPX close. PnL from intrinsic of the call butterfly:
//   pnl = (lower_intrinsic − 2*body_intrinsic + upper_intrinsic − debit) × 100
async function settleBobfEOD(env, etNow, spxClose, dateISO = null) {
  // dateISO: settle a PAST day's orphaned trade (P18 sweep / manual heal).
  const raw = await env.SIGNAL_KV.get('bobf_open_trade');
  if (!raw) return { status: 'no-trade' };
  const trade = JSON.parse(raw);
  const targetISO = dateISO || isoDateET(etNow);
  if (trade.openDate !== targetISO) return { status: 'wrong-date', openDate: trade.openDate };
  if (trade.status === 'closed') return { status: 'already-closed' };
  if (trade.status === 'expired') return { status: 'expired-no-trade' };
  if (trade.status !== 'filled') return { status: trade.status };

  const lowerI = Math.max(spxClose - trade.lowerStrike, 0);
  const bodyI  = Math.max(spxClose - trade.bodyStrike,  0);
  const upperI = Math.max(spxClose - trade.upperStrike, 0);
  const intrinsic = lowerI - 2 * bodyI + upperI;
  const pnl = Math.round((intrinsic - trade.entryDebit) * 100 * trade.contracts);

  // History P/L FIRST, mark 'closed' after (audit P2 2026-07-06 — idempotent re-settle).
  const bobfFields = { bobfPL: pnl };
  if (dateISO) bobfFields.spxClose = parseFloat(spxClose.toFixed(2));
  await upsertHistoryGitHub(env, trade.openDate, bobfFields);

  trade.status = 'closed';
  trade.closeDate = targetISO;
  trade.spxClose = parseFloat(spxClose.toFixed(2));
  trade.closeIntrinsic = parseFloat(intrinsic.toFixed(2));
  trade.pnl = pnl;
  await env.SIGNAL_KV.put('bobf_open_trade', JSON.stringify(trade));

  const logRaw = await env.SIGNAL_KV.get('bobf_closed_log');
  const log = logRaw ? JSON.parse(logRaw) : [];
  log.unshift(trade);
  await env.SIGNAL_KV.put('bobf_closed_log', JSON.stringify(log.slice(0, 30)));
  return { status: 'settled', pnl, type: trade.type, strikes: [trade.lowerStrike, trade.bodyStrike, trade.upperStrike], debit: trade.entryDebit, intrinsic };
}

// ════════════════════════════════════════════════════════════════════
// GXBF TRADE HANDLER (live)
// ────────────────────────────────────────────────────────────────────
// GXBF = compute volume-weighted dealer-gamma peak live from the Schwab
// 0DTE call chain → use that strike as the butterfly center → widen the
// wing until risk ≈ reward (closest to exactly 50/50) → 0DTE SPXW long
// CALL fly → hold to 4 PM cash close.
//
// Pipeline mirrors BOBF/Straddle verbatim:
//   - Gate: only act when the morning signal theme is 'gxbf' (analogous to
//     the Straddle `signal.theme === 'strad'` branch). STRATEGY
//     INDEPENDENCE: GXBF never blocks / is blocked by M8BF/Straddle/BOBF;
//     its card reflects only GXBF's own state.
//   - Idempotent via gxbf_done_<date>.
//   - Center computed in-house via computeGxbfCenterLive (Black-Scholes
//     gamma × totalVolume × spot² × 100 × 0.01, per-strike, ±5% range).
//     Was Discord-scraped historically; now uses the live chain only.
//   - Long-call butterfly: BUY 1 @ K−W, SELL 2 @ K, BUY 1 @ K+W.
//   - Wing W ∈ [5,150] step 5. netDebit = mid(K−W) − 2·mid(K) + mid(K+W).
//     risk = netDebit, reward = W − netDebit. Keep candidates where
//     netDebit > 0 && risk ≤ reward (netDebit ≤ W/2). Pick W minimizing
//     |netDebit − W/2|.
//   - Held to expiration. PnL from intrinsic at SPX close S:
//     clamp(max(0,S−(K−W)) − 2·max(0,S−K) + max(0,S−(K+W)), 0, W).
//     pnl = (intrinsic − netDebit) × 100 × contracts  (same convention as
//     settleBobfEOD / settleStraddleEOD).
// ════════════════════════════════════════════════════════════════════

const GXBF_WING_MIN  = 5;
const GXBF_WING_MAX  = 100;   // cap → max risk = W/2 = 50pt = $5,000/contract
const GXBF_WING_STEP = 5;

// GXBF entry window — opens at 09:35 ET (NOT earlier).
//
// 2026-06-09 FIX: window used to start at 09:33 ET, which dated back to the
// pre-2026-05 Discord-scrape era where the worker watched for the "Major
// Positive by Volume" Discord post that landed ~09:34:59 ET. With the
// in-house live-gamma replacement (computeGxbfCenterLive), there is NO
// reason to attempt entry before 09:35 — the SPX 0DTE call chain quotes
// settle in the first ~5 minutes of regular session, and firing at 09:33
// reads stale/transient quotes for the gamma center.
//
// User feedback (2026-06-09): "GXBF should not trade before 9:35 — at 9:35
// you check the KX (gamma) levels, then fire ~9:36." Window aligned with
// signal-engine.js `rec = "GXBF @ 9:36 AM"`.
function gxbfInWindow(etNow) {
  const h = etNow.getHours(), m = etNow.getMinutes();
  return h === 9 && m >= 35 && m <= 45;
}
function gxbfPastWindow(etNow) {
  const h = etNow.getHours(), m = etNow.getMinutes();
  return (h === 9 && m > 45) || h > 9;
}

// Compute the GXBF gamma center LIVE from the Schwab/Tasty option chain.
// Replaces the old Discord "Major Positive by Volume" scraper — same idea
// (volume-weighted dealer gamma peak) but computed in-house from the live
// 0DTE call chain. Uses the same Black-Scholes gamma formula as calculateGEX
// (R=0.043, Q=0.013) and the same ±5% strike range filter. Live entry runs
// at 09:35 ET so we hard-code today's 16:00 ET close for T (half-day case
// is rare and the morning T is dominated by the day-fraction anyway).
//
// Returns { center, centerOI, spot, _source: 'live-chain' } where:
//   - center   = strike with max volume-weighted gamma (snapped to 5)
//   - centerOI = strike with max open-interest-weighted gamma (snapped to 5)
//
// ─────────────────────────────────────────────────────────────────────────
// FULL METHODOLOGY: see tasks/GXBF_METHODOLOGY.md  (the durable reference).
// Read it before touching this function, fetch_thetadata_gxbf.py::build_day,
// or any GXBF gating logic in signal-engine.js. It covers:
//   • why per-strike IV (NOT a uniform VIX-as-σ),
//   • the S²·100·0.01 dealer-exposure factors (mirrored from calculateGEX),
//   • the hybrid live rule (OI on OPEX-1 / VIX-exp / FED, else volume),
//   • known bugs we've hit and the fixes that stuck,
//   • the deleted Discord scraper (commit 8280d55) — do not chase it.
// ─────────────────────────────────────────────────────────────────────────
function computeGxbfCenterLive(callExpDateMap, expDate, spot) {
  if (!callExpDateMap || !spot || spot <= 0) return null;
  const R = 0.043, Q = 0.013, MULT = 100;
  const expKey = Object.keys(callExpDateMap).find(k => k.startsWith(expDate + ':'));
  if (!expKey) return null;

  // T = hours-to-16:00-ET / (365·24). Floor of 15 min so T never collapses
  // to zero if called late in the window. (Matches calculateGEX::zeroDteT.)
  const etNow = toET(new Date());
  const hrsLeft = (16 - etNow.getHours()) - (etNow.getMinutes() / 60) - (etNow.getSeconds() / 3600);
  const safeHrs = Math.max(hrsLeft, 0.25);
  const T = safeHrs / (365 * 24);

  const normPdf = (x) => Math.exp(-0.5 * x * x) / Math.sqrt(2 * Math.PI);
  const bsGamma = (S, K, sigma) => {
    if (T <= 0 || sigma <= 0 || S <= 0) return 0;
    const d1 = (Math.log(S / K) + (R - Q + 0.5 * sigma * sigma) * T) / (sigma * Math.sqrt(T));
    return normPdf(d1) * Math.exp(-Q * T) / (S * sigma * Math.sqrt(T));
  };

  // ±5% range matches calculateGEX's chart-window filter (line 3840-3841).
  const rangePct = 0.05;
  const lo = spot * (1 - rangePct), hi = spot * (1 + rangePct);

  const strikes = callExpDateMap[expKey] || {};
  let maxVol = 0, centerByVolume = null;
  let maxOI  = 0, centerByOI     = null;

  for (const strikeStr of Object.keys(strikes)) {
    const contracts = strikes[strikeStr] || [];
    for (const c of contracts) {
      const K = parseFloat(c.strikePrice != null ? c.strikePrice : strikeStr);
      if (!isFinite(K) || K <= 0) continue;
      if (K < lo || K > hi) continue;
      const vol = Math.max(c.totalVolume || 0, 0);
      const oi  = Math.max(c.openInterest || 0, 0);
      if (vol === 0 && oi === 0) continue;
      const iv = (c.volatility || 0) / 100;
      const gamma = bsGamma(spot, K, iv > 0 ? iv : 0.2);
      const gex_vol = gamma * vol * spot * spot * MULT * 0.01;
      const gex_oi  = gamma * oi  * spot * spot * MULT * 0.01;
      if (gex_vol > maxVol) { maxVol = gex_vol; centerByVolume = K; }
      if (gex_oi  > maxOI)  { maxOI  = gex_oi;  centerByOI     = K; }
    }
  }

  if (centerByVolume == null) return null;
  return {
    center:   snap5(Math.round(centerByVolume)),
    centerOI: centerByOI != null ? snap5(Math.round(centerByOI)) : null,
    spot,
    _source: 'live-chain',
  };
}

// Wing selection: KEEP WIDENING, take the WIDEST wing where risk ≤ reward.
// Iterate W from GXBF_WING_MIN to GXBF_WING_MAX step GXBF_WING_STEP.
// `midFn(strike)` returns the option mid or null. For each W require all 3
// strikes (K−W, K, K+W) to exist with a valid mid. netDebit = mid(K−W) −
// 2·mid(K) + mid(K+W). risk = netDebit, reward = W − netDebit. A wing is
// valid iff netDebit > 0 && risk ≤ reward (i.e. netDebit ≤ W/2). It does
// NOT need to be exactly 50/50 — only that risk never exceeds reward. Since
// W ascends, overwriting on every valid W leaves the WIDEST valid wing.
// Returns that candidate, or null if none qualify (→ no trade).
function selectGxbfWing(K, midFn) {
  let bestC = null;
  for (let W = GXBF_WING_MIN; W <= GXBF_WING_MAX; W += GXBF_WING_STEP) {
    const lowerMid = midFn(K - W);
    const centerMid = midFn(K);
    const upperMid = midFn(K + W);
    if (lowerMid == null || centerMid == null || upperMid == null) continue;
    const netDebit = lowerMid - 2 * centerMid + upperMid;
    const risk = netDebit;
    const reward = W - netDebit;
    if (!(netDebit > 0 && risk <= reward)) continue;  // risk ≤ reward (netDebit ≤ W/2)
    bestC = { W, netDebit, risk, reward,
              lowerMid, centerMid, upperMid };          // widest valid so far
  }
  return bestC;
}

// Main GXBF entry flow — called from the morning post-Discord deferred block
// (gated on signal.theme === 'gxbf'). Idempotent via gxbf_done_<date>.
async function handleGxbfEntry(env, etNow, signal, preChain = null) {
  const todayISO = isoDateET(etNow);
  const out = { date: todayISO, status: null };
  const doneKey = `gxbf_done_${todayISO}`;

  const done = await env.SIGNAL_KV.get(doneKey);
  // A `claim:` value is OUR OWN caller's in-flight claim token (P22 gate), not a
  // completed run — treat only terminal markers as done. (2026-07-20: the claim
  // gate made this self-check abort every tick → GXBF never fired on OPEX+1.)
  if (done && !done.startsWith('claim:')) return { ...out, status: 'already-done', why: done };

  // 2026-06-09 belt-and-suspenders: even if the caller's window gate is
  // wrong or removed, refuse to fire before 09:35 ET. This guarantees the
  // SPX 0DTE chain quotes have had a few minutes to settle so the live
  // gamma center is real, not a transient post-open value. User reported
  // an early 09:33 fire today — gxbfInWindow was the root cause, but if
  // any other entry-point ever calls this directly, it stays protected.
  {
    const h = etNow.getHours(), m = etNow.getMinutes();
    const beforeFireTime = (h < 9) || (h === 9 && m < 35);
    if (beforeFireTime) {
      console.warn(`[gxbf] entry refused — too early (${h}:${String(m).padStart(2,'0')} ET, need ≥ 09:35)`);
      return { ...out, status: 'too-early', etTime: `${h}:${String(m).padStart(2,'0')}` };
    }
  }

  // ── 9:35 gamma-sign gate (owner order 2026-07-31): a NEGATIVE same-day
  // 0DTE book at signal time = skip GXBF. Backtest (73 traded days): negative
  // 9:35 book −$369/d 59% WR (lost 2024+2025) vs positive +$999/d 70%.
  // FAIL-OPEN: a missing/stale snapshot must never eat a valid trade — gate
  // only acts on a fresh reading. Interim rule; all-expiry decision pending.
  try {
    const g0Raw = await env.SIGNAL_KV.get('gex_current_0dte');
    const g0 = g0Raw ? JSON.parse(g0Raw) : null;
    const fresh = g0 && g0.timestamp && (Date.now() / 1000 - g0.timestamp) < 600;
    if (fresh && typeof g0.totalGex === 'number' && g0.totalGex <= 0) {
      const bn = (g0.totalGex / 1e9).toFixed(1);
      // GXBF itself stands down. AUDIT FIX 2026-07-31 (marker-before-send):
      // the terminal `gamma-gate:` marker is now written at the END of each
      // COMPLETED outcome below — not here. A transient failure mid-branch
      // leaves no terminal, the caller releases its claim, and the next tick
      // in the 9:35–9:45 window retries the whole gate. In-flight double-entry
      // is prevented by the caller's claimSendSlot on gxbf_done_<date>.

      // ── GATED-DAY STRADDLE (owner ship order 2026-07-30 PM) ──
      // A gated day converts to the standard open-strike straddle: strike =
      // snap5(SPX open), limit ≤ $32, work until 13:30, hold to settlement.
      // Once the trade is in straddle_open_trade KV the normal Straddle
      // lifecycle (refresh → fill/expire → EOD stradPL) runs it untouched.
      // Basis (12 historical gated days): straddle +$11,711, 5/7 fills,
      // +$3,058 ex-crash-days, 2026 +$2,104 — the only tested expression
      // positive without a crash. No day-of-week ban (tested recipe had none;
      // the regular-Wednesday straddle rule applies to the DAILY signal only).
      // `gated: true` exempts it from the /straddle-today phantom-#c cleaner
      // (which otherwise deletes straddles opened on a non-strad-theme day).
      // Direction check (owner 2026-07-31): the straddle conversion requires
      // overnight VIX DOWN. Trigger days qualify by definition (drop > 0.65 IS
      // the trigger); a pure OPEX+1 gated morning with VIX flat/up takes NO
      // trade — regular logic, and the regular straddle needs a drop too.
      let oNight = (signal && typeof signal.oNight === 'number' && isFinite(signal.oNight)) ? signal.oNight : null;
      if (oNight == null) {
        try {
          const msdRawG = await env.SIGNAL_KV.get(`morning_signal_data_${todayISO}`);
          const msdG = msdRawG ? JSON.parse(msdRawG) : null;
          if (msdG && typeof msdG.oNight === 'number' && isFinite(msdG.oNight)) oNight = msdG.oNight;
        } catch (_) {}
      }
      if (!(oNight != null && oNight > 0)) {
        const why = oNight == null ? 'no VIX reading' : `VIX ${oNight < 0 ? 'up' : 'flat'} overnight (${(-oNight).toFixed(2)})`;
        try {
          const dcRaw = await env.SIGNAL_KV.get('discord_config');
          const dc = dcRaw ? JSON.parse(dcRaw) : null;
          if (dc && dc.channelId) await sendDiscordDM(env, dc.channelId,
            `⚪ **GXBF** — gamma-gated (${bn}B) on a ${why} morning. No straddle conversion (regular logic: overnight VIX must be down). No trade today.`, dc.proxyUrl);
        } catch (_) {}
        await env.SIGNAL_KV.put(doneKey, `gamma-gate:${bn}B:no-conv`, { expirationTtl: 86400 });
        return { ...out, status: 'gamma-gate', totalGex: g0.totalGex, gatedStraddle: `no-conversion:${why}` };
      }
      let gatedStraddle = null;
      try {
        const exRaw = await env.SIGNAL_KV.get('straddle_open_trade');
        const ex = exRaw ? JSON.parse(exRaw) : null;
        if (ex && ex.openDate === todayISO) {
          gatedStraddle = 'straddle-already-open';
        } else {
          const tok = await getAccessToken(env);
          const gs = await openStraddleTrade(env, tok, etNow, { badge: 'GATED STRADDLE' }, preChain);
          gs.gated = true;
          await env.SIGNAL_KV.put('straddle_open_trade', JSON.stringify(gs));
          await logEvent(env, 'info', 'gated-strad', `opened ${gs.status}`, {
            strike: gs.strike, entryDebit: gs.entryDebit, maxDebit: gs.maxDebit, gate: `${bn}B` });
          const _MON = ['JAN','FEB','MAR','APR','MAY','JUN','JUL','AUG','SEP','OCT','NOV','DEC'];
          const tosD = `${+todayISO.slice(8, 10)} ${_MON[+todayISO.slice(5, 7) - 1]} ${todayISO.slice(2, 4)}`;
          const lmt = (gs.status === 'filled' ? gs.entryDebit : gs.maxDebit).toFixed(2);
          const msg = `**GATED STRADDLE**\n` +
            `BUY +1 STRADDLE SPX 100 (Weeklys) ${tosD} ${gs.strike} CALL/PUT @${lmt} LMT\n` +
            (gs.status === 'filled'
              ? `Fillable now (mid $${gs.entryDebit.toFixed(2)}).`
              : `Working limit $${gs.maxDebit.toFixed(2)} — cancel 13:30 ET if unfilled.`) +
            ` Hold to settlement — no TP/SL.\n` +
            `-# GXBF stood down: 9:35 0DTE gamma ${bn}B (negative book) — gated days trade the straddle instead.`;
          try {
            const dcRaw = await env.SIGNAL_KV.get('discord_config');
            const dc = dcRaw ? JSON.parse(dcRaw) : null;
            if (dc && dc.channelId) await sendDiscordDM(env, dc.channelId, msg, dc.proxyUrl);
          } catch (_) {}
          try { await fanoutSubscribers(env, msg); } catch (_) {}
          gatedStraddle = `${gs.status}:K${gs.strike}@${gs.entryDebit}`;
        }
      } catch (se) {
        console.warn('[gxbf] gated straddle open failed:', se.message);
        try { await logEvent(env, 'error', 'gated-strad', 'open failed', { msg: se.message, stack: (se.stack || '').slice(0, 300) }); } catch (_) {}
        try {
          const dcRaw = await env.SIGNAL_KV.get('discord_config');
          const dc = dcRaw ? JSON.parse(dcRaw) : null;
          if (dc && dc.channelId) await sendDiscordDM(env, dc.channelId,
            `⚠️ **GATED STRADDLE failed to open** (${se.message}) — GXBF gamma-gated at ${bn}B. ` +
            `Auto-retrying each tick until 9:45 ET. If it keeps failing, place manually: open-strike straddle, limit $32, cancel 13:30, hold to settle.`, dc.proxyUrl);
        } catch (_) {}
        // AUDIT FIX 2026-07-31: NO terminal marker on failure — the caller
        // releases its claim and the next in-window tick retries the gate.
        return { ...out, status: 'gamma-gate', totalGex: g0.totalGex, gatedStraddle: 'open-FAILED-retrying' };
      }
      await env.SIGNAL_KV.put(doneKey, `gamma-gate:${bn}B`, { expirationTtl: 86400 });
      return { ...out, status: 'gamma-gate', totalGex: g0.totalGex, gatedStraddle };
    }
  } catch (e) { console.warn('[gxbf] gamma gate check failed (fail-open):', e.message); }

  if (gxbfPastWindow(etNow)) {
    // If a center was never computable all window, the live SPX chain was likely
    // unavailable (Schwab down → greeks/volume-less fallback) and GXBF can't build a
    // volume-weighted center from it. Alert instead of skipping silently (audit P1-f).
    if (!(await env.SIGNAL_KV.get(`gxbf_center_seen_${todayISO}`))) {
      try {
        const dcRaw = await env.SIGNAL_KV.get('discord_config');
        const dc = dcRaw ? JSON.parse(dcRaw) : null;
        if (dc && dc.channelId) await sendDiscordDM(env, dc.channelId,
          `⚠️ **GXBF** — no trade. Entry window passed without a computable center (live SPX chain unavailable — the fallback chain lacks volume/OI). No order placed.`, dc.proxyUrl);
      } catch (_) {}
    }
    await env.SIGNAL_KV.put(doneKey, 'window-passed', { expirationTtl: 86400 });
    return { ...out, status: 'window-passed' };
  }

  // 1. Build the chain FIRST (reuse master chain — zero extra Schwab calls).
  //    We need the chain to compute the gamma center live.
  let token, spot, callExpDateMap;
  try {
    token = preChain ? null : await getAccessToken(env);   // skip token when the chain is in hand — a dead Schwab token must not error a preChain entry (audit P1-f)
    const chain = preChain || await fetchSpxFullChain(token, todayISO, env);
    spot = chain.spot; callExpDateMap = chain.callExpDateMap;
  } catch (e) { return { ...out, status: 'error', error: 'chain fetch: ' + e.message }; }
  if (!spot) return { ...out, status: 'error', error: 'no SPX spot' };

  // 2. Compute the gamma center LIVE from the chain (volume- AND OI-weighted).
  //    Replaces the old Discord scraper. snap5/round already applied inside.
  const computed = computeGxbfCenterLive(callExpDateMap, todayISO, spot);
  if (!computed) {
    // No qualifying strikes (empty 0DTE chain, or a greeks/volume-less fallback chain).
    // Don't mark done — later ticks retry; window-passed alerts if we never compute a
    // center all window (audit P1-f 2026-07-06).
    return { ...out, status: 'no-center', reason: 'live-gamma compute returned null' };
  }
  await env.SIGNAL_KV.put(`gxbf_center_seen_${todayISO}`, '1', { expirationTtl: 86400 });

  // 2a. Pick the per-day center via signal.centerSource (hybrid routing).
  //     'oi'  → OPEX-1 / VIX-expiry / FED days (per signal-engine.js)
  //     'vol' → all other GXBF days (default)
  //     Fallback to volume center with a warning if OI was requested but null.
  const requestedSource = (signal && signal.centerSource) || 'vol';
  let centerSource = requestedSource;
  let K;
  if (requestedSource === 'oi') {
    if (computed.centerOI != null) {
      K = computed.centerOI;
    } else {
      K = computed.center;
      centerSource = 'vol-fallback';
      console.warn(`[gxbf] centerSource=oi requested but computed.centerOI is null; falling back to volume center ${K}`);
      await logEvent(env, 'warn', 'gxbf-center-fallback',
        'centerSource=oi requested but centerOI null; using volume center',
        { volCenter: computed.center, centerOI: computed.centerOI });
    }
  } else {
    K = computed.center;
  }

  // 4. Wing 50/50 selection. midFn pulls the call mid for an exact strike
  //    (no fuzz — symmetric debit math requires exact strikes).
  const legCache = new Map();
  const legFor = (strike) => {
    if (legCache.has(strike)) return legCache.get(strike);
    const leg = pickContractFromChain(callExpDateMap, todayISO, strike);
    const exact = (leg && leg.strike === strike && leg.mid != null) ? leg : null;
    legCache.set(strike, exact);
    return exact;
  };
  const midFn = (strike) => { const l = legFor(strike); return l ? l.mid : null; };

  const pick = selectGxbfWing(K, midFn);
  if (!pick) {
    await env.SIGNAL_KV.put(doneKey, 'no-wing: no W with netDebit>0 & risk≤reward', { expirationTtl: 86400 });
    await logEvent(env, 'warn', 'gxbf-skip', 'no qualifying wing (risk≤reward) found', { center: K, centerSource, centerOI: computed.centerOI, centerVol: computed.center });
    try {
      const dcRaw = await env.SIGNAL_KV.get('discord_config');
      if (dcRaw) {
        const dc = JSON.parse(dcRaw);
        if (dc.channelId) await sendDiscordDM(env, dc.channelId,
          `⚠️ **GXBF** — no trade. Center ${K} (${centerSource} grid); no wing 5–100 had netDebit>0 with risk ≤ reward.`,
          dc.proxyUrl);
      }
    } catch (_) { /* non-critical */ }
    return { ...out, status: 'no-wing', center: K, centerSource };
  }

  const W = pick.W;
  const kLower = K - W, kUpper = K + W;
  const lowerLeg = legFor(kLower);
  const centerLeg = legFor(K);
  const upperLeg = legFor(kUpper);
  if (!lowerLeg || !centerLeg || !upperLeg) {
    return { ...out, status: 'error', error: `selected wing ${W} leg vanished on re-fetch (K=${K})` };
  }

  const netDebit = pick.netDebit;
  const maxRisk = netDebit;
  const maxReward = W - netDebit;

  // Daily total-risk cap (2026-06-09): fly max loss = netDebit × 100.
  // Status-return: stays un-done so later ticks in the 9:35-9:45 window
  // retry if exposure clears.
  {
    const gxbfGate = await enforceRiskCap(env, etNow, 'gxbf', netDebit * 100);
    if (!gxbfGate.ok) {
      return { ...out, status: 'risk-cap-blocked', detail: gxbfGate.reason };
    }
  }

  const trade = {
    openDate: todayISO,
    openTimeET: `${String(etNow.getHours()).padStart(2,'0')}:${String(etNow.getMinutes()).padStart(2,'0')}`,
    label: 'GXBF',
    center: K,
    wing: W,
    centerSource,                  // 'oi' | 'vol' | 'vol-fallback' (hybrid per-day)
    centerSourceRequested: requestedSource,
    centerVol: computed.center,
    centerOI: computed.centerOI,
    spotEntry: parseFloat(spot.toFixed(2)),
    lowerStrike: kLower, centerStrike: K, upperStrike: kUpper,
    expDate: todayISO,
    lowerSymbol: lowerLeg.symbol, centerSymbol: centerLeg.symbol, upperSymbol: upperLeg.symbol,
    entryLowerMid:  parseFloat(lowerLeg.mid.toFixed(2)),
    entryCenterMid: parseFloat(centerLeg.mid.toFixed(2)),
    entryUpperMid:  parseFloat(upperLeg.mid.toFixed(2)),
    netDebit:  parseFloat(netDebit.toFixed(2)),
    maxRisk:   parseFloat(maxRisk.toFixed(2)),
    maxReward: parseFloat(maxReward.toFixed(2)),
    contracts: 1,
    // Live fields
    currentSpot:      parseFloat(spot.toFixed(2)),
    currentLowerMid:  parseFloat(lowerLeg.mid.toFixed(2)),
    currentCenterMid: parseFloat(centerLeg.mid.toFixed(2)),
    currentUpperMid:  parseFloat(upperLeg.mid.toFixed(2)),
    currentValue:     parseFloat(netDebit.toFixed(2)),
    currentPnl: 0,
    lastQuoteAt: new Date().toISOString(),
    status: 'filled',
  };

  // Write trade record FIRST (source of truth), then the done-key marker.
  await env.SIGNAL_KV.put('gxbf_open_trade', JSON.stringify(trade));
  await env.SIGNAL_KV.put(doneKey, 'filled', { expirationTtl: 86400 });

  console.log(`[gxbf] opened filled center=${K} (source=${centerSource}) wing=${W} netDebit=${netDebit.toFixed(2)} (risk ${maxRisk.toFixed(2)} ≤ reward ${maxReward.toFixed(2)})`);
  await logEvent(env, 'info', 'gxbf-open', 'opened filled', {
    center: K, wing: W, netDebit: parseFloat(netDebit.toFixed(2)),
    maxRisk: parseFloat(maxRisk.toFixed(2)), maxReward: parseFloat(maxReward.toFixed(2)),
    centerSource, centerSourceRequested: requestedSource,
    centerVol: computed.center, centerOI: computed.centerOI, spot: parseFloat(spot.toFixed(2)),
  });

  // Independent Discord notification (separate from the morning signal post).
  try {
    const dcRaw = await env.SIGNAL_KV.get('discord_config');
    if (dcRaw) {
      const dc = JSON.parse(dcRaw);
      const sourceLabel = centerSource === 'oi' ? 'OI-weighted' : centerSource === 'vol-fallback' ? 'Volume (OI fallback)' : 'Volume-weighted';
      const altLabel = centerSource === 'oi' ? `Volume ${computed.center}` : (computed.centerOI != null ? `OI ${computed.centerOI}` : null);
      if (dc.channelId) await sendDiscordDM(env, dc.channelId,
        `🦋 **GXBF opened** — SPX ${kLower}/${K}/${kUpper} CALL fly (wing ${W})\n` +
        `Net debit $${netDebit.toFixed(2)} · max risk $${(maxRisk*100).toFixed(0)} · max reward $${(maxReward*100).toFixed(0)} · 0DTE\n` +
        `Center ${K} (${sourceLabel} · live gamma calc)${altLabel ? ` · alt ${altLabel}` : ''} · spot ${spot.toFixed(2)}`,
        dc.proxyUrl);
    }
  } catch (_) { /* non-critical */ }

  return { ...out, status: 'opened', center: K, wing: W, netDebit, centerSource };
}

// Refresh live mids on the open GXBF trade. Reuses the master chain (zero
// extra Schwab calls). Mirrors refreshBobfLiveQuotes.
async function refreshGxbfLiveQuotes(env, token, etNow, preChain = null) {
  const raw = await env.SIGNAL_KV.get('gxbf_open_trade');
  if (!raw) return null;
  const trade = JSON.parse(raw);
  if (trade.status === 'closed' || trade.status === 'expired') return trade;

  try {
    const chain = preChain || await fetchSpxFullChain(token, trade.expDate, env);
    const { spot, callExpDateMap } = chain;
    const lower  = pickContractFromChain(callExpDateMap, trade.expDate, trade.lowerStrike);
    const center = pickContractFromChain(callExpDateMap, trade.expDate, trade.centerStrike);
    const upper  = pickContractFromChain(callExpDateMap, trade.expDate, trade.upperStrike);
    if (!lower || !center || !upper || lower.mid == null || center.mid == null || upper.mid == null) return trade;

    const newDebit = lower.mid - 2 * center.mid + upper.mid;
    trade.currentSpot      = spot ? parseFloat(spot.toFixed(2)) : trade.currentSpot;
    trade.currentLowerMid  = parseFloat(lower.mid.toFixed(2));
    trade.currentCenterMid = parseFloat(center.mid.toFixed(2));
    trade.currentUpperMid  = parseFloat(upper.mid.toFixed(2));
    trade.currentValue     = parseFloat(newDebit.toFixed(2));
    if (trade.status === 'filled') {
      trade.currentPnl = Math.round((trade.currentValue - trade.netDebit) * 100 * trade.contracts);
    }
    trade.lastQuoteAt = new Date().toISOString();
    await env.SIGNAL_KV.put('gxbf_open_trade', JSON.stringify(trade));
  } catch (e) { console.warn('[gxbf] refresh failed:', e.message); }
  return trade;
}

// Mark-to-market the open M8BF butterfly every market tick from the shared
// master chain (zero extra Schwab calls) and stash it in m8bf_live_<date>.
// Without this, GET /trade has no live mid and live.html falls back to the
// AT-EXPIRATION intrinsic, which overstates intraday profit while the short
// body still carries extrinsic (observed: card showed +$1,320 vs ~+$400 real
// at 11:05). Mirrors refreshBobf/GxbfLiveQuotes. M8BF is stateless (no
// m8bf_open_trade KV), so the trade is re-derived via the SHARED
// selectM8bfQualifying — guaranteeing the quoted legs == the /trade legs.
async function refreshM8bfLiveQuotes(env, token, etNow, preChain = null) {
  try {
    if (await m8bfBannedReason(env, etNow)) return;
    const sel = await selectM8bfQualifying(env, etNow);
    if (sel.status !== 'open' || !sel.qualifying) return;
    const q = sel.qualifying;
    const expDate = sel.todayT;  // M8BF is 0DTE
    const chain = preChain || await fetchSpxFullChain(token, expDate, env);
    const spot = chain.spot;
    // cp 0 = CALL fly, 1 = PUT fly. Long-fly net debit is the same convex
    // combination of the option mids either way: low − 2·center + high.
    const map = (q.cp === 1) ? chain.putExpDateMap : chain.callExpDateMap;
    if (!map) return;
    const lower  = pickContractFromChain(map, expDate, q.lower);
    const center = pickContractFromChain(map, expDate, q.center);
    const upper  = pickContractFromChain(map, expDate, q.upper);
    if (!lower || !center || !upper ||
        lower.mid == null || center.mid == null || upper.mid == null) return;
    const curVal = lower.mid - 2 * center.mid + upper.mid;
    const rec = {
      currentValue:     parseFloat(curVal.toFixed(2)),
      currentLowerMid:  parseFloat(lower.mid.toFixed(2)),
      currentCenterMid: parseFloat(center.mid.toFixed(2)),
      currentUpperMid:  parseFloat(upper.mid.toFixed(2)),
      currentSpot:      spot ? parseFloat(spot.toFixed(2)) : null,
      currentPnl:       Math.round((curVal - q.premium) * 100),  // 1 contract (M8BF convention everywhere)
      signal_time:      q.time,
      lastQuoteAt:      new Date().toISOString(),
    };
    await env.SIGNAL_KV.put(`m8bf_live_${sel.todayT}`, JSON.stringify(rec), { expirationTtl: 86400 });
  } catch (e) { console.warn('[m8bf] live refresh failed:', e.message); }
}

// Settle GXBF at SPX close. Long-call-fly intrinsic at SPX close S:
//   clamp( max(0,S−(K−W)) − 2·max(0,S−K) + max(0,S−(K+W)), 0, W )
//   pnl = (intrinsic − netDebit) × 100 × contracts
// (same per-point-per-contract convention as settleBobfEOD/settleStraddleEOD)
async function settleGxbfEOD(env, etNow, spxClose, dateISO = null) {
  // dateISO: settle a PAST day's orphaned trade (P18 sweep / manual heal).
  const raw = await env.SIGNAL_KV.get('gxbf_open_trade');
  if (!raw) return { status: 'no-trade' };
  const trade = JSON.parse(raw);
  const targetISO = dateISO || isoDateET(etNow);
  if (trade.openDate !== targetISO) return { status: 'wrong-date', openDate: trade.openDate };
  if (trade.status === 'closed') return { status: 'already-closed' };
  if (trade.status === 'expired') return { status: 'expired-no-trade' };
  if (trade.status !== 'filled') return { status: trade.status };

  const W = trade.wing;
  const lowerI  = Math.max(spxClose - trade.lowerStrike,  0);
  const centerI = Math.max(spxClose - trade.centerStrike, 0);
  const upperI  = Math.max(spxClose - trade.upperStrike,  0);
  const rawIntrinsic = lowerI - 2 * centerI + upperI;
  const intrinsic = Math.min(Math.max(rawIntrinsic, 0), W);
  const pnl = Math.round((intrinsic - trade.netDebit) * 100 * trade.contracts);

  // History P/L FIRST, mark 'closed' after (audit P2 2026-07-06 — idempotent re-settle).
  const gxbfFields = { gxbfPL: pnl };
  if (dateISO) gxbfFields.spxClose = parseFloat(spxClose.toFixed(2));
  await upsertHistoryGitHub(env, trade.openDate, gxbfFields);

  trade.status = 'closed';
  trade.closeDate = targetISO;
  trade.spxClose = parseFloat(spxClose.toFixed(2));
  trade.closeIntrinsic = parseFloat(intrinsic.toFixed(2));
  trade.pnl = pnl;
  await env.SIGNAL_KV.put('gxbf_open_trade', JSON.stringify(trade));

  const logRaw = await env.SIGNAL_KV.get('gxbf_closed_log');
  const log = logRaw ? JSON.parse(logRaw) : [];
  log.unshift(trade);
  await env.SIGNAL_KV.put('gxbf_closed_log', JSON.stringify(log.slice(0, 30)));
  return { status: 'settled', pnl, center: trade.centerStrike, wing: W,
           strikes: [trade.lowerStrike, trade.centerStrike, trade.upperStrike],
           debit: trade.netDebit, intrinsic };
}

// ════════════════════════════════════════════════════════════════════
// TAIL HEDGE — live-trade parity (2026-06-22). Brings Tail Hedge onto the
// SAME conveyor as strad/bobf/gxbf: cron freezes the open → intraday P&L →
// EOD settle writes tailPL → first profitable day STOPS the campaign.
// Entry cost basis = candidate MID (matches the backtest that produced the
// historical tailPL column; skipper tracks its own real fill separately).
// ────────────────────────────────────────────────────────────────────

// Freeze today's tail open from the 9:45 put snapshot once we're past 9:45 ET
// on a TRADE day. Idempotent (first call wins). Called by BOTH the cron (robust,
// no page-poll needed) and GET /tail-today.
// Fan out the frozen tail order to subscribers EXACTLY ONCE per day, ONLY when
// announce=true (the cron path). /tail-today (a GET the live page polls every
// few seconds) also freezes the trade for display but passes announce=false — a
// GET must never send Discord, else each poll near 9:45 re-fires the DM before
// the KV guard propagates (the 2→3 duplicate-tail bug, 2026-06-30). Runs
// regardless of WHO froze the trade first, so a poll freezing it never
// suppresses the cron's announce. `tail_fanout_<date>` caps it at one send even
// across back-to-back cron ticks. Format = live page's renderTail copy button
// (SPX 100 (Weeklys), see [[feedback_tos_copy_spx_weeklys]]).
async function announceTailIfDue(env, tailOpen, todayISO, announce) {
  if (TAIL_RETIRED) return null;              // retired 2026-08-03 (announce)
  if (!announce || !tailOpen) return tailOpen;
  const fkey = `tail_fanout_${todayISO}`;
  if (await env.SIGNAL_KV.get(fkey)) return tailOpen;
  await env.SIGNAL_KV.put(fkey, '1', { expirationTtl: 7 * 86400 });
  try {
    const _MON = ['JAN','FEB','MAR','APR','MAY','JUN','JUL','AUG','SEP','OCT','NOV','DEC'];
    const e = String(tailOpen.expDate);
    const tosD = `${+e.slice(8,10)} ${_MON[+e.slice(5,7)-1]} ${e.slice(2,4)}`;
    const px = tailOpen.entryMid ?? tailOpen.entryAsk ?? tailOpen.entryBid;
    if (px != null) {
      const qty = tailOpen.contracts || 1;
      const tos = `BUY +${qty} SPX 100 (Weeklys) ${tosD} ${tailOpen.strike} PUT @${Number(px).toFixed(2)} LMT`;
      const sizeNote = qty >= 2 ? ' · 2× (overnight VIX down)' : '';
      await fanoutSubscribers(env, `🛡️ Tail Hedge — 0DTE put · 9:45 ET entry${sizeNote}\n${tos}`);
      try { await logEvent(env, 'info', 'trade', `tail fan-out: ${tos}`, {}); } catch (_) {}
    }
  } catch (e) { console.warn('[tail-fanout]', e.message); }
  return tailOpen;
}

async function freezeTailOpenIfDue(env, etNow, line = null, announce = false) {
  if (TAIL_RETIRED) return null;              // retired 2026-08-03 (freeze)
  const todayISO = isoDateET(etNow);
  // No 9:45 entry on non-trading days — market closed, no live chain, no
  // snapshot. Without this the campaign stays TRIGGERED over the weekend, the
  // status line still says "▶ TRADE", and any /tail-today poll (or a weekend/
  // holiday tick) past 9:48 fired the spurious "no snapshot — enter manually"
  // DM (2026-07-11/12 Sat+Sun). Gate the whole freeze/alert path on a real
  // trading day.
  const dow = etNow.getDay();
  if (dow === 0 || dow === 6 || isHol(etNow)) return null;
  const existing = await env.SIGNAL_KV.get(`tail_open_trade_${todayISO}`);
  // Even if a /tail-today poll already froze the trade, the cron (announce=true)
  // must still get a chance to fan out — so route through announceTailIfDue
  // rather than returning early.
  if (existing) return announceTailIfDue(env, JSON.parse(existing), todayISO, announce);
  const statusLine = line || await getTailHedgeStatusLine(env);
  const h = etNow.getHours(), m = etNow.getMinutes();
  const pastEntry = h > 9 || (h === 9 && m >= 45);
  if (!(statusLine.includes('▶ TRADE') && pastEntry && h < 16)) return null;
  const snapRaw = await env.SIGNAL_KV.get(`tail_put_snap_${todayISO}`);
  const snap = snapRaw ? JSON.parse(snapRaw) : null;
  if (!snap || !Array.isArray(snap.puts) || !snap.puts.length) {
    // TRADE day, past 9:45, but no put snapshot — the live chain was unavailable at
    // entry. Alert ONCE instead of silently skipping the tail (audit P1-f 2026-07-06).
    // Grace until 9:48: a client poll at 9:45:00 can race the cron tick that
    // captures the snapshot (2026-07-09: spurious "enter manually" DM at 9:45,
    // trade then froze normally at 9:46). Retry silently for the first ticks.
    if (h === 9 && m < 48) return null;
    const ak = `tail_nosnap_alert_${todayISO}`;
    if (!(await env.SIGNAL_KV.get(ak))) {
      await env.SIGNAL_KV.put(ak, '1', { expirationTtl: 86400 });
      try {
        const dcRaw = await env.SIGNAL_KV.get('discord_config');
        const dc = dcRaw ? JSON.parse(dcRaw) : null;
        if (dc && dc.channelId) await sendDiscordDM(env, dc.channelId,
          `⚠️ **Tail Hedge** — TRADE signal today but no 9:45 put snapshot (live SPX chain unavailable at entry). No tail frozen — enter the 0DTE Δ-0.10 put manually if still desired.`, dc.proxyUrl);
      } catch (_) {}
    }
    return null;
  }
  const e0 = snap.puts[0].e;
  const candidate = snap.puts.filter(p => p.e === e0)
    .sort((a, b) => Math.abs(a.d + 0.10) - Math.abs(b.d + 0.10))[0] || null;
  if (!candidate) return null;
  const entryMid = (candidate.b != null && candidate.a != null)
    ? parseFloat(((candidate.b + candidate.a) / 2).toFixed(2))
    : (candidate.a ?? candidate.b ?? null);
  // Size: 2 contracts when overnight VIX is DOWN (prior VIX close > today's 9:30
  // open), else 1. Reuses the morning signal's oNight (= vixYClose − vixToday;
  // positive = VIX fell overnight), persisted to KV ~9:30 — before this 9:45 freeze.
  // Backtest: the tail's entire edge concentrates on these days (7 of 8 winners,
  // ~+108%) — user-approved 2026-07-07. Null/NaN/flat oNight → 1 (safe default).
  let contracts = 1;
  try {
    const msdRaw = await env.SIGNAL_KV.get(`morning_signal_data_${todayISO}`);
    const msd = msdRaw ? JSON.parse(msdRaw) : null;
    if (msd && typeof msd.oNight === 'number' && isFinite(msd.oNight) && msd.oNight > 0) contracts = 2;
  } catch (_) { /* default to 1 */ }
  const tailOpen = {
    openDate: todayISO, openTimeET: '09:45', strike: candidate.k, expDate: candidate.e,
    entryBid: candidate.b, entryAsk: candidate.a, entryMid, contracts, status: 'filled',
    label: 'Tail Hedge', currentSpot: snap.spot ?? null,
  };
  await env.SIGNAL_KV.put(`tail_open_trade_${todayISO}`, JSON.stringify(tailOpen), { expirationTtl: 7 * 86400 });

  return announceTailIfDue(env, tailOpen, todayISO, announce);
}

// Refresh intraday mid + live P&L on today's open tail put (mirrors
// refreshBobfLiveQuotes). Single long put: currentPnl = (mid − entryMid)×100×qty.
async function refreshTailLiveQuotes(env, etNow, preChain = null) {
  if (TAIL_RETIRED) return null;              // retired 2026-08-03 (refresh)
  const todayISO = isoDateET(etNow);
  const raw = await env.SIGNAL_KV.get(`tail_open_trade_${todayISO}`);
  if (!raw) return null;
  const trade = JSON.parse(raw);
  if (trade.status === 'closed') return trade;
  try {
    if (!preChain || !preChain.putExpDateMap) return trade;
    const put = pickContractFromChain(preChain.putExpDateMap, trade.expDate, trade.strike);
    if (!put || put.mid == null) return trade;
    const entryCost = trade.entryMid != null ? trade.entryMid
      : (trade.entryBid != null && trade.entryAsk != null) ? (trade.entryBid + trade.entryAsk) / 2
      : (trade.entryAsk ?? trade.entryBid);
    trade.currentSpot  = preChain.spot ? parseFloat(preChain.spot.toFixed(2)) : trade.currentSpot;
    trade.currentMid   = parseFloat(put.mid.toFixed(2));
    trade.currentValue = parseFloat(put.mid.toFixed(2));
    trade.currentPnl   = Math.round((put.mid - entryCost) * 100 * (trade.contracts || 1));
    trade.lastQuoteAt  = new Date().toISOString();
    await env.SIGNAL_KV.put(`tail_open_trade_${todayISO}`, JSON.stringify(trade), { expirationTtl: 7 * 86400 });
  } catch (e) { console.warn('[tail] refresh failed:', e.message); }
  return trade;
}

// Settle Tail Hedge at SPX close. Single long 0DTE SPXW put:
//   intrinsic = max(0, strike − spxClose);  pnl = (intrinsic − entryMid) × 100 × contracts
// (mirrors research_tail_hedge.py; same ×100×contracts + Math.round as the other settles)
// On the first PROFITABLE day, flip tail_trigger_state → RESOLVED so the campaign
// stops signalling TRADE until COR1M crosses below 7.75 again.
async function settleTailEOD(env, etNow, spxClose, dateISO = null) {
  // dateISO: settle a PAST day's trade (orphan sweep / manual ?date=). Without
  // it the function is locked to today — a missed EOD could never be retried
  // (2026-07-09 orphaned this way: spxClose null at 16:17 → skip → stuck 'filled').
  const todayISO = dateISO || isoDateET(etNow);
  const raw = await env.SIGNAL_KV.get(`tail_open_trade_${todayISO}`);
  if (!raw) return { status: 'no-trade' };
  const trade = JSON.parse(raw);
  if (trade.openDate !== todayISO) return { status: 'wrong-date', openDate: trade.openDate };
  if (trade.status === 'closed') return { status: 'already-closed', pnl: trade.pnl };

  let S = spxClose;
  if (S == null) { try { S = await getSpxCloseForDate(todayISO, env); } catch (_) {} }
  if (S == null) return { status: 'no-close' };

  const contracts = trade.contracts || 1;
  const entryCost = trade.entryMid != null ? trade.entryMid
    : (trade.entryBid != null && trade.entryAsk != null) ? (trade.entryBid + trade.entryAsk) / 2
    : (trade.entryAsk ?? trade.entryBid);
  const intrinsic = Math.max(0, trade.strike - S);
  const pnl = Math.round((intrinsic - entryCost) * 100 * contracts);

  // History P/L FIRST, mark 'closed' after (audit P2 2026-07-06 — the status guard
  // above makes re-settle idempotent; the campaign-stop below keys on pnl, not status).
  // Past-day heals (dateISO set) also repair the row's spxClose — that day's main
  // EOD failed (which is why the trade orphaned), so the row lacks it, and
  // backfillMissingPL skips rows without spxClose. NOT on the today-path:
  // spxClose is alwaysOverwrite in upsertHistoryGitHub and handleEOD owns it there.
  const histFields = { tailPL: pnl };
  if (dateISO) histFields.spxClose = parseFloat(Number(S).toFixed(2));
  await upsertHistoryGitHub(env, todayISO, histFields);

  trade.status = 'closed';
  trade.closeDate = todayISO;
  trade.spxClose = parseFloat(S.toFixed(2));
  trade.closeIntrinsic = parseFloat(intrinsic.toFixed(2));
  trade.entryMid = parseFloat(Number(entryCost).toFixed(2));
  trade.pnl = pnl;
  await env.SIGNAL_KV.put(`tail_open_trade_${todayISO}`, JSON.stringify(trade), { expirationTtl: 7 * 86400 });

  const logRaw = await env.SIGNAL_KV.get('tail_closed_log');
  const log = logRaw ? JSON.parse(logRaw) : [];
  log.unshift(trade);
  await env.SIGNAL_KV.put('tail_closed_log', JSON.stringify(log.slice(0, 30)));

  // First profitable day STOPS the campaign (previously only the offline bundle
  // did this via exit_reason='profitable'; now the live worker ends it too).
  if (pnl > 0) {
    try {
      const stRaw = await env.SIGNAL_KV.get('tail_trigger_state');
      const st = stRaw ? JSON.parse(stRaw) : null;
      if (st && st.state === 'TRIGGERED') {
        st.state = 'RESOLVED'; st.resolvedOn = todayISO; st.exitReason = 'profitable'; st.exitPnl = pnl;
        await env.SIGNAL_KV.put('tail_trigger_state', JSON.stringify(st));
        _tailHedgeCache = { value: null, fetchedAt: 0 };  // bust the 5-min status cache
      }
    } catch (_) { /* campaign-stop is best-effort */ }
  }
  return { status: 'settled', pnl, strike: trade.strike, entryMid: entryCost, intrinsic, spxClose: S };
}

// Settle any prior-session trade left 'filled' by a missed EOD (lessons P18 —
// 2026-07-09 stranded BOTH the tail put AND a winning BOBF fly this way).
// Tail uses dated keys (scan last 5 sessions); strad/bobf/gxbf share ONE
// undated key each, so an orphan there gets OVERWRITTEN by today's entry —
// which is why the morning call runs before any entry window. Uses Stooq for
// the past day's close (published by the next session even when Schwab's EOD
// fetch failed at 16:17).
async function sweepOrphanSettles(env, etNow) {
  const todayISO = isoDateET(etNow);
  const results = [];
  // Prefer the close already in the history row (canonical, written by
  // handleEOD or an earlier heal) — external lookups only when it's absent.
  let histSw = null;
  try { histSw = await getHistory(env); } catch (_) {}
  const rowCloseSw = (d) => {
    const r = (histSw || []).find(x => x.date === d);
    return (r && r.spxClose != null) ? parseFloat(r.spxClose) : null;
  };
  const notify = async (label, dISO, pnl) => {
    try {
      const dcRaw = await env.SIGNAL_KV.get('discord_config');
      const dc = dcRaw ? JSON.parse(dcRaw) : null;
      if (dc && dc.channelId) await sendDiscordDM(env, dc.channelId,
        `🧹 **${label}** — settled orphaned ${dISO} trade (missed EOD): P/L $${pnl}.`, dc.proxyUrl);
    } catch (_) {}
  };
  for (let i = 1; i <= 5; i++) {
    const dPast = new Date(etNow); dPast.setDate(dPast.getDate() - i);
    const dISO = isoDateET(dPast);
    try {
      const rawT = await env.SIGNAL_KV.get(`tail_open_trade_${dISO}`);
      if (rawT) {
        const trT = JSON.parse(rawT);
        if (trT.status === 'filled') {
          const resT = await settleTailEOD(env, etNow, rowCloseSw(dISO), dISO);
          results.push({ strat: 'tail', date: dISO, ...resT });
          if (resT.status === 'settled') await notify('Tail Hedge', dISO, resT.pnl);
        }
      }
    } catch (e) { console.warn('[orphan-sweep] tail', dISO, e.message); }
  }
  const undated = [
    ['straddle_open_trade', 'Straddle', settleStraddleEOD],
    ['bobf_open_trade', 'BOBF', settleBobfEOD],
    ['gxbf_open_trade', 'GXBF', settleGxbfEOD],
  ];
  for (const [key, label, fn] of undated) {
    try {
      const raw = await env.SIGNAL_KV.get(key);
      if (!raw) continue;
      const tr = JSON.parse(raw);
      if (tr.status !== 'filled' || !tr.openDate || tr.openDate >= todayISO) continue;
      let S = rowCloseSw(tr.openDate);
      if (S == null) S = await getSpxCloseForDate(tr.openDate, env);
      if (S == null) { results.push({ strat: label.toLowerCase(), date: tr.openDate, status: 'no-close' }); continue; }
      const res = await fn(env, etNow, S, tr.openDate);
      results.push({ strat: label.toLowerCase(), date: tr.openDate, ...res });
      if (res.status === 'settled') await notify(label, tr.openDate, res.pnl);
    } catch (e) { console.warn('[orphan-sweep]', key, e.message); }
  }
  return results;
}

// ════════════════════════════════════════════════════════════════════
// PNBF (paper) — FINAL v3 recipe, locked 2026-07-15.
// 30-wide CALL fly at the gamma magnet, 12:00 ET, only when the M8BF
// service's fly center is exactly 5 pts from the magnet (T1==magnet),
// calendar clear (EOM last-2-days / CPI / FED / Mon-Thu of OPEX week),
// debit ≤ $17. Paper OCO bracket TP +3.0 / SL −5.0, else settle.
// INDEPENDENT strategy: touches no other strategy's state, ever.
// KV: mf_magnet_<date> (10:30 magnet snap) · mf_today_<date> (status for
// the page) · mf_open_trade · mf_closed_log (paper record, KV = truth) ·
// mf_webhook_url (Discord channel feed; DM fallback via discord_config).
// Full spec + backtest: tasks/MAGNET_FLY_RESEARCH.md (private), magnetfly.html.
// ════════════════════════════════════════════════════════════════════
const MF_SIGNALS_CHANNEL = '1048242197029458040';   // same feed the M8BF archive scrapes
const MF_TP = 3.0, MF_SL = 5.0, MF_WIDTH = 30, MF_DEBIT_CAP = 17.0;
const MF_LOTS = 10;   // default position size (Sigma 3 tracking, 2026-07-15)

function mfOpexWeekMidBlock(etNow) {
  // Mon-Thu of the monthly OPEX week (OPEX Friday itself is traded).
  for (const s of opexSch) {
    const o = parseLong(s);
    if (!o || o.getMonth() !== etNow.getMonth() || o.getFullYear() !== etNow.getFullYear()) continue;
    const monday = new Date(o); monday.setDate(o.getDate() - 4);   // OPEX is a Friday
    if (etNow >= new Date(monday.getFullYear(), monday.getMonth(), monday.getDate())
        && isoDateET(etNow) < isoDateET(o)) return true;
  }
  return false;
}

function mfCalendarBlock(etNow) {
  if (isEomN(0, etNow) || isEomN(1, etNow)) return 'EOM (last 2 trading days)';
  if (cpiSch.includes(todayLong(etNow))) return 'CPI day';
  if (fedSch.includes(todayLong(etNow))) return 'FED day';
  if (mfOpexWeekMidBlock(etNow)) return 'mid-OPEX week (Mon-Thu)';
  return null;
}

function mfExactLeg(expDateMap, expISO, strike) {
  const q = pickContractFromChain(expDateMap, expISO, strike);
  return (q && q.strike === strike && q.bid != null && q.ask != null && q.ask > 0) ? q : null;
}

// Fly quote at exact legs K±W: {mid, entryEff (slippage-adjusted), spreads}
function mfFlyQuote(chain, todayISO, K) {
  const lo = mfExactLeg(chain.callExpDateMap, todayISO, K - MF_WIDTH);
  const ce = mfExactLeg(chain.callExpDateMap, todayISO, K);
  const hi = mfExactLeg(chain.callExpDateMap, todayISO, K + MF_WIDTH);
  if (!lo || !ce || !hi) return null;
  const mid = lo.mid - 2 * ce.mid + hi.mid;
  const halfspread = ((lo.ask - lo.bid) + (ce.ask - ce.bid) + (hi.ask - hi.bid)) / 2;
  return { mid, slip: halfspread * 0.25 };
}

// Claim-token send gate — same protocol as the morning signal (~line 6683).
// A naive check-then-act KV gate lets EVERY overlapping cron tick pass before
// the first one writes its marker (KV is eventually consistent and edge-caches
// misses for ~60s), which is exactly how the PNBF 11:30/noon posts went out
// 6×/4× on their first live day (2026-07-17). Write a unique claim BEFORE any
// slow work, wait for propagation, verify we won. Returns true iff this tick
// owns the slot. Caller puts 'sent' (86400s) on success, or deletes the key on
// a posted-nothing error so the next tick retries; a tick that dies mid-send
// leaves a claim that goes stale after 40s and is taken over.
// Claim-token send gate. CRITICAL: CLAIM_STALE_MS must exceed the SLOWEST
// handler this gate wraps, or a still-running claim looks "stale" to the next
// cron tick, which takes over and DOUBLE-SENDS. PNBF's noon/11:30 handlers
// scrape Discord + fetch the full SPX chain = up to ~90s; the old 40s window
// let them post 2–5× (2026-07-20). 180s covers any handler here; a genuinely
// crashed tick unblocks either via the caller's explicit delete (error paths)
// or after this window. Reads are UNCACHED so a 'sent' marker is seen at once.
const CLAIM_STALE_MS = 180_000;
async function claimSendSlot(env, key) {
  try { return await _claimSendSlotInner(env, key); }
  catch (e) { console.warn('[claimSendSlot]', key, e.message); return false; }  // a gate error must never kill the tick (P26)
}
async function _claimSendSlotInner(env, key) {
  const cur = await env.SIGNAL_KV.get(key);   // plain read — cacheTtl<60 THROWS on CF KV (P26)
  if (cur && cur.startsWith('claim:')) {
    const ts = parseInt(cur.split(':')[2] || '0', 10);
    if (!ts || Date.now() - ts <= CLAIM_STALE_MS) return false;   // claim in flight
    // older than any real handler run → the owner crashed; take over
  } else if (cur) {
    // any other non-empty value ('sent', 'done', 'window-passed', 'blackout:…')
    // is a terminal marker written by us or the handler itself — slot is spent
    return false;
  }
  const mine = `claim:${crypto.randomUUID()}:${Date.now()}`;
  await env.SIGNAL_KV.put(key, mine, { expirationTtl: 300 });
  await new Promise(r => setTimeout(r, 1500));            // let racers write too
  return (await env.SIGNAL_KV.get(key)) === mine;   // last-write-wins
}

// PNBF is a Sigma 3 signal now (2026-07-15): route through the SAME
// fan-out as every other Sigma 3 trade — the signals channel + every
// subscriber DM, with the compliance disclaimer. No separate scalp webhook.
async function postMagnetFly(env, message) {
  try { await fanoutSubscribers(env, message); return true; }
  catch (e) { console.warn('[scalp] fanout failed:', e.message); return false; }
}

async function mfSetToday(env, todayISO, obj) {
  await env.SIGNAL_KV.put(`mf_today_${todayISO}`, JSON.stringify({ date: todayISO, ...obj }),
    { expirationTtl: 3 * 86400 });
}

// Shared input read: magnet (10:30 snap), M8BF center ≤cut, fly debit. Pure
// read, no side effects — used by both the pre-alert and the noon check.
async function mfReadInputs(env, token, etNow, preChain, cut) {
  const todayISO = isoDateET(etNow);
  let magnet = null, magnetSrc = '10:30 snap';
  // PNBF magnet MUST be the 0DTE max-positive-gamma strike — the basis the
  // recipe was backtested on (2026-07-20 fix). The all-expiry magnet drifts to
  // far monthly-OPEX call walls (7600 vs the true 7500 on 2026-07-20) and cost a
  // real aligned trade that day. Prefer magnet0dte from the snapshot; fall back
  // to the 0DTE live snapshot; legacy snapshots (magnet0dte absent) use .magnet.
  const snapRaw = await env.SIGNAL_KV.get(`mf_magnet_${todayISO}`);
  if (snapRaw) { const s = JSON.parse(snapRaw); magnet = s.magnet0dte ?? s.magnet;
    magnetSrc = s.magnet0dte != null ? '10:30 snap (0DTE)' : '10:30 snap (legacy all-exp)'; }
  if (magnet == null) {
    const curRaw = await env.SIGNAL_KV.get('gex_current_0dte');
    const cur = curRaw ? JSON.parse(curRaw) : null;
    if (cur?.maxPosStrike != null) { magnet = cur.maxPosStrike; magnetSrc = 'live fallback (0DTE)'; }
  }
  if (magnet == null) return { error: 'no magnet available' };
  // M8BF center from signals_today (maintained by pollDiscordSignals every
  // tick) — the 10-page Discord re-scrape made this the slowest handler in
  // the worker and got noon ticks killed mid-fanout (P27 dupes). Scrape only
  // as a fallback when the poller has nothing for today.
  let center = null, sigTime = null;
  try {
    const stRaw = await env.SIGNAL_KV.get('signals_today');
    const st = stRaw ? JSON.parse(stRaw) : null;
    if (st && st.date === todayISO && Array.isArray(st.signals)) {
      for (const sg of st.signals) {
        if (sg.time && sg.time <= cut && Number.isFinite(sg.center)) { center = sg.center; sigTime = sg.time; }
      }
    }
  } catch (_) {}
  if (center == null && env.DISCORD_USER_TOKEN) {
    const rows = await scrapeRawRowsForDate(env.DISCORD_USER_TOKEN, MF_SIGNALS_CHANNEL, todayISO);
    for (const line of rows || []) {
      const c = line.split(',');
      if (c.length < 25 || !c[2] || c[2] > cut) continue;
      const bc = parseFloat(c[24]);
      if (Number.isFinite(bc)) { center = bc; sigTime = c[2]; }
    }
  }
  if (center == null) return { magnet, magnetSrc, center: null };
  const chain = preChain || await fetchMasterSpxChain(token, env);
  const q = mfFlyQuote(chain, todayISO, magnet);
  const entry = q ? Math.round((q.mid + q.slip) * 100) / 100 : null;
  return { magnet, magnetSrc, center, sigTime, entry };
}

// Morning status (~9:40 ET) — "possible today or not", from the calendar
// alone (magnet + M8BF center aren't known yet). Posts to Sigma channel + DMs.
async function handleMagnetFlyMorning(env, etNow) {
  const todayISO = isoDateET(etNow);
  const block = mfCalendarBlock(etNow);
  // NO standalone 9:40 post anymore (owner 2026-07-15) — the PNBF row is now in
  // the morning Plan card. We still write mf_today KV so the card + page + the
  // 11:30/12:00 alerts read this state.
  if (block) {
    await mfSetToday(env, todayISO, { status: 'NO', pre: true,
      headline: `No PNBF today (${block})`, detail: 'calendar filter, known in advance' });
    return { morning: 'NO', reason: block };
  }
  await mfSetToday(env, todayISO, { status: 'POSSIBLE', pre: true,
    headline: 'PNBF possible today', detail: 'calendar clear · 11:30 heads-up, then 12:00 final' });
  return { morning: 'POSSIBLE' };
}

// 11:30 ET heads-up — a LEAN advisory, not the trade. Idempotent via
// mf_prealert_<date>. Honest confidence bands (measured on 182 days: an
// early call matches the noon verdict ~88% of the time):
//   calendar block → certain NO
//   aligned + debit ≤ $15.5   → LIKELY GO
//   aligned + debit $15.5–17  → LIKELY GO (borderline on price)
//   aligned + debit > $17     → leaning NO (aligned but pricey)
//   not aligned, |c−mag| ≤ 15 → ON THE FENCE (center could snap onto magnet)
//   not aligned, |c−mag| > 15 → likely NO trade
async function handleMagnetFlyPreAlert(env, token, etNow, preChain) {
  const todayISO = isoDateET(etNow);
  const block = mfCalendarBlock(etNow);
  if (block) {
    // NO post on calendar-blocked days (owner 2026-07-15: "I don't need a
    // heads-up on days we don't trade, like mid-OPEX week"). These skips are
    // known days in advance and the morning Plan card row already reads NO.
    // KV write stays so the card + magnetfly.html show the state.
    await mfSetToday(env, todayISO, { status: 'NO', pre: true,
      headline: `No PNBF today (${block})`, detail: 'calendar filter, known in advance' });
    return { pre: 'NO', reason: block };
  }
  const inp = await mfReadInputs(env, token, etNow, preChain, '11:30');
  if (inp.error) return { error: inp.error };
  const preKey = `mf_prealert_${todayISO}`;
  const markPre = () => env.SIGNAL_KV.put(preKey, 'sent', { expirationTtl: 86400 });
  const { magnet, center, entry } = inp;
  if (center == null) {
    await markPre();                                    // decision made: no heads-up today
    await mfSetToday(env, todayISO, { status: 'POSSIBLE', pre: true,
      headline: 'Heads-up — no M8BF signal yet at 11:30', detail: 'watching for the noon check' });
    return { pre: 'PEND' };
  }
  const dist = Math.abs(center - magnet);
  let lean, headline, emoji;
  if (dist === 5 && entry != null && entry <= 15.5) {
    lean = 'LIKELY GO'; emoji = '🟢';
    headline = `Likely GO at noon — aligned, 30w ~$${entry.toFixed(2)}`;
  } else if (dist === 5 && entry != null && entry <= 17) {
    lean = 'LIKELY GO (borderline price)'; emoji = '🟡';
    headline = `Leaning GO — aligned but 30w ~$${entry.toFixed(2)}, near the $17 cap`;
  } else if (dist === 5) {
    lean = 'LEANING NO (too pricey)'; emoji = '🟠';
    headline = `Aligned but 30w ~$${entry != null ? entry.toFixed(2) : '?'} > $17 — leaning NO, watch for it to cheapen`;
  } else if (dist <= 15) {
    lean = 'ON THE FENCE'; emoji = '🟡';
    headline = `On the fence — M8BF center ${center} is ${dist} off magnet ${magnet}; could snap on by noon`;
  } else {
    lean = 'LIKELY NO'; emoji = '⚪';
    headline = `Likely no trade — center ${center} is ${dist} pts off magnet ${magnet}`;
  }
  await markPre();                                    // marker BEFORE the post (P27)
  await mfSetToday(env, todayISO, {
    status: (lean.startsWith('LIKELY GO')) ? 'POSSIBLE' : 'PRE-NO', pre: true, headline,
    detail: `11:30 heads-up · noon check is final · ~88% of early calls hold`,
    kpis: [['magnet', magnet], ['M8BF center', center], ['distance', dist + ' pts'],
           ['30w debit', entry != null ? '$' + entry.toFixed(2) : 'n/a']] });
  await postMagnetFly(env, `🧲 **PNBF** ${todayISO} — **${emoji} 11:30 heads-up: ${lean}**\n` +
    `${headline}\n_magnet ${magnet} · center ${center} · noon check is final (~88% of early calls hold)_`);
  return { pre: lean, dist, entry };
}

// Noon check — DEDUP CONTRACT (P27, matches GXBF: the only PNBF sender that
// never duped writes its terminal marker BEFORE posting): decision → write
// mf_done_<date>='sent' → THEN mfSetToday/post. If the invocation dies
// mid-fanout the marker already exists, so a takeover tick can never re-post.
// Worst case a message is lost once — never duplicated. Caller releases the
// claim only on pre-decision errors (which post nothing).
async function handleMagnetFlyNoon(env, token, etNow, preChain) {
  const todayISO = isoDateET(etNow);
  const doneKey = `mf_done_${todayISO}`;
  const mark = () => env.SIGNAL_KV.put(doneKey, 'sent', { expirationTtl: 86400 });
  const block = mfCalendarBlock(etNow);
  if (block) {
    await mark();
    await mfSetToday(env, todayISO, { status: 'NO', headline: `No PNBF — ${block}`,
      detail: 'calendar filter (recipe rule 2)' });
    return { skipped: block };
  }
  const inp = await mfReadInputs(env, token, etNow, preChain, '12:00');
  if (inp.error) return { error: inp.error };
  const { magnet, magnetSrc, center, sigTime, entry } = inp;
  if (center == null) return { error: 'no M8BF signal rows yet' };

  const dist = Math.abs(center - magnet);
  if (dist !== 5) {
    await mark();                                        // marker BEFORE post
    await mfSetToday(env, todayISO, { status: 'NO',
      headline: `No PNBF — T1 ≠ magnet (center ${center} vs magnet ${magnet}, dist ${dist})`,
      detail: `magnet ${magnetSrc} · M8BF signal @${sigTime}`,
      kpis: [['magnet', magnet], ['M8BF center', center], ['distance', dist + ' pts']] });
    await postMagnetFly(env, `🧲 **PNBF** ${todayISO} — **NO TRADE** · T1≠magnet (center ${center}, magnet ${magnet}, dist ${dist})`);
    return { skipped: 'no alignment', center, magnet };
  }

  if (entry == null) return { error: `fly legs missing at K=${magnet}` };
  if (entry > MF_DEBIT_CAP) {
    await mark();                                        // marker BEFORE post
    await mfSetToday(env, todayISO, { status: 'NO',
      headline: `No PNBF — 30w costs $${entry.toFixed(2)} > $${MF_DEBIT_CAP} cap`,
      detail: `aligned (center ${center} == magnet±5) but too expensive`,
      kpis: [['magnet', magnet], ['M8BF center', center], ['fly debit', '$' + entry.toFixed(2)], ['cap', '$17.00']] });
    await postMagnetFly(env, `🧲 **PNBF** ${todayISO} — **NO TRADE** · aligned but 30w costs $${entry.toFixed(2)} > $17 cap`);
    return { skipped: 'debit cap', entry };
  }

  const trade = {
    openDate: todayISO, magnet, center, entry,
    tp: Math.round((entry + MF_TP) * 100) / 100, sl: Math.round((entry - MF_SL) * 100) / 100,
    status: 'open', openedAt: `${etNow.getHours()}:${String(etNow.getMinutes()).padStart(2, '0')}`,
    paper: true,
  };
  await env.SIGNAL_KV.put('mf_open_trade', JSON.stringify(trade));
  await mark();                                          // marker BEFORE the GO post
  await mfSetToday(env, todayISO, { status: 'GO',
    headline: `GO — 30w fly at ${magnet}, debit $${entry.toFixed(2)}`,
    detail: `TP $${trade.tp.toFixed(2)} (+$300/lot) · SL $${trade.sl.toFixed(2)} (−$500/lot) · M8BF @${sigTime}`,
    kpis: [['magnet', magnet], ['M8BF center', center], ['debit', '$' + entry.toFixed(2)],
           ['TP / SL', `+3.0 / −5.0`]] });
  // GO = EXACT M8BF-message shape (owner 2026-07-15, screenshot): bold name,
  // plain order line, "BUTTERFLY" uppercase, CALL strikes LOW→HIGH, SPX 100
  // (Weeklys), @debit LMT. 0DTE → expiration is today. Plain text. TP/SL below.
  const _MON = ['JAN','FEB','MAR','APR','MAY','JUN','JUL','AUG','SEP','OCT','NOV','DEC'];
  const tosD = `${+todayISO.slice(8,10)} ${_MON[+todayISO.slice(5,7)-1]} ${todayISO.slice(2,4)}`;
  const tos = `BUY +${MF_LOTS} BUTTERFLY SPX 100 (Weeklys) ${tosD} ${magnet - MF_WIDTH}/${magnet}/${magnet + MF_WIDTH} CALL @${entry.toFixed(2)} LMT`;
  await postMagnetFly(env,
    `**PNBF**\n${tos}\nTP ${trade.tp.toFixed(2)} · SL ${trade.sl.toFixed(2)}`);
  return { opened: true, trade };
}

// Paper bracket watcher — every market tick after noon. Conservative like
// the backtest: exit checks use slippage-adjusted mid, SL checked first.
async function refreshMagnetFlyLiveQuotes(env, token, etNow, preChain) {
  const raw = await env.SIGNAL_KV.get('mf_open_trade');
  if (!raw) return;
  const tr = JSON.parse(raw);
  const todayISO = isoDateET(etNow);
  if (tr.status !== 'open' || tr.openDate !== todayISO) return;
  const chain = preChain || await fetchMasterSpxChain(token, env);
  const q = mfFlyQuote(chain, todayISO, tr.magnet);
  if (!q) return;
  const m = q.mid - q.slip;
  tr.lastMid = Math.round(q.mid * 100) / 100;
  tr.lastQuoteAt = Date.now();
  let exit = null, pnl = 0;
  if (m <= tr.entry - MF_SL) { exit = 'SL'; pnl = -MF_SL * 100; }
  else if (m >= tr.entry + MF_TP) { exit = 'TP'; pnl = MF_TP * 100; }
  if (exit) {
    tr.status = 'closed'; tr.exit = exit; tr.pnl = Math.round(pnl);
    tr.exitTime = `${etNow.getHours()}:${String(etNow.getMinutes()).padStart(2, '0')}`;
    await mfAppendClosed(env, tr);
    const dollars = tr.pnl * MF_LOTS;
    await postMagnetFly(env, `🧲 **PNBF** ${todayISO} — **${exit === 'TP' ? '✅ TP hit' : '🛑 stopped'}** ${dollars >= 0 ? '+' : '−'}$${Math.abs(dollars).toLocaleString()} (${MF_LOTS} lots) @ ${tr.exitTime} ET (fly ${tr.lastMid.toFixed(2)})`);
  }
  await env.SIGNAL_KV.put('mf_open_trade', JSON.stringify(tr));
}

// EOD settle fallback (never fired in the 87-trade backtest).
async function settleMagnetFlyEod(env, etNow, preChain) {
  const raw = await env.SIGNAL_KV.get('mf_open_trade');
  if (!raw) return;
  const tr = JSON.parse(raw);
  if (tr.status !== 'open' || tr.openDate !== isoDateET(etNow)) return;
  const spot = preChain?.spot;
  if (!spot) return;
  const intr = Math.max(0, MF_WIDTH - Math.abs(spot - tr.magnet));
  tr.status = 'closed'; tr.exit = 'SETTLE';
  tr.pnl = Math.round((intr - tr.entry) * 100);
  tr.exitTime = '16:15';
  await mfAppendClosed(env, tr);
  await env.SIGNAL_KV.put('mf_open_trade', JSON.stringify(tr));
  const dollars = tr.pnl * MF_LOTS;
  await postMagnetFly(env, `🧲 **PNBF** ${tr.openDate} — settled ${dollars >= 0 ? '+' : '−'}$${Math.abs(dollars).toLocaleString()} (${MF_LOTS} lots, rare: bracket never filled)`);
}

async function mfAppendClosed(env, tr) {
  const logRaw = await env.SIGNAL_KV.get('mf_closed_log');
  const log = logRaw ? JSON.parse(logRaw) : [];
  if (!log.some(x => x.date === tr.openDate)) {
    log.push({ date: tr.openDate, day: ['Sun','Mon','Tue','Wed','Thu','Fri','Sat'][new Date(tr.openDate + 'T12:00:00').getUTCDay()],
      magnet: tr.magnet, center: tr.center, debit: Math.round(tr.entry * 100),
      exit: tr.exit, exitTime: tr.exitTime, pnl: tr.pnl });
    await env.SIGNAL_KV.put('mf_closed_log', JSON.stringify(log));
  }
  // Sigma 3 history: write scalpPL (10-lot dollars) for the day — same safe
  // row-level path the other strategies use (KV-first → GitHub mirror).
  try { await upsertHistoryGitHub(env, tr.openDate, { scalpPL: tr.pnl * MF_LOTS }); }
  catch (e) { console.warn('[scalp] history write failed:', e.message); }
}

// Wrapper (2026-07-26): records this tick's Schwab call count on EVERY exit
// path (the inner function has ~8 returns) without touching any of them.
async function handleScheduled(env) {
  _schwabCalls = 0; _schwab429 = 0;
  const _t0 = toET();
  try {
    return await handleScheduledInner(env);
  } finally {
    await recordSchwabUsage(env, _t0);
  }
}

async function handleScheduledInner(env) {
  const etNow = toET();
  const dow = etNow.getDay();

  // Not a weekday → skip
  if (dow === 0 || dow === 6) return { status: 'skipped', reason: 'weekend' };

  // Not a trading day (holiday) → skip
  if (!isTrade(etNow)) return { status: 'skipped', reason: 'holiday' };

  const etHour = etNow.getHours();
  const etMin = etNow.getMinutes();
  const todayISO = `${etNow.getFullYear()}-${String(etNow.getMonth()+1).padStart(2,'0')}-${String(etNow.getDate()).padStart(2,'0')}`;

  // EOD self-ping: fire on ANY cron tick after 16:16 ET when eod_done_<date> is
  // unset. Previously only 16:16–16:25 ET triggered EOD — if Cloudflare dropped
  // every tick in that 9-min window (as happened 2026-04-17, 4+ hours of cron
  // silence), EOD stayed missing until something external hit /gex. With this
  // widening, the dedicated 17:17 ET cron (and any `*/2` afternoon tick that
  // makes it through) rescues the EOD write without needing a browser hit.
  const afterEOD = (etHour === 16 && etMin >= 16) || etHour >= 17;
  const eodKey = `eod_done_${todayISO}`;
  // P37 (2026-08-05): the old `!eodAlreadyDone` check-then-act let the 16:16
  // minute tick and the 16:17 dedicated cron BOTH run handleEOD — 'done' is
  // only written at the END of a full run, so the second tick raced past the
  // check. The duplicate's history mirror then pushed a pre-settle snapshot
  // LAST and erased stradPL/bobfPL from GitHub (Aug 4 triple-settle day).
  // Claim-gate the whole EOD run instead (same P22 pattern as sends).
  const eodAlreadyDone = afterEOD ? await env.SIGNAL_KV.get(eodKey) : null;
  const isEOD = afterEOD && (!eodAlreadyDone || eodAlreadyDone.startsWith('claim:'));

  const isMarket  = (etHour > 9 || (etHour === 9 && etMin >= 30)) && etHour < 16;

  // Always poll Discord during market hours
  let discordResult = {};
  if (isMarket && env.DISCORD_USER_TOKEN) {
    discordResult = await pollDiscordSignals(env);
  }

  // ── 17:05–17:25 ET: M8BF GEX-gate verdict for the NEXT session (owner DM).
  // Rule (audited 2026-07-23): today's 10:30 0DTE total GEX ranked against the
  // trailing 120 series entries strictly before today; bottom 20% → SKIP.
  // Owner-only DM (decision support, not a subscriber signal). P27: terminal
  // marker written BEFORE the post; claim gate around the whole handler.
  if (etHour === 17 && etMin >= 5 && etMin < 25 && etNow.getDay() >= 1 && etNow.getDay() <= 5) {
    const gateKey = `gexgate_dm_${todayISO}`;
    const doneG = await env.SIGNAL_KV.get(gateKey);
    if (!doneG || doneG.startsWith('claim:')) {
      if (await claimSendSlot(env, gateKey)) {
        try {
          const v = await computeGexGateVerdict(env);
          await env.SIGNAL_KV.put(gateKey, 'sent', { expirationTtl: 86400 });  // marker BEFORE post (P27)
          if (!v || !v.verdict) {
            // Fail LOUD: a gate that silently can't compute looks identical to GO.
            const dcRaw0 = await env.SIGNAL_KV.get('discord_config');
            const dc0 = dcRaw0 ? JSON.parse(dcRaw0) : null;
            if (dc0?.channelId) {
              await sendDiscordDM(env, dc0.channelId,
                `⚠️ **M8BF Anchor Filter — no verdict for next session** (${v?.reason || 'series unavailable'}). ` +
                `10:30 GEX capture may have failed — check /gexgate-status. Card will treat the day as GO.`,
                dc0.proxyUrl);
            }
          } else if (v && v.verdict) {
            const dcRaw = await env.SIGNAL_KV.get('discord_config');
            const dc = dcRaw ? JSON.parse(dcRaw) : null;
            if (dc?.channelId) {
              const bn = (x) => `${(x / 1e9).toFixed(2)}B`;
              const msg = v.verdict === 'SKIP'
                ? `🚪 **M8BF Anchor Filter — next session: SKIP** 🚫\nToday's 10:30 anchor reads p${Math.round(v.rank * 100)} of trailing ${v.n} (${bn(v.gex)}) — below the p17 line = unanchored, wild-day risk.`
                : `🚪 **M8BF Anchor Filter — next session: GO** ✅\nToday's 10:30 anchor reads p${Math.round(v.rank * 100)} of trailing ${v.n} (${bn(v.gex)}) — above the p17 line.`;
              await sendDiscordDM(env, dc.channelId, msg, dc.proxyUrl);
            }
          }
        } catch (e) { console.warn('[gexgate-dm]', e.message); }
      }
    }
  }

  // EOD cron: capture vixClose + spxClose + m8bfPL + backfill any missing m8bfWR
  // P37: claim before running — exactly ONE tick may execute the EOD sequence.
  if (isEOD && await claimSendSlot(env, eodKey)) {
    const eodResult = await handleEOD(env, etNow);
    // Only mark eod_done after real write. If Schwab token expired AND Stooq
    // failed, fields object is empty — DELETE the claim so the next cron tick
    // (or /gex hit) retries instead of silently giving up.
    if (eodResult.wroteFields) {
      await env.SIGNAL_KV.put(eodKey, 'done', { expirationTtl: 86400 });
    } else {
      await env.SIGNAL_KV.delete(eodKey);
    }
    // Orphan sweep (lessons P18): settle prior-session trades stranded by a
    // missed EOD — ALL strategies, not just tail. Runs BEFORE the backfills so
    // a healed row's spxClose lets backfillMissingPL repair m8bfPL this same
    // tick. Cheap when clean (8 KV reads), no subrequest-budget risk (P17).
    try { await sweepOrphanSettles(env, etNow); }
    catch (e) { console.warn('[orphan-sweep-eod]', e.message); }
    let backfillWR = {}, backfillPL = {};
    try { backfillWR = await backfillMissingWR(env); } catch(e) { backfillWR = { error: e.message }; }
    try { backfillPL = await backfillMissingPL(env); } catch(e) { backfillPL = { error: e.message }; }
    // NOTE: appendScrapedSignals (raw signal CSV) MOVED to the 16:25 aux tick
    // below — bundled here it starved on the subrequest budget after handleEOD +
    // backfillWR/PL and silently stopped archiving on 2026-06-04 (lesson P17).
    // Heavy GitHub-write jobs MOVED to their own 16:25 tick (2026-06-12,
    // lessons P17): bundled here they pushed the EOD invocation past the
    // subrequest budget — settle succeeded, every later persist silently
    // died (06-11: fly marks/surface/decomp/cyclicality all missing while
    // captures sat healthy in KV). See eodAuxJobs().
    return { ...eodResult, discord: discordResult, backfill_wr: backfillWR, backfill_pl: backfillPL };
  }

  // ── 16:25–16:40 ET aux tick (2026-06-12, lessons P17): the GitHub-heavy
  // research jobs get their own invocation so the settle's subrequest
  // budget can't starve them. Once per day; evening 18:00 retries remain.
  if (etHour === 16 && etMin >= 25 && etMin < 40) {
    const auxKey = `eod_aux_${todayISO}`;
    if (!(await env.SIGNAL_KV.get(auxKey))) {
      await env.SIGNAL_KV.put(auxKey, 'running', { expirationTtl: 86400 });
      let ok = true;
      try { await persistResearchArtifacts(env, etNow); } catch (e) { ok = false; console.warn('[research-persist]', e.message); }
      try { await computeVixDecompDaily(env, etNow); } catch (e) { ok = false; console.warn('[vix-decomp]', e.message); }
      try { await appendCyclicalityDays(env); } catch (e) { ok = false; console.warn('[cyclelab]', e.message); }
      try { await appendCyclicalityDays(env, { symbol: '%24NDX', file: 'cyclicality_ndx.json' }); } catch (e) { console.warn('[cyclelab-ndx]', e.message); }
      // Raw Discord signal archive → scraped_signals.csv (moved here from EOD,
      // lesson P17). Durable status in KV so a failure can't go unnoticed again.
      try {
        const sa = await appendScrapedSignals(env, etNow);
        await env.SIGNAL_KV.put('scrape_append_last', JSON.stringify({ ...sa, ts: Date.now() }));
      } catch (e) {
        ok = false;
        await env.SIGNAL_KV.put('scrape_append_last', JSON.stringify({ error: e.message, ts: Date.now() }));
        console.warn('[scrape-append]', e.message);
      }
      if (etNow.getDay() === 5) {
        try { await cotWeeklyRefresh(env); } catch (e) { console.warn('[cot]', e.message); }
      }
      if (ok) await env.SIGNAL_KV.put(auxKey, 'done', { expirationTtl: 86400 });
      else await env.SIGNAL_KV.delete(auxKey);   // retry next tick within the window
      return { eod_aux: ok ? 'done' : 'partial-retry' };
    }
  }


  // ── Master SPX chain fetch — ONE call per market tick, shared across all
  //    chain-consuming handlers (GEX, diagonal, straddle, BOBF). Cuts Schwab
  //    API usage from ~7 chain calls per tick to 2 (one CALL + one PUT).
  let masterChain = null;
  let schwabToken = null;
  if (isMarket) {
    // Try Schwab token first — but a failure here MUST NOT block the master
    // chain fetch, which has a Tasty fallback. Schwab-token-dependent ops
    // (order placement, GEX update) still gate on `schwabToken` below.
    try { schwabToken = await getAccessToken(env); }
    catch (e) { console.warn('[chain] Schwab token unavailable, chain will use Tasty:', e.message); }
    try {
      masterChain = await fetchMasterSpxChain(schwabToken, env);
    } catch (e) {
      console.warn('[chain] master chain fetch failed (both Schwab and Tasty):', e.message);
      // Handlers will fall through to their own targeted fetches.
    }
  }

  // ── EARNINGS PLAY scanner jobs (window-gated inside; cheap no-ops otherwise) ──
  {
    let earnToken = schwabToken;
    const earnWindow = (etHour === 9) || (etHour >= 10 && etHour <= 15) ||
                       (etHour === 16 && etMin >= 41) || (etHour === 17 && etMin <= 15);
    if (earnWindow) {
      if (!earnToken) { try { earnToken = await getAccessToken(env); } catch (_) {} }
      if (earnToken) {
        try { await earnMorningJob(env, etNow, earnToken); } catch (e) { console.warn('[earn-morning]', e.message); }
        try { await earnResolveJob(env, etNow, earnToken); } catch (e) { console.warn('[earn-resolve]', e.message); }
        // (gamma-regime + book-flip blocks moved OUT of the earnToken guard
        //  2026-07-31 audit fix — they are Discord/KV-only, see below.)

        try { await earnAfterJob(env, etNow, earnToken); } catch (e) { console.warn('[earn-after]', e.message); }
        try { await earnExitJob(env, etNow, earnToken); } catch (e) { console.warn('[earn-exit]', e.message); }
        try { await earnRescoreJob(env, etNow, earnToken); } catch (e) { console.warn('[earn-rescore]', e.message); }
        try { await earnFinalJob(env, etNow, earnToken); } catch (e) { console.warn('[earn-final]', e.message); }
        try { await earnNightlyCollect(env, etNow, earnToken); } catch (e) { console.warn('[earn-collect]', e.message); }
      }
    }
  }

  // ── Morning book posts (Discord/KV-only — AUDIT FIX 2026-07-31) ──
  // Moved OUT of the earnings `if (earnToken)` guard: the 9:35 gamma-regime
  // post, the g935/g1000 snaps and the 10:00 book-flip note need NO Schwab
  // token (they read gex_current_0dte KV and post to Discord), so a Schwab
  // OAuth failure must not delay or kill them. Windows are self-gated inside.
  if (isMarket) {
  // 9:35 gamma-regime post to the Sigma 3 CHANNEL (owner 2026-07-31).
  // Lives HERE because this section provably runs on every market tick —
  // the first placement (inside the once-at-9:31 morning function) never
  // executed at 9:35 and 2026-07-31's post was missed. Window runs to
  // 10:30 so a late tick still posts; the message carries the actual
  // read time. Claim-gated (P22), freshness checked before claiming.
  try {
    const _gh = etNow.getHours(), _gm = etNow.getMinutes();
    if ((_gh === 9 && _gm >= 35) || (_gh === 10 && _gm <= 30)) {
      const grRaw = await env.SIGNAL_KV.get('gex_current_0dte');
      const gr = grRaw ? JSON.parse(grRaw) : null;
      const grFresh = gr && gr.timestamp && (Date.now() / 1000 - gr.timestamp) < 600 && typeof gr.totalGex === 'number';
      const grKey = `gamma_regime_${isoDateET(etNow)}`;
      // Persist the day's canonical 9:35 reading for the 10:00 flip
      // check (gex_current_0dte is overwritten every minute).
      if (grFresh && !(await env.SIGNAL_KV.get(`g935_snap_${isoDateET(etNow)}`))) {
        await env.SIGNAL_KV.put(`g935_snap_${isoDateET(etNow)}`,
          JSON.stringify({ g: gr.totalGex, at: `${_gh}:${String(_gm).padStart(2, '0')}` }), { expirationTtl: 86400 });
      }
      if (grFresh && await claimSendSlot(env, grKey)) {
        const bn = (gr.totalGex / 1e9).toFixed(1);
        const tLbl = `${_gh}:${String(_gm).padStart(2, '0')}`;
        let msg = gr.totalGex > 0
          ? `🟢 **Gamma regime: POSITIVE** — 0DTE book +${bn}B at ${tLbl}. Dealer hedging dampens moves — pin-friendly tape.`
          : `🔴 **Gamma regime: NEGATIVE** — 0DTE book ${bn}B at ${tLbl}. Dealer hedging amplifies moves — trend/whipsaw risk day.`;
        // Fed rider (owner 2026-07-31): the decision, not the odds, is the risk.
        try {
          if (fedSch.includes(todayLong(etNow))) {
            msg += gr.totalGex > 0
              ? ` FOMC day — the 2:00 PM decision can break any morning regime.`
              : ` FOMC day — a negative book has never produced a good pin day on a Fed decision.`;
          }
        } catch (_) {}
        // Σ3 SIGNALS CHANNEL (owner: same room as the trade signals,
        // channel post only — no subscriber DMs). Disclaimer appended
        // like every other public post.
        let grOk = false;
        try { const grRes = await postSignalsChannel(env, msg + FANOUT_DISCLAIMER); grOk = !!(grRes && grRes.ok); } catch (e) { console.warn('[gamma-regime] channel post:', e.message); }
        // only terminal-mark on CONFIRMED delivery — a failed send leaves
        // the claim to expire (300s) so the next tick retries.
        if (grOk) await env.SIGNAL_KV.put(grKey, 'sent', { expirationTtl: 86400 });
        else console.warn('[gamma-regime] channel send not ok — will retry');
      }
    }
  } catch (e) { console.warn('[gamma-regime]', e.message); }
  // 10:00 BOOK-FLIP note (owner 2026-07-31 'do 1'): when the book's sign
  // at ~10:00 differs from the 9:35 snap, post ONE line to the signals
  // channel — flip days historically ran ~¼-strength at coin-flip WR for
  // the whole board (portfolio +$349-394/d vs +$1,318-1,753 stable, n=48).
  // Posts ONLY on a flip; stable days stay silent. Claim-gated, delivery-
  // confirmed, window 10:00–10:25.
  try {
    const _fh = etNow.getHours(), _fm = etNow.getMinutes();
    if (_fh === 10 && _fm >= 0 && _fm <= 25) {
      const snapRaw = await env.SIGNAL_KV.get(`g935_snap_${isoDateET(etNow)}`);
      const snap = snapRaw ? JSON.parse(snapRaw) : null;
      // Persist the day's canonical 10:00 ENDPOINT: first fresh read in the
      // window (normally 10:00–10:01; late-labeled on degraded mornings so
      // the endpoint always exists — audit fix 2026-07-31, was ≤ :10 only).
      let e1000 = null;
      try { const eRaw = await env.SIGNAL_KV.get(`g1000_snap_${isoDateET(etNow)}`); e1000 = eRaw ? JSON.parse(eRaw) : null; } catch (_) {}
      if (!e1000) {
        const curRaw = await env.SIGNAL_KV.get('gex_current_0dte');
        const cur = curRaw ? JSON.parse(curRaw) : null;
        const curFresh = cur && cur.timestamp && (Date.now() / 1000 - cur.timestamp) < 600 && typeof cur.totalGex === 'number';
        if (curFresh) {
          e1000 = { g: cur.totalGex, at: `${_fh}:${String(_fm).padStart(2, '0')}` };
          await env.SIGNAL_KV.put(`g1000_snap_${isoDateET(etNow)}`, JSON.stringify(e1000), { expirationTtl: 86400 });
        }
      }
      // AUDIT FIX 2026-07-31: the flip note compares ENDPOINT vs ENDPOINT
      // (9:35 snap sign vs the persisted 10:00 snap sign) — never the live
      // tick. The research says only endpoint flips matter; a mid-window
      // wiggle at 10:17 must NOT fire the note.
      if (snap && typeof snap.g === 'number' && e1000 && typeof e1000.g === 'number' && (snap.g > 0) !== (e1000.g > 0)) {
        const fKey = `book_flip_${isoDateET(etNow)}`;
        if (await claimSendSlot(env, fKey)) {
          const b0 = (snap.g / 1e9).toFixed(1), b1 = (e1000.g / 1e9).toFixed(1);
          const dir = e1000.g > 0 ? `NEGATIVE (${b0}B) → POSITIVE (+${b1}B)` : `POSITIVE (+${b0}B) → NEGATIVE (${b1}B)`;
          const fMsg = `⚠️ **Book flipped** — ${dir} since ${snap.at || '9:35'}. Unstable-gamma days have historically run ~¼-strength at coin-flip odds across the whole board. Size easy.`;
          let fOk = false;
          try { const fr = await postSignalsChannel(env, fMsg + FANOUT_DISCLAIMER); fOk = !!(fr && fr.ok); } catch (e2) { console.warn('[book-flip] post:', e2.message); }
          if (fOk) await env.SIGNAL_KV.put(fKey, 'sent', { expirationTtl: 86400 });
        }
      }
    }
  } catch (e) { console.warn('[book-flip]', e.message); }
  }

  // ── FINAL-card delivery alarm (owner 2026-08-03: "make sure I get the
  // signal no matter the result") ── 15:59–16:08: if the FINAL's marker is
  // not 'sent', DM the owner the plain-text board directly so the day's call
  // ALWAYS arrives, and flag the delivery failure.
  if ((etHour === 15 && etMin >= 59) || (etHour === 16 && etMin <= 8)) {
    try {
      const fKeyA = `earn_done_${todayISO}_final`;
      const fVal = await env.SIGNAL_KV.get(fKeyA);
      const alKey = `earn_final_alarm_${todayISO}`;
      if (fVal !== 'sent' && etNow.getDay() >= 1 && etNow.getDay() <= 5 && !isHol(etNow) && !(await env.SIGNAL_KV.get(alKey))) {
        if (await claimSendSlot(env, alKey)) {
          const bRaw = await env.SIGNAL_KV.get(`earn_board_${todayISO}`);
          let bodyA = '⚠️ **Earnings FINAL failed to deliver through normal sinks.**';
          if (bRaw) {
            try { const bA = JSON.parse(bRaw); bA.final = true; bodyA += '\n' + earnBoardMsg(bA, null, 'final').slice(0, 1700); } catch (_) {}
          } else bodyA += ' No board stored either — the 15:30 job never ran.';
          const dcRaw = await env.SIGNAL_KV.get('discord_config');
          const dc = dcRaw ? JSON.parse(dcRaw) : null;
          if (dc && dc.channelId) {
            const rA = await sendDiscordDM(env, dc.channelId, bodyA, dc.proxyUrl);
            if (rA && rA.ok) await env.SIGNAL_KV.put(alKey, 'sent', { expirationTtl: 86400 });
          }
        }
      }
    } catch (e) { console.warn('[earn-final-alarm]', e.message); }
  }

  // ── Evening-book cron backstop (AUDIT FIX 2026-07-31) ──
  // The gap-ladder series must not depend on someone loading a page after the
  // close. Cron ticks 16:30–17:10 compute tonight's bank-scale book if the
  // lazy GET /evening-book hasn't already; same claim slot, so no double
  // compute. Failure releases the claim → next tick retries.
  if ((etHour === 16 && etMin >= 30) || (etHour === 17 && etMin <= 10)) {
    try { await ensureEveningBook(env, etNow); } catch (e) { console.warn('[evening-book] backstop:', e.message); }
  }

  // ── Freshness watchdog (P33, owner 2026-08-03: "make sure this will not
  // happen again") ── Once per weekday ~10:40-10:55 ET, verify every feed
  // that can rot silently; DM the owner ONLY on violations (health checks
  // stay silent when green). Catches the whole disease family: dead cache
  // (pipeline weekend TTL), dead write path (btoa era), dead cron backstop
  // (evening book), dead morning stack (g935 snap).
  if (etNow.getDay() >= 1 && etNow.getDay() <= 5 && !isHol(etNow) &&
      etHour === 10 && etMin >= 40 && etMin <= 55) {
    const fwKey = `fresh_watch_${todayISO}`;
    if (!(await env.SIGNAL_KV.get(fwKey))) {
      try {
        const bad = [];
        const dayMs = 86400000;
        // 1. pipeline cache built-date (weekday tolerance 3 days)
        try {
          const pRaw = await env.SIGNAL_KV.get('earn_pipeline_cache');
          const p = pRaw ? JSON.parse(pRaw) : null;
          if (!p || !p.built) bad.push('earnings pipeline cache: MISSING');
          else if ((Date.now() - Date.parse(p.built + 'T12:00:00Z')) > 3 * dayMs)
            bad.push(`earnings pipeline cache stale (built ${p.built})`);
        } catch (e) { bad.push('earnings pipeline cache: unreadable'); }
        // 2. evening book — must cover the previous trading day
        try {
          const ebRaw = await env.SIGNAL_KV.get('evening_book_latest');
          const eb = ebRaw ? JSON.parse(ebRaw) : null;
          const prevT = isoDateET(prevTrade(etNow));
          if (!eb || !eb.date) bad.push('evening book: MISSING');
          else if (eb.date < prevT) bad.push(`evening book stale (${eb.date}, need ≥ ${prevT}) — cron backstop failed`);
        } catch (e) { bad.push('evening book: unreadable'); }
        // 3. morning stack — g935 snap must exist by 10:40 on a trading day
        if (!(await env.SIGNAL_KV.get(`g935_snap_${todayISO}`)))
          bad.push('9:35 book snap missing — morning regime stack did not fire');
        // 3b. gate series — today's 10:30 capture must be in gex1030_series_v1
        // by 10:40 (a 12-session seam went unnoticed for a month — 2026-08-03)
        try {
          const gsRaw = await env.SIGNAL_KV.get('gex1030_series_v1');
          const gs = gsRaw ? JSON.parse(gsRaw) : {};
          if (gs[todayISO] == null) bad.push('10:30 gate capture missing — anchor-filter series has a hole forming');
        } catch (_) { bad.push('gate series: unreadable'); }
        // 3c. earnings morning-after job — must leave SOME terminal marker
        // (sent / no-board / no-rows / no-webhook) by 10:40. An absent marker
        // means the 9:40–9:55 window never invoked the job at all (token blip
        // skipping the earnings block — observed 2026-08-03).
        if (!(await env.SIGNAL_KV.get(`earn_after_${todayISO}`)))
          bad.push('9:40 earnings morning-after job never ran (no terminal marker)');
        // 4. GitHub write path — the mirrored fallback must be ≤ 3 days old
        try {
          const gh = await fetch('https://raw.githubusercontent.com/rava8989/brave/main/data/earnings_pipeline.json',
            { headers: { 'User-Agent': 'schwab-proxy-worker/1.0' }, cf: { cacheTtl: 0 } });
          if (gh.ok) {
            const gj = await gh.json();
            if (gj && gj.built && (Date.now() - Date.parse(gj.built + 'T12:00:00Z')) > 3 * dayMs)
              bad.push(`GitHub fallback mirror stale (built ${gj.built}) — write path may be dead`);
          }
        } catch (_) { /* raw fetch flake — never alert on the checker's own failure */ }
        // 5. History mirror parity (P37, owner 2026-08-05: Aug 4 triple-settle
        // day — GitHub dropped stradPL+bobfPL while KV kept them; the OWNER was
        // the alarm). Compare the previous trading day's row field-by-field:
        // KV (truth) vs the GitHub mirror. Any KV field absent or different on
        // GitHub = drift → name the fields so /remirror-history is a one-click heal.
        try {
          const prevT = isoDateET(prevTrade(etNow));
          const kvHist = await getHistory(env);
          const kvRow = Array.isArray(kvHist) ? kvHist.find(r => r.date === prevT) : null;
          const ghH = await fetch('https://raw.githubusercontent.com/rava8989/brave/main/history_data.json',
            { headers: { 'User-Agent': 'schwab-proxy-worker/1.0' }, cf: { cacheTtl: 0 } });
          if (kvRow && ghH.ok) {
            const ghRow = (await ghH.json()).find(r => r.date === prevT) || {};
            const drift = Object.keys(kvRow).filter(k => JSON.stringify(kvRow[k]) !== JSON.stringify(ghRow[k]));
            if (drift.length)
              bad.push(`history mirror drift on ${prevT}: GitHub differs on ${drift.join(', ')} — run /remirror-history`);
          }
        } catch (_) { /* checker flake must not alert */ }
        if (bad.length) {
          const dcRaw = await env.SIGNAL_KV.get('discord_config');
          const dc = dcRaw ? JSON.parse(dcRaw) : null;
          if (dc && dc.channelId) {
            const r = await sendDiscordDM(env, dc.channelId,
              `🩺 **Freshness watchdog** — ${bad.length} feed(s) rotting:\n` + bad.map(x => `• ${x}`).join('\n'), dc.proxyUrl);
            if (r && r.ok) await env.SIGNAL_KV.put(fwKey, 'alerted', { expirationTtl: 86400 });
          }
        } else {
          await env.SIGNAL_KV.put(fwKey, 'ok', { expirationTtl: 86400 });
        }
      } catch (e) { console.warn('[fresh-watch]', e.message); }
    }
  }

  // ── Weekend earnings-calendar audit trigger (owner 2026-08-03) ──
  // Weekend crons tick at ~11:07 and ~15:07 ET Sat+Sun. One audit per
  // weekend, keyed to that weekend's Saturday; Sunday retries a failed
  // Saturday run. Claim-gated; failure releases the claim.
  if (etNow.getDay() === 6 || etNow.getDay() === 0) {
    const satISO = etNow.getDay() === 6 ? todayISO : isoDateET(new Date(etNow.getTime() - 86400000));
    const auditKey = `earn_cal_audit_${satISO}`;
    const auditDone = await env.SIGNAL_KV.get(auditKey);
    if (!auditDone || auditDone.startsWith('claim:')) {
      if (await claimSendSlot(env, auditKey)) {
        try {
          const res = await earnCalendarAudit(env);
          await env.SIGNAL_KV.put(auditKey, 'done', { expirationTtl: 6 * 86400 });
          console.log('[earn-cal-audit]', JSON.stringify(res));
        } catch (e) {
          console.warn('[earn-cal-audit]', e.message);
          try { const c = await env.SIGNAL_KV.get(auditKey); if (c && c.startsWith('claim:')) await env.SIGNAL_KV.delete(auditKey); } catch (_) {}
        }
      }
    }
  }

  // ── GEXMAGNET chain collector: RETIRED 2026-07-04 (owner call — strategy
  // shelved; Σ3 outperforms it ~10x, page stays unlisted). Collection stopped;
  // code + /gexm-trigger endpoint kept for manual re-runs if ever revived.
  // Historical data preserved: data/gexm_chains/ (GitHub) + local theta_raw.

  // ── GEX update during market hours (every cron tick) ──
  let gexResult = {};
  if (isMarket && schwabToken) {
    try {
      // Fix 1 (2026-06-30): do NOT pass masterChain — it's only ~±$200 (±2.7%) wide, so
      // GEX's walls/flip/regime/curve were silently confined there while the UI claimed ±8%.
      // handleGEXUpdate now always fetches its own wide (strikeCount=150 ≈ ±7.9%) chain.
      gexResult = await handleGEXUpdate(env, schwabToken);
    } catch (e) {
      gexResult = { gex: 'error', error: e.message };
      console.warn('[proxy] GEX update failed:', e.message || e);
    }
    // SPY heatmap feed — every ~10 min (display-only; see handleSpyGexUpdate).
    try {
      // 2-minute cadence (raised from 10 min on 2026-07-26): the flow panels
      // classify volume DELTAS per bar, so coarse bars would mean coarse flow.
      // Cost is ~1 extra Schwab call/min — measured usage stays ~7-10 of 120.
      const lastSpy = await env.SIGNAL_KV.get('gex_spy_last_ts');
      if (!lastSpy || Date.now() - parseInt(lastSpy, 10) > 110_000) {
        await env.SIGNAL_KV.put('gex_spy_last_ts', String(Date.now()), { expirationTtl: 86400 });
        await handleSpyGexUpdate(env, schwabToken);
      }
    } catch (e) { console.warn('[gex-spy]', e.message || e); }
  }

  // ── COR1M + VVIX cloud capture (2026-06-09 — machine-independence) ──
  // Schwab quotes $COR1M and $VVIX live (validated against ThetaData:
  // exact match). The worker samples them itself so the Tail Hedge status,
  // Diagonal COR1M gate, and the history cor1m column no longer depend on
  // the user's Mac being on (ThetaData/LaunchAgent = research-only now).
  //  • 9:30-9:36: every tick until the day's open is captured
  // Skipper heartbeat watchdog + CRON PROSTHESIS (2026-07-10): Cloudflare's
  // cron scheduler for the skipper stalled silently — worker healthy, ZERO
  // ticks for 3.5h, nobody alerted (its errstreak alarm only counts ticks that
  // RUN), and redeploys + trigger rewrites did NOT revive it. The skipper's
  // tick stamps errstreak.ts each run; when that is stale >3 min (or absent —
  // a never-ticking cron can't stamp one) during market hours, THIS cron
  // drives skipper /api/poll with the shared LINK_SECRET, so trades keep
  // relaying even with the skipper's scheduler dead. DM alarm once/day for
  // visibility. Dormant whenever the skipper's own cron is healthy.
  if (isMarket && env.SKIPPER_KV) {
    try {
      const hbRaw = await env.SKIPPER_KV.get('errstreak');
      const hb = hbRaw ? JSON.parse(hbRaw) : null;
      const hbAge = hb && hb.ts ? (Date.now() - hb.ts) : null;
      const hbDead = hb && (hbAge == null || hbAge > 3 * 60 * 1000);
      if (hbDead) {
        // Prosthesis ticks are spaced ≥5 min (2026-07-10 incident: every-minute
        // ticks outran KV propagation of the skipper's day-key guard and re-sent
        // the fresh BOBF relay ~8× — 5 min ≫ the ~60s KV staleness window, and
        // the skipper now also claims the day-key BEFORE sending). A relay
        // lagging the signal by ≤5 min is fine: paper fills use the trade
        // record's frozen prices, not live quotes.
        if (env.LINK_SECRET) {
          try {
            const lastPRaw = await env.SIGNAL_KV.get('skipper_prosthesis_last');
            if (!lastPRaw || (Date.now() - parseInt(lastPRaw, 10)) > 5 * 60 * 1000) {
              await env.SIGNAL_KV.put('skipper_prosthesis_last', String(Date.now()), { expirationTtl: 86400 });
              const prReq = new Request('https://skipper.internal/api/poll', {
                method: 'POST', headers: { 'X-Link-Secret': env.LINK_SECRET },
              });
              const pr = env.SKIPPER_WORKER
                ? await env.SKIPPER_WORKER.fetch(prReq)
                : await fetch('https://skipper.ravamt4.workers.dev/api/poll', {
                    method: 'POST', headers: { 'X-Link-Secret': env.LINK_SECRET } });
              console.log('[skipper-hb] prosthesis tick →', pr.status);
            }
          } catch (e) { console.warn('[skipper-hb] prosthesis failed:', e.message); }
        }
        const akSk = `skipper_dead_alert_${todayISO}`;
        if ((hbAge == null || hbAge > 10 * 60 * 1000) && !(await env.SIGNAL_KV.get(akSk))) {
          await env.SIGNAL_KV.put(akSk, '1', { expirationTtl: 86400 });
          const dcSk = JSON.parse(await env.SIGNAL_KV.get('discord_config') || 'null');
          const ageSk = hbAge != null ? `no tick for ${Math.round(hbAge / 60000)} min` : 'no tick since heartbeat shipped';
          if (dcSk && dcSk.channelId) await sendDiscordDM(env, dcSk.channelId,
            `🚨 **Skipper cron DEAD** — ${ageSk}. This worker is driving skipper ticks as a fallback (trades still relay). Permanent fix: redeploy skipper.`, dcSk.proxyUrl);
        }
      }
    } catch (e) { console.warn('[skipper-hb]', e.message); }
  }

  // Morning orphan sweep (lessons P18): settle any prior-session trade left
  // 'filled' by a missed EOD BEFORE today's entries can overwrite the undated
  // strad/bobf/gxbf KV slots (BOBF enters ~10:30, straddle ~9:33). Once per
  // day; the EOD block runs the sweep again as backstop. Token-independent
  // (Stooq closes), so it lives outside the schwabToken gate.
  if (isMarket) {
    try {
      const swKey = `orphan_sweep_${isoDateET(etNow)}`;
      if (!(await env.SIGNAL_KV.get(swKey))) {
        await env.SIGNAL_KV.put(swKey, '1', { expirationTtl: 86400 });
        await sweepOrphanSettles(env, etNow);
      }
    } catch (e) { console.warn('[orphan-sweep-am]', e.message); }
  }

  //  • after: every 5th minute → intraday series for cross detection
  if (isMarket && schwabToken) {
    try { await captureCor1mVvix(env, etNow, schwabToken); }
    catch (e) { console.warn('[cor1m] capture failed:', e.message || e); }
    // Research capture: morning (~10:00) GEX snapshot → gex_daily_<date>.am
    // (EOD persist pairs it with the close snapshot into data/gex_daily/)
    try {
      const hG = etNow.getHours(), mG = etNow.getMinutes();
      if (hG === 9 && mG >= 55 || hG === 10 && mG <= 10) {
        const kG = `gex_daily_${isoDateET(etNow)}`;
        if (!(await env.SIGNAL_KV.get(kG))) {
          const cur = await env.SIGNAL_KV.get('gex_current');
          if (cur) {
            const g = JSON.parse(cur);
            await env.SIGNAL_KV.put(kG, JSON.stringify({ am: {
              t: g.timestamp, spot: g.spot, regime: g.regime, totalGex: g.totalGex,
              flip: g.flipStrike ?? null, maxPos: g.maxPosStrike ?? null, maxNeg: g.maxNegStrike ?? null,
            } }), { expirationTtl: 90 * 86400 });
          }
        }
      }
    } catch (e) { console.warn('[gex-daily]', e.message); }
    // Research capture: 9:45-ish SPX put snapshot (Tail Hedge dataset, ThetaData-free)
    try { await captureTailPutSnap(env, etNow, masterChain); } catch (e) { console.warn('[tail-snap]', e.message); }
    // PNBF: 10:30 magnet snapshot (recipe uses the 10:30 OI-basis magnet)
    try {
      const hM = etNow.getHours(), mM = etNow.getMinutes();
      if (hM === 10 && mM >= 25 && mM < 45) {
        const kM = `mf_magnet_${isoDateET(etNow)}`;
        if (!(await env.SIGNAL_KV.get(kM))) {
          const cur = await env.SIGNAL_KV.get('gex_current');
          const g = cur ? JSON.parse(cur) : null;
          const freshG = sn => sn && (Date.now() / 1000 - (sn.timestamp || 0)) < 900;
          if (g?.maxPosStrike != null && freshG(g)) {
            // Store BOTH magnets for the live 0DTE-vs-all-expiry comparison
            // (2026-07-20): magnet = all-expiry (what PNBF uses today);
            // magnet0dte = 0DTE-only (the basis the backtest was validated on).
            const cur0 = await env.SIGNAL_KV.get('gex_current_0dte');
            const g0 = cur0 ? JSON.parse(cur0) : null;
            await env.SIGNAL_KV.put(kM, JSON.stringify({ magnet: g.maxPosStrike, t: g.timestamp,
              totalGex: g.totalGex ?? null,
              magnet0dte: freshG(g0) ? g0?.maxPosStrike ?? null : null,
              totalGex0dte: freshG(g0) ? g0?.totalGex ?? null : null }),
              { expirationTtl: 30 * 86400 });
          }
        }
      }
    } catch (e) { console.warn('[mf-magnet]', e.message); }
    // GEX gate (2026-07-23, owner-approved): persist the 10:30 0DTE total GEX
    // into a durable series (mf_magnet_* keys expire after 30d; the gate needs a
    // 120-day trailing window). Seeded from the 2023-06→2026-07 research bank.
    // SELF-HEALING (2026-07-24, item D): besides today's append, sweep the last
    // 28 days for holes (a capture outage left a Jul 3–18 gap once already) and
    // backfill from mf_magnet_* snapshots while their 30-day TTL still holds.
    // Every day the series changes it is also mirrored to GitHub
    // (gex1030_series.json), so a KV loss can never orphan the gate's window.
    try {
      const hG = etNow.getHours(), mG = etNow.getMinutes();
      if (hG === 10 && mG >= 26 && mG < 50) {
        const dG = isoDateET(etNow);
        const serRaw = await env.SIGNAL_KV.get('gex1030_series_v1');
        const ser = serRaw ? JSON.parse(serRaw) : {};
        let filled = 0;
        for (let back = 0; back <= 28; back++) {
          const pd = new Date(new Date(dG + 'T12:00:00Z').getTime() - back * 86400000);
          if (pd.getUTCDay() === 0 || pd.getUTCDay() === 6) continue;
          if (isHol(toET(new Date(pd.getTime() + 64800000)))) continue;
          const iso = pd.toISOString().slice(0, 10);
          if (ser[iso] != null) continue;
          const snapB = await env.SIGNAL_KV.get(`mf_magnet_${iso}`);
          const gexB = snapB ? JSON.parse(snapB).totalGex0dte : null;
          if (gexB != null) { ser[iso] = gexB; filled++; }
        }
        if (filled > 0) {
          const keys = Object.keys(ser).sort();
          const trimmed = {};
          for (const k of keys.slice(-1000)) trimmed[k] = ser[k];
          await env.SIGNAL_KV.put('gex1030_series_v1', JSON.stringify(trimmed));
          if (filled > 1) { try { await logEvent(env, 'info', 'gexgate', `series self-heal: backfilled ${filled - 1} hole(s) beyond today`, {}); } catch (_) {} }
          try { await mirrorGexSeriesToGitHub(env, trimmed); } catch (e) { console.warn('[gexgate-mirror]', e.message); }
        }
        // Dealer-liquidity daily series (2026-07-28, informational): one row/day
        // [medC, fly30] from today's 10:30 slot (10:00 fallback). No TTL — small.
        try {
          const lraw = await env.SIGNAL_KV.get(`gex_liq_${dG}`);
          const lslots = lraw ? (JSON.parse(lraw).slots || {}) : {};
          const l = lslots['10:30'] || lslots['10:00'];
          if (l && l.medC != null) {
            const lsRaw = await env.SIGNAL_KV.get('liq_series_v1');
            const ls = lsRaw ? JSON.parse(lsRaw) : {};
            if (ls[dG] == null) {
              ls[dG] = [l.medC, l.fly30];
              await env.SIGNAL_KV.put('liq_series_v1', JSON.stringify(ls));
            }
          }
        } catch (e) { console.warn('[liq-series]', e.message); }
      }
    } catch (e) { console.warn('[gexgate-series]', e.message); }
    // Tail Hedge live-trade parity: freeze today's open at/after 9:45 (robust —
    // no page-poll needed) and refresh its intraday P&L every tick.
    try { await freezeTailOpenIfDue(env, etNow, null, true); } catch (e) { console.warn('[tail-freeze]', e.message); }
    try { await refreshTailLiveQuotes(env, etNow, masterChain); } catch (e) { console.warn('[tail-refresh]', e.message); }
    // PNBF live strip (2026-07-28, display-only): magnet vs latest M8BF center,
    // refreshed every tick 9:30–16:00. The noon verdict stays the decision —
    // this only feeds the page's live row. Before the 10:30 snap the magnet is
    // mfReadInputs' live-fallback preview and is labeled as such.
    try {
      const hL = etNow.getHours(), mL = etNow.getMinutes(), dowL = etNow.getDay();
      if (dowL >= 1 && dowL <= 5 && !isHol(etNow) && hL < 16 &&
          (hL > 9 || (hL === 9 && mL >= 30)) && schwabToken) {
        const cutL = `${String(hL).padStart(2, '0')}:${String(mL).padStart(2, '0')}`;
        const li = await mfReadInputs(env, schwabToken, etNow, masterChain, cutL);
        const snapped = !!(await env.SIGNAL_KV.get(`mf_magnet_${isoDateET(etNow)}`));
        const live = {
          ts: Math.floor(Date.now() / 1000), t: cutL,
          magnet: li.magnet ?? null, magnetSrc: snapped ? '10:30 snap' : 'pre-snap preview',
          center: li.center ?? null, sigTime: li.sigTime ?? null,
          dist: (li.magnet != null && li.center != null) ? Math.abs(li.center - li.magnet) : null,
          entry: li.entry ?? null,
        };
        await env.SIGNAL_KV.put('mf_live', JSON.stringify(live), { expirationTtl: 900 });
      }
    } catch (e) { console.warn('[mf-live]', e.message); }
    // Research capture: Diagonal 12:30 put chain + GXBF 9:35 call chain
    // (with these all three bt datasets grow Schwab-only — ThetaData optional)
    try { await captureDiagChainSnap(env, etNow, masterChain); } catch (e) { console.warn('[diag-snap]', e.message); }
    try { await captureGxbfChainSnap(env, etNow, masterChain); } catch (e) { console.warn('[gxbf-snap]', e.message); }
    // Research capture: ~15:45 30DTE smile (VIX decomposition dataset)
    try { await captureVixSurfaceSnap(env, etNow, schwabToken); } catch (e) { console.warn('[surface-snap]', e.message); }
    // CycleLab live actual — today's session-so-far into KV (every 5 min)
    try { await captureCycTodaySlots(env, etNow, schwabToken); } catch (e) { console.warn('[cyc-live]', e.message); }
  }

  // ── Diagonal trade: open/close at 12:30 ET. Idempotent via diag_done_<date>.
  // Window 12:30–12:40 ET gives up to 5 retry attempts (cron is */2). We only
  // mark `diag_done` after a clean run — if the chain fetch / GitHub commit
  // throws, the next tick within the window retries automatically. After 12:40
  // we stop trying for the day and the live page shows the error state.
  let diagResult = {};
  const diagDoneKey = `diag_done_${todayISO}`;
  const isDiagonalEntry = (etHour === 12 && etMin >= 30 && etMin < 40);
  if (isDiagonalEntry) {
    // claim-token gate (P22): naive check-then-act let overlapping ticks race
    if (await claimSendSlot(env, diagDoneKey)) {
      try {
        diagResult = await handleDiagonalTrade(env, etNow, masterChain);
        // Only mark done when we either (a) opened a new trade, (b) cleanly
        // skipped per signal, OR (c) closed a prior trade without error.
        // Transient errors (token, chain, GitHub) release the claim so the
        // next 2-min tick retries within the 12:30–12:40 window.
        const hadError = !!(diagResult.error || diagResult.openError || diagResult.closeError);
        if (!hadError) {
          await env.SIGNAL_KV.put(diagDoneKey, 'sent', { expirationTtl: 86400 });
        } else {
          await env.SIGNAL_KV.delete(diagDoneKey);
          console.warn('[diag] not marking done — will retry next tick:', JSON.stringify(diagResult));
        }
      } catch (e) {
        diagResult = { diagonal: 'error', error: e.message };
        console.warn('[diag] handler threw:', e.message);
      }
    }
  }

  // ── PNBF morning status: 9:38–9:50 ET, idempotent via mf_morning_<date>.
  //    Calendar-only "possible today / not today" to the Sigma channel + DMs.
  const mfMornKey = `mf_morning_${todayISO}`;
  if (etHour === 9 && etMin >= 38 && etMin < 50) {
    try {
      if (await claimSendSlot(env, mfMornKey)) {
        await handleMagnetFlyMorning(env, etNow);
        await env.SIGNAL_KV.put(mfMornKey, 'sent', { expirationTtl: 86400 });
      }
    } catch (e) { console.warn('[mf-morning] threw:', e.message); }
  }

  // ── PNBF 11:30 heads-up: 11:28–11:38 ET, idempotent via mf_prealert_<date>.
  //    Advisory only — writes no trade; the noon check remains the source of truth.
  const mfPreKey = `mf_prealert_${todayISO}`;
  if (etHour === 11 && etMin >= 28 && etMin < 38) {
    try {
      if (await claimSendSlot(env, mfPreKey)) {
        // handler writes the 'sent' marker ITSELF before posting (P27) —
        // caller only releases the claim on pre-decision errors (post nothing)
        const pre = await handleMagnetFlyPreAlert(env, schwabToken, etNow, masterChain);
        if (pre.error) {
          await env.SIGNAL_KV.delete(mfPreKey);
          console.warn('[mf-pre] retry next tick:', pre.error);
        }
      }
    } catch (e) { console.warn('[mf-pre] threw:', e.message); }
  }

  // ── PNBF noon check: 12:00–12:15 ET window, idempotent via mf_done_<date>.
  //    Transient errors (scrape empty, chain gap) leave the slot unmarked so the
  //    next tick retries inside the window. Independent of every other strategy.
  let mfResult = {};
  const mfDoneKey = `mf_done_${todayISO}`;
  if (etHour === 12 && etMin < 15) {
    try {
      if (await claimSendSlot(env, mfDoneKey)) {
        // handler writes the 'sent' marker ITSELF before posting (P27)
        mfResult = await handleMagnetFlyNoon(env, schwabToken, etNow, masterChain);
        if (mfResult.error) {
          await env.SIGNAL_KV.delete(mfDoneKey);
          console.warn('[mf] not marking done — retry next tick:', mfResult.error);
        }
      }
    } catch (e) { console.warn('[mf] noon handler threw:', e.message); }
  }
  // PNBF EOD settle fallback (only if the bracket never filled)
  if (etHour === 16 && etMin >= 16 && etMin < 40) {
    try { await settleMagnetFlyEod(env, etNow, masterChain); } catch (e) { console.warn('[mf-eod]', e.message); }
  }

  // ── Refresh live quotes on the open diagonal, straddle, AND BOBF every market tick ──
  //    All three reuse the master chain fetched above (zero extra Schwab calls).
  if (isMarket && schwabToken) {
    // ── Straddle RETRY-OPEN ──────────────────────────────────────────
    // If today's morning signal said theme=strad but no straddle_open_trade
    // record exists, the 9:32 open attempt failed (Schwab glitch / chain
    // no-quote / a thrown exception we didn't see). Retry every minute
    // until cutoff so a transient blip doesn't lose us the whole day.
    // Once it succeeds, refreshStraddleLiveQuotes (below) will watch the
    // debit and flip to 'filled' the moment price hits the limit.
    try {
      const todayISORet = isoDateET(etNow);
      const stradExisting = await env.SIGNAL_KV.get('straddle_open_trade');
      const stradExistingObj = stradExisting ? JSON.parse(stradExisting) : null;
      const haveTodayStrad = stradExistingObj && stradExistingObj.openDate === todayISORet;
      const beforeCutoff = etNow.getHours() < STRADDLE_WORK_CUTOFF_HR ||
        (etNow.getHours() === STRADDLE_WORK_CUTOFF_HR && etNow.getMinutes() < STRADDLE_WORK_CUTOFF_MIN);
      if (!haveTodayStrad && beforeCutoff) {
        const msdRaw = await env.SIGNAL_KV.get(`morning_signal_data_${todayISORet}`);
        if (msdRaw) {
          const msd = JSON.parse(msdRaw);
          if (msd.theme === 'strad') {
            try {
              const retrySig = { theme: 'strad', badge: msd.badge || 'STRADDLE', rec: msd.rec };
              const trade = await openStraddleTrade(env, schwabToken, etNow, retrySig, masterChain);
              await env.SIGNAL_KV.put('straddle_open_trade', JSON.stringify(trade));
              await logEvent(env, 'info', 'strad-retry', `retry-opened ${trade.status}`, {
                strike: trade.strike, entryDebit: trade.entryDebit, maxDebit: trade.maxDebit,
                attemptedAt: `${etNow.getHours()}:${String(etNow.getMinutes()).padStart(2,'0')} ET`,
              });
              // Clear the cosmetic skip so the live page flips off "strad-missed"
              await env.SIGNAL_KV.delete(`straddle_skip_${todayISORet}`);
              console.log(`[strad-retry] opened ${trade.status} K=${trade.strike} debit=${trade.entryDebit}`);
            } catch (rErr) {
              // Don't spam the log on every minute — but keep console visibility.
              console.warn('[strad-retry] still failing:', rErr.message);
            }
          }
        }
      }
    } catch (e) { console.warn('[strad-retry] outer:', e.message); }

    try {
      await refreshDiagonalLiveQuotes(env, schwabToken, masterChain);
      await refreshStraddleLiveQuotes(env, schwabToken, etNow, masterChain);
      await refreshBobfLiveQuotes(env, schwabToken, etNow, masterChain);
      await refreshGxbfLiveQuotes(env, schwabToken, etNow, masterChain);
      await refreshM8bfLiveQuotes(env, schwabToken, etNow, masterChain);
      if (etHour >= 12) await refreshMagnetFlyLiveQuotes(env, schwabToken, etNow, masterChain);
    } catch (e) {
      console.warn('[live] refresh failed:', e.message);
    }
    // Research capture: archive each open fly\'s mid every 5 min (TP/stop research)
    try { await archiveFlyMarks(env, etNow); } catch (e) { console.warn('[fly-marks]', e.message); }
  }

  // ── BOBF entry: check every market tick during 10:29-12:15 ET window ──
  //    Also reuses the master chain. handleBobfEntry is self-retrying (it
  //    re-evaluates each tick when no open trade exists); refreshBobfLiveQuotes
  //    below handles working→filled transitions once a trade is recorded.
  let bobfResult = {};
  if (isMarket) {
    try { bobfResult = await handleBobfEntry(env, etNow, masterChain); }
    catch (e) {
      bobfResult = { bobf: 'error', error: e.message };
      console.warn('[bobf] entry failed:', e.message);
      // Upgrade silent failure → KV log so we can SEE why entry threw.
      try { await logEvent(env, 'error', 'bobf-open', `entry attempt threw`, { msg: e.message, stack: (e.stack || '').slice(0, 300) }); } catch (_) {}
    }
  }

  // ── GXBF entry: retry every market tick during the 9:35-9:45 ET window ──
  // 2026-06-09 FIX: window updated from 9:33 → 9:35 ET (see gxbfInWindow doc).
  // The chain quotes settle in the first ~5 minutes of regular session, so
  // 09:33 fires were reading transient post-open quotes for the gamma center.
  // Retry every tick (mirrors how BOBF retries every tick) until the entry
  // completes or the window passes. STRATEGY INDEPENDENCE:
  // gated solely on GXBF's OWN theme, read from the persisted morning signal
  // (morning_signal_data_<date>). Never consults M8BF/Straddle/BOBF state.
  let gxbfResult = {};
  if (isMarket && gxbfInWindow(etNow)) {
    // claim-token gate (P22). The handler writes its own terminal markers
    // ('done'/'window-passed'/'blackout:…') into gxbf_done_<date>; on retryable
    // outcomes we release our claim so the next tick can try again.
    const gxbfKey = `gxbf_done_${todayISO}`;
    if (await claimSendSlot(env, gxbfKey)) {
      try {
        const msdRaw = await env.SIGNAL_KV.get(`morning_signal_data_${todayISO}`);
        const msd = msdRaw ? JSON.parse(msdRaw) : null;
        if (msd && msd.theme === 'gxbf') {
          gxbfResult = await handleGxbfEntry(env, etNow, msd, masterChain);
        } else {
          gxbfResult = { status: 'not-gxbf-theme', theme: msd ? msd.theme : 'pending' };
        }
      } catch (e) { gxbfResult = { gxbf: 'error', error: e.message }; console.warn('[gxbf] entry failed:', e.message); }
      // release our claim unless the handler replaced it with a terminal marker
      try {
        const curV = await env.SIGNAL_KV.get(gxbfKey);
        if (curV && curV.startsWith('claim:')) await env.SIGNAL_KV.delete(gxbfKey);
      } catch (_) {}
    }
  }

  // ── 9:35 ET morning-signal self-check + Discord alert ──
  // If by 9:35 ET we still don't have a 'sent' marker for today's morning
  // signal, fire ONE Discord alert so silent failures don't rot for hours.
  // Most failures self-heal within 1-2 cron ticks; this only catches the
  // "stuck for 5+ min" case (Schwab outage, stuck claim, cron stall, etc.).
  // Rate-limited via `morning_alert_<date>` flag (24-hr TTL).
  const morningAlertKey = `morning_alert_${todayISO}`;
  try {
    const past935 = etHour > 9 || (etHour === 9 && etMin >= 35);
    if (past935 && isMarket) {
      const alreadyAlerted = await env.SIGNAL_KV.get(morningAlertKey);
      const currentStatus = await env.SIGNAL_KV.get(`morning_signal_${todayISO}`);
      // An in-flight 'claim:' means a tick is actively sending RIGHT NOW
      // (claims are ≤40s and self-release). That is NOT "missing" — it's
      // in-progress. Only alarm on GENUINE absence (null/expired). This
      // kills the false "Morning signal MISSING" DM that fired during the
      // self-heal recovery window on 2026-05-15.
      const inFlight = !!(currentStatus && currentStatus.startsWith('claim:'));
      if (!alreadyAlerted && currentStatus !== 'sent' && !inFlight) {
        const minsLate = (etHour - 9) * 60 + etMin - 30;
        const stRaw = await env.SIGNAL_KV.get('schwab_refresh_state');
        const refreshState = stRaw ? JSON.parse(stRaw) : null;
        const schwabHealthy = !refreshState || refreshState.ok !== false;
        const claimStuck = !!(currentStatus && currentStatus.startsWith('claim:'));
        const reasons = [];
        if (!schwabHealthy) {
          const errs = refreshState?.consecutiveErrors || 0;
          reasons.push(`Schwab refresh degraded (${errs} errors)`);
        }
        if (claimStuck) {
          const parts = currentStatus.split(':');
          const claimAgeMs = parts[2] ? Date.now() - parseInt(parts[2], 10) : 0;
          reasons.push(`stuck claim (age ${Math.round(claimAgeMs/1000)}s)`);
        }
        if (!currentStatus) reasons.push('no claim yet — cron may have stalled');
        const dcRaw = await env.SIGNAL_KV.get('discord_config');
        if (dcRaw) {
          const dc = JSON.parse(dcRaw);
          if (dc.channelId) {
            const result = await sendDiscordDM(env, dc.channelId,
              `🚨 **Morning signal MISSING** — ${minsLate} min past 9:30 ET\n` +
              `State: \`${currentStatus || 'none'}\`\n` +
              (reasons.length ? `Suspected: ${reasons.join('; ')}\n` : '') +
              `→ Check worker logs, hit \`/trigger\` to retry, or re-auth Schwab via dashboard.`,
              dc.proxyUrl);
            if (result.ok) {
              await env.SIGNAL_KV.put(morningAlertKey, 'sent', { expirationTtl: 86400 });
              console.warn(`[morning-check] FIRED ALERT: ${minsLate} min late, status=${currentStatus || 'none'} via ${result.source}`);
              await logEvent(env, 'error', 'morning-check', `morning signal ${minsLate} min late — Discord alert sent`,
                             { status: currentStatus || null, reasons, source: result.source });
            } else {
              console.warn('[morning-check] post failed:', result.error);
            }
          }
        }
      }
    }
  } catch (selfCheckErr) {
    console.error('[morning-check] failed:', selfCheckErr.message);
  }

  // ── Morning signal: retries every cron tick until sent ──
  const morningDoneKey = `morning_signal_${todayISO}`;
  const morningDone = await env.SIGNAL_KV.get(morningDoneKey);
  const preMarket = etHour < 9 || (etHour === 9 && etMin < 30);

  // Self-heal: if morning signal already sent today but no straddle_skip
  // recorded AND no live straddle trade for today, derive + write a skip
  // reason so the live page shows the correct status. Covers days when the
  // morning cron ran BEFORE the skip-write code was deployed (and any future
  // case where the skip write was lost mid-flight).
  if (morningDone === 'sent' && isMarket) {
    const stradSkipKey = `straddle_skip_${todayISO}`;
    const haveSkip = await env.SIGNAL_KV.get(stradSkipKey);
    if (!haveSkip) {
      const stradOpenRaw = await env.SIGNAL_KV.get('straddle_open_trade');
      const stradOpen = stradOpenRaw ? JSON.parse(stradOpenRaw) : null;
      const haveStrad = stradOpen && stradOpen.openDate === todayISO;
      if (!haveStrad) {
        // Recompute signal cheaply just to get rec/theme (mirrors morning block).
        try {
          const recoveryToken = await getAccessToken(env);
          // Pull just the prices we need; we don't need full vix history here.
          const [vixHist, spxHist] = await Promise.all([
            fetchSchwabJSON(`https://api.schwabapi.com/marketdata/v1/pricehistory?symbol=%24VIX&periodType=day&period=3&frequencyType=minute&frequency=1&needExtendedHoursData=true`, recoveryToken, env),
            fetchSchwabJSON(`https://api.schwabapi.com/marketdata/v1/pricehistory?symbol=%24SPX&periodType=day&period=3&frequencyType=minute&frequency=1&needExtendedHoursData=true`, recoveryToken, env),
          ]);
          // Derive vixToday/vixYClose/vixYOpen/spxGapPct from candles
          const todayStr = etNow.toDateString();
          const vCandles = (vixHist.candles || []).slice().sort((a,b) => a.datetime - b.datetime);
          const vToday = vCandles.find(c => toET(new Date(c.datetime)).toDateString() === todayStr && toET(new Date(c.datetime)).getHours() === 9 && toET(new Date(c.datetime)).getMinutes() >= 30);
          const vYesterday = vCandles.filter(c => toET(new Date(c.datetime)).toDateString() !== todayStr);
          const vY = vYesterday[vYesterday.length - 1];
          const vYOpen = vYesterday.find(c => toET(new Date(c.datetime)).getHours() === 9 && toET(new Date(c.datetime)).getMinutes() >= 30);
          const sCandles = (spxHist.candles || []).slice().sort((a,b) => a.datetime - b.datetime);
          const sToday = sCandles.find(c => toET(new Date(c.datetime)).toDateString() === todayStr && toET(new Date(c.datetime)).getHours() === 9 && toET(new Date(c.datetime)).getMinutes() >= 30);
          const sYesterday = sCandles.filter(c => toET(new Date(c.datetime)).toDateString() !== todayStr);
          const sY = sYesterday[sYesterday.length - 1];
          const vixT = vToday?.open, vixYC = vY?.close, vixYO = vYOpen?.open;
          const spxGap = (sToday?.open != null && sY?.close != null) ? ((sToday.open - sY.close) / sY.close) * 100 : 0;
          if (vixT != null && vixYC != null && vixYO != null) {
            const recoverySig = calculateSignal({
              vixToday: vixT, vixYOpen: vixYO, vixYClose: vixYC,
              spxGapPct: spxGap, etDate: etNow,
            });
            { const _g = await gexGateEval(env, isoDateET(etNow)); applyGexGateToSignal(recoverySig, _g.skip, _g.rank); }
            if (recoverySig.theme !== 'strad') {
              await env.SIGNAL_KV.put(stradSkipKey, JSON.stringify({
                theme: recoverySig.theme,
                rec: recoverySig.rec,
                recordedAt: new Date().toISOString(),
                source: 'self-heal',
              }), { expirationTtl: 86400 });
              console.log(`[strad] self-heal wrote skip: theme=${recoverySig.theme} rec=${recoverySig.rec}`);
            }
          }
        } catch (e) { console.warn('[strad] self-heal failed:', e.message); }
      }
    }
  }

  if (morningDone === 'sent' || globalThis.__morningSentDay === todayISO || preMarket) {
    return { status: 'discord_poll', discord: discordResult, gex: gexResult, diagonal: diagResult, time: `${etHour}:${String(etMin).padStart(2,'0')} ET` };
  }

  // ── Stuck-claim self-heal + notify ──
  // Claims carry a timestamp suffix (claim:<uuid>:<ms>). If we find one >40s
  // old here (post-9:30 ET) it means the previous tick crashed between claim
  // and 'sent'. The send path is now ≤~35s, so a claim older than 40s is
  // genuinely dead — clear it immediately and let THIS tick retry. Combined
  // with the 90s claim TTL, an orphaned claim can never block more than ~40s.
  if (morningDone && morningDone.startsWith('claim:')) {
    const parts = morningDone.split(':');
    const claimTsMs = parseInt(parts[2] || '0', 10);
    const ageMs = claimTsMs ? Date.now() - claimTsMs : 0;
    if (claimTsMs && ageMs > 40_000) {
      const ageS = Math.round(ageMs / 1000);
      console.warn(`[proxy] Stuck claim detected (age ${ageS}s) — clearing and notifying`);
      await env.SIGNAL_KV.delete(morningDoneKey);
      // Fire-and-forget Discord notification
      try {
        const dcRaw = await env.SIGNAL_KV.get('discord_config');
        if (dcRaw) {
          const dc = JSON.parse(dcRaw);
          if (dc.channelId) {
            await sendDiscordDM(env, dc.channelId,
              `⚠️ **Stuck morning claim** (${ageS}s old) — cleared, retrying this tick.`,
              dc.proxyUrl);
          }
        }
      } catch (notifyErr) {
        console.warn('[proxy] stuck-claim notify failed:', notifyErr.message || notifyErr);
      }
      // Fall through: acquire a fresh claim below
    } else if (claimTsMs && ageMs <= 40_000) {
      // Another tick owns a fresh claim (<40s, still actively sending) —
      // don't stomp it. It will either finish ('sent') or self-release.
      return {
        status: 'claim_in_flight',
        claim: morningDone,
        age_ms: ageMs,
        time: `${etHour}:${String(etMin).padStart(2,'0')} ET`,
      };
    }
    // If claimTsMs === 0 (legacy value without timestamp) we also fall through;
    // short TTL will age it out on its own.
  }

  // ── Claim the slot BEFORE doing slow API work ──
  // Concurrent cron ticks can pass the gate above before any of them writes,
  // because the slot claim used to live ~30s later (after VIX/SPX fetches).
  // We write a unique token, wait for KV to propagate, then verify our token won.
  //
  // TTL is 90s (not 300s, not 86400s): the send path is now fast (≤~35s:
  // 20s VIX race + 12s fallback + quick compute/post), so a healthy claim
  // never lives long. If a tick is hard-killed mid-send, the claim
  // self-expires within 90s and the next minute's cron re-fires — no
  // 3-min stuck-claim window. 'sent' marker still uses 86400s.
  const claimToken = crypto.randomUUID();
  const claimValue = `claim:${claimToken}:${Date.now()}`;
  await env.SIGNAL_KV.put(morningDoneKey, claimValue, { expirationTtl: 90 });
  await new Promise(r => setTimeout(r, 1500)); // let concurrent ticks also write
  const claimCheck = await env.SIGNAL_KV.get(morningDoneKey, { cacheTtl: 30 });
  if (claimCheck !== claimValue) {
    console.log(`[proxy] Lost claim race (saw ${claimCheck}, mine was ${claimValue}) — skipping`);
    return { status: 'duplicate_skipped', claimWinner: claimCheck, time: `${etHour}:${String(etMin).padStart(2,'0')} ET` };
  }

  console.log('[proxy] Morning window — sending signal');

  // 1. Get access token
  const token = await getAccessToken(env);

  // 2. Fetch VIX 5-day history → yesterday open + close
  const end = Date.now();
  const start = end - 5 * 24 * 60 * 60 * 1000;
  const vixHistUrl = `https://api.schwabapi.com/marketdata/v1/pricehistory?symbol=%24VIX&periodType=day&period=5&frequencyType=minute&frequency=1&startDate=${start}&endDate=${end}&needExtendedHoursData=true`;
  const vixHist = await fetchSchwabJSON(vixHistUrl, token, env);
  if (!vixHist.candles || !vixHist.candles.length) throw new Error('No VIX history data');

  const candles = vixHist.candles;
  const todayStr = etNow.toDateString();

  // Find yesterday's trading day
  let yDate = null;
  for (let i = candles.length - 1; i >= 0; i--) {
    const d = new Date(candles[i].datetime);
    const dET = toET(d);
    if (dET.toDateString() !== todayStr) { yDate = dET.toDateString(); break; }
  }

  let vixYOpen = null, vixYClose = null;
  if (yDate) {
    const yCandles = candles.filter(c => toET(new Date(c.datetime)).toDateString() === yDate);
    yCandles.sort((a, b) => a.datetime - b.datetime);
    const openCandle = yCandles.find(c => {
      const d = toET(new Date(c.datetime));
      return d.getHours() === 9 && d.getMinutes() >= 30 && d.getMinutes() <= 35;
    });
    const closeCandle = yCandles.slice().reverse().find(c => {
      const d = toET(new Date(c.datetime));
      return d.getHours() === 16 && d.getMinutes() >= 10 && d.getMinutes() <= 15;
    }) || yCandles[yCandles.length - 1];

    if (openCandle) vixYOpen = parseFloat(openCandle.open.toFixed(2));
    if (closeCandle) vixYClose = parseFloat(closeCandle.close.toFixed(2));
  }

  // Keep the minute-candle close as an INDEPENDENT second opinion before the
  // authoritative override, so the two can be cross-checked below.
  const _vixYCloseCandle = vixYClose;

  // AUTHORITATIVE: prefer the SETTLED prior-day VIX from history_data.json.
  // The minute-candle "close" above can latch a stale late-session 1-min bar
  // (2026-06-24: candle 19.44 vs the true settled close 18.63), flipping the
  // overnight-VIX drop + the GXBF/Straddle gate and showing a wrong prev-close
  // on the card. The EOD settle wrote the real close last night and it's the
  // same value the dashboard uses — trust it; candle/quote stay as fallback.
  try {
    const _hist = await getHistory(env);
    const _todayISO = isoDateET(etNow);
    const _prior = (_hist || [])
      .filter(r => r && r.date && r.date < _todayISO && r.vixClose != null)
      .sort((a, b) => (a.date < b.date ? -1 : 1));
    if (_prior.length) {
      const _last = _prior[_prior.length - 1];
      if (_last.vixClose != null) vixYClose = parseFloat(_last.vixClose);
      if (_last.vixOpen  != null) vixYOpen  = parseFloat(_last.vixOpen);
    }
  } catch (e) { console.warn('[proxy] history prior-VIX lookup failed, using candle:', e.message); }

  // CROSS-CHECK GUARD (2026-06-25): the prior VIX close gates GXBF↔Straddle, so
  // two INDEPENDENT derivations (settled history + minute-candle) must agree.
  // If they diverge > 0.40 VIX pts the gate is in doubt → fire a Discord alert
  // NOW (signal lands ~9:30, GXBF entry 9:36 → time to react) and keep the
  // settled-history value. This SCREAMS instead of silently trading a bad gate —
  // it would have caught 6/24 (history 18.63 vs candle 19.44, Δ0.81). No single
  // source is trusted blindly. See [[feedback_quote_closeprice_holiday]].
  if (vixYClose != null && _vixYCloseCandle != null && Math.abs(vixYClose - _vixYCloseCandle) > 0.40) {
    const _msg = `⚠️ VIX PRIOR-CLOSE MISMATCH — settled history ${vixYClose} vs minute-candle ${_vixYCloseCandle} (Δ${(vixYClose - _vixYCloseCandle).toFixed(2)}). Using history (${vixYClose}). Overnight-VIX gates GXBF↔Straddle — VERIFY before the 9:36 entry.`;
    console.warn('[proxy]', _msg);
    try {
      const _dcRaw = await env.SIGNAL_KV.get('discord_config');
      const _dc = _dcRaw ? JSON.parse(_dcRaw) : null;
      if (_dc && _dc.channelId) await sendDiscordDM(env, _dc.channelId, _msg, _dc.proxyUrl);
    } catch (_) {}
  }

  // FALLBACK ONLY (2026-06-22 fix). quote.closePrice is NOT holiday-aware:
  // after a market holiday it returns the HOLIDAY's phantom close instead of the
  // real prior-session close (Juneteenth 2026-06-19 → 16.78 vs the true 6-18
  // close 16.40), which flips the overnight-VIX sign and mis-fires the Straddle
  // /GXBF gate. The minute-candle path above already skips holidays (no intraday
  // bars on a closed day), so trust it; only use quote.closePrice when the
  // candle yielded nothing.
  if (vixYClose === null) {
    try {
      const qData = await fetchSchwabJSON(`https://api.schwabapi.com/marketdata/v1/quotes?symbols=%24VIX&fields=quote`, token, env);
      const qClose = qData?.['$VIX']?.quote?.closePrice;
      if (qClose) vixYClose = parseFloat(qClose.toFixed(2));
    } catch (e) { console.warn('[proxy]', e.message || e); }
  }

  if (vixYClose === null) throw new Error('Could not determine yesterday VIX close');

  // 3. Get today's VIX open = FIRST CHANGE in VIX after 9:30:00 ET.
  //
  // DUAL-SOURCE RACE: poll Schwab quote.lastPrice AND Tastytrade
  // market-data.last in parallel (500ms each). First source to detect a
  // value/timestamp change from its baseline WINS. The other source keeps
  // running briefly until it notices the winner has captured, then exits.
  //
  // Why dual-source: either feed can lag or stay stale-cached at 9:30. By
  // racing both, we get the FIRST genuine new VIX publication regardless
  // of which vendor's CDN updates first.
  //
  // PER-TICK budget: 20s, NOT 5 min. The cron fires every minute during
  // market hours, so the cron itself is the retry loop. Holding the claim
  // across a 5-min in-tick poll was the root cause of stuck claims —
  // Cloudflare kills the cron tick long before 5 min, orphaning the claim
  // for ~3 min until self-heal. 20s catches the first genuine Cboe
  // publication (they republish ~every 15s); if this tick juuust misses,
  // the next minute's tick catches it. Signal lands 9:30:xx, worst 9:31:xx.
  let vixToday = null;
  let vixSource = null;
  {
    const maxWaitMs = 20_000;
    const pollIntervalMs = 500;
    const deadlineMs = Date.now() + maxWaitMs;
    const startedAt = Date.now();
    const state = { schwab: null, tasty: null };  // separate slots — Schwab is always preferred

    async function pollSchwab() {
      // CANONICAL VIX OPEN — matches dashboard's schwabFetchHistorical() exactly:
      // fetch pricehistory 1-min VIX bars, find first candle with ET hour=9 min>=30,
      // use its `.open` field (the actual first 9:30 tick value).
      //
      // Previously this used /quotes lastPrice which drifts from the 9:30
      // candle.open by 0.05-0.10 because quote.lastPrice is the CURRENT tick,
      // not the first 9:30 tick. Caused the 2026-06-08 Discord vs Dashboard
      // signal divergence (Discord said 90% dead-zone, Dashboard said 95% edge).
      // See user feedback 2026-06-08.
      let attempt = 0;
      while (Date.now() < deadlineMs && state.schwab === null) {
        attempt++;
        try {
          // 5-day window matches dashboard for cache parity
          const end = Date.now();
          const start = end - 5 * 24 * 60 * 60 * 1000;
          const histUrl = `https://api.schwabapi.com/marketdata/v1/pricehistory?symbol=%24VIX&periodType=day&period=5&frequencyType=minute&frequency=1&startDate=${start}&endDate=${end}&needExtendedHoursData=true`;
          const hist = await fetchSchwabJSON(histUrl, token, env);
          if (hist?.candles?.length) {
            // Find first candle in today's session at-or-after 9:30 ET (matches
            // dashboard filter: hour > 9 OR (hour === 9 && min >= 30)).
            const todayStr = etNow.toDateString();
            const open930 = hist.candles
              .filter(c => {
                const d = toET(new Date(c.datetime));
                return d.toDateString() === todayStr &&
                       (d.getHours() > 9 || (d.getHours() === 9 && d.getMinutes() >= 30));
              })
              .sort((a, b) => a.datetime - b.datetime)[0];
            if (open930 && open930.open != null) {
              const price = parseFloat(open930.open.toFixed(2));
              const tET = toET(new Date(open930.datetime));
              state.schwab = price;
              console.log(`[proxy] SCHWAB 9:30 candle.open captured: ${price} @ ${tET.toTimeString().slice(0,8)} ET (attempt ${attempt}, ${Math.round((Date.now()-startedAt)/1000)}s)`);
              return;
            }
          }
        } catch (e) { /* keep trying — candle may not exist yet at 9:30:00 */ }
        if (state.schwab !== null) return;
        await new Promise(r => setTimeout(r, pollIntervalMs));
      }
    }

    async function pollTasty() {
      // CANONICAL TASTY OPEN — uses /market-data/Index/VIX `open` field
      // (today's session open). Matches dashboard's Schwab pricehistory 9:30
      // candle.open methodology in spirit. Tasty's `open` field appears
      // once Cboe publishes the day's first regular-session print (~9:30:01).
      //
      // FIX (2026-06-09): freshness check must require timestamp >= 9:30 ET,
      // not just today's date. Without the time check, a cron tick firing at
      // exactly 9:30:00 ET would accept Tasty's pre-market cached `open`
      // value (today's date, but pre-9:30 timestamp) and post a message with
      // STALE VIX BEFORE Cboe's first regular-session publication.
      // Matches Schwab's candle filter at line ~3568 and _isFreshTick below.
      let attempt = 0;
      while (Date.now() < deadlineMs && state.tasty === null && state.schwab === null) {
        attempt++;
        try {
          const result = await tastyGetVix(env);  // { open, price, asOf, raw }
          if (result.open != null && state.tasty === null) {
            // Sanity: only accept if updated-at timestamp is from today's
            // REGULAR-SESSION (date matches AND time >= 9:30 ET) — defends
            // against stale prior-session cache AND pre-market cache.
            const ts = result.raw?.['updated-at'] || result.asOf;
            const dt = ts ? new Date(String(ts)) : null;
            let fresh = false;
            if (dt && isFinite(dt.getTime())) {
              const tET = toET(dt);
              const sameDay = tET.toDateString() === etNow.toDateString();
              const postOpen = (tET.getHours() * 60 + tET.getMinutes()) >= 570; // 9:30 ET
              fresh = sameDay && postOpen;
            }
            if (fresh) {
              state.tasty = parseFloat(result.open.toFixed(2));
              console.log(`[proxy] TASTY captured 9:30 open from /Index/VIX.open: ${state.tasty} @ ${ts} (attempt ${attempt}, ${Math.round((Date.now()-startedAt)/1000)}s)`);
              return;
            }
            // open present but stale (pre-market or yesterday) — keep polling.
          }
        } catch (e) { /* keep trying — Tasty may not have today's open yet */ }
        if (state.tasty !== null || state.schwab !== null) return;
        await new Promise(r => setTimeout(r, pollIntervalMs));
      }
    }

    // DUAL-SOURCE RACE — both vendors now read TODAY'S OPEN (not current tick):
    //   Schwab: pricehistory 1-min, first candle @ hour=9 min>=30, use `.open`
    //   Tasty:  /market-data/Index/VIX, use `open` field
    // Whichever vendor publishes the 9:30 open first wins. The dual-source
    // 2nd Discord message still posts the OTHER vendor's data for cross-check.
    // Both methodologies match the dashboard's schwabFetchHistorical() now.
    // CLAUDE.md rule 11 satisfied: worker + dashboard agree on vix_today_open.
    // VIX OPEN = the first print at/after 9:30:00 ET = the open of the first
    // 9:30 1-min candle (pollSchwab). SCHWAB ONLY. Tasty's /Index/VIX `.open`
    // is the spiky auction open, NOT the first print, so it must never set the
    // displayed open. If Schwab's candle is slow, the quote-advancing-tick
    // fallback below (also first-print) covers it — we never fall to Tasty.open.
    // (user, 2026-06-18, settled for good: "first print after 9:30 ... today 16.67")
    await pollSchwab();
    vixToday = state.schwab;
    vixSource = state.schwab != null ? 'schwab' : null;
    if (vixToday !== null) {
      console.log(`[proxy] VIX OPEN ${vixToday} captured via ${vixSource} after ${Math.round((Date.now()-startedAt)/1000)}s of polling`);
    } else {
      console.warn('[proxy] Neither source caught a VIX change in 5min — both feeds may be stale');
    }
  }

  if (vixToday === null) {
    // Last-resort fallback to old quote-polling logic.
  //    VIX is a calculated index — Cboe republishes a new value every ~15s.
  //    The first poll we issue may return a cached/stale tick (Schwab's edge
  //    caches the last published value), which can show a pre-open snapshot
  //    timestamped at 9:30:00 but reflecting pre-market data. Bug observed
  //    2026-05-13: first tick showed 18.25 (yesterday's vintage) while the
  //    9:31 minute bar opened at 17.98.
  //
  //    Fix: poll FAST (200ms), record the first observed tradeTime as
  //    "baseline", and only accept a tick whose tradeTime has advanced past
  //    that baseline AND is >= 9:30:00 ET. That guarantees we see a
  //    genuinely-new Cboe publication, not Schwab's cached pre-open snapshot.
  //
  //    Why not candles/openPrice:
  //    - Schwab's pricehistory for $VIX can lag 60-90 min behind real time
  //    - Schwab's quote.openPrice is unreliable for calculated indices (VIX)
  //      e.g. 2026-04-14: openPrice=18.73 but first print was 18.25
  const quoteUrl = `https://api.schwabapi.com/marketdata/v1/quotes?symbols=%24VIX&fields=quote`;
  const maxWaitMs = 12_000;  // per-tick — cron retries next minute, never hold claim long
  const pollIntervalMs = 200;  // tight loop — Cboe pubs ~every 15s, catch ASAP
  const deadlineMs = Date.now() + maxWaitMs;
  let attempt = 0;
  let baselineTradeTime = null;  // first tradeTime we see — proves "tick advanced"
  while (Date.now() < deadlineMs) {
    attempt++;
    const vixQuote = await fetchSchwabJSON(quoteUrl, token, env);
    const vixQ = vixQuote?.['$VIX']?.quote;
    if (vixQ?.lastPrice && vixQ?.tradeTime) {
      const tt = vixQ.tradeTime;
      const tradeET = toET(new Date(tt));
      const tradeMin = tradeET.getHours() * 60 + tradeET.getMinutes();
      const isToday = tradeET.toDateString() === todayStr;
      const postOpen = tradeMin >= 570;

      // First observed tradeTime becomes the baseline.
      if (baselineTradeTime === null) {
        baselineTradeTime = tt;
        console.log(`[proxy] VIX baseline tick: lastPrice=${vixQ.lastPrice}, tradeTime=${tradeET.toTimeString().slice(0,8)} ET (waiting for next publication)`);
      } else if (tt > baselineTradeTime && isToday && postOpen) {
        // Accept: tradeTime has advanced AND is post-9:30 ET = genuinely new
        // Cboe publication.
        vixToday = parseFloat(vixQ.lastPrice.toFixed(2));
        vixSource = 'schwab';  // fallback path uses Schwab quotes
        console.log(`[proxy] VIX open ${vixToday} (tradeTime ${tradeET.toTimeString().slice(0,8)} ET, advanced from baseline, attempt ${attempt})`);
        break;
      }
    }
    await new Promise(r => setTimeout(r, pollIntervalMs));
  }

  if (vixToday === null) {
    // VIX not published yet THIS tick. Do NOT throw (that would noise an
    // error DM) and do NOT leave the claim sitting (that orphans it →
    // stuck-claim → ~3 min silence). RELEASE the claim and bail cleanly;
    // the next cron tick (≤60s) re-claims fresh and tries again. By then
    // Cboe has definitely published. Net: at most 1 extra minute, no spam,
    // no stuck claim — vs the old 5-min-poll-then-orphan failure mode.
    await env.SIGNAL_KV.delete(morningDoneKey);
    console.warn(`[proxy] VIX not ready this tick (${attempt} attempts) — claim released, cron retries next minute`);
    return { status: 'vix_pending_retry', time: `${etHour}:${String(etMin).padStart(2,'0')} ET` };
  }
  }  // end Schwab-fallback else block

  // 4. Fetch SPX quote → gap % + today's SPX open
  let spxGapPct = null;
  let spxTodayOpen = null;
  let spxYClose = null;   // hoisted to function scope — also read by dual-source 2nd-post block below
  try {
    // Get SPX yesterday close from history
    const spxHistUrl = `https://api.schwabapi.com/marketdata/v1/pricehistory?symbol=%24SPX&periodType=day&period=5&frequencyType=minute&frequency=1&startDate=${start}&endDate=${end}&needExtendedHoursData=true`;
    const spxHist = await fetchSchwabJSON(spxHistUrl, token, env);
    if (spxHist.candles && yDate) {
      const spxYCandles = spxHist.candles.filter(c => toET(new Date(c.datetime)).toDateString() === yDate);
      spxYCandles.sort((a, b) => a.datetime - b.datetime);
      const spxCloseCandle = spxYCandles.slice().reverse().find(c => {
        const d = toET(new Date(c.datetime));
        return d.getHours() === 16 && d.getMinutes() >= 10 && d.getMinutes() <= 15;
      }) || (spxYCandles.length ? spxYCandles[spxYCandles.length - 1] : null);
      if (spxCloseCandle) spxYClose = spxCloseCandle.close;
    }

    // Get SPX today open — Tasty PRIMARY (refresh_token never expires, so a
    // dead Schwab token can't null this and break the signal), Schwab FALLBACK.
    // FIX (2026-06-09): Tasty's `open` field must pass freshness check (today's
    // date AND timestamp >= 9:30 ET). Previously a cron tick firing at 9:30:00
    // accepted Tasty's pre-market cached `open` (yesterday's value with a
    // today-dated timestamp), producing a stale SPX open in the morning signal.
    // Also no longer falls back to `last`/`price` — those are CURRENT ticks
    // and drift after open; `open` is the only reliable session-open marker.
    try {
      const ts = await tastyGetSpx(env);
      const dt = ts.asOf ? new Date(String(ts.asOf)) : null;
      let freshSpx = false;
      if (dt && isFinite(dt.getTime())) {
        const tET = toET(dt);
        const sameDay = tET.toDateString() === etNow.toDateString();
        const postOpen = (tET.getHours() * 60 + tET.getMinutes()) >= 570;
        freshSpx = sameDay && postOpen;
      }
      if (ts.open != null && isFinite(ts.open) && ts.open > 0 && freshSpx) {
        spxTodayOpen = parseFloat(ts.open.toFixed(2));
        console.log(`[proxy] SPX open ${spxTodayOpen} via tastytrade (primary, fresh @ ${ts.asOf})`);
      } else if (ts.open != null) {
        console.log(`[proxy] SPX Tasty open ${ts.open} REJECTED — stale (asOf=${ts.asOf}). Falling back to Schwab.`);
      }
    } catch (e) { console.warn('[proxy] Tasty SPX failed, trying Schwab:', e.message || e); }
    if (spxTodayOpen == null) {
      // Schwab fallback — prefer pricehistory 9:30 candle (matches dashboard);
      // quote.openPrice is unreliable for indices pre/at open.
      try {
        const spxHistUrl = `https://api.schwabapi.com/marketdata/v1/pricehistory?symbol=%24SPX&periodType=day&period=2&frequencyType=minute&frequency=1&needExtendedHoursData=false`;
        const spxHist = await fetchSchwabJSON(spxHistUrl, token, env);
        const todayStr = etNow.toDateString();
        const open930 = (spxHist.candles || [])
          .filter(c => {
            const d = toET(new Date(c.datetime));
            return d.toDateString() === todayStr &&
                   (d.getHours() > 9 || (d.getHours() === 9 && d.getMinutes() >= 30));
          })
          .sort((a, b) => a.datetime - b.datetime)[0];
        if (open930 && open930.open != null) {
          spxTodayOpen = parseFloat(open930.open.toFixed(2));
          console.log(`[proxy] SPX open ${spxTodayOpen} via Schwab pricehistory 9:30 candle (fallback)`);
        }
      } catch (e) { console.warn('[proxy] Schwab SPX pricehistory failed:', e.message); }
      // Last resort: quote endpoint (kept for emergency, but openPrice
      // can be 0/null at exactly 9:30 — only use if pricehistory failed).
      if (spxTodayOpen == null) {
        try {
          const spxQuoteUrl = `https://api.schwabapi.com/marketdata/v1/quotes?symbols=%24SPX&fields=quote`;
          const spxQuote = await fetchSchwabJSON(spxQuoteUrl, token, env);
          const spxQ = spxQuote?.['$SPX']?.quote;
          if (spxQ?.openPrice != null && spxQ.openPrice > 0) {
            spxTodayOpen = parseFloat(spxQ.openPrice.toFixed(2));
            console.log(`[proxy] SPX open ${spxTodayOpen} via Schwab quote.openPrice (last resort)`);
          }
        } catch (e) { console.warn('[proxy] Schwab quote.openPrice failed:', e.message); }
      }
    }

    if (spxYClose && spxTodayOpen) {
      spxGapPct = ((spxTodayOpen - spxYClose) / spxYClose) * 100;
    }
  } catch (e) { console.warn('[proxy]', e.message || e); }

  // 4b. GitHub PUT for vixOpen + spxOpen is DEFERRED until after Discord post
  // (it takes ~1-2s and would block the Discord message — runs after section 9).

  // 4c. Fetch previous day's m8bfWR + last-20 vixClose from history_data.json.
  //     vixPct20d is required for the diagonal filter (VIX_MID 50–80% dead zone).
  //     rsi14 is required for the BOBF type-aware gate (Friday 40-65 / vix-down ≤70).
  let prevWR = null;
  let vixPct20d = null;
  let rsi14 = null;
  try {
    const histData = await getHistory(env);
    if (Array.isArray(histData) && histData.length) {
      // Find the most recent entry before today that has m8bfWR
      const sorted = histData
        .filter(r => r.date < todayISO && r.m8bfWR != null)
        .sort((a, b) => b.date.localeCompare(a.date));
      if (sorted.length > 0) {
        prevWR = parseFloat(sorted[0].m8bfWR);
        console.log(`[proxy] prevWR = ${prevWR}% (from ${sorted[0].date})`);
      }

      // Pull the last 20 prior vixClose values (newest last) for the diagonal
      // regime filter. CANONICAL via signal-engine.js computeVixPct20d — DO
      // NOT inline the percentile math here. See lessons.md P5.
      const vix20 = histData
        .filter(r => r.date < todayISO && r.vixClose != null && r.vixClose > 0)
        .sort((a, b) => a.date.localeCompare(b.date))
        .slice(-20)
        .map(r => parseFloat(r.vixClose));
      const { pct: pctComputed } = computeVixPct20d(vixToday, vix20);
      if (pctComputed != null) {
        vixPct20d = pctComputed;
        console.log(`[proxy] vixPct20d = ${vixPct20d}% (vixToday=${vixToday} vs last ${vix20.length} closes)`);
      }

      // Compute RSI(14) on prior daily closes for the BOBF type-aware gate.
      // Same data source the dashboard uses; ensures the Discord message and
      // /trade endpoint show "No BOBF (RSI X.X outside 40-65)" instead of
      // wrongly claiming BOBF is in play.
      const closes30 = histData
        .filter(r => r.date < todayISO && r.spxClose != null && r.spxClose > 0)
        .sort((a, b) => a.date.localeCompare(b.date))
        .slice(-30)
        .map(r => parseFloat(r.spxClose));
      if (closes30.length >= 15) {
        rsi14 = computeRSI14(closes30);
        console.log(`[proxy] rsi14 = ${rsi14?.toFixed(2)} (from ${closes30.length} prior closes)`);
      }
    }
  } catch (e) { console.warn('[proxy] history fetch failed:', e.message || e); }

  // 5. Calculate signal
  // COR1M open from the cloud capture — may still be null at 9:30–9:37
  // (calculated index prints late); the Diagonal line then shows "pending"
  // and the /trade endpoint + dashboard pick up the real status minutes later.
  const cor1mOpenToday = await getCor1mOpenToday(env, isoDateET(etNow));
  const signal = calculateSignal({
    vixToday,
    vixYOpen,
    vixYClose,
    spxGapPct,
    etDate: etNow,
    prevWR,
    vixPct20d,
    rsi14,
    cor1m: cor1mOpenToday,
  });
  { const _g = await gexGateEval(env, isoDateET(etNow)); applyGexGateToSignal(signal, _g.skip, _g.rank); }

  // 5a. Persist the morning signal so /straddle-today and other endpoints
  // can read EXACTLY what the morning cron computed (including the live
  // quote-polled vixToday and the official quote-overridden vixYClose
  // — those differ from the 9:30 candle data and matter for theme).
  try {
    await env.SIGNAL_KV.put(`morning_signal_data_${isoDateET(etNow)}`, JSON.stringify({
      ...signal,
      vixToday, vixYOpen, vixYClose, spxGapPct, prevWR, vixPct20d, rsi14,
      spxOpen: spxTodayOpen,  // ← straddle center anchor (snap5 of this)
      computedAt: new Date().toISOString(),
    }), { expirationTtl: 86400 });
  } catch (_) { /* non-critical */ }

  // 5b/5c: Straddle entry + BOBF prefilter are DEFERRED until after the Discord
  // post (they take ~500ms+ each and would block the message). Run after section 9.

  // 6. Build Discord message — include Tail Hedge today's signal
  const vixValues = { yOpen: vixYOpen, yClose: vixYClose, todayOpen: vixToday };
  const canonBanner = vixSource === 'schwab' ? '📡 **SCHWAB DATA**\n\n' : '📡 **TASTYTRADE DATA**\n\n';
  const tailLineCanon = await getTailHedgeStatusLine(env);
  signal._tailLine = tailLineCanon;  // for embed builder
  try { signal._tiltLine = await computeTiltLine(env, isoDateET(etNow)); } catch (_) { /* advisory only */ }
  try { signal._gexLine = await computeGexLine(env); } catch (_) { /* advisory only */ }
  try { signal._cycleLine = await computeCycleLine(env, etNow); } catch (_) { /* advisory only */ }
  try { signal._volFlowLine = await computeVolFlowLine(env, etNow); } catch (_) { /* advisory only */ }
  try { signal._m8bfWrLine = await computeM8bfWrLine(env, etNow); } catch (_) { /* advisory only */ }
  try { signal._skewLine = await computeSkewLine(); } catch (_) { /* advisory only */ }
  const message = canonBanner + buildDiscordMessage(signal, vixValues, tailLineCanon);

  // 7. Slot already claimed at the top of the morning block. Reuse the same key.
  const msDoneKey = morningDoneKey;

  // 8. Post to Discord (via consolidated helper — tries DISCORD_TOKEN first)
  const dcRaw = await env.SIGNAL_KV.get('discord_config');
  if (!dcRaw) { await env.SIGNAL_KV.delete(msDoneKey); throw new Error('No Discord config in KV — sync from browser'); }
  const dc = JSON.parse(dcRaw);

  // 8-pre. LAST-CALL dupe gate (2026-06-10: TRIPLE send at 9:33:18/9:33:33/
  // 9:34:09). The claim verify at the top runs ~20-35s before this point;
  // KV is eventually consistent, so a parallel invocation (overlapping cron
  // tick or /gex fallback) can pass its own claim verify without either side
  // seeing the other. By NOW the rival's claim/'sent' write has had those
  // ~30s to propagate — one fresh read here kills the duplicate pre-post.
  const lastCall = await env.SIGNAL_KV.get(msDoneKey);
  if (lastCall === 'sent' || (lastCall && lastCall.startsWith('claim:') && lastCall !== claimValue)) {
    console.log(`[proxy] Pre-send dupe gate: slot='${(lastCall || '').slice(0, 24)}…' not mine — aborting duplicate`);
    return { status: 'duplicate_avoided_presend', time: `${etHour}:${String(etMin).padStart(2, '0')} ET` };
  }

  // MORNING CARD IMAGE (2026-06-18) — render the forecast card to a PNG and post
  // it; the M8BF skip/combo "strikes" go as a small (-#) subtext BELOW the image.
  // ANY failure (render/font/upload) falls back to the original text message so
  // the morning signal can never go silent.
  let result = null;
  try {
    const cardData = buildMorningCardData(signal, vixValues, tailLineCanon,
      { block: mfCalendarBlock(toET(new Date())) });
    const png = await renderMorningCardPng(cardData);
    // Footer rides as the message content (renders ABOVE the image in Discord)
    // so the live link + disclaimer sit on top of the card, link clickable.
    result = await sendDiscordImage(env, dc.channelId, png, dc.proxyUrl, 'morning.png', DISCORD_FOOTER);
  } catch (e) {
    await logEvent(env, 'warn', 'morning', 'card image failed — text fallback', { msg: e && (e.message || String(e)) });
    result = null;
  }
  if (!result || !result.ok) {
    result = await sendDiscordDM(env, dc.channelId, message.slice(0, 2000), dc.proxyUrl);
  }
  let dcData = result.data || {};
  if (!result.ok) {
    // Retry once on 429 (rate limit) after Retry-After delay
    if (result.status === 429 && dcData.retry_after) {
      await new Promise(r => setTimeout(r, (dcData.retry_after + 0.5) * 1000));
      const retry = await sendDiscordDM(env, dc.channelId, message.slice(0, 2000), dc.proxyUrl);
      if (!retry.ok) {
        await env.SIGNAL_KV.delete(msDoneKey);
        throw new Error('Discord post failed after retry: ' + JSON.stringify(retry));
      }
      await env.SIGNAL_KV.put(msDoneKey, 'sent', { expirationTtl: 86400 });
      globalThis.__morningSentDay = todayISO;
      return retry.data || { ok: true };
    }
    await env.SIGNAL_KV.delete(msDoneKey);
    throw new Error('Discord post failed: ' + (result.error || JSON.stringify(dcData)));
  }

  // Mark morning signal as fully sent
  await env.SIGNAL_KV.put(msDoneKey, 'sent', { expirationTtl: 86400 });
  globalThis.__morningSentDay = todayISO;   // isolate-local guard (KV-lag immune)
  // NOTE: subscriber fan-out was MOVED to live trade EXECUTIONS (2026-06-16,
  // user: "send the actual trade each time it fills, not the morning signal").
  // See the /link-notify route + skipper's fill handler (fanoutText). The
  // morning signal is intentionally NOT fanned out to subscribers anymore.
  await logEvent(env, 'info', 'morning', 'canonical signal posted', {
    source: vixSource,   // 'schwab' or 'tastytrade' — which won the VIX race
    rec: signal.rec, badge: signal.badge, theme: signal.theme,
    vix: { todayOpen: vixToday, yOpen: vixYOpen, yClose: vixYClose },
    spxOpen: spxTodayOpen, spxGapPct,
  });

  // ── 8b. (DISABLED 2026-06-18) Dual-source "2nd message from the OTHER vendor".
  //    Removed because the open is Schwab first-print ONLY. Tasty's `.open` is
  //    the wrong field (spiky auction open, not the first 9:30 print), so
  //    posting it as a peer message only ever showed a conflicting number and
  //    caused endless confusion. ONE Schwab morning message now, period.
  //    The block below is gated off (safe to delete in a future cleanup).
  //    (user req 2026-06-18 — "first print after 9:30 ... today was 16.67")
  if (false) try {
    const otherSource = vixSource === 'schwab' ? 'tastytrade' : 'schwab';
    let vixOther = null, spxOther = null;
    let vixOtherTs = null, spxOtherTs = null;
    // Freshness check: timestamp is from today's regular session (>= 9:30 ET).
    // Accepts epoch ms (Schwab tradeTime) or ISO string (Tasty updated-at).
    function _isFreshTick(ts) {
      if (ts == null) return false;
      const d = (typeof ts === 'number') ? new Date(ts) : new Date(String(ts));
      if (!isFinite(d.getTime())) return false;
      const et = toET(d);
      if (et.toDateString() !== etNow.toDateString()) return false;
      return (et.getHours() * 60 + et.getMinutes()) >= 570;  // 9*60+30
    }
    // Budget for the 2nd source — reduced from 60s to 22s (2026-06-09 fix).
    // Cloudflare scheduled handlers have a ~30s wall-clock limit. The canonical
    // post takes ~5-8s; 60s here was being killed mid-poll, dropping the 2nd
    // message entirely. 22s + 8s canonical leaves headroom.
    //
    // 2026-06-09 FIX: 2nd-source Schwab now uses pricehistory 9:30 candle
    // (same methodology as pollSchwab in the main race) instead of the quote
    // endpoint. The quote endpoint's tradeTime can lag 10-30s at market open,
    // making _isFreshTick fail repeatedly within the budget. pricehistory
    // publishes the 9:30 candle within 1-2 seconds of Cboe's first print.
    // SPX 2nd-source Tasty branch: use s.open ONLY (no fallback to last/
    // price — those are CURRENT ticks, not 9:30 OPEN, so the cross-check
    // would compare apples to oranges).
    const dualDeadline = Date.now() + 22_000;
    let dualAttempts = 0;
    let lastVixTs = null, lastSpxTs = null, lastVixVal = null, lastSpxVal = null;
    while (Date.now() < dualDeadline && (vixOther == null || spxOther == null)) {
      dualAttempts++;
      let lastFetchErr = null;
      if (otherSource === 'tastytrade') {
        if (vixOther == null) {
          try {
            const r = await tastyGetVix(env);
            // Use the `open` field ONLY (today's session open, matches Schwab
            // pricehistory 9:30 candle.open) — never `price` (current tick),
            // same as the SPX 2nd-source branch. tastyGetVix null-guards a
            // stale pre-open `open`, so v stays null until a real open prints.
            const v = r.open;
            const ts = r.raw?.['updated-at'] || r.asOf;
            if (v != null) { lastVixVal = parseFloat(v.toFixed(2)); lastVixTs = ts; }
            if (v != null && _isFreshTick(ts)) { vixOther = lastVixVal; vixOtherTs = ts; }
          } catch (e) { lastFetchErr = `tasty-vix:${e.message}`; }
        }
        if (spxOther == null) {
          try {
            const s = await tastyGetSpx(env);
            const ts = s.asOf || s.raw?.['updated-at'];
            // Only use s.open (today's session open). s.last is the current
            // tick which drifts from open; including it would make the 2nd
            // message compare current price vs canonical open — misleading.
            if (s.open != null) { lastSpxVal = parseFloat(s.open.toFixed(2)); lastSpxTs = ts; }
            if (s.open != null && _isFreshTick(ts)) { spxOther = lastSpxVal; spxOtherTs = ts; }
          } catch (e) { lastFetchErr = `tasty-spx:${e.message}`; }
        }
      } else if (token) {
        // SCHWAB 2nd-source via pricehistory — NOT quote endpoint. pricehistory
        // publishes the 9:30 candle within 1-2 seconds of Cboe's first print;
        // quote.tradeTime can lag 10-30 seconds at market open.
        if (vixOther == null) {
          try {
            const end = Date.now(); const start = end - 5 * 24 * 60 * 60 * 1000;
            const histUrl = `https://api.schwabapi.com/marketdata/v1/pricehistory?symbol=%24VIX&periodType=day&period=5&frequencyType=minute&frequency=1&startDate=${start}&endDate=${end}&needExtendedHoursData=true`;
            const hist = await fetchSchwabJSON(histUrl, token, env);
            if (hist?.candles?.length) {
              const todayStr2 = etNow.toDateString();
              const open930 = hist.candles
                .filter(c => {
                  const d = toET(new Date(c.datetime));
                  return d.toDateString() === todayStr2 &&
                         (d.getHours() > 9 || (d.getHours() === 9 && d.getMinutes() >= 30));
                })
                .sort((a, b) => a.datetime - b.datetime)[0];
              if (open930 && open930.open != null) {
                lastVixVal = parseFloat(open930.open.toFixed(2));
                lastVixTs = open930.datetime;
                if (_isFreshTick(open930.datetime)) { vixOther = lastVixVal; vixOtherTs = open930.datetime; }
              }
            }
          } catch (e) { lastFetchErr = `schwab-vix:${e.message}`; }
        }
        if (spxOther == null) {
          try {
            const end = Date.now(); const start = end - 2 * 24 * 60 * 60 * 1000;
            const histUrl = `https://api.schwabapi.com/marketdata/v1/pricehistory?symbol=%24SPX&periodType=day&period=2&frequencyType=minute&frequency=1&startDate=${start}&endDate=${end}&needExtendedHoursData=false`;
            const hist = await fetchSchwabJSON(histUrl, token, env);
            if (hist?.candles?.length) {
              const todayStr2 = etNow.toDateString();
              const open930 = hist.candles
                .filter(c => {
                  const d = toET(new Date(c.datetime));
                  return d.toDateString() === todayStr2 &&
                         (d.getHours() > 9 || (d.getHours() === 9 && d.getMinutes() >= 30));
                })
                .sort((a, b) => a.datetime - b.datetime)[0];
              if (open930 && open930.open != null) {
                lastSpxVal = parseFloat(open930.open.toFixed(2));
                lastSpxTs = open930.datetime;
                if (_isFreshTick(open930.datetime)) { spxOther = lastSpxVal; spxOtherTs = open930.datetime; }
              }
            }
          } catch (e) { lastFetchErr = `schwab-spx:${e.message}`; }
        }
      } else {
        await logEvent(env, 'warn', 'morning', `2nd-source dropped — Schwab token unavailable (otherSource=${otherSource})`, { vixSource });
        break;
      }
      if (lastFetchErr) console.warn('[proxy] dual-msg fetch err:', lastFetchErr);
      if (vixOther == null || spxOther == null) {
        await new Promise(r => setTimeout(r, 800));
      }
    }
    if (vixOther == null) {
      // Log to KV so /debug-morning-log shows WHY the 2nd message didn't post.
      await logEvent(env, 'warn', 'morning', `2nd-source dropped — no fresh VIX after ${Math.round((Date.now()-(dualDeadline-22000))/1000)}s`, {
        otherSource, canonicalSource: vixSource, attempts: dualAttempts,
        lastVixVal, lastVixTs, lastSpxVal, lastSpxTs,
      });
    }
    if (vixOther != null) {
      const spxGapOther = (spxYClose && spxOther) ? ((spxOther - spxYClose) / spxYClose) * 100 : spxGapPct;
      const signalOther = calculateSignal({
        vixToday: vixOther, vixYOpen, vixYClose,
        spxGapPct: spxGapOther,
        etDate: etNow, prevWR, vixPct20d, rsi14,
        cor1m: cor1mOpenToday,
      });
      { const _g = await gexGateEval(env, isoDateET(etNow)); applyGexGateToSignal(signalOther, _g.skip, _g.rank); }
      signalOther._tiltLine = signal._tiltLine;   // same advisory on the 2nd copy
      signalOther._gexLine = signal._gexLine;
      signalOther._cycleLine = signal._cycleLine;
      signalOther._volFlowLine = signal._volFlowLine;
      signalOther._m8bfWrLine = signal._m8bfWrLine;
      const otherBanner = otherSource === 'schwab' ? '📡 **SCHWAB DATA**\n\n' : '📡 **TASTYTRADE DATA**\n\n';
      const msgOther = otherBanner + buildDiscordMessage(signalOther, { yOpen: vixYOpen, yClose: vixYClose, todayOpen: vixOther }, tailLineCanon);
      await new Promise(r => setTimeout(r, 1500));   // Discord rate-limit safety
      const r2 = await sendDiscordDM(env, dc.channelId, msgOther.slice(0, 2000), dc.proxyUrl);
      if (r2.ok) {
        await logEvent(env, 'info', 'morning', `2nd-source signal posted (${otherSource})`, {
          rec: signalOther.rec, badge: signalOther.badge, theme: signalOther.theme,
          vix: vixOther, vixTs: vixOtherTs, spxOpen: spxOther, spxTs: spxOtherTs,
          attempts: dualAttempts,
        });
      } else {
        // 2nd-source send failed — log it so /debug-morning-log shows the error.
        await logEvent(env, 'warn', 'morning', `2nd-source Discord post FAILED (${otherSource})`, {
          status: r2.status, error: r2.error, data: JSON.stringify(r2.data || {}).slice(0, 240),
          rec: signalOther.rec, vix: vixOther,
        });
      }
    }
  } catch (e) {
    await logEvent(env, 'warn', 'morning', `dual-source 2nd post EXCEPTION (canonical already sent)`, { msg: e.message, stack: (e.stack || '').slice(0, 240) });
  }

  // ════════════════════════════════════════════════════════════════════
  // POST-DISCORD WORK (deferred until after the message went out)
  // ────────────────────────────────────────────────────────────────────
  // Everything here was previously running BEFORE Discord, delaying the
  // message by 2-5 seconds. Now Discord fires first, then these run.
  // Each block is wrapped in try/catch so a failure here doesn't break
  // the morning_signal_<today>='sent' state (already marked).
  // ════════════════════════════════════════════════════════════════════

  // a) GitHub PUT — write vixOpen + spxOpen to history_data.json
  try {
    await upsertHistoryGitHub(env, todayISO, {
      vixOpen: vixToday,
      ...(spxTodayOpen != null ? { spxOpen: spxTodayOpen } : {}),
    });
  } catch (e) { console.warn('[proxy/post] GitHub PUT:', e.message || e); }

  // b) Open straddle if signal says so, OR record skip reason
  if (signal.theme === 'strad') {
    try {
      const existingRaw = await env.SIGNAL_KV.get('straddle_open_trade');
      const existing = existingRaw ? JSON.parse(existingRaw) : null;
      if (!existing || existing.openDate !== isoDateET(etNow)) {
        // ── Item 9: pre-trade signal validator ──
        // Re-pull VIX from a fresh source (Tasty) and recompute theme.
        // If theme flips (rare, but caught May-14-like off-strategy fires),
        // abort with skip reason instead of opening a phantom trade.
        let validatorAborted = false;
        try {
          // tastyGetVix returns an OBJECT — extract a NUMERIC cross-check VIX:
          // prefer the validated session open, fall back to the current tick.
          // (Bug fix 2026-06-16: this used the object directly → drift was NaN
          //  and freshVix.toFixed() threw, so the validator silently aborted /
          //  no-op'd every live straddle. Now it compares real numbers.)
          const fresh = await tastyGetVix(env);
          const freshVix = fresh.open ?? fresh.price;
          if (freshVix != null && Number.isFinite(freshVix)) {
            const drift = Math.abs(freshVix - vixToday);
            // Recompute signal with the fresh VIX
            const freshSig = calculateSignal({
              vixToday: freshVix, vixYOpen, vixYClose, spxGapPct,
              etDate: etNow, prevWR, vixPct20d, rsi14,
            });
            { const _g = await gexGateEval(env, isoDateET(etNow)); applyGexGateToSignal(freshSig, _g.skip, _g.rank); }
            if (freshSig.theme !== 'strad') {
              validatorAborted = true;
              console.warn(`[strad-validate] ABORT: theme flipped (morning vix ${vixToday} → fresh ${freshVix.toFixed(2)}, theme=${freshSig.theme})`);
              await logEvent(env, 'error', 'strad-validate',
                `aborted open: theme flipped at fire time`,
                { morningVix: vixToday, freshVix: parseFloat(freshVix.toFixed(2)),
                  drift: parseFloat(drift.toFixed(2)),
                  morningTheme: 'strad', freshTheme: freshSig.theme });
              // Record skip reason so live page shows correct status
              await env.SIGNAL_KV.put(`straddle_skip_${isoDateET(etNow)}`, JSON.stringify({
                theme: freshSig.theme,
                rec: `Pre-trade validator aborted (vix drift ${drift.toFixed(2)}): ${freshSig.rec}`,
                recordedAt: new Date().toISOString(),
                source: 'pre-trade-validator',
              }), { expirationTtl: 86400 });
            } else if (drift > 0.5) {
              console.log(`[strad-validate] drift ${drift.toFixed(2)} but theme=strad — proceeding`);
            }
          } else {
            // No usable fresh VIX (Tasty `open` still the stale pre-open snapshot
            // and no tick) — do NOT abort on a missing cross-check. A degraded
            // open beats a phantom skip; the morning signal already gated entry.
            console.log('[strad-validate] no usable fresh VIX — skipping validation, proceeding with open');
            try { await logEvent(env, 'info', 'strad-validate', 'validation skipped (no usable fresh VIX)', { morningVix: vixToday }); } catch (_) {}
          }
        } catch (vErr) {
          console.warn('[strad-validate] fresh VIX fetch failed (proceeding):', vErr.message);
        }
        if (!validatorAborted) {
          const stradToken = await getAccessToken(env);
          const trade = await openStraddleTrade(env, stradToken, etNow, signal, masterChain);
          await env.SIGNAL_KV.put('straddle_open_trade', JSON.stringify(trade));
          console.log(`[strad] opened ${trade.status} K=${trade.strike} debit=${trade.entryDebit} maxDebit=${trade.maxDebit}`);
          await logEvent(env, 'info', 'strad-open', `opened ${trade.status}`, {
            strike: trade.strike, entryDebit: trade.entryDebit, maxDebit: trade.maxDebit,
          });
        }
      }
    } catch (e) {
      // Upgrade from console.warn to logEvent so we can SEE these failures.
      // Today (2026-05-22): the 9:32 cron threw silently and the bot never
      // retried — straddle_open_trade stayed null all morning, live page
      // misleadingly showed "Working order" via the cosmetic skip-state.
      console.warn('[strad] open failed:', e.message);
      try { await logEvent(env, 'error', 'strad-open', `open attempt failed`, { msg: e.message, stack: (e.stack || '').slice(0, 300) }); } catch (_) {}
    }
  } else {
    try {
      await env.SIGNAL_KV.put(`straddle_skip_${isoDateET(etNow)}`, JSON.stringify({
        theme: signal.theme,
        rec: signal.rec,
        recordedAt: new Date().toISOString(),
      }), { expirationTtl: 86400 });
    } catch (_) { /* non-critical */ }
  }

  // b2) Open GXBF if signal says so, OR record skip reason.
  // STRATEGY INDEPENDENCE: this branch reads ONLY signal.theme === 'gxbf'
  // (GXBF's own gate — exactly analogous to the Straddle theme === 'strad'
  // branch above). It never blocks / is blocked by M8BF/Straddle/BOBF.
  if (signal.theme === 'gxbf') {
    try {
      const existingRaw = await env.SIGNAL_KV.get('gxbf_open_trade');
      const existing = existingRaw ? JSON.parse(existingRaw) : null;
      if (!existing || existing.openDate !== isoDateET(etNow)) {
        // P22 (audit 2026-07-31): claim the SAME slot the per-tick GXBF section
        // uses — on a delayed-signal morning an overlapping tick can reach its
        // claimed section while this deferred block is still running; without a
        // claim here both would enter (the gated-straddle path fans out to every
        // subscriber — the old triple-send failure mode). Same release rule as
        // the tick caller: drop our claim unless the handler wrote a terminal.
        const gxbfKeyB2 = `gxbf_done_${isoDateET(etNow)}`;
        if (await claimSendSlot(env, gxbfKeyB2)) {
          try {
            const gxbfResult = await handleGxbfEntry(env, etNow, signal, masterChain);
            console.log(`[gxbf] entry: ${JSON.stringify(gxbfResult)}`);
          } finally {
            try {
              const curV = await env.SIGNAL_KV.get(gxbfKeyB2);
              if (curV && curV.startsWith('claim:')) await env.SIGNAL_KV.delete(gxbfKeyB2);
            } catch (_) {}
          }
        } else {
          console.log('[gxbf] b2 deferred entry skipped — slot claimed/done (tick section owns it)');
        }
      }
    } catch (e) { console.warn('[gxbf] open failed:', e.message); }
  } else {
    try {
      await env.SIGNAL_KV.put(`gxbf_skip_${isoDateET(etNow)}`, JSON.stringify({
        theme: signal.theme,
        rec: signal.gxbfText || signal.rec,
        recordedAt: new Date().toISOString(),
      }), { expirationTtl: 86400 });
    } catch (_) { /* non-critical */ }
  }

  // c) BOBF static-filter pre-flight
  try {
    const pf = await prefilterBobf(env, etNow, vixToday, vixYClose);
    if (pf?.skipped) console.log(`[bobf] prefilter skipped: ${pf.skipped} ${pf.reason || (pf.reasons||[]).join(',')||''}`);
  } catch (e) { console.warn('[bobf] prefilter failed:', e.message); }

  return {
    status: 'success',
    signal: signal.rec,
    badge: signal.badge,
    vix: { todayOpen: vixToday, yOpen: vixYOpen, yClose: vixYClose },
    spxGapPct,
    spxOpen: spxTodayOpen,
    githubDate: todayISO,
    postedAt: new Date().toISOString(),
  };
}

// ════════════════════════════════════════════════════════════════════
// GEX (GAMMA EXPOSURE) CALCULATION
// ════════════════════════════════════════════════════════════════════

// ── Dealer-liquidity metric (2026-07-28): how wide the 0DTE book is quoting ──
// Rides the SAME chain snapshot as GEX — zero extra API calls. Nearest expiry
// only, strikes within ±60 pts of spot. medC/medP = median ask−bid in points;
// fly30 = half-spread round-trip (points) of a 30-wide 1/−2/1 call fly at the
// ATM strike: (s_lo + 2·s_ctr + s_hi)/2 — what crossing half the spread on all
// 4 contracts costs vs a pure mid fill. ×100 = $/lot.
function computeLiquidity(chainData, spot) {
  const nearest = m => Object.keys(m || {}).sort()[0];
  const grab = (map) => {
    const out = {};
    const strikes = (map || {})[nearest(map)] || {};
    for (const ks of Object.keys(strikes)) {
      const k = parseFloat(ks);
      const c = Array.isArray(strikes[ks]) ? strikes[ks][0] : null;
      if (!c || !spot || Math.abs(k - spot) > 60) continue;
      const b = c.bid, a = c.ask;
      if (typeof b === 'number' && typeof a === 'number' && a > 0 && a >= b && b >= 0)
        out[k] = +(a - b).toFixed(2);
    }
    return out;
  };
  const cs = grab(chainData.callExpDateMap);
  const ps = grab(chainData.putExpDateMap);
  const med = o => { const v = Object.values(o).sort((x, y) => x - y); return v.length ? v[(v.length - 1) >> 1] : null; };
  const ck = Object.keys(cs).map(Number).sort((a, b) => Math.abs(a - spot) - Math.abs(b - spot));
  const atmK = ck.length ? ck[0] : null;
  let fly30 = null;
  if (atmK != null) {
    const near = t => {
      let best = null;
      for (const k of ck) if (best == null || Math.abs(k - t) < Math.abs(best - t)) best = k;
      return (best != null && Math.abs(best - t) <= 10) ? cs[best] : null;
    };
    const sl = near(atmK - 30), sc = cs[atmK], sh = near(atmK + 30);
    if (sl != null && sc != null && sh != null) fly30 = +(((sl + 2 * sc + sh) / 2).toFixed(2));
  }
  return { medC: med(cs), medP: med(ps), atmC: atmK != null ? cs[atmK] : null, fly30,
           n: Object.keys(cs).length + Object.keys(ps).length };
}

function calculateGEX(chainData, spot, onlyNearest = false) {
  const R = 0.043, Q = 0.013, MULT = 100;

  function normPdf(x) { return Math.exp(-0.5 * x * x) / Math.sqrt(2 * Math.PI); }

  function bsGamma(S, K, T, sigma) {
    if (T <= 0 || sigma <= 0 || S <= 0) return 0;
    const d1 = (Math.log(S / K) + (R - Q + 0.5 * sigma * sigma) * T) / (sigma * Math.sqrt(T));
    return normPdf(d1) * Math.exp(-Q * T) / (S * sigma * Math.sqrt(T));
  }

  // ── VANNA / CHARM greeks (information-only, same R/Q/MULT/normPdf/d1 convention as bsGamma) ──
  // Vanna = dDelta/dSigma = dVega/dSpot. Per 1.00 (=100 vol-pt) sigma move.
  function bsVanna(S, K, T, sigma) {
    if (T <= 0 || sigma <= 0 || S <= 0) return 0;
    const sqrtT = Math.sqrt(T);
    const d1 = (Math.log(S / K) + (R - Q + 0.5 * sigma * sigma) * T) / (sigma * sqrtT);
    const d2 = d1 - sigma * sqrtT;
    return -Math.exp(-Q * T) * normPdf(d1) * d2 / sigma;
  }

  // normCdf — Abramowitz-Stegun 7.1.26, needed for the charm carry-drift term.
  function normCdf(x) {
    const t = 1 / (1 + 0.2316419 * Math.abs(x));
    const d = 0.3989422804014327 * Math.exp(-0.5 * x * x); // = N'(x)
    const p = d * t * (0.319381530 + t * (-0.356563782 + t * (1.781477937 +
            t * (-1.821255978 + t * 1.330274429))));
    return x >= 0 ? 1 - p : p;
  }

  // helper: carry-drift term = (2(R-Q)T - d2*sigma*sqrtT) / (2 T sigma sqrtT)
  function N_carryDriftHelper(d1, d2, sigma, sqrtT, T) {
    return (2 * (R - Q) * T - d2 * sigma * sqrtT) / (2 * T * sigma * sqrtT);
  }

  // Charm = -dDelta/dT (delta change as time PASSES). isCall picks the carry term.
  // Per 1 YEAR of T; the CEX scaler converts to 1 calendar day.
  function bsCharm(S, K, T, sigma, isCall) {
    if (T <= 0 || sigma <= 0 || S <= 0) return 0;
    const sqrtT = Math.sqrt(T);
    const d1 = (Math.log(S / K) + (R - Q + 0.5 * sigma * sigma) * T) / (sigma * sqrtT);
    const d2 = d1 - sigma * sqrtT;
    const Nd1 = isCall ? normCdf(d1) : (normCdf(d1) - 1);
    const inner = N_carryDriftHelper(d1, d2, sigma, sqrtT, T);
    const dDelta_dT = -Q * Math.exp(-Q * T) * Nd1
                    + Math.exp(-Q * T) * normPdf(d1) * inner;
    return -dDelta_dT;
  }

  // Greek-exposure unit scalers (mirror the gex `* S*S*MULT*0.01` base).
  const VOL_PT   = 0.01;      // 1 implied-vol POINT = 0.01 in sigma
  const DAY_FRAC = 1 / 365;   // 1 calendar day as fraction of a year

  const callMap = chainData.callExpDateMap || {};
  const putMap = chainData.putExpDateMap || {};
  const S = spot;

  // Aggregate expirations: all (default) or only nearest (0DTE mode)
  const allExpiries = [...new Set([...Object.keys(callMap), ...Object.keys(putMap)])].sort();
  if (allExpiries.length === 0) return null;
  const expiriesToUse = onlyNearest ? [allExpiries[0]] : allExpiries;

  // Compute real hours remaining until today's 16:00 ET close (for 0DTE T calc)
  // Falls back to a safe minimum of 15 minutes (1/(365*96)) near / after close.
  function zeroDteT() {
    const etNow = toET(new Date());
    const hrsLeft = (16 - etNow.getHours()) - (etNow.getMinutes() / 60) - (etNow.getSeconds() / 3600);
    const safeHrs = Math.max(hrsLeft, 0.25); // 15 min floor
    return safeHrs / (365 * 24);
  }

  // Accumulate per-strike across selected expirations
  const strikeAccum = {}; // strike → { callGex, putGex, callOI, putOI, callVex, putVex, callCex, putCex }
  // Per-(expiry, strike) net GEX for the term-structure heatmap (2026-07-25).
  // Same loop, no extra chain work: expGridAccum[expKey][strike] = net $ gamma.
  const expGridAccum = {};
  let totalCallGex = 0, totalPutGex = 0;
  // INFO-ONLY flow: total traded VOLUME (not OI) across all selected expirations.
  // Volume is the day's flow → summed unconditionally (even when oi===0), not gated on OI.
  let totalCallVol = 0, totalPutVol = 0;
  let nearestDte = Infinity, nearestT = 0;
  // Per-contract gamma inputs (OI>0 rows that fed gex) — reused for the spot-shifted curve.
  const gammaInputs = [];
  // Per-contract traded-volume + quote snapshot (vol>0 rows) → the caller classifies
  // trade-side (buyers lifting the ask vs sellers hitting the bid). Internal only.
  const flowContracts = [];
  // Trade-side from last vs the spread: +1 customer bought (lifted ask), −1 sold (hit bid), 0 neutral.
  // Used for the flow/volume-based GEX (today's book) vs the OI-based GEX (standing inventory).
  const sideSign = (last, bid, ask) => {
    const l = (typeof last === 'number') ? last : null;
    const b = (typeof bid === 'number') ? bid : null;
    const a = (typeof ask === 'number') ? ask : null;
    if (l == null) return 0;
    if (a != null && a > 0 && l >= a) return 1;
    if (b != null && b > 0 && l <= b) return -1;
    const mid = (b != null && a != null && a >= b) ? (b + a) / 2 : null;
    if (mid != null) { if (l > mid) return 1; if (l < mid) return -1; }
    return 0;
  };

  for (const expKey of expiriesToUse) {
    const dteParts = expKey.split(':');
    const dte = parseInt(dteParts[1]) || 0;
    if (dte < nearestDte) nearestDte = dte;
    // 0DTE uses real hours-to-close; longer-dated uses calendar dte/365
    const T_years = dte === 0 ? zeroDteT() : Math.max(dte / 365, 1 / (365 * 24));
    if (dte === nearestDte) nearestT = T_years;   // nearest-expiry T → charm-per-day (Fix 5)

    const calls = callMap[expKey] || {};
    const puts = putMap[expKey] || {};
    const strikeSet = new Set([...Object.keys(calls), ...Object.keys(puts)]);

    for (const strikeStr of strikeSet) {
      const K = parseFloat(strikeStr);
      if (isNaN(K)) continue;

      if (!strikeAccum[strikeStr]) strikeAccum[strikeStr] = { strike: K, callGex: 0, putGex: 0, callOI: 0, putOI: 0, callVex: 0, putVex: 0, callCex: 0, putCex: 0, callGexVol: 0, putGexVol: 0 };
      const acc = strikeAccum[strikeStr];

      if (!expGridAccum[expKey]) expGridAccum[expKey] = {};
      const eg = expGridAccum[expKey];
      if (eg[strikeStr] == null) eg[strikeStr] = 0;
      const gexBefore = acc.callGex + acc.putGex;

      const callContracts = calls[strikeStr] || [];
      const putContracts = puts[strikeStr] || [];

      for (const c of callContracts) {
        const oi = Math.max(c.openInterest || 0, 0);
        const iv = (c.volatility || 0) / 100;
        // INFO-ONLY flow: sum traded volume for ALL contracts (count it even when oi===0).
        const cVol = Math.max(c.totalVolume || 0, 0);
        totalCallVol += cVol;
        if (cVol > 0) {
          const lastC = (typeof c.last === 'number') ? c.last : (typeof c.mark === 'number' ? c.mark : null);
          flowContracts.push({ k: expKey + '|' + strikeStr + '|C', v: cVol, l: lastC,
            b: (typeof c.bid === 'number' ? c.bid : null), a: (typeof c.ask === 'number' ? c.ask : null) });
          // Flow GEX (today's book): dealer takes the OTHER side of customer flow → −side.
          const cside = sideSign(lastC, c.bid, c.ask);
          if (cside !== 0) acc.callGexVol += bsGamma(S, K, T_years, iv > 0 ? iv : 0.2) * (-cside * cVol) * S * S * MULT * 0.01;
        }
        // Standard GEX: use open interest only (volume is turnover, not dealer inventory)
        if (oi === 0) continue;
        acc.callOI += oi;
        const sig = iv > 0 ? iv : 0.2;
        const gamma = bsGamma(S, K, T_years, sig);
        const gex = gamma * oi * S * S * MULT * 0.01;
        acc.callGex += gex;
        totalCallGex += gex;
        // VEX (per +1 vol-pt) / CEX (delta drift over REMAINING life to expiry) — same base & dealer sign as gex.
        // CEX uses × T_years (not per-day): bounded + interpretable, avoids the 0DTE per-day 1/T charm blow-up.
        const vex = bsVanna(S, K, T_years, sig) * oi * S * S * MULT * 0.01 * VOL_PT;
        acc.callVex += vex;
        const cex = bsCharm(S, K, T_years, sig, true) * oi * S * S * MULT * 0.01 * T_years;
        acc.callCex += cex;
        gammaInputs.push({ K, T: T_years, iv: sig, oi, isCall: true });
      }

      for (const p of putContracts) {
        const oi = Math.max(p.openInterest || 0, 0);
        const iv = (p.volatility || 0) / 100;
        // INFO-ONLY flow: sum traded volume for ALL contracts (count it even when oi===0).
        const pVol = Math.max(p.totalVolume || 0, 0);
        totalPutVol += pVol;
        if (pVol > 0) {
          const lastP = (typeof p.last === 'number') ? p.last : (typeof p.mark === 'number' ? p.mark : null);
          flowContracts.push({ k: expKey + '|' + strikeStr + '|P', v: pVol, l: lastP,
            b: (typeof p.bid === 'number' ? p.bid : null), a: (typeof p.ask === 'number' ? p.ask : null) });
          // Flow GEX: same dealer-side rule for puts (sign comes from flow, not option type).
          const pside = sideSign(lastP, p.bid, p.ask);
          if (pside !== 0) acc.putGexVol += bsGamma(S, K, T_years, iv > 0 ? iv : 0.2) * (-pside * pVol) * S * S * MULT * 0.01;
        }
        if (oi === 0) continue;
        acc.putOI += oi;
        const sig = iv > 0 ? iv : 0.2;
        const gamma = bsGamma(S, K, T_years, sig);
        const gex = gamma * oi * S * S * MULT * 0.01;
        acc.putGex -= gex; // puts negative
        totalPutGex -= gex;
        // Dealer short puts → subtract
        const vexP = bsVanna(S, K, T_years, sig) * oi * S * S * MULT * 0.01 * VOL_PT;
        acc.putVex -= vexP;
        const cexP = bsCharm(S, K, T_years, sig, false) * oi * S * S * MULT * 0.01 * T_years;
        acc.putCex -= cexP;
        gammaInputs.push({ K, T: T_years, iv: sig, oi, isCall: false });
      }
      eg[strikeStr] += (acc.callGex + acc.putGex) - gexBefore;   // this expiry's contribution
    }
  }

  // Filter strikes to ±8% from spot (widened 2026-06-21 from ±5% to include far walls)
  const rangePct = 0.08;
  const lo = S * (1 - rangePct), hi = S * (1 + rangePct);

  const strikeResults = Object.values(strikeAccum)
    .map(s => ({ ...s, netGex: s.callGex + s.putGex, netVex: s.callVex + s.putVex, netCex: s.callCex + s.putCex, netGexVol: s.callGexVol + s.putGexVol }))
    .filter(s => (s.callOI > 0 || s.putOI > 0 || s.callGexVol !== 0 || s.putGexVol !== 0) && s.strike >= lo && s.strike <= hi)
    .sort((a, b) => a.strike - b.strike);

  // Structural OI metrics (flip, walls, max-gamma) iterate OI-bearing strikes ONLY — vol-only
  // strikes have netGex 0 and must not shift the flip-crossing bracket or pad the walls list.
  const oiStrikes = strikeResults.filter(s => s.callOI > 0 || s.putOI > 0);

  const dte = nearestDte;

  // Compute totalGex from filtered strikes only (matches what the chart displays)
  let totalCallGexFiltered = 0, totalPutGexFiltered = 0;
  let totalVex = 0, totalCex = 0; // net VEX / CEX over the same filtered strikes as gex
  let totalGexVol = 0;            // net flow-based (signed-volume) GEX over the band — today's book
  for (const r of strikeResults) {
    totalCallGexFiltered += r.callGex;
    totalPutGexFiltered += r.putGex;
    totalVex += r.netVex;
    totalCex += r.netCex;
    totalGexVol += r.netGexVol;
  }
  const totalGex = totalCallGexFiltered + totalPutGexFiltered;

  // Max positive gamma strike
  let maxPosStrike = null, maxPosGex = 0;
  let maxNegStrike = null, maxNegGex = 0;
  for (const r of oiStrikes) {
    if (r.netGex > maxPosGex) { maxPosStrike = r.strike; maxPosGex = r.netGex; }
    if (r.netGex < maxNegGex) { maxNegStrike = r.strike; maxNegGex = r.netGex; }
  }

  // GEX flip: cumulative net_gex zero crossing nearest to spot.
  // Significance gate (2026-06-11, user caught flip 6972 with spot 7293):
  // on a one-sided day the cumulative can graze zero at the band edge off a
  // noise pocket (+62M vs −48B that day) — a technically-true crossing with
  // no meaning. A real flip requires the cumulative to hold REAL mass on
  // BOTH sides (≥2% of |total|); otherwise flip = null ("no flip in range").
  let flipStrike = null;
  {
    const crossings = [];
    let cumGex = 0, maxCum = -Infinity, minCum = Infinity;
    let minAbsCum = Infinity, minAbsCumStrike = null; // fallback: closest to zero
    for (let i = 0; i < oiStrikes.length; i++) {
      const prevCum = cumGex;
      cumGex += oiStrikes[i].netGex;
      if (cumGex > maxCum) maxCum = cumGex;
      if (cumGex < minCum) minCum = cumGex;
      if (i > 0 && ((prevCum < 0 && cumGex >= 0) || (prevCum > 0 && cumGex <= 0))) {
        const s0 = oiStrikes[i - 1].strike;
        const s1 = oiStrikes[i].strike;
        const ratio = Math.abs(prevCum) / (Math.abs(prevCum) + Math.abs(cumGex));
        crossings.push(Math.round(s0 + ratio * (s1 - s0)));
      }
      if (Math.abs(cumGex) < minAbsCum) {
        minAbsCum = Math.abs(cumGex);
        minAbsCumStrike = oiStrikes[i].strike;
      }
    }
    const sig = Math.abs(totalGex) * 0.02;
    const twoSided = maxCum > sig && minCum < -sig;
    if (twoSided && crossings.length > 0) {
      // Pick the crossing nearest to spot price
      crossings.sort((a, b) => Math.abs(a - S) - Math.abs(b - S));
      flipStrike = crossings[0];
    } else if (twoSided && minAbsCumStrike !== null) {
      // No true zero crossing — use the strike where cumulative is closest to zero
      flipStrike = minAbsCumStrike;
    }
    // else: book is one-sided across the whole band → no meaningful flip
  }

  // Top 10 walls by absolute net GEX
  const walls = [...oiStrikes]
    .sort((a, b) => Math.abs(b.netGex) - Math.abs(a.netGex))
    .slice(0, 10)
    .map(w => ({
      strike: w.strike,
      callGex: Math.round(w.callGex),
      putGex: Math.round(w.putGex),
      netGex: Math.round(w.netGex),
      callOI: w.callOI,
      putOI: w.putOI,
      direction: w.netGex >= 0 ? 'stabilizing' : 'amplifying',
    }));

  // ── Spot-shifted gamma curve: net dealer GEX if spot were S', book (IV/OI/T) fixed.
  //    ±rangePct in 25 steps. Re-evaluates bsGamma at each simulated spot.
  //    Emitted as [{shift, gamma}] where shift = S'−spot, gamma = netGex at S'
  //    (UI plots x = data.spot + shift, y = gamma).
  const gammaCurve = [];
  {
    const steps = 25;
    for (let i = 0; i < steps; i++) {
      const frac = -rangePct + (2 * rangePct) * (i / (steps - 1));
      const Sp = S * (1 + frac);
      let netGex = 0;
      for (const g of gammaInputs) {
        let gex = bsGamma(Sp, g.K, g.T, g.iv) * g.oi * Sp * Sp * MULT * 0.01;
        if (!g.isCall) gex = -gex; // dealer short puts → negative
        netGex += gex;
      }
      gammaCurve.push({ shift: parseFloat((Sp - S).toFixed(2)), gamma: Math.round(netGex) });
    }
  }

  const regime = totalGex > 0 ? 'PIN' : 'BREAKOUT';

  // Actual strike coverage of the chain we fetched (OI-bearing strikes only). Report what
  // we can SEE, not a nominal band: on the old master-chain path the real window was
  // ~±2.7% while the UI claimed ±8% (Fix 1, 2026-06-30).
  const _covK = oiStrikes.map(s => s.strike);
  const coverageLo = _covK.length ? Math.min(..._covK) : null;
  const coverageHi = _covK.length ? Math.max(..._covK) : null;
  const coveragePct = (coverageLo != null && S > 0)
    ? parseFloat((100 * Math.max(S - coverageLo, coverageHi - S) / S).toFixed(1)) : null;

  return {
    timestamp: Math.floor(Date.now() / 1000),
    spot: parseFloat(S.toFixed(2)),
    regime,
    totalGex: Math.round(totalGex),
    totalCallGex: Math.round(totalCallGexFiltered),
    totalPutGex: Math.round(totalPutGexFiltered),
    totalGexVol: Math.round(totalGexVol),   // flow-based (signed-volume) net GEX — today's 0DTE book
    flipStrike,
    maxPosStrike,
    maxPosGex: Math.round(maxPosGex),
    maxNegStrike,
    maxNegGex: Math.round(maxNegGex),
    // Information-only greek scalars (net dealer exposure over the filtered band)
    vanna: Math.round(totalVex),
    charm: Math.round(totalCex),
    // Information-only traded-volume flow (all expirations, all contracts w/ volume)
    callVol: totalCallVol,
    putVol: totalPutVol,
    pcRatio: totalCallVol > 0 ? +(totalPutVol / totalCallVol).toFixed(2) : null,
    _flowContracts: flowContracts, // INTERNAL: stripped by caller before persist (trade-side classification)
    gammaCurve,
    windowPct: coveragePct != null
      ? Math.min(parseFloat((rangePct * 100).toFixed(2)), coveragePct)
      : parseFloat((rangePct * 100).toFixed(2)),
    coverageLo, coverageHi, coveragePct,
    tYears: nearestT,   // nearest-expiry T (years) — charm-per-day normalization (Fix 5)
    pctChange1m: null, // filled in by history comparison
    pctChange5m: null,
    walls,
    strikes: strikeResults.map(s => ({
      strike: s.strike,
      netGex: Math.round(s.netGex),
      callGex: Math.round(s.callGex),
      putGex: Math.round(s.putGex),
      cex: Math.round(s.netCex),   // per-strike charm exposure (time-decay hedging pressure)
      netGexVol: Math.round(s.netGexVol),   // per-strike flow GEX (signed volume — today's book)
      oiC: s.callOI, oiP: s.putOI, // raw contracts — the stable positioning map (2026-07-30)
    })),
    events: [],
    updatedAt: new Date().toISOString(),
    expiry: expiriesToUse[0],
    expiryCount: expiriesToUse.length,
    dte,
    expGrid: (() => {
      // Compact term-structure grid for the heatmap: nearest 8 expiries ×
      // strikes within ±2.5% of spot, values in $M (1dp). Small enough to ride
      // in KV next to the snapshot; the page renders it directly.
      try {
        const exps = expiriesToUse.slice(0, 8).map(k => {
          const parts = String(k).split(':');
          return { key: k, d: parts[0], dte: parseInt(parts[1], 10) };
        }).sort((a, b) => a.dte - b.dte);
        const gLo = S * 0.975, gHi = S * 1.025;
        const ks = [...new Set(exps.flatMap(e => Object.keys(expGridAccum[e.key] || {})))]
          .map(parseFloat).filter(k => k >= gLo && k <= gHi).sort((a, b) => b - a);
        if (!ks.length || !exps.length) return null;
        return {
          exps: exps.map(e => ({ d: e.d, dte: e.dte })),
          rows: ks.map(k => ({ k, v: exps.map(e => {
            const v = (expGridAccum[e.key] || {})[String(k)] ?? (expGridAccum[e.key] || {})[k.toFixed(1)];
            return v == null ? null : +(v / 1e6).toFixed(1);
          }) })),
        };
      } catch (_) { return null; }
    })(),
  };
}

// ── Combined SPX+SPY snapshot (2026-07-26) ───────────────────────────────────
// Produces a FULL gex-payload-shaped object so every chain-derived panel on the
// page (cards, level map, profile, gamma curve, charm-by-strike, walls) can be
// rendered for the merged book with no per-panel special-casing.
//
// Values add directly: dollar-GEX is gamma×OI×S²×100×0.01, and a SPY contract's
// ~10× gamma against a ~100× smaller S² makes it exactly 1/10 of an SPX
// contract — its true notional ratio. Only the STRIKE AXIS needs translating,
// by the LIVE spotSPX/spotSPY ratio (~10.03, drifts with accrued dividends);
// a naive ×10 misplaces every SPY wall by ~5 SPX strikes.
//
// Not merged (SPY has no capture for them): flowSeries / signed-flow and the
// charm-into-the-close series stay SPX — the page labels those panels.
function buildCombinedGex(X, Y) {
  if (!X || !Y || !X.spot || !Y.spot) return null;
  const ratio = X.spot / Y.spot;
  const snap5 = v => Math.round(v / 5) * 5;
  const acc = {};
  const put = (k, s, sign = 1) => {
    if (!acc[k]) acc[k] = { strike: k, netGex: 0, callGex: 0, putGex: 0, cex: 0, netGexVol: 0 };
    acc[k].netGex += (s.netGex || 0) * sign;
    acc[k].callGex += (s.callGex || 0) * sign;
    acc[k].putGex += (s.putGex || 0) * sign;
    acc[k].cex += (s.cex || 0) * sign;
    acc[k].netGexVol += (s.netGexVol || 0) * sign;
  };
  for (const s of (X.strikes || [])) put(s.strike, s);
  for (const s of (Y.strikes || [])) put(snap5(s.strike * ratio), s);
  const strikes = Object.values(acc).sort((a, b) => a.strike - b.strike)
    .map(s => ({ strike: s.strike, netGex: Math.round(s.netGex), callGex: Math.round(s.callGex),
                 putGex: Math.round(s.putGex), cex: Math.round(s.cex), netGexVol: Math.round(s.netGexVol) }));
  // walls = top 10 by |netGex|, same convention as the single-book payload
  const walls = [...strikes].filter(s => s.netGex)
    .sort((a, b) => Math.abs(b.netGex) - Math.abs(a.netGex)).slice(0, 10)
    .map(s => ({ strike: s.strike, callGex: s.callGex, putGex: s.putGex, netGex: s.netGex,
                 callOI: null, putOI: null, direction: s.netGex >= 0 ? 'stabilizing' : 'amplifying' }));
  // flip = first strike where the running cumulative crosses zero
  let cum = 0, flipStrike = null, prevK = null, prevCum = 0;
  for (const s of strikes) {
    cum += s.netGex;
    if (prevK != null && ((prevCum <= 0 && cum > 0) || (prevCum >= 0 && cum < 0))) { flipStrike = s.strike; break; }
    prevK = s.strike; prevCum = cum;
  }
  const pos = strikes.filter(s => s.netGex > 0), neg = strikes.filter(s => s.netGex < 0);
  const maxPos = pos.length ? pos.reduce((a, b) => b.netGex > a.netGex ? b : a) : null;
  const maxNeg = neg.length ? neg.reduce((a, b) => b.netGex < a.netGex ? b : a) : null;
  // gamma curve: both are "total $ gamma at simulated spot"; translate SPY's
  // shift axis into SPX points and add via linear interpolation onto X's grid.
  let gammaCurve = X.gammaCurve || [];
  if (Array.isArray(Y.gammaCurve) && Y.gammaCurve.length > 1 && gammaCurve.length) {
    const ys = Y.gammaCurve.map(p => ({ x: p.shift * ratio, y: p.gamma }));
    const interp = x => {
      if (x <= ys[0].x || x >= ys[ys.length - 1].x) return 0;
      for (let i = 1; i < ys.length; i++) {
        if (x <= ys[i].x) {
          const a = ys[i - 1], b = ys[i];
          const t = (x - a.x) / ((b.x - a.x) || 1);
          return a.y + t * (b.y - a.y);
        }
      }
      return 0;
    };
    gammaCurve = gammaCurve.map(p => ({ shift: p.shift, gamma: Math.round(p.gamma + interp(p.shift)) }));
  }
  const totalGex = (X.totalGex || 0) + (Y.totalGex || 0);
  return {
    ...X,                              // inherit timestamps, coverage, dte, etc.
    symbol: 'both', ratio: +ratio.toFixed(4), spotSpy: Y.spot,
    spot: X.spot, strikes, walls, gammaCurve,
    flipStrike, maxPosStrike: maxPos?.strike ?? null, maxPosGex: maxPos?.netGex ?? null,
    maxNegStrike: maxNeg?.strike ?? null, maxNegGex: maxNeg?.netGex ?? null,
    totalGex, totalCallGex: (X.totalCallGex || 0) + (Y.totalCallGex || 0),
    totalPutGex: (X.totalPutGex || 0) + (Y.totalPutGex || 0),
    totalGexVol: (X.totalGexVol || 0) + (Y.totalGexVol || 0),
    vanna: (X.vanna || 0) + (Y.vanna || 0), charm: (X.charm || 0) + (Y.charm || 0),
    regime: totalGex > 0 ? 'PIN' : 'BREAKOUT',
    callVol: (X.callVol || 0) + (Y.callVol || 0), putVol: (X.putVol || 0) + (Y.putVol || 0),
    pctChange1m: null, pctChange5m: null,
    flowSeries: mergeFlowSeries(X.flowSeries || [], Y.flowSeries || [], ratio),
    combinedNote: 'SPY strikes translated by live SPX/SPY ratio; SPY contract counts converted to SPX-equivalent (÷ratio) before summing, premium and charm summed directly',
  };
}

// Merge two intraday flow series onto a common 1-minute time grid.
// Premium ($) and charm ($) are already comparable and add directly. Contract
// COUNTS are not: a SPY contract is ~1/ratio of an SPX contract's notional, so
// SPY counts are converted to SPX-equivalent before summing — otherwise SPY's
// much larger contract volume would dwarf SPX in a "contracts" chart while
// representing a tenth of the money.
function mergeFlowSeries(A, B, ratio) {
  if (!A.length) return B.length ? B : [];
  if (!B.length) return A;
  const bucket = ts => Math.floor(ts / 60) * 60;
  const out = new Map();
  const addRow = (r, scale) => {
    const k = bucket(r.ts);
    const cur = out.get(k) || { ts: k, cv: 0, pv: 0, ch: 0, cb: 0, cs: 0, pb: 0, ps: 0, cn: 0, pn: 0,
                                cbP: 0, csP: 0, pbP: 0, psP: 0, spot: null };
    for (const f of ['cv', 'pv', 'cb', 'cs', 'pb', 'ps', 'cn', 'pn']) cur[f] += (r[f] || 0) / scale;
    for (const f of ['ch', 'cbP', 'csP', 'pbP', 'psP']) cur[f] += (r[f] || 0);
    if (cur.spot == null && scale === 1) cur.spot = r.spot;      // price line stays SPX
    out.set(k, cur);
  };
  for (const r of A) addRow(r, 1);
  for (const r of B) addRow(r, ratio || 10);
  return [...out.values()].sort((a, b) => a.ts - b.ts).map(r => ({
    ...r,
    cv: Math.round(r.cv), pv: Math.round(r.pv), ch: Math.round(r.ch),
    cb: Math.round(r.cb), cs: Math.round(r.cs), pb: Math.round(r.pb), ps: Math.round(r.ps),
    cn: Math.round(r.cn), pn: Math.round(r.pn),
  }));
}

// ── SPY GEX (2026-07-26, display-only) ───────────────────────────────────────
// SPY has its own dealer book and its own daily expirations, so the heatmap is
// worth showing for it. Reuses calculateGEX unchanged (same $ multiplier, same
// per-expiry grid). Deliberately NOT part of any research or signal path — the
// standing rule is SPX-only for backtests; SPY is never a proxy here.
// Throttled to one chain fetch every ~10 min (2 Schwab calls) to stay light,
// and captures the same 30-min intraday slots as SPX.
async function handleSpyGexUpdate(env, token, opts = {}) {
  const baseParams = 'symbol=SPY&strikeCount=120&includeUnderlyingQuote=true&strategy=SINGLE';
  const [callData, putData] = await Promise.all([
    fetchSchwabJSON(`https://api.schwabapi.com/marketdata/v1/chains?${baseParams}&contractType=CALL`, token),
    fetchSchwabJSON(`https://api.schwabapi.com/marketdata/v1/chains?${baseParams}&contractType=PUT`, token),
  ]);
  const chainData = { callExpDateMap: callData.callExpDateMap || {}, putExpDateMap: putData.putExpDateMap || {} };
  const u = callData.underlying || putData.underlying || {};
  const spot = u.last ?? u.mark ?? u.closePrice ?? null;
  if (!spot) return { spy: 'no-spot' };
  const g = calculateGEX(chainData, spot, false);
  if (!g) return { spy: 'no-expirations' };
  const g0 = calculateGEX(chainData, spot, true);        // 0DTE-only — for per-day charm
  const flowContracts = Array.isArray(g._flowContracts) ? g._flowContracts : [];
  delete g._flowContracts;
  if (g0) delete g0._flowContracts;
  const degenerate = !g.totalGex && !(g.walls || []).length &&
                     !(g.strikes || []).some(s => s.callGex || s.putGex);
  const summary = { spy: degenerate ? 'skipped-degenerate' : 'updated', spot,
                    strikes: (g.strikes || []).length, totalGex: g.totalGex,
                    expiries: g.expGrid ? g.expGrid.exps.length : 0 };
  if (degenerate) return summary;          // same guard as SPX: never store zeros
  await env.SIGNAL_KV.put('gex_spy', JSON.stringify(g), { expirationTtl: 7 * 86400 });

  // SPY signed-flow + charm series (2026-07-26) — identical snapshot-proxy method
  // to the SPX capture (volume delta since last bar, sided by last vs bid/ask),
  // so the Net Flow / 0DTE Flow / Net Premium / Charm-into-close panels work for
  // SPY too. Same `flowSeries` field shape the page already reads.
  try {
    if (opts.flow !== false && flowContracts.length) {
      const etF = toET(new Date());
      const dayF = isoDateET(etF);
      const snapKey = `gex_spy_volsnap_${dayF}`;
      const prevRaw = await env.SIGNAL_KV.get(snapKey);
      const prevSnap = prevRaw ? JSON.parse(prevRaw) : null;
      const hadPrev = !!prevSnap;
      const newSnap = {};
      let cb = 0, cs = 0, cn = 0, pb = 0, ps = 0, pn = 0, cbP = 0, csP = 0, pbP = 0, psP = 0;
      for (const ct of flowContracts) {
        const v = Math.max(ct.v || 0, 0);
        newSnap[ct.k] = v;
        const dV = (hadPrev && (ct.k in prevSnap)) ? (v - prevSnap[ct.k]) : 0;
        if (dV <= 0) continue;
        const b = (typeof ct.b === 'number') ? ct.b : null;
        const a = (typeof ct.a === 'number') ? ct.a : null;
        const l = (typeof ct.l === 'number') ? ct.l : null;
        const mid = (b != null && a != null && a >= b) ? (b + a) / 2 : null;
        let side;
        if (l != null && a != null && a > 0 && l >= a) side = 'buy';
        else if (l != null && b != null && b > 0 && l <= b) side = 'sell';
        else if (l != null && mid != null && l > mid) side = 'buy';
        else if (l != null && mid != null && l < mid) side = 'sell';
        else side = 'neutral';
        const px = (l != null && l > 0) ? l : (mid != null && mid > 0 ? mid : 0);
        const prem = dV * px * 100;
        if (ct.k.endsWith('|C')) { if (side === 'buy') { cb += dV; cbP += prem; } else if (side === 'sell') { cs += dV; csP += prem; } else cn += dV; }
        else                     { if (side === 'buy') { pb += dV; pbP += prem; } else if (side === 'sell') { ps += dV; psP += prem; } else pn += dV; }
      }
      await env.SIGNAL_KV.put(snapKey, JSON.stringify(newSnap), { expirationTtl: 172800 });
      const flowKey = `gex_spy_flow_${dayF}`;
      const fRaw = await env.SIGNAL_KV.get(flowKey);
      let flow = fRaw ? JSON.parse(fRaw) : [];
      const ch0 = (g0 && typeof g0.charm === 'number' && g0.tYears > 0)
        ? Math.round(g0.charm / g0.tYears / 365) : null;
      flow.push({ ts: Math.floor(Date.now() / 1000), cv: g.callVol, pv: g.putVol, ch: ch0,
                  cb, cs, pb, ps, cn, pn,
                  cbP: Math.round(cbP), csP: Math.round(csP),
                  pbP: Math.round(pbP), psP: Math.round(psP), spot });
      if (flow.length > 500) flow = flow.slice(-500);
      await env.SIGNAL_KV.put(flowKey, JSON.stringify(flow), { expirationTtl: 172800 });
    }
  } catch (e) { console.warn('[gex-spy-flow]', e.message); }
  // intraday 30-min slots (±1.5% band, $M) — mirrors the SPX capture
  try {
    const etI = toET(new Date());
    const hI = etI.getHours(), mI = etI.getMinutes();
    if (opts.slots !== false && hI >= 9 && hI <= 15 && !(hI === 9 && mI < 30)) {
      const slot = `${String(hI).padStart(2, '0')}:${mI < 30 ? '00' : '30'}`;
      const dayI = isoDateET(etI);
      const key = `gex_spy_intraday_${dayI}`;
      const raw = await env.SIGNAL_KV.get(key);
      const rec = raw ? JSON.parse(raw) : { date: dayI, slots: {} };
      if (rec.slots[slot] == null) {
        const row = {};
        for (const s of g.strikes) {
          if (Math.abs(s.strike - spot) / spot > 0.015) continue;
          row[s.strike] = +(s.netGex / 1e6).toFixed(2);
        }
        if (Object.keys(row).length) {
          rec.slots[slot] = row;
          rec.spot = Math.round(spot * 100) / 100;
          await env.SIGNAL_KV.put(key, JSON.stringify(rec), { expirationTtl: 4 * 86400 });
        }
      }
    }
  } catch (e) { console.warn('[gex-spy-intraday]', e.message); }
  return summary;
}

async function handleGEXUpdate(env, token, preChain = null) {
  // 1. Use pre-fetched master chain if available (saves 2 Schwab calls per tick),
  //    else fetch our own. preChain is the same shape we'd build below.
  let chainData, spot;
  if (preChain) {
    chainData = { callExpDateMap: preChain.callExpDateMap, putExpDateMap: preChain.putExpDateMap };
    spot = preChain.spot;
  } else {
    // GEX wants a wide strike window. strikeCount=150 empirically returns ~±7.9% of spot
    // (far strikes have wider spacing) — verified from a live snapshot 2026-06-30. The real
    // Fix-1 change is that the CRON now takes THIS self-fetch path instead of the ±2.7%
    // 80-strike master chain; 150 is the proven value the page-poll path already used.
    const baseParams = 'symbol=%24SPX&strikeCount=150&includeUnderlyingQuote=true&strategy=SINGLE';
    const [callData, putData] = await Promise.all([
      fetchSchwabJSON(`https://api.schwabapi.com/marketdata/v1/chains?${baseParams}&contractType=CALL`, token),
      fetchSchwabJSON(`https://api.schwabapi.com/marketdata/v1/chains?${baseParams}&contractType=PUT`, token),
    ]);
    chainData = {
      callExpDateMap: callData.callExpDateMap || {},
      putExpDateMap: putData.putExpDateMap || {},
    };
    spot = callData.underlyingPrice || callData.underlying?.last || callData.underlying?.mark
        || putData.underlyingPrice || putData.underlying?.last || putData.underlying?.mark;
  }
  if (!spot) throw new Error('No SPX spot price in chain response');

  // Fix 3 (2026-06-30): reject a DEAD chain — every contract openInterest=0 AND
  // totalVolume=0 (e.g. a Tasty fallback). Writing it would zero gex_current AND
  // poison the gex_volsnap baseline → double-count the next real bar into
  // signed_flow_daily. A real SPX chain never has zero OI and zero vol everywhere,
  // so this guard (before ANY KV write) cannot misfire on live data.
  {
    let oiSum = 0, volSum = 0;
    for (const m of [chainData.callExpDateMap || {}, chainData.putExpDateMap || {}]) {
      for (const exp in m) for (const k in m[exp]) for (const c of (m[exp][k] || [])) {
        oiSum += (c.openInterest || 0); volSum += (c.totalVolume || 0);
      }
    }
    if (oiSum === 0 && volSum === 0) {
      console.warn('[gex] dead chain (fallback?) — skipping snapshot to protect gex_current + volsnap');
      return { gex: 'skipped', reason: 'dead-chain' };
    }
  }

  // 4. Calculate GEX — both all-expiry and 0DTE-only
  const gexData = calculateGEX(chainData, spot, false);     // all expirations
  const gex0dte = calculateGEX(chainData, spot, true);      // 0DTE only
  if (!gexData) throw new Error('GEX calculation returned null (no expirations)');

  // Dealer-liquidity snapshot (2026-07-28) — same chain pull, SPX book only.
  try { gexData.liq = computeLiquidity(chainData, spot); } catch (_) { gexData.liq = null; }

  // Pull the internal per-contract snapshot off before gexData is persisted / sent /
  // committed. Used only by the trade-side flow classifier (step 5c). 0DTE-ONLY —
  // same-day flow is the edge (0DTE OI is born + dies today → today's tape = the book);
  // falls back to all-expiry only if there is no 0DTE expiry.
  const flowContracts = Array.isArray(gex0dte?._flowContracts) ? gex0dte._flowContracts
                      : (Array.isArray(gexData._flowContracts) ? gexData._flowContracts : []);
  delete gexData._flowContracts;
  if (gex0dte) delete gex0dte._flowContracts;

  // Store 0DTE snapshot separately (its expGrid would be a single column — drop it)
  if (gex0dte) {
    delete gex0dte.expGrid;
    await env.SIGNAL_KV.put('gex_current_0dte', JSON.stringify(gex0dte));
  }

  // Intraday per-strike 0DTE GEX heatmap capture (2026-07-25): one compact slot
  // every 30 min, strikes within ±1.5% of spot, values in $M. Feeds the
  // time-of-day view of the heatmap panel so walls can be watched building and
  // decaying. First writer per slot wins (idempotent across overlapping ticks).
  try {
    if (gex0dte && Array.isArray(gex0dte.strikes) && gex0dte.strikes.length) {
      const etI = toET(new Date());
      const hI = etI.getHours(), mI = etI.getMinutes();
      if (hI >= 9 && hI <= 15 && !(hI === 9 && mI < 30)) {
        const slot = `${String(hI).padStart(2, '0')}:${mI < 30 ? '00' : '30'}`;
        const dayI = isoDateET(etI);
        const key = `gex_intraday_${dayI}`;
        const raw = await env.SIGNAL_KV.get(key);
        const rec = raw ? JSON.parse(raw) : { date: dayI, slots: {} };
        if (rec.slots[slot] == null) {
          const S0 = gex0dte.spot || 0;
          const row = {};
          for (const s of gex0dte.strikes) {
            if (!S0 || Math.abs(s.strike - S0) / S0 > 0.015) continue;
            row[s.strike] = +(s.netGex / 1e6).toFixed(1);
          }
          if (Object.keys(row).length) {
            rec.slots[slot] = row;
            rec.spot = Math.round(S0 * 100) / 100;
            await env.SIGNAL_KV.put(key, JSON.stringify(rec), { expirationTtl: 4 * 86400 });
          }
        }
        // Liquidity slot capture (2026-07-28): tiny parallel record, 30d TTL —
        // feeds the future fill-quality validation series. First writer wins.
        if (gexData.liq && gexData.liq.medC != null) {
          const lkey = `gex_liq_${dayI}`;
          const lraw = await env.SIGNAL_KV.get(lkey);
          const lrec = lraw ? JSON.parse(lraw) : { date: dayI, slots: {} };
          if (lrec.slots[slot] == null) {
            lrec.slots[slot] = { medC: gexData.liq.medC, fly30: gexData.liq.fly30 };
            await env.SIGNAL_KV.put(lkey, JSON.stringify(lrec), { expirationTtl: 30 * 86400 });
          }
        }
      }
    }
  } catch (e) { console.warn('[gex-intraday]', e.message); }

  // Store SPX price tick for live.html chart (replaces dead live_updater.py → spx_history.json)
  try {
    const etNowGex = toET(new Date());
    const todayGex = `${etNowGex.getFullYear()}-${String(etNowGex.getMonth()+1).padStart(2,'0')}-${String(etNowGex.getDate()).padStart(2,'0')}`;
    const hh = String(etNowGex.getHours()).padStart(2, '0');
    const mm = String(etNowGex.getMinutes()).padStart(2, '0');
    // Snap to 5-min intervals for consistency
    const mm5 = String(Math.floor(parseInt(mm) / 5) * 5).padStart(2, '0');
    const timeKey = `${hh}:${mm5}`;

    const spxHistKey = `spx_history_${todayGex}`;
    const spxHistRaw = await env.SIGNAL_KV.get(spxHistKey);
    const spxHist = spxHistRaw ? JSON.parse(spxHistRaw) : [];

    // Only add if this 5-min slot doesn't exist yet
    if (!spxHist.some(p => p.time === timeKey)) {
      spxHist.push({ time: timeKey, price: parseFloat(spot.toFixed(2)) });
      spxHist.sort((a, b) => a.time.localeCompare(b.time));
      await env.SIGNAL_KV.put(spxHistKey, JSON.stringify(spxHist), { expirationTtl: 86400 });
    }
  } catch (e) { console.warn('[gex] spx history tick:', e.message); }

  // 5. Load history from KV for % change tracking
  const historyRaw = await env.SIGNAL_KV.get('gex_history');
  let history = historyRaw ? JSON.parse(historyRaw) : [];

  // Calculate % changes
  const now = Date.now();
  if (history.length > 0) {
    // 1-min change: find snapshot closest to 1 min ago
    const target1m = now - 60_000;
    const snap1m = history.reduce((best, s) => {
      return Math.abs(s.ts - target1m) < Math.abs(best.ts - target1m) ? s : best;
    }, history[0]);
    if (snap1m.totalGex !== 0 && Math.abs(snap1m.ts - target1m) < 180_000) {
      gexData.pctChange1m = parseFloat(((gexData.totalGex - snap1m.totalGex) / Math.abs(snap1m.totalGex) * 100).toFixed(1));
    }

    // 5-min change
    const target5m = now - 300_000;
    const snap5m = history.reduce((best, s) => {
      return Math.abs(s.ts - target5m) < Math.abs(best.ts - target5m) ? s : best;
    }, history[0]);
    if (snap5m.totalGex !== 0 && Math.abs(snap5m.ts - target5m) < 600_000) {
      gexData.pctChange5m = parseFloat(((gexData.totalGex - snap5m.totalGex) / Math.abs(snap5m.totalGex) * 100).toFixed(1));
    }
  }

  // 5. Detect events by comparing with previous snapshot
  const events = [];
  const prevRaw = await env.SIGNAL_KV.get('gex_current');
  if (prevRaw) {
    const prev = JSON.parse(prevRaw);
    // Regime flip
    if (prev.regime && prev.regime !== gexData.regime) {
      events.push('regime_flip');
    }
    // Wall break: spot crossed a top-5 wall
    if (prev.walls && prev.spot) {
      const top5 = prev.walls.slice(0, 5);
      for (const wall of top5) {
        const crossed = (prev.spot < wall.strike && gexData.spot >= wall.strike) ||
                        (prev.spot > wall.strike && gexData.spot <= wall.strike);
        if (crossed) {
          events.push('wall_break');
          break;
        }
      }
    }
    // GEX surge: >20% change in total_gex
    if (prev.totalGex && prev.totalGex !== 0) {
      const pctChange = Math.abs((gexData.totalGex - prev.totalGex) / prev.totalGex * 100);
      if (pctChange > 20) events.push('gex_surge');
    }
  }
  gexData.events = events;

  // 5a. Append new events to persistent daily event log in KV
  if (events.length > 0) {
    try {
      const etNow = toET();
      const todayISO = `${etNow.getFullYear()}-${String(etNow.getMonth()+1).padStart(2,'0')}-${String(etNow.getDate()).padStart(2,'0')}`;
      const logKey = `gex_events_${todayISO}`;
      const logRaw = await env.SIGNAL_KV.get(logKey);
      const log = logRaw ? JSON.parse(logRaw) : [];
      const ts = gexData.updatedAt;
      for (const evt of events) {
        log.push({ type: evt, ts, spot: gexData.spot, regime: gexData.regime });
      }
      await env.SIGNAL_KV.put(logKey, JSON.stringify(log), { expirationTtl: 86400 });
    } catch (e) { console.warn('[gex] event log save failed:', e.message); }
  }

  // (5b AI commentary REMOVED — owner 2026-07-17. The generator had been dead
  // since April and the carry-forward served a 3-month-stale snapshot forever.)

  // 5c. Append today's traded-volume snapshot to a daily intraday flow series,
  //     AND classify it by trade-side. SNAPSHOT PROXY (not tick-level Lee-Ready):
  //     each contract's volume DELTA since the last snapshot is sided by its last
  //     price vs the bid/ask — buyers lifting the ask vs sellers hitting the bid.
  //     Best free, SPX-native read; accumulates a durable daily dataset
  //     (signed_flow_daily, no TTL) for later edge testing. INFO-ONLY. Wrapped so a
  //     flow-capture failure can NEVER break the GEX update.
  if (typeof gexData.callVol === 'number' && gexData.callVol >= 0 &&
      typeof gexData.putVol === 'number' && gexData.putVol >= 0) {
    try {
      const etNowFlow = toET(new Date());
      const dayISO = isoDateET(etNowFlow);
      const flowKey = `gex_flow_${dayISO}`;

      // ── Trade-side classification from per-contract volume deltas ──
      const snapKey = `gex_volsnap_${dayISO}`;          // prev cumulative volume per contract
      const prevSnapRaw = await env.SIGNAL_KV.get(snapKey);
      const prevSnap = prevSnapRaw ? JSON.parse(prevSnapRaw) : null;   // { k: cumVol } or null on first bar
      const hadPrev = !!prevSnap;
      const newSnap = {};
      let cb = 0, cs = 0, cn = 0, pb = 0, ps = 0, pn = 0; // this-bar buy/sell/neutral, calls & puts (contracts)
      let cbP = 0, csP = 0, pbP = 0, psP = 0;             // this-bar sided PREMIUM $ (dV × price × 100)
      for (const ct of flowContracts) {
        const v = Math.max(ct.v || 0, 0);
        newSnap[ct.k] = v;
        // First bar of day, OR a contract appearing for the FIRST time mid-day (e.g. when
        // the wider 250-strike chain brings in a new strike) → baseline only (dV=0), NOT its
        // full cumulative volume counted as one spike. Protects signed_flow_daily from the
        // chain-width change (Fix 1, 2026-06-30).
        let dV = (hadPrev && (ct.k in prevSnap)) ? (v - prevSnap[ct.k]) : 0;
        if (dV <= 0) continue;                               // cumulative only grows; guard resets
        const b = (typeof ct.b === 'number') ? ct.b : null;
        const a = (typeof ct.a === 'number') ? ct.a : null;
        const l = (typeof ct.l === 'number') ? ct.l : null;
        const mid = (b != null && a != null && a >= b) ? (b + a) / 2 : null;
        let side; // buyer-initiated (lifted ask) | seller-initiated (hit bid) | neutral
        if (l != null && a != null && a > 0 && l >= a) side = 'buy';
        else if (l != null && b != null && b > 0 && l <= b) side = 'sell';
        else if (l != null && mid != null && l > mid) side = 'buy';
        else if (l != null && mid != null && l < mid) side = 'sell';
        else side = 'neutral';
        const px = (l != null && l > 0) ? l : (mid != null && mid > 0 ? mid : 0);
        const prem = dV * px * 100;                          // premium $ traded this bar at this strike
        if (ct.k.endsWith('|C')) { if (side === 'buy') { cb += dV; cbP += prem; } else if (side === 'sell') { cs += dV; csP += prem; } else cn += dV; }
        else                     { if (side === 'buy') { pb += dV; pbP += prem; } else if (side === 'sell') { ps += dV; psP += prem; } else pn += dV; }
      }
      await env.SIGNAL_KV.put(snapKey, JSON.stringify(newSnap), { expirationTtl: 172800 }); // ~2 days

      // Intraday series (live chart): keep legacy cv/pv/ch, add this-bar sided deltas.
      const flowRaw = await env.SIGNAL_KV.get(flowKey);
      let flow = flowRaw ? JSON.parse(flowRaw) : [];
      // ch: 0DTE charm as $-delta drift per calendar DAY (T divided out). Raw CEX carries
      // ×T_years, so the all-expiry gexData.charm decays to ~0 into the close BY CONSTRUCTION
      // and read as fake "charm fading" — the opposite of the real story, where charm pressure
      // intensifies into the close (Fix 5, 2026-06-30). 0DTE-only + per-day now.
      const ch0dte = (gex0dte && typeof gex0dte.charm === 'number' && gex0dte.tYears > 0)
        ? Math.round(gex0dte.charm / gex0dte.tYears / 365) : null;
      flow.push({ ts: Math.floor(Date.now() / 1000), cv: gexData.callVol, pv: gexData.putVol,
                  ch: ch0dte,
                  cb, cs, pb, ps, cn, pn,       // sided deltas: call/put buy/sell/neutral this bar (contracts)
                  cbP: Math.round(cbP), csP: Math.round(csP),   // sided PREMIUM $ this bar (for the net-premium panel)
                  pbP: Math.round(pbP), psP: Math.round(psP),
                  spot: gexData.spot });        // SPX at this bar (price line on the flow panel)
      // 500 > ~390 bars/full trading day — the cap is a safety BOUND, not a window.
      // At 250 the trim kicked in ~13:40 ET and silently dropped the morning, breaking
      // every "since the open" cumulative sum + the Level Map flow arrow (Fix 2).
      if (flow.length > 500) flow = flow.slice(-500);
      await env.SIGNAL_KV.put(flowKey, JSON.stringify(flow), { expirationTtl: 172800 }); // ~2 days

      // Durable daily roll-up (the multi-week dataset for testing) — accumulate today's sided totals.
      if (hadPrev && (cb || cs || cn || pb || ps || pn)) {
        const DKEY = 'signed_flow_daily';
        const dRaw = await env.SIGNAL_KV.get(DKEY);
        let daily = dRaw ? JSON.parse(dRaw) : [];
        let row = daily.find(r => r.date === dayISO);
        if (!row) { row = { date: dayISO, cb: 0, cs: 0, cn: 0, pb: 0, ps: 0, pn: 0 }; daily.push(row); }
        row.cb += cb; row.cs += cs; row.cn += cn; row.pb += pb; row.ps += ps; row.pn += pn;
        row.spot = gexData.spot; row.regime = gexData.regime; row.ts = Math.floor(Date.now() / 1000);
        if (daily.length > 400) daily = daily.slice(-400);
        await env.SIGNAL_KV.put(DKEY, JSON.stringify(daily)); // no TTL — permanent
      }
    } catch (e) { console.warn('[gex] flow series capture failed:', e.message); }
  }

  // 6. Store current snapshot in KV — but NEVER persist a degenerate one.
  // Schwab serves the chain outside market hours with open interest stripped,
  // so every strike computes 0 (learned 2026-07-26: a weekend force-refresh
  // overwrote a good snapshot with all-zero GEX). Same shape protects against
  // a mid-session chain outage. Keep the prior snapshot instead; the page's
  // stale banner already covers "old but real". Also keep a last-known-good
  // copy so a future recovery has something to restore from.
  const _degenerate = !gexData.totalGex && !(gexData.walls || []).length &&
                      !(gexData.strikes || []).some(s => s.callGex || s.putGex);
  if (_degenerate) {
    console.warn('[gex] degenerate snapshot (no OI — market closed / chain outage) — keeping previous');
    return { gex: 'skipped-degenerate', regime: null, totalGex: 0, events: [] };
  }
  await env.SIGNAL_KV.put('gex_current', JSON.stringify(gexData));
  try { await env.SIGNAL_KV.put('gex_last_good', JSON.stringify(gexData), { expirationTtl: 14 * 86400 }); } catch (_) {}

  // 7. Append to history (keep last 60 snapshots for % change tracking)
  history.push({ ts: now, totalGex: gexData.totalGex, regime: gexData.regime });
  if (history.length > 60) history = history.slice(-60);
  await env.SIGNAL_KV.put('gex_history', JSON.stringify(history));

  // 8. (REMOVED 2026-07-03) The every-10-min gex_data.json GitHub mirror is gone.
  // Nothing consumed that file (the page reads /gex → KV), and each commit
  // triggered a GitHub Pages deploy — ~30 junk deploys/day was the main source
  // of "pages build and deployment" failure emails (deploy races + the
  // ~10-builds/hour Pages throttle). gex_daily research capture (EOD) is the
  // durable GEX record; intraday state lives in KV where it belongs.

  return { gex: 'updated', regime: gexData.regime, totalGex: gexData.totalGex, events };
}

// ── Phone MCP connector ──────────────────────────────────────────────
// Minimal MCP (JSON-RPC over streamable HTTP) so claude.ai chats on the
// owner's phone can read the live feeds + banked research. Auth = secret
// URL path segment (env.MCP_TOKEN). Read-only tools; nothing mutates.
const MCP_TOOLS = [
  { name: 'gex_now',
    description: "Live SPX GEX snapshot: spot, regime, total GEX, top walls, charm/vanna, signed options flow. DEFAULT = 0DTE-only book (what every Sigma 3 strategy trades: PNBF magnet, M8BF walls, GXBF). Pass mode:'all' for the full multi-expiry board (includes far monthly walls like the OPEX strikes). Call this first for any 'what is happening right now' question.",
    inputSchema: { type: 'object', properties: {
      mode: { type: 'string', enum: ['0dte', 'all'],
              description: "'0dte' (default) = same-day expiry book · 'all' = every expiration" } },
      additionalProperties: false } },
  { name: 'plan_today',
    description: "Today's Sigma 3 signal state: M8BF feed count + latest center/T1, PNBF status headline.",
    inputSchema: { type: 'object', properties: {}, additionalProperties: false } },
  { name: 'research_notes',
    description: 'House research context: validated verdicts with numbers, strategy recipes, the t>=4 deployment bar, and the column legend + example filters for research_days. Read this BEFORE any analysis or backtest-style answer.',
    inputSchema: { type: 'object', properties: {}, additionalProperties: false } },
  { name: 'research_days',
    description: "751-day per-day research table (CSV, 2023-06 to 2026-07): 14:00 GEX/wall geometry, M8BF center gap, every strategy's daily P/L, the 13:20 straddle/put study, calendar flags. Answer 'how did X perform on days like this' by filtering rows and computing n / win rate / mean / median yourself. Column legend lives in research_notes.",
    inputSchema: { type: 'object', properties: {}, additionalProperties: false } },
  { name: 'quote',
    description: 'Live Schwab quote for any stock or index symbol (e.g. NVDA, SPY, SPX, VIX — $ prefix for indices is added automatically). Returns last, change, bid/ask, day range, volume.',
    inputSchema: { type: 'object', properties: { symbol: { type: 'string', description: 'ticker symbol' } },
                   required: ['symbol'], additionalProperties: false } },
  { name: 'spx_today',
    description: "Today's intraday SPX tape: 5-minute price series since the open plus open/high/low/last summary. Use for 'what did SPX do since X' questions.",
    inputSchema: { type: 'object', properties: {}, additionalProperties: false } },
  { name: 'vix_now',
    description: 'Live VIX term structure via Schwab: VIX9D / VIX / VIX3M / VIX6M / VVIX / COR1M with the contango and 9D-spread readings the Tail Hedge and Diagonal use.',
    inputSchema: { type: 'object', properties: {}, additionalProperties: false } },
  { name: 'flow_today',
    description: "Today's SPX 0DTE net options-premium flow (like the Unusual Whales net-premium chart, but SPX). Cumulative net CALL premium and net PUT premium ($ = buys−sells), net volume, and where SPX is — read for 'who's aggressive today, calls or puts'. Buys = lifting the ask, sells = hitting the bid.",
    inputSchema: { type: 'object', properties: {}, additionalProperties: false } },
];

const _MCP_IDX = new Set(['SPX', 'VIX', 'VIX9D', 'VIX3M', 'VIX6M', 'VVIX', 'COR1M', 'VIXEQ', 'DJI', 'NDX', 'RUT']);
function mcpFmtQuote(sym, q) {
  if (!q) return `${sym}: no quote`;
  const pct = q.netPercentChange ?? q.netPercentChangeInDouble;
  return `${sym}: last ${q.lastPrice} (${q.netChange >= 0 ? '+' : ''}${q.netChange}${pct != null ? ` / ${pct.toFixed(2)}%` : ''})` +
    ` · bid/ask ${q.bidPrice}/${q.askPrice} · day ${q.lowPrice}-${q.highPrice} · prev close ${q.closePrice}` +
    (q.totalVolume ? ` · vol ${q.totalVolume.toLocaleString()}` : '');
}

async function mcpToolText(env, name, args) {
  if (name === 'quote') {
    const raw = String(args?.symbol || '').trim().toUpperCase().replace(/^\$/, '');
    if (!raw) return 'symbol required';
    const sym = _MCP_IDX.has(raw) ? '$' + raw : raw;
    const token = await getAccessToken(env);
    const d = await fetchSchwabJSON(
      `https://api.schwabapi.com/marketdata/v1/quotes?symbols=${encodeURIComponent(sym)}&fields=quote`, token, env);
    const q = d?.[sym]?.quote;
    return mcpFmtQuote(sym, q) + (q?.quoteTime ? `\n(quote time ${new Date(q.quoteTime).toISOString()})` : '');
  }
  if (name === 'spx_today') {
    const todayISO = isoDateET(toET(new Date()));
    const raw = await env.SIGNAL_KV.get(`spx_history_${todayISO}`);
    const ser = raw ? JSON.parse(raw) : [];
    if (!ser.length) return `no SPX ticks stored for ${todayISO} (market closed or pre-open)`;
    const px = ser.map(p => p.price);
    const head = `SPX ${todayISO} · first ${ser[0].time} ${px[0]} · last ${ser[ser.length - 1].time} ${px[px.length - 1]}` +
      ` · high ${Math.max(...px)} · low ${Math.min(...px)}`;
    const tape = ser.map(p => `${p.time} ${p.price}`).join(' · ');
    return `${head}\n5-min tape: ${tape}`;
  }
  if (name === 'flow_today') {
    const todayISO = isoDateET(toET(new Date()));
    const raw = await env.SIGNAL_KV.get(`gex_flow_${todayISO}`);
    const fs = raw ? JSON.parse(raw) : [];
    if (!fs.length) return `no SPX flow captured yet for ${todayISO} (market closed or pre-open)`;
    let ncp = 0, npp = 0, ncv = 0, npv = 0;      // cumulative net call/put premium $, net call/put volume (contracts)
    for (const t of fs) {
      ncp += (t.cbP || 0) - (t.csP || 0);
      npp += (t.pbP || 0) - (t.psP || 0);
      ncv += (t.cb || 0) - (t.cs || 0);
      npv += (t.pb || 0) - (t.ps || 0);
    }
    const last = fs[fs.length - 1];
    const $m = (v) => (v < 0 ? '-' : '+') + '$' + (Math.abs(v) / 1e6).toFixed(1) + 'M';
    const bias = ncp - npp;
    return [
      `SPX 0DTE net-premium flow · ${todayISO} · ${fs.length} bars · spot ${last.spot ?? '?'}`,
      `cumulative NET CALL premium: ${$m(ncp)}   (net call volume ${ncv >= 0 ? '+' : ''}${ncv.toLocaleString()})`,
      `cumulative NET PUT  premium: ${$m(npp)}   (net put  volume ${npv >= 0 ? '+' : ''}${npv.toLocaleString()})`,
      `bias (call − put premium): ${$m(bias)} → ${bias > 0 ? 'call-side aggressive' : bias < 0 ? 'put-side aggressive' : 'balanced'}`,
      `(premium = buys lifting the ask minus sells hitting the bid; SPX index options only, 1-min snapshots)`,
    ].join('\n');
  }
  if (name === 'vix_now') {
    const syms = ['$VIX9D', '$VIX', '$VIX3M', '$VIX6M', '$VVIX', '$COR1M'];
    const token = await getAccessToken(env);
    const d = await fetchSchwabJSON(
      `https://api.schwabapi.com/marketdata/v1/quotes?symbols=${encodeURIComponent(syms.join(','))}&fields=quote`, token, env);
    const v = {};
    for (const s of syms) v[s] = d?.[s]?.quote?.lastPrice ?? null;
    const lines = syms.map(s => `${s.slice(1)}: ${v[s] ?? 'n/a'}`);
    if (v.$VIX != null && v.$VIX3M != null) {
      lines.push(`contango (VIX3M−VIX): ${(v.$VIX3M - v.$VIX).toFixed(2)} (${v.$VIX3M > v.$VIX ? 'contango' : 'BACKWARDATION'})`);
    }
    if (v.$VIX != null && v.$VIX9D != null) {
      lines.push(`VIX−VIX9D spread: ${(v.$VIX - v.$VIX9D).toFixed(2)} (negative = short-term stress)`);
    }
    return lines.join('\n');
  }
  if (name === 'research_notes') {
    return (await env.SIGNAL_KV.get('research_notes_v1')) || 'notes not loaded';
  }
  if (name === 'research_days') {
    return (await env.SIGNAL_KV.get('research_days_v1')) || 'table not loaded';
  }
  if (name === 'gex_now') {
    const mode = (args?.mode === 'all') ? 'all' : '0dte';
    let raw = null, label = mode;
    if (mode === '0dte') {
      raw = await env.SIGNAL_KV.get('gex_current_0dte');
      if (!raw) { raw = await env.SIGNAL_KV.get('gex_current'); label = 'all (0DTE snapshot unavailable)'; }
    } else {
      raw = await env.SIGNAL_KV.get('gex_current');
    }
    if (!raw) return 'no GEX snapshot available (market closed and no cache?)';
    const g = JSON.parse(raw);
    const walls = (g.walls || []).slice(0, 8)
      .map(w => `${w.strike} ${(w.netGex / 1e9).toFixed(1)}B ${w.direction}`).join(' · ');
    const todayISO = isoDateET(toET(new Date()));
    let flowLine = 'no flow ticks yet';
    try {
      const fr = await env.SIGNAL_KV.get(`gex_flow_${todayISO}`);
      const fs = fr ? JSON.parse(fr).slice(-5) : [];
      if (fs.length) {
        const cb = fs.reduce((a, t) => a + (t.cb || 0) - (t.cs || 0), 0);
        const pb = fs.reduce((a, t) => a + (t.pb || 0) - (t.ps || 0), 0);
        flowLine = `signed flow last ${fs.length} ticks: calls net ${cb >= 0 ? '+' : ''}${cb} · puts net ${pb >= 0 ? '+' : ''}${pb} (buys minus sells)`;
      }
    } catch (_) {}
    return [
      `book: ${label === '0dte' ? '0DTE only (strategy basis)' : label} · as of ${g.updatedAt} · SPX ${g.spot} · regime ${g.regime} · total GEX ${(g.totalGex / 1e9).toFixed(1)}B`,
      `flip ${g.flipStrike ?? 'none'} · maxPos ${g.maxPosStrike} · maxNeg ${g.maxNegStrike} · charm ${(g.charm / 1e12).toFixed(2)}T · vanna ${(g.vanna / 1e12).toFixed(2)}T · P/C vol ${g.pcRatio}`,
      `top walls: ${walls}`, flowLine,
    ].join('\n');
  }
  if (name === 'plan_today') {
    const todayISO = isoDateET(toET(new Date()));
    const parts = [`${todayISO} (ET)`];
    try {
      const sRaw = await env.SIGNAL_KV.get('signals_today');
      if (sRaw) {
        const s = JSON.parse(sRaw);
        const sig = s.signals || [];
        const last = sig[sig.length - 1];
        parts.push(`M8BF feed: ${sig.length} signals${last ? ` · latest ${last.time} center ${last.center} T1 ${last.t1}${last.banned ? ' (banned strike)' : ''}` : ''}`);
      }
    } catch (_) {}
    try {
      const mRaw = await env.SIGNAL_KV.get(`mf_today_${todayISO}`);
      if (mRaw) { const m = JSON.parse(mRaw); parts.push(`PNBF: ${m.status}${m.headline ? ' — ' + m.headline : ''}`); }
    } catch (_) {}
    return parts.join('\n');
  }
  return `unknown tool ${name}`;
}

async function handleMcpMessage(env, msg) {
  if (!msg || typeof msg.method !== 'string') {
    return { jsonrpc: '2.0', id: msg?.id ?? null, error: { code: -32600, message: 'invalid request' } };
  }
  if (msg.method.startsWith('notifications/')) return null;
  const ok = (result) => ({ jsonrpc: '2.0', id: msg.id, result });
  if (msg.method === 'initialize') {
    return ok({
      protocolVersion: msg.params?.protocolVersion || '2025-03-26',
      capabilities: { tools: { listChanged: false } },
      serverInfo: { name: 'sigma3-feeds', title: 'Sigma 3 live feeds + research', version: '1.0.0' },
    });
  }
  if (msg.method === 'ping') return ok({});
  if (msg.method === 'tools/list') return ok({ tools: MCP_TOOLS });
  if (msg.method === 'tools/call') {
    try {
      const text = await mcpToolText(env, msg.params?.name, msg.params?.arguments);
      return ok({ content: [{ type: 'text', text }] });
    } catch (e) {
      return ok({ content: [{ type: 'text', text: `tool error: ${e.message}` }], isError: true });
    }
  }
  return { jsonrpc: '2.0', id: msg.id ?? null, error: { code: -32601, message: `unknown method ${msg.method}` } };
}

// Durable mirror of the gate's 10:30 GEX series (item D, 2026-07-24). KV is
// the live truth; this file is the disaster-recovery copy — if KV is ever
// lost, re-seed via POST /gexgate-seed from this file's contents.
async function mirrorGexSeriesToGitHub(env, series) {
  const ghToken = env.GITHUB_TOKEN;
  if (!ghToken) return;
  const apiUrl = 'https://api.github.com/repos/rava8989/brave/contents/gex1030_series.json';
  const headers = {
    'Authorization': `Bearer ${ghToken}`,
    'Accept': 'application/vnd.github+json',
    'User-Agent': 'schwab-proxy-worker/1.0',
    'X-GitHub-Api-Version': '2022-11-28',
  };
  let sha = null;
  try {
    const getResp = await fetch(apiUrl, { headers });
    if (getResp.ok) sha = (await getResp.json()).sha;
  } catch (_) { /* first write */ }
  const keys = Object.keys(series);
  const body = {
    message: `auto: gate series ${keys[keys.length - 1] || '?'} (${keys.length} entries)`,
    content: btoa(unescape(encodeURIComponent(JSON.stringify(series)))),
  };
  if (sha) body.sha = sha;
  const putResp = await fetch(apiUrl, { method: 'PUT', headers: { ...headers, 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
  if (!putResp.ok) throw new Error(`GitHub PUT gex1030_series.json: ${putResp.status}`);
}

async function commitGexToGitHub(env, gexData) {
  const ghToken = env.GITHUB_TOKEN;
  if (!ghToken) return;

  const apiUrl = 'https://api.github.com/repos/rava8989/brave/contents/gex_data.json';
  const headers = {
    'Authorization': `Bearer ${ghToken}`,
    'Accept': 'application/vnd.github+json',
    'User-Agent': 'schwab-proxy-worker/1.0',
    'X-GitHub-Api-Version': '2022-11-28',
  };

  // Get current file SHA (may not exist yet)
  let sha = null;
  try {
    const getResp = await fetch(apiUrl, { headers });
    if (getResp.ok) {
      const meta = await getResp.json();
      sha = meta.sha;
    }
  } catch (e) { /* file may not exist yet */ }

  const body = {
    message: `auto: GEX update ${gexData.regime} ${new Date().toISOString().slice(0, 16)}`,
    content: btoa(unescape(encodeURIComponent(JSON.stringify(gexData, null, 2)))),
  };
  if (sha) body.sha = sha;

  const putResp = await fetch(apiUrl, {
    method: 'PUT',
    headers: { ...headers, 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  if (!putResp.ok) {
    const err = await putResp.text();
    throw new Error(`GitHub PUT gex_data.json failed: ${putResp.status} — ${err.slice(0, 200)}`);
  }
}

// ════════════════════════════════════════════════════════════════════
// KV-BACKED HISTORY STORE (Item 5)
// ────────────────────────────────────────────────────────────────────
// Single source of truth: KV key `history_data` (JSON array).
// Worker writes are fast (~10ms) and atomic per key. GitHub mirror
// happens asynchronously after each KV write so git history is preserved
// but no caller waits for the 1-2s PUT.
//
// First read on a fresh deploy falls back to GitHub raw (one-time seed),
// then everything stays in KV. POST /history-migrate forces a re-seed.
// ════════════════════════════════════════════════════════════════════

const HISTORY_KV_KEY = 'history_data';
const HISTORY_GH_RAW = 'https://raw.githubusercontent.com/rava8989/brave/main/history_data.json';

async function getHistory(env) {
  // Primary read: KV (fast)
  try {
    const raw = await env.SIGNAL_KV.get(HISTORY_KV_KEY);
    if (raw) {
      const data = JSON.parse(raw);
      if (Array.isArray(data) && data.length > 0) return data;
    }
  } catch (e) {
    console.warn('[history] KV read failed, falling back to GitHub:', e.message);
  }
  // Fallback: GitHub raw (cold start, or KV was wiped). Also seeds KV.
  try {
    const ghResp = await fetch(`${HISTORY_GH_RAW}?t=${Date.now()}`,
      { headers: { 'User-Agent': 'schwab-proxy', 'Cache-Control': 'no-cache' } });
    if (!ghResp.ok) throw new Error(`GitHub raw ${ghResp.status}`);
    const data = await ghResp.json();
    if (Array.isArray(data)) {
      // Seed KV so subsequent reads are fast (do NOT await — fire-and-forget)
      try { await env.SIGNAL_KV.put(HISTORY_KV_KEY, JSON.stringify(data)); } catch (_) {}
      return data;
    }
    throw new Error('GitHub raw returned non-array');
  } catch (e) {
    console.error('[history] GitHub fallback failed:', e.message);
    return [];
  }
}

async function setHistory(env, contentArray, opts = {}) {
  // Snapshot pre-write state to KV backups index (re-uses Item 4 helper).
  if (!opts.skipBackup) {
    try {
      const prev = await getHistory(env);
      await backupHistorySnapshot(env, prev, opts.dateStr || 'kv-write', opts.fields || {});
    } catch (e) {
      console.warn('[history] backup before setHistory failed:', e.message);
    }
  }
  // Primary write: KV (atomic per key, ~10ms)
  await env.SIGNAL_KV.put(HISTORY_KV_KEY, JSON.stringify(contentArray));
}

async function mirrorHistoryToGitHub(env, contentArray, message) {
  // Async GitHub mirror — preserves git history but doesn't block writes.
  // Errors logged but never thrown (mirroring failure must not break trades).
  if (!env.GITHUB_TOKEN) return { skipped: 'no GITHUB_TOKEN' };
  const apiUrl = 'https://api.github.com/repos/rava8989/brave/contents/history_data.json';
  const ghHeaders = {
    'Authorization': `Bearer ${env.GITHUB_TOKEN}`,
    'Accept': 'application/vnd.github+json',
    'User-Agent': 'schwab-proxy-worker/1.0',
    'X-GitHub-Api-Version': '2022-11-28',
  };
  // Retry on sha conflict (409/422) + transient errors. Two near-simultaneous
  // settles (e.g. m8bf + tail at EOD) each do GET-sha→PUT; GitHub's sha lags
  // briefly so the second PUT 409s. The old code swallowed that with NO retry
  // → KV had tailPL but GitHub never did (2026-06-23 tail-PL drift). On each
  // retry we re-GET a fresh sha AND re-read the latest KV, so the GitHub copy
  // converges to current KV regardless of which mirror lands last.
  const maxAttempts = 4;
  let lastErr = null;
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      let body = contentArray;
      if (attempt > 1) await new Promise(r => setTimeout(r, 250 * (attempt - 1)));
      // P37 (2026-08-05): re-read KV on EVERY attempt, not only retries. A sha
      // conflict is not the only clobber path — a mirror that merely STARTS
      // early and lands LAST gets a legitimately fresh sha and pushes its stale
      // in-memory array cleanly (Aug 4: duplicate-EOD mirror erased stradPL +
      // bobfPL from GitHub with zero 409s). Sha freshness ≠ content freshness;
      // pushing current KV makes every mirror convergent no matter the order.
      try { const fresh = await getHistory(env); if (Array.isArray(fresh) && fresh.length) body = fresh; } catch (_) {}
      const getResp = await fetch(apiUrl, { headers: ghHeaders });
      if (!getResp.ok) throw new Error(`GH GET ${getResp.status}`);
      const meta = await getResp.json();
      const putResp = await fetch(apiUrl, {
        method: 'PUT',
        headers: { ...ghHeaders, 'Content-Type': 'application/json' },
        body: JSON.stringify({
          message: message || 'auto: KV mirror',
          content: btoa(JSON.stringify(body, null, 0)),
          sha: meta.sha,
        }),
      });
      if (putResp.status === 409 || putResp.status === 422) {
        lastErr = `GH PUT ${putResp.status} (sha conflict)`;
        continue; // sha raced — re-GET + retry
      }
      if (!putResp.ok) {
        const err = await putResp.text();
        throw new Error(`GH PUT ${putResp.status}: ${err.slice(0, 200)}`);
      }
      await recordMirrorHealth(env, true);
      return { ok: true, attempts: attempt };
    } catch (e) {
      lastErr = e.message;
      if (attempt === maxAttempts) break;
    }
  }
  console.warn('[history-mirror] failed after retries (KV state still good):', lastErr);
  await recordMirrorHealth(env, false, lastErr);
  return { ok: false, error: lastErr };
}

// ════════════════════════════════════════════════════════════════════
// GITHUB HISTORY UPSERT (legacy name — now writes KV first, mirrors to GH)
// Same merge semantics: alwaysOverwrite=['vixClose','spxClose','m8bfWR'].
// ════════════════════════════════════════════════════════════════════

// ── Recover today's spxOpen + vixOpen from Schwab 9:30 candle ──
// Used as auto-recovery when the morning signal block failed to write them
// to history (Schwab outage at 9:30, claim stuck, etc.). The first 1-min
// candle at 9:30 ET has the OPEN of the regular session — this matches TOS.
// Returns { spxOpen, vixOpen } with either field possibly null if its
// candle didn't materialize.
async function recoverOpenPricesFromSchwab(env, etNow) {
  const token = await getAccessToken(env);
  const todayDateStr = etNow.toDateString();
  const baseUrl = (sym) =>
    `https://api.schwabapi.com/marketdata/v1/pricehistory?symbol=${sym}&periodType=day&period=2&frequencyType=minute&frequency=1&needExtendedHoursData=false`;

  async function fetchOpen(symbol) {
    try {
      const hist = await fetchSchwabJSON(baseUrl(symbol), token, env);
      const cs = hist.candles || [];
      // Prefer the exact 9:30 minute bar; fall back to the first bar in
      // 9:30-9:35 if 9:30 itself is missing.
      const todayCandles = cs.filter(c => toET(new Date(c.datetime)).toDateString() === todayDateStr);
      todayCandles.sort((a, b) => a.datetime - b.datetime);
      const exact930 = todayCandles.find(c => {
        const d = toET(new Date(c.datetime));
        return d.getHours() === 9 && d.getMinutes() === 30;
      });
      const fallback = todayCandles.find(c => {
        const d = toET(new Date(c.datetime));
        return d.getHours() === 9 && d.getMinutes() >= 30 && d.getMinutes() <= 35;
      });
      const pick = exact930 || fallback;
      return pick?.open != null ? parseFloat(pick.open.toFixed(2)) : null;
    } catch (e) {
      console.warn(`[recover-open] ${symbol} fetch failed:`, e.message);
      return null;
    }
  }

  const [spxOpen, vixOpen] = await Promise.all([fetchOpen('%24SPX'), fetchOpen('%24VIX')]);
  return { spxOpen, vixOpen };
}

// ── Snapshot the PRE-WRITE history state into KV (last 10 only) ──
// Defense against bad writes: any field overwrite, deleted row, or schema
// regression can be inspected/restored from these. Each backup is its own
// KV key (cheap reads); an index key tracks them for rotation.
async function backupHistorySnapshot(env, contentJson, dateStr, fields) {
  const ts = new Date().toISOString();
  const backupKey = `history_backup_${ts}`;
  try {
    await env.SIGNAL_KV.put(backupKey, JSON.stringify(contentJson), { expirationTtl: 14 * 86400 });
    let idx = [];
    try {
      const idxRaw = await env.SIGNAL_KV.get('history_backups_index');
      idx = idxRaw ? JSON.parse(idxRaw) : [];
      if (!Array.isArray(idx)) idx = [];
    } catch { idx = []; }
    idx.unshift({ key: backupKey, ts, dateStr, fields: Object.keys(fields || {}) });
    while (idx.length > 10) {
      const drop = idx.pop();
      try { await env.SIGNAL_KV.delete(drop.key); } catch (delErr) {
        console.warn('[history-backup] rotate delete failed:', delErr.message);
      }
    }
    await env.SIGNAL_KV.put('history_backups_index', JSON.stringify(idx));
  } catch (e) {
    // Non-fatal: write proceeds even if backup fails. Don't block trading.
    console.warn('[history-backup] snapshot failed:', e.message);
  }
}

async function upsertHistoryGitHub(env, dateStr, fields, _retries = 3) {
  // 1. Read current state from KV (Item 5 — primary store)
  const content = await getHistory(env);
  if (!Array.isArray(content) || content.length === 0) {
    throw new Error('history KV empty — run POST /history-migrate to seed from GitHub first');
  }

  // 1a. Backup PRE-WRITE state to KV (Item 4)
  await backupHistorySnapshot(env, content, dateStr, fields);

  // 2. Upsert today's row (same merge semantics as before)
  const idx = content.findIndex(r => r.date === dateStr);
  if (idx >= 0) {
    for (const [k, v] of Object.entries(fields)) {
      const alwaysOverwrite = ['vixClose', 'spxClose', 'm8bfWR'].includes(k);
      if (alwaysOverwrite || content[idx][k] == null) content[idx][k] = v;
    }
  } else {
    content.push({ date: dateStr, ...fields });
    content.sort((a, b) => a.date.localeCompare(b.date));
  }

  // 3. Ensure 10 future trading day placeholders always exist (skip weekends + holidays)
  const today = dateStr;
  const lastDate = content[content.length - 1]?.date || today;
  const futureRows = content.filter(r => r.date > today).length;
  if (futureRows < 10) {
    const needed = 10 - futureRows;
    let d = new Date(lastDate + 'T12:00:00Z');
    let added = 0;
    const existingDates = new Set(content.map(r => r.date));
    while (added < needed) {
      d.setUTCDate(d.getUTCDate() + 1);
      const dow = d.getUTCDay();
      if (dow === 0 || dow === 6) continue;
      if (isHol(d)) continue;
      const iso = d.toISOString().slice(0, 10);
      if (!existingDates.has(iso)) {
        content.push({ date: iso });
        existingDates.add(iso);
        added++;
      }
    }
    content.sort((a, b) => a.date.localeCompare(b.date));
  }

  // 4. Primary write to KV — atomic, ~10ms (no merge conflicts ever)
  await setHistory(env, content, { dateStr, fields, skipBackup: true });

  // 5. Mirror to GitHub asynchronously (git history + backup; never blocks).
  //    We swallow errors here so a flaky GitHub doesn't break trading.
  if (env.GITHUB_TOKEN) {
    try {
      await mirrorHistoryToGitHub(env, content,
        `auto: history update for ${dateStr} (${Object.keys(fields).join(', ')})`);
    } catch (e) {
      console.warn('[history-mirror] non-fatal:', e.message);
    }
  }
}

// ════════════════════════════════════════════════════════════════════
// BACKFILL MISSING m8bfWR
// ════════════════════════════════════════════════════════════════════

async function fetchAllDiscordSignalsForDate(token, channelId, dateISO) {
  // Fetch all butterfly signals posted on dateISO ET, paginated
  const [y, m, d] = dateISO.split('-').map(Number);
  // 12:00-22:00 UTC covers both EDT (9:30-4 ET = 13:30-20 UTC) and EST (9:30-4 ET = 14:30-21 UTC)
  const startMs = Date.UTC(y, m - 1, d, 12, 0, 0);
  const endMs   = Date.UTC(y, m - 1, d, 22, 0, 0);
  const discordEpoch = 1420070400000n;
  let afterSnowflake = ((BigInt(startMs) - discordEpoch) << 22n).toString();
  const beforeSnowflake = ((BigInt(endMs) - discordEpoch) << 22n).toString();

  const allSignals = [];

  for (let page = 0; page < 5; page++) {
    const resp = await fetch(
      `https://discord.com/api/v9/channels/${channelId}/messages?limit=100&after=${afterSnowflake}&before=${beforeSnowflake}`,
      { headers: { 'Authorization': token, 'User-Agent': 'Mozilla/5.0' } }
    );
    if (!resp.ok) throw new Error(`Discord API ${resp.status} for ${dateISO}`);
    const batch = await resp.json();
    if (!Array.isArray(batch) || batch.length === 0) break;

    batch.sort((a, b) => a.id.localeCompare(b.id));
    for (const msg of batch) {
      // Verify message is from the correct ET date
      const msgET = toET(new Date(msg.timestamp));
      const msgDate = `${msgET.getFullYear()}-${String(msgET.getMonth()+1).padStart(2,'0')}-${String(msgET.getDate()).padStart(2,'0')}`;
      if (msgDate !== dateISO) continue;

      const sig = parseDiscordSignal(msg.content || '');
      if (!sig) continue;
      // Attach posting time (ET) for TRADES TIME column
      sig.time = `${String(msgET.getHours()).padStart(2,'0')}:${String(msgET.getMinutes()).padStart(2,'0')}`;
      allSignals.push(sig); // no dedup — each post counts
    }
    if (batch.length < 100) break;
    afterSnowflake = batch[batch.length - 1].id;
  }
  return allSignals;
}

// ── Scrape raw Discord signal CSV rows for ONE ET date ──
// Shared by appendScrapedSignals (daily) and backfillScrapedSignals (recovery)
// so the 38-column parsing lives in exactly one place.
// Raw message texts from a channel for one date (12:00–22:00 UTC window).
async function scrapeRawEarnMsgs(token, channelId, dateISO, withAttachments = false) {
  const [y, m, d] = dateISO.split('-').map(Number);
  const discordEpoch = 1420070400000n;
  let after = ((BigInt(Date.UTC(y, m - 1, d, 12, 0, 0)) - discordEpoch) << 22n).toString();
  const before = ((BigInt(Date.UTC(y, m - 1, d, 23, 0, 0)) - discordEpoch) << 22n).toString();
  const out = [];
  for (let page = 0; page < 5; page++) {
    const resp = await fetch(`https://discord.com/api/v9/channels/${channelId}/messages?limit=100&after=${after}&before=${before}`,
      { headers: { 'Authorization': token, 'User-Agent': 'Mozilla/5.0' } });
    if (!resp.ok) break;
    const batch = await resp.json();
    if (!Array.isArray(batch) || !batch.length) break;
    batch.sort((a, b) => (BigInt(a.id) < BigInt(b.id) ? -1 : 1));
    // Discord ignores `before` when `after` is present (they're mutually
    // exclusive), so enforce the day's end bound here or every scan reads
    // forward across days and misattributes messages.
    let past = false;
    for (const msg of batch) {
      if (BigInt(msg.id) >= BigInt(before)) { past = true; break; }
      if (withAttachments) out.push({ t: String(msg.content || '').slice(0, 300), att: (msg.attachments || []).map(a => a.url) });
      else out.push(String(msg.content || ''));
    }
    if (past) break;
    after = batch[batch.length - 1].id;
    if (batch.length < 100) break;
  }
  return out;
}

async function scrapeRawRowsForDate(token, channelId, dateISO) {
  const [y, m, d] = dateISO.split('-').map(Number);
  const startMs = Date.UTC(y, m - 1, d, 12, 0, 0);
  const endMs = Date.UTC(y, m - 1, d, 22, 0, 0);
  const discordEpoch = 1420070400000n;
  let afterSnowflake = ((BigInt(startMs) - discordEpoch) << 22n).toString();
  const beforeSnowflake = ((BigInt(endMs) - discordEpoch) << 22n).toString();

  const rows = [];
  for (let page = 0; page < 10; page++) {
    const resp = await fetch(
      `https://discord.com/api/v9/channels/${channelId}/messages?limit=100&after=${afterSnowflake}&before=${beforeSnowflake}`,
      { headers: { 'Authorization': token, 'User-Agent': 'Mozilla/5.0' } }
    );
    if (!resp.ok) break;
    const batch = await resp.json();
    if (!Array.isArray(batch) || batch.length === 0) break;
    batch.sort((a, b) => a.id.localeCompare(b.id));

    for (const msg of batch) {
      const content = msg.content || '';
      const msgET = toET(new Date(msg.timestamp));
      const msgDate = `${msgET.getFullYear()}-${String(msgET.getMonth()+1).padStart(2,'0')}-${String(msgET.getDate()).padStart(2,'0')}`;
      if (msgDate !== dateISO) continue;
      const msgTime = `${String(msgET.getHours()).padStart(2,'0')}:${String(msgET.getMinutes()).padStart(2,'0')}`;

      const priceM = content.match(/Price:\s*([\d.]+)/i);
      if (!priceM) continue; // not a signal message

      const field = (pat) => { const m = content.match(pat); return m ? m[1] : ''; };
      const bfStrikesM = content.match(/Butterfly[^/]*(\d{4,5})\/(\d{4,5})\/(\d{4,5})\s*(CALL|PUT)/i);
      const bfPriceM = content.match(/Butterfly[^@]*@([\d.]+)/i);
      const icStrikesM = content.match(/Iron Condor[^/]*(\d{4,5})\/(\d{4,5})\/(\d{4,5})\/(\d{4,5})/i);
      const icPriceM = content.match(/Iron Condor[^@]*@([\d.]+)/i);
      const vtStrikesM = content.match(/Vertical[^/]*(\d{4,5})\/(\d{4,5})\s*(CALL|PUT)/i);
      const vtPriceM = content.match(/Vertical[^@]*@([\d.]+)/i);

      // Sonar IC (appears after "Sonar:" label in the message)
      const sonarBlock = content.split(/Sonar:/i)[1] || '';
      const sonarStrikesM = sonarBlock.match(/Iron Condor[^/]*(\d{4,5})\/(\d{4,5})\/(\d{4,5})\/(\d{4,5})/i);
      const sonarPriceM = sonarBlock.match(/Iron Condor[^@]*@([\d.]+)/i);

      const csvLine = [
        msg.timestamp, msgDate, msgTime,
        priceM[1],
        field(/Trend:\s*(\w+)/i),
        field(/Predicted Close:\s*([\d.]+)/i),
        field(/Strength:\s*([\d.]+)/i),
        field(/Short term:\s*([\d.]+)/i),
        field(/Long term:\s*([\d.]+)/i),
        field(/Short term bias:\s*(\w+)/i),
        field(/Long term bias:\s*(\w+)/i),
        field(/Calls:\s*([\d.]+)/i),
        field(/Puts:\s*([\d.]+)/i),
        field(/Center:\s*([\d.]+)/i),
        field(/Range:\s*([\d.]+)/i),
        field(/Target 1:\s*([\d.]+)/i),
        field(/Target 2:\s*([\d.]+)/i),
        field(/Delta:\s*([\d.]+)/i),
        field(/Gamma:\s*([\d.]+)/i),
        field(/Interest:\s*([\d.]+)/i),
        field(/Sonar:\s*([\d.]+)/i),
        field(/Volume:\s*([\d.]+)/i),
        bfStrikesM ? 'BUY' : '',
        bfStrikesM ? `${bfStrikesM[1]}/${bfStrikesM[2]}/${bfStrikesM[3]}` : '',
        bfStrikesM ? bfStrikesM[2] : '',
        bfStrikesM ? bfStrikesM[1] : '',
        bfStrikesM ? bfStrikesM[3] : '',
        bfPriceM ? bfPriceM[1] : '',
        icStrikesM ? 'SELL' : '',
        icStrikesM ? `${icStrikesM[1]}/${icStrikesM[2]}/${icStrikesM[3]}/${icStrikesM[4]}` : '',
        icPriceM ? icPriceM[1] : '',
        sonarStrikesM ? 'SELL' : '',
        sonarStrikesM ? `${sonarStrikesM[1]}/${sonarStrikesM[2]}/${sonarStrikesM[3]}/${sonarStrikesM[4]}` : '',
        sonarPriceM ? sonarPriceM[1] : '',
        vtStrikesM ? 'SELL' : '',
        vtStrikesM ? `${vtStrikesM[1]}/${vtStrikesM[2]}` : '',
        vtStrikesM ? vtStrikesM[3] : '',
        vtPriceM ? vtPriceM[1] : '',
      ].map(v => String(v).includes(',') ? `"${v}"` : v).join(',');
      rows.push(csvLine);
    }
    if (batch.length < 100) break;
    afterSnowflake = batch[batch.length - 1].id;
  }
  return rows;
}

// ── GitHub read/write helpers for scraped_signals.csv ──
const SCRAPED_CSV_API = 'https://api.github.com/repos/rava8989/brave/contents/scraped_signals.csv';
function scrapedCsvHeaders(env) {
  return {
    'Authorization': `Bearer ${env.GITHUB_TOKEN}`,
    'Accept': 'application/vnd.github+json',
    'User-Agent': 'schwab-proxy-worker/1.0',
    'X-GitHub-Api-Version': '2022-11-28',
  };
}
async function fetchScrapedCsv(env) {
  const resp = await fetch(SCRAPED_CSV_API, { headers: scrapedCsvHeaders(env) });
  if (resp.status === 404) return { content: '', sha: '' };   // file absent → safe to create
  if (!resp.ok) throw new Error(`scraped csv GET ${resp.status}`);
  const meta = await resp.json();
  let content;
  if (meta.content && meta.encoding === 'base64') {
    content = atob(meta.content.replace(/\n/g, ''));
  } else {
    // Files >1MB: the Contents API returns EMPTY content (encoding 'none'). Reading
    // it as '' and appending would CLOBBER the whole file (this is exactly what wiped
    // April–June on 2026-06-29). Read the blob via the Git Data API instead — no size
    // limit, authoritative, not CDN-cached — using the sha from the metadata.
    const blob = await fetch(`https://api.github.com/repos/rava8989/brave/git/blobs/${meta.sha}`, { headers: scrapedCsvHeaders(env) });
    if (!blob.ok) throw new Error(`scraped csv blob GET ${blob.status}`);
    const bd = await blob.json();
    content = atob(bd.content.replace(/\n/g, ''));
  }
  // Final guard: a file that exists (has a sha) must never read as empty before an append.
  if (meta.sha && !content) throw new Error('scraped csv read empty — aborting to avoid clobber');
  return { content, sha: meta.sha };
}
async function putScrapedCsv(env, content, sha, message) {
  const putResp = await fetch(SCRAPED_CSV_API, {
    method: 'PUT',
    headers: scrapedCsvHeaders(env),
    body: JSON.stringify({ message, content: btoa(content), sha }),
  });
  if (!putResp.ok) throw new Error(`GitHub PUT failed: ${putResp.status}`);
}

// ── Append ONE day's full Discord signals to scraped_signals.csv on GitHub ──
// Runs in the 16:25 aux tick (its own subrequest budget). Was previously in the
// EOD invocation and silently starved on the subrequest budget after handleEOD +
// backfillWR/PL — leaving a gap from 2026-06-04 onward (lesson P17).
async function appendScrapedSignals(env, etNow) {
  const todayISO = `${etNow.getFullYear()}-${String(etNow.getMonth()+1).padStart(2,'0')}-${String(etNow.getDate()).padStart(2,'0')}`;
  const doneKey = `scrape_appended_${todayISO}`;
  if (await env.SIGNAL_KV.get(doneKey)) return { skipped: true, date: todayISO };

  const rows = await scrapeRawRowsForDate(env.DISCORD_USER_TOKEN, '1048242197029458040', todayISO);
  if (rows.length === 0) return { date: todayISO, appended: 0 };

  const { content: existingContent, sha } = await fetchScrapedCsv(env);
  const newContent = existingContent.trimEnd() + '\n' + rows.join('\n') + '\n';
  await putScrapedCsv(env, newContent, sha, `auto: append ${rows.length} scraped signals for ${todayISO}`);
  await env.SIGNAL_KV.put(doneKey, 'done', { expirationTtl: 86400 * 3 });
  return { date: todayISO, appended: rows.length };
}

// ── One-shot backfill: scrape every MISSING trading day in [fromISO, toISO]
//    and append them all in a SINGLE GitHub commit. Weekends/holidays yield
//    0 rows and are skipped automatically. Triggered via KV
//    'scrape_backfill_trigger' = "from,to". ──
async function backfillScrapedSignals(env, fromISO, toISO) {
  const { content: existingContent, sha } = await fetchScrapedCsv(env);
  const have = new Set();
  for (const line of existingContent.split('\n')) {
    const c = line.split(',');
    if (c[1] && /^\d{4}-\d{2}-\d{2}$/.test(c[1])) have.add(c[1]);
  }
  const start = new Date(`${fromISO}T12:00:00Z`).getTime();
  const end = new Date(`${toISO}T12:00:00Z`).getTime();
  const allRows = [];
  const filled = [];
  for (let t = start; t <= end; t += 86400000) {
    const dt = new Date(t);
    const iso = `${dt.getUTCFullYear()}-${String(dt.getUTCMonth()+1).padStart(2,'0')}-${String(dt.getUTCDate()).padStart(2,'0')}`;
    const dow = dt.getUTCDay();
    if (dow === 0 || dow === 6) continue;     // weekend
    if (have.has(iso)) continue;              // already archived
    const rows = await scrapeRawRowsForDate(env.DISCORD_USER_TOKEN, '1048242197029458040', iso);
    if (rows.length > 0) { allRows.push(...rows); filled.push(`${iso}:${rows.length}`); }
  }
  if (allRows.length === 0) return { backfilled: 0, dates: [] };
  const newContent = existingContent.trimEnd() + '\n' + allRows.join('\n') + '\n';
  await putScrapedCsv(env, newContent, sha, `backfill: ${allRows.length} scraped signals across ${filled.length} day(s)`);
  return { backfilled: allRows.length, dates: filled };
}

function computeWinRateFromSignals(signals, spxClose) {
  if (!signals || signals.length === 0) return null;
  let wins = 0;
  for (const sig of signals) {
    const intrinsic = Math.max(0, Math.min(spxClose - sig.lower, sig.upper - spxClose));
    if (intrinsic > sig.premium) wins++;
  }
  return Math.round(wins / signals.length * 100);
}

async function getSpxCloseForDate(dateISO, env = null) {
  // Schwab daily candle first when env is available — canonical, and Stooq now
  // sits behind a JS bot-challenge that returns HTML instead of CSV (2026-07-10,
  // broke every backfill with "Invalid close from Stooq"). PAST days only: a
  // same-day daily candle is a live partial print, not a close — returning it
  // pre-16:15 would poison m8bfWR and the 90%-override that reads it.
  if (env) {
    try {
      const etNowC = toET(new Date());
      if (dateISO < isoDateET(etNowC)) {
        const tkC = await getAccessToken(env);
        if (tkC) {
          const [yC, mC, dC] = dateISO.split('-').map(Number);
          const startC = Date.UTC(yC, mC - 1, dC) - 4 * 86400000;
          const endC = Date.UTC(yC, mC - 1, dC) + 86400000;
          const phC = await fetchSchwabJSON(`https://api.schwabapi.com/marketdata/v1/pricehistory?symbol=%24SPX&periodType=month&frequencyType=daily&frequency=1&startDate=${startC}&endDate=${endC}`, tkC);
          const cdC = (phC.candles || []).find(c => isoDateET(toET(new Date(c.datetime))) === dateISO);
          if (cdC && cdC.close) return parseFloat(cdC.close.toFixed(2));
        }
      }
    } catch (_) { /* fall through to Stooq */ }
  }
  // Stooq CSV API (legacy fallback; may be behind a bot challenge)
  const [y, m, d] = dateISO.split('-');
  const dateCompact = `${y}${m}${d}`;
  const url = `https://stooq.com/q/d/l/?s=^spx&d1=${dateCompact}&d2=${dateCompact}&i=d`;
  const resp = await fetch(url, { headers: { 'User-Agent': 'Mozilla/5.0' } });
  if (!resp.ok) throw new Error(`Stooq fetch failed: ${resp.status}`);
  const text = await resp.text();
  // CSV: Date,Open,High,Low,Close,Volume
  const lines = text.trim().split('\n');
  if (lines.length < 2) throw new Error('No data from Stooq');
  const parts = lines[1].split(',');
  const close = parseFloat(parts[4]);
  if (isNaN(close)) throw new Error('Invalid close from Stooq');
  return parseFloat(close.toFixed(2));
}

async function backfillMissingWR(env, force = false, targetDates = null) {
  const token = env.DISCORD_USER_TOKEN;
  const channelId = '1048242197029458040';
  if (!token) throw new Error('DISCORD_USER_TOKEN not set');

  // 1. Read history from KV (source of truth). The old path read+PUT GitHub directly,
  //    which the next KV→GitHub mirror reverted → perpetual backfill/null churn. Now
  //    KV-first: read KV, mutate, setHistory + mirror once (audit 2026-06-22).
  const content = await getHistory(env);

  // 2. Find entries to process
  const today = new Date().toISOString().slice(0, 10);
  const missing = content.filter(r => {
    if (r.date > today) return false;
    if (targetDates) return targetDates.includes(r.date); // specific dates
    if (force) return true; // all past dates
    return r.m8bfWR == null; // default: only missing
  });

  const filled = [];
  const failed = [];

  for (const entry of missing) {
    try {
      // Get SPX close
      // Row's own canonical close first — Stooq is unreliable (bot challenge)
      const spxClose = entry.spxClose ?? await getSpxCloseForDate(entry.date, env);
      if (spxClose == null) { failed.push({ date: entry.date, reason: 'no SPX close' }); continue; }

      // Fetch ALL butterfly signals for that day from Discord
      const signals = await fetchAllDiscordSignalsForDate(token, channelId, entry.date);
      if (signals.length === 0) {
        failed.push({ date: entry.date, reason: 'no signals found in Discord' }); continue;
      }

      // Win rate = % of signals where intrinsic > premium
      const m8bfWR = computeWinRateFromSignals(signals, spxClose);

      // Update in-memory content (always overwrite m8bfWR)
      const idx = content.findIndex(r => r.date === entry.date);
      if (idx >= 0) {
        content[idx].m8bfWR = m8bfWR;
        if (content[idx].spxClose == null) content[idx].spxClose = spxClose;
      } else {
        content.push({ date: entry.date, spxClose, m8bfWR });
        content.sort((a, b) => a.date.localeCompare(b.date));
      }
      filled.push({ date: entry.date, spxClose, signals: signals.length, m8bfWR });
    } catch (e) {
      failed.push({ date: entry.date, reason: e.message });
    }
  }

  // 3. Persist — RE-READ current history and apply ONLY the fields we computed, so a
  //    concurrent EOD settle during our slow Discord loop isn't clobbered by a stale
  //    full-array overwrite (audit P1-h 2026-07-06). content was read minutes ago.
  if (filled.length > 0) {
    const fresh = await getHistory(env);
    for (const f of filled) {
      let row = fresh.find(r => r.date === f.date);
      if (!row) { row = { date: f.date }; fresh.push(row); }
      row.m8bfWR = f.m8bfWR;
      if (row.spxClose == null) row.spxClose = f.spxClose;
    }
    fresh.sort((a, b) => a.date.localeCompare(b.date));
    await setHistory(env, fresh, { dateStr: 'backfill-wr' });
    await mirrorHistoryToGitHub(env, fresh, `auto: backfill m8bfWR for ${filled.map(f => f.date).join(', ')}`);
  }

  return { filled, failed, total_missing: missing.length };
}

// ════════════════════════════════════════════════════════════════════
// BACKFILL MISSING m8bfPL
// ════════════════════════════════════════════════════════════════════

// Trade entry windows per JS getDay() (0=Sun,1=Mon...6=Sat) — from discord_scraper.py
const M8BF_WINDOWS = {
  1: [11*60,     11*60+30],  // Mon 11:00–11:30
  2: [13*60+30,  14*60],     // Tue 13:30–14:00
  3: [12*60,     12*60+30],  // Wed 12:00–12:30
  4: [11*60,     11*60+30],  // Thu 11:00–11:30
  5: [13*60,     13*60+30],  // Fri 13:00–13:30
};
const M8BF_WINDOWS_2ND_THU = [13*60+30, 14*60]; // 2nd trading Thu: 13:30–14:00

// Is dateISO the 2nd trading Thursday of its month?
function isSecondTradingThursday(dateISO) {
  const d = new Date(dateISO + 'T12:00:00Z');
  if (d.getUTCDay() !== 4) return false; // not Thursday
  const year = d.getUTCFullYear(), month = d.getUTCMonth();
  let thuCount = 0;
  for (let day = 1; day <= d.getUTCDate(); day++) {
    const check = new Date(Date.UTC(year, month, day));
    if (check.getUTCDay() === 4) thuCount++;
  }
  return thuCount === 2;
}

function getM8BFWindow(dow, dateISO) {
  if (dow === 4 && isSecondTradingThursday(dateISO)) return M8BF_WINDOWS_2ND_THU;
  return M8BF_WINDOWS[dow];
}

// M8BF banned-day reason — pure date math, no side effects. Mirrors the
// inline check that GET /trade used (signal-engine.js m8bfBanned + NM-non-Mon
// + CPI). Returns the human reason string, or null when M8BF is tradeable.
// Returns the M8BF ban reason for `etNow`, OR null if M8BF is allowed.
// HONORS the 90% override: if prevWR ≥ 90% and it's not a CPI day and GXBF
// isn't firing today, the override forces M8BF through any calendar ban.
// Mirrors signal-engine.js calculateSignal lines ~489-495 — keeps /trade
// (and refreshM8bfLiveQuotes) in lock-step with the Discord auto-message.
// 2026-05-28: bug bit when EOM-1 + prevWR=99% — /trade said banned, Discord
// said fire. Both now agree via the same override path.
async function m8bfBannedReason(env, etNow) {
  // Anchor Filter first — absolute, never overridden (audit 2026-07-24 [37]):
  // this single chokepoint gates /trade, refreshM8bfLiveQuotes and the bot relay.
  if (await gexGateSkipFor(env, isoDateET(etNow))) return 'Anchor Filter — unanchored';
  const eomDay = isLastTradeMo(etNow);
  const eom1   = isEomN(1, etNow);
  const opex1  = opexSch.some(ds => isTodayBefore(ds, etNow));
  const vixExpAfterOpex = isVixAfterOpexDay(etNow);
  const nonAmznTslaEarn = isNonAmznTslaEarningsDay(etNow);
  const cpiDay = cpiSch.includes(todayLong(etNow));
  const nmDay = isFirstTradeMo(etNow);
  const nmMon = isFirstTradeMon(etNow);
  const nmNonMon = nmDay && !nmMon;
  const m8bfBanned = eomDay || eom1 || opex1 || vixExpAfterOpex || nonAmznTslaEarn || nmNonMon;
  if (!(m8bfBanned || cpiDay)) return null;
  if (cpiDay) return 'CPI day';   // CPI is never overridden

  // 90% override check — prevWR from most recent prior history row.
  try {
    const todayISO = isoDateET(etNow);
    const hist = await getHistory(env);
    if (Array.isArray(hist) && hist.length) {
      const sorted = hist
        .filter(r => r.date && r.date < todayISO && r.m8bfWR != null)
        .sort((a, b) => b.date.localeCompare(a.date));
      const prevWR = sorted.length ? parseFloat(sorted[0].m8bfWR) : null;
      if (prevWR != null && prevWR >= 90) {
        // Override fires unless GXBF would also fire today (90% rule cannot
        // cancel GXBF — strategy independence).
        const todayRow = hist.find(r => r.date === todayISO);
        const vToday  = todayRow?.vixOpen != null ? parseFloat(todayRow.vixOpen) : null;
        const vYClose = sorted[0]?.vixClose != null ? parseFloat(sorted[0].vixClose) : null;
        const oNight  = (vToday != null && vYClose != null) ? (vYClose - vToday) : null;
        const gxbfFires = oNight != null && oNight > 0.65 && vToday < 25;
        if (!gxbfFires) return null;   // M8BF fires via 90% override
      }
    }
  } catch { /* if history fetch fails, fall through to ban-reason return */ }

  return eomDay ? 'EOM'
       : eom1   ? 'EOM-1'
       : opex1  ? 'day before OPEX'
       : vixExpAfterOpex ? 'VIX exp day'
       : nonAmznTslaEarn ? 'earnings'
       : nmNonMon ? 'NM (Straddle day)'
       : 'banned';
}

// Pure M8BF qualifying-signal selection (no side effects, no Discord poll).
// SINGLE SOURCE OF TRUTH shared by GET /trade and refreshM8bfLiveQuotes so
// the live-quoted legs are ALWAYS the exact trade /trade reports (real money:
// a divergence would mark-to-market the wrong strikes). Caller handles the
// banned-day gate first via m8bfBannedReason(). Byte-faithful to the previous
// inline /trade selection.
async function selectM8bfQualifying(env, etNow) {
  const todayT = isoDateET(etNow);
  const dow = etNow.getDay();
  const win = getM8BFWindow(dow, todayT);
  const sigRaw = await env.SIGNAL_KV.get('signals_today');
  const sigData = sigRaw ? JSON.parse(sigRaw) : { date: '', signals: [] };
  if (!win || sigData.date !== todayT) {
    return { status: 'waiting', reason: 'No window today or no signals', todayT };
  }
  // Manual-cancellation skip list — write this KV to ignore specific signal
  // times for the rest of today (bot keeps monitoring for any other signal
  // in the window). Cleared automatically by EOD via TTL.
  //   key:   m8bf_skip_signals_<YYYY-MM-DD>
  //   value: JSON array of "HH:MM" times, e.g. ["13:02"]
  let skipTimes = new Set();
  try {
    const skipRaw = await env.SIGNAL_KV.get(`m8bf_skip_signals_${todayT}`);
    if (skipRaw) skipTimes = new Set(JSON.parse(skipRaw) || []);
  } catch (_) { /* no-op */ }

  const [winLo, winHi] = win;
  let qualifying = null;
  for (const sig of (sigData.signals || [])) {
    if (!sig.time) continue;
    if (skipTimes.has(sig.time)) continue;   // ← manual cancellation
    const [h, m] = sig.time.split(':').map(Number);
    const mins = h * 60 + m;
    if (mins >= winLo && mins < winHi && !sig.banned) { qualifying = sig; break; }
  }
  if (!qualifying) {
    const nowMins = etNow.getHours() * 60 + etNow.getMinutes();
    const winStr = `${Math.floor(winLo/60)}:${String(winLo%60).padStart(2,'0')}-${Math.floor(winHi/60)}:${String(winHi%60).padStart(2,'0')}`;
    if (nowMins >= winHi) {
      const reason = skipTimes.size
        ? `Window passed — ${skipTimes.size} signal(s) manually cancelled (${[...skipTimes].join(', ')})`
        : 'Window passed, no qualifying signal';
      return { status: 'no_signal', reason, todayT };
    }
    return {
      status: 'waiting',
      window: winStr,
      reason: skipTimes.size
        ? `Cancelled ${[...skipTimes].join(', ')} — watching for new signal in ${winStr} ET`
        : undefined,
      todayT,
    };
  }
  return { status: 'open', qualifying, todayT };
}

async function backfillMissingPL(env, targetDates = null) {
  const token = env.DISCORD_USER_TOKEN;
  const channelId = '1048242197029458040';
  if (!token) throw new Error('DISCORD_USER_TOKEN not set');

  // KV-first read (source of truth) — the old GitHub read+PUT path was reverted by
  // the next KV→GitHub mirror, causing perpetual backfill/null churn (audit 2026-06-22).
  const content = await getHistory(env);

  const etNow = toET(new Date());
  const todayISO = `${etNow.getFullYear()}-${String(etNow.getMonth()+1).padStart(2,'0')}-${String(etNow.getDate()).padStart(2,'0')}`;

  let missing;
  if (targetDates) {
    missing = content.filter(r => targetDates.includes(r.date));
  } else {
    missing = content.filter(r => r.date <= todayISO && r.m8bfPL == null && r.spxClose != null);
  }

  const filled = [], failed = [];

  for (const row of missing) {
    try {
      // Get day of week in ET
      const etDate = toET(new Date(row.date + 'T20:00:00Z'));
      const dow = etDate.getDay(); // 0=Sun,1=Mon...6=Sat

      // Calendar-only M8BF bans (earnings, EOM, OPEX-1, VIX-after-OPEX, CPI).
      // FIX (2026-06-09 audit P0 #3): now respects the 90%-WR override. On a
      // calendar-banned day where the prior trading day's m8bfWR ≥ 90 AND
      // it's not a CPI day, the override forces M8BF to fire (matches
      // signal-engine.js:502). Previously this branch wrote m8bfPL=0 even
      // when the live bot actually executed an M8BF trade — silent loss.
      const eomDay = isEomN(0, etDate);
      const eom1 = isEomN(1, etDate);
      const opex1 = opexSch.some(ds => isTodayBefore(ds, etDate));
      const vixExpAfterOpex = isVixAfterOpexDay(etDate);
      const nonAmznTslaEarn = isNonAmznTslaEarningsDay(etDate);
      const cpiDay = cpiSch.includes(todayLong(etDate));
      if (await gexGateSkipFor(env, row.date)) {
        row.m8bfPL = 0;
        filled.push({ date: row.date, pl: 0, gexGate: true });
        continue;
      }
      const calendarBlocked = eomDay || eom1 || opex1 || vixExpAfterOpex || nonAmznTslaEarn || cpiDay;
      if (calendarBlocked) {
        // Check 90% override: look for the most recent prior m8bfWR in the
        // content array we already loaded.
        const priorWREntry = content
          .filter(r => r.date < row.date && r.m8bfWR != null)
          .sort((a, b) => b.date.localeCompare(a.date))[0];
        const priorWR = priorWREntry ? parseFloat(priorWREntry.m8bfWR) : null;
        const ninetyOverride = (priorWR != null && priorWR >= 90 && !cpiDay);
        // GXBF check: if GXBF fired today (gxbfPL non-null and non-zero),
        // the 90% override doesn't apply per signal-engine.js:502.
        const gxbfFired = (row.gxbfPL != null && row.gxbfPL !== 0);
        if (!ninetyOverride || gxbfFired) {
          row.m8bfPL = 0;
          filled.push({ date: row.date, pl: 0, blocked: { eom: eomDay, 'eom-1': eom1, 'opex-1': opex1, vixAfterOpex: vixExpAfterOpex, earn: nonAmznTslaEarn, cpi: cpiDay }, ninetyOverride: false });
          continue;
        }
        // Otherwise fall through — calendar block overridden by 90% rule.
        console.log(`[backfill] 90% override at ${row.date}: priorWR=${priorWR}, allowing M8BF P/L computation`);
      }

      const win = getM8BFWindow(dow, row.date);
      if (!win) {
        failed.push({ date: row.date, reason: 'no window for dow=' + dow });
        continue;
      }
      const [winLo, winHi] = win;

      // Fetch all signals for this date in chronological order
      const signals = await fetchAllDiscordSignalsForDate(token, channelId, row.date);

      // First qualifying signal: in window + not banned
      let qualifying = null;
      for (const sig of signals) {
        if (!sig.time) continue;
        const [h, m] = sig.time.split(':').map(Number);
        const mins = h * 60 + m;
        if (mins >= winLo && mins < winHi && !isBanned(sig.center, sig.lower, sig.t1)) {
          qualifying = sig;
          break;
        }
      }

      if (!qualifying) {
        failed.push({ date: row.date, reason: 'no qualifying signal in window' });
        continue;
      }

      // PL = round((min(intrinsic, wing) - premium) * 100)
      const lo = qualifying.lower;
      const hi = qualifying.upper;
      const wing = (hi - lo) / 2;
      const intrinsic = Math.max(0, Math.min(row.spxClose - lo, hi - row.spxClose));
      const clipped = Math.min(intrinsic, wing);
      const pl = Math.round((clipped - qualifying.premium) * 100);

      row.m8bfPL = pl;
      filled.push({ date: row.date, pl, center: qualifying.center, lower: lo, upper: hi, premium: qualifying.premium, spxClose: row.spxClose });
    } catch (e) {
      failed.push({ date: row.date, error: e.message });
    }
  }

  if (filled.length > 0) {
    // RE-READ fresh + apply ONLY m8bfPL for the dates we filled, so a concurrent EOD
    // settle isn't clobbered by a stale full-array overwrite (audit P1-h 2026-07-06).
    const fresh = await getHistory(env);
    for (const f of filled) {
      let row = fresh.find(r => r.date === f.date);
      if (!row) { row = { date: f.date }; fresh.push(row); }
      row.m8bfPL = f.pl;
    }
    fresh.sort((a, b) => a.date.localeCompare(b.date));
    await setHistory(env, fresh, { dateStr: 'backfill-pl' });
    await mirrorHistoryToGitHub(env, fresh, `auto: backfill m8bfPL for ${filled.map(f => f.date).join(', ')}`);
  }

  return { filled, failed, total_missing: missing.length };
}

// ════════════════════════════════════════════════════════════════════
// APPEND DAILY SIGNALS TO TRADES DATABASE (backtester.html)
// ════════════════════════════════════════════════════════════════════

async function appendTradesToBacktester(env, todayISO, etNow, signals, spxClose, addToSkip = false) {
  if (!signals || signals.length === 0) return { appended: 0, reason: 'no signals' };
  if (spxClose == null) return { appended: 0, reason: 'no spxClose' };

  const token = env.GITHUB_TOKEN;
  if (!token) return { appended: 0, reason: 'no GITHUB_TOKEN' };

  const owner = 'rava8989';
  const repo = 'brave';
  const path = 'backtester.html';
  const ghHeaders = {
    'Authorization': `Bearer ${token}`,
    'Accept': 'application/vnd.github+json',
    'User-Agent': 'schwab-proxy-worker/1.0',
    'X-GitHub-Api-Version': '2022-11-28',
  };

  // 1. Fetch raw file content
  const rawResp = await fetch(`https://raw.githubusercontent.com/${owner}/${repo}/main/${path}?t=${Date.now()}`, {
    headers: { 'Cache-Control': 'no-cache' },
  });
  if (!rawResp.ok) throw new Error(`Raw fetch failed: ${rawResp.status}`);
  let html = await rawResp.text();

  // 2. Dedup check — skip if today's date already in TRADES
  if (html.includes(`["${todayISO}",`)) {
    return { appended: 0, reason: `already exists: ${todayISO}` };
  }

  // 3. Build new TRADES rows — 9-field schema must match backtester.html and rebake_trades.py
  // [D, DAY, TIME, PREM, SPR, PROF, MAXP, CTR, BANNED]
  // SPR is half-wing width (center→wing), not full span.
  // BANNED encodes both full bans AND combo bans (computed from real T1).
  // DAY: 0=Mon..4=Fri  (JS getDay: 0=Sun..6=Sat)
  const dayIdx = etNow.getDay() - 1;

  // Bucket signals into 5-min buckets, keeping the FIRST signal per bucket
  // (matches rebake_trades.py behavior so daily append matches a fresh rebake).
  const buckets = new Map();
  for (const sig of signals) {
    const t = sig.time || '10:00';
    const [hh, mm] = t.split(':').map(Number);
    const bucket = `${String(hh).padStart(2,'0')}:${String(Math.floor(mm/5)*5).padStart(2,'0')}`;
    if (!buckets.has(bucket)) buckets.set(bucket, { ...sig, bucket });
  }

  const newRows = [];
  for (const bucket of [...buckets.keys()].sort()) {
    const sig = buckets.get(bucket);
    const spr = Math.floor((sig.upper - sig.lower) / 2);
    if (spr <= 0) continue;
    const maxp = Math.round((spr - sig.premium) * 100);
    if (maxp <= 0) continue;
    const intrinsic = Math.max(0, Math.min(spxClose - sig.lower, sig.upper - spxClose));
    const prof = Math.round((intrinsic - sig.premium) * 100);
    const banned = isBanned(sig.center, sig.lower, sig.t1);
    newRows.push([todayISO, dayIdx, bucket, sig.premium, spr, prof, maxp, sig.center, banned]);
  }

  if (newRows.length === 0) return { appended: 0, reason: 'all rows filtered out' };

  // 4. Find injection point — right before ];\nconst META
  const injectionMarker = '];\nconst META';
  const injIdx = html.indexOf(injectionMarker);
  if (injIdx === -1) throw new Error('Cannot find TRADES injection point in backtester.html');

  const rowsStr = newRows.map(r => JSON.stringify(r)).join(',');
  html = html.slice(0, injIdx) + ',' + rowsStr + html.slice(injIdx);

  // 5. Update META: maxDate and count
  html = html.replace(/("maxDate"\s*:\s*)"[^"]*"/, `$1"${todayISO}"`);
  const countMatch = html.match(/"count"\s*:\s*(\d+)/);
  if (countMatch) {
    const newCount = parseInt(countMatch[1]) + newRows.length;
    html = html.replace(/"count"\s*:\s*\d+/, `"count": ${newCount}`);
  }

  // 5b. If today is a SKIP date (live system blocked M8BF), inject into M8BF_SKIP set
  if (addToSkip && !html.includes(`"${todayISO}"`)) {
    const skipMatch = html.match(/(const M8BF_SKIP = new Set\(\[)([\s\S]*?)(\]\);)/);
    if (skipMatch) {
      const inner = skipMatch[2].trimEnd();
      const sep = inner.endsWith(',') ? '' : ',';
      const injected = `${skipMatch[1]}${inner}${sep}\n  "${todayISO}"\n${skipMatch[3]}`;
      html = html.replace(skipMatch[0], injected);
    }
  }

  // 6. Push via Git Data API (handles files >1MB — GitHub Contents API has 1MB limit)

  // 6a. Create blob
  const blobResp = await fetch(`https://api.github.com/repos/${owner}/${repo}/git/blobs`, {
    method: 'POST',
    headers: { ...ghHeaders, 'Content-Type': 'application/json' },
    body: JSON.stringify({ content: html, encoding: 'utf-8' }),
  });
  if (!blobResp.ok) {
    const err = await blobResp.text();
    throw new Error(`Blob create failed: ${blobResp.status} — ${err.slice(0, 200)}`);
  }
  const { sha: blobSha } = await blobResp.json();

  // 6b–6f. Get HEAD, create tree+commit, update ref (retry on 422 race condition)
  for (let attempt = 1; attempt <= 3; attempt++) {
    // 6b. Get current HEAD commit SHA
    const refResp = await fetch(`https://api.github.com/repos/${owner}/${repo}/git/ref/heads/main`, { headers: ghHeaders });
    if (!refResp.ok) throw new Error(`Ref fetch failed: ${refResp.status}`);
    const { object: { sha: headSha } } = await refResp.json();

    // 6c. Get tree SHA from HEAD commit
    const commitResp = await fetch(`https://api.github.com/repos/${owner}/${repo}/git/commits/${headSha}`, { headers: ghHeaders });
    if (!commitResp.ok) throw new Error(`Commit fetch failed: ${commitResp.status}`);
    const { tree: { sha: treeSha } } = await commitResp.json();

    // 6d. Create new tree
    const treeResp = await fetch(`https://api.github.com/repos/${owner}/${repo}/git/trees`, {
      method: 'POST',
      headers: { ...ghHeaders, 'Content-Type': 'application/json' },
      body: JSON.stringify({
        base_tree: treeSha,
        tree: [{ path, mode: '100644', type: 'blob', sha: blobSha }],
      }),
    });
    if (!treeResp.ok) {
      const err = await treeResp.text();
      throw new Error(`Tree create failed: ${treeResp.status} — ${err.slice(0, 200)}`);
    }
    const { sha: newTreeSha } = await treeResp.json();

    // 6e. Create new commit
    const newCommitResp = await fetch(`https://api.github.com/repos/${owner}/${repo}/git/commits`, {
      method: 'POST',
      headers: { ...ghHeaders, 'Content-Type': 'application/json' },
      body: JSON.stringify({
        message: `auto: append ${newRows.length} trades for ${todayISO}`,
        tree: newTreeSha,
        parents: [headSha],
      }),
    });
    if (!newCommitResp.ok) {
      const err = await newCommitResp.text();
      throw new Error(`Commit create failed: ${newCommitResp.status} — ${err.slice(0, 200)}`);
    }
    const { sha: newCommitSha } = await newCommitResp.json();

    // 6f. Update ref to point at new commit
    const updateRefResp = await fetch(`https://api.github.com/repos/${owner}/${repo}/git/refs/heads/main`, {
      method: 'PATCH',
      headers: { ...ghHeaders, 'Content-Type': 'application/json' },
      body: JSON.stringify({ sha: newCommitSha }),
    });
    if (updateRefResp.ok) break; // success
    if (updateRefResp.status === 422 && attempt < 3) {
      console.warn(`[proxy] Ref update 422 race condition, retry ${attempt}/3`);
      continue;
    }
    const err = await updateRefResp.text();
    throw new Error(`Ref update failed: ${updateRefResp.status} — ${err.slice(0, 200)}`);
  }

  return { appended: newRows.length, date: todayISO, signals: signals.length };
}

// ════════════════════════════════════════════════════════════════════
// WORKER EXPORT
// ════════════════════════════════════════════════════════════════════

// ════════════════════════════════════════════════════════════════════
// MORNING CARD IMAGE — forecast-only card rendered SVG → PNG (resvg-wasm).
// wasm is bundled; fonts are fetched from CDN once and cached per isolate.
// Any failure here is caught by the caller, which falls back to text.
// ════════════════════════════════════════════════════════════════════
let _resvgReady = null;
async function ensureResvg() {
  if (!_resvgReady) _resvgReady = initWasm(resvgWasm);
  await _resvgReady;
}
let _cardFonts = null;
async function getCardFonts() {
  if (_cardFonts) return _cardFonts;
  const SETS = [
    ['https://cdn.jsdelivr.net/npm/@expo-google-fonts/inter/Inter_400Regular.ttf',
     'https://cdn.jsdelivr.net/npm/@expo-google-fonts/inter/Inter_600SemiBold.ttf'],
    ['https://cdn.jsdelivr.net/npm/dejavu-fonts-ttf@2.37.3/ttf/DejaVuSans.ttf',
     'https://cdn.jsdelivr.net/npm/dejavu-fonts-ttf@2.37.3/ttf/DejaVuSans-Bold.ttf'],
  ];
  let lastErr = null;
  for (const set of SETS) {
    try {
      const bufs = await Promise.all(set.map(async u => {
        const r = await fetch(u, { cf: { cacheTtl: 604800, cacheEverything: true } });
        if (!r.ok) throw new Error(`font ${r.status} ${u}`);
        return new Uint8Array(await r.arrayBuffer());
      }));
      _cardFonts = bufs;
      return _cardFonts;
    } catch (e) { lastErr = e; }
  }
  throw lastErr || new Error('no fonts');
}
function _cardEsc(s) {
  return String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}
// Approx Inter advance widths (fraction of em) so we can wrap card text before
// it clips past the inner width — SVG <text> has no auto-wrap. Biased slightly
// wide so estimation errors wrap early rather than overflow the card.
function _cardCharEm(ch) {
  if (ch === ' ') return 0.28;
  if ('il.,:;\'!|'.includes(ch)) return 0.30;
  if ('jtrf()[]{}-/\\·'.includes(ch)) return 0.42;
  if ('mwMW%—@&→'.includes(ch)) return 0.90;
  if (ch >= 'A' && ch <= 'Z') return 0.72;
  if (ch >= '0' && ch <= '9') return 0.58;
  return 0.56;
}
function _cardTextW(str, fs) {
  let em = 0;
  for (const ch of String(str)) em += _cardCharEm(ch);
  return em * fs;
}
// Word-wrap a value into lines: the first line has `firstMax` px available
// (room left after its label), continuation lines get `contMax`. Never splits
// a single word.
function _cardWrap(value, fs, firstMax, contMax) {
  const words = String(value == null ? '' : value).split(' ');
  const lines = [];
  let cur = '';
  for (const w of words) {
    const test = cur ? cur + ' ' + w : w;
    const max = lines.length === 0 ? firstMax : contMax;
    if (cur && _cardTextW(test, fs) > max) { lines.push(cur); cur = w; }
    else cur = test;
  }
  lines.push(cur);
  return lines;
}
// data: { title, date, vix, vixSub, rows:[{n,det,yes}], tiles:[[label,val,color]], stats:[[label,val]] }
function buildMorningCardSvg(d) {
  const W = 460, P = 18;
  const C = { card: '#1e1f22', yesBg: '#2a2c30', noBg: '#26282c', text: '#f2f3f5',
    sub: '#b5bac1', mute: '#80848e', nameNo: '#dadde1', green: '#4ade80', red: '#f87171', amber: '#f5b942' };
  const F = 'Inter, DejaVu Sans, sans-serif';
  let s = '';
  s += `<text x="${P}" y="36" font-family="${F}" font-size="17" font-weight="600" fill="${C.text}">${_cardEsc(d.title)}</text>`;
  s += `<text x="${P}" y="53" font-family="${F}" font-size="12" fill="${C.mute}">${_cardEsc(d.date)}</text>`;
  s += `<text x="${W - P}" y="36" text-anchor="end" font-family="${F}" font-size="21" font-weight="600" fill="${C.text}">${_cardEsc(d.vix)}</text>`;
  s += `<text x="${W - P}" y="53" text-anchor="end" font-family="${F}" font-size="11" fill="${d.vixSubUp ? C.red : C.mute}">${_cardEsc(d.vixSub)}</text>`;
  if (d.vixPrior) s += `<text x="${W - P}" y="65" text-anchor="end" font-family="${F}" font-size="9.5" fill="#6b7078">${_cardEsc(d.vixPrior)}</text>`;
  const innerW = W - 2 * P, y0 = 76, rowH = 38, step = 44;
  d.rows.forEach((r, i) => {
    // 3-state: 'possible' (amber) for affirmative-but-not-final, else yes/no.
    const st = r.state || (r.yes ? 'yes' : 'no');
    const isYes = st === 'yes', isPoss = st === 'possible';
    const top = y0 + i * step, bg = (isYes || isPoss) ? C.yesBg : C.noBg;
    const bar = isYes ? C.green : isPoss ? C.amber : C.red;
    const tag = isYes ? 'YES' : isPoss ? 'POSSIBLE' : 'NO';
    const pillW = isPoss ? 74 : isYes ? 42 : 34, pillX = P + innerW - 10 - pillW;
    const pillBg = isYes ? 'rgba(74,222,128,0.14)' : isPoss ? 'rgba(245,185,66,0.15)' : 'rgba(248,113,113,0.13)';
    s += `<rect x="${P}" y="${top}" width="${innerW}" height="${rowH}" rx="8" fill="${bg}"/>`;
    s += `<rect x="${P}" y="${top + 1}" width="4" height="${rowH - 2}" rx="2" fill="${bar}"/>`;
    s += `<clipPath id="rc${i}"><rect x="${P + 10}" y="${top}" width="${pillX - 8 - (P + 10)}" height="${rowH}"/></clipPath>`;
    s += `<text x="${P + 14}" y="${top + 24}" clip-path="url(#rc${i})" font-family="${F}" font-size="14"><tspan font-weight="600" fill="${(isYes || isPoss) ? C.text : C.nameNo}">${_cardEsc(r.n)}</tspan><tspan font-size="13" fill="${C.sub}">  ${_cardEsc(r.det)}</tspan></text>`;
    s += `<rect x="${pillX}" y="${top + 10}" width="${pillW}" height="18" rx="6" fill="${pillBg}"/>`;
    s += `<text x="${pillX + pillW / 2}" y="${top + 23}" text-anchor="middle" font-family="${F}" font-size="11" font-weight="600" fill="${bar}">${tag}</text>`;
  });
  const tileY = y0 + d.rows.length * step + 4, tileH = 38, gap = 7, tileW = (innerW - (d.tiles.length - 1) * gap) / d.tiles.length;
  d.tiles.forEach((t, i) => {
    const x = P + i * (tileW + gap);
    s += `<rect x="${x}" y="${tileY}" width="${tileW}" height="${tileH}" rx="8" fill="${C.yesBg}"/>`;
    s += `<text x="${x + tileW / 2}" y="${tileY + 15}" text-anchor="middle" font-family="${F}" font-size="10" fill="${C.mute}">${_cardEsc(t[0])}</text>`;
    s += `<text x="${x + tileW / 2}" y="${tileY + 31}" text-anchor="middle" font-family="${F}" font-size="13" font-weight="600" fill="${t[2]}">${_cardEsc(t[1])}</text>`;
  });
  const statLabelY = tileY + tileH + 22;
  s += `<text x="${P}" y="${statLabelY}" font-family="${F}" font-size="11" letter-spacing="0.5" fill="${C.mute}">STATS</text>`;
  // SVG <text> does not wrap; long stat/strike values used to clip past the
  // card's inner width. emitKV word-wraps the value under its label and returns
  // the baseline of the last line it drew.
  const emitKV = (label, val, fs, yStart, contStep) => {
    const off = _cardTextW(label + '   ', fs);
    const lines = _cardWrap(val, fs, Math.max(60, innerW - off), innerW);
    s += `<text x="${P}" y="${yStart}" font-family="${F}" font-size="${fs}"><tspan fill="${C.mute}">${_cardEsc(label)}</tspan><tspan fill="${C.sub}">   ${_cardEsc(lines[0])}</tspan></text>`;
    let yy = yStart;
    for (let k = 1; k < lines.length; k++) {
      yy += contStep;
      s += `<text x="${P}" y="${yy}" font-family="${F}" font-size="${fs}" fill="${C.sub}">${_cardEsc(lines[k])}</text>`;
    }
    return yy;
  };
  const sy0 = statLabelY + 18;
  let statY = sy0, lastY = sy0;
  d.stats.forEach((st) => {
    lastY = emitKV(String(st[0] || ''), String(st[1] || ''), 12, statY, 16);
    statY = lastY + 20;
  });
  let H = lastY + 8;
  // Optional M8BF strikes block — INSIDE the card, small + muted, only when armed.
  if (d.m8bfStrikes) {
    const sepY = H + 2;
    s += `<line x1="${P}" y1="${sepY}" x2="${W - P}" y2="${sepY}" stroke="#34363c" stroke-width="1"/>`;
    let strikeY = emitKV('M8BF skip-ends', d.m8bfStrikes.skip, 11, sepY + 19, 15);
    strikeY = emitKV('combo bans', d.m8bfStrikes.combos, 11, strikeY + 18, 15);
    H = strikeY + 8;
  }
  return `<svg xmlns="http://www.w3.org/2000/svg" width="${W}" height="${H}" viewBox="0 0 ${W} ${H}"><rect x="0" y="0" width="${W}" height="${H}" rx="16" fill="${C.card}"/>${s}</svg>`;
}
async function renderMorningCardPng(d) {
  await ensureResvg();
  const fonts = await getCardFonts();
  const r = new Resvg(buildMorningCardSvg(d), {
    fitTo: { mode: 'width', value: 920 },
    font: { fontBuffers: fonts, defaultFontFamily: 'Inter', loadSystemFonts: false },
  });
  return r.render().asPng();
}

// ════════════════════════════════════════════════════════════════════
// EARNINGS CARD IMAGE (2026-07-10) — the nightly earnings board rendered
// SVG → PNG, reusing the SAME rasterizer + fonts + Discord-attachment path
// as the morning card. Every state (LONG night / mixed board / quiet /
// VIX-spike skip) collapses to one card. ANY failure → caller falls back
// to the text board, so the earnings signal can never go silent.
// ════════════════════════════════════════════════════════════════════
function _earnDotColor(g) { return g === true ? '#4ade80' : g === false ? '#f87171' : '#4a4d54'; }
function _earnReason(r) {
  if (r.verdict === 'LONG') return `flow ${r.pw_ratio ?? '—'}`;
  if (r.verdict === 'CROWDED') return `crowded · flow ${r.pw_ratio ?? '—'}`;
  if (r.g1 === false) return r.pw_ratio != null ? `flow light ${r.pw_ratio}` : 'no flow';
  if (r.g2 === false) return 'extended · at highs';
  if (r.g3 === false) return 'weak track record';
  if (r.g4 === false) return 'VIX elevated';
  return 'pass';
}
const _EARN_MON = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
const _EARN_DOW = ['Sun','Mon','Tue','Wed','Thu','Fri','Sat'];
function _earnCardDate(iso) {
  const [y, m, d] = String(iso).split('-').map(Number);
  const dow = new Date(Date.UTC(y, m - 1, d)).getUTCDay();
  return `${_EARN_DOW[dow]} ${_EARN_MON[m - 1]} ${d}`;
}
// b = earnBuildBoard output {date, vix:{ratio,ok}, park:{sleeve}, board:[...], longs:[...]}
function buildEarningsCardSvg(b) {
  const W = 460, P = 18, innerW = W - 2 * P;
  const F = 'Inter, DejaVu Sans, sans-serif';
  const C = { card: '#1e1f22', skipCard: '#241a1a', text: '#f2f3f5', sub: '#b5bac1',
    mute: '#80848e', longBg: '#17251c', failBg: '#251a1c', green: '#4ade80', red: '#f87171',
    accent: '#a78bfa', foot: '#2a2c30', footTxt: '#e7e3ff', footGood: '#1d3a29',
    footGoodTxt: '#c9f5da', footBad: '#3a2020', footBadTxt: '#f7c9c9' };
  const vixOk = b.vix ? b.vix.ok !== false : true;
  const scored = (b.board || []).filter(r => !(r.notes || []).some(n => String(n).startsWith('outside universe')));
  const longs = (b.longs && b.longs.length) ? b.longs : scored.filter(r => r.verdict === 'LONG');
  const parkTxt = b.park ? ({ SPY: 'stay in SPY', GLD: 'stay in GLD', CASH: 'sit in cash' }[b.park.sleeve] || 'stay parked') : 'stay parked';

  const header = (bg) => {
    let h = `<rect x="0" y="0" width="${W}" height="__H__" rx="16" fill="${bg}"/>`;
    h += `<text x="${P}" y="34" font-family="${F}" font-size="16" font-weight="600" fill="${C.accent}">Σ3 earnings</text>`;
    h += `<text x="${W - P}" y="34" text-anchor="end" font-family="${F}" font-size="12" fill="${C.mute}">${_cardEsc(_earnCardDate(b.date))}</text>`;
    // Stage label so a morning PREVIEW is never mistaken for the final call
    // (user 2026-07-13: banks all "pass" at 9am — that's provisional, 3:30 decides).
    const stageTxt = b.after ? 'morning after — how the board opened (close → next open)'
                   : b.final ? 'FINAL board — this is the call' : 'morning preview · final board decides at 3:30pm ET';
    h += `<text x="${P}" y="49" font-family="${F}" font-size="10.5" fill="${b.after ? C.accent : b.final ? '#4ade80' : C.mute}">${stageTxt}</text>`;
    return h;
  };
  const footBar = (y, bg, txtColor, msg) => {
    let f = `<rect x="${P}" y="${y}" width="${innerW}" height="34" rx="9" fill="${bg}"/>`;
    f += `<clipPath id="fc"><rect x="${P + 10}" y="${y}" width="${innerW - 20}" height="34"/></clipPath>`;
    f += `<text x="${W / 2}" y="${y + 22}" clip-path="url(#fc)" text-anchor="middle" font-family="${F}" font-size="12.5" fill="${txtColor}">${_cardEsc(msg)}</text>`;
    return f;
  };
  const centerMsg = (bigColor, big, small) => {
    let m = `<text x="${W / 2}" y="86" text-anchor="middle" font-family="${F}" font-size="17" font-weight="600" fill="${bigColor}">${_cardEsc(big)}</text>`;
    m += `<text x="${W / 2}" y="108" text-anchor="middle" font-family="${F}" font-size="12.5" fill="${C.mute}">${_cardEsc(small)}</text>`;
    return m;
  };

  // STATE E — VIX spike: everything skipped.
  if (!vixOk) {
    const H = 168;
    const vixR = b.vix && b.vix.ratio != null ? b.vix.ratio : '—';
    let s = header(C.skipCard).replace('__H__', H);
    s += centerMsg(C.red, 'All nights skipped', `VIX ${vixR}× its 100-day mean — fear spiking`);
    s += footBar(H - 48, C.footBad, C.footBadTxt, 'no earnings trades until VIX calms');
    return `<svg xmlns="http://www.w3.org/2000/svg" width="${W}" height="${H}" viewBox="0 0 ${W} ${H}">${s}</svg>`;
  }
  // STATE D — quiet night: nobody in-universe reports.
  if (!scored.length) {
    // On SPY-parking days, add one small line noting the optional 2× leveraged-ETF
    // alternatives (informational, user 2026-07-12; SPY stays the default).
    const spySleeve = b.park && b.park.sleeve === 'SPY';
    const H = spySleeve ? 188 : 168;
    let s = header(C.card).replace('__H__', H);
    s += centerMsg(C.sub, 'No trades tonight', 'nothing to trade today');
    s += footBar(120, C.foot, C.footTxt, parkTxt);
    if (spySleeve) s += `<text x="${W / 2}" y="${H - 12}" text-anchor="middle" font-family="${F}" font-size="10.5" fill="${C.mute}">optional 2×: SSO (S&amp;P) · QLD (Nasdaq) — more return, more drawdown</text>`;
    return `<svg xmlns="http://www.w3.org/2000/svg" width="${W}" height="${H}" viewBox="0 0 ${W} ${H}">${s}</svg>`;
  }

  // STATES A/B/C — a board. LONGs first, then the rest; cap the visible rows.
  // Full list, always (owner 2026-07-29): the card grows as tall as the night
  // needs — no '+N more', every universe reporter visible.
  const ordered = [...scored].sort((a, b2) => (a.verdict === 'LONG' ? 0 : 1) - (b2.verdict === 'LONG' ? 0 : 1));
  const shown = ordered;
  const overflow = 0;
  const w = longs.length ? Math.round(100 / longs.length) : 0;
  const rowH = 46, step = 52, y0 = 54;
  let s = '';
  shown.forEach((r, i) => {
    const top = y0 + i * step;
    const isLong = r.verdict === 'LONG';
    const bg = isLong ? C.longBg : C.failBg, bar = isLong ? C.green : C.red;
    s += `<rect x="${P}" y="${top}" width="${innerW}" height="${rowH}" rx="9" fill="${bg}"/>`;
    s += `<rect x="${P}" y="${top + 1}" width="4" height="${rowH - 2}" rx="2" fill="${bar}"/>`;
    const clipW = innerW - 120;
    s += `<clipPath id="er${i}"><rect x="${P + 12}" y="${top}" width="${clipW}" height="${rowH}"/></clipPath>`;
    s += `<text x="${P + 14}" y="${top + 20}" clip-path="url(#er${i})" font-family="${F}" font-size="15"><tspan font-weight="600" fill="${C.text}">${_cardEsc(r.ticker)}</tspan><tspan font-size="11" fill="${C.mute}">  ${_cardEsc(r.when || '')}</tspan></text>`;
    s += `<text x="${P + 14}" y="${top + 37}" clip-path="url(#er${i})" font-family="${F}" font-size="12" fill="${C.sub}">${_cardEsc(_earnReason(r))}</text>`;
    // Right: weight% for a LONG, else the verdict word. In after-mode the
    // overnight result takes the prime spot and the verdict shifts left.
    if (b.after) {
      const ap = r.afterPct;
      const apTxt = ap == null ? '—' : `${ap >= 0 ? '+' : ''}${(ap * 100).toFixed(1)}%`;
      const apCol = ap == null ? C.mute : ap >= 0 ? C.green : C.red;
      s += `<text x="${W - P - 14}" y="${top + 22}" text-anchor="end" font-family="${F}" font-size="16" font-weight="600" fill="${apCol}">${apTxt}</text>`;
      const vTxt = isLong ? `LONG ${w}%` : (r.verdict === 'CROWDED' ? 'crowded' : 'pass');
      s += `<text x="${W - P - 76}" y="${top + 22}" text-anchor="end" font-family="${F}" font-size="11" fill="${isLong ? C.green : C.red}">${vTxt}</text>`;
    } else if (isLong) s += `<text x="${W - P - 14}" y="${top + 22}" text-anchor="end" font-family="${F}" font-size="17" font-weight="600" fill="${C.green}">${w}%</text>`;
    else s += `<text x="${W - P - 14}" y="${top + 22}" text-anchor="end" font-family="${F}" font-size="12" fill="${C.red}">${r.verdict === 'CROWDED' ? 'crowded' : 'pass'}</text>`;
    // 4 gate dots, right-aligned, ending at W-P-14.
    const dots = [r.g1, r.g2, r.g3, r.g4];
    dots.forEach((g, k) => {
      const cx = (W - P - 16) - (3 - k) * 13;
      s += `<circle cx="${cx}" cy="${top + 34}" r="4" fill="${_earnDotColor(g)}"/>`;
    });
  });
  let footY = y0 + shown.length * step + 4;
  let footBg, footTxt, msg;
  if (b.after) {
    const withPct = shown.filter(r => r.afterPct != null);
    const avg = withPct.length ? withPct.reduce((a, r) => a + r.afterPct, 0) / withPct.length : null;
    const longRes = longs.map(l => shown.find(r => r.ticker === l.ticker)).filter(r => r && r.afterPct != null);
    footBg = C.foot; footTxt = C.footTxt;
    msg = longRes.length
      ? `traded: ${longRes.map(r => `${r.ticker} ${r.afterPct >= 0 ? '+' : ''}${(r.afterPct * 100).toFixed(1)}%`).join(' · ')}`
      : `no positions were taken — observation only${avg != null ? ` · board avg ${avg >= 0 ? '+' : ''}${(avg * 100).toFixed(1)}%` : ''}`;
  } else if (longs.length) {
    footBg = C.footGood; footTxt = C.footGoodTxt;
    msg = `buy 3:45–3:55 close · ${longs.map(l => `${l.ticker} ${w}%`).join(' · ')} → sell at open`;
  } else {
    footBg = C.foot; footTxt = C.footTxt;
    msg = `no qualifiers tonight · ${parkTxt}`;
  }
  const H = footY + 34 + P;
  s = header(C.card).replace('__H__', H) + s + footBar(footY, footBg, footTxt, msg);
  return `<svg xmlns="http://www.w3.org/2000/svg" width="${W}" height="${H}" viewBox="0 0 ${W} ${H}">${s}</svg>`;
}
async function renderEarningsCardPng(b) {
  await ensureResvg();
  const fonts = await getCardFonts();
  const r = new Resvg(buildEarningsCardSvg(b), {
    fitTo: { mode: 'width', value: 920 },
    font: { fontBuffers: fonts, defaultFontFamily: 'Inter', loadSystemFonts: false },
  });
  return r.render().asPng();
}
// Post a PNG to a Discord WEBHOOK (multipart) — the earnings channel sink.
async function postWebhookImage(url, pngBytes, content, filename = 'earnings.png') {
  try {
    const fd = new FormData();
    fd.append('payload_json', JSON.stringify({ content: String(content || '').slice(0, 1900) }));
    fd.append('files[0]', new Blob([pngBytes], { type: 'image/png' }), filename);
    const r = await fetch(url, { method: 'POST', body: fd });
    return r.ok || r.status === 204;
  } catch (_) { return false; }
}
const EARN_CARD_FOOTER = '⚠️ Not financial advice — for educational purposes only. You are solely responsible for your own trades.';
// Send the earnings board as a CARD when KV `earnings_card_mode` === 'on', else
// as text (current behavior). Card path: owner DM + subscriber webhook; ANY
// render/upload failure falls back to the text board so nothing goes silent.
// `dmOnly` (test endpoint) sends the card to the owner DM only, ignores the flag.
async function earnSendCard(env, b, stage, dmOnly = false) {
  const flag = await env.SIGNAL_KV.get('earnings_card_mode');
  if (!dmOnly && flag !== 'on') return earnSend(env, earnBoardMsg(b, null, stage));  // flag off → text (earnSend adds its own disclaimer)
  // PER-SINK resilience (2026-07-10): render once; then EACH sink (owner DM,
  // subscriber webhook) tries the image and, if that fails, sends TEXT to that
  // same sink. A broken webhook-image post can no longer leave subscribers with
  // NOTHING while the owner got the card — the recurring silent-drop class.
  const DISC = '\n\n_Not financial advice. For informational/educational purposes only. You are solely responsible for your own trades._';
  const text = earnBoardMsg(b, null, stage) + DISC;
  let png = null;
  try { png = await renderEarningsCardPng(b); }
  catch (e) { try { await logEvent(env, 'warn', 'earn-card', 'render failed — text per sink', { msg: e && (e.message || String(e)) }); } catch (_) {} }
  const dcRaw = await env.SIGNAL_KV.get('discord_config');
  const dc = dcRaw ? JSON.parse(dcRaw) : null;
  const out = { card: !!png };
  if (dc && dc.channelId) {
    let ok = false;
    if (png) ok = (await sendDiscordImage(env, dc.channelId, png, dc.proxyUrl, 'earnings.png', EARN_CARD_FOOTER)).ok;
    if (!ok) ok = (await sendDiscordDM(env, dc.channelId, text.slice(0, 2000), dc.proxyUrl)).ok;
    out.dm = ok;
  }
  if (!dmOnly) {
    const wh = await env.SIGNAL_KV.get('earnings_webhook_url');
    if (wh) {
      let ok = false;
      if (png) ok = await postWebhookImage(wh, png, EARN_CARD_FOOTER, 'earnings.png');
      if (!ok) {
        try {
          const r = await fetch(wh, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ content: text.slice(0, 1900) }) });
          ok = r.ok || r.status === 204;
        } catch (_) {}
      }
      out.webhook = ok;
    }
  }
  return out;
}

// Map the live `signal` (same object the text builder uses) → card data.
// Faithful: reuses the exact status strings + the active/blocked rule (a status
// starting with "No " = blocked = red/NO). M8BF is shown as CONDITIONAL
// ("watching … on flow signal") because it only fires if a flow signal lands
// in its window — it is NOT a scheduled trade.
function buildMorningCardData(signal, vixValues, tailLine, pnbf) {
  const isNo = t => !t || /^No\s/i.test(String(t).trim());
  const strip = (t, name) => {
    let d = String(t || '').trim();
    d = d.replace(new RegExp('^(No\\s+)?' + name + '\\b\\s*', 'i'), '');
    d = d.replace(/^[—\-:│|@]\s*/, '').trim();
    const pm = d.match(/^\((.*)\)$/); if (pm) d = pm[1];
    return d.trim();
  };
  const rows = [];
  {
    // GXBF (owner 2026-07-30): a calendar-affirmative morning is POSSIBLE, not
    // YES — the 9:35 gamma gate has the final word (negative 0DTE book = skip).
    // Same P23 amber convention as BOBF/PNBF: affirmative-but-not-final.
    // AUDIT FIX 2026-07-31: the gated outcome depends on overnight VIX — down
    // = straddle conversion, up/flat = no trade. Say the real outcome; oNight
    // is known at card time (9:31).
    const gxYes = !isNo(signal.gxbfText);
    const gxDet = strip(signal.gxbfText, 'GXBF') || '—';
    const _oN = (typeof signal.oNight === 'number' && isFinite(signal.oNight)) ? signal.oNight : null;
    const gateTail = _oN == null ? 'gate: pos fly / neg strad·VIX↓'
      : (_oN > 0 ? 'gate: pos fly / neg strad' : 'gate: pos fly / neg no-trade');
    rows.push(gxYes
      ? { n: 'GXBF', det: `${gxDet} · ${gateTail}`, yes: true, state: 'possible' }
      : { n: 'GXBF', det: gxDet, yes: false });
  }
  {
    const m8Active = !isNo(signal.m8bfText) && /^M8BF/i.test(String(signal.m8bfText || '').trim());
    let det;
    if (m8Active) {
      const win = (String(signal.m8bfText).match(/(\d{1,2}:\d{2}\s*[–-]\s*\d{1,2}:\d{2})/) || [])[1];
      // Fat-gamma tier (info only, owner 2026-07-24): story on the card, no
      // advice. Replaces the "on flow signal" filler so the row can't overflow.
      // No emoji — the card font has no color glyphs (renders as a broken box).
      const tail = signal.gexFatP != null ? `FAT GAMMA p${signal.gexFatP}` : 'on flow signal';
      det = win ? `watching ${win} · ${tail}` : `watching · ${tail}`;
    } else { det = strip(signal.m8bfText, 'M8BF') || '—'; }
    rows.push({ n: 'M8BF', det, yes: m8Active });
  }
  {
    // A GXBF-possible morning is a Straddle-possible morning too (owner
    // 2026-07-30): the 9:35 gamma gate converts a gated day to the GATED
    // STRADDLE, so the row can't read NO while that path is open.
    // AUDIT FIX 2026-07-31: conversion also requires overnight VIX DOWN — on a
    // VIX-up/flat morning the gated outcome is no-trade, so no POSSIBLE row.
    // oNight unknown → keep the amber row but state the full condition.
    const stradYes = !isNo(signal.stradText);
    const gxPossible = rows.some(r => r.n === 'GXBF' && r.state === 'possible');
    const _oNs = (typeof signal.oNight === 'number' && isFinite(signal.oNight)) ? signal.oNight : null;
    if (!stradYes && gxPossible && (_oNs == null || _oNs > 0)) {
      rows.push({ n: 'Straddle', yes: true, state: 'possible',
        det: _oNs == null ? 'if GXBF gates + VIX down' : 'if GXBF gamma-gates at 9:35' });
    } else {
      rows.push({ n: 'Straddle', det: strip(signal.stradText, 'Straddle') || '—', yes: stradYes });
    }
  }
  {
    // Owner rule (2026-07-15, re-broken 07-20): BOBF is never YES on the morning
    // card — its own later decision is final. Affirmative renders amber POSSIBLE.
    const bobfAff = !isNo(signal.bobfRec);
    rows.push({ n: 'BOBF', det: strip(signal.bobfRec, 'BOBF') || '—',
                yes: bobfAff, state: bobfAff ? 'possible' : 'no' });
  }
  if (signal.diagText) rows.push({ n: 'Diagonal', det: strip(signal.diagText, 'Diagonal') || '—', yes: !isNo(signal.diagText) });
  if (tailLine) {
    const tl = String(tailLine);
    const tYes = /\bTRADE\b/.test(tl) && !/No trade|\bSKIP\b/i.test(tl);
    let tdet;
    if (tYes) {                                  // concise — the full line overflowed the row
      const dm = tl.match(/Δ\s*-?[\d.]+/);
      const twoX = (typeof signal.oNight === 'number' && isFinite(signal.oNight) && signal.oNight > 0) ? ' ×2' : '';
      tdet = `9:45 · 0DTE put ${dm ? dm[0].replace(/\s+/g, '') : 'Δ-0.10'}${twoX}`;
    } else if (/\bSKIP\b/i.test(tl)) {
      tdet = 'SKIP · VVIX ≥ 110';
    } else {
      tdet = strip(tl.replace(/^Tail\s*Hedge\s*[│|]?\s*/i, ''), 'Tail Hedge') || 'no trade';
    }
    rows.push({ n: 'Tail Hedge', det: tdet, yes: tYes });
  }
  // PNBF (2026-07-15): fold the old standalone 9:40 "possible today" alert into
  // this card as its own row. Morning knows only the CALENDAR — T1==magnet is
  // decided at noon — so a clear calendar reads POSSIBLE (amber), a blocked one
  // reads NO. pnbf = { block: string|null } from mfCalendarBlock at card time.
  if (pnbf) {
    rows.push(pnbf.block
      ? { n: 'PNBF', det: pnbf.block, state: 'no' }
      : { n: 'PNBF', det: 'watching · noon decides (T1 on magnet)', state: 'possible' });
  }
  const vix = (vixValues.todayOpen != null) ? String(vixValues.todayOpen) : '—';
  // Overnight VIX direction in plain words. oNight = priorClose − todayOpen:
  // positive = VIX FELL overnight ("down"); negative = VIX ROSE ("up").
  const on = (typeof signal.oNight === 'number' && isFinite(signal.oNight)) ? signal.oNight : null;
  let vixSub = 'VIX', vixSubUp = false;
  if (on != null) {
    const mag = Math.abs(on).toFixed(2);
    if (on > 0.005) vixSub = `VIX down ${mag}`;
    else if (on < -0.005) { vixSub = `VIX up ${mag}`; vixSubUp = true; }
    else vixSub = 'VIX flat';
  }
  // Audit line (small/subtle): prior-day VIX close + open so the overnight-gate
  // inputs are verifiable on the card itself (prior close − today open = drop above).
  const _yc = vixValues.yClose != null ? Number(vixValues.yClose).toFixed(2) : null;
  const _yo = vixValues.yOpen  != null ? Number(vixValues.yOpen).toFixed(2)  : null;
  const vixPrior = (_yc || _yo)
    ? `prev${_yc ? ' ' + _yc + ' cls' : ''}${_yc && _yo ? ' ·' : ''}${_yo ? ' ' + _yo + ' opn' : ''}`
    : null;
  const gapStr = (signal.spxGapPct != null) ? `${signal.spxGapPct > 0 ? '+' : ''}${signal.spxGapPct.toFixed(2)}%` : '—';
  const tiles = [
    ['SPX GAP', gapStr, (signal.spxGapPct != null && signal.spxGapPct < 0) ? '#f87171' : '#4ade80'],
  ];
  const statLine = (line) => {
    if (!line) return null;
    let s = String(line).replace(/\x1b\[[0-9;]*m/g, '').trim();
    let m = s.match(/^([^│|—]+?)\s*[│|—]\s*(.+)$/);
    if (m) return [m[1].trim(), m[2].trim()];
    m = s.match(/^(\S+(?:\s\S+)?)\s{2,}(.+)$/);
    if (m) return [m[1].trim(), m[2].trim()];
    return [s, ''];
  };
  const stats = [signal._cycleLine, signal._volFlowLine, signal._m8bfWrLine]
    .map(statLine).filter(Boolean).slice(0, 6);
  let m8bfStrikes = null;
  {
    const si = signal.m8bfStrikeInfo;
    const m8on = signal.m8bfText && /^M8BF/i.test(String(signal.m8bfText).trim());
    if (m8on && si && Array.isArray(si.blocked) && si.blocked.length) {
      m8bfStrikes = {
        skip: si.blocked.join(' · '),
        combos: Object.entries(si.comboBans || {}).map(([k, v]) => `${k}→${v}`).join(' · '),
      };
    }
  }
  return {
    title: 'Σ3 — Today’s Plan',
    date: `${signal.dateStr || ''}${signal.dayLabel ? ' · ' + signal.dayLabel : ''}`.trim(),
    vix, vixSub, vixSubUp, vixPrior, rows, tiles, stats, m8bfStrikes,
  };
}
// The M8BF skip-list / combo-bans → Discord small (-#) subtext, posted BELOW
// the image. Only when M8BF is actually armed today.
function buildM8bfSubtext(signal) {
  const si = signal.m8bfStrikeInfo;
  const active = signal.m8bfText && /^M8BF/i.test(String(signal.m8bfText).trim());
  if (!active || !si || !si.blocked) return null;
  const skip = (si.blocked || []).join(' · ');
  const combos = Object.entries(si.comboBans || {}).map(([k, v]) => `${k}→${v}`).join(' · ');
  let line = `-# M8BF · skip center-ends: ${skip}`;
  if (combos) line += `  |  combo bans: ${combos}`;
  return line.slice(0, 1900);
}
// Post a PNG as a Discord attachment (multipart). Image upload requires the
// bot token path; the legacy proxies can't pass files, so callers fall back to
// text when this returns !ok.
function _b64FromBytes(bytes) {
  let bin = ''; const chunk = 0x8000;
  for (let i = 0; i < bytes.length; i += chunk) bin += String.fromCharCode.apply(null, bytes.subarray(i, i + chunk));
  return btoa(bin);
}
async function sendDiscordImage(env, userId, pngBytes, proxyUrl = null, filename = 'morning.png', content = '') {
  // Path 1: direct bot token (multipart) if this worker has it.
  if (env.DISCORD_TOKEN) {
    try {
      const dmResp = await fetch('https://discord.com/api/v10/users/@me/channels', {
        method: 'POST', headers: { Authorization: `Bot ${env.DISCORD_TOKEN}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({ recipient_id: userId }),
      });
      if (!dmResp.ok) return { ok: false, status: dmResp.status, error: `dm-chan ${dmResp.status}` };
      const dm = await dmResp.json();
      const fd = new FormData();
      fd.append('payload_json', JSON.stringify({ content, attachments: [{ id: 0, filename }] }));
      fd.append('files[0]', new Blob([pngBytes], { type: 'image/png' }), filename);
      const r = await fetch(`https://discord.com/api/v10/channels/${dm.id}/messages`, {
        method: 'POST', headers: { Authorization: `Bot ${env.DISCORD_TOKEN}` }, body: fd,
      });
      let data; try { data = await r.json(); } catch { data = {}; }
      return { ok: r.ok, status: r.status, data, source: 'image-direct', ...(r.ok ? {} : { error: `img ${r.status}` }) };
    } catch (e) { return { ok: false, error: 'image-direct: ' + e.message }; }
  }
  // Paths 2/3: base64 → discord-proxy (which holds the bot token) decodes + uploads.
  const payload = JSON.stringify({ userId, imageB64: _b64FromBytes(pngBytes), filename, content });
  const hdrs = { 'Content-Type': 'application/json' };
  if (env.PROXY_SECRET) hdrs['Authorization'] = `Bearer ${env.PROXY_SECRET}`;
  if (env.DISCORD_PROXY) {
    try {
      const r = await env.DISCORD_PROXY.fetch(new Request('https://dummy/', { method: 'POST', headers: hdrs, body: payload }));
      let data; try { data = await r.json(); } catch { data = {}; }
      const ok = r.ok && data.ok !== false;
      return { ok, status: r.status, data, source: 'image-binding', ...(ok ? {} : { error: `proxy-img ${r.status} ${data.error || ''}` }) };
    } catch (e) { return { ok: false, error: 'image-binding: ' + e.message }; }
  }
  if (proxyUrl && proxyUrl.startsWith('https://')) {
    try {
      const r = await fetch(proxyUrl, { method: 'POST', headers: hdrs, body: payload });
      let data; try { data = await r.json(); } catch { data = {}; }
      const ok = r.ok && data.ok !== false;
      return { ok, status: r.status, data, source: 'image-http', ...(ok ? {} : { error: `proxy-img ${r.status} ${data.error || ''}` }) };
    } catch (e) { return { ok: false, error: 'image-http: ' + e.message }; }
  }
  return { ok: false, error: 'no image transport (no DISCORD_TOKEN/DISCORD_PROXY/proxyUrl)' };
}
// M8BF conditional context notes — worker port of index.html's dashboard block
// (kept byte-identical so the card matches the dashboard). history rows:
// { date, m8bfWR, vixOpen, vixClose, spxOpen, spxClose }. etNow must be ET.
function computeM8bfContextNotes(history, etNow, todayVixOpen) {
  const rows = (history || [])
    .filter(r => r.m8bfWR != null)
    .sort((a, b) => a.date.localeCompare(b.date));
  if (rows.length < 2) return [];
  const notes = [];
  const last = rows[rows.length - 1];
  const daysDiff = (etNow.getTime() - new Date(last.date + 'T12:00:00').getTime()) / 86400000;
  function longToISO(s) { const d = new Date(s + ' 12:00:00'); if (isNaN(d)) return null; return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`; }
  function dateToISO(d) { return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`; }
  const twISO = new Set();
  for (let y = 2024; y <= 2027; y++) for (const m of [2, 5, 8, 11]) { let count = 0; for (let d = 1; d <= 31; d++) { const dt = new Date(y, m, d); if (dt.getMonth() !== m) break; if (dt.getDay() === 5 && ++count === 3) { twISO.add(dateToISO(dt)); break; } } }
  const todayLongStr = dateLong(etNow);
  const isOpex = () => opexSch.includes(todayLongStr);
  const isFed = () => fedSch.includes(todayLongStr);
  const isVixExp = () => vixSch.includes(todayLongStr);
  const isLastTradeMoToday = () => isLastTradeMo(etNow);
  const isTodayBeforeTW = () => twISO.has(dateToISO(nextTrade(etNow)));
  const isTodayAfterTW = () => twISO.has(dateToISO(prevTrade(etNow)));
  const isNMplus1 = () => isFirstTradeMo(prevTrade(etNow));
  const byDateISO = Object.fromEntries(rows.map(r => [r.date, r]));
  const isoDates = rows.map(r => r.date);
  const isoIdx = Object.fromEntries(isoDates.map((d, i) => [d, i]));
  const prevTradeISO = d => { const i = isoIdx[d]; return (i != null && i > 0) ? isoDates[i - 1] : null; };
  const nextTradeISO = d => { const i = isoIdx[d]; return (i != null && i < isoDates.length - 1) ? isoDates[i + 1] : null; };
  const isFirstTradeMoISO = d => { const p = prevTradeISO(d); return !p || p.slice(0, 7) !== d.slice(0, 7); };
  const isLastTradeMoISO = d => { const n = nextTradeISO(d); return !n || n.slice(0, 7) !== d.slice(0, 7); };
  const isNM1ISO = d => { const p = prevTradeISO(d); return p && isFirstTradeMoISO(p); };
  const opexISO = new Set(opexSch.map(longToISO).filter(Boolean));
  const fedISO = new Set(fedSch.map(longToISO).filter(Boolean));
  const vixExpISO = new Set(vixSch.map(longToISO).filter(Boolean));
  const earnISO = {};
  earningsSchedule.forEach(e => { const iso = longToISO(e.date); if (!iso) return; if (!earnISO[e.ticker]) earnISO[e.ticker] = new Set(); earnISO[e.ticker].add(iso); });
  const allWR = rows.map(r => parseFloat(r.m8bfWR)).filter(w => !isNaN(w));
  const baselineWR = allWR.length ? allWR.reduce((a, b) => a + b, 0) / allWR.length : 55;
  const NOTE_DELTA_MIN_PP = 10;
  function wrOn(predicate) {
    const wrs = [];
    for (const r of rows) { if (!predicate(r)) continue; const w = parseFloat(r.m8bfWR); if (!isNaN(w)) wrs.push(w); }
    if (wrs.length < 3) return null;
    const avg = wrs.reduce((a, b) => a + b, 0) / wrs.length;
    const delta = avg - baselineWR;
    if (Math.abs(delta) < NOTE_DELTA_MIN_PP) return null;
    return { wr: avg.toFixed(1), delta: (delta >= 0 ? '+' : '') + delta.toFixed(1), n: wrs.length };
  }
  const tag = s => s ? `${s.wr}% (${s.delta}pp vs avg, n=${s.n})` : null;
  const pOpex = r => opexISO.has(r.date);
  const pFed = r => fedISO.has(r.date);
  const pVixExp = r => vixExpISO.has(r.date);
  const pEom = r => isLastTradeMoISO(r.date);
  const pNM1 = r => isNM1ISO(r.date);
  const pEarn = t => r => earnISO[t] && earnISO[t].has(r.date);
  const pDayAfter = pred => r => { const p = prevTradeISO(r.date); return p && pred(byDateISO[p]); };
  const pDayBefore = pred => r => { const n = nextTradeISO(r.date); return n && pred(byDateISO[n]); };
  const todayVix = parseFloat(todayVixOpen);
  if (isOpex()) {
    if (!isNaN(todayVix) && todayVix < 18) { const s = wrOn(r => pOpex(r) && r.vixOpen != null && parseFloat(r.vixOpen) < 18); if (s) notes.push(`Today is OPEX with VIX below 18 — M8BF historically averages ${tag(s)}.`); }
    else { const s = wrOn(pOpex); if (s) notes.push(`Today is OPEX — M8BF historically averages ${tag(s)}.`); }
  }
  { const s = wrOn(pFed); if (isFed() && s) notes.push(`Today is a FED day — M8BF historically averages ${tag(s)}.`); }
  { const s = wrOn(pEom); if (isLastTradeMoToday() && s) notes.push(`Today is EOM (last trading day of month) — M8BF historically averages ${tag(s)}.`); }
  { const s = wrOn(pVixExp); if (isVixExp() && s) notes.push(`Today is VIX expiry — M8BF historically averages ${tag(s)}.`); }
  { const s = wrOn(pNM1); if (isNMplus1() && s) notes.push(`Today is the 2nd trading day of the month — M8BF historically averages ${tag(s)}.`); }
  earningsSchedule.filter(e => e.date === todayLongStr).forEach(e => { const s = wrOn(pEarn(e.ticker)); if (s) notes.push(`Today is ${e.company} earnings — M8BF historically averages ${tag(s)} on ${e.ticker} earnings days.`); });
  if (!isNaN(todayVix)) {
    if (todayVix >= 30) { const s = wrOn(r => r.vixOpen != null && parseFloat(r.vixOpen) >= 30); if (s) notes.push(`VIX opened at ${todayVix.toFixed(1)} today — M8BF averages ${tag(s)} when VIX opens ≥30.`); }
    else if (todayVix >= 25) { const s = wrOn(r => r.vixOpen != null && parseFloat(r.vixOpen) >= 25 && parseFloat(r.vixOpen) < 30); if (s) notes.push(`VIX opened at ${todayVix.toFixed(1)} today — M8BF averages ${tag(s)} when VIX opens 25-30.`); }
  }
  { const s = wrOn(pDayBefore(pFed)); if (fedSch.some(ds => isTodayBefore(ds, etNow)) && s) notes.push(`Tomorrow is a FED day — M8BF averages ${tag(s)} the day before Fed decisions.`); }
  { const s = wrOn(pDayBefore(pOpex)); if (opexSch.some(ds => isTodayBefore(ds, etNow)) && s) notes.push(`Tomorrow is OPEX — M8BF averages ${tag(s)} the day before OPEX.`); }
  { const s = wrOn(pDayBefore(r => twISO.has(r.date))); if (isTodayBeforeTW() && s) notes.push(`Tomorrow is Triple Witching — M8BF averages ${tag(s)} the day before TW.`); }
  earningsSchedule.filter(e => isTodayBefore(e.date, etNow)).forEach(e => { const s = wrOn(pDayBefore(pEarn(e.ticker))); if (s) notes.push(`Tomorrow is ${e.company} earnings — M8BF averages ${tag(s)} the day before ${e.ticker} earnings.`); });
  if (daysDiff <= 3) {
    { const s = wrOn(pDayAfter(r => twISO.has(r.date))); if (isTodayAfterTW() && s) notes.push(`Yesterday was Triple Witching — M8BF averages ${tag(s)} the day after TW.`); }
    { const s = wrOn(pDayAfter(pOpex)); if (opexSch.some(ds => isTodayAfter(ds, etNow)) && s) notes.push(`Yesterday was OPEX — M8BF averages ${tag(s)} the day after OPEX.`); }
    earningsSchedule.filter(e => longToISO(e.date) === last.date).forEach(e => { const s = wrOn(pDayAfter(pEarn(e.ticker))); if (s) notes.push(`Yesterday was ${e.company} earnings — M8BF averages ${tag(s)} the day after ${e.ticker} earnings.`); });
    if (parseFloat(last.m8bfWR) === 0) { const s = wrOn(pDayAfter(r => parseFloat(r.m8bfWR) === 0)); if (s) notes.push(`Last session (${last.date}) was 0% win rate — next-day average is ${tag(s)}.`); }
    if (last.vixClose) {
      const vc = parseFloat(last.vixClose);
      if (vc >= 30) { const s = wrOn(pDayAfter(r => r.vixClose != null && parseFloat(r.vixClose) >= 30)); if (s) notes.push(`VIX closed at ${vc.toFixed(1)} last session — next-day M8BF averages ${tag(s)} when prior VIX close ≥30.`); }
      else if (vc >= 25) { const s = wrOn(pDayAfter(r => r.vixClose != null && parseFloat(r.vixClose) >= 25 && parseFloat(r.vixClose) < 30)); if (s) notes.push(`VIX closed at ${vc.toFixed(1)} last session — next-day M8BF averages ${tag(s)} when prior VIX close 25-30.`); }
    }
    const vixIntra = r => { if (!r || r.vixOpen == null || r.vixClose == null) return null; const vo = parseFloat(r.vixOpen), vc = parseFloat(r.vixClose); return (vc - vo) / vo * 100; };
    if (last.vixOpen && last.vixClose) {
      const vi = vixIntra(last);
      if (vi >= 10) { const s = wrOn(pDayAfter(r => { const v = vixIntra(r); return v != null && v >= 10; })); if (s) notes.push(`VIX spiked ${vi.toFixed(1)}% intraday last session — next-day M8BF averages ${tag(s)} when VIX rises 10%+.`); }
      else if (vi >= 5) { const s = wrOn(pDayAfter(r => { const v = vixIntra(r); return v != null && v >= 5 && v < 10; })); if (s) notes.push(`VIX rose ${vi.toFixed(1)}% intraday last session — next-day M8BF averages ${tag(s)} when VIX rises 5-10%.`); }
      else if (vi <= -10) { const s = wrOn(pDayAfter(r => { const v = vixIntra(r); return v != null && v <= -10; })); if (s) notes.push(`VIX dropped ${Math.abs(vi).toFixed(1)}% intraday last session — next-day M8BF averages ${tag(s)} when VIX drops 10%+.`); }
    }
    const spxRet = r => { if (!r || r.spxOpen == null || r.spxClose == null) return null; const so = parseFloat(r.spxOpen), sc = parseFloat(r.spxClose); return (sc - so) / so * 100; };
    if (last.spxOpen && last.spxClose) {
      const sr = spxRet(last);
      if (sr <= -2) { const s = wrOn(pDayAfter(r => { const x = spxRet(r); return x != null && x <= -2; })); if (s) notes.push(`SPX fell ${Math.abs(sr).toFixed(1)}% last session — next-day M8BF averages ${tag(s)} after a 2%+ SPX down day.`); }
      else if (sr <= -1) { const s = wrOn(pDayAfter(r => { const x = spxRet(r); return x != null && x > -2 && x <= -1; })); if (s) notes.push(`SPX fell ${Math.abs(sr).toFixed(1)}% last session — next-day M8BF averages ${tag(s)} after a 1-2% SPX down day.`); }
      else if (sr >= 1 && sr < 2) { const s = wrOn(pDayAfter(r => { const x = spxRet(r); return x != null && x >= 1 && x < 2; })); if (s) notes.push(`SPX rose ${sr.toFixed(1)}% last session — next-day M8BF averages ${tag(s)} after a 1-2% SPX up day.`); }
    }
  }
  return notes;
}
const SAMPLE_MORNING_CARD = {
  title: 'Σ3 — Today’s Plan', date: 'Mon · Jun 22 2026 · OPEX+1', vix: '16.67', vixSub: 'VIX up 0.27', vixSubUp: true, vixPrior: 'prev 16.40 cls · 16.32 opn',
  rows: [
    { n: 'GXBF', det: 'fires 9:36 AM · gate: pos fly / neg strad', yes: true, state: 'possible' }, { n: 'M8BF', det: 'window 11:00–11:30', yes: true },
    { n: 'Straddle', det: 'if GXBF gamma-gates at 9:35', yes: true, state: 'possible' }, { n: 'BOBF', det: 'OPEX', yes: false },
    { n: 'Diagonal', det: 'COR1M 6.79 < 10', yes: false },
    // Tail Hedge row removed 2026-08-03 — strategy retired; the live builder
    // omits it via `if (tailLine)` since getTailHedgeStatusLine returns null.
    { n: 'PNBF', det: 'watching · noon decides (T1 on magnet)', state: 'possible' },
  ],
  tiles: [['SPX GAP', '+0.91%', '#4ade80']],
  stats: [
    ['Day-type', 'NEUTRAL/BULL · Strad below norm ($695 vs $1091)'],
    ['Vol-flow', 'VOL_BID · M8BF $149 vs $434 · Strad $1902 vs $1091'],
    ['M8BF svc', 'yday WR 32% — soft next-day history ($310–350 vs $427), not proven'],
  ],
  m8bfStrikes: { skip: '10 · 25 · 35 · 40 · 65 · 80', combos: '0→95 · 20→15 · 55→50 · 65→60 · 85→90' },
};

export default {
  async fetch(request, env, ctx) {
    const origin = request.headers.get('Origin') || '';
    if (!env.ALLOWED_ORIGIN) {
      console.error('ALLOWED_ORIGIN env var is not set — all cross-origin requests will be blocked');
    }
    const allowed = env.ALLOWED_ORIGIN || 'null';
    const corsOk = origin !== '' && (origin === allowed || origin.startsWith('http://localhost'));

    const corsHeaders = {
      'Access-Control-Allow-Origin': corsOk ? origin || '*' : '',
      'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type, Authorization, X-Sync-Secret',
      'Access-Control-Max-Age': '86400',
    };

    const url = new URL(request.url);

    // ── Rate limiting ──
    if (checkRateLimit(request)) {
      return jsonResp({ error: 'Rate limit exceeded' }, 429, corsHeaders);
    }

    // ── GET /status ── Secured debug endpoint
    if (url.pathname === '/status' && request.method === 'GET') {
      if (request.headers.get('X-Sync-Secret') !== env.SYNC_SECRET) {
        return jsonResp({ error: 'Unauthorized' }, 401, corsHeaders);
      }
      try {
        const lastRun = await env.SIGNAL_KV.get('last_run');
        return jsonResp(lastRun ? JSON.parse(lastRun) : { status: 'never_run' }, 200, {});
      } catch (e) {
        return jsonResp({ error: e.message }, 500, {});
      }
    }

    // ── GET /raw-discord?date=YYYY-MM-DD ── Show raw Discord messages for debugging
    if (url.pathname === '/test-card' && request.method === 'GET') {
      try {
        let cardData = SAMPLE_MORNING_CARD;
        // ?fatp=NN — preview the fat-gamma tier M8BF row (render-only, no send).
        const fatp = parseInt(url.searchParams.get('fatp') || '', 10);
        if (Number.isFinite(fatp)) {
          cardData = JSON.parse(JSON.stringify(SAMPLE_MORNING_CARD));
          const m8 = (cardData.rows || []).find(r => r.n === 'M8BF');
          if (m8) { m8.det = `watching 13:30–14:00 · FAT GAMMA p${fatp}`; m8.yes = true; m8.state = undefined; }
        }
        const png = await renderMorningCardPng(cardData);
        return new Response(png, { headers: { 'content-type': 'image/png', 'cache-control': 'no-store' } });
      } catch (e) {
        return new Response('card render failed: ' + (e && (e.stack || e.message) || e), { status: 500 });
      }
    }

    if (url.pathname === '/test-card-discord' && request.method === 'GET') {
      // AUTH (2026-07-06 audit): side-effecting Discord send — gate it so a
      // stranger can't spam the channel / burn the bot's rate budget.
      // Accept GEXM_TRIGGER_TOKEN too (zshrc SYNC_SECRET is stale) — both
      // owner-only, same as earnings-card-test / magnetfly-test.
      const _tcSec = request.headers.get('X-Sync-Secret') || url.searchParams.get('secret');
      if (!_tcSec || (_tcSec !== env.SYNC_SECRET && _tcSec !== env.GEXM_TRIGGER_TOKEN)) {
        return new Response('Unauthorized', { status: 401 });
      }
      try {
        const dcRaw = await env.SIGNAL_KV.get('discord_config');
        if (!dcRaw) return new Response('no discord_config', { status: 500 });
        const dc = JSON.parse(dcRaw);
        const png = await renderMorningCardPng(SAMPLE_MORNING_CARD);
        const r = await sendDiscordImage(env, dc.channelId, png, dc.proxyUrl, 'morning.png', DISCORD_FOOTER);
        return new Response(JSON.stringify({ image: r.ok, status: r.status, error: r.error || null }), { headers: { 'content-type': 'application/json' } });
      } catch (e) {
        return new Response('test-card-discord failed: ' + (e && (e.stack || e.message) || e), { status: 500 });
      }
    }

    // POST /send-card — manual "Send Card" from the dashboard. Renders the card
    // from the CALCULATOR'S OWN signal (sent in the body), so it works any time
    // (weekend, or when the live data feed is down — exactly when you'd reach for
    // the manual fallback). No live re-pull. Image-first, text fallback. Server
    // enriches with advisory lines + tail (KV-backed, work offline). Gated to the
    // dashboard Origin. Body is parsed regardless of content-type (no CORS preflight).
    if (url.pathname === '/send-card' && request.method === 'POST') {
      const cors = { 'Access-Control-Allow-Origin': 'https://rava8989.github.io' };
      if ((request.headers.get('Origin') || '') !== 'https://rava8989.github.io') {
        return jsonResp({ ok: false, error: 'forbidden — dashboard only' }, 403, cors);
      }
      try {
        const body = await request.json();
        const signal = body && body.signal;
        const vixValues = (body && body.vixValues) || {};
        if (!signal) return jsonResp({ ok: false, error: 'no signal in body' }, 400, cors);
        const dcRaw = await env.SIGNAL_KV.get('discord_config');
        const dc = dcRaw ? JSON.parse(dcRaw) : null;
        if (!dc || !dc.channelId) return jsonResp({ ok: false, error: 'no discord_config in KV' }, 200, cors);
        const etNow = toET(new Date());
        // Enrich with server-side stats (KV-backed → available even with no live feed).
        try { signal._cycleLine   = await computeCycleLine(env, etNow); } catch (_) {}
        try { signal._volFlowLine = await computeVolFlowLine(env, etNow); } catch (_) {}
        try { signal._m8bfWrLine  = await computeM8bfWrLine(env, etNow); } catch (_) {}
        let tailLine = null; try { tailLine = await getTailHedgeStatusLine(env); } catch (_) {}
        let r = null;
        try {
          const png = await renderMorningCardPng(buildMorningCardData(signal, vixValues, tailLine));
          r = await sendDiscordImage(env, dc.channelId, png, dc.proxyUrl, 'morning.png', DISCORD_FOOTER);
        } catch (e) { r = { ok: false, error: 'render: ' + e.message }; }
        if (!r || !r.ok) {
          const txt = buildDiscordMessage(signal, vixValues, tailLine);
          const rt = await sendDiscordDM(env, dc.channelId, txt.slice(0, 2000), dc.proxyUrl);
          return jsonResp({ ok: !!(rt && rt.ok), kind: 'text-fallback', imgErr: r && r.error }, 200, cors);
        }
        return jsonResp({ ok: true, kind: 'image' }, 200, cors);
      } catch (e) { return jsonResp({ ok: false, error: e.message }, 200, cors); }
    }

    if (url.pathname === '/raw-discord' && request.method === 'GET') {
      if (request.headers.get('X-Sync-Secret') !== env.SYNC_SECRET) {
        return jsonResp({ error: 'Unauthorized' }, 401, {});
      }
      try {
        const dateISO = url.searchParams.get('date') || new Date().toISOString().slice(0,10);
        const [y, m, d] = dateISO.split('-').map(Number);
        const startMs = Date.UTC(y, m-1, d, 12, 0, 0);
        const endMs   = Date.UTC(y, m-1, d, 22, 0, 0);
        const discordEpoch = 1420070400000n;
        const afterSnowflake = ((BigInt(startMs) - discordEpoch) << 22n).toString();
        const beforeSnowflake = ((BigInt(endMs) - discordEpoch) << 22n).toString();
        const resp = await fetch(
          `https://discord.com/api/v9/channels/1048242197029458040/messages?limit=10&after=${afterSnowflake}&before=${beforeSnowflake}`,
          { headers: { 'Authorization': env.DISCORD_USER_TOKEN, 'User-Agent': 'Mozilla/5.0' } }
        );
        if (!resp.ok) throw new Error(`Discord API ${resp.status}`);
        const msgs = await resp.json();
        const sample = (Array.isArray(msgs) ? msgs : []).map(m => ({
          id: m.id, ts: m.timestamp, content: (m.content||'').slice(0,200),
          embeds: (m.embeds||[]).map(e => ({ title: e.title, desc: (e.description||'').slice(0,200) }))
        }));
        return jsonResp({ date: dateISO, count: Array.isArray(msgs) ? msgs.length : msgs, sample }, 200, {});
      } catch(e) {
        return jsonResp({ error: e.message }, 500, {});
      }
    }

    // ── GET /check-wr?from=YYYY-MM-DD&to=YYYY-MM-DD ── Compare stored vs recalculated m8bfWR
    if (url.pathname === '/check-wr' && request.method === 'GET') {
      if (request.headers.get('X-Sync-Secret') !== env.SYNC_SECRET) {
        return jsonResp({ error: 'Unauthorized' }, 401, {});
      }
      try {
        const from = url.searchParams.get('from');
        const to   = url.searchParams.get('to');
        if (!from || !to) return jsonResp({ error: 'missing from/to params' }, 400, {});

        const ghResp = await fetch('https://api.github.com/repos/rava8989/brave/contents/history_data.json', {
          headers: { 'Authorization': `Bearer ${env.GITHUB_TOKEN}`, 'Accept': 'application/vnd.github+json', 'User-Agent': 'schwab-proxy-worker/1.0', 'X-GitHub-Api-Version': '2022-11-28' }
        });
        if (!ghResp.ok) throw new Error(`GitHub GET failed: ${ghResp.status}`);
        const meta = await ghResp.json();
        const content = JSON.parse(atob(meta.content.replace(/\n/g, '')));
        const rows = content.filter(r => r.m8bfWR != null && r.date >= from && r.date <= to);

        const results = [];
        for (const row of rows) {
          try {
            const spxClose = row.spxClose ?? await getSpxCloseForDate(row.date, env);
            const signals = await fetchAllDiscordSignalsForDate(env.DISCORD_USER_TOKEN, '1048242197029458040', row.date);
            const calc = signals.length > 0 ? computeWinRateFromSignals(signals, spxClose) : null;
            const diff = calc != null ? Math.abs(calc - row.m8bfWR) : null;
            results.push({ date: row.date, stored: row.m8bfWR, calc, signals: signals.length, diff, match: diff != null && diff <= 2 });
          } catch(e) {
            results.push({ date: row.date, stored: row.m8bfWR, calc: null, error: e.message });
          }
        }
        const matched = results.filter(r => r.match).length;
        const mismatched = results.filter(r => r.calc != null && !r.match);
        return jsonResp({ from, to, total: results.length, matched, mismatched_count: mismatched.length, mismatched, all: results }, 200, {});
      } catch(e) {
        return jsonResp({ error: e.message }, 500, {});
      }
    }

    // ── GET /backfill-wr ── Fill missing m8bfWR from Discord history + Stooq SPX
    // ?force=true recalculates last 60 days regardless of existing values
    if (url.pathname === '/backfill-wr' && request.method === 'GET') {
      if (request.headers.get('X-Sync-Secret') !== env.SYNC_SECRET) {
        return jsonResp({ error: 'Unauthorized' }, 401, {});
      }
      try {
        const force = url.searchParams.get('force') === 'true';
        const datesParam = url.searchParams.get('dates'); // e.g. 2026-03-24,2026-03-25
        const results = await backfillMissingWR(env, force, datesParam ? datesParam.split(',') : null);
        return jsonResp(results, 200, {});
      } catch (e) {
        return jsonResp({ error: e.message }, 500, {});
      }
    }

    // ── GET /backfill-pl ── Fill missing m8bfPL from Discord history
    if (url.pathname === '/backfill-pl' && request.method === 'GET') {
      if (request.headers.get('X-Sync-Secret') !== env.SYNC_SECRET) {
        return jsonResp({ error: 'Unauthorized' }, 401, {});
      }
      try {
        const datesParam = url.searchParams.get('dates');
        const results = await backfillMissingPL(env, datesParam ? datesParam.split(',') : null);
        return jsonResp(results, 200, {});
      } catch (e) {
        return jsonResp({ error: e.message }, 500, {});
      }
    }

    // ── GET /rescrape?date=YYYY-MM-DD ── Re-scrape all Discord signals for a date into KV
    if (url.pathname === '/rescrape' && request.method === 'GET') {
      const secret = request.headers.get('X-Sync-Secret') || url.searchParams.get('secret');
      if (!secret || secret !== env.SYNC_SECRET) {
        return jsonResp({ error: 'Unauthorized' }, 401, { 'Access-Control-Allow-Origin': '*' });
      }
      try {
        const dateISO = url.searchParams.get('date') || (() => { const et = toET(); return `${et.getFullYear()}-${String(et.getMonth()+1).padStart(2,'0')}-${String(et.getDate()).padStart(2,'0')}`; })();
        const allSigs = await fetchAllDiscordSignalsForDate(env.DISCORD_USER_TOKEN, '1048242197029458040', dateISO);
        // Build signals array with banned flag
        const signals = allSigs.map(s => ({
          time: s.time,
          center: s.center,
          lower: s.lower,
          upper: s.upper,
          t1: s.t1,
          premium: s.premium,
          cp: s.cp ?? 0,
          banned: isBanned(s.center, s.lower, s.t1),
        }));
        await env.SIGNAL_KV.put('signals_today', JSON.stringify({ date: dateISO, signals }));
        return jsonResp({ date: dateISO, total: signals.length, banned: signals.filter(s => s.banned).length }, 200, { 'Access-Control-Allow-Origin': '*' });
      } catch (e) {
        return jsonResp({ error: e.message }, 500, { 'Access-Control-Allow-Origin': '*' });
      }
    }

    // ── GET /scrape-raw?from=YYYY-MM-DD&to=YYYY-MM-DD ── Fetch raw Discord signals for CSV
    if (url.pathname === '/scrape-raw' && request.method === 'GET') {
      const secret = request.headers.get('X-Sync-Secret') || url.searchParams.get('secret');
      if (!secret || secret !== env.SYNC_SECRET) {
        return jsonResp({ error: 'Unauthorized' }, 401, { 'Access-Control-Allow-Origin': '*' });
      }
      try {
        const from = url.searchParams.get('from');
        const to = url.searchParams.get('to');
        if (!from || !to) return jsonResp({ error: 'missing from/to' }, 400, {});

        const token = env.DISCORD_USER_TOKEN;
        const channelId = '1048242197029458040';
        const results = [];

        // Iterate trading days
        const startD = new Date(from + 'T12:00:00Z');
        const endD = new Date(to + 'T12:00:00Z');
        for (let d = new Date(startD); d <= endD; d.setUTCDate(d.getUTCDate() + 1)) {
          const dow = d.getUTCDay();
          if (dow === 0 || dow === 6) continue;
          const dateISO = d.toISOString().slice(0, 10);

          // Fetch raw messages for this date
          const [y, m, dd] = dateISO.split('-').map(Number);
          const startMs = Date.UTC(y, m - 1, dd, 12, 0, 0);
          const endMs = Date.UTC(y, m - 1, dd, 22, 0, 0);
          const discordEpoch = 1420070400000n;
          let afterSnowflake = ((BigInt(startMs) - discordEpoch) << 22n).toString();
          const beforeSnowflake = ((BigInt(endMs) - discordEpoch) << 22n).toString();

          for (let page = 0; page < 10; page++) {
            const resp = await fetch(
              `https://discord.com/api/v9/channels/${channelId}/messages?limit=100&after=${afterSnowflake}&before=${beforeSnowflake}`,
              { headers: { 'Authorization': token, 'User-Agent': 'Mozilla/5.0' } }
            );
            if (!resp.ok) break;
            const batch = await resp.json();
            if (!Array.isArray(batch) || batch.length === 0) break;
            batch.sort((a, b) => a.id.localeCompare(b.id));

            for (const msg of batch) {
              const content = msg.content || '';
              const msgET = toET(new Date(msg.timestamp));
              const msgDate = `${msgET.getFullYear()}-${String(msgET.getMonth()+1).padStart(2,'0')}-${String(msgET.getDate()).padStart(2,'0')}`;
              if (msgDate !== dateISO) continue;
              const msgTime = `${String(msgET.getHours()).padStart(2,'0')}:${String(msgET.getMinutes()).padStart(2,'0')}`;

              // Parse full signal fields
              const priceM = content.match(/Price:\s*([\d.]+)/i);
              const trendM = content.match(/Trend:\s*(\w+)/i);
              const predM = content.match(/Predicted Close:\s*([\d.]+)/i);
              const strM = content.match(/Strength:\s*([\d.]+)/i);
              const stM = content.match(/Short term:\s*([\d.]+)/i);
              const ltM = content.match(/Long term:\s*([\d.]+)/i);
              const sbM = content.match(/Short term bias:\s*(\w+)/i);
              const lbM = content.match(/Long term bias:\s*(\w+)/i);
              const callsM = content.match(/Calls:\s*([\d.]+)/i);
              const putsM = content.match(/Puts:\s*([\d.]+)/i);
              const centerM = content.match(/Center:\s*([\d.]+)/i);
              const rangeM = content.match(/Range:\s*([\d.]+)/i);
              const t1M = content.match(/Target 1:\s*([\d.]+)/i);
              const t2M = content.match(/Target 2:\s*([\d.]+)/i);
              const deltaM = content.match(/Delta:\s*([\d.]+)/i);
              const gammaM = content.match(/Gamma:\s*([\d.]+)/i);
              const interestM = content.match(/Interest:\s*([\d.]+)/i);
              const sonarM = content.match(/Sonar:\s*([\d.]+)/i);
              const volumeM = content.match(/Volume:\s*([\d.]+)/i);

              // Butterfly
              const bfM = content.match(/(BUY \+1 Butterfly SPX[^@]*@([\d.]+)\s*LMT)/i);
              const bfStrikesM = content.match(/Butterfly[^/]*(\d{4,5})\/(\d{4,5})\/(\d{4,5})\s*(CALL|PUT)/i);

              // IC
              const icM = content.match(/(SELL -1 Iron Condor SPX[^@]*@([\d.]+)\s*LMT)/i);
              const icStrikesM = content.match(/Iron Condor[^/]*(\d{4,5})\/(\d{4,5})\/(\d{4,5})\/(\d{4,5})/i);

              // Sonar IC
              const sonarICM = content.match(/Sonar:[^S]*SELL -1 Iron Condor SPX[^@]*@([\d.]+)/i);
              const sonarStrikesM = content.match(/Sonar:[^S]*Iron Condor[^/]*(\d{4,5})\/(\d{4,5})\/(\d{4,5})\/(\d{4,5})/i);

              // Vertical
              const vtM = content.match(/(SELL -1 Vertical SPX[^@]*@([\d.]+)\s*LMT)/i);
              const vtStrikesM = content.match(/Vertical[^/]*(\d{4,5})\/(\d{4,5})\s*(CALL|PUT)/i);

              if (!priceM) continue; // not a signal message

              results.push({
                datetime: msg.timestamp,
                date: msgDate,
                time: msgTime,
                price: priceM ? priceM[1] : '',
                trend: trendM ? trendM[1] : '',
                predicted_close: predM ? predM[1] : '',
                strength: strM ? strM[1] : '',
                short_term: stM ? stM[1] : '',
                long_term: ltM ? ltM[1] : '',
                short_bias: sbM ? sbM[1] : '',
                long_bias: lbM ? lbM[1] : '',
                calls: callsM ? callsM[1] : '',
                puts: putsM ? putsM[1] : '',
                center: centerM ? centerM[1] : '',
                range: rangeM ? rangeM[1] : '',
                target1: t1M ? t1M[1] : '',
                target2: t2M ? t2M[1] : '',
                delta: deltaM ? deltaM[1] : '',
                gamma: gammaM ? gammaM[1] : '',
                interest: interestM ? interestM[1] : '',
                sonar: sonarM ? sonarM[1] : '',
                volume: volumeM ? volumeM[1] : '',
                bf_action: bfM ? 'BUY' : '',
                bf_strikes: bfStrikesM ? `${bfStrikesM[1]}/${bfStrikesM[2]}/${bfStrikesM[3]}` : '',
                bf_center: bfStrikesM ? bfStrikesM[2] : '',
                bf_upper: bfStrikesM ? bfStrikesM[1] : '',
                bf_lower: bfStrikesM ? bfStrikesM[3] : '',
                bf_price: bfM ? bfM[2] : '',
                ic_action: icM ? 'SELL' : '',
                ic_strikes: icStrikesM ? `${icStrikesM[1]}/${icStrikesM[2]}/${icStrikesM[3]}/${icStrikesM[4]}` : '',
                ic_price: icM ? icM[2] : '',
                sonar_action: sonarICM ? 'SELL' : '',
                sonar_strikes: sonarStrikesM ? `${sonarStrikesM[1]}/${sonarStrikesM[2]}/${sonarStrikesM[3]}/${sonarStrikesM[4]}` : '',
                sonar_price: sonarICM ? sonarICM[1] : '',
                vt_action: vtM ? 'SELL' : '',
                vt_strikes: vtStrikesM ? `${vtStrikesM[1]}/${vtStrikesM[2]}` : '',
                vt_side: vtStrikesM ? vtStrikesM[3] : '',
                vt_price: vtM ? vtM[2] : '',
              });
            }
            if (batch.length < 100) break;
            afterSnowflake = batch[batch.length - 1].id;
          }
          // Small delay between days to avoid rate limits
          await new Promise(r => setTimeout(r, 500));
        }
        return jsonResp({ total: results.length, signals: results }, 200, { 'Access-Control-Allow-Origin': '*' });
      } catch (e) {
        return jsonResp({ error: e.message }, 500, { 'Access-Control-Allow-Origin': '*' });
      }
    }

    // ── GET /vix-for-date ── No CORS restriction (script access), secured by secret
    if (url.pathname === '/vix-for-date' && request.method === 'GET') {
      const secret = request.headers.get('X-Sync-Secret');
      if (!secret || secret !== env.SYNC_SECRET) {
        return jsonResp({ error: 'Unauthorized' }, 401, {});
      }
      try {
        const dateStr = url.searchParams.get('date');
        if (!dateStr || !/^\d{4}-\d{2}-\d{2}$/.test(dateStr)) {
          return jsonResp({ error: 'Invalid date param, expected YYYY-MM-DD' }, 400, {});
        }
        const token = await getAccessToken(env);
        const [y, m, d] = dateStr.split('-').map(Number);
        // Use noon–22:00 UTC to stay within the ET trading day (avoids prev-day bleed)
        // 12:00 UTC = ~7-8 AM ET (before open), 22:00 UTC = ~5-6 PM ET (after close)
        const dayStart = Date.UTC(y, m - 1, d, 12, 0, 0);
        const dayEnd   = Date.UTC(y, m - 1, d, 22, 0, 0);
        const vixUrl = `https://api.schwabapi.com/marketdata/v1/pricehistory?symbol=%24VIX&periodType=day&period=1&frequencyType=minute&frequency=1&startDate=${dayStart}&endDate=${dayEnd}&needExtendedHoursData=true`;
        const spxUrl = `https://api.schwabapi.com/marketdata/v1/pricehistory?symbol=%24SPX&periodType=day&period=1&frequencyType=minute&frequency=1&startDate=${dayStart}&endDate=${dayEnd}&needExtendedHoursData=true`;
        const [vixData, spxData] = await Promise.all([fetchSchwabJSON(vixUrl, token), fetchSchwabJSON(spxUrl, token)]);
        function extractOHLC(candles) {
          if (!candles || !candles.length) return { open: null, close: null };
          candles.sort((a, b) => a.datetime - b.datetime);
          const openCandle  = candles.find(c => { const et = toET(new Date(c.datetime)); return et.getHours() * 60 + et.getMinutes() >= 570; });
          const closeCandle = candles.slice().reverse().find(c => { const et = toET(new Date(c.datetime)); return et.getHours() * 60 + et.getMinutes() <= 975; });
          return { open: openCandle ? parseFloat(openCandle.open.toFixed(2)) : null, close: closeCandle ? parseFloat(closeCandle.close.toFixed(2)) : null };
        }
        const vixOHLC = extractOHLC(vixData.candles);
        const spxOHLC = extractOHLC(spxData.candles);
        return jsonResp({ date: dateStr, vixOpen: vixOHLC.open, vixClose: vixOHLC.close, spxOpen: spxOHLC.open, spxClose: spxOHLC.close }, 200, {});
      } catch (e) {
        return jsonResp({ error: e.message }, 500, {});
      }
    }

    // ── GET /vix-bulk ── Fetch VIX+SPX open/close for a date range (requires sync secret)
    // Usage: /vix-bulk?start=2024-08-01&end=2024-09-01&secret=xxx
    if (url.pathname === '/vix-bulk' && request.method === 'GET') {
      const secret = request.headers.get('X-Sync-Secret') || url.searchParams.get('secret');
      if (!secret || secret !== env.SYNC_SECRET) {
        return jsonResp({ error: 'Unauthorized' }, 401, { 'Access-Control-Allow-Origin': '*' });
      }
      try {
        const startDate = url.searchParams.get('start');
        const endDate = url.searchParams.get('end');
        if (!startDate || !endDate) return jsonResp({ error: 'Need start and end params (YYYY-MM-DD)' }, 400, { 'Access-Control-Allow-Origin': '*' });
        const token = await getAccessToken(env);
        const [sy, sm, sd] = startDate.split('-').map(Number);
        const [ey, em, ed] = endDate.split('-').map(Number);
        const startMs = Date.UTC(sy, sm - 1, sd, 0, 0, 0);
        const endMs = Date.UTC(ey, em - 1, ed, 23, 59, 59);
        const vixUrl = `https://api.schwabapi.com/marketdata/v1/pricehistory?symbol=%24VIX&periodType=day&period=1&frequencyType=minute&frequency=1&startDate=${startMs}&endDate=${endMs}&needExtendedHoursData=true`;
        const spxUrl = `https://api.schwabapi.com/marketdata/v1/pricehistory?symbol=%24SPX&periodType=day&period=1&frequencyType=minute&frequency=1&startDate=${startMs}&endDate=${endMs}&needExtendedHoursData=true`;
        const [vixData, spxData] = await Promise.all([fetchSchwabJSON(vixUrl, token), fetchSchwabJSON(spxUrl, token)]);

        // Group candles by date, extract 9:31 open and last candle <= 4:15 close
        function groupByDate(candles) {
          if (!candles || !candles.length) return {};
          const byDate = {};
          for (const c of candles) {
            const et = toET(new Date(c.datetime));
            const key = `${et.getFullYear()}-${String(et.getMonth()+1).padStart(2,'0')}-${String(et.getDate()).padStart(2,'0')}`;
            if (!byDate[key]) byDate[key] = [];
            byDate[key].push({ ...c, etMins: et.getHours() * 60 + et.getMinutes() });
          }
          const result = {};
          for (const [date, dayCandles] of Object.entries(byDate)) {
            dayCandles.sort((a, b) => a.datetime - b.datetime);
            // Open: first candle at or after 9:30 (570 mins)
            const openCandle = dayCandles.find(c => c.etMins >= 570);
            // Close: last candle at or before 4:15 (975 mins)
            const closeCandle = dayCandles.slice().reverse().find(c => c.etMins <= 975);
            result[date] = {
              open: openCandle ? parseFloat(openCandle.open.toFixed(2)) : null,
              close: closeCandle ? parseFloat(closeCandle.close.toFixed(2)) : null,
            };
          }
          return result;
        }
        const vixByDate = groupByDate(vixData.candles);
        const spxByDate = groupByDate(spxData.candles);
        const dates = [...new Set([...Object.keys(vixByDate), ...Object.keys(spxByDate)])].sort();
        const rows = dates.map(d => ({
          date: d,
          vixOpen: vixByDate[d]?.open ?? null,
          vixClose: vixByDate[d]?.close ?? null,
          spxOpen: spxByDate[d]?.open ?? null,
          spxClose: spxByDate[d]?.close ?? null,
        }));
        return jsonResp({ count: rows.length, rows }, 200, { 'Access-Control-Allow-Origin': '*' });
      } catch (e) {
        return jsonResp({ error: e.message }, 500, { 'Access-Control-Allow-Origin': '*' });
      }
    }

    // ── GET /vix-bulk-daily ── Fetch VIX+SPX daily open/close for a date range (years of history)
    // Usage: /vix-bulk-daily?start=2024-08-01&end=2026-04-01&secret=xxx
    if (url.pathname === '/vix-bulk-daily' && request.method === 'GET') {
      const secret = request.headers.get('X-Sync-Secret') || url.searchParams.get('secret');
      if (!secret || secret !== env.SYNC_SECRET) {
        return jsonResp({ error: 'Unauthorized' }, 401, { 'Access-Control-Allow-Origin': '*' });
      }
      try {
        const startDate = url.searchParams.get('start');
        const endDate = url.searchParams.get('end');
        if (!startDate || !endDate) return jsonResp({ error: 'Need start and end params (YYYY-MM-DD)' }, 400, { 'Access-Control-Allow-Origin': '*' });
        const token = await getAccessToken(env);
        const [sy, sm, sd] = startDate.split('-').map(Number);
        const [ey, em, ed] = endDate.split('-').map(Number);
        const startMs = Date.UTC(sy, sm - 1, sd, 0, 0, 0);
        const endMs = Date.UTC(ey, em - 1, ed, 23, 59, 59);
        const vixUrl = `https://api.schwabapi.com/marketdata/v1/pricehistory?symbol=%24VIX&periodType=year&period=2&frequencyType=daily&frequency=1&startDate=${startMs}&endDate=${endMs}`;
        const spxUrl = `https://api.schwabapi.com/marketdata/v1/pricehistory?symbol=%24SPX&periodType=year&period=2&frequencyType=daily&frequency=1&startDate=${startMs}&endDate=${endMs}`;
        const [vixData, spxData] = await Promise.all([fetchSchwabJSON(vixUrl, token), fetchSchwabJSON(spxUrl, token)]);
        function toDateMap(candles) {
          if (!candles || !candles.length) return {};
          const m = {};
          for (const c of candles) {
            const et = toET(new Date(c.datetime));
            const key = `${et.getFullYear()}-${String(et.getMonth()+1).padStart(2,'0')}-${String(et.getDate()).padStart(2,'0')}`;
            m[key] = { open: parseFloat(c.open.toFixed(2)), close: parseFloat(c.close.toFixed(2)) };
          }
          return m;
        }
        const vixByDate = toDateMap(vixData.candles);
        const spxByDate = toDateMap(spxData.candles);
        const dates = [...new Set([...Object.keys(vixByDate), ...Object.keys(spxByDate)])].sort();
        const rows = dates.map(d => ({
          date: d,
          vixOpen: vixByDate[d]?.open ?? null,
          vixClose: vixByDate[d]?.close ?? null,
          spxOpen: spxByDate[d]?.open ?? null,
          spxClose: spxByDate[d]?.close ?? null,
        }));
        return jsonResp({ count: rows.length, rows }, 200, { 'Access-Control-Allow-Origin': '*' });
      } catch (e) {
        return jsonResp({ error: e.message }, 500, { 'Access-Control-Allow-Origin': '*' });
      }
    }

    // ── GET /kv-debug ── List KV keys (requires sync secret)
    if (url.pathname === '/kv-debug' && request.method === 'GET') {
      const secret = request.headers.get('X-Sync-Secret') || url.searchParams.get('secret');
      if (!secret || secret !== env.SYNC_SECRET) {
        return jsonResp({ error: 'Unauthorized' }, 401, { 'Access-Control-Allow-Origin': '*' });
      }
      const list = await env.SIGNAL_KV.list();
      return jsonResp({ keys: list.keys.map(k => k.name) }, 200, { 'Access-Control-Allow-Origin': '*' });
    }

    // ── GET /trigger ── Manually trigger the scheduled handler (requires sync secret)
    if (url.pathname === '/trigger' && request.method === 'GET') {
      const secret = request.headers.get('X-Sync-Secret') || url.searchParams.get('secret');
      if (!secret || secret !== env.SYNC_SECRET) {
        return jsonResp({ error: 'Unauthorized' }, 401, { 'Access-Control-Allow-Origin': '*' });
      }
      try {
        const result = await handleScheduled(env);
        result.date = result.date || new Date().toISOString();
        await env.SIGNAL_KV.put('last_run', JSON.stringify(result));
        return jsonResp(result, 200, { 'Access-Control-Allow-Origin': '*' });
      } catch (e) {
        return jsonResp({ error: e.message }, 500, { 'Access-Control-Allow-Origin': '*' });
      }
    }

    // ── POST /discord-send ── Browser/external endpoint that lets us retire
    // the standalone discord-proxy worker. Same body shape as the legacy
    // worker: { userId, message }. Optionally accepts Bearer PROXY_SECRET.
    if (url.pathname === '/link-notify' && request.method === 'POST') {
      // Worker-to-worker Discord note (skipper). LINK_SECRET-gated.
      if (!env.LINK_SECRET || request.headers.get('X-Link-Secret') !== env.LINK_SECRET) {
        return jsonResp({ error: 'Unauthorized' }, 401, corsHeaders);
      }
      try {
        const { text, fanoutText } = await request.json();
        const dcRaw = await env.SIGNAL_KV.get('discord_config');
        if (!dcRaw) return jsonResp({ ok: false, error: 'no discord_config' }, 200, corsHeaders);
        const dc = JSON.parse(dcRaw);
        // Channel post is OPTIONAL — skip it when no text (fanout-only mode), so
        // paper trades can relay to subscribers WITHOUT cluttering the channel.
        let r = { ok: true };
        if (text && String(text).trim()) {
          r = await sendDiscordDM(env, dc.channelId, String(text).slice(0, 1800), dc.proxyUrl);
        }
        // fanoutText (optional) = subscriber-facing trade message, relayed to the
        // signal_subscribers DM list. Set for EVERY skipper trade entry (paper or
        // live fill) so subscribers get each trade regardless of our exec mode.
        let fanned = 0;
        if (fanoutText) {
          // Dedup backstop (2026-07-10 spam incident: racing skipper ticks
          // re-sent the same BOBF relay ~8×): identical fanout text within
          // 30 min is always a duplicate from a retrying/racing caller —
          // each trade fans out once per day and every message differs.
          let dupF = false;
          try {
            const sF = String(fanoutText); let hF = 0;
            for (let i = 0; i < sF.length; i++) hF = (hF * 31 + sF.charCodeAt(i)) >>> 0;
            const dkF = `fanout_dedup_${hF}`;
            if (await env.SIGNAL_KV.get(dkF)) dupF = true;
            else await env.SIGNAL_KV.put(dkF, '1', { expirationTtl: 1800 });
          } catch (_) {}
          if (dupF) {
            console.warn('[link-notify/fanout] duplicate suppressed');
          } else {
            try {
              let msgOut = String(fanoutText);
              // Fat-gamma tier footer on M8BF trade relays (info only, owner
              // 2026-07-24): story, not sizing advice. \bM8BF\b cannot match
              // BOBF/PNBF/GXBF, so only the M8BF relay gets the line.
              try {
                if (/\bM8BF\b/.test(msgOut) && !/fat-gamma/i.test(msgOut)) {
                  const g = await gexGateEval(env, isoDateET(toET(new Date())));
                  const p = gexFatTierP(g.rank);
                  if (p != null && !g.skip) {
                    msgOut += `\n-# 🟢 Fat-gamma tier (p${p}): prev-session dealer gamma in its upper range — historically 72% M8BF win rate in this tier vs 66% on other days.`;
                  }
                }
              } catch (_) {}
              const o = await fanoutSubscribers(env, msgOut.slice(0, 1800));
              fanned = o.filter(x => x.ok).length;
            } catch (e) { console.warn('[link-notify/fanout]', e.message); }
          }
        }
        return jsonResp({ ok: !!r.ok, fanned }, 200, corsHeaders);
      } catch (e) { return jsonResp({ ok: false, error: e.message }, 400, corsHeaders); }
    }

    if (url.pathname === '/discord-send' && request.method === 'POST') {
      const cors = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'POST, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type, Authorization',
      };
      // AUTH (2026-07-06 audit C1): this route DMs arbitrary users as the bot,
      // so it MUST be secret-gated. The old check was bypassable two ways —
      // skipped entirely when PROXY_SECRET was unset, and `origin.length>0`
      // treated any (forgeable) Origin header as a trusted browser. No repo
      // page calls /discord-send, so lock it hard with SYNC_SECRET (header or
      // ?secret=, same pattern as /trigger). A bearer PROXY_SECRET still works
      // for any legacy caller that used it.
      {
        const provided = request.headers.get('X-Sync-Secret') || url.searchParams.get('secret') || '';
        const bearer = (request.headers.get('Authorization') || '').replace(/^Bearer\s+/i, '');
        const ok = (env.SYNC_SECRET && provided === env.SYNC_SECRET) ||
                   (env.PROXY_SECRET && bearer === env.PROXY_SECRET);
        if (!ok) return jsonResp({ ok: false, error: 'Unauthorized' }, 401, cors);
      }
      try {
        const body = await request.json();
        // Accept either body.embed (Option E rich card) or body.message (legacy text)
        const payload = body.embed || body.message;
        const result = await sendDiscordDM(env, body.userId, payload);
        return jsonResp(result, result.ok ? 200 : 500, cors);
      } catch (e) {
        return jsonResp({ ok: false, error: e.message }, 400, cors);
      }
    }

    // ── GET /history ── Public read of the KV-backed history store.
    //   GET /history                  → full array
    //   GET /history?date=YYYY-MM-DD  → single date row
    //   GET /history?since=YYYY-MM-DD → all rows >= date
    // Public CORS (no auth) — same audience as raw.githubusercontent.com was.
    if (url.pathname === '/history' && request.method === 'GET') {
      const cors = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type',
        // Short cache so writes propagate quickly (KV is fast anyway)
        'Cache-Control': 'public, max-age=15',
      };
      try {
        const data = await getHistory(env);
        const dateQ = url.searchParams.get('date');
        const sinceQ = url.searchParams.get('since');
        if (dateQ) {
          const row = (data || []).find(r => r.date === dateQ) || null;
          return jsonResp({ date: dateQ, row }, 200, cors);
        }
        if (sinceQ) {
          const rows = (data || []).filter(r => r.date >= sinceQ);
          return jsonResp({ since: sinceQ, count: rows.length, rows }, 200, cors);
        }
        return jsonResp(data || [], 200, cors);
      } catch (e) {
        return jsonResp({ error: e.message }, 500, cors);
      }
    }

    // ── POST /history-migrate ── Force-seed KV from GitHub raw.
    // One-time call after deploy (or after KV wipe). Returns row count + diff.
    if (url.pathname === '/history-migrate' && request.method === 'POST') {
      const secret = request.headers.get('X-Sync-Secret') || url.searchParams.get('secret');
      if (!secret || secret !== env.SYNC_SECRET) {
        return jsonResp({ error: 'Unauthorized' }, 401, { 'Access-Control-Allow-Origin': '*' });
      }
      const cors = { 'Access-Control-Allow-Origin': '*' };
      try {
        const ghResp = await fetch(`${HISTORY_GH_RAW}?t=${Date.now()}`,
          { headers: { 'User-Agent': 'schwab-proxy', 'Cache-Control': 'no-cache' } });
        if (!ghResp.ok) return jsonResp({ error: `GitHub raw ${ghResp.status}` }, 500, cors);
        const ghData = await ghResp.json();
        if (!Array.isArray(ghData)) return jsonResp({ error: 'GitHub raw returned non-array' }, 500, cors);
        // Snapshot current KV state before overwriting
        let prevCount = 0;
        try {
          const prev = await env.SIGNAL_KV.get(HISTORY_KV_KEY);
          if (prev) {
            const p = JSON.parse(prev);
            prevCount = Array.isArray(p) ? p.length : 0;
            await backupHistorySnapshot(env, p, 'pre-migrate', { source: 'history-migrate' });
          }
        } catch (_) {}
        await env.SIGNAL_KV.put(HISTORY_KV_KEY, JSON.stringify(ghData));
        await logEvent(env, 'info', 'history-migrate', 'KV seeded from GitHub raw',
                       { prevCount, newCount: ghData.length });
        return jsonResp({
          migrated: true, prevCount, newCount: ghData.length,
          delta: ghData.length - prevCount,
        }, 200, cors);
      } catch (e) {
        return jsonResp({ error: e.message }, 500, cors);
      }
    }

    // ── GET /logs ── Read daily event log from KV. Auth required.
    //   GET /logs                            → today's events
    //   GET /logs?date=YYYY-MM-DD            → that day's events
    //   GET /logs?date=...&level=error|warn  → filter by level
    //   GET /logs?date=...&tag=morning       → filter by tag
    // GET /debug-morning-log?date=YYYY-MM-DD
    // Read-only public endpoint that returns ONLY morning-tagged log entries
    // for a single date. No auth — but it only exposes signal-related logs
    // (tag='morning'), not credentials/tokens/PII. Temporary diagnostic.
    if (url.pathname === '/debug-morning-log' && request.method === 'GET') {
      const cors = { 'Access-Control-Allow-Origin': '*' };
      try {
        const dateParam = url.searchParams.get('date') || isoDateET(toET(new Date()));
        if (!/^\d{4}-\d{2}-\d{2}$/.test(dateParam)) {
          return jsonResp({ error: 'invalid date format' }, 400, cors);
        }
        const raw = await env.SIGNAL_KV.get(`daily_log_${dateParam}`);
        let entries = raw ? JSON.parse(raw) : [];
        if (!Array.isArray(entries)) entries = [];
        // Filter to morning-tagged only — that's the only thing safe to expose
        const morningOnly = entries.filter(e => e.tag === 'morning');
        return jsonResp({ date: dateParam, count: morningOnly.length, entries: morningOnly }, 200, cors);
      } catch (e) {
        return jsonResp({ error: e.message }, 500, cors);
      }
    }

    if (url.pathname === '/logs' && request.method === 'GET') {
      const secret = request.headers.get('X-Sync-Secret') || url.searchParams.get('secret');
      if (!secret || secret !== env.SYNC_SECRET) {
        return jsonResp({ error: 'Unauthorized' }, 401, { 'Access-Control-Allow-Origin': '*' });
      }
      const cors = { 'Access-Control-Allow-Origin': '*' };
      try {
        const etNowLg = toET(new Date());
        const dateParam = url.searchParams.get('date') || isoDateET(etNowLg);
        if (!/^\d{4}-\d{2}-\d{2}$/.test(dateParam)) {
          return jsonResp({ error: 'invalid date format (need YYYY-MM-DD)' }, 400, cors);
        }
        const levelFilter = url.searchParams.get('level');
        const tagFilter = url.searchParams.get('tag');
        const raw = await env.SIGNAL_KV.get(`daily_log_${dateParam}`);
        let entries = raw ? JSON.parse(raw) : [];
        if (!Array.isArray(entries)) entries = [];
        if (levelFilter) entries = entries.filter(e => e.level === levelFilter);
        if (tagFilter) entries = entries.filter(e => e.tag === tagFilter);
        return jsonResp({ date: dateParam, count: entries.length, entries }, 200, cors);
      } catch (e) {
        return jsonResp({ error: e.message }, 500, cors);
      }
    }

    // ── GET /history-backups ── List/inspect/restore pre-write history snapshots.
    //   GET  /history-backups                    → list last 10 backup metadata
    //   GET  /history-backups?key=<backupKey>    → return full JSON content of one backup
    //   POST /history-backups?restore=<key>      → push that backup back to GitHub (DESTRUCTIVE)
    // All operations require X-Sync-Secret. Restores REPLACE current history.
    if (url.pathname === '/history-backups') {
      const secret = request.headers.get('X-Sync-Secret') || url.searchParams.get('secret');
      if (!secret || secret !== env.SYNC_SECRET) {
        return jsonResp({ error: 'Unauthorized' }, 401, { 'Access-Control-Allow-Origin': '*' });
      }
      const cors = { 'Access-Control-Allow-Origin': '*' };
      try {
        if (request.method === 'GET') {
          const key = url.searchParams.get('key');
          if (key) {
            if (!key.startsWith('history_backup_')) {
              return jsonResp({ error: 'invalid key prefix' }, 400, cors);
            }
            const raw = await env.SIGNAL_KV.get(key);
            if (!raw) return jsonResp({ error: 'backup not found (may have expired)' }, 404, cors);
            return jsonResp({ key, content: JSON.parse(raw) }, 200, cors);
          }
          const idxRaw = await env.SIGNAL_KV.get('history_backups_index');
          const idx = idxRaw ? JSON.parse(idxRaw) : [];
          return jsonResp({ backups: idx, count: idx.length }, 200, cors);
        }
        if (request.method === 'POST') {
          const restoreKey = url.searchParams.get('restore');
          if (!restoreKey) return jsonResp({ error: 'missing ?restore=<key>' }, 400, cors);
          if (!restoreKey.startsWith('history_backup_')) {
            return jsonResp({ error: 'invalid key prefix' }, 400, cors);
          }
          const backupRaw = await env.SIGNAL_KV.get(restoreKey);
          if (!backupRaw) return jsonResp({ error: 'backup not found' }, 404, cors);
          const restoredContent = JSON.parse(backupRaw);
          // Push directly to GitHub, bypassing the upsert merge logic
          const token = env.GITHUB_TOKEN;
          if (!token) return jsonResp({ error: 'GITHUB_TOKEN not set' }, 500, cors);
          const apiUrl = 'https://api.github.com/repos/rava8989/brave/contents/history_data.json';
          const ghHeaders = {
            'Authorization': `Bearer ${token}`,
            'Accept': 'application/vnd.github+json',
            'User-Agent': 'schwab-proxy-worker/1.0',
            'X-GitHub-Api-Version': '2022-11-28',
          };
          const getResp = await fetch(apiUrl, { headers: ghHeaders });
          if (!getResp.ok) return jsonResp({ error: `GitHub GET ${getResp.status}` }, 500, cors);
          const meta = await getResp.json();
          // Snapshot the CURRENT state before overwriting (safety net for the restore itself)
          const currentContent = JSON.parse(atob(meta.content.replace(/\n/g, '')));
          await backupHistorySnapshot(env, currentContent, 'pre-restore', { restoredFrom: restoreKey });
          const putResp = await fetch(apiUrl, {
            method: 'PUT',
            headers: { ...ghHeaders, 'Content-Type': 'application/json' },
            body: JSON.stringify({
              message: `restore: history rewound to backup ${restoreKey}`,
              content: btoa(JSON.stringify(restoredContent, null, 0)),
              sha: meta.sha,
            }),
          });
          if (!putResp.ok) {
            const err = await putResp.text();
            return jsonResp({ error: `GitHub PUT ${putResp.status}: ${err}` }, 500, cors);
          }
          return jsonResp({ restored: restoreKey, rows: restoredContent.length }, 200, cors);
        }
        return jsonResp({ error: 'method not allowed' }, 405, cors);
      } catch (e) {
        return jsonResp({ error: e.message }, 500, cors);
      }
    }

    // ── GET /schwab-health ── Public circuit-breaker endpoint
    // Returns current Schwab refresh-token health so the dashboard can
    // display a red "re-authenticate now" banner when KV says the token
    // is valid but Schwab has been rejecting it. Populated by
    // recordRefreshHealth() in every token-refresh path.
    if (url.pathname === '/schwab-health' && request.method === 'GET') {
      const publicCors = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type',
        'Access-Control-Max-Age': '86400',
      };
      try {
        const raw = await env.SIGNAL_KV.get('schwab_refresh_state');
        const state = raw ? JSON.parse(raw) : { ok: null, lastSuccess: null, msg: 'never recorded' };
        // Add a computed "stale" flag so the browser doesn't need to do clock math
        const now = Date.now();
        const lastSuccess = state.lastSuccess || 0;
        const lastError = state.lastError || 0;
        const minutesSinceSuccess = lastSuccess ? Math.round((now - lastSuccess) / 60000) : null;
        const minutesSinceError = lastError ? Math.round((now - lastError) / 60000) : null;
        // Show red banner when: explicit failure + 3+ consecutive errors + last success >30min ago
        const alarming = state.ok === false
          && (state.consecutiveErrors || 0) >= 3
          && (!minutesSinceSuccess || minutesSinceSuccess > 30);
        return jsonResp({
          ...state,
          now,
          minutesSinceSuccess,
          minutesSinceError,
          alarming,
        }, 200, publicCors);
      } catch (e) {
        return jsonResp({ error: e.message }, 500, publicCors);
      }
    }

    // ── GET /health ── Unified public health check for uptime monitoring.
    // Returns a structured view of every critical subsystem (morning signal,
    // EOD, Schwab refresh, cron liveness, GEX freshness). The `alarming`
    // boolean flips true whenever any subsystem is degraded, with specific
    // reasons in `alerts[]`. Designed to be polled every 5-10 min by an
    // external uptime monitor so silent failures surface before the user
    // notices Discord stayed quiet.
    // ── GET /diagonal-trigger ── Manually run handleDiagonalTrade NOW
    // Bypasses the normal 12:30 ET window so we can verify the open path
    // end-to-end without waiting for the cron. Auth-required.
    // ── GET /earnings-board ── today's board (public read for the page) ──
    if (url.pathname === '/earnings-pipeline' && request.method === 'GET') {
      const force = url.searchParams.get('force') === '1';
      let cached = force ? null : await env.SIGNAL_KV.get('earn_pipeline_cache');
      if (!cached) { const d = await earnRefreshPipeline(env); cached = JSON.stringify(d || { built: isoDateET(toET()), rows: [] }); }
      return jsonResp(JSON.parse(cached), 200, { 'Access-Control-Allow-Origin': '*' });
    }

    if (url.pathname === '/earnings-board' && request.method === 'GET') {
      const iso = isoDateET(toET());
      const raw = await env.SIGNAL_KV.get(`earn_board_${iso}`)
               || await env.SIGNAL_KV.get(`earn_board_${isoDateET(prevTrade(toET()))}`);
      const logRaw = await env.SIGNAL_KV.get('earn_log');
      return jsonResp({ board: raw ? JSON.parse(raw) : null,
                        live_log: logRaw ? JSON.parse(logRaw) : [] },
        200, { 'Access-Control-Allow-Origin': '*' });
    }

    // ── GET /earnings-status ── "where the package stands right now": EARNINGS
    // (+ held names) during a signal-night hold, else the parking sleeve
    // (SPY/GLD/CASH). Public; the status card on earnings-play.html polls it.
    if (url.pathname === '/earnings-status' && request.method === 'GET') {
      const st = await earnCurrentStatus(env);
      return jsonResp(st, 200, { 'Access-Control-Allow-Origin': '*', 'Cache-Control': 'public, max-age=120' });
    }

    // ── GET /earnings-scan-trigger?step=morning|rescore|final|exit|collect&date=ISO ──
    // Manual/dry-run driver. Auth. date override runs the board builder for
    // any date WITHOUT sending messages unless &send=1.
    if (url.pathname === '/earnings-scan-trigger' && request.method === 'GET') {
      const secret = request.headers.get('X-Sync-Secret') || url.searchParams.get('secret');
      if (!secret || (secret !== env.SYNC_SECRET && secret !== env.GEXM_TRIGGER_TOKEN)) {
        return jsonResp({ error: 'Unauthorized' }, 401, { 'Access-Control-Allow-Origin': '*' });
      }
      const token = await getAccessToken(env);
      const step = url.searchParams.get('step') || 'board';
      const dateISO = url.searchParams.get('date') || isoDateET(toET());
      if (step === 'board') {
        const b = await earnBuildBoard(env, token, dateISO,
          { withIntraday: url.searchParams.get('intraday') === '1' });
        // store=1 (owner 2026-07-29): persist the rebuilt board so the page
        // shows the current full-universe list, not a stale pre-fix snapshot.
        if (url.searchParams.get('store') === '1') {
          await env.SIGNAL_KV.put(`earn_board_${dateISO}`, JSON.stringify(b), { expirationTtl: 3 * 86400 });
        }
        const setl = url.searchParams.get('setlongs');
        if (setl != null) {
          await env.SIGNAL_KV.put(`earn_actual_longs_${dateISO}`,
            JSON.stringify(setl ? setl.split(',').map(s => s.trim().toUpperCase()) : []), { expirationTtl: 3 * 86400 });
        }
        if (url.searchParams.get('send') === '1') {
          const mode = (await env.SIGNAL_KV.get('earnings_mode')) || 'paper';
          await earnSend(env, earnBoardMsg(b, mode,
            url.searchParams.get('stage') === 'final' ? 'final' : 'morning'));
        }
        return jsonResp(b);
      }
      const etNow = toET();
            if (step === 'backfill-log') {
        // One-off (2026-07-29): reconstruct July LONG results from the FINAL
        // messages in the owner DM channel (boards expired from KV after 3d).
        const from = url.searchParams.get('from'), to = url.searchParams.get('to');
        const dc = JSON.parse(await env.SIGNAL_KV.get('discord_config') || '{}');
        if (!dc.channelId || !env.DISCORD_USER_TOKEN) return jsonResp({ error: 'no discord config' }, 500, {});
        // FINALs reach subscribers via the earnings webhook — its channel is the
        // one the user token can actually read (dc.channelId is the bot-DM, which
        // the scrape can't see; that made every July probe come back empty).
        let scanChannel = dc.channelId;
        try {
          const wh = await env.SIGNAL_KV.get('earnings_webhook_url');
          if (wh) { const wj = await (await fetch(wh)).json(); if (wj && wj.channel_id) scanChannel = wj.channel_id; }
        } catch (_) {}
        const found = [], added = [];
        const d0 = new Date(from + 'T12:00:00Z'), d1 = new Date(to + 'T12:00:00Z');
        for (let d = new Date(d0); d <= d1; d.setUTCDate(d.getUTCDate() + 1)) {
          const iso = d.toISOString().slice(0, 10);
          const dow = d.getUTCDay();
          if (dow === 0 || dow === 6) continue;
          let msgs = [];
          try { msgs = await scrapeRawEarnMsgs(env.DISCORD_USER_TOKEN, scanChannel, iso, url.searchParams.get('raw') === '1'); } catch (_) {}
          if (url.searchParams.get('raw') === '1') { found.push({ date: iso, msgs }); continue; }
          for (const c of msgs) {
            if (!c.includes('[EARNINGS] FINAL BOARD')) continue;
            const act = c.match(/ACTION: buy at 3:45[^\n]*?—([^\n]*)/) || c.match(/ACTION: buy at 3:45[^\n]*?-([^\n]*)/);
            const tks = act ? [...act[1].matchAll(/([A-Z]{1,5}) [0-9.]+%/g)].map(x => x[1]) : [];
            found.push({ date: iso, longs: tks });
          }
        }
        // manual override: &manual=YYYY-MM-DD:TICK|TICK,YYYY-MM-DD:TICK
        const man = url.searchParams.get('manual');
        if (man) {
          found.length = 0;
          for (const part of man.split(',')) {
            const [dd, tks] = part.split(':');
            found.push({ date: dd, longs: (tks || '').split('|').filter(Boolean) });
          }
        }
        // price + append
        const rows = [];
        for (const f of found) {
          for (const tk of (f.longs || [])) {   // raw=1 entries carry msgs, not longs
            try {
              const entryD = new Date(f.date + 'T12:00:00Z');
              const j = await fetchSchwabJSON(
                `https://api.schwabapi.com/marketdata/v1/pricehistory?symbol=${encodeURIComponent(tk)}` +
                `&periodType=month&period=1&frequencyType=daily&frequency=1&startDate=${entryD.getTime() - 3 * 86400000}&endDate=${entryD.getTime() + 6 * 86400000}`, token, env);
              const cs = (j?.candles || []).map(c => ({ d: isoDateET(toET(new Date(c.datetime))), o: c.open, c: c.close }));
              const i = cs.findIndex(c => c.d === f.date);
              if (i >= 0 && cs[i + 1]) {
                rows.push({ date: f.date, ticker: tk, pw_ratio: null, deep_itm_usd: 0, runup_2w: null,
                            base_rate: null, verdict: 'LONG', move_24h: +((cs[i + 1].o / cs[i].c - 1).toFixed(4)), pl_r: null, live: true });
              }
            } catch (e) { console.warn('[earn-backfill]', tk, e.message); }
          }
        }
        if (rows.length) {
          await githubUpsertResearchFile(env, 'data/earnings_play_today.json',
            cur => {
              cur.log = cur.log || [];
              for (const row of rows) {
                if (!cur.log.some(x => x.date === row.date && x.ticker === row.ticker)) { cur.log.unshift(row); added.push(`${row.date}:${row.ticker}`); }
              }
              cur.log.sort((a, b) => a.date < b.date ? 1 : -1);
              return cur;
            }, `auto: earnings live-log backfill ${from}..${to}`);
        }
        return jsonResp({ finals: found, priced: rows, added, scanChannel, viaWebhook: scanChannel !== dc.channelId });
      }
      if (step === 'morning') { await earnMorningJob(env, etNow, token); }
      else if (step === 'rescore') { await earnRescoreJob(env, etNow, token); }
      else if (step === 'final') { await earnFinalJob(env, etNow, token); }
      else if (step === 'exit') { await earnExitJob(env, etNow, token); }
      else if (step === 'collect') { await earnNightlyCollect(env, etNow, token); }
      return jsonResp({ ok: true, step });
    }

    // ── GET /earnings-test-dm ── one test DM to the owner's private channel
    // (discord_config.channelId — the watchdog path; NEVER subscribers). Auth.
    if (url.pathname === '/earnings-test-dm' && request.method === 'GET') {
      const secret = request.headers.get('X-Sync-Secret') || url.searchParams.get('secret');
      if (!secret || (secret !== env.SYNC_SECRET && secret !== env.GEXM_TRIGGER_TOKEN)) {
        return jsonResp({ error: 'Unauthorized' }, 401, { 'Access-Control-Allow-Origin': '*' });
      }
      const dcRaw = await env.SIGNAL_KV.get('discord_config');
      if (!dcRaw) return jsonResp({ ok: false, error: 'no discord_config in KV' });
      const dc = JSON.parse(dcRaw);
      const r = await sendDiscordDM(env, dc.channelId,
        '🌙 **[EARNINGS] Private DM wired.** You (and only you) get: morning preview ~9:15 · final board 3:30 · sell reminder 9:32. Subscribers never see these. — test, no signal',
        dc.proxyUrl);
      return jsonResp({ ok: r.ok, source: r.source || null, error: r.error || null });
    }

    // ── GET /earnings-card-test ── Render the earnings CARD and (unless ?png=1)
    // DM it to the owner ONLY — never the subscriber webhook. Validates every
    // visual state before flipping KV `earnings_card_mode` → 'on'.
    //   ?sample=A|B|C|D|E  synthetic board for that state
    //   ?date=YYYY-MM-DD   real board from KV earn_board_<date>
    //   (default)          today's board from KV, else sample C
    //   ?png=1             return the raw PNG (eyeball in a browser, no Discord)
    if (url.pathname === '/earnings-card-test' && request.method === 'GET') {
      const secret = request.headers.get('X-Sync-Secret') || url.searchParams.get('secret');
      if (!secret || (secret !== env.SYNC_SECRET && secret !== env.GEXM_TRIGGER_TOKEN)) {
        return jsonResp({ error: 'Unauthorized' }, 401, { 'Access-Control-Allow-Origin': '*' });
      }
      const g = (v) => v;   // gate value passthrough for readability
      const SAMPLES = {
        A: { date: '2026-07-22', vix: { ratio: 0.82, ok: true }, park: { sleeve: 'SPY' },
             board: [{ ticker: 'TSLA', when: 'AMC', verdict: 'LONG', pw_ratio: 3.4, g1: g(true), g2: g(true), g3: g(true), g4: g(true), notes: [] }],
             longs: [{ ticker: 'TSLA' }] },
        B: { date: '2026-07-14', vix: { ratio: 0.81, ok: true }, park: null,
             board: [
               { ticker: 'JPM', when: 'BMO', verdict: 'LONG', pw_ratio: 3.1, g1: true, g2: true, g3: true, g4: true, notes: [] },
               { ticker: 'C', when: 'BMO', verdict: 'LONG', pw_ratio: 2.9, g1: true, g2: true, g3: true, g4: true, notes: [] },
               { ticker: 'WFC', when: 'BMO', verdict: 'LONG', pw_ratio: 2.6, g1: true, g2: true, g3: true, g4: true, notes: [] }],
             longs: [{ ticker: 'JPM' }, { ticker: 'C' }, { ticker: 'WFC' }] },
        C: { date: '2026-07-17', vix: { ratio: 0.9, ok: true }, park: { sleeve: 'SPY' },
             board: [
               { ticker: 'NFLX', when: 'AMC', verdict: 'LONG', pw_ratio: 3.1, g1: true, g2: true, g3: true, g4: true, notes: [] },
               { ticker: 'GS', when: 'BMO', verdict: 'CROWDED', pw_ratio: 5.8, g1: false, g2: true, g3: true, g4: true, notes: [] },
               { ticker: 'IBM', when: 'AMC', verdict: 'PASS', pw_ratio: 2.7, g1: true, g2: false, g3: false, g4: true, notes: [] }],
             longs: [{ ticker: 'NFLX' }] },
        D: { date: '2026-07-11', vix: { ratio: 0.8, ok: true }, park: { sleeve: 'SPY' }, board: [], longs: [] },
        E: { date: '2026-08-06', vix: { ratio: 1.34, ok: false }, park: { sleeve: 'CASH' },
             board: [{ ticker: 'DIS', when: 'AMC', verdict: 'PASS', pw_ratio: 3.0, g1: true, g2: true, g3: true, g4: false, notes: [] }],
             longs: [] },
      };
      let b = null;
      const samp = (url.searchParams.get('sample') || '').toUpperCase();
      const dq = url.searchParams.get('date');
      // live=1 (2026-07-29): build a FRESH board and render it — no KV write,
      // no send. Lets the owner preview exactly what the next card looks like.
      if (url.searchParams.get('live') === '1') {
        const tokenL = await getAccessToken(env);
        b = await earnBuildBoard(env, tokenL, isoDateET(toET()), { withIntraday: true });
        b.final = true;
      }
      else if (samp && SAMPLES[samp]) b = SAMPLES[samp];
      else if (dq && /^\d{4}-\d{2}-\d{2}$/.test(dq)) {
        const raw = await env.SIGNAL_KV.get(`earn_board_${dq}`);
        b = raw ? JSON.parse(raw) : null;
      } else {
        const raw = await env.SIGNAL_KV.get(`earn_board_${isoDateET(toET())}`);
        b = raw ? JSON.parse(raw) : SAMPLES.C;
      }
      if (!b) return jsonResp({ ok: false, error: 'no board for that date' }, 404, {});
      // after=1: preview the 9:40 "morning after" variant. With a sample board,
      // fake results are injected so the layout can be eyeballed.
      if (url.searchParams.get('after') === '1') {
        b = JSON.parse(JSON.stringify(b));
        b.after = true; b.final = false;
        const fake = [0.062, -0.031, 0.008, -0.113, 0.024, 0.001, -0.007, 0.157, -0.049, 0.012];
        (b.board || []).forEach((r, i) => { if (r.afterPct == null) r.afterPct = fake[i % fake.length]; });
      }
      if (url.searchParams.get('png') === '1') {
        try {
          const png = await renderEarningsCardPng(b);
          return new Response(png, { status: 200, headers: { 'Content-Type': 'image/png', 'Access-Control-Allow-Origin': '*' } });
        } catch (e) { return jsonResp({ ok: false, error: e.message }, 500, {}); }
      }
      const res = await earnSendCard(env, b, b.final ? 'final' : 'morning', true);
      return jsonResp({ ok: res.card === true, result: res });
    }

    // ── GET /gexm-status ── GEXMAGNET chain-collector status. Auth-required
    // (SYNC_SECRET or the scoped GEXM_TRIGGER_TOKEN — the latter exists so
    // local tooling can hit these two endpoints without the dashboard secret).
    // ── GET /cor1m-series ── owner-gated dump of the KV intraday COR1M
    // samples (7d TTL) for the Tail Hedge bundle refresh — the ThetaData
    // index sub lapsed 2026-07-23, so the local refresh now feeds from the
    // worker's own Schwab capture (2026-07-29).
    if (url.pathname === '/cor1m-series' && request.method === 'GET') {
      const secret = request.headers.get('X-Sync-Secret') || url.searchParams.get('secret');
      if (!secret || (secret !== env.SYNC_SECRET && secret !== env.GEXM_TRIGGER_TOKEN)) {
        return jsonResp({ error: 'Unauthorized' }, 401, { 'Access-Control-Allow-Origin': '*' });
      }
      const from = url.searchParams.get('from'), to = url.searchParams.get('to');
      if (!/^\d{4}-\d{2}-\d{2}$/.test(from || '') || !/^\d{4}-\d{2}-\d{2}$/.test(to || '')) {
        return jsonResp({ error: 'from/to required (YYYY-MM-DD)' }, 400, {});
      }
      const out = {};
      const d = new Date(from + 'T12:00:00Z'), end = new Date(to + 'T12:00:00Z');
      while (d <= end) {
        const iso = d.toISOString().slice(0, 10);
        try {
          const raw = await env.SIGNAL_KV.get(`cor1m_series_${iso}`);
          if (raw) out[iso] = JSON.parse(raw);
        } catch (_) {}
        d.setUTCDate(d.getUTCDate() + 1);
      }
      return jsonResp({ from, to, days: out }, 200, { 'Access-Control-Allow-Origin': '*' });
    }

    if (url.pathname === '/gexm-status' && request.method === 'GET') {
      const secret = request.headers.get('X-Sync-Secret') || url.searchParams.get('secret');
      if (!secret || (secret !== env.SYNC_SECRET && secret !== env.GEXM_TRIGGER_TOKEN)) {
        return jsonResp({ error: 'Unauthorized' }, 401, { 'Access-Control-Allow-Origin': '*' });
      }
      const todayISO = isoDateET(toET());
      const [last, done, partRaw] = await Promise.all([
        env.SIGNAL_KV.get('gexm_chains_last'),
        env.SIGNAL_KV.get(`gexm_chains_done_${todayISO}`),
        env.SIGNAL_KV.get(`gexm_chains_part_${todayISO}`),
      ]);
      const part = partRaw ? JSON.parse(partRaw) : null;
      return jsonResp({
        universe: GEXM_UNIVERSE.length,
        last: last ? JSON.parse(last) : null,
        today: { date: todayISO, done: !!done,
                 partial: part ? Object.keys(part.tickers).length : 0,
                 errs: part ? part.errs : {} },
      });
    }

    // ── GET /gexm-trigger ── Run one collector chunk NOW, ignoring the
    // 16:41–17:15 ET window (once-per-day done-key still respected). Call
    // repeatedly until {gexm:'committed'}. Auth-required.
    if (url.pathname === '/gexm-trigger' && request.method === 'GET') {
      const secret = request.headers.get('X-Sync-Secret') || url.searchParams.get('secret');
      if (!secret || (secret !== env.SYNC_SECRET && secret !== env.GEXM_TRIGGER_TOKEN)) {
        return jsonResp({ error: 'Unauthorized' }, 401, { 'Access-Control-Allow-Origin': '*' });
      }
      const res = await gexmCollectChains(env, toET(), { force: true });
      return jsonResp(res || { gexm: 'no-op' });
    }

    // ── GET /straddle-trigger ── Manually open a straddle NOW.
    // Useful when the 9:32 cron's openStraddleTrade threw and we need to
    // surface the actual error or do a late open. Auth-required.
    if (url.pathname === '/straddle-trigger' && request.method === 'GET') {
      const secret = request.headers.get('X-Sync-Secret') || url.searchParams.get('secret');
      if (!secret || secret !== env.SYNC_SECRET) {
        return jsonResp({ error: 'Unauthorized' }, 401, { 'Access-Control-Allow-Origin': '*' });
      }
      try {
        const etNowDT = toET(new Date());
        const todayDT = isoDateET(etNowDT);
        const tokenDT = await getAccessToken(env);

        // Recompute signal so we know the badge (NM/EOM/plain) for max debit
        const _phEnd = Date.now();
        const _phStart = _phEnd - 4 * 24 * 60 * 60 * 1000;
        const [vH, sH] = await Promise.all([
          fetchSchwabJSON(`https://api.schwabapi.com/marketdata/v1/pricehistory?symbol=%24VIX&periodType=day&period=3&frequencyType=minute&frequency=1&startDate=${_phStart}&endDate=${_phEnd}&needExtendedHoursData=true`, tokenDT, env),
          fetchSchwabJSON(`https://api.schwabapi.com/marketdata/v1/pricehistory?symbol=%24SPX&periodType=day&period=3&frequencyType=minute&frequency=1&startDate=${_phStart}&endDate=${_phEnd}&needExtendedHoursData=true`, tokenDT, env),
        ]);
        const todayStr = etNowDT.toDateString();
        const findFirstAt930 = (cs) => (cs || []).slice().sort((a,b) => a.datetime - b.datetime).find(c => {
          const d = toET(new Date(c.datetime));
          return d.toDateString() === todayStr && (d.getHours() > 9 || (d.getHours() === 9 && d.getMinutes() >= 30));
        });
        const findYesterdayClose = (cs) => {
          const sorted = (cs || []).slice().sort((a,b) => a.datetime - b.datetime);
          const yesterday = sorted.filter(c => toET(new Date(c.datetime)).toDateString() !== todayStr);
          return yesterday[yesterday.length - 1];
        };
        const findYesterdayOpenAt930 = (cs) => {
          const sorted = (cs || []).slice().sort((a,b) => a.datetime - b.datetime);
          const yesterday = sorted.filter(c => toET(new Date(c.datetime)).toDateString() !== todayStr);
          if (!yesterday.length) return null;
          const yStr = toET(new Date(yesterday[yesterday.length - 1].datetime)).toDateString();
          return yesterday.find(c => {
            const d = toET(new Date(c.datetime));
            return d.toDateString() === yStr && (d.getHours() > 9 || (d.getHours() === 9 && d.getMinutes() >= 30));
          });
        };
        const vT = findFirstAt930(vH.candles)?.open;
        const vYC = findYesterdayClose(vH.candles)?.close;
        const vYO = findYesterdayOpenAt930(vH.candles)?.open;
        const sT = findFirstAt930(sH.candles)?.open;
        const sYC = findYesterdayClose(sH.candles)?.close;
        const gap = (sT != null && sYC != null) ? ((sT - sYC) / sYC) * 100 : 0;
        const sig = calculateSignal({
          vixToday: vT, vixYOpen: vYO, vixYClose: vYC,
          spxGapPct: gap, etDate: etNowDT,
        });
        { const _g = await gexGateEval(env, isoDateET(etNowDT)); applyGexGateToSignal(sig, _g.skip, _g.rank); }
        if (sig.theme !== 'strad') {
          return jsonResp({ ok: false, reason: 'signal-not-strad', theme: sig.theme, rec: sig.rec }, 200, { 'Access-Control-Allow-Origin': '*' });
        }
        // Try to open. Surfaces the actual error from openStraddleTrade.
        const trade = await openStraddleTrade(env, tokenDT, etNowDT, sig);
        await env.SIGNAL_KV.put('straddle_open_trade', JSON.stringify(trade));
        // Clear the skip — we have a real trade now
        await env.SIGNAL_KV.delete(`straddle_skip_${todayDT}`);
        return jsonResp({ ok: true, trade, signal: sig }, 200, { 'Access-Control-Allow-Origin': '*' });
      } catch (e) {
        return jsonResp({ ok: false, error: e.message, stack: e.stack }, 500, { 'Access-Control-Allow-Origin': '*' });
      }
    }

    // ── GET /m8bf-backfill?date=YYYY-MM-DD ── Force-recompute m8bfPL + WR
    // for a specific historical date by re-scraping Discord. Bypasses the
    // "only if m8bfPL is null" guard in backfillMissingPL so we can fix
    // days where the cron stalled, set m8bfPL=0 by default, but real
    // Discord signals exist. Auth-required.
    if (url.pathname === '/m8bf-backfill' && request.method === 'GET') {
      const secret = request.headers.get('X-Sync-Secret') || url.searchParams.get('secret');
      if (!secret || secret !== env.SYNC_SECRET) {
        return jsonResp({ error: 'Unauthorized' }, 401, { 'Access-Control-Allow-Origin': '*' });
      }
      const targetDate = url.searchParams.get('date');
      const verbose = url.searchParams.get('verbose') === '1';
      if (verbose && targetDate) {
        // Just dump raw scraped signals for the date — no writing
        try {
          const dcToken = env.DISCORD_USER_TOKEN;
          const channelId = '1048242197029458040';
          const sigs = await fetchAllDiscordSignalsForDate(dcToken, channelId, targetDate);
          const etD = toET(new Date(targetDate + 'T20:00:00Z'));
          const dow = etD.getDay();
          const win = getM8BFWindow(dow, targetDate);
          return jsonResp({
            date: targetDate,
            dow,
            windowMinutes: win,
            totalSignalsScraped: sigs.length,
            signals: sigs.slice(0, 100).map(s => ({
              time: s.time, center: s.center, t1: s.t1,
              lower: s.lower, upper: s.upper, premium: s.premium,
              banned: isBanned(s.center, s.lower, s.t1),
            })),
          }, 200, { 'Access-Control-Allow-Origin': '*' });
        } catch (e) {
          return jsonResp({ error: e.message }, 500, { 'Access-Control-Allow-Origin': '*' });
        }
      }
      if (!targetDate || !/^\d{4}-\d{2}-\d{2}$/.test(targetDate)) {
        return jsonResp({ error: 'date param required (YYYY-MM-DD)' }, 400, { 'Access-Control-Allow-Origin': '*' });
      }
      try {
        // First null out the existing m8bfPL so backfill picks it up
        // (the function's null-check is bypassed by targetDates anyway,
        // but we want a clean re-write).
        const ghHeaders = {
          'Authorization': `Bearer ${env.GITHUB_TOKEN}`,
          'Accept': 'application/vnd.github+json',
          'User-Agent': 'schwab-proxy-worker/1.0',
          'X-GitHub-Api-Version': '2022-11-28',
        };
        const apiUrl = 'https://api.github.com/repos/rava8989/brave/contents/history_data.json';
        const getR = await fetch(apiUrl, { headers: ghHeaders });
        const meta = await getR.json();
        const content = JSON.parse(atob(meta.content.replace(/\n/g, '')));
        const idx = content.findIndex(r => r.date === targetDate);
        if (idx < 0) return jsonResp({ error: `Date ${targetDate} not found in history` }, 404, { 'Access-Control-Allow-Origin': '*' });
        const before = { m8bfPL: content[idx].m8bfPL, m8bfWR: content[idx].m8bfWR };
        delete content[idx].m8bfPL;
        delete content[idx].m8bfWR;
        // Write back temporarily so backfill sees null
        const putR = await fetch(apiUrl, {
          method: 'PUT',
          headers: { ...ghHeaders, 'Content-Type': 'application/json' },
          body: JSON.stringify({
            message: `auto: clear m8bfPL/WR for ${targetDate} before recompute`,
            content: btoa(JSON.stringify(content, null, 0)),
            sha: meta.sha,
          }),
        });
        if (!putR.ok) return jsonResp({ error: `pre-clear failed: ${putR.status}` }, 500, { 'Access-Control-Allow-Origin': '*' });

        // Now run backfill on this specific date — it'll re-scrape Discord
        // history for the date and recompute m8bfPL + m8bfWR.
        const result = await backfillMissingPL(env, [targetDate]);

        // Re-read to show what got written
        const verifyR = await fetch(apiUrl, { headers: ghHeaders });
        const verifyMeta = await verifyR.json();
        const verifyContent = JSON.parse(atob(verifyMeta.content.replace(/\n/g, '')));
        const after = verifyContent.find(r => r.date === targetDate) || {};

        return jsonResp({
          ok: true,
          date: targetDate,
          before,
          after: { m8bfPL: after.m8bfPL, m8bfWR: after.m8bfWR },
          backfillResult: result,
        }, 200, { 'Access-Control-Allow-Origin': '*' });
      } catch (e) {
        return jsonResp({ error: e.message, stack: e.stack }, 500, { 'Access-Control-Allow-Origin': '*' });
      }
    }

    if (url.pathname === '/diagonal-trigger' && request.method === 'GET') {
      const secret = request.headers.get('X-Sync-Secret') || url.searchParams.get('secret');
      if (!secret || secret !== env.SYNC_SECRET) {
        return jsonResp({ error: 'Unauthorized' }, 401, { 'Access-Control-Allow-Origin': '*' });
      }
      try {
        const etNowDT = toET(new Date());
        const result = await handleDiagonalTrade(env, etNowDT);
        return jsonResp(result, 200, { 'Access-Control-Allow-Origin': '*' });
      } catch (e) {
        return jsonResp({ error: e.message, stack: e.stack }, 500, { 'Access-Control-Allow-Origin': '*' });
      }
    }

    // ── GET /diagonal-today ── Public live diagonal state (no auth)
    // Returns the currently-open diagonal trade with live mid-quote refresh,
    // plus the most recent closed trade for the live page's tape view.
    // The cron refreshes mids every 2 min so this endpoint just reads KV
    // (zero Schwab API cost). On stale data the live page can show an age
    // indicator using `lastQuoteAt`.
    // ── GET /bobf-today ── Public live BOBF state (no auth)
    // Returns the currently-open/working/expired/closed BOBF trade.
    if (url.pathname === '/bobf-today' && request.method === 'GET') {
      const publicCors = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type',
        'Access-Control-Max-Age': '86400',
      };
      try {
        const etNowB = toET(new Date());
        const todayB = isoDateET(etNowB);
        const isWeekendB = etNowB.getDay() === 0 || etNowB.getDay() === 6;
        const isHolidayB = isHol(etNowB);

        let openRaw = await env.SIGNAL_KV.get('bobf_open_trade');
        let open = openRaw ? JSON.parse(openRaw) : null;

        // Stale-trade filter: settleBobfEOD overwrites bobf_open_trade with the
        // settled (status='closed') trade instead of deleting it, so on the next
        // trading day the slot still holds yesterday's closed trade. The render
        // logic treats anything in `open` as today's active position. Drop it
        // here — the settled trade is already preserved in bobf_closed_log and
        // surfaces as `lastClosed`, so no data is lost.
        if (open && (open.openDate !== todayB || open.status === 'closed' || open.status === 'expired')) {
          open = null;
        }

        // Staleness-triggered self-refresh (defends against cron stalls)
        if (open && (open.status === 'filled' || open.status === 'working') && !isWeekendB && !isHolidayB) {
          const isMktHr = (etNowB.getHours() > 9 || (etNowB.getHours() === 9 && etNowB.getMinutes() >= 30)) && etNowB.getHours() < 16;
          const ageMs = open.lastQuoteAt ? (Date.now() - new Date(open.lastQuoteAt).getTime()) : Infinity;
          if (isMktHr && ageMs > 90_000) {
            try {
              const tk = await getAccessToken(env);
              await refreshBobfLiveQuotes(env, tk, etNowB);
              openRaw = await env.SIGNAL_KV.get('bobf_open_trade');
              open = openRaw ? JSON.parse(openRaw) : null;
            } catch (e) { console.warn('[bobf-today] on-demand refresh failed:', e.message); }
          }
        }

        const logRaw = await env.SIGNAL_KV.get('bobf_closed_log');
        const closedLog = logRaw ? JSON.parse(logRaw) : [];
        const lastClosed = closedLog[0] || null;

        const doneKey = `bobf_done_${todayB}`;
        const doneState = await env.SIGNAL_KV.get(doneKey);

        // Surface the prefilter cache (RSI, SMA5, spxOpen, type, etc.)
        // so the live page can show today's RSI/SMA without re-fetching.
        const staticRaw = await env.SIGNAL_KV.get(`bobf_static_${todayB}`);
        const staticData = staticRaw ? JSON.parse(staticRaw) : null;

        // ── Calendar-blackout PREVIEW ──
        // The prefilter only runs at 9:30 ET, so before then `doneState` is
        // null and the live page wrongly shows "trade fires". Compute BOBF's
        // OWN calendar blackouts here (VIX-independent ones only) so the card
        // can show "No BOBF — OPEX" pre-market. This is a READ-ONLY mirror of
        // the prefilterBobf calendar block — it changes NO strategy rule, it
        // just surfaces the existing one earlier. Per strategy-independence:
        // this reflects BOBF's own rules, not any other strategy's.
        let blackoutPreview = null;
        if (!isWeekendB && !isHolidayB) {
          const _bo = [];
          if (cpiSch.includes(todayLong(etNowB)))        _bo.push('CPI');
          if (isFirstTradeMon(etNowB))                   _bo.push('NM Mon');
          if (vixSch.includes(todayLong(etNowB)))        _bo.push('VIX exp');
          if (opexSch.includes(todayLong(etNowB)))       _bo.push('OPEX');
          if (opexSch.some(ds => isTodayBefore(ds, etNowB))) _bo.push('OPEX-1');
          if (isEomN(2, etNowB))                         _bo.push('EOM-2');
          if (isEomN(1, etNowB))                         _bo.push('EOM-1');
          if (isEarningsDay(etNowB))                     _bo.push('earnings');
          if (_bo.length) blackoutPreview = _bo.join(',');
        }

        return jsonResp({
          date: todayB,
          isWeekend: isWeekendB,
          isHoliday: isHolidayB,
          open,
          lastClosed,
          doneState,
          blackoutPreview,    // calendar-only blackout string, or null (VIX>23 not included — needs live VIX)
          static: staticData,  // {rsi14, sma5, spxOpen, vixToday, vixYClose, type, label, bodyOffset}
          serverTimeET: `${String(etNowB.getHours()).padStart(2,'0')}:${String(etNowB.getMinutes()).padStart(2,'0')}`,
          windowET: '10:29 - 12:15',
          maxPremiumFriday: BOBF_FRIDAY_MAX_PREMIUM,
        }, 200, publicCors);
      } catch (e) {
        return jsonResp({ error: e.message }, 500, publicCors);
      }
    }

    // ── GET /gxbf-today ── Public live GXBF state (no auth)
    // Mirrors /bobf-today: returns the open/closed GXBF trade, doneState,
    // and skip reason. Center is computed live from the chain at entry
    // (no Discord scrape). Phantom prior-day/closed-slot cleanup +
    // staleness self-refresh during market hours. STRATEGY INDEPENDENCE:
    // surfaces only GXBF's own state.
    if (url.pathname === '/magnetfly-today' && request.method === 'GET') {
      const publicCors = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type',
        'Access-Control-Max-Age': '86400',
      };
      try {
        const etNowM = toET(new Date());
        const todayM = isoDateET(etNowM);
        const todayRaw = await env.SIGNAL_KV.get(`mf_today_${todayM}`);
        const openRaw = await env.SIGNAL_KV.get('mf_open_trade');
        const open = openRaw ? JSON.parse(openRaw) : null;
        const liveRaw = await env.SIGNAL_KV.get('mf_live');
        const live = liveRaw ? JSON.parse(liveRaw) : null;
        let out = todayRaw ? JSON.parse(todayRaw) : {
          date: todayM, status: 'WAIT',
          headline: (etNowM.getDay() === 0 || etNowM.getDay() === 6 || isHol(etNowM))
            ? 'market closed' : 'waiting for the 12:00 ET check',
          detail: 'magnet snapshot at 10:30 · alignment + price check at noon',
        };
        if (open && open.openDate === todayM) {
          out.trade = open;
          if (open.status === 'open') {
            out.headline = `OPEN — 30w fly at ${open.magnet}, entry $${open.entry.toFixed(2)}` +
              (open.lastMid != null ? ` · now $${open.lastMid.toFixed(2)}` : '');
          } else if (open.status === 'closed') {
            out.headline = `${open.exit === 'TP' ? 'TP hit +$300' : open.exit === 'SL' ? 'stopped −$500' : 'settled ' + open.pnl} @ ${open.exitTime} ET`;
          }
        }
        // Live tracker row (2026-07-28): fresh within 5 min only — a stale
        // tracker (worker hiccup, after-hours) must vanish, not mislead.
        if (live && (Date.now() / 1000 - (live.ts || 0)) < 300) out.live = live;
        return jsonResp(out, 200, publicCors);
      } catch (e) {
        return jsonResp({ error: e.message }, 500, publicCors);
      }
    }

    if (url.pathname === '/magnetfly-history' && request.method === 'GET') {
      const publicCors = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type',
        'Access-Control-Max-Age': '86400',
      };
      try {
        const logRaw = await env.SIGNAL_KV.get('mf_closed_log');
        return jsonResp({ trades: logRaw ? JSON.parse(logRaw) : [] }, 200, publicCors);
      } catch (e) {
        return jsonResp({ error: e.message }, 500, publicCors);
      }
    }

    if (url.pathname === '/magnetfly-test' && request.method === 'POST') {
      const secret = request.headers.get('X-Sync-Secret') || url.searchParams.get('secret');
      if (!secret || (secret !== env.SYNC_SECRET && secret !== env.GEXM_TRIGGER_TOKEN)) {
        return jsonResp({ error: 'unauthorized' }, 401);
      }
      // ?dm=1 — OWNER DM ONLY example of the full signal-day alert set
      // (9:40 / 11:30 / 12:00 as they'd read on a live GO day). Never the
      // channel, never subscribers — same dmOnly discipline as the earnings
      // card test.
      if (url.searchParams.get('dm') === '1') {
        const dcRaw = await env.SIGNAL_KV.get('discord_config');
        const dc = dcRaw ? JSON.parse(dcRaw) : null;
        if (!dc?.channelId) return jsonResp({ error: 'no owner DM channel' }, 500);
        const ex = { magnet: 7550, entry: 14.85 };
        const msg =
          `🧲 **PNBF — EXAMPLE of a signal day** (test, DM-only — real alerts land here + Sigma channel)\n\n` +
          `**Morning Plan card** — PNBF shows as a row: _watching · noon decides_ with a **POSSIBLE** badge (calendar clear).\n\n` +
          `**11:30 ET heads-up** — 🧲 **PNBF** — 🟢 leaning GO, aligned, 30w ~$${ex.entry.toFixed(2)}. _Noon is final._\n\n` +
          `**12:00 ET — the trade** (same shape as M8BF, paste into ToS):\n` +
          `**PNBF**\n` +
          `BUY +10 BUTTERFLY SPX 100 (Weeklys) 17 JUL 26 ${ex.magnet - 30}/${ex.magnet}/${ex.magnet + 30} CALL @${ex.entry.toFixed(2)} LMT\n` +
          `TP ${(ex.entry + 3).toFixed(2)} · SL ${(ex.entry - 5).toFixed(2)}` +
          FANOUT_DISCLAIMER +      // live GO gets this via fanout; the sample bypasses fanout, so append it here too
          `\n\n-# No-signal days: PNBF row reads NO on the card, no separate message.`;
        const r = await sendDiscordDM(env, dc.channelId, msg, dc.proxyUrl);
        return jsonResp({ ok: r.ok, dmOnly: true });
      }
      const posted = await postMagnetFly(env,
        `🧲 **PNBF** — test card. Feed is wired (webhook ${await env.SIGNAL_KV.get('mf_webhook_url') ? 'SET' : 'NOT set — DM fallback'}). ` +
        `Recipe: 30w fly at magnet @12:00 when T1==magnet, debit ≤ $17, TP +3 / SL −5.`);
      return jsonResp({ ok: true, posted });
    }

    // ── GET /fatgamma-test?secret=… — OWNER DM ONLY example of the M8BF trade
    // relay on a fat-gamma tier day. Never the channel, never subscribers —
    // same dmOnly discipline as magnetfly-test?dm=1.
    if (url.pathname === '/fatgamma-test' && request.method === 'GET') {
      const secret = request.headers.get('X-Sync-Secret') || url.searchParams.get('secret');
      if (!secret || (secret !== env.SYNC_SECRET && secret !== env.GEXM_TRIGGER_TOKEN)) {
        return jsonResp({ error: 'unauthorized' }, 401);
      }
      const dcRaw = await env.SIGNAL_KV.get('discord_config');
      const dc = dcRaw ? JSON.parse(dcRaw) : null;
      if (!dc?.channelId) return jsonResp({ error: 'no owner DM channel' }, 500);
      // ?card=1 — DM the morning-card PNG with the FAT GAMMA row instead of the text relay.
      if (url.searchParams.get('card') === '1') {
        const cardData = JSON.parse(JSON.stringify(SAMPLE_MORNING_CARD));
        const m8 = (cardData.rows || []).find(r => r.n === 'M8BF');
        if (m8) { m8.det = `watching 13:30–14:00 · FAT GAMMA p68`; m8.yes = true; m8.state = undefined; }
        const png = await renderMorningCardPng(cardData);
        const r = await sendDiscordImage(env, dc.channelId, png, dc.proxyUrl, 'morning-fatgamma-example.png',
          'EXAMPLE — morning card on a fat-gamma tier day (test, DM-only)');
        return jsonResp({ ok: r.ok, dmOnly: true, kind: 'card' });
      }
      const msg =
        `**M8BF — EXAMPLE of a fat-gamma tier day** (test, DM-only — the real thing rides the normal M8BF relay)\n\n` +
        `**M8BF**\n` +
        `BUY +10 BUTTERFLY SPX 100 (Weeklys) 28 JUL 26 7380/7405/7430 CALL @24.50 LMT\n` +
        `\n-# 🟢 Fat-gamma tier (p68): prev-session dealer gamma in its upper range — historically 72% M8BF win rate in this tier vs 66% on other days.` +
        FANOUT_DISCLAIMER +
        `\n\n-# Morning card that day: the M8BF row reads "watching 13:30–14:00 · FAT GAMMA p68". Normal days carry no extra line.`;
      const r = await sendDiscordDM(env, dc.channelId, msg, dc.proxyUrl);
      return jsonResp({ ok: r.ok, dmOnly: true });
    }

    if (url.pathname === '/gxbf-today' && request.method === 'GET') {
      const publicCors = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type',
        'Access-Control-Max-Age': '86400',
      };
      try {
        const etNowG = toET(new Date());
        const todayG = isoDateET(etNowG);
        const isWeekendG = etNowG.getDay() === 0 || etNowG.getDay() === 6;
        const isHolidayG = isHol(etNowG);

        let openRaw = await env.SIGNAL_KV.get('gxbf_open_trade');
        let open = openRaw ? JSON.parse(openRaw) : null;

        // Stale-trade filter: settleGxbfEOD overwrites gxbf_open_trade with
        // the settled (status='closed') trade instead of deleting it, so on
        // the next trading day the slot still holds yesterday's closed trade.
        // Drop it here — the settled trade is preserved in gxbf_closed_log
        // and surfaces as `lastClosed`, so no data is lost. (Mirrors the
        // /bobf-today stale-trade filter.)
        if (open && (open.openDate !== todayG || open.status === 'closed' || open.status === 'expired')) {
          open = null;
        }

        // Staleness-triggered self-refresh (defends against cron stalls)
        if (open && open.status === 'filled' && !isWeekendG && !isHolidayG) {
          const isMktHr = (etNowG.getHours() > 9 || (etNowG.getHours() === 9 && etNowG.getMinutes() >= 30)) && etNowG.getHours() < 16;
          const ageMs = open.lastQuoteAt ? (Date.now() - new Date(open.lastQuoteAt).getTime()) : Infinity;
          if (isMktHr && ageMs > 90_000) {
            try {
              const tk = await getAccessToken(env);
              await refreshGxbfLiveQuotes(env, tk, etNowG);
              openRaw = await env.SIGNAL_KV.get('gxbf_open_trade');
              open = openRaw ? JSON.parse(openRaw) : null;
              if (open && (open.openDate !== todayG || open.status === 'closed' || open.status === 'expired')) open = null;
            } catch (e) { console.warn('[gxbf-today] on-demand refresh failed:', e.message); }
          }
        }

        const logRaw = await env.SIGNAL_KV.get('gxbf_closed_log');
        const closedLog = logRaw ? JSON.parse(logRaw) : [];
        const lastClosed = closedLog[0] || null;

        const doneKey = `gxbf_done_${todayG}`;
        const doneState = await env.SIGNAL_KV.get(doneKey);

        // Skip reason recorded at 9:30 if signal.theme !== 'gxbf' that day.
        let skip = null;
        const skipRaw = await env.SIGNAL_KV.get(`gxbf_skip_${todayG}`);
        if (skipRaw) skip = JSON.parse(skipRaw);

        // On-demand recovery from the persisted morning signal (mirrors the
        // /straddle-today morning-data fallback). Only GXBF's own theme.
        const haveOpenTodayG = open && open.openDate === todayG;
        if (!haveOpenTodayG && !skip) {
          const morningDataRaw = await env.SIGNAL_KV.get(`morning_signal_data_${todayG}`);
          if (morningDataRaw) {
            const morningData = JSON.parse(morningDataRaw);
            if (morningData.theme === 'gxbf') {
              skip = { theme: 'gxbf-missed', rec: `${morningData.gxbfText || morningData.rec} — open failed`,
                       recordedAt: new Date().toISOString(), source: 'morning-data' };
            } else {
              skip = { theme: morningData.theme, rec: morningData.gxbfText || morningData.rec,
                       recordedAt: new Date().toISOString(), source: 'morning-data' };
            }
            await env.SIGNAL_KV.put(`gxbf_skip_${todayG}`, JSON.stringify(skip), { expirationTtl: 86400 });
          }
        }

        return jsonResp({
          date: todayG,
          isWeekend: isWeekendG,
          isHoliday: isHolidayG,
          open,
          lastClosed,
          doneState,
          skip,                 // {theme, rec} when signal said no-GXBF today
          serverTimeET: `${String(etNowG.getHours()).padStart(2,'0')}:${String(etNowG.getMinutes()).padStart(2,'0')}`,
          windowET: '09:35 - 09:45',
        }, 200, publicCors);
      } catch (e) {
        return jsonResp({ error: e.message }, 500, publicCors);
      }
    }

    // ── GET /earn-final-resend ── Owner-gated: re-send today's FINAL board
    // through the delivery-verified path (owner 2026-08-03: "make sure I get
    // the signal no matter the result"). Reads the stored board — no rebuild.
    if (url.pathname === '/earn-final-resend' && request.method === 'GET') {
      const sec = url.searchParams.get('secret');
      if (!sec || (sec !== env.SYNC_SECRET && sec !== env.GEXM_TRIGGER_TOKEN))
        return new Response('forbidden', { status: 403 });
      try {
        const iso = isoDateET(toET(new Date()));
        const raw = await env.SIGNAL_KV.get(`earn_board_${iso}`);
        if (!raw) return new Response(JSON.stringify({ error: 'no board for ' + iso }), { status: 404, headers: { 'Content-Type': 'application/json' } });
        const b = JSON.parse(raw);
        b.final = true;
        const sent = await earnSendCard(env, b, 'final');
        const delivered = !!(sent && (sent.dm || sent.webhook));
        if (delivered) await env.SIGNAL_KV.put(`earn_done_${iso}_final`, 'sent', { expirationTtl: 86400 });
        return new Response(JSON.stringify({ delivered, sinks: sent, rows: (b.board || []).length, longs: (b.longs || []).length }),
          { headers: { 'Content-Type': 'application/json' } });
      } catch (e) {
        return new Response(JSON.stringify({ error: e.message }), { status: 500, headers: { 'Content-Type': 'application/json' } });
      }
    }

    // ── GET /earn-after-debug ── Owner-gated: run earnAfterJob NOW with a
    // synthetic in-window clock and report the terminal marker (2026-08-03:
    // the 9:40 card left no marker at all — this proves whether the body runs).
    if (url.pathname === '/earn-after-debug' && request.method === 'GET') {
      const sec = url.searchParams.get('secret');
      if (!sec || (sec !== env.SYNC_SECRET && sec !== env.GEXM_TRIGGER_TOKEN))
        return new Response('forbidden', { status: 403 });
      try {
        const fake = toET(new Date());
        fake.setHours(9, 45, 0, 0);
        const tok = await getAccessToken(env);
        await earnAfterJob(env, fake, tok);
        const iso = isoDateET(toET(new Date()));
        const marker = await env.SIGNAL_KV.get(`earn_after_${iso}`);
        const board = await env.SIGNAL_KV.get(`earn_board_2026-07-31`);
        return new Response(JSON.stringify({ marker, boardLen: board ? board.length : null }),
          { headers: { 'Content-Type': 'application/json' } });
      } catch (e) {
        return new Response(JSON.stringify({ error: e.message, stack: (e.stack || '').slice(0, 400) }),
          { status: 500, headers: { 'Content-Type': 'application/json' } });
      }
    }

    // ── GET /morning-book ── Public: the day's persisted 9:35 + 10:00 book
    // snaps for the GEX page's Morning Book panel (owner 2026-07-31, info only).
    if (url.pathname === '/morning-book' && request.method === 'GET') {
      const cors = { 'Access-Control-Allow-Origin': '*', 'Content-Type': 'application/json' };
      try {
        const iso = isoDateET(toET(new Date()));
        const a = await env.SIGNAL_KV.get(`g935_snap_${iso}`);
        const b = await env.SIGNAL_KV.get(`g1000_snap_${iso}`);
        return new Response(JSON.stringify({ date: iso,
          g935: a ? JSON.parse(a) : null, g1000: b ? JSON.parse(b) : null }), { headers: cors });
      } catch (e) { return new Response(JSON.stringify({ error: e.message }), { status: 500, headers: cors }); }
    }

    // ── GET /evening-book ── Public: bank-scale ALL-EXPIRY book at the close
    // for the GEX page's Gap card (owner 2026-07-31, info only). Lazily
    // computes once per trading day after 16:25 ET (claim-gated, then served
    // from KV forever); the cron backstop covers no-visitor nights. Also returns today's open (from morning_signal_data)
    // so the card can score the overnight gap against the prior snapshot,
    // and the rolling log's last entries for context. Continues the
    // 1,009-night ThetaData bank series — same formula, same scale.
    if (url.pathname === '/evening-book' && request.method === 'GET') {
      const cors = { 'Access-Control-Allow-Origin': '*', 'Content-Type': 'application/json' };
      try {
        const etNowEb = toET(new Date());
        const isoEb = isoDateET(etNowEb);
        // Lazy compute via the shared helper (claim-gated, idempotent) — the
        // cron backstop covers nights with no page hits (audit 2026-07-31).
        const latest = await ensureEveningBook(env, etNowEb);
        let prev = null;
        try {
          const log = JSON.parse((await env.SIGNAL_KV.get('evening_book_log')) || '[]');
          const before = log.filter(r => latest && r.d < latest.date);
          if (before.length) prev = before[before.length - 1];
        } catch (_) {}
        let todayOpen = null;
        try {
          const msd = JSON.parse((await env.SIGNAL_KV.get(`morning_signal_data_${isoEb}`)) || 'null');
          if (msd && msd.spxOpen) todayOpen = msd.spxOpen;
        } catch (_) {}
        return new Response(JSON.stringify({ date: isoEb, latest, prev, todayOpen }), { headers: cors });
      } catch (e) { return new Response(JSON.stringify({ error: e.message }), { status: 500, headers: cors }); }
    }

    // ── GET /straddle-today ── Public live straddle state (no auth)
    // Returns the currently-open/working/expired/closed straddle trade plus
    // the most recent closed straddle. Live mids refreshed by the cron, so
    // this is a cheap KV read (no Schwab API call here).
    if (url.pathname === '/straddle-today' && request.method === 'GET') {
      const publicCors = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type',
        'Access-Control-Max-Age': '86400',
      };
      try {
        const etNowSt = toET(new Date());
        const todaySt = isoDateET(etNowSt);
        const isWeekendSt = etNowSt.getDay() === 0 || etNowSt.getDay() === 6;
        const isHolidaySt = isHol(etNowSt);

        let openRaw = await env.SIGNAL_KV.get('straddle_open_trade');
        let open = openRaw ? JSON.parse(openRaw) : null;

        // ── Self-heal #0: phantom-trade cleanup ─────────────────────────────
        // Mirrors the Diagonal pattern (line 4879). Three cases:
        //  (a) Prior-day trade left in open slot — close+open lifecycle failed
        //      to KV.delete after settle. Refresh loop keeps mutating ghost.
        //  (b) Today's slot holds an already-closed/expired trade — settle
        //      wrote the closed_log entry but didn't drop the open key.
        //  (c) Off-strategy phantom (observed 2026-05-14): trade exists for
        //      today, but morning signal said theme!='strad'. Means it fired
        //      on stale/wrong data and slipped through the gate. Delete it
        //      so live page reflects the morning signal's actual outcome.
        if (open && open.openDate && open.openDate < todaySt) {
          console.warn(`[strad-today] phantom #a: prior-day open (openDate=${open.openDate}) — clearing`);
          await env.SIGNAL_KV.delete('straddle_open_trade');
          await logEvent(env, 'warn', 'strad-phantom', `phantom #a cleared: prior-day open`, { openDate: open.openDate, todayDate: todaySt });
          open = null;
          openRaw = null;
        } else if (open && (open.status === 'closed' || open.status === 'expired')) {
          console.warn(`[strad-today] phantom #b: open slot held ${open.status} trade — clearing`);
          await env.SIGNAL_KV.delete('straddle_open_trade');
          await logEvent(env, 'warn', 'strad-phantom', `phantom #b cleared: open slot held ${open.status} trade`, { status: open.status });
          open = null;
          openRaw = null;
        } else if (open && open.openDate === todaySt) {
          try {
            const morningDataRaw = await env.SIGNAL_KV.get(`morning_signal_data_${todaySt}`);
            if (morningDataRaw) {
              const morningData = JSON.parse(morningDataRaw);
              // A GATED STRADDLE (gated: true) legitimately opens on a
              // theme==='gxbf' day — the 9:35 gamma gate converts the day.
              if (morningData.theme && morningData.theme !== 'strad' && !open.gated) {
                console.warn(`[strad-today] phantom #c: off-strategy open (morning theme=${morningData.theme}) — clearing`);
                await env.SIGNAL_KV.delete('straddle_open_trade');
                await logEvent(env, 'error', 'strad-phantom', `phantom #c cleared: off-strategy open`, { openTheme: 'strad', morningTheme: morningData.theme });
                open = null;
                openRaw = null;
              }
            }
          } catch (phantomErr) {
            console.warn('[strad-today] phantom-c check failed:', phantomErr.message);
          }
        }

        // Staleness-triggered self-refresh (defends against cron stalls)
        if (open && (open.status === 'filled' || open.status === 'working') && !isWeekendSt && !isHolidaySt) {
          const isMktHr = (etNowSt.getHours() > 9 || (etNowSt.getHours() === 9 && etNowSt.getMinutes() >= 30)) && etNowSt.getHours() < 16;
          const ageMs = open.lastQuoteAt ? (Date.now() - new Date(open.lastQuoteAt).getTime()) : Infinity;
          if (isMktHr && ageMs > 90_000) {
            try {
              const tk = await getAccessToken(env);
              await refreshStraddleLiveQuotes(env, tk, etNowSt);
              openRaw = await env.SIGNAL_KV.get('straddle_open_trade');
              open = openRaw ? JSON.parse(openRaw) : null;
            } catch (e) { console.warn('[strad-today] on-demand refresh failed:', e.message); }
          }
        }

        const logRaw = await env.SIGNAL_KV.get('straddle_closed_log');
        const closedLog = logRaw ? JSON.parse(logRaw) : [];
        const lastClosed = closedLog[0] || null;

        // Skip reason recorded at 9:30 if signal.theme !== 'strad' that day.
        let skip = null;
        const skipRaw = await env.SIGNAL_KV.get(`straddle_skip_${todaySt}`);
        if (skipRaw) skip = JSON.parse(skipRaw);

        // ── On-demand recovery (uses ACTUAL morning signal data) ───────────
        // The morning cron writes morning_signal_data_<date> with the signal
        // it computed (including live-quote-polled vixToday and official
        // vixYClose from quotes endpoint, which differ from candle data).
        // Reading that gives us the EXACT signal the worker saw, not a
        // recomputation that might disagree.
        const haveOpenToday = open && open.openDate === todaySt;
        if (!haveOpenToday) {
          // Clear stale "strad-missed" skip from old recovery code that
          // recomputed signal from candle data and got it wrong.
          if (skip && skip.source === 'on-demand-recovery-missed') {
            await env.SIGNAL_KV.delete(`straddle_skip_${todaySt}`);
            skip = null;
          }
          // Backfill: existing strad-missed skip lacks plannedStrike / plannedMaxDebit
          // (added 2026-05-22). Force recompute so live page can show working-order info.
          if (skip && skip.theme === 'strad-missed' && skip.plannedStrike === undefined) {
            await env.SIGNAL_KV.delete(`straddle_skip_${todaySt}`);
            skip = null;
          }
          // Backfill: if cached plannedMaxDebit doesn't match the current
          // straddleMaxDebit() for this badge, the maxDebit constants changed
          // — force recompute so live page shows the right limit.
          if (skip && skip.theme === 'strad-missed' && skip.plannedMaxDebit != null
              && skip.badge && straddleMaxDebit(skip.badge) !== skip.plannedMaxDebit) {
            await env.SIGNAL_KV.delete(`straddle_skip_${todaySt}`);
            skip = null;
          }
          if (!skip) {
            const morningDataRaw = await env.SIGNAL_KV.get(`morning_signal_data_${todaySt}`);
            if (morningDataRaw) {
              const morningData = JSON.parse(morningDataRaw);
              if (morningData.theme === 'strad') {
                // Signal genuinely said straddle but no live trade exists.
                // Compute planned strike + maxDebit so the live page can show
                // "Working order at strike X · limit $Y" instead of a generic
                // "open failed" — until workCutoffET (13:30) hits, this is the
                // working state. After cutoff the live page flips to "price
                // target not hit".
                const spxOpen = parseFloat(morningData.spxOpen);
                const plannedStrike = isFinite(spxOpen) && spxOpen > 0
                  ? Math.round(spxOpen / 5) * 5
                  : null;
                const plannedMaxDebit = straddleMaxDebit(morningData.badge || 'STRADDLE');
                skip = {
                  theme: 'strad-missed',
                  rec: `${morningData.rec} — open failed`,
                  badge: morningData.badge || 'STRADDLE',
                  plannedStrike,
                  plannedMaxDebit,
                  recordedAt: new Date().toISOString(),
                  source: 'morning-data',
                };
              } else {
                skip = { theme: morningData.theme, rec: morningData.rec,
                         recordedAt: new Date().toISOString(), source: 'morning-data' };
              }
              await env.SIGNAL_KV.put(`straddle_skip_${todaySt}`, JSON.stringify(skip), { expirationTtl: 86400 });
            }
            // If morningData isn't present yet (today, since this code is
            // newly deployed), leave skip as null. Live page will show
            // generic 'WAITING SIGNAL' which is honest — we don't know
            // what signal said.
          }
        }

        return jsonResp({
          date: todaySt,
          isWeekend: isWeekendSt,
          isHoliday: isHolidaySt,
          open,                                  // current trade or null
          lastClosed,                            // most recent settled trade
          skip,                                  // {theme, rec} when signal said no-straddle today
          serverTimeET: `${String(etNowSt.getHours()).padStart(2,'0')}:${String(etNowSt.getMinutes()).padStart(2,'0')}`,
          maxDebits: { NM: STRADDLE_MAX_DEBIT_NM, EOM: STRADDLE_MAX_DEBIT_EOM, plain: STRADDLE_MAX_DEBIT_OTHER },
          workCutoffET: '13:30',
        }, 200, publicCors);
      } catch (e) {
        return jsonResp({ error: e.message }, 500, publicCors);
      }
    }

    if (url.pathname === '/diagonal-today' && request.method === 'GET') {
      const publicCors = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type',
        'Access-Control-Max-Age': '86400',
      };
      try {
        const etNowDT = toET(new Date());
        const todayDT = isoDateET(etNowDT);
        const isWeekendDT = etNowDT.getDay() === 0 || etNowDT.getDay() === 6;
        const isHolidayDT = isHol(etNowDT);

        let openRaw = await env.SIGNAL_KV.get('diagonal_open_trade');
        let open = openRaw ? JSON.parse(openRaw) : null;

        // ── Self-heal #0: phantom-trade cleanup ─────────────────────────────
        // If today's close+open lifecycle already ran but the KV.delete after
        // close silently failed, the prior-day trade sticks around as a ghost
        // and keeps getting refreshed. Detect on first endpoint hit and clear.
        if (open && open.openDate && open.openDate < todayDT && open.status === 'open') {
          const diagDone = await env.SIGNAL_KV.get(`diag_done_${todayDT}`);
          if (diagDone && !diagDone.startsWith('claim:')) {   // claim: = in flight, not done (P24)
            console.warn(`[diag-today] phantom open trade (openDate=${open.openDate}) — clearing`);
            await env.SIGNAL_KV.delete('diagonal_open_trade');
            await logEvent(env, 'warn', 'diag-phantom', 'phantom open trade cleared at endpoint',
                           { openDate: open.openDate, todayDate: todayDT });
            open = null;
            openRaw = null;
          }
        }

        // ── Self-heal #1: close+open lifecycle (cron-stall defense) ─────────
        // The 12:30 ET close+open cycle is supposed to be triggered by the
        // cron tick. If the cron stalls (recurring Cloudflare issue), the
        // prior-day trade stays open and no new one opens. Self-heal here:
        // if it's past 12:30 ET on a market day AND we have an open trade
        // whose openDate is before today AND no diag_done_<today> key,
        // run handleDiagonalTrade (which is idempotent — sets diag_done
        // after success, so concurrent endpoint hits won't double-trigger).
        if (open && open.openDate && open.openDate < todayDT && !isWeekendDT && !isHolidayDT) {
          const past1230 = etNowDT.getHours() > 12 || (etNowDT.getHours() === 12 && etNowDT.getMinutes() >= 30);
          const beforeMarketClose = etNowDT.getHours() < 16;
          if (past1230 && beforeMarketClose) {
            const diagDone = await env.SIGNAL_KV.get(`diag_done_${todayDT}`);
            if (!diagDone) {
              try {
                console.log('[diag-today] cron-stall recovery: triggering close+open lifecycle');
                await handleDiagonalTrade(env, etNowDT);
                // handleDiagonalTrade writes both the close result AND the new
                // trade. Re-read to pick up the new state.
                openRaw = await env.SIGNAL_KV.get('diagonal_open_trade');
                open = openRaw ? JSON.parse(openRaw) : null;
                // Mark done so subsequent endpoint calls don't re-trigger
                await env.SIGNAL_KV.put(`diag_done_${todayDT}`, 'self-heal', { expirationTtl: 86400 });
              } catch (e) { console.warn('[diag-today] cron-stall recovery failed:', e.message); }
            }
          }
        }

        // ── Self-heal #2: stale live mids (cron-stall mid-trade) ────────────
        // If we have an open trade and its lastQuoteAt is older than 90s,
        // fetch fresh chain mids on this endpoint hit. Browser polls every
        // 30s so first stale poll triggers refresh, subsequent ones cache.
        if (open && open.status === 'open' && !isWeekendDT && !isHolidayDT) {
          const isMarketHoursDT = (etNowDT.getHours() > 9 ||
                                   (etNowDT.getHours() === 9 && etNowDT.getMinutes() >= 30)) &&
                                  etNowDT.getHours() < 16;
          const ageMs = open.lastQuoteAt ? (Date.now() - new Date(open.lastQuoteAt).getTime()) : Infinity;
          if (isMarketHoursDT && ageMs > 90_000) {
            try {
              const refreshToken = await getAccessToken(env);
              await refreshDiagonalLiveQuotes(env, refreshToken);
              // Re-read to get the freshly written values
              openRaw = await env.SIGNAL_KV.get('diagonal_open_trade');
              open = openRaw ? JSON.parse(openRaw) : null;
            } catch (e) { console.warn('[diag-today] on-demand refresh failed:', e.message); }
          }
        }

        const logRaw = await env.SIGNAL_KV.get('diagonal_closed_log');
        const closedLog = logRaw ? JSON.parse(logRaw) : [];
        const lastClosed = closedLog[0] || null;

        // Today's signal preview — even if 12:30 hasn't fired yet, the live
        // page can show "Diagonal pending" / "Skipped (NM)" / "GO at 12:30".
        // Fetch vixPct20d via the same path the cron uses.
        let vixPct20d = null, vixToday = null, sigPreview = null;
        try {
          const histData = await getHistory(env);
          if (Array.isArray(histData) && histData.length) {
            const todayRow = histData.find(r => r.date === todayDT);
            vixToday = todayRow?.vixOpen != null ? parseFloat(todayRow.vixOpen) : null;
            // FALLBACK: if morning signal block didn't write today's vixOpen
            // (Schwab outage at 9:30 AM), use the most recent vixClose from
            // history. Slightly off (close vs open) but typically within a few
            // basis points and beats "no signal at all". Keeps VIX 20d
            // percentile working without any live Schwab dependency.
            if (vixToday == null) {
              const prior = histData
                .filter(r => r.date < todayDT && r.vixClose != null && r.vixClose > 0)
                .sort((a, b) => a.date.localeCompare(b.date));
              if (prior.length) {
                vixToday = parseFloat(prior[prior.length - 1].vixClose);
                console.warn(`[diag-today] vixOpen missing for ${todayDT}; falling back to prior vixClose=${vixToday}`);
              }
            }
            const vix20 = histData
              .filter(r => r.date < todayDT && r.vixClose != null && r.vixClose > 0)
              .sort((a, b) => a.date.localeCompare(b.date))
              .slice(-20)
              .map(r => parseFloat(r.vixClose));
            // CANONICAL — single source: signal-engine.js computeVixPct20d.
            vixPct20d = computeVixPct20d(vixToday, vix20).pct;
          }
        } catch (_) { /* fall through to null */ }
        try { sigPreview = computeDiagonalSignal(etNowDT, vixPct20d); } catch (_) {}

        return jsonResp({
          date: todayDT,
          isWeekend: isWeekendDT,
          isHoliday: isHolidayDT,
          open,                              // currently active trade or null
          lastClosed,                        // most recently closed trade (for context)
          signal: sigPreview,                // {diagText, diagBadge, diagGo, diagSkipCode, vixPct20d}
          vixPct20d,
          serverTimeET: `${String(etNowDT.getHours()).padStart(2,'0')}:${String(etNowDT.getMinutes()).padStart(2,'0')}`,
        }, 200, publicCors);
      } catch (e) {
        return jsonResp({ error: e.message }, 500, publicCors);
      }
    }

    // ── GET /heartbeat?token=... ── External 1-min cron replacement.
    // Cloudflare Workers' built-in cron scheduler drops ticks silently
    // (recurring issue — see live page self-heal endpoints elsewhere).
    // External monitors (cron-job.org, UptimeRobot Pro, GH Actions, etc.)
    // hit this endpoint every minute during market hours and force
    // handleScheduled() to run — bypassing CF's mood.
    //
    // Dedicated token (HEARTBEAT_TOKEN secret, separate from SYNC_SECRET)
    // so the URL is safe to paste into 3rd-party monitor config without
    // exposing the master admin secret.
    //
    // Logs last_heartbeat_at in KV so the user can verify the external
    // monitor is actually hitting us.
    if (url.pathname === '/heartbeat' && request.method === 'GET') {
      const tokenH = url.searchParams.get('token');
      if (!tokenH || tokenH !== env.HEARTBEAT_TOKEN) {
        return jsonResp({ error: 'Unauthorized — bad/missing token param' }, 401,
          { 'Access-Control-Allow-Origin': '*' });
      }
      // Respond instantly with 200. Run handleScheduled() in the background
      // via ctx.waitUntil() so cron-job.org (and other 30s-timeout monitors)
      // don't log timeouts on the busy 9:30 ET tick. Cloudflare keeps the
      // worker alive up to its wall-clock budget to complete the background
      // task — even if the client connection closed.
      const startMs = Date.now();
      const runScheduled = async () => {
        let scheduledResult, error = null;
        try {
          scheduledResult = await handleScheduled(env);
        } catch (e) {
          error = e.message;
          scheduledResult = { status: 'error', error: e.message };
        }
        const ranMs = Date.now() - startMs;
        try {
          await env.SIGNAL_KV.put('last_heartbeat', JSON.stringify({
            at: new Date().toISOString(),
            ranMs,
            status: scheduledResult?.status || 'unknown',
            error,
            ua: request.headers.get('User-Agent') || '',
            ip: request.headers.get('cf-connecting-ip') || '',
          }));
        } catch (_) {}
        try {
          await env.SIGNAL_KV.put('last_run', JSON.stringify({
            ...(scheduledResult || {}),
            date: new Date().toISOString(),
            source: 'heartbeat',
          }));
        } catch (_) {}
      };
      if (ctx && typeof ctx.waitUntil === 'function') {
        ctx.waitUntil(runScheduled());
        return jsonResp({ ok: true, queued: true }, 200, { 'Access-Control-Allow-Origin': '*' });
      }
      // Fallback: synchronous (e.g., in `wrangler dev` without ctx)
      await runScheduled();
      return jsonResp({ ok: true, ranMs: Date.now() - startMs }, 200, { 'Access-Control-Allow-Origin': '*' });
    }

    // GET /tasty-oauth-start — kick off Tastytrade OAuth.
    // Visit this in a browser → redirects to Tastytrade authorize page →
    // user approves → Tastytrade redirects back to /tasty-oauth-callback.
    if (url.pathname === '/tasty-oauth-start' && request.method === 'GET') {
      if (!env.TASTYTRADE_CLIENT_ID) {
        return jsonResp({ error: 'TASTYTRADE_CLIENT_ID not configured' }, 500, { 'Access-Control-Allow-Origin': '*' });
      }
      return Response.redirect(tastyAuthorizeUrl(env), 302);
    }

    // GET /tasty-oauth-callback?code=... — Tastytrade redirects here after
    // the user approves. Exchanges code for refresh_token, stores it in KV,
    // and shows a confirmation page.
    if (url.pathname === '/tasty-oauth-callback' && request.method === 'GET') {
      const code = url.searchParams.get('code');
      const err  = url.searchParams.get('error');
      if (err) {
        return new Response(`<h1>OAuth error</h1><p>${err}: ${url.searchParams.get('error_description') || ''}</p>`,
          { status: 400, headers: { 'Content-Type': 'text/html' } });
      }
      if (!code) {
        return new Response(`<h1>Missing code parameter</h1>`,
          { status: 400, headers: { 'Content-Type': 'text/html' } });
      }
      try {
        const tokens = await tastyExchangeCode(env, code);
        if (!tokens.refresh_token) throw new Error('no refresh_token in response: ' + JSON.stringify(tokens).slice(0, 300));
        // Persist refresh token (long-lived) and cache access token
        await env.SIGNAL_KV.put('tasty_refresh_token', tokens.refresh_token);
        if (tokens.access_token) {
          await env.SIGNAL_KV.put('tasty_access_token', JSON.stringify({
            access_token: tokens.access_token,
            expires_at: Date.now() + ((tokens.expires_in || 900) * 1000),
          }), { expirationTtl: tokens.expires_in || 900 });
        }
        return new Response(
          `<html><body style="font-family:system-ui;padding:40px;background:#0f1117;color:#e5e7eb">
            <h1 style="color:#22c55e">✓ Tastytrade connected</h1>
            <p>Refresh token saved. The worker can now mint access tokens automatically.</p>
            <p>Test it: <a style="color:#818cf8" href="/tasty-vix-test">/tasty-vix-test</a></p>
          </body></html>`,
          { status: 200, headers: { 'Content-Type': 'text/html' } }
        );
      } catch (e) {
        return new Response(
          `<html><body style="font-family:system-ui;padding:40px;background:#0f1117;color:#e5e7eb">
            <h1 style="color:#ef4444">OAuth callback failed</h1>
            <pre>${e.message}</pre>
          </body></html>`,
          { status: 500, headers: { 'Content-Type': 'text/html' } }
        );
      }
    }

    // GET /tasty-vix-test — debug endpoint to verify Tastytrade VIX path
    // Returns the current VIX price as Tastytrade sees it, plus session
    // status and which endpoint shape worked. No auth required — read-only
    // debug. Behind public CORS like /health.
    if (url.pathname === '/tasty-vix-test' && request.method === 'GET') {
      const publicCors = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type',
        'Access-Control-Max-Age': '86400',
      };
      try {
        const cached = await env.SIGNAL_KV.get('tasty_session_token');
        const result = await tastyGetVix(env);
        return jsonResp({
          ok: true,
          sessionCached: !!cached,
          vix: result,
        }, 200, publicCors);
      } catch (e) {
        return jsonResp({ ok: false, error: e.message }, 500, publicCors);
      }
    }

    if (url.pathname === '/tasty-spx-test' && request.method === 'GET') {
      const publicCors = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type',
        'Access-Control-Max-Age': '86400',
      };
      try {
        const cached = await env.SIGNAL_KV.get('tasty_session_token');
        const result = await tastyGetSpx(env);
        return jsonResp({
          ok: true,
          sessionCached: !!cached,
          spx: result,
        }, 200, publicCors);
      } catch (e) {
        return jsonResp({ ok: false, error: e.message }, 500, publicCors);
      }
    }

    // GET /tasty-chain-test?exp=YYYY-MM-DD[&strikes=20&type=BOTH&compare=1]
    // Test the tastyFetchSpxChain wrapper end-to-end. With ?compare=1 it also
    // fetches the same chain via Schwab so you can verify shape parity and
    // quote agreement. Read-only. Doesn't affect any live path.
    if (url.pathname === '/tasty-chain-test' && request.method === 'GET') {
      const cors = { 'Access-Control-Allow-Origin': '*' };
      try {
        const root = url.searchParams.get('root') || 'SPXW';
        const exp = url.searchParams.get('exp');
        const strikes = parseInt(url.searchParams.get('strikes') || '20', 10);
        const ctype = (url.searchParams.get('type') || 'BOTH').toUpperCase();
        const compare = url.searchParams.get('compare') === '1';
        const opts = { root, strikeCount: strikes, contractType: ctype };
        if (exp) opts.expirations = [exp];

        const tStart = Date.now();
        const tasty = await tastyFetchSpxChain(env, opts);
        const tastyMs = Date.now() - tStart;

        const sample = {};
        const firstCallExp = Object.keys(tasty.callExpDateMap)[0];
        if (firstCallExp) {
          const strikeKeys = Object.keys(tasty.callExpDateMap[firstCallExp]).sort((a,b) =>
            Math.abs(parseFloat(a) - (tasty.spot||0)) - Math.abs(parseFloat(b) - (tasty.spot||0)));
          const atm = strikeKeys.slice(0, 3);
          sample.tasty = atm.map(k => {
            const c = tasty.callExpDateMap[firstCallExp]?.[k]?.[0];
            const p = tasty.putExpDateMap[firstCallExp]?.[k]?.[0];
            return { strike: k, call: c && { bid: c.bid, ask: c.ask, mark: c.mark, delta: c.delta, gamma: c.gamma, symbol: c.symbol }, put: p && { bid: p.bid, ask: p.ask, mark: p.mark, delta: p.delta, gamma: p.gamma, symbol: p.symbol } };
          });
        }

        let schwab = null, schwabMs = null, schwabErr = null, schwabSample = null;
        if (compare) {
          const sStart = Date.now();
          try {
            const token = await getAccessToken(env);
            const params = new URLSearchParams({
              symbol: '$SPX', strikeCount: String(strikes), includeUnderlyingQuote: 'true', strategy: 'SINGLE',
            });
            if (exp) { params.set('fromDate', exp); params.set('toDate', exp); }
            if (ctype !== 'BOTH') params.set('contractType', ctype);
            const data = await fetchSchwabJSON(`https://api.schwabapi.com/marketdata/v1/chains?${params}`, token, env);
            schwabMs = Date.now() - sStart;
            schwab = {
              spot: data.underlyingPrice || data.underlying?.last,
              callExps: Object.keys(data.callExpDateMap || {}),
              putExps: Object.keys(data.putExpDateMap || {}),
            };
            // Sample ATM strikes via Schwab
            const sExp = Object.keys(data.callExpDateMap || {})[0];
            if (sExp) {
              const sStrikes = Object.keys(data.callExpDateMap[sExp]).sort((a,b) =>
                Math.abs(parseFloat(a) - (schwab.spot||0)) - Math.abs(parseFloat(b) - (schwab.spot||0)));
              schwabSample = sStrikes.slice(0, 3).map(k => {
                const c = data.callExpDateMap[sExp]?.[k]?.[0];
                const p = (data.putExpDateMap || {})[sExp]?.[k]?.[0];
                return { strike: k, call: c && { bid: c.bid, ask: c.ask, mark: c.mark, delta: c.delta, gamma: c.gamma, symbol: c.symbol }, put: p && { bid: p.bid, ask: p.ask, mark: p.mark, delta: p.delta, gamma: p.gamma, symbol: p.symbol } };
              });
            }
          } catch (e) { schwabErr = e.message; }
        }

        return jsonResp({
          ok: true,
          opts,
          tasty: {
            spot: tasty.spot, fetchMs: tastyMs,
            callExpKeys: Object.keys(tasty.callExpDateMap),
            putExpKeys: Object.keys(tasty.putExpDateMap),
            stats: tasty._stats,
            sample: sample.tasty,
          },
          schwab: compare ? { spot: schwab?.spot, fetchMs: schwabMs, error: schwabErr, callExps: schwab?.callExps, putExps: schwab?.putExps, sample: schwabSample } : 'skip (pass ?compare=1)',
        }, 200, cors);
      } catch (e) {
        return jsonResp({ ok: false, error: e.message, stack: e.stack?.slice(0, 400) }, 500, cors);
      }
    }

    // GET /debug-morning-dual
    // Exercises the SAME dual-source signal logic the morning cron uses,
    // but with single quick fetches (no polling for new ticks). Returns
    // JSON with both signals + both rendered Discord messages + any per-
    // source errors. Does NOT post to Discord. Use to verify the code path
    // works without waiting for 9:30 ET.
    //   ?force_no_schwab=1  — simulate Schwab failure (skip Schwab fetch)
    //   ?force_no_tasty=1   — simulate Tasty failure (skip Tasty fetch)
    //   ?force_no_token=1   — simulate Schwab token unavailable
    if (url.pathname === '/tasty-index-test' && request.method === 'GET') {
      const sym = url.searchParams.get('sym') || 'VIX';
      let result;
      try { result = await tastyGetIndexQuote(env, sym); delete result.raw; }
      catch (e) { result = { error: e.message }; }
      return new Response(JSON.stringify({ sym, ...result }, null, 2),
        { status: 200, headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' } });
    }

    // Manual "*-now" trigger routes hit Schwab / GitHub / Discord. Gate them with
    // SYNC_SECRET (header or ?secret=), like the other trigger routes, so they can't
    // be invoked anonymously (audit 2026-06-22). Cron calls the underlying functions
    // directly — not these HTTP routes — so scheduled runs are unaffected.
    if (request.method !== 'OPTIONS' &&
        ['/cyclicality-append-now', '/score-advisories-now', '/research-persist-now',
         '/cot-refresh-now', '/watchdog-now', '/weekly-digest-now', '/vix-decomp-now',
         '/remirror-history', '/history-patch'].includes(url.pathname)) {
      const s = request.headers.get('X-Sync-Secret') || url.searchParams.get('secret');
      if (!s || (s !== env.SYNC_SECRET && s !== env.GEXM_TRIGGER_TOKEN)) return jsonResp({ error: 'Unauthorized' }, 401, corsHeaders);
    }

    if (url.pathname === '/cyclicality-append-now' && request.method === 'GET') {
      let result;
      try { result = await appendCyclicalityDays(env, { symbol: url.searchParams.get('symbol') === 'ndx' ? '%24NDX' : '%24SPX', file: url.searchParams.get('symbol') === 'ndx' ? 'cyclicality_ndx.json' : 'cyclicality_data.json', backDays: parseInt(url.searchParams.get('backDays') || '12', 10) }); } catch (e) { result = { error: e.message }; }
      return new Response(JSON.stringify(result, null, 2),
        { status: 200, headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' } });
    }

    if (url.pathname === '/score-advisories-now' && request.method === 'GET') {
      let result;
      try { result = await scoreAdvisories(env); } catch (e) { result = { error: e.message }; }
      return new Response(JSON.stringify(result, null, 2),
        { status: 200, headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' } });
    }

    if (url.pathname === '/advisory-lines' && request.method === 'GET') {
      // The three informational lines the REAL morning message carries —
      // served to the dashboard so its test/manual sends match production.
      const out = { tilt: null, gex: null, daytype: null, volflow: null };
      const etNowA = toET(new Date());
      try { out.tilt = await computeTiltLine(env, isoDateET(etNowA)); } catch (_) {}
      try { out.gex = await computeGexLine(env); } catch (_) {}
      try { out.daytype = await computeCycleLine(env, etNowA); } catch (_) {}
      try { out.volflow = await computeVolFlowLine(env, etNowA); } catch (_) {}
      try { out.m8bfwr = await computeM8bfWrLine(env, etNowA); } catch (_) {}
      return new Response(JSON.stringify(out),
        { status: 200, headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' } });
    }

    if (url.pathname === '/research-persist-now' && request.method === 'GET') {
      // Manual trigger for the EOD research persist (idempotent upserts).
      // ?date=YYYY-MM-DD back-heals a missed day (KV captures live 90d).
      let etNowR = toET(new Date());
      const ovr = url.searchParams.get('date');
      if (ovr && /^\d{4}-\d{2}-\d{2}$/.test(ovr)) etNowR = toET(new Date(`${ovr}T16:30:00-04:00`));
      let result;
      try { result = await persistResearchArtifacts(env, etNowR); }
      catch (e) { result = { error: e.message }; }
      return new Response(JSON.stringify({ date: isoDateET(etNowR), result }, null, 2),
        { status: 200, headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' } });
    }

    if (url.pathname === '/tail-today' && request.method === 'GET') {
      // Tail Hedge live status for the Live page (user 2026-06-11: the only
      // manual strategy must appear among today's trades when triggered).
      // Truth source = getTailHedgeStatusLine (same line as the morning msg).
      // RETIRED 2026-08-03: the route stays (old clients poll it) but reports
      // the retirement instead of a signal. See tasks/TAIL_HEDGE_ARCHIVE.md.
      if (TAIL_RETIRED) {
        return new Response(JSON.stringify({ retired: true, since: '2026-08-03',
          line: null, trigger: null, candidate: null, open: null,
          note: 'Tail Hedge retired 2026-08-03 — no live signal.' }),
          { headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' } });
      }
      const etNowT = toET(new Date());
      const todayT = isoDateET(etNowT);
      const line = await getTailHedgeStatusLine(env);
      let st = null, snap = null, openRec = null;
      try { st = JSON.parse(await env.SIGNAL_KV.get('tail_trigger_state') || 'null'); } catch (_) {}
      try { snap = JSON.parse(await env.SIGNAL_KV.get(`tail_put_snap_${todayT}`) || 'null'); } catch (_) {}
      try { openRec = JSON.parse(await env.SIGNAL_KV.get(`cor1m_open_${todayT}`) || 'null'); } catch (_) {}
      // candidate = nearest-expiry put closest to Δ-0.10 from the 9:45 snapshot
      let candidate = null;
      if (snap && Array.isArray(snap.puts) && snap.puts.length) {
        const e0 = snap.puts[0].e;
        candidate = snap.puts.filter(p => p.e === e0)
          .sort((a, b) => Math.abs(a.d + 0.10) - Math.abs(b.d + 0.10))[0] || null;
      }
      // Frozen bot-tradeable open: cron freezes it at/after 9:45 (this is a
      // fallback that freezes if a client polls first). After settleTailEOD it
      // carries status='closed' + pnl and is surfaced as `lastClosed`.
      let tailOpen = null;
      try { tailOpen = await freezeTailOpenIfDue(env, etNowT, line); } catch (_) {}
      // Settled days → Final P&L card on the live page (mirrors bobf/gxbf lastClosed).
      let lastClosed = null;
      try {
        const lcRaw = await env.SIGNAL_KV.get('tail_closed_log');
        const lc = lcRaw ? JSON.parse(lcRaw) : [];
        lastClosed = lc[0] || null;
      } catch (_) {}
      // A settled (closed) open is shown as lastClosed, not as an active open.
      if (tailOpen && tailOpen.status === 'closed') tailOpen = null;
      return new Response(JSON.stringify({
        date: todayT, line,
        active: line.includes('▶ TRADE'), skip: line.includes('SKIP today'),
        state: st, cor1m: openRec?.cor1m ?? null, vvix: openRec?.vvix ?? null,
        spot: snap?.spot ?? null, snapAt: snap?.at ?? null, candidate,
        open: tailOpen, lastClosed,
      }), { status: 200, headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' } });
    }

    if (url.pathname === '/settle-tail' && request.method === 'POST') {
      // Manual/backfill trigger for the Tail Hedge EOD settle + safety fallback
      // if the EOD cron missed it. Origin-gated (mirrors /send-card). Idempotent:
      // settleTailEOD flips status→'closed' and upsertHistoryGitHub won't
      // overwrite an existing tailPL. Optional ?spxClose=NNNN overrides the close.
      const originS = request.headers.get('Origin') || '';
      if (originS !== 'https://rava8989.github.io') {
        return new Response(JSON.stringify({ error: 'forbidden' }),
          { status: 403, headers: { 'Content-Type': 'application/json' } });
      }
      const etNowS = toET(new Date());
      // Optional ?date=YYYY-MM-DD settles a PAST day's orphaned trade (must not
      // be in the future). Default: today, as before.
      const dateQ = url.searchParams.get('date');
      const dateS = (dateQ && /^\d{4}-\d{2}-\d{2}$/.test(dateQ) && dateQ <= isoDateET(etNowS))
        ? dateQ : null;
      const targetISO = dateS || isoDateET(etNowS);
      let spxCloseS = null;
      const qpS = url.searchParams.get('spxClose');
      if (qpS && !isNaN(parseFloat(qpS))) spxCloseS = parseFloat(qpS);
      if (spxCloseS == null) {
        // Use the CANONICAL EOD close already in history (1-min candle ≤16:15,
        // the exact value the other strategies settled against) so tail P&L is
        // consistent — NOT quote.closePrice (that's a prior/quote field).
        try {
          const histS = await getHistory(env);
          const rowS = (histS || []).find(r => r.date === targetISO);
          if (rowS && rowS.spxClose != null) spxCloseS = parseFloat(rowS.spxClose);
        } catch (_) {}
      }
      let resultS;
      try { resultS = await settleTailEOD(env, etNowS, spxCloseS, dateS); }
      catch (e) { resultS = { error: e.message }; }
      return new Response(JSON.stringify({ date: targetISO, spxClose: spxCloseS, result: resultS }, null, 2),
        { status: 200, headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': 'https://rava8989.github.io' } });
    }

    // ── POST /scrape-backfill?from=ISO&to=ISO ── Recover missing days in
    // scraped_signals.csv in a DEDICATED invocation. The KV-trigger path
    // piggybacks on a heavy tick and gets killed mid-flight when the day is
    // large (2026-07-09 recovery died silently — trigger consumed, no result,
    // no rows). Auth: SYNC_SECRET or GEXM token.
    if (url.pathname === '/scrape-backfill' && request.method === 'POST') {
      const secretSB = request.headers.get('X-Sync-Secret') || url.searchParams.get('secret');
      if (!secretSB || (secretSB !== env.SYNC_SECRET && secretSB !== env.GEXM_TRIGGER_TOKEN)) {
        return jsonResp({ error: 'Unauthorized' }, 401, {});
      }
      const fromSB = url.searchParams.get('from'), toSB = url.searchParams.get('to') || fromSB;
      if (!fromSB || !/^\d{4}-\d{2}-\d{2}$/.test(fromSB)) return jsonResp({ error: 'bad from' }, 400, {});
      try {
        const resSB = await backfillScrapedSignals(env, fromSB, toSB);
        await env.SIGNAL_KV.put('scrape_backfill_result', JSON.stringify({ ...resSB, ts: Date.now() }), { expirationTtl: 7 * 86400 });
        return jsonResp(resSB, 200, {});
      } catch (e) { return jsonResp({ error: e.message }, 500, {}); }
    }

    if (url.pathname === '/settle-orphans' && request.method === 'POST') {
      // Manual heal for a missed EOD (lessons P18): sweep ALL strategies'
      // stranded trades, then run the m8bf backfills (which need the swept
      // rows' spxClose). Origin-gated like /settle-tail. Idempotent.
      const originO = request.headers.get('Origin') || '';
      if (originO !== 'https://rava8989.github.io') {
        return new Response(JSON.stringify({ error: 'forbidden' }),
          { status: 403, headers: { 'Content-Type': 'application/json' } });
      }
      const etNowO = toET(new Date());
      let sweepO = [];
      try { sweepO = await sweepOrphanSettles(env, etNowO); }
      catch (e) { sweepO = [{ error: e.message }]; }
      let bplO = {}, bwrO = {};
      try { bplO = await backfillMissingPL(env); } catch (e) { bplO = { error: e.message }; }
      try { bwrO = await backfillMissingWR(env); } catch (e) { bwrO = { error: e.message }; }
      return new Response(JSON.stringify({ sweep: sweepO, backfill_pl: bplO, backfill_wr: bwrO }, null, 2),
        { status: 200, headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': 'https://rava8989.github.io' } });
    }

    if (url.pathname === '/eod-history' && request.method === 'GET') {
      // Read-only Schwab daily-candle proxy (2026-06-12, COT edge study +
      // general research). Public like the other market-data debug routes —
      // serves OHLC only, no account access. ?symbol=FXE&years=12
      const sym = (url.searchParams.get('symbol') || '').toUpperCase().replace(/[^A-Z0-9$./]/g, '');
      const years = Math.min(20, Math.max(1, parseInt(url.searchParams.get('years') || '10', 10)));
      if (!sym) return new Response('{"error":"symbol required"}',
        { status: 400, headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' } });
      try {
        const tk = await getAccessToken(env);
        // ?minuteDay=YYYY-MM-DD probes 1-min depth: returns that day's 1-min count
        const md = url.searchParams.get('minuteDay');
        if (md && /^\d{4}-\d{2}-\d{2}$/.test(md)) {
          const s = Date.parse(`${md}T08:00:00Z`), e = Date.parse(`${md}T23:00:00Z`);
          const dd = await fetchSchwabJSON(
            `https://api.schwabapi.com/marketdata/v1/pricehistory?symbol=${encodeURIComponent(sym)}&periodType=day&frequencyType=minute&frequency=1&startDate=${s}&endDate=${e}&needExtendedHoursData=false`,
            tk, env);
          const cc = (dd.candles || []).filter(c => c.close > 0);
          const bars = cc.map(c => { const d = toET(new Date(c.datetime)); return { t: `${String(d.getHours()).padStart(2,'0')}:${String(d.getMinutes()).padStart(2,'0')}`, o: c.open, c: c.close }; });
          const at = hhmm => bars.find(b => b.t === hhmm) || null;
          return new Response(JSON.stringify({ symbol: sym, minuteDay: md, n: cc.length,
            open0930: at('09:30'), close1559: at('15:59'), close1600: at('16:00'), close1615: at('16:15'), last: bars[bars.length - 1] ?? null }),
            { status: 200, headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' } });
        }
        const end = Date.now(), start = end - years * 365.25 * 86400000;
        const data = await fetchSchwabJSON(
          `https://api.schwabapi.com/marketdata/v1/pricehistory?symbol=${encodeURIComponent(sym)}&periodType=year&frequencyType=daily&startDate=${Math.round(start)}&endDate=${end}&needExtendedHoursData=false`,
          tk, env);
        const ohlc = url.searchParams.get('ohlc') === '1';
        const candles = (data.candles || []).filter(c => c.close > 0)
          .map(c => ohlc
            ? [isoDateET(toET(new Date(c.datetime))), c.open, c.high, c.low, c.close]
            : [isoDateET(toET(new Date(c.datetime))), c.close]);
        return new Response(JSON.stringify({ symbol: sym, n: candles.length, candles }),
          { status: 200, headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' } });
      } catch (e) {
        return new Response(JSON.stringify({ error: e.message }),
          { status: 500, headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' } });
      }
    }

    if (url.pathname === '/cot-refresh-now' && request.method === 'GET') {
      // Manual trigger for the weekly COT self-feed (idempotent).
      let result;
      try { result = await cotWeeklyRefresh(env); }
      catch (e) { result = { error: e.message }; }
      return new Response(JSON.stringify(result, null, 2),
        { status: 200, headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' } });
    }

    if (url.pathname === '/subscribers') {
      // Manage signal-DM recipients. Gated by the sync secret. CORS-open for
      // the dashboard (which sends the secret in the body / query).
      const sCors = { 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'GET, POST, OPTIONS', 'Access-Control-Allow-Headers': 'Content-Type, X-Sync-Secret' };
      if (request.method === 'OPTIONS') return new Response(null, { status: 204, headers: sCors });
      const body = request.method === 'POST' ? await request.json().catch(() => ({})) : {};
      const secret = request.headers.get('X-Sync-Secret') || url.searchParams.get('secret') || body.secret;
      if (!secret || secret !== env.SYNC_SECRET) return jsonResp({ error: 'Unauthorized' }, 401, sCors);

      let subs = await getSubscribers(env);
      if (request.method === 'GET') return jsonResp({ subscribers: subs }, 200, sCors);

      const act = body.action;
      if (act === 'add') {
        const id = String(body.id || '').trim();
        if (!/^\d{15,21}$/.test(id)) return jsonResp({ error: 'id must be a 15-21 digit Discord user ID' }, 400, sCors);
        if (!subs.find(s => s.id === id)) subs.push({ id, label: (body.label || '').slice(0, 40), paused: false });
        await env.SIGNAL_KV.put('signal_subscribers', JSON.stringify(subs));
        return jsonResp({ ok: true, subscribers: subs }, 200, sCors);
      }
      if (act === 'remove') {
        subs = subs.filter(s => s.id !== String(body.id));
        await env.SIGNAL_KV.put('signal_subscribers', JSON.stringify(subs));
        return jsonResp({ ok: true, subscribers: subs }, 200, sCors);
      }
      if (act === 'pause') {
        subs = subs.map(s => s.id === String(body.id) ? { ...s, paused: !s.paused } : s);
        await env.SIGNAL_KV.put('signal_subscribers', JSON.stringify(subs));
        return jsonResp({ ok: true, subscribers: subs }, 200, sCors);
      }
      if (act === 'test') {
        // DM one recipient a test, surfacing Discord's exact error (e.g. 403
        // = not in a mutual server / DMs closed).
        const r = await sendDiscordDM(env, String(body.id),
          '🔔 Test from Σ3 Signals — you are set to receive trade signals here. (If you got this, delivery works.)');
        return jsonResp({ ok: !!r.ok, status: r.status, error: r.error,
          hint: !r.ok && r.status === 403 ? 'This user must share a server with the bot AND allow DMs from server members.' : undefined }, 200, sCors);
      }
      if (act === 'test-channel') {
        // Post a test to the broadcast channel via its webhook (KV signals_webhook_url).
        const r = await postSignalsChannel(env, '🔔 **Test from Σ3** — signals channel is wired up. Live trades will post here. (manual test)');
        return jsonResp({ ok: !!r.ok, status: r.status, error: r.error,
          hint: r.skipped ? 'No signals_webhook_url set in KV — create a channel webhook first.' : undefined }, 200, sCors);
      }
      return jsonResp({ error: 'unknown action' }, 400, sCors);
    }

    if (url.pathname === '/bot-info' && request.method === 'GET') {
      // Diagnostic: which servers (guilds) the bot is in + its identity.
      // The bot can DM anyone who shares one of these guilds with it.
      // AUTH (2026-07-06 audit): leaks bot identity + guild list — gate it.
      if ((request.headers.get('X-Sync-Secret') || url.searchParams.get('secret')) !== env.SYNC_SECRET) {
        return jsonResp({ error: 'Unauthorized' }, 401, corsHeaders);
      }
      if (!env.DISCORD_TOKEN) return jsonResp({ error: 'no DISCORD_TOKEN (bot uses a proxy path)' }, 200, corsHeaders);
      try {
        const meR = await fetch('https://discord.com/api/v10/users/@me', { headers: { Authorization: `Bot ${env.DISCORD_TOKEN}` } });
        const me = await meR.json();
        const gR = await fetch('https://discord.com/api/v10/users/@me/guilds', { headers: { Authorization: `Bot ${env.DISCORD_TOKEN}` } });
        const guilds = await gR.json();
        return jsonResp({ bot: `${me.username}#${me.discriminator} (${me.id})`,
          guilds: Array.isArray(guilds) ? guilds.map(g => ({ name: g.name, id: g.id })) : guilds }, 200, corsHeaders);
      } catch (e) { return jsonResp({ error: e.message }, 500, corsHeaders); }
    }

    if (url.pathname === '/cyclicality-today' && request.method === 'GET') {
      // CycleLab live actual — today's session-so-far (KV, written by the
      // cron every 5 min during RTH). Pure KV read: zero Schwab calls.
      const raw = await env.SIGNAL_KV.get(url.searchParams.get('symbol') === 'ndx' ? 'cyc_today_ndx' : 'cyc_today');
      return new Response(raw || 'null',
        { status: 200, headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' } });
    }

    if (url.pathname === '/watchdog-now' && request.method === 'GET') {
      let result;
      try { result = await dataCompletenessCheck(env, toET(new Date())); }
      catch (e) { result = { error: e.message }; }
      return new Response(JSON.stringify(result, null, 2),
        { status: 200, headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' } });
    }

    // GET /remirror-history — Repair: force-push current KV history → GitHub.
    // KV is the source of truth; this re-mirrors it to the GitHub copy the
    // dashboard reads, healing any silent mirror drift (e.g. a settle whose
    // KV→GitHub PUT lost a sha race, as 2026-06-23 tailPL did). Read-only on
    // KV — never the reverse. Auth: SYNC_SECRET via the guard above.
    if (url.pathname === '/remirror-history' && request.method === 'GET') {
      let result;
      try {
        const content = await getHistory(env);
        const r = await mirrorHistoryToGitHub(env, content, 'manual: re-mirror KV→GitHub (repair drift)');
        result = { ok: r.ok, entries: Array.isArray(content) ? content.length : null, mirror: r };
      } catch (e) { result = { ok: false, error: e.message }; }
      return new Response(JSON.stringify(result, null, 2),
        { status: result.ok ? 200 : 502, headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' } });
    }

    // POST /history-patch — owner repair tool: field-level patches applied to
    // the AUTHORITATIVE history store (KV) and then mirrored to GitHub.
    // Body: [{date:'YYYY-MM-DD', set:{field:value,...}, del:['field',...]}].
    // Direct git edits to history_data.json get stomped by the next KV mirror
    // (learned 2026-07-30: the gated-day restatement was wiped by /remirror-
    // history) — restatements must go through here.
    if (url.pathname === '/history-patch' && request.method === 'POST') {
      let result;
      try {
        const patches = await request.json();
        if (!Array.isArray(patches)) throw new Error('body must be an array of {date, set, del}');
        const rows = await getHistory(env);
        const applied = [];
        for (const p of patches) {
          const row = rows.find(r => r.date === p.date);
          if (!row) { applied.push({ date: p.date, missing: true }); continue; }
          for (const [k, v] of Object.entries(p.set || {})) row[k] = v;
          for (const k of (p.del || [])) delete row[k];
          applied.push({ date: p.date, ok: true });
        }
        await setHistory(env, rows, { dateStr: 'history-patch' });
        const m = await mirrorHistoryToGitHub(env, rows, 'manual: history-patch (owner repair)');
        result = { ok: true, applied, mirror: m };
      } catch (e) { result = { ok: false, error: e.message }; }
      return new Response(JSON.stringify(result, null, 2),
        { status: result.ok ? 200 : 400, headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' } });
    }

    if (url.pathname === '/weekly-digest-now' && request.method === 'GET') {
      let result;
      try { result = await weeklyDigest(env); }
      catch (e) { result = { error: e.message }; }
      return new Response(JSON.stringify(result, null, 2),
        { status: 200, headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' } });
    }

    if (url.pathname === '/vix-decomp-now' && request.method === 'GET') {
      // Manual trigger for the daily vol-flow decomposition (idempotent).
      // ?date=YYYY-MM-DD back-heals a missed day from the KV surface snap.
      let etNowV = toET(new Date());
      const ovrV = url.searchParams.get('date');
      if (ovrV && /^\d{4}-\d{2}-\d{2}$/.test(ovrV)) etNowV = toET(new Date(`${ovrV}T16:30:00-04:00`));
      let result;
      try { result = await computeVixDecompDaily(env, etNowV); }
      catch (e) { result = { error: e.message }; }
      let line = null;
      try { line = await computeVolFlowLine(env, etNowV); } catch (_) {}
      return new Response(JSON.stringify({ date: isoDateET(etNowV), result, advisoryLine: line }, null, 2),
        { status: 200, headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' } });
    }

    if (url.pathname === '/debug-morning-dual' && request.method === 'GET') {
      const cors = { 'Access-Control-Allow-Origin': '*' };
      const forceNoSchwab = url.searchParams.get('force_no_schwab') === '1';
      const forceNoTasty  = url.searchParams.get('force_no_tasty')  === '1';
      const forceNoToken  = url.searchParams.get('force_no_token')  === '1';
      const out = { ok: true, simulated: { forceNoSchwab, forceNoTasty, forceNoToken }, errors: {} };
      try {
        // etNow needed by the freshness check below — declare BEFORE Schwab fetch.
        const etNow = toET(new Date());
        const todayISO = isoDateET(etNow);
        // 1. Pull VIX + SPX from BOTH sources independently (single-fetch).
        let token = null;
        if (!forceNoToken) {
          try { token = await getAccessToken(env); }
          catch (e) { out.errors.schwabToken = e.message; }
        } else {
          out.errors.schwabToken = '(forced)';
        }

        let vixSchwab = null, spxSchwab = null, vixTasty = null, spxTasty = null;
        let vixSchwabTs = null, spxSchwabTs = null, vixTastyTs = null, spxTastyTs = null;
        let vixSchwabFresh = null, spxSchwabFresh = null, vixTastyFresh = null, spxTastyFresh = null;

        // Same freshness gate the morning cron uses: today's date + >= 9:30 ET.
        function _isFresh(ts) {
          if (ts == null) return false;
          const d = (typeof ts === 'number') ? new Date(ts) : new Date(String(ts));
          if (!isFinite(d.getTime())) return false;
          const et = toET(d);
          if (et.toDateString() !== etNow.toDateString()) return false;
          return (et.getHours() * 60 + et.getMinutes()) >= 570;
        }

        // Schwab VIX + SPX (parallel, single fetch each) — captures ts + freshness
        if (token && !forceNoSchwab) {
          const [vR, sR] = await Promise.allSettled([
            fetchSchwabJSON(`https://api.schwabapi.com/marketdata/v1/quotes?symbols=%24VIX&fields=quote`, token, env),
            fetchSchwabJSON(`https://api.schwabapi.com/marketdata/v1/quotes?symbols=%24SPX&fields=quote`, token, env),
          ]);
          if (vR.status === 'fulfilled') {
            const q = vR.value?.['$VIX']?.quote;
            if (q?.lastPrice != null) {
              vixSchwab = parseFloat(parseFloat(q.lastPrice).toFixed(2));
              vixSchwabTs = q.tradeTime;
              vixSchwabFresh = _isFresh(q.tradeTime);
            }
          } else { out.errors.schwabVix = vR.reason?.message || String(vR.reason); }
          if (sR.status === 'fulfilled') {
            const q = sR.value?.['$SPX']?.quote;
            // morning cron uses openPrice only (rejects lastPrice — that's yesterday's close pre-market)
            if (q?.openPrice != null && q.openPrice > 0) {
              spxSchwab = parseFloat(parseFloat(q.openPrice).toFixed(2));
              spxSchwabTs = q.tradeTime;
              spxSchwabFresh = _isFresh(q.tradeTime);
            }
          } else { out.errors.schwabSpx = sR.reason?.message || String(sR.reason); }
        } else if (forceNoSchwab) {
          out.errors.schwabVix = '(forced)'; out.errors.schwabSpx = '(forced)';
        }

        // Tasty VIX + SPX (parallel)
        if (!forceNoTasty) {
          const [vR, sR] = await Promise.allSettled([tastyGetVix(env), tastyGetSpx(env)]);
          if (vR.status === 'fulfilled') {
            vixTasty = parseFloat(vR.value.price.toFixed(2));
            vixTastyTs = vR.value.raw?.['updated-at'] || vR.value.asOf;
            vixTastyFresh = _isFresh(vixTastyTs);
          } else out.errors.tastyVix = vR.reason?.message || String(vR.reason);
          if (sR.status === 'fulfilled') {
            const v = sR.value.open ?? sR.value.last ?? sR.value.price;
            if (v != null) {
              spxTasty = parseFloat(v.toFixed(2));
              spxTastyTs = sR.value.asOf || sR.value.raw?.['updated-at'];
              spxTastyFresh = _isFresh(spxTastyTs);
            }
          } else { out.errors.tastySpx = sR.reason?.message || String(sR.reason); }
        } else {
          out.errors.tastyVix = '(forced)'; out.errors.tastySpx = '(forced)';
        }

        out.values = { vixSchwab, spxSchwab, vixTasty, spxTasty };
        out.timestamps = { vixSchwabTs, spxSchwabTs, vixTastyTs, spxTastyTs };
        out.freshness = { vixSchwabFresh, spxSchwabFresh, vixTastyFresh, spxTastyFresh };
        // Apply the freshness gate to determine which messages WOULD post in production.
        // If VIX isn't fresh, the morning cron skips that source's message.
        if (vixSchwabFresh === false) vixSchwab = null;
        if (vixTastyFresh === false) vixTasty = null;
        if (spxSchwabFresh === false) spxSchwab = null;
        if (spxTastyFresh === false) spxTasty = null;

        // 2. Read history for prevWR / vixPct20d / rsi14 (mirrors morning cron)
        // (etNow + todayISO already declared above for the freshness check)
        let prevWR = null, vixPct20d = null, rsi14 = null, vixYOpen = null, vixYClose = null, spxYClose = null;
        try {
          const histData = await getHistory(env);
          if (Array.isArray(histData) && histData.length) {
            const sorted = histData.filter(r => r.date < todayISO && r.m8bfWR != null)
              .sort((a, b) => b.date.localeCompare(a.date));
            if (sorted.length) prevWR = parseFloat(sorted[0].m8bfWR);

            const vix20 = histData.filter(r => r.date < todayISO && r.vixClose != null && r.vixClose > 0)
              .sort((a, b) => a.date.localeCompare(b.date)).slice(-20).map(r => parseFloat(r.vixClose));
            const refVix = vixTasty ?? vixSchwab;
            // CANONICAL — single source: signal-engine.js computeVixPct20d.
            vixPct20d = computeVixPct20d(refVix, vix20).pct;

            const closes30 = histData.filter(r => r.date < todayISO && r.spxClose != null && r.spxClose > 0)
              .sort((a, b) => a.date.localeCompare(b.date)).slice(-30).map(r => parseFloat(r.spxClose));
            if (closes30.length >= 15) rsi14 = computeRSI14(closes30);

            // Prior trading day's vix open/close + spx close — used by calculateSignal
            const prior = histData.filter(r => r.date < todayISO && r.vixClose != null)
              .sort((a, b) => b.date.localeCompare(a.date))[0];
            if (prior) {
              vixYOpen = prior.vixOpen != null ? parseFloat(prior.vixOpen) : null;
              vixYClose = parseFloat(prior.vixClose);
              spxYClose = prior.spxClose != null ? parseFloat(prior.spxClose) : null;
            }
          }
        } catch (e) { out.errors.history = e.message; }
        out.context = { prevWR, vixPct20d, rsi14, vixYOpen, vixYClose, spxYClose };

        // 3. Compute signal twice (once per source) — same calculateSignal call
        const cor1mOpenTrade = await getCor1mOpenToday(env, isoDateET(etNow));
        function makeSignal(vixVal, spxOpen) {
          if (vixVal == null) return null;
          const gap = (spxYClose && spxOpen) ? ((spxOpen - spxYClose) / spxYClose) * 100 : null;
          return calculateSignal({
            vixToday: vixVal, vixYOpen, vixYClose,
            spxGapPct: gap,
            etDate: etNow, prevWR, vixPct20d, rsi14,
            cor1m: cor1mOpenTrade,
          });
        }
        const sigSchwab = makeSignal(vixSchwab, spxSchwab);
        const sigTasty  = makeSignal(vixTasty,  spxTasty);
        // Tail Hedge today (fetched once, cached). Shared between both renders.
        const tailLine = await getTailHedgeStatusLine(env);
        // Advisory lines — keep the preview identical to the real morning message
        let tiltL = null, gexL = null, cycL = null, volL = null, wrL = null;
        try { tiltL = await computeTiltLine(env, isoDateET(etNow)); } catch (_) {}
        try { gexL = await computeGexLine(env); } catch (_) {}
        try { cycL = await computeCycleLine(env, etNow); } catch (_) {}
        try { volL = await computeVolFlowLine(env, etNow); } catch (_) {}
        try { wrL = await computeM8bfWrLine(env, etNow); } catch (_) {}
        for (const s of [sigSchwab, sigTasty]) if (s) { s._tiltLine = tiltL; s._gexLine = gexL; s._cycleLine = cycL; s._volFlowLine = volL; s._m8bfWrLine = wrL; }

        function renderMsg(sig, vixVal, source) {
          if (!sig) return null;
          const banner = source === 'schwab' ? '📡 **SCHWAB DATA**\n\n' : '📡 **TASTYTRADE DATA**\n\n';
          return (banner + buildDiscordMessage(sig, { yOpen: vixYOpen, yClose: vixYClose, todayOpen: vixVal }, tailLine)).slice(0, 2000);
        }
        out.schwab = sigSchwab ? {
          rec: sigSchwab.rec, theme: sigSchwab.theme, badge: sigSchwab.badge,
          message: renderMsg(sigSchwab, vixSchwab, 'schwab'),
        } : null;
        out.tasty = sigTasty ? {
          rec: sigTasty.rec, theme: sigTasty.theme, badge: sigTasty.badge,
          message: renderMsg(sigTasty, vixTasty, 'tastytrade'),
        } : null;

        // Summary signals
        out.summary = {
          schwabMessageBuilt: !!out.schwab,
          tastyMessageBuilt: !!out.tasty,
          recsAgree: sigSchwab && sigTasty ? sigSchwab.rec === sigTasty.rec : null,
          vixDelta: (vixSchwab != null && vixTasty != null) ? +(vixSchwab - vixTasty).toFixed(2) : null,
          spxDelta: (spxSchwab != null && spxTasty != null) ? +(spxSchwab - spxTasty).toFixed(2) : null,
        };

        return jsonResp(out, 200, cors);
      } catch (e) {
        return jsonResp({ ok: false, error: e.message, stack: e.stack?.slice(0, 400) }, 500, cors);
      }
    }

    // GET /tasty-chain-probe?root=SPXW[&exp=YYYY-MM-DD]
    // Diagnostic ONLY. Hits Tasty's option-chains endpoints and a sample
    // market-data quote so we can see the response shape before building a
    // wrapper. Read-only, additive — no impact on live signal/trading.
    if (url.pathname === '/tasty-chain-probe' && request.method === 'GET') {
      const cors = { 'Access-Control-Allow-Origin': '*' };
      try {
        const root = url.searchParams.get('root') || 'SPXW';
        const expFilter = url.searchParams.get('exp');  // YYYY-MM-DD optional
        const token = await getTastyAccessToken(env);
        const hdr = tastyHeaders({ 'Authorization': `Bearer ${token}` });
        const out = { root, expFilter, ts: new Date().toISOString() };

        // 1. NESTED chain (structured by expiration → strike → call/put)
        const r1 = await fetch(`${TASTY_BASE}/option-chains/${encodeURIComponent(root)}/nested`, { headers: hdr });
        const t1 = await r1.text();
        let j1 = null;
        try { j1 = JSON.parse(t1); } catch (_) {}
        out.nested = { status: r1.status, ok: r1.ok };
        if (j1) {
          const d = j1?.data || j1;
          const items = d?.items || [];
          out.nested.topKeys = Object.keys(d || {});
          out.nested.itemCount = items.length;
          // Show the structure of the first item + first expiration deep
          if (items[0]) {
            out.nested.firstItemKeys = Object.keys(items[0]);
            const exps = items[0]?.expirations || items[0]?.['expirations'] || [];
            out.nested.firstItemExpCount = exps.length;
            if (exps[0]) {
              out.nested.firstExpKeys = Object.keys(exps[0]);
              const strikes = exps[0]?.strikes || [];
              out.nested.firstExpStrikeCount = strikes.length;
              if (strikes[0]) {
                out.nested.firstStrikeKeys = Object.keys(strikes[0]);
                out.nested.firstStrikeSample = strikes[0];
              }
            }
          }
        } else {
          out.nested.bodyHead = t1.slice(0, 400);
        }

        // 2. COMPACT chain (flat list of option symbols)
        const r2 = await fetch(`${TASTY_BASE}/option-chains/${encodeURIComponent(root)}/compact`, { headers: hdr });
        const t2 = await r2.text();
        let j2 = null;
        try { j2 = JSON.parse(t2); } catch (_) {}
        out.compact = { status: r2.status, ok: r2.ok };
        if (j2) {
          const d = j2?.data || j2;
          const items = d?.items || [];
          out.compact.topKeys = Object.keys(d || {});
          out.compact.itemCount = items.length;
          if (items[0]) {
            out.compact.firstItemKeys = Object.keys(items[0]);
            const syms = items[0]?.['option-chains'] || items[0]?.symbols || items[0]?.['streamer-symbols'] || [];
            out.compact.symbolFieldFound = Object.keys(items[0]).filter(k => Array.isArray(items[0][k])).slice(0, 5);
            out.compact.firstFewSymbols = (Array.isArray(syms) ? syms : []).slice(0, 6);
            out.compact.firstItemSample = items[0];  // tiny; just the top wrapper
          }
        } else {
          out.compact.bodyHead = t2.slice(0, 400);
        }

        // 3. Pick a sample option symbol and probe market-data endpoints
        let sampleSym = null;
        try {
          const compactItems = j2?.data?.items || j2?.items || [];
          for (const it of compactItems) {
            for (const k of Object.keys(it)) {
              if (Array.isArray(it[k]) && it[k].length) {
                const candidate = it[k].find(s => typeof s === 'string') || it[k][0];
                if (typeof candidate === 'string') { sampleSym = candidate; break; }
              }
            }
            if (sampleSym) break;
          }
        } catch (_) {}
        out.sampleSym = sampleSym;

        if (sampleSym) {
          // Try /market-data/{Index|Equity Option|Future Option}/{symbol} variants
          for (const instr of ['Equity Option', 'EquityOption', 'Option']) {
            const path = `${TASTY_BASE}/market-data/${encodeURIComponent(instr)}/${encodeURIComponent(sampleSym)}`;
            const r = await fetch(path, { headers: hdr });
            const t = await r.text();
            let j = null; try { j = JSON.parse(t); } catch (_) {}
            out[`mdSingle_${instr.replace(/\s/g,'_')}`] = {
              path, status: r.status, ok: r.ok,
              topKeys: j ? Object.keys(j) : null,
              data: j?.data || null,
              bodyHead: !j ? t.slice(0, 300) : undefined,
            };
            if (r.ok) break;  // first one that works is enough
          }
          // Try batch quote endpoints with several URL/symbol-format variants.
          // We need ONE batch call that returns quotes for many option symbols.
          // Collect 4 sample syms so we can validate batching, not just single.
          const syms = [];
          try {
            const items = j2?.data?.items || [];
            for (const it of items) {
              for (const arr of [it['symbols'], it['streamer-symbols']]) {
                if (Array.isArray(arr)) syms.push(...arr.slice(0, 2));
              }
              if (syms.length >= 4) break;
            }
          } catch (_) {}
          const sym1 = syms[0] || sampleSym;
          const sym2 = syms[1] || sampleSym;
          const sym1NoDouble = sym1?.replace(/\s+/g, ' ');
          const sym1Strip = sym1?.replace(/\s+/g, '');
          // Streamer-symbol form (.SPXW260520C2800) from nested first strike
          const streamerSym = j1?.data?.items?.[0]?.expirations?.[0]?.strikes?.[0]?.['call-streamer-symbol'];
          const variants = [
            { tag: 'by-type_single_double-space',  url: `${TASTY_BASE}/market-data/by-type?option=${encodeURIComponent(sym1)}` },
            { tag: 'by-type_single_single-space',  url: `${TASTY_BASE}/market-data/by-type?option=${encodeURIComponent(sym1NoDouble)}` },
            { tag: 'by-type_single_no-space',      url: `${TASTY_BASE}/market-data/by-type?option=${encodeURIComponent(sym1Strip)}` },
            { tag: 'by-type_two_comma',            url: `${TASTY_BASE}/market-data/by-type?option=${encodeURIComponent(sym1 + ',' + sym2)}` },
            { tag: 'by-type_two_repeated',         url: `${TASTY_BASE}/market-data/by-type?option=${encodeURIComponent(sym1)}&option=${encodeURIComponent(sym2)}` },
            { tag: 'by-type_streamer',             url: streamerSym ? `${TASTY_BASE}/market-data/by-type?option=${encodeURIComponent(streamerSym)}` : null },
            { tag: 'md_root_symbols',              url: `${TASTY_BASE}/market-data?symbols=${encodeURIComponent(sym1)}` },
            { tag: 'md_root_symbols_comma2',       url: `${TASTY_BASE}/market-data?symbols=${encodeURIComponent(sym1 + ',' + sym2)}` },
            { tag: 'md_root_symbols_comma4',       url: `${TASTY_BASE}/market-data?symbols=${encodeURIComponent(syms.slice(0,4).join(','))}` },
            { tag: 'md_root_symbols_repeated',     url: `${TASTY_BASE}/market-data?symbols=${encodeURIComponent(sym1)}&symbols=${encodeURIComponent(sym2)}` },
            { tag: 'md_root_options',              url: `${TASTY_BASE}/market-data?options[]=${encodeURIComponent(sym1)}&options[]=${encodeURIComponent(sym2)}` },
            { tag: 'chain_quotes',                 url: `${TASTY_BASE}/option-chains/${encodeURIComponent(root)}/quotes` },
            { tag: 'chain_quotes_with_filter',     url: expFilter ? `${TASTY_BASE}/option-chains/${encodeURIComponent(root)}/quotes?expiration-date=${expFilter}` : null },
          ];
          out.batchProbe = {};
          for (const v of variants) {
            if (!v.url) { out.batchProbe[v.tag] = { skipped: true }; continue; }
            const r = await fetch(v.url, { headers: hdr });
            const t = await r.text();
            let j = null; try { j = JSON.parse(t); } catch (_) {}
            const items = j?.data?.items;
            out.batchProbe[v.tag] = {
              url: v.url,
              status: r.status, ok: r.ok,
              hasItems: Array.isArray(items),
              itemCount: Array.isArray(items) ? items.length : null,
              firstItemKeys: Array.isArray(items) && items[0] ? Object.keys(items[0]).slice(0, 12) : null,
              bodyHead: !j ? t.slice(0, 240) : (Array.isArray(items) && items.length === 0 ? t.slice(0, 240) : undefined),
            };
          }
          out.batchProbe._sampleSyms = { sym1, sym2, sym1NoDouble, sym1Strip, streamerSym };
        }

        return jsonResp({ ok: true, probe: out }, 200, cors);
      } catch (e) {
        return jsonResp({ ok: false, error: e.message, stack: e.stack?.slice(0, 400) }, 500, cors);
      }
    }

    if (url.pathname === '/health' && request.method === 'GET') {
      const publicCors = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type',
        'Access-Control-Max-Age': '86400',
      };
      // ?refresh=now triggers a force refresh against Schwab — useful for
      // recovering after a re-auth without waiting for natural token expiry.
      if (url.searchParams.get('refresh') === 'now') {
        try {
          const token = await getAccessToken(env, true);
          return jsonResp({ ok: true, refreshed: true, accessLen: token.length, accessTail: token.slice(-8) }, 200, publicCors);
        } catch (e) {
          return jsonResp({ ok: false, refreshed: false, error: e.message }, 500, publicCors);
        }
      }
      // ?run=now manually invokes the scheduled handler — recovery path when
      // Cloudflare crons stall (observed 2026-05-01: last cron 16h ago).
      // Public to avoid lock-out when the SYNC_SECRET is in the dashboard
      // but the dashboard's own crons are also down.
      if (url.searchParams.get('run') === 'now') {
        try {
          const result = await handleScheduled(env);
          result.date = result.date || new Date().toISOString();
          await env.SIGNAL_KV.put('last_run', JSON.stringify(result));
          return jsonResp({ ok: true, ran: true, result }, 200, publicCors);
        } catch (e) {
          return jsonResp({ ok: false, ran: false, error: e.message }, 500, publicCors);
        }
      }
      try {
        const now = Date.now();
        const etNow = toET(new Date(now));
        const etH = etNow.getHours();
        const etM = etNow.getMinutes();
        const dow = etNow.getDay();
        const todayISO = `${etNow.getFullYear()}-${String(etNow.getMonth()+1).padStart(2,'0')}-${String(etNow.getDate()).padStart(2,'0')}`;
        const isWeekday = dow >= 1 && dow <= 5;
        const postOpen = etH > 9 || (etH === 9 && etM >= 32);  // +2 min grace
        const postClose = etH > 16 || (etH === 16 && etM >= 16);
        const inMarketHours = isWeekday && ((etH === 9 && etM >= 30) || (etH >= 10 && etH < 16));

        // Fetch all KV keys in parallel
        const [morningRaw, eodRaw, schwabRaw, lastRunRaw, gexRaw, mirrorRaw] = await Promise.all([
          env.SIGNAL_KV.get(`morning_signal_${todayISO}`),
          env.SIGNAL_KV.get(`eod_done_${todayISO}`),
          env.SIGNAL_KV.get('schwab_refresh_state'),
          env.SIGNAL_KV.get('last_run'),
          env.SIGNAL_KV.get('gex_current'),
          env.SIGNAL_KV.get('history_mirror_state'),
        ]);

        const alerts = [];

        // ── Morning signal state ──
        let morningState = null;
        let morningClaimAgeS = null;
        let morningStuck = false;
        if (morningRaw === 'sent') {
          morningState = 'sent';
        } else if (morningRaw && morningRaw.startsWith('claim:')) {
          morningState = 'claim';
          const parts = morningRaw.split(':');
          const ts = parseInt(parts[2] || '0', 10);
          if (ts) {
            morningClaimAgeS = Math.round((now - ts) / 1000);
            if (morningClaimAgeS > 120) morningStuck = true;
          }
        }
        if (isWeekday && postOpen && !postClose && morningState !== 'sent') {
          alerts.push(morningStuck ? 'morning_signal_stuck' : 'morning_signal_not_sent');
        }

        // ── EOD state ──
        if (isWeekday && postClose && eodRaw !== 'done') {
          alerts.push('eod_not_done');
        }

        // ── Schwab refresh ──
        const schwab = schwabRaw ? JSON.parse(schwabRaw) : null;
        const schwabSuccessMs = schwab?.lastSuccess || 0;
        const schwabMinutesSinceSuccess = schwabSuccessMs ? Math.round((now - schwabSuccessMs) / 60000) : null;
        // Refresh-token countdown (7-day Schwab policy) — dashboard banner fuel
        let refreshDaysLeft = null;
        try {
          const tkRaw2 = await env.SIGNAL_KV.get('schwab_tokens');
          const exp2 = tkRaw2 ? JSON.parse(tkRaw2).refreshExpiry : null;
          if (exp2) refreshDaysLeft = Math.round((exp2 - now) / 8640000) / 10;   // 0.1d precision
        } catch (_) {}
        if (refreshDaysLeft != null && refreshDaysLeft <= 1.5) alerts.push('schwab_token_expiring');
        if (schwab && schwab.ok === false && (schwab.consecutiveErrors || 0) >= 3) {
          alerts.push('schwab_refresh_degraded');
        }

        // ── GitHub mirror (2026-06-09 — expired-PAT class of failure) ──
        // Mirror writes happen only a few times/day, so even 2 consecutive
        // failures means KV has been drifting ahead of GitHub for hours.
        const mirror = mirrorRaw ? JSON.parse(mirrorRaw) : null;
        const mirrorSuccessMs = mirror?.lastSuccess || 0;
        const mirrorMinutesSinceSuccess = mirrorSuccessMs ? Math.round((now - mirrorSuccessMs) / 60000) : null;
        if (mirror && mirror.ok === false && (mirror.consecutiveErrors || 0) >= 2) {
          alerts.push('history_mirror_degraded');
        }

        // ── Open risk exposure (2026-06-09 — daily total-risk cap) ──
        let openRisk = null;
        try {
          const cfgRaw2 = await env.SIGNAL_KV.get('risk_config');
          const cfg2 = { ...RISK_CAP_DEFAULTS, ...(cfgRaw2 ? JSON.parse(cfgRaw2) : {}) };
          const exp = await computeOpenRiskExposureUsd(env, todayISO);
          openRisk = { ...exp, capUsd: cfg2.maxOpenRiskUsd, enabled: cfg2.enabled, mode: cfg2.mode || 'warn' };
          if (cfg2.enabled && exp.totalUsd > cfg2.maxOpenRiskUsd) {
            alerts.push('open_risk_over_cap');
          }
        } catch (_) { /* informational only */ }

        // ── COR1M cloud capture (2026-06-09 — machine-independence) ──
        let cor1mCloud = null;
        try {
          const c = await env.SIGNAL_KV.get(`cor1m_open_${todayISO}`);
          if (c) cor1mCloud = JSON.parse(c);
          // Capture missing after 10:05 ET on a weekday = capture path broken.
          // (Start was 9:40, but $COR1M's first print can arrive 9:35–9:40 and
          // the freshness-gated capture window now runs to 10:00.)
          if (!cor1mCloud && isWeekday && (etH > 10 || (etH === 10 && etM >= 5)) && etH < 16) {
            alerts.push('cor1m_capture_missing');
          }
        } catch (_) {}

        // ── Cron liveness via last_run ──
        const lastRun = lastRunRaw ? JSON.parse(lastRunRaw) : null;
        const lastRunMs = lastRun?.date ? Date.parse(lastRun.date) : 0;
        const lastRunAgeS = lastRunMs ? Math.round((now - lastRunMs) / 1000) : null;
        // Cron fires every 2 min during market hours — 10 min without a run = stall
        if (inMarketHours && (lastRunAgeS === null || lastRunAgeS > 600)) {
          alerts.push('cron_stalled');
        }

        // ── GEX freshness ──
        // gexData.updatedAt is written as an ISO string by handleGEXUpdate,
        // so parse it to ms before doing clock math.
        let gexUpdatedAtIso = null;
        let gexAgeS = null;
        try {
          if (gexRaw) {
            const g = JSON.parse(gexRaw);
            gexUpdatedAtIso = g.updatedAt || null;
            const ms = gexUpdatedAtIso ? (
              typeof gexUpdatedAtIso === 'number' ? gexUpdatedAtIso : Date.parse(gexUpdatedAtIso)
            ) : 0;
            if (ms && !Number.isNaN(ms)) gexAgeS = Math.round((now - ms) / 1000);
          }
        } catch { /* ignore malformed JSON */ }
        if (inMarketHours && (gexAgeS === null || gexAgeS > 600)) {
          alerts.push('gex_stale');
        }

        const alarming = alerts.length > 0;
        return jsonResp({
          now,
          todayISO,
          etTime: `${etH}:${String(etM).padStart(2,'0')} ET`,
          isWeekday,
          inMarketHours,
          morning_signal: {
            state: morningState,
            raw: morningRaw,
            claim_age_s: morningClaimAgeS,
            stuck: morningStuck,
          },
          eod_done: eodRaw === 'done',
          schwab_refresh: schwab ? {
            ok: schwab.ok,
            consecutiveErrors: schwab.consecutiveErrors || 0,
            minutesSinceSuccess: schwabMinutesSinceSuccess,
            refreshDaysLeft,
            msg: schwab.msg,
          } : { state: 'never_recorded', refreshDaysLeft },
          history_mirror: mirror ? {
            ok: mirror.ok,
            consecutiveErrors: mirror.consecutiveErrors || 0,
            minutesSinceSuccess: mirrorMinutesSinceSuccess,
            msg: mirror.msg,
          } : { state: 'never_recorded' },
          open_risk: openRisk,
          cor1m_cloud: cor1mCloud || { state: 'not_captured_today' },
          last_run: lastRun ? {
            status: lastRun.status,
            date: lastRun.date,
            age_s: lastRunAgeS,
          } : null,
          gex: {
            available: !!gexRaw,
            updatedAt: gexUpdatedAtIso,
            age_s: gexAgeS,
          },
          heartbeat: await (async () => {
            const hb = await env.SIGNAL_KV.get('last_heartbeat');
            if (!hb) return { configured: false };
            const p = JSON.parse(hb);
            const ageS = Math.round((Date.now() - new Date(p.at).getTime()) / 1000);
            return { configured: true, lastAt: p.at, age_s: ageS, status: p.status, ranMs: p.ranMs };
          })(),
          alarming,
          alerts,
        }, 200, publicCors);
      } catch (e) {
        return jsonResp({ error: e.message, alarming: true, alerts: ['health_endpoint_threw'] }, 500, publicCors);
      }
    }

    // ── GET /gex ── Public endpoint, returns current GEX data from KV
    // Auto-refreshes if data is stale (>3 min) during market hours (cron is unreliable on free tier)
    // ── Debug: exercise claimSendSlot in the real runtime (authed; P26 rule 3) ──
    // ── GEX gate: PUBLIC today-verdict (dashboard M8BF card overlay). Read-only,
    // non-sensitive (one boolean + percentile), CORS like /magnetfly-today. ──
    if (url.pathname === '/gexgate-today' && request.method === 'GET') {
      const pub = { 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'GET, OPTIONS' };
      try {
        const qd = url.searchParams.get('date');
        const forDate = (qd && /^\d{4}-\d{2}-\d{2}$/.test(qd)) ? qd : isoDateET(toET(new Date()));
        let rank = null;
        const serRaw = await env.SIGNAL_KV.get('gex1030_series_v1');
        if (serRaw && forDate >= GEXGATE_LIVE_FROM) {
          const ser = JSON.parse(serRaw);
          const prior = Object.keys(ser).filter(d => d < forDate).sort();
          if (prior.length >= 21) {
            const refDate = prior[prior.length - 1];
            let pd = new Date(forDate + 'T12:00:00Z');
            do { pd = new Date(pd.getTime() - 86400000); }
            while (pd.getUTCDay() === 0 || pd.getUTCDay() === 6 || isHol(toET(new Date(pd.getTime() + 64800000))));
            if (refDate === pd.toISOString().slice(0, 10)) {
              const ref = ser[refDate];
              const base = prior.slice(0, -1).slice(-120).map(d => ser[d]);
              rank = base.filter(x => x <= ref).length / base.length;
            }
          }
        }
        return jsonResp({ forDate, skip: rank != null && rank < ANCHOR_THRESHOLD,
          rank: rank != null ? Math.round(rank * 100) : null, liveFrom: GEXGATE_LIVE_FROM }, 200, pub);
      } catch (e) { return jsonResp({ error: e.message }, 500, {}); }
    }
    // ── Heatmap data (2026-07-25): term structure (strike × expiry) and
    // intraday (strike × 30-min slot). Read-only derived market data, same
    // public posture as /gex and /gexgate-today.
    if (url.pathname === '/gex-heatmap' && request.method === 'GET') {
      const pub = { 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'GET, OPTIONS' };
      try {
        const view = url.searchParams.get('view') === 'intraday' ? 'intraday' : 'expiry';
        const qsym = url.searchParams.get('symbol');
        const sym = qsym === 'spy' ? 'spy' : (qsym === 'both' ? 'both' : 'spx');
        const qd = url.searchParams.get('date');
        const day = (qd && /^\d{4}-\d{2}-\d{2}$/.test(qd)) ? qd : isoDateET(toET(new Date()));

        // ── Combined view (2026-07-26) ──────────────────────────────────────
        // SPY dollar-gamma is already in the same units as SPX (its 1/10
        // notional falls out of gamma×S²), so the VALUES add directly. Only the
        // strike axis needs translating, and the ratio is NOT 10: it is
        // spotSPX/spotSPY (≈10.03 and drifting with accrued dividends), so a
        // naive ×10 misplaces every SPY wall by ~5 SPX strikes. Each SPY strike
        // is mapped to k×ratio and snapped to the nearest 5-point SPX strike;
        // collisions sum (two SPY strikes can land in one SPX bin).
        if (sym === 'both') {
          const snap5 = x => Math.round(x / 5) * 5;
          const [rawX, rawY] = await Promise.all([
            env.SIGNAL_KV.get(view === 'expiry' ? 'gex_current' : `gex_intraday_${day}`),
            env.SIGNAL_KV.get(view === 'expiry' ? 'gex_spy' : `gex_spy_intraday_${day}`),
          ]);
          const X = rawX ? JSON.parse(rawX) : null, Y = rawY ? JSON.parse(rawY) : null;
          const spotX = X?.spot ?? null, spotY = Y?.spot ?? null;
          const ratio = (spotX && spotY) ? spotX / spotY : null;
          if (view === 'expiry') {
            const gx = X?.expGrid, gy = Y?.expGrid;
            if (!gx && !gy) return jsonResp({ view, symbol: sym, spot: spotX, ratio, grid: null }, 200, pub);
            // column union by expiry DATE (both books share the same calendar)
            const exps = [];
            for (const src2 of [gx, gy]) for (const e of (src2?.exps || []))
              if (!exps.some(x => x.d === e.d)) exps.push({ d: e.d, dte: e.dte });
            exps.sort((a, b) => a.dte - b.dte);
            const cell = {};                        // snappedStrike -> [per-column $M]
            const add = (k, ci, v) => {
              if (v == null) return;
              if (!cell[k]) cell[k] = new Array(exps.length).fill(null);
              cell[k][ci] = (cell[k][ci] ?? 0) + v;
            };
            for (const r of (gx?.rows || [])) (gx.exps || []).forEach((e, i) => {
              const ci = exps.findIndex(x => x.d === e.d); if (ci >= 0) add(r.k, ci, r.v[i]);
            });
            if (gy && ratio) for (const r of (gy.rows || [])) (gy.exps || []).forEach((e, i) => {
              const ci = exps.findIndex(x => x.d === e.d); if (ci >= 0) add(snap5(r.k * ratio), ci, r.v[i]);
            });
            const rows = Object.keys(cell).map(Number).sort((a, b) => b - a)
              .map(k => ({ k, v: cell[k].map(v => v == null ? null : +v.toFixed(1)) }));
            return jsonResp({ view, symbol: sym, spot: spotX, spotSpy: spotY, ratio: ratio ? +ratio.toFixed(4) : null,
                              updatedAt: X?.updatedAt ?? null,
                              grid: rows.length ? { exps, rows } : null }, 200, pub);
          }
          // intraday: union of 30-min slots, SPY strikes translated + snapped
          const sx = X?.slots || {}, sy = Y?.slots || {};
          const times = [...new Set([...Object.keys(sx), ...Object.keys(sy)])].sort();
          const merged = {};
          for (const t of times) {
            const row = {};
            for (const [k, v] of Object.entries(sx[t] || {})) row[k] = (row[k] ?? 0) + v;
            if (ratio) for (const [k, v] of Object.entries(sy[t] || {})) {
              const kk = String(snap5(parseFloat(k) * ratio));
              row[kk] = (row[kk] ?? 0) + v;
            }
            for (const k of Object.keys(row)) row[k] = +row[k].toFixed(1);
            if (Object.keys(row).length) merged[t] = row;
          }
          return jsonResp({ view, symbol: sym, date: day, spot: spotX, spotSpy: spotY,
                            ratio: ratio ? +ratio.toFixed(4) : null,
                            slots: Object.keys(merged).length ? merged : null }, 200, pub);
        }

        if (view === 'expiry') {
          const raw = await env.SIGNAL_KV.get(sym === 'spy' ? 'gex_spy' : 'gex_current');
          const g = raw ? JSON.parse(raw) : null;
          return jsonResp({ view, symbol: sym, spot: g?.spot ?? null, updatedAt: g?.updatedAt ?? null,
                            grid: g?.expGrid ?? null }, 200, pub);
        }
        const raw = await env.SIGNAL_KV.get(`${sym === 'spy' ? 'gex_spy_intraday' : 'gex_intraday'}_${day}`);
        const rec = raw ? JSON.parse(raw) : null;
        return jsonResp({ view, symbol: sym, date: day, spot: rec?.spot ?? null, slots: rec?.slots ?? null }, 200, pub);
      } catch (e) { return jsonResp({ error: e.message }, 500, {}); }
    }
    // ── Resend today's morning card (owner-gated) ──
    // Clears the morning 'sent' marker so the normal morning path rebuilds and
    // re-posts the card from LIVE state (used 2026-07-27 after a mid-session
    // tail-arming fix made the posted card stale). The claim gate still guards
    // against duplicates of the resend itself.
    if (url.pathname === '/resend-morning' && request.method === 'GET') {
      const sec = url.searchParams.get('secret');
      if (!sec || (sec !== env.SYNC_SECRET && sec !== env.GEXM_TRIGGER_TOKEN)) {
        return jsonResp({ error: 'Unauthorized' }, 401, {});
      }
      try {
        const today = isoDateET(toET(new Date()));
        await env.SIGNAL_KV.delete(`morning_signal_${today}`);
        globalThis.__morningSentDay = null;
        const r = await handleScheduled(env);
        return jsonResp({ ok: true, cleared: `morning_signal_${today}`, result: r }, 200, {});
      } catch (e) { return jsonResp({ error: e.message }, 500, {}); }
    }
    // ── Schwab API usage telemetry (owner-gated) ──
    if (url.pathname === '/schwab-usage' && request.method === 'GET') {
      const sec = url.searchParams.get('secret');
      if (!sec || (sec !== env.SYNC_SECRET && sec !== env.GEXM_TRIGGER_TOKEN)) {
        return jsonResp({ error: 'Unauthorized' }, 401, {});
      }
      const raw = await env.SIGNAL_KV.get('schwab_usage');
      const log = raw ? JSON.parse(raw) : null;
      if (!log) return jsonResp({ note: 'no ticks recorded yet' }, 200, {});
      const ns = log.ticks.map(t => t.n);
      const avg = ns.length ? +(ns.reduce((a, b) => a + b, 0) / ns.length).toFixed(1) : 0;
      return jsonResp({ limitPerMin: 120, peak: log.peak, peakAt: log.peakAt,
                        avgPerTick: avg, maxRecent: ns.length ? Math.max(...ns) : 0,
                        headroomPct: log.peak ? Math.round((1 - log.peak / 120) * 100) : null,
                        total429: log.total429 || 0, ticks: log.ticks.slice(-30) }, 200, {});
    }
    // ── SPY GEX refresh (owner-gated debug/verification) ──
    if (url.pathname === '/gex-spy-refresh' && request.method === 'GET') {
      const sec = url.searchParams.get('secret');
      if (!sec || (sec !== env.SYNC_SECRET && sec !== env.GEXM_TRIGGER_TOKEN)) {
        return jsonResp({ error: 'Unauthorized' }, 401, {});
      }
      try {
        const token = await getAccessToken(env);
        return jsonResp(await handleSpyGexUpdate(env, token, { slots: false }), 200, {});
      } catch (e) { return jsonResp({ error: e.message }, 500, {}); }
    }
    // ── GEX gate: public series tail for gex.html panels (gate strip histogram,
    // persistence stats). Read-only market data — same public posture as
    // /gexgate-today. Page computes ranks/streaks client-side from the tail.
    if (url.pathname === '/gexgate-hist' && request.method === 'GET') {
      const pub = { 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'GET, OPTIONS' };
      try {
        const serRaw = await env.SIGNAL_KV.get('gex1030_series_v1');
        const ser = serRaw ? JSON.parse(serRaw) : {};
        const keys = Object.keys(ser).sort();
        const tail = keys.slice(-400).map(k => [k, ser[k]]);
        return jsonResp({ tail, liveFrom: GEXGATE_LIVE_FROM, updated: keys[keys.length - 1] ?? null }, 200, pub);
      } catch (e) { return jsonResp({ error: e.message }, 500, {}); }
    }
    // ── One-shot reconcile (2026-07-24, verified vs ThetaData ground truth):
    // Jul 20 PNBF scalpPL was a phantom (10:30 capture outage — mf_magnet null,
    // 0DTE magnet 7525 vs center 7495 = no signal, no trade) → remove it.
    // Jul 21 was a real verified trade (magnet 7500, ctr 7505, debit 15.37,
    // TP 14:20, +$300/lot) that never reached mf_closed_log → backfill the row
    // so the PNBF page's paper record matches history. Idempotent.
    if (url.pathname === '/pnbf-reconcile-jul20' && request.method === 'POST') {
      const sec = url.searchParams.get('secret');
      if (!sec || (sec !== env.SYNC_SECRET && sec !== env.GEXM_TRIGGER_TOKEN)) {
        return jsonResp({ error: 'Unauthorized' }, 401, {});
      }
      try {
        const out = {};
        const content = await getHistory(env);
        const row = (content || []).find(r => r.date === '2026-07-20');
        if (row && row.scalpPL != null) {
          await backupHistorySnapshot(env, content, '2026-07-20', { scalpPL: 'DELETE(phantom)' });
          delete row.scalpPL;
          await setHistory(env, content, { dateStr: '2026-07-20', fields: { scalpPL: 'deleted' }, skipBackup: true });
          try { await mirrorHistoryToGitHub(env, content, 'fix: remove phantom PNBF scalpPL 2026-07-20 (capture outage, no signal — verified vs ThetaData)'); } catch (_) {}
          out.jul20 = 'scalpPL removed';
        } else out.jul20 = 'already clean';
        const logRaw = await env.SIGNAL_KV.get('mf_closed_log');
        const log = logRaw ? JSON.parse(logRaw) : [];
        if (!log.some(x => x.date === '2026-07-21')) {
          log.push({ date: '2026-07-21', day: 'Tue', magnet: 7500, center: 7505, debit: 1537,
                     exit: 'TP', exitTime: '14:20', pnl: 300 });
          log.sort((a, b) => a.date.localeCompare(b.date));
          await env.SIGNAL_KV.put('mf_closed_log', JSON.stringify(log));
          out.jul21 = 'paper-log row backfilled';
        } else out.jul21 = 'already present';
        return jsonResp({ ok: true, ...out }, 200, {});
      } catch (e) { return jsonResp({ error: e.message }, 500, {}); }
    }
    // ── Debug: read a stored mf_magnet_<date> snapshot (both magnet bases) ──
    if (url.pathname === '/mf-magnet' && request.method === 'GET') {
      const sec = url.searchParams.get('secret');
      if (!sec || (sec !== env.SYNC_SECRET && sec !== env.GEXM_TRIGGER_TOKEN)) {
        return jsonResp({ error: 'Unauthorized' }, 401, {});
      }
      const qd = url.searchParams.get('date');
      if (!qd || !/^\d{4}-\d{2}-\d{2}$/.test(qd)) return jsonResp({ error: 'date=YYYY-MM-DD required' }, 400, {});
      const raw = await env.SIGNAL_KV.get(`mf_magnet_${qd}`);
      return jsonResp({ date: qd, snapshot: raw ? JSON.parse(raw) : null }, 200, {});
    }
    // ── GEX gate: status probe (seam check + live verdict preview) ──
    if (url.pathname === '/gexgate-status' && request.method === 'GET') {
      const sec = url.searchParams.get('secret');
      if (!sec || (sec !== env.SYNC_SECRET && sec !== env.GEXM_TRIGGER_TOKEN)) {
        return jsonResp({ error: 'Unauthorized' }, 401, {});
      }
      try {
        const serRaw = await env.SIGNAL_KV.get('gex1030_series_v1');
        const ser = serRaw ? JSON.parse(serRaw) : {};
        const keys = Object.keys(ser).sort();
        const v = await computeGexGateVerdict(env);
        // live mf_magnet snapshots for the seam check (last 12 calendar days)
        const liveRecent = {};
        for (let i = 0; i < 12; i++) {
          const d = new Date(Date.now() - i * 86400000);
          const iso = d.toISOString().slice(0, 10);
          const raw = await env.SIGNAL_KV.get(`mf_magnet_${iso}`);
          if (raw) { const j = JSON.parse(raw); liveRecent[iso] = j.totalGex0dte ?? null; }
        }
        return jsonResp({ seriesLen: keys.length, first: keys[0] ?? null, last: keys[keys.length - 1] ?? null,
          lastVals: Object.fromEntries(keys.slice(-5).map(k => [k, ser[k]])), verdict: v, liveRecent }, 200, {});
      } catch (e) { return jsonResp({ error: e.message }, 500, {}); }
    }
    // ── GEX gate: one-time series seed from the research bank ──
    if (url.pathname === '/gexgate-seed' && request.method === 'POST') {
      const sec = request.headers.get('X-Sync-Secret') || url.searchParams.get('secret');
      if (!sec || (sec !== env.SYNC_SECRET && sec !== env.GEXM_TRIGGER_TOKEN)) {
        return jsonResp({ error: 'Unauthorized' }, 401, {});
      }
      try {
        const body = await request.json();
        if (!body || typeof body !== 'object') return jsonResp({ error: 'bad body' }, 400, {});
        const serRaw = await env.SIGNAL_KV.get('gex1030_series_v1');
        const ser = serRaw ? JSON.parse(serRaw) : {};
        let added = 0;
        for (const [k, val] of Object.entries(body)) {
          if (/^\d{4}-\d{2}-\d{2}$/.test(k) && typeof val === 'number') {
            if (ser[k] == null) added++;
            ser[k] = val;
          }
        }
        const keys = Object.keys(ser).sort();
        const trimmed = {};
        for (const k of keys.slice(-1200)) trimmed[k] = ser[k];
        await env.SIGNAL_KV.put('gex1030_series_v1', JSON.stringify(trimmed));
        return jsonResp({ ok: true, added, total: Object.keys(trimmed).length }, 200, {});
      } catch (e) { return jsonResp({ error: e.message }, 500, {}); }
    }
    if (url.pathname === '/debug-claim' && request.method === 'GET') {
      const sec = url.searchParams.get('secret');
      if (!sec || (sec !== env.GEXM_TRIGGER_TOKEN && sec !== env.SYNC_SECRET)) {
        return new Response('forbidden', { status: 403 });
      }
      const key = 'debug_claim_' + (url.searchParams.get('key') || 'test');
      const won = await claimSendSlot(env, key);
      const val = await env.SIGNAL_KV.get(key);
      if (url.searchParams.get('clean') === '1') await env.SIGNAL_KV.delete(key);
      return new Response(JSON.stringify({ won, storedValue: val }),
        { status: 200, headers: { 'Content-Type': 'application/json' } });
    }

    // ── Phone MCP connector (claude.ai custom connector; secret-path auth) ──
    if (env.MCP_TOKEN && url.pathname === `/mcp/${env.MCP_TOKEN}`) {
      if (request.method === 'OPTIONS') {
        return new Response(null, { status: 204, headers: {
          'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'POST, OPTIONS',
          'Access-Control-Allow-Headers': 'Content-Type, Mcp-Session-Id, MCP-Protocol-Version' } });
      }
      if (request.method !== 'POST') return new Response('sigma3 mcp', { status: 200 });
      let msg;
      try { msg = await request.json(); } catch (_) {
        return new Response(JSON.stringify({ jsonrpc: '2.0', id: null, error: { code: -32700, message: 'parse error' } }),
          { status: 200, headers: { 'Content-Type': 'application/json' } });
      }
      const replies = Array.isArray(msg)
        ? (await Promise.all(msg.map(m => handleMcpMessage(env, m)))).filter(r => r !== null)
        : await handleMcpMessage(env, msg);
      if (replies === null || (Array.isArray(replies) && replies.length === 0)) {
        return new Response(null, { status: 202 });
      }
      return new Response(JSON.stringify(replies), { status: 200, headers: { 'Content-Type': 'application/json' } });
    }

    if (url.pathname === '/gex' && request.method === 'GET') {
      const publicCors = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type',
        'Access-Control-Max-Age': '86400',
      };
      try {
        // ── One-shot rescrape trigger (set via KV key 'rescrape_trigger') ──
        const rescrapeDate = await env.SIGNAL_KV.get('rescrape_trigger');
        if (rescrapeDate) {
          try {
            await env.SIGNAL_KV.delete('rescrape_trigger');
            const allSigs = await fetchAllDiscordSignalsForDate(env.DISCORD_USER_TOKEN, '1048242197029458040', rescrapeDate);
            const signals = allSigs.map(s => ({
              time: s.time, center: s.center, lower: s.lower, upper: s.upper,
              t1: s.t1, premium: s.premium, cp: s.cp ?? 0, banned: isBanned(s.center, s.lower, s.t1),
            }));
            await env.SIGNAL_KV.put('signals_today', JSON.stringify({ date: rescrapeDate, signals }));
            console.log(`[gex] Rescrape complete: ${signals.length} signals for ${rescrapeDate}`);
          } catch (e) { console.warn('[gex] rescrape failed:', e.message); }
        }

        // ── One-shot scrape-backfill trigger (KV 'scrape_backfill_trigger' = "from,to") ──
        // Recovers missing trading days in scraped_signals.csv (one GET + scrape
        // all missing days + one PUT). Delete first so a failure can't loop.
        const backfillRange = await env.SIGNAL_KV.get('scrape_backfill_trigger');
        if (backfillRange) {
          await env.SIGNAL_KV.delete('scrape_backfill_trigger');
          try {
            const [bf, bt] = backfillRange.split(',').map(s => s.trim());
            const res = await backfillScrapedSignals(env, bf, bt || bf);
            await env.SIGNAL_KV.put('scrape_backfill_result', JSON.stringify({ ...res, ts: Date.now() }), { expirationTtl: 86400 * 7 });
            console.log(`[gex] scrape-backfill: ${res.backfilled} rows across ${res.dates.length} day(s)`);
          } catch (e) {
            await env.SIGNAL_KV.put('scrape_backfill_result', JSON.stringify({ error: e.message, ts: Date.now() }), { expirationTtl: 86400 * 7 });
            console.warn('[gex] scrape-backfill failed:', e.message);
          }
        }

        let data = await env.SIGNAL_KV.get('gex_current');

        // Owner-gated force refresh (?force=1&secret=…) — recompute now even
        // outside market hours. Verification/debug affordance; Schwab returns
        // valid OI + chain structure when closed, so the snapshot is usable.
        const _fSec = url.searchParams.get('secret');
        if (url.searchParams.get('force') === '1' && _fSec &&
            (_fSec === env.SYNC_SECRET || _fSec === env.GEXM_TRIGGER_TOKEN)) {
          try {
            const token = await getAccessToken(env);
            await handleGEXUpdate(env, token);
            data = await env.SIGNAL_KV.get('gex_current');
          } catch (e) { console.warn('[gex] force refresh failed:', e.message || e); }
        }

        // Auto-refresh: if no data or stale during market hours, trigger inline update
        const etNow = toET();
        const etH = etNow.getHours(), etM = etNow.getMinutes(), dow = etNow.getDay();
        const marketOpen = dow >= 1 && dow <= 5 && (etH > 9 || (etH === 9 && etM >= 30)) && etH < 16;
        if (marketOpen) {
          let needsRefresh = !data;
          if (data && !needsRefresh) {
            const parsed = JSON.parse(data);
            const age = Date.now() / 1000 - (parsed.timestamp || 0);
            if (age > 180) needsRefresh = true; // stale if >3 min old
          }
          if (needsRefresh) {
            // Cooldown: only one auto-refresh per 2 min to stay within KV write limits
            const lastRefresh = await env.SIGNAL_KV.get('gex_last_refresh_ts');
            const sinceRefresh = lastRefresh ? Date.now() - parseInt(lastRefresh) : Infinity;
            if (sinceRefresh > 120_000) {
              try {
                await env.SIGNAL_KV.put('gex_last_refresh_ts', String(Date.now()));
                const token = await getAccessToken(env);
                await handleGEXUpdate(env, token);
                data = await env.SIGNAL_KV.get('gex_current');
              } catch (e) {
                console.warn('[gex] inline refresh failed:', e.message || e);
              }
            }
          }
        }

        // ── Morning signal fallback: fire from /gex if cron missed it ──
        // ≥9:40 ET only (2026-06-10): cron normally completes by ~9:34; at
        // 9:33 this fallback fired DURING the cron's in-flight send (the
        // 'sent' marker hadn't propagated) and produced a duplicate message.
        // A fallback that jumps in 3 min after open isn't a fallback.
        if (marketOpen && (etH > 9 || (etH === 9 && etM >= 40))) {
          const todayCheck = `${etNow.getFullYear()}-${String(etNow.getMonth()+1).padStart(2,'0')}-${String(etNow.getDate()).padStart(2,'0')}`;
          const mKey = `morning_signal_${todayCheck}`;
          const mFailKey = `morning_signal_fail_${todayCheck}`;
          const mDone = await env.SIGNAL_KV.get(mKey);
          if (!mDone) {
            // Failure cooldown: don't retry more than once per 60s to avoid hammering APIs
            const lastFail = await env.SIGNAL_KV.get(mFailKey);
            const sinceFail = lastFail ? Date.now() - parseInt(lastFail) : Infinity;
            if (sinceFail > 60_000) {
              try {
                console.log('[gex] Morning signal not sent yet — triggering from /gex');
                await handleScheduled(env);
              } catch (e) {
                console.warn('[gex] morning signal fallback failed:', e.message || e);
                await env.SIGNAL_KV.put(mFailKey, String(Date.now()), { expirationTtl: 3600 });
              }
            }
          }
        }

        // ── EOD fallback: fire from /gex any time after 4:16 PM ET on a weekday
        // if EOD hasn't already completed for today. Backs up the self-ping in
        // handleScheduled for cases where every afternoon cron missed (rare but
        // observed 2026-04-17 — 4+ hours of Cloudflare cron silence). Any browser
        // hit on /gex resurrects the EOD write.
        const afterEOD = (etH === 16 && etM >= 16) || (etH >= 17 && etH < 24);
        const isEODWindow = dow >= 1 && dow <= 5 && afterEOD;
        if (isEODWindow) {
          const todayCheck = `${etNow.getFullYear()}-${String(etNow.getMonth()+1).padStart(2,'0')}-${String(etNow.getDate()).padStart(2,'0')}`;
          const eodKey = `eod_done_${todayCheck}`;
          const eodDone = await env.SIGNAL_KV.get(eodKey);
          if (!eodDone) {
            try {
              console.log('[gex] EOD not run yet — triggering from /gex');
              const r = await handleEOD(env, etNow);
              // Only mark done after a real write. If Schwab expired + Stooq
              // down, fields are empty — leaving the flag unset lets the next
              // tick/hit retry.
              if (r.wroteFields) {
                await env.SIGNAL_KV.put(eodKey, 'done', { expirationTtl: 86400 });
              } else {
                console.warn('[gex] EOD ran but wrote no fields — leaving eod_done unset for retry');
              }
            } catch (e) {
              console.warn('[gex] EOD fallback failed:', e.message || e);
            }
          }
        }

        if (!data) return jsonResp({ error: 'No GEX data available yet' }, 404, publicCors);

        // Mode switch: ?mode=0dte returns 0DTE-only GEX, default is all-expiry
        const gexMode = url.searchParams.get('mode');
        let actualMode = 'all';
        if (gexMode === '0dte') {
          const dte0 = await env.SIGNAL_KV.get('gex_current_0dte');
          if (dte0) { data = dte0; actualMode = '0dte'; }
          // else: falls back to all-expiry data, actualMode stays 'all'
        }

        // Symbol switch (2026-07-26): ?symbol=spy serves SPY's own book,
        // ?symbol=both serves the merged SPX+SPY snapshot (see buildCombinedGex).
        // SPY is captured all-expiry only, so 0DTE mode falls back to all for it.
        const qSym = url.searchParams.get('symbol');
        let actualSym = 'spx';
        if (qSym === 'spy' || qSym === 'both') {
          const spyRaw = await env.SIGNAL_KV.get('gex_spy');
          if (spyRaw) {
            if (qSym === 'spy') { data = spyRaw; actualSym = 'spy'; }
            else {
              try {
                const merged = buildCombinedGex(JSON.parse(data), JSON.parse(spyRaw));
                if (merged) { data = JSON.stringify(merged); actualSym = 'both'; }
              } catch (e) { console.warn('[gex] combine failed:', e.message); }
            }
          }
          // no SPY snapshot yet → silently stays SPX (actualSym reports the truth)
        }

        // Inject full daily event + commentary logs so all devices see complete history
        try {
          const etNow2 = toET();
          const todayISO2 = `${etNow2.getFullYear()}-${String(etNow2.getMonth()+1).padStart(2,'0')}-${String(etNow2.getDate()).padStart(2,'0')}`;
          const parsed = JSON.parse(data);
          parsed.gexMode = actualMode; // tells frontend which mode is actually served
          parsed.gexSymbol = actualSym; // ...and which book (spx | spy | both)
          const logRaw = await env.SIGNAL_KV.get(`gex_events_${todayISO2}`);
          if (logRaw) parsed.eventLog = JSON.parse(logRaw);
          // Intraday call-vs-put flow series for the "Call vs Put Flow — Today" chart
          if (actualSym === 'spy') {
            const fSpy = await env.SIGNAL_KV.get(`gex_spy_flow_${todayISO2}`);
            parsed.flowSeries = fSpy ? JSON.parse(fSpy) : [];
          } else if (actualSym === 'both') {
            const [fx, fy] = await Promise.all([
              env.SIGNAL_KV.get(`gex_flow_${todayISO2}`),
              env.SIGNAL_KV.get(`gex_spy_flow_${todayISO2}`),
            ]);
            parsed.flowSeries = mergeFlowSeries(fx ? JSON.parse(fx) : [], fy ? JSON.parse(fy) : [],
                                                parsed.ratio || 10);
          } else {
            const flowRaw = await env.SIGNAL_KV.get(`gex_flow_${todayISO2}`);
            parsed.flowSeries = flowRaw ? JSON.parse(flowRaw) : [];
          }
          data = JSON.stringify(parsed);
        } catch (e) { /* serve without full logs if parse fails */ }

        return new Response(data, {
          status: 200,
          headers: { 'Content-Type': 'application/json', ...publicCors },
        });
      } catch (e) {
        return jsonResp({ error: e.message }, 500, publicCors);
      }
    }

    // Preflight
    if (request.method === 'OPTIONS') {
      // Allow CORS preflight for /gex from any origin
      if (url.pathname === '/gex') {
        return new Response(null, {
          status: 204,
          headers: {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET, OPTIONS',
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Max-Age': '86400',
          },
        });
      }
      return new Response(null, { status: 204, headers: corsHeaders });
    }

    if (!corsOk) {
      return new Response('Forbidden', { status: 403 });
    }

    try {
      // ── POST /sync ── Browser pushes tokens/creds/discord to KV
      if (url.pathname === '/sync' && request.method === 'POST') {
        const secret = request.headers.get('X-Sync-Secret');
        if (!secret || secret !== env.SYNC_SECRET) {
          return jsonResp({ error: 'Unauthorized' }, 401, corsHeaders);
        }

        const json = await request.json();

        // Validate optional history record fields if present
        if ('date' in json && (typeof json.date !== 'string' || !/^\d{4}-\d{2}-\d{2}$/.test(json.date))) {
          return jsonResp({ error: 'Invalid payload: date' }, 400, corsHeaders);
        }
        const numOrNull = ['m8bfPL', 'stradPL', 'gxbfPL', 'bobfPL', 'm8bfWR', 'vixOpen', 'vixClose', 'spxOpen', 'spxClose'];
        for (const field of numOrNull) {
          if (field in json && json[field] !== null && typeof json[field] !== 'number') {
            return jsonResp({ error: `Invalid payload: ${field}` }, 400, corsHeaders);
          }
        }

        const { schwab_tokens, schwab_creds, discord_config } = json;

        if (schwab_tokens) await env.SIGNAL_KV.put('schwab_tokens', JSON.stringify(schwab_tokens));
        if (schwab_creds)  await env.SIGNAL_KV.put('schwab_creds', JSON.stringify(schwab_creds));
        if (discord_config) await env.SIGNAL_KV.put('discord_config', JSON.stringify(discord_config));

        return jsonResp({ ok: true, synced: Object.keys(json) }, 200, corsHeaders);
      }

      // ── GET /access-token ──
      // Central token source for browser + Python scraper. The Worker is the
      // only party that rotates Schwab's refresh_token; other clients call
      // here and get a valid access_token without ever touching refresh_token
      // themselves. This eliminates the 3-way rotation race that was kicking
      // every client into 401 hell.
      if (url.pathname === '/access-token' && request.method === 'GET') {
        const accTokenSecret = request.headers.get('X-Sync-Secret') || url.searchParams.get('secret');
        const linkSecret = request.headers.get('X-Link-Secret');
        const linkOk = env.LINK_SECRET && linkSecret === env.LINK_SECRET;   // skipper (2026-06-12)
        if (!linkOk && (!accTokenSecret || accTokenSecret !== env.SYNC_SECRET)) {
          return jsonResp({ error: 'Unauthorized' }, 401, corsHeaders);
        }
        try {
          // ?force=true triggers an immediate refresh against Schwab, useful
          // for diagnosing token issues without waiting for natural expiry.
          const force = url.searchParams.get('force') === 'true';
          const token = await getAccessToken(env, force);
          const tokensRaw = await env.SIGNAL_KV.get('schwab_tokens');
          const expiry = tokensRaw ? JSON.parse(tokensRaw).expiry : null;
          return jsonResp({ access_token: token, expiry, forced: force }, 200, corsHeaders);
        } catch (e) {
          return jsonResp({ error: e.message }, 500, corsHeaders);
        }
      }

      // ── POST /token ──
      if (url.pathname === '/token' && request.method === 'POST') {
        const tokenSecret = request.headers.get('X-Sync-Secret');
        if (!tokenSecret || tokenSecret !== env.SYNC_SECRET) {
          return jsonResp({ error: 'Unauthorized' }, 401, corsHeaders);
        }

        const json = await request.json();
        const { app_key, grant_type, code, redirect_uri, refresh_token } = json;
        if (!app_key || !grant_type) {
          return jsonResp({ error: 'Missing app_key or grant_type' }, 400, corsHeaders);
        }

        const body = new URLSearchParams({ grant_type });
        if (grant_type === 'authorization_code') {
          body.set('code', code);
          body.set('redirect_uri', redirect_uri);
        } else if (grant_type === 'refresh_token') {
          body.set('refresh_token', refresh_token);
        }

        const resp = await fetch('https://api.schwabapi.com/v1/oauth/token', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Authorization': 'Basic ' + btoa(`${app_key}:${env.SCHWAB_APP_SECRET}`),
          },
          body,
        });

        const data = await resp.json();
        return jsonResp(data, resp.status, corsHeaders);
      }

      // ── GET /market/* ──
      // Pass-through to Schwab market data API. If caller omits Authorization,
      // worker auto-fetches a fresh token from KV (refreshes if needed).
      if (url.pathname.startsWith('/market/') && request.method === 'GET') {
        const subpath = url.pathname.slice('/market'.length);
        const ALLOWED = ['/pricehistory', '/quotes', '/chains', '/markets', '/instruments'];
        if (!ALLOWED.some(p => subpath.startsWith(p))) {
          return jsonResp({ error: 'Path not allowed' }, 403, corsHeaders);
        }
        const upstream = `https://api.schwabapi.com/marketdata/v1${subpath}${url.search}`;

        // Use provided Authorization header, else auto-fetch from KV + refresh if expired
        let authHeader = request.headers.get('Authorization');
        if (!authHeader) {
          const token = await getAccessToken(env);
          authHeader = `Bearer ${token}`;
        }

        const resp = await fetch(upstream, { headers: { 'Authorization': authHeader } });
        const data = await resp.json();
        return jsonResp(data, resp.status, corsHeaders);
      }

      // ── GET /history ── Historical signals table
      if (url.pathname === '/history' && request.method === 'GET') {
        const months = Math.min(Math.max(parseInt(url.searchParams.get('months')) || 6, 1), 12);
        const token = await getAccessToken(env);

        const vixUrl = `https://api.schwabapi.com/marketdata/v1/pricehistory?symbol=%24VIX&periodType=month&period=${months}&frequencyType=daily&frequency=1`;
        const spxUrl = `https://api.schwabapi.com/marketdata/v1/pricehistory?symbol=%24SPX&periodType=month&period=${months}&frequencyType=daily&frequency=1`;

        const [vixData, spxData] = await Promise.all([
          fetchSchwabJSON(vixUrl, token),
          fetchSchwabJSON(spxUrl, token),
        ]);

        if (!vixData.candles?.length) return jsonResp({ error: 'No VIX data' }, 502, corsHeaders);
        if (!spxData.candles?.length) return jsonResp({ error: 'No SPX data' }, 502, corsHeaders);

        // Sort ascending
        vixData.candles.sort((a, b) => a.datetime - b.datetime);
        spxData.candles.sort((a, b) => a.datetime - b.datetime);

        // Build SPX lookup by date string
        const spxByDate = new Map();
        for (const c of spxData.candles) {
          const d = toET(new Date(c.datetime));
          const key = `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}-${String(d.getDate()).padStart(2,'0')}`;
          spxByDate.set(key, c);
        }

        const rows = [];
        const vix = vixData.candles;
        for (let i = 1; i < vix.length; i++) {
          const d = toET(new Date(vix[i].datetime));
          const etDate = new Date(d.getFullYear(), d.getMonth(), d.getDate(), 12);
          if (!isTrade(etDate)) continue;

          const dateKey = `${etDate.getFullYear()}-${String(etDate.getMonth()+1).padStart(2,'0')}-${String(etDate.getDate()).padStart(2,'0')}`;
          const prevD = toET(new Date(vix[i-1].datetime));
          const prevKey = `${prevD.getFullYear()}-${String(prevD.getMonth()+1).padStart(2,'0')}-${String(prevD.getDate()).padStart(2,'0')}`;

          const vixTodayOpen = parseFloat(vix[i].open.toFixed(2));
          const vixPrevClose = parseFloat(vix[i-1].close.toFixed(2));
          const vixPrevOpen = parseFloat(vix[i-1].open.toFixed(2));

          let spxGapPct = null;
          const spxToday = spxByDate.get(dateKey);
          const spxPrev = spxByDate.get(prevKey);
          if (spxToday && spxPrev) {
            spxGapPct = ((spxToday.open - spxPrev.close) / spxPrev.close) * 100;
          }

          const signal = calculateSignal({
            vixToday: vixTodayOpen,
            vixYOpen: vixPrevOpen,
            vixYClose: vixPrevClose,
            spxGapPct,
            etDate,
          });
          { const _g = await gexGateEval(env, dateKey); applyGexGateToSignal(signal, _g.skip, _g.rank); }

          rows.push({
            date: dateKey,
            dateLong: dateLong(etDate),
            dayLabel: signal.dayLabel,
            vixOpen: vixTodayOpen,
            vixPrevClose,
            vixPrevOpen,
            overnightDrop: parseFloat((vixPrevClose - vixTodayOpen).toFixed(2)),
            o2o: parseFloat((vixPrevOpen - vixTodayOpen).toFixed(2)),
            spxGapPct: spxGapPct !== null ? parseFloat(spxGapPct.toFixed(2)) : null,
            signal,
          });
        }

        rows.reverse(); // most recent first
        return jsonResp(rows, 200, corsHeaders);
      }

      // ── GET /signals ── Today's Discord signals from KV
      if (url.pathname === '/signals' && request.method === 'GET') {
        const data = await env.SIGNAL_KV.get('signals_today');
        return jsonResp(data ? JSON.parse(data) : { date: '', signals: [] }, 200, corsHeaders);
      }

      // ── GET /spx-history ── Today's SPX price ticks (replaces dead spx_history.json)
      if (url.pathname === '/spx-history' && request.method === 'GET') {
        const etNowH = toET(new Date());
        const todayH = `${etNowH.getFullYear()}-${String(etNowH.getMonth()+1).padStart(2,'0')}-${String(etNowH.getDate()).padStart(2,'0')}`;
        const spxHistRaw = await env.SIGNAL_KV.get(`spx_history_${todayH}`);
        return jsonResp({ date: todayH, data: spxHistRaw ? JSON.parse(spxHistRaw) : [] }, 200, corsHeaders);
      }

      // ── GET /trade ── Today's M8BF trade status (replaces dead today_trade.json)
      if (url.pathname === '/trade' && request.method === 'GET') {
        const etNowT = toET(new Date());
        const todayT = isoDateET(etNowT);  // byte-identical to the old manual y-m-d

        // ── lastClosed (mirrors /bobf-today, /gxbf-today, etc.). bobf-bot's
        // stats card relies on this for "TRADES" / "CLOSED P/L" counting on
        // M8BF — without it, every M8BF paper trade stays in 'logged' status
        // forever (the bot's upLookup only ever sees today's open). Most
        // recent date < today with m8bfPL non-null. Same-day open + close
        // because M8BF is 0DTE. See lessons.md P6. ──
        let lastClosedM8bf = null, todaySettled = null;
        try {
          const _hist = await getHistory(env);
          if (Array.isArray(_hist)) {
            const _prior = _hist
              .filter(r => r.date && r.date < todayT && r.m8bfPL != null)
              .sort((a, b) => b.date.localeCompare(a.date));
            if (_prior.length) {
              lastClosedM8bf = {
                date: _prior[0].date,
                openDate: _prior[0].date,
                closeDate: _prior[0].date,
                pnl: parseFloat(_prior[0].m8bfPL),
                status: 'settled',
              };
            }
            // Today's settled row (2026-06-15): once EOD writes m8bfPL +
            // spxClose, the live trade is DONE — serve the settled value so
            // the page stops marking 0DTE to a possibly-stale intraday spot
            // (live page showed +$1,774 @ a frozen 7549 vs +$1,181 @ the real
            // 7555 close).
            const _td = _hist.find(r => r.date === todayT);
            if (_td && _td.m8bfPL != null && _td.spxClose != null) {
              todaySettled = { pnl: parseFloat(_td.m8bfPL), spxClose: parseFloat(_td.spxClose) };
            }
          }
        } catch { /* if history fetch fails, lastClosed stays null */ }

        // ── M8BF banned-day gate (early return — no Discord poll on banned
        //    days, exactly as before). Logic moved verbatim into
        //    m8bfBannedReason() so refreshM8bfLiveQuotes shares it. ──
        const bannedReason = await m8bfBannedReason(env, etNowT);
        if (bannedReason) {
          return jsonResp({ date: todayT, triggered: false, status: 'banned', reason: `No M8BF (${bannedReason})`, lastClosed: lastClosedM8bf }, 200, corsHeaders);
        }

        // ── Cron-stall self-heal: if the last cron tick is stale AND we're
        // in market hours, force a Discord poll on demand so today's
        // signals_today gets refreshed. Without this, when cron stalls during
        // the M8BF window the signals never get captured and live page shows
        // 'no signal' forever even if Discord did post one.
        const isMktHrT = (etNowT.getHours() > 9 || (etNowT.getHours() === 9 && etNowT.getMinutes() >= 30)) && etNowT.getHours() < 16;
        if (isMktHrT) {
          const lastRunRaw = await env.SIGNAL_KV.get('last_run');
          const lastRun = lastRunRaw ? JSON.parse(lastRunRaw) : null;
          const lastRunMs = lastRun?.date ? new Date(lastRun.date).getTime() : 0;
          const ageMs = Date.now() - lastRunMs;
          if (ageMs > 5 * 60 * 1000) {  // >5 min stale → poll now
            try {
              if (env.DISCORD_USER_TOKEN) {
                const pollResult = await pollDiscordSignals(env);
                console.log('[/trade] cron-stall self-heal poll:', JSON.stringify(pollResult));
              }
            } catch (e) { console.warn('[/trade] cron-stall poll failed:', e.message); }
          }
        }

        // Shared selection (identical logic to the previous inline block).
        const sel = await selectM8bfQualifying(env, etNowT);
        if (sel.status === 'waiting' && sel.reason) {
          return jsonResp({ date: todayT, triggered: false, status: 'waiting', reason: sel.reason, lastClosed: lastClosedM8bf }, 200, corsHeaders);
        }
        if (sel.status === 'no_signal') {
          return jsonResp({ date: todayT, triggered: false, status: 'no_signal', reason: sel.reason, lastClosed: lastClosedM8bf }, 200, corsHeaders);
        }
        if (sel.status === 'waiting') {
          return jsonResp({ date: todayT, triggered: false, status: 'waiting', window: sel.window, lastClosed: lastClosedM8bf }, 200, corsHeaders);
        }

        // status === 'open'
        const qualifying = sel.qualifying;
        const resp = {
          date: todayT,
          triggered: true,
          status: 'open',
          signal_time: qualifying.time,
          center: qualifying.center,
          bf_lower: qualifying.lower,
          bf_upper: qualifying.upper,
          t1: qualifying.t1,
          premium: qualifying.premium,
          cp: qualifying.cp,
          lastClosed: lastClosedM8bf,
        };
        // Merge the REAL mark-to-market spread mid written every tick by
        // refreshM8bfLiveQuotes. live.html prefers this over the
        // at-expiration intrinsic (which overstates intraday profit). Absent
        // (pre-quote / chain miss / cron stall) → client falls back to the
        // intrinsic formula, i.e. no worse than the old behavior.
        try {
          const liveRaw = await env.SIGNAL_KV.get(`m8bf_live_${todayT}`);
          if (liveRaw) {
            const lv = JSON.parse(liveRaw);
            if (lv && lv.currentValue != null) {
              resp.currentValue     = lv.currentValue;
              resp.currentPnl       = lv.currentPnl;
              resp.currentSpot      = lv.currentSpot;
              resp.currentLowerMid  = lv.currentLowerMid;
              resp.currentCenterMid = lv.currentCenterMid;
              resp.currentUpperMid  = lv.currentUpperMid;
              resp.lastQuoteAt      = lv.lastQuoteAt;
            }
          }
        } catch { /* fall back to intrinsic on the client */ }
        // Settled override: EOD has the day's real m8bfPL @ the actual close.
        // Set currentValue so the client's (currentValue − premium) yields the
        // settled P/L exactly, and flag it so the page labels it "Final".
        if (todaySettled) {
          resp.status = 'settled';
          resp.settled = true;
          resp.spxClose = todaySettled.spxClose;
          resp.currentSpot = todaySettled.spxClose;
          resp.currentPnl = todaySettled.pnl;
          resp.currentValue = parseFloat((resp.premium + todaySettled.pnl / 100).toFixed(2));
        }
        return jsonResp(resp, 200, corsHeaders);
      }

      return jsonResp({ error: 'Not found' }, 404, corsHeaders);
    } catch (e) {
      return jsonResp({ error: e.message }, 500, corsHeaders);
    }
  },

  async scheduled(event, env, ctx) {
    console.log('[cron] Triggered at', new Date().toISOString());

    // Sunday digest cron runs ONLY the digest — the main tick's guards
    // assume weekdays and must not run on Sundays.
    // Cloudflare returns event.cron VERBATIM as configured in wrangler.toml — the
    // cron is 'SUN', not '0', so the old '0 22 * * 0' guard never matched and the
    // weekly digest + signed_flow backup never ran (audit P2 2026-07-06). Accept both.
    if (event.cron === '0 22 * * SUN' || event.cron === '0 22 * * 0') {
      try { await weeklyDigest(env); } catch (e) { console.warn('[digest]', e.message); }
      return;
    }

    // ── Slow-degradation watchdog: alert Discord if Schwab refresh has been
    //    failing for too long. Without this, a broken refresh chain rots
    //    silently for hours (observed 2026-04-30: 376 errors / 17 hrs before
    //    user noticed missing morning signal).
    //    Rate-limited to one alert per hour to avoid spam during outages.
    try {
      const stRaw = await env.SIGNAL_KV.get('schwab_refresh_state');
      if (stRaw) {
        const st = JSON.parse(stRaw);
        const errs = st.consecutiveErrors || 0;
        const lastAlert = parseInt(await env.SIGNAL_KV.get('schwab_alert_last_ms') || '0');
        const ALERT_COOLDOWN_MS = 60 * 60 * 1000;  // 1 hour
        const ERR_THRESHOLD = 10;
        if (!st.ok && errs >= ERR_THRESHOLD && (Date.now() - lastAlert) > ALERT_COOLDOWN_MS) {
          const dcRaw = await env.SIGNAL_KV.get('discord_config');
          if (dcRaw) {
            const dc = JSON.parse(dcRaw);
            if (dc.channelId) {
              const minsSinceOK = st.lastSuccess ? Math.round((Date.now() - st.lastSuccess) / 60000) : null;
              const alertResult = await sendDiscordDM(env, dc.channelId,
                `🚨 **Schwab refresh degraded** — ${errs} consecutive errors${minsSinceOK ? ` (${minsSinceOK} min since last success)` : ''}.\nMessage: \`${(st.msg || '').slice(0, 150)}\`\n→ Re-authenticate Schwab in dashboard, then hit \`/health?refresh=now\` to recover.`,
                dc.proxyUrl);
              if (alertResult.ok) {
                await env.SIGNAL_KV.put('schwab_alert_last_ms', String(Date.now()), { expirationTtl: 86400 });
                console.log('[cron] Posted Schwab degraded alert via', alertResult.source);
                await logEvent(env, 'error', 'schwab-degraded',
                  `${errs} consecutive Schwab refresh errors — Discord alert sent`,
                  { errs, minsSinceOK, msg: (st.msg || '').slice(0, 150) });
              } else {
                console.warn('[cron] Schwab degraded alert post failed:', alertResult.error);
              }
            }
          }
        }
      }
    } catch (watchdogErr) {
      console.error('[cron] Watchdog failed:', watchdogErr.message);
    }

    // ── GitHub-mirror watchdog (2026-06-09) ──
    // Mirror writes fire only a few times a day (morning open, EOD settle,
    // strategy settles), so 2 consecutive failures ≈ half a day of KV→GitHub
    // drift. Alert once per 6h. The expired-PAT incident sat silent for 2
    // days because nothing surfaced these failures.
    try {
      const mirrorRaw = await env.SIGNAL_KV.get('history_mirror_state');
      if (mirrorRaw) {
        const mst = JSON.parse(mirrorRaw);
        const merrs = mst.consecutiveErrors || 0;
        const lastMirrorAlert = parseInt(await env.SIGNAL_KV.get('mirror_alert_last_ms') || '0');
        const MIRROR_COOLDOWN_MS = 6 * 60 * 60 * 1000;  // 6 hours
        if (!mst.ok && merrs >= 2 && (Date.now() - lastMirrorAlert) > MIRROR_COOLDOWN_MS) {
          const dcRaw = await env.SIGNAL_KV.get('discord_config');
          if (dcRaw) {
            const dc = JSON.parse(dcRaw);
            if (dc.channelId) {
              const minsSinceOK = mst.lastSuccess ? Math.round((Date.now() - mst.lastSuccess) / 60000) : null;
              const alertResult = await sendDiscordDM(env, dc.channelId,
                `🚨 **GitHub history mirror failing** — ${merrs} consecutive errors${minsSinceOK ? ` (${Math.round(minsSinceOK/60)}h since last success)` : ''}.\nError: \`${(mst.msg || '').slice(0, 150)}\`\n→ KV is fine but the dashboard's history_data.json is going stale. Likely an expired GITHUB_TOKEN — rotate it:\n\`echo NEW_PAT | npx wrangler secret put GITHUB_TOKEN --name schwab-proxy\``,
                dc.proxyUrl);
              if (alertResult.ok) {
                await env.SIGNAL_KV.put('mirror_alert_last_ms', String(Date.now()), { expirationTtl: 86400 });
                await logEvent(env, 'error', 'mirror-degraded',
                  `${merrs} consecutive GitHub mirror failures — Discord alert sent`,
                  { merrs, minsSinceOK, msg: (mst.msg || '').slice(0, 150) });
              }
            }
          }
        }
      }
    } catch (mirrorWatchdogErr) {
      console.error('[cron] Mirror watchdog failed:', mirrorWatchdogErr.message);
    }

    // ── Pages-deploy watchdog (2026-07-03) ──
    // GitHub Pages deploys race when commits land close together; a losing
    // deploy normally self-heals on the next push — but when the LAST commit
    // in a burst loses, the site silently stays one commit stale (observed
    // 2026-07-03: earnings-play.html 404 for an hour). Hourly: if the most
    // recent pages run FAILED and nothing newer is queued, bump a nudge file
    // via the contents API → fresh commit → fresh deploy.
    try {
      const lastNudge = parseInt(await env.SIGNAL_KV.get('pages_watchdog_ms') || '0');
      if (Date.now() - lastNudge > 55 * 60 * 1000 && env.GITHUB_TOKEN) {
        await env.SIGNAL_KV.put('pages_watchdog_ms', String(Date.now()), { expirationTtl: 7 * 86400 });
        const runsResp = await fetch(
          'https://api.github.com/repos/rava8989/brave/actions/runs?per_page=1',
          { headers: { 'Authorization': `Bearer ${env.GITHUB_TOKEN}`,
                       'Accept': 'application/vnd.github+json',
                       'User-Agent': 'schwab-proxy-worker/1.0' } });
        if (runsResp.ok) {
          const run = (await runsResp.json()).workflow_runs?.[0];
          const ageMin = run ? (Date.now() - Date.parse(run.created_at)) / 60000 : 0;
          if (run && run.conclusion === 'failure' && ageMin > 10) {
            await githubUpsertResearchFile(env, 'data/.deploy_nudge',
              () => ({ t: Date.now(), reason: `pages run ${run.id} failed` }),
              'chore: pages deploy retry nudge (watchdog)');
            await logEvent(env, 'warn', 'pages-watchdog',
              `latest pages deploy failed (run ${run.id}, ${Math.round(ageMin)} min ago) — nudged redeploy`, {});
            console.log('[pages-watchdog] nudged redeploy after failed run', run.id);
          }
        }
      }
    } catch (pwErr) { console.warn('[pages-watchdog]', pwErr.message); }

    // ── Collector watchdog (2026-07-03, owner request) ──
    // Data collectors fail SILENTLY (no page breaks, no trade misfires) —
    // weeks of dataset can rot before anyone notices. Morning-after check
    // (9:40-9:59 ET, DST-proof, race-free): if the previous trading day's
    // collection done-marker is missing, DM Discord once. Covers the GEXM
    // chain recorder now; the earnings watchlist collector adds its own
    // entry to COLLECTOR_CHECKS when it ships.
    try {
      const etW = toET(new Date());
      if (etW.getHours() === 9 && etW.getMinutes() >= 40) {
        const prevISO = isoDateET(prevTrade(etW));
        const COLLECTOR_CHECKS = [
          // GEXM chain recorder retired 2026-07-04 (strategy shelved) — do not re-add
          ['Earnings scanner (morning board)', `earnscan_done_${prevISO}`,
           '→ /earnings-scan-trigger?step=morning to diagnose; no board was built'],
        ];
        for (const [label, key, hint] of COLLECTOR_CHECKS) {
          const alertKey = `collector_alert_${key}`;
          if (await env.SIGNAL_KV.get(alertKey)) continue;   // once per miss
          if (await env.SIGNAL_KV.get(key)) continue;        // collected fine
          // P34 sweep (2026-08-03): the marker used to be written BEFORE the
          // DM — a failed send permanently suppressed the alert about a
          // collector that never ran. Mark only after confirmed delivery.
          const dcRaw = await env.SIGNAL_KV.get('discord_config');
          if (dcRaw) {
            const dc = JSON.parse(dcRaw);
            if (dc.channelId) {
              const rC = await sendDiscordDM(env, dc.channelId,
                `🚨 **${label} did NOT run for ${prevISO}** — no done-marker found this morning.\n${hint}`,
                dc.proxyUrl);
              if (rC && rC.ok) {
                await env.SIGNAL_KV.put(alertKey, 'sent', { expirationTtl: 3 * 86400 });
                await logEvent(env, 'error', 'collector-watchdog',
                  `${label} missing done-marker for ${prevISO} — Discord alert sent`, {});
                console.log(`[collector-watchdog] alerted: ${label} missed ${prevISO}`);
              } else console.warn(`[collector-watchdog] alert undelivered for ${label} — will retry`);
            }
          }
        }
      }
    } catch (cwErr) { console.warn('[collector-watchdog]', cwErr.message); }

    // ── Evening preview (2026-06-10): tomorrow\'s special days + health ──
    // Once per weekday 18:00-18:20 ET. Tells the user TONIGHT what tomorrow
    // is (CPI/FED/OPEX+-1/VIX-exp/EOM/NM/earnings), which strategies that
    // gates, plus a one-line system health check and the tilt advisory.
    try {
      const etP = toET(new Date());
      const inWinP = etP.getDay() >= 1 && etP.getDay() <= 5 && etP.getHours() === 18 && etP.getMinutes() < 20;
      if (inWinP) {
        const todayP = isoDateET(etP);
        const prevKey = `evening_preview_${todayP}`;
        if (!(await env.SIGNAL_KV.get(prevKey))) {
          await env.SIGNAL_KV.put(prevKey, 'sent', { expirationTtl: 86400 });
          const tm = nextTrade(etP);
          const tags = [];
          if (cpiSch.includes(todayLong(tm)))  tags.push('CPI → M8BF/Straddle/BOBF blocked (GXBF + Diagonal exempt)');
          if (fedSch.includes(todayLong(tm)))  tags.push('FED day');
          if (opexSch.includes(todayLong(tm))) tags.push('OPEX');
          if (opexSch.some(ds => isTodayBefore(ds, tm))) tags.push('OPEX-1 → Diagonal blocked');
          if (opexSch.some(ds => isTodayAfter(ds, tm)))  tags.push('OPEX+1 → GXBF auto-trigger (unless VIX gaps ≥2% up)');
          if (vixSch.includes(todayLong(tm)))  tags.push('VIX expiry');
          if (isLastTradeMo(tm))               tags.push('EOM → GXBF + Diagonal blocked, EOM Straddle day');
          if (isEomN(1, tm))                   tags.push('EOM-1 → Diagonal blocked');
          if (isFirstTradeMo(tm))              tags.push(`NM → ${tm.getDay() === 1 ? 'Monday (M8BF stands)' : 'non-Monday → NM Straddle'}; Diagonal blocked`);
          if (isEarningsDay(tm))               tags.push('big-tech earnings day');
          let health = [];
          try {
            const ms = await env.SIGNAL_KV.get('history_mirror_state');
            health.push(ms && JSON.parse(ms).ok === false ? 'mirror ⚠️' : 'mirror ✓');
          } catch (_) {}
          try {
            const co = await env.SIGNAL_KV.get(`cor1m_open_${todayP}`);
            health.push(co ? `COR1M ✓ (${JSON.parse(co).cor1m})` : 'COR1M ⚠️ not captured');
          } catch (_) {}
          // Schwab refresh-token age warning (2026-06-11, user-approved):
          // tokens die 7 days after re-auth; warn while there's still time
          // to reconnect instead of discovering a dead dashboard at 8 AM.
          let tokenWarn = null, tokenDaysLeft = null;
          try {
            const tkRaw = await env.SIGNAL_KV.get('schwab_tokens');
            if (tkRaw) {
              const tk = JSON.parse(tkRaw);
              if (tk.refreshExpiry) {
                tokenDaysLeft = (tk.refreshExpiry - Date.now()) / 86400000;
                // 3.5d threshold: a Friday-evening warning still covers
                // weekend expiries (preview only sends on weekdays).
                if (tokenDaysLeft <= 3.5) {
                  const when = tokenDaysLeft <= 1 ? 'within 24h' : `in ~${Math.ceil(tokenDaysLeft)} days`;
                  tokenWarn = `⚠️ Schwab token expires ${when} — open the dashboard and press Connect Schwab (takes 30s)`;
                }
              }
            }
          } catch (_) {}
          let tiltP = null;
          try { tiltP = await computeTiltLine(env, isoDateET(nextTrade(etP))); } catch (_) {}
          // Retry research persistence (idempotent) — catches EOD-time failures
          try { await persistResearchArtifacts(env, etP); } catch (_) {}
          // Vol-flow decomposition retry (idempotent) — then tomorrow's context
          // line: today's label IS what the morning message will report.
          let volP = null;
          try {
            await computeVixDecompDaily(env, etP);
            const vdRaw = await env.SIGNAL_KV.get(`vix_decomp_${todayP}`);
            if (vdRaw) {
              const vd = JSON.parse(vdRaw);
              volP = `Vol flow today: ${vd.label} (slide ${vd.slide >= 0 ? '+' : ''}${vd.slide} · real ${vd.parallel >= 0 ? '+' : ''}${vd.parallel})`;
            }
          } catch (_) {}
          // Score our own advisory claims (GEX regime + Day-type + Vol-flow cells)
          try { await scoreAdvisories(env); } catch (e) { console.warn('[scorecard]', e.message); }
          // EVENING PREVIEW MESSAGE KILLED (owner 2026-07-16: "I don't find any
          // of these messages useful"). All the underlying work above (health
          // checks, COR1M capture, research persists, vol-flow decomp, tilt)
          // still runs — it feeds the dashboard, morning message, and research.
          // The ONLY message that survives is the Schwab token-expiry warning,
          // now a standalone DM whenever it fires (≤3.5 days left) — that
          // warning is what the 2026-07-12 token death lacked.
          const dcRaw = await env.SIGNAL_KV.get('discord_config');
          if (dcRaw) {
            const dc = JSON.parse(dcRaw);
            if (dc.channelId && tokenWarn) {
              // P34 sweep (2026-08-03): this is the single most consequential
              // alert in the system — the 2026-07-12 token death is exactly
              // what it exists to prevent. It used to ride on the block's
              // once-per-day marker written BEFORE any send, so one failed DM
              // silently buried it for the day. Own key, delivery-verified,
              // retried on every 18:00–18:20 tick until it lands.
              const tKey = `token_warn_${todayP}`;
              if (!(await env.SIGNAL_KV.get(tKey))) {
                const rTk = await sendDiscordDM(env, dc.channelId,
                  (tokenDaysLeft != null && tokenDaysLeft <= 1)
                    ? `🚨 **SCHWAB TOKEN DIES WITHIN 24H** — without re-auth the bot cannot trade tomorrow.\nDashboard → Connect Schwab.`
                    : tokenWarn,
                  dc.proxyUrl);
                if (rTk && rTk.ok) await env.SIGNAL_KV.put(tKey, 'sent', { expirationTtl: 86400 });
                else console.warn('[token-warn] UNDELIVERED — retrying next tick');
              }
            }
          }
        }
      }
    } catch (e) { console.warn('[evening-preview]', e.message); }

    // ── Nightly data watchdog: own 18:35-18:50 window (lessons P17) ──
    const etW = toET(new Date());
    if (etW.getDay() >= 1 && etW.getDay() <= 5 && etW.getHours() === 18
        && etW.getMinutes() >= 35 && etW.getMinutes() < 50 && !isHol(etW)) {
      const wdKey = `watchdog_${isoDateET(etW)}`;
      if (!(await env.SIGNAL_KV.get(wdKey))) {
        await env.SIGNAL_KV.put(wdKey, 'running', { expirationTtl: 86400 });
        try {
          const wd = await dataCompletenessCheck(env, etW);
          if (!wd.failed.length) await env.SIGNAL_KV.put(wdKey, 'done', { expirationTtl: 86400 });
          else await env.SIGNAL_KV.delete(wdKey);   // retry within window
        } catch (e) { await env.SIGNAL_KV.delete(wdKey); console.warn('[watchdog]', e.message); }
      }
    }

    // ── Tail-bundle staleness watchdog (2026-06-09) ──
    // The Tail Hedge LaunchAgent (5 PM ET) can fail silently — observed today:
    // macOS TCC blocks launchd from reading ~/Desktop ("Operation not
    // permitted", exit 126), so the bundle (and the cor1m history column)
    // silently stops refreshing. Once per weekday evening (18:00-18:20 ET),
    // fetch the bundle's last daily date; if it isn't today, alert Discord
    // with the manual-run command. One fetch/day (~1.8MB) — negligible.
    try {
      const etW = toET(new Date());
      const isWkdayW = etW.getDay() >= 1 && etW.getDay() <= 5;
      const inWindow = etW.getHours() === 18 && etW.getMinutes() < 20;
      if (isWkdayW && inWindow) {
        const todayW = isoDateET(etW);
        const checkedKey = `tail_bundle_check_${todayW}`;
        if (!(await env.SIGNAL_KV.get(checkedKey))) {
          await env.SIGNAL_KV.put(checkedKey, 'checked', { expirationTtl: 86400 });
          const r = await fetch('https://raw.githubusercontent.com/rava8989/brave/main/cor1m_contango_bundle.json',
            { headers: { 'User-Agent': 'schwab-proxy-worker/1.0' } });
          if (r.ok) {
            const bundle = await r.json();
            const daily = bundle?.daily || [];
            const lastDate = daily.length ? daily[daily.length - 1].date : null;
            if (lastDate && lastDate < todayW) {
              const dcRaw = await env.SIGNAL_KV.get('discord_config');
              if (dcRaw) {
                const dc = JSON.parse(dcRaw);
                if (dc.channelId) {
                  await sendDiscordDM(env, dc.channelId,
                    `ℹ️ **Backtester bundle is stale** — last day ${lastDate}, expected ${todayW}.\nLIVE is unaffected (COR1M/VVIX are cloud-captured from Schwab). Only the Tail Hedge backtester page lags.\n→ Refresh when the Mac is on: \`bash scripts/refresh_tail_hedge.sh\``,
                    dc.proxyUrl);
                  await logEvent(env, 'warn', 'tail-bundle-stale',
                    `bundle last=${lastDate}, expected ${todayW} — Discord alert sent`, { lastDate });
                }
              }
            }
          }
        }
      }
    } catch (tailWatchdogErr) {
      console.warn('[cron] Tail-bundle watchdog failed:', tailWatchdogErr.message);
    }

    let result;
    try {
      result = await handleScheduled(env);
      console.log('[cron] Result:', JSON.stringify(result).slice(0, 500));
    } catch (e) {
      console.error('[cron] Error:', e.message || e);
      result = { status: 'error', error: e.message, date: new Date().toISOString() };

      // Send error notification to Discord so failures aren't silent
      try {
        const dcRaw = await env.SIGNAL_KV.get('discord_config');
        if (dcRaw) {
          const dc = JSON.parse(dcRaw);
          if (dc.channelId) {
            await sendDiscordDM(env, dc.channelId,
              `⚠️ **Signal Error**\n\`${e.message}\`\nCheck schwab-proxy logs.`,
              dc.proxyUrl);
            await logEvent(env, 'error', 'cron-error', e.message);
          }
        }
      } catch (notifyErr) {
        console.error('[cron] Failed to send error notification:', notifyErr.message);
      }
    }
    result.date = result.date || new Date().toISOString();
    await env.SIGNAL_KV.put('last_run', JSON.stringify(result));
  },
};

function jsonResp(data, status, headers) {
  return new Response(JSON.stringify(data), {
    status,
    headers: { 'Content-Type': 'application/json', ...headers },
  });
}
