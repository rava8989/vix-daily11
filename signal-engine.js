/**
 * SIGNAL ENGINE — single source of truth for M8BF/Straddle/GXBF/BOBF logic.
 *
 * Used by BOTH:
 *   - schwab-proxy.js (Cloudflare worker → Discord message)
 *   - index.html      (browser calculator)
 *   - history.html    (history page)
 *
 * Loaded in browser via: <script type="module">
 * Loaded in worker via:  import { ... } from './signal-engine.js'
 *
 * NEVER duplicate this logic elsewhere. If signal rules change, they change HERE.
 */

// ════════════════════════════════════════════════════════════════════
// SCHEDULES (calendar data)
// ════════════════════════════════════════════════════════════════════
// Notes on cpiSch:
//   • October 24, 2025 is the ACTUAL Sep-2025 CPI release. The originally
//     scheduled date was October 15, 2025, postponed due to the Oct 2025
//     government shutdown. The live worker on Oct 15 correctly skipped
//     BOBF/Straddle based on the original schedule, which is why
//     history_data.json has bobfPL=null on 2025-10-15 even though it
//     isn't currently in cpiSch.
//   • November 2025 CPI was suspended (no release for that month due to
//     the same shutdown disruption) — that's why there's no Nov 2025 entry.

export const cpiSch   = ["January 11, 2024","February 13, 2024","March 12, 2024","April 10, 2024","May 15, 2024","June 12, 2024","July 11, 2024","August 14, 2024","September 11, 2024","October 10, 2024","November 13, 2024","December 11, 2024","January 15, 2025","February 12, 2025","March 12, 2025","April 10, 2025","May 13, 2025","June 11, 2025","July 15, 2025","August 12, 2025","September 11, 2025","October 24, 2025","December 18, 2025","January 13, 2026","February 13, 2026","March 11, 2026","April 10, 2026","May 12, 2026","June 10, 2026","July 14, 2026","August 12, 2026","September 11, 2026","October 14, 2026","November 10, 2026","December 10, 2026","January 13, 2027","February 10, 2027","March 10, 2027","April 13, 2027","May 12, 2027","June 10, 2027","July 13, 2027","August 11, 2027","September 14, 2027","October 13, 2027","November 10, 2027","December 10, 2027"];
export const fedSch   = ["January 31, 2024","March 20, 2024","May 1, 2024","June 12, 2024","July 31, 2024","September 18, 2024","November 7, 2024","December 18, 2024","January 29, 2025","March 19, 2025","May 7, 2025","June 18, 2025","July 30, 2025","September 17, 2025","October 29, 2025","December 10, 2025","January 28, 2026","March 18, 2026","April 29, 2026","June 17, 2026","July 29, 2026","September 16, 2026","October 28, 2026","December 9, 2026","January 27, 2027","March 17, 2027","April 28, 2027","June 9, 2027","July 28, 2027","September 15, 2027","October 27, 2027","December 8, 2027"];
export const opexSch  = ["January 19, 2024","February 16, 2024","March 15, 2024","April 19, 2024","May 17, 2024","June 21, 2024","July 19, 2024","August 16, 2024","September 20, 2024","October 18, 2024","November 15, 2024","December 20, 2024","January 17, 2025","February 21, 2025","March 21, 2025","April 17, 2025","May 16, 2025","June 20, 2025","July 18, 2025","August 15, 2025","September 19, 2025","October 17, 2025","November 21, 2025","December 19, 2025","January 16, 2026","February 20, 2026","March 20, 2026","April 17, 2026","May 15, 2026","June 18, 2026","July 17, 2026","August 21, 2026","September 18, 2026","October 16, 2026","November 20, 2026","December 18, 2026","January 15, 2027","February 19, 2027","March 19, 2027","April 16, 2027","May 21, 2027","June 17, 2027","July 16, 2027","August 20, 2027","September 17, 2027","October 15, 2027","November 19, 2027","December 17, 2027"];
export const holidays = ["June 20, 2022","July 4, 2022","September 5, 2022","November 24, 2022","December 26, 2022","January 2, 2023","January 16, 2023","February 20, 2023","April 7, 2023","May 29, 2023","June 19, 2023","July 4, 2023","September 4, 2023","November 23, 2023","December 25, 2023","January 1, 2024","January 15, 2024","February 19, 2024","March 29, 2024","May 27, 2024","June 19, 2024","July 4, 2024","September 2, 2024","November 28, 2024","December 25, 2024","January 1, 2025","January 9, 2025","January 20, 2025","February 17, 2025","April 18, 2025","May 26, 2025","June 19, 2025","July 4, 2025","September 1, 2025","November 27, 2025","December 25, 2025","January 1, 2026","January 19, 2026","February 16, 2026","April 3, 2026","May 25, 2026","June 19, 2026","July 3, 2026","September 7, 2026","November 26, 2026","December 25, 2026","January 1, 2027","January 18, 2027","February 15, 2027","March 26, 2027","May 31, 2027","June 18, 2027","July 5, 2027","September 6, 2027","November 25, 2027","December 24, 2027"];
export const vixSch   = ["January 17, 2024","February 14, 2024","March 20, 2024","April 17, 2024","May 22, 2024","June 18, 2024","July 17, 2024","August 21, 2024","September 18, 2024","October 16, 2024","November 20, 2024","December 18, 2024","January 22, 2025","February 19, 2025","March 18, 2025","April 16, 2025","May 21, 2025","June 18, 2025","July 16, 2025","August 20, 2025","September 17, 2025","October 22, 2025","November 19, 2025","December 17, 2025","January 21, 2026","February 18, 2026","March 18, 2026","April 15, 2026","May 20, 2026","June 17, 2026","July 22, 2026","August 19, 2026","September 16, 2026","October 21, 2026","November 18, 2026","December 16, 2026","January 20, 2027","February 17, 2027","March 17, 2027","April 21, 2027","May 18, 2027","June 16, 2027","July 21, 2027","August 18, 2027","September 15, 2027","October 20, 2027","November 17, 2027","December 22, 2027"];

export const earningsSchedule = [
  // ── 2024 (historical, confirmed) ──
  { date:"January 24, 2024",  company:"Tesla",     ticker:"TSLA", timing:"AH", confirmed:true },
  { date:"January 30, 2024",  company:"Alphabet",  ticker:"GOOGL",timing:"AH", confirmed:true },
  { date:"January 30, 2024",  company:"Microsoft", ticker:"MSFT", timing:"AH", confirmed:true },
  { date:"February 1, 2024",  company:"Apple",     ticker:"AAPL", timing:"AH", confirmed:true },
  { date:"February 1, 2024",  company:"Meta",      ticker:"META", timing:"AH", confirmed:true },
  { date:"February 1, 2024",  company:"Amazon",    ticker:"AMZN", timing:"AH", confirmed:true },
  { date:"February 21, 2024", company:"NVIDIA",    ticker:"NVDA", timing:"AH", confirmed:true },
  { date:"April 23, 2024",    company:"Tesla",     ticker:"TSLA", timing:"AH", confirmed:true },
  { date:"April 24, 2024",    company:"Meta",      ticker:"META", timing:"AH", confirmed:true },
  { date:"April 25, 2024",    company:"Microsoft", ticker:"MSFT", timing:"AH", confirmed:true },
  { date:"April 25, 2024",    company:"Alphabet",  ticker:"GOOGL",timing:"AH", confirmed:true },
  { date:"April 30, 2024",    company:"Amazon",    ticker:"AMZN", timing:"AH", confirmed:true },
  { date:"May 2, 2024",       company:"Apple",     ticker:"AAPL", timing:"AH", confirmed:true },
  { date:"May 22, 2024",      company:"NVIDIA",    ticker:"NVDA", timing:"AH", confirmed:true },
  { date:"July 23, 2024",     company:"Tesla",     ticker:"TSLA", timing:"AH", confirmed:true },
  { date:"July 23, 2024",     company:"Alphabet",  ticker:"GOOGL",timing:"AH", confirmed:true },
  { date:"July 30, 2024",     company:"Microsoft", ticker:"MSFT", timing:"AH", confirmed:true },
  { date:"July 31, 2024",     company:"Meta",      ticker:"META", timing:"AH", confirmed:true },
  { date:"August 1, 2024",    company:"Apple",     ticker:"AAPL", timing:"AH", confirmed:true },
  { date:"August 1, 2024",    company:"Amazon",    ticker:"AMZN", timing:"AH", confirmed:true },
  { date:"August 28, 2024",   company:"NVIDIA",    ticker:"NVDA", timing:"AH", confirmed:true },
  { date:"October 23, 2024",  company:"Tesla",     ticker:"TSLA", timing:"AH", confirmed:true },
  { date:"October 29, 2024",  company:"Alphabet",  ticker:"GOOGL",timing:"AH", confirmed:true },
  { date:"October 30, 2024",  company:"Microsoft", ticker:"MSFT", timing:"AH", confirmed:true },
  { date:"October 30, 2024",  company:"Meta",      ticker:"META", timing:"AH", confirmed:true },
  { date:"October 31, 2024",  company:"Apple",     ticker:"AAPL", timing:"AH", confirmed:true },
  { date:"October 31, 2024",  company:"Amazon",    ticker:"AMZN", timing:"AH", confirmed:true },
  { date:"November 20, 2024", company:"NVIDIA",    ticker:"NVDA", timing:"AH", confirmed:true },
  // ── 2025 (historical, confirmed) ──
  { date:"January 29, 2025",  company:"Microsoft", ticker:"MSFT", timing:"AH", confirmed:true },
  { date:"January 29, 2025",  company:"Meta",      ticker:"META", timing:"AH", confirmed:true },
  { date:"January 29, 2025",  company:"Tesla",     ticker:"TSLA", timing:"AH", confirmed:true },
  { date:"January 30, 2025",  company:"Apple",     ticker:"AAPL", timing:"AH", confirmed:true },
  { date:"February 4, 2025",  company:"Alphabet",  ticker:"GOOGL",timing:"AH", confirmed:true },
  { date:"February 6, 2025",  company:"Amazon",    ticker:"AMZN", timing:"AH", confirmed:true },
  { date:"February 26, 2025", company:"NVIDIA",    ticker:"NVDA", timing:"AH", confirmed:true },
  { date:"April 22, 2025",    company:"Tesla",     ticker:"TSLA", timing:"AH", confirmed:true },
  { date:"April 24, 2025",    company:"Alphabet",  ticker:"GOOGL",timing:"AH", confirmed:true },
  { date:"April 30, 2025",    company:"Microsoft", ticker:"MSFT", timing:"AH", confirmed:true },
  { date:"April 30, 2025",    company:"Meta",      ticker:"META", timing:"AH", confirmed:true },
  { date:"May 1, 2025",       company:"Apple",     ticker:"AAPL", timing:"AH", confirmed:true },
  { date:"May 1, 2025",       company:"Amazon",    ticker:"AMZN", timing:"AH", confirmed:true },
  { date:"May 28, 2025",      company:"NVIDIA",    ticker:"NVDA", timing:"AH", confirmed:true },
  { date:"July 23, 2025",     company:"Tesla",     ticker:"TSLA", timing:"AH", confirmed:true },
  { date:"July 23, 2025",     company:"Alphabet",  ticker:"GOOGL",timing:"AH", confirmed:true },
  { date:"July 30, 2025",     company:"Microsoft", ticker:"MSFT", timing:"AH", confirmed:true },
  { date:"July 30, 2025",     company:"Meta",      ticker:"META", timing:"AH", confirmed:true },
  { date:"July 31, 2025",     company:"Apple",     ticker:"AAPL", timing:"AH", confirmed:true },
  { date:"July 31, 2025",     company:"Amazon",    ticker:"AMZN", timing:"AH", confirmed:true },
  { date:"August 27, 2025",   company:"NVIDIA",    ticker:"NVDA", timing:"AH", confirmed:true },
  { date:"October 22, 2025",  company:"Tesla",     ticker:"TSLA", timing:"AH", confirmed:true },
  { date:"October 29, 2025",  company:"Microsoft", ticker:"MSFT", timing:"AH", confirmed:true },
  { date:"October 29, 2025",  company:"Meta",      ticker:"META", timing:"AH", confirmed:true },
  { date:"October 29, 2025",  company:"Alphabet",  ticker:"GOOGL",timing:"AH", confirmed:true },
  { date:"October 30, 2025",  company:"Apple",     ticker:"AAPL", timing:"AH", confirmed:true },
  { date:"October 30, 2025",  company:"Amazon",    ticker:"AMZN", timing:"AH", confirmed:true },
  { date:"November 19, 2025", company:"NVIDIA",    ticker:"NVDA", timing:"AH", confirmed:true },
  // ── Q1 2026 (Jan/Feb reports) ──
  { date:"January 28, 2026",  company:"Microsoft", ticker:"MSFT", timing:"AH", confirmed:true },
  { date:"January 28, 2026",  company:"Meta",      ticker:"META", timing:"AH", confirmed:true },
  { date:"January 29, 2026",  company:"Tesla",     ticker:"TSLA", timing:"AH", confirmed:true },
  { date:"January 29, 2026",  company:"Apple",     ticker:"AAPL", timing:"AH", confirmed:true },
  { date:"February 4, 2026",  company:"Alphabet",  ticker:"GOOGL",timing:"AH", confirmed:true },
  { date:"February 5, 2026",  company:"Amazon",    ticker:"AMZN", timing:"AH", confirmed:true },
  { date:"February 26, 2026", company:"NVIDIA",    ticker:"NVDA", timing:"AH", confirmed:true },
  // ── Q2 2026 (Apr/May reports) ──
  { date:"April 22, 2026",    company:"Tesla",     ticker:"TSLA", timing:"AH", confirmed:true  },
  { date:"April 29, 2026",    company:"Alphabet",  ticker:"GOOGL",timing:"AH", confirmed:true  },
  { date:"April 29, 2026",    company:"Microsoft", ticker:"MSFT", timing:"AH", confirmed:true  },
  { date:"April 29, 2026",    company:"Meta",      ticker:"META", timing:"AH", confirmed:true  },
  { date:"April 29, 2026",    company:"Amazon",    ticker:"AMZN", timing:"AH", confirmed:true  },
  { date:"April 30, 2026",    company:"Apple",     ticker:"AAPL", timing:"AH", confirmed:true  },
  { date:"May 20, 2026",      company:"NVIDIA",    ticker:"NVDA", timing:"AH", confirmed:true  },
  // ── Q3 2026 (Jul/Aug reports — estimates) ──
  { date:"July 22, 2026",     company:"Tesla",     ticker:"TSLA", timing:"AH", confirmed:true  },
  { date:"July 22, 2026",     company:"Alphabet",  ticker:"GOOGL",timing:"AH", confirmed:true  },
  { date:"July 29, 2026",     company:"Microsoft", ticker:"MSFT", timing:"AH", confirmed:true  },
  { date:"July 29, 2026",     company:"Meta",      ticker:"META", timing:"AH", confirmed:false },
  { date:"July 30, 2026",     company:"Apple",     ticker:"AAPL", timing:"AH", confirmed:false },
  { date:"July 30, 2026",     company:"Amazon",    ticker:"AMZN", timing:"AH", confirmed:false },
  { date:"August 26, 2026",   company:"NVIDIA",    ticker:"NVDA", timing:"AH", confirmed:false },
  // ── Q4 2026 (Oct/Nov reports — estimates) ──
  { date:"October 21, 2026",  company:"Tesla",     ticker:"TSLA", timing:"AH", confirmed:false },
  { date:"October 22, 2026",  company:"Alphabet",  ticker:"GOOGL",timing:"AH", confirmed:false },
  { date:"October 27, 2026",  company:"Microsoft", ticker:"MSFT", timing:"AH", confirmed:false },
  { date:"October 28, 2026",  company:"Meta",      ticker:"META", timing:"AH", confirmed:false },
  { date:"October 29, 2026",  company:"Apple",     ticker:"AAPL", timing:"AH", confirmed:false },
  { date:"October 29, 2026",  company:"Amazon",    ticker:"AMZN", timing:"AH", confirmed:false },
  { date:"November 18, 2026", company:"NVIDIA",    ticker:"NVDA", timing:"AH", confirmed:false },
];

// ════════════════════════════════════════════════════════════════════
// THRESHOLDS
// ════════════════════════════════════════════════════════════════════

export const T = {
  DROP_GXBF: 0.65,
  O2O_M8BF: 1.4, VIX_MAX_GXBF: 25, VIX_MAX_BOBF: 23,
  SPX_GAP_THRESHOLD: 0.9,
};

// ════════════════════════════════════════════════════════════════════
// DATE HELPERS (ET-based)
// ════════════════════════════════════════════════════════════════════

export function toET(d = new Date()) {
  // Engine-agnostic ET conversion. The old form
  //   new Date(d.toLocaleString('en-US', { timeZone: 'America/New_York' }))
  // breaks on Safari/WebKit (iPhone): modern toLocaleString emits a U+202F
  // narrow-no-break-space before AM/PM and Safari's date parser returns
  // Invalid Date for that string (Chrome/V8 tolerate it). That made every
  // calculateSignal() call on history.html throw → page failed on iPhone.
  //
  // formatToParts + the numeric Date constructor has NO string-parsing step,
  // so it yields the SAME local wall-clock fields on V8 (worker), Chrome AND
  // Safari — behaviour is byte-identical to the old code on V8/Chrome.
  const p = new Intl.DateTimeFormat('en-US', {
    timeZone: 'America/New_York',
    year: 'numeric', month: '2-digit', day: '2-digit',
    hour: '2-digit', minute: '2-digit', second: '2-digit',
    hourCycle: 'h23',
  }).formatToParts(d).reduce((a, x) => (a[x.type] = x.value, a), {});
  return new Date(+p.year, +p.month - 1, +p.day, +p.hour, +p.minute, +p.second);
}

// PERF (iPhone crash fix): toLocaleDateString is ~100-1000x slower than a
// manual format and is called tens of thousands of times synchronously by
// history.html (calculateSignal runs ~436x, each doing 6+ schedule lookups
// via todayLong + isTodayBefore/After scans). On a fast Mac that's ~2s; on a
// slower iPhone CPU it blocked the main thread long enough to trip iOS's
// unresponsive-page watchdog ("a problem repeatedly occurred"). This manual
// formatter returns a BYTE-IDENTICAL string ("May 15, 2026" — month long,
// no leading zero, no padding), so every schedule.includes(todayLong(d))
// comparison still matches exactly. Zero logic/number change.
export const _MONTHS_LONG = ['January','February','March','April','May','June',
  'July','August','September','October','November','December'];
export function dateLong(d) {
  return `${_MONTHS_LONG[d.getMonth()]} ${d.getDate()}, ${d.getFullYear()}`;
}

export function todayLong(etDate) { return dateLong(etDate); }

export const isWkend = d => { const w = d.getDay(); return w === 0 || w === 6; };
export const isHol   = d => holidays.includes(dateLong(d));
export const isTrade = d => !isWkend(d) && !isHol(d);

export function nextTrade(d) { const n = new Date(d); do { n.setDate(n.getDate() + 1); } while (isWkend(n) || isHol(n)); return n; }
export function prevTrade(d) { const p = new Date(d); do { p.setDate(p.getDate() - 1); } while (isWkend(p) || isHol(p)); return p; }

export function isTodayAfter(ds, etDate) {
  const d = new Date(ds); if (isNaN(d)) return false;
  const a = nextTrade(d);
  return a.getFullYear() === etDate.getFullYear() && a.getMonth() === etDate.getMonth() && a.getDate() === etDate.getDate();
}
export function isTodayBefore(ds, etDate) {
  const d = new Date(ds); if (isNaN(d)) return false;
  const b = prevTrade(d);
  return b.getFullYear() === etDate.getFullYear() && b.getMonth() === etDate.getMonth() && b.getDate() === etDate.getDate();
}

export function parseLong(s) { const d = new Date(s); return isNaN(d) ? null : new Date(d.getFullYear(), d.getMonth(), d.getDate(), 12); }
export function schedInMonth(list, ref) {
  const y = ref.getFullYear(), m = ref.getMonth();
  for (const s of list) { const d = parseLong(s); if (d && d.getFullYear() === y && d.getMonth() === m) return d; }
  return null;
}

export function isVixAfterOpexDay(ref) {
  if (!vixSch.includes(todayLong(ref))) return false;
  const vx = schedInMonth(vixSch, ref), op = schedInMonth(opexSch, ref);
  if (!vx || !op) return false;
  return vx > op;
}

export const isPostOpexMon = (etDate) => opexSch.some(ds => isTodayAfter(ds, etDate)) && etDate.getDay() === 1;
export const isLastTradeMo = (d) => nextTrade(d).getMonth() !== d.getMonth();
export function isEomN(n, d) { const f = new Date(d.getFullYear(), d.getMonth() + 1, 1); let t = prevTrade(f); for (let i = 0; i < n; i++) t = prevTrade(t); return t.getFullYear() === d.getFullYear() && t.getMonth() === d.getMonth() && t.getDate() === d.getDate(); }
export const isFirstTradeMo = (d) => prevTrade(d).getMonth() !== d.getMonth();
export function isFirstTradeMon(d) { return isFirstTradeMo(d) && d.getDay() === 1; }

// ════════════════════════════════════════════════════════════════════
// M8BF SCHEDULE + HELPERS
// ════════════════════════════════════════════════════════════════════

// 2nd trading Thursday override — M8BF afternoon window (13:30-14:00) on the
// 2nd Thursday of the month, instead of the default morning window. Mirrors
// the inline impl in index.html (line ~992) and backtester.html (line ~617).
export function isSecondTradingThursday(d) {
  if (!d || d.getDay() !== 4) return false;
  let thuCount = 0;
  for (let day = 1; day <= d.getDate(); day++) {
    if (new Date(d.getFullYear(), d.getMonth(), day).getDay() === 4) thuCount++;
  }
  return thuCount === 2;
}

export function m8Sched(dow, d) {
  const blocked = ["10","25","35","40","65","80"];
  const comboBans = {0:95,20:15,55:50,65:60,85:90};
  switch (dow) {
    case 1: return { t: "11:00", window: "11:00–11:30", blocked, comboBans };
    case 2: return { t: "13:30", window: "13:30–14:00", blocked, comboBans };
    case 3: return { t: "12:00", window: "12:00–12:30", blocked, comboBans };
    case 4:
      if (d && isSecondTradingThursday(d)) return { t: "13:30", window: "13:30–14:00 (2nd Thu)", blocked, comboBans };
      return { t: "11:00", window: "11:00–11:30", blocked, comboBans };
    case 5: return { t: "13:00", window: "13:00–13:30", blocked, comboBans };
    default: return null;
  }
}
export function m8Msg(d) { const sc = m8Sched(d.getDay(), d); return sc ? `M8BF — Window ${sc.window}` : "M8BF"; }

// ════════════════════════════════════════════════════════════════════
// DAY-OF-WEEK LABELING
// ════════════════════════════════════════════════════════════════════

export function ordinal(n) { const s = ["th", "st", "nd", "rd"], v = n % 100; return n + (s[(v - 20) % 10] || s[v] || s[0]); }
export function wdName(d) { return ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"][d] || ""; }
export function tradeWdLabel(d) {
  const t = new Date(d.getFullYear(), d.getMonth(), d.getDate());
  const dow = t.getDay(), ms = new Date(t.getFullYear(), t.getMonth(), 1);
  let c = 0;
  for (let x = new Date(ms); x <= t; x.setDate(x.getDate() + 1)) { if (!isTrade(x)) continue; if (x.getDay() === dow) c++; }
  if (!isTrade(t)) return `${wdName(dow)} (market closed)`;
  return `${ordinal(c)} ${wdName(dow)}`;
}

// ════════════════════════════════════════════════════════════════════
// EARNINGS HELPERS
// ════════════════════════════════════════════════════════════════════

export function isEarningsDay(etDate) { return earningsSchedule.some(e => e.date === todayLong(etDate)); }
export function isNonAmznTslaEarningsDay(etDate) { return earningsSchedule.some(e => e.date === todayLong(etDate) && e.ticker !== 'AMZN' && e.ticker !== 'TSLA'); }
export function isDayAfterAnyEarnings(etDate) { return earningsSchedule.some(e => isTodayAfter(e.date, etDate)); }

// ════════════════════════════════════════════════════════════════════
// ════════════════════════════════════════════════════════════════════
// CANONICAL VIX 20-DAY PERCENTILE COMPUTATION
// ════════════════════════════════════════════════════════════════════
// THE single source of truth for the diagonal regime filter. Used by:
//   • schwab-proxy.js (live worker — handleScheduled + handleDiagonalTrade)
//   • diagonal.html (backtester — buildSpecialDateSets)
//   • computeDiagonalSignal below
//
// DO NOT re-implement this inline anywhere. Drift between live + backtest
// is how the 2026-05-15/18/19/20 "diagonal traded in backtest but live
// signal blocked" bug happened — bs_data.json had null vix_open AND the
// backtester compared open-vs-open while live compared open-vs-close. The
// pre-commit hook scripts/check-vix-pct-canonical.sh enforces this.
//
// Inputs:
//   vixToday              today's first VIX print at-or-after 9:30 ET
//                         (history_data.json: vixOpen)
//   prior20VixCloses      newest-last list of the prior 20 trading days'
//                         vixClose values (history_data.json: vixClose)
//   opts.lo, opts.hi      dead-zone band (default 50 < pct ≤ 80)
//                         2026-06-02: (40, 90] → (50, 90] (corrected VIX data;
//                         pct 41-50% adds +$12.4k / 24 trades / lower MaxDD).
//                         2026-06-09: hi 90 → 80 (COR1M-aware sweep; the
//                         80-90 tail was net-negative under the COR1M filter).
//
// Output: { pct, inDeadZone, reason }
//   • pct          — rounded percentile, or null if data insufficient
//   • inDeadZone   — true if pct ∈ (lo, hi] OR data is insufficient.
//                    Missing-data → block is the safety bias: a percentile
//                    we can't compute is not one we trust.
//   • reason       — short human string for logs / Discord output
export function computeVixPct20d(vixToday, prior20VixCloses, opts = {}) {
  // Default band matches the canonical Diagonal dead zone: (50, 80].
  // hi re-tuned 90 → 80 on 2026-06-09 (COR1M-aware sweep); default updated
  // 2026-06-09 for coherence — live callers pass explicit opts or use .pct
  // only, so this default has no behavioral consumers, but a stale default
  // is a drift trap for future callers.
  const lo = opts.lo ?? 50;
  const hi = opts.hi ?? 80;
  if (vixToday == null || !isFinite(vixToday) || vixToday <= 0) {
    return { pct: null, inDeadZone: true, reason: 'no-vix-today' };
  }
  const valid = (prior20VixCloses || []).filter(c => typeof c === 'number' && isFinite(c) && c > 0);
  if (valid.length < 10) {
    return { pct: null, inDeadZone: true, reason: 'insufficient-prior-data' };
  }
  const below = valid.filter(c => c < vixToday).length;
  const pct = Math.round(100 * below / valid.length);
  const inDeadZone = pct > lo && pct <= hi;
  return {
    pct,
    inDeadZone,
    reason: inDeadZone ? `dead-zone (${pct}% in ${lo}-${hi})` : `edge (${pct}%)`,
  };
}

// ════════════════════════════════════════════════════════════════════
// DIAGONAL SIGNAL (companion strategy, single source of truth)
// Consumed by: schwab-proxy.js (via calculateSignal), index.html (direct)
// ────────────────────────────────────────────────────────────────────
// Canonical 6-filter stack — priority:
//   OPEX-1 > EOM > EOM-1 > NM > COR1M_LOW (<10) > VIX_MID (50–80%)
// Mirrors compute_diagonal_pnl.py DEFAULT_PARAMS.special_active exactly.
// FED / CPI / VIX-EXP / OPEX / OPEX+1 / per-ticker earnings intentionally NOT
// in the stack. Earnings filters dropped 2026-04-29 (per backtest sweep —
// removing earnings gates while widening to 30/40 + EOM/EOM-1 captured more
// edge).
// Entry 12:30–15:00 ET (window; pick any clock time, must equal Exit Time).
// Exit 12:30–15:00 ET next trading day · 1/25 DTE · short +10 ITM · long −20 below · ±5 pt tol.
// Entry and exit must use the SAME wall-clock time so only one diagonal is live at a time.
// ════════════════════════════════════════════════════════════════════
// Diagonal COR1M floor — added 2026-06-09 after empirical review showed
// Diagonal loses money below COR1M 10 (3-yr backtest: 34 trades, 50% WR,
// -$9,667 total, -$284 avg; worst -$4,510). Below ~10 the constituents
// are doing their own thing; the 24h-hold diagonal whipsaws. Default 10
// — backtester/dashboard can override via the optional `cor1mMin` param.
export const DIAGONAL_COR1M_MIN_DEFAULT = 10;

export function computeDiagonalSignal(etDate, vixPct20d = null, cor1m = null, cor1mMin = DIAGONAL_COR1M_MIN_DEFAULT) {
  const opex1 = opexSch.some(ds => isTodayBefore(ds, etDate));
  const eomDay = isLastTradeMo(etDate);
  const eom1Day = isEomN(1, etDate);
  const nmDay = isFirstTradeMo(etDate);

  let diagText, diagBadge = '…', diagGo = false, diagSkipCode = null;
  if (opex1) {
    diagSkipCode = 'OPEX-1';
    diagText = 'No Diagonal (OPEX-1)';
    diagBadge = 'SKIP';
  } else if (eomDay) {
    diagSkipCode = 'EOM';
    diagText = 'No Diagonal (EOM)';
    diagBadge = 'SKIP';
  } else if (eom1Day) {
    diagSkipCode = 'EOM-1';
    diagText = 'No Diagonal (EOM-1)';
    diagBadge = 'SKIP';
  } else if (nmDay) {
    diagSkipCode = 'NM';
    diagText = 'No Diagonal (NM)';
    diagBadge = 'SKIP';
  } else if (cor1m !== null && cor1m !== undefined && cor1m < cor1mMin) {
    // COR1M floor — Diagonal needs at least moderate stock correlation to work.
    diagSkipCode = 'COR1M_LOW';
    diagText = `No Diagonal (COR1M ${cor1m.toFixed(2)} < ${cor1mMin})`;
    diagBadge = 'SKIP';
  } else if (vixPct20d !== null && vixPct20d !== undefined && vixPct20d > 50 && vixPct20d <= 80) {
    diagSkipCode = 'VIX_MID';
    diagText = `No Diagonal (VIX 20d ${vixPct20d}% — dead zone)`;
    diagBadge = 'SKIP';
  } else if (vixPct20d === null || vixPct20d === undefined) {
    // All calendar filters cleared but no VIX percentile yet — waiting state.
    diagText = 'Diagonal pending VIX 20d data';
    diagBadge = '…';
  } else if (cor1m === null || cor1m === undefined) {
    // VIX OK but COR1M data missing — defer with waiting state.
    diagText = 'Diagonal pending COR1M data';
    diagBadge = '…';
  } else {
    // All filters cleared → GO.
    diagGo = true;
    const band = vixPct20d <= 50 ? 'calm' : 'panic';  // > 80 since 50-80 is filtered above
    diagText = `Diagonal 12:30–15:00 ET window (VIX 20d ${vixPct20d}% · COR1M ${cor1m.toFixed(2)} — ${band} edge)`;
    diagBadge = '⏰ 12:30–15:00 ET';
  }

  return { diagText, diagBadge, diagGo, diagSkipCode, vixPct20d, cor1m, cor1mMin };
}

// ════════════════════════════════════════════════════════════════════
// CYCLELAB SHAPE ADVISORY (2026-06-10) — INFORMATIONAL ONLY
// Classifies today\'s 4-week same-weekday prediction curve into the user\'s
// shape classes and carries the validated per-strategy reference numbers.
// Consumed by the worker morning message AND the dashboard calculator —
// never by any gating logic. Definition (user, 2026-06-10): choppy = the
// cumulative curve spends real time on BOTH sides of zero (30-70% above);
// one-sided after an early cross is a trend, not chop.
// Stats from research 2026-06-10 (445 joined days; ✓ = held in both
// halves of history, (!) = thin sample, treat as a hint).
// ════════════════════════════════════════════════════════════════════

export function classifyCyclePrediction(cycDays, etDate = new Date(), lookback = 4) {
  // cycDays = parsed cyclicality_data.json `days` array
  const w = (etDate.getDay() + 6) % 7;            // 0=Mon
  if (w > 4) return null;
  const iso = `${etDate.getFullYear()}-${String(etDate.getMonth() + 1).padStart(2, '0')}-${String(etDate.getDate()).padStart(2, '0')}`;
  const pri = cycDays.filter(x => x.w === w && x.d < iso).slice(-lookback);
  if (pri.length < lookback) return null;
  const ns = pri[0].m.length;
  let c = 0, above = 0, end = 0;
  const cum = [];
  for (let s = 0; s < ns; s++) {
    let v = 0;
    for (const p of pri) v += (p.m[s] || 0);
    c += v; cum.push(c);
    if (c > 0) above++;
  }
  end = c;
  const aboveShare = above / ns;
  let cls = 'MIXED';
  if (aboveShare > 0.3 && aboveShare < 0.7) cls = 'CHOPPY';
  else if (aboveShare >= 0.7 && end > 0) cls = 'BULLISH';
  else if (aboveShare <= 0.3 && end < 0) cls = 'BEARISH';
  return { cls, end: Math.round(end * 10) / 10, aboveShare: Math.round(100 * aboveShare),
           basedOn: pri.map(p => p.d) };
}

// avg $/trade (winRate%, n) per shape vs the strategy\'s own normal.
export const CYCLE_SHAPE_STATS = {
  asOf: '2026-06-10',
  normals: { m8bf: [434, 68, 274], strad: [1091, 63, 84], bobf: [510, 64, 113], gxbf: [856, 70, 69], diag: [378, 74, 234] },
  BULLISH: { m8bf: [422, 65, 133, '⚪'], strad: [818, 58, 38, '🔴'], bobf: [517, 65, 55, '⚪'], gxbf: [886, 70, 30, '⚪'], diag: [435, 74, 113, '⚪'] },
  BEARISH: { m8bf: [476, 73, 86, '⚪'], strad: [1248, 71, 35, '🟢✓'], bobf: [630, 67, 33, '🟢'], gxbf: [1218, 72, 25, '🟢'], diag: [218, 70, 71, '🔴(flip-risk)'] },
  CHOPPY:  { m8bf: [535, 74, 42, '🟢✓'], strad: [327, 43, 7, '🔴(n=7!)'], bobf: [321, 55, 20, '🔴'], gxbf: [-36, 56, 9, '🔴(n=9!)'], diag: [536, 80, 41, '🟢✓'] },
  MIXED: null,
};

// Combined day-type (2026-06-10): COR1M regime group × CycleLab shape.
// Validated cells only (both halves of 2024-09→2026-06 agree, n>=15/cell);
// all other day-types show the label with no strategy claims.
export const DAYTYPE_STATS = {
  asOf: '2026-06-10',
  cells: {
    'NEUTRAL/BEAR': [
      ['Diag', 6, 356, 50, 'historically FLAT — dead weight'],
      ['BOBF', 684, 510, 28, 'favored'],
    ],
    'NEUTRAL/CHOP': [['Diag', 470, 356, 31, 'favored']],
    'NEUTRAL/BULL': [['Strad', 695, 1091, 30, 'below its normal']],
  },
};

export function regimeGroup(bundleRegime) {
  return { R1: 'COMPLACENT', R0: 'NEUTRAL', R2: 'NEUTRAL', R3: 'STRESS', R4: 'STRESS' }[bundleRegime] || null;
}

// Vol-flow day-type (2026-06-11): PRIOR day's VIX-decomposition label —
// ΔATM-IV(~30DTE) = sticky-strike slide + parallel (real fixed-strike
// repricing) + twist; labels from compute_vix_decomposition.py / the worker
// EOD port. Validated cells only (both halves of 2024-09→2026-06 agree,
// n≥10 per half; data/research_vix_instruments_dataset.json). Straddle
// VOL_BID is n=9/9 per half — one short of the bar, shown as suggestive,
// NOT scored. INFORMATIONAL ONLY (tasks/ADVISORY_STACK.md).
export const VOLFLOW_STATS = {
  asOf: '2026-06-11',
  cells: {
    VOL_BID: [
      ['M8BF', 149, 434, 59, 'below its normal'],
      ['Strad', 1902, 1091, 18, 'above (suggestive, small n)'],
    ],
    VOL_SUPPLY: [['Diag', 113, 378, 36, 'below its normal']],
    MIXED: [
      ['M8BF', 595, 434, 108, 'favored'],
      ['Diag', 518, 378, 93, 'favored'],
    ],
    MECHANICAL: null,   // M8BF mildly below normal but deviation is noise-grade
  },
};

// M8BF service-WR advisory (2026-06-12): the Discord service's whole-day win
// rate (all their 5-min fly signals — history m8bfWR). Research findings:
// TRAILING AVERAGES carry no signal (terciles flat). YESTERDAY's value has a
// monotone gradient; only the >=90 cell replicated both halves ($601/79% vs
// $427/68% normal). Cold-streak context (trailing-5 <=25%): 10 prior
// occurrences, 9 wins avg ~$1.1k — crash-aftermath thaws; n tiny, context
// only. INFORMATIONAL — never gates.
export function m8bfWrAdvisoryLine(yWR, trail5, trail5Pctile) {
  if (yWR == null) return null;
  let line = `M8BF svc   │ yday WR ${yWR}%`;
  if (yWR >= 90) line += ` — next-day M8BF historically $601 vs $427 normal (79% WR, ✓both halves)`;
  else if (yWR < 50) line += ` — soft next-day history ($310-350 vs $427), not proven both halves`;
  else line += ` — neutral zone`;
  if (trail5 != null && trail5 <= 25)
    line += ` · COLDEST-STRETCH territory (5d avg ${trail5.toFixed(0)}%${trail5Pctile != null ? `, p${trail5Pctile.toFixed(0)}` : ''}; prior thaws 9/10 wins, n=10 — context only)`;
  return line;
}

export function volFlowAdvisoryLine(label) {
  if (!label) return null;
  const head = `Vol flow   │ yday ${label}`;
  const cells = VOLFLOW_STATS.cells[label];
  if (!cells) return `${head} — no proven strategy deviations`;
  const parts = cells.map(([s, v, n, cnt, note]) => `${s} ${note} ($${v} vs $${n})`);
  return `${head} — ` + parts.join(' · ');
}

// ── SPX options-skew reading (informational gauge, 2026-06-16) ──
// `series` = ascending-by-date array of { date, net, spot }, where
// net = put_skew − call_skew from the daily VIX-smile decomposition
// (data/vix_decomposition.json). Returns the LATEST day's reading, or null.
//   pct    = trailing-120d percentile of today's net skew (0 = complacent, 100 = max fear)
//   regime = price-trend(10d) × skew-change(5d). The cell with edge is "Distribution":
//            Healthy = rally + skew falling · Distribution = rally + skew RISING (caution)
//            Capitulation = selloff + skew rising · Drift = soft + skew falling
// INFORMATIONAL CONTEXT ONLY — never gates a trade. Research 2026-06-16:
// Distribution ~64% up at 20d vs ~77% for Healthy; extremes (pct) mark turns.
export function computeSkewReading(series, opts = {}) {
  const W = opts.window || 120;
  if (!Array.isArray(series) || series.length < W + 11) return null;
  const i = series.length - 1;
  const net = series.map(s => (s ? s.net : null));
  const spot = series.map(s => (s ? s.spot : null));
  const cur = net[i];
  if (cur == null || spot[i] == null || spot[i - 10] == null) return null;
  const hist = net.slice(i - W, i).filter(x => x != null);
  if (hist.length < W * 0.6) return null;
  const pct = Math.round(100 * hist.filter(x => x < cur).length / hist.length);
  const d5 = net[i - 5] == null ? 0 : +(cur - net[i - 5]).toFixed(2);
  const mom10 = +((spot[i] / spot[i - 10] - 1) * 100).toFixed(2);
  const up = mom10 > 0, skewUp = d5 > 0;
  const regime = up && skewUp ? 'Distribution'
               : up && !skewUp ? 'Healthy'
               : !up && skewUp ? 'Capitulation' : 'Drift';
  const zone = pct <= 20 ? 'complacent' : pct >= 80 ? 'elevated fear' : 'normal';
  const line = `Skew     │ ${pct}%ile (${zone}) · ${regime}`;
  return { date: series[i].date, net: +cur.toFixed(2), pct, d5, mom10, regime, zone, line };
}

export function dayTypeAdvisoryLine(group, cycInfo) {
  if (!cycInfo || !cycInfo.cls) return null;
  const shape = { BULLISH: 'BULL', BEARISH: 'BEAR', CHOPPY: 'CHOP', MIXED: 'MIX' }[cycInfo.cls];
  const key = group ? `${group}/${shape}` : null;
  const head = `Day-type   │ ${group || '?'}/${shape}`;
  const cells = key && DAYTYPE_STATS.cells[key];
  if (!cells) return `${head} — no proven strategy deviations`;
  const parts = cells.map(([s, v, n, cnt, note]) => `${s} ${note} ($${v} vs $${n})`);
  return `${head} — ` + parts.join(' · ');
}

// One compact Discord line: class + only the strategies that DEVIATE.
export function cycleAdvisoryLine(cycInfo) {
  if (!cycInfo || !cycInfo.cls) return null;
  const st = CYCLE_SHAPE_STATS[cycInfo.cls];
  if (!st) return `CycleLab   │ ${cycInfo.cls} week-pattern`;
  const names = { m8bf: 'M8BF', strad: 'Strad', bobf: 'BOBF', gxbf: 'GXBF', diag: 'Diag' };
  const parts = [];
  for (const [k, v] of Object.entries(st)) {
    const flag = v[3] || '';
    if (flag.startsWith('⚪')) continue;
    const norm = CYCLE_SHAPE_STATS.normals[k][0];
    parts.push(`${names[k]} $${v[0]} vs $${norm}${flag.includes('✓') ? '✓' : flag.includes('!') || flag.includes('flip') ? '?' : ''}`);
  }
  return `CycleLab   │ ${cycInfo.cls} — ` + (parts.length ? parts.join(' · ') : 'all strategies ≈ normal');
}

// ════════════════════════════════════════════════════════════════════
// SIGNAL CALCULATION (single source of truth)
// Consumed by: schwab-proxy.js, index.html, history.html
// ════════════════════════════════════════════════════════════════════

export function calculateSignal({ vixToday, vixYOpen, vixYClose, spxGapPct, etDate, prevWR = null, vixPct20d = null, rsi14 = null, cor1m = null }) {
  const cpiDay = cpiSch.includes(todayLong(etDate));
  const dow = etDate.getDay();
  const isMon = dow === 1, isFri = dow === 5, isWed = dow === 3;
  const nmDay = isFirstTradeMo(etDate), nmMon = isFirstTradeMon(etDate);
  const fedDay = fedSch.includes(todayLong(etDate));
  const opexDay = opexSch.includes(todayLong(etDate));
  const postOpMon = isPostOpexMon(etDate);
  const postOpDay = opexSch.some(ds => isTodayAfter(ds, etDate));
  const eomDay = isLastTradeMo(etDate), eom1 = isEomN(1, etDate), eom2 = isEomN(2, etDate);
  const vixExpDay = vixSch.includes(todayLong(etDate));
  const opex1 = opexSch.some(ds => isTodayBefore(ds, etDate));
  const earningsDay = isEarningsDay(etDate);

  const o2o = (vixYOpen != null) ? vixYOpen - vixToday : NaN;
  // FIX (2026-06-09 audit): guard null vixYClose — without this, null - vixToday
  // = -vixToday, triggering "No Straddle (overnight VIX up)" misattribution
  // instead of a "waiting for prior VIX close" diagnosis.
  const oNight = (vixYClose != null) ? (vixYClose - vixToday) : NaN;

  const spxGapCancelsStrad = (spxGapPct !== null && spxGapPct !== undefined && Math.abs(spxGapPct) >= T.SPX_GAP_THRESHOLD);
  const vixExpAfterOpex = isVixAfterOpexDay(etDate);
  const nonAmznTslaEarn = isNonAmznTslaEarningsDay(etDate);
  const m8bfBanned = eomDay || eom1 || opex1 || vixExpAfterOpex || nonAmznTslaEarn;

  let rec = "", theme = "neutral", crossed = false, pmNote = false;
  let blockT = "", blockD = "", entryT = "", badge = "";
  let strikeInfo = null;
  let cpiLongCall = false;

  // ── GXBF evaluation (STRATEGY-INDEPENDENT) ──
  // Hybrid gating: blocks are VIX≥max, OPEX+1 + VIX gap-up≥%, EOM.
  // OPEX-1, CPI day, and NM-non-Mon are ALLOWED (previously banned — removed).
  // Trigger = (overnight VIX drop > DROP_GXBF) OR OPEX+1 (auto-trigger).
  // Center routing: OPEX-1 / VIX-expiry / FED → OI grid; else Volume.
  // Computed here so CPI day no longer suppresses GXBF (other strategies still
  // block on CPI per their own rules).
  const gxbfVixOvernightPct = (vixYClose != null && vixYClose !== 0) ? (vixToday - vixYClose) / vixYClose * 100 : 0;
  const gxbfTrigger = (oNight > T.DROP_GXBF) || postOpDay;
  let gxbfFires = false, gxbfBlockedReason = null;
  if (gxbfTrigger) {
    // CPI day is ALLOWED for GXBF (matches docblock above and gxbf-backtester.html
    // gating). Previously a `if (cpiDay) gxbfBlockedReason = 'CPI day'` line
    // suppressed GXBF on CPI days — that was code-vs-doc drift (2026-06-09
    // audit). gxbf-backtester historical P/L always included CPI day trades.
    if (vixToday >= T.VIX_MAX_GXBF) {
      gxbfBlockedReason = `VIX ${vixToday} ≥ ${T.VIX_MAX_GXBF}`;
    } else if (postOpDay && gxbfVixOvernightPct >= 2) {
      gxbfBlockedReason = `VIX gapped up ${gxbfVixOvernightPct.toFixed(1)}% overnight`;
    } else if (eomDay) {
      gxbfBlockedReason = `EOM`;
    } else {
      gxbfFires = true;
    }
  }

  if (cpiDay && !gxbfFires) {
    // CPI day hard-blocks M8BF / Straddle / BOBF. GXBF is EXEMPT (matches
    // docblock above and gxbf-backtester.html — CPI day is allowed for GXBF
    // as of the 2026-06-09 audit fix). Diagonal companion has its own gating.
    rec = "No trades (CPI day)";
    theme = "block";
    crossed = true;
    blockT = "cpi-day";
    blockD = "CPI day — M8BF/Straddle/BOBF blocked (GXBF exempt)";
    badge = "BLOCKED";
    strikeInfo = null;
  } else if (gxbfFires) {
    // GXBF fires regardless of OPEX-1 / NM-non-Mon (strategy-independent).
    if (postOpDay) { rec = "GXBF @ 9:36 AM (OPEX+1)"; theme = "gxbf"; entryT = "9:36 AM"; badge = "GXBF"; }
    else            { rec = `GXBF @ 9:36 AM`;          theme = "gxbf"; entryT = "9:36 AM"; badge = "GXBF"; }
  } else {
    // STRADDLE — independent strategy. Has its OWN range: overnight VIX
    // drop must be between 0 (exclusive) and T.DROP_GXBF (inclusive, =0.65).
    // No reference to GXBF — Straddle stands on its own.
    // M8BF is also independent — its status lives in m8bfText only.
    if (!Number.isFinite(oNight)) {
      // FIX (2026-06-09 audit): vixYClose missing → can't compute overnight delta.
      // Don't silently emit "VIX up overnight" — surface the data gap.
      rec = "No Straddle (waiting for prior VIX close)"; theme = "block"; crossed = true;
      blockT = "data"; blockD = "vixYClose missing"; badge = "BLOCKED"; strikeInfo = null;
    } else if (oNight > 0 && oNight <= T.DROP_GXBF) {
      rec = "Straddle @ 9:32 AM"; theme = "strad"; entryT = "9:32 AM"; badge = "STRADDLE";
    } else if (oNight > T.DROP_GXBF) {
      rec = `No Straddle (overnight VIX drop > ${T.DROP_GXBF})`; theme = "block"; crossed = true; blockT = "vix-down-big"; blockD = `Drop > ${T.DROP_GXBF}`; badge = "BLOCKED"; strikeInfo = null;
    } else {
      rec = "No Straddle (overnight VIX up)"; theme = "block"; crossed = true; blockT = "vix-up"; blockD = "No overnight VIX drop"; badge = "BLOCKED"; strikeInfo = null;
    }

    // NM-non-Mon override: Straddle/NoStraddle → NM Straddle. (GXBF unaffected.)
    if (nmDay && !isMon && (rec.startsWith("Straddle") || rec.startsWith("No Straddle"))) { rec = "NM Straddle @ 9:32 AM"; theme = "strad"; crossed = false; blockT = ""; entryT = "9:32 AM"; badge = "NM STRADDLE"; strikeInfo = null; }
    if (eomDay) { rec = "Straddle @ 9:32 AM (EOM)"; theme = "strad"; crossed = false; blockT = ""; entryT = "9:32 AM"; badge = "EOM STRADDLE"; strikeInfo = null; }
    if (opexDay && rec.startsWith("Straddle")) { rec = "No Straddle (OPEX day)"; theme = "block"; crossed = true; blockT = "hard"; blockD = "Straddle not on OPEX"; badge = "BLOCKED"; }
  }

  if (pmNote) rec += " (afternoon times preferred)";

  // SPX gap cancels straddle. FAIL-SAFE: if rec is Straddle-flavored but
  // spxGapPct couldn't be computed (caller didn't supply it), block with
  // a "waiting" message rather than silently passing the gate. Worker
  // always has spxGapPct (computed from today's spxOpen / yesterday's
  // spxClose in history_data.json); dashboards must mirror that path.
  const recIsStraddleVariant = (rec === "Straddle @ 9:32 AM" || rec === "Straddle @ 9:32 AM (EOM)" || rec.startsWith("NM Straddle"));
  if (recIsStraddleVariant && blockT !== '0%rule') {
    if (spxGapPct === null || spxGapPct === undefined) {
      rec = `No Straddle (waiting for SPX gap data)`; theme = "block"; crossed = true; blockT = "data"; blockD = "SPX gap not yet computed"; badge = "BLOCKED"; strikeInfo = null;
    } else if (spxGapCancelsStrad) {
      const dir = spxGapPct > 0 ? '▲' : '▼';
      rec = `No Straddle (SPX gap ${dir}${Math.abs(spxGapPct).toFixed(2)}%)`; theme = "block"; crossed = true; blockT = "gap"; blockD = `SPX gap ≥ ${T.SPX_GAP_THRESHOLD}%`; badge = "BLOCKED"; strikeInfo = null;
    }
  }

  // o2o cancels straddle — and FAIL CLOSED when vixYOpen is missing: NaN > T is
  // false, which would fire the straddle with the o2o filter unevaluated
  // (audit P2 2026-07-06). Mirrors the SPX-gap "waiting" block above.
  if (blockT !== '0%rule' && (rec === "Straddle @ 9:32 AM" || rec.startsWith("NM Straddle"))) {
    if (!isFinite(o2o)) {
      rec = `No Straddle (waiting for prior VIX open)`; theme = "block"; crossed = true; blockT = "data"; blockD = `prior VIX open not yet available`; badge = "BLOCKED"; strikeInfo = null;
    } else if (o2o > T.O2O_M8BF) {
      rec = `No Straddle (o2o ${o2o.toFixed(1)} > ${T.O2O_M8BF})`; theme = "block"; crossed = true; blockT = "o2o"; blockD = `Open-to-open ${o2o.toFixed(1)} > ${T.O2O_M8BF}`; badge = "BLOCKED"; strikeInfo = null;
    }
  }

  // ── Regular-Wednesday straddle cancel (user rule, restored 2026-06-24) ──
  // The SPX 0DTE straddle is NOT taken on a plain Wednesday. Fed days, EOM
  // (last trading day) and NM (first trading day) still straddle — those are
  // exempt via the !fedDay/!eomDay/!nmDay guards (their recs are "(EOM)" /
  // "NM Straddle" anyway). theme === 'strad' here means a live straddle that
  // survived the gap/o2o blocks. Placed BEFORE the WR overrides so a prior-day
  // 0% win-rate STILL force-fires the straddle through (WR rules ignore the
  // weekday gate, exactly as they ignore Fed days).
  if (theme === "strad" && dow === 3 && !fedDay && !eomDay && !nmDay) {
    rec = "No Straddle (Wednesday)"; theme = "block"; crossed = true;
    blockT = "wed"; blockD = "No straddle on a regular Wednesday (Fed / EOM / NM exempt)";
    badge = "BLOCKED"; strikeInfo = null;
  }

  // WR=0% and WR>=90% overrides
  if (prevWR != null) {
    if (prevWR === 0 && !cpiDay && !rec.includes("GXBF")) {
      // 0% WR forces Straddle the next day — Fed day is NOT a special case
      // (canonical rule per user: WR rules treat Fed days like any other day).
      // CPI stays in the gate because CPI hard-blocks ALL strategies upstream;
      // the 0% rule cannot override a CPI block.
      // GXBF is also EXCLUDED — like the 90% override, the 0% rule cannot
      // cancel GXBF because GXBF is strategy-independent (overnight VIX drop).
      // 2026-06-09 audit: added `!rec.includes("GXBF")` guard to match the
      // 90% rule's GXBF exclusion at the next branch.
      // 2026-05-28: removed an erroneous `!fedDay` clause.
      // 2026-06-22 audit fix: PRESERVE the EOM/NM straddle badge (and its higher
      // max-debit cap). The old code hardcoded badge="STRADDLE", which on an EOM day
      // with prior WR=0 silently cut the live max-debit cap from $35 to $32.
      rec = eomDay ? "Straddle @ 9:32 AM (EOM)" : (nmDay && !isMon) ? "NM Straddle @ 9:32 AM" : "Straddle @ 9:32 AM";
      theme = "strad"; crossed = false; blockT = "0%rule"; entryT = "9:32 AM";
      badge = eomDay ? "EOM STRADDLE" : (nmDay && !isMon) ? "NM STRADDLE" : "STRADDLE"; strikeInfo = null;
    } else if (prevWR >= 90 && !cpiDay && !rec.includes("GXBF")) {
      // 90% rule cannot cancel GXBF — GXBF is independent (overnight VIX drop).
      // For everything else (Straddle, NM Straddle, gap/o2o blocks, M8BF bans
      // like EOM/EOM-1/OPEX-1), 90% rule still forces M8BF as documented.
      const sc = m8Sched(dow, etDate);
      rec = m8Msg(etDate); theme = "m8bf"; badge = "M8BF";
      strikeInfo = sc; entryT = sc?.window || ""; blockT = "90%rule";
    }
  }

  // ── FRIDAY straddle cancel — ABSOLUTE (owner rule 2026-08-14) ──
  // 2026 Fridays ran 0-for-9 (−$12,848) across EVERY subtype — regular, EOM,
  // NM and Fed alike — while non-Friday 2026 straddles made +$23k. Gamma
  // filters tested dead (no separation at any rank), so there is no
  // conditional version. Unlike the Wednesday rule this has NO exemptions and
  // sits AFTER the WR overrides: nothing force-fires a straddle on a Friday
  // (a 0%-WR Friday stays blocked). GXBF is untouched — it decides upstream.
  if (theme === "strad" && dow === 5) {
    rec = "No Straddle (Friday)"; theme = "block"; crossed = true;
    blockT = "fri"; blockD = "No straddle on Fridays (owner rule 2026-08-14 — 2026 Fridays 0/9, all subtypes)";
    badge = "BLOCKED"; strikeInfo = null; entryT = "";
  }

  // OPEX+1 GXBF override is now folded into the upfront strategy-independent
  // GXBF evaluation above (postOpDay is part of gxbfTrigger). The standalone
  // block here is removed — GXBF decides for itself before M8BF/Straddle paths
  // even run, so there's nothing to override at this point.

  // ── BOBF card logic ──
  const bobfBlocks = [];
  if (cpiDay) bobfBlocks.push("CPI day");
  if (nmMon) bobfBlocks.push("NM Monday");
  if (vixExpDay) bobfBlocks.push("VIX expiration");
  if (opexDay) bobfBlocks.push("OPEX");
  if (opex1) bobfBlocks.push("OPEX-1");
  if (eom2) bobfBlocks.push("EOM-2");
  if (eom1) bobfBlocks.push("EOM-1");
  if (earningsDay) { const tickers = earningsSchedule.filter(e => e.date === todayLong(etDate)).map(e => e.ticker).join(','); bobfBlocks.push(`earnings (${tickers})`); }
  if (vixToday > T.VIX_MAX_BOBF) bobfBlocks.push("high VIX");

  // ── BOBF type qualification + RSI gate (matches schwab-proxy.js prefilterBobf) ──
  // Three BOBF types, each with its own RSI band:
  //   Friday RSI BOBF   — Fri only, RSI must be in 40-65
  //   BOBF Vix up       — Mon-Thu, overnight VIX up ≥ 0.01 pts (no RSI filter)
  //   BOBF Vix down     — Mon-Thu, overnight VIX down ≥ 0.01 pts, RSI ≤ 70
  // Flat overnight VIX on Mon-Thu = no qualifying type → block.
  // RSI filter only evaluated when rsi14 is supplied (caller computes from
  // history_data.json daily closes; null on the first session before history
  // is fetched — block message defers until data arrives).
  let bobfType = null;
  if (isFri) {
    bobfType = 'friday';
    // FAIL-SAFE: if the caller couldn't supply RSI(14), block rather than
    // silently passing the gate. The dashboard had this bug on 2026-05-29
    // EOM Friday — Schwab was disconnected, rsi14 stayed null, the
    // dashboard rendered "BOBF (Friday RSI)" green while the worker
    // (which always reads RSI from history_data.json) correctly blocked
    // with "No BOBF (RSI 72.9 outside 40-65 band)". Mismatch is now
    // structurally impossible: missing data → block, never silent-pass.
    if (rsi14 == null) {
      bobfBlocks.push('Waiting for RSI data');
    } else if (rsi14 < 40 || rsi14 > 65) {
      bobfBlocks.push(`RSI ${rsi14.toFixed(1)} outside 40-65 band`);
    }
  } else if (dow >= 1 && dow <= 4) {
    // FAIL-SAFE: a null prior VIX close coerces to 0 in subtraction
    // (vixToday - null = vixToday → would falsely fire 'vix_up'); block on
    // missing data instead, like the RSI/gap fail-safes above.
    const oNightDelta = (vixToday == null || vixYClose == null) ? null : vixToday - vixYClose;  // + = VIX up overnight
    if (oNightDelta != null && oNightDelta >= 0.01)       bobfType = 'vix_up';
    else if (oNightDelta != null && oNightDelta <= -0.01) bobfType = 'vix_down';
    if (oNightDelta == null) {
      bobfBlocks.push('Waiting for VIX data (overnight type)');
    } else if (!bobfType) {
      bobfBlocks.push('flat overnight VIX (no qualifying type)');
    } else if (bobfType === 'vix_down') {
      // FAIL-SAFE: vix-down BOBF type also needs RSI to evaluate. Block
      // on missing data rather than silently passing.
      if (rsi14 == null) {
        bobfBlocks.push('Waiting for RSI data (vix-down type)');
      } else if (rsi14 > 70) {
        bobfBlocks.push(`RSI ${rsi14.toFixed(1)} > 70 (vix-down type)`);
      }
    }
    // vix_up type has no RSI filter — fires regardless of rsi14 by design
  }

  let bobfRec, bobfBadge;
  if (bobfBlocks.length) {
    bobfRec = `No BOBF (${bobfBlocks.join(", ")})`;
    bobfBadge = "BLOCKED";
  } else {
    // Type-specific labels so the dashboard / Discord message reflect which
    // BOBF variant is actually queued for today.
    // Badge = certainty level, rec = setup type. "POSSIBLE" (owner 2026-07-15):
    // an affirmative badge before the entry confirms read like a done deal —
    // nothing is 100% until the fill, so the yes-state says POSSIBLE.
    if (bobfType === 'friday')        { bobfRec = "BOBF (Friday RSI)";       bobfBadge = "POSSIBLE"; }
    else if (bobfType === 'vix_up')   { bobfRec = "BOBF (VIX up)";           bobfBadge = "POSSIBLE"; }
    else if (bobfType === 'vix_down') { bobfRec = "BOBF (VIX down)";         bobfBadge = "POSSIBLE"; }
    else                              { bobfRec = "BOBF in play";            bobfBadge = "POSSIBLE"; }
  }

  // ── Build dimmed card texts for inactive strategies ──
  // Each strategy card MUST evaluate its own rules (STRATEGY INDEPENDENCE).
  // Helper keeps the reasoning in ONE place so drift cannot happen.
  const m8bfOwnText = () => m8bfBanned
    ? (eomDay?`No M8BF (EOM)`:eom1?`No M8BF (EOM-1)`:opex1?`No M8BF (day before OPEX)`:nonAmznTslaEarn?`No M8BF (earnings)`:vixExpAfterOpex?`No M8BF (VIX exp day)`:`No M8BF`)
    : m8Msg(etDate);
  // GXBF own status — strategy-independent. As of 2026-06-01, CPI day
  // hard-blocks GXBF (added to gxbfBlockedReason chain above). Other gating:
  // VIX≥max, EOM, OPEX+1 + VIX gap-up. Trigger: overnight drop > DROP_GXBF
  // OR OPEX+1 auto-trigger.
  const gxbfOwnText = () => {
    if (!gxbfTrigger) return `No GXBF (overnight VIX drop ≤ ${T.DROP_GXBF})`;
    if (gxbfBlockedReason) return `No GXBF (${gxbfBlockedReason})`;
    return postOpDay ? `GXBF @ 9:36 AM (OPEX+1)` : `GXBF @ 9:36 AM`;
  };
  // Straddle own status — strategy-independent. Range: 0 < oNight ≤ DROP_GXBF.
  // When in range and not 90%-WR-suppressed and not its own block (gap/o2o/OPEX),
  // Straddle WOULD fire even if another strategy claimed the primary `rec` slot.
  // WR≥90% cancels the Straddle on its OWN merits, independent of whether the
  // rec-level 90% rule actually fired. The rec-level rule is skipped on GXBF days
  // (its `!rec.includes("GXBF")` guard protects GXBF) — which previously let the
  // Straddle leak through on an OPEX+1 GXBF day even with WR≥90%. This flag closes
  // that gap: 90%→cancel-Straddle no longer depends on the GXBF guard.
  const wr90Suppress = (prevWR != null && prevWR >= 90 && !cpiDay);
  const stradOwnText = () => {
    // FAIL CLOSED on missing inputs — NaN comparisons are all false and would
    // otherwise fall through to a live-GO "Straddle @ 9:32 AM" with the overnight
    // filters unevaluated (audit P2 2026-07-06).
    if (!isFinite(oNight))        return `No Straddle (waiting for prior VIX close)`;
    if (oNight <= 0)              return `No Straddle (overnight VIX up)`;
    if (oNight > T.DROP_GXBF)     return `No Straddle (overnight VIX drop > ${T.DROP_GXBF})`;
    if (blockT === '90%rule' || wr90Suppress) return `No Straddle (WR ≥ 90%)`;
    if (spxGapCancelsStrad)       return `No Straddle (SPX gap ${spxGapPct >= 0 ? '▲' : '▼'}${Math.abs(spxGapPct).toFixed(2)}%)`;
    if (!isFinite(o2o))           return `No Straddle (waiting for prior VIX open)`;
    if (o2o > T.O2O_M8BF)         return `No Straddle (o2o ${o2o.toFixed(1)} > ${T.O2O_M8BF})`;
    if (opexDay)                  return `No Straddle (OPEX day)`;
    // In range and not blocked — Straddle would fire on its own rules.
    return `Straddle @ 9:32 AM`;
  };

  let m8bfText = rec, stradText = rec, gxbfText = rec;

  if (cpiDay) {
    // CPI day blocks M8BF and Straddle, but GXBF is independent — show its own status.
    m8bfText = `No M8BF (CPI day)`;
    stradText = `No Straddle (CPI day)`;
    gxbfText = gxbfOwnText();
  } else if (theme === 'm8bf') {
    stradText = stradOwnText();
    gxbfText = gxbfOwnText();
  } else if (theme === 'strad') {
    m8bfText = m8bfOwnText();
    gxbfText = gxbfOwnText();
  } else if (theme === 'gxbf') {
    m8bfText = m8bfOwnText();
    stradText = stradOwnText();
  } else if (theme === 'block') {
    if (rec.includes('M8BF')) {
      stradText = stradOwnText();
      gxbfText = gxbfOwnText();
    } else if (rec.includes('Straddle') || rec.includes('Trade')) {
      m8bfText = m8bfOwnText();
      gxbfText = gxbfOwnText();
    } else if (rec.includes('GXBF')) {
      m8bfText = m8bfOwnText();
      stradText = stradOwnText();
    }
  }

  // m8bfStrikeInfo — independent of main strikeInfo, so consumers can show
  // the M8BF banned-strike list even when the main signal was overridden
  // (e.g. OPEX+1 GXBF auto-fire, which nulls the main strikeInfo).
  // null when the M8BF's OWN status is blocked (CPI/m8bfBanned).
  const m8bfStrikeInfo = (cpiDay || m8bfBanned) ? null : m8Sched(dow, etDate);

  // ── GXBF center routing (hybrid) ──
  // Only meaningful when theme === 'gxbf'. OPEX-1 / VIX-expiry / FED → OI grid;
  // all other GXBF days use the volume-weighted center (live default).
  // Consumed by schwab-proxy.js handleGxbfEntry to pick computed.center vs centerOI.
  // 2026-06-10: hybrid OI routing RETIRED (user-approved). The OPEX-1/VIX-exp/
  // FED → OI rule failed out-of-sample validation: always-volume beat hybrid
  // on the test half (+$33.2k vs +$24.4k, n=33) and tied on train
  // (scripts/research_gxbf_negamma_oos.py). Volume center on every GXBF day.
  const centerSource = (theme === 'gxbf') ? 'vol' : null;

  // ── DIAGONAL (companion strategy) — delegated to single source of truth ──
  // cor1m = today's COR1M open (worker: KV cloud capture; dashboard: /health
  // cor1m_cloud; history: the row's cor1m column). Without it the COR1M_LOW
  // filter can't evaluate and the signal stays in the "pending" waiting state.
  const { diagText, diagBadge, diagGo, diagSkipCode } = computeDiagonalSignal(etDate, vixPct20d, cor1m);

  return {
    rec, theme, crossed, badge, entryT, blockT, blockD, pmNote,
    strikeInfo, m8bfStrikeInfo, cpiLongCall,
    m8bfText, stradText, gxbfText,
    bobfRec, bobfBadge, bobfBlocks,
    centerSource,
    // Diagonal (companion — independent of Sigma 3)
    diagText, diagBadge, diagGo, diagSkipCode, vixPct20d, cor1m,
    oNight, o2o,
    spxGapPct, spxGapCancelsStrad, m8bfBanned,
    dayLabel: tradeWdLabel(etDate),
    dateStr: todayLong(etDate),
    cpiDay, fedDay, opexDay, postOpDay, eomDay,
    // flags each consumer may need for its own rendering
    eom1, eom2, opex1, nmDay, nmMon, isMon, isFri, isWed,
    vixExpDay, vixExpAfterOpex, nonAmznTslaEarn, earningsDay, postOpMon,
  };
}

// Convenience: attach to globalThis for any non-module loader that needs it.
// Modules prefer `import { calculateSignal } from './signal-engine.js'`.
if (typeof globalThis !== 'undefined') {
  globalThis.SignalEngine = {
    T, cpiSch, fedSch, opexSch, holidays, vixSch, earningsSchedule,
    toET, dateLong, todayLong, isWkend, isHol, isTrade,
    nextTrade, prevTrade, isTodayAfter, isTodayBefore, parseLong, schedInMonth,
    isVixAfterOpexDay, isPostOpexMon, isLastTradeMo, isEomN, isFirstTradeMo, isFirstTradeMon,
    isSecondTradingThursday, m8Sched, m8Msg, ordinal, wdName, tradeWdLabel,
    isEarningsDay, isNonAmznTslaEarningsDay, isDayAfterAnyEarnings,
    computeDiagonalSignal,
    computeVixPct20d,
    calculateSignal,
    classifyCyclePrediction, CYCLE_SHAPE_STATS, cycleAdvisoryLine,
    DAYTYPE_STATS, regimeGroup, dayTypeAdvisoryLine, m8bfWrAdvisoryLine,
    computeSkewReading,
  };
}
