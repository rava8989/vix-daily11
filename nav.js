// Shared site nav (2026-06-10) — single source for cross-page navigation.
// Every page includes:  <script src="nav.js?v=..." defer></script>
// Edit the PAGES list HERE only; never inline per-page copies (CLAUDE.md rule 2 spirit).
(function () {
  const PAGES = [
    ['index.html',                 '⌂ Dashboard'],
    ['live.html',                  '● Live'],
    ['history.html',               '☰ History'],
    ['backtester.html',            'M8BF BT'],
    ['gxbf-backtester.html',       'GXBF BT'],
    ['diagonal.html',              '◢ Diagonal'],
    ['multi-strategy-tester.html', '⊞ Multi'],
    ['cyclicality.html',           '◐ CycleLab'],
    ['gex.html',                   'Γ GEX'],
    ['earnings-play.html',         '🌙 Earnings'],
    ['magnetfly.html',             '🧲 PNBF'],
  ];
  function build() {
    const here = (location.pathname.split('/').pop() || 'index.html');
    const bar = document.createElement('nav');
    bar.id = 'siteNav';
    bar.style.cssText =
      'position:sticky;top:0;z-index:9999;display:flex;gap:4px;align-items:center;' +
      'overflow-x:auto;-webkit-overflow-scrolling:touch;padding:7px 10px;' +
      'background:#0b1220;border-bottom:1px solid #2d3f55;font:600 12.5px Inter,system-ui,sans-serif;';
    for (const [href, label] of PAGES) {
      const a = document.createElement('a');
      a.href = href;
      a.textContent = label;
      const on = here === href;
      a.style.cssText =
        'white-space:nowrap;text-decoration:none;padding:5px 11px;border-radius:7px;' +
        (on ? 'background:#6366f1;color:#fff;'
            : 'color:#94a3b8;background:#162032;border:1px solid #22324a;');
      if (!on) {
        a.onmouseenter = () => { a.style.color = '#e2e8f0'; a.style.background = '#1e293b'; };
        a.onmouseleave = () => { a.style.color = '#94a3b8'; a.style.background = '#162032'; };
      }
      bar.appendChild(a);
    }
    document.body.prepend(bar);
    // Site-wide disclaimer strip (2026-06-12, user request) — every page,
    // right under the nav.
    const disc = document.createElement('div');
    disc.textContent = '⚠️ Not financial advice. Educational & informational use only. Options trading involves substantial risk of loss and is not suitable for everyone. Past performance does not guarantee future results. Nothing here is a recommendation to buy or sell any security — you are solely responsible for your own trading decisions.';
    disc.style.cssText = 'font-size:10.5px;color:#64748b;background:rgba(100,116,139,.07);border-bottom:1px solid rgba(45,63,85,.5);padding:4px 14px;line-height:1.4;width:100%';
    bar.insertAdjacentElement('afterend', disc);
    // flex/grid bodies: span the full row like the bar itself
    const disp0 = getComputedStyle(document.body).display;
    if (disp0.includes('flex')) disc.style.flex = '0 0 100%';
    else if (disp0.includes('grid')) disc.style.gridColumn = '1 / -1';
    // Pages had their own header link rows (nav.nav) before this bar existed —
    // hide them so there's ONE navigation (titles/meta/theme buttons stay).
    // index.html has no nav.nav (sidebar layout), so the dashboard menu is safe.
    document.querySelectorAll('nav.nav').forEach(n => { n.style.display = 'none'; });
    // Flex/grid bodies (e.g. the dashboard's sidebar+content row): a prepended
    // bar becomes a narrow LEFT COLUMN and shoves the page sideways. Make the
    // bar span the full first row instead — and drop sticky there so it can't
    // overlap the page's own sticky sidebar.
    const disp = getComputedStyle(document.body).display;
    if (disp.includes('flex')) {
      document.body.style.flexWrap = 'wrap';
      bar.style.flex = '0 0 100%';
      bar.style.width = '100%';
      bar.style.position = 'static';
    } else if (disp.includes('grid')) {
      bar.style.gridColumn = '1 / -1';
      bar.style.position = 'static';
    }
  }
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', build);
  else build();
})();
