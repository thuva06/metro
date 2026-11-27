// Metro on the phone. Everything comes from the TV (/api/state, every 30 s while open; kept on the phone, so it opens
// at once and still shows the last of it while the TV is off); the remote and the actions go back to the TV.
// Windows Phone 8.1 as Metro looks on the TV: the aura, a hero with a pivot, live tiles that flip one at a time, rows
// of films, the turnstile between Start and a page, a pivot on every page (scrolled by the phone itself), the app bar.
'use strict';
const $ = (s) => document.querySelector(s);
let code = localStorage.getItem('metro-code') || '';
let S = null;           // what the TV knows
let seen = 0;           // when the TV last answered
let page = null;        // the page open: { id, at, show }
const stack = [];       // the pages under it
const tileAt = {};      // each tile's face on show

/* ---------------------------------------------------------------- talking to the TV */
async function call(p, a, ms) {
  if (AWAY) {
    // a match, a scorecard or a story: from ESPN straight when the TV is off (or doesn't answer)
    const cq = CLOUD[p] && Object.fromEntries(new URLSearchParams(a || ''));
    if (cq && Date.now() - seen > 300000) return CLOUD[p](cq);
    try { return await Relay.call(p, a, cq ? Math.min(ms || 12000, 8000) : ms); } catch (e) { if (cq) return CLOUD[p](cq); throw e; }
  }
  const ctl = new AbortController();
  const t = setTimeout(() => ctl.abort(), ms || 12000);
  try {
    const r = await fetch('/api/' + p + '?code=' + encodeURIComponent(code) + (a ? '&' + a : ''), { cache: 'no-store', signal: ctl.signal });
    return await r.json();
  } finally { clearTimeout(t); }
}
/* ---------------------------------------------------------------- away from home: through the relay */
// Served by the TV (port 8796), Metro talks to it straight. From its public address (thuva06.github.io/metro) it goes
// through a public MQTT broker instead, which only ever carries sealed messages (AES-GCM, with the key the TV made and
// gave this phone once, at home): to the TV, and to the AC's hub, so the AC answers with the TV off. The broker also
// keeps the last of what the TV knows, so the app opens with everything even when the TV is off.
const AWAY = location.port !== '8796';
const daysSaid = (d) => d === 127 ? 'every day' : d === 31 ? 'weekdays' : d === 96 ? 'weekends' : ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'].filter((_, i) => d & (1 << i)).join(' ');
const Relay = (() => {
  const BROKERS = ['wss://broker.hivemq.com:8884/mqtt', 'wss://broker.emqx.io:8084/mqtt'];
  const NAMES = ['HiveMQ', 'EMQX'];
  const te = new TextEncoder(), td = new TextDecoder();
  const rand = (n) => crypto.getRandomValues(new Uint8Array(n));
  const hex = (b) => Array.from(b, (x) => x.toString(16).padStart(2, '0')).join('');
  const cat = (...a) => { const o = new Uint8Array(a.reduce((n, x) => n + x.length, 0)); let i = 0; a.forEach((x) => { o.set(x, i); i += x.length; }); return o; };
  const str = (s) => { const b = te.encode(s); return cat(new Uint8Array([b.length >> 8, b.length & 255]), b); };
  const unb64 = (s) => Uint8Array.from(atob(s.replace(/-/g, '+').replace(/_/g, '/') + '==='.slice((s.length + 3) % 4)), (c) => c.charCodeAt(0));
  const b64 = (b) => { let s = ''; for (let i = 0; i < b.length; i += 32768) s += String.fromCharCode.apply(null, b.subarray(i, i + 32768)); return btoa(s); };
  const packet = (type, body) => { const l = []; let n = body.length; do { let d = n % 128; n = Math.floor(n / 128); if (n) d |= 128; l.push(d); } while (n); return cat(new Uint8Array([type].concat(l)), body); };
  const store = (k, v) => { try { v == null ? localStorage.removeItem(k) : localStorage.setItem(k, v); } catch (e) { } };
  const kept = (k) => { try { return localStorage.getItem(k) || ''; } catch (e) { return ''; } };
  let key = null, base = '', me = '', order = Promise.resolve();
  const waiting = new Map(), icons = new Map();
  // both brokers at once: everything is sent on each (the TV and the hub take a message once, by its one-off number),
  // so whichever the hub is on, and the faster of the two, answers
  const conns = BROKERS.map((url, n) => ({ url, n, ws: null, up: null, buf: new Uint8Array(0), ping: 0, live: false }));
  const R = { onState: null, get on() { return conns.some((c) => c.live); }, get via() { return conns.filter((c) => c.live).map((c) => NAMES[c.n]).join(' and ') || 'none'; } };

  // the key: from the pairing link (#pair=…), or pasted; kept on this phone only
  R.take = (k) => { k = String(k || '').trim().replace(/^.*pair=/, ''); if (!/^[A-Za-z0-9_-]{43}$/.test(k)) return false; store('metro-key', k); key = null; return true; };
  R.forget = () => { store('metro-key', null); store('metro-state', null); key = null; conns.forEach((c) => { try { c.ws && c.ws.close(); } catch (e) { } }); };
  R.paired = () => !!kept('metro-key') || /pair=[A-Za-z0-9_-]{43}/.test(location.hash);
  async function ready() {
    if (key) return true;
    const m = location.hash.match(/pair=([A-Za-z0-9_-]{43})/);
    if (m) { store('metro-key', m[1]); if (navigator.standalone) history.replaceState(null, '', location.pathname); }
    const k = kept('metro-key');
    if (!k) return false;
    const raw = unb64(k);
    if (raw.length !== 32) return false;
    key = await crypto.subtle.importKey('raw', raw, 'AES-GCM', false, ['encrypt', 'decrypt']);
    base = 'metro/' + hex(new Uint8Array(await crypto.subtle.digest('SHA-256', cat(te.encode('metro-topic'), raw)))).slice(0, 24);
    me = kept('metro-me') || hex(rand(6));
    store('metro-me', me);
    return true;
  }
  R.ready = ready;

  // MQTT 3.1.1 over a WebSocket: connect, subscribe to this phone's answers and the TV's kept state, ping
  function drop(c, s) { if (c.ws !== s) return; c.ws = null; c.up = null; c.live = false; clearInterval(c.ping); }
  function link(c) {
    if (c.up) return c.up;
    c.up = new Promise((ok, no) => {
      let done = false;
      const s = new WebSocket(c.url, ['mqtt']);
      s.binaryType = 'arraybuffer';
      c.buf = new Uint8Array(0);
      const give = (e) => { if (done) return; done = true; clearTimeout(t); if (e) no(e); else { c.live = true; ok(c); } };
      const t = setTimeout(() => { drop(c, s); try { s.close(); } catch (e) { } give(new Error('slow')); }, 8000);
      s.onopen = () => s.send(packet(0x10, cat(str('MQTT'), new Uint8Array([4, 2, 0, 60]), str('metro-ph-' + hex(rand(6))))));
      s.onmessage = (e) => { c.buf = cat(c.buf, new Uint8Array(e.data)); parse(c, s, give); };
      s.onclose = s.onerror = () => { drop(c, s); give(new Error('closed')); };
      c.ws = s;
    });
    c.up.catch(() => { });
    return c.up;
  }
  const connect = () => Promise.any(conns.map(link));
  function parse(c, s, give) {
    for (;;) {
      if (c.buf.length < 2) return;
      let n = 0, mul = 1, i = 1, d;
      do { if (i >= c.buf.length) return; d = c.buf[i++]; n += (d & 127) * mul; mul *= 128; } while (d & 128);
      if (c.buf.length < i + n) return;
      const type = c.buf[0], body = c.buf.slice(i, i + n);
      c.buf = c.buf.slice(i + n);
      if (type >> 4 === 2) {
        if (body[1] !== 0) { s.close(); continue; }
        s.send(packet(0x82, cat(new Uint8Array([0, 1]), str(base + '/ph/' + me), new Uint8Array([0]), str(base + '/state'), new Uint8Array([0]))));
        clearInterval(c.ping);
        c.ping = setInterval(() => { try { s.send(new Uint8Array([0xc0, 0])); } catch (e) { } }, 25000);
        give();
      } else if (type >> 4 === 3) {
        const tl = (body[0] << 8) | body[1];
        got(td.decode(body.subarray(2, 2 + tl)), body.subarray(2 + tl + ((type >> 1) & 3 ? 2 : 0)));
      }
    }
  }

  // the seal: [1, flags] + 12 random bytes + AES-GCM, bound to the topic (flag 1: gzipped inside)
  async function seal(topic, o) {
    const head = new Uint8Array([1, 0]), iv = rand(12);
    const ct = await crypto.subtle.encrypt({ name: 'AES-GCM', iv, additionalData: cat(head, te.encode(topic)) }, key, te.encode(JSON.stringify(o)));
    return cat(head, iv, new Uint8Array(ct));
  }
  async function open(topic, m) {
    if (m.length < 30 || m[0] !== 1) return null;
    const plain = new Uint8Array(await crypto.subtle.decrypt({ name: 'AES-GCM', iv: m.subarray(2, 14), additionalData: cat(m.subarray(0, 2), te.encode(topic)) }, key, m.subarray(14)));
    const text = m[1] & 1 ? await new Response(new Blob([plain]).stream().pipeThrough(new DecompressionStream('gzip'))).text() : td.decode(plain);
    return JSON.parse(text);
  }
  async function got(topic, m) {
    let j;
    try { j = await open(topic, m); } catch (e) { return; }
    if (!j) return;
    if (topic === base + '/state') { if (j.state && R.onState) R.onState(j.state, j.t); return; }
    if (Math.abs(Date.now() - j.t) > 300000) return;
    const f = waiting.get(j.i);
    if (f) { waiting.delete(j.i); f(j); }
  }
  async function ask(to, p, q, ms, b) {
    if (!(await ready())) throw new Error('unpaired');
    await connect();
    const i = hex(rand(6)), topic = base + '/' + to;
    return new Promise((ok, no) => {
      const t = setTimeout(() => { waiting.delete(i); no(new Error('no answer')); }, ms || 12000);
      waiting.set(i, (j) => { clearTimeout(t); ok(j); });
      order = order.then(async () => {
        const m = packet(0x30, cat(str(topic), await seal(topic, { t: Date.now(), n: hex(rand(8)), i, p, q: q || {}, r: base + '/ph/' + me, b })));
        let sent = 0;
        conns.forEach((c) => { if (c.live && c.ws) try { c.ws.send(m); sent++; } catch (e) { } });
        if (!sent) throw new Error('down');
      }).catch((e) => { clearTimeout(t); waiting.delete(i); no(e); });
    });
  }
  const json = (r) => r.j || JSON.parse(r.b || '{}');
  // the hub first for the AC (so it works with the TV off), the TV only if the hub isn't on the relay: never both
  // (a second press turns the AC back)
  const hub = () => ask('hub', '/health', {}, 5000).then(() => true, () => false);
  R.call = async (p, a, ms) => {
    if (!(await ready())) return { ok: false, need: 'pair' };
    const q = Object.fromEntries(new URLSearchParams(a || ''));
    if (p === 'ac' && await hub()) return json(await ask('hub', '/press', q, 40000));
    if (p === 'schedules' && await hub()) {
      const j = json(await ask('hub', '/schedules', q, 10000));
      j.ok = true;
      (j.items || []).forEach((it) => (it.said = it.said || daysSaid(it.days)));
      return j;
    }
    return json(await ask('tv', '/api/' + p, q, ms));
  };
  R.photo = async (blob) => json(await ask('tv', '/api/photo', {}, 60000, b64(new Uint8Array(await blob.arrayBuffer()))));
  R.icon = (pkg) => {
    if (!icons.has(pkg)) icons.set(pkg, ask('tv', '/api/icon', { pkg }, 15000).then((r) => (r.x ? 'data:image/png;base64,' + r.x : ''), () => { icons.delete(pkg); return ''; }));
    return icons.get(pkg);
  };
  // Segoe, as on the TV: it isn't on the public page (it's Microsoft's), so the TV sends it, sealed, once; then it's kept
  R.fonts = async () => {
    let box = null;
    try { box = await caches.open('metro-fonts'); } catch (e) { }
    await Promise.all([['light', 300], ['semilight', 350], ['regular', 400], ['semibold', 600]].map(async ([n, w]) => {
      try {
        let bytes = null;
        const hit = box && await box.match('/font/' + n);
        if (hit) bytes = await hit.arrayBuffer();
        else {
          const r = await ask('tv', '/font/' + n + '.ttf', {}, 30000);
          if (!r.x) return;
          bytes = unb64(r.x.replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '')).buffer;
          if (box) await box.put('/font/' + n, new Response(bytes, { headers: { 'Content-Type': 'font/ttf' } }));
        }
        const f = new FontFace('Segoe', bytes, { weight: String(w) });
        await f.load();
        document.fonts.add(f);
      } catch (e) { }
    }));
  };
  R.wake = () => { if (key) conns.forEach((c) => { if (!c.up) link(c); }); };
  return R;
})();
async function act(p, a, quiet) {
  try {
    const j = await call(p, a);
    if (j.need) return gate();
    if (!quiet || !j.ok) toast(j.line || (j.ok ? 'Done' : "That didn't work"));
    return j;
  } catch (e) { toast(AWAY ? (Relay.on ? "The TV didn't answer: it may be off" : "The relay can't be reached: is this phone online?") : S ? 'The TV is off or away from the Wi-Fi' : "The TV didn't answer"); offline(true); }
}
const q = (k, v) => k + '=' + encodeURIComponent(v);
function toast(t) {
  if (!t) return;
  const m = $('#toast'); m.textContent = t; m.classList.add('on');
  clearTimeout(m.t); m.t = setTimeout(() => m.classList.remove('on'), 2800);
}
function offline(on) {
  const o = $('#offline');
  o.hidden = !on;
  const live = AWAY && (cloudAt || sportsAt);
  if (on) o.textContent = (AWAY && !Relay.on ? 'Offline' : 'The TV is off') + (live ? ' · weather, scores, news and money live; the rest' : ' · showing what it said') +
    (seen ? (live ? ' from ' : ' at ') + (Date.now() - seen > 864e5 ? dayOf(seen) + ' ' : '') + hm(seen) : ' last');
  else { cloudAt = 0; sportsAt = 0; }
}
async function load() {
  try {
    const j = await call('state', '', 20000);
    if (j.need) return gate();
    const sig = sigOf(j), same = sig === lastSig;
    lastSig = sig;
    S = j; seen = Date.now();
    try { localStorage.setItem('metro-state', JSON.stringify(j)); localStorage.setItem('metro-seen', String(seen)); } catch (e) { }
    offline(false);
    applyAccent();
    if (same) return;
    if (page) { if (!['remote', 'title', 'photos', 'films', 'settings', 'money', 'timer', 'apps'].includes(page.id) && pivot) pivot.panels().forEach((_, i) => redrawPanel(i)); }
    else drawStart();
  } catch (e) { offline(true); if (!S) $('#startBody').innerHTML = '<p class="sub" style="margin-top:40px">' + (AWAY ? 'The TV didn\'t answer through the relay, and nothing is kept from it yet. Is it on?' : 'The TV didn\'t answer. Is it on, and this phone on the home Wi-Fi?') + '</p>'; }
}
// what a redraw depends on (not the clock, which ticks on its own)
let lastSig = '';
function sigOf(j) { const c = Object.assign({}, j); delete c.now; delete c.clocks; delete c.timer; delete c.net; return JSON.stringify(c); }
const applyAccent = () => document.documentElement.style.setProperty('--accent', (S && S.accent) || '#1ba1e2');
function gate() {
  if (AWAY) return pairGate();
  $('#gate').hidden = false; call('hello').catch(() => {}); setTimeout(() => $('#code').focus(), 300);
}
$('#codego').onclick = async () => {
  code = $('#code').value.trim();
  const j = await call('hello').catch(() => ({}));
  if (j.ok) { localStorage.setItem('metro-code', code); $('#gate').hidden = true; load(); }
  else toast('Not that one: look at the TV');
};
// away from home, unpaired: the key from Metro at home (its settings › anywhere opens this page with it, and copies it)
function pairGate() {
  const g = $('#gate');
  g.hidden = false;
  g.innerHTML = '<div class="kicker">METRO · ANYWHERE</div><h1 class="h1">pair</h1><p class="dim">At home, open Metro from the TV (on the Wi-Fi), then settings › anywhere › ' +
    'use Metro anywhere. It opens this page already paired, and copies the key: if this app asks, paste it here.</p>' +
    '<div class="row"><input id="key" placeholder="the key" autocomplete="off" autocapitalize="off" autocorrect="off" spellcheck="false"><button class="go" id="keygo">OK</button></div>' +
    (navigator.clipboard && navigator.clipboard.readText ? '<button class="btn" id="paste" style="margin-top:18px">' + ico('copy') + 'paste the key</button>' : '');
  const go = async (k) => {
    if (!Relay.take(k)) return toast("That isn't Metro's key");
    g.hidden = true;
    toast('Paired · asking the TV…');
    boot();
  };
  $('#keygo').onclick = () => go($('#key').value);
  const p = $('#paste');
  if (p) p.onclick = async () => { try { const t = await navigator.clipboard.readText(); $('#key').value = t; go(t); } catch (e) { toast('Paste it into the box'); } };
}
// the relay's last copy of what the TV knows: drawn at once if newer than what this phone kept
if (AWAY) Relay.onState = (st, t) => {
  if (t <= seen && S) return;
  const fresh = !S;
  S = st; seen = t;
  try { localStorage.setItem('metro-state', JSON.stringify(st)); localStorage.setItem('metro-seen', String(t)); } catch (e) { }
  applyAccent();
  if (Date.now() - t > 120000) offline(true);
  if (fresh || !page) drawStart();
};

/* ---------------------------------------------------------------- small helpers */
const esc = (s) => String(s == null ? '' : s).replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
const TZ = { timeZone: 'Asia/Dubai' };
const hm = (ms) => new Date(ms).toLocaleTimeString('en-GB', { hour: '2-digit', minute: '2-digit', ...TZ });
const dayOf = (ms) => new Date(ms).toLocaleDateString('en-GB', { weekday: 'short', day: 'numeric', month: 'short', ...TZ });
const n0 = (v) => Math.round(v).toLocaleString('en-GB');
const n2 = (v) => v.toLocaleString('en-GB', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
function when(ms) {
  const m = Math.round((ms - Date.now()) / 60000);
  if (m < 0) return 'now';
  if (m < 60) return 'in ' + m + ' min';
  if (m < 24 * 60) return 'in ' + Math.floor(m / 60) + ' h ' + (m % 60) + ' min';
  return dayOf(ms) + ' ' + hm(ms);
}
function ago(ms) { const m = Math.round((Date.now() - ms) / 60000); return m < 1 ? 'just now' : m < 60 ? m + ' min ago' : m < 1440 ? Math.round(m / 60) + ' h ago' : Math.round(m / 1440) + ' d ago'; }
const crest = (url, cls) => url ? '<img class="' + (cls || 'crest') + '" src="' + esc(url) + '" alt="" loading="lazy">' : '';
const bg = (url) => url ? "background-image:url('" + esc(url) + "')" : '';
function spark(arr) {
  if (!arr || arr.length < 2) return '';
  const lo = Math.min(...arr), hi = Math.max(...arr), span = hi - lo || 1;
  const pts = arr.map((v, i) => (i / (arr.length - 1) * 300).toFixed(1) + ',' + (64 - (v - lo) / span * 56).toFixed(1)).join(' ');
  return '<svg class="spark" viewBox="0 0 300 70" preserveAspectRatio="none"><polyline points="' + pts + '" stroke-width="2.5" vector-effect="non-scaling-stroke"/></svg>';
}
function press(el) {
  el.addEventListener('touchstart', () => el.classList.add('press'), { passive: true });
  ['touchend', 'touchcancel', 'touchmove'].forEach((e) => el.addEventListener(e, () => el.classList.remove('press'), { passive: true }));
}

/* ---------------------------------------------------------------- glyphs (Lucide, ISC) */
const I = {
  sun: '<circle cx="12" cy="12" r="4"/><path d="M12 2v2M12 20v2M4.93 4.93l1.41 1.41M17.66 17.66l1.41 1.41M2 12h2M20 12h2M6.34 17.66l-1.41 1.41M19.07 4.93l-1.41 1.41"/>',
  moon: '<path d="M12 3a6 6 0 0 0 9 9 9 9 0 1 1-9-9Z"/>',
  cloud: '<path d="M17.5 19H9a7 7 0 1 1 6.71-9h1.79a4.5 4.5 0 1 1 0 9Z"/>',
  partly: '<path d="M12 2v2M4.93 4.93l1.41 1.41M20 12h2M19.07 4.93l-1.41 1.41M15.95 12.65a4 4 0 0 0-5.93-4.13"/><path d="M13 22H7a5 5 0 1 1 4.9-6H13a3 3 0 0 1 0 6Z"/>',
  rain: '<path d="M4 14.9A7 7 0 1 1 15.71 8h1.79a4.5 4.5 0 0 1 2.5 8.24"/><path d="M16 14v6M8 14v6M12 16v6"/>',
  storm: '<path d="M6 16.33A7 7 0 1 1 15.71 8h1.79a4.5 4.5 0 0 1 .5 8.97"/><path d="m13 12-3 5h4l-3 5"/>',
  fog: '<path d="M4 14.9A7 7 0 1 1 15.71 8h1.79a4.5 4.5 0 0 1 2.5 8.24"/><path d="M16 17H7M17 21H9"/>',
  snow: '<path d="M2 12h20M12 2v20M20 16l-4-4 4-4M4 8l4 4-4 4M16 4l-4 4-4-4M8 20l4-4 4 4"/>',
  back: '<path d="m12 19-7-7 7-7M19 12H5"/>',
  refresh: '<path d="M3 12a9 9 0 0 1 9-9 9.75 9.75 0 0 1 6.74 2.74L21 8"/><path d="M21 3v5h-5"/><path d="M21 12a9 9 0 0 1-9 9 9.75 9.75 0 0 1-6.74-2.74L3 16"/><path d="M8 16H3v5"/>',
  tv: '<rect width="20" height="15" x="2" y="7" rx="2"/><path d="m17 2-5 5-5-5"/>',
  power: '<path d="M12 2v10"/><path d="M18.4 6.6a9 9 0 1 1-12.77.04"/>',
  pad: '<rect x="2" y="6" width="20" height="12" rx="2"/><path d="M6 12h4M8 10v4M15 13h.01M18 11h.01"/>',
  home: '<path d="m3 9 9-7 9 7v11a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2z"/><path d="M9 22V12h6v10"/>',
  play: '<path d="M6 3l14 9-14 9z"/>',
  stop: '<rect x="5" y="5" width="14" height="14"/>',
  rew: '<path d="M11 19 2 12l9-7zM22 19l-9-7 9-7z"/>',
  ff: '<path d="m13 19 9-7-9-7zM2 19l9-7-9-7z"/>',
  volup: '<path d="M11 5 6 9H2v6h4l5 4zM15.54 8.46a5 5 0 0 1 0 7.07M19.07 4.93a10 10 0 0 1 0 14.14"/>',
  voldown: '<path d="M11 5 6 9H2v6h4l5 4zM15.54 8.46a5 5 0 0 1 0 7.07"/>',
  mute: '<path d="M11 5 6 9H2v6h4l5 4zM22 9l-6 6M16 9l6 6"/>',
  up: '<path d="m18 15-6-6-6 6"/>', down: '<path d="m6 9 6 6 6-6"/>', left: '<path d="m15 18-6-6 6-6"/>', right: '<path d="m9 18 6-6-6-6"/>',
  search: '<circle cx="11" cy="11" r="8"/><path d="m21 21-4.3-4.3"/>',
  sliders: '<path d="M4 21v-7M4 10V3M12 21v-9M12 8V3M20 21v-5M20 12V3M2 14h4M10 8h4M18 16h4"/>',
  yt: '<path d="M2.5 17a24 24 0 0 1 0-10 2 2 0 0 1 1.4-1.4 49.6 49.6 0 0 1 16.2 0A2 2 0 0 1 21.5 7a24 24 0 0 1 0 10 2 2 0 0 1-1.4 1.4 49.6 49.6 0 0 1-16.2 0A2 2 0 0 1 2.5 17"/><path d="m10 15 5-3-5-3z"/>',
  chat: '<path d="M7.9 20A9 9 0 1 0 4 16.1L2 22z"/>',
  hdmi: '<rect width="20" height="14" x="2" y="3" rx="2"/><path d="M8 21h8M12 17v4"/>',
  bell: '<path d="M6 8a6 6 0 0 1 12 0c0 7 3 9 3 9H3s3-2 3-9M10.3 21a1.94 1.94 0 0 0 3.4 0"/>',
  trophy: '<path d="M6 9H4.5a2.5 2.5 0 0 1 0-5H6M18 9h1.5a2.5 2.5 0 0 0 0-5H18M4 22h16M10 14.66V17c0 .55-.47.98-.97 1.21C7.85 18.75 7 20.24 7 22M14 14.66V17c0 .55.47.98.97 1.21C16.15 18.75 17 20.24 17 22M18 2H6v7a6 6 0 0 0 12 0V2Z"/>',
  moonstar: '<path d="M12 3a6 6 0 0 0 9 9 9 9 0 1 1-9-9ZM20 3v4M22 5h-4"/>',
  keyboard: '<rect width="20" height="16" x="2" y="4" rx="2"/><path d="M6 8h.01M10 8h.01M14 8h.01M18 8h.01M8 12h.01M12 12h.01M16 12h.01M7 16h10"/>',
  music: '<path d="M9 18V5l12-2v13"/><circle cx="6" cy="18" r="3"/><circle cx="18" cy="16" r="3"/>',
  radio: '<path d="M4.9 19.1C1 15.2 1 8.8 4.9 4.9M7.8 16.2c-2.3-2.3-2.3-6.1 0-8.5M16.2 7.8c2.3 2.3 2.3 6.1 0 8.5M19.1 4.9C23 8.8 23 15.1 19.1 19"/><circle cx="12" cy="12" r="2"/>',
  image: '<rect width="18" height="18" x="3" y="3" rx="2"/><circle cx="9" cy="9" r="2"/><path d="m21 15-3.09-3.09a2 2 0 0 0-2.82 0L6 21"/>',
  film: '<rect width="18" height="18" x="3" y="3" rx="2"/><path d="M7 3v18M3 7.5h4M3 12h18M3 16.5h4M17 3v18M17 7.5h4M17 16.5h4"/>',
  gear: '<path d="M12.22 2h-.44a2 2 0 0 0-2 2v.18a2 2 0 0 1-1 1.73l-.43.25a2 2 0 0 1-2 0l-.15-.08a2 2 0 0 0-2.73.73l-.22.38a2 2 0 0 0 .73 2.73l.15.1a2 2 0 0 1 1 1.72v.51a2 2 0 0 1-1 1.74l-.15.09a2 2 0 0 0-.73 2.73l.22.38a2 2 0 0 0 2.73.73l.15-.08a2 2 0 0 1 2 0l.43.25a2 2 0 0 1 1 1.73V20a2 2 0 0 0 2 2h.44a2 2 0 0 0 2-2v-.18a2 2 0 0 1 1-1.73l.43-.25a2 2 0 0 1 2 0l.15.08a2 2 0 0 0 2.73-.73l.22-.39a2 2 0 0 0-.73-2.73l-.15-.08a2 2 0 0 1-1-1.74v-.5a2 2 0 0 1 1-1.74l.15-.09a2 2 0 0 0 .73-2.73l-.22-.38a2 2 0 0 0-2.73-.73l-.15.08a2 2 0 0 1-2 0l-.43-.25a2 2 0 0 1-1-1.73V4a2 2 0 0 0-2-2z"/><circle cx="12" cy="12" r="3"/>',
  grid: '<rect width="7" height="7" x="3" y="3"/><rect width="7" height="7" x="14" y="3"/><rect width="7" height="7" x="14" y="14"/><rect width="7" height="7" x="3" y="14"/>',
  clock: '<circle cx="12" cy="12" r="10"/><path d="M12 6v6l4 2"/>',
  coins: '<circle cx="8" cy="8" r="6"/><path d="M18.09 10.37A6 6 0 1 1 10.34 18M7 6h1v4M16.71 13.88l.7.71-2.82 2.82"/>',
  globe: '<circle cx="12" cy="12" r="10"/><path d="M2 12h20M12 2a15 15 0 0 1 0 20M12 2a15 15 0 0 0 0 20"/>',
  copy: '<rect x="9" y="9" width="12" height="12" rx="1"/><path d="M5 15H4a1 1 0 0 1-1-1V4a1 1 0 0 1 1-1h10a1 1 0 0 1 1 1v1"/>',
  key: '<circle cx="7.5" cy="15.5" r="4.5"/><path d="m10.7 12.3 9.8-9.8M16 7l3 3M14 9l2 2"/>',
};
const ico = (n, size) => '<svg viewBox="0 0 24 24"' + (size ? ' style="width:' + size + 'px;height:' + size + 'px"' : '') + '>' + (I[n] || '') + '</svg>';
function skyIcon(sky, day) {
  switch (sky) {
    case 'Clear': return day === false ? 'moon' : 'sun';
    case 'PartlyCloudy': return day === false ? 'cloud' : 'partly';
    case 'Cloudy': return 'cloud';
    case 'Fog': return 'fog';
    case 'Storm': return 'storm';
    case 'Snow': return 'snow';
    default: return 'rain';
  }
}

/* ---------------------------------------------------------------- Windows Phone's accents, as on the TV */
const C = { cyan: '#1ba1e2', cobalt: '#0050ef', red: '#b3121b', crimson: '#a20025', steel: '#647687', navy: '#0b4a9e', teal: '#00aba9',
  amber: '#f0a30a', mauve: '#76608a', magenta: '#d80073', graphite: '#333333', orange: '#fa6800', emerald: '#008a00', violet: '#aa00ff', ytred: '#cc0000', indigo: '#6a00ff' };
const SVC = { Netflix: '#e50914', 'Prime Video': '#1399ff', Shahid: '#00a651' };
const svcTag = (t) => t.svc ? '<span class="svc" style="--sc:' + (SVC[t.svc] || t.svcColor || '#444') + '">' + esc(t.svc) + '</span>' : '';
const films = () => (S && S.films) || {};

/* ---------------------------------------------------------------- Start */
function greeting() {
  const h = +new Date().toLocaleString('en-GB', { hour: '2-digit', hour12: false, ...TZ });
  return h < 5 ? 'good night' : h < 12 ? 'good morning' : h < 17 ? 'good afternoon' : h < 22 ? 'good evening' : 'good night';
}
function drawStatus() {
  const w = S && S.weather;
  $('#status').innerHTML = (w ? ico(skyIcon(w.sky, w.day)) + '<span>' + w.temp + '°</span>' : '') + '<span class="t">' + hm(Date.now()) + '</span>';
}

// the hero's slides: the film at No. 1, Bayern's match, the cricket, the weather, the rupee; the phone scrolls them
function slides() {
  const L = [];
  const f = films();
  const top = (f.netflix || [])[0] || (f.trending || [])[0];
  if (top) L.push({ name: (top.svc || 'films').toLowerCase(), open: () => openTitle(top), html: '<div class="hcard" style="' + bg(top.backdrop) + '"><div class="words"><div class="k">' +
    (top.rank ? 'No. ' + top.rank + ' film on Netflix this week · UAE' : 'trending on your services') + '</div><div class="ti">' + esc(top.title) + '</div><div class="ln">' +
    esc([top.year, (top.genres || []).slice(0, 2).join(' · ')].filter(Boolean).join(' · ')) + '</div></div></div>' });
  const b = S.bayern, g = b && (b.live || b.next);
  if (g) L.push({ name: 'bayern', open: () => openGame(g), html: '<div class="hcard flat" style="--c:' + C.red + '"><div class="body"><div class="vsbig">' + crest(g.home.logo, '') +
    '<div class="sc">' + esc(g.state === 'pre' ? hm(g.start) : g.home.score + '–' + g.away.score) + '</div>' + crest(g.away.logo, '') + '</div></div><div class="words"><div class="k">' +
    esc((g.state === 'in' ? 'live · ' : 'next · ') + g.league) + '</div><div class="ti" style="font-size:28px">' + esc(g.home.name + ' v ' + g.away.name) + '</div><div class="ln">' +
    esc(g.state === 'in' ? g.status : when(g.start)) + '</div></div></div>' });
  const ck = (S.cricket || []).find((x) => x.state === 'in') || (S.cricket || []).find((x) => x.state === 'pre');
  if (ck) L.push({ name: 'cricket', open: () => openGame(ck), html: '<div class="hcard flat" style="--c:' + C.steel + '"><div class="body" style="flex-direction:column;align-items:stretch">' +
    [ck.home, ck.away].map((s) => '<div class="cside">' + (s.logo ? '<img src="' + esc(s.logo) + '" alt="">' : '') + '<span class="nm">' + esc(s.name) +
    '</span><span class="sc">' + esc((s.score || '–').split('(')[0].trim()) + (s.score && s.score.includes('(') ? '<small>' + esc(s.score.split('(')[1].replace(')', '')) + '</small>' : '') + '</span></div>').join('') + '</div><div class="words"><div class="k">' + esc((ck.state === 'in' ? 'live · ' : 'next · ') + ck.league) +
    '</div><div class="ln">' + esc(ck.state === 'pre' ? when(ck.start) : (ck.note || ck.status)) + '</div></div></div>' });
  const w = S.weather;
  if (w) L.push({ name: 'weather', open: () => open('weather'), html: '<div class="hcard flat" style="--c:' + (w.day === false ? C.cobalt : C.cyan) + '"><div class="body" style="justify-content:flex-start;gap:16px">' +
    '<div class="big" style="font-size:96px">' + w.temp + '°</div>' + ico(skyIcon(w.sky, w.day), 64) + '</div><div class="words"><div class="k">' + esc(w.city) + '</div><div class="ti" style="font-size:28px">' +
    esc(w.condition.toLowerCase()) + '</div><div class="ln">' + w.high + '° / ' + w.low + '° · humidity ' + w.humidity + '%</div></div></div>' });
  const m = S.money;
  if (m) L.push({ name: 'rupees', open: () => open('money'), html: '<div class="hcard flat" style="--c:' + C.teal + '"><div class="body" style="flex-direction:column;align-items:flex-start;justify-content:center">' +
    '<div class="sub" style="color:#fff">AED 1,000 in Sri Lanka</div><div class="big" style="font-size:64px">' + n0(m.lkr * 1000) + '</div><div class="sub" style="color:#fff;margin-top:8px">in India ' +
    n0(m.inr * 1000) + '</div></div><div class="words"><div class="k">rupees</div>' + (m.gold ? '<div class="ln">gold 22K · AED ' + n2(m.gold.k22) + ' a gram</div>' : '') + '</div></div>' });
  return L;
}
/* ---------------------------------------------------------------- Windows Phone's pivot */
// Items side by side on a track the finger moves, round and round. The page still scrolls up and down by itself
// (touch-action: pan-y), so only a sideways drag is the pivot's: the track follows the finger, the names over it
// follow the track, and on release it glides on to the next item (or back) on Windows Phone's curve.
const GLIDE = 'transform .46s cubic-bezier(.1,.9,.2,1)';
class Pivot {
  constructor(box, html, o) {
    this.box = box; this.n = html.length; this.loop = this.n > 1; this.o = o || {};
    const copy = (h) => '<section class="panel copy" aria-hidden="true">' + h.replace(/ id="/g, ' data-cid="') + '</section>';
    box.innerHTML = '<div class="track">' + (this.loop ? copy(html[this.n - 1]) : '') + html.map((h) => '<section class="panel">' + h + '</section>').join('') + (this.loop ? copy(html[0]) : '') + '</div>';
    this.track = box.firstChild;
    this.at = Math.max(0, Math.min(this.o.at || 0, this.n - 1));
    this.pos = this.at + (this.loop ? 1 : 0);
    this.draw(this.pos, false);
    if (!box.pv) {
      box.addEventListener('pointerdown', (e) => box.pv && box.pv.down(e));
      box.addEventListener('pointermove', (e) => box.pv && box.pv.move(e));
      box.addEventListener('pointerup', (e) => box.pv && box.pv.up(e));
      box.addEventListener('pointercancel', () => box.pv && box.pv.cancel());
    }
    box.pv = this;
  }
  get w() { return this.box.clientWidth || innerWidth; }
  draw(p, glide) {
    this.track.style.transition = glide ? GLIDE : 'none';
    this.track.style.transform = 'translate3d(' + (-p * this.w) + 'px,0,0)';
    if (this.o.onMove) this.o.onMove(p - (this.loop ? 1 : 0), glide);
  }
  down(e) {
    if (this.n < 2 || (e.pointerType === 'mouse' && e.button) || e.target.closest('input,select,textarea,.pad,.keys,.touch,.row-x')) return;
    this.s = { x: e.clientX, y: e.clientY, t: performance.now(), id: e.pointerId }; this.mode = null;
  }
  move(e) {
    const s = this.s;
    if (!s || e.pointerId !== s.id) return;
    const dx = e.clientX - s.x, dy = e.clientY - s.y;
    if (!this.mode) {
      if (Math.abs(dx) > 9 && Math.abs(dx) > Math.abs(dy) * 1.15) { this.mode = 'x'; try { this.box.setPointerCapture(s.id); } catch (_) { } if (this.o.onGrab) this.o.onGrab(); }
      else if (Math.abs(dy) > 9) { this.s = null; return; }
    }
    if (this.mode === 'x') {
      // at the ends of a list that doesn't go round, it gives a little and no more
      let p = this.pos - dx / this.w;
      if (!this.loop) p = p < 0 ? p / 3 : p > this.n - 1 ? this.n - 1 + (p - this.n + 1) / 3 : p;
      cancelAnimationFrame(this.raf); this.raf = requestAnimationFrame(() => this.draw(p, false));
    }
  }
  up(e) {
    const s = this.s; this.s = null;
    if (!s || this.mode !== 'x') return;
    cancelAnimationFrame(this.raf);
    const dx = e.clientX - s.x, v = dx / Math.max(1, performance.now() - s.t);
    this.go(Math.abs(dx) > this.w * 0.2 || Math.abs(v) > 0.3 ? (dx < 0 ? 1 : -1) : 0);
    // the lift that ends a drag isn't a tap on what is under it
    const stop = (ev) => { ev.stopPropagation(); ev.preventDefault(); };
    addEventListener('click', stop, { capture: true, once: true });
    setTimeout(() => removeEventListener('click', stop, { capture: true }), 60);
  }
  cancel() { if (this.mode === 'x') this.go(0); this.s = null; }
  go(by) {
    let p = this.pos + by;
    if (!this.loop) p = Math.max(0, Math.min(this.n - 1, p));
    this.pos = p;
    this.draw(p, true);
    const at = ((p - (this.loop ? 1 : 0)) % this.n + this.n) % this.n;
    if (this.o.onAt) this.o.onAt(at);
    clearTimeout(this.done);
    this.done = setTimeout(() => {
      // on a copy: straight on to the item itself
      if (this.loop && this.pos === 0) { this.pos = this.n; this.draw(this.pos, false); }
      else if (this.loop && this.pos === this.n + 1) { this.pos = 1; this.draw(this.pos, false); }
      if (at !== this.at) { this.at = at; if (this.o.onChange) this.o.onChange(at); }
    }, 470);
  }
  // to item i the short way round
  to(i) { let d = i - this.at; if (this.loop && Math.abs(d) > this.n / 2) d -= Math.sign(d) * this.n; this.go(d); }
  panel(i) { return this.track.querySelectorAll('.panel:not(.copy)')[i]; }
  panels() { return [...this.track.querySelectorAll('.panel:not(.copy)')]; }
}
addEventListener('resize', () => document.querySelectorAll('.pv').forEach((b) => b.pv && b.pv.draw(b.pv.pos, false)));

// a line of names over a pivot: three times over, so it runs on round the ends; the current one white
function strip(el, list, loop) {
  el.innerHTML = (loop ? [0, 1, 2] : [0]).map(() => list.map((t, i) => '<span data-at="' + i + '">' + esc(t) + '</span>').join('')).join('');
  return [...el.querySelectorAll('span')];
}
function stripMove(el, spans, n, loop, f, glide) {
  if (!spans.length) return;
  const g = f + (loop ? n : 0), a = Math.floor(g), k = g - a;
  const A = spans[Math.max(0, Math.min(spans.length - 1, a))], B = spans[Math.max(0, Math.min(spans.length - 1, a + 1))];
  el.style.transition = glide ? GLIDE : 'none';
  el.style.transform = 'translate3d(' + (-(A.offsetLeft + (B.offsetLeft - A.offsetLeft) * k)) + 'px,0,0)';
}
function stripOn(spans, n, loop, at) { spans.forEach((s, j) => s.classList.toggle('on', j === (loop ? n : 0) + at)); }

let heroL = [], heroPv = null, heroSpans = [], heroAt = 0, heroGrab = 0;
const HERO_MS = 7000;
function drawHero() {
  heroL = slides();
  const el = $('#hero');
  if (!el) return;
  if (!heroL.length) { el.innerHTML = ''; return; }
  const n = heroL.length;
  el.innerHTML = '<div class="hpivot"><div class="hstrip" id="hstrip"></div></div><div class="pv hpv" id="hpv"></div>';
  heroSpans = strip($('#hstrip'), heroL.map((x) => x.name), n > 1);
  heroAt = Math.min(heroAt, n - 1);
  heroPv = new Pivot($('#hpv'), heroL.map((x) => x.html), {
    at: heroAt,
    onMove: (f, glide) => stripMove($('#hstrip'), heroSpans, n, n > 1, f, glide),
    onAt: (i) => { heroAt = i; stripOn(heroSpans, n, n > 1, i); heroTime(); },
    onGrab: () => { heroGrab = Date.now(); },
  });
  stripOn(heroSpans, n, n > 1, heroAt);
  // a name goes straight to its slide (the short way round)
  heroSpans.forEach((sp) => (sp.onclick = () => { heroGrab = Date.now(); heroPv.to(+sp.dataset.at); }));
  heroPv.panels().concat([...$('#hpv').querySelectorAll('.copy')]).forEach((p) => {
    const c = p.querySelector('.hcard');
    if (!c) return;
    press(c);
    c.onclick = () => heroL[heroPv.at].open();
  });
  heroTime();
}
// the line under the current name fills over the slide's time (drawn by the phone, not ticked)
function heroTime() {
  const on = document.querySelector('#hstrip span.on');
  document.querySelectorAll('#hstrip span').forEach((s) => s.classList.remove('run'));
  if (on) { void on.offsetWidth; on.classList.add('run'); }
  clearTimeout(heroTime.t);
  heroTime.t = setTimeout(() => {
    if (page || document.hidden || !heroPv || Date.now() - heroGrab < 4000) return heroTime();
    heroPv.go(1);
  }, HERO_MS);
}
function teamFaces(t, small) {
  const f = [];
  const g = t.live || t.next;
  if (g && small) f.push('<div class="t-k ellip">' + esc((t.live ? 'live · ' : 'next · ') + g.league) + '</div><div class="vs small" style="margin-top:10px">' + crest(g.home.logo, '') +
    '<div class="t-mid" style="font-size:24px">' + esc(t.live ? g.home.score + '–' + g.away.score : hm(g.start)) + '</div>' + crest(g.away.logo, '') + '</div><div class="t-line t-soft ellip" style="margin-top:8px">' +
    esc((g.home.short || g.home.name) + ' v ' + (g.away.short || g.away.name)) + '</div><div class="t-line t-soft ellip">' + esc(t.live ? g.status : dayOf(g.start)) + '</div>');
  else if (g) f.push('<div class="t-k">' + esc(t.live ? 'live · ' + g.league : 'next · ' + g.league) + '</div><div class="vs" style="margin-top:10px">' + crest(g.home.logo, '') +
    '<div class="t-mid">' + esc(t.live ? g.home.score + '–' + g.away.score : hm(g.start)) + '</div>' + crest(g.away.logo, '') + '</div><div class="t-line t-soft" style="margin-top:8px">' +
    esc(g.home.name + ' v ' + g.away.name) + '</div><div class="t-line t-soft">' + esc(t.live ? g.status : when(g.start)) + '</div>');
  const r = (t.results || [])[0];
  if (r) f.push('<div class="t-k ellip">last · ' + esc(r.league) + '</div><div class="t-mid" style="margin-top:8px' + (small ? ';font-size:20px' : '') + '">' + esc((small ? (r.home.short || r.home.name) : r.home.name) + ' ' + r.home.score + '–' + r.away.score + ' ' + (small ? (r.away.short || r.away.name) : r.away.name)) +
    '</div><div style="margin-top:10px">' + (t.results || []).slice(0, 5).map((x) => '<span class="sq ' + x.outcome + '">' + x.outcome + '</span>').join('') + '</div>');
  const us = t.table && t.table.rows.find((x) => x.us);
  if (us) f.push('<div class="t-k">' + esc(t.table.name) + '</div><div class="t-big" style="margin-top:6px">' + us.rank + '</div><div class="t-line t-soft">' + us.points + ' points · ' + us.played + ' played</div>');
  return f.length ? f : ['<div class="t-mid">' + esc(t.name) + '</div>'];
}
function tiles() {
  const T = [];
  const w = S.weather;
  if (w) T.push({ id: 'weather', size: 'w', c: w.day === false ? C.cobalt : C.cyan, label: 'Weather · ' + w.city, faces: [
    '<div style="display:flex;align-items:center;gap:14px"><div class="t-big" style="font-size:72px">' + w.temp + '°</div><div>' + ico(skyIcon(w.sky, w.day), 40) +
      '<div class="t-mid" style="font-size:22px;margin-top:4px">' + esc(w.condition) + '</div><div class="t-line t-soft">' + w.high + '° / ' + w.low + '°</div></div></div>',
    '<div class="t-k">the next hours</div><div class="cols">' + w.hours.filter((_, i) => i % 2 === 1).slice(0, 6).map((h) => '<div>' + esc(h.label) + ico(skyIcon(h.sky, h.day)) + h.temp + '°</div>').join('') + '</div>',
    '<div class="t-k">this week</div><div class="cols">' + w.days.slice(1, 6).map((d) => '<div>' + esc(d.day) + ico(skyIcon(d.sky, true)) + d.high + '° <span class="t-soft">' + d.low + '°</span></div>').join('') + '</div>',
  ] });
  T.push({ id: 'remote', size: 'm', c: S.accent, label: 'Remote', faces: ['<div style="display:flex;height:100%;align-items:center;justify-content:center">' + ico('pad', 64) + '</div>'] });
  const ac = S.ac;
  T.push({ id: 'ac', size: 'm', c: C.steel, label: 'Air con', faces: ['<div style="margin-top:4px">' + ico('power', 40) + '</div><div class="t-mid" style="margin-top:14px">press</div><div class="t-line t-soft">' +
    (ac && ac.ok ? 'last ' + hm(ac.at) : 'on or off') + '</div>'] });
  if (S.bayern) T.push({ id: 'bayern', size: 'w', c: C.red, label: 'FC Bayern', faces: teamFaces(S.bayern) });
  const ck = S.cricket || [];
  const cf = [ck.find((g) => g.state === 'in'), ck.find((g) => g.state === 'pre'), [...ck].reverse().find((g) => g.state === 'post')].filter(Boolean).map((g) =>
    '<div class="t-k">' + (g.state === 'in' ? 'live' : g.state === 'pre' ? 'next' : 'result') + '</div><div class="t-line" style="margin-top:6px;font-weight:600">' + esc(g.home.short || g.home.name) + ' ' + esc(g.home.score) +
    '</div><div class="t-line" style="font-weight:600">' + esc(g.away.short || g.away.name) + ' ' + esc(g.away.score) + '</div><div class="t-line t-soft" style="margin-top:4px">' + esc(g.state === 'pre' ? when(g.start) : (g.note || g.status)) + '</div>');
  if (cf.length) T.push({ id: 'cricket', size: 'm', c: C.steel, label: 'Cricket', faces: cf });
  const now = new Date(S.now);
  const ev = (S.calendar || [])[0];
  T.push({ id: 'calendar', size: 'm', c: C.navy, label: 'Calendar', faces: ['<div class="t-big" style="font-size:66px">' + now.toLocaleDateString('en-GB', { day: 'numeric', ...TZ }) +
    '</div><div class="t-mid" style="font-size:24px">' + now.toLocaleDateString('en-GB', { weekday: 'long', ...TZ }).toLowerCase() + '</div>']
    .concat(ev ? ['<div class="t-k">next</div><div class="t-head" style="margin-top:6px">' + esc(ev.name) + '</div><div class="t-line t-soft" style="margin-top:4px">' + esc(ev.when) + '</div>'] : []) });
  const f = films(), fl = (f.trending || []).slice(0, 5);
  if (fl.length) T.push({ id: 'films', size: 'w', c: C.graphite, label: 'Films and series', pics: fl.map((t) => t.wide), faces: fl.map((t) =>
    '<div style="position:absolute;left:12px;right:12px;bottom:28px"><div class="t-k">' + esc(t.svc || 'trending') + '</div><div class="t-head" style="margin-top:4px;font-size:20px">' + esc(t.title) + '</div></div>') });
  const m = S.money;
  if (m) T.push({ id: 'money', size: 'm', c: C.teal, label: 'Money', faces: [
    '<div class="t-k">AED 1,000 · Sri Lanka</div><div class="t-mid" style="margin-top:8px;font-size:32px">' + n0(m.lkr * 1000) + '</div><div class="t-line t-soft">rupees</div>',
    '<div class="t-k">AED 1,000 · India</div><div class="t-mid" style="margin-top:8px;font-size:32px">' + n0(m.inr * 1000) + '</div><div class="t-line t-soft">rupees</div>',
  ].concat(m.gold ? ['<div class="t-k">gold 22K · a gram</div><div class="t-mid" style="margin-top:8px;font-size:32px">' + n2(m.gold.k22) + '</div><div class="t-line t-soft">dirhams</div>'] : []) });
  T.push({ id: 'clocks', size: 'm', c: C.cobalt, label: 'World clocks', faces: (S.clocks || []).filter((x) => x.city !== 'Dubai').map((x) =>
    '<div class="t-k">' + esc(x.city) + '</div><div class="t-big" style="font-size:50px;margin-top:6px">' + x.time + '</div><div class="t-line t-soft">' +
    (x.city === 'Colombo' || x.city === 'Mumbai' ? (x.awake ? 'awake' : 'asleep') : x.day) + '</div>') });
  const news = (S.news || []).slice(0, 4);
  if (news.length) T.push({ id: 'news', size: 'w', c: C.crimson, label: 'News', pics: news.map((n) => n.image), faces: news.map((n) =>
    '<div style="position:absolute;left:12px;right:12px;bottom:28px"><div class="t-k">' + esc(n.source) + (n.ago ? ' · ' + esc(n.ago) + ' ago' : '') + '</div><div class="t-head" style="margin-top:4px">' + esc(n.title) + '</div></div>') });
  const r = S.radio;
  if (r) T.push({ id: 'radio', size: 'm', c: C.orange, label: 'Radio', faces: [r.on ? '<div class="t-k">' + (r.playing ? 'on now' : r.tuning ? 'tuning in' : 'paused') + '</div><div class="t-mid" style="margin-top:8px;font-size:26px">' + esc(r.name) +
    '</div><div class="t-line t-soft ellip">' + esc(r.song) + '</div>' : '<div style="margin-top:4px">' + ico('radio', 40) + '</div><div class="t-mid" style="margin-top:14px;font-size:24px">metro radio</div><div class="t-line t-soft">Tamil, Hindi, Sinhala</div>'] });
  const vids = (S.videos || []).slice(0, 4);
  if (vids.length) T.push({ id: 'youtube', size: 'm', c: C.ytred, label: 'YouTube', pics: vids.map((v) => v.pic), faces: vids.map((v) =>
    '<div style="position:absolute;left:10px;right:10px;bottom:28px"><div class="t-line" style="font-weight:600;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;overflow:hidden">' + esc(v.title) + '</div></div>') });
  if (S.playing) T.push({ id: 'notes', at: 1, size: 'w', c: C.orange, label: 'Playing · ' + S.playing.app, faces: ['<div style="display:flex;gap:12px;align-items:center;height:100%">' + ico('music', 44) +
    '<div style="min-width:0"><div class="t-head">' + esc(S.playing.title) + '</div><div class="t-line t-soft ellip">' + esc(S.playing.artist) + '</div></div></div>'] });
  const notes = S.notes || [];
  T.push({ id: 'notes', size: 'm', c: C.violet, label: 'Notifications', count: notes.length || '', faces: [notes.length ? '<div class="t-big" style="font-size:66px">' + notes.length + '</div><div class="t-line t-soft ellip">' + esc(notes[0].app) + '</div>' :
    '<div style="margin-top:6px">' + ico('bell', 40) + '</div><div class="t-line t-soft" style="margin-top:14px">all caught up</div>'] });
  const lv = S.live || [];
  T.push({ id: 'cricket', at: 2, size: 'm', c: C.emerald, label: 'Scores', count: lv.length ? lv.length + ' live' : '', faces: lv.length ? lv.slice(0, 3).map((g) =>
    '<div class="t-k ellip">' + esc(g.league) + '</div><div class="t-line" style="margin-top:6px;font-weight:600">' + esc(g.home.short || g.home.name) + ' ' + esc(g.home.score) + '</div><div class="t-line" style="font-weight:600">' +
    esc(g.away.short || g.away.name) + ' ' + esc(g.away.score) + '</div><div class="t-line t-soft">' + esc(g.status) + '</div>') : ['<div style="margin-top:6px">' + ico('trophy', 40) + '</div><div class="t-line t-soft" style="margin-top:14px">nothing live</div>'] });
  if (S.germany) T.push({ id: 'bayern', at: 5, size: 'm', c: C.graphite, label: 'Germany', faces: teamFaces(S.germany, true).slice(0, 2) });
  T.push({ id: 'photos', size: 'm', c: C.magenta, label: 'Photos', faces: ['<div style="display:flex;height:100%;align-items:center;justify-content:center">' + ico('image', 56) + '</div>',
    '<div class="t-mid" style="font-size:22px;margin-top:4px">your photos on the TV</div><div class="t-line t-soft" style="margin-top:6px">full screen, in a moment</div>'] });
  if (S.verse) T.push({ id: 'verse', size: 'w', c: C.mauve, label: 'Verse of the day', faces: ['<div class="t-line" style="font-size:17px;line-height:1.35;font-weight:350;display:-webkit-box;-webkit-line-clamp:4;-webkit-box-orient:vertical;overflow:hidden">“' +
    esc(S.verse.text) + '”</div><div class="t-line t-soft" style="margin-top:6px">' + esc(S.verse.ref) + '</div>'] });
  T.push({ id: 'films', size: 's', c: C.graphite, label: 'Search', faces: [ico('search')] });
  T.push({ id: 'apps', size: 's', c: C.cobalt, label: 'Apps', faces: [ico('grid')] });
  T.push({ id: 'timer', size: 's', c: C.amber, label: 'Timer', faces: [ico('clock')] });
  T.push({ id: 'cricket', at: 3, size: 's', c: C.emerald, label: 'Rankings', faces: [ico('trophy')] });
  T.push({ id: 'remote', at: 1, size: 's', c: C.navy, label: 'Inputs', faces: [ico('hdmi')] });
  T.push({ id: 'remote', at: 2, size: 's', c: C.magenta, label: 'Message', faces: [ico('chat')] });
  T.push({ id: 'settings', at: 1, size: 's', c: C.steel, label: 'This TV', faces: [ico('tv')] });
  T.push({ id: 'settings', size: 's', c: C.graphite, label: 'Settings', faces: [ico('gear')] });
  return T;
}
let picked = [];   // the titles shown, by the index their cards carry
// a poster, with the title's wide picture under it: when the poster is missing or doesn't load, the picture shows
const bg2 = (a, b) => { const l = [a, b].filter(Boolean).map((u) => "url('" + esc(u) + "')"); return l.length ? 'background-image:' + l.join(',') : ''; };
const posterCard = (t, ranked) => { picked.push(t); return '<div class="poster' + (ranked ? ' ranked' : '') + '" data-t="' + (picked.length - 1) + '">' + (ranked ? '<div class="num">' + t.rank + '</div>' : '') +
  '<div class="art" style="' + bg2(t.poster, t.wide) + '"><b class="alt">' + esc(t.title) + '</b>' + (ranked ? '' : svcTag(t)) + '</div><div class="n">' + esc(t.title) + '</div><div class="s">' + esc([t.year || '', t.kind].filter(Boolean).join(' · ')) + '</div></div>'; };
const wideCard = (t) => { picked.push(t); return '<div class="card" data-t="' + (picked.length - 1) + '"><div class="art" style="' + bg2(t.wide, t.poster) + '">' + svcTag(t) + '</div><div class="n">' + esc(t.title) + '</div><div class="s">' +
  esc(t.label || [t.kind, (t.genres || [])[0]].filter(Boolean).join(' · ')) + '</div></div>'; };
function filmRow(title, note, l, kind, at) {
  if (!l || !l.length) return '';
  return '<div class="head"><b>' + title + '</b><span>' + esc(note) + '</span><em data-films="' + at + '">all</em></div><div class="row-x' + (kind === 'ranked' ? ' ranked-row' : '') + '">' + l.map((t) => kind === 'wide' ? wideCard(t) : posterCard(t, kind === 'ranked')).join('') + '</div>';
}

function redrawTiles() {
  const box = $('#tiles');
  if (!box) return drawStart();
  const T = tiles();
  box.innerHTML = tileHtml(T);
  box.querySelectorAll('.tile').forEach((el) => { const t = T[+el.dataset.i]; press(el); el.onclick = () => open(t.id, t.at || 0); });
  flips(T);
}
function tileHtml(T) {
  return T.map((t, i) => {
    const k = t.id + i, at = (tileAt[k] || 0) % t.faces.length;
    const pic = (j) => t.pics && t.pics[j] ? ' pic" style="' + bg(t.pics[j]) : '';
    return '<div class="tile ' + t.size + '" data-i="' + i + '" style="--c:' + t.c + '"><div class="flip"><div class="face front' + pic(at) + '">' + t.faces[at] +
      '</div><div class="face back' + pic((at + 1) % t.faces.length) + '">' + (t.faces.length > 1 ? t.faces[(at + 1) % t.faces.length] : '') + '</div></div>' +
      '<div class="label">' + esc(t.label) + '</div>' + (t.count ? '<div class="count">' + esc(t.count) + '</div>' : '') + '</div>';
  }).join('');
}
let flipTimer = null;
// Windows Phone never turned two tiles at once: one every couple of seconds
function flips(T) {
  clearInterval(flipTimer);
  let n = 0;
  flipTimer = setInterval(() => {
    if (page || document.hidden) return;
    const many = T.map((t, i) => i).filter((i) => T[i].faces.length > 1);
    if (!many.length) return;
    const i = many[n++ % many.length], t = T[i], k = t.id + i;
    const el = document.querySelector('#tiles .tile[data-i="' + i + '"] .flip');
    if (!el) return;
    const next = ((tileAt[k] || 0) + 1) % t.faces.length;
    const hidden = el.classList.contains('turned') ? el.querySelector('.front') : el.querySelector('.back');
    hidden.innerHTML = t.faces[next];
    if (t.pics) { hidden.classList.toggle('pic', !!t.pics[next]); hidden.style.backgroundImage = t.pics[next] ? "url('" + t.pics[next] + "')" : ''; }
    el.classList.toggle('turned');
    tileAt[k] = next;
  }, 2600);
}
function drawStart() {
  drawStatus();
  const T = tiles();
  const f = films();
  picked = []; games = []; storyList = [];
  const d = new Date().toLocaleDateString('en-GB', { weekday: 'long', day: 'numeric', month: 'long', ...TZ });
  const w = S.weather;
  $('#startBody').innerHTML = '<div class="greet"><b>' + greeting() + '</b><span>' + esc(d) + '</span></div><section class="hero" id="hero"></section>' +
    '<div class="head"><b>today</b><span>' + esc(w ? w.temp + '° and ' + w.condition.toLowerCase() + ' in ' + w.city : '') + '</span></div><section id="tiles" class="tiles">' +
    tileHtml(T) + '</section>' +
    filmRow('trending', 'Netflix · Prime Video · Shahid', f.trending, 'wide', 1) +
    filmRow('netflix top 10', 'films in the UAE this week', f.netflix, 'ranked', 2) +
    filmRow('prime top 10', 'films in the UAE', f.prime, 'ranked', 3) +
    filmRow('new on your services', 'just added in the UAE', f.fresh, 'wide', 4) +
    filmRow('netflix series', 'top 10 this week', f.netflixShows, 'ranked', 5) +
    filmRow('indian films', 'on your services', f.indian, 'poster', 6) +
    filmRow('highly rated', 'IMDb 8 and above', f.rated, 'poster', 7);
  document.querySelectorAll('#tiles .tile').forEach((el) => { const t = T[+el.dataset.i]; press(el); el.onclick = () => open(t.id, t.at || 0); });
  document.querySelectorAll('#startBody [data-t]').forEach((el) => { press(el); el.onclick = () => openTitle(picked[+el.dataset.t]); });
  document.querySelectorAll('#startBody [data-films]').forEach((el) => (el.onclick = () => open('films', +el.dataset.films)));
  drawHero();
  $('#foot').textContent = 'From your TV · Metro ' + (S.version || '') + '. On an iPhone, Share › Add to Home Screen makes this an app.';
  drawBar();
  flips(T);
}

/* ---------------------------------------------------------------- page pieces */
const TV = { timer: 'Timer', apps: 'Apps', weather: 'Weather', bayern: 'Bayern', cricket: 'Sports', money: 'Money', news: 'Reader', calendar: 'Calendar', ac: 'Ac', clocks: 'Clocks', verse: 'Bible', notes: 'Notifications', radio: 'Music' };
const none = (t) => '<p class="sub" style="margin-top:14px">' + esc(t) + '</p>';
const cell = (s, v) => '<div class="cell"><div class="s">' + s + '</div><div class="v">' + v + '</div></div>';
const padBtn = (k) => '<button data-k="' + k + '">' + ico(k) + '</button>';
const keyBtn = (k, n) => '<button data-k="' + k + '">' + ico(k) + esc(n) + '</button>';
const tileBtn = (p, a, i, n, s, c) => '<div class="tilebtn" style="--c:' + c + '" data-act="' + p + '" data-arg="' + esc(a) + '">' + ico(i) + '<div><div class="n">' + esc(n) + '</div><div class="s">' + esc(s) + '</div></div></div>';
const field = (p, name, hint, arg, i) => '<div class="sec">' + esc(name) + '</div><div class="row"><input data-for="' + p + '" data-arg="' + arg + '" placeholder="' + esc(hint) + '"><button class="go" data-send="' + p + '">' + ico(i) + '</button></div>';
function rate(place, v, ago7, cur) {
  const ch = ago7 ? (v - ago7) / ago7 * 100 : null;
  return '<div class="sec">AED 1,000 in ' + place + '</div><div class="big" style="font-size:66px">' + n0(v * 1000) + '</div><div class="sub">' + cur +
    (ch == null ? '' : ' · ' + (ch >= 0 ? '▲ ' : '▼ ') + Math.abs(ch).toFixed(2) + '% on a week ago') + '</div>';
}
function matchBlock(g, c) {
  if (!g) return none('No match coming up');
  return '<div class="band match tap" data-g="' + gref(g) + '" style="--c:' + c + '"><div class="t-k">' + esc(g.league) + (g.detail ? ' · ' + esc(g.detail) : '') + '</div><div class="crests"><div>' + crest(g.home.logo, '') + esc(g.home.name) +
    '</div><div class="score">' + (g.state === 'pre' ? hm(g.start) : esc(g.home.score + '–' + g.away.score)) + '</div><div>' + crest(g.away.logo, '') + esc(g.away.name) + '</div></div><div class="t-line" style="text-align:center">' +
    esc(g.state === 'in' ? 'live · ' + g.status : g.state === 'pre' ? dayOf(g.start) + ' · ' + when(g.start) : g.status) + '</div></div>';
}
let games = [], storyList = [];
const gref = (g) => { games.push(g); return games.length - 1; };
const chev = '<svg class="chev" viewBox="0 0 24 24"><path d="m9 18 6-6-6-6"/></svg>';
function footItem(g, res) {
  return '<div class="item tap" data-g="' + gref(g) + '">' + crest(g.home.logo) + '<div class="grow"><div class="t ellip">' + esc(g.home.name + ' v ' + g.away.name) + '</div><div class="s">' + esc(g.league) + ' · ' +
    esc(g.state === 'pre' ? dayOf(g.start) + ' ' + hm(g.start) : g.status) + '</div></div><div class="r">' + (g.state === 'pre' ? '' : esc(g.home.score + '–' + g.away.score)) +
    (res && g.outcome ? ' <span class="sq ' + g.outcome + '">' + g.outcome + '</span>' : '') + '</div>' + chev + '</div>';
}
function cricketItem(g) {
  return '<div class="item tap" data-g="' + gref(g) + '">' + ico('trophy') + '<div class="grow"><div class="t">' + esc(g.home.name) + ' <span class="dim">' + esc(g.home.score) + '</span></div><div class="t">' + esc(g.away.name) +
    ' <span class="dim">' + esc(g.away.score) + '</span></div><div class="s">' + esc(g.league) + ' · ' + esc(g.state === 'pre' ? dayOf(g.start) + ' ' + hm(g.start) : (g.note || g.status)) + '</div></div>' + chev + '</div>';
}
const list = (gs, title, res) => (gs && gs.length ? (title ? '<div class="sec">' + title + '</div>' : '') + gs.map((g) => footItem(g, res)).join('') : '');
function table(t) {
  return '<div class="sub">' + esc(t.name) + '</div><table class="tbl">' + t.rows.map((r) => '<tr class="' + (r.us ? 'us' : '') + '"><td class="n">' + r.rank + '</td><td>' + crest(r.logo, '') + esc(r.team) +
    '</td><td class="g">' + esc(r.gd) + '</td><td class="p">' + r.points + '</td></tr>').join('') + '</table>';
}
// a city's weather (Mannar, Mumbai): now, the next hours, the week
function cityPanel(c) {
  return '<div style="display:flex;align-items:center;gap:16px;margin-top:8px"><div class="big" style="font-size:88px">' + c.temp + '°</div><div>' + ico(skyIcon(c.sky, c.day), 48) + '<div class="mid" style="font-size:24px">' +
    esc(c.condition.toLowerCase()) + '</div></div></div><div class="sub" style="margin-top:6px">' + esc(c.country) + ' · ' + c.time + ' there · feels like ' + c.feels + '° · ' + c.high + '° / ' + c.low + '°</div>' +
    hoursChart(c.hours.concat(c.hours)) + '<div class="sec">the week</div>' + c.days.map((d, i) => '<div class="item">' + ico(skyIcon(d.sky, true)) + '<div class="grow"><div class="t">' + (i === 0 ? 'today' : esc(d.day)) +
    '</div>' + (d.rain ? '<div class="s">rain ' + d.rain + '%</div>' : '') + '</div><div class="r" style="font-weight:300;font-size:24px">' + d.high + '° <span class="faint">' + d.low + '°</span></div></div>').join('');
}
function board(m) {
  if (!m.board) return '';
  const v = (x) => x == null ? '–' : n2(x);
  return '<div class="sec">the dubai board · AED a gram</div><table class="tbl board"><tr class="hd"><td></td><td>morning</td><td>afternoon</td><td>evening</td><td>change</td></tr>' +
    m.board.map((r) => '<tr' + (r.karat === '22K' ? ' class="us"' : '') + '><td class="n">' + esc(r.karat) + '</td><td>' + v(r.morning) + '</td><td>' + v(r.afternoon) + '</td><td>' + v(r.evening) + '</td><td>' +
      (r.change == null ? '–' : (r.change >= 0 ? '▲ ' : '▼ ') + n2(Math.abs(r.change))) + '</td></tr>').join('') + '</table>';
}
function fuel(m) {
  if (!m || !m.fuel) return none('No fuel prices yet');
  return '<div class="sub">' + esc(m.fuel.month) + ' · AED a litre</div>' + m.fuel.prices.map((p) => '<div class="item"><div class="grow"><div class="t">' + esc(p.grade) + '</div></div><div class="r" style="font-weight:300;font-size:30px">' +
    n2(p.now) + '</div><div class="s" style="width:64px;text-align:right">' + (p.change == null ? '' : (p.change > 0 ? '▲ ' : p.change < 0 ? '▼ ' : '') + n2(Math.abs(p.change))) + '</div></div>').join('') +
    '<p class="sub" style="margin-top:10px">against ' + esc(m.fuel.before) + '</p>';
}
function leaders(t) {
  if (!t || !t.leaders || !t.leaders.length) return none('No top scorers yet');
  return t.leaders.map((l) => '<div class="item"><div class="grow"><div class="t">' + esc(l.name) + '</div><div class="s">' + esc(l.what) + (l.number ? ' · #' + esc(l.number) : '') + '</div></div><div class="r" style="font-weight:300;font-size:30px">' + l.value + '</div></div>').join('');
}
function teamNews(l) {
  if (!l || !l.length) return none('No news yet');
  return l.map((n) => { storyList.push(n); return '<div class="news tap" data-story="' + (storyList.length - 1) + '">' + (n.image ? '<div class="pic" style="' + bg(n.image) + '"></div>' : '') + '<div class="s">' + ago(n.at) +
    (n.by ? ' · ' + esc(n.by) : '') + '</div><div class="t">' + esc(n.title) + '</div><p class="sub" style="margin:6px 0 0">' + esc(n.text) + '</p></div>'; }).join('');
}
function scorecard(given) {
  const c = given || S.scorecard;
  if (!c) return none('No Sri Lanka or India scorecard right now');
  return '<div class="band" style="--c:' + C.steel + '"><div class="t-k">' + esc(c.league) + '</div><div class="mid" style="font-size:24px;margin-top:4px">' + esc(c.title) + '</div><div class="t-line" style="margin-top:6px">' + esc(c.status) +
    '</div></div>' + c.innings.slice().reverse().map((i) => '<div class="sec">' + esc(i.team) + ' · ' + esc(i.total) + (i.overs ? ' (' + esc(i.overs) + ' ov)' : '') + (i.current ? ' · batting' : '') + '</div>' +
    '<table class="tbl card"><tr class="hd"><td>batting</td><td>R</td><td>B</td><td>4s</td><td>6s</td></tr>' + i.batters.map((b) => '<tr><td><div>' + esc(b.name) + (b.notOut ? ' *' : '') + '</div><div class="s">' + esc(b.out) +
    '</div></td><td><b>' + b.runs + '</b></td><td>' + b.balls + '</td><td>' + b.fours + '</td><td>' + b.sixes + '</td></tr>').join('') + '</table>' +
    '<table class="tbl card"><tr class="hd"><td>bowling</td><td>O</td><td>M</td><td>R</td><td>W</td></tr>' + i.bowlers.map((b) => '<tr><td>' + esc(b.name) + '</td><td>' + esc(b.overs) + '</td><td>' + b.maidens + '</td><td>' + b.runs +
    '</td><td><b>' + b.wickets + '</b></td></tr>').join('') + '</table>').join('') + (c.toss ? '<p class="sub" style="margin-top:12px">' + esc(c.toss) + (c.venue ? ' · ' + esc(c.venue) : '') + '</p>' : '');
}
function scoresByLeague() {
  const g = S.games || [];
  if (!g.length) return none('No games today');
  const by = {};
  g.forEach((x) => (by[x.league] = by[x.league] || []).push(x));
  return Object.keys(by).map((l) => '<div class="sec">' + esc(l) + '</div>' + by[l].map((x) => x.sport === 'Cricket' ? cricketItem(x) : footItem(x)).join('')).join('');
}
function rankings() {
  const r = S.rankings || [];
  if (!r.length) return none('No rankings yet');
  return r.map((t) => '<div class="sec">' + esc(t.format) + ' · ' + esc(t.updated) + '</div><table class="tbl">' + t.rows.map((x) => '<tr class="' + (/Sri Lanka|India/.test(x.team) ? 'us' : '') + '"><td class="n">' + x.rank + '</td><td>' +
    crest(x.flag, '') + esc(x.team) + '</td><td class="g">' + x.rating + '</td></tr>').join('') + '</table>').join('');
}
function net() {
  const n = S.net || {}, sp = S.speed;
  return '<div class="sec">network</div><div class="grid2">' + cell('connection', esc(n.kind || '–')) + cell('network', esc(n.ssid || '–')) + cell('signal', esc(n.signal || '–')) + cell('address', esc(n.ip || '–')) +
    (n.band ? cell('band', esc(n.band) + ' GHz') : '') + (n.link ? cell('link', n.link + ' Mbps') : '') + '</div>' + (sp ? '<div class="sec">last speed test · ' + ago(sp.at) + '</div><div class="grid2">' +
    cell('download', sp.down.toFixed(1) + ' Mbps') + cell('upload', sp.up.toFixed(1) + ' Mbps') + cell('ping', Math.round(sp.ping) + ' ms') + '</div>' : '');
}
// a game: football's match centre, or cricket's scorecard; fetched from the TV as it opens (live ones again every 30 s)
function openGame(g) { if (g) open('match', 0, g); }
async function loadMatch() {
  const g = page && page.id === 'match' && page.show;
  if (!g) return;
  const j = await act(g.sport === 'Cricket' ? 'cricket' : 'match', q('id', g.id) + (g.sport === 'Cricket' ? '&' + q('league', g.leagueId || '') : ''), true);
  if (!j || !j.ok || !page || page.id !== 'match' || page.show !== g) return;
  page.data = j;
  drawPage(false);
}
const ballIco = '<svg class="mi" viewBox="0 0 24 24"><circle cx="12" cy="12" r="9"/><path d="M12 7l4 3-1.5 5h-5L8 10z"/></svg>';
function momentIcon(k) {
  if (/goal|pen|own/.test(k)) return ballIco;
  if (/red/.test(k)) return '<span class="cardi red"></span>';
  if (/yellow|card/.test(k)) return '<span class="cardi"></span>';
  if (/sub/.test(k)) return '<svg class="mi" viewBox="0 0 24 24"><path d="m16 3 4 4-4 4M20 7H4M8 21l-4-4 4-4M4 17h16"/></svg>';
  return '<span class="dot"></span>';
}
function matchHead(g, d) {
  const live = g.state === 'in';
  return '<div class="mhead"><div class="t-k">' + esc(g.league) + (d && d.venue ? ' · ' + esc(d.venue) : '') + '</div><div class="crests"><div>' + crest(g.home.logo, '') + '<span>' + esc(g.home.name) + '</span></div><div class="mscore">' +
    (g.state === 'pre' ? hm(g.start) : esc(g.home.score + '–' + g.away.score)) + '</div><div>' + crest(g.away.logo, '') + '<span>' + esc(g.away.name) + '</span></div></div><div class="mstate' + (live ? ' live' : '') + '">' +
    esc(live ? 'live · ' + g.status : g.state === 'pre' ? dayOf(g.start) + ' · ' + when(g.start) : g.status) + '</div></div>';
}
function lineupList(l, logo) {
  if (!l) return none('Not named yet');
  const kind = l.kind === 'Predicted' ? 'predicted' : l.kind === 'Likely' ? 'likely' + (l.note ? ' · ' + l.note : '') : 'official';
  const p = (x) => '<div class="pl"><span class="no">' + esc(x.n) + '</span><div class="grow"><div class="t">' + esc(x.name) + (x.goals ? ' ' + ballIco.repeat(Math.min(x.goals, 4)) : '') +
    (x.card ? '<span class="cardi' + (/red/i.test(x.card) ? ' red' : '') + '"></span>' : '') + '</div><div class="s">' + esc(x.pos) + (x.out && x.min ? ' · off ' + esc(x.min) : '') + (x.in && x.min ? ' · on ' + esc(x.min) + (x.for ? ' for ' + esc(x.for) : '') : '') +
    '</div></div>' + (x.rating ? '<span class="rt" style="--r:' + rateColor(+x.rating) + '">' + esc(x.rating) + '</span>' : '') + '</div>';
  return '<div class="lhead">' + crest(logo, '') + '<div><div class="t">' + esc(l.formation || '') + '</div><div class="s">' + kind + (l.coach ? ' · ' + esc(l.coach) : '') + '</div></div></div>' +
    l.starters.map(p).join('') + (l.bench.length ? '<div class="sec">bench</div>' + l.bench.map(p).join('') : '');
}
const rateColor = (r) => r >= 8 ? '#008a00' : r >= 7 ? '#60a917' : r >= 6 ? '#f0a30a' : '#b3121b';
function formBlock(name, logo, f) {
  if (!f) return '';
  return '<div class="lhead">' + crest(logo, '') + '<div><div class="t">' + esc(name) + '</div><div class="s">' + (f.rank ? f.rank + ordinal(f.rank) + ' · ' + f.points + ' pts' : '') + (f.record ? ' · ' + esc(f.record) : '') + '</div></div></div>' +
    (f.form.length ? '<div class="formrow">' + f.form.map((x) => '<div><span class="sq ' + esc(x.r) + '">' + esc(x.r) + '</span><div class="s">' + esc(x.score) + '</div><div class="s ellip">' + esc(x.opp) + '</div></div>').join('') + '</div>' : '') +
    (f.scored ? '<div class="sub" style="margin-top:6px">scored ' + esc(f.scored) + ' · conceded ' + esc(f.conceded) + '</div>' : '') +
    (f.out.length ? '<div class="sec">out</div>' + f.out.map((o) => '<div class="item"><div class="grow"><div class="t">' + esc(o.name) + '</div><div class="s">' + esc(o.why) + '</div></div></div>').join('') : '');
}
const ordinal = (n) => (n % 100 >= 11 && n % 100 <= 13) ? 'th' : ({ 1: 'st', 2: 'nd', 3: 'rd' })[n % 10] || 'th';
function matchItems() {
  const g = page.show, d = page.data;
  if (!d) { setTimeout(loadMatch, 0); return [[g.sport === 'Cricket' ? 'scorecard' : 'summary', () => matchHead(g) + '<div class="skel" style="height:120px;margin-top:16px"></div><p class="sub">getting the match from the TV…</p>']]; }
  if (g.sport === 'Cricket') return [['scorecard', () => scorecard(d)]];
  const G = d.game || g;
  // in the order they happened: half time after 45 and its added minutes, 90'+2' as 92
  const at = (m) => /^HT/i.test(m.min) || m.kind === 'ht' ? 45.9 : /^FT/i.test(m.min) ? 999 : (m.min.match(/\d+/g) || [0]).reduce((x, y) => +x + +y, 0);
  const moments = d.moments.slice().sort((a, b) => at(a) - at(b));
  const items = [['summary', () => matchHead(G, d) + (moments.length ? '<div class="sec">the match</div>' + moments.map((m) => (m.kind === 'ht' || /^(HT|FT)$/i.test(m.min) ? '<div class="msep">' + (/^FT/i.test(m.min) ? 'full time' : 'half time') + '</div>' : '<div class="mom ' + (m.home ? 'h' : 'a') + '"><span class="min">' + esc(m.min) + '</span>' +
    momentIcon(m.kind) + '<div class="grow"><div class="t">' + esc(m.who) + '</div>' + (m.extra ? '<div class="s">' + esc(m.extra) + '</div>' : '') + '</div>' + crest(m.home ? G.home.logo : G.away.logo, 'mc') + '</div>')).join('') :
    (G.state === 'pre' ? none('Kick-off ' + when(G.start)) : none('Nothing yet')))]];
  if (d.stats.length) items.push(['stats', () => '<div class="shead">' + crest(G.home.logo, '') + '<span></span>' + crest(G.away.logo, '') + '</div>' + d.stats.map((x) => '<div class="stat"><div class="sl"><b>' + esc(x.home) +
    '</b><span>' + esc(x.label) + '</span><b>' + esc(x.away) + '</b></div><div class="sb"><i style="width:' + Math.round(x.share * 100) + '%;background:' + esc(d.homeTone || '#fff') + '"></i><i style="width:' + Math.round((1 - x.share) * 100) +
    '%;background:' + esc(d.awayTone || 'var(--accent)') + '"></i></div></div>').join('')]);
  if (d.homeLineup || d.awayLineup) {
    items.push([(G.home.short || G.home.name).toLowerCase(), () => lineupList(d.homeLineup, G.home.logo)]);
    items.push([(G.away.short || G.away.name).toLowerCase(), () => lineupList(d.awayLineup, G.away.logo)]);
  }
  if (d.commentary.length) items.push(['commentary', () => d.commentary.map((c) => '<div class="com"><span class="min">' + esc(c.min) + '</span><div class="grow">' + (c.kind && c.kind !== 'comment' ? '<div class="t-k" style="color:var(--dim)">' + esc(c.kind) + '</div>' : '') +
    '<div>' + esc(c.text) + '</div></div></div>').join('')]);
  items.push(['form', () => formBlock(G.home.name, G.home.logo, d.homeFacts) + '<div style="height:20px"></div>' + formBlock(G.away.name, G.away.logo, d.awayFacts) +
    (d.h2h ? '<div class="sec">head to head · since ' + d.h2h.since + '</div><div class="grid2" style="grid-template-columns:1fr 1fr 1fr">' + cell(esc(G.home.short || G.home.name) + ' won', d.h2h.wins) + cell('drawn', d.h2h.draws) +
      cell(esc(G.away.short || G.away.name) + ' won', d.h2h.losses) + '</div>' + d.h2h.meetings.map((x) => '<div class="item"><div class="grow"><div class="t">' + (x.homeAtHome ? esc(G.home.name) + ' ' + x.hg + '–' + x.ag + ' ' + esc(G.away.name) :
      esc(G.away.name) + ' ' + x.ag + '–' + x.hg + ' ' + esc(G.home.name)) + '</div><div class="s">' + esc(x.comp) + ' · ' + new Date(x.at).toLocaleDateString('en-GB', { day: 'numeric', month: 'short', year: 'numeric' }) + '</div></div></div>').join('') : '')]);
  return items;
}
// a news story in full, as the TV's reader has it
function storyPage(n) {
  if (!page.data) setTimeout(async () => { const j = await act('story', q('id', n.id), true); if (page && page.id === 'story' && page.show === n && j) { page.data = j; drawPage(false); } }, 0);
  const paras = (page.data && page.data.paras) || [];
  return (n.image ? '<div class="title-hero" style="' + bg(n.image) + '"></div>' : '<div style="height:30px"></div>') + '<div class="title-name" style="font-size:32px;line-height:1.12;margin-top:' + (n.image ? '-30px' : '0') + '">' + esc(n.title) + '</div>' +
    '<div class="sub" style="margin-top:8px">' + ago(n.at) + (n.by ? ' · ' + esc(n.by) : '') + '</div>' + (paras.length ? paras.map((p) => p.head ? '<div class="sec">' + esc(p.text) + '</div>' : '<p class="para">' + esc(p.text) + '</p>').join('') :
    '<p class="para">' + esc(n.text) + '</p>' + (page.data ? '' : '<p class="sub">getting the whole story…</p>'));
}
const grid = (l, ranked) => l && l.length ? '<div class="grid-posters">' + l.map((t) => posterCard(t, ranked)).join('') + '</div>' : none('Nothing here yet');

// the next 24 hours as a line, with the temperatures over it and the hours under it
function hoursChart(hours) {
  const h = hours.slice(0, 24).filter((_, i) => i % 2 === 0);
  if (h.length < 2) return '';
  const lo = Math.min(...h.map((x) => x.temp)), hi = Math.max(...h.map((x) => x.temp)), span = hi - lo || 1;
  const X = (i) => 10 + i * (340 / (h.length - 1)), Y = (t) => 78 - (t - lo) / span * 46;
  return '<svg class="chart" viewBox="0 0 360 120"><polyline points="' + h.map((x, i) => X(i).toFixed(1) + ',' + Y(x.temp).toFixed(1)).join(' ') + '" stroke-width="2"/>' +
    h.map((x, i) => (i % 2 === 0 ? '<text x="' + X(i) + '" y="' + (Y(x.temp) - 9) + '" text-anchor="middle">' + x.temp + '°</text><text x="' + X(i) + '" y="112" text-anchor="middle">' + esc(x.label) + '</text>' : '') +
      '<circle cx="' + X(i) + '" cy="' + Y(x.temp) + '" r="2.5" fill="#fff" stroke="none"/>').join('') + '</svg>';
}
// this month as Windows Phone's calendar: today white, a day with something on it marked
function month() {
  const now = new Date(new Date().toLocaleString('en-US', TZ));
  const y = now.getFullYear(), m = now.getMonth(), first = new Date(y, m, 1), days = new Date(y, m + 1, 0).getDate();
  const lead = (first.getDay() + 6) % 7;
  const marked = new Set((S.calendar || []).map((e) => e.date));
  let cells = ['mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun'].map((d) => '<div class="h">' + d + '</div>').join('');
  for (let i = 0; i < lead; i++) cells += '<div class="x"></div>';
  for (let d = 1; d <= days; d++) {
    const iso = y + '-' + String(m + 1).padStart(2, '0') + '-' + String(d).padStart(2, '0');
    cells += '<div class="' + (d === now.getDate() ? 'today' : '') + '">' + d + (marked.has(iso) ? '<i></i>' : '') + '</div>';
  }
  return '<div class="mid" style="margin-top:4px">' + first.toLocaleDateString('en-GB', { month: 'long', year: 'numeric' }).toLowerCase() + '</div><div class="month">' + cells + '</div>';
}
function converter() {
  const m = S.money;
  if (!m) return none('No rates yet');
  return '<div class="sec">dirhams</div><div class="row"><input id="aed" inputmode="decimal" value="1000"></div><div class="grid2" id="conv"></div>' +
    (m.gold ? '<div class="sec">gold · grams of 22K</div><div class="row"><input id="grams" inputmode="decimal" value="8"></div><div class="grid2" id="goldconv"></div>' : '') +
    '<p class="sub" style="margin-top:14px">At today\'s rates, as the TV has them.</p>';
}
function convWire(panel) {
  const a = panel.querySelector('#aed'), g = panel.querySelector('#grams'), m = S.money;
  if (!a) return;
  const draw = () => {
    const v = parseFloat(a.value.replace(/,/g, '')) || 0;
    panel.querySelector('#conv').innerHTML = cell('Sri Lanka · LKR', n0(v * m.lkr)) + cell('India · INR', n0(v * m.inr));
    if (g && m.gold) { const x = parseFloat(g.value) || 0; panel.querySelector('#goldconv').innerHTML = cell('22K · AED', n0(x * m.gold.k22)) + cell('24K · AED', n0(x * m.gold.k24)); }
  };
  a.oninput = draw; if (g) g.oninput = draw; draw();
}
const agoShort = (ms) => { if (!ms) return ''; const m = Math.max(0, Math.round((Date.now() - ms) / 60000)); return m < 60 ? m + ' min' : m < 1440 ? Math.round(m / 60) + ' h' : Math.round(m / 1440) + ' d'; };
const newsList = (l) => l.map((n) => '<div class="news">' + (n.image ? '<div class="pic" style="' + bg(n.image) + '"></div>' : '') + '<div class="s">' + esc(n.source) +
  ((n.published ? agoShort(n.published) : n.ago) ? ' · ' + esc(n.published ? agoShort(n.published) : n.ago) + ' ago' : '') +
  '</div><div class="t">' + esc(n.title) + '</div><div class="sum">' + esc(n.summary) + '</div></div>').join('') || none('No stories here yet');
function newsBy(cat) { return newsList((S.news || []).filter((n) => !cat || (n.cat || '').toLowerCase() === cat)); }
function newsItems() {
  const secs = (feeds && feeds.sections) || [];
  if (secs.length) return [['top stories', () => newsBy(null)]].concat(secs.map((s) => [s.name.toLowerCase(), () => newsList(s.stories)]));
  const cats = [...new Set((S.news || []).map((n) => (n.cat || '').toLowerCase()).filter(Boolean))];
  return [['top stories', () => newsBy(null)]].concat(cats.map((c) => [c, () => newsBy(c)]));
}

/* ---------------------------------------------------------------- the news and the money from Metro's feeds job */
// A GitHub job reads the papers and the rates every 20 minutes (as the TV does) and publishes them with the page as
// feeds.json: the news page's sections always come from it; with the TV off, the top stories and the money too.
let feeds = null, feedsAt = 0;
async function getFeeds() {
  if (Date.now() - feedsAt < 600000) return false;
  feedsAt = Date.now();
  try {
    const r = await fetch((AWAY ? '' : 'https://thuva06.github.io/metro/') + 'feeds.json', { cache: 'no-store' });
    if (!r.ok) return false;
    const j = await r.json();
    if (!j || !j.at || (feeds && j.at <= feeds.at)) return false;
    feeds = j;
    return true;
  } catch (e) { return false; }
}
function feedsIn() {
  if (!feeds || !S || !AWAY || Date.now() - seen < 300000) return false;
  let changed = false;
  if (feeds.news && feeds.news.length) { S.news = feeds.news; changed = true; }
  if (feeds.money && feeds.money.lkr) { S.money = Object.assign({}, S.money, feeds.money, { gold: feeds.money.gold || (S.money || {}).gold, board: feeds.money.board || (S.money || {}).board, fuel: feeds.money.fuel || (S.money || {}).fuel }); changed = true; }
  return changed;
}

/* ---------------------------------------------------------------- the pages */
const PAGES = {
  weather: { name: 'weather', items: () => [
    ['dubai', () => { const w = S.weather; if (!w) return none('No weather yet');
      return '<div style="display:flex;align-items:center;gap:18px;margin-top:8px"><div class="big">' + w.temp + '°</div><div>' + ico(skyIcon(w.sky, w.day), 56) + '<div class="mid" style="font-size:26px;margin-top:4px">' + esc(w.condition.toLowerCase()) + '</div></div></div>' +
        '<div class="sub" style="margin-top:8px">' + esc(w.city) + ' · feels like ' + w.feels + '° · ' + w.high + '° / ' + w.low + '°</div>' + hoursChart(w.hours) + '<div class="grid2">' +
        cell('humidity', w.humidity + '%') + cell('wind', w.wind + ' km/h ' + esc(w.windFrom)) + cell('UV index', w.uv) + cell('air', w.aqi == null ? '–' : w.aqi + ' <span style="font-size:14px">' + esc(w.aqiLabel) + '</span>') +
        cell('sunrise', esc(w.sunrise)) + cell('sunset', esc(w.sunset)) + cell('visibility', (w.visibility || '–') + ' km') + cell('pressure', (w.pressure || '–') + ' hPa') +
        cell('cloud', (w.cloud == null ? '–' : w.cloud + '%')) + cell('PM2.5', w.pm25 == null ? '–' : w.pm25 + ' µg') + '</div><p class="sub" style="margin-top:12px">updated ' + (w.at ? ago(w.at) : '') + '</p>'; }],
    ['hourly', () => (S.weather ? S.weather.hours : []).map((h) => '<div class="item">' + ico(skyIcon(h.sky, h.day)) + '<div class="grow"><div class="t">' + esc(h.label) + '</div>' +
      (h.rain ? '<div class="s">rain ' + h.rain + '%</div>' : '') + '</div><div class="r" style="font-weight:300;font-size:28px">' + h.temp + '°</div></div>').join('') || none('No forecast yet')],
    ['week', () => (S.weather ? S.weather.days : []).map((d, i) => '<div class="item">' + ico(skyIcon(d.sky, true)) + '<div class="grow"><div class="t">' + (i === 0 ? 'today' : esc(d.day)) + ' <span class="faint">' + esc(d.date) +
      '</span></div><div class="s">' + esc(d.condition || '') + (d.rain ? ' · rain ' + d.rain + '%' : '') + '</div></div><div class="r" style="font-weight:300;font-size:26px">' + d.high + '° <span class="faint">' + d.low + '°</span></div></div>').join('') || none('No forecast yet')],
  ].concat((S.cities || []).map((c) => [c.city.toLowerCase(), () => cityPanel(c)])) },
  bayern: { name: 'bayern', items: () => [
    ['next', () => S.bayern ? matchBlock(S.bayern.live || S.bayern.next, C.red) + list(S.bayern.fixtures.slice(S.bayern.live ? 0 : 1), 'coming up') : none('No matches yet')],
    ['results', () => S.bayern ? list(S.bayern.results, null, true) : none('No results yet')],
    ['tables', () => S.bayern && S.bayern.tables && S.bayern.tables.length ? S.bayern.tables.map((t, i) => (i ? '<div style="height:18px"></div>' : '') + table(t)).join('') : none('No table yet')],
    ['scorers', () => leaders(S.bayern)],
    ['news', () => teamNews((S.teamNews || {}).bayern)],
    ['germany', () => S.germany ? matchBlock(S.germany.live || S.germany.next, C.graphite) + list(S.germany.fixtures, 'coming up') + list(S.germany.results, 'results', true) +
      (S.germany.tables || []).map((t) => '<div style="height:18px"></div>' + table(t)).join('') : none('No Germany matches yet')],
    ['germany news', () => teamNews((S.teamNews || {}).germany)],
  ] },
  cricket: { name: 'cricket', items: () => [
    ['sri lanka · india', () => { const g = S.cricket || []; if (!g.length) return none('No Sri Lanka or India matches in the next days');
      return ['in', 'pre', 'post'].map((st) => { const x = g.filter((y) => y.state === st); return x.length ? '<div class="sec">' + ({ in: 'live', pre: 'coming up', post: 'results' })[st] + '</div>' + x.map(cricketItem).join('') : ''; }).join(''); }],
    ['scorecard', scorecard],
    ['scores', scoresByLeague],
    ['rankings', rankings],
  ] },
  money: { name: 'money', items: () => [
    ['rupees', () => { const m = S.money; if (!m) return none('No rates yet');
      return rate('Sri Lanka', m.lkr, m.lkrWeekAgo, 'LKR') + spark(m.lkrMonth) + rate('India', m.inr, m.inrWeekAgo, 'INR') + spark(m.inrMonth); }],
    ['gold', () => { const g = S.money && S.money.gold; if (!g) return none('No gold rate yet');
      return '<div class="sub">Dubai · ' + esc(g.session) + ' · a gram</div><div class="big" style="font-size:78px;margin-top:6px">' + n2(g.k22) + '</div><div class="sub">22K in dirhams</div><div class="grid2">' +
        cell('24K', n2(g.k24)) + cell('21K', n2(g.k21)) + cell('18K', n2(g.k18)) + cell('a poun of 22K', n0(g.poun)) + '</div>' + board(S.money) + '<div class="sec">24K over the month</div>' + spark(g.month); }],
    ['convert', converter],
    ['fuel', () => fuel(S.money)],
  ] },
  news: { name: 'news', items: newsItems },
  calendar: { name: 'calendar', items: () => [
    ['month', month],
    ['coming up', () => (S.calendar || []).map((e) => '<div class="item"><div style="width:52px;text-align:center;flex:none"><div style="font-weight:300;font-size:32px;line-height:1">' +
      +e.date.slice(8) + '</div><div class="s">' + new Date(e.date + 'T12:00:00').toLocaleDateString('en-GB', { month: 'short' }).toLowerCase() + '</div></div><div class="grow"><div class="t">' + esc(e.name) + '</div><div class="s">' +
      esc(e.when) + (e.start ? ' · ' + hm(e.start) : '') + (e.detail ? ' · ' + esc(e.detail) : '') + '</div></div></div>').join('') || none('Nothing in the next six weeks')],
  ] },
  clocks: { name: 'clocks', items: () => [['world', () => (S.clocks || []).map((x) => '<div class="item"><div class="grow"><div class="t">' + esc(x.city) + '</div><div class="s">' + esc(x.day) +
    (x.city === 'Colombo' || x.city === 'Mumbai' ? ' · ' + (x.awake ? 'awake: a good time to call' : 'asleep') : '') + '</div></div><div class="r" style="font-weight:300;font-size:44px">' + x.time + '</div></div>').join('')]] },
  verse: { name: 'verse', items: () => [['today', () => S.verse ? '<div class="verse">“' + esc(S.verse.text) + '”</div><div class="sub" style="margin-top:12px">' + esc(S.verse.ref) + ' · ' + esc(S.verse.version) + '</div>' : none('No verse yet')]] },
  notes: { name: 'notifications', items: () => [
    ['notifications', () => (S.notes || []).map((n) => '<div class="item">' + ico('bell') + '<div class="grow"><div class="s" style="text-transform:uppercase;letter-spacing:.08em;font-weight:600">' + esc(n.app) + ' · ' + hm(n.time) +
      '</div><div class="t">' + esc(n.title) + '</div><div class="s">' + esc(n.text) + '</div></div></div>').join('') || none("You're all caught up")],
    ['playing', () => S.playing ? '<div class="mid" style="margin-top:8px">' + esc(S.playing.title) + '</div><div class="sub">' + esc(S.playing.artist) + ' · ' + esc(S.playing.app) + '</div>' +
      '<div class="keys" style="margin-top:18px">' + keyBtn('rew', 'back') + keyBtn('play', 'play') + keyBtn('ff', 'forward') + keyBtn('mute', 'mute') + '</div>' : none('Nothing playing')],
  ] },
  films: { name: 'films', items: () => [
    ['search', () => '<div class="row" style="margin-top:4px"><input id="find" placeholder="any film or series" enterkeyhint="search" autocomplete="off"><button class="go" id="findgo">' + ico('search') + '</button></div>' +
      '<p class="sub" id="findnote" style="margin-top:10px">Everything on JustWatch: where it streams in the UAE, or the VPN location that gets it.</p><div id="found"></div>'],
    ['trending', () => grid(films().trending)],
    ['netflix', () => grid(films().netflix, true)],
    ['prime video', () => grid(films().prime, true)],
    ['new', () => grid(films().fresh)],
    ['series', () => grid(films().netflixShows, true)],
    ['indian', () => grid(films().indian)],
    ['highly rated', () => grid(films().rated)],
  ] },
  radio: { name: 'radio', items: () => {
    const r = S.radio || { rows: [] };
    const st = (s, c) => '<div class="item tap" data-radio="' + esc(s.id) + '">' + (s.logo ? '<img class="logo" src="' + esc(s.logo) + '" alt="" loading="lazy">' : '<div style="width:48px;height:48px;background:' + c + '"></div>') +
      '<div class="grow"><div class="t">' + esc(s.name) + '</div><div class="s">' + esc(s.line) + '</div></div>' + (r.on === s.id ? '<div class="r" style="color:var(--accent)">' + ico('music') + '</div>' : '') + '</div>';
    return [['on now', () => r.on ? '<div class="band" style="--c:' + C.orange + '"><div class="t-k">' + (r.playing ? 'on the TV now' : r.tuning ? 'tuning in' : 'paused') + '</div><div class="mid" style="margin-top:6px">' + esc(r.name) +
      '</div><div class="sub" style="color:#fff">' + esc(r.song) + '</div></div><div class="btns"><button class="btn primary" data-radio-do="toggle">' + ico(r.playing ? 'stop' : 'play') + (r.playing ? 'Pause' : 'Play') +
      '</button><button class="btn" data-radio-do="stop">' + ico('power') + 'Off</button></div>' : none('The radio is off. Pick a station: it plays on the TV.')]]
      .concat(r.rows.map((row) => [row.title.toLowerCase(), () => row.stations.map((s) => st(s, row.color)).join('')]));
  } },
  youtube: { name: 'youtube', items: () => [['latest', () => (S.videos || []).map((v) => '<div class="item tap" data-video="' + esc(v.id) + '"><img class="thumb" style="width:120px;height:68px" src="' + esc(v.pic) + '" alt="" loading="lazy"><div class="grow"><div class="t" style="font-size:16px;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;overflow:hidden">' +
    esc(v.title) + '</div><div class="s">' + esc(v.channel) + ' · ' + ago(v.at) + '</div></div></div>').join('') || none('No videos yet: add channels on the TV')]] },
  settings: { name: 'settings', items: () => {
    const st = S.settings || {};
    const tog = (k, t, s, on) => '<div class="toggle" data-set="' + k + '" data-on="' + (on ? 1 : 0) + '"><div><div class="t">' + t + '</div><div class="s">' + s + '</div></div><div class="switch' + (on ? ' on' : '') + '"></div></div>';
    return [['metro', () => '<div class="sec">accent colour</div><div class="swatches">' + (st.accents || []).map((a, i) => '<div style="--c:' + (i === 0 ? '#fff' : a.color) + '" class="' + (i === st.accent ? 'on' : '') + '" data-accent="' + i + '"></div>').join('') + '</div>' +
      '<p class="sub" style="margin-top:8px">' + esc(((st.accents || [])[st.accent] || {}).name || '') + ' · focus, the pivots and progress on the TV, and this app</p>' +
      tog('alerts', 'match alerts', 'kick-off, goals and full time over any app; the score box', st.alerts) + tog('gold', 'gold alerts', 'each new Dubai gold rate over any app', st.gold) +
      tog('night', 'night mode', 'softer and calmer from 23:00 to 06:00', st.night)],
    ['this tv', () => net() + '<div class="sec">the TV</div><div class="grid3">' + tileBtn('sleep', '', 'moonstar', 'TV sleep', '30 · 60 · 90 min', C.graphite) + tileBtn('quick', '', 'sliders', 'quick settings', 'on the TV', C.graphite) +
      tileBtn('open', 'what=Briefing', 'sun', 'briefing', 'on the TV', C.graphite) + '</div><div class="sec">this phone</div><p class="sub">Metro ' + esc(S.version || '') + ' on the TV · ' +
      (AWAY ? 'the app comes from ' + esc(location.host) + ', everything else from the TV, sealed, through the relay' : 'the app comes from the TV at ' + esc(location.host)) + '. With the TV off it shows what it said last.</p>'],
    ['anywhere', () => anywhere()]];
  } },
  remote: { name: 'remote', items: () => [
    ['remote', () => (S && S.playing ? '<div class="playing">' + ico('music') + '<div style="min-width:0;flex:1"><div class="ellip">' + esc(S.playing.title) + '</div><div class="sub ellip" style="font-size:13px">' +
      esc(S.playing.app) + '</div></div></div>' : '') + '<div class="touch" id="touch">swipe to move · tap for OK</div><div class="pad">' +
      '<button class="blank"></button>' + padBtn('up') + '<button class="blank"></button>' + padBtn('left') + '<button class="ok" data-k="ok">OK</button>' + padBtn('right') +
      '<button class="blank"></button>' + padBtn('down') + '<button class="blank"></button></div><div class="keys">' +
      keyBtn('back', 'back') + keyBtn('home', 'home') + '<button data-quick>' + ico('sliders') + 'quick</button>' + keyBtn('play', 'play') +
      keyBtn('voldown', 'vol −') + keyBtn('mute', 'mute') + keyBtn('volup', 'vol +') + keyBtn('ff', 'forward') + '</div>'],
    ['inputs', () => '<div class="grid3">' + tileBtn('input', 'port=1', 'hdmi', 'Streamer', 'HDMI 1', C.navy) + tileBtn('input', 'port=2', 'hdmi', 'mini PC', 'HDMI 2', C.navy) +
      tileBtn('input', 'port=3', 'hdmi', 'HDMI 3', 'Poseidon', C.navy) + tileBtn('sleep', '', 'moonstar', 'TV sleep', '30 · 60 · 90', C.graphite) + tileBtn('quick', '', 'sliders', 'quick settings', 'on the TV', C.graphite) +
      tileBtn('ac', '', 'power', 'air con', 'press', C.steel) + '</div>'],
    ['send', () => field('search', 'search Metro on the TV', 'a film, a series or an app', 'q', 'search') + field('youtube', 'a YouTube link', 'paste it, and it plays on the TV', 'url', 'yt') +
      field('alert', 'a message on the screen', 'over whatever is on', 'text', 'chat') + field('type', 'type on the TV', 'into whatever box is open', 'text', 'keyboard')],
  ] },
  apps: { name: 'apps', items: () => [['on the tv', () => '<div class="apps">' + (S.apps || []).map((a) => '<div class="app" data-launch="' + esc(a.pkg) + '"><img ' + (AWAY ? 'data-icon="' + esc(a.pkg) + '"' : 'src="/api/icon?code=' + encodeURIComponent(code) + '&pkg=' +
    encodeURIComponent(a.pkg) + '"') + ' alt="" loading="lazy"><span>' + esc(a.label) + '</span></div>').join('') + '</div>' || none('No apps')]] },
  timer: { name: 'timer', items: () => [['timer', () => { const t = S.timer || {};
    const left = t.phase === 'Running' ? Math.max(0, t.left - (Date.now() - seen)) : t.left || 0;
    const mm = Math.floor(left / 60000), ss = Math.floor(left / 1000) % 60;
    return '<div class="big" id="tleft" data-end="' + (t.phase === 'Running' ? seen + t.left : 0) + '">' + mm + ':' + String(ss).padStart(2, '0') + '</div><div class="sub">' + esc((t.phase || 'Ready').toLowerCase()) + ' · on the TV</div>' +
      '<div class="btns"><button class="btn primary" data-timer="do=toggle">' + ico(t.phase === 'Running' ? 'stop' : 'play') + (t.phase === 'Running' ? 'Pause' : 'Start') + '</button><button class="btn" data-timer="do=reset">' + ico('refresh') + 'Reset</button></div>' +
      '<div class="sec">start one</div><div class="grid3">' + [1, 5, 10, 15, 20, 30, 45, 60, 90].map((m) => '<div class="tilebtn" style="--c:' + C.amber + ';min-height:74px" data-timer="min=' + m + '"><div class="mid" style="font-size:28px">' + m + '</div><div class="s">min</div></div>').join('') + '</div>'; }]] },
  photos: { name: 'photos', items: () => [['send', () => '<div class="tilebtn" style="--c:' + C.magenta + ';min-height:190px;margin-top:4px" id="pick">' + ico('image') +
    '<div><div class="mid">choose photos</div><div class="s">they show full screen on the TV; Left and Right there go through them</div></div></div>' +
    '<input type="file" id="files" accept="image/*" multiple hidden><div id="sendlog" class="sub" style="margin-top:14px"></div>']] },
  ac: { name: 'air con', items: () => [
    ['press', () => '<div class="tilebtn" style="--c:' + C.steel + ';min-height:190px;margin-top:4px" data-act="ac">' + ico('power') + '<div><div class="mid">press</div><div class="s">' +
      (S.ac ? (S.ac.ok ? 'last pressed at ' + hm(S.ac.at) : esc(S.ac.line)) : 'turns the AC on or off') + '</div></div></div><p class="sub" style="margin-top:14px">One press turns it on or off: nothing can see which it is.</p>'],
    ['schedules', () => '<div id="sched" class="sub">asking the hub…</div><div id="slist"></div><div class="row"><input id="at" type="time" value="23:30"><select id="days">' +
      '<option value="127">every day</option><option value="31">weekdays</option><option value="96">weekends</option></select><button class="go" id="add">add</button></div>' +
      '<p class="sub" style="margin-top:12px">The hub presses at these times by itself, with the TV off too.</p>'],
  ] },
  title: { name: '', items: () => [['', () => titlePage(page.show)]] },
  match: { name: 'match', items: matchItems },
  story: { name: '', items: () => [['', () => storyPage(page.show)]] },
};

/* a film or series: its picture, what it is, where it streams, and the TV one press away */
function titlePage(t) {
  const len = t.runtime ? (t.runtime >= 60 ? Math.floor(t.runtime / 60) + ' h ' + (t.runtime % 60) + ' min' : t.runtime + ' min') : '';
  const meta = [t.year, t.kind, len, t.imdb ? '★ ' + t.imdb.toFixed(1) : ''].filter(Boolean);
  setTimeout(() => where(t), 0);
  return '<div class="title-hero" style="' + bg(t.backdrop || t.wide) + '"></div><div class="title-name">' + esc(t.title) + '</div><div class="chips">' + meta.map((m) => '<span>' + esc(m) + '</span>').join('<span>·</span>') +
    (t.age ? '<span class="b">' + esc(t.age) + '</span>' : '') + (t.badges || []).map((b) => '<span class="b">' + esc(b) + '</span>').join('') + svcTag(t) + '</div>' +
    '<p class="sub" style="color:rgba(255,255,255,.88);font-size:17px;margin-top:14px">' + esc(t.synopsis || 'No synopsis yet.') + '</p>' +
    (t.cast && t.cast.length ? '<p class="sub">with ' + esc(t.cast.join(', ')) + '</p>' : '') + '<div id="where"></div><div class="btns">' +
    (t.svc ? '<button class="btn primary" data-play>' + ico('play') + 'Play on ' + esc(t.svc) + '</button>' : '') + '<button class="btn" data-show>' + ico('tv') + 'Show on TV</button>' +
    (t.trailer ? '<button class="btn" data-trailer>' + ico('yt') + 'Trailer on TV</button>' : '') + '</div>';
}
async function where(t) {
  if (t.svc) return;
  const j = await act('where', q('id', t.id), true);
  const box = $('#where');
  if (!box || !j || !j.ok) return;
  box.innerHTML = j.vpn ? '<div class="vpn"><div class="k">NOT ON YOUR SERVICES · VPN</div><div class="t">Connect your VPN to ' + esc(j.vpn) + ', then play it on ' + esc(j.on.join(' or ')) + '.</div>' +
    (j.netflix ? '<div class="sub" style="margin-top:6px">Netflix has it in ' + esc(j.netflix) + '</div>' : '') + (j.prime ? '<div class="sub">Prime Video has it in ' + esc(j.prime) + '</div>' : '') + '</div>'
    : '<div class="vpn plain"><div class="t">' + esc(j.line) + '</div></div>';
}

/* ---------------------------------------------------------------- moving between Start and pages */
function turnAway(then) {
  const els = [...document.querySelectorAll('#startBody > .greet, #startBody > .hero, #startBody > .head, #tiles .tile, #startBody > .row-x')].filter((e) => e.getBoundingClientRect().bottom > 0 && e.getBoundingClientRect().top < innerHeight);
  els.forEach((el, i) => { el.style.setProperty('--d', Math.min(i * 0.025, 0.25) + 's'); el.classList.add('out'); });
  setTimeout(then, 230 + Math.min(els.length * 25, 250));
}
function open(id, at, show) {
  if (!PAGES[id]) return;
  const go = () => { if (page) stack.push(page); page = { id, at: at || 0, show }; history.pushState({ p: id }, ''); drawPage(true); };
  if (page) go(); else turnAway(go);
}
const openTitle = (t) => { if (t) open('title', 0, t); };
function home() {
  page = null; stack.length = 0;
  $('#page').hidden = true; $('#start').hidden = false;
  if (S) drawStart();
  const st = $('#start'); st.classList.remove('back-in'); void st.offsetWidth; st.classList.add('back-in');
}
window.onpopstate = () => {
  if (!page) return;
  if (stack.length) { page = stack.pop(); drawPage(true); } else home();
};

// a page: its names along the top and its items side by side, on Windows Phone's pivot
let pivot = null, spans = [];
function drawPage(enter) {
  const p = PAGES[page.id];
  const items = p.items();
  $('#start').hidden = true; $('#page').hidden = false;
  const isTitle = page.id === 'title' || page.id === 'story';
  $('#pano').textContent = isTitle ? '' : p.name;
  $('#kicker').textContent = isTitle ? '' : 'METRO · ' + p.name.toUpperCase();
  $('.phead').style.display = isTitle ? 'none' : '';
  picked = []; games = []; storyList = [];
  const n = items.length, loop = n > 1;
  spans = isTitle ? [] : strip($('#strip'), items.map((it) => it[0]), loop);
  const box = $('#panels');
  box.classList.add('pv');
  box.classList.toggle('still', !enter);
  pivot = new Pivot(box, items.map((it) => it[1]()), {
    at: page.at,
    onMove: (f, glide) => { stripMove($('#strip'), spans, n, loop, f, glide); $('#pano').style.transition = glide ? GLIDE : 'none'; $('#pano').style.transform = 'translate3d(' + (-f * 40) + 'px,0,0)'; },
    // a glide can end after the page was left (Back straight after a swipe): nothing to mark then
    onAt: (i) => { if (page) page.at = i; stripOn(spans, n, loop, i); },
    onChange: (i) => { if (!page || !pivot) return; page.at = i; const pn = pivot.panel(i); if (pn) pn.scrollTop = pn.scrollTop; },
  });
  stripOn(spans, n, loop, pivot.at);
  spans.forEach((sp) => (sp.onclick = () => pivot.to(+sp.dataset.at)));
  if (enter) { const pg = $('#page'); pg.classList.remove('in'); void pg.offsetWidth; pg.classList.add('in'); }
  pivot.panels().forEach(wire);
  drawBar();
  ticking();
}
// one item again (what is playing changed, a timer started): only that panel is drawn anew
function redrawPanel(i) {
  if (!pivot || !page) return;
  const it = PAGES[page.id].items()[i];
  const pn = pivot.panel(i);
  if (!it || !pn) return;
  const top = pn.scrollTop;
  picked = picked.slice(0);
  pn.innerHTML = it[1]();
  pn.scrollTop = top;
  wire(pn);
}
// the timer's minutes count down while its page is open
function ticking() {
  clearInterval(ticking.t);
  ticking.t = setInterval(() => {
    const el = document.querySelector('.panel:not(.copy) #tleft');
    if (!el) return;
    const end = +el.dataset.end;
    if (!end) return;
    const left = Math.max(0, end - Date.now());
    el.textContent = Math.floor(left / 60000) + ':' + String(Math.floor(left / 1000) % 60).padStart(2, '0');
  }, 500);
}

/* ---------------------------------------------------------------- what a page's controls do */
// Metro anywhere: on the Wi-Fi, the link that pairs this phone's public Metro; away, how it is connected
function anywhere() {
  if (AWAY) return '<div class="tilebtn" style="--c:' + C.cobalt + ';min-height:150px;margin-top:4px">' + ico('globe') + '<div><div class="mid">' + (Relay.on ? 'connected' : 'connecting…') +
    '</div><div class="s">through the relay (' + esc(Relay.via) + ')' + (seen ? ' · the TV last answered ' + ago(seen) : '') + '</div></div></div>' +
    '<p class="sub" style="margin-top:14px">Everything goes sealed with the key only the TV, the AC hub and this phone have: the relay passes on what it can\'t read. ' +
    'The AC goes to its hub straight, so it answers with the TV off.</p><div class="btns"><button class="btn" id="unpair">' + ico('key') + 'forget the key</button></div>';
  return '<a class="tilebtn" style="--c:' + C.cobalt + ';min-height:150px;margin-top:4px;display:flex;color:inherit;text-decoration:none" id="anyw" target="_blank" rel="noopener">' + ico('globe') +
    '<div><div class="mid">use Metro anywhere</div><div class="s" id="anys">the same app away from home, no VPN</div></div></a>' +
    '<p class="sub" style="margin-top:14px">It opens Metro\'s public page already paired, and copies the key. There: Share › Add to Home Screen, and use that one from now on, at home and away. ' +
    'The TV and the AC hub reach it through a public relay that only ever carries sealed messages.</p><div id="anyst" class="sub"></div>' +
    '<div class="btns"><button class="btn" id="newkey">' + ico('key') + 'new key</button></div><p class="sub">A new key shuts out every phone paired before.</p>';
}
// copied on the Wi-Fi too (where the phone's clipboard API isn't allowed: the page isn't https)
function copyText(t) {
  try { if (navigator.clipboard && window.isSecureContext) { navigator.clipboard.writeText(t); return; } } catch (e) { }
  const a = document.createElement('textarea');
  a.value = t; a.setAttribute('readonly', ''); a.style.cssText = 'position:fixed;top:0;opacity:0';
  document.body.appendChild(a); a.select(); a.setSelectionRange(0, t.length);
  try { document.execCommand('copy'); } catch (e) { }
  a.remove();
}
function wireAnywhere(panel) {
  const any = panel.querySelector('#anyw');
  if (any) {
    let k = '';
    const show = (j) => {
      if (!j || !j.ok) return;
      panel.querySelector('#anyst').textContent = j.key ? 'relay ' + (j.online ? 'connected' : 'starting') + ' · the AC hub ' + (j.hub ? 'has the key' : 'gets the key when it next answers the TV') : 'not set up yet';
      if (j.link) { any.href = j.link; k = j.key; panel.querySelector('#anys').textContent = 'opens Metro\'s public page, paired'; }
    };
    call('relay', 'peek=1').then(show, () => { });
    any.onclick = async (e) => {
      if (any.href) { copyText(k); return; }
      e.preventDefault();
      const j = await act('relay', '', true);
      show(j);
      if (j && j.link) toast('Set up: tap again to open it');
    };
    panel.querySelector('#newkey').onclick = async () => {
      if (!confirm('A new key: every phone paired before stops working away from home. Go on?')) return;
      show(await act('relay', 'do=new', true));
      toast('A new key: pair again');
    };
  }
  const un = panel.querySelector('#unpair');
  if (un) un.onclick = () => { if (confirm('Forget the key? This app stops working until it is paired again at home.')) { Relay.forget(); location.reload(); } };
}
function wire(panel) {
  wireAnywhere(panel);
  panel.querySelectorAll('[data-k]').forEach((b) => {
    let rep = null;
    const go = () => key(b.dataset.k);
    b.addEventListener('touchstart', (e) => {
      e.preventDefault(); b.classList.add('down'); go();
      // a held arrow or volume repeats, as on the remote
      if (/up|down|left|right|volup|voldown/.test(b.dataset.k)) rep = setTimeout(function r() { go(); rep = setTimeout(r, 170); }, 450);
    }, { passive: false });
    ['touchend', 'touchcancel'].forEach((ev) => b.addEventListener(ev, () => { clearTimeout(rep); b.classList.remove('down'); }));
    b.addEventListener('click', () => { if (!('ontouchstart' in window)) go(); });
  });
  panel.querySelectorAll('[data-quick]').forEach((b) => (b.onclick = () => act('quick', '', true)));
  panel.querySelectorAll('[data-act]').forEach((b) => (b.onclick = () => act(b.dataset.act, b.dataset.arg || '')));
  panel.querySelectorAll('[data-send]').forEach((b) => (b.onclick = () => {
    const f = panel.querySelector('input[data-for="' + b.dataset.send + '"]');
    if (!f.value.trim()) return;
    act(b.dataset.send, q(f.dataset.arg, f.value.trim())).then((j) => { if (j && j.ok) f.value = ''; });
  }));
  panel.querySelectorAll('.news:not([data-story])').forEach((n) => (n.onclick = () => n.classList.toggle('open')));
  panel.querySelectorAll('[data-g]').forEach((b) => { press(b); b.onclick = () => openGame(games[+b.dataset.g]); });
  panel.querySelectorAll('[data-story]').forEach((b) => { press(b); b.onclick = () => open('story', 0, storyList[+b.dataset.story]); });
  panel.querySelectorAll('[data-t]').forEach((el) => { press(el); el.onclick = () => openTitle(picked[+el.dataset.t]); });
  const t = page && page.show;
  panel.querySelectorAll('[data-play]').forEach((b) => (b.onclick = () => act('play', q('id', t.id))));
  panel.querySelectorAll('[data-show]').forEach((b) => (b.onclick = () => act('show', q('id', t.id))));
  panel.querySelectorAll('[data-trailer]').forEach((b) => (b.onclick = () => act('youtube', q('url', 'https://www.youtube.com/watch?v=' + t.trailer))));
  panel.querySelectorAll('[data-radio]').forEach((b) => (b.onclick = async () => { await act('radio', q('id', b.dataset.radio)); setTimeout(load, 2500); }));
  panel.querySelectorAll('[data-radio-do]').forEach((b) => (b.onclick = async () => { await act('radio', q('do', b.dataset.radioDo), true); setTimeout(load, 1500); }));
  panel.querySelectorAll('[data-launch]').forEach((b) => (b.onclick = () => act('launch', q('pkg', b.dataset.launch))));
  panel.querySelectorAll('[data-timer]').forEach((b) => (b.onclick = async () => { await act('timer', b.dataset.timer); setTimeout(load, 800); }));
  panel.querySelectorAll('[data-video]').forEach((b) => (b.onclick = () => act('youtube', q('url', 'https://www.youtube.com/watch?v=' + b.dataset.video))));
  panel.querySelectorAll('[data-accent]').forEach((b) => (b.onclick = async () => {
    const i = +b.dataset.accent;
    panel.querySelectorAll('[data-accent]').forEach((x) => x.classList.toggle('on', x === b));
    S.accent = (S.settings.accents[i] || {}).tile || S.accent; S.settings.accent = i; applyAccent();
    await act('set', q('accent', i), true);
  }));
  panel.querySelectorAll('[data-set]').forEach((b) => (b.onclick = async () => {
    const on = b.dataset.on !== '1';
    b.dataset.on = on ? '1' : '0'; b.querySelector('.switch').classList.toggle('on', on);
    S.settings[b.dataset.set] = on;
    await act('set', q(b.dataset.set, on ? 'on' : 'off'), true);
  }));
  const pad = panel.querySelector('#touch');
  if (pad) {
    let x0 = 0, y0 = 0;
    pad.addEventListener('touchstart', (e) => { e.preventDefault(); x0 = e.touches[0].clientX; y0 = e.touches[0].clientY; }, { passive: false });
    pad.addEventListener('touchmove', (e) => e.preventDefault(), { passive: false });
    pad.addEventListener('touchend', (e) => {
      const tt = e.changedTouches[0], dx = tt.clientX - x0, dy = tt.clientY - y0, r = pad.getBoundingClientRect();
      const ping = document.createElement('i'); ping.style.left = (tt.clientX - r.left) + 'px'; ping.style.top = (tt.clientY - r.top) + 'px'; pad.appendChild(ping); setTimeout(() => ping.remove(), 500);
      if (Math.max(Math.abs(dx), Math.abs(dy)) < 24) return key('ok');
      key(Math.abs(dx) > Math.abs(dy) ? (dx > 0 ? 'right' : 'left') : (dy > 0 ? 'down' : 'up'));
    });
  }
  if (panel.querySelector('#sched')) schedules();
  convWire(panel);
  const find = panel.querySelector('#find');
  if (find) {
    let timer = null;
    const go = async () => {
      const words = find.value.trim();
      const box = panel.querySelector('#found');
      if (words.length < 2) { box.innerHTML = ''; return; }
      panel.querySelector('#findnote').textContent = 'looking…';
      const j = await act('find', q('q', words), true);
      if (!j || !j.ok) return;
      panel.querySelector('#findnote').textContent = j.items.length ? j.items.length + ' for “' + words + '”' : 'nothing found for “' + words + '”';
      const base = picked.length;
      j.items.forEach((t) => picked.push(t));
      box.innerHTML = j.items.map((t, i) => '<div class="item tap" data-t="' + (base + i) + '"><img class="thumb" src="' + esc(t.poster) + '" alt="" loading="lazy"><div class="grow"><div class="t">' + esc(t.title) + '</div><div class="s">' +
        esc([t.kind, t.year, (t.genres || [])[0], t.imdb ? 'IMDb ' + t.imdb.toFixed(1) : ''].filter(Boolean).join(' · ')) + '</div><div class="s" data-where="' + esc(t.id) + '">' + (t.svc ? svcTag(t) : 'finding where it streams…') + '</div></div></div>').join('');
      box.querySelectorAll('[data-t]').forEach((el) => { press(el); el.onclick = () => openTitle(picked[+el.dataset.t]); });
      // where each one not on the services streams, a few at a time
      for (const t of j.items.filter((x) => !x.svc).slice(0, 8)) {
        const w = await call('where', q('id', t.id)).catch(() => null);
        const el = box.querySelector('[data-where="' + t.id + '"]');
        if (el && w && w.ok) el.innerHTML = w.vpn ? '<span class="svc" style="--sc:#fa6800">VPN</span> ' + esc(w.vpn) + ' · ' + esc(w.on.join(' or ')) : esc(w.line);
      }
    };
    find.addEventListener('input', () => { clearTimeout(timer); timer = setTimeout(go, 600); });
    panel.querySelector('#findgo').onclick = go;
    find.addEventListener('keydown', (e) => { if (e.key === 'Enter') { find.blur(); go(); } });
  }
  const pick = panel.querySelector('#pick');
  if (pick) {
    const files = panel.querySelector('#files');
    pick.onclick = () => files.click();
    files.onchange = async () => {
      const all = [...files.files], log = panel.querySelector('#sendlog');
      for (let i = 0; i < all.length; i++) {
        log.textContent = 'sending ' + (i + 1) + ' of ' + all.length + '…';
        try {
          const blob = await shrink(all[i], AWAY ? 1600 : 2048);
          const j = AWAY ? await Relay.photo(blob) : await (await fetch('/api/photo?code=' + encodeURIComponent(code), { method: 'POST', body: blob })).json();
          if (!j.ok) { log.textContent = j.line || "The TV didn't take it"; return; }
        } catch (e) { log.textContent = "That photo couldn't be sent"; return; }
      }
      log.textContent = all.length === 1 ? 'On the TV' : all.length + ' photos on the TV: Left and Right go through them';
      files.value = '';
    };
  }
}
// a photo at the TV's size (its longest side 2048), as a JPEG: the phone turns it the right way up as it draws it
function shrink(file, most) {
  return new Promise((ok, no) => {
    const img = new Image();
    img.onload = () => {
      const k = Math.min(1, (most || 2048) / Math.max(img.naturalWidth, img.naturalHeight));
      const c = document.createElement('canvas');
      c.width = Math.round(img.naturalWidth * k); c.height = Math.round(img.naturalHeight * k);
      c.getContext('2d').drawImage(img, 0, 0, c.width, c.height);
      URL.revokeObjectURL(img.src);
      c.toBlob((b) => (b ? ok(b) : no()), 'image/jpeg', 0.88);
    };
    img.onerror = no;
    img.src = URL.createObjectURL(file);
  });
}
let keyBusy = Promise.resolve();
function key(k) { if (AWAY) act('key', q('k', k), true); else keyBusy = keyBusy.then(() => act('key', q('k', k), true)); }

/* ---------------------------------------------------------------- the AC's schedules */
let items = [];
async function schedules(set) {
  const j = await act('schedules', set == null ? '' : q('set', set), true);
  const box = $('#slist'), head = $('#sched');
  if (!box) return;
  if (!j || !j.ok) { head.textContent = (j && j.line) || "The hub didn't answer"; return; }
  if (set != null) toast('Saved on the hub');
  items = j.items;
  head.textContent = "the hub's clock: " + j.now + (j.last ? ' · last: ' + j.last : '');
  box.innerHTML = items.length ? items.map((it, i) => '<div class="item"><div class="grow"><div class="t" style="font-size:34px;font-weight:300">' + it.at + '</div><div class="s">' + esc(it.said) +
    '</div></div><button class="btn" data-rm="' + i + '">remove</button></div>').join('') : none('No times yet');
  box.querySelectorAll('[data-rm]').forEach((b) => (b.onclick = () => save(items.filter((_, k) => k !== +b.dataset.rm))));
  $('#add').onclick = () => { const at = $('#at').value; if (at) save(items.concat([{ at, days: +$('#days').value }])); };
}
const save = (l) => schedules(l.map((i) => i.at.replace(':', '') + ':' + i.days).join(','));

/* ---------------------------------------------------------------- the app bar: icons, no frames, spaced out */
function drawBar() {
  const b = (i, n, f, on) => '<button data-b="' + f + '"' + (on ? ' class="on"' : '') + '>' + ico(i) + '<span>' + n + '</span></button>';
  const id = page && page.id;
  $('#bar').innerHTML = page
    ? b('back', 'back', 'back') + (id === 'title' || TV[id] ? b('tv', 'on TV', 'tv') : '') + b('pad', 'remote', 'remote', id === 'remote') + b('film', 'films', 'films', id === 'films') + b('refresh', 'refresh', 'refresh')
    : b('pad', 'remote', 'remote') + b('film', 'films', 'films') + b('radio', 'radio', 'radio') + b('power', 'air con', 'ac') + b('gear', 'settings', 'settings');
  $('#bar').querySelectorAll('button').forEach((x) => (x.onclick = () => {
    const f = x.dataset.b;
    if (f === 'back') history.back();
    else if (f === 'tv') { if (id === 'title') act('show', q('id', page.show.id)); else act('open', q('what', TV[id])); }
    else if (f === 'refresh') { toast('Asking the TV…'); load(); }
    else if (page && page.id === f && pivot) pivot.to(0);
    else open(f, 0);
  }));
}

/* ---------------------------------------------------------------- scores and team news from the internet, with the TV off */
// ESPN as the TV reads it (it lets a page ask it straight): every game today in the big leagues and the men's
// internationals, Bayern's and Germany's next game, results and tables, their news, a match in full, a story, and Sri
// Lanka's or India's scorecard. Away from home with the TV off, every two minutes while something is live, else ten.
const ESPN = 'https://site.api.espn.com/apis/site/v2/sports/soccer';
const ESPN_CRICKET = 'https://site.web.api.espn.com/apis';
const LEAGUES = [['eng.1', 'Premier League'], ['ger.1', 'Bundesliga'], ['esp.1', 'LaLiga'], ['uefa.champions', 'Champions League'], ['ita.1', 'Serie A'],
  ['ger.dfb_pokal', 'DFB-Pokal'], ['uefa.europa', 'Europa League'], ['fra.1', 'Ligue 1']];
const NATIONS = ['India', 'Sri Lanka', 'Australia', 'England', 'South Africa', 'New Zealand', 'Pakistan', 'Bangladesh', 'West Indies', 'Afghanistan', 'Ireland',
  'Zimbabwe', 'United Arab Emirates', 'Netherlands', 'Scotland', 'Nepal'];
const CRICKET_LEAGUES = ['Indian Premier League', 'World Cup', 'Asia Cup', 'Champions Trophy'];
const NOT_SENIOR = /under[- ]?\d\d|\bu-?\d\d\b|emerging|rising stars|legends|masters/i, YOUTH = /under[- ]?\d\d|\bu-?\d\d\b|legends|masters/i, A_TEAM = /\sA(\s|$)/;
const STOPPED = /\b(stumps|lunch|tea|rain|bad light|innings break|delayed)\b/i;
const OURS = ['Sri Lanka', 'India'];
const ourSide = (n) => OURS.some((x) => n === x || n === x + ' A');
const ourCricket = (g) => g.sport === 'Cricket' && (ourSide(g.home.name) || ourSide(g.away.name));
const kept = new Map();
// ESPN's answer, kept a while (a past or coming day's scoreboard for half an hour, today's for under a minute)
async function espn(u, keepMs) {
  const k = kept.get(u);
  if (k && Date.now() - k[0] < (keepMs || 50000)) return k[1];
  const r = await fetch(u);
  if (!r.ok) throw new Error('ESPN ' + r.status);
  const j = await r.json();
  kept.set(u, [Date.now(), j]);
  return j;
}
const dayIn = (ms) => new Date(ms).toLocaleDateString('en-CA', TZ);
function kickoff(ms) {
  const d = dayIn(ms);
  return d === dayIn(Date.now()) ? 'Today ' + hm(ms) : d === dayIn(Date.now() + 864e5) ? 'Tomorrow ' + hm(ms) :
    new Date(ms).toLocaleDateString('en-GB', { weekday: 'short', day: 'numeric', month: 'short', ...TZ }) + ' · ' + hm(ms);
}
function whenLabel(ms) {
  const d = dayIn(ms);
  return (d === dayIn(Date.now()) ? 'Today' : d === dayIn(Date.now() + 864e5) ? 'Tomorrow' : new Date(ms).toLocaleDateString('en-GB', { weekday: 'short', ...TZ })) + ' ' + hm(ms);
}
const tidyLeague = (l) => { const s = String(l || '').replace(/^\d{4}([-/]\d{2,4})?\s+/, '');
  return /Bundesliga/.test(s) ? 'Bundesliga' : /Champions League/.test(s) ? 'Champions League' : /German Cup|DFB/.test(s) ? 'DFB-Pokal' : /super ?cup/i.test(s) ? 'Supercup' :
    /Club World Cup/.test(s) ? 'Club World Cup' : /friendl/i.test(s) ? 'Friendly' : s; };
const unent = (s) => { if (!s || s.indexOf('&') < 0) return s || ''; const t = document.createElement('textarea'); t.innerHTML = s; return t.value; };
const plainE = (s) => unent(s).trim().replace(/\binns\b/g, 'innings').replace(/\bwkts?\b/g, (w) => (w === 'wkt' ? 'wicket' : 'wickets'))
  .replace(/\((\d+)b rem\)/g, (_, n) => (+n === 0 ? 'off the last ball' : 'with ' + n + ' ' + (+n === 1 ? 'ball' : 'balls') + ' to spare'));
const shortScore = (s) => String(s || '').replace(/,\s*target \d+/, '').replace(/\/\d+ ov/, ' ov').trim();
const gameOf = (o) => Object.assign({ leagueId: '', note: '', detail: '' }, o, { live: o.state === 'in' && !STOPPED.test(o.status) });
function sideOf(o, club) {
  const t = o.team || o;
  const logo = (t.logos && t.logos[0] && t.logos[0].href) || t.logo || o.logo || '';
  const score = o.score == null ? '' : typeof o.score === 'object' ? o.score.displayValue || '' : String(o.score);
  const name = club && t.displayName === 'Bayern Munich' ? 'Bayern' : t.displayName || '';
  return { name, short: (club ? t.shortDisplayName || t.abbreviation : t.shortDisplayName || t.abbreviation) || '', score, logo, color: t.color ? '#' + t.color : '' };
}
// one event of a team's schedule (as the TV's Bayern.game)
function teamGame(e) {
  const c = e.competitions[0], cs = c.competitors, h = cs.find((x) => x.homeAway === 'home'), a = cs.find((x) => x.homeAway === 'away');
  const st = c.status || e.status, type = st.type, state = type.state, start = Date.parse(e.date);
  const status = state === 'in' ? st.displayClock || type.shortDetail : state === 'post' ? type.shortDetail || 'FT' : kickoff(start);
  return gameOf({ id: e.id, sport: 'Football', league: tidyLeague((e.league && e.league.name) || (e.season && e.season.displayName) || ''), home: sideOf(h, true), away: sideOf(a, true),
    state, status, detail: (c.venue && c.venue.fullName) || '', start });
}
const events = (j) => (j.events || []).map((e) => { try { return teamGame(e); } catch (x) { return null; } }).filter(Boolean);

// every game today: the leagues day by day (yesterday to four days on; the European nights yesterday to tomorrow), and cricket
async function soccer(code, league) {
  const days = code.startsWith('uefa.') ? [-1, 0, 1] : [-1, 0, 1, 2, 3, 4];
  const boards = await Promise.all(days.map((d) => espn(ESPN + '/' + code + '/scoreboard?dates=' + new Date(Date.now() + d * 864e5).toISOString().slice(0, 10).replace(/-/g, ''), d === 0 ? 50000 : 1800000).catch(() => null)));
  const out = [];
  boards.filter(Boolean).forEach((b) => (b.events || []).forEach((e) => { try {
    const c = e.competitions[0], h = c.competitors.find((x) => x.homeAway === 'home'), a = c.competitors.find((x) => x.homeAway === 'away');
    const type = e.status.type, state = type.state, start = Date.parse(e.date);
    const status = state === 'in' ? e.status.displayClock || type.shortDetail : state === 'post' ? type.shortDetail || 'FT' : whenLabel(start);
    out.push(gameOf({ id: e.id, sport: 'Football', league, home: sideOf(h), away: sideOf(a), state, status, detail: (c.venue && c.venue.fullName) || '', start }));
  } catch (x) { } }));
  return out;
}
async function cricketGames() {
  const j = await espn(ESPN_CRICKET + '/v2/scoreboard/header?sport=cricket&region=in&lang=en', 50000);
  const out = [];
  (j.sports[0].leagues || []).forEach((lg) => { const lname = lg.name || '';
    (lg.events || []).forEach((e) => { try {
      const cs = e.competitors || [];
      if (cs.length < 2) return;
      const names = cs.map((x) => x.displayName || '');
      if (/women/i.test(lname) || names.some((n) => /women/i.test(n))) return;
      const aSides = names.every((n) => A_TEAM.test(n) && NATIONS.includes(n.replace(/ A$/, '').trim()));
      if (aSides) { if ([lname].concat(names).some((n) => YOUTH.test(n))) return; }
      else if ([lname].concat(names).some((n) => NOT_SENIOR.test(n) || A_TEAM.test(n))) return;
      if (!(aSides || names.every((n) => NATIONS.includes(n)) || CRICKET_LEAGUES.some((l) => lname.toLowerCase().includes(l.toLowerCase())))) return;
      const side = (o) => ({ name: o.displayName || '', short: o.abbreviation || '', score: shortScore(o.score), logo: o.logo || '', color: o.color ? '#' + o.color : '' });
      const h = cs.find((x) => x.homeAway === 'home') || cs[0], a = cs.find((x) => x.homeAway === 'away') || cs[1];
      const state = e.status, start = Date.parse(e.date) || 0;
      const status = state === 'pre' ? whenLabel(start) : e.summary || (state === 'post' ? 'Result' : 'Live');
      const full = e.fullStatus || {};
      const note = state === 'pre' ? '' : plainE(full.longSummary || full.summary || '');
      out.push(gameOf({ id: e.id, sport: 'Cricket', league: lname.split(' 20')[0].trim(), home: side(h), away: side(a), state, status,
        detail: [e.title || e.eventType || '', (e.location || '').split(', ').pop()].filter(Boolean).join(' · '), start, leagueId: String(lg.id || ''), note: note !== status ? note : '' }));
    } catch (x) { } }); });
  return out;
}
async function cloudGames() {
  const all = (await Promise.all(LEAGUES.map(([c, n]) => soccer(c, n).catch(() => [])).concat([cricketGames().catch(() => [])]))).flat();
  const now = Date.now(), seenIds = new Set();
  const games = all.filter((g) => g.state === 'in' || (g.state === 'post' ? now - g.start < (g.sport === 'Cricket' ? 144 : 30) * 3600000 : g.start - now < (g.sport === 'Football' ? 5 : 4) * 864e5))
    .filter((g) => !seenIds.has(g.id) && seenIds.add(g.id));
  if (!games.length) return false;
  const rank = (g) => (g.state === 'in' ? 0 : g.state === 'post' ? 1 : 2);
  S.games = games.slice().sort((a, b) => (a.state !== 'in') - (b.state !== 'in') || a.league.localeCompare(b.league) || a.start - b.start).slice(0, 60);
  S.live = games.filter((g) => g.state === 'in').slice(0, 12);
  S.cricket = games.filter(ourCricket).sort((a, b) => a.start - b.start);
  const ours = games.find((g) => ourCricket(g) && g.state === 'in') || games.filter((g) => ourCricket(g) && g.state === 'post').sort((a, b) => b.start - a.start)[0];
  if (ours) { const sc = await cloudScorecard(ours.leagueId, ours.id, ours).catch(() => null); if (sc && sc.ok) S.scorecard = sc; }
  return games.sort((a, b) => rank(a) - rank(b)).some((g) => g.live);
}

// Bayern (132) or Germany (481), as the TV's team page has it
const outcome = (g, team) => { const us = g.home.name.includes(team) ? g.home : g.away, them = us === g.home ? g.away : g.home;
  const a = parseInt(us.score, 10), b = parseInt(them.score, 10); return isNaN(a) || isNaN(b) ? '-' : a > b ? 'W' : a < b ? 'L' : 'D'; };
async function espnTable(code, name) {
  const j = await espn('https://site.api.espn.com/apis/v2/sports/soccer/' + code + '/standings', 600000);
  const g = (j.children && j.children[0]) || j;
  return { name, rows: g.standings.entries.map((e) => { const st = {}; (e.stats || []).forEach((s) => (st[s.name] = s));
    const n = (k) => (st[k] && isFinite(st[k].value) ? Math.round(st[k].value) : 0);
    const t = e.team, short = t.shortDisplayName === 'Bayern Munich' ? 'Bayern' : t.shortDisplayName || t.displayName;
    return { rank: n('rank'), team: short, logo: (t.logos && t.logos[0] && t.logos[0].href) || '', played: n('gamesPlayed'), gd: (st.pointDifferential && st.pointDifferential.displayValue) || '',
      points: n('points'), us: t.id === '132' }; }).sort((a, b) => a.rank - b.rank) };
}
async function espnLeaders() {
  const j = await espn(ESPN + '/ger.1/teams/132/roster', 3600000);
  const ps = (j.athletes || []).map((p) => { const st = {};
    (((p.statistics || {}).splits || {}).categories || []).forEach((c) => (c.stats || []).forEach((s) => (st[s.name] = Math.round(s.value || 0))));
    return { name: p.displayName, number: p.jersey || '', st }; });
  const top = (k, what) => ps.filter((p) => p.st[k] > 0).sort((a, b) => b.st[k] - a.st[k]).slice(0, 3).map((p) => ({ what, name: p.name, number: p.number, value: p.st[k] }));
  return top('totalGoals', 'Goals').concat(top('goalAssists', 'Assists'));
}
async function club(id, name) {
  const year = new Date().getFullYear();
  const fixtures = espn(ESPN + '/all/teams/' + id + '/schedule?fixture=true', 120000).then(events);
  const results = id === '132' ? Promise.all(['ger.1', 'uefa.champions', 'ger.dfb_pokal'].map((c) => espn(ESPN + '/' + c + '/teams/132/schedule', 600000).then(events).catch(() => [])))
    : Promise.all([year, year - 1].map((y) => espn(ESPN + '/all/teams/481/schedule?season=' + y, 600000).then(events).catch(() => [])));
  const tables = id === '132' ? Promise.all([espnTable('ger.1', 'Bundesliga').catch(() => null), espnTable('uefa.champions', 'Champions League').catch(() => null)]) : Promise.resolve([]);
  const lead = id === '132' ? espnLeaders().catch(() => []) : Promise.resolve([]);
  let fx = (await fixtures).filter((g) => g.state !== 'post').sort((a, b) => a.start - b.start);
  // around kick-off, the match's own summary has the live score
  const near = fx.find((g) => g.live || (g.start - Date.now() < 600000 && Date.now() - g.start < 3 * 3600000));
  if (near) { const m = await cloudMatch(near.id).catch(() => null); if (m && m.game) fx = fx.map((g) => (g.id === near.id ? m.game : g)).filter((g) => g.state !== 'post'); }
  const seenIds = new Set();
  const res = (await results).flat().filter((g) => g.state === 'post' && !seenIds.has(g.id) && seenIds.add(g.id)).sort((a, b) => b.start - a.start).slice(0, 8);
  const tl = (await tables).filter(Boolean);
  return { name, tables: tl, leaders: await lead, live: fx.find((g) => g.live) || null, next: fx.find((g) => g.state === 'pre') || null,
    fixtures: fx.filter((g) => g.state === 'pre').slice(0, 6), results: res.slice(0, 6).map((g) => Object.assign({}, g, { outcome: outcome(g, name) })), table: tl[0] || null };
}
async function espnNews(id) {
  const j = await espn(ESPN + '/all/news?limit=24&team=' + id, 1200000);
  const seenT = new Set();
  return (j.articles || []).filter((a) => a.type !== 'Media' && !a.premium && a.id && a.headline && !seenT.has(a.headline) && seenT.add(a.headline)).map((a) => {
    const im = (a.images || []).find((x) => /\/photo\//.test(x.url || '')) || (a.images || [])[0];
    return { id: String(a.id), title: a.headline.trim(), text: (a.description || '').trim(), image: (im && im.url) || '', at: Date.parse(a.published) || 0, by: (a.byline || '').trim() };
  }).sort((a, b) => b.at - a.at).slice(0, 10);
}
async function cloudTeams() {
  const [b, g, nb, ng] = await Promise.all([club('132', 'Bayern').catch(() => null), club('481', 'Germany').catch(() => null), espnNews('132').catch(() => null), espnNews('481').catch(() => null)]);
  if (b) S.bayern = b;
  if (g) S.germany = g;
  if (nb || ng) S.teamNews = { bayern: nb || (S.teamNews || {}).bayern || [], germany: ng || (S.teamNews || {}).germany || [] };
  return !!((b && b.live) || (g && g.live));
}

// a story in full: ESPN's text as paragraphs and headings
async function cloudStory(id) {
  const j = await espn('https://content.core.api.espn.com/v1/sports/news/' + encodeURIComponent(id), 86400000);
  const html = (((j.headlines || [])[0]) || {}).story || '';
  const doc = new DOMParser().parseFromString(html, 'text/html');
  const paras = [...doc.querySelectorAll('p, h2, h3, h4, li, blockquote')].filter((e) => !e.closest('li p, blockquote p') || e.tagName !== 'P')
    .map((e) => ({ text: e.textContent.replace(/[ \t ]+/g, ' ').trim(), head: /^H/.test(e.tagName) })).filter((p) => p.text);
  return { ok: paras.length > 0, paras };
}

// a football match in full, from ESPN's summary (the TV adds FotMob's ratings and photos; the phone has ESPN's alone)
const STATS = [['possessionPct', 'possession'], ['totalShots', 'shots'], ['shotsOnTarget', 'shots on target'], ['wonCorners', 'corners'], ['saves', 'saves'], ['totalPasses', 'passes'],
  ['totalTackles', 'tackles'], ['offsides', 'offsides'], ['foulsCommitted', 'fouls'], ['yellowCards', 'yellow cards'], ['redCards', 'red cards']];
function kindOfPlay(type, text) {
  const t = (type || '').toLowerCase();
  if (t.includes('own-goal') || /^own goal/i.test(text)) return 'own';
  if (t.startsWith('goal') || t === 'penalty---scored' || text.startsWith('Goal!')) return t.includes('penalty') || /penalty(?! area)/i.test(text) ? 'pen' : 'goal';
  if (t.includes('red-card') || t.includes('second-yellow') || /second yellow/i.test(text) || (t.includes('card') && /red card/i.test(text))) return 'red';
  if (t.includes('yellow-card')) return 'yellow';
  if (t.startsWith('substitution')) return 'sub';
  if (t.startsWith('var') || text.startsWith('VAR')) return 'var';
  if (t === 'kickoff' || t === 'halftime' || ((t.startsWith('start-') || t.startsWith('end-')) && !t.includes('delay')) ||
    (!t && /^(Match ends|First Half begins|Second Half begins)|minutes? of added time/.test(text))) return 'whistle';
  if (/^(shot|attempt|penalty)|woodwork|post|bar/.test(t)) return 'chance';
  return null;
}
// each side's colour for the bars, as the TV's: readable on black (else white), and told apart from the other's
function tones(h, a) {
  const rgb = (x) => { const m = /^#?([0-9a-f]{6})$/i.exec(x || ''); if (!m) return null; const n = parseInt(m[1], 16); return [(n >> 16) / 255, ((n >> 8) & 255) / 255, (n & 255) / 255]; };
  const lum = (c) => { const f = (v) => (v <= 0.03928 ? v / 12.92 : Math.pow((v + 0.055) / 1.055, 2.4)); return 0.2126 * f(c[0]) + 0.7152 * f(c[1]) + 0.0722 * f(c[2]); };
  const hex = (c) => '#' + c.map((v) => Math.round(v * 255).toString(16).padStart(2, '0')).join('');
  const ok = (c) => c && lum(c) > 0.04 && hex(c) !== '#8b8fa3';
  const near = (x, y) => Math.abs(x[0] - y[0]) + Math.abs(x[1] - y[1]) + Math.abs(x[2] - y[2]) < 0.5;
  const W = [1, 1, 1], B = [0x1b / 255, 0xa1 / 255, 0xe2 / 255];
  const home = ok(rgb(h)) ? rgb(h) : W;
  let away = ok(rgb(a)) ? rgb(a) : W;
  if (near(home, away)) away = near(home, W) ? B : W;
  return [hex(home), hex(away)];
}
async function cloudMatch(id) {
  const d = await espn(ESPN + '/all/summary?event=' + encodeURIComponent(id), 25000);
  const header = d.header, comp = header.competitions[0], cs = comp.competitors;
  const h = cs.find((x) => x.homeAway === 'home'), a = cs.find((x) => x.homeAway === 'away');
  const homeId = h.team.id, awayId = a.team.id, st = comp.status, type = st.type, state = type.state;
  const start = Date.parse(comp.date || header.date) || 0;
  const status = state === 'in' ? st.displayClock || type.shortDetail : state === 'post' ? type.shortDetail || 'FT' : kickoff(start);
  const venue = (((d.gameInfo || {}).venue) || {}).fullName || '';
  const game = gameOf({ id: String(id), sport: 'Football', league: tidyLeague((header.league || {}).name), home: sideOf(h, true), away: sideOf(a, true), state, status, detail: venue, start });
  const moments = (d.keyEvents || []).map((k) => {
    const t = k.type || {}, text = t.text || '';
    if (t.type === 'halftime') return { min: 'HT', kind: 'ht', who: '', extra: '', home: false };
    const kind = /Own Goal/i.test(text) ? 'own' : /Penalty/i.test(text) && (k.scoringPlay || /Scored/i.test(text)) ? 'pen' : k.scoringPlay || text.startsWith('Goal') ? 'goal' :
      /Red Card/i.test(text) ? 'red' : /Yellow Card/i.test(text) ? 'yellow' : /Substitution/i.test(text) ? 'sub' : null;
    if (!kind) return null;
    const who = (n) => ((((k.participants || [])[n] || {}).athlete) || {}).displayName || '';
    return { min: (k.clock || {}).displayValue || '', kind, who: who(0), extra: who(1), home: (k.team || {}).id === homeId };
  }).filter(Boolean);
  const box = {};
  ((d.boxscore || {}).teams || []).forEach((t) => { const side = t.homeAway || ((t.team || {}).id === homeId ? 'home' : 'away'); box[side] = {}; (t.statistics || []).forEach((s) => (box[side][s.name] = s.displayValue)); });
  const hs = box.home || {}, as = box.away || {};
  const num = (s) => parseFloat((/-?\d+(\.\d+)?/.exec(s || '') || [0])[0]) || 0;
  const stats = state === 'pre' ? [] : STATS.filter(([k]) => hs[k] != null && as[k] != null).map(([k, label]) => {
    const x = num(hs[k]), y = num(as[k]), pct = k === 'possessionPct';
    if (label === 'red cards' && x + y === 0) return null;
    return { label, home: pct ? Math.trunc(x) + '%' : hs[k], away: pct ? Math.trunc(y) + '%' : as[k], share: x + y ? x / (x + y) : 0.5 };
  }).filter(Boolean);
  const lineup = (side) => {
    const r = (d.rosters || []).find((x) => x.homeAway === side);
    if (!r) return null;
    const mine = moments.filter((m) => m.home === (side === 'home'));
    const last = {};
    (r.roster || []).forEach((p) => p.athlete && (last[p.athlete.displayName] = p.athlete.lastName || ''));
    const all = (r.roster || []).map((p) => {
      const name = (p.athlete || {}).displayName || '', subIn = !!p.subbedIn, subOut = !!p.subbedOut;
      const inFor = subIn ? ((mine.find((m) => m.kind === 'sub' && m.who === name) || {}).extra || '') : '';
      const subAt = (off) => ((mine.find((m) => m.kind === 'sub' && (off ? m.extra : m.who) === name) || {}).min || '');
      return { p: { n: p.jersey || '', name, pos: (p.position || {}).abbreviation || '', rating: '', goals: mine.filter((m) => (m.kind === 'goal' || m.kind === 'pen') && m.who === name).length,
        card: mine.some((m) => m.kind === 'red' && m.who === name) ? 'red' : mine.some((m) => m.kind === 'yellow' && m.who === name) ? 'yellow' : '',
        in: subIn, out: subOut, min: subIn ? subAt(false) : subOut ? subAt(true) : '', for: last[inFor] || inFor.split(' ').pop() }, starter: !!p.starter };
    });
    const xi = all.filter((x) => x.starter).map((x) => x.p);
    if (xi.length < 10) return null;
    return { formation: r.formation || '', coach: '', kind: 'Official', note: '', starters: xi, bench: all.filter((x) => !x.starter).map((x) => x.p).sort((p, q) => (q.in - p.in) || (parseInt(p.min, 10) || 0) - (parseInt(q.min, 10) || 0)) };
  };
  const rows = [];
  (((d.standings || {}).groups) || []).forEach((g) => (((g.standings || {}).entries) || []).forEach((e) => rows.push(e)));
  const facts = (teamId, bx) => {
    const row = rows.find((r) => r.id === teamId), rs = {};
    ((row || {}).stats || []).forEach((s) => (rs[s.name] = s.displayValue));
    const l5 = (d.lastFiveGames || []).find((x) => (x.team || {}).id === teamId);
    const form = ((l5 || {}).events || []).map((e) => { const home = e.homeTeamId === teamId, us = e[home ? 'homeTeamScore' : 'awayTeamScore'], them = e[home ? 'awayTeamScore' : 'homeTeamScore'];
      return e.gameResult ? { r: e.gameResult[0], score: us !== '' && them !== '' && us != null && them != null ? us + '–' + them : '', opp: (e.opponent || {}).abbreviation || (e.opponent || {}).displayName || '' } : null; }).filter(Boolean);
    return { rank: +rs.rank || 0, points: +rs.points || 0, record: row ? (rs.wins || 0) + 'W ' + (rs.ties || 0) + 'D ' + (rs.losses || 0) + 'L' : '',
      scored: state === 'pre' ? bx.totalGoals || '' : '', conceded: state === 'pre' ? bx.goalsConceded || '' : '', form, out: [] };
  };
  // the commentary, newest first: the key moments of ESPN's play-by-play (or its key events)
  const teamOf = {};
  (d.keyEvents || []).forEach((k) => (teamOf[k.id] = (k.team || {}).id));
  const line = (p, text, min, key) => { const t = String(text || '').trim(), kind = kindOfPlay(((p || {}).type || {}).type, t);
    if (!kind) return null;
    const tid = teamOf[(p || {}).id];
    return { key: (p && p.id) || key, min, kind, text: kind === 'sub' && /^Substitution/i.test(t) && t.includes('. ') ? t.slice(t.indexOf('. ') + 2) : t, home: tid === homeId ? true : tid === awayId ? false : null }; };
  const plays = d.commentary || [];
  const lines = state === 'pre' ? [] : (plays.length ? plays.map((c) => line(c.play, c.text || (c.play || {}).text, ((c.time || {}).displayValue) || (((c.play || {}).clock || {}).displayValue) || '', 'c' + c.sequence))
    : (d.keyEvents || []).map((k, i) => line(k, k.text, (k.clock || {}).displayValue || '', 'k' + i))).filter(Boolean).reverse();
  const keys = new Set();
  const commentary = lines.filter((l) => !keys.has(l.key) && keys.add(l.key)).slice(0, 80);
  // the earlier meetings (friendlies left out), from this match's sides
  let h2h = null;
  const series = (d.seasonseries || []).find((s) => s.type === 'head-to-head');
  if (series) {
    const ms = (series.events || []).map((e) => {
      if (!((e.statusType || {}).completed || e.status === 'post') || /friendl/i.test(e.competitionName || '')) return null;
      const eh = (e.competitors || []).find((x) => x.homeAway === 'home'), ea = (e.competitors || []).find((x) => x.homeAway === 'away');
      if (!eh || !ea) return null;
      const atHome = (eh.team || {}).id === homeId ? true : (eh.team || {}).id === awayId ? false : null;
      const hg = parseInt(eh.score, 10), ag = parseInt(ea.score, 10), t = Date.parse(e.date);
      if (atHome == null || isNaN(hg) || isNaN(ag) || !t || Math.abs(t - start) < 6 * 3600000) return null;
      return { at: t, comp: tidyLeague(e.competitionName || ''), homeAtHome: atHome, hg: atHome ? hg : ag, ag: atHome ? ag : hg };
    }).filter(Boolean).sort((x, y) => y.at - x.at);
    if (ms.length) h2h = { wins: ms.filter((m) => m.hg > m.ag).length, draws: ms.filter((m) => m.hg === m.ag).length, losses: ms.filter((m) => m.hg < m.ag).length,
      since: new Date(ms[ms.length - 1].at).getUTCFullYear(), meetings: ms.slice(0, 5) };
  }
  const [homeTone, awayTone] = tones(game.home.color, game.away.color);
  return { ok: true, game, venue, moments, stats, homeLineup: lineup('home'), awayLineup: lineup('away'), homeFacts: facts(homeId, hs), awayFacts: facts(awayId, as),
    commentary, h2h, homeTone, awayTone };
}

// a cricket scorecard, from ESPN's summary (as the TV's: each innings' batters with how they were out, and the bowlers)
async function cloudScorecard(leagueId, id, g) {
  const d = await espn(ESPN_CRICKET + '/site/v2/sports/cricket/' + encodeURIComponent(leagueId) + '/summary?event=' + encodeURIComponent(id), 45000);
  const header = d.header, comp = header.competitions[0], st = comp.status || {}, state = ((st.type || {}).state) || '', kind = ((st.type || {}).description) || '';
  const summary = plainE(st.summary || '').replace(/ & /g, ' and ') || kind;
  const playing = state === 'in' && !STOPPED.test(kind) && !STOPPED.test(summary);
  const statMap = (cats) => { const o = {}; (cats || []).forEach((c) => (c.stats || []).forEach((s) => (o[s.name] = s.displayValue))); return o; };
  const heads = {};
  (comp.competitors || []).forEach((c) => (c.linescores || []).filter((l) => l.isBatting).forEach((l) => {
    const allOut = /all out/i.test(l.description || '') || l.wickets >= 10, dec = /dec/i.test(l.description || '');
    heads[l.period] = { team: (c.team || {}).displayName || '', l, overs: l.overs, allOut, dec, text: (allOut ? '' + l.runs : l.runs + '/' + l.wickets) + (dec ? 'd' : '') };
  }));
  const last = Math.max(0, ...Object.keys(heads).map(Number));
  const lh = heads[last];
  const lgen = lh ? statMap(((lh.l.statistics || {}).categories)) : {};
  const current = lh && state === 'in' && !lh.allOut && !lh.dec && !(+lgen.ballLimit > 0 && +lgen.balls >= +lgen.ballLimit) ? last : -1;
  const howOut = (c, out) => ({ c: 'caught', b: 'bowled', lbw: 'lbw', st: 'stumped', ro: 'run out', 'run out': 'run out', 'not out': 'not out', hw: 'hit wicket' })[String(c || '').toLowerCase()] || (c || (out ? 'out' : 'not out'));
  const bat = {}, bowl = {};
  (d.rosters || []).forEach((r) => { const team = (r.team || {}).displayName || '';
    (r.roster || []).forEach((p) => { const a = p.athlete || {};
      (p.linescores || []).forEach((ls) => (ls.linescores || []).forEach((inner) => {
        const s = statMap(((inner.statistics || {}).categories));
        if (s.batted === '1') { const out = s.outs === '1';
          (bat[ls.period] = bat[ls.period] || []).push({ team, id: a.id, name: a.displayName || '', out: howOut(s.dismissalCard, out), runs: +s.runs || 0, balls: +s.ballsFaced || 0, fours: +s.fours || 0, sixes: +s.sixes || 0, notOut: !out, pos: +s.battingPosition || 99 }); }
        if (s.overs && s.overs !== '0') (bowl[ls.period] = bowl[ls.period] || []).push({ team, name: a.displayName || '', overs: s.overs, maidens: +s.maidens || 0, runs: +s.conceded || 0, wickets: +s.wickets || 0, econ: s.economyRate !== '-' ? s.economyRate || '' : '', pos: +s.bowlingPosition || 99 });
      }));
    });
  });
  const periods = [...new Set(Object.keys(heads).concat(Object.keys(bat)).map(Number))].sort((x, y) => x - y);
  const innings = periods.map((n) => {
    const h = heads[n], bs = bat[n] || [];
    const team = h ? h.team : (bs[0] || {}).team;
    if (!team) return null;
    const how = {}, outHow = {};
    ((((h || {}).l || {}).statistics || {}).overs || []).forEach((grp) => (grp || []).forEach((o) => (o.wicket || []).forEach((w) => { const t = plainE(w.shortText || ''); if (t) how[w.number] = t; })));
    (((h || {}).l || {}).fow || []).forEach((f) => { const a = f.athlete || {}; if (how[f.wicketNumber] && a.id) outHow[a.id] = how[f.wicketNumber]; });
    const ist = h ? statMap(((h.l.statistics || {}).categories)) : {};
    return { team, total: h ? h.text : '' + bs.reduce((s, b) => s + b.runs, 0), overs: h ? String(h.overs || '').replace(/\.0$/, '') : '', rate: ist.runRate && ist.runRate !== '-' ? ist.runRate : '', current: n === current,
      batters: bs.filter((b) => b.team === team).sort((x, y) => x.pos - y.pos).map((b) => ({ name: b.name, out: outHow[b.id] || (b.notOut ? (n === current && playing ? 'batting' : 'not out') : b.out), runs: b.runs, balls: b.balls, fours: b.fours, sixes: b.sixes, notOut: b.notOut })),
      bowlers: (bowl[n] || []).filter((b) => b.team !== team).sort((x, y) => x.pos - y.pos).map((b) => ({ name: b.name, overs: b.overs, maidens: b.maidens, runs: b.runs, wickets: b.wickets, econ: b.econ })) };
  }).filter(Boolean);
  const note = (t) => (((d.notes || []).find((x) => x.type === t)) || {}).text || '';
  const sides = (comp.competitors || []).map((c) => (c.team || {}).displayName || '');
  return { ok: true, title: comp.description || (header.title || '').split(' (')[0] || (g ? g.home.name + ' v ' + g.away.name : sides.join(' v ')), league: tidyLeague((header.league || {}).name),
    status: summary, live: playing, venue: (((d.gameInfo || {}).venue) || {}).fullName || '', toss: plainE(note('toss').replace(/ ,/g, ',')), innings };
}

// with the TV off: its pages that ask it for more (a match, a scorecard, a story) ask ESPN instead
const CLOUD = {
  match: (q) => cloudMatch(q.id),
  cricket: (q) => cloudScorecard(q.league, q.id),
  story: (q) => cloudStory(q.id),
};
let sportsAt = 0, sportsLive = false;
async function cloudSports() {
  if (!S || Date.now() - sportsAt < (sportsLive ? 110000 : 590000)) return false;
  sportsAt = Date.now();
  const [a, b] = await Promise.all([cloudGames().catch(() => false), cloudTeams().catch(() => false)]);
  sportsLive = !!(a || b);
  return true;
}

/* ---------------------------------------------------------------- the weather from the internet, with the TV off */
// Away from home with the TV off (no answer for five minutes), the weather comes from Open-Meteo straight, as the TV
// gets it (Dubai in full, Mannar and Mumbai), every half hour; the rest stays as the TV last said it.
const CITIES = [['Dubai', 'United Arab Emirates', 25.2048, 55.2708, 'Asia/Dubai'], ['Mannar', 'Sri Lanka', 8.981, 79.9044, 'Asia/Colombo'], ['Mumbai', 'India', 19.076, 72.8777, 'Asia/Kolkata']];
const skyOf = (c) => c <= 1 ? 'Clear' : c === 2 ? 'PartlyCloudy' : c === 3 ? 'Cloudy' : c === 45 || c === 48 ? 'Fog' : c >= 51 && c <= 57 ? 'Drizzle' : c >= 61 && c <= 67 ? 'Rain' :
  c >= 80 && c <= 82 ? 'Showers' : (c >= 71 && c <= 77) || c === 85 || c === 86 ? 'Snow' : c >= 95 ? 'Storm' : 'Cloudy';
const condOf = (c, day) => c === 0 ? (day ? 'Sunny' : 'Clear') : c === 1 ? (day ? 'Mostly sunny' : 'Mostly clear') : ({ 2: 'Partly cloudy', 3: 'Cloudy', 45: 'Fog', 48: 'Fog', 51: 'Drizzle', 53: 'Drizzle', 55: 'Drizzle',
  56: 'Freezing drizzle', 57: 'Freezing drizzle', 61: 'Light rain', 63: 'Rain', 65: 'Heavy rain', 66: 'Freezing rain', 67: 'Freezing rain', 71: 'Snow', 73: 'Snow', 75: 'Snow', 77: 'Snow',
  80: 'Showers', 81: 'Showers', 82: 'Heavy showers', 85: 'Snow showers', 86: 'Snow showers', 95: 'Thunderstorm', 96: 'Thunderstorm with hail', 99: 'Thunderstorm with hail' })[c] || 'Cloudy';
const compass = (d) => ['N', 'NE', 'E', 'SE', 'S', 'SW', 'W', 'NW'][Math.round((((d % 360) + 360) % 360) / 45) % 8];
const aqiSaid = (a) => a <= 50 ? 'Good' : a <= 100 ? 'Moderate' : a <= 150 ? 'Unhealthy for some' : a <= 200 ? 'Unhealthy' : a <= 300 ? 'Very unhealthy' : 'Hazardous';
async function skyFor([city, country, lat, lon, tz], full) {
  const at = '?latitude=' + lat + '&longitude=' + lon + '&timezone=' + encodeURIComponent(tz);
  const [f, air] = await Promise.all([
    fetch('https://api.open-meteo.com/v1/forecast' + at + '&current=temperature_2m,apparent_temperature,relative_humidity_2m,weather_code,wind_speed_10m,wind_direction_10m,is_day,uv_index,surface_pressure,cloud_cover,visibility' +
      '&hourly=temperature_2m,weather_code,is_day,precipitation_probability&daily=weather_code,temperature_2m_max,temperature_2m_min,sunrise,sunset,precipitation_probability_max&forecast_days=8').then((r) => r.json()),
    full ? fetch('https://air-quality-api.open-meteo.com/v1/air-quality' + at + '&current=us_aqi,pm2_5').then((r) => r.json()).catch(() => null) : null]);
  const c = f.current, h = f.hourly, d = f.daily, day = c.is_day === 1, a = (air && air.current) || {};
  const start = Math.max(0, h.time.findIndex((t) => t.startsWith(c.time.slice(0, 13))));
  const hours = h.time.slice(start, start + 24).map((t, k) => ({ label: k ? t.slice(11) : 'Now', temp: Math.round(h.temperature_2m[start + k]), sky: skyOf(h.weather_code[start + k]),
    day: h.is_day[start + k] === 1, rain: (h.precipitation_probability || [])[start + k] || 0 }));
  const days = d.time.slice(1, 8).map((t, k) => { const i = k + 1, dt = new Date(t + 'T12:00:00Z');
    return { day: dt.toLocaleDateString('en-GB', { weekday: 'short', timeZone: 'UTC' }), date: dt.getUTCDate() + ' ' + dt.toLocaleDateString('en-GB', { month: 'short', timeZone: 'UTC' }),
      high: Math.round(d.temperature_2m_max[i]), low: Math.round(d.temperature_2m_min[i]), sky: skyOf(d.weather_code[i]), rain: (d.precipitation_probability_max || [])[i] || 0, condition: condOf(d.weather_code[i], true) }; });
  return { city, country, temp: Math.round(c.temperature_2m), feels: Math.round(c.apparent_temperature), condition: condOf(c.weather_code, day), sky: skyOf(c.weather_code), day,
    high: Math.round(d.temperature_2m_max[0]), low: Math.round(d.temperature_2m_min[0]), humidity: Math.round(c.relative_humidity_2m), wind: Math.round(c.wind_speed_10m), windFrom: compass(c.wind_direction_10m),
    uv: Math.round(c.uv_index || 0), sunrise: d.sunrise[0].slice(11), sunset: d.sunset[0].slice(11), aqi: a.us_aqi == null ? null : a.us_aqi, aqiLabel: a.us_aqi == null ? '' : aqiSaid(a.us_aqi),
    pm25: a.pm2_5 == null ? null : Math.round(a.pm2_5), at: Date.now(), visibility: Math.round((c.visibility || 0) / 1000), pressure: Math.round(c.surface_pressure || 0), cloud: Math.round(c.cloud_cover || 0),
    time: new Date().toLocaleTimeString('en-GB', { hour: '2-digit', minute: '2-digit', timeZone: tz }), hours, days };
}
let cloudAt = 0;
async function cloud() {
  if (!AWAY || !S || Date.now() - seen < 300000) return;
  let changed = false;
  if (Date.now() - cloudAt >= 1800000) {
    const all = await Promise.all(CITIES.map((c, i) => skyFor(c, i === 0).catch(() => null)));
    if (all[0]) {
      cloudAt = Date.now();
      S.weather = Object.assign({}, S.weather, all[0]);
      const cities = all.slice(1).filter(Boolean).map((w) => Object.assign({}, w, { hours: w.hours.slice(0, 12) }));
      if (cities.length) S.cities = cities;
      changed = true;
    }
  }
  if (await cloudSports()) changed = true;
  await getFeeds();
  if (feedsIn()) changed = true;
  if (!changed || Date.now() - seen < 300000) return;
  offline(true);
  if (!page) drawStart();
  else if (pivot && !['remote', 'title', 'photos', 'films', 'settings', 'money', 'timer', 'apps', 'match', 'story'].includes(page.id)) pivot.panels().forEach((_, i) => redrawPanel(i));
}

/* ---------------------------------------------------------------- live */
// What is playing, the radio, the AC, the timer and the notifications, every four seconds while the app is open:
// only what changed is drawn again (the tiles that show it on Start; the page's item that shows it).
let liveSig = '';
async function live() {
  if (document.hidden || !S || (!code && !AWAY)) return;
  const j = await call('live', '', 5000).catch(() => null);
  if (!j || j.need) return;
  offline(false); seen = Date.now();
  const sig = JSON.stringify(j);
  if (sig === liveSig) return;
  const first = !liveSig;
  liveSig = sig;
  Object.assign(S, j);
  if (first) return;
  if (!page) redrawTiles();
  else if (['radio', 'remote', 'timer', 'notes', 'ac'].includes(page.id) && pivot) redrawPanel(pivot.at);
}
setInterval(live, 4000);

/* ---------------------------------------------------------------- start */
document.addEventListener('visibilitychange', () => { if (!document.hidden) { if (AWAY) Relay.wake(); load(); live(); } });
setInterval(() => {
  if (document.hidden) return;
  if (page && page.id === 'match' && page.show && page.show.state === 'in') return loadMatch();
  if (!(page && ['remote', 'title', 'photos', 'films', 'settings', 'money', 'timer', 'apps', 'match', 'story'].includes(page.id))) load();
  cloud();
  getFeeds().then((got) => { if (got && page && page.id === 'news' && pivot) drawPage(false); });
}, 30000);
setInterval(() => { if (!page && S) drawStatus(); }, 15000);
// away from home, the TV's app icons come through the relay too
if (AWAY) new MutationObserver(() => document.querySelectorAll('img[data-icon]:not([src])').forEach((im) => {
  im.setAttribute('src', 'data:,');
  Relay.icon(im.dataset.icon).then((u) => { if (u) im.src = u; });
})).observe(document.body, { childList: true, subtree: true });
async function boot() {
  getFeeds();
  if (AWAY) { if (!(await Relay.ready())) return pairGate(); Relay.fonts(); }
  const j = await call('hello', '', AWAY ? 9000 : 6000).catch(() => null);
  if (!j) { offline(true); if (!S) load(); cloud(); return; }
  if (j.need) gate(); else load();
}
(async () => {
  // what the TV said last time, at once; then the TV's answer now
  try {
    const kept = (code || AWAY) && JSON.parse(localStorage.getItem('metro-state') || 'null');
    seen = +(localStorage.getItem('metro-seen') || 0);
    if (kept) { S = kept; applyAccent(); drawStart(); }
    else $('#startBody').innerHTML = '<div class="greet"><b class="skel" style="width:60%;height:40px;display:block"></b></div><div class="skel" style="margin-top:20px;aspect-ratio:16/10"></div>' +
      '<p class="sub" style="margin-top:16px">getting everything from the TV…</p>';
  } catch (e) { }
  boot();
})();
