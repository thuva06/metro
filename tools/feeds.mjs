// The news and the money for Metro's phone app, made every 20 minutes by GitHub Actions (.github/workflows/feeds.yml)
// and published with the page as feeds.json: what the TV reads, for when it is off. The papers' feeds and the rates
// page can't be read by a web page itself (they don't allow it), so this reads them and the page reads this.
//   node tools/feeds.mjs <out file>
import { writeFileSync } from 'node:fs';

const UA = 'Mozilla/5.0 (Linux; Android 14) Metro';
const text = async (u, ms = 15000) => {
  const r = await fetch(u, { headers: { 'User-Agent': UA, Accept: '*/*' }, redirect: 'follow', signal: AbortSignal.timeout(ms) });
  if (!r.ok) throw new Error(u + ' ' + r.status);
  return r.text();
};
const json = async (u) => JSON.parse(await text(u));
const safe = (p, fallback = null) => p.catch((e) => { console.error(String(e)); return fallback; });

/* ---------------------------------------------------------------- the news (as the TV's News.kt) */
const KT = 'https://www.khaleejtimes.com/stories.rss';
const NATIONAL = 'https://www.thenationalnews.com/arc/outboundfeeds/rss/?outputType=xml';
const national = (c) => 'https://www.thenationalnews.com/arc/outboundfeeds/rss/category/' + c + '/?outputType=xml';
const GULF = 'https://gulfnews.com/feed';
const feed = (url, source, o = {}) => ({ url, source, keep: o.keep || [], drop: o.drop || [], picturesOnly: !!o.picturesOnly, pagePicture: !!o.pagePicture });
const SECTIONS = [
  ['uae', 'UAE', [feed(national('news/uae'), 'The National'), feed(KT, 'Khaleej Times', { keep: ['/uae/'] }), feed(GULF, 'Gulf News', { keep: ['/uae/'] })]],
  ['world', 'World', [feed('https://feeds.bbci.co.uk/news/world/rss.xml', 'BBC News'), feed(KT, 'Khaleej Times', { keep: ['/world/'] }),
    feed(NATIONAL, 'The National', { keep: ['/news/'], drop: ['/news/uae/'] })]],
  ['business', 'Business', [feed('https://feeds.bbci.co.uk/news/business/rss.xml', 'BBC News'), feed(national('business'), 'The National'), feed(KT, 'Khaleej Times', { keep: ['/business/'] })]],
  ['sport', 'Sport', [feed('https://feeds.bbci.co.uk/sport/rss.xml', 'BBC Sport'), feed(national('sport'), 'The National'), feed(KT, 'Khaleej Times', { keep: ['/sports/'] })]],
  ['tech', 'Technology', [feed('https://feeds.bbci.co.uk/news/technology/rss.xml', 'BBC News'), feed(national('future/technology'), 'The National'), feed(KT, 'Khaleej Times', { keep: ['/tech/'] })]],
  ['lk', 'Sri Lanka', [feed('https://www.dailymirror.lk/rss/breaking-news/108', 'Daily Mirror', { pagePicture: true }), feed('https://www.newswire.lk/feed/', 'Newswire'),
    feed('https://www.adaderana.lk/rss.php', 'Ada Derana')]],
  ['in', 'India', [feed('https://www.hindustantimes.com/feeds/rss/india-news/rssfeed.xml', 'Hindustan Times'), feed('https://indianexpress.com/section/india/feed/', 'The Indian Express'),
    feed('https://www.thehindu.com/news/national/feeder/default.rss', 'The Hindu', { picturesOnly: true })]],
];

const ENT = { amp: '&', lt: '<', gt: '>', quot: '"', apos: "'", nbsp: ' ', ndash: '–', mdash: '—', lsquo: '‘', rsquo: '’', ldquo: '“', rdquo: '”', hellip: '…', eacute: 'é', pound: '£', euro: '€' };
const decode = (s) => s.replace(/&(#x[0-9a-f]+|#\d+|[a-z]+);/gi, (m, e) => e[0] === '#' ? String.fromCodePoint(e[1] === 'x' || e[1] === 'X' ? parseInt(e.slice(2), 16) : +e.slice(1)) : ENT[e.toLowerCase()] ?? m);
const clean = (s) => decode(decode(s).replace(/<[^>]+>/g, ' ')).replace(/\uFFFC/g, ' ').replace(/\s+/g, ' ').trim();
const tag = (x, n) => { const m = new RegExp('<' + n + '(?:\\s[^>]*)?>([\\s\\S]*?)</' + n + '>').exec(x); return m ? m[1].trim().replace(/^<!\[CDATA\[/, '').replace(/\]\]>$/, '').trim() : ''; };
const attr = (x, el) => { const m = new RegExp('<' + el + '\\s[^>]*?url=["\']([^"\']+)["\'][^>]*>').exec(x); return m ? m[1] : ''; };
const unamp = (u) => u.replace(/&amp;/g, '&').trim();

function picture(it, description, content) {
  let img = attr(it, 'media:content') || attr(it, 'media:thumbnail');
  if (!img) { const e = attr(it, 'enclosure'); if (e && !/\.(mp3|mp4)$/.test(e)) img = e; }
  if (!img) { const m = /<image>\s*<url>([^<]+)<\/url>/.exec(it); if (m) img = m[1]; }
  if (!img) {
    const html = (description + ' ' + content).replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&quot;/g, '"').replace(/&#039;/g, "'");
    const t = (/<img\s[^>]+>/i.exec(html) || [])[0];
    if (t) {
      img = ((/\ssrc=["']([^"']+)["']/i.exec(t)) || [])[1] || '';
      const set = ((/\ssrcset=["']([^"']+)["']/i.exec(t)) || [])[1];
      if (set) {
        const sizes = set.split(',').map((e) => { const p = e.trim().split(' '); const w = parseInt(p[1], 10); return isNaN(w) ? null : [p[0], w]; }).filter(Boolean).sort((a, b) => a[1] - b[1]);
        const pick = sizes.find((s) => s[1] >= 900) || sizes[sizes.length - 1];
        if (pick) img = pick[0];
      }
    }
  }
  img = unamp(img).replace('/ace/standard/240/', '/ace/standard/976/').replace(/\/ht-img\/img\/(\d{4}\/\d{2}\/\d{2})\/\d{3,4}x\d{3,4}\//, '/ht-img/img/$1/960x540/');
  if (img.startsWith('https://images.indianexpress.com/') && !img.includes('?')) img += '?w=960';
  return img.startsWith('http') ? img.replace('http://', 'https://') : '';
}
function date(s) {
  const t = Date.parse(s);
  if (!isNaN(t)) return t;
  const m = /^(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2}):(\d{2})$/.exec(s.trim()); // Daily Mirror: Sri Lanka's time
  return m ? Date.parse(m[1] + '-' + m[2] + '-' + m[3] + 'T' + m[4] + ':' + m[5] + ':' + m[6] + '+05:30') : 0;
}
const feeds = new Map();
function items(url) {
  if (!feeds.has(url)) feeds.set(url, text(url).then((xml) => [...xml.matchAll(/<item[\s>]([\s\S]*?)<\/item>/g)].map((m) => {
    const it = m[1], title = clean(tag(it, 'title'));
    if (!title) return null;
    const description = tag(it, 'description'), content = tag(it, 'content:encoded');
    let summary = (clean(description) || clean(tag(it, 'media:description')) || clean(content)).replace(/The post .{0,300} appeared first on .*$/, '').trim();
    if (summary.toLowerCase() === title.toLowerCase()) summary = '';
    if (summary.length > 420) summary = summary.slice(0, 420).replace(/\s+\S*$/, '') + '…';
    return { title, link: unamp(tag(it, 'link')), summary, image: picture(it, description, content), published: date(tag(it, 'pubDate')) };
  }).filter(Boolean)).catch((e) => { console.error(String(e)); return []; }));
  return feeds.get(url);
}
async function pagePicture(url) {
  try { const m = /<meta[^>]+property=["']og:image["'][^>]+content=["']([^"']+)["']/.exec((await text(url, 8000)).slice(0, 60000)); return m && m[1].startsWith('http') ? unamp(m[1]) : null; } catch (e) { return null; }
}
const story = (i, source, cat) => ({ source, cat, title: i.title, summary: i.summary, image: i.image, published: i.published });
const titleKey = (t) => t.toLowerCase().replace(/[^a-z0-9]+/g, ' ').trim().slice(0, 60);

async function top() {
  const [a, b] = await Promise.all([items(KT), items('https://feeds.bbci.co.uk/news/world/rss.xml')]);
  const u = a.filter((x) => x.image).slice(0, 8).map((x) => story(x, 'Khaleej Times', 'UAE')), w = b.filter((x) => x.image).slice(0, 8).map((x) => story(x, 'BBC News', 'World'));
  const out = [];
  for (let i = 0; i < Math.max(u.length, w.length); i++) { if (u[i]) out.push(u[i]); if (w[i]) out.push(w[i]); }
  return out;
}
async function section([key, name, list]) {
  const lists = await Promise.all(list.map(async (f) => {
    const its = (await items(f.url)).filter((i) => (!f.keep.length || f.keep.some((k) => i.link.includes(k))) && !f.drop.some((k) => i.link.includes(k)) && (!f.picturesOnly || i.image))
      .sort((x, y) => y.published - x.published).slice(0, 14);
    const pics = f.pagePicture ? await Promise.all(its.map(async (i) => (await pagePicture(i.link)) || i.image)) : its.map((i) => i.image);
    return its.map((i, n) => Object.assign(story(i, f.source, name), { image: pics[n] }));
  }));
  const now = Date.now(), seen = new Set(), out = [];
  for (let i = 0; i < Math.max(0, ...lists.map((l) => l.length)); i++) for (const l of lists) {
    const s = l[i];
    if (s && (!s.published || now - s.published < 4 * 864e5) && !seen.has(titleKey(s.title))) { seen.add(titleKey(s.title)); out.push(s); }
  }
  return { key, name, stories: out.slice(0, 30) };
}

/* ---------------------------------------------------------------- the money (as the TV's Money.kt) */
const AED_PER_USD = 3.6725, GRAMS_PER_OZ = 31.1034768;
const perGram = (oz) => oz * AED_PER_USD / GRAMS_PER_OZ;
function embedded(html, key) {
  let i = html.indexOf(key + '&quot;:{');
  if (i < 0) i = html.indexOf('"' + key + '":{');
  if (i < 0) return null;
  const start = html.indexOf('{', i), s = html.slice(start, start + 40000).replace(/&quot;/g, '"').replace(/&amp;/g, '&');
  let depth = 0, str = false, esc = false;
  for (let j = 0; j < s.length; j++) {
    const ch = s[j];
    if (str) { if (esc) esc = false; else if (ch === '\\') esc = true; else if (ch === '"') str = false; continue; }
    if (ch === '"') str = true; else if (ch === '{') depth++; else if (ch === '}' && --depth === 0) return JSON.parse(s.slice(0, j + 1));
  }
  return null;
}
const num = (o, k) => { const v = parseFloat(String(o[k] ?? '').replace(/,/g, '').trim()); return isNaN(v) ? null : v; };
function board(html) {
  const o = embedded(html, 'goldRates');
  if (!o || !Array.isArray(o.rates)) return null;
  const rows = o.rates.filter((j) => j.type !== 'Type').map((j) => ({ karat: String(j.type).trim(), morning: num(j, 'morning'), afternoon: num(j, 'afternoon'), evening: num(j, 'evening'), yesterday: num(j, 'yesterday') }));
  const karats = rows.filter((r) => r.karat.endsWith('K')).map((r) => { const now = r.evening ?? r.afternoon ?? r.morning; return Object.assign(r, { now, change: now != null && r.yesterday != null ? now - r.yesterday : null }); });
  return { date: String(o.date || '').trim(), karats };
}
function fuel(html) {
  const o = embedded(html, 'fuelRates');
  if (!o || !Array.isArray(o.rates)) return null;
  const head = o.rates.find((j) => j.type === 'Type');
  if (!head) return null;
  const months = Object.keys(head).filter((k) => k !== 'type' && k !== 'change');
  if (!months.length) return null;
  const dated = String(o.date || '').trim().split(' ')[0].toLowerCase();
  const now = months.find((m) => m.toLowerCase() === dated) || months[months.length - 1], before = months.find((m) => m !== now);
  const prices = o.rates.filter((j) => j !== head).map((j) => { const v = num(j, now); if (v == null) return null; const b = before ? num(j, before) : null; return { grade: String(j.type).trim(), now: v, change: b == null ? null : v - b }; }).filter(Boolean);
  const label = (k) => head[k] || k[0].toUpperCase() + k.slice(1);
  return prices.length ? { month: label(now), before: before ? label(before) : '', prices } : null;
}
const closes = (chart) => {
  const ts = chart.timestamp || [], c = ((chart.indicators || {}).quote || [{}])[0].close || [];
  return ts.map((t, i) => [t, c[i]]).filter(([, v]) => v > 0);
};
async function money() {
  const yahoo = (sym, range) => json('https://query1.finance.yahoo.com/v8/finance/chart/' + sym + '?range=' + range + '&interval=1d').then((j) => j.chart.result[0]);
  const weekDay = new Date(Date.now() + 4 * 3600000 - 7 * 864e5).toISOString().slice(0, 10);
  const [er, week, lkrM, inrM, gold, page] = await Promise.all([
    json('https://open.er-api.com/v6/latest/AED'),
    safe(json('https://cdn.jsdelivr.net/npm/@fawazahmed0/currency-api@' + weekDay + '/v1/currencies/aed.json').then((j) => j.aed)),
    safe(yahoo('LKR=X', '1mo').then((r) => closes(r).map(([, v]) => v / AED_PER_USD)), []),
    safe(yahoo('INR=X', '1mo').then((r) => closes(r).map(([, v]) => v / AED_PER_USD)), []),
    safe(yahoo('GC=F', '1mo')),
    safe(text('https://www.khaleejtimes.com/gold-forex')),
  ]);
  const b = page ? (() => { try { return board(page); } catch (e) { return null; } })() : null;
  const k24 = b && (b.karats.find((r) => r.karat === '24K') || {}).now;
  const spot = gold ? perGram(gold.meta.regularMarketPrice) : NaN;
  const scale = k24 != null && spot > 0 ? k24 / spot : 1;
  const g24 = k24 ?? spot;
  const karat = (k) => (b && (b.karats.find((r) => r.karat === k + 'K') || {}).now) ?? g24 * k / 24;
  const month = gold ? closes(gold).map(([, oz]) => perGram(oz) * scale) : [];
  if (k24 != null && month.length) month[month.length - 1] = k24;
  const session = b ? (() => { const r = b.karats.find((x) => x.karat === '24K') || {}; return r.evening != null ? 'Evening' : r.afternoon != null ? 'Afternoon' : 'Morning'; })() : 'world price';
  return {
    lkr: er.rates.LKR, inr: er.rates.INR, rates: Object.fromEntries(Object.entries(er.rates || {}).filter(([c, v]) => /^[A-Z]{3}$/.test(c) && Number.isFinite(v) && v > 0)), lkrWeekAgo: week ? week.lkr ?? null : null, inrWeekAgo: week ? week.inr ?? null : null, lkrMonth: lkrM, inrMonth: inrM,
    gold: isNaN(g24) ? null : { k24: karat(24), k22: karat(22), k21: karat(21), k18: karat(18), poun: karat(22) * 8, month, session },
    board: b ? b.karats.map((r) => ({ karat: r.karat, morning: r.morning, afternoon: r.afternoon, evening: r.evening, yesterday: r.yesterday, change: r.change })) : null,
    fuel: page ? (() => { try { return fuel(page); } catch (e) { return null; } })() : null,
    at: Date.now(),
  };
}

/* ---------------------------------------------------------------- out */
const [news, sections, cash] = await Promise.all([safe(top(), []), Promise.all(SECTIONS.map((s) => safe(section(s), null))), safe(money())]);
const out = { at: Date.now(), news, sections: sections.filter((s) => s && s.stories.length), money: cash };
writeFileSync(process.argv[2] || 'feeds.json', JSON.stringify(out));
console.log('news', news.length, '· sections', out.sections.map((s) => s.key + ' ' + s.stories.length).join(', '), '· money', cash ? 'yes' : 'no', cash && cash.board ? '(board)' : '');
