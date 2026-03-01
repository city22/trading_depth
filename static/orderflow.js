'use strict';

// ── Constants ──────────────────────────────────────────────────────
const DISPLAY_COLS = 20;   // completed candles shown
const PRICE_ROWS   = 80;   // visible price rows
const HALF_ROWS    = 40;
const MAX_STORED   = 40;   // max completed candles kept

const ALL_EXCHANGES = [
  'binance','okx','bybit','gate','mexc','htx','bitget','kraken',
  'phemex','bingx','cryptocom','coinex','whitebit','bitfinex','xt',
];
const EXCHANGE_COLORS = {
  binance:'#F0B90B', okx:'#2979ff',   bybit:'#f7a600',  gate:'#00b8b8',
  mexc:'#00b14f',    htx:'#3f9eff',   bitget:'#00c087', kraken:'#8247e5',
  phemex:'#e84142',  bingx:'#1677ff', cryptocom:'#103f68', coinex:'#00a2e8',
  whitebit:'#5c67ff',bitfinex:'#16b157',xt:'#ff6b35',
};

// ── State ──────────────────────────────────────────────────────────
const state = {
  symbol:      'SOL/USDT',
  exchanges:   ['binance', 'okx', 'bybit'],
  precision:   0.01,
  period:      60,       // seconds per candle
  centerPrice: null,
  dirty:       false,
};

// ── Candle storage ─────────────────────────────────────────────────
let completedCandles = [];   // sorted oldest→newest, max MAX_STORED
let liveCandle       = null; // current incomplete candle

function getPeriodTs(ts) {
  return Math.floor(ts / (state.period * 1000)) * (state.period * 1000);
}
function currentPeriodTs() { return getPeriodTs(Date.now()); }

function newCandle(startTs) {
  return { startTs, open: null, high: -Infinity, low: Infinity, close: null,
           volume: 0, buyVol: 0, sellVol: 0, cells: {}, poc: null };
}

function addTrade(price, qty, side, ts) {
  const pt = getPeriodTs(ts);

  // New period started
  if (!liveCandle || pt > liveCandle.startTs) {
    if (liveCandle) {
      completedCandles.push(liveCandle);
      if (completedCandles.length > MAX_STORED) completedCandles.shift();
    }
    liveCandle = newCandle(pt);
  }

  // Route to correct candle
  let candle;
  if (pt === liveCandle.startTs) {
    candle = liveCandle;
  } else {
    // Historical trade for an older candle
    candle = completedCandles.find(c => c.startTs === pt);
    if (!candle) return;
  }

  // Update OHLCV
  if (candle.open === null) candle.open = price;
  candle.close = price;
  if (price > candle.high) candle.high = price;
  if (price < candle.low)  candle.low  = price;
  candle.volume += qty;

  // Update cell
  const pk = priceKey(roundToPrecision(price, state.precision), state.precision);
  if (!candle.cells[pk]) candle.cells[pk] = { buy: 0, sell: 0 };
  if (side === 'buy') { candle.cells[pk].buy += qty; candle.buyVol += qty; }
  else                { candle.cells[pk].sell += qty; candle.sellVol += qty; }

  // Update POC
  const total = candle.cells[pk].buy + candle.cells[pk].sell;
  if (!candle.poc || total > (candle.cells[candle.poc]?.buy || 0) + (candle.cells[candle.poc]?.sell || 0)) {
    candle.poc = pk;
  }

  // Track center price — snap when price moves > 40% of visible range from center
  const target = roundToPrecision(price, state.precision);
  if (!state.centerPrice || Math.abs(target - state.centerPrice) / state.precision > HALF_ROWS * 0.4) {
    state.centerPrice = target;
  }

  state.dirty = true;
}

// ── Formatters ─────────────────────────────────────────────────────
function priceKey(price, precision) {
  const d = precision >= 1 ? 0 : Math.max(0, -Math.floor(Math.log10(precision)));
  return price.toFixed(d);
}
function roundToPrecision(p, prec) { return Math.round(p / prec) * prec; }
function fmtVol(v) {
  if (!v || v <= 0) return '';
  if (v >= 10000) return (v / 1000).toFixed(1) + 'K';
  if (v >= 1000)  return (v / 1000).toFixed(2) + 'K';
  if (v >= 100)   return v.toFixed(0);
  if (v >= 10)    return v.toFixed(1);
  if (v >= 1)     return v.toFixed(2);
  return v.toFixed(3);
}
function fmtTime(ts) {
  const d = new Date(ts);
  return d.getHours().toString().padStart(2, '0') + ':' + d.getMinutes().toString().padStart(2, '0');
}
function fmtPeriod(secs) {
  if (secs < 60)  return secs + 's';
  if (secs < 3600) return (secs / 60) + 'm';
  return (secs / 3600) + 'h';
}

// ── Exchange trade-only WebSocket connections ───────────────────────
const EXCHANGE_TRADE_WS = {

  binance: {
    connect(symbol, cb, exId) {
      const s = symbol.replace('/', '').toLowerCase();
      const ws = new WebSocket(`wss://stream.binance.com:9443/ws/${s}@trade`);
      ws.onmessage = ev => {
        const d = JSON.parse(ev.data);
        const price = +d.p;
        if (price > 0) cb(price, +d.q, d.m ? 'sell' : 'buy', d.T || Date.now());
      };
      return [ws];
    },
  },

  okx: {
    connect(symbol, cb, exId) {
      const instId = symbol.replace('/', '-');
      const ws = new WebSocket('wss://ws.okx.com:8443/ws/v5/public');
      ws.onopen = () => ws.send(JSON.stringify({ op: 'subscribe', args: [{ channel: 'trades', instId }] }));
      ws.onmessage = ev => {
        if (ev.data === 'pong') return;
        let d; try { d = JSON.parse(ev.data); } catch { return; }
        if (!d.data || d.arg?.channel !== 'trades') return;
        d.data.forEach(t => { const p = +t.px; if (p > 0) cb(p, +t.sz, t.side, +t.ts || Date.now()); });
      };
      const ping = setInterval(() => ws.readyState === 1 && ws.send('ping'), 25000);
      ws.addEventListener('close', () => clearInterval(ping));
      return [ws];
    },
  },

  bybit: {
    connect(symbol, cb, exId) {
      const s = symbol.replace('/', '');
      const ws = new WebSocket('wss://stream.bybit.com/v5/public/spot');
      ws.onopen = () => ws.send(JSON.stringify({ op: 'subscribe', args: [`publicTrade.${s}`] }));
      ws.onmessage = ev => {
        let d; try { d = JSON.parse(ev.data); } catch { return; }
        if (d.op === 'pong' || d.ret_msg === 'pong') return;
        if (!d.topic?.startsWith('publicTrade')) return;
        (d.data || []).forEach(t => { const p = +t.p; if (p > 0) cb(p, +t.v, t.S === 'Buy' ? 'buy' : 'sell', +t.T || Date.now()); });
      };
      const ping = setInterval(() => ws.readyState === 1 && ws.send(JSON.stringify({ op: 'ping' })), 20000);
      ws.addEventListener('close', () => clearInterval(ping));
      return [ws];
    },
  },

  gate: {
    connect(symbol, cb, exId) {
      const s = symbol.replace('/', '_');
      const ws = new WebSocket('wss://api.gateio.ws/ws/v4/');
      ws.onopen = () => ws.send(JSON.stringify({ time: Math.floor(Date.now() / 1000), channel: 'spot.trades', event: 'subscribe', payload: [s] }));
      ws.onmessage = ev => {
        let d; try { d = JSON.parse(ev.data); } catch { return; }
        if (d.channel !== 'spot.trades' || d.event !== 'update') return;
        (d.result || []).forEach(t => { const p = +t.price; if (p > 0) cb(p, +t.amount, t.side, +(t.create_time_ms || Date.now())); });
      };
      return [ws];
    },
  },

  mexc: {
    connect(symbol, cb, exId) {
      const s = symbol.replace('/', '');
      const ws = new WebSocket('wss://wbs.mexc.com/ws');
      ws.onopen = () => ws.send(JSON.stringify({ method: 'SUBSCRIPTION', params: [`spot@public.deals.v3.api@${s}`] }));
      ws.onmessage = ev => {
        let d; try { d = JSON.parse(ev.data); } catch { return; }
        if (d.msg === 'PONG') return;
        if (!(d.c || '').includes('deals')) return;
        (d.d?.deals || []).forEach(t => { const p = +t.p; if (p > 0) cb(p, +t.v, t.S === 1 ? 'buy' : 'sell', +t.t || Date.now()); });
      };
      const ping = setInterval(() => ws.readyState === 1 && ws.send(JSON.stringify({ method: 'PING' })), 20000);
      ws.addEventListener('close', () => clearInterval(ping));
      return [ws];
    },
  },

  htx: {
    connect(symbol, cb, exId) {
      const s = symbol.replace('/', '').toLowerCase();
      const ws = new WebSocket('wss://api.huobi.pro/ws');
      ws.binaryType = 'arraybuffer';
      ws.onopen = () => ws.send(JSON.stringify({ sub: `market.${s}.trade.detail`, id: 'tr' }));
      ws.onmessage = async ev => {
        let text;
        try { const ds = new DecompressionStream('gzip'); const stream = new Blob([ev.data]).stream().pipeThrough(ds); text = await new Response(stream).text(); } catch { return; }
        let d; try { d = JSON.parse(text); } catch { return; }
        if (d.ping) { ws.send(JSON.stringify({ pong: d.ping })); return; }
        if (!(d.ch || '').includes('trade.detail')) return;
        (d.tick?.data || []).forEach(t => { const p = +t.price; if (p > 0) cb(p, +t.amount, t.direction, t.ts || Date.now()); });
      };
      return [ws];
    },
  },

  bitget: {
    connect(symbol, cb, exId) {
      const s = symbol.replace('/', '');
      const ws = new WebSocket('wss://ws.bitget.com/v2/ws/public');
      ws.onopen = () => ws.send(JSON.stringify({ op: 'subscribe', args: [{ instType: 'SPOT', channel: 'trade', instId: s }] }));
      ws.onmessage = ev => {
        if (ev.data === 'pong') return;
        let d; try { d = JSON.parse(ev.data); } catch { return; }
        if (d.arg?.channel !== 'trade' || !d.data) return;
        d.data.forEach(t => { const p = +t[1]; if (p > 0) cb(p, +t[2], t[3] === 'buy' ? 'buy' : 'sell', +t[0] || Date.now()); });
      };
      const ping = setInterval(() => ws.readyState === 1 && ws.send('ping'), 20000);
      ws.addEventListener('close', () => clearInterval(ping));
      return [ws];
    },
  },

  kraken: {
    connect(symbol, cb, exId) {
      const s = symbol.replace('BTC/', 'XBT/');
      const ws = new WebSocket('wss://ws.kraken.com/v2');
      ws.onopen = () => ws.send(JSON.stringify({ method: 'subscribe', params: { channel: 'trade', symbol: [s] } }));
      ws.onmessage = ev => {
        let d; try { d = JSON.parse(ev.data); } catch { return; }
        if (d.method === 'pong') return;
        if (d.channel !== 'trade') return;
        (d.data || []).forEach(t => { const p = +t.price; if (p > 0) cb(p, +t.qty, t.side, new Date(t.timestamp).getTime() || Date.now()); });
      };
      const ping = setInterval(() => ws.readyState === 1 && ws.send(JSON.stringify({ method: 'ping' })), 30000);
      ws.addEventListener('close', () => clearInterval(ping));
      return [ws];
    },
  },

  phemex: {
    connect(symbol, cb, exId) {
      const s = 's' + symbol.replace('/', '');
      const ws = new WebSocket('wss://phemex.com/ws');
      ws.onopen = () => ws.send(JSON.stringify({ id: 2, method: 'spot_trade.subscribe', params: [s] }));
      ws.onmessage = ev => {
        let d; try { d = JSON.parse(ev.data); } catch { return; }
        if (d.id === 0 && d.result === 'pong') return;
        if (!d.trades) return;
        d.trades.forEach(([ts_ns, side, p, q]) => { const price = +p; if (price > 0) cb(price, +q, side === 'Buy' ? 'buy' : 'sell', Math.floor(+ts_ns / 1e6) || Date.now()); });
      };
      const ping = setInterval(() => ws.readyState === 1 && ws.send(JSON.stringify({ id: 0, method: 'server.ping', params: [] })), 5000);
      ws.addEventListener('close', () => clearInterval(ping));
      return [ws];
    },
  },

  bingx: {
    connect(symbol, cb, exId) {
      const s = symbol.replace('/', '-');
      const ws = new WebSocket('wss://open-api-ws.bingx.com/market');
      ws.binaryType = 'arraybuffer';
      ws.onopen = () => ws.send(JSON.stringify({ id: '1', reqType: 'sub', dataType: `${s}@trade` }));
      ws.onmessage = async ev => {
        let text;
        if (typeof ev.data === 'string') { text = ev.data; }
        else { try { const ds = new DecompressionStream('gzip'); const st = new Blob([ev.data]).stream().pipeThrough(ds); text = await new Response(st).text(); } catch { return; } }
        if (text === 'Ping') { ws.send('Pong'); return; }
        let d; try { d = JSON.parse(text); } catch { return; }
        if (d.ping) { ws.send(JSON.stringify({ pong: d.ping })); return; }
        const trades = Array.isArray(d.data) ? d.data : (d.data ? [d.data] : []);
        trades.forEach(t => { const p = +t.p; if (p > 0) cb(p, +t.q, t.m ? 'sell' : 'buy', +t.T || Date.now()); });
      };
      return [ws];
    },
  },

  cryptocom: {
    connect(symbol, cb, exId) {
      const s = symbol.replace('/', '_');
      const ws = new WebSocket('wss://stream.crypto.com/exchange/v1/market');
      ws.onopen = () => ws.send(JSON.stringify({ method: 'subscribe', params: { channels: [`trade.${s}`] } }));
      ws.onmessage = ev => {
        let d; try { d = JSON.parse(ev.data); } catch { return; }
        if (d.method === 'public/heartbeat') { ws.send(JSON.stringify({ method: 'public/respond-heartbeat', id: d.id })); return; }
        if (d.result?.channel !== 'trade') return;
        (d.result.data || []).forEach(t => { const p = +t.p; if (p > 0) cb(p, +t.q, t.s === 'BUY' ? 'buy' : 'sell', +t.t || Date.now()); });
      };
      return [ws];
    },
  },

  coinex: {
    connect(symbol, cb, exId) {
      const s = symbol.replace('/', '');
      const ws = new WebSocket('wss://socket.coinex.com/v2/spot');
      ws.onopen = () => ws.send(JSON.stringify({ method: 'deals.subscribe', params: { market_list: [s] }, id: 1 }));
      ws.onmessage = ev => {
        let d; try { d = JSON.parse(ev.data); } catch { return; }
        if (d.method === 'server.pong') return;
        if (d.method !== 'deals.update') return;
        (d.data?.deal_list || []).forEach(t => { const p = +t.price; if (p > 0) cb(p, +t.amount, t.side, Math.floor(+t.created_at / 1000) || Date.now()); });
      };
      const ping = setInterval(() => ws.readyState === 1 && ws.send(JSON.stringify({ method: 'server.ping', params: {}, id: 0 })), 30000);
      ws.addEventListener('close', () => clearInterval(ping));
      return [ws];
    },
  },

  whitebit: {
    connect(symbol, cb, exId) {
      const s = symbol.replace('/', '_');
      const ws = new WebSocket('wss://api.whitebit.com/ws');
      ws.onopen = () => ws.send(JSON.stringify({ id: 1, method: 'deals_subscribe', params: [s] }));
      ws.onmessage = ev => {
        let d; try { d = JSON.parse(ev.data); } catch { return; }
        if (d.id === 0 && !d.method) return; // pong
        if (d.method !== 'deals_update') return;
        const [, trades] = d.params || [];
        (trades || []).forEach(t => { const p = +t.price; if (p > 0) cb(p, +t.amount, t.type, +(t.time * 1000) || Date.now()); });
      };
      const ping = setInterval(() => ws.readyState === 1 && ws.send(JSON.stringify({ id: 0, method: 'ping', params: [] })), 30000);
      ws.addEventListener('close', () => clearInterval(ping));
      return [ws];
    },
  },

  bitfinex: {
    connect(symbol, cb, exId) {
      const s = 't' + symbol.replace('/', '').replace(/USDT$/, 'UST');
      const ws = new WebSocket('wss://api-pub.bitfinex.com/ws/2');
      let tradeChanId = null;
      ws.onopen = () => ws.send(JSON.stringify({ event: 'subscribe', channel: 'trades', symbol: s }));
      ws.onmessage = ev => {
        let d; try { d = JSON.parse(ev.data); } catch { return; }
        if (d.event === 'pong') return;
        if (d.event === 'subscribed' && d.channel === 'trades') { tradeChanId = d.chanId; return; }
        if (!Array.isArray(d) || d[0] !== tradeChanId) return;
        const payload = d[1];
        if (payload === 'hb') return;
        const apply = ([, ts, amount, price]) => { if (+price > 0) cb(+price, Math.abs(amount), amount > 0 ? 'buy' : 'sell', ts); };
        if (payload === 'te' || payload === 'tu') apply(d[2]);
        else if (Array.isArray(payload)) payload.forEach(apply);
      };
      const ping = setInterval(() => ws.readyState === 1 && ws.send(JSON.stringify({ event: 'ping' })), 30000);
      ws.addEventListener('close', () => clearInterval(ping));
      return [ws];
    },
  },

  xt: {
    connect(symbol, cb, exId) {
      const s = symbol.replace('/', '_').toLowerCase();
      const ws = new WebSocket('wss://stream.xt.com/public');
      ws.onopen = () => ws.send(JSON.stringify({ method: 'subscribe', params: [`trade@${s}`], id: '1' }));
      ws.onmessage = ev => {
        let d; try { d = JSON.parse(ev.data); } catch { return; }
        if (d.event === 'ping') { ws.send(JSON.stringify({ event: 'pong', ts: d.ts })); return; }
        if (d.event !== 'trade') return;
        (d.data || []).forEach(t => { const p = +t.p; if (p > 0) cb(p, +t.q, t.b ? 'buy' : 'sell', +t.t || Date.now()); });
      };
      return [ws];
    },
  },
};

// ── Connection lifecycle ───────────────────────────────────────────
const activeConns = {};
const exStatus    = {};  // exId -> 'connecting'|'connected'|'error'|'disconnected'

function connectExchange(exId, symbol) {
  disconnectExchange(exId);
  const def = EXCHANGE_TRADE_WS[exId];
  if (!def) return;

  exStatus[exId] = 'connecting';
  updateStatus();

  let sockets;
  try { sockets = def.connect(symbol, (price, qty, side, ts) => addTrade(price, qty, side, ts), exId); }
  catch { exStatus[exId] = 'error'; updateStatus(); return; }

  activeConns[exId] = sockets;
  sockets.forEach(ws => {
    ws.onopen  = () => { exStatus[exId] = 'connected';    updateStatus(); };
    ws.onerror = () => { exStatus[exId] = 'error';        updateStatus(); };
    ws.onclose = () => {
      exStatus[exId] = 'disconnected'; updateStatus();
      setTimeout(() => { if (state.exchanges.includes(exId) && activeConns[exId] === sockets) connectExchange(exId, state.symbol); }, 3000);
    };
  });
}

function disconnectExchange(exId) {
  (activeConns[exId] || []).forEach(ws => { ws.onclose = null; ws.close(); });
  delete activeConns[exId];
  exStatus[exId] = 'disconnected';
}

function reconnectAll() {
  const next = new Set(state.exchanges);
  Object.keys(activeConns).forEach(id => { if (!next.has(id)) disconnectExchange(id); });
  state.exchanges.forEach(id => { if (!activeConns[id]) connectExchange(id, state.symbol); });
}

// ── Status bar ─────────────────────────────────────────────────────
function updateStatus() {
  const bar = document.getElementById('of-status');
  if (!bar) return;
  bar.innerHTML = state.exchanges.map(exId => {
    const st  = exStatus[exId] || 'disconnected';
    const dot = st === 'connected' ? 'dot-ok' : st === 'connecting' ? 'dot-wait' : 'dot-err';
    const col = EXCHANGE_COLORS[exId] || '#6e7681';
    return `<span class="ex-chip ex-chip-${st}"><span class="ex-dot ${dot}"></span><span style="color:${col}">${exId}</span></span>`;
  }).join('');
}

// ── Session start ──────────────────────────────────────────────────
let sessionStartTs = null;   // period ts when session/apply began

// ── Table DOM refs ─────────────────────────────────────────────────
let tableCols  = DISPLAY_COLS;      // current period is the rightmost column
let prevLiveCol = -1;               // which col currently has of-live-* class
let domRows    = [];   // domRows[r] = { priceTd, cells: [{td, buySpan, sep, sellSpan}] }
let prevPrices = [];   // prev price labels per row
let prevBuys   = [];   // prevBuys[r][c]
let prevSells  = [];   // prevSells[r][c]
let prevBg     = [];
let prevPoc    = [];   // prevPoc[r][c] = bool
let prevHasData= [];   // prevHasData[r][c] = bool (for separator)
let colHeaders = [];   // colHeaders[c] = { th1, th2 }
let builtCols  = 0;

// ── Build table ────────────────────────────────────────────────────
function buildTable() {
  builtCols = tableCols;
  const thead = document.getElementById('of-thead');
  const tbody = document.getElementById('of-tbody');
  thead.innerHTML = '';
  tbody.innerHTML = '';
  domRows = []; prevPrices = []; prevBuys = []; prevSells = []; prevBg = []; prevPoc = []; prevHasData = [];
  colHeaders = []; prevLiveCol = -1;

  // Header row 1: time labels
  const hRow1 = thead.insertRow();
  const th0 = document.createElement('th'); th0.className = 'of-price-th'; th0.rowSpan = 2; th0.textContent = 'Price';
  hRow1.appendChild(th0);
  for (let c = 0; c < tableCols; c++) {
    const th = document.createElement('th');
    hRow1.appendChild(th);
    colHeaders.push({ th1: th });
  }

  // Header row 2: buy / sell sub-labels
  const hRow2 = thead.insertRow();
  for (let c = 0; c < tableCols; c++) {
    const th = document.createElement('th');
    th.innerHTML = '<span style="color:#3fb950">Buy</span> / <span style="color:#f85149">Sell</span>';
    hRow2.appendChild(th);
    colHeaders[c].th2 = th;
  }

  // Body rows
  for (let r = 0; r < PRICE_ROWS; r++) {
    const tr = tbody.insertRow();
    const priceTd = tr.insertCell();
    priceTd.className = 'of-price-td';
    const cells = [];
    for (let c = 0; c < tableCols; c++) {
      const td = tr.insertCell();
      const inner = document.createElement('div');
      inner.className = 'of-cell-inner';
      const buySpan  = document.createElement('span'); buySpan.className  = 'of-buy';
      const sep      = document.createElement('span'); sep.className      = 'of-sep';
      const sellSpan = document.createElement('span'); sellSpan.className = 'of-sell';
      inner.append(buySpan, sep, sellSpan);
      td.appendChild(inner);
      cells.push({ td, buySpan, sep, sellSpan });
    }
    domRows.push({ priceTd, cells });
    prevPrices.push('');
    prevBuys.push(new Array(tableCols).fill(''));
    prevSells.push(new Array(tableCols).fill(''));
    prevBg.push(new Array(tableCols).fill(''));
    prevPoc.push(new Array(tableCols).fill(false));
    prevHasData.push(new Array(tableCols).fill(false));
  }
}

// ── Render ─────────────────────────────────────────────────────────
function render() {
  state.dirty = false;
  if (!state.centerPrice) return;

  const { precision, centerPrice } = state;

  // Build price array (top = highest)
  const prices = [];
  for (let r = 0; r < PRICE_ROWS; r++) {
    prices.push(roundToPrecision(centerPrice + (HALF_ROWS - 1 - r) * precision, precision));
  }

  // Ensure live candle exists for current period
  const livePts = currentPeriodTs();
  if (!liveCandle) liveCandle = newCandle(livePts);
  else if (liveCandle.startTs !== livePts) {
    completedCandles.push(liveCandle);
    if (completedCandles.length > MAX_STORED) completedCandles.shift();
    liveCandle = newCandle(livePts);
  }

  // ── Time axis: anchored to sessionStartTs, slides when > DISPLAY_COLS periods ──
  const periodMs     = state.period * 1000;
  const elapsedPeriods = (livePts - sessionStartTs) / periodMs;
  const axisStart    = elapsedPeriods < DISPLAY_COLS - 1
    ? sessionStartTs
    : livePts - (DISPLAY_COLS - 1) * periodMs;
  const liveCol      = Math.round((livePts - axisStart) / periodMs);  // 0..DISPLAY_COLS-1

  const timeAxis = [];
  for (let c = 0; c < DISPLAY_COLS; c++) {
    timeAxis.push(axisStart + c * periodMs);   // oldest → newest (left → right)
  }

  // Map each time slot to its candle; current period maps to liveCandle
  const candleMap = new Map(completedCandles.map(c => [c.startTs, c]));
  const cols = timeAxis.map(ts =>
    ts === livePts ? liveCandle : (candleMap.get(ts) || null)
  );

  // Update live-column CSS marker dynamically
  if (prevLiveCol !== liveCol) {
    if (prevLiveCol >= 0 && prevLiveCol < tableCols) {
      colHeaders[prevLiveCol].th1.classList.remove('of-live-th');
      colHeaders[prevLiveCol].th2.classList.remove('of-live-th');
      domRows.forEach(row => row.cells[prevLiveCol].td.classList.remove('of-live-td'));
    }
    colHeaders[liveCol].th1.classList.add('of-live-th');
    colHeaders[liveCol].th2.classList.add('of-live-th');
    domRows.forEach(row => row.cells[liveCol].td.classList.add('of-live-td'));
    prevLiveCol = liveCol;
  }

  // Update column headers: all show HH:MM + optional delta
  for (let c = 0; c < tableCols; c++) {
    const ts     = timeAxis[c];
    const candle = cols[c];
    const { th1 } = colHeaders[c];
    let label = fmtTime(ts);
    if (candle && candle.open !== null) {
      const delta = candle.buyVol - candle.sellVol;
      const sign  = delta >= 0 ? '+' : '';
      label += `\n${sign}${fmtVol(Math.abs(delta))}`;
    }
    if (th1.textContent !== label) th1.textContent = label;
  }

  // Compute per-column max volume for background intensity (only visible price range)
  const colMaxVol = cols.map(candle => {
    if (!candle) return 0;
    let mx = 0;
    prices.forEach(price => {
      const pk = priceKey(price, precision);
      const cell = candle.cells[pk];
      if (cell) { const t = cell.buy + cell.sell; if (t > mx) mx = t; }
    });
    return mx;
  });

  // Render cells
  prices.forEach((price, r) => {
    const pk = priceKey(price, precision);

    // Price label
    if (prevPrices[r] !== pk) { domRows[r].priceTd.textContent = pk; prevPrices[r] = pk; }

    cols.forEach((candle, c) => {
      const cell = candle?.cells[pk];
      const buy  = cell?.buy  || 0;
      const sell = cell?.sell || 0;
      const total = buy + sell;

      const buyTxt  = fmtVol(buy);
      const sellTxt = fmtVol(sell);

      let bg = '';
      if (total > 0 && colMaxVol[c] > 0) {
        const intensity = 0.12 + 0.55 * (total / colMaxVol[c]);
        bg = buy >= sell
          ? `rgba(35,134,54,${intensity.toFixed(3)})`
          : `rgba(248,81,73,${intensity.toFixed(3)})`;
      }

      const isPoc = candle?.poc === pk && total > 0;

      const { td, buySpan, sep, sellSpan } = domRows[r].cells[c];
      const hasData = buyTxt !== '' || sellTxt !== '';

      if (prevBuys[r][c]    !== buyTxt)  { buySpan.textContent  = buyTxt;  prevBuys[r][c]  = buyTxt; }
      if (prevSells[r][c]   !== sellTxt) { sellSpan.textContent = sellTxt; prevSells[r][c] = sellTxt; }
      if (prevBg[r][c]      !== bg)      { td.style.background  = bg;      prevBg[r][c]    = bg; }
      if (prevHasData[r][c] !== hasData) { sep.textContent = hasData ? '/' : ''; prevHasData[r][c] = hasData; }
      if (prevPoc[r][c] !== isPoc) {
        if (isPoc) td.classList.add('of-poc-cell'); else td.classList.remove('of-poc-cell');
        prevPoc[r][c] = isPoc;
      }
    });

    // Price column: highlight if it's the POC of the live candle
    const isLivePoc = liveCandle?.poc === pk;
    const pTd = domRows[r].priceTd;
    if (isLivePoc) pTd.classList.add('poc-row'); else pTd.classList.remove('poc-row');
  });

  // Update footer info
  const info = document.getElementById('of-info');
  if (info && liveCandle && liveCandle.open !== null) {
    const delta = liveCandle.buyVol - liveCandle.sellVol;
    const sign  = delta >= 0 ? '+' : '';
    const fmtP  = v => (isFinite(v) && v !== null) ? priceKey(v, precision) : '—';
    info.textContent = `Live: O ${fmtP(liveCandle.open)}  H ${fmtP(liveCandle.high)}  L ${fmtP(liveCandle.low)}  Vol ${fmtVol(liveCandle.volume)}  Δ ${sign}${fmtVol(Math.abs(delta))}`;
  } else if (info) {
    info.textContent = 'Waiting for data…';
  }
}

// ── Auto-scroll to keep center price in viewport ───────────────────
let lastCenterScrolled = null;
let userScrolling = false;
let userScrollTimer = null;

function setupScrollDetect() {
  const wrap = document.getElementById('of-wrap');
  if (!wrap) return;
  wrap.addEventListener('scroll', () => {
    userScrolling = true;
    clearTimeout(userScrollTimer);
    userScrollTimer = setTimeout(() => { userScrolling = false; }, 3000);
  }, { passive: true });
}

function autoScroll() {
  if (userScrolling) return;
  if (state.centerPrice === lastCenterScrolled) return;
  lastCenterScrolled = state.centerPrice;
  const wrap  = document.getElementById('of-wrap');
  const row   = domRows[HALF_ROWS - 1];
  if (!wrap || !row) return;
  const tr     = row.priceTd.closest('tr');
  const thead  = document.getElementById('of-thead');
  const theadH = thead ? thead.offsetHeight : 0;
  const target = tr.offsetTop - theadH - (wrap.clientHeight - theadH) / 2 + tr.offsetHeight / 2;
  wrap.scrollTo({ top: Math.max(0, target), behavior: 'smooth' });
}

// ── RAF loop ───────────────────────────────────────────────────────
function rafLoop() { if (state.dirty) { render(); autoScroll(); } requestAnimationFrame(rafLoop); }

// Refresh live column header every second (for time countdown etc.)
setInterval(() => { state.dirty = true; }, 1000);

// ── Controls ───────────────────────────────────────────────────────
function populateExchanges() {
  const container = document.getElementById('of-exchanges');
  const defaultOn = new Set(['binance', 'okx', 'bybit']);
  ALL_EXCHANGES.forEach(exId => {
    const label = document.createElement('label');
    label.className = 'ex-label';
    const cb = document.createElement('input');
    cb.type = 'checkbox'; cb.value = exId; cb.checked = defaultOn.has(exId);
    label.appendChild(cb);
    label.appendChild(document.createTextNode(exId));
    container.appendChild(label);
  });
}

function getSelected() {
  return [...document.querySelectorAll('#of-exchanges input:checked')].map(cb => cb.value);
}

function setupBtnGroup(groupId, key) {
  document.querySelectorAll(`#${groupId} button`).forEach(btn => {
    btn.addEventListener('click', () => {
      document.querySelectorAll(`#${groupId} button`).forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      state[key] = parseFloat(btn.dataset.value);
      // Reset candles when precision/period changes
      if (key === 'precision' || key === 'period') {
        if (key === 'period') sessionStartTs = currentPeriodTs();
        completedCandles.length = 0;
        liveCandle = null;
        state.centerPrice = null;
        prevPrices.fill('');
        prevBuys.forEach(r => r.fill(''));
        prevSells.forEach(r => r.fill(''));
        prevBg.forEach(r => r.fill(''));
        prevPoc.forEach(r => r.fill(false));
        prevHasData.forEach(r => r.fill(false));
        state.dirty = true;
      }
    });
  });
}

function applySettings() {
  const sym = document.getElementById('of-symbol').value.trim().toUpperCase();
  const exs = getSelected();
  if (!sym || exs.length === 0) return;

  const symbolChanged = sym !== state.symbol;
  state.symbol    = sym;
  state.exchanges = exs;

  if (symbolChanged) {
    sessionStartTs = currentPeriodTs();
    completedCandles.length = 0;
    liveCandle = null;
    state.centerPrice = null;
  }

  reconnectAll();
  updateStatus();
  state.dirty = true;
}

// ── Init ───────────────────────────────────────────────────────────
(function init() {
  sessionStartTs = currentPeriodTs();
  populateExchanges();
  buildTable();
  setupBtnGroup('period-group', 'period');
  setupBtnGroup('prec-group', 'precision');
  document.getElementById('of-apply').addEventListener('click', applySettings);
  document.getElementById('of-symbol').addEventListener('keydown', e => { if (e.key === 'Enter') applySettings(); });
  reconnectAll();
  setupScrollDetect();
  requestAnimationFrame(rafLoop);
})();
