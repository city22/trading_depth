'use strict';

// ── Constants ──────────────────────────────────────────────────────
const LADDER_ROWS = 60;
const HALF        = Math.floor(LADDER_ROWS / 2);
const COL_TYPES   = ['depth_bid', 'trade_buy', 'delta', 'trade_sell', 'depth_ask'];
const COL_LABELS  = {
  depth_bid: 'Depth Bid', trade_buy: 'Trade Buy',
  trade_sell: 'Trade Sell', delta: 'Delta', depth_ask: 'Depth Ask',
};

const EXCHANGE_COLORS = {
  binance: '#F0B90B', okx: '#2979ff',  bybit:  '#f7a600',
  gate:    '#00b8b8', mexc: '#00b14f', htx:    '#3f9eff',
  bitget:  '#00c087', kraken: '#8247e5',
};
const DEFAULT_EX_COLOR = '#6e7681';

// Exchanges available in pure-frontend mode (direct public WS)
const ALL_EXCHANGES = ['binance', 'okx', 'bybit', 'gate', 'mexc', 'htx', 'bitget', 'kraken'];

// ── Application State ──────────────────────────────────────────────
const state = {
  symbol:      'BTC/USDT',
  exchanges:   ['binance', 'okx', 'bybit'],
  timeWindow:  5,
  precision:   1,
  dirty:       false,
  lastData:    null,
  centerPrice: null,
  lastPrice:   null,
};

// ── Per-exchange raw data ──────────────────────────────────────────
// exRaw[id] = { bids, asks, trades, lastPrice, snapshotBids, snapshotAsks, status }
const exRaw = {};

// ── DOM refs ──────────────────────────────────────────────────────
let ladderRows     = [];
let ladderCells    = [];
let priceLabels    = [];
let prevValues     = [];
let prevBg         = [];
let prevBorderTop  = [];
let prevMarkers    = [];
let builtExchanges = [];
let exHeaderCells  = {};

// ── Formatters ────────────────────────────────────────────────────
function fmt(v) {
  if (!v || v === 0) return '';
  if (v >= 10000) return (v / 1000).toFixed(1) + 'K';
  if (v >= 100)   return v.toFixed(1);
  if (v >= 1)     return v.toFixed(2);
  return v.toFixed(4);
}

function priceKey(price, precision) {
  if (precision >= 1) return price.toFixed(0);
  const d = Math.max(0, -Math.floor(Math.log10(precision)));
  return price.toFixed(d);
}

function roundToPrecision(price, precision) {
  return Math.round(price / precision) * precision;
}

// ── Exchange WebSocket Definitions ─────────────────────────────────
const EXCHANGE_WS = {

  binance: {
    connect(symbol, exId) {
      const sLow = symbol.replace('/', '').toLowerCase();

      // Full diff depth stream — builds complete order book incrementally.
      // No REST needed; depth data accumulates within a few seconds.
      const wsOb = new WebSocket(`wss://stream.binance.com:9443/ws/${sLow}@depth@100ms`);
      wsOb.onmessage = (ev) => {
        const d = JSON.parse(ev.data);
        (d.b || []).forEach(([p, q]) => {
          if (+q > 0) exRaw[exId].bids[p] = +q; else delete exRaw[exId].bids[p];
        });
        (d.a || []).forEach(([p, q]) => {
          if (+q > 0) exRaw[exId].asks[p] = +q; else delete exRaw[exId].asks[p];
        });
      };

      // Trade stream
      const wsTr = new WebSocket(`wss://stream.binance.com:9443/ws/${sLow}@trade`);
      wsTr.onmessage = (ev) => {
        const d = JSON.parse(ev.data);
        const price = +d.p;
        if (price > 0) {
          exRaw[exId].lastPrice = price;
          pushTrade(exId, { price, qty: +d.q, side: d.m ? 'sell' : 'buy', ts: d.T || Date.now() });
        }
      };

      return [wsOb, wsTr];
    },
  },

  okx: {
    connect(symbol, exId) {
      const instId = symbol.replace('/', '-');
      const ws = new WebSocket('wss://ws.okx.com:8443/ws/v5/public');
      let snapBids = {}, snapAsks = {};

      ws.onopen = () => ws.send(JSON.stringify({
        op: 'subscribe',
        // 'books': 400-level full book, WS snapshot + incremental delta
        args: [{ channel: 'books', instId }, { channel: 'trades', instId }],
      }));

      ws.onmessage = (ev) => {
        if (ev.data === 'pong') return;
        let d; try { d = JSON.parse(ev.data); } catch { return; }
        if (!d.arg || !d.data) return;

        const ch = d.arg.channel;
        if (ch === 'books') {
          const book = d.data[0] || {};
          if (d.action === 'snapshot') {
            snapBids = {}; snapAsks = {};
            (book.bids || []).forEach(([p, q]) => { if (+q > 0) snapBids[p] = +q; });
            (book.asks || []).forEach(([p, q]) => { if (+q > 0) snapAsks[p] = +q; });
          } else {
            (book.bids || []).forEach(([p, q]) => {
              if (+q > 0) snapBids[p] = +q; else delete snapBids[p];
            });
            (book.asks || []).forEach(([p, q]) => {
              if (+q > 0) snapAsks[p] = +q; else delete snapAsks[p];
            });
          }
          exRaw[exId].bids = { ...snapBids };
          exRaw[exId].asks = { ...snapAsks };
        } else if (ch === 'trades') {
          d.data.forEach(t => {
            const price = +t.px;
            if (price > 0) {
              exRaw[exId].lastPrice = price;
              pushTrade(exId, { price, qty: +t.sz, side: t.side, ts: +t.ts || Date.now() });
            }
          });
        }
      };

      const pingTimer = setInterval(() => {
        if (ws.readyState === WebSocket.OPEN) ws.send('ping');
      }, 25000);
      ws.addEventListener('close', () => clearInterval(pingTimer));

      return [ws];
    },
  },

  bybit: {
    connect(symbol, exId) {
      const s = symbol.replace('/', '');
      const ws = new WebSocket('wss://stream.bybit.com/v5/public/spot');

      ws.onopen = () => ws.send(JSON.stringify({
        op: 'subscribe',
        args: [`orderbook.200.${s}`, `publicTrade.${s}`],
      }));

      ws.onmessage = (ev) => {
        let d; try { d = JSON.parse(ev.data); } catch { return; }
        if (!d.topic) return;

        if (d.topic.startsWith('orderbook')) {
          const raw = exRaw[exId];
          if (d.type === 'snapshot') {
            raw.snapshotBids = {};
            raw.snapshotAsks = {};
            (d.data?.b || []).forEach(([p, q]) => { if (+q > 0) raw.snapshotBids[p] = +q; });
            (d.data?.a || []).forEach(([p, q]) => { if (+q > 0) raw.snapshotAsks[p] = +q; });
          } else {
            (d.data?.b || []).forEach(([p, q]) => {
              if (+q > 0) raw.snapshotBids[p] = +q; else delete raw.snapshotBids[p];
            });
            (d.data?.a || []).forEach(([p, q]) => {
              if (+q > 0) raw.snapshotAsks[p] = +q; else delete raw.snapshotAsks[p];
            });
          }
          raw.bids = { ...raw.snapshotBids };
          raw.asks = { ...raw.snapshotAsks };

        } else if (d.topic.startsWith('publicTrade')) {
          (d.data || []).forEach(t => {
            const price = +t.p;
            if (price > 0) {
              exRaw[exId].lastPrice = price;
              pushTrade(exId, { price, qty: +t.v, side: t.S === 'Buy' ? 'buy' : 'sell', ts: +t.T || Date.now() });
            }
          });
        }
      };

      const pingTimer = setInterval(() => {
        if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify({ op: 'ping' }));
      }, 20000);
      ws.addEventListener('close', () => clearInterval(pingTimer));

      return [ws];
    },
  },

  gate: {
    connect(symbol, exId) {
      const s = symbol.replace('/', '_');
      const ws = new WebSocket('wss://api.gateio.ws/ws/v4/');

      ws.onopen = () => {
        const t = Math.floor(Date.now() / 1000);
        ws.send(JSON.stringify({ time: t, channel: 'spot.order_book', event: 'subscribe', payload: [s, '20', '100ms'] }));
        ws.send(JSON.stringify({ time: t, channel: 'spot.trades',     event: 'subscribe', payload: [s] }));
      };

      ws.onmessage = (ev) => {
        let d; try { d = JSON.parse(ev.data); } catch { return; }
        if (d.channel === 'spot.order_book' && d.event === 'update') {
          const bids = {}, asks = {};
          (d.result?.bids || []).forEach(([p, q]) => { if (+q > 0) bids[p] = +q; });
          (d.result?.asks || []).forEach(([p, q]) => { if (+q > 0) asks[p] = +q; });
          exRaw[exId].bids = bids;
          exRaw[exId].asks = asks;
        } else if (d.channel === 'spot.trades' && d.event === 'update') {
          (d.result || []).forEach(t => {
            const price = +t.price;
            if (price > 0) {
              exRaw[exId].lastPrice = price;
              pushTrade(exId, { price, qty: +t.amount, side: t.side, ts: +(t.create_time_ms || Date.now()) });
            }
          });
        }
      };

      return [ws];
    },
  },

  mexc: {
    connect(symbol, exId) {
      const s = symbol.replace('/', '');
      const ws = new WebSocket('wss://wbs.mexc.com/ws');

      ws.onopen = () => ws.send(JSON.stringify({
        method: 'SUBSCRIPTION',
        params: [`spot@public.limit.depth.v3.api@${s}@20`, `spot@public.deals.v3.api@${s}`],
      }));

      ws.onmessage = (ev) => {
        let d; try { d = JSON.parse(ev.data); } catch { return; }
        const c = d.c || '';
        if (c.includes('depth')) {
          const bids = {}, asks = {};
          (d.d?.bids || []).forEach(b => { if (+b.p && +b.v > 0) bids[b.p] = +b.v; });
          (d.d?.asks || []).forEach(a => { if (+a.p && +a.v > 0) asks[a.p] = +a.v; });
          exRaw[exId].bids = bids;
          exRaw[exId].asks = asks;
        } else if (c.includes('deals')) {
          (d.d?.deals || []).forEach(t => {
            const price = +t.p;
            if (price > 0) {
              exRaw[exId].lastPrice = price;
              pushTrade(exId, { price, qty: +t.v, side: t.S === 1 ? 'buy' : 'sell', ts: +t.t || Date.now() });
            }
          });
        }
      };

      const pingTimer = setInterval(() => {
        if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify({ method: 'PING' }));
      }, 20000);
      ws.addEventListener('close', () => clearInterval(pingTimer));

      return [ws];
    },
  },

  // HTX (Huobi) — messages are gzip-compressed binary frames
  htx: {
    connect(symbol, exId) {
      const s = symbol.replace('/', '').toLowerCase(); // BTCUSDT -> btcusdt
      const ws = new WebSocket('wss://api.huobi.pro/ws');
      ws.binaryType = 'arraybuffer';

      ws.onopen = () => {
        ws.send(JSON.stringify({ sub: `market.${s}.mbp.refresh.20`, id: 'ob' }));
        ws.send(JSON.stringify({ sub: `market.${s}.trade.detail`,   id: 'tr' }));
      };

      ws.onmessage = async (ev) => {
        let text;
        try {
          const ds     = new DecompressionStream('gzip');
          const stream = new Blob([ev.data]).stream().pipeThrough(ds);
          text = await new Response(stream).text();
        } catch { return; }

        let d; try { d = JSON.parse(text); } catch { return; }

        // HTX heartbeat
        if (d.ping) { ws.send(JSON.stringify({ pong: d.ping })); return; }

        const ch = d.ch || '';
        if (ch.includes('mbp.refresh')) {
          const tick = d.tick || {};
          const bids = {}, asks = {};
          (tick.bids || []).forEach(([p, q]) => { if (q > 0) bids[String(p)] = q; });
          (tick.asks || []).forEach(([p, q]) => { if (q > 0) asks[String(p)] = q; });
          exRaw[exId].bids = bids;
          exRaw[exId].asks = asks;
        } else if (ch.includes('trade.detail')) {
          (d.tick?.data || []).forEach(t => {
            const price = +t.price;
            if (price > 0) {
              exRaw[exId].lastPrice = price;
              pushTrade(exId, { price, qty: +t.amount, side: t.direction, ts: t.ts || Date.now() });
            }
          });
        }
      };

      return [ws];
    },
  },

  // Bitget — books channel gives full depth with snapshot+delta
  bitget: {
    connect(symbol, exId) {
      const s = symbol.replace('/', '');
      const ws = new WebSocket('wss://ws.bitget.com/v2/ws/public');
      let snapshotBids = {}, snapshotAsks = {};

      ws.onopen = () => ws.send(JSON.stringify({
        op: 'subscribe',
        args: [
          { instType: 'SPOT', channel: 'books', instId: s },
          { instType: 'SPOT', channel: 'trade', instId: s },
        ],
      }));

      ws.onmessage = (ev) => {
        if (ev.data === 'pong') return;
        let d; try { d = JSON.parse(ev.data); } catch { return; }
        const ch = d.arg?.channel;

        if (ch === 'books' && d.data?.[0]) {
          const book = d.data[0];
          if (d.action === 'snapshot') {
            snapshotBids = {}; snapshotAsks = {};
            (book.bids || []).forEach(([p, q]) => { if (+q > 0) snapshotBids[p] = +q; });
            (book.asks || []).forEach(([p, q]) => { if (+q > 0) snapshotAsks[p] = +q; });
          } else {
            (book.bids || []).forEach(([p, q]) => {
              if (+q > 0) snapshotBids[p] = +q; else delete snapshotBids[p];
            });
            (book.asks || []).forEach(([p, q]) => {
              if (+q > 0) snapshotAsks[p] = +q; else delete snapshotAsks[p];
            });
          }
          exRaw[exId].bids = { ...snapshotBids };
          exRaw[exId].asks = { ...snapshotAsks };
        } else if (ch === 'trade' && d.data) {
          // data: [[ts, price, size, side], ...]
          d.data.forEach(t => {
            const price = +t[1];
            if (price > 0) {
              exRaw[exId].lastPrice = price;
              pushTrade(exId, { price, qty: +t[2], side: t[3] === 'buy' ? 'buy' : 'sell', ts: +t[0] || Date.now() });
            }
          });
        }
      };

      const pingTimer = setInterval(() => {
        if (ws.readyState === WebSocket.OPEN) ws.send('ping');
      }, 20000);
      ws.addEventListener('close', () => clearInterval(pingTimer));

      return [ws];
    },
  },

  // Kraken v2 WebSocket
  kraken: {
    connect(symbol, exId) {
      // Kraken uses XBT instead of BTC
      const s = symbol.replace('BTC/', 'XBT/');
      const ws = new WebSocket('wss://ws.kraken.com/v2');
      let snapshotBids = {}, snapshotAsks = {};

      ws.onopen = () => {
        ws.send(JSON.stringify({ method: 'subscribe', params: { channel: 'book',  symbol: [s], depth: 25 } }));
        ws.send(JSON.stringify({ method: 'subscribe', params: { channel: 'trade', symbol: [s] } }));
      };

      ws.onmessage = (ev) => {
        let d; try { d = JSON.parse(ev.data); } catch { return; }

        if (d.channel === 'book') {
          if (d.type === 'snapshot') {
            snapshotBids = {}; snapshotAsks = {};
            (d.data?.[0]?.bids || []).forEach(({ price, qty }) => { if (qty > 0) snapshotBids[String(price)] = qty; });
            (d.data?.[0]?.asks || []).forEach(({ price, qty }) => { if (qty > 0) snapshotAsks[String(price)] = qty; });
          } else {
            (d.data?.[0]?.bids || []).forEach(({ price, qty }) => {
              if (qty > 0) snapshotBids[String(price)] = qty; else delete snapshotBids[String(price)];
            });
            (d.data?.[0]?.asks || []).forEach(({ price, qty }) => {
              if (qty > 0) snapshotAsks[String(price)] = qty; else delete snapshotAsks[String(price)];
            });
          }
          exRaw[exId].bids = { ...snapshotBids };
          exRaw[exId].asks = { ...snapshotAsks };

        } else if (d.channel === 'trade') {
          (d.data || []).forEach(t => {
            const price = +t.price;
            if (price > 0) {
              exRaw[exId].lastPrice = price;
              pushTrade(exId, { price, qty: +t.qty, side: t.side, ts: new Date(t.timestamp).getTime() || Date.now() });
            }
          });
        }
      };

      const pingTimer = setInterval(() => {
        if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify({ method: 'ping' }));
      }, 30000);
      ws.addEventListener('close', () => clearInterval(pingTimer));

      return [ws];
    },
  },
};

// ── Trade buffer ───────────────────────────────────────────────────
const MAX_TRADES = 5000;

function pushTrade(exId, trade) {
  const t = exRaw[exId].trades;
  t.push(trade);
  if (t.length > MAX_TRADES) t.splice(0, t.length - MAX_TRADES);
}

// Prune trades older than 10 min to keep memory bounded
setInterval(() => {
  const cutoff = Date.now() - 600000;
  for (const exId of Object.keys(exRaw)) {
    const t = exRaw[exId].trades;
    let i = 0;
    while (i < t.length && t[i].ts < cutoff) i++;
    if (i > 0) t.splice(0, i);
  }
}, 60000);

// ── Connection lifecycle ───────────────────────────────────────────
const activeConns = {};  // exId -> [WebSocket, ...]

function initExRaw(exId) {
  if (exRaw[exId]?.pollTimer) clearInterval(exRaw[exId].pollTimer);
  exRaw[exId] = {
    bids: {}, asks: {}, trades: [],
    lastPrice: null,
    snapshotBids: {}, snapshotAsks: {},
    status: 'connecting',
    pollTimer: null,
  };
}

function disconnectExchange(exId) {
  if (exRaw[exId]?.pollTimer) { clearInterval(exRaw[exId].pollTimer); exRaw[exId].pollTimer = null; }
  (activeConns[exId] || []).forEach(ws => { ws.onclose = null; ws.close(); });
  delete activeConns[exId];
}

function connectExchange(exId, symbol) {
  disconnectExchange(exId);
  const def = EXCHANGE_WS[exId];
  if (!def) return;

  initExRaw(exId);
  let sockets;
  try { sockets = def.connect(symbol, exId); } catch (e) { return; }
  activeConns[exId] = sockets;

  sockets.forEach(ws => {
    const origOpen  = ws.onopen;
    const origClose = ws.onclose;

    ws.onopen = (ev) => {
      exRaw[exId].status = 'connected';
      updateStatus();
      if (origOpen) origOpen.call(ws, ev);
    };
    ws.onerror = () => { exRaw[exId].status = 'error'; updateStatus(); };
    ws.onclose = (ev) => {
      exRaw[exId].status = 'disconnected';
      updateStatus();
      if (origClose) origClose.call(ws, ev);
      // Auto-reconnect if still selected
      setTimeout(() => {
        if (state.exchanges.includes(exId) && activeConns[exId] === sockets) {
          connectExchange(exId, state.symbol);
        }
      }, 3000);
    };
  });

  updateStatus();
}

function reconnectAll() {
  const next = new Set(state.exchanges);
  // Disconnect removed
  Object.keys(activeConns).forEach(exId => { if (!next.has(exId)) disconnectExchange(exId); });
  // Connect new
  state.exchanges.forEach(exId => { if (!activeConns[exId]) connectExchange(exId, state.symbol); });
}

// ── Status display ─────────────────────────────────────────────────
function updateStatus() {
  const total = state.exchanges.length;
  const ok = state.exchanges.filter(id => exRaw[id]?.status === 'connected').length;
  const dot   = document.getElementById('status-dot');
  const label = document.getElementById('status-label');
  if (ok === 0) {
    dot.className = 'status-dot disconnected'; label.textContent = 'Disconnected';
  } else if (ok < total) {
    dot.className = 'status-dot connecting';   label.textContent = `${ok}/${total}`;
  } else {
    dot.className = 'status-dot connected';    label.textContent = 'Connected';
  }
}

// ── Aggregation (raw → state.lastData) ────────────────────────────
function aggregateDepth(rawBook, precision) {
  const out = {};
  for (const [ps, sz] of Object.entries(rawBook)) {
    const p = parseFloat(ps);
    if (!isFinite(p) || sz <= 0) continue;
    const k = priceKey(roundToPrecision(p, precision), precision);
    out[k] = (out[k] || 0) + sz;
  }
  return out;
}

function aggregateAll() {
  const { precision, timeWindow, exchanges } = state;
  const cutoff = Date.now() - timeWindow * 1000;
  const exOut = {};
  const allPrices = [];

  for (const exId of exchanges) {
    const raw = exRaw[exId];
    if (!raw) continue;

    const bids = aggregateDepth(raw.bids, precision);
    const asks = aggregateDepth(raw.asks, precision);
    const tradeBuys = {}, tradeSells = {};

    for (const t of raw.trades) {
      if (t.ts < cutoff) continue;
      const k = priceKey(roundToPrecision(t.price, precision), precision);
      if (t.side === 'buy') tradeBuys[k]  = (tradeBuys[k]  || 0) + t.qty;
      else                  tradeSells[k] = (tradeSells[k] || 0) + t.qty;
    }

    const bidNums = Object.keys(raw.bids).map(Number).filter(isFinite);
    const askNums = Object.keys(raw.asks).map(Number).filter(isFinite);

    exOut[exId] = {
      bids, asks, trade_buys: tradeBuys, trade_sells: tradeSells,
      last_price: raw.lastPrice,
      best_bid: bidNums.length ? Math.max(...bidNums) : null,
      best_ask: askNums.length ? Math.min(...askNums) : null,
    };
    if (raw.lastPrice) allPrices.push(raw.lastPrice);
  }

  const overallLast = allPrices.length
    ? allPrices.reduce((a, b) => a + b, 0) / allPrices.length : null;

  state.lastData = { type: 'update', exchanges: exOut, last_price: overallLast, timestamp: Date.now() / 1000 };

  // Update center price
  if (allPrices.length > 0) {
    const sorted = allPrices.slice().sort((a, b) => a - b);
    const mid    = Math.floor(sorted.length / 2);
    const median = sorted.length % 2 !== 0 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
    const maxDev = HALF * precision;
    const inRange = sorted.filter(p => Math.abs(p - median) <= maxDev);
    const lp = inRange.reduce((a, b) => a + b, 0) / inRange.length;
    state.lastPrice = lp;
    const newCenter = Math.round(lp / precision) * precision;
    if (newCenter !== state.centerPrice) state.centerPrice = newCenter;
  }

  state.dirty = true;
}

setInterval(aggregateAll, 200);

// ── Build / Rebuild the ladder table ──────────────────────────────
function buildTable(exchanges) {
  builtExchanges = exchanges.slice();
  exHeaderCells  = {};
  const thead = document.getElementById('ladder-thead');
  const tbody = document.getElementById('ladder-tbody');
  thead.innerHTML = '';
  tbody.innerHTML = '';
  ladderRows    = [];
  ladderCells   = [];
  priceLabels   = [];
  prevValues    = [];
  prevBg        = [];
  prevBorderTop = [];
  prevMarkers   = [];

  const hRow1 = thead.insertRow();
  const th0 = document.createElement('th');
  th0.className = 'price-col'; th0.rowSpan = 2; th0.textContent = 'Price';
  hRow1.appendChild(th0);

  exchanges.forEach((exId, ei) => {
    const th = document.createElement('th');
    th.textContent = exId.charAt(0).toUpperCase() + exId.slice(1);
    th.colSpan = COL_TYPES.length;
    if (ei > 0) th.classList.add('ex-group-header');
    hRow1.appendChild(th);
    exHeaderCells[exId] = th;
  });

  const hRow2 = thead.insertRow();
  exchanges.forEach((exId, ei) => {
    COL_TYPES.forEach((ct, ci) => {
      const th = document.createElement('th');
      th.textContent = COL_LABELS[ct];
      if (ei > 0 && ci === 0) th.classList.add('ex-group-header');
      hRow2.appendChild(th);
    });
  });

  for (let r = 0; r < LADDER_ROWS; r++) {
    const tr = tbody.insertRow();
    ladderRows.push(tr);
    ladderCells.push([]);
    prevValues.push([]);
    prevBg.push([]);
    prevBorderTop.push([]);

    const priceCell = tr.insertCell();
    priceCell.className = 'price-col';
    const textEl    = document.createElement('span');
    const markersEl = document.createElement('span');
    markersEl.className = 'price-markers';
    priceCell.appendChild(markersEl);
    priceCell.appendChild(textEl);
    priceLabels.push({ textEl, markersEl });
    prevMarkers.push('');

    exchanges.forEach((exId, ei) => {
      COL_TYPES.forEach((ct, ci) => {
        const td = tr.insertCell();
        td.className = `cell-${ct.replace('_', '-')}`;
        if (ei > 0 && ci === 0) td.classList.add('ex-group-first');
        ladderCells[r].push(td);
        prevValues[r].push(null);
        prevBg[r].push(null);
        prevBorderTop[r].push('');
      });
    });
  }
}

// ── Render ────────────────────────────────────────────────────────
function render() {
  state.dirty = false;
  if (!state.lastData || !state.centerPrice) return;

  const data      = state.lastData;
  const precision = state.precision;
  const exchanges = builtExchanges;

  const prices = [];
  for (let r = 0; r < LADDER_ROWS; r++) {
    prices.push(state.centerPrice + (HALF - 1 - r) * precision);
  }

  const lpRows = {};
  const topPrice = prices[0];
  const botPrice = prices[prices.length - 1];

  exchanges.forEach(exId => {
    const exData = data.exchanges[exId];
    lpRows[exId] = -1;
    const lp = exData?.last_price;
    const th = exHeaderCells[exId];
    if (th) {
      const name = exId.charAt(0).toUpperCase() + exId.slice(1);
      if (lp && lp > topPrice) {
        th.textContent = `${name} ↑${priceKey(lp, precision)}`; th.style.color = '#f85149';
      } else if (lp && lp < botPrice) {
        th.textContent = `${name} ↓${priceKey(lp, precision)}`; th.style.color = '#3fb950';
      } else {
        th.textContent = name; th.style.color = '';
      }
    }
    if (lp) {
      const bucket = Math.round(lp / precision) * precision;
      const ri = prices.findIndex(p => Math.abs(p - bucket) < precision * 0.001);
      lpRows[exId] = ri;
    }
  });

  const colMax = {};
  exchanges.forEach(exId => {
    const exData = data.exchanges[exId];
    if (!exData) return;
    prices.forEach(price => {
      const pk = priceKey(price, precision);
      ['depth_bid', 'depth_ask', 'trade_buy', 'trade_sell'].forEach(ct => {
        const src = ctToSrc(exData, ct);
        const v = src ? (src[pk] || 0) : 0;
        const key = `${exId}:${ct}`;
        colMax[key] = Math.max(colMax[key] || 0, v);
      });
      const buyV  = exData.trade_buys?.[pk]  || 0;
      const sellV = exData.trade_sells?.[pk] || 0;
      const dKey  = `${exId}:delta`;
      colMax[dKey] = Math.max(colMax[dKey] || 0, Math.abs(buyV - sellV));
    });
  });

  prices.forEach((price, r) => {
    const pk   = priceKey(price, precision);
    const isCP = (price === state.centerPrice);
    const tr   = ladderRows[r];

    if (isCP && !tr.classList.contains('row-current-price')) tr.classList.add('row-current-price');
    else if (!isCP && tr.classList.contains('row-current-price')) tr.classList.remove('row-current-price');

    const { textEl, markersEl } = priceLabels[r];
    const pText = priceKey(price, precision);
    if (textEl.textContent !== pText) textEl.textContent = pText;

    let markersHTML = '';
    exchanges.forEach(exId => {
      if (lpRows[exId] === r) {
        const c = EXCHANGE_COLORS[exId] || DEFAULT_EX_COLOR;
        markersHTML += `<span class="lp-dot" style="color:${c}" title="${exId}:${pText}">●</span>`;
      }
    });
    if (markersHTML !== prevMarkers[r]) { markersEl.innerHTML = markersHTML; prevMarkers[r] = markersHTML; }

    let colIdx = 0;
    exchanges.forEach(exId => {
      const exData = data.exchanges[exId];
      COL_TYPES.forEach(ct => {
        const td   = ladderCells[r][colIdx];
        const key  = `${exId}:${ct}`;
        const maxV = colMax[key] || 0;
        let v, text, bg = '';

        if (ct === 'delta') {
          const buyV  = exData?.trade_buys?.[pk]  || 0;
          const sellV = exData?.trade_sells?.[pk] || 0;
          v = buyV - sellV;
          const absV = Math.abs(v);
          text = absV === 0 ? '' : (v > 0 ? '+' : '-') + fmt(absV);
          if (absV > 0 && maxV > 0) {
            const intensity = 0.1 + 0.7 * (absV / maxV);
            bg = v > 0
              ? `rgba(35, 134, 54, ${intensity.toFixed(3)})`
              : `rgba(248, 81, 73, ${intensity.toFixed(3)})`;
          }
        } else {
          const src = exData ? ctToSrc(exData, ct) : null;
          v = src ? (src[pk] || 0) : 0;
          text = fmt(v);
          if (v > 0 && maxV > 0) {
            const intensity = 0.1 + 0.7 * (v / maxV);
            if (ct === 'depth_bid') bg = `rgba(35, 134, 54, ${intensity.toFixed(3)})`;
            else if (ct === 'depth_ask') bg = `rgba(248, 81, 73, ${intensity.toFixed(3)})`;
          }
        }

        const borderTop = lpRows[exId] === r
          ? `2px solid ${EXCHANGE_COLORS[exId] || DEFAULT_EX_COLOR}` : '';

        if (text !== prevValues[r][colIdx])    { td.textContent = text; prevValues[r][colIdx] = text; }
        if (bg   !== prevBg[r][colIdx])        { td.style.background = bg; prevBg[r][colIdx] = bg; }
        if (borderTop !== prevBorderTop[r][colIdx]) { td.style.borderTop = borderTop; prevBorderTop[r][colIdx] = borderTop; }
        colIdx++;
      });
    });
  });

  updateArbIndicator(data, exchanges);
  updateArbSidebar();
}

function ctToSrc(exData, ct) {
  switch (ct) {
    case 'depth_bid':  return exData.bids;
    case 'depth_ask':  return exData.asks;
    case 'trade_buy':  return exData.trade_buys;
    case 'trade_sell': return exData.trade_sells;
    default: return null;
  }
}

// ── RAF loop ──────────────────────────────────────────────────────
function rafLoop() { if (state.dirty) render(); requestAnimationFrame(rafLoop); }

// ── Arb history ───────────────────────────────────────────────────
const ARB_ALERT_PCT = 0.2;
const ARB_HIST_MAX  = 30;
const ARB_ALERT_MAX = 20;
let arbMinAcc = { minute: null, maxPct: -Infinity, maxSpread: -Infinity, exStr: '' };
const arbMinHistory = [];
const arbAlerts     = [];
let arbSidebarDirty = false;

function fmtMinute(ts) {
  const d = new Date(ts);
  return d.getHours().toString().padStart(2, '0') + ':' + d.getMinutes().toString().padStart(2, '0');
}

function recordArb(spread, pct, exStr) {
  const minute = Math.floor(Date.now() / 60000) * 60000;
  if (arbMinAcc.minute !== null && arbMinAcc.minute !== minute) {
    const entry = { minute: arbMinAcc.minute, maxPct: arbMinAcc.maxPct, maxSpread: arbMinAcc.maxSpread, exStr: arbMinAcc.exStr };
    arbMinHistory.unshift(entry);
    if (arbMinHistory.length > ARB_HIST_MAX) arbMinHistory.pop();
    if (entry.maxPct > ARB_ALERT_PCT) { arbAlerts.unshift(entry); if (arbAlerts.length > ARB_ALERT_MAX) arbAlerts.pop(); }
    arbMinAcc = { minute, maxPct: -Infinity, maxSpread: -Infinity, exStr: '' };
  }
  arbMinAcc.minute = minute;
  if (pct > arbMinAcc.maxPct) { arbMinAcc.maxPct = pct; arbMinAcc.maxSpread = spread; arbMinAcc.exStr = exStr; }
  if (pct > ARB_ALERT_PCT) {
    const existing = arbAlerts.find(a => a.minute === minute);
    if (!existing) { arbAlerts.unshift({ minute, maxPct: pct, maxSpread: spread, exStr }); if (arbAlerts.length > ARB_ALERT_MAX) arbAlerts.pop(); }
    else if (pct > existing.maxPct) { existing.maxPct = pct; existing.maxSpread = spread; existing.exStr = exStr; }
  }
  arbSidebarDirty = true;
}

function updateArbSidebar() {
  if (!arbSidebarDirty) return;
  arbSidebarDirty = false;
  const alertsList = document.getElementById('arb-alerts-list');
  alertsList.innerHTML = arbAlerts.length === 0
    ? '<div class="arb-sb-empty">No alerts yet</div>'
    : arbAlerts.map(a =>
        `<div class="arb-sb-row arb-alert"><span class="arb-sb-time">${fmtMinute(a.minute)}</span>` +
        `<span class="arb-sb-val">+${a.maxPct.toFixed(4)}%</span><span class="arb-sb-ex">${a.exStr}</span></div>`
      ).join('');
  const rows = [];
  if (arbMinAcc.minute !== null) {
    const pct = arbMinAcc.maxPct, isPos = pct > 0, isAlrt = pct > ARB_ALERT_PCT;
    const pctStr = pct > -Infinity ? (isPos ? '+' : '') + pct.toFixed(4) + '%' : '—';
    rows.push(`<div class="arb-sb-row arb-sb-live${isAlrt ? ' arb-alert' : (isPos ? ' arb-pos' : '')}"><span class="arb-sb-time">${fmtMinute(arbMinAcc.minute)}</span><span class="arb-sb-val">${pctStr}</span><span class="arb-sb-ex">${arbMinAcc.exStr}</span></div>`);
  }
  arbMinHistory.forEach(m => {
    const isPos = m.maxPct > 0, isAlrt = m.maxPct > ARB_ALERT_PCT;
    const pctStr = (isPos ? '+' : '') + m.maxPct.toFixed(4) + '%';
    rows.push(`<div class="arb-sb-row${isAlrt ? ' arb-alert' : (isPos ? ' arb-pos' : '')}"><span class="arb-sb-time">${fmtMinute(m.minute)}${isAlrt ? '⚡' : ''}</span><span class="arb-sb-val">${pctStr}</span><span class="arb-sb-ex">${m.exStr}</span></div>`);
  });
  document.getElementById('arb-minute-list').innerHTML = rows.join('');
}

function updateArbIndicator(data, exchanges) {
  const elRoot = document.getElementById('arb-indicator');
  const elExch = document.getElementById('arb-exchanges');
  const elSprd = document.getElementById('arb-spread');
  const elPct  = document.getElementById('arb-pct');

  let bestBid = -Infinity, bestBidEx = null;
  let bestAsk =  Infinity, bestAskEx = null;

  exchanges.forEach(exId => {
    const exData = data.exchanges[exId];
    if (!exData) return;
    if (exData.best_bid != null && exData.best_bid > bestBid) { bestBid = exData.best_bid; bestBidEx = exId; }
    if (exData.best_ask != null && exData.best_ask < bestAsk) { bestAsk = exData.best_ask; bestAskEx = exId; }
  });

  if (!bestBidEx || !bestAskEx) {
    elRoot.className = 'arb-none'; elExch.textContent = ''; elSprd.textContent = '—'; elPct.textContent = '';
    return;
  }

  const spread = bestBid - bestAsk;
  const pct    = (spread / bestAsk) * 100;
  const sign   = spread >= 0 ? '+' : '';
  elExch.textContent = `${bestAskEx}→${bestBidEx}`;
  elSprd.textContent = `${sign}${spread.toFixed(2)}`;
  elPct.textContent  = `(${sign}${pct.toFixed(4)}%)`;
  recordArb(spread, pct, `${bestAskEx}→${bestBidEx}`);
  elRoot.className = spread > 0 ? 'arb-active' : 'arb-inactive';
}

// ── Controls setup ────────────────────────────────────────────────
function populateExchangeCheckboxes() {
  const container = document.getElementById('exchange-checkboxes');
  container.innerHTML = '';
  const defaultOn = new Set(['binance', 'okx', 'bybit']);
  ALL_EXCHANGES.forEach(exId => {
    const label = document.createElement('label');
    label.className = 'exchange-label';
    const cb = document.createElement('input');
    cb.type    = 'checkbox';
    cb.value   = exId;
    cb.checked = defaultOn.has(exId);
    label.appendChild(cb);
    label.appendChild(document.createTextNode(exId));
    container.appendChild(label);
  });
}

function getSelectedExchanges() {
  return [...document.querySelectorAll('#exchange-checkboxes input:checked')].map(cb => cb.value);
}

function resetArbHistory() {
  arbMinAcc = { minute: null, maxPct: -Infinity, maxSpread: -Infinity, exStr: '' };
  arbMinHistory.length = 0; arbAlerts.length = 0;
  arbSidebarDirty = true; updateArbSidebar();
}

function applySettings() {
  const sym = document.getElementById('symbol-input').value.trim().toUpperCase();
  const exs = getSelectedExchanges();
  if (!sym || exs.length === 0) return;

  const prevExchanges = builtExchanges.join(',');
  const symbolChanged = sym !== state.symbol;
  if (symbolChanged) {
    resetArbHistory();
    // Disconnect all on symbol change
    Object.keys(activeConns).forEach(id => disconnectExchange(id));
  }

  state.symbol    = sym;
  state.exchanges = exs;

  if (exs.join(',') !== prevExchanges || symbolChanged) {
    buildTable(exs);
    state.centerPrice = null;
    state.lastData    = null;
    prevValues.forEach(row => row.fill(null));
    prevBg.forEach(row => row.fill(null));
    prevBorderTop.forEach(row => row.fill(''));
    prevMarkers.fill('');
  }

  reconnectAll();
}

function setupBtnGroup(groupId, stateKey) {
  const btns = document.querySelectorAll(`#${groupId} button`);
  btns.forEach(btn => btn.addEventListener('click', () => {
    btns.forEach(b => b.classList.remove('active'));
    btn.classList.add('active');
    state[stateKey] = parseFloat(btn.dataset.value);
  }));
}

// ── Init ──────────────────────────────────────────────────────────
(async () => {
  populateExchangeCheckboxes();
  buildTable(state.exchanges);

  setupBtnGroup('tw-group',   'timeWindow');
  setupBtnGroup('prec-group', 'precision');

  document.getElementById('apply-btn').addEventListener('click', applySettings);
  document.getElementById('symbol-input').addEventListener('keydown', e => {
    if (e.key === 'Enter') applySettings();
  });

  reconnectAll();
  requestAnimationFrame(rafLoop);
})();
