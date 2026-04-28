export async function buildRecentRows(limit = 12) {
  const now = Math.floor(Date.now() / 1000);
  const currentBucket = Math.floor(now / 300) * 300;
  const slugs = [];

  for (let i = 2; i <= Math.min(limit + 1, 31); i++) {
    slugs.push(`btc-updown-5m-${currentBucket - i * 300}`);
  }

  const rows = [];
  for (const slug of slugs) {
    const row = await buildMarketRow(slug).catch(() => null);
    if (row) rows.push(row);
  }

  return rows;
}

export async function buildMarketRow(slug) {
  const market = await fetchMarket(slug);
  if (!market || !market.conditionId || !market.endDate) return null;

  const endTs = new Date(market.endDate).getTime();
  const startTs = endTs - 5 * 60 * 1000;
  const trades = await fetchTrades(market.conditionId, startTs);
  if (!trades.length) return null;

  const upTrades = trades.filter(t => t.outcome === 'Up' && inWindow(t, startTs, endTs));
  const downTrades = trades.filter(t => t.outcome === 'Down' && inWindow(t, startTs, endTs));
  if (!upTrades.length && !downTrades.length) return null;

  const upFirst = firstTrade(upTrades);
  const downFirst = firstTrade(downTrades);
  const upEx = extremes(upTrades);
  const downEx = extremes(downTrades);
  const lastUp = [...upTrades].sort((a, b) => b.timestamp - a.timestamp)[0];
  const outcome = lastUp && Number(lastUp.price) >= 0.9 ? 'UP' : 'DOWN';
  const upStart = upFirst ? pct(upFirst.price) : null;
  const downStart = downFirst ? pct(downFirst.price) : null;

  return {
    slug,
    time: new Date(startTs).toISOString(),
    market: market.question || slug,
    up_start: upStart,
    down_start: downStart,
    up_min: upEx.min != null ? pct(upEx.min) : null,
    up_min_time: upEx.minTime || null,
    up_max: upEx.max != null ? pct(upEx.max) : null,
    up_max_time: upEx.maxTime || null,
    down_min: downEx.min != null ? pct(downEx.min) : null,
    down_min_time: downEx.minTime || null,
    down_max: downEx.max != null ? pct(downEx.max) : null,
    down_max_time: downEx.maxTime || null,
    outcome,
    skew: skewLabel(upStart, downStart)
  };
}

export async function fetchMarket(slug) {
  const response = await fetch(
    `https://gamma-api.polymarket.com/markets?slug=${encodeURIComponent(slug)}&_=${Date.now()}`,
    { headers: { 'User-Agent': 'Mozilla/5.0', 'Cache-Control': 'no-cache' }, cache: 'no-store' }
  );
  if (!response.ok) throw new Error('Gamma API ' + response.status);
  const data = await response.json();
  return Array.isArray(data) ? data[0] : data;
}

async function fetchTrades(conditionId, startTs) {
  const trades = [];
  let offset = 0;
  let reachedStart = false;

  while (!reachedStart) {
    const response = await fetch(
      `https://data-api.polymarket.com/trades?market=${conditionId}&limit=500&offset=${offset}`
    );
    if (!response.ok) break;

    const batch = await response.json();
    if (!Array.isArray(batch) || !batch.length) break;

    for (const trade of batch) {
      if (trade.timestamp * 1000 >= startTs) trades.push(trade);
    }

    const oldest = batch[batch.length - 1];
    reachedStart = oldest.timestamp * 1000 < startTs;
    offset += 500;
    if (offset > 15000) break;
  }

  return trades;
}

function inWindow(trade, startTs, endTs) {
  const ts = trade.timestamp * 1000;
  return ts >= startTs && ts <= endTs;
}

function firstTrade(trades) {
  if (!trades.length) return null;
  return trades.reduce((a, b) => a.timestamp < b.timestamp ? a : b);
}

function extremes(trades) {
  if (!trades.length) return { min: null, minTime: null, max: null, maxTime: null };
  let min = Infinity;
  let max = -Infinity;
  let minTime = null;
  let maxTime = null;

  for (const trade of trades) {
    const price = Number(trade.price);
    const time = new Date(trade.timestamp * 1000).toISOString();
    if (price < min) {
      min = price;
      minTime = time;
    }
    if (price > max) {
      max = price;
      maxTime = time;
    }
  }

  return {
    min: min === Infinity ? null : min,
    minTime,
    max: max === -Infinity ? null : max,
    maxTime
  };
}

function pct(price) {
  return Math.round(Number(price) * 100);
}

function skewLabel(up, down) {
  if (!up || !down) return 'НЕЙТРАЛЬНО';
  const diff = up - down;
  if (diff > 30) return 'СИЛЬНО UP';
  if (diff > 15) return 'УМЕРЕННО UP';
  if (diff > 5) return 'СЛАБО UP';
  if (diff < -30) return 'СИЛЬНО DOWN';
  if (diff < -15) return 'УМЕРЕННО DOWN';
  if (diff < -5) return 'СЛАБО DOWN';
  return 'НЕЙТРАЛЬНО';
}
