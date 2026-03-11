// api/history.js
// Возвращает данные маркета из trades API — весь 5-минутный интервал
// MIN/MAX для UP и DOWN с временными метками (ISO), исход маркета

export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method !== 'GET') return res.status(405).json({ error: 'Method not allowed' });

  const { slug } = req.query;
  if (!slug) return res.status(400).json({ error: 'slug required' });

  try {
    // 1. Получаем conditionId и endDate из Gamma API
    const gammaResp = await fetch(
      `https://gamma-api.polymarket.com/markets?slug=${encodeURIComponent(slug)}`
    );
    if (!gammaResp.ok) throw new Error('Gamma API ' + gammaResp.status);
    const gammaData = await gammaResp.json();
    const market = Array.isArray(gammaData) ? gammaData[0] : gammaData;
    if (!market) return res.status(404).json({ error: 'Market not found' });

    const conditionId = market.conditionId;
    const endDateStr  = market.endDate;
    if (!conditionId || !endDateStr) {
      return res.status(400).json({ error: 'Missing conditionId or endDate' });
    }

    const endTs   = new Date(endDateStr).getTime();   // ms
    const startTs = endTs - 5 * 60 * 1000;            // 5 минут = весь маркет

    // 2. Тянем все сделки
    const tradesResp = await fetch(
      `https://data-api.polymarket.com/trades?market=${conditionId}&limit=500&offset=0`
    );
    if (!tradesResp.ok) throw new Error('Trades API ' + tradesResp.status);
    const trades = await tradesResp.json();
    if (!Array.isArray(trades) || trades.length === 0) {
      return res.status(200).json({ error: 'No trades', slug, conditionId });
    }

    // 3. Разбиваем по стороне, фильтруем по окну маркета
    const upTrades   = trades.filter(t => t.outcome === 'Up'   && inWindow(t, startTs, endTs));
    const downTrades = trades.filter(t => t.outcome === 'Down' && inWindow(t, startTs, endTs));

    // Стартовые цены (первые сделки в окне)
    const upFirst   = firstTrade(upTrades);
    const downFirst = firstTrade(downTrades);

    // MIN / MAX за весь маркет
    const upEx   = extremes(upTrades);
    const downEx = extremes(downTrades);

    // Исход: последняя сделка UP — если цена >= 0.9, UP победил
    const lastUp = [...upTrades].sort((a, b) => b.timestamp - a.timestamp)[0];
    const outcome = lastUp && parseFloat(lastUp.price) >= 0.9 ? 'UP' : 'DOWN';

    return res.status(200).json({
      slug,
      conditionId,
      time:     new Date(startTs).toISOString(),
      question: market.question || slug,

      up_start:   upFirst   ? pct(upFirst.price)   : null,
      down_start: downFirst ? pct(downFirst.price) : null,

      up_min:       upEx.min  != null ? pct(upEx.min)  : null,
      up_min_time:  upEx.minTime  || null,
      up_max:       upEx.max  != null ? pct(upEx.max)  : null,
      up_max_time:  upEx.maxTime  || null,

      down_min:      downEx.min != null ? pct(downEx.min)  : null,
      down_min_time: downEx.minTime || null,
      down_max:      downEx.max != null ? pct(downEx.max)  : null,
      down_max_time: downEx.maxTime || null,

      outcome,
      endDate:     endDateStr,
      tradesTotal: trades.length
    });

  } catch (e) {
    console.error('history.js:', e);
    return res.status(500).json({ error: e.message });
  }
}

// ── helpers ──────────────────────────────────

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
  let min = Infinity, max = -Infinity, minTime = null, maxTime = null;
  for (const t of trades) {
    const p = parseFloat(t.price);
    const tm = new Date(t.timestamp * 1000).toISOString();
    if (p < min) { min = p; minTime = tm; }
    if (p > max) { max = p; maxTime = tm; }
  }
  return {
    min: min === Infinity  ? null : min, minTime,
    max: max === -Infinity ? null : max, maxTime
  };
}

// price (0.0–1.0) → cents integer
function pct(price) { return Math.round(parseFloat(price) * 100); }
