// api/backfill.js
// Берёт последние N завершённых маркетов из серии btc-up-or-down-5m
// Тянет сделки за ВЕСЬ 5-минутный интервал (не только первые 2:30)
// Пишет в таблицу markets5min

const SB_URL = process.env.NEXT_PUBLIC_SUPABASE_URL;
const SB_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method !== 'GET') return res.status(405).json({ error: 'Method not allowed' });

  const limit = Math.min(parseInt(req.query.limit || '20', 10), 50);

  try {
    // 1. Последние N маркетов из серии
    const seriesResp = await fetch(
      `https://gamma-api.polymarket.com/markets?series_slug=btc-up-or-down-5m&limit=${limit}&order=endDate&ascending=false`
    );
    if (!seriesResp.ok) throw new Error('Series API ' + seriesResp.status);
    const markets = await seriesResp.json();
    if (!Array.isArray(markets) || !markets.length) {
      return res.status(200).json({ filled: 0, total: 0 });
    }

    let filled = 0;
    const errors = [];

    for (const market of markets) {
      try {
        const { conditionId, endDate: endDateStr, slug } = market;
        if (!conditionId || !endDateStr || !slug) continue;

        const endTs   = new Date(endDateStr).getTime();
        const startTs = endTs - 5 * 60 * 1000;

        // Пропускаем незавершённые маркеты
        if (endTs > Date.now()) continue;

        // Проверяем — уже есть запись с данными?
        const check = await fetch(
          `${SB_URL}/rest/v1/markets5min?slug=eq.${encodeURIComponent(slug)}&select=slug,up_min`,
          { headers: { apikey: SB_KEY, Authorization: `Bearer ${SB_KEY}` } }
        );
        if (check.ok) {
          const ex = await check.json();
          // Пропустить если уже есть И данные заполнены
          if (ex && ex.length > 0 && ex[0].up_min != null) continue;
        }

        // 2. Сделки маркета
        const tradesResp = await fetch(
          `https://data-api.polymarket.com/trades?market=${conditionId}&limit=500&offset=0`
        );
        if (!tradesResp.ok) continue;
        const trades = await tradesResp.json();
        if (!Array.isArray(trades) || !trades.length) continue;

        // 3. Фильтр по окну маркета + разбивка по стороне
        const upTrades   = trades.filter(t => t.outcome === 'Up'   && inW(t, startTs, endTs));
        const downTrades = trades.filter(t => t.outcome === 'Down' && inW(t, startTs, endTs));

        const upFirst   = first(upTrades);
        const downFirst = first(downTrades);
        const upEx      = extr(upTrades);
        const downEx    = extr(downTrades);

        // Исход
        const lastUp = [...upTrades].sort((a, b) => b.timestamp - a.timestamp)[0];
        const outcome = lastUp && parseFloat(lastUp.price) >= 0.9 ? 'UP' : 'DOWN';

        const upStart   = upFirst   ? pct(upFirst.price)   : null;
        const downStart = downFirst ? pct(downFirst.price) : null;

        const row = {
          slug,
          time:          new Date(startTs).toISOString(),
          market:        market.question || slug,
          up_start:      upStart,
          down_start:    downStart,
          up_min:        upEx.min  != null ? pct(upEx.min)   : null,
          up_min_time:   upEx.minTime   || null,
          up_max:        upEx.max  != null ? pct(upEx.max)   : null,
          up_max_time:   upEx.maxTime   || null,
          down_min:      downEx.min != null ? pct(downEx.min) : null,
          down_min_time: downEx.minTime || null,
          down_max:      downEx.max != null ? pct(downEx.max) : null,
          down_max_time: downEx.maxTime || null,
          outcome,
          skew: skewLabel(upStart, downStart)
        };

        // 4. Upsert в Supabase
        const upsert = await fetch(`${SB_URL}/rest/v1/markets5min`, {
          method: 'POST',
          headers: {
            apikey: SB_KEY,
            Authorization: `Bearer ${SB_KEY}`,
            'Content-Type': 'application/json',
            Prefer: 'resolution=merge-duplicates'
          },
          body: JSON.stringify(row)
        });

        if (upsert.ok || upsert.status === 201) {
          filled++;
        } else {
          const err = await upsert.text();
          errors.push({ slug, err: err.slice(0, 120) });
        }

      } catch (e) {
        errors.push({ slug: market.slug, err: e.message });
      }
    }

    return res.status(200).json({ filled, total: markets.length, errors: errors.slice(0, 5) });

  } catch (e) {
    console.error('backfill.js:', e);
    return res.status(500).json({ error: e.message });
  }
}

// ── helpers ──────────────────────────────────

function inW(t, startTs, endTs) {
  const ts = t.timestamp * 1000;
  return ts >= startTs && ts <= endTs;
}

function first(trades) {
  if (!trades.length) return null;
  return trades.reduce((a, b) => a.timestamp < b.timestamp ? a : b);
}

function extr(trades) {
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

function pct(price) { return Math.round(parseFloat(price) * 100); }

function skewLabel(up, down) {
  if (!up || !down) return 'НЕЙТРАЛЬНО';
  const d = up - down;
  if (d > 30)  return 'СИЛЬНО UP';
  if (d > 15)  return 'УМЕРЕННО UP';
  if (d > 5)   return 'СЛАБО UP';
  if (d < -30) return 'СИЛЬНО DOWN';
  if (d < -15) return 'УМЕРЕННО DOWN';
  if (d < -5)  return 'СЛАБО DOWN';
  return 'НЕЙТРАЛЬНО';
}
