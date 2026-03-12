// api/backfill.js
// Генерирует slugи btc-updown-5m-{timestamp} самостоятельно
// Берёт последние N завершённых маркетов и пишет в Supabase

const SB_URL = process.env.NEXT_PUBLIC_SUPABASE_URL;
const SB_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method !== 'GET') return res.status(405).json({ error: 'Method not allowed' });

  const limit = Math.min(parseInt(req.query.limit || '10', 10), 20);

  try {
    // Генерируем последние N завершённых маркетов
    // Каждый маркет = 300 секунд, идём назад от текущего bucket
    const now = Math.floor(Date.now() / 1000);
    const currentBucket = Math.floor(now / 300) * 300; // текущий (незавершённый) — пропускаем

    const slugs = [];
    for (let i = 1; i <= limit; i++) {
      const ts = currentBucket - (i * 300);
      slugs.push(`btc-updown-5m-${ts}`);
    }

    let filled = 0;
    let skipped = 0;
    const errors = [];

    for (const slug of slugs) {
      try {
        // Проверяем — уже есть в базе с данными?
        const check = await fetch(
          `${SB_URL}/rest/v1/markets5min?slug=eq.${encodeURIComponent(slug)}&select=slug,up_min`,
          { headers: { apikey: SB_KEY, Authorization: `Bearer ${SB_KEY}` } }
        );
        if (check.ok) {
          const ex = await check.json();
          if (ex && ex.length > 0 && ex[0].up_min != null) {
            skipped++;
            continue;
          }
        }

        // Получаем conditionId и endDate из Gamma API
        const gammaResp = await fetch(
          `https://gamma-api.polymarket.com/markets?slug=${encodeURIComponent(slug)}`
        );
        if (!gammaResp.ok) continue;
        const gammaData = await gammaResp.json();
        const market = Array.isArray(gammaData) ? gammaData[0] : gammaData;
        if (!market || !market.conditionId || !market.endDate) continue;

        const conditionId = market.conditionId;
        const endTs = new Date(market.endDate).getTime();

        // Берём точный startTime маркета из Gamma API (не endDate - 5min)
        // eventStartTime — точное время открытия торгов данного периода
        const eventStartTime = market.eventStartTime || market.events?.[0]?.startTime;
        const startTs = eventStartTime
          ? new Date(eventStartTime).getTime()
          : endTs - 5 * 60 * 1000;

        // Пропускаем если маркет ещё не завершился
        if (endTs > Date.now()) continue;

        // Тянем сделки с пагинацией (API отдаёт от новых к старым)
        const trades = [];
        let offset = 0;
        let reachedStart = false;
        while (!reachedStart) {
          const tradesResp = await fetch(
            `https://data-api.polymarket.com/trades?market=${conditionId}&limit=500&offset=${offset}`
          );
          if (!tradesResp.ok) break;
          const batch = await tradesResp.json();
          if (!Array.isArray(batch) || !batch.length) break;
          for (const t of batch) {
            if (t.timestamp * 1000 >= startTs) trades.push(t);
          }
          const oldest = batch[batch.length - 1];
          if (oldest.timestamp * 1000 < startTs) reachedStart = true;
          offset += 500;
          if (offset > 15000) break; // защита: макс 30 запросов на маркет
        }
        if (!trades.length) continue;

        // Фильтр по окну маркета + разбивка по стороне
        const upTrades   = trades.filter(t => t.outcome === 'Up'   && inW(t, startTs, endTs));
        const downTrades = trades.filter(t => t.outcome === 'Down' && inW(t, startTs, endTs));

        if (!upTrades.length && !downTrades.length) continue;

        const upFirst   = first(upTrades);
        const downFirst = first(downTrades);
        const upEx      = extr(upTrades);
        const downEx    = extr(downTrades);

        // Исход: последняя сделка UP >= 0.9 → UP победил
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
          up_min:        upEx.min  != null ? pct(upEx.min)  : null,
          up_min_time:   upEx.minTime   || null,
          up_max:        upEx.max  != null ? pct(upEx.max)  : null,
          up_max_time:   upEx.maxTime   || null,
          down_min:      downEx.min != null ? pct(downEx.min) : null,
          down_min_time: downEx.minTime || null,
          down_max:      downEx.max != null ? pct(downEx.max) : null,
          down_max_time: downEx.maxTime || null,
          outcome,
          skew: skewLabel(upStart, downStart)
        };

        // Upsert в Supabase
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
        errors.push({ slug, err: e.message });
      }
    }

    return res.status(200).json({
      filled,
      skipped,
      total: slugs.length,
      errors: errors.slice(0, 5)
    });

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
    const p  = parseFloat(t.price);
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
