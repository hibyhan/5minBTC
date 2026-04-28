// api/history.js
// Возвращает данные маркета из trades API — весь 5-минутный интервал
// MIN/MAX для UP и DOWN с временными метками (ISO), исход маркета

import { buildMarketRow } from './market-data.js';

export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method !== 'GET') return res.status(405).json({ error: 'Method not allowed' });

  const { slug } = req.query;
  if (!slug) return res.status(400).json({ error: 'slug required' });

  try {
    const row = await buildMarketRow(slug);
    if (!row) return res.status(404).json({ error: 'Market history not found', slug });
    return res.status(200).json(row);

  } catch (e) {
    console.error('history.js:', e);
    return res.status(500).json({ error: e.message });
  }
}
