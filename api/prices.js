// api/prices.js
// Проксирует запросы к Polymarket Gamma API
export default async function handler(req, res) {
  const slug = req.query.slug;
  if (!slug) return res.status(400).json({ error: 'No slug' });
  res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate');
  res.setHeader('Access-Control-Allow-Origin', '*');
  try {
    const ts = Date.now();
    const gammaRes = await fetch(
      `https://gamma-api.polymarket.com/markets?slug=${slug}&_=${ts}`,
      { headers: { 'User-Agent': 'Mozilla/5.0', 'Cache-Control': 'no-cache' }, cache: 'no-store' }
    );
    const data = await gammaRes.json();
    if (!data || !data[0]) return res.status(404).json({ error: 'Market not found' });
    return res.status(200).json(data[0]);
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
}
