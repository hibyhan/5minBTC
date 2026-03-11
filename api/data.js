// api/data.js
// Читает данные из Supabase через service_role key (обходит RLS)
export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Cache-Control', 'no-store');
  const SUPABASE_URL = process.env.NEXT_PUBLIC_SUPABASE_URL;
  const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
  if (!SUPABASE_URL || !SUPABASE_KEY) {
    return res.status(500).json({ error: 'Missing env vars' });
  }
  try {
    const response = await fetch(
      SUPABASE_URL + '/rest/v1/markets5min?select=*&order=time.desc&limit=500',
      {
        headers: {
          'apikey': SUPABASE_KEY,
          'Authorization': 'Bearer ' + SUPABASE_KEY
        }
      }
    );
    const data = await response.json();
    return res.status(200).json(Array.isArray(data) ? data : []);
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
}
