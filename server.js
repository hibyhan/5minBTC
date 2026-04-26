// server.js — Express server replacing Vercel + Supabase
// Serves index.html statically and routes /api/* to handler modules
// Data is persisted in data/markets5min.json (local file store)

import express from 'express';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const app = express();
const PORT = process.env.PORT || 3000;

app.use(express.json());

// ── Serve static files (index.html etc.) ──────────────────────────────────
app.use(express.static(__dirname));

// ── Lazy-load API handlers ─────────────────────────────────────────────────
// Each handler exports a default function(req, res) compatible with Vercel style

async function loadHandler(name) {
  const mod = await import(`./api/${name}.js`);
  return mod.default;
}

// Wrap Vercel-style handler into Express route
function apiRoute(handlerName) {
  return async (req, res) => {
    try {
      const handler = await loadHandler(handlerName);
      await handler(req, res);
    } catch (e) {
      console.error(`[${handlerName}]`, e);
      res.status(500).json({ error: e.message });
    }
  };
}

app.get('/api/data',     apiRoute('data'));
app.get('/api/prices',   apiRoute('prices'));
app.get('/api/history',  apiRoute('history'));
app.get('/api/backfill', apiRoute('backfill'));

// ── Fallback: serve index.html for any non-API route ──────────────────────
app.get('*', (req, res) => {
  res.sendFile(path.join(__dirname, 'index.html'));
});

app.listen(PORT, () => {
  console.log(`BTC 5MIN TRACKER running at http://localhost:${PORT}`);
});
