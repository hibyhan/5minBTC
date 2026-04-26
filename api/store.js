// api/store.js — Local JSON file store replacing Supabase
// Thread-safe read/write for markets5min table

import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DB_PATH = path.join(__dirname, '..', 'data', 'markets5min.json');

function readAll() {
  try {
    const raw = fs.readFileSync(DB_PATH, 'utf8');
    return JSON.parse(raw);
  } catch (e) {
    return [];
  }
}

function writeAll(rows) {
  fs.writeFileSync(DB_PATH, JSON.stringify(rows, null, 2), 'utf8');
}

/**
 * Get all rows, sorted by time desc, limited to `limit`
 */
export function getRows(limit = 500) {
  const rows = readAll();
  return rows
    .sort((a, b) => new Date(b.time) - new Date(a.time))
    .slice(0, limit);
}

/**
 * Upsert a row by slug (merge-duplicates behaviour like Supabase)
 */
export function upsertRow(row) {
  const rows = readAll();
  const idx = rows.findIndex(r => r.slug === row.slug);
  if (idx >= 0) {
    rows[idx] = { ...rows[idx], ...row };
  } else {
    rows.push(row);
  }
  writeAll(rows);
}

/**
 * Check if a row with given slug already has up_min filled
 */
export function rowExists(slug) {
  const rows = readAll();
  const r = rows.find(r => r.slug === slug);
  return r && r.up_min != null;
}
