import http from 'node:http';
import { readFile } from 'node:fs/promises';
import { extname, join, normalize } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

const root = fileURLToPath(new URL('.', import.meta.url));
const port = Number(process.env.PORT || 3000);

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url || '/', `http://${req.headers.host || 'localhost'}`);

  try {
    if (url.pathname.startsWith('/api/')) {
      await handleApi(url, req, res);
      return;
    }

    await serveStatic(url.pathname, res);
  } catch (error) {
    sendJson(res, 500, { error: error.message || 'Internal server error' });
  }
});

server.listen(port, () => {
  console.log(`BTC 5MIN tracker running at http://localhost:${port}`);
});

async function handleApi(url, req, res) {
  const name = url.pathname.slice('/api/'.length).replace(/\.js$/, '');
  if (!/^[a-z0-9-]+$/i.test(name)) {
    sendJson(res, 404, { error: 'API route not found' });
    return;
  }

  const moduleUrl = pathToFileURL(join(root, 'api', `${name}.js`)).href;
  const mod = await import(`${moduleUrl}?t=${Date.now()}`);
  const query = Object.fromEntries(url.searchParams.entries());
  const body = await readBody(req);

  const apiReq = { method: req.method || 'GET', query, body, headers: req.headers };
  const apiRes = createApiResponse(res);
  await mod.default(apiReq, apiRes);
}

async function serveStatic(pathname, res) {
  const safePath = normalize(decodeURIComponent(pathname)).replace(/^(\.\.[/\\])+/, '');
  const requested = safePath === '/' ? 'index.html' : safePath.replace(/^[/\\]/, '');
  const filePath = join(root, requested);

  if (!filePath.startsWith(root)) {
    res.writeHead(403).end('Forbidden');
    return;
  }

  try {
    const data = await readFile(filePath);
    res.writeHead(200, { 'Content-Type': contentType(filePath) });
    res.end(data);
  } catch {
    res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
    res.end('Not found');
  }
}

function createApiResponse(res) {
  return {
    statusCode: 200,
    setHeader(name, value) {
      res.setHeader(name, value);
      return this;
    },
    status(code) {
      this.statusCode = code;
      return this;
    },
    json(data) {
      sendJson(res, this.statusCode, data);
      return this;
    },
    end(data = '') {
      res.writeHead(this.statusCode);
      res.end(data);
      return this;
    }
  };
}

async function readBody(req) {
  const chunks = [];
  for await (const chunk of req) chunks.push(chunk);
  const raw = Buffer.concat(chunks).toString();
  if (!raw) return undefined;

  try {
    return JSON.parse(raw);
  } catch {
    return raw;
  }
}

function sendJson(res, statusCode, data) {
  if (!res.headersSent) {
    const headers = { 'Content-Type': 'application/json; charset=utf-8' };
    for (const [name, value] of res.getHeaders ? Object.entries(res.getHeaders()) : []) {
      headers[name] = value;
    }
    res.writeHead(statusCode, headers);
  }
  res.end(JSON.stringify(data));
}

function contentType(filePath) {
  switch (extname(filePath)) {
    case '.html':
      return 'text/html; charset=utf-8';
    case '.css':
      return 'text/css; charset=utf-8';
    case '.js':
      return 'text/javascript; charset=utf-8';
    case '.json':
      return 'application/json; charset=utf-8';
    default:
      return 'application/octet-stream';
  }
}
