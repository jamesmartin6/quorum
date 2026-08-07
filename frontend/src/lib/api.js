// REST client for a single node's gateway. Every node speaks the exact
// same API (see backend/internal/gateway/http.go) - which node a given
// call targets is just which base URL you pass in.

// The full set of node base URLs the dashboard polls. Configurable via
// VITE_NODE_URLS (comma-separated), falling back to the 5 ports
// docker-compose.yml publishes to localhost.
export const NODE_URLS = (
  import.meta.env.VITE_NODE_URLS ||
  "http://localhost:8081,http://localhost:8082,http://localhost:8083,http://localhost:8084,http://localhost:8085"
)
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);

const REQUEST_TIMEOUT_MS = 2500;

async function request(baseUrl, path, options = {}) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);
  try {
    const res = await fetch(`${baseUrl}${path}`, {
      ...options,
      signal: controller.signal,
      headers: { "Content-Type": "application/json", ...(options.headers || {}) },
    });
    const body = await res.json().catch(() => null);
    if (!res.ok) {
      const err = new Error(body?.error || `HTTP ${res.status}`);
      err.status = res.status;
      err.body = body;
      throw err;
    }
    return body;
  } finally {
    clearTimeout(timer);
  }
}

export function getStatus(baseUrl) {
  return request(baseUrl, "/cluster/status");
}

export function getLog(baseUrl) {
  return request(baseUrl, "/cluster/log");
}

export function setKey(baseUrl, key, value) {
  return request(baseUrl, `/kv/${encodeURIComponent(key)}`, {
    method: "POST",
    body: JSON.stringify({ value }),
  });
}

export function getKey(baseUrl, key) {
  return request(baseUrl, `/kv/${encodeURIComponent(key)}`);
}

export function deleteKey(baseUrl, key) {
  return request(baseUrl, `/kv/${encodeURIComponent(key)}`, { method: "DELETE" });
}

export function killNode(baseUrl) {
  return request(baseUrl, "/chaos/kill", { method: "POST" });
}

export function reviveNode(baseUrl) {
  return request(baseUrl, "/chaos/revive", { method: "POST" });
}
