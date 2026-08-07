import { useState } from "react";
import { setKey, getKey, deleteKey } from "../lib/api";
import "./KVConsole.css";

export default function KVConsole({ nodes, leader }) {
  const [target, setTarget] = useState("auto");
  const [key, setKeyInput] = useState("");
  const [value, setValue] = useState("");
  const [pending, setPending] = useState(false);
  const [history, setHistory] = useState([]);

  function resolveTargetUrl() {
    if (target === "auto") {
      return leader?.url || nodes[0]?.url;
    }
    return target;
  }

  async function run(action, fn) {
    const url = resolveTargetUrl();
    if (!url || !key) return;
    setPending(true);
    const startedAt = performance.now();
    try {
      const result = await fn(url, key, value);
      const ms = Math.round(performance.now() - startedAt);
      pushHistory({ action, key, ok: true, ms, result, url });
    } catch (err) {
      const ms = Math.round(performance.now() - startedAt);
      pushHistory({ action, key, ok: false, ms, error: err.body || { error: err.message }, url });
    } finally {
      setPending(false);
    }
  }

  function pushHistory(entry) {
    setHistory((h) => [{ ...entry, at: new Date(), id: crypto.randomUUID() }, ...h].slice(0, 8));
  }

  function nodeLabel(url) {
    const n = nodes.find((n) => n.url === url);
    return n?.id || url;
  }

  return (
    <section className="kv-console card">
      <h2>KV Console</h2>

      <div className="kv-form">
        <label className="kv-field">
          <span>Target node</span>
          <select value={target} onChange={(e) => setTarget(e.target.value)}>
            <option value="auto">Auto (current leader)</option>
            {nodes.map((n) => (
              <option key={n.url} value={n.url} disabled={!n.reachable}>
                {n.id || n.url} {n.status?.role ? `(${n.status.role})` : ""}
              </option>
            ))}
          </select>
        </label>

        <label className="kv-field">
          <span>Key</span>
          <input value={key} onChange={(e) => setKeyInput(e.target.value)} placeholder="e.g. username" />
        </label>

        <label className="kv-field">
          <span>Value</span>
          <input value={value} onChange={(e) => setValue(e.target.value)} placeholder="e.g. alice" />
        </label>

        <div className="kv-actions">
          <button className="btn btn-set" disabled={pending || !key} onClick={() => run("SET", setKey)}>
            SET
          </button>
          <button className="btn btn-get" disabled={pending || !key} onClick={() => run("GET", (u, k) => getKey(u, k))}>
            GET
          </button>
          <button className="btn btn-delete" disabled={pending || !key} onClick={() => run("DELETE", (u, k) => deleteKey(u, k))}>
            DELETE
          </button>
        </div>
      </div>

      <div className="kv-history">
        {history.length === 0 && <p className="kv-empty">No requests yet — try SET-ing a key above.</p>}
        {history.map((h) => (
          <div key={h.id} className={`kv-history-row ${h.ok ? "ok" : "err"}`}>
            <span className="kv-history-action">{h.action}</span>
            <span className="kv-history-key mono">{h.key}</span>
            <span className="kv-history-node">via {nodeLabel(h.url)}</span>
            <span className="kv-history-detail">
              {h.ok
                ? h.result?.value !== undefined
                  ? h.result.found
                    ? `= "${h.result.value}"`
                    : "(not found)"
                  : h.result?.committed
                    ? `committed @ idx ${h.result.index}`
                    : "pending commit…"
                : h.error?.leaderHttpAddr
                  ? `not leader → try ${nodeLabel(h.error.leaderHttpAddr)}`
                  : h.error?.error || "error"}
            </span>
            <span className="kv-history-ms mono">{h.ms}ms</span>
          </div>
        ))}
      </div>
    </section>
  );
}
