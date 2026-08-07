import "./LogViewer.css";

function opBadge(op) {
  return op === "NOOP" ? "op-noop" : op === "DELETE" ? "op-delete" : "op-set";
}

export default function LogViewer({ nodes }) {
  return (
    <section className="log-viewer card">
      <h2>Replicated Log</h2>
      <p className="log-viewer-hint">
        Each column is one node's log. Rows at or below the <span className="commit-swatch" /> line are committed
        and applied; rows above are still replicating.
      </p>
      <div className="log-columns">
        {nodes.map((node) => {
          const commitIndex = node.status?.commitIndex ?? 0;
          const entries = [...node.log].reverse();
          return (
            <div key={node.url} className="log-column">
              <div className="log-column-header">
                <span className="mono">{node.id || node.url}</span>
                <span className={`log-role-tag role-${node.status?.role || "unknown"}`}>
                  {node.reachable ? node.status?.role || "…" : "unreachable"}
                </span>
              </div>
              <div className="log-table-wrap">
                {entries.length === 0 && <p className="log-empty">empty</p>}
                <table className="log-table">
                  <thead>
                    <tr>
                      <th>idx</th>
                      <th>term</th>
                      <th>op</th>
                      <th>key</th>
                      <th>value</th>
                    </tr>
                  </thead>
                  <tbody>
                    {entries.map((e) => (
                      <tr key={e.index} className={e.index <= commitIndex ? "committed" : "pending"}>
                        <td className="mono">{e.index}</td>
                        <td className="mono">{e.term}</td>
                        <td>
                          <span className={`op-badge ${opBadge(e.op)}`}>{e.op}</span>
                        </td>
                        <td className="mono">{e.key || "—"}</td>
                        <td className="mono">{e.value || "—"}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          );
        })}
      </div>
    </section>
  );
}
