import "./ClusterView.css";

const RADIUS = 150;
const CENTER = 200;
const NODE_R = 34;

function positionFor(index, total) {
  const angle = (2 * Math.PI * index) / total - Math.PI / 2;
  return {
    x: CENTER + RADIUS * Math.cos(angle),
    y: CENTER + RADIUS * Math.sin(angle),
  };
}

function roleClass(node) {
  if (!node.reachable) return "offline";
  if (node.status?.alive === false) return "killed";
  return node.status?.role || "unknown";
}

function shortId(node, index) {
  return node.id || node.status?.id || `node-${index + 1}`;
}

export default function ClusterView({ nodes, term }) {
  const total = nodes.length;
  const positions = nodes.map((_, i) => positionFor(i, total));
  const leaderIndex = nodes.findIndex((n) => roleClass(n) === "leader");

  return (
    <section className="cluster-view card">
      <div className="cluster-view-header">
        <h2>Cluster</h2>
        <div className="term-badge" title="Current Raft term">
          <span className="term-label">term</span>
          <span className="term-value">{term}</span>
        </div>
      </div>

      <svg viewBox="0 0 400 400" className="cluster-svg" role="img" aria-label="Cluster topology">
        {leaderIndex >= 0 &&
          positions.map((pos, i) => {
            if (i === leaderIndex) return null;
            const leaderPos = positions[leaderIndex];
            const target = nodes[i];
            const dim = !target.reachable || target.status?.alive === false;
            return (
              <line
                key={`edge-${i}`}
                x1={leaderPos.x}
                y1={leaderPos.y}
                x2={pos.x}
                y2={pos.y}
                className={`heartbeat-line${dim ? " dim" : ""}`}
                style={{ animationDelay: `${i * 0.15}s` }}
              />
            );
          })}

        {nodes.map((node, i) => {
          const pos = positions[i];
          const cls = roleClass(node);
          return (
            <g
              key={node.url}
              transform={`translate(${pos.x}, ${pos.y})`}
              className={`node-group node-${cls}${node.justCommitted ? " just-committed" : ""}`}
            >
              {cls === "leader" && <circle r={NODE_R + 10} className="leader-ring" />}
              <circle r={NODE_R} className="node-circle" />
              {cls === "leader" && (
                <text y={-NODE_R - 14} textAnchor="middle" className="crown" aria-hidden="true">
                  👑
                </text>
              )}
              <text y={5} textAnchor="middle" className="node-id">
                {shortId(node, i)}
              </text>
              <text y={NODE_R + 20} textAnchor="middle" className="node-role-label">
                {cls}
              </text>
              {node.status && (
                <text y={NODE_R + 36} textAnchor="middle" className="node-term-label">
                  t{node.status.term}
                </text>
              )}
            </g>
          );
        })}
      </svg>

      <ul className="cluster-legend">
        <li>
          <span className="dot leader" /> leader
        </li>
        <li>
          <span className="dot follower" /> follower
        </li>
        <li>
          <span className="dot candidate" /> candidate
        </li>
        <li>
          <span className="dot killed" /> killed (chaos)
        </li>
        <li>
          <span className="dot offline" /> unreachable
        </li>
      </ul>
    </section>
  );
}
