import { useState } from "react";
import { killNode, reviveNode } from "../lib/api";
import "./ChaosControls.css";

export default function ChaosControls({ nodes }) {
  const [pending, setPending] = useState(null);

  async function handle(url, action, fn) {
    setPending(`${url}:${action}`);
    try {
      await fn(url);
    } catch {
      // The node may briefly stop responding to its own request right as
      // it's killed - the next poll cycle will reflect the true state.
    } finally {
      setPending(null);
    }
  }

  return (
    <section className="chaos-controls card">
      <h2>Chaos Testing</h2>
      <p className="chaos-hint">
        Kill a node's Raft participation without stopping its process, then watch the cluster
        recover. Killing the leader triggers a real election.
      </p>
      <div className="chaos-rows">
        {nodes.map((node) => {
          const alive = node.status?.alive !== false;
          const isLeader = node.status?.role === "leader" && alive;
          return (
            <div key={node.url} className="chaos-row">
              <div className="chaos-row-id">
                <span className="mono">{node.id || node.url}</span>
                {isLeader && <span className="chaos-leader-tag">leader</span>}
              </div>
              <span className={`chaos-status ${node.reachable ? (alive ? "up" : "killed") : "unreachable"}`}>
                {node.reachable ? (alive ? "alive" : "killed") : "unreachable"}
              </span>
              <div className="chaos-actions">
                <button
                  className="btn btn-kill"
                  disabled={!node.reachable || !alive || pending === `${node.url}:kill`}
                  onClick={() => handle(node.url, "kill", killNode)}
                >
                  Kill
                </button>
                <button
                  className="btn btn-revive"
                  disabled={!node.reachable || alive || pending === `${node.url}:revive`}
                  onClick={() => handle(node.url, "revive", reviveNode)}
                >
                  Revive
                </button>
              </div>
            </div>
          );
        })}
      </div>
    </section>
  );
}
