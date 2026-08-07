import { useClusterSocket } from "./hooks/useClusterSocket";
import { NODE_URLS } from "./lib/api";
import ClusterView from "./components/ClusterView";
import KVConsole from "./components/KVConsole";
import LogViewer from "./components/LogViewer";
import ChaosControls from "./components/ChaosControls";
import "./App.css";

export default function App() {
  const { nodes, leader, term } = useClusterSocket(NODE_URLS);
  const reachableCount = nodes.filter((n) => n.reachable).length;

  return (
    <div className="app-shell">
      <header className="app-header">
        <div className="app-title">
          <span className="app-logo" aria-hidden="true">
            🗳️
          </span>
          <div>
            <h1>Quorum</h1>
            <p>A distributed key-value store built on the Raft consensus algorithm</p>
          </div>
        </div>
        <div className="app-summary">
          <div className="summary-stat">
            <span className="summary-value">{reachableCount}</span>
            <span className="summary-label">/ {nodes.length} nodes up</span>
          </div>
          <div className="summary-stat">
            <span className="summary-value mono">{leader ? leader.id : "—"}</span>
            <span className="summary-label">leader</span>
          </div>
        </div>
      </header>

      <main className="app-grid">
        <ClusterView nodes={nodes} term={term} />
        <div className="app-side-stack">
          <KVConsole nodes={nodes} leader={leader} />
          <ChaosControls nodes={nodes} />
        </div>
        <LogViewer nodes={nodes} />
      </main>

      <footer className="app-footer">
        <a href="https://github.com/jamesmartin6/quorum" target="_blank" rel="noreferrer">
          github.com/jamesmartin6/quorum
        </a>
      </footer>
    </div>
  );
}
