import { useEffect, useRef, useState } from "react";
import { getStatus, getLog } from "../lib/api";

// Despite the filename (kept to match the original project layout), this
// is polling-based rather than a WebSocket event stream: the backend
// exposes /cluster/status and /cluster/log, and polling every ~300ms is
// simple, robust to a node disappearing mid-connection, and visually
// indistinguishable from push updates at this refresh rate. See
// progress.md's "Key decisions" for the full rationale.
const POLL_INTERVAL_MS = 300;

function emptyNode(url) {
  return {
    url,
    id: null,
    status: null,
    log: [],
    reachable: false,
    justCommitted: false,
  };
}

export function useClusterSocket(nodeUrls, intervalMs = POLL_INTERVAL_MS) {
  const [nodes, setNodes] = useState(() => nodeUrls.map(emptyNode));
  const prevCommitRef = useRef({});
  const urlsKey = nodeUrls.join(",");

  useEffect(() => {
    let cancelled = false;
    let timeoutId;

    async function pollOnce() {
      const results = await Promise.all(
        nodeUrls.map(async (url) => {
          try {
            const [status, logResp] = await Promise.all([getStatus(url), getLog(url)]);
            return { url, status, log: logResp.entries || [], reachable: true };
          } catch {
            return { url, status: null, log: [], reachable: false };
          }
        })
      );
      if (cancelled) return;

      setNodes((prev) =>
        results.map((r) => {
          const prevNode = prev.find((p) => p.url === r.url);
          const id = r.status?.id ?? prevNode?.id ?? null;
          const commitIndex = r.status?.commitIndex ?? 0;
          const prevCommit = prevCommitRef.current[r.url] ?? 0;
          const justCommitted = r.reachable && commitIndex > prevCommit;
          prevCommitRef.current[r.url] = commitIndex;
          return {
            url: r.url,
            id,
            status: r.status,
            log: r.log,
            reachable: r.reachable,
            justCommitted,
          };
        })
      );
    }

    // Self-scheduling rather than setInterval: each cycle waits for the
    // previous one to fully finish before scheduling the next. Firing a
    // fixed-rate setInterval instead would let batches of requests pile up
    // faster than the network resolves them under any latency/unreachable
    // node, blowing past the browser's per-origin connection limit and
    // causing healthy requests to queue behind stuck ones until our own
    // AbortController times them out - the "every node shows unreachable"
    // bug this replaced.
    async function loop() {
      await pollOnce();
      if (!cancelled) {
        timeoutId = setTimeout(loop, intervalMs);
      }
    }
    loop();

    return () => {
      cancelled = true;
      clearTimeout(timeoutId);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [urlsKey, intervalMs]);

  const leader = nodes.find((n) => n.reachable && n.status?.role === "leader" && n.status?.alive !== false) || null;
  const term = nodes.reduce((max, n) => Math.max(max, n.status?.term ?? 0), 0);

  return { nodes, leader, term };
}
