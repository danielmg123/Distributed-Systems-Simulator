import { useEffect, useRef, useState } from "react";

// The topology hero. Edges come from the static per-run adjacency map; node color,
// role, and state come from the live, polled node statuses, so the graph tracks the
// cluster in real time. The motion is layered on top:
//   - each node gently drifts on its own out-of-phase sine cycle (pure CSS),
//   - a soft radial glow behind each node is colored by state and transitions smoothly,
//   - edges carry a steady ambient ripple from the leader (Raft heartbeat feel) plus a
//     one-shot pulse from the leader/proposer on propose/commit.
// All animation is driven by CSS plus a single effect that reacts only when the shared
// WebSocket event list grows -- never per message, and no new backend events.

// Above this many nodes the per-node role caption is dropped for plain followers and
// acceptors (which dominate the count and crowd the ring); the leader and candidates
// stay labeled because those are the roles worth calling out.
const ROLE_LABEL_LIMIT = 12;

function stateOf(node) {
  if (!node || node.status === "FAILED") return "failed";
  if (node.roleLabel === "LEADER") return "leader";
  if (node.roleLabel === "CANDIDATE") return "candidate";
  return "active"; // FOLLOWER / ACCEPTOR / anything else healthy
}

const STATE_COLOR = {
  leader: "var(--status-leader)",
  candidate: "var(--status-candidate)",
  active: "var(--status-active)",
  failed: "var(--status-failed)",
};

// Geometry scales with the node count so the ring stays legible from 3 to 20 nodes:
// nodes and their glows shrink while the ring widens, and the viewBox grows to keep
// everything (glow halos + the role caption below the lowest node) in frame. At small N
// this reproduces the original 440 / r26 / ring140 look exactly.
function geometry(n) {
  const r = Math.max(18, Math.min(26, 32 - n)); // node radius
  const ring = Math.min(180, 140 + Math.max(0, n - 12) * 5); // layout radius
  const glow = r + 8;
  const blur = Math.max(4, Math.round(r * 0.23));
  const idFont = Math.round(8 + (r - 18) * 0.625); // 8px at r18 -> 13px at r26
  const size = 2 * (ring + 2 * r + 28);
  return { r, ring, glow, blur, idFont, size, center: size / 2 };
}

export default function TopologyGraph({ nodes, topology, events = [] }) {
  const [pulse, setPulse] = useState(null);
  const lastLenRef = useRef(0);

  // Stable ordering so nodes keep their position across status polls (numeric-aware so
  // node10 sorts after node2). Computed before the early return to keep hook order stable.
  const ids = (nodes || [])
    .map((n) => n.id)
    .sort((a, b) => a.localeCompare(b, undefined, { numeric: true }));
  const byId = Object.fromEntries((nodes || []).map((n) => [n.id, n]));
  const leaderId =
    ids.find((id) => byId[id]?.roleLabel === "LEADER" && byId[id]?.status !== "FAILED") || null;

  // Fire a one-shot edge pulse when a new propose/commit event arrives. This runs only
  // when the event list grows, so it costs nothing per message -- just per high-level
  // event. Commit beats propose if both land in the same batch.
  useEffect(() => {
    if (events.length < lastLenRef.current) lastLenRef.current = 0; // sim reset
    if (events.length === lastLenRef.current) return;
    const fresh = events.slice(lastLenRef.current);
    lastLenRef.current = events.length;

    let kind = null;
    let ev = null;
    for (const e of fresh) {
      if (e.type === "VALUE_COMMITTED") {
        kind = "commit";
        ev = e;
      } else if (e.type === "VALUE_PROPOSED" && kind !== "commit") {
        kind = "propose";
        ev = e;
      }
    }
    if (!kind) return;

    // Commit radiates from the node named in the details ("Node {id} committed value..."),
    // which makes leaderless Paxos show the proposer as the source of consensus. Propose
    // has no node in its details, so it radiates from the leader (Raft / Multi-Paxos) or
    // falls back to a soft global flash when there is no leader.
    let sourceId = leaderId;
    if (kind === "commit") {
      const m = /Node\s+(\S+)\s+committed/.exec(ev.details || "");
      sourceId = (m && byId[m[1]] && m[1]) || leaderId;
    }
    setPulse({ key: `${lastLenRef.current}-${Math.random()}`, sourceId: sourceId || null });
    // byId/leaderId are rebuilt every render; depending on them would re-run this each
    // render. The length guard above is what keeps a pulse from firing twice.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [events]);

  if (!nodes || nodes.length === 0) {
    return null;
  }

  const g = geometry(ids.length);
  const showRole = (role) =>
    role === "LEADER" || role === "CANDIDATE" || ids.length <= ROLE_LABEL_LIMIT;

  const pos = {};
  ids.forEach((id, i) => {
    const angle = (2 * Math.PI * i) / ids.length - Math.PI / 2;
    pos[id] = {
      x: g.center + g.ring * Math.cos(angle),
      y: g.center + g.ring * Math.sin(angle),
    };
  });

  // Dedupe undirected edges so A-B and B-A aren't drawn twice.
  const edges = [];
  const seen = new Set();
  ids.forEach((id) => {
    (topology?.[id] || []).forEach((neighbor) => {
      if (!pos[neighbor]) return;
      const key = [id, neighbor].sort().join("--");
      if (seen.has(key)) return;
      seen.add(key);
      edges.push([id, neighbor]);
    });
  });

  const incident = (src) => (src ? edges.filter(([a, b]) => a === src || b === src) : edges);
  // Orient a pulse line so it always starts at the source and travels outward.
  const oriented = (src, [a, b]) => {
    const other = a === src ? b : a;
    return { x1: pos[src].x, y1: pos[src].y, x2: pos[other].x, y2: pos[other].y };
  };

  return (
    <div className="topology-graph">
      <svg
        viewBox={`0 0 ${g.size} ${g.size}`}
        className="topology-graph__svg"
        role="img"
        aria-label="Cluster topology graph"
      >
        <defs>
          <filter id="topo-glow" x="-75%" y="-75%" width="250%" height="250%">
            <feGaussianBlur stdDeviation={g.blur} />
          </filter>
        </defs>

        {/* Idle edges -- subtle hairlines. */}
        <g className="topo-edges-idle">
          {edges.map(([a, b]) => (
            <line
              key={`${a}--${b}`}
              x1={pos[a].x}
              y1={pos[a].y}
              x2={pos[b].x}
              y2={pos[b].y}
              className="topo-edge"
            />
          ))}
        </g>

        {/* Ambient heartbeat ripple -- only when a leader exists (so Paxos has none). */}
        {leaderId && (
          <g className="topo-edges-ambient">
            {incident(leaderId).map((edge) => (
              <line
                key={`amb-${edge[0]}--${edge[1]}`}
                {...oriented(leaderId, edge)}
                pathLength="100"
                className="topo-pulse topo-pulse--ambient"
              />
            ))}
          </g>
        )}

        {/* One-shot pulse on propose/commit. Keyed so it remounts and replays. */}
        {pulse && (
          <g className="topo-edges-pulse" key={pulse.key}>
            {pulse.sourceId
              ? incident(pulse.sourceId).map((edge) => (
                  <line
                    key={`p-${edge[0]}--${edge[1]}`}
                    {...oriented(pulse.sourceId, edge)}
                    pathLength="100"
                    className="topo-pulse topo-pulse--strong"
                  />
                ))
              : edges.map(([a, b]) => (
                  <line
                    key={`pf-${a}--${b}`}
                    x1={pos[a].x}
                    y1={pos[a].y}
                    x2={pos[b].x}
                    y2={pos[b].y}
                    pathLength="100"
                    className="topo-pulse topo-pulse--flash"
                  />
                ))}
          </g>
        )}

        {/* Nodes -- positioned by the outer translate, then drifted by the nested groups. */}
        {ids.map((id, i) => {
          const node = byId[id];
          const st = stateOf(node);
          const durX = `${7 + (i % 3) * 0.9}s`;
          const durY = `${9 + (i % 4) * 0.7}s`;
          const delayX = `${-(i * 1.3)}s`;
          const delayY = `${-(i * 0.8 + 0.4)}s`;
          return (
            <g
              key={id}
              className={`topo-node topo-node--${st}`}
              transform={`translate(${pos[id].x} ${pos[id].y})`}
              style={{ color: STATE_COLOR[st] }}
            >
              <g
                className="topo-drift-x"
                style={{ animationDuration: durX, animationDelay: delayX }}
              >
                <g
                  className="topo-drift-y"
                  style={{ animationDuration: durY, animationDelay: delayY }}
                >
                  <circle className="topo-glow" r={g.glow} filter="url(#topo-glow)" />
                  <circle className="topo-body" r={g.r} />
                  <text
                    className="topology-graph__id"
                    x="0"
                    y="4"
                    textAnchor="middle"
                    style={{ fontSize: g.idFont }}
                  >
                    {id}
                  </text>
                  {node?.roleLabel && showRole(node.roleLabel) && (
                    <text
                      className="topology-graph__role"
                      x="0"
                      y={g.r + 14}
                      textAnchor="middle"
                    >
                      {node.roleLabel}
                    </text>
                  )}
                </g>
              </g>
            </g>
          );
        })}
      </svg>
    </div>
  );
}
