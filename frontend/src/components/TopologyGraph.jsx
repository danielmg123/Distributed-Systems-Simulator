// Renders the simulation's topology as an SVG node graph. Edges come from the static
// per-run adjacency map (GET /api/simulations/{id}/topology); node colors and labels
// come from the live, polled node statuses, so a node turns red the moment it fails and
// the Raft leader is highlighted as leadership moves -- the graph tracks state in real
// time, not just on initial load.
// SIZE leaves padding around the LAYOUT_RADIUS ring so the role caption drawn just below
// the lowest node (see NODE_RADIUS + 13 offset) still falls inside the viewBox.
const SIZE = 360;
const CENTER = SIZE / 2;
const LAYOUT_RADIUS = 120;
const NODE_RADIUS = 24;

function colorFor(node) {
  if (!node || node.status === "FAILED") {
    return { fill: "#fdecea", stroke: "#c62828" };
  }
  if (node.roleLabel === "LEADER") {
    return { fill: "#e3f2fd", stroke: "#1565c0" };
  }
  return { fill: "#e8f5e9", stroke: "#2e7d32" };
}

export default function TopologyGraph({ nodes, topology }) {
  if (!nodes || nodes.length === 0) {
    return null;
  }

  // Stable ordering so nodes keep their position across status polls (numeric-aware so
  // node10 sorts after node2, not after node1).
  const ids = nodes.map((n) => n.id).sort((a, b) => a.localeCompare(b, undefined, { numeric: true }));
  const byId = Object.fromEntries(nodes.map((n) => [n.id, n]));

  const pos = {};
  ids.forEach((id, i) => {
    const angle = (2 * Math.PI * i) / ids.length - Math.PI / 2;
    pos[id] = {
      x: CENTER + LAYOUT_RADIUS * Math.cos(angle),
      y: CENTER + LAYOUT_RADIUS * Math.sin(angle),
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

  return (
    <div className="topology-graph">
      <svg viewBox={`0 0 ${SIZE} ${SIZE}`} className="topology-graph__svg">
        {edges.map(([a, b]) => (
          <line
            key={`${a}--${b}`}
            x1={pos[a].x}
            y1={pos[a].y}
            x2={pos[b].x}
            y2={pos[b].y}
            stroke="#c0c0c0"
            strokeWidth="1.5"
          />
        ))}
        {ids.map((id) => {
          const node = byId[id];
          const { fill, stroke } = colorFor(node);
          const { x, y } = pos[id];
          return (
            <g key={id}>
              <circle cx={x} cy={y} r={NODE_RADIUS} fill={fill} stroke={stroke} strokeWidth="2" />
              <text x={x} y={y + 4} textAnchor="middle" className="topology-graph__id">
                {id}
              </text>
              {/* Role sits just below the circle, not inside it: labels like "CANDIDATE"
                  are wider than the node and were getting clipped against the circle. */}
              {node?.roleLabel && (
                <text x={x} y={y + NODE_RADIUS + 13} textAnchor="middle" className="topology-graph__role">
                  {node.roleLabel}
                </text>
              )}
            </g>
          );
        })}
      </svg>
    </div>
  );
}
